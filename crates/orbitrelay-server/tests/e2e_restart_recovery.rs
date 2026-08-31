mod support;

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use lopdf::{dictionary, Document, Object, StringFormat};
use orbitrelay_canvas::{
    CanvasPoint, RgbaColor, StrokeAppendPayload, StrokeBeginPayload, StrokeCancelPayload,
    StrokeEndPayload, StrokeId, StrokeRemovePayload, StrokeStyle, StrokeTool,
    STROKE_BEGAN_EVENT_TYPE, STROKE_CANCELLED_EVENT_TYPE, STROKE_COMPLETED_EVENT_TYPE,
    STROKE_POINTS_APPENDED_EVENT_TYPE, STROKE_REMOVED_EVENT_TYPE,
};
use orbitrelay_protocol::{ActorId, Event, EventType, MessageId, Payload, SessionId};
use orbitrelay_query::{QueryResult, QueryType};
use orbitrelay_server::{AssetAccessAuthorization, CanvasHistoryPageDto};
use orbitrelay_transport::{
    Authenticate, Hello, InboundCredentials, InboundMessage, OutboundMessage, QueryMessage,
    SubscriptionRequest, QUERY_PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use support::{connect_socket, next, send, Socket, TestClient};

const CANVAS_EVENTS: [&str; 5] = [
    STROKE_BEGAN_EVENT_TYPE,
    STROKE_POINTS_APPENDED_EVENT_TYPE,
    STROKE_COMPLETED_EVENT_TYPE,
    STROKE_CANCELLED_EVENT_TYPE,
    STROKE_REMOVED_EVENT_TYPE,
];

fn pdf_fixture() -> Vec<u8> {
    let pages_id = (2, 0);
    let mut document = Document::with_version("1.7");
    document.max_id = 20;
    document.objects.insert(
        pages_id,
        dictionary!("Type" => "Pages", "Kids" => vec![Object::Reference((10, 0)), Object::Reference((11, 0))], "Count" => 2).into(),
    );
    document.objects.insert(
        (1, 0),
        dictionary!("Type" => "Catalog", "Pages" => Object::Reference(pages_id)).into(),
    );
    document.trailer.set("Root", Object::Reference((1, 0)));
    for (id, width, height, rotation) in [(10, 612, 792, 0), (11, 800, 600, 90)] {
        let mut page = dictionary!("Type" => "Page", "Parent" => Object::Reference(pages_id), "MediaBox" => vec![0.into(), 0.into(), width.into(), height.into()]);
        if rotation != 0 {
            page.set("Rotate", rotation);
        }
        document.objects.insert((id, 0), page.into());
    }
    document.objects.insert(
        (3, 0),
        dictionary!("Title" => Object::String(b"Restart Lesson".to_vec(), StringFormat::Literal))
            .into(),
    );
    document.trailer.set("Info", Object::Reference((3, 0)));
    let mut bytes = Vec::new();
    document.save_to(&mut bytes).expect("fixture should save");
    bytes
}

struct ProcessServer {
    child: Child,
    lines: mpsc::Receiver<String>,
    stderr: Option<thread::JoinHandle<String>>,
}

impl ProcessServer {
    fn spawn(root: &Path, pdf_path: &Path, session_id: &SessionId) -> Self {
        Self::spawn_with_modes(root, pdf_path, session_id, true, true, true)
    }

    fn spawn_with_modes(
        root: &Path,
        pdf_path: &Path,
        session_id: &SessionId,
        persistent_event: bool,
        persistent_asset: bool,
        persistent_catalog: bool,
    ) -> Self {
        let event_path = root.join("events.sqlite");
        let catalog_path = root.join("catalog.sqlite");
        let asset_root = root.join("assets");
        let mut command = Command::new(env!("CARGO_BIN_EXE_orbitrelay-server"));
        command
            .env("ORBITRELAY_DEVELOPMENT_MODE", "true")
            .env("ORBITRELAY_DEVELOPMENT_PDF_PATH", pdf_path)
            .env(
                "ORBITRELAY_DEVELOPMENT_CANVAS_SESSION_ID",
                session_id.to_string(),
            )
            .env("ORBITRELAY_BIND_ADDR", "127.0.0.1:0")
            .env("ORBITRELAY_ASSET_DELIVERY_ENABLED", "true")
            .env("ORBITRELAY_ASSET_LISTEN_ADDR", "127.0.0.1:0")
            .env("ORBITRELAY_TEST_SHUTDOWN_STDIN", "1")
            .env("ORBITRELAY_HISTORY_STORE_SCAN_LIMIT", "4")
            .env("RUST_LOG", "info")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if persistent_event {
            command.env("ORBITRELAY_EVENT_STORE_PATH", &event_path);
        } else {
            command.env_remove("ORBITRELAY_EVENT_STORE_PATH");
        }
        if persistent_asset {
            command.env("ORBITRELAY_ASSET_STORE_DIR", &asset_root);
        } else {
            command.env_remove("ORBITRELAY_ASSET_STORE_DIR");
        }
        if persistent_catalog {
            command.env("ORBITRELAY_CATALOG_STORE_PATH", &catalog_path);
        } else {
            command.env_remove("ORBITRELAY_CATALOG_STORE_PATH");
        }
        let mut child = command.spawn().expect("server process should start");
        let stdout = child.stdout.take().expect("stdout should be piped");
        let stderr = child.stderr.take().expect("stderr should be piped");
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let _ = sender.send(line);
            }
        });
        let stderr_reader = thread::spawn(move || {
            let mut output = String::new();
            let _ = BufReader::new(stderr).read_to_string(&mut output);
            output
        });
        Self {
            child,
            lines,
            stderr: Some(stderr_reader),
        }
    }

    fn wait_for(&self, prefix: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = self
                .lines
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("server did not report {prefix}"));
            if let Some(value) = line.strip_prefix(prefix) {
                return value.to_owned();
            }
        }
    }

    fn stop_graceful(mut self) -> String {
        if let Some(mut stdin) = self.child.stdin.take() {
            let _ = stdin.write_all(b"shutdown\n");
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                _ => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    break;
                }
            }
        }
        self.stderr
            .take()
            .and_then(|reader| reader.join().ok())
            .unwrap_or_default()
    }

    fn stop_forced(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.stderr.take() {
            let _ = reader.join();
        }
    }

    fn expect_not_ready(mut self) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Ok(line) = self.lines.recv_timeout(Duration::from_millis(50)) {
                assert!(
                    !line.starts_with("ORBITRELAY_LISTENING="),
                    "failed process unexpectedly reported a WebSocket listener"
                );
                assert!(
                    !line.starts_with("ORBITRELAY_ASSET_LISTENING="),
                    "failed process unexpectedly reported an Asset listener"
                );
            }
            if let Some(status) = self
                .child
                .try_wait()
                .expect("process status should be readable")
            {
                break status;
            }
            assert!(Instant::now() < deadline, "failed process did not exit");
        };
        assert!(
            !status.success(),
            "corrupt persistent data must fail startup"
        );
        self.stderr
            .take()
            .and_then(|reader| reader.join().ok())
            .unwrap_or_default()
    }
}

impl Drop for ProcessServer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

async fn connect_query(url: &str, actor: ActorId, session: SessionId) -> Socket {
    let mut socket = connect_socket(url).await;
    send(
        &mut socket,
        InboundMessage::Hello(Hello::new(
            vec![QUERY_PROTOCOL_VERSION],
            vec!["json".to_owned()],
        )),
    )
    .await;
    assert!(matches!(
        next(&mut socket).await,
        OutboundMessage::HelloAccepted(_)
    ));
    send(
        &mut socket,
        InboundMessage::Authenticate(Authenticate::new(
            MessageId::new(),
            InboundCredentials::new("development", actor.to_string()),
        )),
    )
    .await;
    send(
        &mut socket,
        InboundMessage::Subscribe(SubscriptionRequest::new(
            MessageId::new(),
            session,
            std::iter::empty(),
        )),
    )
    .await;
    assert!(matches!(
        next(&mut socket).await,
        OutboundMessage::SubscriptionAccepted(_)
    ));
    socket
}

async fn query(socket: &mut Socket, kind: &str, value: Value) -> QueryResult {
    let request_id = MessageId::new();
    let payload: Payload =
        serde_json::from_value(value).expect("query payload should be an object");
    send(
        socket,
        InboundMessage::Query(QueryMessage::new(
            QUERY_PROTOCOL_VERSION,
            request_id.clone(),
            QueryType::new(kind).expect("query type should be valid"),
            payload,
        )),
    )
    .await;
    loop {
        match next(socket).await {
            OutboundMessage::QueryResponse(response) if response.request_id() == &request_id => {
                return response.result().clone()
            }
            OutboundMessage::Error(error) => panic!("unexpected transport error: {error:?}"),
            _ => {}
        }
    }
}

async fn history(socket: &mut Socket, canvas_id: &str) -> (Vec<Value>, Value, Option<Value>) {
    let mut events = Vec::new();
    let mut request = json!({"canvas_id": canvas_id});
    loop {
        let result = query(socket, "canvas.history.page", request).await;
        let QueryResult::Success(payload) = result else {
            panic!("history query should succeed: {result:?}")
        };
        let page_value = serde_json::to_value(payload).expect("history payload should encode");
        let page: CanvasHistoryPageDto =
            serde_json::from_value(page_value.clone()).expect("history page should decode");
        let checkpoint = serde_json::to_value(page.checkpoint()).expect("checkpoint should encode");
        events.extend(
            page.events()
                .iter()
                .map(|event| serde_json::to_value(event).expect("event should encode")),
        );
        let cursor = page
            .next_cursor()
            .map(|value| serde_json::to_value(value).expect("cursor should encode"));
        if page.complete() {
            return (events, checkpoint, cursor);
        }
        request = json!({
            "canvas_id": canvas_id,
            "checkpoint": checkpoint,
            "cursor": cursor.expect("incomplete page has cursor"),
        });
    }
}

async fn first_history_continuation(socket: &mut Socket, canvas_id: &str) -> (Value, Value) {
    let result = query(
        socket,
        "canvas.history.page",
        json!({"canvas_id": canvas_id}),
    )
    .await;
    let QueryResult::Success(payload) = result else {
        panic!("first history query should succeed: {result:?}")
    };
    let page: CanvasHistoryPageDto = serde_json::from_value(
        serde_json::to_value(payload).expect("history payload should encode"),
    )
    .expect("history page should decode");
    assert!(
        !page.complete(),
        "fixture should require a continuation page"
    );
    (
        serde_json::to_value(page.checkpoint()).expect("checkpoint should encode"),
        serde_json::to_value(page.next_cursor().expect("cursor should exist"))
            .expect("cursor should encode"),
    )
}

async fn http_get(address: &str, path: &str, token: &str) -> (u16, Vec<u8>, String) {
    let mut stream = TcpStream::connect(address)
        .await
        .expect("HTTP listener should connect");
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("HTTP request should write");
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .expect("HTTP response should read");
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP headers");
    let head = String::from_utf8(bytes[..split].to_vec()).expect("HTTP headers should be UTF-8");
    let status = head
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status number");
    (status, bytes[split + 4..].to_vec(), head)
}

fn page_canvas_ids(view: &Value) -> Vec<(String, String)> {
    view["page_canvases"]
        .as_array()
        .expect("page canvases")
        .iter()
        .map(|entry| {
            (
                entry["canvas"]["canvas_id"]
                    .as_str()
                    .expect("canvas id")
                    .to_owned(),
                entry["canvas"]["default_layer_id"]
                    .as_str()
                    .expect("layer id")
                    .to_owned(),
            )
        })
        .collect()
}

fn canvas_payload<T>(payload: T) -> Payload
where
    Payload: TryFrom<T, Error = orbitrelay_canvas::CanvasError>,
{
    Payload::try_from(payload).expect("Canvas payload should encode")
}

async fn send_canvas<T>(client: &mut TestClient, action_type: &str, payload: T) -> Event
where
    Payload: TryFrom<T, Error = orbitrelay_canvas::CanvasError>,
{
    let action_id = client
        .send_action(
            orbitrelay_protocol::ActionType::new(action_type),
            canvas_payload(payload),
        )
        .await;
    let (_, event) = client.action_result(&action_id).await;
    event
}

fn point(value: f64) -> CanvasPoint {
    CanvasPoint::new(value, value).expect("point should be valid")
}

fn style() -> StrokeStyle {
    StrokeStyle::new(2.0, RgbaColor::new(30, 60, 90, 255)).expect("style should be valid")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_server_recovers_document_asset_canvas_and_history_across_process_restart() {
    let root = std::env::temp_dir().join(format!("orbitrelay-restart-{}", MessageId::new()));
    fs::create_dir_all(&root).expect("temporary root should be created");
    let pdf_path = root.join("development.pdf");
    let pdf_bytes = pdf_fixture();
    fs::write(&pdf_path, &pdf_bytes).expect("PDF fixture should be written");
    let session_id = SessionId::new();

    let server_a = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let ws_a = server_a.wait_for("ORBITRELAY_LISTENING=");
    let asset_a = server_a.wait_for("ORBITRELAY_ASSET_LISTENING=");
    let mut query_a = connect_query(&ws_a, ActorId::new(), session_id.clone()).await;
    let QueryResult::Success(list_a) = query(
        &mut query_a,
        "document.list",
        json!({"session_id": session_id}),
    )
    .await
    else {
        panic!("document.list should succeed")
    };
    let list_a = serde_json::to_value(list_a).expect("list should encode");
    let document_id = list_a["documents"][0]["document_id"]
        .as_str()
        .expect("document id")
        .to_owned();
    let QueryResult::Success(view_a_payload) = query(
        &mut query_a,
        "document.get",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("document.get should succeed")
    };
    let view_a = serde_json::to_value(view_a_payload).expect("view should encode");
    let asset_id = view_a["source_asset"]["asset_id"]
        .as_str()
        .expect("asset id")
        .to_owned();
    let expected_hash = view_a["source_asset"]["content_hash"]
        .as_str()
        .expect("content hash")
        .to_owned();
    let page_canvases = page_canvas_ids(&view_a);
    assert_eq!(page_canvases.len(), 2);
    assert_eq!(view_a["source_asset"]["byte_length"], pdf_bytes.len());
    let QueryResult::Success(access_payload) = query(
        &mut query_a,
        "asset.access.resolve",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("asset access should succeed")
    };
    let access = serde_json::to_value(access_payload).expect("access should encode");
    let token = match serde_json::from_value::<orbitrelay_server::AssetAccessDescriptor>(access)
        .expect("access descriptor")
        .authorization()
    {
        AssetAccessAuthorization::Bearer { token } => token.to_owned(),
    };
    let (status, body, headers) = http_get(&asset_a, &format!("/assets/{asset_id}"), &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, pdf_bytes);
    assert!(headers.contains(&format!("ETag: \"sha256-{expected_hash}\"")));
    query_a
        .close(None)
        .await
        .expect("query client should close");

    let actor_a = ActorId::new();
    let actor_b = ActorId::new();
    let mut client_a = TestClient::connect_with_events(
        &ws_a,
        actor_a.clone(),
        session_id.clone(),
        CANVAS_EVENTS.iter().copied().map(EventType::new),
    )
    .await;
    let mut client_b = TestClient::connect_with_events(
        &ws_a,
        actor_b,
        session_id.clone(),
        CANVAS_EVENTS.iter().copied().map(EventType::new),
    )
    .await;
    let canvas0: orbitrelay_canvas::CanvasId = page_canvases[0].0.parse().expect("Canvas id");
    let layer0: orbitrelay_canvas::LayerId = page_canvases[0].1.parse().expect("Layer id");
    let canvas1: orbitrelay_canvas::CanvasId = page_canvases[1].0.parse().expect("Canvas id");
    let layer1: orbitrelay_canvas::LayerId = page_canvases[1].1.parse().expect("Layer id");
    let mut event_ids = Vec::new();
    let completed = StrokeId::new();
    event_ids.push(
        send_canvas(
            &mut client_a,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas0.clone(),
                layer0.clone(),
                completed.clone(),
                StrokeTool::Pen,
                style(),
                0,
                [point(1.0)],
            )
            .expect("begin"),
        )
        .await
        .id()
        .to_string(),
    );
    event_ids.push(
        send_canvas(
            &mut client_a,
            "canvas.stroke.append",
            StrokeAppendPayload::new(canvas0.clone(), completed.clone(), 1, [point(2.0)])
                .expect("append"),
        )
        .await
        .id()
        .to_string(),
    );
    event_ids.push(
        send_canvas(
            &mut client_a,
            "canvas.stroke.end",
            StrokeEndPayload::new(canvas0.clone(), completed.clone(), 1),
        )
        .await
        .id()
        .to_string(),
    );
    let multi = StrokeId::new();
    event_ids.push(
        send_canvas(
            &mut client_b,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas0.clone(),
                layer0.clone(),
                multi.clone(),
                StrokeTool::Pen,
                style(),
                0,
                [point(3.0)],
            )
            .expect("begin"),
        )
        .await
        .id()
        .to_string(),
    );
    event_ids.push(
        send_canvas(
            &mut client_b,
            "canvas.stroke.append",
            StrokeAppendPayload::new(canvas0.clone(), multi.clone(), 1, [point(4.0)])
                .expect("append"),
        )
        .await
        .id()
        .to_string(),
    );
    event_ids.push(
        send_canvas(
            &mut client_b,
            "canvas.stroke.append",
            StrokeAppendPayload::new(canvas0.clone(), multi.clone(), 2, [point(5.0)])
                .expect("append"),
        )
        .await
        .id()
        .to_string(),
    );
    event_ids.push(
        send_canvas(
            &mut client_b,
            "canvas.stroke.end",
            StrokeEndPayload::new(canvas0.clone(), multi.clone(), 2),
        )
        .await
        .id()
        .to_string(),
    );
    let cancelled = StrokeId::new();
    event_ids.push(
        send_canvas(
            &mut client_a,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas0.clone(),
                layer0.clone(),
                cancelled.clone(),
                StrokeTool::Pen,
                style(),
                0,
                [point(6.0)],
            )
            .expect("begin"),
        )
        .await
        .id()
        .to_string(),
    );
    event_ids.push(
        send_canvas(
            &mut client_a,
            "canvas.stroke.cancel",
            StrokeCancelPayload::new(canvas0.clone(), cancelled.clone(), 0),
        )
        .await
        .id()
        .to_string(),
    );
    let removed = StrokeId::new();
    event_ids.push(
        send_canvas(
            &mut client_a,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas0.clone(),
                layer0.clone(),
                removed.clone(),
                StrokeTool::Pen,
                style(),
                0,
                [point(7.0)],
            )
            .expect("begin"),
        )
        .await
        .id()
        .to_string(),
    );
    event_ids.push(
        send_canvas(
            &mut client_a,
            "canvas.stroke.end",
            StrokeEndPayload::new(canvas0.clone(), removed.clone(), 0),
        )
        .await
        .id()
        .to_string(),
    );
    event_ids.push(
        send_canvas(
            &mut client_a,
            "canvas.stroke.remove",
            StrokeRemovePayload::new(canvas0.clone(), removed.clone()),
        )
        .await
        .id()
        .to_string(),
    );
    let page1_stroke = StrokeId::new();
    event_ids.push(
        send_canvas(
            &mut client_b,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas1.clone(),
                layer1,
                page1_stroke,
                StrokeTool::Pen,
                style(),
                0,
                [point(8.0)],
            )
            .expect("begin"),
        )
        .await
        .id()
        .to_string(),
    );
    let history_before_restart = {
        let mut history_probe = connect_query(&ws_a, ActorId::new(), session_id.clone()).await;
        let (events, _, _) = history(&mut history_probe, &page_canvases[0].0).await;
        history_probe
            .close(None)
            .await
            .expect("history probe should close");
        events
    };
    let (checkpoint_before_restart, cursor_before_restart) = {
        let mut history_probe = connect_query(&ws_a, ActorId::new(), session_id.clone()).await;
        let tokens = first_history_continuation(&mut history_probe, &page_canvases[0].0).await;
        history_probe
            .close(None)
            .await
            .expect("history probe should close");
        tokens
    };
    client_a.close().await;
    client_b.close().await;

    let server_a_logs = server_a.stop_graceful();
    assert!(server_a_logs.contains("OrbitRelay server shutdown completed"));
    assert!(server_a_logs.contains("restart_recovery_capable=true"));
    fs::remove_file(&pdf_path).expect("source PDF should be removable after publication");

    let server_b = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let ws_b = server_b.wait_for("ORBITRELAY_LISTENING=");
    let asset_b = server_b.wait_for("ORBITRELAY_ASSET_LISTENING=");
    let mut query_b = connect_query(&ws_b, ActorId::new(), session_id.clone()).await;
    let QueryResult::Success(list_b_payload) = query(
        &mut query_b,
        "document.list",
        json!({"session_id": session_id}),
    )
    .await
    else {
        panic!("document.list should recover")
    };
    let list_b = serde_json::to_value(list_b_payload).expect("list should encode");
    let QueryResult::Success(view_b_payload) = query(
        &mut query_b,
        "document.get",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("document.get should recover")
    };
    let view_b = serde_json::to_value(view_b_payload).expect("view should encode");
    assert_eq!(list_b, list_a);
    assert_eq!(view_b, view_a);
    let QueryResult::Success(access_b_payload) = query(
        &mut query_b,
        "asset.access.resolve",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("asset access should recover")
    };
    let access_b = serde_json::to_value(access_b_payload).expect("access should encode");
    let token_b = match serde_json::from_value::<orbitrelay_server::AssetAccessDescriptor>(access_b)
        .expect("access descriptor")
        .authorization()
    {
        AssetAccessAuthorization::Bearer { token } => token.to_owned(),
    };
    let (status, body, _) = http_get(&asset_b, &format!("/assets/{asset_id}"), &token_b).await;
    assert_eq!(status, 200);
    assert_eq!(body, pdf_bytes);
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(Sha256::digest(&body).as_slice());
    assert_eq!(
        orbitrelay_asset::ContentHash::from_bytes(digest).to_string(),
        expected_hash
    );
    let (history_b, _final_checkpoint, final_cursor) = {
        let mut socket = connect_query(&ws_b, ActorId::new(), session_id.clone()).await;
        let result = history(&mut socket, &page_canvases[0].0).await;
        socket
            .close(None)
            .await
            .expect("history client should close");
        result
    };
    let recovered_ids = history_b
        .iter()
        .map(|event| event["event_id"].as_str().expect("event id"))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(recovered_ids, event_ids[..event_ids.len() - 1].to_vec());
    assert_eq!(history_b, history_before_restart);
    assert!(final_cursor.is_none());
    let history_page1 = {
        let mut socket = connect_query(&ws_b, ActorId::new(), session_id.clone()).await;
        let result = history(&mut socket, &page_canvases[1].0).await;
        socket
            .close(None)
            .await
            .expect("Page 1 history client should close");
        result.0
    };
    assert_eq!(
        history_page1
            .iter()
            .map(|event| event["event_id"].as_str().expect("Page 1 event id"))
            .collect::<Vec<_>>(),
        vec![event_ids
            .last()
            .expect("Page 1 event should exist")
            .as_str()]
    );
    let old_continuation = query(
        &mut query_b,
        "canvas.history.page",
        json!({
            "canvas_id": page_canvases[0].0,
            "checkpoint": checkpoint_before_restart,
            "cursor": cursor_before_restart,
        }),
    )
    .await;
    assert!(matches!(old_continuation, QueryResult::Error(_)));

    let mut restarted_client = TestClient::connect_with_events(
        &ws_b,
        actor_a,
        session_id.clone(),
        CANVAS_EVENTS.iter().copied().map(EventType::new),
    )
    .await;
    let new_event = send_canvas(
        &mut restarted_client,
        "canvas.stroke.begin",
        StrokeBeginPayload::new(
            canvas0,
            layer0,
            StrokeId::new(),
            StrokeTool::Pen,
            style(),
            0,
            [point(99.0)],
        )
        .expect("new begin"),
    )
    .await;
    assert!(!event_ids.contains(&new_event.id().to_string()));
    restarted_client.close().await;
    let (history_after_restart, _, _) = {
        let mut late_join = connect_query(&ws_b, ActorId::new(), session_id.clone()).await;
        let result = history(&mut late_join, &page_canvases[0].0).await;
        late_join.close(None).await.expect("late join should close");
        result
    };
    let mut expected_after_restart = event_ids[..event_ids.len() - 1].to_vec();
    expected_after_restart.push(new_event.id().to_string());
    assert_eq!(
        history_after_restart
            .iter()
            .map(|event| event["event_id"].as_str().expect("event id").to_owned())
            .collect::<Vec<_>>(),
        expected_after_restart
    );
    query_b
        .close(None)
        .await
        .expect("query client should close");
    let server_b_logs = server_b.stop_graceful();
    assert!(server_b_logs.contains("OrbitRelay server shutdown completed"));
    assert!(server_b_logs.contains("restart_recovery_capable=true"));

    assert!(root.join("events.sqlite").exists());
    assert!(root.join("catalog.sqlite").exists());
    assert!(root.join("assets").exists());
    let _ = fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_process_termination_reopens_committed_catalog() {
    let root = std::env::temp_dir().join(format!("orbitrelay-unclean-{}", MessageId::new()));
    fs::create_dir_all(&root).expect("temporary root should be created");
    let pdf_path = root.join("development.pdf");
    fs::write(&pdf_path, pdf_fixture()).expect("PDF fixture should be written");
    let session_id = SessionId::new();

    let server_a = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let _ws_a = server_a.wait_for("ORBITRELAY_LISTENING=");
    let _asset_a = server_a.wait_for("ORBITRELAY_ASSET_LISTENING=");
    server_a.stop_forced();

    let server_b = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let ws_b = server_b.wait_for("ORBITRELAY_LISTENING=");
    let _asset_b = server_b.wait_for("ORBITRELAY_ASSET_LISTENING=");
    let mut client = connect_query(&ws_b, ActorId::new(), session_id.clone()).await;
    let QueryResult::Success(payload) = query(
        &mut client,
        "document.list",
        json!({"session_id": session_id}),
    )
    .await
    else {
        panic!("committed Document should survive forced termination")
    };
    assert_eq!(
        serde_json::to_value(payload).expect("list should encode")["documents"]
            .as_array()
            .expect("documents")
            .len(),
        1
    );
    client.close(None).await.expect("query client should close");
    let logs = server_b.stop_graceful();
    assert!(logs.contains("OrbitRelay server shutdown completed"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_persistence_runs_but_is_not_restart_capable() {
    let root = std::env::temp_dir().join(format!("orbitrelay-mixed-{}", MessageId::new()));
    fs::create_dir_all(&root).expect("temporary root should be created");
    let pdf_path = root.join("development.pdf");
    fs::write(&pdf_path, pdf_fixture()).expect("PDF fixture should be written");
    let session_id = SessionId::new();

    let server = ProcessServer::spawn_with_modes(&root, &pdf_path, &session_id, true, true, false);
    let _ws = server.wait_for("ORBITRELAY_LISTENING=");
    let _asset = server.wait_for("ORBITRELAY_ASSET_LISTENING=");
    let logs = server.stop_graceful();
    assert!(logs.contains("event_store_persistent=true"));
    assert!(logs.contains("asset_store_persistent=true"));
    assert!(logs.contains("catalog_store_persistent=false"));
    assert!(logs.contains("restart_recovery_capable=false"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_process_mode_remains_available() {
    let root = std::env::temp_dir().join(format!("orbitrelay-memory-{}", MessageId::new()));
    fs::create_dir_all(&root).expect("temporary root should be created");
    let pdf_path = root.join("development.pdf");
    fs::write(&pdf_path, pdf_fixture()).expect("PDF fixture should be written");
    let session_id = SessionId::new();

    let server =
        ProcessServer::spawn_with_modes(&root, &pdf_path, &session_id, false, false, false);
    let _ws = server.wait_for("ORBITRELAY_LISTENING=");
    let _asset = server.wait_for("ORBITRELAY_ASSET_LISTENING=");
    let logs = server.stop_graceful();
    assert!(logs.contains("event_store_persistent=false"));
    assert!(logs.contains("asset_store_persistent=false"));
    assert!(logs.contains("catalog_store_persistent=false"));
    assert!(logs.contains("restart_recovery_capable=false"));
    let _ = fs::remove_dir_all(root);
}

async fn prepare_persistent_graph() -> (PathBuf, PathBuf, SessionId, Value) {
    let root = std::env::temp_dir().join(format!("orbitrelay-matrix-{}", MessageId::new()));
    fs::create_dir_all(&root).expect("temporary root should be created");
    let pdf_path = root.join("development.pdf");
    fs::write(&pdf_path, pdf_fixture()).expect("PDF fixture should be written");
    let session_id = SessionId::new();
    let server = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let ws = server.wait_for("ORBITRELAY_LISTENING=");
    let _asset = server.wait_for("ORBITRELAY_ASSET_LISTENING=");
    let mut client = connect_query(&ws, ActorId::new(), session_id.clone()).await;
    let QueryResult::Success(list) = query(
        &mut client,
        "document.list",
        json!({"session_id": session_id}),
    )
    .await
    else {
        panic!("persistent graph should list during setup")
    };
    let list = serde_json::to_value(list).expect("list should encode");
    assert_eq!(list["documents"].as_array().expect("documents").len(), 1);
    let document_id = list["documents"][0]["document_id"]
        .as_str()
        .expect("document id")
        .to_owned();
    let QueryResult::Success(view) = query(
        &mut client,
        "document.get",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("persistent graph should get during setup")
    };
    let view = serde_json::to_value(view).expect("view should encode");
    client.close(None).await.expect("setup client should close");
    let logs = server.stop_graceful();
    assert!(logs.contains("OrbitRelay server shutdown completed"));
    (root, pdf_path, session_id, view)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_development_source_is_ignored_after_publication() {
    let (root, pdf_path, session_id, expected_view) = prepare_persistent_graph().await;
    fs::write(&pdf_path, b"this is not the published PDF")
        .expect("changed development source should be writable");

    let server = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let ws = server.wait_for("ORBITRELAY_LISTENING=");
    let _asset = server.wait_for("ORBITRELAY_ASSET_LISTENING=");
    let mut client = connect_query(&ws, ActorId::new(), session_id.clone()).await;
    let QueryResult::Success(list) = query(
        &mut client,
        "document.list",
        json!({"session_id": session_id}),
    )
    .await
    else {
        panic!("changed-source document.list should succeed")
    };
    let list = serde_json::to_value(list).expect("changed-source list should encode");
    let document_id = list["documents"][0]["document_id"]
        .as_str()
        .expect("changed-source document id");
    let QueryResult::Success(view) = query(
        &mut client,
        "document.get",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("changed-source document.get should succeed")
    };
    assert_eq!(
        serde_json::to_value(view).expect("view should encode"),
        expected_view
    );
    client
        .close(None)
        .await
        .expect("changed-source query should close");
    let logs = server.stop_graceful();
    assert!(logs.contains("restart_recovery_capable=true"));
    let _ = fs::remove_dir_all(root);
}

fn mutate_catalog(root: &Path, view: &Value, kind: &str) {
    let connection = rusqlite::Connection::open(root.join("catalog.sqlite"))
        .expect("catalog database should open for deterministic corruption");
    connection
        .execute_batch("PRAGMA foreign_keys = OFF;")
        .expect("foreign key override should succeed for test corruption");
    match kind {
        "missing_canvas" => {
            let canvas_id = view["page_canvases"][0]["canvas"]["canvas_id"]
                .as_str()
                .expect("canvas id");
            connection
                .execute("DELETE FROM canvases WHERE canvas_id = ?1", [canvas_id])
                .expect("canvas deletion should succeed");
        }
        "missing_layer" => {
            let layer_id = view["page_canvases"][0]["canvas"]["default_layer_id"]
                .as_str()
                .expect("layer id");
            connection
                .execute("DELETE FROM layers WHERE layer_id = ?1", [layer_id])
                .expect("layer deletion should succeed");
        }
        "wrong_default_layer" => {
            connection
                .execute(
                    "UPDATE canvases SET default_layer_id = 'not-an-existing-layer'",
                    [],
                )
                .expect("default layer corruption should succeed");
        }
        "geometry_mismatch" => {
            connection
                .execute("UPDATE document_pages SET width = width + 1", [])
                .expect("geometry corruption should succeed");
        }
        _ => panic!("unknown Catalog corruption {kind}"),
    }
}

fn mutate_schema(root: &Path, store: &str) {
    let path = match store {
        "event" => root.join("events.sqlite"),
        "asset" => root.join("assets").join("metadata.sqlite"),
        "catalog" => root.join("catalog.sqlite"),
        _ => panic!("unknown schema store {store}"),
    };
    let connection = rusqlite::Connection::open(path).expect("schema database should open");
    connection
        .execute(
            "UPDATE storage_metadata SET value = '999' WHERE key = 'schema_version'",
            [],
        )
        .expect("schema version mutation should succeed");
}

fn mutate_event(root: &Path) {
    let connection = rusqlite::Connection::open(root.join("events.sqlite"))
        .expect("EventStore database should open for deterministic corruption");
    connection
        .execute_batch(
            "INSERT INTO events (append_sequence, event_id, session_id, actor_id, action_id, occurred_at_json, event_type, payload_json, metadata_json) VALUES (0, 'not-an-id', 'not-an-id', 'not-an-id', 'not-an-id', 'null', 'event', '{}', '{}')",
        )
        .expect("malformed Event row should be insertable for test corruption");
}

fn mutate_asset(root: &Path, view: &Value) {
    let asset_id = view["source_asset"]["asset_id"].as_str().expect("asset id");
    let blob = root
        .join("assets")
        .join("blobs")
        .join(format!("{asset_id}.blob"));
    let mut bytes = fs::read(&blob).expect("published blob should exist");
    bytes[0] ^= 0x01;
    fs::write(blob, bytes).expect("corrupted blob should be written");
}

fn standalone_identity(logs: &str) -> (String, String) {
    let line = logs
        .lines()
        .find(|line| line.contains("OrbitRelay development Canvas is enabled"))
        .expect("development Canvas log should be present");
    let field = |name: &str| {
        line.split_whitespace()
            .find_map(|part| part.strip_prefix(&format!("{name}=")))
            .map(str::to_owned)
            .expect("development Canvas identity field should be present")
    };
    (field("canvas_id"), field("default_layer_id"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_failure_matrix_fails_closed_before_listener_ready() {
    for kind in [
        "missing_canvas",
        "missing_layer",
        "wrong_default_layer",
        "geometry_mismatch",
    ] {
        let (root, pdf_path, session_id, view) = prepare_persistent_graph().await;
        mutate_catalog(&root, &view, kind);
        let server = ProcessServer::spawn(&root, &pdf_path, &session_id);
        let logs = server.expect_not_ready();
        assert!(!logs.contains("ORBITRELAY_LISTENING="));
        let _ = fs::remove_dir_all(root);
    }

    for store in ["event", "asset", "catalog"] {
        let (root, pdf_path, session_id, _view) = prepare_persistent_graph().await;
        mutate_schema(&root, store);
        let server = ProcessServer::spawn(&root, &pdf_path, &session_id);
        let _logs = server.expect_not_ready();
        let _ = fs::remove_dir_all(root);
    }

    let (root, pdf_path, session_id, _view) = prepare_persistent_graph().await;
    mutate_event(&root);
    let server = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let _logs = server.expect_not_ready();
    let _ = fs::remove_dir_all(root);

    let (root, pdf_path, session_id, view) = prepare_persistent_graph().await;
    mutate_asset(&root, &view);
    let server = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let _logs = server.expect_not_ready();
    let _ = fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ten_process_restarts_keep_one_graph_and_grow_history() {
    let root = std::env::temp_dir().join(format!("orbitrelay-loop-{}", MessageId::new()));
    fs::create_dir_all(&root).expect("temporary root should be created");
    let pdf_path = root.join("development.pdf");
    fs::write(&pdf_path, pdf_fixture()).expect("PDF fixture should be written");
    let session_id = SessionId::new();
    let actor_id = ActorId::new();
    let mut stable_view = None;
    let mut stable_standalone = None;
    let mut history_ids = Vec::new();

    for round in 0..10 {
        let server = ProcessServer::spawn(&root, &pdf_path, &session_id);
        let ws = server.wait_for("ORBITRELAY_LISTENING=");
        let _asset = server.wait_for("ORBITRELAY_ASSET_LISTENING=");
        let mut query_client = connect_query(&ws, actor_id.clone(), session_id.clone()).await;
        let QueryResult::Success(list) = query(
            &mut query_client,
            "document.list",
            json!({"session_id": session_id}),
        )
        .await
        else {
            panic!("restart loop document.list should succeed")
        };
        let list = serde_json::to_value(list).expect("list should encode");
        assert_eq!(list["documents"].as_array().expect("documents").len(), 1);
        let document_id = list["documents"][0]["document_id"]
            .as_str()
            .expect("document id")
            .to_owned();
        let QueryResult::Success(view) = query(
            &mut query_client,
            "document.get",
            json!({"document_id": document_id}),
        )
        .await
        else {
            panic!("restart loop document.get should succeed")
        };
        let view = serde_json::to_value(view).expect("view should encode");
        if let Some(expected) = stable_view.as_ref() {
            assert_eq!(&view, expected);
        } else {
            stable_view = Some(view.clone());
        }
        let canvas_id: orbitrelay_canvas::CanvasId = view["page_canvases"][0]["canvas"]
            ["canvas_id"]
            .as_str()
            .expect("canvas id")
            .parse()
            .expect("Canvas id should parse");
        let layer_id: orbitrelay_canvas::LayerId = view["page_canvases"][0]["canvas"]
            ["default_layer_id"]
            .as_str()
            .expect("layer id")
            .parse()
            .expect("Layer id should parse");
        let mut action_client = TestClient::connect_with_events(
            &ws,
            actor_id.clone(),
            session_id.clone(),
            CANVAS_EVENTS.iter().copied().map(EventType::new),
        )
        .await;
        let event = send_canvas(
            &mut action_client,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas_id,
                layer_id,
                StrokeId::new(),
                StrokeTool::Pen,
                style(),
                0,
                [point(round as f64)],
            )
            .expect("loop stroke should be valid"),
        )
        .await;
        history_ids.push(event.id().to_string());
        action_client.close().await;
        query_client
            .close(None)
            .await
            .expect("loop query should close");
        let logs = server.stop_graceful();
        assert!(logs.contains("OrbitRelay server shutdown completed"));
        let standalone = standalone_identity(&logs);
        if let Some(expected) = stable_standalone.as_ref() {
            assert_eq!(&standalone, expected);
        } else {
            stable_standalone = Some(standalone);
        }
        if round == 0 {
            fs::remove_file(&pdf_path).expect("source PDF should be removable after first round");
        }
    }

    assert_eq!(history_ids.len(), 10);
    let server = ProcessServer::spawn(&root, &pdf_path, &session_id);
    let ws = server.wait_for("ORBITRELAY_LISTENING=");
    let _asset = server.wait_for("ORBITRELAY_ASSET_LISTENING=");
    let mut client = connect_query(&ws, actor_id, session_id.clone()).await;
    let QueryResult::Success(list) = query(
        &mut client,
        "document.list",
        json!({"session_id": session_id}),
    )
    .await
    else {
        panic!("final restart loop document.list should succeed")
    };
    let list = serde_json::to_value(list).expect("final list should encode");
    let documents = list["documents"].as_array().expect("final documents");
    assert_eq!(documents.len(), 1, "restart must not duplicate Documents");
    let document_id = documents[0]["document_id"]
        .as_str()
        .expect("final document id");
    let QueryResult::Success(final_view) = query(
        &mut client,
        "document.get",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("final restart loop document.get should succeed")
    };
    let final_view = serde_json::to_value(final_view).expect("final view should encode");
    assert_eq!(stable_view.as_ref(), Some(&final_view));
    let canvas_id = final_view["page_canvases"][0]["canvas"]["canvas_id"]
        .as_str()
        .expect("final canvas id");
    let (history, _checkpoint, _cursor) = history(&mut client, canvas_id).await;
    let actual_ids = history
        .iter()
        .map(|event| event["event_id"].as_str().expect("history event id"))
        .collect::<Vec<_>>();
    assert_eq!(actual_ids, history_ids);
    client
        .close(None)
        .await
        .expect("final loop query should close");
    let logs = server.stop_graceful();
    assert!(logs.contains("OrbitRelay server shutdown completed"));
    let _ = fs::remove_dir_all(root);
}
