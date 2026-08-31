use std::{fs, path::PathBuf};

use lopdf::{dictionary, Document, Object, StringFormat};
use orbitrelay_canvas::{
    CanvasPoint, RgbaColor, StrokeBeginPayload, StrokeStyle, StrokeTool, STROKE_BEGAN_EVENT_TYPE,
};
use orbitrelay_core::{Metadata, Timestamp};
use orbitrelay_document::DocumentId;
use orbitrelay_protocol::{
    Action, ActionId, ActionType, ActorId, EventType, MessageEnvelope, MessageId, MessageType,
    Payload, SessionId,
};
use orbitrelay_query::{QueryResult, QueryType};
use orbitrelay_server::DevelopmentCanvasConfig;
use orbitrelay_transport::{
    Authenticate, Hello, InboundCredentials, InboundMessage, OutboundMessage, QueryMessage,
    SubscriptionRequest, CURRENT_PROTOCOL_VERSION, QUERY_PROTOCOL_VERSION,
};
use serde_json::json;
use sha2::{Digest, Sha256};

mod support;

use support::{connect_socket, development_config, next, send, RunningServer};

fn pdf_fixture() -> Vec<u8> {
    let pages_id = (2, 0);
    let mut document = Document::with_version("1.7");
    document.max_id = 20;
    document.objects.insert(
        pages_id,
        dictionary!(
            "Type" => "Pages",
            "Kids" => vec![Object::Reference((10, 0)), Object::Reference((11, 0)), Object::Reference((12, 0))],
            "Count" => 3
        )
        .into(),
    );
    document.objects.insert(
        (1, 0),
        dictionary!("Type" => "Catalog", "Pages" => Object::Reference(pages_id)).into(),
    );
    document.trailer.set("Root", Object::Reference((1, 0)));
    for (id, width, height, rotation) in
        [(10, 612, 792, 0), (11, 800, 600, 90), (12, 500, 500, 180)]
    {
        let mut page = dictionary!(
            "Type" => "Page",
            "Parent" => Object::Reference(pages_id),
            "MediaBox" => vec![0.into(), 0.into(), width.into(), height.into()]
        );
        if rotation != 0 {
            page.set("Rotate", rotation);
        }
        document.objects.insert((id, 0), page.into());
    }
    document.objects.insert(
        (3, 0),
        dictionary!("Title" => Object::String(
            b"Development Lesson".to_vec(),
            StringFormat::Literal
        ))
        .into(),
    );
    document.trailer.set("Info", Object::Reference((3, 0)));
    let mut bytes = Vec::new();
    document.save_to(&mut bytes).expect("fixture should save");
    bytes
}

async fn connect_query_client(url: &str, actor: ActorId, session_id: SessionId) -> support::Socket {
    let mut socket = connect_socket(url).await;
    send(
        &mut socket,
        InboundMessage::Hello(Hello::new(
            vec![QUERY_PROTOCOL_VERSION, CURRENT_PROTOCOL_VERSION],
            vec!["json".to_owned()],
        )),
    )
    .await;
    match next(&mut socket).await {
        OutboundMessage::HelloAccepted(accepted) => {
            assert_eq!(accepted.selected_version(), QUERY_PROTOCOL_VERSION);
        }
        other => panic!("unexpected hello response: {other:?}"),
    }
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
            session_id,
            [
                EventType::new("dev.echoed"),
                EventType::new(STROKE_BEGAN_EVENT_TYPE),
            ],
        )),
    )
    .await;
    assert!(matches!(
        next(&mut socket).await,
        OutboundMessage::SubscriptionAccepted(_)
    ));
    socket
}

async fn query(
    socket: &mut support::Socket,
    query_type: &str,
    payload: serde_json::Value,
) -> (MessageId, QueryResult) {
    let request_id = MessageId::new();
    let query_type = QueryType::new(query_type).expect("valid query type");
    let payload: Payload = serde_json::from_value(payload).expect("object payload");
    send(
        socket,
        InboundMessage::Query(QueryMessage::new(
            QUERY_PROTOCOL_VERSION,
            request_id.clone(),
            query_type,
            payload,
        )),
    )
    .await;
    loop {
        match next(socket).await {
            OutboundMessage::QueryResponse(response) if response.request_id() == &request_id => {
                return (request_id, response.result().clone());
            }
            OutboundMessage::Error(error) => panic!("unexpected transport error: {error:?}"),
            _ => {}
        }
    }
}

fn temp_pdf_path() -> PathBuf {
    std::env::temp_dir().join(format!("orbitrelay-discovery-{}.pdf", MessageId::new()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocol_02_lists_and_gets_bootstrapped_pdf_metadata() {
    let path = temp_pdf_path();
    let bytes = pdf_fixture();
    fs::write(&path, &bytes).expect("fixture should be written");
    let session_id = SessionId::new();
    let config = development_config()
        .with_development_canvas(DevelopmentCanvasConfig::new().with_session_id(session_id.clone()))
        .with_development_pdf_path(&path);
    let server = RunningServer::start(config).await;
    let actor_id = ActorId::new();
    let mut client = connect_query_client(&server.url, actor_id.clone(), session_id.clone()).await;

    let (_, list) = query(
        &mut client,
        "document.list",
        json!({ "session_id": session_id }),
    )
    .await;
    let QueryResult::Success(payload) = list else {
        panic!("document.list should succeed")
    };
    let list_value = serde_json::to_value(payload).expect("list payload");
    let documents = list_value
        .get("documents")
        .and_then(serde_json::Value::as_array)
        .expect("document list array");
    assert_eq!(documents.len(), 1);
    let document_id = documents[0]
        .get("document_id")
        .and_then(serde_json::Value::as_str)
        .expect("document id")
        .to_owned();
    assert_eq!(documents[0]["title"], "Development Lesson");
    assert_eq!(documents[0]["document_type"], "pdf");
    assert_eq!(documents[0]["page_count"], 3);

    let (_, get) = query(
        &mut client,
        "document.get",
        json!({ "document_id": document_id }),
    )
    .await;
    let QueryResult::Success(payload) = get else {
        panic!("document.get should succeed")
    };
    let view = serde_json::to_value(payload).expect("view payload");
    assert_eq!(view["source_asset"]["media_type"], "application/pdf");
    assert_eq!(view["source_asset"]["byte_length"], bytes.len());
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(Sha256::digest(&bytes).as_slice());
    assert_eq!(
        view["source_asset"]["content_hash"],
        orbitrelay_asset::ContentHash::from_bytes(digest).to_string()
    );
    assert_eq!(
        view["source_asset"]["original_filename"],
        path.file_name()
            .and_then(|name| name.to_str())
            .expect("fixture filename")
    );
    let pages = view["document"]["pages"].as_array().expect("pages");
    let page_canvases = view["page_canvases"].as_array().expect("page canvases");
    assert_eq!(pages.len(), 3);
    assert_eq!(page_canvases.len(), 3);
    for (index, page) in pages.iter().enumerate() {
        assert_eq!(page["page_index"], index);
        assert_eq!(
            page["overlay_canvas_id"],
            page_canvases[index]["canvas"]["canvas_id"]
        );
        assert_eq!(
            page_canvases[index]["canvas"]["session_id"],
            view["document"]["session_id"]
        );
        assert!(page_canvases[index]["canvas"]["layer_ids"]
            .as_array()
            .unwrap()
            .contains(&page_canvases[index]["canvas"]["default_layer_id"]));
    }
    let encoded = serde_json::to_string(&view).expect("view JSON");
    assert!(!encoded.contains("base64"));
    assert!(!encoded.contains("download"));
    assert!(!encoded.contains(path.to_string_lossy().as_ref()));

    let page_canvas = &page_canvases[0]["canvas"];
    let canvas_id =
        serde_json::from_value(page_canvas["canvas_id"].clone()).expect("canvas id should decode");
    let layer_id = serde_json::from_value(page_canvas["default_layer_id"].clone())
        .expect("layer id should decode");
    let stroke_id = orbitrelay_canvas::StrokeId::new();
    let point = CanvasPoint::new(10.0, 10.0).expect("point should be valid");
    let style = StrokeStyle::new(2.0, RgbaColor::new(20, 30, 40, 255)).expect("style");
    let begin = StrokeBeginPayload::new(
        canvas_id,
        layer_id,
        stroke_id,
        StrokeTool::Pen,
        style,
        0,
        [point],
    )
    .expect("stroke payload should be valid");
    let action = Action::new(
        ActionId::new(),
        session_id.clone(),
        actor_id,
        ActionType::new("canvas.stroke.begin"),
        Timestamp::now_utc(),
        serde_json::from_value(serde_json::to_value(begin).expect("stroke payload JSON"))
            .expect("stroke payload object"),
        Metadata::new(),
    );
    let action_id = action.id().clone();
    send(
        &mut client,
        InboundMessage::Action(MessageEnvelope::new(
            QUERY_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            action,
        )),
    )
    .await;
    let mut saw_ack = false;
    let mut saw_event = false;
    while !saw_ack || !saw_event {
        match next(&mut client).await {
            OutboundMessage::ActionAcknowledgement(ack) if ack.action_id() == &action_id => {
                saw_ack = true;
            }
            OutboundMessage::Event(event) if event.payload().action_id() == &action_id => {
                assert_eq!(
                    event.payload().event_type().as_str(),
                    STROKE_BEGAN_EVENT_TYPE
                );
                saw_event = true;
            }
            OutboundMessage::Error(error) => panic!("stroke action failed: {error:?}"),
            _ => {}
        }
    }

    client.close(None).await.expect("client close");
    server.shutdown().await;
    fs::remove_file(path).expect("fixture should be removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocol_02_without_pdf_returns_empty_list_and_rejects_other_session() {
    let session_id = SessionId::new();
    let server = RunningServer::start(development_config().with_development_canvas(
        DevelopmentCanvasConfig::new().with_session_id(session_id.clone()),
    ))
    .await;
    let mut client = connect_query_client(&server.url, ActorId::new(), session_id.clone()).await;
    let (_, list) = query(
        &mut client,
        "document.list",
        json!({ "session_id": session_id }),
    )
    .await;
    let QueryResult::Success(payload) = list else {
        panic!("empty document list should succeed")
    };
    assert_eq!(
        serde_json::to_value(payload).unwrap()["documents"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    let (_, denied) = query(
        &mut client,
        "document.list",
        json!({ "session_id": SessionId::new() }),
    )
    .await;
    assert!(
        matches!(denied, QueryResult::Error(error) if error.code() == orbitrelay_query::QueryFailureCode::Unauthorized)
    );
    let (_, missing) = query(
        &mut client,
        "document.get",
        json!({ "document_id": DocumentId::new() }),
    )
    .await;
    assert!(
        matches!(missing, QueryResult::Error(error) if error.code() == orbitrelay_query::QueryFailureCode::NotFound)
    );
    client.close(None).await.expect("client close");
    server.shutdown().await;
}
