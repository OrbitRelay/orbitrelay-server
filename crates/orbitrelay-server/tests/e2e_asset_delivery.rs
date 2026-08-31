use std::{fs, path::PathBuf};

use lopdf::{dictionary, Document, Object, StringFormat};
use orbitrelay_protocol::{ActorId, MessageId, Payload, SessionId};
use orbitrelay_query::{QueryResult, QueryType};
use orbitrelay_server::{AssetAccessAuthorization, AssetDeliveryConfig, DevelopmentCanvasConfig};
use orbitrelay_transport::{
    Authenticate, Hello, InboundCredentials, InboundMessage, OutboundMessage, QueryMessage,
    SubscriptionRequest, QUERY_PROTOCOL_VERSION,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

mod support;

use support::{connect_socket, development_config, next, send, RunningServer};

fn pdf_fixture() -> Vec<u8> {
    let pages_id = (2, 0);
    let mut document = Document::with_version("1.7");
    document.max_id = 20;
    document.objects.insert(
        pages_id,
        dictionary!("Type" => "Pages", "Kids" => vec![Object::Reference((10, 0)), Object::Reference((11, 0)), Object::Reference((12, 0))], "Count" => 3).into(),
    );
    document.objects.insert(
        (1, 0),
        dictionary!("Type" => "Catalog", "Pages" => Object::Reference(pages_id)).into(),
    );
    document.trailer.set("Root", Object::Reference((1, 0)));
    for (id, width, height, rotation) in
        [(10, 612, 792, 0), (11, 800, 600, 90), (12, 500, 500, 180)]
    {
        let mut page = dictionary!("Type" => "Page", "Parent" => Object::Reference(pages_id), "MediaBox" => vec![0.into(), 0.into(), width.into(), height.into()]);
        if rotation != 0 {
            page.set("Rotate", rotation);
        }
        document.objects.insert((id, 0), page.into());
    }
    document.objects.insert(
        (3, 0),
        dictionary!("Title" => Object::String(b"Delivery Lesson".to_vec(), StringFormat::Literal))
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
            session_id,
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

async fn query(socket: &mut support::Socket, kind: &str, value: serde_json::Value) -> QueryResult {
    let request_id = MessageId::new();
    let payload: Payload = serde_json::from_value(value).expect("object payload");
    send(
        socket,
        InboundMessage::Query(QueryMessage::new(
            QUERY_PROTOCOL_VERSION,
            request_id.clone(),
            QueryType::new(kind).expect("query type"),
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

async fn http_request(
    address: std::net::SocketAddr,
    path: &str,
    token: Option<&str>,
    range: Option<&str>,
) -> (u16, Vec<u8>, String) {
    http_request_method(address, "GET", path, token, range).await
}

async fn http_request_method(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    range: Option<&str>,
) -> (u16, Vec<u8>, String) {
    let mut stream = TcpStream::connect(address).await.expect("HTTP connect");
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n");
    if let Some(token) = token {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    if let Some(range) = range {
        request.push_str(&format!("Range: {range}\r\n"));
    }
    request.push_str("Connection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("HTTP request");
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.expect("HTTP response");
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP headers");
    let head = String::from_utf8(bytes[..split].to_vec()).expect("HTTP head");
    let status = head
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status number");
    (status, bytes[split + 4..].to_vec(), head)
}

fn temp_pdf_path() -> PathBuf {
    std::env::temp_dir().join(format!("orbitrelay-delivery-{}.pdf", MessageId::new()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_resolve_and_http_download_form_a_closed_loop() {
    let path = temp_pdf_path();
    let bytes = pdf_fixture();
    fs::write(&path, &bytes).expect("fixture");
    let session_id = SessionId::new();
    let config = development_config()
        .with_development_canvas(DevelopmentCanvasConfig::new().with_session_id(session_id.clone()))
        .with_development_pdf_path(&path)
        .with_asset_delivery(
            AssetDeliveryConfig::default()
                .with_enabled(true)
                .with_listen_addr("127.0.0.1:0".parse().expect("address")),
        );
    let server = RunningServer::start(config).await;
    let asset_address = server.asset_address.expect("asset listener");
    let actor_id = ActorId::new();
    let mut client = connect_query_client(&server.url, actor_id, session_id.clone()).await;
    let QueryResult::Success(list) = query(
        &mut client,
        "document.list",
        json!({"session_id": session_id}),
    )
    .await
    else {
        panic!("list")
    };
    let list = serde_json::to_value(list).expect("list value");
    let document_id = list
        .get("documents")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("document_id"))
        .and_then(serde_json::Value::as_str)
        .expect("document id")
        .to_owned();
    let QueryResult::Success(view) = query(
        &mut client,
        "document.get",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("get")
    };
    let view = serde_json::to_value(view).expect("view value");
    let asset_id = view
        .get("source_asset")
        .and_then(|asset| asset.get("asset_id"))
        .and_then(serde_json::Value::as_str)
        .expect("asset id");
    let expected_hash = view
        .get("source_asset")
        .and_then(|asset| asset.get("content_hash"))
        .and_then(serde_json::Value::as_str)
        .expect("hash");
    let QueryResult::Success(access) = query(
        &mut client,
        "asset.access.resolve",
        json!({"document_id": document_id}),
    )
    .await
    else {
        panic!("access")
    };
    let access = serde_json::to_value(access).expect("access value");
    assert_eq!(access["asset_id"], asset_id);
    assert_eq!(access["delivery_kind"], "http");
    let token =
        match serde_json::from_value::<orbitrelay_server::AssetAccessDescriptor>(access.clone())
            .expect("descriptor")
            .authorization()
        {
            AssetAccessAuthorization::Bearer { token } => token.to_owned(),
        };
    assert!(!access.to_string().contains("bytes"));
    let (status, full_body, head) = http_request(
        asset_address,
        &format!("/assets/{asset_id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(full_body, bytes);
    assert!(head.contains("Accept-Ranges: bytes"));
    assert!(head.contains("Content-Length:"));
    assert!(head.contains(&format!("ETag: \"sha256-{expected_hash}\"")));
    let (status, suffix_body, _) = http_request(
        asset_address,
        &format!("/assets/{asset_id}"),
        Some(&token),
        Some("bytes=-16"),
    )
    .await;
    assert_eq!(status, 206);
    assert_eq!(suffix_body, bytes[bytes.len() - 16..]);
    let (status, head_body, head) = http_request_method(
        asset_address,
        "HEAD",
        &format!("/assets/{asset_id}"),
        Some(&token),
        Some("bytes=0-1"),
    )
    .await;
    assert_eq!(status, 200);
    assert!(head_body.is_empty());
    assert!(head.contains(&format!("Content-Length: {}", bytes.len())));
    let (status, prefix_body, head) = http_request(
        asset_address,
        &format!("/assets/{asset_id}"),
        Some(&token),
        Some("bytes=0-3"),
    )
    .await;
    assert_eq!(status, 206);
    assert_eq!(prefix_body, bytes[..4]);
    assert!(head.contains(&format!("Content-Range: bytes 0-3/{}", bytes.len())));
    let (status, open_body, _) = http_request(
        asset_address,
        &format!("/assets/{asset_id}"),
        Some(&token),
        Some(&format!("bytes={}-", bytes.len() - 4)),
    )
    .await;
    assert_eq!(status, 206);
    assert_eq!(open_body, bytes[bytes.len() - 4..]);
    let (status, _, head) = http_request(
        asset_address,
        &format!("/assets/{asset_id}"),
        Some(&token),
        Some("bytes=999999-"),
    )
    .await;
    assert_eq!(status, 416);
    assert!(head.contains(&format!("Content-Range: bytes */{}", bytes.len())));
    let (status, _, _) = http_request(
        asset_address,
        &format!("/assets/{asset_id}"),
        Some(&token),
        Some("bytes=a-b"),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _, _) =
        http_request(asset_address, &format!("/assets/{asset_id}"), None, None).await;
    assert_eq!(status, 401);
    let (status, _, _) = http_request(
        asset_address,
        &format!("/assets/{}", orbitrelay_asset::AssetId::new()),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 401);
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(Sha256::digest(&full_body).as_slice());
    assert_eq!(
        orbitrelay_asset::ContentHash::from_bytes(digest).to_string(),
        orbitrelay_asset::ContentHash::parse(expected_hash)
            .expect("hash")
            .to_string()
    );
    client.close(None).await.expect("close");
    server.shutdown().await;
    fs::remove_file(path).expect("cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enabled_delivery_starts_without_a_pdf_and_keeps_empty_discovery() {
    let session_id = SessionId::new();
    let config = development_config()
        .with_development_canvas(DevelopmentCanvasConfig::new().with_session_id(session_id.clone()))
        .with_asset_delivery(
            AssetDeliveryConfig::default()
                .with_enabled(true)
                .with_listen_addr("127.0.0.1:0".parse().expect("address")),
        );
    let server = RunningServer::start(config).await;
    assert!(server.asset_address.is_some());
    let mut client = connect_query_client(&server.url, ActorId::new(), session_id.clone()).await;
    let QueryResult::Success(list) = query(
        &mut client,
        "document.list",
        json!({"session_id": session_id}),
    )
    .await
    else {
        panic!("list")
    };
    assert_eq!(
        serde_json::to_value(list).expect("list value")["documents"]
            .as_array()
            .expect("documents")
            .len(),
        0
    );
    client.close(None).await.expect("close");
    server.shutdown().await;
}
