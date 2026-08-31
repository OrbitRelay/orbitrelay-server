mod support;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use orbitrelay_core::Version;
use orbitrelay_protocol::{
    Action, ActionId, ActionType, ActorId, MessageEnvelope, MessageId, MessageType, Payload,
    SessionId,
};
use orbitrelay_server::{ServerConfig, WebSocketListenerConfig};
use orbitrelay_transport::{
    Authenticate, InboundCredentials, InboundMessage, TransportErrorCode, CURRENT_PROTOCOL_VERSION,
};
use tokio::io::AsyncReadExt;
use tokio_tungstenite::tungstenite::Message;

use support::{
    connect_socket, development_config, expect_error, negotiate, send, RunningServer, TestClient,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abrupt_disconnect_releases_the_connection_permit() {
    let config = development_config().with_websocket_listener(
        WebSocketListenerConfig::default()
            .with_bind_addr("127.0.0.1:0".parse().expect("valid address"))
            .with_max_connections(1),
    );
    let server = RunningServer::start(config).await;
    let client = TestClient::connect(&server.url, ActorId::new(), SessionId::new()).await;
    drop(client.socket);

    let replacement = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match tokio_tungstenite::connect_async(&server.url).await {
                Ok((socket, _)) => break socket,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("permit should be released after abrupt disconnect");
    drop(replacement);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_handshake_times_out_and_releases_the_permit() {
    let listener = WebSocketListenerConfig::default()
        .with_bind_addr("127.0.0.1:0".parse().expect("valid address"))
        .with_max_connections(1)
        .with_handshake_timeout_milliseconds(50);
    let server = RunningServer::start(
        ServerConfig::default()
            .with_development_mode(true)
            .with_websocket_listener(listener),
    )
    .await;
    let address = server
        .url
        .strip_prefix("ws://")
        .and_then(|value| value.strip_suffix("/ws"))
        .expect("test URL should have expected form");
    let mut raw = tokio::net::TcpStream::connect(address)
        .await
        .expect("raw TCP should connect");
    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(2), raw.read(&mut byte))
        .await
        .expect("server should close incomplete handshake")
        .expect("TCP read should complete");
    assert_eq!(read, 0);
    drop(raw);

    let socket = connect_socket(&server.url).await;
    drop(socket);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_websocket_rejects_invalid_protocol_inputs_without_panicking() {
    let server = RunningServer::start(development_config()).await;

    let mut incompatible = connect_socket(&server.url).await;
    let response = negotiate(&mut incompatible, vec![Version::new(9, 0, 0)]).await;
    match response {
        orbitrelay_transport::OutboundMessage::Error(error) => {
            assert_eq!(error.code(), TransportErrorCode::UnsupportedVersion);
        }
        other => panic!("expected unsupported version, got {other:?}"),
    }

    let mut duplicate_hello = connect_socket(&server.url).await;
    let _ = negotiate(&mut duplicate_hello, vec![CURRENT_PROTOCOL_VERSION]).await;
    send(
        &mut duplicate_hello,
        InboundMessage::Hello(orbitrelay_transport::Hello::new(
            vec![CURRENT_PROTOCOL_VERSION],
            vec!["json".to_owned()],
        )),
    )
    .await;
    expect_error(&mut duplicate_hello, TransportErrorCode::InternalError).await;

    let mut unauthenticated = connect_socket(&server.url).await;
    let _ = negotiate(&mut unauthenticated, vec![CURRENT_PROTOCOL_VERSION]).await;
    send(
        &mut unauthenticated,
        InboundMessage::Subscribe(orbitrelay_transport::SubscriptionRequest::new(
            MessageId::new(),
            SessionId::new(),
            std::iter::empty(),
        )),
    )
    .await;
    expect_error(
        &mut unauthenticated,
        TransportErrorCode::AuthenticationRequired,
    )
    .await;

    let actor_id = ActorId::new();
    send(
        &mut unauthenticated,
        InboundMessage::Authenticate(Authenticate::new(
            MessageId::new(),
            InboundCredentials::new("development", actor_id.to_string()),
        )),
    )
    .await;
    let forged = Action::new(
        ActionId::new(),
        SessionId::new(),
        ActorId::new(),
        ActionType::new("dev.echo"),
        orbitrelay_core::Timestamp::now_utc(),
        Payload::new(),
        orbitrelay_core::Metadata::new(),
    );
    send(
        &mut unauthenticated,
        InboundMessage::Action(MessageEnvelope::new(
            CURRENT_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            forged,
        )),
    )
    .await;
    expect_error(&mut unauthenticated, TransportErrorCode::IdentityMismatch).await;

    let mut invalid_json = connect_socket(&server.url).await;
    invalid_json
        .send(Message::Text("not-json".into()))
        .await
        .expect("invalid JSON frame should send");
    expect_error(&mut invalid_json, TransportErrorCode::InvalidMessage).await;

    let mut binary = connect_socket(&server.url).await;
    binary
        .send(Message::Binary(vec![1, 2, 3].into()))
        .await
        .expect("binary frame should send");
    expect_error(&mut binary, TransportErrorCode::InvalidMessage).await;
    let closed = tokio::time::timeout(Duration::from_secs(2), binary.next())
        .await
        .expect("binary connection should close");
    assert!(closed.is_none() || matches!(closed, Some(Ok(Message::Close(_)))));

    drop(incompatible);
    drop(duplicate_hello);
    drop(unauthenticated);
    drop(invalid_json);
    server.shutdown().await;
}
