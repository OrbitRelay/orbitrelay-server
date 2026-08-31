use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use orbitrelay_core::{Metadata, Timestamp};
use orbitrelay_node::NodeState;
use orbitrelay_protocol::{
    Action, ActionId, ActionType, ActorId, EventType, MessageEnvelope, MessageId, MessageType,
    Payload, SessionId,
};
use orbitrelay_runtime::{
    ActionAuthorizer, ActionHandler, AuthorizationError, EventDraft, HandlerError, RuntimeContext,
};
use orbitrelay_server::{
    AssetDeliveryConfig, Bootstrap, ServerApplication, ServerDependencies, ServerError,
    WebSocketListenerConfig,
};
use orbitrelay_transport::{
    ActorBinding, Authenticate, Hello, IdentityError, IdentityResolver, IdentitySource,
    InboundCredentials, InboundMessage, OutboundMessage, SubscriptionAuthorizationError,
    SubscriptionAuthorizer, SubscriptionRequest, CURRENT_PROTOCOL_VERSION,
};
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

struct AllowActionAuthorizer;

#[async_trait]
impl ActionAuthorizer for AllowActionAuthorizer {
    async fn authorize(&self, _action: &Action) -> Result<(), AuthorizationError> {
        Ok(())
    }
}

struct StaticIdentityResolver {
    actor_id: ActorId,
}

#[async_trait]
impl IdentityResolver for StaticIdentityResolver {
    async fn resolve(
        &self,
        _connection_id: &orbitrelay_transport::ConnectionId,
        _credentials: &InboundCredentials,
    ) -> Result<ActorBinding, IdentityError> {
        Ok(ActorBinding::new(
            self.actor_id.clone(),
            IdentitySource::new("integration_test"),
        ))
    }
}

struct AllowSubscriptionAuthorizer;

#[async_trait]
impl SubscriptionAuthorizer for AllowSubscriptionAuthorizer {
    async fn authorize(
        &self,
        _binding: &ActorBinding,
        _request: &SubscriptionRequest,
    ) -> Result<(), SubscriptionAuthorizationError> {
        Ok(())
    }
}

struct EchoHandler;

#[async_trait]
impl ActionHandler for EchoHandler {
    async fn validate(
        &self,
        _action: &Action,
        _context: &RuntimeContext,
    ) -> Result<(), HandlerError> {
        Ok(())
    }

    async fn handle(
        &self,
        _action: &Action,
        _context: &RuntimeContext,
    ) -> Result<Vec<EventDraft>, HandlerError> {
        Ok(vec![EventDraft::new(
            EventType::new("test.echoed"),
            Payload::new(),
            Metadata::new(),
        )])
    }
}

fn config() -> orbitrelay_server::ServerConfig {
    let listener = WebSocketListenerConfig::default()
        .with_bind_addr("127.0.0.1:0".parse().expect("valid address"));
    orbitrelay_server::ServerConfig::default().with_websocket_listener(listener)
}

fn dependencies(actor_id: ActorId) -> ServerDependencies {
    ServerDependencies::new(
        Arc::new(AllowActionAuthorizer),
        Arc::new(StaticIdentityResolver { actor_id }),
        Arc::new(AllowSubscriptionAuthorizer),
    )
}

async fn next_application_message<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
) -> OutboundMessage
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .expect("server response should arrive")
            .expect("server should send a frame")
            .expect("WebSocket frame should be valid");
        if let Message::Text(text) = frame {
            return serde_json::from_slice(text.as_bytes()).expect("valid transport JSON");
        }
    }
}

async fn send_message<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    message: InboundMessage,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket
        .send(Message::Text(
            serde_json::to_string(&message)
                .expect("transport message should serialize")
                .into(),
        ))
        .await
        .expect("WebSocket message should send");
}

async fn authenticate<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    send_message(
        socket,
        InboundMessage::Hello(Hello::new(
            vec![CURRENT_PROTOCOL_VERSION],
            vec!["json".to_owned()],
        )),
    )
    .await;
    assert!(matches!(
        next_application_message(socket).await,
        OutboundMessage::HelloAccepted(_)
    ));
    send_message(
        socket,
        InboundMessage::Authenticate(Authenticate::new(
            MessageId::new(),
            InboundCredentials::new("test", "credential"),
        )),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_action_round_trip_persists_and_broadcasts() {
    let actor_id = ActorId::new();
    let session_id = SessionId::new();
    let config = config();
    let deps = dependencies(actor_id.clone());
    let bootstrap = Bootstrap::new(config.clone(), deps.action_authorizer());
    let context = bootstrap
        .initialize()
        .await
        .expect("bootstrap should succeed");
    context
        .runtime()
        .registry()
        .register(ActionType::new("test.echo"), Arc::new(EchoHandler))
        .expect("test handler should register");
    let mut application = ServerApplication::new(context.clone(), config, deps);
    application.start().await.expect("listener should bind");
    assert_eq!(context.node_state(), NodeState::Ready);
    let address = application
        .local_addr()
        .expect("bound address should exist");
    let cancellation = application.cancellation_token();
    let server_task = tokio::spawn(async move { application.serve().await });

    let (mut client, _) = connect_async(format!("ws://{address}/ws"))
        .await
        .expect("WebSocket handshake should succeed");
    authenticate(&mut client).await;
    send_message(
        &mut client,
        InboundMessage::Subscribe(SubscriptionRequest::new(
            MessageId::new(),
            session_id.clone(),
            [EventType::new("test.echoed")],
        )),
    )
    .await;
    let subscription_id = match next_application_message(&mut client).await {
        OutboundMessage::SubscriptionAccepted(message) => message.subscription_id().clone(),
        other => panic!("expected subscription acceptance, got {other:?}"),
    };

    let action = Action::new(
        ActionId::new(),
        session_id,
        actor_id,
        ActionType::new("test.echo"),
        Timestamp::now_utc(),
        Payload::new(),
        Metadata::new(),
    );
    let action_id = action.id().clone();
    send_message(
        &mut client,
        InboundMessage::Action(MessageEnvelope::new(
            CURRENT_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            action,
        )),
    )
    .await;

    let mut saw_ack = false;
    let mut saw_event = false;
    let mut generated_event_id = None;
    for _ in 0..3 {
        match next_application_message(&mut client).await {
            OutboundMessage::ActionAcknowledgement(message) => {
                assert_eq!(message.action_id(), &action_id);
                assert_eq!(message.generated_event_ids().len(), 1);
                generated_event_id = message.generated_event_ids().first().cloned();
                saw_ack = true;
            }
            OutboundMessage::Event(envelope) => {
                assert_eq!(envelope.payload().action_id(), &action_id);
                saw_event = true;
            }
            other => panic!("unexpected application message: {other:?}"),
        }
        if saw_ack && saw_event {
            break;
        }
    }
    assert!(saw_ack && saw_event);
    let generated_event_id = generated_event_id.expect("ack should include an event id");
    assert!(context
        .event_store()
        .get(&generated_event_id)
        .await
        .expect("event store lookup should succeed")
        .is_some());

    send_message(
        &mut client,
        InboundMessage::Unsubscribe(orbitrelay_transport::Unsubscribe::new(
            MessageId::new(),
            subscription_id,
        )),
    )
    .await;
    let _ = next_application_message(&mut client).await;
    let _ = client.close(None).await;
    cancellation.cancel();
    let _ = server_task.await.expect("server should stop cleanly");
}

#[tokio::test]
async fn websocket_listener_rejects_paths_other_than_ws() {
    let actor_id = ActorId::new();
    let config = config();
    let deps = dependencies(actor_id);
    let bootstrap = Bootstrap::new(config.clone(), deps.action_authorizer());
    let context = bootstrap
        .initialize()
        .await
        .expect("bootstrap should succeed");
    let mut application = ServerApplication::new(context, config, deps);
    application.start().await.expect("listener should bind");
    let address = application
        .local_addr()
        .expect("bound address should exist");
    let cancellation = application.cancellation_token();
    let server_task = tokio::spawn(async move { application.serve().await });

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        connect_async(format!("ws://{address}/not-ws")),
    )
    .await
    .expect("wrong-path handshake should complete");
    assert!(
        result.is_err(),
        "unexpected paths must fail the HTTP upgrade"
    );

    cancellation.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(8), server_task)
        .await
        .expect("server shutdown should complete")
        .expect("server task should join")
        .expect("server should stop cleanly");
}

#[tokio::test]
async fn websocket_listener_rejects_connections_at_capacity() {
    let listener = WebSocketListenerConfig::default()
        .with_bind_addr("127.0.0.1:0".parse().expect("valid address"))
        .with_max_connections(1)
        .with_handshake_timeout_milliseconds(1_000);
    let config = orbitrelay_server::ServerConfig::default().with_websocket_listener(listener);
    let deps = dependencies(ActorId::new());
    let bootstrap = Bootstrap::new(config.clone(), deps.action_authorizer());
    let context = bootstrap
        .initialize()
        .await
        .expect("bootstrap should succeed");
    let mut application = ServerApplication::new(context, config, deps);
    application.start().await.expect("listener should bind");
    let address = application
        .local_addr()
        .expect("bound address should exist");
    let cancellation = application.cancellation_token();
    let server_task = tokio::spawn(async move { application.serve().await });

    let first_stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("first TCP connection should be accepted");
    let second = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        connect_async(format!("ws://{address}/ws")),
    )
    .await
    .expect("capacity rejection should complete")
    .expect_err("second connection should be rejected");
    assert!(second.to_string().contains("IO") || second.to_string().contains("handshake"));

    drop(first_stream);
    cancellation.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(8), server_task)
        .await
        .expect("server shutdown should complete")
        .expect("server task should join")
        .expect("server should stop cleanly");
}

#[tokio::test]
async fn bind_failure_rolls_starting_node_back_without_ready() {
    let occupied = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = occupied.local_addr().expect("address should exist");
    let config = WebSocketListenerConfig::default().with_bind_addr(address);
    let config = orbitrelay_server::ServerConfig::default().with_websocket_listener(config);
    let actor_id = ActorId::new();
    let deps = dependencies(actor_id);
    let bootstrap = Bootstrap::new(config.clone(), deps.action_authorizer());
    let context = bootstrap
        .initialize()
        .await
        .expect("bootstrap should succeed");
    let node_id = context.node_id().clone();
    let mut application = ServerApplication::new(context.clone(), config, deps);

    let error = application
        .start()
        .await
        .expect_err("occupied address should fail");
    assert!(matches!(error, ServerError::Listener { .. }));
    assert_eq!(
        context.lifecycle().state(),
        orbitrelay_server::LifecycleState::Stopped
    );
    assert!(context
        .node_registry()
        .get(&node_id)
        .await
        .expect("registry lookup should succeed")
        .is_none());
    drop(occupied);
}

#[tokio::test]
async fn asset_bind_failure_closes_already_bound_websocket() {
    let websocket_guard = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("temporary WebSocket listener should bind");
    let websocket_address = websocket_guard
        .local_addr()
        .expect("WebSocket address should exist");
    drop(websocket_guard);

    let asset_guard = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("temporary Asset listener should bind");
    let asset_address = asset_guard
        .local_addr()
        .expect("Asset address should exist");

    let config = orbitrelay_server::ServerConfig::default()
        .with_websocket_listener(
            WebSocketListenerConfig::default().with_bind_addr(websocket_address),
        )
        .with_asset_delivery(
            AssetDeliveryConfig::default()
                .with_enabled(true)
                .with_listen_addr(asset_address)
                .with_public_base_url(format!("http://{asset_address}")),
        );
    let actor_id = ActorId::new();
    let deps = dependencies(actor_id);
    let bootstrap = Bootstrap::new(config.clone(), deps.action_authorizer());
    let context = bootstrap
        .initialize()
        .await
        .expect("bootstrap should succeed before listener bind");
    let node_id = context.node_id().clone();
    let mut application = ServerApplication::new(context.clone(), config, deps);

    let error = application
        .start()
        .await
        .expect_err("occupied Asset address should fail startup");
    assert!(matches!(error, ServerError::Listener { .. }));
    assert_eq!(
        context.lifecycle().state(),
        orbitrelay_server::LifecycleState::Stopped
    );
    assert!(context
        .node_registry()
        .get(&node_id)
        .await
        .expect("registry lookup should succeed")
        .is_none());

    drop(asset_guard);
    let rebound = TcpListener::bind(websocket_address)
        .await
        .expect("WebSocket listener should have been rolled back");
    drop(rebound);
}
