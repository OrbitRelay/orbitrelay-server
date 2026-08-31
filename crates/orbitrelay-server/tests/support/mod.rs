#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use orbitrelay_core::{Metadata, Timestamp, Version};
use orbitrelay_protocol::{
    Action, ActionId, ActionType, ActorId, Event, EventType, MessageEnvelope, MessageId,
    MessageType, Payload, SessionId,
};
use orbitrelay_server::{
    Bootstrap, DevelopmentActionAuthorizer, DevelopmentIdentityResolver,
    DevelopmentSubscriptionAuthorizer, ServerApplication, ServerConfig, ServerContext,
    ServerDependencies, WebSocketListenerConfig,
};
use orbitrelay_transport::{
    Authenticate, Hello, InboundCredentials, InboundMessage, OutboundMessage, SubscriptionRequest,
    TransportErrorCode, CURRENT_PROTOCOL_VERSION,
};
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

pub type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub struct RunningServer {
    pub url: String,
    pub address: std::net::SocketAddr,
    pub asset_address: Option<std::net::SocketAddr>,
    pub context: ServerContext,
    cancellation: CancellationToken,
    task: JoinHandle<Result<(), orbitrelay_server::ServerError>>,
}

impl RunningServer {
    pub async fn start(config: ServerConfig) -> Self {
        let dependencies = development_dependencies();
        let bootstrap = Bootstrap::new(config.clone(), dependencies.action_authorizer());
        let context = bootstrap
            .initialize()
            .await
            .expect("development bootstrap should succeed");
        let asset_enabled = config.asset_delivery().enabled();
        let mut application = ServerApplication::new(context.clone(), config, dependencies);
        application.start().await.expect("listener should bind");
        let address = application
            .local_addr()
            .expect("listener address should exist");
        let asset_address = if asset_enabled {
            Some(
                application
                    .asset_local_addr()
                    .expect("asset listener address should exist"),
            )
        } else {
            None
        };
        let cancellation = application.cancellation_token();
        let task = tokio::spawn(async move { application.serve().await });
        Self {
            url: format!("ws://{address}/ws"),
            address,
            asset_address,
            context,
            cancellation,
            task,
        }
    }

    pub async fn shutdown(self) {
        self.cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(8), self.task)
            .await
            .expect("server shutdown timed out")
            .expect("server task should join")
            .expect("server should shut down cleanly");
    }
}

pub fn development_config() -> ServerConfig {
    ServerConfig::default()
        .with_development_mode(true)
        .with_websocket_listener(
            WebSocketListenerConfig::default()
                .with_bind_addr("127.0.0.1:0".parse().expect("valid loopback address")),
        )
}

pub fn development_dependencies() -> ServerDependencies {
    ServerDependencies::new(
        Arc::new(DevelopmentActionAuthorizer),
        Arc::new(DevelopmentIdentityResolver),
        Arc::new(DevelopmentSubscriptionAuthorizer),
    )
}

pub struct TestClient {
    pub socket: Socket,
    pub actor_id: ActorId,
    pub session_id: SessionId,
}

impl TestClient {
    pub async fn connect(url: &str, actor_id: ActorId, session_id: SessionId) -> Self {
        Self::connect_with_events(url, actor_id, session_id, [EventType::new("dev.echoed")]).await
    }

    pub async fn connect_with_events(
        url: &str,
        actor_id: ActorId,
        session_id: SessionId,
        event_types: impl IntoIterator<Item = EventType>,
    ) -> Self {
        let mut socket = connect_socket(url).await;
        send(
            &mut socket,
            InboundMessage::Hello(Hello::new(
                vec![CURRENT_PROTOCOL_VERSION],
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
                InboundCredentials::new("development", actor_id.to_string()),
            )),
        )
        .await;
        send(
            &mut socket,
            InboundMessage::Subscribe(SubscriptionRequest::new(
                MessageId::new(),
                session_id.clone(),
                event_types,
            )),
        )
        .await;
        assert!(matches!(
            next(&mut socket).await,
            OutboundMessage::SubscriptionAccepted(_)
        ));
        Self {
            socket,
            actor_id,
            session_id,
        }
    }

    pub async fn send_echo(&mut self, payload: Payload) -> ActionId {
        self.send_action(ActionType::new("dev.echo"), payload).await
    }

    pub async fn send_action(&mut self, action_type: ActionType, payload: Payload) -> ActionId {
        let action = Action::new(
            ActionId::new(),
            self.session_id.clone(),
            self.actor_id.clone(),
            action_type,
            Timestamp::now_utc(),
            payload,
            Metadata::new(),
        );
        let id = action.id().clone();
        send(
            &mut self.socket,
            InboundMessage::Action(MessageEnvelope::new(
                CURRENT_PROTOCOL_VERSION,
                MessageId::new(),
                MessageType::new("action"),
                action,
            )),
        )
        .await;
        id
    }

    pub async fn action_result(&mut self, action_id: &ActionId) -> (Vec<String>, Event) {
        let mut acknowledgement = None;
        let mut event = None;
        while acknowledgement.is_none() || event.is_none() {
            match next(&mut self.socket).await {
                OutboundMessage::ActionAcknowledgement(message)
                    if message.action_id() == action_id =>
                {
                    acknowledgement = Some(
                        message
                            .generated_event_ids()
                            .iter()
                            .map(ToString::to_string)
                            .collect(),
                    );
                }
                OutboundMessage::Event(envelope) if envelope.payload().action_id() == action_id => {
                    event = Some(envelope.into_payload());
                }
                OutboundMessage::Error(error) => {
                    panic!("unexpected transport error: {:?}", error.code())
                }
                _ => {}
            }
        }
        (
            acknowledgement.expect("acknowledgement should exist"),
            event.expect("event should exist"),
        )
    }

    pub async fn next_event(&mut self) -> Event {
        loop {
            match next(&mut self.socket).await {
                OutboundMessage::Event(envelope) => return envelope.into_payload(),
                OutboundMessage::Error(error) => {
                    panic!("unexpected transport error: {:?}", error.code())
                }
                _ => {}
            }
        }
    }

    pub async fn close(mut self) {
        let _ = self.socket.close(None).await;
    }
}

pub async fn connect_socket(url: &str) -> Socket {
    tokio::time::timeout(Duration::from_secs(5), connect_async(url))
        .await
        .expect("WebSocket connect timed out")
        .expect("WebSocket connect should succeed")
        .0
}

pub async fn send(socket: &mut Socket, message: InboundMessage) {
    let encoded = serde_json::to_string(&message).expect("inbound message should encode");
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(encoded.into())),
    )
    .await
    .expect("WebSocket send timed out")
    .expect("WebSocket send should succeed");
}

pub async fn next(socket: &mut Socket) -> OutboundMessage {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("WebSocket receive timed out")
            .expect("server should not close the WebSocket")
            .expect("WebSocket frame should be valid");
        if let Message::Text(text) = frame {
            return serde_json::from_slice(text.as_bytes())
                .expect("outbound message should decode");
        }
    }
}

pub async fn negotiate(socket: &mut Socket, versions: Vec<Version>) -> OutboundMessage {
    send(
        socket,
        InboundMessage::Hello(Hello::new(versions, vec!["json".to_owned()])),
    )
    .await;
    next(socket).await
}

pub async fn expect_error(socket: &mut Socket, code: TransportErrorCode) {
    match next(socket).await {
        OutboundMessage::Error(error) => assert_eq!(error.code(), code),
        message => panic!("expected {code:?}, got {message:?}"),
    }
}
