//! Minimal development and acceptance client for the OrbitRelay WebSocket protocol.

use std::{env, process, time::Duration};

use futures_util::{SinkExt, StreamExt};
use orbitrelay_core::{Metadata, Timestamp};
use orbitrelay_protocol::{
    Action, ActionId, ActionType, ActorId, EventType, MessageEnvelope, MessageId, MessageType,
    Payload, SessionId,
};
use orbitrelay_transport::{
    Authenticate, CloseMessage, Hello, InboundCredentials, InboundMessage, OutboundMessage,
    PingMessage, SubscriptionRequest, CURRENT_PROTOCOL_VERSION,
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type ClientSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug)]
struct ClientOptions {
    url: String,
    actor_id: ActorId,
    session_id: SessionId,
    action_type: ActionType,
    event_types: Vec<EventType>,
    payload: Payload,
    send_action: bool,
    receive_events: usize,
    ping: bool,
}

impl ClientOptions {
    fn parse() -> Result<Self, String> {
        let mut url = "ws://127.0.0.1:8080/ws".to_owned();
        let mut actor_id = None;
        let mut session_id = None;
        let mut action_type = ActionType::new("dev.echo");
        let mut event_types = Vec::new();
        let mut payload = serde_json::from_str(r#"{"message":"hello from ws_client"}"#)
            .map_err(|_| "default payload is invalid".to_owned())?;
        let mut send_action = false;
        let mut receive_events = 0;
        let mut ping = false;
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--url" => url = next_value(&mut arguments, "--url")?,
                "--actor-id" => {
                    actor_id = Some(
                        next_value(&mut arguments, "--actor-id")?
                            .parse()
                            .map_err(|_| "--actor-id must be a valid ActorId".to_owned())?,
                    );
                }
                "--session-id" => {
                    session_id = Some(
                        next_value(&mut arguments, "--session-id")?
                            .parse()
                            .map_err(|_| "--session-id must be a valid SessionId".to_owned())?,
                    );
                }
                "--action-type" => {
                    action_type = ActionType::new(next_value(&mut arguments, "--action-type")?);
                }
                "--event-type" => {
                    event_types.push(EventType::new(next_value(&mut arguments, "--event-type")?));
                }
                "--payload-json" => {
                    payload = serde_json::from_str(&next_value(&mut arguments, "--payload-json")?)
                        .map_err(|_| "--payload-json must be a JSON object".to_owned())?;
                }
                "--send-action" => send_action = true,
                "--receive-events" => {
                    receive_events = next_value(&mut arguments, "--receive-events")?
                        .parse()
                        .map_err(|_| {
                            "--receive-events must be a non-negative integer".to_owned()
                        })?;
                }
                "--ping" => ping = true,
                "--help" | "-h" => {
                    print_help();
                    process::exit(0);
                }
                value => return Err(format!("unknown argument `{value}`")),
            }
        }
        Ok(Self {
            url,
            actor_id: actor_id.unwrap_or_else(ActorId::new),
            session_id: session_id.unwrap_or_else(SessionId::new),
            action_type,
            event_types: if event_types.is_empty() {
                vec![EventType::new("dev.echoed")]
            } else {
                event_types
            },
            payload,
            send_action,
            receive_events,
            ping,
        })
    }
}

fn next_value(arguments: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{name} requires a value"))
}

fn print_help() {
    println!(
        "OrbitRelay development WebSocket client\n\
         --url <ws-url>\n\
         --actor-id <uuid>\n\
         --session-id <uuid>\n\
         --action-type <type>       default: dev.echo\n\
         --event-type <type>        repeatable; default: dev.echoed\n\
         --payload-json <object>\n\
         --send-action\n\
         --receive-events <count>\n\
         --ping"
    );
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("ws_client failed: {error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let options = ClientOptions::parse()?;
    let (mut socket, _) = within(connect_async(&options.url))
        .await?
        .map_err(|error| format!("connect failed: {error}"))?;
    send(
        &mut socket,
        InboundMessage::Hello(Hello::new(
            vec![CURRENT_PROTOCOL_VERSION],
            vec!["json".to_owned()],
        )),
    )
    .await?;
    expect_hello(&mut socket).await?;
    send(
        &mut socket,
        InboundMessage::Authenticate(Authenticate::new(
            MessageId::new(),
            InboundCredentials::new("development", options.actor_id.to_string()),
        )),
    )
    .await?;
    send(
        &mut socket,
        InboundMessage::Subscribe(SubscriptionRequest::new(
            MessageId::new(),
            options.session_id.clone(),
            options.event_types.iter().cloned(),
        )),
    )
    .await?;
    expect_subscription(&mut socket).await?;
    println!(
        "READY actor_id={} session_id={}",
        options.actor_id, options.session_id
    );

    if options.ping {
        send(&mut socket, InboundMessage::Ping(PingMessage::new(1))).await?;
    }

    let mut expected_action_id = None;
    if options.send_action {
        let action = Action::new(
            ActionId::new(),
            options.session_id,
            options.actor_id,
            options.action_type,
            Timestamp::now_utc(),
            options.payload,
            Metadata::new(),
        );
        expected_action_id = Some(action.id().clone());
        send(
            &mut socket,
            InboundMessage::Action(MessageEnvelope::new(
                CURRENT_PROTOCOL_VERSION,
                MessageId::new(),
                MessageType::new("action"),
                action,
            )),
        )
        .await?;
    }

    let mut received_events = 0;
    let mut acknowledgement_received = !options.send_action;
    let mut pong_received = !options.ping;
    while received_events < options.receive_events || !acknowledgement_received || !pong_received {
        match next_outbound(&mut socket).await? {
            OutboundMessage::ActionAcknowledgement(acknowledgement) => {
                if expected_action_id.as_ref() == Some(acknowledgement.action_id()) {
                    acknowledgement_received = true;
                }
                let event_ids = acknowledgement
                    .generated_event_ids()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                println!(
                    "ACK action_id={} event_ids={event_ids}",
                    acknowledgement.action_id()
                );
            }
            OutboundMessage::Event(envelope) => {
                received_events += 1;
                let event = envelope.payload();
                println!(
                    "EVENT event_id={} action_id={} actor_id={} session_id={} event_type={}",
                    event.id(),
                    event.action_id(),
                    event.actor_id(),
                    event.session_id(),
                    event.event_type()
                );
            }
            OutboundMessage::Pong(pong) => {
                pong_received = true;
                println!("PONG nonce={}", pong.nonce());
            }
            OutboundMessage::Error(error) => {
                return Err(format!(
                    "server error code={:?} message={}",
                    error.code(),
                    error.message()
                ));
            }
            _ => {}
        }
    }

    send(
        &mut socket,
        InboundMessage::Close(CloseMessage::new(Some("client complete".to_owned()))),
    )
    .await?;
    let _ = within(socket.close(None)).await;
    Ok(())
}

async fn expect_hello(socket: &mut ClientSocket) -> Result<(), String> {
    match next_outbound(socket).await? {
        OutboundMessage::HelloAccepted(_) => Ok(()),
        message => Err(format!("expected hello_accepted, received {message:?}")),
    }
}

async fn expect_subscription(socket: &mut ClientSocket) -> Result<(), String> {
    match next_outbound(socket).await? {
        OutboundMessage::SubscriptionAccepted(_) => Ok(()),
        OutboundMessage::Error(error) => Err(format!("subscription failed: {:?}", error.code())),
        message => Err(format!(
            "expected subscription_accepted, received {message:?}"
        )),
    }
}

async fn send(socket: &mut ClientSocket, message: InboundMessage) -> Result<(), String> {
    let encoded = serde_json::to_string(&message).map_err(|_| "JSON encoding failed".to_owned())?;
    within(socket.send(Message::Text(encoded.into())))
        .await?
        .map_err(|error| format!("WebSocket send failed: {error}"))
}

async fn next_outbound(socket: &mut ClientSocket) -> Result<OutboundMessage, String> {
    loop {
        let frame = within(socket.next())
            .await?
            .ok_or_else(|| "WebSocket closed".to_owned())?
            .map_err(|error| format!("WebSocket receive failed: {error}"))?;
        if let Message::Text(text) = frame {
            return serde_json::from_slice(text.as_bytes())
                .map_err(|_| "server returned invalid transport JSON".to_owned());
        }
    }
}

async fn within<F>(future: F) -> Result<F::Output, String>
where
    F: std::future::Future,
{
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .map_err(|_| "operation timed out".to_owned())
}
