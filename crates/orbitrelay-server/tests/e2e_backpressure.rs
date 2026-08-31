mod support;

use std::time::Duration;

use futures_util::StreamExt;
use orbitrelay_core::{Metadata, Timestamp};
use orbitrelay_protocol::{ActionId, ActorId, Event, EventId, EventType, Payload, SessionId};
use orbitrelay_server::{LifecycleState, ServerConfig, WebSocketListenerConfig};
use orbitrelay_transport::{TransportConfig, WebSocketAdapterConfig};
use serde_json::json;

use support::{development_config, RunningServer, TestClient};

fn event(session_id: &SessionId, payload: Payload) -> Event {
    Event::new(
        EventId::new(),
        session_id.clone(),
        ActorId::new(),
        ActionId::new(),
        EventType::new("dev.echoed"),
        Timestamp::now_utc(),
        payload,
        Metadata::new(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn subscriber_lag_closes_only_the_lagging_session_connection() {
    let config = development_config().with_subscription_queue_capacity(1);
    let server = RunningServer::start(config).await;
    let lagging_session = SessionId::new();
    let lagging = TestClient::connect(&server.url, ActorId::new(), lagging_session.clone()).await;
    let mut healthy = TestClient::connect(&server.url, ActorId::new(), SessionId::new()).await;

    for _ in 0..3 {
        server
            .context
            .event_bus()
            .publish(event(&lagging_session, Payload::new()))
            .await
            .expect("publication should not block on a lagging subscriber");
    }
    let mut lagging_socket = lagging.socket;
    let mut observed_lag_or_close = false;
    for _ in 0..4 {
        let result = tokio::time::timeout(Duration::from_secs(2), lagging_socket.next())
            .await
            .expect("lagging connection should terminate");
        match result {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                let message: orbitrelay_transport::OutboundMessage =
                    serde_json::from_slice(text.as_bytes())
                        .expect("transport message should decode");
                if let orbitrelay_transport::OutboundMessage::Error(error) = message {
                    assert_eq!(
                        error.code(),
                        orbitrelay_transport::TransportErrorCode::SubscriptionLagged
                    );
                    observed_lag_or_close = true;
                    break;
                }
            }
            Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | None | Some(Err(_)) => {
                observed_lag_or_close = true;
                break;
            }
            Some(Ok(_)) => {}
        }
    }
    assert!(observed_lag_or_close);

    let action_id = healthy.send_echo(Payload::new()).await;
    let (_, event) = healthy.action_result(&action_id).await;
    assert_eq!(event.action_id(), &action_id);

    healthy.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_client_is_closed_without_blocking_a_healthy_client() {
    const EVENT_COUNT: usize = 64;
    let adapter = WebSocketAdapterConfig::new(
        TransportConfig::new(4, 2 * 1024 * 1024, 30_000, 10_000),
        64,
        64,
        90_000,
    )
    .with_write_timeout_milliseconds(250);
    let listener = WebSocketListenerConfig::default()
        .with_bind_addr("127.0.0.1:0".parse().expect("valid address"))
        .with_max_connections(2);
    let config = ServerConfig::default()
        .with_development_mode(true)
        .with_subscription_queue_capacity(512)
        .with_websocket_listener(listener)
        .with_websocket_adapter(adapter);
    let server = RunningServer::start(config).await;
    let session_id = SessionId::new();
    let slow = TestClient::connect(&server.url, ActorId::new(), session_id.clone()).await;
    let mut healthy = TestClient::connect(&server.url, ActorId::new(), session_id.clone()).await;
    let (progress_sender, mut progress_receiver) = tokio::sync::mpsc::channel(1);
    let healthy_reader = tokio::spawn(async move {
        for _ in 0..EVENT_COUNT {
            let received = healthy.next_event().await;
            assert_eq!(received.session_id(), &session_id);
            progress_sender
                .send(())
                .await
                .expect("publisher should observe healthy progress");
        }
        healthy
    });

    let mut payload = Payload::new();
    payload.insert("blob", json!("x".repeat(1024 * 1024)));
    for _ in 0..EVENT_COUNT {
        server
            .context
            .event_bus()
            .publish(event(&slow.session_id, payload.clone()))
            .await
            .expect("event publication should succeed");
        tokio::time::timeout(Duration::from_secs(2), progress_receiver.recv())
            .await
            .expect("healthy client should keep up")
            .expect("healthy progress channel should remain open");
    }

    let healthy = tokio::time::timeout(Duration::from_secs(20), healthy_reader)
        .await
        .expect("healthy client should receive every event")
        .expect("healthy reader should join");
    let replacement = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match tokio_tungstenite::connect_async(&server.url).await {
                Ok((socket, _)) => break socket,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("slow client should eventually release its permit");

    drop(replacement);
    drop(slow.socket);
    healthy.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_cancels_a_writer_blocked_by_a_real_tcp_client() {
    const EVENT_COUNT: usize = 64;
    let adapter = WebSocketAdapterConfig::new(
        TransportConfig::new(256, 2 * 1024 * 1024, 30_000, 10_000),
        64,
        64,
        90_000,
    )
    .with_write_timeout_milliseconds(10_000);
    let listener = WebSocketListenerConfig::default()
        .with_bind_addr("127.0.0.1:0".parse().expect("valid address"))
        .with_max_connections(1)
        .with_shutdown_grace_period_milliseconds(2_000);
    let config = ServerConfig::default()
        .with_development_mode(true)
        .with_subscription_queue_capacity(512)
        .with_websocket_listener(listener)
        .with_websocket_adapter(adapter);
    let server = RunningServer::start(config).await;
    let context = server.context.clone();
    let slow = TestClient::connect(&server.url, ActorId::new(), SessionId::new()).await;
    let mut payload = Payload::new();
    payload.insert("blob", json!("x".repeat(1024 * 1024)));

    for _ in 0..EVENT_COUNT {
        server
            .context
            .event_bus()
            .publish(event(&slow.session_id, payload.clone()))
            .await
            .expect("event publication should succeed");
        tokio::task::yield_now().await;
    }

    tokio::time::timeout(Duration::from_secs(3), server.shutdown())
        .await
        .expect("slow TCP writer must not delay server shutdown");
    assert_eq!(context.lifecycle().state(), LifecycleState::Stopped);
    drop(slow.socket);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual architecture smoke: 100 connections and 1000 events"]
async fn one_hundred_subscribers_receive_one_thousand_events() {
    const CONNECTIONS: usize = 100;
    const EVENTS: usize = 1_000;
    let adapter = WebSocketAdapterConfig::new(
        TransportConfig::new(2_048, 1024 * 1024, 30_000, 10_000),
        64,
        64,
        90_000,
    );
    let config = development_config()
        .with_subscription_queue_capacity(2_048)
        .with_websocket_adapter(adapter);
    let server = RunningServer::start(config).await;
    let session_id = SessionId::new();
    let mut clients = Vec::with_capacity(CONNECTIONS);
    for _ in 0..CONNECTIONS {
        clients.push(TestClient::connect(&server.url, ActorId::new(), session_id.clone()).await);
    }
    let readers = clients
        .into_iter()
        .map(|mut client| {
            tokio::spawn(async move {
                for _ in 0..EVENTS {
                    let event = client.next_event().await;
                    assert_eq!(event.event_type().as_str(), "dev.echoed");
                }
                client
            })
        })
        .collect::<Vec<_>>();

    for index in 0..EVENTS {
        server
            .context
            .event_bus()
            .publish(event(&session_id, Payload::new()))
            .await
            .expect("event publication should succeed");
        if index % 10 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let clients = tokio::time::timeout(Duration::from_secs(60), async {
        let mut clients = Vec::with_capacity(CONNECTIONS);
        for reader in readers {
            clients.push(reader.await.expect("reader should join"));
        }
        clients
    })
    .await
    .expect("performance smoke timed out");
    for client in clients {
        client.close().await;
    }
    server.shutdown().await;
}
