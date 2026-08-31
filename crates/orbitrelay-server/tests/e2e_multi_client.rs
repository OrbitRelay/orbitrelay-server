mod support;

use std::time::Duration;

use orbitrelay_protocol::{ActorId, Payload, SessionId};
use orbitrelay_storage::EventQuery;
use serde_json::json;

use support::{development_config, RunningServer, TestClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clients_share_one_fact_while_other_sessions_are_isolated() {
    let server = RunningServer::start(development_config()).await;
    let session_x = SessionId::new();
    let session_y = SessionId::new();
    let actor_a = ActorId::new();
    let mut client_a = TestClient::connect(&server.url, actor_a.clone(), session_x.clone()).await;
    let mut client_b = TestClient::connect(&server.url, ActorId::new(), session_x.clone()).await;
    let mut client_c = TestClient::connect(&server.url, ActorId::new(), session_y).await;
    let mut payload = Payload::new();
    payload.insert("message", json!("multi-client fact"));

    let action_id = client_a.send_echo(payload.clone()).await;
    let (generated_ids, event_a) = client_a.action_result(&action_id).await;
    let event_b = client_b.next_event().await;

    assert_eq!(generated_ids, vec![event_a.id().to_string()]);
    assert_eq!(event_a, event_b);
    assert_eq!(event_a.action_id(), &action_id);
    assert_eq!(event_a.actor_id(), &actor_a);
    assert_eq!(event_a.payload(), &payload);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), client_c.next_event())
            .await
            .is_err()
    );

    let stored = server
        .context
        .event_store()
        .query(EventQuery::for_session(session_x))
        .await
        .expect("storage query should succeed");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored.events()[0].event(), &event_a);

    client_a.close().await;
    client_b.close().await;
    client_c.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_actor_can_use_multiple_connections_and_reconnect() {
    let server = RunningServer::start(development_config()).await;
    let actor_id = ActorId::new();
    let session_id = SessionId::new();
    let mut first = TestClient::connect(&server.url, actor_id.clone(), session_id.clone()).await;
    let second = TestClient::connect(&server.url, actor_id.clone(), session_id.clone()).await;
    drop(second.socket);

    let mut reconnected =
        TestClient::connect(&server.url, actor_id.clone(), session_id.clone()).await;
    let action_id = first.send_echo(Payload::new()).await;
    let (_, first_event) = first.action_result(&action_id).await;
    let reconnect_event = reconnected.next_event().await;

    assert_eq!(first_event, reconnect_event);
    assert_eq!(reconnect_event.actor_id(), &actor_id);
    assert_eq!(reconnect_event.session_id(), &session_id);

    first.close().await;
    reconnected.close().await;
    server.shutdown().await;
}
