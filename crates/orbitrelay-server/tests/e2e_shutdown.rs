mod support;

use std::time::Duration;

use futures_util::StreamExt;
use orbitrelay_node::NodeState;
use orbitrelay_protocol::{ActorId, SessionId};
use orbitrelay_server::LifecycleState;

use support::{development_config, RunningServer, TestClient};

async fn assert_closed(socket: &mut support::Socket) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            match socket.next().await {
                None
                | Some(Err(_))
                | Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                    break;
                }
                Some(Ok(_)) => {}
            }
        }
    })
    .await
    .expect("client should observe server shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_closes_clients_stops_accepting_and_unregisters_node() {
    let server = RunningServer::start(development_config()).await;
    let url = server.url.clone();
    let address = server.address;
    let context = server.context.clone();
    let node_id = context.node_id().clone();
    let mut first = TestClient::connect(&url, ActorId::new(), SessionId::new()).await;
    let mut second = TestClient::connect(&url, ActorId::new(), SessionId::new()).await;

    server.shutdown().await;

    assert_eq!(context.lifecycle().state(), LifecycleState::Stopped);
    assert_eq!(context.node_state(), NodeState::Offline);
    assert!(context
        .node_registry()
        .get(&node_id)
        .await
        .expect("registry lookup should succeed")
        .is_none());
    assert_closed(&mut first.socket).await;
    assert_closed(&mut second.socket).await;
    let rebound = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpListener::bind(address),
    )
    .await
    .expect("post-shutdown bind should finish")
    .expect("shutdown should release the listener address");
    drop(rebound);
}
