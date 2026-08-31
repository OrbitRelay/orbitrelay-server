mod support;

use std::{
    io::{BufRead, BufReader, Read},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

use orbitrelay_protocol::{ActorId, Payload, SessionId};

use support::TestClient;

struct ChildServer {
    child: Child,
    stderr_reader: Option<thread::JoinHandle<String>>,
}

impl ChildServer {
    fn spawn(bind_addr: &str) -> (Self, mpsc::Receiver<String>) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_orbitrelay-server"))
            .env("ORBITRELAY_DEVELOPMENT_MODE", "true")
            .env("ORBITRELAY_BIND_ADDR", bind_addr)
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("server process should start");
        let stdout = child.stdout.take().expect("server stdout should be piped");
        let stderr = child.stderr.take().expect("server stderr should be piped");
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(url) = line.strip_prefix("ORBITRELAY_LISTENING=") {
                    let _ = sender.send(url.to_owned());
                }
            }
        });
        let stderr_reader = thread::spawn(move || {
            let mut output = String::new();
            let _ = BufReader::new(stderr).read_to_string(&mut output);
            output
        });
        (
            Self {
                child,
                stderr_reader: Some(stderr_reader),
            },
            receiver,
        )
    }

    fn stop(mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stderr_reader
            .take()
            .and_then(|reader| reader.join().ok())
            .unwrap_or_default()
    }
}

impl Drop for ChildServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_server_process_broadcasts_to_multiple_clients() {
    let (server, address_receiver) = ChildServer::spawn("127.0.0.1:0");
    let url = address_receiver
        .recv_timeout(Duration::from_secs(10))
        .expect("server should report its actual listener URL");
    let session_x = SessionId::new();
    let session_y = SessionId::new();
    let mut client_a = TestClient::connect(&url, ActorId::new(), session_x.clone()).await;
    let mut client_b = TestClient::connect(&url, ActorId::new(), session_x).await;
    let mut client_c = TestClient::connect(&url, ActorId::new(), session_y).await;

    let action_id = client_a.send_echo(Payload::new()).await;
    let (event_ids, event_a) = client_a.action_result(&action_id).await;
    let event_b = client_b.next_event().await;
    assert_eq!(event_ids, vec![event_a.id().to_string()]);
    assert_eq!(event_a, event_b);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), client_c.next_event())
            .await
            .is_err()
    );

    client_a.close().await;
    client_b.close().await;
    client_c.close().await;
    let logs = server.stop();
    assert!(logs.contains("OrbitRelay development authorization is enabled"));
    assert!(logs.contains("WebSocket listener bound"));
    assert!(logs.contains("OrbitRelay server node is ready"));
    assert!(logs.contains("development authentication succeeded"));
    assert!(!logs.contains("credential"));
}

#[test]
fn second_process_cannot_bind_the_same_port() {
    let temporary =
        std::net::TcpListener::bind("127.0.0.1:0").expect("temporary listener should bind");
    let address = temporary.local_addr().expect("address should exist");
    drop(temporary);
    let (first, first_address) = ChildServer::spawn(&address.to_string());
    first_address
        .recv_timeout(Duration::from_secs(10))
        .expect("first server should bind");

    let (mut second, _second_address) = ChildServer::spawn(&address.to_string());
    let status = second
        .child
        .wait()
        .expect("second server should exit after bind failure");
    let logs = second
        .stderr_reader
        .take()
        .and_then(|reader| reader.join().ok())
        .unwrap_or_default();
    assert!(!status.success());
    assert!(logs.contains("failed to bind configured listener"));
    drop(first);
}
