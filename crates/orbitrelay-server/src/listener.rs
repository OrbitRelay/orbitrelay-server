//! TCP/WebSocket listener and connection task supervision.

use std::{net::Shutdown, sync::Arc, time::Duration};

use orbitrelay_transport::{
    ConnectionId, ConnectionMetadata, TransportConnection, WebSocketAdapterConfig,
    WebSocketSessionDependencies,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::{JoinError, JoinSet},
    time,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        http::{HeaderValue, StatusCode},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{ServerError, WebSocketListenerConfig};

/// Owns all active WebSocket connection tasks and their connection limit.
pub struct ConnectionSupervisor {
    tasks: JoinSet<Result<(), orbitrelay_transport::WebSocketAdapterError>>,
    permits: Arc<Semaphore>,
}

impl ConnectionSupervisor {
    /// Creates a supervisor with a bounded number of active connections.
    pub fn new(max_connections: usize) -> Result<Self, ServerError> {
        if max_connections == 0 {
            return Err(ServerError::config(
                "max connections must be greater than zero",
            ));
        }
        Ok(Self {
            tasks: JoinSet::new(),
            permits: Arc::new(Semaphore::new(max_connections)),
        })
    }

    /// Returns the number of tasks currently owned by the supervisor.
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    /// Attempts to reserve a connection slot and spawn its handshake/session task.
    pub fn try_spawn(
        &mut self,
        stream: TcpStream,
        expected_path: String,
        handshake_timeout: Duration,
        dependencies: WebSocketSessionDependencies,
        adapter_config: WebSocketAdapterConfig,
        cancellation: CancellationToken,
    ) -> bool {
        let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
            return false;
        };
        self.tasks.spawn(run_connection(
            stream,
            expected_path,
            handshake_timeout,
            dependencies,
            adapter_config,
            cancellation,
            permit,
        ));
        true
    }

    /// Reaps one completed task, if any task is ready.
    pub async fn join_next(
        &mut self,
    ) -> Option<Result<Result<(), orbitrelay_transport::WebSocketAdapterError>, JoinError>> {
        self.tasks.join_next().await
    }

    /// Aborts and waits for all remaining connection tasks.
    pub async fn abort_all(&mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }

    /// Waits for active sessions up to the graceful shutdown deadline.
    pub async fn drain(&mut self, grace_period: Duration) {
        let wait = async { while self.tasks.join_next().await.is_some() {} };
        if time::timeout(grace_period, wait).await.is_err() {
            self.abort_all().await;
        }
    }
}

/// Owns the bound TCP listener and accepts only the configured WebSocket path.
pub struct WebSocketListener {
    listener: TcpListener,
    config: WebSocketListenerConfig,
    adapter_config: WebSocketAdapterConfig,
    dependencies: WebSocketSessionDependencies,
    supervisor: ConnectionSupervisor,
}

impl WebSocketListener {
    /// Binds a listener without starting the accept loop.
    pub async fn bind(
        config: WebSocketListenerConfig,
        adapter_config: WebSocketAdapterConfig,
        dependencies: WebSocketSessionDependencies,
    ) -> Result<Self, ServerError> {
        config.validate()?;
        adapter_config
            .validate()
            .map_err(|_| ServerError::config("invalid WebSocket adapter configuration"))?;
        let listener = TcpListener::bind(config.bind_addr())
            .await
            .map_err(|_| ServerError::listener("failed to bind configured listener"))?;
        let supervisor = ConnectionSupervisor::new(config.max_connections())?;
        info!(address = %listener.local_addr().map_err(|_| ServerError::listener("listener address unavailable"))?, path = config.websocket_path(), "WebSocket listener bound");
        Ok(Self {
            listener,
            config,
            adapter_config,
            dependencies,
            supervisor,
        })
    }

    /// Returns the OS-selected local address, useful when binding port zero.
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, ServerError> {
        self.listener
            .local_addr()
            .map_err(|_| ServerError::listener("listener address unavailable"))
    }

    /// Returns the number of currently owned connection tasks.
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.supervisor.task_count()
    }

    /// Runs the accept loop until cancellation, continuously reaping tasks.
    pub async fn run(&mut self, cancellation: CancellationToken) -> Result<(), ServerError> {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                completed = self.supervisor.join_next(), if self.supervisor.has_tasks() => {
                    if let Some(result) = completed {
                        match result {
                            Ok(Ok(())) => debug!("WebSocket connection closed"),
                            Ok(Err(orbitrelay_transport::WebSocketAdapterError::WriteTimeout {
                                operation,
                                timeout_milliseconds,
                            })) => warn!(
                                operation,
                                timeout_milliseconds,
                                "WebSocket write timed out; connection forced close"
                            ),
                            Ok(Err(error)) => warn!(error = %error, "WebSocket connection closed with adapter error"),
                            Err(error) => warn!(error = %error, "WebSocket connection task failed"),
                        }
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, address) = accepted
                        .map_err(|_| ServerError::listener("accept failed"))?;
                    let cancellation = cancellation.child_token();
                    let spawned = self.supervisor.try_spawn(
                        stream,
                        self.config.websocket_path().to_owned(),
                        Duration::from_millis(self.config.handshake_timeout_milliseconds()),
                        self.dependencies.clone(),
                        self.adapter_config.clone(),
                        cancellation,
                    );
                    if spawned {
                        info!(peer = %address, "WebSocket connection accepted");
                    } else {
                        warn!(peer = %address, "WebSocket connection rejected at capacity");
                    }
                }
            }
        }
        Ok(())
    }

    /// Stops accepting and drains all currently owned connection tasks.
    pub async fn shutdown(&mut self, grace_period: Duration) {
        self.supervisor.drain(grace_period).await;
    }
}

#[allow(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback uses its protocol response as the rejection value"
)]
async fn run_connection(
    stream: TcpStream,
    expected_path: String,
    handshake_timeout: Duration,
    dependencies: WebSocketSessionDependencies,
    adapter_config: WebSocketAdapterConfig,
    cancellation: CancellationToken,
    _permit: OwnedSemaphorePermit,
) -> Result<(), orbitrelay_transport::WebSocketAdapterError> {
    let standard_stream = stream.into_std().map_err(|_| {
        orbitrelay_transport::WebSocketAdapterError::Frame(
            "could not prepare accepted TCP stream".to_owned(),
        )
    })?;
    let shutdown_stream = standard_stream.try_clone().map_err(|_| {
        orbitrelay_transport::WebSocketAdapterError::Frame(
            "could not create TCP shutdown handle".to_owned(),
        )
    })?;
    let stream = TcpStream::from_std(standard_stream).map_err(|_| {
        orbitrelay_transport::WebSocketAdapterError::Frame(
            "could not restore accepted TCP stream".to_owned(),
        )
    })?;
    let callback_path = expected_path.clone();
    let handshake = accept_hdr_async(stream, move |request: &Request, response: Response| {
        if request.uri().path() == callback_path {
            Ok(response)
        } else {
            let mut rejected: ErrorResponse =
                response.map(|_| Some("WebSocket path not found".to_owned()));
            *rejected.status_mut() = StatusCode::NOT_FOUND;
            rejected
                .headers_mut()
                .insert("Connection", HeaderValue::from_static("close"));
            rejected
                .headers_mut()
                .insert("Content-Length", HeaderValue::from_static("23"));
            Err(rejected)
        }
    });
    let websocket = time::timeout(handshake_timeout, handshake)
        .await
        .map_err(|_| {
            orbitrelay_transport::WebSocketAdapterError::Frame(
                "WebSocket handshake timed out".to_owned(),
            )
        })?
        .map_err(|_| {
            orbitrelay_transport::WebSocketAdapterError::Frame(
                "WebSocket handshake rejected".to_owned(),
            )
        })?;
    let connection = TransportConnection::new(ConnectionId::new(), ConnectionMetadata::new());
    info!(connection_id = %connection.id(), "WebSocket session established");
    let session = orbitrelay_transport::run_websocket_session(
        websocket,
        connection,
        dependencies,
        adapter_config,
    );
    tokio::pin!(session);
    tokio::select! {
        result = &mut session => result,
        _ = cancellation.cancelled() => {
            let _ = shutdown_stream.shutdown(Shutdown::Both);
            match time::timeout(Duration::from_secs(2), &mut session).await {
                Ok(result) => result,
                Err(_) => Err(orbitrelay_transport::WebSocketAdapterError::Task(
                    "WebSocket session did not stop after TCP shutdown".to_owned(),
                )),
            }
        },
    }
}
