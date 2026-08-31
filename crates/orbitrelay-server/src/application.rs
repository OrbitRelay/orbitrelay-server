//! Process-level server application owner.

use std::{sync::Arc, time::Duration};

use orbitrelay_transport::{CompatibleVersionPolicy, JsonCodec, WebSocketSessionDependencies};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    AssetHttpListener, RuntimeActionExecutor, ServerConfig, ServerContext, ServerDependencies,
    ServerError, SyncEventSourceFactory, WebSocketListener,
};

/// Owns the composed context, listener, connection tasks, and process cancellation.
pub struct ServerApplication {
    context: ServerContext,
    config: ServerConfig,
    dependencies: ServerDependencies,
    cancellation: CancellationToken,
    listener: Option<WebSocketListener>,
    asset_listener: Option<AssetHttpListener>,
}

impl ServerApplication {
    /// Creates an application around an initialized, starting context.
    #[must_use]
    pub fn new(
        context: ServerContext,
        config: ServerConfig,
        dependencies: ServerDependencies,
    ) -> Self {
        Self {
            context,
            config,
            dependencies,
            cancellation: CancellationToken::new(),
            listener: None,
            asset_listener: None,
        }
    }

    /// Returns the composed server context.
    #[must_use]
    pub const fn context(&self) -> &ServerContext {
        &self.context
    }

    /// Binds the listener and only then marks the node and lifecycle ready.
    pub async fn start(&mut self) -> Result<(), ServerError> {
        if self.listener.is_some() {
            return Ok(());
        }
        if let Err(error) = self.config.validate() {
            self.rollback_startup().await;
            return Err(error);
        }
        let runtime = Arc::new(RuntimeActionExecutor::new(self.context.runtime_arc()));
        let source_factory = Arc::new(SyncEventSourceFactory::new(self.context.event_bus_arc()));
        let mut dependencies = WebSocketSessionDependencies::new(
            runtime,
            self.dependencies.identity_resolver(),
            self.dependencies.subscription_authorizer(),
            source_factory,
            Arc::new(CompatibleVersionPolicy),
            Arc::new(JsonCodec),
        );
        if let Some(query_executor) = self.context.query_executor_arc() {
            dependencies = dependencies.with_query_executor(query_executor);
        }
        let listener = WebSocketListener::bind(
            self.config.websocket_listener().clone(),
            self.config.websocket_adapter().clone(),
            dependencies,
        )
        .await;
        let listener = match listener {
            Ok(listener) => listener,
            Err(error) => {
                self.rollback_startup().await;
                return Err(error);
            }
        };
        self.listener = Some(listener);
        if self.config.asset_delivery().enabled() {
            let Some(delivery) = self.context.asset_delivery_arc() else {
                self.listener
                    .as_mut()
                    .expect("WebSocket listener was bound")
                    .shutdown(Duration::from_millis(0))
                    .await;
                self.listener = None;
                self.rollback_startup().await;
                return Err(ServerError::bootstrap(
                    "Asset Delivery service was not composed",
                ));
            };
            match AssetHttpListener::bind(self.config.asset_delivery().clone(), delivery).await {
                Ok(listener) => self.asset_listener = Some(listener),
                Err(error) => {
                    if let Some(listener) = self.listener.as_mut() {
                        listener.shutdown(Duration::from_millis(0)).await;
                    }
                    self.listener = None;
                    self.rollback_startup().await;
                    return Err(error);
                }
            }
        }
        if let Err(error) = self.context.mark_ready().await {
            self.cancellation.cancel();
            if let Some(listener) = self.listener.as_mut() {
                listener.shutdown(Duration::from_millis(0)).await;
            }
            if let Some(listener) = self.asset_listener.as_mut() {
                listener.shutdown(Duration::from_millis(0)).await;
            }
            self.listener = None;
            self.asset_listener = None;
            self.rollback_startup().await;
            return Err(error);
        }
        info!("OrbitRelay server node is ready");
        Ok(())
    }

    /// Runs the accept loop until the application cancellation token is cancelled.
    pub async fn serve(&mut self) -> Result<(), ServerError> {
        self.start().await?;
        let result = self.serve_after_start().await;
        if self.context.lifecycle().state() != crate::LifecycleState::Stopped {
            let shutdown_result = self.shutdown().await;
            if result.is_ok() {
                shutdown_result?;
            } else if let Err(error) = shutdown_result {
                warn!(error = %error, "shutdown after listener failure was incomplete");
            }
        }
        result
    }

    /// Requests the accept loop and all owned sessions to stop.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Returns a clone of the process cancellation token for embedding supervisors.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Starts the service, waits for Ctrl-C, and performs graceful shutdown.
    pub async fn run(&mut self) -> Result<(), ServerError> {
        self.start().await?;
        let serve_result = tokio::select! {
            result = self.serve_after_start() => result,
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|_| ServerError::Shutdown { message: "failed to wait for process shutdown signal".to_owned() })?;
                Ok(())
            }
        };
        let shutdown_result = self.shutdown().await;
        serve_result?;
        shutdown_result
    }

    /// Starts the service and waits for a line on standard input before
    /// performing the same graceful shutdown used by the process signal path.
    ///
    /// This is intentionally an explicit process-level hook for deterministic
    /// integration harnesses. It does not add a wire command or alter the
    /// normal production signal behaviour.
    pub async fn run_with_stdin_shutdown(&mut self) -> Result<(), ServerError> {
        use tokio::io::{self, AsyncBufReadExt, BufReader};

        self.start().await?;
        let mut input = BufReader::new(io::stdin());
        let mut line = String::new();
        let serve_result = tokio::select! {
            result = self.serve_after_start() => result,
            result = input.read_line(&mut line) => {
                result.map(|_| ()).map_err(|_| ServerError::Shutdown {
                    message: "failed to wait for process shutdown input".to_owned(),
                })
            }
        };
        let shutdown_result = self.shutdown().await;
        serve_result?;
        shutdown_result
    }

    async fn serve_after_start(&mut self) -> Result<(), ServerError> {
        let listener = self
            .listener
            .as_mut()
            .ok_or_else(|| ServerError::listener("listener was not initialized"))?;
        if let Some(asset_listener) = self.asset_listener.as_mut() {
            tokio::select! {
                result = listener.run(self.cancellation.clone()) => result,
                result = asset_listener.run(self.cancellation.clone()) => result,
            }
        } else {
            listener.run(self.cancellation.clone()).await
        }
    }

    /// Stops accepting connections, drains sessions, and unregisters the node.
    pub async fn shutdown(&mut self) -> Result<(), ServerError> {
        if self.context.lifecycle().state() == crate::LifecycleState::Stopped {
            return Ok(());
        }
        info!("OrbitRelay server shutdown started");
        self.context.begin_shutdown().await?;
        self.cancellation.cancel();
        if let Some(listener) = self.listener.as_mut() {
            listener
                .shutdown(Duration::from_millis(
                    self.config
                        .websocket_listener()
                        .shutdown_grace_period_milliseconds(),
                ))
                .await;
        }
        if let Some(listener) = self.asset_listener.as_mut() {
            listener
                .shutdown(Duration::from_millis(
                    self.config
                        .websocket_listener()
                        .shutdown_grace_period_milliseconds(),
                ))
                .await;
        }
        self.listener = None;
        self.asset_listener = None;
        self.context.finish_shutdown().await?;
        info!("OrbitRelay server shutdown completed");
        Ok(())
    }

    /// Returns the bound listener address once started.
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, ServerError> {
        self.listener
            .as_ref()
            .ok_or_else(|| ServerError::listener("listener is not started"))?
            .local_addr()
    }

    /// Returns the bound Asset HTTP address once Asset Delivery is started.
    pub fn asset_local_addr(&self) -> Result<std::net::SocketAddr, ServerError> {
        self.asset_listener
            .as_ref()
            .ok_or_else(|| ServerError::listener("Asset HTTP listener is not started"))?
            .local_addr()
    }

    async fn rollback_startup(&self) {
        if self.context.lifecycle().state() != crate::LifecycleState::Stopped {
            if let Err(error) = self.context.begin_shutdown().await {
                warn!(error = %error, "startup rollback could not begin cleanly");
            }
            if let Err(error) = self.context.finish_shutdown().await {
                warn!(error = %error, "startup rollback could not finish cleanly");
            }
        }
    }
}
