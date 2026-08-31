//! Process-level configuration for the composition root.

use std::{net::SocketAddr, path::PathBuf};

use orbitrelay_canvas::{CanvasId, LayerId};
use orbitrelay_core::Metadata;
use orbitrelay_node::NodeId;
use orbitrelay_pdf::PdfInspectionLimits;
use orbitrelay_protocol::SessionId;
use orbitrelay_storage_sqlite::DEFAULT_COMMAND_QUEUE_CAPACITY;
use orbitrelay_transport::WebSocketAdapterConfig;

use crate::{
    AssetDeliveryConfig, ServerError, DEFAULT_HISTORY_STORE_SCAN_LIMIT,
    MAX_HISTORY_STORE_SCAN_LIMIT,
};

/// Default capacity for each in-memory event subscription queue.
pub const DEFAULT_SUBSCRIPTION_QUEUE_CAPACITY: usize = 64;

/// Development-only Canvas descriptor inputs.
#[derive(Clone, Debug, PartialEq)]
pub struct DevelopmentCanvasConfig {
    session_id: Option<SessionId>,
    canvas_id: Option<CanvasId>,
    layer_id: Option<LayerId>,
    width: f64,
    height: f64,
}

impl Default for DevelopmentCanvasConfig {
    fn default() -> Self {
        Self {
            session_id: None,
            canvas_id: None,
            layer_id: None,
            width: 1920.0,
            height: 1080.0,
        }
    }
}

impl DevelopmentCanvasConfig {
    /// Creates the default 1920x1080 development Canvas configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates configured Canvas dimensions.
    pub fn validate(&self) -> Result<(), ServerError> {
        if !self.width.is_finite() || self.width <= 0.0 {
            return Err(ServerError::config(
                "development Canvas width must be finite and positive",
            ));
        }
        if !self.height.is_finite() || self.height <= 0.0 {
            return Err(ServerError::config(
                "development Canvas height must be finite and positive",
            ));
        }
        Ok(())
    }

    /// Returns an optionally configured development Session identifier.
    #[must_use]
    pub const fn session_id(&self) -> Option<&SessionId> {
        self.session_id.as_ref()
    }

    /// Returns an optionally configured development Canvas identifier.
    #[must_use]
    pub const fn canvas_id(&self) -> Option<&CanvasId> {
        self.canvas_id.as_ref()
    }

    /// Returns an optionally configured default Layer identifier.
    #[must_use]
    pub const fn layer_id(&self) -> Option<&LayerId> {
        self.layer_id.as_ref()
    }

    /// Returns the development Canvas width.
    #[must_use]
    pub const fn width(&self) -> f64 {
        self.width
    }

    /// Returns the development Canvas height.
    #[must_use]
    pub const fn height(&self) -> f64 {
        self.height
    }

    /// Sets the development Session identifier.
    #[must_use]
    pub fn with_session_id(mut self, value: SessionId) -> Self {
        self.session_id = Some(value);
        self
    }

    /// Sets the development Canvas identifier.
    #[must_use]
    pub fn with_canvas_id(mut self, value: CanvasId) -> Self {
        self.canvas_id = Some(value);
        self
    }

    /// Sets the development default Layer identifier.
    #[must_use]
    pub fn with_layer_id(mut self, value: LayerId) -> Self {
        self.layer_id = Some(value);
        self
    }

    /// Sets the development Canvas dimensions.
    #[must_use]
    pub const fn with_size(mut self, width: f64, height: f64) -> Self {
        self.width = width;
        self.height = height;
        self
    }
}

/// Process-level WebSocket listener settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebSocketListenerConfig {
    bind_addr: SocketAddr,
    websocket_path: String,
    max_connections: usize,
    handshake_timeout_milliseconds: u64,
    shutdown_grace_period_milliseconds: u64,
}

impl Default for WebSocketListenerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 8080)),
            websocket_path: "/ws".to_owned(),
            max_connections: 1024,
            handshake_timeout_milliseconds: 10_000,
            shutdown_grace_period_milliseconds: 5_000,
        }
    }
}

impl WebSocketListenerConfig {
    /// Validates listener limits and the configured HTTP handshake path.
    pub fn validate(&self) -> Result<(), ServerError> {
        if self.websocket_path.is_empty() || !self.websocket_path.starts_with('/') {
            return Err(ServerError::config("websocket path must start with `/`"));
        }
        if self.max_connections == 0 {
            return Err(ServerError::config(
                "max connections must be greater than zero",
            ));
        }
        if self.handshake_timeout_milliseconds == 0 {
            return Err(ServerError::config(
                "handshake timeout must be greater than zero",
            ));
        }
        if self.shutdown_grace_period_milliseconds == 0 {
            return Err(ServerError::config(
                "shutdown grace period must be greater than zero",
            ));
        }
        Ok(())
    }

    /// Returns the address the listener will bind.
    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Returns the exact accepted WebSocket request path.
    #[must_use]
    pub fn websocket_path(&self) -> &str {
        &self.websocket_path
    }

    /// Returns the maximum number of active connections.
    #[must_use]
    pub const fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Returns the handshake timeout in milliseconds.
    #[must_use]
    pub const fn handshake_timeout_milliseconds(&self) -> u64 {
        self.handshake_timeout_milliseconds
    }

    /// Returns the graceful shutdown wait in milliseconds.
    #[must_use]
    pub const fn shutdown_grace_period_milliseconds(&self) -> u64 {
        self.shutdown_grace_period_milliseconds
    }

    /// Sets the listener bind address.
    #[must_use]
    pub const fn with_bind_addr(mut self, bind_addr: SocketAddr) -> Self {
        self.bind_addr = bind_addr;
        self
    }

    /// Sets the accepted WebSocket path.
    #[must_use]
    pub fn with_websocket_path(mut self, path: impl Into<String>) -> Self {
        self.websocket_path = path.into();
        self
    }

    /// Sets the connection limit.
    #[must_use]
    pub const fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Sets the handshake timeout.
    #[must_use]
    pub const fn with_handshake_timeout_milliseconds(mut self, value: u64) -> Self {
        self.handshake_timeout_milliseconds = value;
        self
    }

    /// Sets the graceful shutdown wait.
    #[must_use]
    pub const fn with_shutdown_grace_period_milliseconds(mut self, value: u64) -> Self {
        self.shutdown_grace_period_milliseconds = value;
        self
    }
}

/// Minimal configuration needed to compose a local OrbitRelay server.
#[derive(Clone, Debug, PartialEq)]
pub struct ServerConfig {
    node_id: Option<NodeId>,
    node_metadata: Metadata,
    development_mode: bool,
    subscription_queue_capacity: usize,
    websocket_listener: WebSocketListenerConfig,
    websocket_adapter: WebSocketAdapterConfig,
    development_canvas: DevelopmentCanvasConfig,
    development_pdf_path: Option<PathBuf>,
    development_pdf_inspection_limits: PdfInspectionLimits,
    asset_delivery: AssetDeliveryConfig,
    history_store_scan_limit: usize,
    event_store_path: Option<PathBuf>,
    asset_store_root: Option<PathBuf>,
    catalog_store_path: Option<PathBuf>,
    sqlite_command_queue_capacity: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: None,
            node_metadata: Metadata::new(),
            development_mode: false,
            subscription_queue_capacity: DEFAULT_SUBSCRIPTION_QUEUE_CAPACITY,
            websocket_listener: WebSocketListenerConfig::default(),
            websocket_adapter: WebSocketAdapterConfig::default(),
            development_canvas: DevelopmentCanvasConfig::default(),
            development_pdf_path: None,
            development_pdf_inspection_limits: PdfInspectionLimits::DEFAULT,
            asset_delivery: AssetDeliveryConfig::default(),
            history_store_scan_limit: DEFAULT_HISTORY_STORE_SCAN_LIMIT,
            event_store_path: None,
            asset_store_root: None,
            catalog_store_path: None,
            sqlite_command_queue_capacity: DEFAULT_COMMAND_QUEUE_CAPACITY,
        }
    }
}

impl ServerConfig {
    /// Loads supported environment overrides over the default configuration.
    ///
    /// Supported variables include `ORBITRELAY_NODE_ID`,
    /// `ORBITRELAY_DEVELOPMENT_MODE`, `ORBITRELAY_BIND_ADDR`,
    /// `ORBITRELAY_DEVELOPMENT_CANVAS_SESSION_ID`,
    /// `ORBITRELAY_DEVELOPMENT_CANVAS_ID`,
    /// `ORBITRELAY_DEVELOPMENT_CANVAS_LAYER_ID`,
    /// `ORBITRELAY_DEVELOPMENT_CANVAS_WIDTH`,
    /// `ORBITRELAY_DEVELOPMENT_CANVAS_HEIGHT`,
    /// `ORBITRELAY_DEVELOPMENT_PDF_PATH`,
    /// `ORBITRELAY_WEBSOCKET_PATH`, `ORBITRELAY_MAX_CONNECTIONS`,
    /// `ORBITRELAY_HANDSHAKE_TIMEOUT_MILLISECONDS`,
    /// `ORBITRELAY_SHUTDOWN_GRACE_PERIOD_MILLISECONDS`,
    /// `ORBITRELAY_SUBSCRIPTION_QUEUE_CAPACITY`,
    /// `ORBITRELAY_HISTORY_STORE_SCAN_LIMIT`, `ORBITRELAY_EVENT_STORE_PATH`,
    /// `ORBITRELAY_ASSET_STORE_DIR`, `ORBITRELAY_CATALOG_STORE_PATH`,
    /// `ORBITRELAY_SQLITE_COMMAND_QUEUE_CAPACITY`, and Asset Delivery variables.
    /// Unknown variables are ignored.
    pub fn load() -> Result<Self, ServerError> {
        let mut config = Self::default();

        if let Some(value) = std::env::var_os("ORBITRELAY_NODE_ID") {
            let value = value
                .into_string()
                .map_err(|_| ServerError::config("ORBITRELAY_NODE_ID is not valid UTF-8"))?;
            config.node_id =
                Some(value.parse().map_err(|_| {
                    ServerError::config("ORBITRELAY_NODE_ID is not a valid node ID")
                })?);
        }

        if let Some(value) = std::env::var_os("ORBITRELAY_SUBSCRIPTION_QUEUE_CAPACITY") {
            let value = value.into_string().map_err(|_| {
                ServerError::config("ORBITRELAY_SUBSCRIPTION_QUEUE_CAPACITY is not valid UTF-8")
            })?;
            config.subscription_queue_capacity = value.parse().map_err(|_| {
                ServerError::config(
                    "ORBITRELAY_SUBSCRIPTION_QUEUE_CAPACITY must be a positive integer",
                )
            })?;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_HISTORY_STORE_SCAN_LIMIT")? {
            config.history_store_scan_limit =
                parse_number_env("ORBITRELAY_HISTORY_STORE_SCAN_LIMIT", &value)?;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_EVENT_STORE_PATH")? {
            config.event_store_path = Some(PathBuf::from(value));
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_STORE_DIR")? {
            config.asset_store_root = Some(PathBuf::from(value));
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_CATALOG_STORE_PATH")? {
            config.catalog_store_path = Some(PathBuf::from(value));
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_SQLITE_COMMAND_QUEUE_CAPACITY")? {
            config.sqlite_command_queue_capacity =
                parse_number_env("ORBITRELAY_SQLITE_COMMAND_QUEUE_CAPACITY", &value)?;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_DEVELOPMENT_MODE")? {
            config.development_mode = parse_bool_env("ORBITRELAY_DEVELOPMENT_MODE", &value)?;
        }

        if config.development_mode {
            if let Some(value) = read_utf8_env("ORBITRELAY_DEVELOPMENT_PDF_PATH")? {
                config.development_pdf_path = Some(PathBuf::from(value));
            }
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_DEVELOPMENT_CANVAS_SESSION_ID")? {
            config.development_canvas.session_id = Some(value.parse().map_err(|_| {
                ServerError::config(
                    "ORBITRELAY_DEVELOPMENT_CANVAS_SESSION_ID is not a valid SessionId",
                )
            })?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_DEVELOPMENT_CANVAS_ID")? {
            config.development_canvas.canvas_id = Some(value.parse().map_err(|_| {
                ServerError::config("ORBITRELAY_DEVELOPMENT_CANVAS_ID is not a valid CanvasId")
            })?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_DEVELOPMENT_CANVAS_LAYER_ID")? {
            config.development_canvas.layer_id = Some(value.parse().map_err(|_| {
                ServerError::config("ORBITRELAY_DEVELOPMENT_CANVAS_LAYER_ID is not a valid LayerId")
            })?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_DEVELOPMENT_CANVAS_WIDTH")? {
            config.development_canvas.width = value.parse().map_err(|_| {
                ServerError::config("ORBITRELAY_DEVELOPMENT_CANVAS_WIDTH must be a number")
            })?;
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_DEVELOPMENT_CANVAS_HEIGHT")? {
            config.development_canvas.height = value.parse().map_err(|_| {
                ServerError::config("ORBITRELAY_DEVELOPMENT_CANVAS_HEIGHT must be a number")
            })?;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_BIND_ADDR")? {
            config.websocket_listener.bind_addr = value.parse().map_err(|_| {
                ServerError::config("ORBITRELAY_BIND_ADDR is not a valid socket address")
            })?;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_WEBSOCKET_PATH")? {
            config.websocket_listener.websocket_path = value;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_MAX_CONNECTIONS")? {
            config.websocket_listener.max_connections =
                parse_number_env("ORBITRELAY_MAX_CONNECTIONS", &value)?;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_HANDSHAKE_TIMEOUT_MILLISECONDS")? {
            config.websocket_listener.handshake_timeout_milliseconds =
                parse_number_env("ORBITRELAY_HANDSHAKE_TIMEOUT_MILLISECONDS", &value)?;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_SHUTDOWN_GRACE_PERIOD_MILLISECONDS")? {
            config.websocket_listener.shutdown_grace_period_milliseconds =
                parse_number_env("ORBITRELAY_SHUTDOWN_GRACE_PERIOD_MILLISECONDS", &value)?;
        }

        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_DELIVERY_ENABLED")? {
            config.asset_delivery = config
                .asset_delivery
                .clone()
                .with_enabled(parse_bool_env("ORBITRELAY_ASSET_DELIVERY_ENABLED", &value)?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_LISTEN_ADDR")? {
            config.asset_delivery =
                config
                    .asset_delivery
                    .clone()
                    .with_listen_addr(value.parse().map_err(|_| {
                        ServerError::config(
                            "ORBITRELAY_ASSET_LISTEN_ADDR is not a valid socket address",
                        )
                    })?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_PUBLIC_BASE_URL")? {
            config.asset_delivery = config.asset_delivery.clone().with_public_base_url(value);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_MAX_CONNECTIONS")? {
            config.asset_delivery =
                config
                    .asset_delivery
                    .clone()
                    .with_max_connections(parse_number_env(
                        "ORBITRELAY_ASSET_MAX_CONNECTIONS",
                        &value,
                    )?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_MAX_ACTIVE_DOWNLOADS")? {
            config.asset_delivery =
                config
                    .asset_delivery
                    .clone()
                    .with_max_active_downloads(parse_number_env(
                        "ORBITRELAY_ASSET_MAX_ACTIVE_DOWNLOADS",
                        &value,
                    )?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_CHUNK_SIZE")? {
            config.asset_delivery = config
                .asset_delivery
                .clone()
                .with_chunk_size(parse_number_env("ORBITRELAY_ASSET_CHUNK_SIZE", &value)?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_IDLE_TIMEOUT_MILLISECONDS")? {
            config.asset_delivery = config
                .asset_delivery
                .clone()
                .with_idle_timeout_milliseconds(parse_number_env(
                    "ORBITRELAY_ASSET_IDLE_TIMEOUT_MILLISECONDS",
                    &value,
                )?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_GRANT_TTL_SECONDS")? {
            config.asset_delivery =
                config
                    .asset_delivery
                    .clone()
                    .with_grant_ttl_seconds(value.parse().map_err(|_| {
                        ServerError::config("ORBITRELAY_ASSET_GRANT_TTL_SECONDS must be an integer")
                    })?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_MAX_GRANTS")? {
            config.asset_delivery = config
                .asset_delivery
                .clone()
                .with_max_grants(parse_number_env("ORBITRELAY_ASSET_MAX_GRANTS", &value)?);
        }
        if let Some(value) = read_utf8_env("ORBITRELAY_ASSET_ALLOWED_ORIGINS")? {
            config.asset_delivery = config.asset_delivery.clone().with_allowed_origins(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|origin| !origin.is_empty())
                    .map(str::to_owned),
            );
        }

        config.validate()?;
        Ok(config)
    }

    /// Validates configuration invariants independent of a concrete backend.
    pub fn validate(&self) -> Result<(), ServerError> {
        if self.subscription_queue_capacity == 0 {
            return Err(ServerError::config(
                "subscription queue capacity must be greater than zero",
            ));
        }
        if self.history_store_scan_limit == 0
            || self.history_store_scan_limit > MAX_HISTORY_STORE_SCAN_LIMIT
        {
            return Err(ServerError::config(format!(
                "history Store scan limit must be between 1 and {MAX_HISTORY_STORE_SCAN_LIMIT}"
            )));
        }

        self.websocket_listener.validate()?;
        self.websocket_adapter
            .validate()
            .map_err(|_| ServerError::config("invalid WebSocket adapter configuration"))?;

        self.development_canvas.validate()?;
        self.asset_delivery.validate()?;
        if self
            .event_store_path
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ServerError::config("event store path must not be empty"));
        }
        if self
            .asset_store_root
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ServerError::config("asset store root must not be empty"));
        }
        if self
            .catalog_store_path
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ServerError::config("catalog store path must not be empty"));
        }
        if self.sqlite_command_queue_capacity == 0 {
            return Err(ServerError::config(
                "SQLite command queue capacity must be greater than zero",
            ));
        }

        Ok(())
    }

    /// Sets the stable identifier advertised by this process.
    #[must_use]
    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Sets the business-neutral metadata advertised by this process.
    #[must_use]
    pub fn with_node_metadata(mut self, node_metadata: Metadata) -> Self {
        self.node_metadata = node_metadata;
        self
    }

    /// Sets the bounded capacity of each in-memory subscription queue.
    #[must_use]
    pub const fn with_subscription_queue_capacity(mut self, capacity: usize) -> Self {
        self.subscription_queue_capacity = capacity;
        self
    }

    /// Returns the configured stable node identifier, if one was supplied.
    #[must_use]
    pub const fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    /// Returns the metadata advertised by the local node.
    #[must_use]
    pub const fn node_metadata(&self) -> &Metadata {
        &self.node_metadata
    }

    /// Returns the in-memory subscription queue capacity.
    #[must_use]
    pub const fn subscription_queue_capacity(&self) -> usize {
        self.subscription_queue_capacity
    }

    /// Returns the bounded number of candidate Store events scanned per history Query.
    #[must_use]
    pub const fn history_store_scan_limit(&self) -> usize {
        self.history_store_scan_limit
    }

    /// Returns the optional SQLite EventStore database path.
    #[must_use]
    pub fn event_store_path(&self) -> Option<&std::path::Path> {
        self.event_store_path.as_deref()
    }

    /// Sets the SQLite EventStore database path. No path selects MemoryEventStore.
    #[must_use]
    pub fn with_event_store_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.event_store_path = Some(path.into());
        self
    }

    /// Clears the SQLite EventStore path and selects MemoryEventStore.
    #[must_use]
    pub fn without_event_store_path(mut self) -> Self {
        self.event_store_path = None;
        self
    }

    /// Returns the optional persistent local Asset store root.
    #[must_use]
    pub fn asset_store_root(&self) -> Option<&std::path::Path> {
        self.asset_store_root.as_deref()
    }

    /// Sets the persistent local Asset store root. No root selects the memory Asset store.
    #[must_use]
    pub fn with_asset_store_root(mut self, path: impl Into<PathBuf>) -> Self {
        self.asset_store_root = Some(path.into());
        self
    }

    /// Clears the persistent Asset store root and selects the memory Asset store.
    #[must_use]
    pub fn without_asset_store_root(mut self) -> Self {
        self.asset_store_root = None;
        self
    }

    /// Returns the optional SQLite Catalog database path.
    #[must_use]
    pub fn catalog_store_path(&self) -> Option<&std::path::Path> {
        self.catalog_store_path.as_deref()
    }

    /// Sets the SQLite Catalog database path. No path selects memory catalogs.
    #[must_use]
    pub fn with_catalog_store_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.catalog_store_path = Some(path.into());
        self
    }

    /// Clears the Catalog path and selects memory catalogs.
    #[must_use]
    pub fn without_catalog_store_path(mut self) -> Self {
        self.catalog_store_path = None;
        self
    }

    /// Returns the bounded SQLite worker command queue capacity.
    #[must_use]
    pub const fn sqlite_command_queue_capacity(&self) -> usize {
        self.sqlite_command_queue_capacity
    }

    /// Sets the bounded SQLite worker command queue capacity.
    #[must_use]
    pub const fn with_sqlite_command_queue_capacity(mut self, capacity: usize) -> Self {
        self.sqlite_command_queue_capacity = capacity;
        self
    }

    /// Sets the bounded number of candidate Store events scanned per history Query.
    #[must_use]
    pub const fn with_history_store_scan_limit(mut self, limit: usize) -> Self {
        self.history_store_scan_limit = limit;
        self
    }

    /// Returns whether explicitly unsafe development authorization is enabled.
    #[must_use]
    pub const fn development_mode(&self) -> bool {
        self.development_mode
    }

    /// Enables or disables development-only identity, authorization, and handler behavior.
    #[must_use]
    pub const fn with_development_mode(mut self, enabled: bool) -> Self {
        self.development_mode = enabled;
        self
    }

    /// Returns development Canvas configuration.
    #[must_use]
    pub const fn development_canvas(&self) -> &DevelopmentCanvasConfig {
        &self.development_canvas
    }

    /// Replaces development Canvas configuration.
    #[must_use]
    pub fn with_development_canvas(mut self, config: DevelopmentCanvasConfig) -> Self {
        self.development_canvas = config;
        self
    }

    /// Sets the Server-local PDF path used only by Development bootstrap.
    #[must_use]
    pub fn with_development_pdf_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.development_pdf_path = Some(path.into());
        self
    }

    /// Returns the optional Server-local Development PDF path.
    #[must_use]
    pub fn development_pdf_path(&self) -> Option<&std::path::Path> {
        self.development_pdf_path.as_deref()
    }

    /// Replaces the optional Development PDF path.
    #[must_use]
    pub fn with_optional_development_pdf_path(mut self, path: Option<PathBuf>) -> Self {
        self.development_pdf_path = path;
        self
    }

    /// Returns the PDF inspection policy used by Development bootstrap.
    #[must_use]
    pub const fn development_pdf_inspection_limits(&self) -> PdfInspectionLimits {
        self.development_pdf_inspection_limits
    }

    /// Returns the independent Asset Delivery configuration.
    #[must_use]
    pub const fn asset_delivery(&self) -> &AssetDeliveryConfig {
        &self.asset_delivery
    }

    /// Replaces the independent Asset Delivery configuration.
    #[must_use]
    pub fn with_asset_delivery(mut self, config: AssetDeliveryConfig) -> Self {
        self.asset_delivery = config;
        self
    }

    /// Sets the PDF inspection policy used by Development bootstrap.
    #[must_use]
    pub const fn with_development_pdf_inspection_limits(
        mut self,
        limits: PdfInspectionLimits,
    ) -> Self {
        self.development_pdf_inspection_limits = limits;
        self
    }

    /// Returns process-level WebSocket listener settings.
    #[must_use]
    pub const fn websocket_listener(&self) -> &WebSocketListenerConfig {
        &self.websocket_listener
    }

    /// Returns WebSocket adapter limits.
    #[must_use]
    pub const fn websocket_adapter(&self) -> &WebSocketAdapterConfig {
        &self.websocket_adapter
    }

    /// Replaces process-level WebSocket listener settings.
    #[must_use]
    pub fn with_websocket_listener(mut self, config: WebSocketListenerConfig) -> Self {
        self.websocket_listener = config;
        self
    }

    /// Replaces WebSocket adapter limits.
    #[must_use]
    pub fn with_websocket_adapter(mut self, config: WebSocketAdapterConfig) -> Self {
        self.websocket_adapter = config;
        self
    }
}

fn read_utf8_env(name: &'static str) -> Result<Option<String>, ServerError> {
    std::env::var_os(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| ServerError::config(format!("{name} is not valid UTF-8")))
        })
        .transpose()
}

fn parse_bool_env(name: &'static str, value: &str) -> Result<bool, ServerError> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(ServerError::config(format!(
            "{name} must be true, false, 1, or 0"
        ))),
    }
}

fn parse_number_env<T>(name: &'static str, value: &str) -> Result<T, ServerError>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| ServerError::config(format!("{name} must be a positive integer")))
}

#[cfg(test)]
mod tests {
    use super::{DevelopmentCanvasConfig, ServerConfig};
    use crate::ServerError;

    #[test]
    fn default_configuration_is_valid() {
        let config = ServerConfig::default();

        config.validate().expect("default config should be valid");
        assert!(config.node_id().is_none());
        assert!(!config.development_mode());
        assert_eq!(config.subscription_queue_capacity(), 64);
        assert_eq!(
            config.sqlite_command_queue_capacity(),
            orbitrelay_storage_sqlite::DEFAULT_COMMAND_QUEUE_CAPACITY
        );
        assert!(config.event_store_path().is_none());
        assert_eq!(
            config.history_store_scan_limit(),
            crate::DEFAULT_HISTORY_STORE_SCAN_LIMIT
        );
    }

    #[test]
    fn rejects_zero_queue_capacity() {
        let error = ServerConfig::default()
            .with_subscription_queue_capacity(0)
            .validate()
            .expect_err("zero capacity should be rejected");

        assert!(matches!(error, ServerError::Config { .. }));

        assert!(ServerConfig::default()
            .with_sqlite_command_queue_capacity(0)
            .validate()
            .is_err());
    }

    #[test]
    fn history_store_scan_limit_is_bounded() {
        assert!(ServerConfig::default()
            .with_history_store_scan_limit(0)
            .validate()
            .is_err());
        assert!(ServerConfig::default()
            .with_history_store_scan_limit(crate::MAX_HISTORY_STORE_SCAN_LIMIT + 1)
            .validate()
            .is_err());
        assert!(ServerConfig::default()
            .with_history_store_scan_limit(crate::MAX_HISTORY_STORE_SCAN_LIMIT)
            .validate()
            .is_ok());
    }

    #[test]
    fn validates_development_canvas_dimensions() {
        assert!(DevelopmentCanvasConfig::new()
            .with_size(1920.0, 1080.0)
            .validate()
            .is_ok());
        for (width, height) in [
            (0.0, 100.0),
            (100.0, -1.0),
            (f64::NAN, 100.0),
            (100.0, f64::INFINITY),
        ] {
            assert!(DevelopmentCanvasConfig::new()
                .with_size(width, height)
                .validate()
                .is_err());
        }
    }

    #[test]
    fn development_pdf_path_is_optional_and_server_local() {
        let config = ServerConfig::default()
            .with_development_mode(true)
            .with_development_pdf_path("fixtures/lesson.pdf");
        assert_eq!(
            config.development_pdf_path().and_then(|path| path.to_str()),
            Some("fixtures/lesson.pdf")
        );
        assert!(ServerConfig::default().development_pdf_path().is_none());
    }

    #[test]
    fn persistent_store_paths_are_independent_optional_settings() {
        let config = ServerConfig::default()
            .with_event_store_path("events.sqlite")
            .with_asset_store_root("assets")
            .with_catalog_store_path("catalog.sqlite");
        assert_eq!(
            config.event_store_path().and_then(|path| path.to_str()),
            Some("events.sqlite")
        );
        assert_eq!(
            config.asset_store_root().and_then(|path| path.to_str()),
            Some("assets")
        );
        assert_eq!(
            config.catalog_store_path().and_then(|path| path.to_str()),
            Some("catalog.sqlite")
        );
        assert!(ServerConfig::default()
            .without_event_store_path()
            .without_asset_store_root()
            .without_catalog_store_path()
            .validate()
            .is_ok());
    }
}
