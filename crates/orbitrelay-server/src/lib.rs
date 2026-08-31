//! Composition root for the OrbitRelay server process.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapters;
mod application;
mod asset_delivery;
mod bootstrap;
mod canvas;
mod canvas_history;
mod config;
mod context;
mod dependencies;
mod development;
mod error;
mod health;
mod lifecycle;
mod listener;
mod pipeline;

pub use adapters::{RuntimeActionExecutor, SyncEventSource, SyncEventSourceFactory};
pub use application::ServerApplication;
pub use asset_delivery::{
    register_asset_access_query_handler, AssetAccessAuthorization, AssetAccessDescriptor,
    AssetAccessGrant, AssetAccessGrantIssuer, AssetAccessQueryHandler, AssetDeliveryConfig,
    AssetDeliveryService, AssetHttpListener, DeliveryClock, GrantError, RangeParseError,
    ResolvedRange, SystemDeliveryClock, ASSET_ACCESS_RESOLVE_QUERY_TYPE,
};
pub use bootstrap::Bootstrap;
pub use canvas::{
    DevelopmentCanvasCatalog, EventStoreCanvasStateReader, TokioExecutionCoordinator,
};
pub use canvas_history::{
    register_canvas_history_query_handler, CanvasHistoryPageDto, CanvasHistoryQueryHandler,
    CanvasHistoryReadAuthorizationError, CanvasHistoryReadAuthorizer,
    DevelopmentCanvasHistoryReadAuthorizer, HistoryEventDto, RejectAllCanvasHistoryReadAuthorizer,
    CANVAS_HISTORY_PAGE_QUERY_TYPE, DEFAULT_HISTORY_STORE_SCAN_LIMIT, MAX_HISTORY_STORE_SCAN_LIMIT,
};
pub use config::{DevelopmentCanvasConfig, ServerConfig, WebSocketListenerConfig};
pub use context::ServerContext;
pub use dependencies::{
    RejectAllActionAuthorizer, RejectAllDocumentReadAuthorizer, RejectAllIdentityResolver,
    RejectAllSubscriptionAuthorizer, ServerDependencies,
};
pub use development::{
    DevelopmentActionAuthorizer, DevelopmentDocumentReadAuthorizer, DevelopmentIdentityResolver,
    DevelopmentSubscriptionAuthorizer, DEVELOPMENT_IDENTITY_SCHEME,
};
pub use error::{LifecycleError, ServerError};
pub use health::{HealthState, HealthStatus};
pub use lifecycle::{LifecycleState, ServerLifecycle};
pub use listener::{ConnectionSupervisor, WebSocketListener};
pub use pipeline::PipelineAdapter;
