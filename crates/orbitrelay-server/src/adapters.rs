//! Adapters from server-owned Runtime and Sync implementations to Transport ports.

use std::sync::Arc;

use async_trait::async_trait;
use orbitrelay_protocol::Event;
use orbitrelay_runtime::{Runtime, RuntimeError};
use orbitrelay_sync::{EventBus, EventFilter, Subscription, SyncError};
use orbitrelay_transport::{
    EventSource, EventSourceError, EventSourceFactory, SubscriptionRequest,
    TransportExecutionError, TransportSubscriptionId,
};
use tracing::{debug, info};

/// Adapts the server Runtime to the transport action execution port.
pub struct RuntimeActionExecutor {
    runtime: Arc<Runtime>,
}

impl RuntimeActionExecutor {
    /// Creates an executor around the composed Runtime.
    #[must_use]
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl orbitrelay_transport::ActionExecutor for RuntimeActionExecutor {
    async fn execute(
        &self,
        action: orbitrelay_protocol::Action,
    ) -> Result<Vec<Event>, TransportExecutionError> {
        self.runtime
            .execute(action)
            .await
            .map_err(map_runtime_error)
    }
}

fn map_runtime_error(error: RuntimeError) -> TransportExecutionError {
    match error {
        RuntimeError::HandlerNotFound { .. }
        | RuntimeError::ValidationFailed { .. }
        | RuntimeError::AuthorizationFailed { .. } => TransportExecutionError::Rejected {
            detail: "action was rejected by the runtime".to_owned(),
        },
        RuntimeError::HandlerFailed { .. } => TransportExecutionError::Rejected {
            detail: "action was rejected by the action handler".to_owned(),
        },
        RuntimeError::PipelineFailed { .. } | RuntimeError::CoordinationFailed { .. } => {
            TransportExecutionError::Unavailable {
                detail: "event pipeline is unavailable".to_owned(),
            }
        }
        RuntimeError::CoordinationUnavailable { .. } => TransportExecutionError::Unavailable {
            detail: "execution coordination is unavailable".to_owned(),
        },
        _ => TransportExecutionError::Failed {
            detail: "runtime execution failed".to_owned(),
        },
    }
}

/// Adapts the synchronization EventBus to the transport event-source port.
pub struct SyncEventSourceFactory {
    event_bus: Arc<dyn EventBus>,
}

impl SyncEventSourceFactory {
    /// Creates a factory around an abstract EventBus.
    #[must_use]
    pub fn new(event_bus: Arc<dyn EventBus>) -> Self {
        Self { event_bus }
    }
}

#[async_trait]
impl EventSourceFactory for SyncEventSourceFactory {
    async fn subscribe(
        &self,
        request: SubscriptionRequest,
    ) -> Result<Box<dyn EventSource>, EventSourceError> {
        let mut filter = EventFilter::for_session(request.session_id().clone());
        for event_type in request.event_types().iter().cloned() {
            filter = filter.with_event_type(event_type);
        }
        let subscription = self
            .event_bus
            .subscribe(filter)
            .await
            .map_err(map_sync_error)?;
        info!(session_id = %request.session_id(), "event subscription opened");
        Ok(Box::new(SyncEventSource::new(subscription)))
    }
}

/// Wraps one synchronization subscription without exposing its concrete type.
pub struct SyncEventSource {
    id: TransportSubscriptionId,
    subscription: Box<dyn Subscription>,
}

impl SyncEventSource {
    fn new(subscription: Box<dyn Subscription>) -> Self {
        Self {
            id: TransportSubscriptionId::new(),
            subscription,
        }
    }
}

#[async_trait]
impl EventSource for SyncEventSource {
    fn id(&self) -> &TransportSubscriptionId {
        &self.id
    }

    async fn next_event(&mut self) -> Result<Option<Event>, EventSourceError> {
        self.subscription.next_event().await.map_err(map_sync_error)
    }

    async fn close(&mut self) -> Result<(), EventSourceError> {
        let result = self.subscription.close().await.map_err(map_sync_error);
        debug!(subscription_id = %self.id, "event subscription closed");
        result
    }
}

fn map_sync_error(error: SyncError) -> EventSourceError {
    match error {
        SyncError::SubscriberLagged { .. } => EventSourceError::SubscriptionLagged,
        SyncError::SubscriptionClosed { .. } => EventSourceError::SubscriptionClosed,
        SyncError::InvalidFilter { .. } | SyncError::InvalidQueueCapacity => {
            EventSourceError::Unavailable {
                detail: "event source rejected the subscription".to_owned(),
            }
        }
        _ => EventSourceError::Failed {
            detail: "event source failed".to_owned(),
        },
    }
}
