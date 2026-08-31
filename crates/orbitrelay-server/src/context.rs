//! Shared dependencies assembled for one server process.

use std::sync::Arc;
use std::sync::RwLock;

use orbitrelay_asset_local_store::LocalAssetStore;
use orbitrelay_canvas::CanvasDescriptor;
use orbitrelay_catalog_sqlite::SQLiteCatalogStore;
use orbitrelay_core::Metadata;
use orbitrelay_node::{Capability, Node, NodeId, NodeRegistry, NodeState};
use orbitrelay_query::QueryExecutor;
use orbitrelay_runtime::Runtime;
use orbitrelay_storage::EventStore;
use orbitrelay_sync::EventBus;

use crate::{AssetDeliveryService, HealthStatus, LifecycleState, ServerError, ServerLifecycle};

/// Read-only access to the dependencies of one OrbitRelay server process.
#[derive(Clone)]
pub struct ServerContext {
    node_id: NodeId,
    node_metadata: Metadata,
    node_capabilities: Vec<Capability>,
    node_state: Arc<RwLock<NodeState>>,
    lifecycle: ServerLifecycle,
    runtime: Arc<Runtime>,
    event_store: Arc<dyn EventStore>,
    event_bus: Arc<dyn EventBus>,
    node_registry: Arc<dyn NodeRegistry>,
    development_canvas: Option<CanvasDescriptor>,
    query_executor: Option<Arc<dyn QueryExecutor>>,
    asset_delivery: Option<Arc<AssetDeliveryService>>,
    asset_store: Option<Arc<LocalAssetStore>>,
    catalog_store: Option<Arc<SQLiteCatalogStore>>,
}

impl ServerContext {
    /// Creates a context from already-composed dependencies.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        runtime: Arc<Runtime>,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        node_registry: Arc<dyn NodeRegistry>,
    ) -> Self {
        let local_node = Node::new(
            node_id,
            Metadata::new(),
            NodeState::Ready,
            std::iter::empty::<Capability>(),
        );
        let lifecycle = ServerLifecycle::from_state(LifecycleState::Ready);

        Self::new_composed(
            local_node,
            lifecycle,
            runtime,
            event_store,
            event_bus,
            node_registry,
        )
    }

    pub(crate) fn new_composed(
        local_node: Node,
        lifecycle: ServerLifecycle,
        runtime: Arc<Runtime>,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        node_registry: Arc<dyn NodeRegistry>,
    ) -> Self {
        Self::new_composed_with_canvas(
            local_node,
            lifecycle,
            runtime,
            event_store,
            event_bus,
            node_registry,
            None,
        )
    }

    pub(crate) fn new_composed_with_canvas(
        local_node: Node,
        lifecycle: ServerLifecycle,
        runtime: Arc<Runtime>,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        node_registry: Arc<dyn NodeRegistry>,
        development_canvas: Option<CanvasDescriptor>,
    ) -> Self {
        Self::new_composed_with_services(
            local_node,
            lifecycle,
            runtime,
            event_store,
            event_bus,
            node_registry,
            development_canvas,
            None,
            None,
            None,
            None,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root passes explicit ownership boundaries"
    )]
    pub(crate) fn new_composed_with_services(
        local_node: Node,
        lifecycle: ServerLifecycle,
        runtime: Arc<Runtime>,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        node_registry: Arc<dyn NodeRegistry>,
        development_canvas: Option<CanvasDescriptor>,
        query_executor: Option<Arc<dyn QueryExecutor>>,
        asset_delivery: Option<Arc<AssetDeliveryService>>,
        asset_store: Option<Arc<LocalAssetStore>>,
        catalog_store: Option<Arc<SQLiteCatalogStore>>,
    ) -> Self {
        Self {
            node_id: local_node.id().clone(),
            node_metadata: local_node.metadata().clone(),
            node_capabilities: local_node.capabilities().iter().cloned().collect(),
            node_state: Arc::new(RwLock::new(local_node.state())),
            lifecycle,
            runtime,
            event_store,
            event_bus,
            node_registry,
            development_canvas,
            query_executor,
            asset_delivery,
            asset_store,
            catalog_store,
        }
    }

    /// Returns the local node identifier.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the current local node state.
    #[must_use]
    pub fn node_state(&self) -> NodeState {
        *self
            .node_state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns the process lifecycle state machine.
    #[must_use]
    pub const fn lifecycle(&self) -> &ServerLifecycle {
        &self.lifecycle
    }

    /// Returns the read-only process health view.
    #[must_use]
    pub const fn health(&self) -> &HealthStatus {
        self.lifecycle.health()
    }

    /// Returns the composed runtime.
    #[must_use]
    pub fn runtime(&self) -> &Runtime {
        self.runtime.as_ref()
    }

    pub(crate) fn runtime_arc(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    /// Returns the abstract event store.
    #[must_use]
    pub fn event_store(&self) -> &dyn EventStore {
        self.event_store.as_ref()
    }

    /// Returns a shared handle to the abstract event store for server adapters.
    #[must_use]
    pub fn event_store_arc(&self) -> Arc<dyn EventStore> {
        Arc::clone(&self.event_store)
    }

    /// Returns the abstract event bus.
    #[must_use]
    pub fn event_bus(&self) -> &dyn EventBus {
        self.event_bus.as_ref()
    }

    /// Returns the development Canvas descriptor when development mode is enabled.
    #[must_use]
    pub fn development_canvas(&self) -> Option<&CanvasDescriptor> {
        self.development_canvas.as_ref()
    }

    /// Returns the composed Query executor when this context has a read plane.
    pub(crate) fn query_executor_arc(&self) -> Option<Arc<dyn QueryExecutor>> {
        self.query_executor.as_ref().map(Arc::clone)
    }

    /// Returns the composed Asset Delivery service when enabled.
    pub(crate) fn asset_delivery_arc(&self) -> Option<Arc<AssetDeliveryService>> {
        self.asset_delivery.as_ref().map(Arc::clone)
    }

    pub(crate) fn event_bus_arc(&self) -> Arc<dyn EventBus> {
        Arc::clone(&self.event_bus)
    }

    /// Returns the abstract node registry.
    #[must_use]
    pub fn node_registry(&self) -> &dyn NodeRegistry {
        self.node_registry.as_ref()
    }

    /// Gracefully drains, unregisters, and stops this server context.
    pub async fn shutdown(&self) -> Result<(), ServerError> {
        self.begin_shutdown().await?;
        self.finish_shutdown().await
    }

    /// Marks the node and lifecycle ready after the listener is bound.
    pub async fn mark_ready(&self) -> Result<(), ServerError> {
        self.register_node_state(NodeState::Ready).await?;
        self.lifecycle.ready()?;
        Ok(())
    }

    /// Starts graceful shutdown and stops advertising readiness.
    pub async fn begin_shutdown(&self) -> Result<(), ServerError> {
        if self.lifecycle.state() != LifecycleState::Draining {
            self.lifecycle.begin_shutdown()?;
        }
        if self.node_state() != NodeState::Draining {
            self.register_node_state(NodeState::Draining).await?;
        }
        Ok(())
    }

    /// Marks the node offline, unregisters it, and stops the lifecycle.
    pub async fn finish_shutdown(&self) -> Result<(), ServerError> {
        self.event_store
            .close()
            .await
            .map_err(|_| ServerError::Shutdown {
                message: "event store shutdown failed".to_owned(),
            })?;
        if let Some(store) = self.asset_store.as_ref() {
            store.close().await.map_err(|_| ServerError::Shutdown {
                message: "asset store shutdown failed".to_owned(),
            })?;
        }
        if let Some(store) = self.catalog_store.as_ref() {
            store.close().await.map_err(|_| ServerError::Shutdown {
                message: "catalog store shutdown failed".to_owned(),
            })?;
        }
        if self.node_state() != NodeState::Offline {
            self.register_node_state(NodeState::Offline).await?;
        }
        self.node_registry
            .unregister(self.node_id())
            .await
            .map_err(|_| ServerError::node_lifecycle("node unregister failed"))?;
        if self.lifecycle.state() == LifecycleState::Draining {
            self.lifecycle.stop()?;
        }
        Ok(())
    }

    async fn register_node_state(&self, state: NodeState) -> Result<(), ServerError> {
        self.node_registry
            .register(Node::new(
                self.node_id.clone(),
                self.node_metadata.clone(),
                state,
                self.node_capabilities.iter().cloned(),
            ))
            .await
            .map_err(|_| ServerError::node_lifecycle("node state registration failed"))?;
        *self
            .node_state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = state;
        Ok(())
    }
}
