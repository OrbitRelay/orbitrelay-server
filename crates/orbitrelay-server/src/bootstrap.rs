//! Dependency construction and node lifecycle management.

use std::{path::Path, sync::Arc};

use bytes::Bytes;
use orbitrelay_asset::{ContentHash, SourceAssetDescriptor};
use orbitrelay_asset_local_store::LocalAssetStore;
use orbitrelay_asset_runtime::{AssetCatalog, AssetInsertOutcome, AssetReader, MemoryAssetStore};
use orbitrelay_canvas_runtime::{
    register_canvas_handlers, CanvasCatalog, CanvasCommandService, CanvasStateReader,
};
use orbitrelay_catalog_sqlite::{BootstrapBinding, CatalogPublishOutcome, SQLiteCatalogStore};
use orbitrelay_document_runtime::{
    register_document_query_handlers, DocumentCatalog, DocumentComposeInput, DocumentComposer,
    DocumentInsertOutcome, DocumentReadService, DocumentSourcePage, MemoryDocumentCatalog,
};
use orbitrelay_node::{Capability, MemoryNodeRegistry, Node, NodeId, NodeRegistry, NodeState};
use orbitrelay_pdf::{PdfInspectionLimits, PdfInspector};
use orbitrelay_protocol::SessionId;
use orbitrelay_query::{QueryRegistry, RegisteredQueryExecutor};
use orbitrelay_runtime::{
    ActionAuthorizer, EventPipeline, HandlerRegistry, Runtime, RuntimeContext, SystemClock,
};
use orbitrelay_storage::{EventStore, MemoryEventStore};
use orbitrelay_storage_sqlite::SQLiteEventStore;
use orbitrelay_sync::{EventBus, MemoryEventBus};
use sha2::{Digest, Sha256};

use crate::asset_delivery::{register_asset_access_query_handler, AssetDeliveryService};
use crate::canvas::{
    development_canvas_descriptor, DevelopmentCanvasCatalog, EventStoreCanvasStateReader,
    TokioExecutionCoordinator,
};
use crate::canvas_history::{
    register_canvas_history_query_handler, CanvasHistoryReadAuthorizer,
    DevelopmentCanvasHistoryReadAuthorizer, RejectAllCanvasHistoryReadAuthorizer,
};
use crate::dependencies::RejectAllDocumentReadAuthorizer;
use crate::development::{DevelopmentDocumentReadAuthorizer, DevelopmentEchoHandler};
use crate::{PipelineAdapter, ServerConfig, ServerContext, ServerError, ServerLifecycle};

type AssetPorts = (
    Arc<dyn AssetCatalog>,
    Arc<dyn AssetReader>,
    Option<Arc<LocalAssetStore>>,
);
type CatalogPorts = (
    Arc<dyn DocumentCatalog>,
    Arc<dyn CanvasCatalog>,
    Option<Arc<SQLiteCatalogStore>>,
);

/// Creates the in-process dependencies and manages the local node lifecycle.
#[derive(Clone)]
pub struct Bootstrap {
    config: ServerConfig,
    authorizer: Arc<dyn ActionAuthorizer>,
}

impl Bootstrap {
    /// Creates a bootstrapper with an externally supplied authorizer.
    #[must_use]
    pub fn new(config: ServerConfig, authorizer: Arc<dyn ActionAuthorizer>) -> Self {
        Self { config, authorizer }
    }

    /// Creates the memory-backed context in the starting state.
    pub async fn build(&self) -> Result<ServerContext, ServerError> {
        self.initialize().await
    }

    /// Creates dependencies and registers the local node as starting.
    pub async fn initialize(&self) -> Result<ServerContext, ServerError> {
        self.config.validate()?;

        let event_store: Arc<dyn EventStore> = if let Some(path) = self.config.event_store_path() {
            let path = path.to_path_buf();
            let queue_capacity = self.config.sqlite_command_queue_capacity();
            let store = tokio::task::spawn_blocking(move || {
                SQLiteEventStore::open_with_queue_capacity(path, queue_capacity)
            })
            .await
            .map_err(|_| ServerError::bootstrap("SQLite EventStore initialization failed"))?
            .map_err(|_| ServerError::bootstrap("SQLite EventStore initialization failed"))?;
            Arc::new(store)
        } else {
            Arc::new(MemoryEventStore::new())
        };
        let event_bus: Arc<dyn EventBus> = Arc::new(
            MemoryEventBus::with_queue_capacity(self.config.subscription_queue_capacity())
                .map_err(|_| ServerError::bootstrap("event bus initialization failed"))?,
        );
        let node_registry: Arc<dyn NodeRegistry> = Arc::new(MemoryNodeRegistry::new());

        self.initialize_with(event_store, event_bus, node_registry)
            .await
    }

    async fn initialize_with(
        &self,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        node_registry: Arc<dyn NodeRegistry>,
    ) -> Result<ServerContext, ServerError> {
        self.config.validate()?;

        let node_id = self.config.node_id().cloned().unwrap_or_else(NodeId::new);
        let lifecycle = ServerLifecycle::new();
        lifecycle.start()?;
        self.register_state_with(node_registry.as_ref(), &node_id, NodeState::Starting)
            .await?;
        let result = async {
            let pipeline: Arc<dyn EventPipeline> =
                Arc::new(PipelineAdapter::new(event_store.clone(), event_bus.clone()));
            let handler_registry = Arc::new(HandlerRegistry::new());
            let coordinator = Arc::new(TokioExecutionCoordinator::new());
            let memory_asset_store = Arc::new(MemoryAssetStore::new());
            let memory_document_catalog = Arc::new(MemoryDocumentCatalog::new());
            let (asset_catalog, asset_reader, persistent_asset_store): AssetPorts =
                if let Some(root) = self.config.asset_store_root() {
                let store = Arc::new(
                    LocalAssetStore::open_with_queue_capacity(
                        root,
                        self.config.sqlite_command_queue_capacity(),
                    )
                    .map_err(|_| ServerError::bootstrap("persistent Asset store initialization failed"))?,
                );
                (
                    store.clone() as Arc<dyn AssetCatalog>,
                    store.clone() as Arc<dyn AssetReader>,
                    Some(store),
                )
            } else {
                (
                    memory_asset_store.clone() as Arc<dyn AssetCatalog>,
                    memory_asset_store.clone() as Arc<dyn AssetReader>,
                    None,
                )
            };
            let (document_catalog_port, canvas_catalog_port, persistent_catalog_store): CatalogPorts =
                if let Some(path) = self.config.catalog_store_path() {
                let store = Arc::new(
                    SQLiteCatalogStore::open_with_queue_capacity(
                        path,
                        self.config.sqlite_command_queue_capacity(),
                    )
                    .map_err(|_| ServerError::bootstrap("persistent Catalog initialization failed"))?,
                );
                (
                    store.clone() as Arc<dyn DocumentCatalog>,
                    store.clone() as Arc<dyn CanvasCatalog>,
                    Some(store),
                )
            } else {
                let documents: Arc<dyn DocumentCatalog> =
                    memory_document_catalog.clone();
                let canvases: Arc<dyn CanvasCatalog> = Arc::new(DevelopmentCanvasCatalog::empty());
                (documents, canvases, None)
            };
            tracing::info!(
                event_store_persistent = self.config.event_store_path().is_some(),
                asset_store_persistent = persistent_asset_store.is_some(),
                catalog_store_persistent = persistent_catalog_store.is_some(),
                restart_recovery_capable = self.config.event_store_path().is_some()
                    && persistent_asset_store.is_some()
                    && persistent_catalog_store.is_some(),
                "OrbitRelay persistence modes selected"
            );
            let mut canvas_descriptors = Vec::new();
            let mut development_canvas = None;

            if self.config.development_mode() {
                handler_registry
                    .register(
                        orbitrelay_protocol::ActionType::new("dev.echo"),
                        Arc::new(DevelopmentEchoHandler),
                    )
                    .map_err(|_| ServerError::bootstrap("development handler registration failed"))?;

                let descriptor = if let Some(store) = persistent_catalog_store.as_ref() {
                    let key = SQLiteCatalogStore::default_bootstrap_binding_key();
                    if let Some(binding) = store
                        .get_bootstrap_binding(key)
                        .await
                        .map_err(|_| ServerError::bootstrap("Development bootstrap binding lookup failed"))?
                    {
                        let descriptor = store
                            .get_canvas(&binding.canvas_id)
                            .await
                            .map_err(|_| ServerError::bootstrap("Development Canvas lookup failed"))?
                            .ok_or_else(|| ServerError::bootstrap("Development bootstrap binding references a missing Canvas"))?;
                        if descriptor.session_id() != &binding.session_id {
                            return Err(ServerError::bootstrap("Development bootstrap Session mismatch"));
                        }
                        descriptor
                    } else {
                        let configured = self.config.development_canvas();
                        let descriptor = development_canvas_descriptor(
                            configured.session_id().cloned(),
                            configured.canvas_id().cloned(),
                            configured.layer_id().cloned(),
                            configured.width(),
                            configured.height(),
                        )
                        .map_err(|_| ServerError::bootstrap("development Canvas configuration is invalid"))?;
                        match store
                            .publish_standalone_canvas_with_binding(
                                key,
                                descriptor.clone(),
                                BootstrapBinding {
                                    session_id: descriptor.session_id().clone(),
                                    canvas_id: descriptor.canvas_id().clone(),
                                },
                            )
                            .await
                            .map_err(|_| ServerError::bootstrap("Development Canvas publication failed"))?
                        {
                            CatalogPublishOutcome::Inserted | CatalogPublishOutcome::Existing => {}
                            CatalogPublishOutcome::Conflict => {
                                return Err(ServerError::bootstrap("Development Canvas identity conflict"));
                            }
                        }
                        descriptor
                    }
                } else {
                    let configured = self.config.development_canvas();
                    development_canvas_descriptor(
                        configured.session_id().cloned(),
                        configured.canvas_id().cloned(),
                        configured.layer_id().cloned(),
                        configured.width(),
                        configured.height(),
                    )
                    .map_err(|_| ServerError::bootstrap("development Canvas configuration is invalid"))?
                };
                let development_session_id = descriptor.session_id().clone();
                canvas_descriptors.push(descriptor.clone());
                development_canvas = Some(descriptor.clone());

                let catalog_has_documents = if let Some(store) = persistent_catalog_store.as_ref() {
                    !store
                        .list_all_documents()
                        .await
                        .map_err(|_| {
                            ServerError::bootstrap("Development Document catalog lookup failed")
                        })?
                        .is_empty()
                } else {
                    !document_catalog_port
                        .list_documents(&development_session_id)
                        .await
                        .map_err(|_| {
                            ServerError::bootstrap("Development Document catalog lookup failed")
                        })?
                        .is_empty()
                };
                if !catalog_has_documents {
                    if let Some(path) = self.config.development_pdf_path() {
                        let composition = bootstrap_development_pdf(
                            path,
                            development_session_id.clone(),
                            self.config.development_pdf_inspection_limits(),
                            asset_catalog.clone(),
                            asset_reader.clone(),
                            Some(memory_asset_store.as_ref()),
                            persistent_asset_store.as_ref().map(Arc::as_ref),
                        )
                        .await?;
                        if let Some(store) = persistent_catalog_store.as_ref() {
                            match store
                                .publish_document(composition.clone())
                                .await
                                .map_err(|_| ServerError::bootstrap("Development Document publication failed"))?
                            {
                                CatalogPublishOutcome::Inserted | CatalogPublishOutcome::Existing => {}
                                CatalogPublishOutcome::Conflict => {
                                    return Err(ServerError::bootstrap("development Document identity conflict"));
                                }
                            }
                        } else {
                            // Memory mode has no transactional graph publisher;
                            // the in-process Canvas catalog is populated below.
                            match memory_document_catalog.insert(composition.document().clone()) {
                                DocumentInsertOutcome::Inserted => {}
                                DocumentInsertOutcome::Existing | DocumentInsertOutcome::Conflict => {
                                    return Err(ServerError::bootstrap(
                                        "development Document identity collision",
                                    ));
                                }
                            }
                            canvas_descriptors.extend(
                                composition
                                    .page_canvases()
                                    .iter()
                                    .map(|entry| entry.canvas().clone()),
                            );
                        }
                    tracing::warn!(
                        session_id = %composition.document().session_id(),
                        document_id = %composition.document().document_id(),
                        asset_id = %composition.source_asset().asset_id(),
                        page_count = composition.document().pages().len(),
                        title = composition.document().title(),
                        "Development Document enabled"
                    );
                    for page in composition.document().pages() {
                        tracing::debug!(
                            page_index = page.page_index(),
                            page_id = %page.page_id(),
                            canvas_id = %page.overlay_canvas_id(),
                            width = page.display_geometry().width(),
                            height = page.display_geometry().height(),
                            rotation = ?page.display_geometry().rotation(),
                            "Development Document page mapping"
                        );
                    }
                    }
                }

                tracing::warn!(
                    session_id = %development_session_id,
                    canvas_id = %development_canvas.as_ref().expect("descriptor set").canvas_id(),
                    default_layer_id = %development_canvas.as_ref().expect("descriptor set").default_layer_id(),
                    canvas_width = development_canvas.as_ref().expect("descriptor set").space().width(),
                    canvas_height = development_canvas.as_ref().expect("descriptor set").space().height(),
                    "OrbitRelay development Canvas is enabled; development authorization is enabled"
                );
            }

            let canvas_catalog_port: Arc<dyn CanvasCatalog> = if persistent_catalog_store.is_some() {
                canvas_catalog_port
            } else {
                Arc::new(
                    DevelopmentCanvasCatalog::from_descriptors(canvas_descriptors)
                        .map_err(|_| ServerError::bootstrap("duplicate Canvas identity"))?,
                )
            };
            if self.config.development_mode() {
                let state_reader: Arc<dyn CanvasStateReader> =
                    Arc::new(EventStoreCanvasStateReader::new(event_store.clone()));
                let service = Arc::new(CanvasCommandService::new(
                    canvas_catalog_port.clone(),
                    state_reader,
                ));
                register_canvas_handlers(&handler_registry, service)
                    .map_err(|_| ServerError::bootstrap("Canvas handler registration failed"))?;
            }

            let read_service = Arc::new(DocumentReadService::new(
                document_catalog_port.clone(),
                asset_catalog.clone(),
                canvas_catalog_port.clone(),
            ));
            let read_authorizer: Arc<dyn orbitrelay_document_runtime::DocumentReadAuthorizer> =
                if self.config.development_mode() {
                    let session_id = development_canvas
                        .as_ref()
                        .expect("Development Canvas must exist")
                        .session_id()
                        .clone();
                    Arc::new(DevelopmentDocumentReadAuthorizer::new(session_id))
                } else {
                    Arc::new(RejectAllDocumentReadAuthorizer)
                };
            let asset_delivery = if self.config.asset_delivery().enabled() {
                Some(Arc::new(AssetDeliveryService::new(
                    asset_catalog.clone(),
                    asset_reader.clone(),
                    self.config.asset_delivery(),
                )?))
            } else {
                None
            };
            let mut query_registry = QueryRegistry::new();
            register_document_query_handlers(
                &mut query_registry,
                document_catalog_port.clone(),
                read_service.clone(),
                read_authorizer.clone(),
            )
            .map_err(|_| ServerError::bootstrap("Document Query handler registration failed"))?;
            let canvas_history_authorizer: Arc<dyn CanvasHistoryReadAuthorizer> =
                if self.config.development_mode() {
                    let session_id = development_canvas
                        .as_ref()
                        .expect("Development Canvas must exist")
                        .session_id()
                        .clone();
                    Arc::new(DevelopmentCanvasHistoryReadAuthorizer::new(session_id))
                } else {
                    Arc::new(RejectAllCanvasHistoryReadAuthorizer)
                };
            register_canvas_history_query_handler(
                &mut query_registry,
                canvas_catalog_port,
                canvas_history_authorizer,
                event_store.clone(),
                self.config.history_store_scan_limit(),
                self.config
                    .websocket_adapter()
                    .transport()
                    .max_message_bytes(),
            )
            .map_err(|_| ServerError::bootstrap("Canvas history Query registration failed"))?;
            if let Some(delivery) = asset_delivery.as_ref() {
                register_asset_access_query_handler(
                    &mut query_registry,
                    document_catalog_port.clone(),
                    Arc::clone(&read_authorizer),
                    Arc::clone(delivery),
                )
                .map_err(|_| ServerError::bootstrap("Asset access Query handler registration failed"))?;
            }
            let query_executor = Arc::new(RegisteredQueryExecutor::new(Arc::new(query_registry)));

            // Validate every published Document against all three catalogs
            // before exposing a listener. This deliberately does not scan
            // or decode the complete Event history at startup.
            if let Some(store) = persistent_catalog_store.as_ref() {
                for document in store
                    .list_all_documents()
                    .await
                    .map_err(|_| ServerError::bootstrap("Document catalog validation failed"))?
                {
                    asset_catalog
                        .get_asset(document.source_asset_id())
                        .await
                        .map_err(|_| ServerError::bootstrap("Document source Asset lookup failed"))?
                        .ok_or_else(|| {
                            ServerError::bootstrap("Document references a missing published Asset")
                        })?;
                    read_service
                        .get_document_view(document.document_id())
                        .await
                        .map_err(|_| ServerError::bootstrap("Document read model is inconsistent"))?;
                }
            } else if self.config.development_mode() {
                let session_id = development_canvas
                    .as_ref()
                    .expect("Development Canvas must exist")
                    .session_id()
                    .clone();
                for summary in document_catalog_port
                    .list_documents(&session_id)
                    .await
                    .map_err(|_| ServerError::bootstrap("Document catalog validation failed"))?
                {
                    read_service
                        .get_document_view(summary.document_id())
                        .await
                        .map_err(|_| ServerError::bootstrap("Document read model is inconsistent"))?;
                }
            }

            let runtime_context = RuntimeContext::new(Arc::new(SystemClock), self.authorizer.clone())
                .with_execution_coordinator(coordinator);
            let runtime = Arc::new(Runtime::new(handler_registry, runtime_context, pipeline));
            Ok(ServerContext::new_composed_with_services(
                self.node(&node_id, NodeState::Starting),
                lifecycle,
                runtime,
                event_store,
                event_bus,
                node_registry.clone(),
                development_canvas,
                Some(query_executor),
                asset_delivery,
                persistent_asset_store,
                persistent_catalog_store,
            ))
        }
        .await;

        if result.is_err() {
            // No context exists to run the normal shutdown state machine yet,
            // so publish the terminal state and remove the node directly.
            let _ = node_registry
                .register(self.node(&node_id, NodeState::Offline))
                .await;
            let _ = node_registry.unregister(&node_id).await;
        }
        result
    }

    #[cfg(test)]
    async fn build_with(
        &self,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        node_registry: Arc<dyn NodeRegistry>,
    ) -> Result<ServerContext, ServerError> {
        self.initialize_with(event_store, event_bus, node_registry)
            .await
    }

    /// Transitions the local node through shutdown states and unregisters it.
    pub async fn shutdown(&self, context: &ServerContext) -> Result<(), ServerError> {
        context.shutdown().await
    }

    async fn register_state_with(
        &self,
        node_registry: &dyn NodeRegistry,
        node_id: &NodeId,
        state: NodeState,
    ) -> Result<(), ServerError> {
        node_registry
            .register(self.node(node_id, state))
            .await
            .map_err(|_| ServerError::node_lifecycle("node state registration failed"))
    }

    fn node(&self, node_id: &NodeId, state: NodeState) -> Node {
        Node::new(
            node_id.clone(),
            self.config.node_metadata().clone(),
            state,
            [
                Capability::new("runtime"),
                Capability::new("storage"),
                Capability::new("sync"),
            ],
        )
    }
}

async fn bootstrap_development_pdf(
    path: &Path,
    session_id: SessionId,
    limits: PdfInspectionLimits,
    asset_catalog: Arc<dyn AssetCatalog>,
    asset_reader: Arc<dyn AssetReader>,
    memory_asset_store: Option<&MemoryAssetStore>,
    persistent_asset_store: Option<&LocalAssetStore>,
) -> Result<orbitrelay_document_runtime::DocumentComposition, ServerError> {
    let metadata = std::fs::metadata(path)
        .map_err(|_| ServerError::bootstrap("development PDF metadata could not be read"))?;
    let length = metadata.len();
    if length > limits.max_asset_bytes() {
        return Err(ServerError::bootstrap(
            "development PDF exceeds the inspection size limit",
        ));
    }
    let bytes = std::fs::read(path)
        .map_err(|_| ServerError::bootstrap("development PDF bytes could not be read"))?;
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(Sha256::digest(&bytes).as_slice());
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned);
    let asset = SourceAssetDescriptor::new(
        orbitrelay_asset::AssetId::new(),
        "application/pdf",
        length,
        ContentHash::from_bytes(digest),
        filename,
    )
    .map_err(|_| ServerError::bootstrap("development PDF asset metadata is invalid"))?;
    if let Some(store) = persistent_asset_store {
        store
            .insert_verified(asset.clone(), Bytes::from(bytes))
            .await
            .map_err(|_| ServerError::bootstrap("development PDF asset verification failed"))?;
    } else if let Some(store) = memory_asset_store {
        match store
            .insert_verified(asset.clone(), Bytes::from(bytes))
            .map_err(|_| ServerError::bootstrap("development PDF asset verification failed"))?
        {
            AssetInsertOutcome::Inserted | AssetInsertOutcome::Existing => {}
        }
    } else {
        return Err(ServerError::bootstrap(
            "no Asset ingest backend is available",
        ));
    }
    let inspector = PdfInspector::new(asset_catalog, asset_reader, limits);
    let pdf = inspector
        .inspect(asset.asset_id())
        .await
        .map_err(|_| ServerError::bootstrap("development PDF inspection failed"))?;
    if pdf.asset_id() != asset.asset_id() {
        return Err(ServerError::bootstrap(
            "development PDF metadata identity mismatch",
        ));
    }
    let pages = pdf
        .pages()
        .iter()
        .map(|page| DocumentSourcePage::new(page.page_index(), page.display_geometry()))
        .collect();
    let input = DocumentComposeInput::new(
        session_id,
        orbitrelay_document::DocumentType::Pdf,
        asset,
        pdf.title().map(str::to_owned),
        pages,
    )
    .map_err(|_| ServerError::bootstrap("development Document composition input is invalid"))?;
    DocumentComposer::new()
        .compose(input)
        .map_err(|_| ServerError::bootstrap("development Document composition failed"))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use orbitrelay_asset::{AssetId, ContentHash, SourceAssetDescriptor};
    use orbitrelay_catalog_sqlite::SQLiteCatalogStore;
    use orbitrelay_core::{Metadata, Timestamp};
    use orbitrelay_document::{DocumentType, PageDisplayGeometry, PageRotation};
    use orbitrelay_document_runtime::{DocumentComposeInput, DocumentComposer, DocumentSourcePage};
    use orbitrelay_node::{Node, NodeError, NodeId, NodeRegistry, NodeState};
    use orbitrelay_protocol::{
        Action, ActionId, ActorId, Event, EventId, EventType, Payload, SessionId,
    };
    use orbitrelay_runtime::{ActionAuthorizer, AuthorizationError};
    use orbitrelay_storage::{EventStore, MemoryEventStore};
    use orbitrelay_storage_sqlite::SQLiteEventStore;
    use orbitrelay_sync::{EventBus, MemoryEventBus};

    use super::Bootstrap;
    use crate::ServerConfig;

    struct TestAuthorizer;

    #[async_trait]
    impl ActionAuthorizer for TestAuthorizer {
        async fn authorize(&self, _action: &Action) -> Result<(), AuthorizationError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingRegistry {
        states: Mutex<Vec<NodeState>>,
        node: Mutex<Option<Node>>,
    }

    impl RecordingRegistry {
        fn states(&self) -> Vec<NodeState> {
            self.states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait]
    impl NodeRegistry for RecordingRegistry {
        async fn register(&self, node: Node) -> Result<(), NodeError> {
            self.states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(node.state());
            *self
                .node
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(node);
            Ok(())
        }

        async fn unregister(&self, _node_id: &NodeId) -> Result<(), NodeError> {
            *self
                .node
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            Ok(())
        }

        async fn get(&self, node_id: &NodeId) -> Result<Option<Node>, NodeError> {
            Ok(self
                .node
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .filter(|node| node.id() == node_id)
                .cloned())
        }

        async fn list(&self) -> Result<Vec<Node>, NodeError> {
            Ok(self
                .node
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .cloned()
                .collect())
        }
    }

    #[tokio::test]
    async fn initializes_context_and_registers_starting_node() {
        let bootstrap = Bootstrap::new(ServerConfig::default(), Arc::new(TestAuthorizer));
        let context = bootstrap.build().await.expect("bootstrap should succeed");

        let node = context
            .node_registry()
            .get(context.node_id())
            .await
            .expect("node lookup should succeed")
            .expect("node should be registered");
        assert_eq!(node.state(), NodeState::Starting);
        assert_eq!(context.lifecycle().state(), crate::LifecycleState::Starting);
        assert_eq!(context.health().state(), crate::HealthState::Starting);
        assert!(!context.health().is_ready());
        assert_eq!(
            context
                .node_registry()
                .list()
                .await
                .expect("list should succeed")
                .len(),
            1
        );
        assert!(!context
            .runtime()
            .registry()
            .contains(&orbitrelay_protocol::ActionType::new("unregistered.action")));
        assert!(!context
            .runtime()
            .registry()
            .contains(&orbitrelay_protocol::ActionType::new("dev.echo")));
    }

    #[tokio::test]
    async fn registers_echo_handler_only_in_development_mode() {
        let bootstrap = Bootstrap::new(
            ServerConfig::default().with_development_mode(true),
            Arc::new(TestAuthorizer),
        );
        let context = bootstrap.build().await.expect("bootstrap should succeed");

        assert!(context
            .runtime()
            .registry()
            .contains(&orbitrelay_protocol::ActionType::new("dev.echo")));
    }

    #[tokio::test]
    async fn shuts_down_and_unregisters_node() {
        let bootstrap = Bootstrap::new(ServerConfig::default(), Arc::new(TestAuthorizer));
        let registry = Arc::new(RecordingRegistry::default());
        let event_store: Arc<dyn EventStore> = Arc::new(MemoryEventStore::new());
        let event_bus: Arc<dyn EventBus> = Arc::new(MemoryEventBus::new());
        let context = bootstrap
            .build_with(event_store, event_bus, registry.clone())
            .await
            .expect("bootstrap should succeed");

        context
            .mark_ready()
            .await
            .expect("node should become ready");

        context.shutdown().await.expect("shutdown should succeed");

        assert_eq!(
            registry.states(),
            vec![
                NodeState::Starting,
                NodeState::Ready,
                NodeState::Draining,
                NodeState::Offline,
            ]
        );
        assert_eq!(context.lifecycle().state(), crate::LifecycleState::Stopped);
        assert_eq!(context.health().state(), crate::HealthState::Stopped);
        assert!(context
            .node_registry()
            .get(context.node_id())
            .await
            .expect("node lookup should succeed")
            .is_none());
    }

    #[tokio::test]
    async fn production_ignores_development_pdf_path() {
        let bootstrap = Bootstrap::new(
            ServerConfig::default().with_development_pdf_path(
                std::env::temp_dir().join("orbitrelay-path-must-not-be-read.pdf"),
            ),
            Arc::new(TestAuthorizer),
        );
        bootstrap
            .build()
            .await
            .expect("production bootstrap must ignore development PDF paths");
    }

    #[tokio::test]
    async fn invalid_development_pdf_unregisters_starting_node() {
        let path = std::env::temp_dir().join(format!("orbitrelay-invalid-{}.pdf", NodeId::new()));
        fs::write(&path, b"not a PDF").expect("invalid fixture should be written");
        let bootstrap = Bootstrap::new(
            ServerConfig::default()
                .with_development_mode(true)
                .with_development_pdf_path(&path),
            Arc::new(TestAuthorizer),
        );
        let registry = Arc::new(RecordingRegistry::default());
        let event_store: Arc<dyn EventStore> = Arc::new(MemoryEventStore::new());
        let event_bus: Arc<dyn EventBus> = Arc::new(MemoryEventBus::new());
        let result = bootstrap
            .build_with(event_store, event_bus, registry.clone())
            .await;
        assert!(result.is_err());
        assert_eq!(
            registry.states(),
            vec![NodeState::Starting, NodeState::Offline]
        );
        assert!(registry.list().await.expect("registry list").is_empty());
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn initializes_and_closes_sqlite_event_store_from_config() {
        let path = std::env::temp_dir().join(format!("orbitrelay-bootstrap-{}.db", NodeId::new()));
        let bootstrap = Bootstrap::new(
            ServerConfig::default().with_event_store_path(&path),
            Arc::new(TestAuthorizer),
        );
        let context = bootstrap
            .build()
            .await
            .expect("SQLite bootstrap should succeed");
        let session_id = SessionId::new();
        let event = Event::new(
            EventId::new(),
            session_id,
            ActorId::new(),
            ActionId::new(),
            EventType::new("bootstrap.sqlite"),
            Timestamp::from_unix_timestamp(1_700_000_000).expect("timestamp"),
            Payload::new(),
            Metadata::new(),
        );
        context
            .event_store()
            .append(event.clone())
            .await
            .expect("SQLite append should succeed");
        context
            .shutdown()
            .await
            .expect("SQLite shutdown should close worker");

        let reopened = SQLiteEventStore::open(&path).expect("reopen SQLite store");
        assert_eq!(
            reopened
                .get(event.id())
                .await
                .expect("get should succeed")
                .expect("event should persist")
                .event(),
            &event
        );
        reopened.close().await.expect("reopen close");
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("db-wal"));
        let _ = fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn persistent_development_canvas_binding_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "orbitrelay-catalog-bootstrap-{}.sqlite",
            NodeId::new()
        ));
        let config = ServerConfig::default()
            .with_development_mode(true)
            .with_catalog_store_path(&path);
        let first = Bootstrap::new(config.clone(), Arc::new(TestAuthorizer));
        let first_context = first.build().await.expect("persistent bootstrap");
        let first_canvas = first_context
            .development_canvas()
            .expect("development Canvas")
            .clone();
        first_context.shutdown().await.expect("first shutdown");

        let second = Bootstrap::new(config, Arc::new(TestAuthorizer));
        let second_context = second.build().await.expect("persistent reopen");
        assert_eq!(
            second_context
                .development_canvas()
                .expect("development Canvas"),
            &first_canvas
        );
        second_context.shutdown().await.expect("second shutdown");
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio::test]
    async fn persistent_catalog_missing_source_asset_blocks_bootstrap() {
        let path = std::env::temp_dir().join(format!(
            "orbitrelay-catalog-missing-asset-{}.sqlite",
            NodeId::new()
        ));
        let session_id = SessionId::new();
        let asset = SourceAssetDescriptor::new(
            AssetId::new(),
            "application/pdf",
            1,
            ContentHash::from_bytes([0x55; 32]),
            Some("missing.pdf".to_owned()),
        )
        .expect("asset");
        let input = DocumentComposeInput::new(
            session_id,
            DocumentType::Pdf,
            asset,
            Some("Missing Asset".to_owned()),
            vec![DocumentSourcePage::new(
                0,
                PageDisplayGeometry::new(10.0, 20.0, PageRotation::Deg0).expect("geometry"),
            )],
        )
        .expect("input");
        let composition = DocumentComposer::new().compose(input).expect("composition");
        let catalog = SQLiteCatalogStore::open(&path).expect("catalog");
        catalog
            .publish_document(composition)
            .await
            .expect("publish");
        catalog.close().await.expect("catalog close");

        let bootstrap = Bootstrap::new(
            ServerConfig::default()
                .with_development_mode(true)
                .with_catalog_store_path(&path),
            Arc::new(TestAuthorizer),
        );
        assert!(bootstrap.build().await.is_err());
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
