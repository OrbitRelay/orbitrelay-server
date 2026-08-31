//! SQLite-backed immutable Document and Canvas catalogs.
//!
//! This crate owns only the Catalog persistence boundary. Document and Canvas
//! read ports remain independent while one worker-owned SQLite connection
//! provides atomic publication of a complete Document/Page/Canvas/Layer graph.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use async_trait::async_trait;
use orbitrelay_canvas::{CanvasDescriptor, CanvasId, CanvasSpace, LayerId};
use orbitrelay_canvas_runtime::{CanvasCatalog, CanvasCatalogError};
use orbitrelay_core::EntityId;
use orbitrelay_document::{
    DocumentDescriptor, DocumentId, DocumentPageDescriptor, PageDisplayGeometry, PageRotation,
};
use orbitrelay_document_runtime::{
    DocumentCatalog, DocumentCatalogError, DocumentComposition, DocumentSummary,
};
use orbitrelay_protocol::SessionId;
use rusqlite::{params, Connection, Error as SqliteError, OptionalExtension, Transaction};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// The first Catalog schema supported by this adapter.
pub const SUPPORTED_SCHEMA_VERSION: i64 = 1;
/// Default bounded Catalog worker queue capacity.
pub const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 128;
const BUSY_TIMEOUT_MILLISECONDS: i64 = 5_000;
const BOOTSTRAP_DEFAULT_CANVAS: &str = "development/default_canvas";

/// The result of an immutable Catalog publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogPublishOutcome {
    /// A new immutable graph was committed.
    Inserted,
    /// The exact same graph was already committed.
    Existing,
    /// An immutable identity conflicts with the supplied graph.
    Conflict,
}

/// A private Development bootstrap binding persisted by the Catalog adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapBinding {
    /// The bound Development Session identity.
    pub session_id: SessionId,
    /// The bound Development Canvas identity.
    pub canvas_id: CanvasId,
}

/// Errors produced by Catalog persistence and recovery.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CatalogStoreError {
    /// A worker queue capacity was invalid.
    #[error("catalog command queue capacity must be greater than zero")]
    InvalidQueueCapacity,
    /// The worker or database is unavailable.
    #[error("catalog backend unavailable: {detail}")]
    Unavailable {
        /// Stable backend-neutral detail.
        detail: &'static str,
    },
    /// SQLite returned a backend error.
    #[error("catalog backend failure: {detail}")]
    Backend {
        /// Safe SQLite diagnostic.
        detail: String,
    },
    /// A persisted row or graph violates an invariant.
    #[error("catalog persistence corruption: {detail}")]
    Corruption {
        /// Stable corruption category.
        detail: &'static str,
    },
    /// The database schema cannot be served by this adapter.
    #[error("catalog schema is incompatible: {detail}")]
    Schema {
        /// Stable schema compatibility category.
        detail: &'static str,
    },
    /// An immutable identity conflicts with existing content.
    #[error("catalog immutable identity conflict")]
    Conflict,
    /// The supplied graph was invalid before persistence.
    #[error("catalog graph is invalid: {detail}")]
    InvalidGraph {
        /// Stable graph validation category.
        detail: &'static str,
    },
}

enum Command {
    GetDocument {
        document_id: DocumentId,
        reply: oneshot::Sender<Result<Option<DocumentDescriptor>, CatalogStoreError>>,
    },
    ListDocuments {
        session_id: SessionId,
        reply: oneshot::Sender<Result<Vec<DocumentSummary>, CatalogStoreError>>,
    },
    ListAllDocuments {
        reply: oneshot::Sender<Result<Vec<DocumentDescriptor>, CatalogStoreError>>,
    },
    GetCanvas {
        canvas_id: CanvasId,
        reply: oneshot::Sender<Result<Option<CanvasDescriptor>, CatalogStoreError>>,
    },
    PublishDocument {
        composition: DocumentComposition,
        reply: oneshot::Sender<Result<CatalogPublishOutcome, CatalogStoreError>>,
    },
    PublishStandalone {
        descriptor: CanvasDescriptor,
        reply: oneshot::Sender<Result<CatalogPublishOutcome, CatalogStoreError>>,
    },
    PublishStandaloneWithBinding {
        key: String,
        descriptor: CanvasDescriptor,
        binding: BootstrapBinding,
        reply: oneshot::Sender<Result<CatalogPublishOutcome, CatalogStoreError>>,
    },
    GetBinding {
        key: String,
        reply: oneshot::Sender<Result<Option<BootstrapBinding>, CatalogStoreError>>,
    },
    PutBinding {
        key: String,
        binding: BootstrapBinding,
        reply: oneshot::Sender<Result<(), CatalogStoreError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), CatalogStoreError>>,
    },
}

struct WorkerHandle {
    sender: Mutex<Option<mpsc::Sender<Command>>>,
    join: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
}

impl WorkerHandle {
    fn sender(&self) -> Result<mpsc::Sender<Command>, CatalogStoreError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable("catalog is closed"));
        }
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned()
            .ok_or_else(|| unavailable("catalog worker is unavailable"))
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let sender = self
                .sender
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(sender);
        }
        let join = self
            .join
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(join) = join {
            let _ = thread::Builder::new()
                .name("orbitrelay-catalog-join".to_owned())
                .spawn(move || {
                    let _ = join.join();
                });
        }
    }
}

/// A cloneable persistent Catalog sharing one worker and SQLite connection.
#[derive(Clone)]
pub struct SQLiteCatalogStore {
    path: Arc<PathBuf>,
    worker: Arc<WorkerHandle>,
    physical_store_id: EntityId,
}

impl SQLiteCatalogStore {
    /// Opens or creates a Catalog database with the default queue capacity.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CatalogStoreError> {
        Self::open_with_queue_capacity(path, DEFAULT_COMMAND_QUEUE_CAPACITY)
    }

    /// Opens or creates a Catalog database with an explicit bounded queue.
    pub fn open_with_queue_capacity(
        path: impl AsRef<Path>,
        queue_capacity: usize,
    ) -> Result<Self, CatalogStoreError> {
        if queue_capacity == 0 {
            return Err(CatalogStoreError::InvalidQueueCapacity);
        }
        let path = path.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let worker_path = path.clone();
        let join = thread::Builder::new()
            .name("orbitrelay-catalog-sqlite".to_owned())
            .spawn(move || match initialize_storage(&worker_path) {
                Ok((connection, physical_store_id)) => {
                    let _ = ready_sender.send(Ok(physical_store_id));
                    run_worker(connection, receiver);
                }
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                }
            })
            .map_err(|_| unavailable("could not start catalog worker"))?;
        let physical_store_id = match ready_receiver.recv() {
            Ok(Ok(id)) => id,
            Ok(Err(error)) => {
                let _ = join.join();
                return Err(error);
            }
            Err(_) => {
                let _ = join.join();
                return Err(unavailable("catalog worker initialization failed"));
            }
        };
        Ok(Self {
            path: Arc::new(path),
            worker: Arc::new(WorkerHandle {
                sender: Mutex::new(Some(sender)),
                join: Mutex::new(Some(join)),
                closed: AtomicBool::new(false),
            }),
            physical_store_id,
        })
    }

    /// Returns the adapter-private database path.
    #[must_use]
    pub fn database_path(&self) -> &Path {
        self.path.as_path()
    }

    /// Returns the persistent physical Store identity.
    #[must_use]
    pub const fn physical_store_id(&self) -> &EntityId {
        &self.physical_store_id
    }

    /// Publishes a complete Document/Page/Canvas/Layer graph atomically.
    pub async fn publish_document(
        &self,
        composition: DocumentComposition,
    ) -> Result<CatalogPublishOutcome, CatalogStoreError> {
        self.dispatch(|reply| Command::PublishDocument { composition, reply })
            .await
    }

    /// Loads all persisted Documents for an application recovery scan.
    ///
    /// This adapter-specific method does not change the public DocumentCatalog
    /// port; the Server uses it only for cross-store Asset validation.
    pub async fn list_all_documents(&self) -> Result<Vec<DocumentDescriptor>, CatalogStoreError> {
        self.dispatch(|reply| Command::ListAllDocuments { reply })
            .await
    }

    /// Publishes one standalone Canvas and all of its Layers atomically.
    pub async fn publish_standalone_canvas(
        &self,
        descriptor: CanvasDescriptor,
    ) -> Result<CatalogPublishOutcome, CatalogStoreError> {
        self.dispatch(|reply| Command::PublishStandalone { descriptor, reply })
            .await
    }

    /// Publishes a standalone Canvas and its application-private binding in one transaction.
    pub async fn publish_standalone_canvas_with_binding(
        &self,
        key: impl Into<String>,
        descriptor: CanvasDescriptor,
        binding: BootstrapBinding,
    ) -> Result<CatalogPublishOutcome, CatalogStoreError> {
        self.dispatch(|reply| Command::PublishStandaloneWithBinding {
            key: key.into(),
            descriptor,
            binding,
            reply,
        })
        .await
    }

    /// Gets an application-private Development bootstrap binding.
    pub async fn get_bootstrap_binding(
        &self,
        key: impl Into<String>,
    ) -> Result<Option<BootstrapBinding>, CatalogStoreError> {
        self.dispatch(|reply| Command::GetBinding {
            key: key.into(),
            reply,
        })
        .await
    }

    /// Stores an application-private Development bootstrap binding.
    pub async fn put_bootstrap_binding(
        &self,
        key: impl Into<String>,
        binding: BootstrapBinding,
    ) -> Result<(), CatalogStoreError> {
        self.dispatch(|reply| Command::PutBinding {
            key: key.into(),
            binding,
            reply,
        })
        .await
    }

    /// Returns the default Development standalone Canvas binding key.
    #[must_use]
    pub const fn default_bootstrap_binding_key() -> &'static str {
        BOOTSTRAP_DEFAULT_CANVAS
    }

    /// Gracefully drains accepted commands and joins the worker.
    pub async fn close(&self) -> Result<(), CatalogStoreError> {
        let already_closed = self.worker.closed.swap(true, Ordering::AcqRel);
        if !already_closed {
            let sender = self
                .worker
                .sender
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(sender) = sender {
                let (reply_sender, reply_receiver) = oneshot::channel();
                sender
                    .send(Command::Shutdown {
                        reply: reply_sender,
                    })
                    .await
                    .map_err(|_| unavailable("catalog worker stopped"))?;
                reply_receiver
                    .await
                    .map_err(|_| unavailable("catalog shutdown response was lost"))??;
            }
        }
        let join = self
            .worker
            .join
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(join) = join {
            tokio::task::spawn_blocking(move || {
                join.join()
                    .map_err(|_| unavailable("catalog worker panicked"))
            })
            .await
            .map_err(|_| unavailable("catalog worker join failed"))??;
        }
        Ok(())
    }

    async fn dispatch<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, CatalogStoreError>>) -> Command,
    ) -> Result<T, CatalogStoreError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(make(reply_sender))
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("catalog worker response was lost"))?
    }
}

#[async_trait]
impl DocumentCatalog for SQLiteCatalogStore {
    async fn get_document(
        &self,
        document_id: &DocumentId,
    ) -> Result<Option<DocumentDescriptor>, DocumentCatalogError> {
        self.dispatch(|reply| Command::GetDocument {
            document_id: document_id.clone(),
            reply,
        })
        .await
        .map_err(|error| DocumentCatalogError::new(error.to_string()))
    }

    async fn list_documents(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<DocumentSummary>, DocumentCatalogError> {
        self.dispatch(|reply| Command::ListDocuments {
            session_id: session_id.clone(),
            reply,
        })
        .await
        .map_err(|error| DocumentCatalogError::new(error.to_string()))
    }
}

#[async_trait]
impl CanvasCatalog for SQLiteCatalogStore {
    async fn get_canvas(
        &self,
        canvas_id: &CanvasId,
    ) -> Result<Option<CanvasDescriptor>, CanvasCatalogError> {
        self.dispatch(|reply| Command::GetCanvas {
            canvas_id: canvas_id.clone(),
            reply,
        })
        .await
        .map_err(|error| CanvasCatalogError::new(error.to_string()))
    }
}

fn initialize_storage(path: &Path) -> Result<(Connection, EntityId), CatalogStoreError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .map_err(|_| unavailable("catalog database directory could not be created"))?;
    }
    let connection = Connection::open(path).map_err(map_sqlite)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(map_sqlite)?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(map_sqlite)?;
    connection
        .pragma_update(None, "synchronous", "NORMAL")
        .map_err(map_sqlite)?;
    connection
        .busy_timeout(std::time::Duration::from_millis(
            BUSY_TIMEOUT_MILLISECONDS as u64,
        ))
        .map_err(map_sqlite)?;
    create_schema(&connection)?;
    let schema = metadata_value(&connection, "schema_version")?
        .ok_or_else(|| corruption("missing catalog schema version"))?;
    let schema_version = schema
        .parse::<i64>()
        .map_err(|_| corruption("malformed catalog schema version"))?;
    if schema_version > SUPPORTED_SCHEMA_VERSION {
        return Err(CatalogStoreError::Schema {
            detail: "schema version is newer than this adapter",
        });
    }
    if schema_version < SUPPORTED_SCHEMA_VERSION {
        return Err(CatalogStoreError::Schema {
            detail: "schema migration is required",
        });
    }
    let physical = metadata_value(&connection, "physical_store_id")?
        .ok_or_else(|| corruption("missing physical store identity"))?
        .parse::<EntityId>()
        .map_err(|_| corruption("malformed physical store identity"))?;
    let quick_check: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(map_sqlite)?;
    if quick_check != "ok" {
        return Err(corruption("SQLite quick_check failed"));
    }
    scan_integrity(&connection)?;
    Ok((connection, physical))
}

fn create_schema(connection: &Connection) -> Result<(), CatalogStoreError> {
    connection
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS storage_metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS documents (
                document_id TEXT PRIMARY KEY NOT NULL,
                session_id TEXT NOT NULL,
                document_type TEXT NOT NULL,
                source_asset_id TEXT NOT NULL,
                title TEXT NOT NULL,
                page_count INTEGER NOT NULL,
                publication_sequence INTEGER NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS canvases (
                canvas_id TEXT PRIMARY KEY NOT NULL,
                session_id TEXT NOT NULL,
                width REAL NOT NULL,
                height REAL NOT NULL,
                layer_count INTEGER NOT NULL,
                default_layer_id TEXT NOT NULL,
                FOREIGN KEY (canvas_id, default_layer_id)
                    REFERENCES layers(canvas_id, layer_id)
                    DEFERRABLE INITIALLY DEFERRED
            );
            CREATE TABLE IF NOT EXISTS layers (
                canvas_id TEXT NOT NULL,
                layer_id TEXT PRIMARY KEY NOT NULL,
                FOREIGN KEY (canvas_id) REFERENCES canvases(canvas_id),
                UNIQUE (canvas_id, layer_id)
            );
            CREATE TABLE IF NOT EXISTS document_pages (
                page_id TEXT PRIMARY KEY NOT NULL,
                document_id TEXT NOT NULL,
                page_index INTEGER NOT NULL,
                width REAL NOT NULL,
                height REAL NOT NULL,
                rotation INTEGER NOT NULL,
                overlay_canvas_id TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES documents(document_id),
                FOREIGN KEY (overlay_canvas_id) REFERENCES canvases(canvas_id),
                UNIQUE (document_id, page_index),
                UNIQUE (document_id, overlay_canvas_id)
            );
            CREATE TABLE IF NOT EXISTS bootstrap_bindings (
                binding_key TEXT PRIMARY KEY NOT NULL,
                session_id TEXT NOT NULL,
                canvas_id TEXT NOT NULL,
                FOREIGN KEY (canvas_id) REFERENCES canvases(canvas_id)
            );
            CREATE INDEX IF NOT EXISTS documents_session_sequence
                ON documents(session_id, publication_sequence);
            CREATE INDEX IF NOT EXISTS pages_document_index
                ON document_pages(document_id, page_index);
            CREATE INDEX IF NOT EXISTS layers_canvas
                ON layers(canvas_id, layer_id);
            ",
        )
        .map_err(map_sqlite)?;
    ensure_metadata(
        connection,
        "schema_version",
        &SUPPORTED_SCHEMA_VERSION.to_string(),
    )?;
    if metadata_value(connection, "physical_store_id")?.is_none() {
        let id = EntityId::new();
        connection
            .execute(
                "INSERT INTO storage_metadata (key, value) VALUES (?1, ?2)",
                params!["physical_store_id", id.to_string()],
            )
            .map_err(map_sqlite)?;
    }
    Ok(())
}

fn ensure_metadata(
    connection: &Connection,
    key: &str,
    value: &str,
) -> Result<(), CatalogStoreError> {
    connection
        .execute(
            "INSERT OR IGNORE INTO storage_metadata (key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .map_err(map_sqlite)?;
    Ok(())
}

fn metadata_value(connection: &Connection, key: &str) -> Result<Option<String>, CatalogStoreError> {
    connection
        .query_row(
            "SELECT value FROM storage_metadata WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_sqlite)
}

fn scan_integrity(connection: &Connection) -> Result<(), CatalogStoreError> {
    let mut foreign_keys = connection
        .prepare("PRAGMA foreign_key_check")
        .map_err(map_sqlite)?;
    if foreign_keys
        .query_map([], |_| Ok(()))
        .map_err(map_sqlite)?
        .count()
        != 0
    {
        return Err(corruption("Catalog foreign key check failed"));
    }
    let mut statement = connection
        .prepare(
            "SELECT document_id, session_id, document_type, source_asset_id, title, page_count, publication_sequence
             FROM documents ORDER BY publication_sequence ASC",
        )
        .map_err(map_sqlite)?;
    let document_ids = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .map_err(map_sqlite)?;
    let mut expected_sequence = 0_i64;
    for row in document_ids {
        let (document_id, session_id, document_type, asset_id, title, page_count, sequence) =
            row.map_err(map_sqlite)?;
        if sequence < 0 || sequence < expected_sequence {
            return Err(corruption("invalid Document publication sequence"));
        }
        expected_sequence = sequence.saturating_add(1);
        let document_id = document_id
            .parse::<DocumentId>()
            .map_err(|_| corruption("malformed document ID"))?;
        let session_id = session_id
            .parse::<SessionId>()
            .map_err(|_| corruption("malformed document Session ID"))?;
        let asset_id = asset_id
            .parse::<orbitrelay_asset::AssetId>()
            .map_err(|_| corruption("malformed source Asset ID"))?;
        let document_type = parse_document_type(&document_type)?;
        let pages = load_pages(connection, &document_id)?;
        if page_count <= 0 || page_count as usize != pages.len() {
            return Err(corruption("persisted Document page count mismatch"));
        }
        let document = DocumentDescriptor::new(
            document_id,
            session_id,
            document_type,
            asset_id,
            title,
            pages,
        )
        .map_err(|_| corruption("invalid persisted Document descriptor"))?;
        for page in document.pages() {
            let canvas = load_canvas(connection, page.overlay_canvas_id())?
                .ok_or_else(|| corruption("Document Page references missing Canvas"))?;
            if canvas.session_id() != document.session_id()
                || canvas.space().width() != page.display_geometry().width()
                || canvas.space().height() != page.display_geometry().height()
            {
                return Err(corruption("Document Page and Canvas descriptor mismatch"));
            }
        }
    }
    let mut canvases = connection
        .prepare("SELECT canvas_id FROM canvases ORDER BY canvas_id")
        .map_err(map_sqlite)?;
    let ids = canvases
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(map_sqlite)?;
    for id in ids {
        let id = id
            .map_err(map_sqlite)?
            .parse::<CanvasId>()
            .map_err(|_| corruption("malformed Canvas ID"))?;
        load_canvas(connection, &id)?
            .ok_or_else(|| corruption("Canvas disappeared during scan"))?;
    }
    let orphan: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM layers l LEFT JOIN canvases c ON c.canvas_id = l.canvas_id WHERE c.canvas_id IS NULL",
            [],
            |row| row.get(0),
        )
        .map_err(map_sqlite)?;
    if orphan != 0 {
        return Err(corruption("orphan Layer row"));
    }
    let mut bindings = connection
        .prepare("SELECT session_id, canvas_id FROM bootstrap_bindings")
        .map_err(map_sqlite)?;
    let rows = bindings
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(map_sqlite)?;
    for row in rows {
        let (session, canvas) = row.map_err(map_sqlite)?;
        let session = session
            .parse::<SessionId>()
            .map_err(|_| corruption("malformed bootstrap Session ID"))?;
        let canvas = canvas
            .parse::<CanvasId>()
            .map_err(|_| corruption("malformed bootstrap Canvas ID"))?;
        let descriptor = load_canvas(connection, &canvas)?
            .ok_or_else(|| corruption("bootstrap binding references missing Canvas"))?;
        if descriptor.session_id() != &session {
            return Err(corruption("bootstrap binding Session mismatch"));
        }
    }
    Ok(())
}

fn run_worker(mut connection: Connection, mut receiver: mpsc::Receiver<Command>) {
    while let Some(command) = receiver.blocking_recv() {
        let shutdown = matches!(command, Command::Shutdown { .. });
        match command {
            Command::GetDocument { document_id, reply } => {
                let _ = reply.send(load_document(&connection, &document_id));
            }
            Command::ListDocuments { session_id, reply } => {
                let _ = reply.send(list_documents(&connection, &session_id));
            }
            Command::ListAllDocuments { reply } => {
                let _ = reply.send(list_all_documents(&connection));
            }
            Command::GetCanvas { canvas_id, reply } => {
                let _ = reply.send(load_canvas(&connection, &canvas_id));
            }
            Command::PublishDocument { composition, reply } => {
                let _ = reply.send(publish_document(&mut connection, &composition));
            }
            Command::PublishStandalone { descriptor, reply } => {
                let _ = reply.send(publish_standalone(&mut connection, &descriptor));
            }
            Command::PublishStandaloneWithBinding {
                key,
                descriptor,
                binding,
                reply,
            } => {
                let _ = reply.send(publish_standalone_with_binding(
                    &mut connection,
                    &key,
                    &descriptor,
                    &binding,
                ));
            }
            Command::GetBinding { key, reply } => {
                let _ = reply.send(load_binding(&connection, &key));
            }
            Command::PutBinding {
                key,
                binding,
                reply,
            } => {
                let _ = reply.send(store_binding(&connection, &key, &binding));
            }
            Command::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
            }
        }
        if shutdown {
            break;
        }
    }
}

fn publish_document(
    connection: &mut Connection,
    composition: &DocumentComposition,
) -> Result<CatalogPublishOutcome, CatalogStoreError> {
    let document = composition.document();
    let transaction = connection.transaction().map_err(map_sqlite)?;
    if let Some(existing) = load_document_tx(&transaction, document.document_id())? {
        let mut same = existing == *document;
        for entry in composition.page_canvases() {
            let existing_canvas = load_canvas_tx(&transaction, entry.canvas().canvas_id())?
                .ok_or_else(|| corruption("published Document is missing its Canvas"))?;
            same &= existing_canvas == *entry.canvas();
        }
        if same {
            transaction.commit().map_err(map_sqlite)?;
            return Ok(CatalogPublishOutcome::Existing);
        }
        return Err(CatalogStoreError::Conflict);
    }
    if document_ids_collide(&transaction, composition)? {
        return Err(CatalogStoreError::Conflict);
    }
    let sequence: i64 = transaction
        .query_row(
            "SELECT COALESCE(MAX(publication_sequence), -1) + 1 FROM documents",
            [],
            |row| row.get(0),
        )
        .map_err(map_sqlite)?;
    for entry in composition.page_canvases() {
        insert_canvas_tx(&transaction, entry.canvas())?;
    }
    transaction
        .execute(
            "INSERT INTO documents (document_id, session_id, document_type, source_asset_id, title, page_count, publication_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                document.document_id().to_string(),
                document.session_id().to_string(),
                document_type_name(document.document_type()),
                document.source_asset_id().to_string(),
                document.title(),
                i64::try_from(document.pages().len())
                    .map_err(|_| CatalogStoreError::InvalidGraph {
                        detail: "Document page count exceeds SQLite capacity",
                    })?,
                sequence
            ],
        )
        .map_err(map_sqlite)?;
    for page in document.pages() {
        transaction
            .execute(
                "INSERT INTO document_pages (page_id, document_id, page_index, width, height, rotation, overlay_canvas_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    page.page_id().to_string(),
                    document.document_id().to_string(),
                    i64::from(page.page_index()),
                    page.display_geometry().width(),
                    page.display_geometry().height(),
                    i64::from(page.display_geometry().rotation().degrees()),
                    page.overlay_canvas_id().to_string()
                ],
            )
            .map_err(map_sqlite)?;
    }
    transaction.commit().map_err(map_sqlite)?;
    Ok(CatalogPublishOutcome::Inserted)
}

fn publish_standalone(
    connection: &mut Connection,
    descriptor: &CanvasDescriptor,
) -> Result<CatalogPublishOutcome, CatalogStoreError> {
    let transaction = connection.transaction().map_err(map_sqlite)?;
    if let Some(existing) = load_canvas_tx(&transaction, descriptor.canvas_id())? {
        if existing == *descriptor {
            transaction.commit().map_err(map_sqlite)?;
            return Ok(CatalogPublishOutcome::Existing);
        }
        return Err(CatalogStoreError::Conflict);
    }
    for layer_id in descriptor.layer_ids() {
        let exists: Option<String> = transaction
            .query_row(
                "SELECT canvas_id FROM layers WHERE layer_id = ?1",
                params![layer_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(map_sqlite)?;
        if exists.is_some() {
            return Err(CatalogStoreError::Conflict);
        }
    }
    insert_canvas_tx(&transaction, descriptor)?;
    transaction.commit().map_err(map_sqlite)?;
    Ok(CatalogPublishOutcome::Inserted)
}

fn publish_standalone_with_binding(
    connection: &mut Connection,
    key: &str,
    descriptor: &CanvasDescriptor,
    binding: &BootstrapBinding,
) -> Result<CatalogPublishOutcome, CatalogStoreError> {
    if binding.canvas_id != *descriptor.canvas_id()
        || binding.session_id != *descriptor.session_id()
    {
        return Err(CatalogStoreError::Conflict);
    }
    let transaction = connection.transaction().map_err(map_sqlite)?;
    let existing_binding: Option<(String, String)> = transaction
        .query_row(
            "SELECT session_id, canvas_id FROM bootstrap_bindings WHERE binding_key = ?1",
            params![key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(map_sqlite)?;
    if let Some((session, canvas)) = existing_binding {
        if session != binding.session_id.to_string() || canvas != binding.canvas_id.to_string() {
            return Err(CatalogStoreError::Conflict);
        }
        let existing = load_canvas_tx(&transaction, descriptor.canvas_id())?
            .ok_or_else(|| corruption("bootstrap binding references missing Canvas"))?;
        if existing != *descriptor {
            return Err(CatalogStoreError::Conflict);
        }
        transaction.commit().map_err(map_sqlite)?;
        return Ok(CatalogPublishOutcome::Existing);
    }
    let outcome = if let Some(existing) = load_canvas_tx(&transaction, descriptor.canvas_id())? {
        if existing != *descriptor {
            return Err(CatalogStoreError::Conflict);
        }
        CatalogPublishOutcome::Existing
    } else {
        insert_canvas_tx(&transaction, descriptor)?;
        CatalogPublishOutcome::Inserted
    };
    transaction
        .execute(
            "INSERT INTO bootstrap_bindings (binding_key, session_id, canvas_id) VALUES (?1, ?2, ?3)",
            params![key, binding.session_id.to_string(), binding.canvas_id.to_string()],
        )
        .map_err(map_sqlite)?;
    transaction.commit().map_err(map_sqlite)?;
    Ok(outcome)
}

fn document_ids_collide(
    transaction: &Transaction<'_>,
    composition: &DocumentComposition,
) -> Result<bool, CatalogStoreError> {
    let document = composition.document();
    let document_exists: Option<String> = transaction
        .query_row(
            "SELECT document_id FROM documents WHERE document_id = ?1",
            params![document.document_id().to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_sqlite)?;
    if document_exists.is_some() {
        return Ok(true);
    }
    for entry in composition.page_canvases() {
        let id = entry.canvas().canvas_id().to_string();
        let exists: Option<String> = transaction
            .query_row(
                "SELECT canvas_id FROM canvases WHERE canvas_id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()
            .map_err(map_sqlite)?;
        if exists.is_some() {
            return Ok(true);
        }
        for layer in entry.canvas().layer_ids() {
            let exists: Option<String> = transaction
                .query_row(
                    "SELECT layer_id FROM layers WHERE layer_id = ?1",
                    params![layer.to_string()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(map_sqlite)?;
            if exists.is_some() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn insert_canvas_tx(
    transaction: &Transaction<'_>,
    descriptor: &CanvasDescriptor,
) -> Result<(), CatalogStoreError> {
    transaction
        .execute(
            "INSERT INTO canvases (canvas_id, session_id, width, height, layer_count, default_layer_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                descriptor.canvas_id().to_string(),
                descriptor.session_id().to_string(),
                descriptor.space().width(),
                descriptor.space().height(),
                i64::try_from(descriptor.layer_ids().len()).map_err(|_| {
                    CatalogStoreError::InvalidGraph {
                        detail: "Canvas Layer count exceeds SQLite capacity",
                    }
                })?,
                descriptor.default_layer_id().to_string()
            ],
        )
        .map_err(map_sqlite)?;
    for layer_id in descriptor.layer_ids() {
        transaction
            .execute(
                "INSERT INTO layers (canvas_id, layer_id) VALUES (?1, ?2)",
                params![descriptor.canvas_id().to_string(), layer_id.to_string()],
            )
            .map_err(map_sqlite)?;
    }
    Ok(())
}

fn load_document(
    connection: &Connection,
    document_id: &DocumentId,
) -> Result<Option<DocumentDescriptor>, CatalogStoreError> {
    load_document_tx(connection, document_id)
}

fn load_document_tx(
    connection: &Connection,
    document_id: &DocumentId,
) -> Result<Option<DocumentDescriptor>, CatalogStoreError> {
    let row: Option<(String, String, String, String, String, i64)> = connection
        .query_row(
            "SELECT document_id, session_id, document_type, source_asset_id, title, page_count
             FROM documents WHERE document_id = ?1",
            params![document_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(map_sqlite)?;
    let Some((id, session, kind, asset, title, page_count)) = row else {
        return Ok(None);
    };
    let id = id
        .parse::<DocumentId>()
        .map_err(|_| corruption("malformed document ID"))?;
    let session = session
        .parse::<SessionId>()
        .map_err(|_| corruption("malformed document Session ID"))?;
    let kind = parse_document_type(&kind)?;
    let asset = asset
        .parse::<orbitrelay_asset::AssetId>()
        .map_err(|_| corruption("malformed source Asset ID"))?;
    let pages = load_pages(connection, &id)?;
    if page_count <= 0 || page_count as usize != pages.len() {
        return Err(corruption("persisted Document page count mismatch"));
    }
    DocumentDescriptor::new(id, session, kind, asset, title, pages)
        .map(Some)
        .map_err(|_| corruption("invalid persisted Document descriptor"))
}

fn load_pages(
    connection: &Connection,
    document_id: &DocumentId,
) -> Result<Vec<DocumentPageDescriptor>, CatalogStoreError> {
    let mut statement = connection
        .prepare(
            "SELECT page_id, page_index, width, height, rotation, overlay_canvas_id
             FROM document_pages WHERE document_id = ?1 ORDER BY page_index ASC",
        )
        .map_err(map_sqlite)?;
    let rows = statement
        .query_map(params![document_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(map_sqlite)?;
    let mut pages = Vec::new();
    for row in rows {
        let (page_id, index, width, height, rotation, canvas_id) = row.map_err(map_sqlite)?;
        let page_id = page_id
            .parse()
            .map_err(|_| corruption("malformed Page ID"))?;
        let page_index = u32::try_from(index).map_err(|_| corruption("invalid Page index"))?;
        let rotation = PageRotation::from_degrees(
            u16::try_from(rotation).map_err(|_| corruption("invalid Page rotation"))?,
        )
        .map_err(|_| corruption("invalid Page rotation"))?;
        let geometry = PageDisplayGeometry::new(width, height, rotation)
            .map_err(|_| corruption("invalid Page geometry"))?;
        let canvas_id = canvas_id
            .parse()
            .map_err(|_| corruption("malformed overlay Canvas ID"))?;
        pages.push(DocumentPageDescriptor::new(
            page_id, page_index, geometry, canvas_id,
        ));
    }
    Ok(pages)
}

fn load_canvas(
    connection: &Connection,
    canvas_id: &CanvasId,
) -> Result<Option<CanvasDescriptor>, CatalogStoreError> {
    load_canvas_tx(connection, canvas_id)
}

fn load_canvas_tx(
    connection: &Connection,
    canvas_id: &CanvasId,
) -> Result<Option<CanvasDescriptor>, CatalogStoreError> {
    let row: Option<(String, String, f64, f64, i64, String)> = connection
        .query_row(
            "SELECT canvas_id, session_id, width, height, layer_count, default_layer_id
             FROM canvases WHERE canvas_id = ?1",
            params![canvas_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(map_sqlite)?;
    let Some((id, session, width, height, layer_count, default_layer)) = row else {
        return Ok(None);
    };
    let id = id
        .parse::<CanvasId>()
        .map_err(|_| corruption("malformed Canvas ID"))?;
    let session = session
        .parse::<SessionId>()
        .map_err(|_| corruption("malformed Canvas Session ID"))?;
    let space = CanvasSpace::new(width, height).map_err(|_| corruption("invalid Canvas space"))?;
    let default_layer = default_layer
        .parse::<LayerId>()
        .map_err(|_| corruption("malformed default Layer ID"))?;
    let mut statement = connection
        .prepare("SELECT layer_id FROM layers WHERE canvas_id = ?1 ORDER BY layer_id ASC")
        .map_err(map_sqlite)?;
    let rows = statement
        .query_map(params![id.to_string()], |row| row.get::<_, String>(0))
        .map_err(map_sqlite)?;
    let mut layers = BTreeSet::new();
    for row in rows {
        layers.insert(
            row.map_err(map_sqlite)?
                .parse::<LayerId>()
                .map_err(|_| corruption("malformed Layer ID"))?,
        );
    }
    if layer_count <= 0 || layer_count as usize != layers.len() {
        return Err(corruption("persisted Canvas Layer count mismatch"));
    }
    CanvasDescriptor::new(id, session, space, layers, default_layer)
        .map(Some)
        .map_err(|_| corruption("invalid persisted Canvas descriptor"))
}

fn list_documents(
    connection: &Connection,
    session_id: &SessionId,
) -> Result<Vec<DocumentSummary>, CatalogStoreError> {
    let mut statement = connection
        .prepare(
            "SELECT document_id FROM documents WHERE session_id = ?1 ORDER BY publication_sequence ASC",
        )
        .map_err(map_sqlite)?;
    let ids = statement
        .query_map(params![session_id.to_string()], |row| {
            row.get::<_, String>(0)
        })
        .map_err(map_sqlite)?;
    let mut summaries = Vec::new();
    for id in ids {
        let id = id
            .map_err(map_sqlite)?
            .parse::<DocumentId>()
            .map_err(|_| corruption("malformed document ID"))?;
        let document = load_document(connection, &id)?
            .ok_or_else(|| corruption("publication order references missing Document"))?;
        summaries.push(
            DocumentSummary::from_document(&document)
                .map_err(|_| corruption("Document page count overflowed"))?,
        );
    }
    Ok(summaries)
}

fn list_all_documents(
    connection: &Connection,
) -> Result<Vec<DocumentDescriptor>, CatalogStoreError> {
    let mut statement = connection
        .prepare("SELECT document_id FROM documents ORDER BY publication_sequence ASC")
        .map_err(map_sqlite)?;
    let ids = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(map_sqlite)?;
    let mut documents = Vec::new();
    for id in ids {
        let id = id
            .map_err(map_sqlite)?
            .parse::<DocumentId>()
            .map_err(|_| corruption("malformed document ID"))?;
        documents.push(
            load_document(connection, &id)?
                .ok_or_else(|| corruption("publication order references missing Document"))?,
        );
    }
    Ok(documents)
}

fn load_binding(
    connection: &Connection,
    key: &str,
) -> Result<Option<BootstrapBinding>, CatalogStoreError> {
    let row: Option<(String, String)> = connection
        .query_row(
            "SELECT session_id, canvas_id FROM bootstrap_bindings WHERE binding_key = ?1",
            params![key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(map_sqlite)?;
    row.map(|(session, canvas)| {
        Ok(BootstrapBinding {
            session_id: session
                .parse()
                .map_err(|_| corruption("malformed bootstrap Session ID"))?,
            canvas_id: canvas
                .parse()
                .map_err(|_| corruption("malformed bootstrap Canvas ID"))?,
        })
    })
    .transpose()
}

fn store_binding(
    connection: &Connection,
    key: &str,
    binding: &BootstrapBinding,
) -> Result<(), CatalogStoreError> {
    let transaction = connection.unchecked_transaction().map_err(map_sqlite)?;
    let existing: Option<(String, String)> = transaction
        .query_row(
            "SELECT session_id, canvas_id FROM bootstrap_bindings WHERE binding_key = ?1",
            params![key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(map_sqlite)?;
    let expected = (
        binding.session_id.to_string(),
        binding.canvas_id.to_string(),
    );
    if let Some(existing) = existing {
        if existing != expected {
            return Err(CatalogStoreError::Conflict);
        }
        transaction.commit().map_err(map_sqlite)?;
        return Ok(());
    }
    transaction
        .execute(
            "INSERT INTO bootstrap_bindings (binding_key, session_id, canvas_id) VALUES (?1, ?2, ?3)",
            params![key, expected.0, expected.1],
        )
        .map_err(map_sqlite)?;
    transaction.commit().map_err(map_sqlite)
}

fn parse_document_type(
    value: &str,
) -> Result<orbitrelay_document::DocumentType, CatalogStoreError> {
    match value {
        "pdf" => Ok(orbitrelay_document::DocumentType::Pdf),
        _ => Err(corruption("unknown Document type")),
    }
}

fn document_type_name(value: orbitrelay_document::DocumentType) -> &'static str {
    match value {
        orbitrelay_document::DocumentType::Pdf => "pdf",
    }
}

fn map_sqlite(error: SqliteError) -> CatalogStoreError {
    CatalogStoreError::Backend {
        detail: error.to_string(),
    }
}

fn map_send_error(error: mpsc::error::TrySendError<Command>) -> CatalogStoreError {
    match error {
        mpsc::error::TrySendError::Full(_) => unavailable("catalog command queue is full"),
        mpsc::error::TrySendError::Closed(_) => unavailable("catalog worker is unavailable"),
    }
}

fn unavailable(detail: &'static str) -> CatalogStoreError {
    CatalogStoreError::Unavailable { detail }
}

fn corruption(detail: &'static str) -> CatalogStoreError {
    CatalogStoreError::Corruption { detail }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbitrelay_asset::{AssetId, ContentHash, SourceAssetDescriptor};
    use orbitrelay_document::{DocumentType, PageDisplayGeometry, PageRotation};
    use orbitrelay_document_runtime::{DocumentComposeInput, DocumentComposer, DocumentSourcePage};
    use tempfile::tempdir;

    fn composition(session_id: SessionId) -> DocumentComposition {
        let asset = SourceAssetDescriptor::new(
            AssetId::new(),
            "application/pdf",
            3,
            ContentHash::from_bytes([7; 32]),
            Some("test.pdf".to_owned()),
        )
        .expect("asset");
        let input = DocumentComposeInput::new(
            session_id,
            DocumentType::Pdf,
            asset,
            Some("Test".to_owned()),
            vec![DocumentSourcePage::new(
                0,
                PageDisplayGeometry::new(10.0, 20.0, PageRotation::Deg90).expect("geometry"),
            )],
        )
        .expect("input");
        DocumentComposer::new().compose(input).expect("composition")
    }

    #[tokio::test]
    async fn publishes_reopens_and_preserves_identity() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("catalog.sqlite");
        let session = SessionId::new();
        let composition = composition(session.clone());
        let document = composition.document().clone();
        let canvas = composition.page_canvases()[0].canvas().clone();
        let store = SQLiteCatalogStore::open(&path).expect("open");
        let physical_store_id = store.physical_store_id().clone();
        assert_eq!(
            store
                .publish_document(composition.clone())
                .await
                .expect("publish"),
            CatalogPublishOutcome::Inserted
        );
        assert_eq!(
            store.publish_document(composition).await.expect("existing"),
            CatalogPublishOutcome::Existing
        );
        store.close().await.expect("close");
        let reopened = SQLiteCatalogStore::open(&path).expect("reopen");
        assert_eq!(reopened.physical_store_id(), &physical_store_id);
        let loaded = reopened
            .get_document(document.document_id())
            .await
            .expect("get")
            .expect("document");
        assert_eq!(loaded, document);
        assert_eq!(
            reopened
                .get_canvas(canvas.canvas_id())
                .await
                .expect("canvas")
                .expect("canvas"),
            canvas
        );
        reopened.close().await.expect("close");
    }

    #[tokio::test]
    async fn standalone_publish_is_idempotent_and_binding_is_stable() {
        let dir = tempdir().expect("tempdir");
        let store = SQLiteCatalogStore::open(dir.path().join("catalog.sqlite")).expect("open");
        let session = SessionId::new();
        let layer = LayerId::new();
        let canvas = CanvasDescriptor::new(
            CanvasId::new(),
            session.clone(),
            CanvasSpace::new(100.0, 50.0).expect("space"),
            [layer.clone()],
            layer,
        )
        .expect("canvas");
        assert_eq!(
            store
                .publish_standalone_canvas(canvas.clone())
                .await
                .expect("publish"),
            CatalogPublishOutcome::Inserted
        );
        assert_eq!(
            store
                .publish_standalone_canvas(canvas.clone())
                .await
                .expect("existing"),
            CatalogPublishOutcome::Existing
        );
        let binding = BootstrapBinding {
            session_id: session.clone(),
            canvas_id: canvas.canvas_id().clone(),
        };
        store
            .put_bootstrap_binding("test", binding.clone())
            .await
            .expect("binding");
        assert_eq!(
            store
                .get_bootstrap_binding("test")
                .await
                .expect("get binding"),
            Some(binding)
        );
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn standalone_canvas_and_binding_can_commit_atomically() {
        let dir = tempdir().expect("tempdir");
        let store = SQLiteCatalogStore::open(dir.path().join("catalog.sqlite")).expect("open");
        let session_id = SessionId::new();
        let layer_id = LayerId::new();
        let canvas = CanvasDescriptor::new(
            CanvasId::new(),
            session_id.clone(),
            CanvasSpace::new(4.0, 5.0).expect("space"),
            [layer_id],
            LayerId::new(),
        );
        assert!(canvas.is_err());
        let layer_id = LayerId::new();
        let canvas = CanvasDescriptor::new(
            CanvasId::new(),
            session_id.clone(),
            CanvasSpace::new(4.0, 5.0).expect("space"),
            [layer_id.clone()],
            layer_id,
        )
        .expect("canvas");
        let binding = BootstrapBinding {
            session_id,
            canvas_id: canvas.canvas_id().clone(),
        };
        assert_eq!(
            store
                .publish_standalone_canvas_with_binding("atomic", canvas.clone(), binding.clone())
                .await
                .expect("publish"),
            CatalogPublishOutcome::Inserted
        );
        assert_eq!(
            store
                .get_bootstrap_binding("atomic")
                .await
                .expect("binding"),
            Some(binding)
        );
        assert_eq!(
            store
                .get_canvas(canvas.canvas_id())
                .await
                .expect("canvas")
                .expect("canvas"),
            canvas
        );
        store.close().await.expect("close");
    }

    #[test]
    fn schema_too_new_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("catalog.sqlite");
        let store = SQLiteCatalogStore::open(&path).expect("open");
        futures_close(store);
        let connection = Connection::open(&path).expect("sqlite");
        connection
            .execute(
                "UPDATE storage_metadata SET value = '99' WHERE key = 'schema_version'",
                [],
            )
            .expect("schema update");
        drop(connection);
        assert!(matches!(
            SQLiteCatalogStore::open(&path),
            Err(CatalogStoreError::Schema { .. })
        ));
    }

    #[tokio::test]
    async fn missing_trailing_page_is_rejected_on_reopen() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("catalog.sqlite");
        let asset = SourceAssetDescriptor::new(
            AssetId::new(),
            "application/pdf",
            1,
            ContentHash::from_bytes([8; 32]),
            None,
        )
        .expect("asset");
        let session = SessionId::new();
        let input = DocumentComposeInput::new(
            session,
            DocumentType::Pdf,
            asset,
            Some("two pages".to_owned()),
            vec![
                DocumentSourcePage::new(
                    0,
                    PageDisplayGeometry::new(10.0, 20.0, PageRotation::Deg0).expect("geometry"),
                ),
                DocumentSourcePage::new(
                    1,
                    PageDisplayGeometry::new(30.0, 40.0, PageRotation::Deg90).expect("geometry"),
                ),
            ],
        )
        .expect("input");
        let composition = DocumentComposer::new().compose(input).expect("composition");
        let store = SQLiteCatalogStore::open(&path).expect("open");
        store.publish_document(composition).await.expect("publish");
        store.close().await.expect("close");
        let connection = Connection::open(&path).expect("sqlite");
        connection
            .execute("DELETE FROM document_pages WHERE page_index = 1", [])
            .expect("delete page");
        drop(connection);
        assert!(matches!(
            SQLiteCatalogStore::open(&path),
            Err(CatalogStoreError::Corruption { .. })
        ));
    }

    #[tokio::test]
    async fn missing_non_default_layer_is_rejected_on_reopen() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("catalog.sqlite");
        let default_layer = LayerId::new();
        let extra_layer = LayerId::new();
        let descriptor = CanvasDescriptor::new(
            CanvasId::new(),
            SessionId::new(),
            CanvasSpace::new(100.0, 100.0).expect("space"),
            [default_layer.clone(), extra_layer.clone()],
            default_layer,
        )
        .expect("Canvas");
        let store = SQLiteCatalogStore::open(&path).expect("open");
        store
            .publish_standalone_canvas(descriptor)
            .await
            .expect("publish");
        store.close().await.expect("close");
        let connection = Connection::open(&path).expect("sqlite");
        connection
            .execute(
                "DELETE FROM layers WHERE layer_id = ?1",
                params![extra_layer.to_string()],
            )
            .expect("delete Layer");
        drop(connection);
        assert!(matches!(
            SQLiteCatalogStore::open(&path),
            Err(CatalogStoreError::Corruption { .. })
        ));
    }

    #[tokio::test]
    async fn immutable_document_conflict_does_not_overwrite_existing_graph() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("catalog.sqlite");
        let original = composition(SessionId::new());
        let document = original.document();
        let conflicting_document = DocumentDescriptor::new(
            document.document_id().clone(),
            document.session_id().clone(),
            document.document_type(),
            document.source_asset_id().clone(),
            "different title",
            document.pages().to_vec(),
        )
        .expect("conflicting document");
        let conflicting = DocumentComposition::new(
            conflicting_document,
            original.source_asset().clone(),
            original.page_canvases().to_vec(),
        )
        .expect("conflicting composition");
        let store = SQLiteCatalogStore::open(&path).expect("open");
        assert_eq!(
            store
                .publish_document(original.clone())
                .await
                .expect("publish"),
            CatalogPublishOutcome::Inserted
        );
        assert_eq!(
            store.publish_document(conflicting).await,
            Err(CatalogStoreError::Conflict)
        );
        assert_eq!(
            store
                .get_document(original.document().document_id())
                .await
                .expect("get")
                .expect("document"),
            *original.document()
        );
        store.close().await.expect("close");
    }

    fn futures_close(store: SQLiteCatalogStore) {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(store.close()).expect("close");
    }
}
