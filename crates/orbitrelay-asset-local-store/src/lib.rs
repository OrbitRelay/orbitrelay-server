//! Persistent local Asset metadata and immutable filesystem blob storage.
//!
//! The adapter keeps the pure Asset domain and read ports independent from
//! filesystem paths, SQLite, and publication credentials. Metadata is stored
//! in SQLite while verified bytes are stored as immutable files under one data
//! root. A small worker owns the SQLite connection; range reads use Tokio
//! filesystem operations and never load an Asset beyond the requested range.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{
    collections::HashSet,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use async_trait::async_trait;
use bytes::Bytes;
use orbitrelay_asset::{AssetId, ContentHash, SourceAssetDescriptor};
use orbitrelay_asset_runtime::{
    AssetByteChunk, AssetByteRange, AssetCatalog, AssetCatalogError, AssetInsertOutcome,
    AssetReadError, AssetReader,
};
use orbitrelay_core::EntityId;
use rusqlite::{params, Connection, Error as SqliteError, ErrorCode, OptionalExtension, Row};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, SeekFrom as AsyncSeekFrom},
    sync::{mpsc, oneshot},
};

/// The first metadata schema supported by this adapter.
pub const SUPPORTED_SCHEMA_VERSION: i64 = 1;

/// Default bounded metadata worker queue capacity.
pub const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 256;

const BUSY_TIMEOUT_MILLISECONDS: i64 = 5_000;
const STAGED: &str = "staged";
const PUBLISHED: &str = "published";
const BLOB_SUFFIX: &str = ".blob";
const READ_BUFFER_SIZE: usize = 64 * 1024;

/// Errors produced by local persistent Asset ingest and lifecycle operations.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum LocalAssetStoreError {
    /// The configured queue capacity is invalid.
    #[error("asset metadata command queue capacity must be greater than zero")]
    InvalidQueueCapacity,

    /// The Asset length cannot be represented by the metadata backend.
    #[error("asset {asset_id} length {length} cannot be represented by SQLite")]
    LengthOverflow {
        /// The affected Asset identity.
        asset_id: AssetId,
        /// The supplied byte length.
        length: u64,
    },

    /// The descriptor length differs from the streamed bytes.
    #[error("asset {asset_id} length mismatch: expected {expected}, actual {actual}")]
    LengthMismatch {
        /// The affected Asset identity.
        asset_id: AssetId,
        /// The descriptor length.
        expected: u64,
        /// The streamed length.
        actual: u64,
    },

    /// The descriptor hash differs from the streamed bytes.
    #[error("asset {asset_id} content hash mismatch")]
    HashMismatch {
        /// The affected Asset identity.
        asset_id: AssetId,
        /// The descriptor hash.
        expected: ContentHash,
        /// The calculated hash.
        actual: ContentHash,
    },

    /// The identity already exists with different immutable content.
    #[error("asset {asset_id} conflicts with immutable existing content")]
    AssetConflict {
        /// The conflicting Asset identity.
        asset_id: AssetId,
    },

    /// A previous staged ingest is still present in the metadata store.
    #[error("asset {asset_id} has an unfinished ingest")]
    AssetInProgress {
        /// The affected Asset identity.
        asset_id: AssetId,
    },

    /// The metadata worker or database is temporarily unavailable.
    #[error("asset metadata backend unavailable: {detail}")]
    BackendUnavailable {
        /// A safe backend-neutral detail.
        detail: &'static str,
    },

    /// The metadata backend failed or contains invalid authoritative data.
    #[error("asset metadata backend failure: {detail}")]
    BackendFailure {
        /// A safe backend-neutral detail.
        detail: &'static str,
    },

    /// Filesystem storage failed at a safe, path-free boundary.
    #[error("asset filesystem operation failed: {detail}")]
    Filesystem {
        /// A safe detail that never contains an absolute path.
        detail: &'static str,
    },

    /// Persistent metadata or blob state is corrupt.
    #[error("asset persistence corruption: {detail}")]
    Corruption {
        /// A safe corruption detail.
        detail: &'static str,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationState {
    Staged,
    Published,
}

impl PublicationState {
    fn parse(value: &str) -> Result<Self, LocalAssetStoreError> {
        match value {
            STAGED => Ok(Self::Staged),
            PUBLISHED => Ok(Self::Published),
            _ => Err(corruption("unknown Asset publication state")),
        }
    }
}

#[derive(Clone)]
struct AssetRecord {
    descriptor: SourceAssetDescriptor,
    blob_key: String,
    state: PublicationState,
}

enum Command {
    Lookup {
        asset_id: AssetId,
        reply: oneshot::Sender<Result<Option<AssetRecord>, LocalAssetStoreError>>,
    },
    Stage {
        descriptor: SourceAssetDescriptor,
        blob_key: String,
        reply: oneshot::Sender<Result<AssetInsertOutcome, LocalAssetStoreError>>,
    },
    Publish {
        descriptor: SourceAssetDescriptor,
        blob_key: String,
        reply: oneshot::Sender<Result<(), LocalAssetStoreError>>,
    },
    RemoveStaged {
        asset_id: AssetId,
        reply: oneshot::Sender<Result<(), LocalAssetStoreError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), LocalAssetStoreError>>,
    },
}

struct WorkerHandle {
    sender: Mutex<Option<mpsc::Sender<Command>>>,
    join: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
}

impl WorkerHandle {
    fn sender(&self) -> Result<mpsc::Sender<Command>, LocalAssetStoreError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable("local Asset store is closed"));
        }
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned()
            .ok_or_else(|| unavailable("local Asset metadata worker is unavailable"))
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
                .name("orbitrelay-asset-local-join".to_owned())
                .spawn(move || {
                    let _ = join.join();
                });
        }
    }
}

/// A local persistent Asset catalog and range reader.
///
/// The adapter is cloneable. Clones share one metadata worker, one SQLite
/// database, and one filesystem root. Ingests are serialized by an adapter
/// lock so concurrent identical or conflicting operations have a simple,
/// deterministic publication boundary.
#[derive(Clone)]
pub struct LocalAssetStore {
    root: Arc<PathBuf>,
    worker: Arc<WorkerHandle>,
    physical_store_id: EntityId,
    ingest_lock: Arc<tokio::sync::Mutex<()>>,
}

impl LocalAssetStore {
    /// Opens or creates a local Asset store with the default queue capacity.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, LocalAssetStoreError> {
        Self::open_with_queue_capacity(root, DEFAULT_COMMAND_QUEUE_CAPACITY)
    }

    /// Opens or creates a local Asset store with an explicit bounded queue.
    pub fn open_with_queue_capacity(
        root: impl AsRef<Path>,
        queue_capacity: usize,
    ) -> Result<Self, LocalAssetStoreError> {
        if queue_capacity == 0 {
            return Err(LocalAssetStoreError::InvalidQueueCapacity);
        }
        let root = root.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let worker_root = root.clone();
        let join = thread::Builder::new()
            .name("orbitrelay-asset-local".to_owned())
            .spawn(move || match initialize_storage(&worker_root) {
                Ok((connection, physical_store_id)) => {
                    let _ = ready_sender.send(Ok(physical_store_id.clone()));
                    run_worker(connection, receiver);
                }
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                }
            })
            .map_err(|_| unavailable("could not start local Asset metadata worker"))?;

        let physical_store_id = match ready_receiver.recv() {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                let _ = join.join();
                return Err(error);
            }
            Err(_) => {
                let _ = join.join();
                return Err(unavailable(
                    "local Asset metadata worker initialization failed",
                ));
            }
        };
        Ok(Self {
            root: Arc::new(root),
            worker: Arc::new(WorkerHandle {
                sender: Mutex::new(Some(sender)),
                join: Mutex::new(Some(join)),
                closed: AtomicBool::new(false),
            }),
            physical_store_id,
            ingest_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Returns the adapter-private data root.
    #[must_use]
    pub fn data_root(&self) -> &Path {
        self.root.as_path()
    }

    /// Returns the persistent physical store identity for diagnostics.
    #[must_use]
    pub const fn physical_store_id(&self) -> &EntityId {
        &self.physical_store_id
    }

    /// Inserts an already available byte value through the streaming ingest
    /// path. This convenience method is adapter-specific and is not an upload
    /// or generic `AssetWriter` API.
    pub async fn insert_verified(
        &self,
        descriptor: SourceAssetDescriptor,
        bytes: Bytes,
    ) -> Result<AssetInsertOutcome, LocalAssetStoreError> {
        self.ingest(descriptor, std::io::Cursor::new(bytes)).await
    }

    /// Streams bytes into a staging file, verifies the descriptor, and
    /// publishes one immutable Asset.
    pub async fn ingest<R>(
        &self,
        descriptor: SourceAssetDescriptor,
        mut reader: R,
    ) -> Result<AssetInsertOutcome, LocalAssetStoreError>
    where
        R: AsyncRead + Unpin + Send,
    {
        if descriptor.byte_length() > i64::MAX as u64 {
            return Err(LocalAssetStoreError::LengthOverflow {
                asset_id: descriptor.asset_id().clone(),
                length: descriptor.byte_length(),
            });
        }

        let _guard = self.ingest_lock.lock().await;
        if let Some(existing) = self.lookup(&descriptor.asset_id().clone()).await? {
            match existing.state {
                PublicationState::Published if existing.descriptor == descriptor => {
                    return Ok(AssetInsertOutcome::Existing);
                }
                PublicationState::Published => {
                    return Err(LocalAssetStoreError::AssetConflict {
                        asset_id: descriptor.asset_id().clone(),
                    });
                }
                PublicationState::Staged => {
                    return Err(LocalAssetStoreError::AssetInProgress {
                        asset_id: descriptor.asset_id().clone(),
                    });
                }
            }
        }

        let asset_id = descriptor.asset_id().clone();
        let blob_key = blob_key(&asset_id);
        let staging_path =
            self.root
                .join("staging")
                .join(format!("{}-{}.part", asset_id, AssetId::new()));
        let final_path = self.root.join("blobs").join(&blob_key);

        let verified = match write_staging(&staging_path, &mut reader, &descriptor).await {
            Ok(value) => value,
            Err(error) => {
                remove_file_if_present(&staging_path).await?;
                return Err(error);
            }
        };
        if verified.length != descriptor.byte_length() {
            remove_file_if_present(&staging_path).await?;
            return Err(LocalAssetStoreError::LengthMismatch {
                asset_id,
                expected: descriptor.byte_length(),
                actual: verified.length,
            });
        }
        if verified.hash != *descriptor.content_hash() {
            remove_file_if_present(&staging_path).await?;
            return Err(LocalAssetStoreError::HashMismatch {
                asset_id,
                expected: descriptor.content_hash().clone(),
                actual: verified.hash,
            });
        }

        let stage_result = self
            .dispatch_stage(descriptor.clone(), blob_key.clone())
            .await;
        let stage_outcome = match stage_result {
            Ok(outcome) => outcome,
            Err(error) => {
                remove_file_if_present(&staging_path).await?;
                return Err(error);
            }
        };
        match stage_outcome {
            AssetInsertOutcome::Existing => {
                remove_file_if_present(&staging_path).await?;
                return Ok(AssetInsertOutcome::Existing);
            }
            AssetInsertOutcome::Inserted => {}
        }

        if let Err(error) = tokio::fs::rename(&staging_path, &final_path).await {
            let _ = self.dispatch_remove_staged(asset_id.clone()).await;
            remove_file_if_present(&staging_path).await?;
            return Err(map_filesystem_error(error));
        }

        if let Err(error) = self
            .dispatch_publish(descriptor.clone(), blob_key.clone())
            .await
        {
            let _ = quarantine_async(&self.root, &final_path).await;
            let _ = self.dispatch_remove_staged(asset_id).await;
            return Err(error);
        }
        Ok(AssetInsertOutcome::Inserted)
    }

    /// Gracefully drains accepted metadata commands and closes the worker.
    pub async fn close(&self) -> Result<(), LocalAssetStoreError> {
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
                    .map_err(|_| unavailable("local Asset metadata worker stopped"))?;
                reply_receiver.await.map_err(|_| {
                    unavailable("local Asset metadata shutdown response was lost")
                })??;
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
                    .map_err(|_| unavailable("local Asset metadata worker panicked"))
            })
            .await
            .map_err(|_| unavailable("local Asset metadata worker join failed"))??;
        }
        Ok(())
    }

    async fn lookup(
        &self,
        asset_id: &AssetId,
    ) -> Result<Option<AssetRecord>, LocalAssetStoreError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(Command::Lookup {
                asset_id: asset_id.clone(),
                reply: reply_sender,
            })
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("local Asset metadata response was lost"))?
    }

    async fn dispatch_stage(
        &self,
        descriptor: SourceAssetDescriptor,
        blob_key: String,
    ) -> Result<AssetInsertOutcome, LocalAssetStoreError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(Command::Stage {
                descriptor,
                blob_key,
                reply: reply_sender,
            })
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("local Asset stage response was lost"))?
    }

    async fn dispatch_publish(
        &self,
        descriptor: SourceAssetDescriptor,
        blob_key: String,
    ) -> Result<(), LocalAssetStoreError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(Command::Publish {
                descriptor,
                blob_key,
                reply: reply_sender,
            })
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("local Asset publish response was lost"))?
    }

    async fn dispatch_remove_staged(&self, asset_id: AssetId) -> Result<(), LocalAssetStoreError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(Command::RemoveStaged {
                asset_id,
                reply: reply_sender,
            })
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("local Asset cleanup response was lost"))?
    }
}

#[async_trait]
impl AssetCatalog for LocalAssetStore {
    async fn get_asset(
        &self,
        asset_id: &AssetId,
    ) -> Result<Option<SourceAssetDescriptor>, AssetCatalogError> {
        match self.lookup(asset_id).await {
            Ok(Some(record)) if record.state == PublicationState::Published => {
                Ok(Some(record.descriptor))
            }
            Ok(_) => Ok(None),
            Err(error) => Err(AssetCatalogError::Unavailable {
                detail: error.safe_detail().to_owned(),
            }),
        }
    }
}

#[async_trait]
impl AssetReader for LocalAssetStore {
    async fn read_range(
        &self,
        asset_id: &AssetId,
        range: AssetByteRange,
    ) -> Result<AssetByteChunk, AssetReadError> {
        let record = match self.lookup(asset_id).await {
            Ok(Some(record)) if record.state == PublicationState::Published => record,
            Ok(_) => {
                return Err(AssetReadError::NotFound {
                    asset_id: asset_id.clone(),
                });
            }
            Err(error) => {
                return Err(AssetReadError::Unavailable {
                    detail: error.safe_detail().to_owned(),
                });
            }
        };
        let total_length = record.descriptor.byte_length();
        if range.offset() > total_length {
            return Err(AssetReadError::RangeOutOfBounds {
                asset_id: asset_id.clone(),
                offset: range.offset(),
                total_length,
            });
        }
        let path = self.root.join("blobs").join(&record.blob_key);
        let metadata =
            tokio::fs::symlink_metadata(&path)
                .await
                .map_err(|_| AssetReadError::Unavailable {
                    detail: "published Asset blob could not be inspected".to_owned(),
                })?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(AssetReadError::Unavailable {
                detail: "published Asset blob is not a regular file".to_owned(),
            });
        }
        let mut file =
            tokio::fs::File::open(&path)
                .await
                .map_err(|_| AssetReadError::Unavailable {
                    detail: "published Asset blob could not be opened".to_owned(),
                })?;
        if range.offset() == total_length {
            return AssetByteChunk::new(range.offset(), Bytes::new(), total_length).map_err(|_| {
                AssetReadError::Unavailable {
                    detail: "published Asset produced an invalid EOF chunk".to_owned(),
                }
            });
        }
        let requested_end = range
            .end_offset()
            .ok_or_else(|| AssetReadError::Unavailable {
                detail: "Asset range overflowed unexpectedly".to_owned(),
            })?;
        let actual_end = requested_end.min(total_length);
        let length = usize::try_from(actual_end - range.offset()).map_err(|_| {
            AssetReadError::Unavailable {
                detail: "Asset range is too large for this platform".to_owned(),
            }
        })?;
        file.seek(AsyncSeekFrom::Start(range.offset()))
            .await
            .map_err(|_| AssetReadError::Unavailable {
                detail: "published Asset seek failed".to_owned(),
            })?;
        let mut bytes = vec![0_u8; length];
        file.read_exact(&mut bytes)
            .await
            .map_err(|_| AssetReadError::Unavailable {
                detail: "published Asset read was truncated".to_owned(),
            })?;
        AssetByteChunk::new(range.offset(), Bytes::from(bytes), total_length).map_err(|_| {
            AssetReadError::Unavailable {
                detail: "published Asset produced an invalid range chunk".to_owned(),
            }
        })
    }
}

#[derive(Clone)]
struct VerifiedWrite {
    length: u64,
    hash: ContentHash,
}

async fn write_staging<R: AsyncRead + Unpin>(
    path: &Path,
    reader: &mut R,
    descriptor: &SourceAssetDescriptor,
) -> Result<VerifiedWrite, LocalAssetStoreError> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(|_| filesystem("staging blob could not be created"))?;
    let mut hasher = Sha256::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; READ_BUFFER_SIZE];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|_| filesystem("Asset source could not be read"))?;
        if read == 0 {
            break;
        }
        let read_u64 = u64::try_from(read).map_err(|_| LocalAssetStoreError::LengthOverflow {
            asset_id: descriptor.asset_id().clone(),
            length: usize::MAX as u64,
        })?;
        let next_length =
            length
                .checked_add(read_u64)
                .ok_or_else(|| LocalAssetStoreError::LengthOverflow {
                    asset_id: descriptor.asset_id().clone(),
                    length: u64::MAX,
                })?;
        if next_length > descriptor.byte_length() {
            return Err(LocalAssetStoreError::LengthMismatch {
                asset_id: descriptor.asset_id().clone(),
                expected: descriptor.byte_length(),
                actual: next_length,
            });
        }
        tokio::io::AsyncWriteExt::write_all(&mut file, &buffer[..read])
            .await
            .map_err(|_| filesystem("staging blob could not be written"))?;
        hasher.update(&buffer[..read]);
        length = next_length;
    }
    file.sync_data()
        .await
        .map_err(|_| filesystem("staging blob could not be synchronized"))?;
    let digest = hasher.finalize();
    let mut digest_bytes = [0_u8; 32];
    digest_bytes.copy_from_slice(&digest);
    Ok(VerifiedWrite {
        length,
        hash: ContentHash::from_bytes(digest_bytes),
    })
}

fn initialize_storage(root: &Path) -> Result<(Connection, EntityId), LocalAssetStoreError> {
    fs::create_dir_all(root).map_err(|_| filesystem("Asset data root could not be created"))?;
    for directory in ["blobs", "staging", "quarantine"] {
        fs::create_dir_all(root.join(directory))
            .map_err(|_| filesystem("Asset data directory could not be created"))?;
    }
    let database_path = root.join("metadata.sqlite");
    let mut connection = Connection::open(database_path)
        .map_err(|error| map_sqlite_error("Asset metadata database could not be opened", error))?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| map_sqlite_error("Asset foreign key configuration failed", error))?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(|error| map_sqlite_error("Asset WAL configuration failed", error))?;
    connection
        .pragma_update(None, "synchronous", "NORMAL")
        .map_err(|error| map_sqlite_error("Asset synchronous configuration failed", error))?;
    connection
        .pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MILLISECONDS)
        .map_err(|error| map_sqlite_error("Asset busy timeout configuration failed", error))?;
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS storage_metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS assets (
                asset_id TEXT PRIMARY KEY NOT NULL,
                media_type TEXT NOT NULL,
                byte_length INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                original_filename TEXT,
                blob_key TEXT NOT NULL,
                publication_state TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS assets_publication_state
                ON assets (publication_state);
            "#,
        )
        .map_err(|error| map_sqlite_error("Asset metadata schema creation failed", error))?;

    let schema_version = metadata_value(&connection, "schema_version")?;
    match schema_version {
        None => {
            connection
                .execute(
                    "INSERT INTO storage_metadata (key, value) VALUES (?1, ?2)",
                    params!["schema_version", SUPPORTED_SCHEMA_VERSION.to_string()],
                )
                .map_err(|error| {
                    map_sqlite_error("Asset schema metadata creation failed", error)
                })?;
        }
        Some(value) => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| corruption("Asset metadata schema version is malformed"))?;
            if parsed != SUPPORTED_SCHEMA_VERSION {
                return Err(corruption(if parsed > SUPPORTED_SCHEMA_VERSION {
                    "Asset metadata schema is newer than this adapter"
                } else {
                    "Asset metadata schema migration is required"
                }));
            }
        }
    }
    let physical_store_id = match metadata_value(&connection, "physical_store_id")? {
        Some(value) => value
            .parse::<EntityId>()
            .map_err(|_| corruption("Asset physical store identity is malformed"))?,
        None => {
            let value = EntityId::new();
            connection
                .execute(
                    "INSERT INTO storage_metadata (key, value) VALUES (?1, ?2)",
                    params!["physical_store_id", value.to_string()],
                )
                .map_err(|error| {
                    map_sqlite_error("Asset physical store identity creation failed", error)
                })?;
            value
        }
    };
    let quick_check: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|_| corruption("Asset SQLite physical integrity check failed"))?;
    if quick_check != "ok" {
        return Err(corruption("Asset SQLite physical integrity check failed"));
    }
    recover_storage(root, &mut connection)?;
    Ok((connection, physical_store_id))
}

fn recover_storage(root: &Path, connection: &mut Connection) -> Result<(), LocalAssetStoreError> {
    let records = load_all_records(connection)?;
    let mut published_keys = HashSet::new();
    let mut staged_ids = Vec::new();
    for record in records {
        match record.state {
            PublicationState::Published => {
                verify_blob(
                    &root.join("blobs").join(&record.blob_key),
                    &record.descriptor,
                )?;
                published_keys.insert(record.blob_key);
            }
            PublicationState::Staged => staged_ids.push(record.descriptor.asset_id().clone()),
        }
    }
    if !staged_ids.is_empty() {
        let transaction = connection
            .transaction()
            .map_err(|error| map_sqlite_error("staged Asset recovery transaction failed", error))?;
        for asset_id in staged_ids {
            transaction
                .execute(
                    "DELETE FROM assets WHERE asset_id = ?1 AND publication_state = ?2",
                    params![asset_id.to_string(), STAGED],
                )
                .map_err(|error| map_sqlite_error("staged Asset recovery failed", error))?;
        }
        transaction
            .commit()
            .map_err(|error| map_sqlite_error("staged Asset recovery commit failed", error))?;
    }
    quarantine_directory_entries(root, "staging")?;
    let blobs = root.join("blobs");
    for entry in
        fs::read_dir(&blobs).map_err(|_| filesystem("Asset blob directory could not be read"))?
    {
        let entry = entry.map_err(|_| filesystem("Asset blob entry could not be read"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !published_keys.contains(&name) {
            quarantine_entry(root, entry.path())?;
        }
    }
    Ok(())
}

fn quarantine_directory_entries(root: &Path, directory: &str) -> Result<(), LocalAssetStoreError> {
    let path = root.join(directory);
    for entry in
        fs::read_dir(path).map_err(|_| filesystem("Asset recovery directory could not be read"))?
    {
        let entry = entry.map_err(|_| filesystem("Asset recovery entry could not be read"))?;
        quarantine_entry(root, entry.path())?;
    }
    Ok(())
}

fn quarantine_entry(root: &Path, source: PathBuf) -> Result<(), LocalAssetStoreError> {
    for _ in 0..8 {
        let target = root
            .join("quarantine")
            .join(format!("orphan-{}", AssetId::new()));
        match fs::rename(&source, &target) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(filesystem("Asset orphan could not be quarantined")),
        }
    }
    Err(filesystem("Asset quarantine name allocation failed"))
}

fn verify_blob(
    path: &Path,
    descriptor: &SourceAssetDescriptor,
) -> Result<(), LocalAssetStoreError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| corruption("published Asset blob is missing"))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(corruption("published Asset blob is not a regular file"));
    }
    if metadata.len() != descriptor.byte_length() {
        return Err(corruption(
            "published Asset blob length does not match metadata",
        ));
    }
    let mut file =
        File::open(path).map_err(|_| corruption("published Asset blob could not be opened"))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; READ_BUFFER_SIZE];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| corruption("published Asset blob could not be read"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let mut digest_bytes = [0_u8; 32];
    digest_bytes.copy_from_slice(&digest);
    if ContentHash::from_bytes(digest_bytes) != *descriptor.content_hash() {
        return Err(corruption(
            "published Asset blob hash does not match metadata",
        ));
    }
    Ok(())
}

fn load_all_records(connection: &Connection) -> Result<Vec<AssetRecord>, LocalAssetStoreError> {
    let mut statement = connection
        .prepare("SELECT asset_id, media_type, byte_length, content_hash, original_filename, blob_key, publication_state FROM assets ORDER BY asset_id ASC")
        .map_err(|error| map_sqlite_error("Asset metadata scan failed", error))?;
    let rows = statement
        .query_map([], read_row)
        .map_err(|error| map_sqlite_error("Asset metadata scan failed", error))?;
    rows.map(|row| {
        row.map_err(|_| corruption("Asset metadata row could not be read"))
            .and_then(decode_row)
    })
    .collect()
}

fn lookup_asset(
    connection: &Connection,
    asset_id: &AssetId,
) -> Result<Option<AssetRecord>, LocalAssetStoreError> {
    connection
        .query_row(
            "SELECT asset_id, media_type, byte_length, content_hash, original_filename, blob_key, publication_state FROM assets WHERE asset_id = ?1",
            params![asset_id.to_string()],
            read_row,
        )
        .optional()
        .map_err(|error| map_sqlite_error("Asset metadata lookup failed", error))?
        .map(decode_row)
        .transpose()
}

fn stage_asset(
    connection: &mut Connection,
    descriptor: &SourceAssetDescriptor,
    blob_key: &str,
) -> Result<AssetInsertOutcome, LocalAssetStoreError> {
    let transaction = connection
        .transaction()
        .map_err(|error| map_sqlite_error("Asset stage transaction could not start", error))?;
    if let Some(existing) = transaction
        .query_row(
            "SELECT asset_id, media_type, byte_length, content_hash, original_filename, blob_key, publication_state FROM assets WHERE asset_id = ?1",
            params![descriptor.asset_id().to_string()],
            read_row,
        )
        .optional()
        .map_err(|error| map_sqlite_error("Asset stage lookup failed", error))?
        .map(decode_row)
        .transpose()? {
        match existing.state {
            PublicationState::Published if existing.descriptor == *descriptor => {
                transaction
                    .commit()
                    .map_err(|error| map_sqlite_error("Asset idempotent stage commit failed", error))?;
                return Ok(AssetInsertOutcome::Existing);
            }
            PublicationState::Published => {
                return Err(LocalAssetStoreError::AssetConflict {
                    asset_id: descriptor.asset_id().clone(),
                });
            }
            PublicationState::Staged => {
                return Err(LocalAssetStoreError::AssetInProgress {
                    asset_id: descriptor.asset_id().clone(),
                });
            }
        }
    }
    let byte_length = i64::try_from(descriptor.byte_length()).map_err(|_| {
        LocalAssetStoreError::LengthOverflow {
            asset_id: descriptor.asset_id().clone(),
            length: descriptor.byte_length(),
        }
    })?;
    transaction
        .execute(
            "INSERT INTO assets (asset_id, media_type, byte_length, content_hash, original_filename, blob_key, publication_state) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                descriptor.asset_id().to_string(),
                descriptor.media_type(),
                byte_length,
                descriptor.content_hash().as_str(),
                descriptor.original_filename(),
                blob_key,
                STAGED,
            ],
        )
        .map_err(|error| map_sqlite_error("Asset staged metadata insert failed", error))?;
    transaction
        .commit()
        .map_err(|error| map_sqlite_error("Asset staged metadata commit failed", error))?;
    Ok(AssetInsertOutcome::Inserted)
}

fn publish_asset(
    connection: &mut Connection,
    descriptor: &SourceAssetDescriptor,
    blob_key: &str,
) -> Result<(), LocalAssetStoreError> {
    let transaction = connection
        .transaction()
        .map_err(|error| map_sqlite_error("Asset publish transaction could not start", error))?;
    let existing = transaction
        .query_row(
            "SELECT asset_id, media_type, byte_length, content_hash, original_filename, blob_key, publication_state FROM assets WHERE asset_id = ?1",
            params![descriptor.asset_id().to_string()],
            read_row,
        )
        .optional()
        .map_err(|error| map_sqlite_error("Asset publish lookup failed", error))?
        .map(decode_row)
        .transpose()?
        .ok_or_else(|| corruption("Asset staged metadata disappeared before publish"))?;
    if existing.state != PublicationState::Staged
        || existing.descriptor != *descriptor
        || existing.blob_key != blob_key
    {
        return Err(corruption(
            "Asset publication metadata changed before publish",
        ));
    }
    transaction
        .execute(
            "UPDATE assets SET publication_state = ?1 WHERE asset_id = ?2 AND publication_state = ?3",
            params![PUBLISHED, descriptor.asset_id().to_string(), STAGED],
        )
        .map_err(|error| map_sqlite_error("Asset publication update failed", error))?;
    transaction
        .commit()
        .map_err(|error| map_sqlite_error("Asset publication commit failed", error))?;
    Ok(())
}

fn remove_staged(
    connection: &mut Connection,
    asset_id: &AssetId,
) -> Result<(), LocalAssetStoreError> {
    connection
        .execute(
            "DELETE FROM assets WHERE asset_id = ?1 AND publication_state = ?2",
            params![asset_id.to_string(), STAGED],
        )
        .map_err(|error| map_sqlite_error("staged Asset metadata cleanup failed", error))?;
    Ok(())
}

fn run_worker(mut connection: Connection, mut receiver: mpsc::Receiver<Command>) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            Command::Lookup { asset_id, reply } => {
                let _ = reply.send(lookup_asset(&connection, &asset_id));
            }
            Command::Stage {
                descriptor,
                blob_key,
                reply,
            } => {
                let _ = reply.send(stage_asset(&mut connection, &descriptor, &blob_key));
            }
            Command::Publish {
                descriptor,
                blob_key,
                reply,
            } => {
                let _ = reply.send(publish_asset(&mut connection, &descriptor, &blob_key));
            }
            Command::RemoveStaged { asset_id, reply } => {
                let _ = reply.send(remove_staged(&mut connection, &asset_id));
            }
            Command::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
                break;
            }
        }
    }
}

fn metadata_value(
    connection: &Connection,
    key: &str,
) -> Result<Option<String>, LocalAssetStoreError> {
    connection
        .query_row(
            "SELECT value FROM storage_metadata WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| map_sqlite_error("Asset storage metadata lookup failed", error))
}

struct RawAssetRow {
    asset_id: String,
    media_type: String,
    byte_length: i64,
    content_hash: String,
    original_filename: Option<String>,
    blob_key: String,
    publication_state: String,
}

fn read_row(row: &Row<'_>) -> rusqlite::Result<RawAssetRow> {
    Ok(RawAssetRow {
        asset_id: row.get(0)?,
        media_type: row.get(1)?,
        byte_length: row.get(2)?,
        content_hash: row.get(3)?,
        original_filename: row.get(4)?,
        blob_key: row.get(5)?,
        publication_state: row.get(6)?,
    })
}

fn decode_row(row: RawAssetRow) -> Result<AssetRecord, LocalAssetStoreError> {
    let asset_id = row
        .asset_id
        .parse::<AssetId>()
        .map_err(|_| corruption("Asset metadata identity is invalid"))?;
    let byte_length = u64::try_from(row.byte_length)
        .map_err(|_| corruption("Asset metadata byte length is invalid"))?;
    let content_hash = row
        .content_hash
        .parse::<ContentHash>()
        .map_err(|_| corruption("Asset metadata content hash is invalid"))?;
    let descriptor = SourceAssetDescriptor::new(
        asset_id.clone(),
        row.media_type,
        byte_length,
        content_hash,
        row.original_filename,
    )
    .map_err(|_| corruption("Asset metadata descriptor is invalid"))?;
    let expected_blob_key = blob_key(&asset_id);
    if row.blob_key != expected_blob_key {
        return Err(corruption("Asset metadata blob key is invalid"));
    }
    Ok(AssetRecord {
        descriptor,
        blob_key: row.blob_key,
        state: PublicationState::parse(&row.publication_state)?,
    })
}

fn blob_key(asset_id: &AssetId) -> String {
    format!("{asset_id}{BLOB_SUFFIX}")
}

async fn remove_file_if_present(path: &Path) -> Result<(), LocalAssetStoreError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(filesystem("temporary Asset blob cleanup failed")),
    }
}

async fn quarantine_async(root: &Path, source: &Path) -> Result<(), LocalAssetStoreError> {
    for _ in 0..8 {
        let target = root
            .join("quarantine")
            .join(format!("orphan-{}", AssetId::new()));
        match tokio::fs::rename(source, target).await {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(filesystem("Asset blob could not be quarantined")),
        }
    }
    Err(filesystem("Asset quarantine name allocation failed"))
}

impl LocalAssetStoreError {
    fn safe_detail(&self) -> &'static str {
        match self {
            Self::InvalidQueueCapacity => "invalid Asset metadata configuration",
            Self::LengthOverflow { .. } => "Asset length overflow",
            Self::LengthMismatch { .. } => "Asset length mismatch",
            Self::HashMismatch { .. } => "Asset content hash mismatch",
            Self::AssetConflict { .. } => "Asset identity conflict",
            Self::AssetInProgress { .. } => "Asset ingest is in progress",
            Self::BackendUnavailable { detail }
            | Self::BackendFailure { detail }
            | Self::Filesystem { detail }
            | Self::Corruption { detail } => detail,
        }
    }
}

fn map_send_error(error: mpsc::error::TrySendError<Command>) -> LocalAssetStoreError {
    match error {
        mpsc::error::TrySendError::Full(_) => unavailable("Asset metadata command queue is full"),
        mpsc::error::TrySendError::Closed(_) => unavailable("Asset metadata worker is unavailable"),
    }
}

fn map_sqlite_error(message: &'static str, error: SqliteError) -> LocalAssetStoreError {
    if matches!(
        &error,
        SqliteError::SqliteFailure(error, _)
            if matches!(error.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    ) {
        return unavailable("Asset metadata SQLite backend is busy");
    }
    LocalAssetStoreError::BackendFailure { detail: message }
}

fn map_filesystem_error(error: std::io::Error) -> LocalAssetStoreError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        filesystem("Asset blob destination already exists")
    } else {
        filesystem("Asset blob rename failed")
    }
}

const fn unavailable(detail: &'static str) -> LocalAssetStoreError {
    LocalAssetStoreError::BackendUnavailable { detail }
}

const fn filesystem(detail: &'static str) -> LocalAssetStoreError {
    LocalAssetStoreError::Filesystem { detail }
}

const fn corruption(detail: &'static str) -> LocalAssetStoreError {
    LocalAssetStoreError::Corruption { detail }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use orbitrelay_asset::{AssetId, ContentHash, SourceAssetDescriptor};
    use orbitrelay_asset_runtime::{
        AssetByteRange, AssetCatalog, AssetInsertOutcome, AssetReadError, AssetReader,
    };
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;
    use tokio::io::AsyncRead;

    use super::{LocalAssetStore, LocalAssetStoreError, PUBLISHED};

    fn descriptor(asset_id: AssetId, bytes: &[u8]) -> SourceAssetDescriptor {
        let digest = Sha256::digest(bytes);
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(&digest);
        SourceAssetDescriptor::new(
            asset_id,
            "application/octet-stream",
            bytes.len() as u64,
            ContentHash::from_bytes(hash),
            Some("payload.bin".to_owned()),
        )
        .expect("descriptor")
    }

    fn root() -> TempDir {
        tempfile::tempdir().expect("temp root")
    }

    #[tokio::test]
    async fn publishes_and_reopens_metadata_bytes_and_ranges() {
        let root = root();
        let bytes = bytes::Bytes::from_static(b"0123456789");
        let id = AssetId::new();
        let descriptor = descriptor(id.clone(), &bytes);
        let store = LocalAssetStore::open(root.path()).expect("open");
        assert_eq!(
            store
                .insert_verified(descriptor.clone(), bytes.clone())
                .await
                .expect("insert"),
            AssetInsertOutcome::Inserted
        );
        assert_eq!(
            store.get_asset(&id).await.expect("catalog"),
            Some(descriptor.clone())
        );
        let first = store
            .read_range(&id, AssetByteRange::new(2, 4).unwrap())
            .await
            .expect("range");
        assert_eq!(first.bytes().as_ref(), b"2345");
        store.close().await.expect("close");

        let reopened = LocalAssetStore::open(root.path()).expect("reopen");
        assert_eq!(
            reopened.get_asset(&id).await.expect("catalog"),
            Some(descriptor)
        );
        let final_chunk = reopened
            .read_range(&id, AssetByteRange::new(8, 8).unwrap())
            .await
            .expect("range");
        assert_eq!(final_chunk.bytes().as_ref(), b"89");
        assert!(final_chunk.is_eof());
        reopened.close().await.expect("close");
    }

    #[tokio::test]
    async fn distinguishes_zero_byte_and_missing_assets() {
        let root = root();
        let store = LocalAssetStore::open(root.path()).expect("open");
        let id = AssetId::new();
        store
            .insert_verified(descriptor(id.clone(), &[]), bytes::Bytes::new())
            .await
            .expect("empty insert");
        let eof = store
            .read_range(&id, AssetByteRange::new(0, 1).unwrap())
            .await
            .expect("EOF");
        assert!(eof.bytes().is_empty());
        assert!(eof.is_eof());
        assert!(matches!(
            store
                .read_range(&AssetId::new(), AssetByteRange::new(0, 1).unwrap())
                .await,
            Err(AssetReadError::NotFound { .. })
        ));
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn validates_length_hash_and_immutable_conflicts() {
        let root = root();
        let store = LocalAssetStore::open(root.path()).expect("open");
        let id = AssetId::new();
        let bytes = bytes::Bytes::from_static(b"valid");
        let mut wrong_length = descriptor(id.clone(), &bytes);
        wrong_length = SourceAssetDescriptor::new(
            id.clone(),
            "application/octet-stream",
            99,
            wrong_length.content_hash().clone(),
            Some("payload.bin".to_owned()),
        )
        .unwrap();
        assert!(matches!(
            store.insert_verified(wrong_length, bytes.clone()).await,
            Err(LocalAssetStoreError::LengthMismatch { .. })
        ));
        let wrong_hash = SourceAssetDescriptor::new(
            id.clone(),
            "application/octet-stream",
            bytes.len() as u64,
            ContentHash::from_bytes([0; 32]),
            Some("payload.bin".to_owned()),
        )
        .unwrap();
        assert!(matches!(
            store.insert_verified(wrong_hash, bytes.clone()).await,
            Err(LocalAssetStoreError::HashMismatch { .. })
        ));
        let valid = descriptor(id.clone(), &bytes);
        assert_eq!(
            store
                .insert_verified(valid.clone(), bytes.clone())
                .await
                .unwrap(),
            AssetInsertOutcome::Inserted
        );
        assert_eq!(
            store.insert_verified(valid, bytes.clone()).await.unwrap(),
            AssetInsertOutcome::Existing
        );
        let conflict = descriptor(id, b"other");
        assert!(matches!(
            store
                .insert_verified(conflict, bytes::Bytes::from_static(b"other"))
                .await,
            Err(LocalAssetStoreError::AssetConflict { .. })
        ));
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn published_blob_corruption_fails_reopen() {
        for mode in 0..3 {
            let root = root();
            let bytes = bytes::Bytes::from_static(b"corruption");
            let id = AssetId::new();
            let store = LocalAssetStore::open(root.path()).expect("open");
            store
                .insert_verified(descriptor(id.clone(), &bytes), bytes)
                .await
                .expect("insert");
            store.close().await.expect("close");
            let blob = root.path().join("blobs").join(format!("{id}.blob"));
            match mode {
                0 => fs::remove_file(&blob).expect("remove blob"),
                1 => fs::write(&blob, b"short").expect("truncate blob"),
                _ => fs::write(&blob, b"corruptioN").expect("modify blob"),
            }
            assert!(matches!(
                LocalAssetStore::open(root.path()),
                Err(LocalAssetStoreError::Corruption { .. })
            ));
        }
    }

    #[tokio::test]
    async fn staged_and_orphan_files_are_not_published_on_reopen() {
        let root = root();
        let store = LocalAssetStore::open(root.path()).expect("open");
        store.close().await.expect("close");
        fs::write(root.path().join("staging").join("stale.part"), b"stale").expect("staging");
        fs::write(root.path().join("blobs").join("orphan.blob"), b"orphan").expect("orphan");
        let reopened = LocalAssetStore::open(root.path()).expect("reopen");
        assert!(fs::read_dir(root.path().join("staging"))
            .unwrap()
            .next()
            .is_none());
        assert!(fs::read_dir(root.path().join("blobs"))
            .unwrap()
            .next()
            .is_none());
        assert!(fs::read_dir(root.path().join("quarantine"))
            .unwrap()
            .next()
            .is_some());
        reopened.close().await.expect("close");
    }

    #[tokio::test]
    async fn schema_and_metadata_corruption_fail_safely() {
        let schema_root = root();
        let store = LocalAssetStore::open(schema_root.path()).expect("open");
        store.close().await.expect("close");
        let connection =
            rusqlite::Connection::open(schema_root.path().join("metadata.sqlite")).expect("raw DB");
        connection
            .execute(
                "UPDATE storage_metadata SET value = '999' WHERE key = 'schema_version'",
                [],
            )
            .expect("schema update");
        drop(connection);
        assert!(matches!(
            LocalAssetStore::open(schema_root.path()),
            Err(LocalAssetStoreError::Corruption { .. })
        ));

        let malformed = root();
        let store = LocalAssetStore::open(malformed.path()).expect("open");
        store.close().await.expect("close");
        let connection =
            rusqlite::Connection::open(malformed.path().join("metadata.sqlite")).expect("raw DB");
        connection
            .execute(
                "INSERT INTO assets (asset_id, media_type, byte_length, content_hash, original_filename, blob_key, publication_state) VALUES ('not-an-id', 'application/octet-stream', -1, 'not-a-hash', NULL, '../escape', 'published')",
                [],
            )
            .expect("malformed metadata insert");
        drop(connection);
        assert!(matches!(
            LocalAssetStore::open(malformed.path()),
            Err(LocalAssetStoreError::Corruption { .. })
        ));
    }

    #[tokio::test]
    async fn concurrent_identical_and_conflicting_ingests_are_linearized() {
        let root = root();
        let store = Arc::new(LocalAssetStore::open(root.path()).expect("open"));
        let id = AssetId::new();
        let bytes = bytes::Bytes::from_static(b"same");
        let descriptor = descriptor(id.clone(), &bytes);
        let first = store.clone();
        let second = store.clone();
        let first_descriptor = descriptor.clone();
        let second_descriptor = descriptor;
        let first_bytes = bytes.clone();
        let second_bytes = bytes;
        let (a, b) = tokio::join!(
            tokio::spawn(async move { first.insert_verified(first_descriptor, first_bytes).await }),
            tokio::spawn(async move {
                second
                    .insert_verified(second_descriptor, second_bytes)
                    .await
            }),
        );
        let outcomes = [a.unwrap().unwrap(), b.unwrap().unwrap()];
        assert!(outcomes.contains(&AssetInsertOutcome::Inserted));
        assert!(outcomes.contains(&AssetInsertOutcome::Existing));
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn concurrent_reads_are_stable() {
        let root = root();
        let store = Arc::new(LocalAssetStore::open(root.path()).expect("open"));
        let id = AssetId::new();
        let bytes = bytes::Bytes::from_static(b"0123456789");
        store
            .insert_verified(descriptor(id.clone(), &bytes), bytes)
            .await
            .expect("insert");
        let first_store = store.clone();
        let first_id = id.clone();
        let second_store = store.clone();
        let second_id = id;
        let (first, second) = tokio::join!(
            tokio::spawn(async move {
                first_store
                    .read_range(&first_id, AssetByteRange::new(0, 5).unwrap())
                    .await
            }),
            tokio::spawn(async move {
                second_store
                    .read_range(&second_id, AssetByteRange::new(5, 5).unwrap())
                    .await
            }),
        );
        assert_eq!(first.unwrap().unwrap().bytes().as_ref(), b"01234");
        assert_eq!(second.unwrap().unwrap().bytes().as_ref(), b"56789");
        store.close().await.expect("close");
    }

    #[tokio::test]
    async fn queue_capacity_zero_is_rejected() {
        let root = root();
        assert!(matches!(
            LocalAssetStore::open_with_queue_capacity(root.path(), 0),
            Err(LocalAssetStoreError::InvalidQueueCapacity)
        ));
    }

    #[allow(dead_code)]
    fn assert_async_read<T: AsyncRead>() {}

    #[test]
    fn publication_marker_constant_is_stable() {
        assert_eq!(PUBLISHED, "published");
    }
}
