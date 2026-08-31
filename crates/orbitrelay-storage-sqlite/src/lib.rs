//! SQLite-backed persistence for the generic OrbitRelay EventStore.
//!
//! The adapter owns one SQLite connection on one dedicated worker thread. The
//! public EventStore port remains backend-neutral and all cursors/checkpoints
//! are opaque, process-epoch-scoped values.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use async_trait::async_trait;
use orbitrelay_core::{EntityId, Metadata, Timestamp};
use orbitrelay_protocol::{ActionId, ActorId, Event, EventId, EventType, Payload, SessionId};
use orbitrelay_storage::{
    EventCursor, EventPage, EventQuery, EventStore, EventStoreCheckpoint, StorageError, StoredEvent,
};
use rusqlite::{params, types::Value, Connection, OptionalExtension, Row};
use rusqlite::{Error as SqliteError, ErrorCode};
use tokio::sync::{mpsc, oneshot};

/// The first persistent schema supported by this adapter.
pub const SUPPORTED_SCHEMA_VERSION: i64 = 1;

/// Default bounded command queue capacity.
pub const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 256;

const BUSY_TIMEOUT_MILLISECONDS: i64 = 5_000;

enum Command {
    CaptureCheckpoint {
        reply: oneshot::Sender<Result<EventStoreCheckpoint, StorageError>>,
    },
    Append {
        event: Event,
        reply: oneshot::Sender<Result<StoredEvent, StorageError>>,
    },
    Get {
        event_id: EventId,
        reply: oneshot::Sender<Result<Option<StoredEvent>, StorageError>>,
    },
    Query {
        query: EventQuery,
        reply: oneshot::Sender<Result<EventPage, StorageError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
}

struct WorkerHandle {
    sender: Mutex<Option<mpsc::Sender<Command>>>,
    join: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
}

impl WorkerHandle {
    fn sender(&self) -> Result<mpsc::Sender<Command>, StorageError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable("SQLite EventStore is closed"));
        }
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned()
            .ok_or_else(|| unavailable("SQLite EventStore worker is unavailable"))
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
            // Drop cannot await. Closing the sender lets the worker drain its
            // already accepted commands and terminate without a panic.
            let _ = thread::Builder::new()
                .name("orbitrelay-sqlite-join".to_owned())
                .spawn(move || {
                    let _ = join.join();
                });
        }
    }
}

/// A cloneable SQLite EventStore using one bounded worker queue and one
/// connection owned exclusively by the worker thread.
#[derive(Clone)]
pub struct SQLiteEventStore {
    worker: Arc<WorkerHandle>,
    physical_store_id: EntityId,
    continuation_epoch: EntityId,
}

impl SQLiteEventStore {
    /// Opens or creates a SQLite database using the default bounded queue.
    ///
    /// The blocking open and integrity gate run on the dedicated worker thread;
    /// callers embedding this in an async bootstrap may still call this
    /// constructor from a `spawn_blocking` boundary.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_with_queue_capacity(path, DEFAULT_COMMAND_QUEUE_CAPACITY)
    }

    /// Opens or creates a SQLite database with an explicit command queue size.
    pub fn open_with_queue_capacity(
        path: impl AsRef<Path>,
        queue_capacity: usize,
    ) -> Result<Self, StorageError> {
        if queue_capacity == 0 {
            return Err(StorageError::InvalidQuery {
                reason: "SQLite command queue capacity must be greater than zero".to_owned(),
            });
        }

        let path = path.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name("orbitrelay-sqlite".to_owned())
            .spawn(move || match initialize_connection(&path) {
                Ok((mut connection, physical_store_id)) => {
                    let continuation_epoch = EntityId::new();
                    let _ = ready_sender.send(Ok(WorkerInit {
                        physical_store_id: physical_store_id.clone(),
                        continuation_epoch: continuation_epoch.clone(),
                    }));
                    run_worker(
                        &mut connection,
                        receiver,
                        physical_store_id,
                        continuation_epoch,
                    );
                }
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                }
            })
            .map_err(|_| unavailable("could not start SQLite worker"))?;

        let init = match ready_receiver.recv() {
            Ok(Ok(init)) => init,
            Ok(Err(error)) => {
                let _ = join.join();
                return Err(error);
            }
            Err(_) => {
                let _ = join.join();
                return Err(unavailable("SQLite worker initialization failed"));
            }
        };
        let worker = Arc::new(WorkerHandle {
            sender: Mutex::new(Some(sender)),
            join: Mutex::new(Some(join)),
            closed: AtomicBool::new(false),
        });
        Ok(Self {
            worker,
            physical_store_id: init.physical_store_id,
            continuation_epoch: init.continuation_epoch,
        })
    }

    /// Returns the persistent physical store identity for diagnostics.
    #[must_use]
    pub const fn physical_store_id(&self) -> &EntityId {
        &self.physical_store_id
    }

    /// Returns the process-scoped continuation epoch for this open handle.
    #[must_use]
    pub const fn continuation_epoch(&self) -> &EntityId {
        &self.continuation_epoch
    }

    async fn close_worker(&self) -> Result<(), StorageError> {
        if self.worker.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
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
                .map_err(|_| unavailable("SQLite worker stopped before shutdown"))?;
            reply_receiver
                .await
                .map_err(|_| unavailable("SQLite worker shutdown response was lost"))??;
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
                    .map_err(|_| unavailable("SQLite worker panicked during shutdown"))
            })
            .await
            .map_err(|_| unavailable("SQLite worker join task failed"))??;
        }
        Ok(())
    }

    async fn dispatch_capture(&self) -> Result<EventStoreCheckpoint, StorageError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(Command::CaptureCheckpoint {
                reply: reply_sender,
            })
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("SQLite worker response was lost"))?
    }

    async fn dispatch_append(&self, event: Event) -> Result<StoredEvent, StorageError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(Command::Append {
                event,
                reply: reply_sender,
            })
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("SQLite worker response was lost"))?
    }

    async fn dispatch_get(&self, event_id: EventId) -> Result<Option<StoredEvent>, StorageError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(Command::Get {
                event_id,
                reply: reply_sender,
            })
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("SQLite worker response was lost"))?
    }

    async fn dispatch_query(&self, query: EventQuery) -> Result<EventPage, StorageError> {
        let sender = self.worker.sender()?;
        let (reply_sender, reply_receiver) = oneshot::channel();
        sender
            .try_send(Command::Query {
                query,
                reply: reply_sender,
            })
            .map_err(map_send_error)?;
        reply_receiver
            .await
            .map_err(|_| unavailable("SQLite worker response was lost"))?
    }
}

#[async_trait]
impl EventStore for SQLiteEventStore {
    async fn close(&self) -> Result<(), StorageError> {
        self.close_worker().await
    }

    async fn capture_checkpoint(&self) -> Result<EventStoreCheckpoint, StorageError> {
        self.dispatch_capture().await
    }

    async fn append(&self, event: Event) -> Result<StoredEvent, StorageError> {
        self.dispatch_append(event).await
    }

    async fn get(&self, event_id: &EventId) -> Result<Option<StoredEvent>, StorageError> {
        self.dispatch_get(event_id.clone()).await
    }

    async fn query(&self, query: EventQuery) -> Result<EventPage, StorageError> {
        self.dispatch_query(query).await
    }
}

#[derive(Clone)]
struct WorkerInit {
    physical_store_id: EntityId,
    continuation_epoch: EntityId,
}

fn run_worker(
    connection: &mut Connection,
    mut receiver: mpsc::Receiver<Command>,
    physical_store_id: EntityId,
    continuation_epoch: EntityId,
) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            Command::CaptureCheckpoint { reply } => {
                let result =
                    capture_checkpoint(connection, &physical_store_id, &continuation_epoch);
                let _ = reply.send(result);
            }
            Command::Append { event, reply } => {
                let result =
                    append_event(connection, &physical_store_id, &event, &continuation_epoch);
                let _ = reply.send(result);
            }
            Command::Get { event_id, reply } => {
                let result = get_event(
                    connection,
                    &physical_store_id,
                    &event_id,
                    &continuation_epoch,
                );
                let _ = reply.send(result);
            }
            Command::Query { query, reply } => {
                let result =
                    query_events(connection, &physical_store_id, query, &continuation_epoch);
                let _ = reply.send(result);
            }
            Command::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
                break;
            }
        }
    }
}

fn initialize_connection(path: &Path) -> Result<(Connection, EntityId), StorageError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .map_err(|_| failure("SQLite database directory could not be created"))?;
    }
    let connection = Connection::open(path)
        .map_err(|error| map_sqlite_error("SQLite database open failed", error))?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| map_sqlite_error("SQLite foreign key configuration failed", error))?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(|error| map_sqlite_error("SQLite WAL configuration failed", error))?;
    connection
        .pragma_update(None, "synchronous", "NORMAL")
        .map_err(|error| map_sqlite_error("SQLite synchronous configuration failed", error))?;
    connection
        .pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MILLISECONDS)
        .map_err(|error| map_sqlite_error("SQLite busy timeout configuration failed", error))?;
    connection
        .execute_batch(
            r#"
                CREATE TABLE IF NOT EXISTS storage_metadata (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    append_sequence INTEGER PRIMARY KEY NOT NULL,
                    event_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    occurred_at_json TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS events_session_type_sequence
                    ON events (session_id, event_type, append_sequence);
                CREATE INDEX IF NOT EXISTS events_actor_sequence
                    ON events (actor_id, append_sequence);
            "#,
        )
        .map_err(|error| map_sqlite_error("SQLite schema creation failed", error))?;

    let schema_version = metadata_value(&connection, "schema_version")?;
    match schema_version {
        None => {
            connection
                .execute(
                    "INSERT INTO storage_metadata (key, value) VALUES (?1, ?2)",
                    params!["schema_version", SUPPORTED_SCHEMA_VERSION.to_string()],
                )
                .map_err(|error| {
                    map_sqlite_error("SQLite schema metadata creation failed", error)
                })?;
        }
        Some(value) => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| failure("SQLite schema version is malformed"))?;
            if parsed != SUPPORTED_SCHEMA_VERSION {
                return Err(failure(if parsed > SUPPORTED_SCHEMA_VERSION {
                    "SQLite schema version is newer than this adapter"
                } else {
                    "SQLite schema migration is required"
                }));
            }
        }
    }

    let physical_store_id = match metadata_value(&connection, "physical_store_id")? {
        Some(value) => value
            .parse::<EntityId>()
            .map_err(|_| failure("SQLite physical store identity is malformed"))?,
        None => {
            let value = EntityId::new();
            connection
                .execute(
                    "INSERT INTO storage_metadata (key, value) VALUES (?1, ?2)",
                    params!["physical_store_id", value.to_string()],
                )
                .map_err(|error| {
                    map_sqlite_error("SQLite physical store identity creation failed", error)
                })?;
            value
        }
    };

    let quick_check: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|_| failure("SQLite physical integrity check failed"))?;
    if quick_check != "ok" {
        return Err(failure("SQLite physical integrity check failed"));
    }
    verify_all_events(&connection)?;
    Ok((connection, physical_store_id))
}

fn metadata_value(connection: &Connection, key: &str) -> Result<Option<String>, StorageError> {
    connection
        .query_row(
            "SELECT value FROM storage_metadata WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| map_sqlite_error("SQLite metadata read failed", error))
}

fn verify_all_events(connection: &Connection) -> Result<(), StorageError> {
    let mut statement = connection
        .prepare("SELECT append_sequence, event_id, session_id, actor_id, action_id, occurred_at_json, event_type, payload_json, metadata_json FROM events ORDER BY append_sequence ASC")
        .map_err(|error| map_sqlite_error("SQLite EventStore integrity query failed", error))?;
    let rows = statement
        .query_map([], read_row)
        .map_err(|error| map_sqlite_error("SQLite EventStore integrity query failed", error))?;
    let mut expected = 0_i64;
    for row in rows {
        let row = row.map_err(|_| failure("SQLite EventStore row read failed"))?;
        if row.append_sequence != expected {
            return Err(failure(
                "SQLite EventStore append sequence is not contiguous",
            ));
        }
        decode_row(row)?;
        expected = expected
            .checked_add(1)
            .ok_or_else(|| failure("SQLite EventStore append sequence overflow"))?;
    }
    Ok(())
}

fn capture_checkpoint(
    connection: &Connection,
    store_id: &EntityId,
    epoch: &EntityId,
) -> Result<EventStoreCheckpoint, StorageError> {
    let boundary = current_count(connection)?;
    Ok(EventStoreCheckpoint::for_storage(store_id, epoch, boundary))
}

fn append_event(
    connection: &mut Connection,
    store_id: &EntityId,
    event: &Event,
    epoch: &EntityId,
) -> Result<StoredEvent, StorageError> {
    let payload_json = serde_json::to_string(event.payload())
        .map_err(|_| failure("event payload serialization failed"))?;
    let metadata_json = serde_json::to_string(event.metadata())
        .map_err(|_| failure("event metadata serialization failed"))?;
    let occurred_at_json = serde_json::to_string(event.occurred_at())
        .map_err(|_| failure("event timestamp serialization failed"))?;
    let tx = connection.transaction().map_err(|error| {
        map_sqlite_error("SQLite EventStore transaction could not start", error)
    })?;
    let existing = tx
        .query_row(
            "SELECT append_sequence, event_id, session_id, actor_id, action_id, occurred_at_json, event_type, payload_json, metadata_json FROM events WHERE event_id = ?1",
            params![event.id().to_string()],
            read_row,
        )
        .optional()
        .map_err(|error| map_sqlite_error("SQLite EventId lookup failed", error))?;
    if let Some(row) = existing {
        let (sequence, existing_event) = decode_row(row)?;
        if &existing_event == event {
            tx.commit().map_err(|error| {
                map_sqlite_error("SQLite EventStore transaction commit failed", error)
            })?;
            return stored_event(sequence, existing_event, store_id, epoch);
        }
        return Err(StorageError::EventConflict {
            event_id: event.id().clone(),
        });
    }

    let sequence = current_count_tx(&tx)?;
    let sequence_i64 = i64::try_from(sequence)
        .map_err(|_| failure("SQLite EventStore append sequence overflow"))?;
    tx.execute(
        "INSERT INTO events (append_sequence, event_id, session_id, actor_id, action_id, occurred_at_json, event_type, payload_json, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            sequence_i64,
            event.id().to_string(),
            event.session_id().to_string(),
            event.actor_id().to_string(),
            event.action_id().to_string(),
            occurred_at_json,
            event.event_type().as_str(),
            payload_json,
            metadata_json,
        ],
    )
    .map_err(|error| map_sqlite_error("SQLite EventStore append failed", error))?;
    tx.commit()
        .map_err(|error| map_sqlite_error("SQLite EventStore transaction commit failed", error))?;
    stored_event(sequence, event.clone(), store_id, epoch)
}

fn get_event(
    connection: &Connection,
    store_id: &EntityId,
    event_id: &EventId,
    epoch: &EntityId,
) -> Result<Option<StoredEvent>, StorageError> {
    let row = connection
        .query_row(
            "SELECT append_sequence, event_id, session_id, actor_id, action_id, occurred_at_json, event_type, payload_json, metadata_json FROM events WHERE event_id = ?1",
            params![event_id.to_string()],
            read_row,
        )
        .optional()
        .map_err(|error| map_sqlite_error("SQLite EventId lookup failed", error))?;
    row.map(|row| {
        let (sequence, event) = decode_row(row)?;
        stored_event(sequence, event, store_id, epoch)
    })
    .transpose()
}

fn query_events(
    connection: &Connection,
    store_id: &EntityId,
    query: EventQuery,
    epoch: &EntityId,
) -> Result<EventPage, StorageError> {
    query.validate()?;
    let count = current_count(connection)?;
    let upper_bound = match query.upper_bound() {
        Some(checkpoint) => checkpoint.storage_position(store_id, epoch).map_err(|_| {
            StorageError::InvalidCheckpoint {
                reason: "checkpoint does not belong to this SQLite store epoch".to_owned(),
            }
        })?,
        None => count,
    };
    if upper_bound > count {
        return Err(StorageError::InvalidCheckpoint {
            reason: "checkpoint is beyond the stored event range".to_owned(),
        });
    }
    let start = match query.after_cursor() {
        Some(cursor) => {
            let position = cursor.storage_position(store_id, epoch).map_err(|_| {
                StorageError::InvalidCursor {
                    reason: "cursor does not belong to this SQLite store epoch".to_owned(),
                }
            })?;
            if position == 0 {
                return Err(StorageError::InvalidCursor {
                    reason: "cursor position must be greater than zero".to_owned(),
                });
            }
            position
        }
        None => 0,
    };
    if start > upper_bound || (start > 0 && start > count) {
        return Err(StorageError::InvalidCursor {
            reason: "cursor is beyond the query checkpoint".to_owned(),
        });
    }

    let mut sql = String::from(
        "SELECT append_sequence, event_id, session_id, actor_id, action_id, occurred_at_json, event_type, payload_json, metadata_json FROM events WHERE append_sequence >= ? AND append_sequence < ?",
    );
    let mut values = vec![
        Value::Integer(i64::try_from(start).map_err(|_| failure("query cursor overflow"))?),
        Value::Integer(i64::try_from(upper_bound).map_err(|_| failure("query bound overflow"))?),
    ];
    if let Some(session_id) = query.session_id() {
        sql.push_str(" AND session_id = ?");
        values.push(Value::Text(session_id.to_string()));
    }
    if let Some(actor_id) = query.actor_id() {
        sql.push_str(" AND actor_id = ?");
        values.push(Value::Text(actor_id.to_string()));
    }
    if !query.event_types().is_empty() {
        sql.push_str(" AND event_type IN (");
        for (index, event_type) in query.event_types().iter().enumerate() {
            if index > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
            values.push(Value::Text(event_type.as_str().to_owned()));
        }
        sql.push(')');
    }
    sql.push_str(" ORDER BY append_sequence ASC");

    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| map_sqlite_error("SQLite EventStore query preparation failed", error))?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(values), read_row)
        .map_err(|error| map_sqlite_error("SQLite EventStore query failed", error))?;
    let mut events = Vec::with_capacity(query.limit());
    let mut has_more = false;
    for row in rows {
        let row = row.map_err(|_| failure("SQLite EventStore row read failed"))?;
        let (sequence, event) = decode_row(row)?;
        if !query.matches(&event) {
            continue;
        }
        if events.len() == query.limit() {
            has_more = true;
            break;
        }
        events.push(stored_event(sequence, event, store_id, epoch)?);
    }
    let next_cursor = if has_more {
        events.last().map(|stored| stored.cursor().clone())
    } else {
        None
    };
    Ok(EventPage::new(events, next_cursor))
}

fn stored_event(
    sequence: u64,
    event: Event,
    store_id: &EntityId,
    epoch: &EntityId,
) -> Result<StoredEvent, StorageError> {
    let position = sequence
        .checked_add(1)
        .ok_or_else(|| failure("SQLite EventStore cursor overflow"))?;
    Ok(StoredEvent::new(
        EventCursor::for_storage(store_id, epoch, position),
        event,
    ))
}

fn current_count(connection: &Connection) -> Result<u64, StorageError> {
    let max: Option<i64> = connection
        .query_row("SELECT MAX(append_sequence) FROM events", [], |row| {
            row.get(0)
        })
        .map_err(|error| map_sqlite_error("SQLite EventStore sequence read failed", error))?;
    max.map_or(Ok(0), |value| {
        if value < 0 {
            return Err(failure("SQLite EventStore append sequence is negative"));
        }
        u64::try_from(value)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| failure("SQLite EventStore append sequence overflow"))
    })
}

fn current_count_tx(transaction: &rusqlite::Transaction<'_>) -> Result<u64, StorageError> {
    let max: Option<i64> = transaction
        .query_row("SELECT MAX(append_sequence) FROM events", [], |row| {
            row.get(0)
        })
        .map_err(|error| map_sqlite_error("SQLite EventStore sequence read failed", error))?;
    max.map_or(Ok(0), |value| {
        if value < 0 {
            return Err(failure("SQLite EventStore append sequence is negative"));
        }
        u64::try_from(value)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| failure("SQLite EventStore append sequence overflow"))
    })
}

fn read_row(row: &Row<'_>) -> rusqlite::Result<StoredRow> {
    Ok(StoredRow {
        append_sequence: row.get(0)?,
        event_id: row.get(1)?,
        session_id: row.get(2)?,
        actor_id: row.get(3)?,
        action_id: row.get(4)?,
        occurred_at_json: row.get(5)?,
        event_type: row.get(6)?,
        payload_json: row.get(7)?,
        metadata_json: row.get(8)?,
    })
}

struct StoredRow {
    append_sequence: i64,
    event_id: String,
    session_id: String,
    actor_id: String,
    action_id: String,
    occurred_at_json: String,
    event_type: String,
    payload_json: String,
    metadata_json: String,
}

fn decode_row(row: StoredRow) -> Result<(u64, Event), StorageError> {
    let sequence = u64::try_from(row.append_sequence)
        .map_err(|_| failure("SQLite EventStore append sequence is invalid"))?;
    let event_id = row
        .event_id
        .parse::<EventId>()
        .map_err(|_| failure("SQLite EventStore EventId is invalid"))?;
    let session_id = row
        .session_id
        .parse::<SessionId>()
        .map_err(|_| failure("SQLite EventStore SessionId is invalid"))?;
    let actor_id = row
        .actor_id
        .parse::<ActorId>()
        .map_err(|_| failure("SQLite EventStore ActorId is invalid"))?;
    let action_id = row
        .action_id
        .parse::<ActionId>()
        .map_err(|_| failure("SQLite EventStore ActionId is invalid"))?;
    let occurred_at = serde_json::from_str::<Timestamp>(&row.occurred_at_json)
        .map_err(|_| failure("SQLite EventStore timestamp is invalid"))?;
    let payload = serde_json::from_str::<Payload>(&row.payload_json)
        .map_err(|_| failure("SQLite EventStore payload is invalid"))?;
    let metadata = serde_json::from_str::<Metadata>(&row.metadata_json)
        .map_err(|_| failure("SQLite EventStore metadata is invalid"))?;
    Ok((
        sequence,
        Event::new(
            event_id,
            session_id,
            actor_id,
            action_id,
            EventType::new(row.event_type),
            occurred_at,
            payload,
            metadata,
        ),
    ))
}

fn map_send_error(error: mpsc::error::TrySendError<Command>) -> StorageError {
    match error {
        mpsc::error::TrySendError::Full(_) => unavailable("SQLite command queue is full"),
        mpsc::error::TrySendError::Closed(_) => unavailable("SQLite worker is unavailable"),
    }
}

fn unavailable(message: &'static str) -> StorageError {
    StorageError::BackendUnavailable {
        message: message.to_owned(),
    }
}

fn failure(message: &'static str) -> StorageError {
    StorageError::BackendFailure {
        message: message.to_owned(),
    }
}

fn map_sqlite_error(message: &'static str, error: SqliteError) -> StorageError {
    if matches!(
        &error,
        SqliteError::SqliteFailure(error, _)
            if matches!(error.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    ) {
        return unavailable("SQLite EventStore is busy");
    }
    failure(message)
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use orbitrelay_core::{Metadata, Timestamp};
    use orbitrelay_protocol::{ActionId, ActorId, Event, EventId, EventType, Payload, SessionId};
    use orbitrelay_storage::{EventQuery, EventStore, StorageError};
    use rusqlite::Connection;

    use super::SQLiteEventStore;

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("orbitrelay-storage-{}.db", EventId::new()))
    }

    fn event(id: EventId, session: SessionId, occurred_at: i64, kind: &str) -> Event {
        Event::new(
            id,
            session,
            ActorId::new(),
            ActionId::new(),
            EventType::new(kind),
            Timestamp::from_unix_timestamp(occurred_at).expect("timestamp"),
            Payload::new(),
            Metadata::new(),
        )
    }

    async fn cleanup(store: &SQLiteEventStore, path: &std::path::Path) {
        store.close().await.expect("close");
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("db-wal"));
        let _ = fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn persists_events_and_order_across_reopen() {
        let path = path();
        let session = SessionId::new();
        let first = event(EventId::new(), session.clone(), 200, "second");
        let second = event(EventId::new(), session, 100, "first");
        let store = SQLiteEventStore::open(&path).expect("open");
        store.append(first.clone()).await.expect("append");
        store.append(second.clone()).await.expect("append");
        let physical_id = store.physical_store_id().clone();
        store.close().await.expect("close");

        let reopened = SQLiteEventStore::open(&path).expect("reopen");
        assert_eq!(reopened.physical_store_id(), &physical_id);
        let page = reopened.query(EventQuery::all()).await.expect("query");
        assert_eq!(page.events()[0].event(), &first);
        assert_eq!(page.events()[1].event(), &second);
        assert_ne!(page.events()[0].cursor(), page.events()[1].cursor());
        cleanup(&reopened, &path).await;
    }

    #[tokio::test]
    async fn idempotency_and_conflict_match_memory_semantics() {
        let path = path();
        let store = SQLiteEventStore::open(&path).expect("open");
        let id = EventId::new();
        let original = event(id.clone(), SessionId::new(), 1, "fact");
        let first = store.append(original.clone()).await.expect("append");
        let duplicate = store.append(original).await.expect("idempotent append");
        assert_eq!(first, duplicate);
        let conflict = store
            .append(event(id.clone(), SessionId::new(), 2, "other"))
            .await
            .expect_err("conflict");
        assert_eq!(conflict, StorageError::EventConflict { event_id: id });
        cleanup(&store, &path).await;
    }

    #[tokio::test]
    async fn checkpoint_excludes_later_events_and_old_tokens_invalidate() {
        let path = path();
        let store = SQLiteEventStore::open(&path).expect("open");
        let session = SessionId::new();
        let first = event(EventId::new(), session.clone(), 1, "one");
        store.append(first).await.expect("append");
        store
            .append(event(EventId::new(), session.clone(), 2, "two"))
            .await
            .expect("append");
        let checkpoint = store.capture_checkpoint().await.expect("checkpoint");
        let cursor = store
            .query(EventQuery::all().with_limit(1))
            .await
            .expect("query")
            .next_cursor()
            .cloned();
        store
            .append(event(EventId::new(), session, 3, "three"))
            .await
            .expect("append");
        let bounded = store
            .query(EventQuery::all().before(checkpoint.clone()))
            .await
            .expect("bounded query");
        assert_eq!(bounded.len(), 2);
        store.close().await.expect("close");

        let reopened = SQLiteEventStore::open(&path).expect("reopen");
        assert!(reopened
            .query(EventQuery::all().before(checkpoint))
            .await
            .is_err());
        if let Some(cursor) = cursor {
            assert!(reopened
                .query(EventQuery::all().after(cursor))
                .await
                .is_err());
        }
        cleanup(&reopened, &path).await;
    }

    #[tokio::test]
    async fn concurrent_appends_have_unique_stable_order() {
        let path = path();
        let store = Arc::new(SQLiteEventStore::open(&path).expect("open"));
        let session = SessionId::new();
        let mut tasks = Vec::new();
        for index in 0..32 {
            let store = Arc::clone(&store);
            let session = session.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .append(event(EventId::new(), session, index, "concurrent"))
                    .await
                    .expect("append")
            }));
        }
        for task in tasks {
            task.await.expect("task");
        }
        let page = store
            .query(EventQuery::all().with_limit(100))
            .await
            .expect("query");
        assert_eq!(page.len(), 32);
        let cursors = page
            .events()
            .iter()
            .map(|stored| stored.cursor().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            cursors.windows(2).filter(|pair| pair[0] < pair[1]).count(),
            31
        );
        cleanup(&store, &path).await;
    }

    #[tokio::test]
    async fn rejects_schema_that_is_newer_than_supported() {
        let path = path();
        let store = SQLiteEventStore::open(&path).expect("open");
        store.close().await.expect("close");
        let connection = Connection::open(&path).expect("raw open");
        connection
            .execute(
                "UPDATE storage_metadata SET value = '999' WHERE key = 'schema_version'",
                [],
            )
            .expect("schema update");
        drop(connection);
        let result = SQLiteEventStore::open(&path);
        assert!(matches!(result, Err(StorageError::BackendFailure { .. })));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("db-wal"));
        let _ = fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn malformed_event_row_fails_integrity_gate() {
        let path = path();
        let store = SQLiteEventStore::open(&path).expect("open");
        store.close().await.expect("close");
        let connection = Connection::open(&path).expect("raw open");
        connection
            .execute(
                "INSERT INTO events (append_sequence, event_id, session_id, actor_id, action_id, occurred_at_json, event_type, payload_json, metadata_json) VALUES (0, 'not-an-id', 'not-an-id', 'not-an-id', 'not-an-id', 'null', 'event', '{}', '{}')",
                [],
            )
            .expect("malformed row insert");
        drop(connection);
        let result = SQLiteEventStore::open(&path);
        assert!(matches!(result, Err(StorageError::BackendFailure { .. })));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("db-wal"));
        let _ = fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn close_stops_accepting_commands() {
        let path = path();
        let store = SQLiteEventStore::open(&path).expect("open");
        store.close().await.expect("close");
        let error = store
            .query(EventQuery::all())
            .await
            .expect_err("closed store must reject commands");
        assert!(matches!(error, StorageError::BackendUnavailable { .. }));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("db-wal"));
        let _ = fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn supports_filters_and_cursor_pagination() {
        let path = path();
        let store = SQLiteEventStore::open(&path).expect("open");
        let session = SessionId::new();
        for (index, kind) in [(100, "selected"), (200, "selected"), (300, "other")] {
            store
                .append(event(EventId::new(), session.clone(), index, kind))
                .await
                .expect("append");
        }
        let page = store
            .query(
                EventQuery::for_session(session)
                    .with_event_type(orbitrelay_protocol::EventType::new("selected"))
                    .with_time_range(
                        Timestamp::from_unix_timestamp(50).expect("timestamp"),
                        Timestamp::from_unix_timestamp(250).expect("timestamp"),
                    )
                    .with_limit(1),
            )
            .await
            .expect("filtered query");
        assert_eq!(page.len(), 1);
        assert!(page.next_cursor().is_some());
        let second = store
            .query(
                EventQuery::all()
                    .with_event_type(orbitrelay_protocol::EventType::new("selected"))
                    .with_limit(1)
                    .after(page.next_cursor().cloned().expect("cursor")),
            )
            .await
            .expect("continuation query");
        assert_eq!(second.len(), 1);
        cleanup(&store, &path).await;
    }

    #[tokio::test]
    async fn rejects_tokens_from_another_database() {
        let first_path = path();
        let second_path = path();
        let first = SQLiteEventStore::open(&first_path).expect("open first");
        let second = SQLiteEventStore::open(&second_path).expect("open second");
        let checkpoint = first.capture_checkpoint().await.expect("checkpoint");
        let error = second
            .query(EventQuery::all().before(checkpoint))
            .await
            .expect_err("foreign checkpoint must fail");
        assert!(matches!(error, StorageError::InvalidCheckpoint { .. }));
        cleanup(&first, &first_path).await;
        cleanup(&second, &second_path).await;
    }
}
