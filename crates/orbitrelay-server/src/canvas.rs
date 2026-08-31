//! Server adapters for the development Canvas runtime.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use async_trait::async_trait;
use orbitrelay_canvas::{
    CanvasDescriptor, CanvasEventData, CanvasEventKind, CanvasId, StrokeId, StrokeProjector,
};
use orbitrelay_canvas_runtime::{
    CanvasCatalog, CanvasCatalogError, CanvasStateReadError, CanvasStateReader,
};
use orbitrelay_protocol::SessionId;
use orbitrelay_runtime::{
    ExecutionCoordinationError, ExecutionCoordinator, ExecutionLease, ExecutionScope,
};
use orbitrelay_storage::{EventQuery, EventStore, StorageError};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tracing::error;

#[derive(Default)]
struct CoordinatorState {
    entries: Mutex<HashMap<ExecutionScope, Weak<KeyedMutex>>>,
}

struct KeyedMutex {
    scope: ExecutionScope,
    mutex: Arc<AsyncMutex<()>>,
    coordinator: Weak<CoordinatorState>,
}

impl KeyedMutex {
    fn new(scope: ExecutionScope, coordinator: &Arc<CoordinatorState>) -> Self {
        Self {
            scope,
            mutex: Arc::new(AsyncMutex::new(())),
            coordinator: Arc::downgrade(coordinator),
        }
    }
}

impl Drop for KeyedMutex {
    fn drop(&mut self) {
        let Some(coordinator) = self.coordinator.upgrade() else {
            return;
        };
        let mut entries = coordinator
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let is_current = entries
            .get(&self.scope)
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self));
        if is_current {
            entries.remove(&self.scope);
        }
    }
}

/// Keyed, in-process execution coordination for aggregate scopes.
#[derive(Clone, Default)]
pub struct TokioExecutionCoordinator {
    state: Arc<CoordinatorState>,
}

impl TokioExecutionCoordinator {
    /// Creates an empty keyed coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of live or stale keys after pruning stale entries.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.prune_stale();
        self.state
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    fn prune_stale(&self) {
        let mut entries = self
            .state
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|_, weak| weak.upgrade().is_some());
    }
}

struct TokioExecutionLease {
    guard: Option<OwnedMutexGuard<()>>,
    entry: Option<Arc<KeyedMutex>>,
}

impl ExecutionLease for TokioExecutionLease {}

impl Drop for TokioExecutionLease {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            drop(guard);
        }
        if let Some(entry) = self.entry.take() {
            drop(entry);
        }
    }
}

#[async_trait]
impl ExecutionCoordinator for TokioExecutionCoordinator {
    async fn acquire(
        &self,
        scope: &ExecutionScope,
    ) -> Result<Box<dyn ExecutionLease>, ExecutionCoordinationError> {
        self.prune_stale();
        let entry = {
            let mut entries = self
                .state
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match entries.get(scope).and_then(Weak::upgrade) {
                Some(entry) => entry,
                None => {
                    let entry = Arc::new(KeyedMutex::new(scope.clone(), &self.state));
                    entries.insert(scope.clone(), Arc::downgrade(&entry));
                    entry
                }
            }
        };
        let guard = entry.mutex.clone().lock_owned().await;
        Ok(Box::new(TokioExecutionLease {
            guard: Some(guard),
            entry: Some(entry),
        }))
    }
}

/// A development-only catalog containing trusted Canvas descriptors.
#[derive(Clone, Debug)]
pub struct DevelopmentCanvasCatalog {
    descriptors: HashMap<CanvasId, CanvasDescriptor>,
}

impl DevelopmentCanvasCatalog {
    /// Creates a catalog from one validated descriptor.
    #[must_use]
    pub fn new(descriptor: CanvasDescriptor) -> Self {
        let mut descriptors = HashMap::new();
        descriptors.insert(descriptor.canvas_id().clone(), descriptor);
        Self { descriptors }
    }

    /// Creates a catalog from a complete descriptor set.
    ///
    /// Duplicate Canvas identities are rejected instead of being silently
    /// overwritten, which keeps bootstrap publication deterministic.
    pub fn from_descriptors(
        descriptors: impl IntoIterator<Item = CanvasDescriptor>,
    ) -> Result<Self, CanvasCatalogError> {
        let mut map = HashMap::new();
        for descriptor in descriptors {
            let id = descriptor.canvas_id().clone();
            if map.contains_key(&id) {
                return Err(CanvasCatalogError::new("duplicate Canvas identity"));
            }
            map.insert(id, descriptor);
        }
        Ok(Self { descriptors: map })
    }

    /// Creates an empty catalog for non-Development processes.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            descriptors: HashMap::new(),
        }
    }

    /// Returns the first descriptor in this catalog.
    ///
    /// This preserves the original single-Canvas adapter API. Callers that
    /// need to distinguish an empty catalog should use [`Self::is_empty`]
    /// first, or use [`Self::try_descriptor`].
    #[must_use]
    pub fn descriptor(&self) -> &CanvasDescriptor {
        self.descriptors
            .values()
            .next()
            .expect("DevelopmentCanvasCatalog has no descriptor")
    }

    /// Returns the first descriptor when the catalog is non-empty.
    #[must_use]
    pub fn try_descriptor(&self) -> Option<&CanvasDescriptor> {
        self.descriptors.values().next()
    }

    /// Returns the number of registered descriptors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.descriptors.len()
    }

    /// Reports whether no descriptors are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }
}

#[async_trait]
impl CanvasCatalog for DevelopmentCanvasCatalog {
    async fn get_canvas(
        &self,
        canvas_id: &CanvasId,
    ) -> Result<Option<CanvasDescriptor>, CanvasCatalogError> {
        Ok(self.descriptors.get(canvas_id).cloned())
    }
}

/// Rebuilds one Stroke projection from append-ordered persisted Canvas events.
pub struct EventStoreCanvasStateReader {
    event_store: Arc<dyn EventStore>,
}

impl EventStoreCanvasStateReader {
    /// Creates a reader over an abstract EventStore.
    #[must_use]
    pub fn new(event_store: Arc<dyn EventStore>) -> Self {
        Self { event_store }
    }
}

#[async_trait]
impl CanvasStateReader for EventStoreCanvasStateReader {
    async fn load_stroke(
        &self,
        session_id: &SessionId,
        canvas_id: &CanvasId,
        stroke_id: &StrokeId,
    ) -> Result<Option<orbitrelay_canvas::StrokeProjection>, CanvasStateReadError> {
        let mut cursor = None;
        let mut projection = None;
        loop {
            let mut query = EventQuery::for_session(session_id.clone());
            if let Some(after) = cursor.take() {
                query = query.after(after);
            }
            let page = self
                .event_store
                .query(query)
                .await
                .map_err(map_storage_error)?;
            for stored in page.events() {
                let event = stored.event();
                let Some(_kind) = CanvasEventKind::from_event_type(event.event_type()) else {
                    continue;
                };
                let data = CanvasEventData::try_from(event).map_err(|source| {
                    error!(event_id = %event.id(), event_type = %event.event_type(), error = %source, "persisted Canvas event payload is corrupted");
                    CanvasStateReadError::projection_corrupted(source)
                })?;
                let (event_canvas, event_stroke) = match &data {
                    CanvasEventData::StrokeBegan(payload) => {
                        (payload.canvas_id(), payload.stroke_id())
                    }
                    CanvasEventData::StrokePointsAppended(payload) => {
                        (payload.canvas_id(), payload.stroke_id())
                    }
                    CanvasEventData::StrokeCompleted(payload) => {
                        (payload.canvas_id(), payload.stroke_id())
                    }
                    CanvasEventData::StrokeCancelled(payload) => {
                        (payload.canvas_id(), payload.stroke_id())
                    }
                    CanvasEventData::StrokeRemoved(payload) => {
                        (payload.canvas_id(), payload.stroke_id())
                    }
                    _ => unreachable!("CanvasEventData variants are exhaustive in this version"),
                };
                if event_canvas != canvas_id || event_stroke != stroke_id {
                    continue;
                }
                projection = Some(
                    StrokeProjector::apply(projection, event).map_err(|source| {
                        error!(event_id = %event.id(), event_type = %event.event_type(), error = %source, "persisted Canvas event sequence is corrupted");
                        CanvasStateReadError::projection_corrupted(source)
                    })?,
                );
            }
            cursor = page.next_cursor().cloned();
            if cursor.is_none() {
                return Ok(projection);
            }
        }
    }
}

fn map_storage_error(error: StorageError) -> CanvasStateReadError {
    error!(error = %error, "Canvas state EventStore query failed");
    let detail = match error {
        StorageError::BackendUnavailable { message } | StorageError::BackendFailure { message } => {
            message
        }
        _ => "event store query failed".to_owned(),
    };
    CanvasStateReadError::unavailable(detail)
}

/// Builds a development Canvas descriptor from optional configured IDs.
pub fn development_canvas_descriptor(
    session_id: Option<SessionId>,
    canvas_id: Option<CanvasId>,
    layer_id: Option<orbitrelay_canvas::LayerId>,
    width: f64,
    height: f64,
) -> Result<CanvasDescriptor, orbitrelay_canvas::CanvasError> {
    let session_id = session_id.unwrap_or_else(SessionId::new);
    let canvas_id = canvas_id.unwrap_or_else(CanvasId::new);
    let layer_id = layer_id.unwrap_or_else(orbitrelay_canvas::LayerId::new);
    let space = orbitrelay_canvas::CanvasSpace::new(width, height)?;
    CanvasDescriptor::new(canvas_id, session_id, space, [layer_id.clone()], layer_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbitrelay_canvas::{CanvasPoint, RgbaColor, StrokeBeginPayload, StrokeStyle, StrokeTool};
    use orbitrelay_canvas_runtime::CanvasStateReader;
    use orbitrelay_core::{Metadata, Timestamp};
    use orbitrelay_protocol::{ActionId, ActorId, Event, EventId, EventType, Payload};
    use orbitrelay_runtime::ExecutionCoordinator;
    use orbitrelay_storage::{EventStore, MemoryEventStore};
    use tokio::{
        sync::Barrier,
        time::{sleep, timeout, Duration},
    };

    fn scope(value: &str) -> ExecutionScope {
        ExecutionScope::new("test", value).expect("scope should be valid")
    }

    #[tokio::test]
    async fn same_scope_is_serial_and_different_scopes_are_parallel() {
        let coordinator = Arc::new(TokioExecutionCoordinator::new());
        let first = coordinator.acquire(&scope("a")).await.expect("lease");
        let waiting = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move { coordinator.acquire(&scope("a")).await })
        };
        assert!(timeout(Duration::from_millis(30), waiting).await.is_err());
        drop(first);
        let second = timeout(Duration::from_secs(1), coordinator.acquire(&scope("a")))
            .await
            .expect("second acquisition")
            .expect("lease");
        drop(second);

        let barrier = Arc::new(Barrier::new(2));
        let b1 = barrier.clone();
        let c1 = coordinator.clone();
        let task1 = tokio::spawn(async move {
            let _lease = c1.acquire(&scope("x")).await.unwrap();
            b1.wait().await;
        });
        let b2 = barrier.clone();
        let c2 = coordinator.clone();
        let task2 = tokio::spawn(async move {
            let _lease = c2.acquire(&scope("y")).await.unwrap();
            b2.wait().await;
        });
        timeout(Duration::from_secs(1), async {
            let _ = tokio::join!(task1, task2);
        })
        .await
        .expect("different scopes should proceed in parallel");
    }

    #[tokio::test]
    async fn stale_keys_are_pruned_and_cancellation_does_not_block_future_acquire() {
        let coordinator = TokioExecutionCoordinator::new();
        for index in 0..500 {
            let lease = coordinator
                .acquire(&scope(&index.to_string()))
                .await
                .unwrap();
            drop(lease);
        }
        assert!(coordinator
            .state
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
        assert_eq!(coordinator.key_count(), 0);
        let held = coordinator.acquire(&scope("cancel")).await.unwrap();
        let task = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move { coordinator.acquire(&scope("cancel")).await })
        };
        sleep(Duration::from_millis(10)).await;
        task.abort();
        let _ = task.await;
        drop(held);
        assert!(timeout(
            Duration::from_secs(1),
            coordinator.acquire(&scope("cancel"))
        )
        .await
        .is_ok());
        assert_eq!(coordinator.key_count(), 0);
    }

    #[test]
    fn old_lease_cleanup_cannot_remove_a_replacement_entry() {
        let coordinator = TokioExecutionCoordinator::new();
        let scope = scope("replacement");
        let old_entry = Arc::new(KeyedMutex::new(scope.clone(), &coordinator.state));
        let replacement = Arc::new(KeyedMutex::new(scope.clone(), &coordinator.state));
        coordinator
            .state
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(scope.clone(), Arc::downgrade(&replacement));

        drop(old_entry);

        let current = coordinator
            .state
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&scope)
            .and_then(Weak::upgrade)
            .expect("replacement entry should remain");
        assert!(Arc::ptr_eq(&current, &replacement));
    }

    #[tokio::test]
    async fn state_reader_replays_across_pages_and_ignores_non_canvas_events() {
        let store = Arc::new(MemoryEventStore::new());
        let session_id = SessionId::new();
        let canvas_id = CanvasId::new();
        let layer_id = orbitrelay_canvas::LayerId::new();
        let stroke_id = StrokeId::new();
        for _ in 0..101 {
            store
                .append(Event::new(
                    EventId::new(),
                    session_id.clone(),
                    ActorId::new(),
                    ActionId::new(),
                    orbitrelay_protocol::EventType::new("dev.echoed"),
                    Timestamp::now_utc(),
                    Payload::new(),
                    Metadata::new(),
                ))
                .await
                .expect("non-Canvas event should append");
        }
        let point = CanvasPoint::new(1.0, 1.0).expect("point should be finite");
        let style = StrokeStyle::new(1.0, RgbaColor::new(0, 0, 0, 255)).expect("style");
        let begin = StrokeBeginPayload::new(
            canvas_id.clone(),
            layer_id,
            stroke_id.clone(),
            StrokeTool::Pen,
            style,
            0,
            [point],
        )
        .expect("begin payload");
        store
            .append(Event::new(
                EventId::new(),
                session_id.clone(),
                ActorId::new(),
                ActionId::new(),
                EventType::new(orbitrelay_canvas::STROKE_BEGAN_EVENT_TYPE),
                Timestamp::now_utc(),
                orbitrelay_protocol::Payload::try_from(&begin).expect("payload"),
                Metadata::new(),
            ))
            .await
            .expect("Canvas event should append");
        let reader = EventStoreCanvasStateReader::new(store);
        let projection = reader
            .load_stroke(&session_id, &canvas_id, &stroke_id)
            .await
            .expect("replay should succeed")
            .expect("stroke should be found on second page");
        assert_eq!(projection.last_chunk_index(), 0);
    }

    #[tokio::test]
    async fn state_reader_rejects_corrupted_recognized_canvas_payloads() {
        let store = Arc::new(MemoryEventStore::new());
        let session_id = SessionId::new();
        store
            .append(Event::new(
                EventId::new(),
                session_id.clone(),
                ActorId::new(),
                ActionId::new(),
                EventType::new(orbitrelay_canvas::STROKE_BEGAN_EVENT_TYPE),
                Timestamp::now_utc(),
                Payload::new(),
                Metadata::new(),
            ))
            .await
            .expect("corrupted fixture should append to append-only store");
        let reader = EventStoreCanvasStateReader::new(store);
        let result = reader
            .load_stroke(&session_id, &CanvasId::new(), &StrokeId::new())
            .await;
        assert!(matches!(
            result,
            Err(CanvasStateReadError::ProjectionCorrupted { .. })
        ));
    }
}
