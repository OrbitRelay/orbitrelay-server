//! Storage-to-sync event pipeline adapter.

use std::sync::Arc;

use async_trait::async_trait;
use orbitrelay_protocol::Event;
use orbitrelay_runtime::{EventPipeline, PipelineError};
use orbitrelay_storage::EventStore;
use orbitrelay_sync::EventBus;

/// Persists a complete event batch before publishing it to subscribers.
pub struct PipelineAdapter {
    event_store: Arc<dyn EventStore>,
    event_bus: Arc<dyn EventBus>,
}

impl PipelineAdapter {
    /// Creates a pipeline adapter from abstract storage and synchronization ports.
    #[must_use]
    pub fn new(event_store: Arc<dyn EventStore>, event_bus: Arc<dyn EventBus>) -> Self {
        Self {
            event_store,
            event_bus,
        }
    }
}

#[async_trait]
impl EventPipeline for PipelineAdapter {
    async fn dispatch(&self, events: &[Event]) -> Result<(), PipelineError> {
        for event in events {
            self.event_store.append(event.clone()).await.map_err(|_| {
                PipelineError::new(format!("event persistence failed for `{}`", event.id()))
            })?;
        }

        for event in events {
            self.event_bus.publish(event.clone()).await.map_err(|_| {
                PipelineError::new(format!("event publication failed for `{}`", event.id()))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use orbitrelay_core::{Metadata, Timestamp};
    use orbitrelay_protocol::{ActionId, ActorId, Event, EventId, EventType, Payload, SessionId};
    use orbitrelay_runtime::EventPipeline;
    use orbitrelay_storage::{
        EventPage, EventQuery, EventStore, EventStoreCheckpoint, MemoryEventStore, StorageError,
        StoredEvent,
    };
    use orbitrelay_sync::{EventBus, EventFilter, MemoryEventBus, Subscription, SyncError};

    use super::PipelineAdapter;

    fn event() -> Event {
        Event::new(
            EventId::new(),
            SessionId::new(),
            ActorId::new(),
            ActionId::new(),
            EventType::new("test.completed"),
            Timestamp::from_unix_timestamp(1_700_000_000).expect("timestamp is valid"),
            Payload::new(),
            Metadata::new(),
        )
    }

    #[tokio::test]
    async fn stores_before_publishing() {
        let store = Arc::new(MemoryEventStore::new());
        let bus = Arc::new(
            MemoryEventBus::with_queue_capacity(2).expect("queue capacity should be valid"),
        );
        let mut subscription = bus
            .subscribe(EventFilter::all())
            .await
            .expect("subscription should succeed");
        let adapter = PipelineAdapter::new(store.clone(), bus);
        let event = event();

        adapter
            .dispatch(std::slice::from_ref(&event))
            .await
            .expect("dispatch should succeed");

        let stored = store
            .get(event.id())
            .await
            .expect("get should succeed")
            .expect("event should be stored");
        assert_eq!(stored.event(), &event);
        assert_eq!(
            subscription
                .next_event()
                .await
                .expect("subscription should succeed"),
            Some(event)
        );
    }

    struct FailingStore {
        inner: MemoryEventStore,
        appends: AtomicUsize,
    }

    impl FailingStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
                appends: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl EventStore for FailingStore {
        async fn capture_checkpoint(&self) -> Result<EventStoreCheckpoint, StorageError> {
            self.inner.capture_checkpoint().await
        }

        async fn append(&self, event: Event) -> Result<StoredEvent, StorageError> {
            if self.appends.fetch_add(1, Ordering::SeqCst) == 1 {
                return Err(StorageError::BackendFailure {
                    message: "test-only storage detail".to_owned(),
                });
            }

            self.inner.append(event).await
        }

        async fn get(&self, event_id: &EventId) -> Result<Option<StoredEvent>, StorageError> {
            self.inner.get(event_id).await
        }

        async fn query(&self, query: EventQuery) -> Result<EventPage, StorageError> {
            self.inner.query(query).await
        }
    }

    struct CountingBus {
        publishes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl EventBus for CountingBus {
        async fn publish(&self, _event: Event) -> Result<(), SyncError> {
            self.publishes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn subscribe(
            &self,
            _filter: EventFilter,
        ) -> Result<Box<dyn Subscription>, SyncError> {
            Err(SyncError::InvalidFilter {
                reason: "not implemented by the test bus".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn storage_failure_does_not_publish_or_leak_backend_error() {
        let publishes = Arc::new(AtomicUsize::new(0));
        let bus = Arc::new(CountingBus {
            publishes: publishes.clone(),
        });
        let adapter = PipelineAdapter::new(Arc::new(FailingStore::new()), bus);
        let events = vec![event(), event()];

        let error = adapter
            .dispatch(&events)
            .await
            .expect_err("storage failure should stop dispatch");

        assert!(error.message().starts_with("event persistence failed for"));
        assert!(!error.message().contains("test-only storage detail"));
        assert_eq!(publishes.load(Ordering::SeqCst), 0);
    }
}
