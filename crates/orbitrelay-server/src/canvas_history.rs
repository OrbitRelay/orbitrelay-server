//! Canvas history Query adapter with stable EventStore replay boundaries.

use std::sync::Arc;

use async_trait::async_trait;
use orbitrelay_canvas::{
    CanvasEventData, CanvasId, STROKE_BEGAN_EVENT_TYPE, STROKE_CANCELLED_EVENT_TYPE,
    STROKE_COMPLETED_EVENT_TYPE, STROKE_POINTS_APPENDED_EVENT_TYPE, STROKE_REMOVED_EVENT_TYPE,
};
use orbitrelay_canvas_runtime::CanvasCatalog;
use orbitrelay_core::{Metadata, Timestamp};
use orbitrelay_protocol::{ActionId, ActorId, Event, EventId, EventType, Payload, SessionId};
use orbitrelay_query::{
    QueryActorContext, QueryHandler, QueryHandlerError, QueryRegistry, QueryRegistryError,
    QueryRequest, QueryResponse, QueryType,
};
use orbitrelay_storage::{EventCursor, EventQuery, EventStore, EventStoreCheckpoint, StorageError};
use orbitrelay_transport::{
    JsonCodec, MessageCodec, OutboundMessage, QueryResponseMessage, QUERY_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::error;

/// Protocol 0.2 Query type for one bounded Canvas history page.
pub const CANVAS_HISTORY_PAGE_QUERY_TYPE: &str = "canvas.history.page";

/// Default number of candidate stored Canvas events scanned by one Query.
pub const DEFAULT_HISTORY_STORE_SCAN_LIMIT: usize = 64;

/// Hard deployment limit for one bounded history Store query.
pub const MAX_HISTORY_STORE_SCAN_LIMIT: usize = 256;

/// Failure returned by the Canvas history read authorization port.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CanvasHistoryReadAuthorizationError {
    /// The actor cannot read the resolved Canvas Session.
    #[error("Canvas history read is unauthorized")]
    Unauthorized,
    /// The authorization backend is temporarily unavailable.
    #[error("Canvas history authorization is unavailable")]
    Unavailable,
    /// The authorization backend failed unexpectedly.
    #[error("Canvas history authorization failed")]
    Internal,
}

/// Authorizes history reads using trusted actor context and resolved Session identity.
#[async_trait]
pub trait CanvasHistoryReadAuthorizer: Send + Sync {
    /// Authorizes one Query after the Server has resolved the Canvas Session.
    async fn authorize_session_read(
        &self,
        actor: &QueryActorContext,
        session_id: &SessionId,
        query_type: &QueryType,
    ) -> Result<(), CanvasHistoryReadAuthorizationError>;
}

/// Safe production placeholder that rejects every Canvas history read.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectAllCanvasHistoryReadAuthorizer;

#[async_trait]
impl CanvasHistoryReadAuthorizer for RejectAllCanvasHistoryReadAuthorizer {
    async fn authorize_session_read(
        &self,
        _actor: &QueryActorContext,
        _session_id: &SessionId,
        _query_type: &QueryType,
    ) -> Result<(), CanvasHistoryReadAuthorizationError> {
        Err(CanvasHistoryReadAuthorizationError::Unauthorized)
    }
}

/// Development authorizer scoped to exactly one configured Session.
#[derive(Clone, Debug)]
pub struct DevelopmentCanvasHistoryReadAuthorizer {
    session_id: SessionId,
}

impl DevelopmentCanvasHistoryReadAuthorizer {
    /// Creates an authorizer for one Development Session.
    #[must_use]
    pub const fn new(session_id: SessionId) -> Self {
        Self { session_id }
    }
}

#[async_trait]
impl CanvasHistoryReadAuthorizer for DevelopmentCanvasHistoryReadAuthorizer {
    async fn authorize_session_read(
        &self,
        _actor: &QueryActorContext,
        session_id: &SessionId,
        _query_type: &QueryType,
    ) -> Result<(), CanvasHistoryReadAuthorizationError> {
        if session_id == &self.session_id {
            Ok(())
        } else {
            Err(CanvasHistoryReadAuthorizationError::Unauthorized)
        }
    }
}

/// Explicit wire representation of an authoritative Event returned by history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEventDto {
    event_id: EventId,
    session_id: SessionId,
    actor_id: ActorId,
    action_id: ActionId,
    event_type: EventType,
    occurred_at: Timestamp,
    payload: Payload,
    metadata: Metadata,
}

impl HistoryEventDto {
    fn from_event(event: &Event) -> Self {
        Self {
            event_id: event.id().clone(),
            session_id: event.session_id().clone(),
            actor_id: event.actor_id().clone(),
            action_id: event.action_id().clone(),
            event_type: event.event_type().clone(),
            occurred_at: event.occurred_at().clone(),
            payload: event.payload().clone(),
            metadata: event.metadata().clone(),
        }
    }

    /// Returns the Event identity used for history/realtime deduplication.
    #[must_use]
    pub const fn event_id(&self) -> &EventId {
        &self.event_id
    }

    /// Returns the owning Session.
    #[must_use]
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the actor that originated the Event's Action.
    #[must_use]
    pub const fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }

    /// Returns the causal Action identity.
    #[must_use]
    pub const fn action_id(&self) -> &ActionId {
        &self.action_id
    }

    /// Returns the stable Event type.
    #[must_use]
    pub const fn event_type(&self) -> &EventType {
        &self.event_type
    }

    /// Returns the Event occurrence time.
    #[must_use]
    pub const fn occurred_at(&self) -> &Timestamp {
        &self.occurred_at
    }

    /// Returns the authoritative Event payload.
    #[must_use]
    pub const fn payload(&self) -> &Payload {
        &self.payload
    }

    /// Returns protocol Event metadata.
    #[must_use]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

/// One stable, append-ordered Canvas history page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanvasHistoryPageDto {
    canvas_id: CanvasId,
    checkpoint: EventStoreCheckpoint,
    events: Vec<HistoryEventDto>,
    next_cursor: Option<EventCursor>,
    complete: bool,
}

impl CanvasHistoryPageDto {
    /// Returns the resolved Canvas identity.
    #[must_use]
    pub const fn canvas_id(&self) -> &CanvasId {
        &self.canvas_id
    }

    /// Returns the stable checkpoint reused by every continuation.
    #[must_use]
    pub const fn checkpoint(&self) -> &EventStoreCheckpoint {
        &self.checkpoint
    }

    /// Returns target Canvas Events in EventStore append order.
    #[must_use]
    pub fn events(&self) -> &[HistoryEventDto] {
        &self.events
    }

    /// Returns the Store continuation cursor when this replay is incomplete.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<&EventCursor> {
        self.next_cursor.as_ref()
    }

    /// Reports whether the checkpoint boundary has been reached.
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CanvasHistoryRequest {
    First(FirstHistoryRequest),
    Continue(ContinueHistoryRequest),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FirstHistoryRequest {
    canvas_id: CanvasId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContinueHistoryRequest {
    canvas_id: CanvasId,
    checkpoint: EventStoreCheckpoint,
    cursor: EventCursor,
}

/// Handles `canvas.history.page` without entering the Action Runtime.
pub struct CanvasHistoryQueryHandler {
    query_type: QueryType,
    canvas_catalog: Arc<dyn CanvasCatalog>,
    authorizer: Arc<dyn CanvasHistoryReadAuthorizer>,
    event_store: Arc<dyn EventStore>,
    store_scan_limit: usize,
    max_message_bytes: usize,
}

impl CanvasHistoryQueryHandler {
    /// Creates a bounded history Query adapter.
    pub fn new(
        canvas_catalog: Arc<dyn CanvasCatalog>,
        authorizer: Arc<dyn CanvasHistoryReadAuthorizer>,
        event_store: Arc<dyn EventStore>,
        store_scan_limit: usize,
        max_message_bytes: usize,
    ) -> Result<Self, QueryHandlerError> {
        if store_scan_limit == 0
            || store_scan_limit > MAX_HISTORY_STORE_SCAN_LIMIT
            || max_message_bytes == 0
        {
            return Err(QueryHandlerError::Internal);
        }
        Ok(Self {
            query_type: QueryType::new(CANVAS_HISTORY_PAGE_QUERY_TYPE)
                .map_err(|_| QueryHandlerError::Internal)?,
            canvas_catalog,
            authorizer,
            event_store,
            store_scan_limit,
            max_message_bytes,
        })
    }
}

#[async_trait]
impl QueryHandler for CanvasHistoryQueryHandler {
    fn query_type(&self) -> &QueryType {
        &self.query_type
    }

    async fn execute(
        &self,
        actor: &QueryActorContext,
        request: QueryRequest,
    ) -> Result<Payload, QueryHandlerError> {
        let request_id = request.request_id().clone();
        let request_query_type = request.query_type().clone();
        let decoded: CanvasHistoryRequest = decode_payload(request.payload())?;
        let (canvas_id, supplied_bounds) = match decoded {
            CanvasHistoryRequest::First(value) => (value.canvas_id, None),
            CanvasHistoryRequest::Continue(value) => {
                (value.canvas_id, Some((value.checkpoint, value.cursor)))
            }
        };

        let descriptor = self
            .canvas_catalog
            .get_canvas(&canvas_id)
            .await
            .map_err(|_| QueryHandlerError::Unavailable)?
            .ok_or(QueryHandlerError::NotFound)?;
        self.authorizer
            .authorize_session_read(actor, descriptor.session_id(), &self.query_type)
            .await
            .map_err(map_authorization_error)?;

        let (checkpoint, cursor) = match supplied_bounds {
            Some((checkpoint, cursor)) => (checkpoint, Some(cursor)),
            None => (
                self.event_store
                    .capture_checkpoint()
                    .await
                    .map_err(map_capture_error)?,
                None,
            ),
        };

        let mut query = canvas_event_query(descriptor.session_id().clone(), checkpoint.clone())
            .with_limit(self.store_scan_limit);
        if let Some(cursor) = cursor {
            query = query.after(cursor);
        }
        let page = self
            .event_store
            .query(query)
            .await
            .map_err(map_query_error)?;

        let mut events = Vec::new();
        for record in page.events() {
            let event = record.event();
            let data = CanvasEventData::try_from(event).map_err(|_| {
                error!(
                    event_id = %event.id(),
                    event_type = %event.event_type(),
                    "recognized Canvas history Event has an invalid payload"
                );
                QueryHandlerError::Internal
            })?;
            if canvas_id_for(&data).ok_or(QueryHandlerError::Internal)? == &canvas_id {
                events.push(HistoryEventDto::from_event(event));
            }
        }

        let next_cursor = page.next_cursor().cloned();
        let response = CanvasHistoryPageDto {
            canvas_id,
            checkpoint,
            events,
            complete: next_cursor.is_none(),
            next_cursor,
        };
        let payload = encode_payload(&response)?;
        guard_response_size(
            &request_id,
            &request_query_type,
            &payload,
            self.max_message_bytes,
        )?;
        Ok(payload)
    }
}

/// Registers the Protocol 0.2 Canvas history Query handler.
pub fn register_canvas_history_query_handler(
    registry: &mut QueryRegistry,
    canvas_catalog: Arc<dyn CanvasCatalog>,
    authorizer: Arc<dyn CanvasHistoryReadAuthorizer>,
    event_store: Arc<dyn EventStore>,
    store_scan_limit: usize,
    max_message_bytes: usize,
) -> Result<(), QueryRegistryError> {
    let handler = CanvasHistoryQueryHandler::new(
        canvas_catalog,
        authorizer,
        event_store,
        store_scan_limit,
        max_message_bytes,
    )
    .map_err(|_| QueryRegistryError::DuplicateQueryType {
        query_type: QueryType::new(CANVAS_HISTORY_PAGE_QUERY_TYPE)
            .expect("Canvas history Query type is static and valid"),
    })?;
    registry.register(Arc::new(handler))
}

fn canvas_event_query(session_id: SessionId, checkpoint: EventStoreCheckpoint) -> EventQuery {
    EventQuery::for_session(session_id)
        .with_event_type(EventType::new(STROKE_BEGAN_EVENT_TYPE))
        .with_event_type(EventType::new(STROKE_POINTS_APPENDED_EVENT_TYPE))
        .with_event_type(EventType::new(STROKE_COMPLETED_EVENT_TYPE))
        .with_event_type(EventType::new(STROKE_CANCELLED_EVENT_TYPE))
        .with_event_type(EventType::new(STROKE_REMOVED_EVENT_TYPE))
        .before(checkpoint)
}

fn canvas_id_for(data: &CanvasEventData) -> Option<&CanvasId> {
    match data {
        CanvasEventData::StrokeBegan(payload) => Some(payload.canvas_id()),
        CanvasEventData::StrokePointsAppended(payload) => Some(payload.canvas_id()),
        CanvasEventData::StrokeCompleted(payload) => Some(payload.canvas_id()),
        CanvasEventData::StrokeCancelled(payload) => Some(payload.canvas_id()),
        CanvasEventData::StrokeRemoved(payload) => Some(payload.canvas_id()),
        _ => None,
    }
}

fn map_authorization_error(error: CanvasHistoryReadAuthorizationError) -> QueryHandlerError {
    match error {
        CanvasHistoryReadAuthorizationError::Unauthorized => QueryHandlerError::Unauthorized,
        CanvasHistoryReadAuthorizationError::Unavailable => QueryHandlerError::Unavailable,
        CanvasHistoryReadAuthorizationError::Internal => QueryHandlerError::Internal,
    }
}

fn map_capture_error(error: StorageError) -> QueryHandlerError {
    match error {
        StorageError::BackendUnavailable { .. } => QueryHandlerError::Unavailable,
        _ => QueryHandlerError::Internal,
    }
}

fn map_query_error(error: StorageError) -> QueryHandlerError {
    match error {
        StorageError::InvalidQuery { .. }
        | StorageError::InvalidCursor { .. }
        | StorageError::InvalidCheckpoint { .. } => QueryHandlerError::InvalidQuery,
        StorageError::BackendUnavailable { .. } => QueryHandlerError::Unavailable,
        _ => QueryHandlerError::Internal,
    }
}

fn decode_payload<T: for<'de> Deserialize<'de>>(payload: &Payload) -> Result<T, QueryHandlerError> {
    let value = serde_json::to_value(payload).map_err(|_| QueryHandlerError::InvalidQuery)?;
    serde_json::from_value(value).map_err(|_| QueryHandlerError::InvalidQuery)
}

fn encode_payload<T: Serialize>(value: &T) -> Result<Payload, QueryHandlerError> {
    let value = serde_json::to_value(value).map_err(|_| QueryHandlerError::Internal)?;
    serde_json::from_value(value).map_err(|_| QueryHandlerError::Internal)
}

fn guard_response_size(
    request_id: &orbitrelay_protocol::MessageId,
    query_type: &QueryType,
    payload: &Payload,
    max_message_bytes: usize,
) -> Result<(), QueryHandlerError> {
    let response = QueryResponse::success(request_id.clone(), query_type.clone(), payload.clone());
    let outbound = OutboundMessage::QueryResponse(QueryResponseMessage::from_response(
        QUERY_PROTOCOL_VERSION,
        response,
    ));
    let size = JsonCodec
        .encode_outbound(&outbound)
        .map_err(|_| QueryHandlerError::Internal)?
        .len();
    if size > max_message_bytes {
        error!(
            encoded_bytes = size,
            max_message_bytes, "Canvas history response exceeds the transport message limit"
        );
        return Err(QueryHandlerError::Internal);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use orbitrelay_canvas::{
        CanvasError, CanvasId, CanvasPoint, LayerId, RgbaColor, StrokeAppendPayload,
        StrokeBeginPayload, StrokeEndPayload, StrokeId, StrokeStyle, StrokeTool,
        MAX_POINTS_PER_CHUNK, STROKE_BEGAN_EVENT_TYPE, STROKE_COMPLETED_EVENT_TYPE,
        STROKE_POINTS_APPENDED_EVENT_TYPE,
    };
    use orbitrelay_protocol::{
        ActionId, ActorId, Event, EventId, EventType, MessageId, Payload, SessionId,
    };
    use orbitrelay_query::{QueryActorContext, QueryHandler, QueryHandlerError, QueryRequest};
    use orbitrelay_storage::{
        EventPage, EventQuery, EventStore, EventStoreCheckpoint, MemoryEventStore, StorageError,
        StoredEvent,
    };
    use serde::Serialize;
    use serde_json::json;

    use super::{
        CanvasHistoryPageDto, CanvasHistoryQueryHandler, DevelopmentCanvasHistoryReadAuthorizer,
        RejectAllCanvasHistoryReadAuthorizer, CANVAS_HISTORY_PAGE_QUERY_TYPE,
        DEFAULT_HISTORY_STORE_SCAN_LIMIT,
    };
    use crate::DevelopmentCanvasCatalog;

    struct Fixture {
        session_id: SessionId,
        actor_id: ActorId,
        canvas_id: CanvasId,
        other_canvas_id: CanvasId,
        layer_id: LayerId,
        stroke_id: StrokeId,
        catalog: Arc<DevelopmentCanvasCatalog>,
        store: Arc<MemoryEventStore>,
    }

    impl Fixture {
        fn new() -> Self {
            let session_id = SessionId::new();
            let canvas_id = CanvasId::new();
            let other_canvas_id = CanvasId::new();
            let layer_id = LayerId::new();
            let first = descriptor(canvas_id.clone(), session_id.clone(), layer_id.clone());
            let second = descriptor(
                other_canvas_id.clone(),
                session_id.clone(),
                layer_id.clone(),
            );
            Self {
                session_id,
                actor_id: ActorId::new(),
                canvas_id,
                other_canvas_id,
                layer_id,
                stroke_id: StrokeId::new(),
                catalog: Arc::new(
                    DevelopmentCanvasCatalog::from_descriptors([first, second])
                        .expect("descriptors should be distinct"),
                ),
                store: Arc::new(MemoryEventStore::new()),
            }
        }

        fn handler(
            &self,
            scan_limit: usize,
            max_message_bytes: usize,
        ) -> CanvasHistoryQueryHandler {
            CanvasHistoryQueryHandler::new(
                self.catalog.clone(),
                Arc::new(DevelopmentCanvasHistoryReadAuthorizer::new(
                    self.session_id.clone(),
                )),
                self.store.clone(),
                scan_limit,
                max_message_bytes,
            )
            .expect("handler should be valid")
        }

        fn request(&self, value: serde_json::Value) -> QueryRequest {
            QueryRequest::new(
                MessageId::new(),
                orbitrelay_query::QueryType::new(CANVAS_HISTORY_PAGE_QUERY_TYPE)
                    .expect("query type is valid"),
                serde_json::from_value(value).expect("request payload should be an object"),
            )
        }

        async fn execute(
            &self,
            handler: &CanvasHistoryQueryHandler,
            value: serde_json::Value,
        ) -> Result<CanvasHistoryPageDto, QueryHandlerError> {
            let payload = handler
                .execute(
                    &QueryActorContext::new(self.actor_id.clone()),
                    self.request(value),
                )
                .await?;
            serde_json::from_value(serde_json::to_value(payload).expect("payload should encode"))
                .map_err(|_| QueryHandlerError::Internal)
        }

        fn event<T>(&self, event_type: &str, payload: T) -> Event
        where
            Payload: TryFrom<T, Error = CanvasError>,
        {
            Event::new(
                EventId::new(),
                self.session_id.clone(),
                self.actor_id.clone(),
                ActionId::new(),
                EventType::new(event_type),
                orbitrelay_core::Timestamp::now_utc(),
                Payload::try_from(payload).expect("Canvas payload should encode"),
                orbitrelay_core::Metadata::new(),
            )
        }
    }

    fn descriptor(
        canvas_id: CanvasId,
        session_id: SessionId,
        layer_id: LayerId,
    ) -> orbitrelay_canvas::CanvasDescriptor {
        orbitrelay_canvas::CanvasDescriptor::new(
            canvas_id,
            session_id,
            orbitrelay_canvas::CanvasSpace::new(1920.0, 1080.0).expect("space should be valid"),
            [layer_id.clone()],
            layer_id,
        )
        .expect("descriptor should be valid")
    }

    fn point(value: f64) -> CanvasPoint {
        CanvasPoint::new(value, value).expect("point should be finite")
    }

    fn style() -> StrokeStyle {
        StrokeStyle::new(2.0, RgbaColor::new(1, 2, 3, 255)).expect("style should be valid")
    }

    fn token_value<T: Serialize>(value: &T) -> serde_json::Value {
        serde_json::to_value(value).expect("token should encode")
    }

    #[tokio::test]
    async fn empty_history_completes_and_request_shape_is_strict() {
        let fixture = Fixture::new();
        let handler = fixture.handler(DEFAULT_HISTORY_STORE_SCAN_LIMIT, 1024 * 1024);
        let page = fixture
            .execute(&handler, json!({"canvas_id": fixture.canvas_id}))
            .await
            .expect("empty history should succeed");

        assert!(page.events().is_empty());
        assert!(page.complete());
        assert!(page.next_cursor().is_none());

        for invalid in [
            json!({"canvas_id": fixture.canvas_id, "checkpoint": "x"}),
            json!({"canvas_id": fixture.canvas_id, "cursor": "x"}),
            json!({"canvas_id": fixture.canvas_id, "page_size": 100}),
        ] {
            assert_eq!(
                handler
                    .execute(
                        &QueryActorContext::new(fixture.actor_id.clone()),
                        fixture.request(invalid),
                    )
                    .await,
                Err(QueryHandlerError::InvalidQuery)
            );
        }
    }

    #[tokio::test]
    async fn other_canvas_is_filtered_while_store_cursor_advances() {
        let fixture = Fixture::new();
        let other_event = fixture.event(
            STROKE_BEGAN_EVENT_TYPE,
            StrokeBeginPayload::new(
                fixture.other_canvas_id.clone(),
                fixture.layer_id.clone(),
                StrokeId::new(),
                StrokeTool::Pen,
                style(),
                0,
                [point(1.0)],
            )
            .expect("begin should be valid"),
        );
        fixture
            .store
            .append(other_event)
            .await
            .expect("other Canvas event should append");
        let target_event = fixture.event(
            STROKE_BEGAN_EVENT_TYPE,
            StrokeBeginPayload::new(
                fixture.canvas_id.clone(),
                fixture.layer_id.clone(),
                fixture.stroke_id.clone(),
                StrokeTool::Pen,
                style(),
                0,
                [point(2.0)],
            )
            .expect("begin should be valid"),
        );
        let target_id = target_event.id().clone();
        fixture
            .store
            .append(target_event)
            .await
            .expect("target event should append");
        let handler = fixture.handler(1, 1024 * 1024);

        let first = fixture
            .execute(&handler, json!({"canvas_id": fixture.canvas_id}))
            .await
            .expect("first page should succeed");
        assert!(first.events().is_empty());
        assert!(!first.complete());
        let second = fixture
            .execute(
                &handler,
                json!({
                    "canvas_id": fixture.canvas_id,
                    "checkpoint": token_value(first.checkpoint()),
                    "cursor": token_value(first.next_cursor().expect("continuation cursor")),
                }),
            )
            .await
            .expect("continuation should succeed");

        assert!(second.complete());
        assert_eq!(second.events().len(), 1);
        assert_eq!(second.events()[0].event_id(), &target_id);
        assert_eq!(second.checkpoint(), first.checkpoint());
    }

    #[tokio::test]
    async fn known_canvas_event_with_corrupted_payload_fails() {
        let fixture = Fixture::new();
        fixture
            .store
            .append(Event::new(
                EventId::new(),
                fixture.session_id.clone(),
                fixture.actor_id.clone(),
                ActionId::new(),
                EventType::new(STROKE_BEGAN_EVENT_TYPE),
                orbitrelay_core::Timestamp::now_utc(),
                Payload::new(),
                orbitrelay_core::Metadata::new(),
            ))
            .await
            .expect("corrupt fact is still storable");
        let handler = fixture.handler(DEFAULT_HISTORY_STORE_SCAN_LIMIT, 1024 * 1024);

        let result = handler
            .execute(
                &QueryActorContext::new(fixture.actor_id.clone()),
                fixture.request(json!({"canvas_id": fixture.canvas_id})),
            )
            .await;

        assert_eq!(result, Err(QueryHandlerError::Internal));
    }

    struct CountingStore {
        captures: AtomicUsize,
    }

    #[async_trait]
    impl EventStore for CountingStore {
        async fn capture_checkpoint(&self) -> Result<EventStoreCheckpoint, StorageError> {
            self.captures.fetch_add(1, Ordering::SeqCst);
            Err(StorageError::BackendUnavailable {
                message: "test".to_owned(),
            })
        }

        async fn append(&self, _event: Event) -> Result<StoredEvent, StorageError> {
            unreachable!("test store does not append")
        }

        async fn get(&self, _event_id: &EventId) -> Result<Option<StoredEvent>, StorageError> {
            unreachable!("test store does not get")
        }

        async fn query(&self, _query: EventQuery) -> Result<EventPage, StorageError> {
            unreachable!("authorization must happen before history query")
        }
    }

    #[tokio::test]
    async fn unauthorized_request_never_captures_a_checkpoint() {
        let fixture = Fixture::new();
        let store = Arc::new(CountingStore {
            captures: AtomicUsize::new(0),
        });
        let handler = CanvasHistoryQueryHandler::new(
            fixture.catalog.clone(),
            Arc::new(DevelopmentCanvasHistoryReadAuthorizer::new(SessionId::new())),
            store.clone(),
            DEFAULT_HISTORY_STORE_SCAN_LIMIT,
            1024 * 1024,
        )
        .expect("handler should be valid");

        let result = handler
            .execute(
                &QueryActorContext::new(fixture.actor_id.clone()),
                fixture.request(json!({"canvas_id": fixture.canvas_id})),
            )
            .await;

        assert_eq!(result, Err(QueryHandlerError::Unauthorized));
        assert_eq!(store.captures.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn authorized_backend_unavailable_maps_to_unavailable() {
        let fixture = Fixture::new();
        let store = Arc::new(CountingStore {
            captures: AtomicUsize::new(0),
        });
        let handler = CanvasHistoryQueryHandler::new(
            fixture.catalog.clone(),
            Arc::new(DevelopmentCanvasHistoryReadAuthorizer::new(
                fixture.session_id.clone(),
            )),
            store.clone(),
            DEFAULT_HISTORY_STORE_SCAN_LIMIT,
            1024 * 1024,
        )
        .expect("handler should be valid");

        let result = handler
            .execute(
                &QueryActorContext::new(fixture.actor_id.clone()),
                fixture.request(json!({"canvas_id": fixture.canvas_id})),
            )
            .await;

        assert_eq!(result, Err(QueryHandlerError::Unavailable));
        assert_eq!(store.captures.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn production_authorizer_fails_closed() {
        let fixture = Fixture::new();
        let handler = CanvasHistoryQueryHandler::new(
            fixture.catalog.clone(),
            Arc::new(RejectAllCanvasHistoryReadAuthorizer),
            fixture.store.clone(),
            DEFAULT_HISTORY_STORE_SCAN_LIMIT,
            1024 * 1024,
        )
        .expect("handler should be valid");

        let result = handler
            .execute(
                &QueryActorContext::new(fixture.actor_id.clone()),
                fixture.request(json!({"canvas_id": fixture.canvas_id})),
            )
            .await;

        assert_eq!(result, Err(QueryHandlerError::Unauthorized));
    }

    #[tokio::test]
    async fn unknown_canvas_is_not_found() {
        let fixture = Fixture::new();
        let handler = fixture.handler(DEFAULT_HISTORY_STORE_SCAN_LIMIT, 1024 * 1024);

        let result = handler
            .execute(
                &QueryActorContext::new(fixture.actor_id.clone()),
                fixture.request(json!({"canvas_id": CanvasId::new()})),
            )
            .await;

        assert_eq!(result, Err(QueryHandlerError::NotFound));
    }

    #[tokio::test]
    async fn wrong_store_continuation_is_invalid_query() {
        let fixture = Fixture::new();
        let foreign = MemoryEventStore::new();
        let foreign_record = foreign
            .append(fixture.event(
                STROKE_COMPLETED_EVENT_TYPE,
                StrokeEndPayload::new(fixture.canvas_id.clone(), fixture.stroke_id.clone(), 0),
            ))
            .await
            .expect("foreign append should succeed");
        let foreign_checkpoint = foreign
            .capture_checkpoint()
            .await
            .expect("foreign checkpoint should succeed");
        let handler = fixture.handler(DEFAULT_HISTORY_STORE_SCAN_LIMIT, 1024 * 1024);

        let result = handler
            .execute(
                &QueryActorContext::new(fixture.actor_id.clone()),
                fixture.request(json!({
                    "canvas_id": fixture.canvas_id,
                    "checkpoint": token_value(&foreign_checkpoint),
                    "cursor": token_value(foreign_record.cursor()),
                })),
            )
            .await;

        assert_eq!(result, Err(QueryHandlerError::InvalidQuery));
    }

    #[tokio::test]
    async fn default_scan_limit_fits_maximum_canvas_events_in_default_transport_message() {
        let fixture = Fixture::new();
        let points = (0..MAX_POINTS_PER_CHUNK)
            .map(|index| point(index as f64))
            .collect::<Vec<_>>();
        for chunk_index in 1..=DEFAULT_HISTORY_STORE_SCAN_LIMIT as u64 {
            fixture
                .store
                .append(
                    fixture.event(
                        STROKE_POINTS_APPENDED_EVENT_TYPE,
                        StrokeAppendPayload::new(
                            fixture.canvas_id.clone(),
                            fixture.stroke_id.clone(),
                            chunk_index,
                            points.clone(),
                        )
                        .expect("maximum append chunk should be valid"),
                    ),
                )
                .await
                .expect("maximum event should append");
        }
        let handler = fixture.handler(DEFAULT_HISTORY_STORE_SCAN_LIMIT, 1024 * 1024);

        let page = fixture
            .execute(&handler, json!({"canvas_id": fixture.canvas_id}))
            .await
            .expect("default maximum page should fit");
        let page_payload: Payload = serde_json::from_value(
            serde_json::to_value(&page).expect("history page should encode"),
        )
        .expect("history page should remain an object payload");
        let response = orbitrelay_query::QueryResponse::success(
            MessageId::new(),
            orbitrelay_query::QueryType::new(CANVAS_HISTORY_PAGE_QUERY_TYPE)
                .expect("query type should be valid"),
            page_payload,
        );
        let outbound = orbitrelay_transport::OutboundMessage::QueryResponse(
            orbitrelay_transport::QueryResponseMessage::from_response(
                orbitrelay_transport::QUERY_PROTOCOL_VERSION,
                response,
            ),
        );
        let encoded = orbitrelay_transport::MessageCodec::encode_outbound(
            &orbitrelay_transport::JsonCodec,
            &outbound,
        )
        .expect("maximum history response should encode");

        println!("max history QueryResponse wire bytes: {}", encoded.len());
        assert_eq!(page.events().len(), DEFAULT_HISTORY_STORE_SCAN_LIMIT);
        assert!(page.complete());
        assert!(encoded.len() < 1024 * 1024);
    }

    #[tokio::test]
    async fn response_size_failure_returns_no_cursor() {
        let fixture = Fixture::new();
        fixture
            .store
            .append(
                fixture.event(
                    STROKE_BEGAN_EVENT_TYPE,
                    StrokeBeginPayload::new(
                        fixture.canvas_id.clone(),
                        fixture.layer_id.clone(),
                        fixture.stroke_id.clone(),
                        StrokeTool::Pen,
                        style(),
                        0,
                        [point(1.0)],
                    )
                    .expect("begin should be valid"),
                ),
            )
            .await
            .expect("event should append");
        let handler = fixture.handler(1, 1);

        let result = handler
            .execute(
                &QueryActorContext::new(fixture.actor_id.clone()),
                fixture.request(json!({"canvas_id": fixture.canvas_id})),
            )
            .await;

        assert_eq!(result, Err(QueryHandlerError::Internal));
    }

    #[test]
    fn protocol_02_history_fixtures_decode_through_transport_and_dto_types() {
        for request in [
            include_bytes!("../../../tests/fixtures/v0.2/query_canvas_history_first.json")
                .as_slice(),
            include_bytes!("../../../tests/fixtures/v0.2/query_canvas_history_continue.json")
                .as_slice(),
        ] {
            let message: orbitrelay_transport::InboundMessage =
                serde_json::from_slice(request).expect("history request fixture should decode");
            assert!(matches!(
                message,
                orbitrelay_transport::InboundMessage::Query(_)
            ));
        }

        for response in [
            include_bytes!("../../../tests/fixtures/v0.2/query_response_canvas_history_page.json")
                .as_slice(),
            include_bytes!(
                "../../../tests/fixtures/v0.2/query_response_canvas_history_complete.json"
            )
            .as_slice(),
            include_bytes!("../../../tests/fixtures/v0.2/query_response_canvas_history_empty.json")
                .as_slice(),
            include_bytes!(
                "../../../tests/fixtures/v0.2/query_response_canvas_history_invalid_cursor.json"
            )
            .as_slice(),
        ] {
            let message: orbitrelay_transport::OutboundMessage =
                serde_json::from_slice(response).expect("history response fixture should decode");
            let orbitrelay_transport::OutboundMessage::QueryResponse(response) = message else {
                panic!("fixture should be a Query response")
            };
            if let orbitrelay_query::QueryResult::Success(payload) = response.result() {
                serde_json::from_value::<CanvasHistoryPageDto>(
                    serde_json::to_value(payload).expect("payload should encode"),
                )
                .expect("success fixture should match history DTO");
            }
        }
    }
}
