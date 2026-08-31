mod support;

use std::collections::HashSet;

use orbitrelay_canvas::{
    CanvasEventData, CanvasId, CanvasPoint, LayerId, RgbaColor, StrokeAppendPayload,
    StrokeBeginPayload, StrokeCancelPayload, StrokeEndPayload, StrokeId, StrokeLifecycle,
    StrokeProjector, StrokeRemovePayload, StrokeStyle, StrokeTool, STROKE_BEGAN_EVENT_TYPE,
    STROKE_CANCELLED_EVENT_TYPE, STROKE_COMPLETED_EVENT_TYPE, STROKE_POINTS_APPENDED_EVENT_TYPE,
    STROKE_REMOVED_EVENT_TYPE,
};
use orbitrelay_canvas_runtime::CanvasStateReader;
use orbitrelay_protocol::{ActorId, Event, EventType, MessageId, Payload, SessionId};
use orbitrelay_query::{QueryResult, QueryType};
use orbitrelay_server::{
    CanvasHistoryPageDto, DevelopmentCanvasConfig, EventStoreCanvasStateReader,
    CANVAS_HISTORY_PAGE_QUERY_TYPE,
};
use orbitrelay_storage::EventQuery;
use orbitrelay_transport::{
    Authenticate, Hello, InboundCredentials, InboundMessage, OutboundMessage, PingMessage,
    QueryMessage, SubscriptionRequest, QUERY_PROTOCOL_VERSION,
};
use serde::Serialize;
use serde_json::json;

use support::{development_config, next, send, RunningServer, TestClient};

fn canvas_event_types() -> [EventType; 5] {
    [
        EventType::new(STROKE_BEGAN_EVENT_TYPE),
        EventType::new(STROKE_POINTS_APPENDED_EVENT_TYPE),
        EventType::new(STROKE_COMPLETED_EVENT_TYPE),
        EventType::new(STROKE_CANCELLED_EVENT_TYPE),
        EventType::new(STROKE_REMOVED_EVENT_TYPE),
    ]
}

fn point(value: f64) -> CanvasPoint {
    CanvasPoint::new(value, value).expect("point should be finite")
}

fn style() -> StrokeStyle {
    StrokeStyle::new(2.0, RgbaColor::new(20, 40, 60, 255)).expect("style should be valid")
}

fn payload<T>(value: T) -> Payload
where
    Payload: TryFrom<T, Error = orbitrelay_canvas::CanvasError>,
{
    Payload::try_from(value).expect("Canvas payload should encode")
}

async fn send_canvas_action<T>(client: &mut TestClient, action_type: &str, value: T) -> Event
where
    Payload: TryFrom<T, Error = orbitrelay_canvas::CanvasError>,
{
    let action_id = client
        .send_action(
            orbitrelay_protocol::ActionType::new(action_type),
            payload(value),
        )
        .await;
    let (event_ids, event) = client.action_result(&action_id).await;
    assert_eq!(event_ids, [event.id().to_string()]);
    event
}

async fn connect_history_client(
    url: &str,
    actor_id: ActorId,
    session_id: SessionId,
) -> support::Socket {
    let mut socket = support::connect_socket(url).await;
    send(
        &mut socket,
        InboundMessage::Hello(Hello::new(
            vec![QUERY_PROTOCOL_VERSION],
            vec!["json".to_owned()],
        )),
    )
    .await;
    assert!(matches!(
        next(&mut socket).await,
        OutboundMessage::HelloAccepted(accepted)
            if accepted.selected_version() == QUERY_PROTOCOL_VERSION
    ));
    send(
        &mut socket,
        InboundMessage::Authenticate(Authenticate::new(
            MessageId::new(),
            InboundCredentials::new("development", actor_id.to_string()),
        )),
    )
    .await;
    send(
        &mut socket,
        InboundMessage::Subscribe(SubscriptionRequest::new(
            MessageId::new(),
            session_id,
            canvas_event_types(),
        )),
    )
    .await;
    assert!(matches!(
        next(&mut socket).await,
        OutboundMessage::SubscriptionAccepted(_)
    ));
    socket
}

async fn send_history_query(socket: &mut support::Socket, value: serde_json::Value) -> MessageId {
    let request_id = MessageId::new();
    let query_type = QueryType::new(CANVAS_HISTORY_PAGE_QUERY_TYPE).expect("query type is valid");
    let payload = serde_json::from_value(value).expect("query payload should be an object");
    send(
        socket,
        InboundMessage::Query(QueryMessage::new(
            QUERY_PROTOCOL_VERSION,
            request_id.clone(),
            query_type,
            payload,
        )),
    )
    .await;
    request_id
}

async fn receive_history_page(
    socket: &mut support::Socket,
    request_id: &MessageId,
) -> CanvasHistoryPageDto {
    loop {
        match next(socket).await {
            OutboundMessage::QueryResponse(response) if response.request_id() == request_id => {
                let QueryResult::Success(payload) = response.result() else {
                    panic!("history Query failed: {:?}", response.result())
                };
                return serde_json::from_value(
                    serde_json::to_value(payload).expect("history payload should encode"),
                )
                .expect("history page should decode");
            }
            OutboundMessage::Error(error) => panic!("unexpected transport error: {error:?}"),
            _ => {}
        }
    }
}

async fn receive_event(socket: &mut support::Socket) -> Event {
    loop {
        match next(socket).await {
            OutboundMessage::Event(envelope) => return envelope.into_payload(),
            OutboundMessage::Error(error) => panic!("unexpected transport error: {error:?}"),
            _ => {}
        }
    }
}

fn token<T: Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).expect("opaque token should encode")
}

fn dto_event(dto: &orbitrelay_server::HistoryEventDto) -> Event {
    Event::new(
        dto.event_id().clone(),
        dto.session_id().clone(),
        dto.actor_id().clone(),
        dto.action_id().clone(),
        dto.event_type().clone(),
        dto.occurred_at().clone(),
        dto.payload().clone(),
        dto.metadata().clone(),
    )
}

fn event_stroke_id(event: &Event) -> StrokeId {
    match CanvasEventData::try_from(event).expect("history event should remain typed") {
        CanvasEventData::StrokeBegan(value) => value.stroke_id().clone(),
        CanvasEventData::StrokePointsAppended(value) => value.stroke_id().clone(),
        CanvasEventData::StrokeCompleted(value) => value.stroke_id().clone(),
        CanvasEventData::StrokeCancelled(value) => value.stroke_id().clone(),
        CanvasEventData::StrokeRemoved(value) => value.stroke_id().clone(),
        _ => panic!("test only uses current Canvas events"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_first_history_and_realtime_handoff_is_gapless() {
    let session_id = SessionId::new();
    let canvas_id = CanvasId::new();
    let layer_id = LayerId::new();
    let config = development_config()
        .with_history_store_scan_limit(2)
        .with_development_canvas(
            DevelopmentCanvasConfig::new()
                .with_session_id(session_id.clone())
                .with_canvas_id(canvas_id.clone())
                .with_layer_id(layer_id.clone()),
        );
    let server = RunningServer::start(config).await;
    let mut producer = TestClient::connect_with_events(
        &server.url,
        ActorId::new(),
        session_id.clone(),
        canvas_event_types(),
    )
    .await;

    let completed_stroke = StrokeId::new();
    let cancelled_stroke = StrokeId::new();
    let long_stroke = StrokeId::new();
    let mut pre_subscription = Vec::new();
    pre_subscription.push(
        send_canvas_action(
            &mut producer,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas_id.clone(),
                layer_id.clone(),
                completed_stroke.clone(),
                StrokeTool::Pen,
                style(),
                0,
                [point(1.0)],
            )
            .expect("begin should be valid"),
        )
        .await,
    );
    pre_subscription.push(
        send_canvas_action(
            &mut producer,
            "canvas.stroke.append",
            StrokeAppendPayload::new(canvas_id.clone(), completed_stroke.clone(), 1, [point(2.0)])
                .expect("append should be valid"),
        )
        .await,
    );
    pre_subscription.push(
        send_canvas_action(
            &mut producer,
            "canvas.stroke.end",
            StrokeEndPayload::new(canvas_id.clone(), completed_stroke.clone(), 1),
        )
        .await,
    );
    pre_subscription.push(
        send_canvas_action(
            &mut producer,
            "canvas.stroke.remove",
            StrokeRemovePayload::new(canvas_id.clone(), completed_stroke.clone()),
        )
        .await,
    );
    pre_subscription.push(
        send_canvas_action(
            &mut producer,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas_id.clone(),
                layer_id.clone(),
                cancelled_stroke.clone(),
                StrokeTool::Pen,
                style(),
                0,
                [point(3.0)],
            )
            .expect("begin should be valid"),
        )
        .await,
    );
    pre_subscription.push(
        send_canvas_action(
            &mut producer,
            "canvas.stroke.cancel",
            StrokeCancelPayload::new(canvas_id.clone(), cancelled_stroke.clone(), 0),
        )
        .await,
    );
    pre_subscription.push(
        send_canvas_action(
            &mut producer,
            "canvas.stroke.begin",
            StrokeBeginPayload::new(
                canvas_id.clone(),
                layer_id.clone(),
                long_stroke.clone(),
                StrokeTool::Pen,
                style(),
                0,
                [point(4.0)],
            )
            .expect("begin should be valid"),
        )
        .await,
    );
    for index in 1..=2 {
        pre_subscription.push(
            send_canvas_action(
                &mut producer,
                "canvas.stroke.append",
                StrokeAppendPayload::new(
                    canvas_id.clone(),
                    long_stroke.clone(),
                    index,
                    [point(4.0 + index as f64)],
                )
                .expect("append should be valid"),
            )
            .await,
        );
    }
    pre_subscription.push(
        send_canvas_action(
            &mut producer,
            "canvas.stroke.end",
            StrokeEndPayload::new(canvas_id.clone(), long_stroke.clone(), 2),
        )
        .await,
    );

    let mut replay = connect_history_client(&server.url, ActorId::new(), session_id.clone()).await;
    let overlap_stroke = StrokeId::new();
    let overlap = send_canvas_action(
        &mut producer,
        "canvas.stroke.begin",
        StrokeBeginPayload::new(
            canvas_id.clone(),
            layer_id,
            overlap_stroke.clone(),
            StrokeTool::Pen,
            style(),
            0,
            [point(10.0)],
        )
        .expect("begin should be valid"),
    )
    .await;
    let buffered_overlap = receive_event(&mut replay).await;
    assert_eq!(buffered_overlap, overlap);

    let first_request = send_history_query(&mut replay, json!({"canvas_id": canvas_id})).await;
    let first = receive_history_page(&mut replay, &first_request).await;
    assert!(!first.complete());

    let after = send_canvas_action(
        &mut producer,
        "canvas.stroke.append",
        StrokeAppendPayload::new(canvas_id.clone(), overlap_stroke.clone(), 1, [point(11.0)])
            .expect("append should be valid"),
    )
    .await;
    let buffered_after = receive_event(&mut replay).await;
    assert_eq!(buffered_after, after);
    let realtime_buffer = vec![buffered_overlap, buffered_after];

    let checkpoint = first.checkpoint().clone();
    let mut history = first.events().iter().map(dto_event).collect::<Vec<_>>();
    let mut next_cursor = first.next_cursor().cloned();
    let mut complete = first.complete();
    let mut sent_ping = false;
    let mut saw_pong = false;
    while !complete {
        let cursor = next_cursor.take().expect("incomplete page has a cursor");
        let request_id = send_history_query(
            &mut replay,
            json!({
                "canvas_id": canvas_id,
                "checkpoint": token(&checkpoint),
                "cursor": token(&cursor),
            }),
        )
        .await;
        if !sent_ping {
            send(&mut replay, InboundMessage::Ping(PingMessage::new(12_606))).await;
            sent_ping = true;
        }
        let page = loop {
            match next(&mut replay).await {
                OutboundMessage::QueryResponse(response)
                    if response.request_id() == &request_id =>
                {
                    let QueryResult::Success(payload) = response.result() else {
                        panic!("continuation failed: {:?}", response.result())
                    };
                    break serde_json::from_value::<CanvasHistoryPageDto>(
                        serde_json::to_value(payload).expect("history payload should encode"),
                    )
                    .expect("history page should decode");
                }
                OutboundMessage::Pong(pong) => {
                    assert_eq!(pong.nonce(), 12_606);
                    saw_pong = true;
                }
                other => panic!("unexpected message during history pagination: {other:?}"),
            }
        };
        assert_eq!(page.checkpoint(), &checkpoint);
        history.extend(page.events().iter().map(dto_event));
        complete = page.complete();
        next_cursor = page.next_cursor().cloned();
    }
    while !saw_pong {
        match next(&mut replay).await {
            OutboundMessage::Pong(pong) => {
                assert_eq!(pong.nonce(), 12_606);
                saw_pong = true;
            }
            other => panic!("unexpected message while awaiting Pong: {other:?}"),
        }
    }

    let history_ids = history
        .iter()
        .map(|event| event.id().clone())
        .collect::<Vec<_>>();
    let expected_history_ids = pre_subscription
        .iter()
        .chain(std::iter::once(&overlap))
        .map(|event| event.id().clone())
        .collect::<Vec<_>>();
    assert_eq!(history_ids, expected_history_ids);
    assert!(history_ids.contains(overlap.id()));
    assert!(!history_ids.contains(after.id()));
    assert_eq!(
        realtime_buffer
            .iter()
            .map(|event| event.id())
            .collect::<Vec<_>>(),
        vec![overlap.id(), after.id()]
    );

    let mut seen = HashSet::new();
    let mut applied = Vec::new();
    for event in history.into_iter().chain(realtime_buffer) {
        if seen.insert(event.id().clone()) {
            applied.push(event);
        }
    }
    let stored = server
        .context
        .event_store()
        .query(EventQuery::for_session(session_id.clone()))
        .await
        .expect("store query should succeed");
    let stored_ids = stored
        .events()
        .iter()
        .map(|record| record.event().id().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        applied
            .iter()
            .map(|event| event.id().clone())
            .collect::<Vec<_>>(),
        stored_ids
    );
    assert_eq!(seen.len(), applied.len());

    let reader = EventStoreCanvasStateReader::new(server.context.event_store_arc());
    for (stroke_id, lifecycle) in [
        (completed_stroke, StrokeLifecycle::Removed),
        (cancelled_stroke, StrokeLifecycle::Cancelled),
        (long_stroke, StrokeLifecycle::Completed),
        (overlap_stroke, StrokeLifecycle::Active),
    ] {
        let mut local = None;
        for event in applied
            .iter()
            .filter(|event| event_stroke_id(event) == stroke_id)
        {
            local = Some(
                StrokeProjector::apply(local, event)
                    .expect("deduped history/realtime should project"),
            );
        }
        let local = local.expect("Stroke should have replayed events");
        let authoritative = reader
            .load_stroke(&session_id, &canvas_id, &stroke_id)
            .await
            .expect("Store projection should succeed")
            .expect("Stroke should exist");
        assert_eq!(local, authoritative);
        assert_eq!(local.lifecycle(), lifecycle);
    }

    producer.close().await;
    let _ = replay.close(None).await;
    server.shutdown().await;
}
