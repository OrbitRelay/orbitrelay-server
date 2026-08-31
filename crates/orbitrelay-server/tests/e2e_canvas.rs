mod support;

use std::time::Duration;

use orbitrelay_canvas::{
    CanvasPoint, RgbaColor, StrokeAppendPayload, StrokeBeginPayload, StrokeEndPayload, StrokeStyle,
    StrokeTool, STROKE_BEGAN_EVENT_TYPE, STROKE_COMPLETED_EVENT_TYPE,
    STROKE_POINTS_APPENDED_EVENT_TYPE,
};
use orbitrelay_canvas_runtime::CanvasStateReader;
use orbitrelay_core::{Metadata, Timestamp};
use orbitrelay_protocol::{
    Action, ActionId, ActionType, ActorId, Event, EventType, MessageEnvelope, MessageId,
    MessageType, Payload, SessionId,
};
use orbitrelay_server::DevelopmentCanvasConfig;
use orbitrelay_storage::EventQuery;
use orbitrelay_transport::{
    InboundMessage, OutboundMessage, TransportErrorCode, CURRENT_PROTOCOL_VERSION,
};
use support::{development_config, next, send, RunningServer, TestClient};

fn canvas_event_types() -> [EventType; 5] {
    [
        EventType::new(STROKE_BEGAN_EVENT_TYPE),
        EventType::new(STROKE_POINTS_APPENDED_EVENT_TYPE),
        EventType::new("canvas.stroke.completed"),
        EventType::new("canvas.stroke.cancelled"),
        EventType::new("canvas.stroke.removed"),
    ]
}

fn point(x: f64, y: f64) -> CanvasPoint {
    CanvasPoint::new(x, y).expect("finite point")
}

fn payload<T>(value: T) -> Payload
where
    Payload: TryFrom<T, Error = orbitrelay_canvas::CanvasError>,
{
    Payload::try_from(value).expect("Canvas payload should encode")
}

async fn send_action(client: &mut TestClient, action_type: &str, payload: Payload) -> ActionId {
    client
        .send_action(ActionType::new(action_type), payload)
        .await
}

async fn ack(
    client: &mut TestClient,
    action_id: &ActionId,
) -> (Vec<orbitrelay_protocol::EventId>, Option<Event>) {
    let mut ack = None;
    let mut event = None;
    while ack.is_none()
        || (event.is_none() && ack.as_ref().is_some_and(|ids: &Vec<_>| !ids.is_empty()))
    {
        match next(&mut client.socket).await {
            OutboundMessage::ActionAcknowledgement(message) if message.action_id() == action_id => {
                ack = Some(message.generated_event_ids().to_vec());
            }
            OutboundMessage::Event(envelope) if envelope.payload().action_id() == action_id => {
                event = Some(envelope.into_payload());
            }
            OutboundMessage::Error(error) if error.request_id().is_some() => {
                panic!("unexpected action error: {:?}", error.code());
            }
            _ => {}
        }
    }
    (ack.expect("acknowledgement"), event)
}

async fn action_outcome(
    client: &mut TestClient,
    action_id: &ActionId,
) -> Result<usize, TransportErrorCode> {
    loop {
        match next(&mut client.socket).await {
            OutboundMessage::ActionAcknowledgement(message) if message.action_id() == action_id => {
                return Ok(message.generated_event_ids().len());
            }
            OutboundMessage::Error(error) => return Err(error.code()),
            _ => {}
        }
    }
}

fn action_for(
    session_id: SessionId,
    actor_id: ActorId,
    action_type: &str,
    payload: Payload,
) -> Action {
    Action::new(
        ActionId::new(),
        session_id,
        actor_id,
        ActionType::new(action_type),
        Timestamp::now_utc(),
        payload,
        Metadata::new(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clients_receive_complete_canvas_stroke_and_retries_are_noops() {
    let canvas_id = orbitrelay_canvas::CanvasId::new();
    let layer_id = orbitrelay_canvas::LayerId::new();
    let session_id = SessionId::new();
    let config = development_config().with_development_canvas(
        DevelopmentCanvasConfig::new()
            .with_session_id(session_id.clone())
            .with_canvas_id(canvas_id.clone())
            .with_layer_id(layer_id.clone()),
    );
    let server = RunningServer::start(config).await;
    let actor_a = ActorId::new();
    let mut client_a = TestClient::connect_with_events(
        &server.url,
        actor_a.clone(),
        session_id.clone(),
        canvas_event_types(),
    )
    .await;
    let mut client_b = TestClient::connect_with_events(
        &server.url,
        ActorId::new(),
        session_id.clone(),
        canvas_event_types(),
    )
    .await;
    let stroke_id = orbitrelay_canvas::StrokeId::new();
    let style = StrokeStyle::new(2.0, RgbaColor::new(10, 20, 30, 255)).unwrap();

    let begin_payload = StrokeBeginPayload::new(
        canvas_id.clone(),
        layer_id.clone(),
        stroke_id.clone(),
        StrokeTool::Pen,
        style.clone(),
        0,
        [point(1.0, 1.0)],
    )
    .unwrap();
    let begin_id = send_action(
        &mut client_a,
        "canvas.stroke.begin",
        payload(begin_payload.clone()),
    )
    .await;
    let (ids, event) = ack(&mut client_a, &begin_id).await;
    assert_eq!(ids.len(), 1);
    let event = event.expect("begin should produce an Event");
    assert_eq!(event.event_type().as_str(), STROKE_BEGAN_EVENT_TYPE);
    assert_eq!(event.session_id(), &session_id);
    assert_eq!(event.actor_id(), &actor_a);
    assert_eq!(event.action_id(), &begin_id);
    assert_eq!(
        StrokeBeginPayload::try_from(event.payload()).expect("begin Event payload"),
        begin_payload
    );
    assert_eq!(client_b.next_event().await, event);

    for index in 1..=2 {
        let append = StrokeAppendPayload::new(
            canvas_id.clone(),
            stroke_id.clone(),
            index,
            [point(index as f64, 1.0)],
        )
        .unwrap();
        let id = send_action(&mut client_a, "canvas.stroke.append", payload(append)).await;
        let (ids, event) = ack(&mut client_a, &id).await;
        assert_eq!(ids.len(), 1);
        assert_eq!(
            event.unwrap().event_type().as_str(),
            STROKE_POINTS_APPENDED_EVENT_TYPE
        );
        assert_eq!(
            client_b.next_event().await.event_type().as_str(),
            STROKE_POINTS_APPENDED_EVENT_TYPE
        );
    }

    let end = StrokeEndPayload::new(canvas_id.clone(), stroke_id.clone(), 2);
    let end_id = send_action(&mut client_a, "canvas.stroke.end", payload(end.clone())).await;
    let (ids, event) = ack(&mut client_a, &end_id).await;
    assert_eq!(ids.len(), 1);
    assert_eq!(
        event.unwrap().event_type().as_str(),
        STROKE_COMPLETED_EVENT_TYPE
    );
    assert_eq!(
        client_b.next_event().await.event_type().as_str(),
        STROKE_COMPLETED_EVENT_TYPE
    );

    let events = server
        .context
        .event_store()
        .query(EventQuery::for_session(session_id.clone()))
        .await
        .unwrap();
    assert_eq!(events.len(), 4);
    let reader =
        orbitrelay_server::EventStoreCanvasStateReader::new(server.context.event_store_arc());
    let projection = reader
        .load_stroke(&session_id, &canvas_id, &stroke_id)
        .await
        .expect("StateReader should rebuild the Stroke")
        .expect("Stroke should exist");
    assert_eq!(projection.chunks().len(), 3);
    assert_eq!(projection.last_chunk_index(), 2);
    assert_eq!(
        projection.lifecycle(),
        orbitrelay_canvas::StrokeLifecycle::Completed
    );

    let retry_begin_id =
        send_action(&mut client_a, "canvas.stroke.begin", payload(begin_payload)).await;
    let (retry_ids, retry_event) = ack(&mut client_a, &retry_begin_id).await;
    assert!(retry_ids.is_empty());
    assert!(retry_event.is_none());
    let retry_append =
        StrokeAppendPayload::new(canvas_id.clone(), stroke_id.clone(), 1, [point(1.0, 1.0)])
            .unwrap();
    let retry_append_id =
        send_action(&mut client_a, "canvas.stroke.append", payload(retry_append)).await;
    let (retry_ids, retry_event) = ack(&mut client_a, &retry_append_id).await;
    assert!(retry_ids.is_empty());
    assert!(retry_event.is_none());
    let retry_end_id = send_action(&mut client_a, "canvas.stroke.end", payload(end)).await;
    let (retry_ids, retry_event) = ack(&mut client_a, &retry_end_id).await;
    assert!(retry_ids.is_empty());
    assert!(retry_event.is_none());
    assert_eq!(
        server
            .context
            .event_store()
            .query(EventQuery::for_session(session_id))
            .await
            .unwrap()
            .len(),
        4
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), client_b.next_event())
            .await
            .is_err()
    );

    client_a.close().await;
    client_b.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_identical_new_chunk_produces_one_fact_and_two_successes() {
    let session_id = SessionId::new();
    let canvas_id = orbitrelay_canvas::CanvasId::new();
    let layer_id = orbitrelay_canvas::LayerId::new();
    let server = RunningServer::start(
        development_config().with_development_canvas(
            DevelopmentCanvasConfig::new()
                .with_session_id(session_id.clone())
                .with_canvas_id(canvas_id.clone())
                .with_layer_id(layer_id.clone()),
        ),
    )
    .await;
    let mut first = TestClient::connect_with_events(
        &server.url,
        ActorId::new(),
        session_id.clone(),
        canvas_event_types(),
    )
    .await;
    let mut second = TestClient::connect_with_events(
        &server.url,
        ActorId::new(),
        session_id.clone(),
        canvas_event_types(),
    )
    .await;
    let stroke_id = orbitrelay_canvas::StrokeId::new();
    let begin = StrokeBeginPayload::new(
        canvas_id.clone(),
        layer_id,
        stroke_id.clone(),
        StrokeTool::Pen,
        StrokeStyle::new(1.0, RgbaColor::new(0, 0, 0, 255)).unwrap(),
        0,
        [point(1.0, 1.0)],
    )
    .unwrap();
    let begin_id = send_action(&mut first, "canvas.stroke.begin", payload(begin)).await;
    let _ = ack(&mut first, &begin_id).await;
    let _ = second.next_event().await;

    let append =
        payload(StrokeAppendPayload::new(canvas_id, stroke_id, 1, [point(2.0, 2.0)]).unwrap());
    let first_action = action_for(
        session_id.clone(),
        first.actor_id.clone(),
        "canvas.stroke.append",
        append.clone(),
    );
    let second_action = action_for(
        session_id.clone(),
        second.actor_id.clone(),
        "canvas.stroke.append",
        append,
    );
    let first_id = first_action.id().clone();
    let second_id = second_action.id().clone();
    send(
        &mut first.socket,
        InboundMessage::Action(MessageEnvelope::new(
            CURRENT_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            first_action,
        )),
    )
    .await;
    send(
        &mut second.socket,
        InboundMessage::Action(MessageEnvelope::new(
            CURRENT_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            second_action,
        )),
    )
    .await;

    let outcomes = [
        action_outcome(&mut first, &first_id).await,
        action_outcome(&mut second, &second_id).await,
    ];
    assert_eq!(
        outcomes.iter().filter(|result| **result == Ok(1)).count(),
        1
    );
    assert_eq!(
        outcomes.iter().filter(|result| **result == Ok(0)).count(),
        1
    );
    assert_eq!(
        server
            .context
            .event_store()
            .query(EventQuery::for_session(session_id))
            .await
            .unwrap()
            .len(),
        2
    );

    first.close().await;
    second.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_rejects_canvas_session_layer_and_bounds_violations_without_facts() {
    let session_id = SessionId::new();
    let other_session_id = SessionId::new();
    let canvas_id = orbitrelay_canvas::CanvasId::new();
    let layer_id = orbitrelay_canvas::LayerId::new();
    let server = RunningServer::start(
        development_config().with_development_canvas(
            DevelopmentCanvasConfig::new()
                .with_session_id(session_id.clone())
                .with_canvas_id(canvas_id.clone())
                .with_layer_id(layer_id.clone())
                .with_size(100.0, 100.0),
        ),
    )
    .await;
    let mut correct_session = TestClient::connect_with_events(
        &server.url,
        ActorId::new(),
        session_id.clone(),
        canvas_event_types(),
    )
    .await;
    let mut wrong_session = TestClient::connect_with_events(
        &server.url,
        ActorId::new(),
        other_session_id.clone(),
        canvas_event_types(),
    )
    .await;
    let style = StrokeStyle::new(1.0, RgbaColor::new(0, 0, 0, 255)).unwrap();

    let unknown_layer = StrokeBeginPayload::new(
        canvas_id.clone(),
        orbitrelay_canvas::LayerId::new(),
        orbitrelay_canvas::StrokeId::new(),
        StrokeTool::Pen,
        style.clone(),
        0,
        [point(10.0, 10.0)],
    )
    .unwrap();
    let action_id = send_action(
        &mut correct_session,
        "canvas.stroke.begin",
        payload(unknown_layer),
    )
    .await;
    assert_eq!(
        action_outcome(&mut correct_session, &action_id).await,
        Err(TransportErrorCode::ExecutionRejected)
    );

    let out_of_bounds = StrokeBeginPayload::new(
        canvas_id.clone(),
        layer_id.clone(),
        orbitrelay_canvas::StrokeId::new(),
        StrokeTool::Pen,
        style.clone(),
        0,
        [point(101.0, 10.0)],
    )
    .unwrap();
    let action_id = send_action(
        &mut correct_session,
        "canvas.stroke.begin",
        payload(out_of_bounds),
    )
    .await;
    assert_eq!(
        action_outcome(&mut correct_session, &action_id).await,
        Err(TransportErrorCode::ExecutionRejected)
    );

    let session_mismatch = StrokeBeginPayload::new(
        canvas_id,
        layer_id,
        orbitrelay_canvas::StrokeId::new(),
        StrokeTool::Pen,
        style,
        0,
        [point(10.0, 10.0)],
    )
    .unwrap();
    let action_id = send_action(
        &mut wrong_session,
        "canvas.stroke.begin",
        payload(session_mismatch),
    )
    .await;
    assert_eq!(
        action_outcome(&mut wrong_session, &action_id).await,
        Err(TransportErrorCode::ExecutionRejected)
    );

    assert!(server
        .context
        .event_store()
        .query(EventQuery::for_session(session_id))
        .await
        .unwrap()
        .is_empty());
    assert!(server
        .context
        .event_store()
        .query(EventQuery::for_session(other_session_id))
        .await
        .unwrap()
        .is_empty());

    correct_session.close().await;
    wrong_session.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_stroke_chunk_has_one_fact_and_identical_retry_is_successful() {
    let session_id = SessionId::new();
    let canvas_id = orbitrelay_canvas::CanvasId::new();
    let layer_id = orbitrelay_canvas::LayerId::new();
    let server = RunningServer::start(
        development_config().with_development_canvas(
            DevelopmentCanvasConfig::new()
                .with_session_id(session_id.clone())
                .with_canvas_id(canvas_id.clone())
                .with_layer_id(layer_id.clone()),
        ),
    )
    .await;
    let mut first = TestClient::connect_with_events(
        &server.url,
        ActorId::new(),
        session_id.clone(),
        canvas_event_types(),
    )
    .await;
    let mut second = TestClient::connect_with_events(
        &server.url,
        ActorId::new(),
        session_id.clone(),
        canvas_event_types(),
    )
    .await;
    let stroke_id = orbitrelay_canvas::StrokeId::new();
    let begin = StrokeBeginPayload::new(
        canvas_id.clone(),
        layer_id,
        stroke_id.clone(),
        StrokeTool::Pen,
        StrokeStyle::new(1.0, RgbaColor::new(0, 0, 0, 255)).unwrap(),
        0,
        [point(1.0, 1.0)],
    )
    .unwrap();
    let begin_id = send_action(&mut first, "canvas.stroke.begin", payload(begin)).await;
    let _ = ack(&mut first, &begin_id).await;
    let _ = second.next_event().await;

    let append_a = payload(
        StrokeAppendPayload::new(canvas_id.clone(), stroke_id.clone(), 1, [point(2.0, 2.0)])
            .unwrap(),
    );
    let append_b = payload(
        StrokeAppendPayload::new(canvas_id.clone(), stroke_id.clone(), 1, [point(3.0, 3.0)])
            .unwrap(),
    );
    let action_a = action_for(
        session_id.clone(),
        first.actor_id.clone(),
        "canvas.stroke.append",
        append_a,
    );
    let action_b = action_for(
        session_id.clone(),
        second.actor_id.clone(),
        "canvas.stroke.append",
        append_b,
    );
    let id_a = action_a.id().clone();
    let id_b = action_b.id().clone();
    send(
        &mut first.socket,
        InboundMessage::Action(MessageEnvelope::new(
            CURRENT_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            action_a,
        )),
    )
    .await;
    send(
        &mut second.socket,
        InboundMessage::Action(MessageEnvelope::new(
            CURRENT_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            action_b,
        )),
    )
    .await;
    let outcome_a = action_outcome(&mut first, &id_a).await;
    let outcome_b = action_outcome(&mut second, &id_b).await;
    assert_eq!(
        [outcome_a, outcome_b]
            .iter()
            .filter(|result| **result == Ok(1))
            .count(),
        1
    );
    assert_eq!(
        server
            .context
            .event_store()
            .query(EventQuery::for_session(session_id.clone()))
            .await
            .unwrap()
            .len(),
        2
    );

    let stored = server
        .context
        .event_store()
        .query(EventQuery::for_session(session_id.clone()))
        .await
        .unwrap();
    let winning_append = stored
        .events()
        .iter()
        .find(|record| record.event().event_type().as_str() == STROKE_POINTS_APPENDED_EVENT_TYPE)
        .expect("one append fact should exist");
    let winning_payload = StrokeAppendPayload::try_from(winning_append.event().payload())
        .expect("stored append payload should decode");
    let retry_payload = payload(
        StrokeAppendPayload::new(
            winning_payload.canvas_id().clone(),
            winning_payload.stroke_id().clone(),
            winning_payload.chunk_index(),
            winning_payload.points().iter().copied(),
        )
        .unwrap(),
    );
    let retry_a = action_for(
        session_id.clone(),
        first.actor_id.clone(),
        "canvas.stroke.append",
        retry_payload.clone(),
    );
    let retry_b = action_for(
        session_id.clone(),
        second.actor_id.clone(),
        "canvas.stroke.append",
        retry_payload,
    );
    let retry_id_a = retry_a.id().clone();
    let retry_id_b = retry_b.id().clone();
    send(
        &mut first.socket,
        InboundMessage::Action(MessageEnvelope::new(
            CURRENT_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            retry_a,
        )),
    )
    .await;
    send(
        &mut second.socket,
        InboundMessage::Action(MessageEnvelope::new(
            CURRENT_PROTOCOL_VERSION,
            MessageId::new(),
            MessageType::new("action"),
            retry_b,
        )),
    )
    .await;
    for (client, id) in [(&mut first, retry_id_a), (&mut second, retry_id_b)] {
        loop {
            match next(&mut client.socket).await {
                OutboundMessage::ActionAcknowledgement(message) if message.action_id() == &id => {
                    assert!(message.generated_event_ids().is_empty());
                    break;
                }
                OutboundMessage::Error(error) => panic!("retry should succeed: {:?}", error.code()),
                _ => {}
            }
        }
    }
    assert_eq!(
        server
            .context
            .event_store()
            .query(EventQuery::for_session(session_id))
            .await
            .unwrap()
            .len(),
        2
    );

    first.close().await;
    second.close().await;
    server.shutdown().await;
}
