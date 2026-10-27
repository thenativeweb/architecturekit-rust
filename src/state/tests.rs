// These tests mirror TestReplay, TestSchemas, TestHasRule, TestStep,
// TestStepStored, TestReplayWithFromLatest, TestReplayStoredWithFromLatest,
// TestFromLatest and TestIgnore of ArchitectureKit for Go. What Go checks while
// a state is being built does not apply to Rust, where the types make it a
// given, or the compiler checks it:
//
// - TestEvolve: a nil function, a pointer or an interface as the event type,
//   and chaining. Two rules for the same event type are two variants of the
//   same event type, which fail to compile (see the refusals).
// - "fails on unknown event type" and "fails on event without rule" for
//   Replay: a typed event is always one of the variants. Both are checked for
//   replay_stored, where they can happen.
// - TestStep "refuses a state that it cannot copy", and TestInitialValue: a
//   state is Clone, every read starts from a fresh Default, and step takes
//   the given state by reference.
// - TestFromLatest: a pointer or an interface as the event type, and calling
//   it twice. TestIgnore: an ignored event is a variant whose arm returns the
//   state unchanged, so its data is decoded, and there is nothing to panic on.
//
// The tests that need a database come with the store.

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    Event, EventSchema, Events, State, UpcastEvent, replay, replay_stored, step, step_stored,
};

#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.incremented")]
struct Incremented {
    by: i64,
}

#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.reset")]
struct Reset {}

#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.annotated")]
struct Annotated {
    note: String,
}

#[derive(Events)]
enum CounterEvent {
    Incremented(Incremented),
    Reset(Reset),
    // It belongs to the subject, but matters for no decision.
    Annotated(Annotated),
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Counter {
    total: i64,
}

impl State for Counter {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        match event {
            CounterEvent::Incremented(incremented) => Counter {
                total: self.total + incremented.by,
            },
            CounterEvent::Reset(_) => Counter { total: 0 },
            CounterEvent::Annotated(_) => self,
        }
    }
}

/// A counter that starts from the latest reset. The rule for a reset sets the
/// total to zero, whatever it was before, so the events before the latest
/// reset do not matter for the state.
#[derive(Clone, Debug, Default, PartialEq)]
struct CounterFromLatest(Counter);

impl State for CounterFromLatest {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        CounterFromLatest(self.0.evolve(event))
    }

    fn from_latest() -> Option<&'static str> {
        Some(Reset::EVENT_TYPE)
    }
}

/// A counter that claims to start from an event type it has no rule for.
#[derive(Clone, Debug, Default, PartialEq)]
struct CounterFromUnheardOf(Counter);

impl State for CounterFromUnheardOf {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        CounterFromUnheardOf(self.0.evolve(event))
    }

    fn from_latest() -> Option<&'static str> {
        Some("io.thenativeweb.test.unheard-of")
    }
}

#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.measured")]
struct Measured {
    value: f64,
}

#[derive(Events)]
enum GaugeEvent {
    Measured(Measured),
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Gauge {
    last: f64,
}

impl State for Gauge {
    type Event = GaugeEvent;

    fn evolve(self, event: &GaugeEvent) -> Self {
        match event {
            GaugeEvent::Measured(measured) => Gauge {
                last: measured.value,
            },
        }
    }
}

fn incremented(by: i64) -> CounterEvent {
    CounterEvent::Incremented(Incremented { by })
}

#[test]
fn returns_the_initial_state_for_an_empty_history() {
    let counter: Counter = replay([]).expect("an empty history replays");

    assert_eq!(counter, Counter::default());
}

#[test]
fn folds_events_in_order() {
    let counter: Counter = replay([
        incremented(1),
        incremented(2),
        CounterEvent::Reset(Reset {}),
        incremented(4),
    ])
    .expect("the history replays");

    assert_eq!(counter.total, 4);
}

#[test]
fn fails_on_an_event_that_does_not_survive_being_written_and_read_back() {
    // Go refuses to encode NaN; serde writes it as null, which can not be read
    // back as a number. Either way, replaying fails as reading would.
    let failure = replay::<Gauge>([GaugeEvent::Measured(Measured { value: f64::NAN })])
        .expect_err("NaN does not survive JSON");

    assert!(failure.to_string().contains("decoding"), "{failure}");
}

#[test]
fn fails_on_data_that_does_not_match_the_rule() {
    let failure = replay_stored::<Counter>([UpcastEvent::new(
        Incremented::EVENT_TYPE,
        json!({ "by": "three" }),
    )])
    .expect_err("a string is no number");

    assert!(failure.to_string().contains("decoding"), "{failure}");
}

#[test]
fn fails_on_an_event_without_rule() {
    let failure = replay_stored::<Counter>([UpcastEvent::new(
        "io.thenativeweb.test.unheard-of",
        json!({}),
    )])
    .expect_err("the state has no rule for it");

    assert_eq!(
        failure.to_string(),
        r#"permanent failure: no rule for event type "io.thenativeweb.test.unheard-of""#,
    );
}

#[test]
fn collects_the_schema_of_every_event() {
    assert_eq!(
        CounterEvent::schemas(),
        vec![
            EventSchema::of::<Incremented>(),
            EventSchema::of::<Reset>(),
            EventSchema::of::<Annotated>(),
        ],
    );
    assert_eq!(
        CounterEvent::EVENT_TYPES,
        [
            "io.thenativeweb.test.incremented",
            "io.thenativeweb.test.reset",
            "io.thenativeweb.test.annotated",
        ],
    );
}

#[test]
fn has_a_rule_for_every_variant_and_none_for_other_types() {
    assert!(CounterEvent::has_rule(Incremented::EVENT_TYPE));
    assert!(
        CounterEvent::has_rule(Annotated::EVENT_TYPE),
        "an ignored event is a variant too"
    );
    assert!(!CounterEvent::has_rule("io.thenativeweb.test.unheard-of"));
}

#[test]
fn names_the_event_type_of_an_event() {
    assert_eq!(incremented(1).event_type(), Incremented::EVENT_TYPE);
    assert_eq!(
        CounterEvent::Reset(Reset {}).event_type(),
        Reset::EVENT_TYPE
    );
}

#[test]
fn takes_an_ignored_event_without_changing() {
    let counter: Counter = replay([
        incremented(3),
        CounterEvent::Annotated(Annotated {
            note: "a remark".to_owned(),
        }),
    ])
    .expect("the history replays");

    assert_eq!(counter.total, 3);
}

#[test]
fn walks_the_same_states_as_replay() {
    let history = || {
        [
            incremented(1),
            incremented(2),
            CounterEvent::Reset(Reset {}),
            incremented(4),
        ]
    };

    let mut current = Counter::default();
    for event in history() {
        current = step(&current, &event).expect("the event steps");
    }

    assert_eq!(current, replay(history()).expect("the history replays"));
}

#[test]
fn leaves_the_given_state_unchanged() {
    let before = Counter { total: 5 };

    let after = step(&before, &incremented(2)).expect("the event steps");

    assert_eq!(before.total, 5);
    assert_eq!(after.total, 7);
}

#[test]
fn fails_on_an_event_that_does_not_survive_a_step() {
    let before = Gauge { last: 1.0 };

    let failure = step(&before, &GaugeEvent::Measured(Measured { value: f64::NAN }))
        .expect_err("NaN does not survive JSON");

    assert!(failure.to_string().contains("decoding"), "{failure}");
    assert_eq!(
        before.last.to_bits(),
        1.0_f64.to_bits(),
        "the given state is unchanged"
    );
}

#[test]
fn starts_from_the_latest_event_of_that_type() {
    let counter: CounterFromLatest = replay([
        incremented(3),
        CounterEvent::Reset(Reset {}),
        incremented(5),
        CounterEvent::Reset(Reset {}),
        incremented(2),
    ])
    .expect("the history replays");

    assert_eq!(counter.0.total, 2);
}

#[test]
fn replays_everything_if_the_type_is_missing() {
    let counter: CounterFromLatest =
        replay([incremented(3), incremented(4)]).expect("the history replays");

    assert_eq!(counter.0.total, 7);
}

#[test]
fn starts_stored_events_from_the_latest_event_of_that_type() {
    // The state has no rule for the first event, so replaying it would fail.
    // Starting from the latest reset skips it.
    let counter: CounterFromLatest = replay_stored([
        UpcastEvent::new("io.thenativeweb.test.unheard-of", json!({})),
        UpcastEvent::new(Reset::EVENT_TYPE, json!({})),
        UpcastEvent::new(Incremented::EVENT_TYPE, json!({ "by": 2 })),
        UpcastEvent::new(Reset::EVENT_TYPE, json!({})),
        UpcastEvent::new(Incremented::EVENT_TYPE, json!({ "by": 5 })),
    ])
    .expect("the history replays");

    assert_eq!(counter.0.total, 5);
}

#[test]
fn starts_from_the_latest_event_of_that_type_not_from_the_first() {
    // A reset erases everything before it, so starting from an earlier reset
    // would yield the same total. An event the state can not read between the
    // two resets tells them apart: only starting from the latest skips it.
    let counter: CounterFromLatest = replay_stored([
        UpcastEvent::new(Reset::EVENT_TYPE, json!({})),
        UpcastEvent::new("io.thenativeweb.test.unheard-of", json!({})),
        UpcastEvent::new(Reset::EVENT_TYPE, json!({})),
        UpcastEvent::new(Incremented::EVENT_TYPE, json!({ "by": 5 })),
    ])
    .expect("the history replays from the latest reset");

    assert_eq!(counter.0.total, 5);
}

#[test]
#[should_panic(
    expected = r#"event type "io.thenativeweb.test.unheard-of" has no rule on the state"#
)]
fn panics_for_a_state_that_starts_from_an_event_type_without_rule() {
    let _ = replay::<CounterFromUnheardOf>([incremented(1)]);
}

#[test]
fn steps_a_stored_event() {
    let after = step_stored(
        &Counter { total: 1 },
        UpcastEvent::new(Incremented::EVENT_TYPE, json!({ "by": 2 })),
    )
    .expect("the event steps");

    assert_eq!(after.total, 3);
}
