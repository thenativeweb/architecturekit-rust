// These tests mirror TestUpcaster, TestUpcast, TestSharedUpcasters,
// TestReplayStored and TestStepStored of ArchitectureKit for Go. Not
// applicable: a nil upcaster, and TestStateUpcastWith, since a state names its
// upcasters with State::upcasters, once, by its type. In Go, the data of a
// stored event is raw JSON, so "error is permanent" stores text that is no
// JSON; in Rust, the data is a serde_json::Value, so it stores a value that
// does not match the old shape instead.

use std::{convert::Infallible, sync::LazyLock};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Event, Events, State, UpcastEvent, Upcasters, replay_stored, step_stored};

/// The current shape of the event, version three.
#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.credited.v3")]
struct Credited {
    amount: i64,
    currency: String,
}

#[derive(Events)]
enum LedgerEvent {
    Credited(Credited),
}

#[derive(Clone, Debug, Default)]
struct Ledger {
    total: i64,
    currency: String,
    entries: i64,
}

fn credit(ledger: &Ledger, event: &LedgerEvent) -> Ledger {
    match event {
        LedgerEvent::Credited(credited) => Ledger {
            total: ledger.total + credited.amount,
            currency: credited.currency.clone(),
            entries: ledger.entries + 1,
        },
    }
}

/// Reaches the current shape from the older ones through a chain of upcasters,
/// one step per version.
static LEDGER_UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
    Upcasters::new()
        // v1 had no currency at all.
        .upcast("io.thenativeweb.test.credited.v1", |event| {
            #[derive(Deserialize)]
            struct Version1 {
                amount: i64,
            }

            let old: Version1 = serde_json::from_value(event.data().clone())?;

            Ok::<_, serde_json::Error>(vec![
                event
                    .with_type("io.thenativeweb.test.credited.v2")
                    .with_data(json!({ "amount": old.amount, "currency": "eur" })),
            ])
        })
        // v2 spelled the currency in lower case.
        .upcast("io.thenativeweb.test.credited.v2", |event| {
            let mut credited: Credited = serde_json::from_value(event.data().clone())?;
            credited.currency = credited.currency.to_uppercase();
            let data = serde_json::to_value(credited)?;

            Ok::<_, serde_json::Error>(vec![event.with_type(Credited::EVENT_TYPE).with_data(data)])
        })
});

impl State for Ledger {
    type Event = LedgerEvent;

    fn evolve(self, event: &LedgerEvent) -> Self {
        credit(&self, event)
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&LEDGER_UPCASTERS)
    }
}

fn stored(event_type: &str, data: Value) -> UpcastEvent {
    UpcastEvent::new(event_type, data)
}

#[test]
fn a_chain_reaches_the_current_shape() {
    let ledger: Ledger = replay_stored([
        stored("io.thenativeweb.test.credited.v1", json!({ "amount": 10 })),
        stored(
            "io.thenativeweb.test.credited.v2",
            json!({ "amount": 5, "currency": "chf" }),
        ),
        stored(
            "io.thenativeweb.test.credited.v3",
            json!({ "amount": 1, "currency": "USD" }),
        ),
    ])
    .expect("the history replays");

    assert_eq!(ledger.total, 16);
    assert_eq!(ledger.entries, 3);
    assert_eq!(ledger.currency, "USD");
}

#[test]
fn a_v1_event_goes_through_both_steps() {
    // Only the second upcaster turns the currency into upper case, so an upper
    // case currency shows that the v1 event went through both.
    let ledger: Ledger = replay_stored([stored(
        "io.thenativeweb.test.credited.v1",
        json!({ "amount": 10 }),
    )])
    .expect("the history replays");

    assert_eq!(ledger.currency, "EUR");
}

#[derive(Clone, Debug, Default)]
struct SplitLedger(Ledger);

static SPLITTING_UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
    Upcasters::new().upcast("io.thenativeweb.test.credited.batch", |event| {
        Ok::<_, Infallible>(vec![
            event
                .clone()
                .with_type(Credited::EVENT_TYPE)
                .with_data(json!({ "amount": 3, "currency": "EUR" })),
            event
                .with_type(Credited::EVENT_TYPE)
                .with_data(json!({ "amount": 4, "currency": "EUR" })),
        ])
    })
});

impl State for SplitLedger {
    type Event = LedgerEvent;

    fn evolve(self, event: &LedgerEvent) -> Self {
        SplitLedger(credit(&self.0, event))
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&SPLITTING_UPCASTERS)
    }
}

#[test]
fn can_split_one_event_into_two() {
    let ledger: SplitLedger =
        replay_stored([stored("io.thenativeweb.test.credited.batch", json!({}))])
            .expect("the history replays");

    assert_eq!(ledger.0.total, 7);
    assert_eq!(ledger.0.entries, 2);
}

#[test]
fn applies_all_events_an_upcaster_splits_one_into_when_stepping() {
    let before = SplitLedger::default();

    let after = step_stored(
        &before,
        stored("io.thenativeweb.test.credited.batch", json!({})),
    )
    .expect("the event steps");

    assert_eq!(after.0.total, 7);
    assert_eq!(before.0.total, 0, "the given state is unchanged");
}

#[test]
fn runs_the_upcasters_when_stepping() {
    let after = step_stored(
        &Ledger::default(),
        stored("io.thenativeweb.test.credited.v1", json!({ "amount": 10 })),
    )
    .expect("the event steps");

    assert_eq!(after.total, 10);
    assert_eq!(after.currency, "EUR");
}

#[test]
fn an_error_of_an_upcaster_is_permanent() {
    let failure = replay_stored::<Ledger>([stored(
        "io.thenativeweb.test.credited.v1",
        json!("not an object"),
    )])
    .expect_err("the v1 upcaster can not read the data");

    assert!(
        failure
            .to_string()
            .starts_with("permanent failure: upcasting"),
        "{failure}"
    );
    assert!(
        std::error::Error::source(&failure).is_some(),
        "the error of the upcaster is the cause"
    );
}

#[derive(Clone, Debug, Default)]
struct LoopingLedger(Ledger);

static LOOPING_UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
    // It keeps the event as it is, including its type.
    Upcasters::new().upcast("io.thenativeweb.test.loop", |event| {
        Ok::<_, Infallible>(vec![event])
    })
});

impl State for LoopingLedger {
    type Event = LedgerEvent;

    fn evolve(self, event: &LedgerEvent) -> Self {
        LoopingLedger(credit(&self.0, event))
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&LOOPING_UPCASTERS)
    }
}

#[test]
fn an_upcaster_that_keeps_its_type_is_stopped() {
    let failure = replay_stored::<LoopingLedger>([stored("io.thenativeweb.test.loop", json!({}))])
        .expect_err("an upcaster that never changes the type must be stopped");

    assert!(
        failure.to_string().contains("exceeded 16 steps"),
        "{failure}"
    );
}

#[test]
#[should_panic(expected = r#"event type "io.thenativeweb.test.same" already has an upcaster"#)]
fn panics_on_duplicate_registration() {
    let _ = Upcasters::new()
        .upcast("io.thenativeweb.test.same", |event| {
            Ok::<_, Infallible>(vec![event])
        })
        .upcast("io.thenativeweb.test.same", |event| {
            Ok::<_, Infallible>(vec![event])
        });
}

#[derive(Clone, Debug, Default)]
struct Total(i64);

impl State for Total {
    type Event = LedgerEvent;

    fn evolve(self, event: &LedgerEvent) -> Self {
        match event {
            LedgerEvent::Credited(credited) => Total(self.0 + credited.amount),
        }
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&LEDGER_UPCASTERS)
    }
}

#[derive(Clone, Debug, Default)]
struct Currencies(String);

impl State for Currencies {
    type Event = LedgerEvent;

    fn evolve(self, event: &LedgerEvent) -> Self {
        match event {
            LedgerEvent::Credited(credited) => Currencies(self.0 + &credited.currency),
        }
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&LEDGER_UPCASTERS)
    }
}

#[test]
fn shared_upcasters_apply_to_every_state_that_uses_them() {
    let history = || {
        [
            stored("io.thenativeweb.test.credited.v1", json!({ "amount": 10 })),
            stored(
                "io.thenativeweb.test.credited.v2",
                json!({ "amount": 5, "currency": "chf" }),
            ),
        ]
    };

    let total: Total = replay_stored(history()).expect("the history replays");
    let currencies: Currencies = replay_stored(history()).expect("the history replays");

    assert_eq!(total.0, 15);
    assert_eq!(currencies.0, "EURCHF");
}

#[test]
fn has_no_rule_for_an_older_type_that_only_an_upcaster_knows() {
    assert!(!LedgerEvent::has_rule("io.thenativeweb.test.credited.v1"));
    assert!(LedgerEvent::has_rule(Credited::EVENT_TYPE));
}

#[test]
fn fails_on_an_event_without_rule() {
    let failure = replay_stored::<Ledger>([stored("io.thenativeweb.test.unheard-of", json!({}))])
        .expect_err("the state has no rule for it");

    assert!(
        failure.to_string().contains("no rule for event type"),
        "{failure}"
    );
}

#[test]
fn fails_on_data_that_does_not_match() {
    let failure = replay_stored::<Ledger>([stored(
        "io.thenativeweb.test.credited.v3",
        json!({ "amount": "not a number" }),
    )])
    .expect_err("a string is no number");

    assert!(failure.to_string().contains("decoding"), "{failure}");
}

#[test]
fn lists_its_event_types() {
    assert_eq!(
        format!("{:?}", *LEDGER_UPCASTERS),
        r#"Upcasters { event_types: ["io.thenativeweb.test.credited.v1", "io.thenativeweb.test.credited.v2"] }"#,
    );
}
