// These tests mirror TestNewDecider of ArchitectureKit for Go. Not applicable:
// a nil state, a nil function, and the zero value of a Decider (TestDecider),
// since a Decider can only be made from a function, and its state is its type.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{Decider, DomainError, Event, Events, State};

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.incremented")]
struct Incremented {
    by: i64,
}

#[derive(Debug, Events)]
enum CounterEvent {
    Incremented(Incremented),
}

#[derive(Clone, Debug, Default)]
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
        }
    }
}

#[derive(Clone)]
struct Increment {
    by: i64,
}

fn counter_decider() -> Decider<Increment, Counter, DomainError> {
    Decider::new(|cmd: Increment, counter: Counter| async move {
        if counter.total + cmd.by > 10 {
            return Err(DomainError::new("the counter can not go beyond 10"));
        }

        Ok(vec![CounterEvent::Incremented(Incremented { by: cmd.by })])
    })
}

#[tokio::test]
async fn creates_a_decider_that_decides_on_the_given_state_with_the_given_function() {
    let decider = counter_decider();

    let events = decider
        .decide(Increment { by: 3 }, Counter { total: 2 })
        .await
        .expect("the command is accepted");
    assert!(matches!(
        events.as_slice(),
        [CounterEvent::Incremented(Incremented { by: 3 })]
    ));

    let refusal = decider
        .decide(Increment { by: 3 }, Counter { total: 9 })
        .await
        .expect_err("the command is refused");
    assert_eq!(refusal.to_string(), "the counter can not go beyond 10");
}

#[tokio::test]
async fn loads_what_else_it_needs_while_it_decides() {
    // A decider may load something within the decision, such as the state of
    // another subject, so its function returns a future.
    let limits = Arc::new(|| async { 4 });

    let decider = Decider::new(move |cmd: Increment, counter: Counter| {
        let limits = Arc::clone(&limits);
        async move {
            let limit = limits().await;
            if counter.total + cmd.by > limit {
                return Err(DomainError::new(format!(
                    "the counter can not go beyond {limit}"
                )));
            }

            Ok(vec![CounterEvent::Incremented(Incremented { by: cmd.by })])
        }
    });

    let refusal = decider
        .decide(Increment { by: 3 }, Counter { total: 2 })
        .await
        .expect_err("the loaded limit refuses the command");
    assert_eq!(refusal.to_string(), "the counter can not go beyond 4");
}

#[test]
fn can_be_shared_across_threads_and_decides_in_a_future_that_can_be_sent() {
    fn assert_send_and_sync<T: Send + Sync>(_: &T) {}
    fn assert_send<T: Send>(_: &T) {}

    let decider = counter_decider();
    assert_send_and_sync(&decider);

    let decision = decider.decide(Increment { by: 1 }, Counter::default());
    assert_send(&decision);
}

#[test]
fn names_its_types_when_debugged() {
    let debugged = format!("{:?}", counter_decider());

    assert!(debugged.starts_with("Decider {"), "{debugged}");
    assert!(debugged.contains("Increment"), "{debugged}");
    assert!(debugged.contains("Counter"), "{debugged}");
}
