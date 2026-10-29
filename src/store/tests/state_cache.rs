// These tests check loading with and without the state cache, and executing
// with it.
//
// A state is known by its type, which is all the cache tells states apart by,
// and every state is Clone. Two different states need two types, which "keeps
// states of different types on the same subject apart" shows.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;

use super::{
    Counter, CounterEvent, Incremented, Reset,
    commands::{Increment, counter_decider, write_past_the_kit},
    database::{Database, subject_for},
};
use crate::{Event, ExecuteError, State, Store, Subject};

/// How many events the counting state of each test has evolved by, which shows
/// how many events were read. Every test counts with a state of its own, by
/// its index, since the tests run at the same time.
static EVOLVED: [AtomicUsize; 8] = [const { AtomicUsize::new(0) }; 8];

fn evolved(test: usize) -> usize {
    EVOLVED[test].load(Ordering::SeqCst)
}

/// The counter, counting the events it evolves by.
#[derive(Clone, Debug, Default)]
struct Counting<const TEST: usize>(Counter);

impl<const TEST: usize> State for Counting<TEST> {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        let _ = EVOLVED[TEST].fetch_add(1, Ordering::SeqCst);
        Counting(self.0.evolve(event))
    }
}

/// The counter, counting the events it evolves by, read from the latest reset
/// onwards.
#[derive(Clone, Debug, Default)]
struct CountingFromLatest<const TEST: usize>(Counter);

impl<const TEST: usize> State for CountingFromLatest<TEST> {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        let _ = EVOLVED[TEST].fetch_add(1, Ordering::SeqCst);
        CountingFromLatest(self.0.evolve(event))
    }

    fn from_latest() -> Option<&'static str> {
        Some(Reset::EVENT_TYPE)
    }
}

/// The amounts of all increments, which a clone copies.
#[derive(Clone, Debug, Default)]
struct History<const TEST: usize>(Vec<i64>);

impl<const TEST: usize> State for History<TEST> {
    type Event = CounterEvent;

    fn evolve(mut self, event: &CounterEvent) -> Self {
        let _ = EVOLVED[TEST].fetch_add(1, Ordering::SeqCst);
        if let CounterEvent::Incremented(incremented) = event {
            self.0.push(incremented.by);
        }

        self
    }
}

/// Counts the increments, where the counter adds them up: another state of
/// the same events.
#[derive(Clone, Debug, Default)]
struct Increments(usize);

impl State for Increments {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        match event {
            CounterEvent::Incremented(_) => Increments(self.0 + 1),
            CounterEvent::Reset(_) | CounterEvent::Annotated(_) => self,
        }
    }
}

async fn cached(database: &Database, max_subjects: usize) -> Store {
    database
        .store_with(|builder| builder.state_cache(max_subjects))
        .await
}

/// Writes increments by the given amounts to the subject past the store, as
/// another process would.
async fn increment(database: &Database, subject: &Subject, amounts: &[i64]) {
    for by in amounts {
        write_past_the_kit(
            database,
            subject,
            Incremented::EVENT_TYPE,
            json!({ "by": by }),
        )
        .await;
    }
}

async fn load<S: State>(store: &Store, subject: &Subject) -> S {
    store.load::<S>(subject).await.expect("the state loads")
}

#[tokio::test]
async fn reads_all_events_every_time_without_a_state_cache() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("cache-without");

    increment(&database, &subject, &[3, 4]).await;
    let _ = load::<Counting<0>>(&store, &subject).await;
    let _ = load::<Counting<0>>(&store, &subject).await;

    assert_eq!(evolved(0), 4);
}

#[tokio::test]
async fn reads_only_the_events_written_since() {
    let database = Database::start().await;
    let store = cached(&database, 10).await;
    let subject = subject_for("cache-since");

    increment(&database, &subject, &[3, 4]).await;
    let _ = load::<Counting<1>>(&store, &subject).await;
    increment(&database, &subject, &[5]).await;
    let current = load::<Counting<1>>(&store, &subject).await;

    assert_eq!(current.0.total, 12);
    assert_eq!(evolved(1), 3);
}

#[tokio::test]
async fn keeps_states_of_different_types_on_the_same_subject_apart() {
    let database = Database::start().await;
    let store = cached(&database, 10).await;
    let subject = subject_for("cache-types");

    increment(&database, &subject, &[3, 4]).await;

    assert_eq!(load::<Increments>(&store, &subject).await.0, 2);
    assert_eq!(load::<Counter>(&store, &subject).await.total, 7);
}

#[tokio::test]
async fn keeps_the_states_of_different_subjects_apart() {
    let database = Database::start().await;
    let store = cached(&database, 10).await;
    let first = Subject::new(&format!("{}/first", subject_for("cache-subjects")));
    let second = Subject::new(&format!("{}/second", subject_for("cache-subjects")));

    increment(&database, &first, &[3, 4]).await;
    increment(&database, &second, &[5]).await;

    assert_eq!(load::<Counter>(&store, &first).await.total, 7);
    assert_eq!(load::<Counter>(&store, &second).await.total, 5);
}

#[tokio::test]
async fn caches_a_state_that_holds_a_vec() {
    let database = Database::start().await;
    let store = cached(&database, 10).await;
    let subject = subject_for("cache-vec");

    increment(&database, &subject, &[3, 4]).await;
    let _ = load::<History<2>>(&store, &subject).await;
    increment(&database, &subject, &[5]).await;
    let current = load::<History<2>>(&store, &subject).await;

    assert_eq!(current.0, [3, 4, 5]);
    assert_eq!(evolved(2), 3);
}

#[tokio::test]
async fn hands_out_a_clone_of_the_cached_state() {
    let database = Database::start().await;
    let store = cached(&database, 10).await;
    let subject = subject_for("cache-clone");

    increment(&database, &subject, &[3]).await;
    let mut first = load::<History<3>>(&store, &subject).await;
    first.0[0] = 999;
    let second = load::<History<3>>(&store, &subject).await;

    assert_eq!(
        second.0,
        [3],
        "changing a loaded state does not change the cache"
    );
}

#[tokio::test]
async fn reads_an_evicted_subject_again() {
    let database = Database::start().await;
    let store = cached(&database, 1).await;
    let first = Subject::new(&format!("{}/first", subject_for("cache-evicted")));
    let second = Subject::new(&format!("{}/second", subject_for("cache-evicted")));

    increment(&database, &first, &[1, 2]).await;
    increment(&database, &second, &[3]).await;
    let _ = load::<Counting<4>>(&store, &first).await;
    let _ = load::<Counting<4>>(&store, &second).await;
    let current = load::<Counting<4>>(&store, &first).await;

    assert_eq!(current.0.total, 3);
    assert_eq!(evolved(4), 5, "the first subject was evicted");
}

#[tokio::test]
async fn works_with_from_latest() {
    let database = Database::start().await;
    let store = cached(&database, 10).await;
    let subject = subject_for("cache-from-latest");

    increment(&database, &subject, &[10]).await;
    write_past_the_kit(&database, &subject, Reset::EVENT_TYPE, json!({})).await;
    increment(&database, &subject, &[2]).await;
    let _ = load::<CountingFromLatest<5>>(&store, &subject).await;
    increment(&database, &subject, &[1]).await;
    let current = load::<CountingFromLatest<5>>(&store, &subject).await;

    assert_eq!(current.0.total, 3);
    assert_eq!(
        evolved(5),
        3,
        "the reset and one increment first, then one more"
    );
}

#[tokio::test]
async fn turns_the_cache_off_with_0_subjects() {
    let database = Database::start().await;
    let store = cached(&database, 0).await;
    let subject = subject_for("cache-off");

    increment(&database, &subject, &[3, 4]).await;
    let _ = load::<Counting<6>>(&store, &subject).await;
    let _ = load::<Counting<6>>(&store, &subject).await;

    assert_eq!(evolved(6), 4, "the state is read in full every time");
}

#[tokio::test]
async fn sees_its_own_writes_when_executing() {
    let database = Database::start().await;
    let store = cached(&database, 10).await;
    let subject = subject_for("cache-execute");

    for _ in 0..2 {
        let _ = store
            .execute(&counter_decider(), Increment::new(&subject, 2).up_to(5))
            .await
            .expect("the limit is not exceeded");
    }

    // The total is 4, so another 2 exceed the limit. A cache that missed the
    // events it did not read itself would still see 0 or 2.
    let failure = store
        .execute(&counter_decider(), Increment::new(&subject, 2).up_to(5))
        .await
        .expect_err("the limit would be exceeded");

    assert!(matches!(failure, ExecuteError::Decider(_)), "{failure:?}");
}
