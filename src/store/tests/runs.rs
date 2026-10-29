// These tests check starting projections and transactional projections, their
// runs, reconnecting and its delays, names, observers of reconnects, waiting
// until runs have caught up, and the panics that end a run.
//
// A run stops through its token, and waiting stops when its future is
// dropped.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use eventsourcingdb::Client;
use futures_util::FutureExt;

use super::{
    Incremented,
    commands::SOURCE,
    database::{Database, subject_for},
    fake::{Answer, Api, Fake, Reply, dead_client},
    revision_of_event,
};
use crate::{
    CancellationToken, Categorized, Category, CaughtUpError, Envelope, EventOn, Phase,
    Precondition, Projection, ProjectionError, ProjectionOptions, ProjectionRun, Reconnect,
    ResumableProjection, Revision, Store, StoreBuilder, Subject, Subjects, Transaction,
    TransactionalProjection, wait_caught_up,
};

const READ_EVENTS: &str = "/api/v1/read-events";
const OBSERVE_EVENTS: &str = "/api/v1/observe-events";

/// The initial delay of the tests, long enough that following a stream that
/// ends at once never takes as long, even on a busy machine.
const DELAY: Duration = Duration::from_millis(200);

/// An event as EventSourcingDB hands it out through the v1 API, with a hash
/// that is not checked, since the stores on the fake are built without.
const EVENT: &str = r#"{"type":"event","payload":{"specversion":"1.0","id":"0","time":"2026-01-01T00:00:00Z","source":"https://thenativeweb.io","subject":"/test","type":"io.thenativeweb.test.incremented","datacontenttype":"application/json","data":{"by":1},"hash":"0","predecessorhash":"0"}}"#;

/// Waits until the condition holds, and fails the test if it does not within
/// a few seconds.
async fn eventually(what: &str, mut holds: impl FnMut() -> bool) {
    let waited = tokio::time::timeout(Duration::from_secs(10), async {
        while !holds() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    assert!(waited.is_ok(), "{what} did not happen in time");
}

async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not happen in time"))
}

/// The failure of a projection of a test, of the given category.
#[derive(Debug)]
struct Refused(Category);

impl std::fmt::Display for Refused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "refused ({})", self.0)
    }
}

impl std::error::Error for Refused {}

impl Categorized for Refused {
    fn category(&self) -> Option<Category> {
        Some(self.0)
    }
}

/// What the projection of a test does besides applying.
#[derive(Debug, Default)]
enum Trouble {
    #[default]
    None,
    RefusesOnceTransiently(AtomicBool),
    RefusesPermanently,
    Panics,
}

/// Remembers the IDs of the events it applies, and of those it saved as its
/// checkpoint, as every event as it is stored.
#[derive(Debug, Default)]
struct Applied {
    ids: Mutex<Vec<Revision>>,
    saved: Mutex<Vec<Revision>>,
    trouble: Trouble,
}

impl Applied {
    fn ids(&self) -> Vec<Revision> {
        self.ids.lock().expect("not poisoned").clone()
    }

    fn saved(&self) -> Vec<Revision> {
        self.saved.lock().expect("not poisoned").clone()
    }
}

impl Projection for Applied {
    type Event = eventsourcingdb::Event;
    type Error = Refused;

    async fn apply(&self, event: Envelope<eventsourcingdb::Event>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        match &self.trouble {
            Trouble::RefusesOnceTransiently(has_refused)
                if !has_refused.swap(true, Ordering::SeqCst) =>
            {
                return Err(Refused(Category::Transient));
            }
            Trouble::RefusesPermanently => return Err(Refused(Category::Permanent)),
            Trouble::Panics => panic!("applying {}", event.id),
            _ => {}
        }
        self.ids.lock().expect("not poisoned").push(event.id);

        Ok(())
    }
}

impl ResumableProjection for Applied {
    async fn checkpoint(&self) -> Result<Option<Revision>, Refused> {
        tokio::task::yield_now().await;
        Ok(self.saved.lock().expect("not poisoned").last().copied())
    }

    async fn save_checkpoint(&self, event_id: Revision) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.saved.lock().expect("not poisoned").push(event_id);

        Ok(())
    }
}

/// A ledger that remembers the ID of every event it committed with.
#[derive(Debug, Default)]
struct Ledger {
    commits: Arc<Mutex<Vec<Revision>>>,
}

struct Booking {
    commits: Arc<Mutex<Vec<Revision>>>,
}

impl TransactionalProjection for Ledger {
    type Event = eventsourcingdb::Event;
    type Error = Refused;
    type Transaction = Booking;

    async fn checkpoint(&self) -> Result<Option<Revision>, Refused> {
        tokio::task::yield_now().await;
        Ok(self.commits.lock().expect("not poisoned").last().copied())
    }

    async fn begin(&self) -> Result<Booking, Refused> {
        tokio::task::yield_now().await;
        Ok(Booking {
            commits: Arc::clone(&self.commits),
        })
    }

    fn catch_up_batch_size(&self) -> usize {
        2
    }
}

impl Transaction for Booking {
    type Event = eventsourcingdb::Event;
    type Error = Refused;

    async fn apply(&mut self, _: Envelope<eventsourcingdb::Event>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn commit(self, last_event_id: Revision) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.commits
            .lock()
            .expect("not poisoned")
            .push(last_event_id);

        Ok(())
    }

    async fn rollback(self) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// The reconnects an observer was told of, as the delay and the attempt.
type Reconnects = Arc<Mutex<Vec<(Duration, usize)>>>;

/// A store on the given client that reads again after the delays of the
/// tests, tells the returned list of every reconnect, and checks no hashes,
/// since the events of the fake have none.
fn observed(client: Client, max_delay: Duration) -> (Store, Reconnects) {
    let reconnects = Reconnects::default();
    let observed = Arc::clone(&reconnects);
    let store = Store::builder(client, SOURCE)
        .without_hash_verification()
        .reconnect_delays(DELAY, max_delay)
        .reconnect_observer(move |reconnect: &Reconnect| {
            observed
                .lock()
                .expect("not poisoned")
                .push((reconnect.delay, reconnect.attempt));
        })
        .build();

    (store, reconnects)
}

fn named(name: &str) -> ProjectionOptions {
    ProjectionOptions::named(name)
}

async fn increment(store: &Store, subject: &Subject, count: usize) -> Vec<Revision> {
    store
        .write(
            (0..count)
                .map(|_| EventOn::new(subject.clone(), &Incremented { by: 1 }))
                .collect(),
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the events are written")
        .iter()
        .map(revision_of_event)
        .collect()
}

// Running.

#[tokio::test]
async fn follows_the_events_once_it_has_caught_up_and_stops_with_its_token() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("run");
    let before = increment(&store, &subject, 2).await;
    let projection = Arc::new(Applied::default());
    let token = CancellationToken::new();

    let run = store.start_projection(
        Subjects::exact(subject.clone()),
        Arc::clone(&projection),
        token.clone(),
        named("books"),
    );
    within("catching up", run.caught_up()).await;
    assert_eq!(projection.ids(), before);

    let after = increment(&store, &subject, 1).await;
    eventually("applying the new event", || projection.ids().len() == 3).await;
    let status = run.status();
    assert_eq!(status.phase, Phase::Live);
    assert!(status.has_caught_up);
    assert_eq!(status.revision, Some(after[0]));
    assert_eq!(run.name(), Some("books"));

    token.cancel();
    within("stopping", run.done()).await;
    assert_eq!(run.status().phase, Phase::Stopped);
    assert!(
        run.error().is_none(),
        "a run stopped by its token has no error"
    );
}

#[tokio::test]
async fn resumes_from_its_checkpoint_and_saves_it_after_every_live_event() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("run-resumable");
    let before = increment(&store, &subject, 2).await;
    let projection = Arc::new(Applied {
        saved: Mutex::new(vec![before[0]]),
        ..Applied::default()
    });
    let token = CancellationToken::new();

    let run = store.start_resumable_projection(
        Subjects::exact(subject.clone()),
        Arc::clone(&projection),
        token.clone(),
        ProjectionOptions::default(),
    );
    within("catching up", run.caught_up()).await;
    assert_eq!(
        projection.ids(),
        &before[1..],
        "it went on from its checkpoint"
    );

    let after = increment(&store, &subject, 2).await;
    eventually("saving the new events", || projection.saved().len() == 4).await;
    assert_eq!(&projection.saved()[2..], after);
    assert_eq!(run.name(), None);

    token.cancel();
    within("stopping", run.done()).await;
}

#[tokio::test]
async fn commits_a_transactional_projection_in_batches_and_then_every_event() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("run-transactional");
    let before = increment(&store, &subject, 3).await;
    let ledger = Arc::new(Ledger::default());
    let token = CancellationToken::new();

    let run = store.start_transactional_projection(
        Subjects::exact(subject.clone()),
        Arc::clone(&ledger),
        token.clone(),
        ProjectionOptions::default(),
    );
    within("catching up", run.caught_up()).await;
    assert_eq!(
        *ledger.commits.lock().expect("not poisoned"),
        [before[1], before[2]],
        "it commits a batch of two, and the rest"
    );
    assert_eq!(run.status().revision, Some(before[2]));

    let after = increment(&store, &subject, 2).await;
    eventually("committing the new events", || {
        ledger.commits.lock().expect("not poisoned").len() == 4
    })
    .await;
    assert_eq!(
        &ledger.commits.lock().expect("not poisoned")[2..],
        after,
        "it commits every live event on its own"
    );

    token.cancel();
    within("stopping", run.done()).await;
}

#[test]
#[should_panic(expected = "architecturekit: a projection needs a name that is not empty, or none")]
fn panics_for_an_empty_name() {
    let _ = ProjectionRun::new(Subject::root(), named(""));
}

// Reconnecting.

#[tokio::test]
async fn reports_a_disruption_keeps_its_beginning_and_is_live_again() {
    let client = Fake::new(Api::V1)
        .route(
            READ_EVENTS,
            Answer::InTurn(vec![
                Answer::Reply(Reply::status(503, "not now")),
                Answer::Reply(Reply::status(503, "not now")),
                Answer::Reply(Reply::status(503, "not now")),
                Answer::Reply(Reply::status(200, "")),
            ]),
        )
        .route(OBSERVE_EVENTS, Answer::Stall(Duration::from_secs(30)))
        .client()
        .await;
    let (store, _) = observed(client, DELAY);
    let token = CancellationToken::new();

    let run = store.start_projection(
        Subjects::exact(Subject::new("/test")),
        Arc::new(Applied::default()),
        token.clone(),
        ProjectionOptions::default(),
    );

    eventually("a disruption", || run.status().phase == Phase::Reconnecting).await;
    let first = run.status();
    assert_eq!(
        first.error.as_ref().and_then(Categorized::category),
        Some(Category::Transient)
    );
    eventually("another attempt", || run.status().attempts >= 2).await;
    assert_eq!(
        run.status().since,
        first.since,
        "a disruption keeps its beginning"
    );

    eventually("being live again", || run.status().phase == Phase::Live).await;
    let live = run.status();
    assert_eq!(live.attempts, 0, "the attempts start over once caught up");
    assert!(live.error.is_none());

    token.cancel();
    within("stopping", run.done()).await;
}

/// Starts a projection on the given fake, lets it reconnect until the
/// observer was told of the given number of reconnects, and returns them.
async fn reconnects_of(fake: Fake, max_delay: Duration, count: usize) -> Vec<(Duration, usize)> {
    let (store, reconnects) = observed(fake.client().await, max_delay);
    let token = CancellationToken::new();

    let run = store.start_projection(
        Subjects::exact(Subject::new("/test")),
        Arc::new(Applied::default()),
        token.clone(),
        ProjectionOptions::default(),
    );
    eventually("the reconnects", || {
        reconnects.lock().expect("not poisoned").len() >= count
    })
    .await;
    token.cancel();
    within("stopping", run.done()).await;

    reconnects.lock().expect("not poisoned")[..count].to_vec()
}

fn ms(milliseconds: u64) -> Duration {
    Duration::from_millis(milliseconds)
}

#[tokio::test]
async fn doubles_the_delay_up_to_the_maximum_if_the_stream_ends_right_after_catching_up() {
    let reconnects = reconnects_of(Fake::new(Api::V1), ms(800), 5).await;

    assert_eq!(
        reconnects,
        [
            (ms(200), 1),
            (ms(400), 2),
            (ms(800), 3),
            (ms(800), 4),
            (ms(800), 5)
        ]
    );
}

#[tokio::test]
async fn doubles_the_delay_if_the_database_fails_before_the_projection_has_caught_up() {
    let fake = Fake::new(Api::V1).route(READ_EVENTS, Answer::Reply(Reply::status(503, "not now")));

    let reconnects = reconnects_of(fake, ms(1600), 3).await;

    assert_eq!(reconnects, [(ms(200), 1), (ms(400), 2), (ms(800), 3)]);
}

#[tokio::test]
async fn starts_over_with_the_initial_delay_after_progress() {
    let fake = Fake::new(Api::V1).route(
        OBSERVE_EVENTS,
        Answer::InTurn(vec![
            Answer::Reply(Reply::status(200, "")),
            Answer::Reply(Reply::status(200, "")),
            Answer::Reply(Reply::status(200, &format!("{EVENT}\n"))),
            Answer::Reply(Reply::status(200, "")),
        ]),
    );

    let reconnects = reconnects_of(fake, ms(1600), 4).await;

    assert_eq!(
        reconnects,
        [(ms(200), 1), (ms(400), 2), (ms(200), 1), (ms(400), 2)]
    );
}

#[tokio::test]
async fn starts_over_with_the_initial_delay_after_following_the_events_for_longer_than_it() {
    let fake = Fake::new(Api::V1).route(
        OBSERVE_EVENTS,
        Answer::InTurn(vec![
            Answer::Reply(Reply::status(200, "")),
            Answer::Reply(Reply::status(200, "")),
            Answer::Stall(ms(600)),
            Answer::Reply(Reply::status(200, "")),
        ]),
    );

    let reconnects = reconnects_of(fake, ms(1600), 4).await;

    assert_eq!(
        reconnects,
        [(ms(200), 1), (ms(400), 2), (ms(200), 1), (ms(400), 2)]
    );
}

#[tokio::test]
async fn reads_again_from_an_unreachable_database() {
    let (store, reconnects) = observed(dead_client().await, DELAY);
    let token = CancellationToken::new();

    let run = store.start_projection(
        Subjects::exact(Subject::new("/test")),
        Arc::new(Applied::default()),
        token.clone(),
        named("books"),
    );
    eventually("reconnecting", || {
        reconnects.lock().expect("not poisoned").len() >= 2
    })
    .await;

    assert_eq!(run.status().phase, Phase::Reconnecting);
    token.cancel();
    within("stopping", run.done()).await;
    assert!(run.error().is_none());
}

#[tokio::test]
async fn tells_the_observer_which_projection_reads_again_and_why() {
    let reconnects = Arc::new(Mutex::new(Vec::<Reconnect>::new()));
    let observed = Arc::clone(&reconnects);
    let store = Store::builder(dead_client().await, SOURCE)
        .reconnect_delays(DELAY, DELAY)
        .reconnect_observer(move |reconnect: &Reconnect| {
            observed
                .lock()
                .expect("not poisoned")
                .push(reconnect.clone());
        })
        .build();
    let token = CancellationToken::new();

    let named_run = store.start_projection(
        Subjects::tree(Subject::new("/books")),
        Arc::new(Applied::default()),
        token.clone(),
        named("books"),
    );
    let unnamed_run = store.start_projection(
        Subjects::exact(Subject::new("/readers")),
        Arc::new(Applied::default()),
        token.clone(),
        ProjectionOptions::default(),
    );
    eventually("both reconnecting", || {
        let reconnects = reconnects.lock().expect("not poisoned");
        reconnects
            .iter()
            .any(|reconnect| reconnect.projection.is_some())
            && reconnects
                .iter()
                .any(|reconnect| reconnect.projection.is_none())
    })
    .await;
    token.cancel();
    within("stopping", named_run.done()).await;
    within("stopping", unnamed_run.done()).await;

    let reconnects = reconnects.lock().expect("not poisoned");
    let named_one = reconnects
        .iter()
        .find(|reconnect| reconnect.projection.is_some())
        .expect("the named projection reconnects");
    assert_eq!(named_one.projection.as_deref(), Some("books"));
    assert_eq!(named_one.subject, Subject::new("/books"));
    assert_eq!(named_one.delay, DELAY);
    assert_eq!(named_one.attempt, 1);
    assert_eq!(named_one.error.category(), Some(Category::Transient));
    let unnamed_one = reconnects
        .iter()
        .find(|reconnect| reconnect.projection.is_none())
        .expect("the unnamed projection reconnects");
    assert_eq!(
        unnamed_one.subject,
        Subject::new("/readers"),
        "a projection without a name is reported by its subject"
    );
}

#[tokio::test]
async fn ends_when_the_database_rejects_the_api_token() {
    let client = Fake::new(Api::V1)
        .route(
            READ_EVENTS,
            Answer::Reply(Reply::status(401, "unauthorized")),
        )
        .client()
        .await;
    let (store, reconnects) = observed(client, DELAY);

    let run = store.start_projection(
        Subjects::exact(Subject::new("/test")),
        Arc::new(Applied::default()),
        CancellationToken::new(),
        ProjectionOptions::default(),
    );
    within("stopping", run.done()).await;

    let error = run.error().expect("the run ends with a failure");
    assert_eq!(error.category(), Some(Category::Permanent));
    assert!(!run.status().has_caught_up);
    assert_eq!(
        reconnects.lock().expect("not poisoned").len(),
        0,
        "it does not read again"
    );
}

#[tokio::test]
async fn ends_on_a_failure_of_the_projection_that_trying_again_will_not_fix() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("run-refused");
    let _ = increment(&store, &subject, 1).await;

    let run = store.start_projection(
        Subjects::exact(subject.clone()),
        Arc::new(Applied {
            trouble: Trouble::RefusesPermanently,
            ..Applied::default()
        }),
        CancellationToken::new(),
        ProjectionOptions::default(),
    );
    within("stopping", run.done()).await;

    let error = run.error().expect("the run ends with a failure");
    assert!(
        matches!(*error, ProjectionError::Projection { .. }),
        "{error:?}"
    );
    assert_eq!(run.status().phase, Phase::Stopped);
}

#[tokio::test]
async fn reads_again_after_a_transient_failure_of_the_projection() {
    let database = Database::start().await;
    let reconnects = Reconnects::default();
    let observed = Arc::clone(&reconnects);
    let store = database
        .store_with(|builder| {
            builder.reconnect_delays(DELAY, DELAY).reconnect_observer(
                move |reconnect: &Reconnect| {
                    observed
                        .lock()
                        .expect("not poisoned")
                        .push((reconnect.delay, reconnect.attempt));
                },
            )
        })
        .await;
    let subject = subject_for("run-transient");
    let written = increment(&store, &subject, 2).await;
    let projection = Arc::new(Applied {
        trouble: Trouble::RefusesOnceTransiently(AtomicBool::new(false)),
        ..Applied::default()
    });
    let token = CancellationToken::new();

    let run = store.start_projection(
        Subjects::exact(subject.clone()),
        Arc::clone(&projection),
        token.clone(),
        ProjectionOptions::default(),
    );
    within("catching up", run.caught_up()).await;

    assert_eq!(projection.ids(), written, "no event is applied twice");
    assert_eq!(reconnects.lock().expect("not poisoned").len(), 1);
    token.cancel();
    within("stopping", run.done()).await;
}

#[tokio::test]
async fn ends_the_run_rather_than_the_task_when_the_projection_panics() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("run-panics");
    let _ = increment(&store, &subject, 1).await;

    let run = store.start_projection(
        Subjects::exact(subject.clone()),
        Arc::new(Applied {
            trouble: Trouble::Panics,
            ..Applied::default()
        }),
        CancellationToken::new(),
        ProjectionOptions::default(),
    );
    within("stopping", run.done()).await;

    let error = run.error().expect("the run ends with a failure");
    assert!(
        matches!(&*error, ProjectionError::Panicked { message, .. } if message.starts_with("applying")),
        "{error:?}"
    );
}

#[tokio::test]
async fn ends_the_run_when_the_observer_of_reconnects_panics() {
    let store = Store::builder(dead_client().await, SOURCE)
        .reconnect_delays(DELAY, DELAY)
        .reconnect_observer(|_: &Reconnect| panic!("observing"))
        .build();

    let run = store.start_projection(
        Subjects::exact(Subject::new("/test")),
        Arc::new(Applied::default()),
        CancellationToken::new(),
        ProjectionOptions::default(),
    );
    within("stopping", run.done()).await;

    let error = run.error().expect("the run ends with a failure");
    assert!(
        matches!(&*error, ProjectionError::Panicked { message, .. } if message == "observing"),
        "{error:?}"
    );
}

fn builder() -> StoreBuilder {
    Store::builder(
        Client::new("http://localhost:3000".parse().expect("a URL"), "secret"),
        SOURCE,
    )
}

#[test]
fn panics_for_reconnect_delays_that_make_no_sense() {
    for (name, initial, max, message) in [
        (
            "an initial delay of zero",
            Duration::ZERO,
            ms(10),
            "architecturekit: reconnect_delays needs an initial delay that is positive, not 0ns",
        ),
        (
            "a maximum delay below the initial delay",
            ms(10),
            ms(5),
            "architecturekit: reconnect_delays needs a maximum delay that is not below the initial delay of 10ms, not 5ms",
        ),
    ] {
        let panicked =
            std::panic::catch_unwind(|| builder().reconnect_delays(initial, max)).expect_err(name);
        assert_eq!(
            panicked.downcast_ref::<String>().map(String::as_str),
            Some(message),
            "{name}"
        );
    }

    let _ = builder().reconnect_delays(ms(10), ms(10));
}

#[tokio::test]
async fn waits_as_long_on_every_attempt_with_equal_delays() {
    let reconnects = reconnects_of(Fake::new(Api::V1), DELAY, 3).await;

    assert_eq!(reconnects, [(DELAY, 1), (DELAY, 2), (DELAY, 3)]);
}

// Waiting until runs have caught up.

#[tokio::test]
async fn returns_once_every_run_has_caught_up_also_if_it_has_ended_since() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("wait");
    let _ = increment(&store, &subject, 2).await;
    let token = CancellationToken::new();

    let first = store.start_projection(
        Subjects::exact(subject.clone()),
        Arc::new(Applied::default()),
        token.clone(),
        named("first"),
    );
    let second = store.start_projection(
        Subjects::tree(Subject::new("/")),
        Arc::new(Applied::default()),
        token.clone(),
        named("second"),
    );
    within("catching up", wait_caught_up(&[&first, &second]))
        .await
        .expect("both runs catch up");

    token.cancel();
    within("stopping", first.done()).await;
    within("waiting again", wait_caught_up(&[&first, &second]))
        .await
        .expect("a run that has caught up counts, although it has ended since");
}

#[tokio::test]
async fn returns_at_once_without_any_run() {
    wait_caught_up(&[])
        .now_or_never()
        .expect("nothing is waited for")
        .expect("no run has failed");
}

/// Starts a run on a fake that refuses to be read with the given status.
async fn refused_run(
    status: u16,
    options: ProjectionOptions,
) -> (ProjectionRun, CancellationToken) {
    let client = Fake::new(Api::V1)
        .route(READ_EVENTS, Answer::Reply(Reply::status(status, "refused")))
        .client()
        .await;
    let (store, _) = observed(client, DELAY);
    let token = CancellationToken::new();
    let run = store.start_projection(
        Subjects::exact(Subject::new("/test")),
        Arc::new(Applied::default()),
        token.clone(),
        options,
    );

    (run, token)
}

/// Starts a run on a fake that never answers the read, so that it never
/// catches up.
async fn stalled_run(options: ProjectionOptions) -> (ProjectionRun, CancellationToken) {
    let client = Fake::new(Api::V1)
        .route(READ_EVENTS, Answer::Stall(Duration::from_secs(30)))
        .client()
        .await;
    let (store, _) = observed(client, DELAY);
    let token = CancellationToken::new();
    let run = store.start_projection(
        Subjects::exact(Subject::new("/test")),
        Arc::new(Applied::default()),
        token.clone(),
        options,
    );

    (run, token)
}

#[tokio::test]
async fn names_a_run_that_ends_with_a_failure_before_it_caught_up() {
    let (named_run, _) = refused_run(401, named("books")).await;
    let (unnamed_run, _) = refused_run(401, ProjectionOptions::default()).await;

    let failure = within("failing", wait_caught_up(&[&named_run]))
        .await
        .expect_err("the run fails");
    assert!(
        matches!(&failure, CaughtUpError::Failed { projection: Some(name), .. } if name == "books"),
        "{failure:?}"
    );
    assert!(
        failure.to_string().starts_with(r#"projection "books": "#),
        "{failure}"
    );
    assert_eq!(failure.category(), Some(Category::Permanent));

    let failure = within("failing", wait_caught_up(&[&unnamed_run]))
        .await
        .expect_err("the run fails");
    let error = unnamed_run.error().expect("the run has failed");
    assert_eq!(
        failure.to_string(),
        format!(r#"projection of "/test": {error}"#),
        "a run without a name is named by its subject"
    );
}

#[tokio::test]
async fn names_a_run_that_stopped_before_it_caught_up() {
    let (named_run, named_token) = stalled_run(named("books")).await;
    let (unnamed_run, unnamed_token) = stalled_run(ProjectionOptions::default()).await;
    named_token.cancel();
    unnamed_token.cancel();

    let failure = within("stopping", wait_caught_up(&[&named_run]))
        .await
        .expect_err("the run stopped");
    assert_eq!(
        failure.to_string(),
        r#"the projection "books" stopped before it caught up"#
    );
    assert_eq!(failure.category(), None);

    let failure = within("stopping", wait_caught_up(&[&unnamed_run]))
        .await
        .expect_err("the run stopped");
    assert_eq!(
        failure.to_string(),
        r#"the projection of "/test" stopped before it caught up"#,
        "a run without a name is named by its subject"
    );
}

#[tokio::test]
async fn prefers_the_failure_of_a_run_to_a_run_that_stopped_and_names_the_first() {
    let (stopped, token) = stalled_run(named("stopped")).await;
    token.cancel();
    within("stopping", stopped.done()).await;
    let (failed, _) = refused_run(401, named("failed")).await;
    within("failing", failed.done()).await;
    let (other_stopped, other_token) = stalled_run(named("also stopped")).await;
    other_token.cancel();
    within("stopping", other_stopped.done()).await;

    let failure = within("deciding", wait_caught_up(&[&stopped, &failed]))
        .await
        .expect_err("a run failed");
    assert!(
        matches!(&failure, CaughtUpError::Failed { projection: Some(name), .. } if name == "failed"),
        "{failure:?}"
    );

    let failure = within("deciding", wait_caught_up(&[&other_stopped, &stopped]))
        .await
        .expect_err("the runs stopped");
    assert!(
        matches!(&failure, CaughtUpError::Stopped { projection: Some(name), .. } if name == "also stopped"),
        "{failure:?}"
    );
}
