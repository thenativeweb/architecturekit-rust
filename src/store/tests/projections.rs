// These tests check catching up, failing checkpoints, batches, panicking
// projections and typed projections, against a database. Running a
// projection that follows the events is part of the tests of runs.
//
// A projection is of one kind by its trait, so a projection that is
// transactional as well can not be handed to the wrong function. The
// projections of the tests yield before they do anything, as a real one waits
// for its target.

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use super::{
    CounterEvent, Incremented, Reset, Unrelated,
    database::{Database, subject_for},
    revision_of_event,
};
use crate::{
    Categorized, Category, Envelope, EventOn, Precondition, Projection, ProjectionError,
    ResumableProjection, Revision, Store, Subject, Subjects, Transaction, TransactionalProjection,
};

/// The failure of a projection of a test, of the given category.
#[derive(Debug)]
struct Refused(Category);

impl fmt::Display for Refused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "refused ({})", self.0)
    }
}

impl std::error::Error for Refused {}

impl Categorized for Refused {
    fn category(&self) -> Option<Category> {
        Some(self.0)
    }
}

/// Where a projection of a test panics or fails.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum Trouble {
    #[default]
    None,
    RefusesToApply,
    RefusesToReadTheCheckpoint,
    PanicsIn(&'static str),
}

impl Trouble {
    fn panics_in(self, function: &'static str) {
        assert!(
            self != Trouble::PanicsIn(function),
            "the projection panics in {function}"
        );
    }
}

/// Adds up the increments it applies, and remembers what it applied and saved.
#[derive(Default)]
struct Totals {
    applied: Mutex<Vec<Revision>>,
    saved: Mutex<Vec<Revision>>,
    checkpoint: Mutex<Option<Revision>>,
    total: Mutex<i64>,
    trouble: Trouble,
    batch_size: usize,
}

impl Totals {
    fn applied(&self) -> Vec<Revision> {
        self.applied.lock().expect("not poisoned").clone()
    }

    fn saved(&self) -> Vec<Revision> {
        self.saved.lock().expect("not poisoned").clone()
    }

    fn total(&self) -> i64 {
        *self.total.lock().expect("not poisoned")
    }
}

impl Projection for Totals {
    type Event = CounterEvent;
    type Error = Refused;

    async fn apply(&self, event: Envelope<CounterEvent>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.trouble.panics_in("apply");
        if self.trouble == Trouble::RefusesToApply {
            return Err(Refused(Category::Transient));
        }

        self.applied.lock().expect("not poisoned").push(event.id);
        if let CounterEvent::Incremented(incremented) = event.data {
            *self.total.lock().expect("not poisoned") += incremented.by;
        }

        Ok(())
    }
}

impl ResumableProjection for Totals {
    async fn checkpoint(&self) -> Result<Option<Revision>, Refused> {
        tokio::task::yield_now().await;
        self.trouble.panics_in("checkpoint");
        if self.trouble == Trouble::RefusesToReadTheCheckpoint {
            return Err(Refused(Category::Transient));
        }

        Ok(*self.checkpoint.lock().expect("not poisoned"))
    }

    async fn save_checkpoint(&self, event_id: Revision) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.trouble.panics_in("save_checkpoint");
        self.saved.lock().expect("not poisoned").push(event_id);
        *self.checkpoint.lock().expect("not poisoned") = Some(event_id);

        Ok(())
    }

    fn catch_up_batch_size(&self) -> usize {
        self.batch_size
    }
}

/// The total of a ledger together with the ID of the last event it counts.
type Committed = Arc<Mutex<(i64, Option<Revision>)>>;

/// A ledger whose transactions add up the increments, and which keeps the
/// committed total together with the ID of the last event.
#[derive(Default)]
struct Ledger {
    committed: Committed,
    trouble: Trouble,
}

struct Booking {
    committed: Committed,
    pending: i64,
    trouble: Trouble,
}

impl Ledger {
    fn committed(&self) -> (i64, Option<Revision>) {
        *self.committed.lock().expect("not poisoned")
    }
}

impl TransactionalProjection for Ledger {
    type Event = CounterEvent;
    type Error = Refused;
    type Transaction = Booking;

    async fn checkpoint(&self) -> Result<Option<Revision>, Refused> {
        tokio::task::yield_now().await;
        self.trouble.panics_in("checkpoint");

        Ok(self.committed.lock().expect("not poisoned").1)
    }

    async fn begin(&self) -> Result<Booking, Refused> {
        tokio::task::yield_now().await;
        self.trouble.panics_in("begin");

        Ok(Booking {
            committed: Arc::clone(&self.committed),
            pending: 0,
            trouble: self.trouble,
        })
    }
}

impl Transaction for Booking {
    type Event = CounterEvent;
    type Error = Refused;

    async fn apply(&mut self, event: Envelope<CounterEvent>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.trouble.panics_in("apply");
        if let CounterEvent::Incremented(incremented) = event.data {
            self.pending += incremented.by;
        }

        Ok(())
    }

    async fn commit(self, last_event_id: Revision) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.trouble.panics_in("commit");
        let mut committed = self.committed.lock().expect("not poisoned");
        committed.0 += self.pending;
        committed.1 = Some(last_event_id);

        Ok(())
    }

    async fn rollback(self) -> Result<(), Refused> {
        tokio::task::yield_now().await;

        Ok(())
    }
}

/// Keeps every event as it is stored, to check what a projection of stored
/// events gets.
struct Stored(Mutex<Vec<eventsourcingdb::Event>>);

impl Projection for Stored {
    type Event = eventsourcingdb::Event;
    type Error = Refused;

    async fn apply(&self, event: Envelope<eventsourcingdb::Event>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.0.lock().expect("not poisoned").push(event.data);

        Ok(())
    }
}

/// Writes increments by the given amounts to the subject, and returns their
/// IDs.
async fn increment(store: &Store, subject: &Subject, amounts: &[i64]) -> Vec<Revision> {
    store
        .write(
            amounts
                .iter()
                .map(|by| EventOn::new(subject.clone(), &Incremented { by: *by }))
                .collect(),
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the events are written")
        .iter()
        .map(revision_of_event)
        .collect()
}

#[tokio::test]
async fn applies_what_is_stored() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up");
    let _ = increment(&store, &subject, &[1, 2, 3]).await;
    let totals = Totals::default();

    store
        .catch_up_projection(&Subjects::exact(subject.clone()), &totals)
        .await
        .expect("the projection catches up");

    assert_eq!(totals.total(), 6);
}

#[tokio::test]
async fn reads_a_subject_tree() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = increment(&store, &Subject::new("/books/42"), &[1]).await;
    let _ = increment(&store, &Subject::new("/books/42/notes"), &[2]).await;
    let _ = increment(&store, &Subject::new("/readers/7"), &[4]).await;
    let totals = Totals::default();

    store
        .catch_up_projection(&Subjects::tree(Subject::new("/books")), &totals)
        .await
        .expect("the projection catches up");

    assert_eq!(totals.total(), 3);
}

#[tokio::test]
async fn leaves_out_events_of_types_the_projection_has_no_variant_for() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up-unknown");
    let _ = increment(&store, &subject, &[1]).await;
    let _ = store
        .write(
            vec![EventOn::new(subject.clone(), &Unrelated {})],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the unrelated event is written");
    let totals = Totals::default();

    store
        .catch_up_projection(&Subjects::exact(subject.clone()), &totals)
        .await
        .expect("the projection catches up");

    assert_eq!(totals.applied(), [Revision::from(0)]);
}

#[tokio::test]
async fn has_nothing_to_do_without_events() {
    let database = Database::start().await;
    let totals = Totals::default();

    database
        .store()
        .await
        .catch_up_projection(&Subjects::tree(Subject::new("/")), &totals)
        .await
        .expect("there is nothing to do");

    assert_eq!(totals.applied(), Vec::<Revision>::new());
}

#[tokio::test]
async fn resumes_from_its_checkpoint() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up-resumable");
    let ids = increment(&store, &subject, &[1, 2, 3]).await;
    let totals = Totals {
        checkpoint: Mutex::new(Some(ids[0])),
        ..Totals::default()
    };

    store
        .catch_up_resumable_projection(&Subjects::exact(subject.clone()), &totals)
        .await
        .expect("the projection catches up");

    assert_eq!(totals.applied(), &ids[1..]);
    assert_eq!(totals.saved(), &ids[1..]);
    assert_eq!(totals.total(), 5);
}

#[tokio::test]
async fn saves_the_checkpoint_once_per_batch_while_catching_up() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up-batches");
    let ids = increment(&store, &subject, &[1, 1, 1, 1, 1]).await;
    let totals = Totals {
        batch_size: 2,
        ..Totals::default()
    };

    store
        .catch_up_resumable_projection(&Subjects::exact(subject.clone()), &totals)
        .await
        .expect("the projection catches up");

    assert_eq!(totals.saved(), [ids[1], ids[3], ids[4]]);
}

#[tokio::test]
async fn reports_a_failing_apply_with_its_category() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up-refused");
    let _ = increment(&store, &subject, &[1]).await;
    let totals = Totals {
        trouble: Trouble::RefusesToApply,
        ..Totals::default()
    };

    let failure = store
        .catch_up_projection(&Subjects::exact(subject.clone()), &totals)
        .await
        .expect_err("the projection refuses");

    assert!(
        matches!(failure, ProjectionError::Projection { .. }),
        "{failure:?}"
    );
    assert_eq!(
        failure.category(),
        Some(Category::Transient),
        "catching up once does not try again"
    );
}

#[tokio::test]
async fn stops_when_the_checkpoint_can_not_be_read() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up-checkpoint");
    let _ = increment(&store, &subject, &[1]).await;
    let totals = Totals {
        trouble: Trouble::RefusesToReadTheCheckpoint,
        ..Totals::default()
    };

    let failure = store
        .catch_up_resumable_projection(&Subjects::exact(subject.clone()), &totals)
        .await
        .expect_err("the checkpoint can not be read");

    assert!(
        matches!(failure, ProjectionError::Projection { .. }),
        "{failure:?}"
    );
    assert_eq!(totals.applied(), Vec::<Revision>::new());
}

#[tokio::test]
async fn commits_a_transaction_with_the_last_event_id_and_resumes_from_it() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up-transactional");
    let first = increment(&store, &subject, &[1, 2]).await;
    let ledger = Ledger::default();

    store
        .catch_up_transactional_projection(&Subjects::exact(subject.clone()), &ledger)
        .await
        .expect("the projection catches up");
    assert_eq!(ledger.committed(), (3, Some(first[1])));

    let second = increment(&store, &subject, &[4]).await;
    store
        .catch_up_transactional_projection(&Subjects::exact(subject.clone()), &ledger)
        .await
        .expect("the projection catches up again");
    assert_eq!(
        ledger.committed(),
        (7, Some(second[0])),
        "it went on from its checkpoint"
    );
}

#[tokio::test]
async fn comes_back_from_catching_up_as_an_error_wherever_the_projection_panics() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up-panics");
    let _ = increment(&store, &subject, &[1]).await;
    let subjects = Subjects::exact(subject.clone());

    let mut failures = Vec::new();
    for function in ["apply", "checkpoint", "save_checkpoint"] {
        let totals = Totals {
            trouble: Trouble::PanicsIn(function),
            ..Totals::default()
        };
        failures.push((
            function,
            store
                .catch_up_resumable_projection(&subjects, &totals)
                .await,
        ));
    }
    for function in ["apply", "checkpoint", "begin", "commit"] {
        let ledger = Ledger {
            trouble: Trouble::PanicsIn(function),
            ..Ledger::default()
        };
        failures.push((
            function,
            store
                .catch_up_transactional_projection(&subjects, &ledger)
                .await,
        ));
    }

    for (function, result) in failures {
        let failure = result.expect_err(function);
        assert!(
            matches!(&failure, ProjectionError::Panicked { message, .. } if message.contains(function)),
            "{function}: {failure:?}"
        );
        assert_eq!(failure.category(), Some(Category::Permanent), "{function}");
    }
}

#[tokio::test]
async fn hands_a_projection_of_stored_events_every_event_with_its_hash() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("catch-up-stored");
    let _ = increment(&store, &subject, &[1]).await;
    let _ = store
        .write(
            vec![
                EventOn::new(subject.clone(), &Reset {}),
                EventOn::new(subject.clone(), &Unrelated {}),
            ],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the events are written");
    let stored = Stored(Mutex::new(Vec::new()));

    store
        .catch_up_projection(&Subjects::exact(subject.clone()), &stored)
        .await
        .expect("the projection catches up");

    let events = stored.0.lock().expect("not poisoned");
    assert_eq!(events.len(), 3, "every event, whatever its type");
    assert!(
        events.iter().all(|event| event.verify_hash().is_ok()),
        "the events keep their hashes"
    );
}
