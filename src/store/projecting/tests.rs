// These tests check driving projections: the size of batches, the bound after
// a checkpoint, the writers and the resumable writer, and a projection that
// fails or panics, also within a transaction, with streams made up by the
// tests. A
// projection names its kind by its trait, and a projection of a single
// function needs the types of its events and its error, so it is an ordinary
// type.
//
// They also check what a projection records as seen, which Projection::seen
// does: a projection keeps its kind and its batch size, and a transactional
// one has no such method.
//
// The projections of the tests yield before they do anything, as a real one
// waits for its target, which lets other tasks run in the meantime.

use std::{
    fmt,
    sync::{Arc, LazyLock, Mutex},
};

use eventsourcingdb::{error::ClientError, request_options::BoundType};
use futures_util::{FutureExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::{RebuildWriter, ResumableWriter, TransactionalWriter, Writer, bound_after, drive};
use crate::{
    Categorized, Category, Envelope, Event, Events, Failure, InMemoryView, Permanent, Projection,
    ProjectionError, ResumableProjection, Revision, Revisioned, Transaction,
    TransactionalProjection, UpcastEvent, Upcasters, projection::tests::stored,
};

/// What the projection of a test was asked to do, in order.
#[derive(Clone, Debug, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn push(&self, entry: impl Into<String>) {
        self.0
            .lock()
            .expect("the log is not poisoned")
            .push(entry.into());
    }

    fn entries(&self) -> Vec<String> {
        self.0.lock().expect("the log is not poisoned").clone()
    }
}

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

/// A projection that records what it is asked to do, and refuses or panics at
/// the event a test names.
#[derive(Default)]
struct Recording {
    log: Log,
    refuses: Option<u32>,
    panics: Option<u32>,
    batch_size: usize,
    checkpoint: Mutex<Option<Revision>>,
}

impl Projection for Recording {
    type Event = eventsourcingdb::Event;
    type Error = Refused;

    async fn apply(&self, event: Envelope<Self::Event>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push(format!("apply {}", event.id));
        assert!(
            self.panics.map(Revision::from) != Some(event.id),
            "applying {}",
            event.id
        );
        if self.refuses.map(Revision::from) == Some(event.id) {
            return Err(Refused(Category::Domain));
        }

        Ok(())
    }
}

impl ResumableProjection for Recording {
    async fn checkpoint(&self) -> Result<Option<Revision>, Refused> {
        tokio::task::yield_now().await;
        Ok(*self
            .checkpoint
            .lock()
            .expect("the checkpoint is not poisoned"))
    }

    async fn save_checkpoint(&self, event_id: Revision) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push(format!("save {event_id}"));
        *self
            .checkpoint
            .lock()
            .expect("the checkpoint is not poisoned") = Some(event_id);

        Ok(())
    }

    fn catch_up_batch_size(&self) -> usize {
        self.batch_size
    }
}

/// Where a transactional projection of a test fails or panics, which may be
/// in several places at once.
#[derive(Clone, Copy, Debug, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "every switch is a place where the projection of a test fails"
)]
struct Trouble {
    refuses_to_begin: bool,
    refuses_to_apply: Option<u32>,
    panics_in_apply: Option<u32>,
    refuses_to_commit: bool,
    refuses_to_roll_back: bool,
    panics_in_rollback: bool,
}

#[derive(Default)]
struct Ledger {
    log: Log,
    trouble: Trouble,
    batch_size: usize,
}

struct Booking {
    log: Log,
    trouble: Trouble,
}

impl TransactionalProjection for Ledger {
    type Event = eventsourcingdb::Event;
    type Error = Refused;
    type Transaction = Booking;

    async fn checkpoint(&self) -> Result<Option<Revision>, Refused> {
        tokio::task::yield_now().await;
        Ok(None)
    }

    async fn begin(&self) -> Result<Booking, Refused> {
        tokio::task::yield_now().await;
        self.log.push("begin");
        if self.trouble.refuses_to_begin {
            return Err(Refused(Category::Transient));
        }

        Ok(Booking {
            log: self.log.clone(),
            trouble: self.trouble,
        })
    }

    fn catch_up_batch_size(&self) -> usize {
        self.batch_size
    }
}

impl Transaction for Booking {
    type Event = eventsourcingdb::Event;
    type Error = Refused;

    async fn apply(&mut self, event: Envelope<Self::Event>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push(format!("apply {}", event.id));
        assert!(
            self.trouble.panics_in_apply.map(Revision::from) != Some(event.id),
            "applying {}",
            event.id
        );
        if self.trouble.refuses_to_apply.map(Revision::from) == Some(event.id) {
            return Err(Refused(Category::Domain));
        }

        Ok(())
    }

    async fn commit(self, last_event_id: Revision) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push(format!("commit {last_event_id}"));
        if self.trouble.refuses_to_commit {
            return Err(Refused(Category::Transient));
        }

        Ok(())
    }

    async fn rollback(self) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push("rollback");
        assert!(!self.trouble.panics_in_rollback, "rolling back");
        if self.trouble.refuses_to_roll_back {
            return Err(Refused(Category::Permanent));
        }

        Ok(())
    }
}

/// The stored events with the given IDs, in a stream as the client hands them
/// out, ending with the given failure, if there is one.
fn events(
    ids: &[&str],
    failure: Option<ClientError>,
) -> impl futures_util::Stream<Item = Result<eventsourcingdb::Event, ClientError>> {
    let mut items: Vec<_> = ids
        .iter()
        .map(|id| {
            Ok(stored(
                id,
                "io.thenativeweb.test.incremented",
                json!({ "by": 1 }),
            ))
        })
        .collect();
    items.extend(failure.map(Err));

    stream::iter(items)
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "driving verifies with a function that may fail, which this one never does"
)]
fn verified(_: &eventsourcingdb::Event) -> Result<(), Permanent> {
    Ok(())
}

/// Drives the events through the writer, and returns the ID of the last event
/// applied, how it ended, and the IDs it reported as committed.
async fn drive_all<W: Writer>(
    writer: &mut W,
    ids: &[&str],
    failure: Option<ClientError>,
    batch_size: usize,
) -> (Option<Revision>, Result<(), ProjectionError>, Vec<String>) {
    let mut committed = Vec::new();
    let (last, result) = drive(
        writer,
        events(ids, failure),
        verified,
        batch_size,
        &CancellationToken::new(),
        |id| committed.push(id.to_string()),
    )
    .await;

    (last, result, committed)
}

fn ledger_with(trouble: Trouble) -> Ledger {
    Ledger {
        trouble,
        ..Ledger::default()
    }
}

#[tokio::test]
async fn keeps_no_checkpoint_in_rebuild_mode_but_the_last_event_of_the_run() {
    let projection = Recording::default();
    let mut writer = RebuildWriter::new(&projection);

    assert_eq!(writer.checkpoint().await.expect("checkpoint"), None);
    let (last, result, committed) = drive_all(&mut writer, &["0", "1"], None, 1).await;

    result.expect("the events are applied");
    assert_eq!(last, Some(Revision::from(1)));
    assert_eq!(committed, ["0", "1"]);
    assert_eq!(projection.log.entries(), ["apply 0", "apply 1"]);
    assert_eq!(
        writer.checkpoint().await.expect("checkpoint"),
        Some(Revision::from(1))
    );
}

#[tokio::test]
async fn saves_the_checkpoint_after_every_event_in_resumable_mode() {
    let projection = Recording::default();

    let (_, result, _) =
        drive_all(&mut ResumableWriter::new(&projection), &["0", "1"], None, 1).await;

    result.expect("the events are applied");
    assert_eq!(
        projection.log.entries(),
        ["apply 0", "save 0", "apply 1", "save 1"]
    );
}

#[tokio::test]
async fn honours_the_batch_size_and_commits_an_incomplete_final_batch() {
    let projection = Recording::default();

    let (_, result, committed) = drive_all(
        &mut ResumableWriter::new(&projection),
        &["0", "1", "2", "3", "4"],
        None,
        2,
    )
    .await;

    result.expect("the events are applied");
    assert_eq!(committed, ["1", "3", "4"]);
    assert_eq!(
        projection.log.entries(),
        [
            "apply 0", "apply 1", "save 1", "apply 2", "apply 3", "save 3", "apply 4", "save 4"
        ]
    );
}

#[tokio::test]
async fn brackets_each_batch_in_a_transaction() {
    let ledger = Ledger::default();

    let (_, result, _) = drive_all(
        &mut TransactionalWriter::new(&ledger),
        &["0", "1", "2"],
        None,
        2,
    )
    .await;

    result.expect("the events are applied");
    assert_eq!(
        ledger.log.entries(),
        [
            "begin", "apply 0", "apply 1", "commit 1", "begin", "apply 2", "commit 2"
        ]
    );
}

#[tokio::test]
async fn rolls_back_when_apply_fails_and_keeps_the_category_of_the_projection() {
    let ledger = ledger_with(Trouble {
        refuses_to_apply: Some(1),
        ..Trouble::default()
    });

    let (last, result, committed) = drive_all(
        &mut TransactionalWriter::new(&ledger),
        &["0", "1", "2"],
        None,
        5,
    )
    .await;

    let failure = result.expect_err("the projection refuses");
    assert_eq!(failure.category(), Some(Category::Domain));
    assert_eq!(last, Some(Revision::from(0)));
    assert_eq!(committed, Vec::<String>::new());
    assert_eq!(
        ledger.log.entries(),
        ["begin", "apply 0", "apply 1", "rollback"]
    );
}

#[tokio::test]
async fn rolls_back_when_an_event_fails_verification() {
    let ledger = Ledger::default();
    let mut writer = TransactionalWriter::new(&ledger);

    let (_, result) = drive(
        &mut writer,
        events(&["0", "1"], None),
        |event| {
            if event.id() == "1" {
                return Err(Permanent::Unverified(Failure::new("tampered", None)));
            }
            Ok(())
        },
        5,
        &CancellationToken::new(),
        |_| {},
    )
    .await;

    assert!(
        matches!(
            result,
            Err(ProjectionError::Permanent(Permanent::Unverified(_)))
        ),
        "{result:?}"
    );
    assert_eq!(ledger.log.entries(), ["begin", "apply 0", "rollback"]);
}

#[tokio::test]
async fn rolls_back_when_the_stream_fails() {
    let ledger = Ledger::default();

    let (_, result, _) = drive_all(
        &mut TransactionalWriter::new(&ledger),
        &["0"],
        Some(ClientError::InvalidServerHeader),
        5,
    )
    .await;

    assert!(
        matches!(result, Err(ProjectionError::Transient(_))),
        "{result:?}"
    );
    assert_eq!(ledger.log.entries(), ["begin", "apply 0", "rollback"]);
}

#[tokio::test]
async fn reports_a_failing_begin_without_applying() {
    let ledger = ledger_with(Trouble {
        refuses_to_begin: true,
        ..Trouble::default()
    });

    let (_, result, _) = drive_all(&mut TransactionalWriter::new(&ledger), &["0"], None, 1).await;

    assert_eq!(
        result.expect_err("begin fails").category(),
        Some(Category::Transient)
    );
    assert_eq!(ledger.log.entries(), ["begin"]);
}

#[tokio::test]
async fn stops_when_a_commit_fails_without_rolling_back() {
    for (name, batch_size) in [("a full batch", 1), ("the final batch", 5)] {
        let ledger = ledger_with(Trouble {
            refuses_to_commit: true,
            ..Trouble::default()
        });

        let (_, result, committed) = drive_all(
            &mut TransactionalWriter::new(&ledger),
            &["0", "1"],
            None,
            batch_size,
        )
        .await;

        assert!(result.is_err(), "{name}");
        assert_eq!(committed, Vec::<String>::new(), "{name}");
        assert!(
            !ledger.log.entries().contains(&"rollback".to_owned()),
            "{name}: commit rolls back itself"
        );
    }
}

#[tokio::test]
async fn reports_a_failing_rollback_alongside_the_cause() {
    for (name, refuses_to_apply, failure, cause_category) in [
        ("after a refusal", Some(0), None, Category::Domain),
        (
            "after a broken stream",
            None,
            Some(ClientError::InvalidServerHeader),
            Category::Transient,
        ),
    ] {
        let ledger = ledger_with(Trouble {
            refuses_to_apply,
            refuses_to_roll_back: true,
            ..Trouble::default()
        });

        let (_, result, _) =
            drive_all(&mut TransactionalWriter::new(&ledger), &["0"], failure, 5).await;

        let failure = result.expect_err(name);
        let ProjectionError::RollbackFailed { cause, error } = &failure else {
            panic!("{name}: the rollback fails as well, got {failure:?}");
        };
        assert_eq!(cause.category(), Some(cause_category), "{name}");
        assert_eq!(error.category(), Some(Category::Permanent), "{name}");
        assert_eq!(
            failure.category(),
            Some(cause_category),
            "{name}: the category is the one of the cause"
        );
    }
}

#[tokio::test]
async fn rolls_the_transaction_back_when_apply_panics() {
    let ledger = ledger_with(Trouble {
        panics_in_apply: Some(1),
        ..Trouble::default()
    });

    let (_, result, _) =
        drive_all(&mut TransactionalWriter::new(&ledger), &["0", "1"], None, 5).await;

    let failure = result.expect_err("the projection panics");
    assert!(failure.has_panicked(), "{failure:?}");
    assert_eq!(failure.category(), Some(Category::Permanent));
    assert_eq!(
        failure.to_string(),
        "permanent failure: applying event 1: the projection panicked: applying 1"
    );
    assert_eq!(
        ledger.log.entries(),
        ["begin", "apply 0", "apply 1", "rollback"]
    );
}

#[tokio::test]
async fn reports_a_panic_in_rollback_alongside_its_cause_and_makes_it_permanent() {
    let ledger = ledger_with(Trouble {
        panics_in_rollback: true,
        ..Trouble::default()
    });

    let (_, result, _) = drive_all(
        &mut TransactionalWriter::new(&ledger),
        &["0"],
        Some(ClientError::InvalidServerHeader),
        5,
    )
    .await;

    let failure = result.expect_err("the stream breaks, and the rollback panics");
    let ProjectionError::RollbackFailed { cause, error } = &failure else {
        panic!("the rollback is reported alongside its cause, got {failure:?}");
    };
    assert!(matches!(**cause, ProjectionError::Transient(_)));
    assert!(
        matches!(**error, ProjectionError::Panicked { event_id: None, .. }),
        "a rollback applies no event: {error:?}"
    );
    assert!(failure.has_panicked());
    assert_eq!(
        failure.category(),
        Some(Category::Permanent),
        "a panic ends the run even if the cause may pass"
    );
}

/// Drives the events 0 and 1 through the writer, whose projection fails at
/// event 1, and returns the failure.
async fn failure_at_event_1<W: Writer>(mut writer: W) -> ProjectionError {
    let (_, result, _) = drive_all(&mut writer, &["0", "1"], None, 5).await;

    result.expect_err("the projection fails at event 1")
}

#[tokio::test]
async fn names_the_event_a_projection_failed_or_panicked_at() {
    let refusing = Recording {
        refuses: Some(1),
        ..Recording::default()
    };
    let panicking = Recording {
        panics: Some(1),
        ..Recording::default()
    };
    let refusing_ledger = ledger_with(Trouble {
        refuses_to_apply: Some(1),
        ..Trouble::default()
    });
    let panicking_ledger = ledger_with(Trouble {
        panics_in_apply: Some(1),
        ..Trouble::default()
    });
    let refused = "applying event 1: refused (domain rule violated)";
    let panicked = "permanent failure: applying event 1: the projection panicked: applying 1";

    for (name, failure, text) in [
        (
            "rebuilding, refused",
            failure_at_event_1(RebuildWriter::new(&refusing)).await,
            refused,
        ),
        (
            "resuming, refused",
            failure_at_event_1(ResumableWriter::new(&refusing)).await,
            refused,
        ),
        (
            "in a transaction, refused",
            failure_at_event_1(TransactionalWriter::new(&refusing_ledger)).await,
            refused,
        ),
        (
            "rebuilding, panicked",
            failure_at_event_1(RebuildWriter::new(&panicking)).await,
            panicked,
        ),
        (
            "resuming, panicked",
            failure_at_event_1(ResumableWriter::new(&panicking)).await,
            panicked,
        ),
        (
            "in a transaction, panicked",
            failure_at_event_1(TransactionalWriter::new(&panicking_ledger)).await,
            panicked,
        ),
    ] {
        assert!(
            matches!(
                failure,
                ProjectionError::Projection { event_id: Some(id), .. }
                    | ProjectionError::Panicked { event_id: Some(id), .. }
                    if id == Revision::from(1)
            ),
            "{name}: {failure:?}"
        );
        assert_eq!(failure.to_string(), text, "{name}");
    }
}

#[tokio::test]
async fn names_no_event_for_a_failure_outside_of_applying_one() {
    for (name, trouble) in [
        (
            "begin",
            Trouble {
                refuses_to_begin: true,
                ..Trouble::default()
            },
        ),
        (
            "commit",
            Trouble {
                refuses_to_commit: true,
                ..Trouble::default()
            },
        ),
    ] {
        let ledger = ledger_with(trouble);

        let (_, result, _) =
            drive_all(&mut TransactionalWriter::new(&ledger), &["0"], None, 5).await;

        let failure = result.expect_err(name);
        assert!(
            matches!(failure, ProjectionError::Projection { event_id: None, .. }),
            "{name}: {failure:?}"
        );
        assert_eq!(failure.to_string(), "refused (transient failure)", "{name}");
    }
}

#[tokio::test]
async fn goes_on_from_the_last_event_it_applied_although_the_batch_was_cut_short() {
    let projection = Recording::default();
    let mut writer = ResumableWriter::new(&projection);

    let (_, result, _) = drive_all(
        &mut writer,
        &["0", "1"],
        Some(ClientError::InvalidServerHeader),
        5,
    )
    .await;

    assert!(result.is_err());
    assert_eq!(
        projection.log.entries(),
        ["apply 0", "apply 1"],
        "the checkpoint was never saved"
    );
    assert_eq!(
        writer.checkpoint().await.expect("checkpoint"),
        Some(Revision::from(1))
    );
}

#[tokio::test]
async fn does_not_count_an_event_it_failed_to_apply() {
    let projection = Recording {
        refuses: Some(1),
        ..Recording::default()
    };
    let mut writer = ResumableWriter::new(&projection);

    let (last, result, _) = drive_all(&mut writer, &["0", "1"], None, 5).await;

    assert!(result.is_err());
    assert_eq!(last, Some(Revision::from(0)));
    assert_eq!(
        writer.checkpoint().await.expect("checkpoint"),
        Some(Revision::from(0))
    );
}

#[tokio::test]
async fn are_harmless_without_transactions() {
    let projection = Recording::default();

    let mut rebuild = RebuildWriter::new(&projection);
    rebuild.begin().await.expect("begin");
    rebuild.commit(Revision::from(0)).await.expect("commit");
    rebuild.rollback().await.expect("rollback");

    let mut resumable = ResumableWriter::new(&projection);
    resumable.begin().await.expect("begin");
    resumable.rollback().await.expect("rollback");

    assert_eq!(projection.log.entries(), Vec::<String>::new());
}

#[test]
fn defaults_the_batch_size_to_one_and_clamps_it() {
    let default = Recording::default();
    let zero = Recording {
        batch_size: 0,
        ..Recording::default()
    };
    let three = Recording {
        batch_size: 3,
        ..Recording::default()
    };
    let ledger = Ledger {
        batch_size: 4,
        ..Ledger::default()
    };

    assert_eq!(ResumableWriter::new(&default).batch_size(), 1);
    assert_eq!(ResumableWriter::new(&zero).batch_size(), 1);
    assert_eq!(ResumableWriter::new(&three).batch_size(), 3);
    assert_eq!(RebuildWriter::new(&three).batch_size(), 1);
    assert_eq!(TransactionalWriter::new(&ledger).batch_size(), 4);
}

#[test]
fn excludes_the_checkpoint_from_the_events_read() {
    let bound = bound_after(Some("7")).expect("a checkpoint is a bound");

    assert_eq!(bound.id, "7");
    assert!(matches!(bound.bound_type, BoundType::Exclusive));
    assert!(bound_after(None).is_none());
}

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.incremented")]
struct Incremented {
    by: i64,
}

/// The events of a counter, which leave out every other type.
#[derive(Debug, Events)]
enum Counted {
    Incremented(Incremented),
}

/// An older version of the counter wrote two increments as one event, which
/// the upcasters split.
const INCREMENTED_TWICE: &str = "io.thenativeweb.test.incremented-twice";

static SPLITTING: LazyLock<Upcasters> = LazyLock::new(|| {
    Upcasters::new().upcast(INCREMENTED_TWICE, |event| {
        Ok::<_, serde_json::Error>(vec![
            UpcastEvent::clone(&event).with_type(Incremented::EVENT_TYPE),
            event.with_type(Incremented::EVENT_TYPE),
        ])
    })
});

/// A projection of counters that records what it applies, part by part, and
/// what it has seen, and refuses the event a test names.
#[derive(Default)]
struct Seeing {
    log: Log,
    refuses: Option<u32>,
}

impl Projection for Seeing {
    type Event = Counted;
    type Error = Refused;

    async fn apply(&self, event: Envelope<Counted>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push(format!("apply {}/{}", event.id, event.part));
        if self.refuses.map(Revision::from) == Some(event.id) {
            return Err(Refused(Category::Domain));
        }

        Ok(())
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&SPLITTING)
    }

    fn seen(&self, event_id: Revision) {
        self.log.push(format!("seen {event_id}"));
    }
}

impl ResumableProjection for Seeing {
    async fn checkpoint(&self) -> Result<Option<Revision>, Refused> {
        tokio::task::yield_now().await;
        Ok(None)
    }

    async fn save_checkpoint(&self, event_id: Revision) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push(format!("save {event_id}"));

        Ok(())
    }
}

/// Drives the given stored events through the writer, and returns how it
/// ended.
async fn drive_stored<W: Writer>(
    writer: &mut W,
    events: Vec<eventsourcingdb::Event>,
) -> Result<(), ProjectionError> {
    drive(
        writer,
        stream::iter(events.into_iter().map(Ok)),
        verified,
        1,
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .1
}

fn incremented(id: &str) -> eventsourcingdb::Event {
    stored(id, Incremented::EVENT_TYPE, json!({ "by": 1 }))
}

#[tokio::test]
async fn records_every_event() {
    let rebuilt = Seeing::default();
    drive_stored(
        &mut RebuildWriter::new(&rebuilt),
        vec![incremented("0"), incremented("1")],
    )
    .await
    .expect("the events are applied");

    assert_eq!(
        rebuilt.log.entries(),
        ["apply 0/0", "seen 0", "apply 1/0", "seen 1"]
    );

    let resumed = Seeing::default();
    drive_stored(
        &mut ResumableWriter::new(&resumed),
        vec![incremented("0"), incremented("1")],
    )
    .await
    .expect("the events are applied");

    assert_eq!(
        resumed.log.entries(),
        [
            "apply 0/0",
            "seen 0",
            "save 0",
            "apply 1/0",
            "seen 1",
            "save 1"
        ],
        "an event is recorded before its checkpoint is saved"
    );
}

#[tokio::test]
async fn also_records_what_the_projection_ignores() {
    // This is the whole reason for seen: a projection skips what does not
    // concern it, but a reader may be waiting for exactly that event.
    let projection = Seeing::default();

    drive_stored(
        &mut RebuildWriter::new(&projection),
        vec![stored("42", "io.thenativeweb.test.unrelated", json!({}))],
    )
    .await
    .expect("the event is skipped");

    assert_eq!(projection.log.entries(), ["seen 42"]);
}

#[tokio::test]
async fn records_a_split_event_once_every_part_is_applied() {
    // A reader that woke up after the first part could read an event that is
    // applied only in half.
    let projection = Seeing::default();

    drive_stored(
        &mut RebuildWriter::new(&projection),
        vec![stored("5", INCREMENTED_TWICE, json!({ "by": 1 }))],
    )
    .await
    .expect("the event is applied");

    assert_eq!(
        projection.log.entries(),
        ["apply 5/0", "apply 5/1", "seen 5"]
    );
}

#[tokio::test]
async fn does_not_record_a_failed_event() {
    let projection = Seeing {
        refuses: Some(1),
        ..Seeing::default()
    };

    let result = drive_stored(
        &mut RebuildWriter::new(&projection),
        vec![incremented("0"), incremented("1")],
    )
    .await;

    assert!(result.is_err(), "the event is refused");
    assert_eq!(
        projection.log.entries(),
        ["apply 0/0", "seen 0", "apply 1/0"],
        "recorded although applying failed"
    );
}

/// A projection that writes to three views, and records what it has seen in
/// each of them.
struct Library {
    books: InMemoryView<String, String>,
    shelves: InMemoryView<String, String>,
    readers: InMemoryView<String, String>,
}

impl Projection for Library {
    type Event = Counted;
    type Error = Refused;

    async fn apply(&self, _: Envelope<Counted>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        Ok(())
    }

    fn seen(&self, event_id: Revision) {
        for view in [&self.books, &self.shelves, &self.readers] {
            view.seen(event_id);
        }
    }
}

#[tokio::test]
async fn records_every_event_in_every_view() {
    let view = || InMemoryView::new(String::clone);
    let projection = Library {
        books: view(),
        shelves: view(),
        readers: view(),
    };

    drive_stored(
        &mut RebuildWriter::new(&projection),
        vec![incremented("3"), incremented("4")],
    )
    .await
    .expect("the events are applied");

    for (name, view) in [
        ("books", &projection.books),
        ("shelves", &projection.shelves),
        ("readers", &projection.readers),
    ] {
        assert_eq!(
            view.revision().now_or_never(),
            Some(Ok(Some(Revision::from(4)))),
            "the view {name} has to record the events"
        );
    }
}
