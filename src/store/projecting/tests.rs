// These tests mirror TestDrive, TestBatchSize, TestBoundAfter, TestWriters
// and TestResumableWriter of ArchitectureKit for Go, and those of
// TestPanickingProjection that concern a transaction, with streams made up
// by the tests. A projection names its kind by its trait, so TestModeOf has
// nothing to check, nor do the checks for nil of TestIsNil. A projection of a
// single function, as ProjectionFunc makes one in Go, needs the types of its
// events and its error in Rust, so it is an ordinary type there.
//
// The projections of the tests yield before they do anything, as a real one
// waits for its target, which lets other tasks run in the meantime.

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use eventsourcingdb::{error::ClientError, request_options::BoundType};
use futures_util::stream;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::{RebuildWriter, ResumableWriter, TransactionalWriter, Writer, bound_after, drive};
use crate::{
    Categorized, Category, Envelope, Failure, Permanent, Projection, ProjectionError,
    ResumableProjection, Transaction, TransactionalProjection, projection::tests::stored,
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

/// A projection that records what it is asked to do, and refuses the event
/// a test names.
#[derive(Default)]
struct Recording {
    log: Log,
    refuses: Option<&'static str>,
    batch_size: usize,
    checkpoint: Mutex<Option<String>>,
}

impl Projection for Recording {
    type Event = eventsourcingdb::Event;
    type Error = Refused;

    async fn apply(&self, event: Envelope<Self::Event>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push(format!("apply {}", event.id));
        if self.refuses == Some(event.id.as_str()) {
            return Err(Refused(Category::Domain));
        }

        Ok(())
    }
}

impl ResumableProjection for Recording {
    async fn checkpoint(&self) -> Result<Option<String>, Refused> {
        tokio::task::yield_now().await;
        Ok(self
            .checkpoint
            .lock()
            .expect("the checkpoint is not poisoned")
            .clone())
    }

    async fn save_checkpoint(&self, event_id: &str) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.push(format!("save {event_id}"));
        *self
            .checkpoint
            .lock()
            .expect("the checkpoint is not poisoned") = Some(event_id.to_owned());

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
    refuses_to_apply: Option<&'static str>,
    panics_in_apply: Option<&'static str>,
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

    async fn checkpoint(&self) -> Result<Option<String>, Refused> {
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
            self.trouble.panics_in_apply != Some(event.id.as_str()),
            "applying {}",
            event.id
        );
        if self.trouble.refuses_to_apply == Some(event.id.as_str()) {
            return Err(Refused(Category::Domain));
        }

        Ok(())
    }

    async fn commit(self, last_event_id: &str) -> Result<(), Refused> {
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
) -> (Option<String>, Result<(), ProjectionError>, Vec<String>) {
    let mut committed = Vec::new();
    let (last, result) = drive(
        writer,
        events(ids, failure),
        verified,
        batch_size,
        &CancellationToken::new(),
        |id| committed.push(id.to_owned()),
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
    assert_eq!(last.as_deref(), Some("1"));
    assert_eq!(committed, ["0", "1"]);
    assert_eq!(projection.log.entries(), ["apply 0", "apply 1"]);
    assert_eq!(
        writer.checkpoint().await.expect("checkpoint").as_deref(),
        Some("1")
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
        refuses_to_apply: Some("1"),
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
    assert_eq!(last.as_deref(), Some("0"));
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
        ("after a refusal", Some("0"), None, Category::Domain),
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
        let ProjectionError::RolledBack { cause, rollback } = &failure else {
            panic!("{name}: the rollback fails as well, got {failure:?}");
        };
        assert_eq!(cause.category(), Some(cause_category), "{name}");
        assert_eq!(rollback.category(), Some(Category::Permanent), "{name}");
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
        panics_in_apply: Some("1"),
        ..Trouble::default()
    });

    let (_, result, _) =
        drive_all(&mut TransactionalWriter::new(&ledger), &["0", "1"], None, 5).await;

    let failure = result.expect_err("the projection panics");
    assert!(failure.has_panicked(), "{failure:?}");
    assert_eq!(failure.category(), Some(Category::Permanent));
    assert_eq!(
        failure.to_string(),
        "permanent failure: the projection panicked: applying 1"
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
    let ProjectionError::RolledBack { cause, .. } = &failure else {
        panic!("the rollback is reported alongside its cause, got {failure:?}");
    };
    assert!(matches!(**cause, ProjectionError::Transient(_)));
    assert!(failure.has_panicked());
    assert_eq!(
        failure.category(),
        Some(Category::Permanent),
        "a panic ends the run even if the cause may pass"
    );
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
        writer.checkpoint().await.expect("checkpoint").as_deref(),
        Some("1")
    );
}

#[tokio::test]
async fn does_not_count_an_event_it_failed_to_apply() {
    let projection = Recording {
        refuses: Some("1"),
        ..Recording::default()
    };
    let mut writer = ResumableWriter::new(&projection);

    let (last, result, _) = drive_all(&mut writer, &["0", "1"], None, 5).await;

    assert!(result.is_err());
    assert_eq!(last.as_deref(), Some("0"));
    assert_eq!(
        writer.checkpoint().await.expect("checkpoint").as_deref(),
        Some("0")
    );
}

#[tokio::test]
async fn are_harmless_without_transactions() {
    let projection = Recording::default();

    let mut rebuild = RebuildWriter::new(&projection);
    rebuild.begin().await.expect("begin");
    rebuild.commit("0").await.expect("commit");
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
