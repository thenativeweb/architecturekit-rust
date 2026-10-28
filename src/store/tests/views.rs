// These tests check the revision that an item of a view keeps, against a
// database: it fits a precondition on its subject only if the item follows
// every event of it. They also check resuming from the checkpoint, and
// waiting for what execute wrote, for a projection that records what it has
// seen in its view.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::FutureExt;

use crate::{
    CancellationToken, Envelope, EventOn, ExecuteError, InMemoryView, KeyedView, Precondition,
    Projection, ProjectionOptions, ResumableProjection, Revisioned, Subjects, Transient, ViewError,
    wait_caught_up, wait_for_written,
};

use super::{
    CounterEvent, Incremented, Reset,
    commands::{Increment, counter_decider},
    database::{Database, subject_for},
};

/// The item of a view of counters, one per subject, which keeps its revision
/// for a precondition on that subject.
#[derive(Clone, Debug, Default, PartialEq)]
struct CounterItem {
    subject: String,
    total: i64,
    revision: String,
}

/// Keeps the total of every counter. If it follows resets, it handles them,
/// which change nothing about the item, with a change that does nothing, so
/// that the revision of the item follows the subject. It records every event
/// it has seen in its view, and keeps a checkpoint, if it is resumed.
struct CounterItems {
    view: InMemoryView<String, CounterItem>,
    follows_resets: bool,
    checkpoint: Mutex<Option<String>>,
}

impl CounterItems {
    fn new(follows_resets: bool) -> Self {
        Self {
            view: InMemoryView::with_revision_in(
                |item: &CounterItem| item.subject.clone(),
                |item| &mut item.revision,
            ),
            follows_resets,
            checkpoint: Mutex::new(None),
        }
    }

    fn checkpoint(&self) -> Option<String> {
        self.checkpoint.lock().expect("not poisoned").clone()
    }
}

impl Projection for CounterItems {
    type Event = CounterEvent;
    type Error = ViewError;

    async fn apply(&self, event: Envelope<CounterEvent>) -> Result<(), ViewError> {
        tokio::task::yield_now().await;

        match &event.data {
            CounterEvent::Incremented(incremented) => {
                let _ = self.view.upsert(event.subject.as_str(), &event, |item| {
                    item.subject.clone_from(&event.subject);
                    item.total += incremented.by;
                })?;
            }
            CounterEvent::Reset(_) if self.follows_resets => {
                let _ = self.view.update(event.subject.as_str(), &event, |_| {})?;
            }
            _ => {}
        }

        Ok(())
    }

    fn seen(&self, event_id: &str) {
        self.view.seen(event_id);
    }
}

impl ResumableProjection for CounterItems {
    async fn checkpoint(&self) -> Result<Option<String>, ViewError> {
        tokio::task::yield_now().await;
        Ok(CounterItems::checkpoint(self))
    }

    async fn save_checkpoint(&self, event_id: &str) -> Result<(), ViewError> {
        tokio::task::yield_now().await;
        *self.checkpoint.lock().expect("not poisoned") = Some(event_id.to_owned());

        Ok(())
    }
}

/// Writes an increment and a reset to a subject of the test, and projects them
/// into a view. Returns the item of the counter, and the IDs of the events it
/// wrote.
async fn project_increment_and_reset(
    database: &Database,
    subject: &str,
    follows_resets: bool,
) -> (CounterItem, Vec<String>) {
    let store = database.store().await;
    let written = store
        .write(
            vec![
                EventOn::new(subject, &Incremented { by: 2 }),
                EventOn::new(subject, &Reset {}),
            ],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the events are written");

    let items = CounterItems::new(follows_resets);
    store
        .catch_up_projection(&Subjects::exact(subject), &items)
        .await
        .expect("the projection catches up");

    let Ok(item) = items.view.get(subject).await;
    let item = item.expect("the counter is there");
    let ids = written.iter().map(|event| event.id().to_owned()).collect();

    (item, ids)
}

#[tokio::test]
async fn fits_a_precondition_on_the_subject_if_the_item_follows_every_event_of_it() {
    let database = Database::start().await;
    let subject = subject_for("revision-in-follows");

    let (item, written) = project_increment_and_reset(&database, &subject, true).await;

    assert_eq!(item.total, 2, "the reset must not change the item");
    assert_eq!(
        item.revision, written[1],
        "the revision has to follow the reset"
    );

    let executed = database
        .store()
        .await
        .execute(
            &counter_decider(),
            Increment::new(&subject, 1).requiring_event_id(&item.revision),
        )
        .await;
    assert!(
        executed.is_ok(),
        "the revision of the item has to fit the subject: {executed:?}"
    );
}

#[tokio::test]
async fn falls_behind_the_subject_if_the_projection_skips_an_event_of_it() {
    let database = Database::start().await;
    let subject = subject_for("revision-in-skips");

    let (item, written) = project_increment_and_reset(&database, &subject, false).await;

    assert_eq!(item.revision, written[0]);

    let executed = database
        .store()
        .await
        .execute(
            &counter_decider(),
            Increment::new(&subject, 1).requiring_event_id(&item.revision),
        )
        .await;
    assert!(
        matches!(
            executed,
            Err(ExecuteError::Transient(Transient::Conflict(_)))
        ),
        "a revision behind the subject has to conflict: {executed:?}"
    );
}

#[tokio::test]
async fn resumes_from_the_checkpoint() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("seen-resumes");
    let _ = store
        .write(
            (1..=3)
                .map(|by| EventOn::new(&subject, &Incremented { by }))
                .collect(),
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the events are written");

    let items = CounterItems::new(true);
    store
        .catch_up_resumable_projection(&Subjects::exact(&subject), &items)
        .await
        .expect("the projection catches up");

    let checkpoint = items.checkpoint();
    assert!(
        checkpoint.is_some(),
        "a resumable projection has to save its checkpoint"
    );
    assert_eq!(
        items.view.revision().now_or_never(),
        Some(Ok(checkpoint)),
        "the view has to have seen what the checkpoint says"
    );
}

#[tokio::test]
async fn waits_for_what_execute_wrote_in_a_view_whose_projection_records_what_it_has_seen() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("seen-execute");

    let items = Arc::new(CounterItems::new(true));
    let token = CancellationToken::new();
    let run = store.start_projection(
        Subjects::exact(&subject),
        Arc::clone(&items),
        token.clone(),
        ProjectionOptions::default(),
    );
    tokio::time::timeout(Duration::from_secs(30), wait_caught_up(&[&run]))
        .await
        .expect("the projection catches up in time")
        .expect("the projection catches up");

    let written = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");

    let waited = wait_for_written(&items.view, &written, Duration::from_secs(5)).await;
    assert!(matches!(waited, Ok(())), "{waited:?}");
    assert_eq!(
        items.view.wait_for(written[0].id()).now_or_never(),
        Some(Ok(())),
        "the view has to have seen the event"
    );

    token.cancel();
    run.done().await;
}
