// These tests mirror the tests of TestRevisionIn of ArchitectureKit for Go
// that need a database: the revision an item keeps fits a precondition on its
// subject only if the item follows every event of it.

use crate::{
    Envelope, EventOn, ExecuteError, InMemoryView, Precondition, Projection, Subjects, Transient,
    ViewError,
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
/// that the revision of the item follows the subject.
struct CounterItems {
    view: InMemoryView<String, CounterItem>,
    follows_resets: bool,
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

    let items = CounterItems {
        view: InMemoryView::with_revision_in(
            |item: &CounterItem| item.subject.clone(),
            |item| &mut item.revision,
        ),
        follows_resets,
    };
    store
        .catch_up_projection(&Subjects::exact(subject), &items)
        .await
        .expect("the projection catches up");

    let item = items.view.get(subject).expect("the counter is there");
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
