use chrono::{DateTime, TimeDelta, Utc};
use futures_util::stream;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::{
    Events, Projection, ProjectionError, TransactionalProjection,
    store::projecting::{RebuildWriter, TransactionalWriter, Writer, drive},
};

#[cfg(test)]
mod tests;

/// The source of the events a test stores.
const SOURCE: &str = "https://architecturekit.test";

/// An event in the shape the database hands it back, to drive a projection
/// without one (see [`project`]). It needs the feature `testing`.
///
/// Its fields are public, so that a test can give it what a typed event can
/// not express, such as a type the projection does not know, data it can not
/// read, or the time it was written at:
///
/// ```
/// # use architecturekit::{Event, Events};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, Event)]
/// # #[event(type = "io.thenativeweb.todo.remembered")]
/// # struct Remembered { intention: String }
/// # #[derive(Events)]
/// # enum ToDoEvent { Remembered(Remembered) }
/// use architecturekit::testing::StoredEvent;
///
/// let mut invented = StoredEvent::new(
///     "/todos/42",
///     "0",
///     &ToDoEvent::Remembered(Remembered { intention: "Buy milk".to_owned() }),
/// );
/// "io.thenativeweb.todo.invented".clone_into(&mut invented.event_type);
/// ```
///
/// It turns into an `eventsourcingdb::Event` with `into`, whose hash is no
/// real one, so verifying it fails.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    /// The ID of the event.
    pub id: String,
    /// When the event was written.
    pub time: DateTime<Utc>,
    /// The source of the event.
    pub source: String,
    /// The subject of the event.
    pub subject: String,
    /// The type of the event.
    pub event_type: String,
    /// The data of the event, as JSON.
    pub data: Value,
    /// The `traceparent` of the event, if it has one.
    pub trace_parent: Option<String>,
    /// The `tracestate` of the event, which counts only together with a
    /// `traceparent`, as for the database.
    pub trace_state: Option<String>,
}

impl StoredEvent {
    /// Turns a typed event into a stored one with the given subject and ID,
    /// written at the beginning of 1970, from the source
    /// `https://architecturekit.test`.
    ///
    /// # Panics
    ///
    /// Fails the test if the data of the event can not be encoded as JSON,
    /// since such an event could never be stored.
    #[must_use]
    #[track_caller]
    pub fn new<E: Events>(subject: &str, id: &str, event: &E) -> Self {
        let data = match event.to_data() {
            Ok(data) => data,
            Err(error) => panic!(
                "architecturekit: StoredEvent::new can not encode an event of type {:?}: {error}",
                event.event_type(),
            ),
        };

        Self {
            id: id.to_owned(),
            time: DateTime::UNIX_EPOCH,
            source: SOURCE.to_owned(),
            subject: subject.to_owned(),
            event_type: event.event_type().to_owned(),
            data,
            trace_parent: None,
            trace_state: None,
        }
    }
}

impl From<StoredEvent> for eventsourcingdb::Event {
    fn from(stored: StoredEvent) -> Self {
        let mut event = json!({
            "specversion": "1.0",
            "id": stored.id,
            "time": stored.time,
            "source": stored.source,
            "subject": stored.subject,
            "type": stored.event_type,
            "datacontenttype": "application/json",
            "data": stored.data,
            "hash": "0".repeat(64),
            "predecessorhash": "0".repeat(64),
        });
        if let Some(trace_parent) = stored.trace_parent {
            event["traceparent"] = Value::String(trace_parent);
        }
        if let Some(trace_state) = stored.trace_state {
            event["tracestate"] = Value::String(trace_state);
        }

        serde_json::from_value(event)
            .expect("the client SDK reads every event of this shape, whatever its values")
    }
}

/// Turns typed events into stored ones of the same subject, and numbers them
/// from zero, as the database does. It needs the feature `testing`.
///
/// # Panics
///
/// Fails the test if the data of an event can not be encoded as JSON.
#[must_use]
#[track_caller]
pub fn stored_events<E: Events>(
    subject: &str,
    events: impl IntoIterator<Item = E>,
) -> Vec<StoredEvent> {
    events
        .into_iter()
        .enumerate()
        .map(|(id, event)| StoredEvent::new(subject, &id.to_string(), &event))
        .collect()
}

/// Turns typed events into stored ones of the same subject, as the database
/// goes on with a history: numbered from the given ID on, and written one
/// minute apart from the given time on. Use it where a projection reads the
/// time of an event, or where the events of several subjects have to follow
/// one another, as they do in the database. It needs the feature `testing`.
///
/// # Panics
///
/// Fails the test if the data of an event can not be encoded as JSON.
#[must_use]
#[track_caller]
pub fn stored_events_at<E: Events>(
    subject: &str,
    first_id: u64,
    at: DateTime<Utc>,
    events: impl IntoIterator<Item = E>,
) -> Vec<StoredEvent> {
    events
        .into_iter()
        .zip(0_u32..)
        .map(|(event, offset)| {
            let id = first_id + u64::from(offset);
            let mut stored = StoredEvent::new(subject, &id.to_string(), &event);
            stored.time = at + TimeDelta::minutes(i64::from(offset));
            stored
        })
        .collect()
}

/// Applies stored events to a projection, one after another, as
/// [`Store::catch_up_projection`](crate::Store::catch_up_projection) does,
/// but without a database. It needs the feature `testing`.
///
/// The events take the same way as in a run: the upcasters of the projection
/// run, an event of a type the projection has no variant for is left out, an
/// event an upcaster splits reaches [`Projection::apply`] once per part, and
/// [`Projection::seen`] learns of every event. A resumable projection saves no
/// checkpoint, and the hashes of the events are not verified, since they are
/// no real ones.
///
/// A projection is driven asynchronously, so a test waits for it:
///
/// ```
/// # use architecturekit::{Envelope, Event, Events, InMemoryView, Projection, ViewError};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, Event)]
/// # #[event(type = "io.thenativeweb.library.book-acquired")]
/// # struct BookAcquired { title: String }
/// # #[derive(Events)]
/// # enum BookEvent { Acquired(BookAcquired) }
/// # struct BookList { titles: InMemoryView<String, String> }
/// # impl Projection for BookList {
/// #     type Event = BookEvent;
/// #     type Error = ViewError;
/// #     async fn apply(&self, event: Envelope<BookEvent>) -> Result<(), ViewError> {
/// #         let BookEvent::Acquired(acquired) = &event.data;
/// #         self.titles.insert(&event, acquired.title.clone())?;
/// #         Ok(())
/// #     }
/// # }
/// use architecturekit::{View, testing::{project, stored_events}};
/// use futures_util::TryStreamExt;
///
/// # let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
/// # runtime.block_on(async {
/// let book_list = BookList { titles: InMemoryView::new(String::clone) };
///
/// // In a #[tokio::test]:
/// project(
///     &book_list,
///     &stored_events("/books/42", [BookEvent::Acquired(BookAcquired { title: "2001".to_owned() })]),
/// )
/// .await
/// .expect("the events are applied");
///
/// let Ok(titles) = book_list.titles.all().try_collect::<Vec<_>>().await;
/// assert_eq!(titles, ["2001"]);
/// # });
/// ```
///
/// # Errors
///
/// Fails with the first failure, of an upcaster, of decoding an event, or of
/// the projection, which a test checks to see that a projection refuses an
/// event. A panic of the projection is not caught, and fails the test.
pub async fn project<P: Projection>(
    projection: &P,
    events: &[StoredEvent],
) -> Result<(), ProjectionError> {
    drive_through(&mut RebuildWriter::new(projection), events, 1).await
}

/// Applies stored events to a transactional projection within one
/// transaction, as
/// [`Store::catch_up_transactional_projection`](crate::Store::catch_up_transactional_projection)
/// does for a batch, but without a database: it commits with the ID of the
/// last event, and begins no transaction without events. It needs the
/// feature `testing`.
///
/// The events take the same way as with [`project`].
///
/// # Errors
///
/// Fails as [`project`] does, and if the transaction can not be begun or
/// committed. A transaction that fails is rolled back, and a failure of the
/// rollback is reported alongside the failure that caused it. A panic in
/// [`Transaction::apply`](crate::Transaction::apply) is rolled back as in a
/// run, and reported as [`ProjectionError::Panicked`].
pub async fn project_transactional<P: TransactionalProjection>(
    projection: &P,
    events: &[StoredEvent],
) -> Result<(), ProjectionError> {
    drive_through(
        &mut TransactionalWriter::new(projection),
        events,
        events.len(),
    )
    .await
}

/// Drives the events through the writer, in batches of the given size.
async fn drive_through<W: Writer>(
    writer: &mut W,
    events: &[StoredEvent],
    batch_size: usize,
) -> Result<(), ProjectionError> {
    let events = events.iter().cloned().map(|event| Ok(event.into()));

    drive(
        writer,
        stream::iter(events),
        |_| Ok(()),
        batch_size,
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .1
}
