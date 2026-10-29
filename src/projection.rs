use std::{error::Error, fmt, future::Future};

use chrono::{DateTime, Utc};

use crate::{
    Categorized, Category, Events, Failure, Permanent, Revision, Subject, Transient, UpcastEvent,
    Upcasters, revision::id_of, subject::subject_of,
};

mod run;
#[cfg(test)]
pub(crate) mod tests;

pub use run::{
    CaughtUpError, Phase, ProjectionOptions, ProjectionRun, ProjectionStatus, Reconnect,
    wait_caught_up,
};

/// An event as a projection gets it: the metadata of the event as the database
/// recorded it, and its data, as [`Projection::Event`] says.
///
/// For a projection whose events are an enum with `#[derive(Events)]`, the
/// data is decoded into that enum, after the upcasters of the projection have
/// run. For one whose events are `eventsourcingdb::Event`, the data is the
/// event as it is stored, with its hash, and no upcasters run.
///
/// It may gain fields, so it can not be built outside the kit with a literal.
/// A test that applies an event to a view directly builds one with
/// [`new`](Envelope::new), and changes its fields as it needs.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Envelope<E> {
    /// The ID of the event, which is the ID of the stored event, also for every
    /// event an upcaster splits it into.
    pub id: Revision,
    /// When the event was written.
    pub time: DateTime<Utc>,
    /// The source of the event.
    pub source: String,
    /// The subject of the event.
    pub subject: Subject,
    /// The type of the event, after the upcasters have run.
    pub event_type: String,
    /// The `traceparent` of the event, if it has one.
    pub trace_parent: Option<String>,
    /// The `tracestate` of the event, if it has one.
    pub trace_state: Option<String>,
    /// The position of the event among those an upcaster splits a stored event
    /// into, which all carry the ID of the stored one: 0 for the first, 1 for
    /// the second, and so on. An event that is not split is at position 0.
    pub part: usize,
    /// The data of the event.
    pub data: E,
}

/// The source of the events that a test makes up.
pub(crate) const MADE_UP_SOURCE: &str = "https://architecturekit.test";

impl<E: Events> Envelope<E> {
    /// An envelope of the given event, with the given subject and ID, as part
    /// 0, written at the beginning of 1970, from the source
    /// `https://architecturekit.test`, and without a trace, for a test that
    /// applies an event to a view directly:
    ///
    /// ```
    /// # use architecturekit::{Event, Events};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize, Deserialize, Event)]
    /// # #[event(type = "io.eventsourcingdb.library.book-acquired")]
    /// # struct BookAcquired { title: String }
    /// # #[derive(Events)]
    /// # enum BookEvent { Acquired(BookAcquired) }
    /// use architecturekit::{Envelope, InMemoryView, Subject};
    ///
    /// let titles = InMemoryView::new(String::clone);
    /// let envelope = Envelope::new(
    ///     Subject::new("/books/42"),
    ///     0,
    ///     BookEvent::Acquired(BookAcquired { title: "2001".to_owned() }),
    /// );
    /// let BookEvent::Acquired(acquired) = &envelope.data;
    /// titles.insert(&envelope, acquired.title.clone())?;
    /// # Ok::<(), architecturekit::ViewError>(())
    /// ```
    #[must_use]
    pub fn new(subject: Subject, id: impl Into<Revision>, data: E) -> Self {
        Self {
            id: id.into(),
            time: DateTime::UNIX_EPOCH,
            source: MADE_UP_SOURCE.to_owned(),
            subject,
            event_type: data.event_type().to_owned(),
            trace_parent: None,
            trace_state: None,
            part: 0,
            data,
        }
    }
}

mod sealed {
    pub trait Sealed {}

    impl<E: crate::Events + Send + 'static> Sealed for E {}

    impl Sealed for eventsourcingdb::Event {}
}

/// What a projection can get as its events: an enum with `#[derive(Events)]`,
/// or `eventsourcingdb::Event`, the event as it is stored. The kit implements
/// it for both, and for nothing else.
pub trait Projected: sealed::Sealed + Sized + Send + 'static {
    /// Turns a stored event into the envelopes the projection gets, after the
    /// given upcasters have run. An enum leaves out the events of types it has
    /// no variant for, since a projection usually reads far more events than
    /// it cares about.
    ///
    /// # Errors
    ///
    /// Fails permanently if the ID of the event is not a revision, or its
    /// subject not a subject, neither of which the database ever writes, if an
    /// upcaster fails, or if the data of an event does not match its type.
    #[doc(hidden)]
    fn project(
        stored: &eventsourcingdb::Event,
        upcasters: Option<&Upcasters>,
    ) -> Result<Vec<Envelope<Self>>, Permanent>;
}

impl<E: Events + Send + 'static> Projected for E {
    fn project(
        stored: &eventsourcingdb::Event,
        upcasters: Option<&Upcasters>,
    ) -> Result<Vec<Envelope<Self>>, Permanent> {
        let upcast = UpcastEvent::from_stored(stored)?;
        let parts = match upcasters {
            Some(upcasters) => upcasters.apply(upcast)?,
            None => vec![upcast],
        };

        let mut envelopes = Vec::new();
        for (part, event) in parts.into_iter().enumerate() {
            let (id, time, source, subject, event_type) = (
                event.id(),
                *event.time(),
                event.source().to_owned(),
                event.subject().clone(),
                event.event_type().to_owned(),
            );
            let data = match E::from_data(&event_type, event.into_data()) {
                None => continue,
                Some(Ok(data)) => data,
                Some(Err(cause)) => {
                    return Err(Permanent::Other(Failure::new(
                        format!("decoding event {id} of type {event_type:?}"),
                        Some(Box::new(cause)),
                    )));
                }
            };

            envelopes.push(Envelope {
                id,
                time,
                source,
                subject,
                event_type,
                trace_parent: stored.traceparent().map(ToOwned::to_owned),
                trace_state: stored.tracestate().map(ToOwned::to_owned),
                part,
                data,
            });
        }

        Ok(envelopes)
    }
}

impl Projected for eventsourcingdb::Event {
    fn project(
        stored: &eventsourcingdb::Event,
        _: Option<&Upcasters>,
    ) -> Result<Vec<Envelope<Self>>, Permanent> {
        Ok(vec![Envelope {
            id: id_of(stored)?,
            time: *stored.time(),
            source: stored.source().to_owned(),
            subject: subject_of(stored)?,
            event_type: stored.ty().to_owned(),
            trace_parent: stored.traceparent().map(ToOwned::to_owned),
            trace_state: stored.tracestate().map(ToOwned::to_owned),
            part: 0,
            data: stored.clone(),
        }])
    }
}

/// A projection that applies events, and is rebuilt from the first event on
/// every start, which is right for a view kept in memory. A projection whose
/// target can hold a checkpoint is a [`ResumableProjection`] as well, and one
/// whose target can make data and checkpoint durable together is a
/// [`TransactionalProjection`] instead.
///
/// Its events are an enum with `#[derive(Events)]`, decoded after its
/// upcasters have run, which leaves out the events of types the enum has no
/// variant for, or `eventsourcingdb::Event`, to get every event as it is
/// stored, with its hash.
///
/// A projection applies events through `&self`, since it usually shares what
/// it builds with those who read it, such as a view.
pub trait Projection: Send + Sync + 'static {
    /// The events the projection applies.
    type Event: Projected;

    /// The error with which applying an event fails. Its category decides
    /// what happens then: a transient one is tried again, after the projection
    /// has caught up anew from its checkpoint, any other one ends the run.
    type Error: Error + Categorized + Send + Sync + 'static;

    /// Applies an event.
    fn apply(
        &self,
        event: Envelope<Self::Event>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Returns the upcasters that the stored events run through before
    /// [`apply`](Projection::apply) sees them, as
    /// [`State::upcasters`](crate::State::upcasters) does for a state. The
    /// default is none. They do not run for `eventsourcingdb::Event`.
    #[must_use]
    fn upcasters() -> Option<&'static Upcasters> {
        None
    }

    /// Records that the projection has seen a stored event, once it has
    /// applied every event an upcaster split it into. The default does
    /// nothing.
    ///
    /// A projection that writes to views records the event in each of them,
    /// so that a reader can wait until they have caught up (see
    /// [`Revisioned`](crate::Revisioned)):
    ///
    /// ```
    /// use architecturekit::{
    ///     Envelope, Event, Events, InMemoryView, Projection, Revision, ViewError,
    /// };
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Serialize, Deserialize, Event)]
    /// #[event(type = "io.thenativeweb.library.book-acquired")]
    /// struct BookAcquired {
    ///     id: String,
    ///     title: String,
    /// }
    ///
    /// #[derive(Debug, Events)]
    /// enum BookEvent {
    ///     Acquired(BookAcquired),
    /// }
    ///
    /// #[derive(Clone, Debug, Default)]
    /// struct Book {
    ///     id: String,
    ///     title: String,
    /// }
    ///
    /// struct BookList {
    ///     books: InMemoryView<String, Book>,
    /// }
    ///
    /// impl Projection for BookList {
    ///     type Event = BookEvent;
    ///     type Error = ViewError;
    ///
    ///     async fn apply(&self, event: Envelope<BookEvent>) -> Result<(), ViewError> {
    ///         let BookEvent::Acquired(acquired) = &event.data;
    ///         let book = Book {
    ///             id: acquired.id.clone(),
    ///             title: acquired.title.clone(),
    ///         };
    ///         self.books.insert(&event, book)?;
    ///
    ///         Ok(())
    ///     }
    ///
    ///     fn seen(&self, event_id: Revision) {
    ///         self.books.seen(event_id);
    ///     }
    /// }
    /// ```
    ///
    /// It is called for every stored event that reaches the projection, also
    /// for one of a type the projection has no variant for, and that is the
    /// point: a projection skips what does not concern it, but a reader may
    /// be waiting for exactly that event. A view whose revision only counted
    /// the events it applied would leave such a reader waiting. It is not
    /// called for an event that failed, and it is called before the
    /// checkpoint of a batch is saved.
    ///
    /// A [`TransactionalProjection`] has no such method, since where data and
    /// revision have to become durable together, the revision belongs inside
    /// the transaction, and the projection writes it itself.
    fn seen(&self, event_id: Revision) {
        let _ = event_id;
    }
}

/// A projection whose target can hold a checkpoint, but not together with the
/// data, so that it goes on from there on the next start. It writes the
/// checkpoint after the data, so an event may be applied twice after a crash,
/// and [`apply`](Projection::apply) has to be idempotent.
pub trait ResumableProjection: Projection {
    /// Returns the ID of the last event whose checkpoint was saved, or `None`
    /// if there is none yet.
    fn checkpoint(&self) -> impl Future<Output = Result<Option<Revision>, Self::Error>> + Send;

    /// Saves the ID of the last event that was applied.
    fn save_checkpoint(
        &self,
        event_id: Revision,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Returns how many events are applied while catching up before the
    /// checkpoint is saved. The default is one, which works for projections
    /// that are not idempotent. Raising it speeds up a rebuild by orders of
    /// magnitude, at the price of applying up to that many events a second
    /// time after a crash. Once the projection has caught up, it saves the
    /// checkpoint after every event, whatever the size, since a batch that
    /// waited to fill up would hold back events that have already arrived. A
    /// size of 0 counts as 1.
    #[must_use]
    fn catch_up_batch_size(&self) -> usize {
        1
    }
}

/// A projection whose target can make data and checkpoint durable together, as
/// a relational database can. It applies events only within a
/// [`Transaction`].
pub trait TransactionalProjection: Send + Sync + 'static {
    /// The events the projection applies, as for [`Projection::Event`].
    type Event: Projected;

    /// The error with which the projection fails, as for
    /// [`Projection::Error`].
    type Error: Error + Categorized + Send + Sync + 'static;

    /// One unit of work of the projection.
    type Transaction: Transaction<Event = Self::Event, Error = Self::Error>;

    /// Returns the ID of the last event that was committed, or `None` if there
    /// is none yet.
    fn checkpoint(&self) -> impl Future<Output = Result<Option<Revision>, Self::Error>> + Send;

    /// Begins a transaction.
    fn begin(&self) -> impl Future<Output = Result<Self::Transaction, Self::Error>> + Send;

    /// Returns the upcasters, as for [`Projection::upcasters`].
    #[must_use]
    fn upcasters() -> Option<&'static Upcasters> {
        None
    }

    /// Returns how many events are applied within one transaction while
    /// catching up, as for [`ResumableProjection::catch_up_batch_size`]. A
    /// larger size makes a larger transaction.
    #[must_use]
    fn catch_up_batch_size(&self) -> usize {
        1
    }
}

/// One unit of work of a [`TransactionalProjection`]. Commit has to make the
/// applied events and the ID of the last one durable together, or neither.
/// Rollback is not called after commit fails, so commit has to roll back
/// itself if it can not finish. A panic in apply is rolled back like an error.
pub trait Transaction: Send {
    /// The events the transaction applies.
    type Event;

    /// The error with which the transaction fails.
    type Error;

    /// Applies an event within the transaction.
    fn apply(
        &mut self,
        event: Envelope<Self::Event>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Makes the applied events and the ID of the last one durable together.
    fn commit(
        self,
        last_event_id: Revision,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Undoes the applied events.
    fn rollback(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Why a projection failed. It may gain variants and fields, so match it with
/// a catch-all arm, or ask for its category.
#[derive(Debug)]
#[non_exhaustive]
pub enum ProjectionError {
    /// Reading the events failed, and trying again later may succeed.
    Transient(Transient),
    /// Reading the events failed, an event failed its verification, could not
    /// be decoded, or has an ID that is not a revision or a subject that is not
    /// one, or an upcaster failed, and trying again will not help.
    Permanent(Permanent),
    /// The projection panicked, with the given message. It is permanent, since
    /// it would panic again, also together with a failure that may pass.
    #[non_exhaustive]
    Panicked {
        /// The value the projection panicked with, if it is text.
        message: String,
        /// The event the projection was applying, if it panicked while it
        /// applied one, rather than at a checkpoint or a transaction.
        event_id: Option<Revision>,
    },
    /// The projection itself failed, with the category its error names.
    #[non_exhaustive]
    Projection {
        /// The category of the error.
        category: Option<Category>,
        /// The error of the projection, which a caller can downcast to its
        /// type.
        error: Box<dyn Error + Send + Sync + 'static>,
        /// The event the projection was applying, if it failed while it
        /// applied one, rather than at a checkpoint or a transaction.
        event_id: Option<Revision>,
    },
    /// Rolling back a transaction failed after another failure, which the
    /// failure of the rollback is reported alongside, never instead of. Its
    /// category is the one of the failure that made it roll back.
    #[non_exhaustive]
    RollbackFailed {
        /// The failure that made the transaction roll back.
        cause: Box<ProjectionError>,
        /// The failure of the rollback.
        error: Box<ProjectionError>,
    },
}

impl ProjectionError {
    /// Wraps an error of a projection, keeping its category.
    pub(crate) fn of<E: Error + Categorized + Send + Sync + 'static>(error: E) -> Self {
        Self::Projection {
            category: error.category(),
            error: Box::new(error),
            event_id: None,
        }
    }

    /// Names the event the projection was applying when it failed or panicked,
    /// which a failure of anything else does not concern.
    pub(crate) fn at_event(self, id: Revision) -> Self {
        match self {
            Self::Panicked { message, .. } => Self::Panicked {
                message,
                event_id: Some(id),
            },
            Self::Projection {
                category, error, ..
            } => Self::Projection {
                category,
                error,
                event_id: Some(id),
            },
            other => other,
        }
    }

    /// Reports a panic of the projection.
    pub(crate) fn panicked(payload: &(dyn std::any::Any + Send)) -> Self {
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("a value that is no text");

        Self::Panicked {
            message: message.to_owned(),
            event_id: None,
        }
    }

    /// Tells whether the projection panicked, also while it rolled back after
    /// another failure.
    pub(crate) fn has_panicked(&self) -> bool {
        match self {
            Self::Panicked { .. } => true,
            Self::RollbackFailed { cause, error } => cause.has_panicked() || error.has_panicked(),
            Self::Transient(_) | Self::Permanent(_) | Self::Projection { .. } => false,
        }
    }
}

impl From<Permanent> for ProjectionError {
    fn from(failure: Permanent) -> Self {
        Self::Permanent(failure)
    }
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient(failure) => failure.fmt(formatter),
            Self::Permanent(failure) => failure.fmt(formatter),
            Self::Panicked {
                message,
                event_id: Some(id),
            } => write!(
                formatter,
                "{}: applying event {id}: the projection panicked: {message}",
                Category::Permanent
            ),
            Self::Panicked {
                message,
                event_id: None,
            } => write!(
                formatter,
                "{}: the projection panicked: {message}",
                Category::Permanent
            ),
            Self::Projection {
                error,
                event_id: Some(id),
                ..
            } => write!(formatter, "applying event {id}: {error}"),
            Self::Projection {
                error,
                event_id: None,
                ..
            } => error.fmt(formatter),
            Self::RollbackFailed { cause, error } => {
                write!(formatter, "{cause}, and rolling back failed: {error}")
            }
        }
    }
}

impl Error for ProjectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transient(failure) => failure.source(),
            Self::Permanent(failure) => failure.source(),
            Self::Panicked { .. } => None,
            Self::Projection { error, .. } => error.source(),
            // The text names the cause already, so this passes on its source.
            Self::RollbackFailed { cause, .. } => cause.source(),
        }
    }
}

impl Categorized for ProjectionError {
    fn category(&self) -> Option<Category> {
        match self {
            Self::Transient(failure) => failure.category(),
            Self::Permanent(failure) => failure.category(),
            Self::Panicked { .. } => Some(Category::Permanent),
            Self::Projection { category, .. } => *category,
            Self::RollbackFailed { error, .. } if error.has_panicked() => error.category(),
            Self::RollbackFailed { cause, .. } => cause.category(),
        }
    }
}
