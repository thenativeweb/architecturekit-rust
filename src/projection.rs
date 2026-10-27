use std::{error::Error, fmt, future::Future};

use chrono::{DateTime, Utc};

use crate::{Categorized, Category, Events, Failure, Permanent, Transient, UpcastEvent, Upcasters};

#[cfg(test)]
pub(crate) mod tests;

/// An event as a projection gets it: the metadata of the event as the database
/// recorded it, and its data, as [`Projection::Event`] says.
///
/// For a projection whose events are an enum with `#[derive(Events)]`, the
/// data is decoded into that enum, after the upcasters of the projection have
/// run. For one whose events are `eventsourcingdb::Event`, the data is the
/// event as it is stored, with its hash, and no upcasters run.
#[derive(Clone, Debug, PartialEq)]
pub struct Envelope<E> {
    /// The ID of the event, which is the ID of the stored event, also for every
    /// event an upcaster splits it into.
    pub id: String,
    /// When the event was written.
    pub time: DateTime<Utc>,
    /// The source of the event.
    pub source: String,
    /// The subject of the event.
    pub subject: String,
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
    /// Fails permanently if an upcaster fails, or if the data of an event does
    /// not match its type.
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
        let upcast = UpcastEvent::from_stored(stored);
        let parts = match upcasters {
            Some(upcasters) => upcasters.apply(upcast)?,
            None => vec![upcast],
        };

        let mut envelopes = Vec::new();
        for (part, event) in parts.into_iter().enumerate() {
            let (id, time, source, subject, event_type) = (
                event.id().to_owned(),
                *event.time(),
                event.source().to_owned(),
                event.subject().to_owned(),
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
            id: stored.id().to_owned(),
            time: *stored.time(),
            source: stored.source().to_owned(),
            subject: stored.subject().to_owned(),
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
}

/// A projection whose target can hold a checkpoint, but not together with the
/// data, so that it goes on from there on the next start. It writes the
/// checkpoint after the data, so an event may be applied twice after a crash,
/// and [`apply`](Projection::apply) has to be idempotent.
pub trait ResumableProjection: Projection {
    /// Returns the ID of the last event whose checkpoint was saved, or `None`
    /// if there is none yet.
    fn checkpoint(&self) -> impl Future<Output = Result<Option<String>, Self::Error>> + Send;

    /// Saves the ID of the last event that was applied.
    fn save_checkpoint(
        &self,
        event_id: &str,
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
    fn checkpoint(&self) -> impl Future<Output = Result<Option<String>, Self::Error>> + Send;

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
    fn commit(self, last_event_id: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Undoes the applied events.
    fn rollback(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Why a projection failed.
#[derive(Debug)]
pub enum ProjectionError {
    /// Reading the events failed, and trying again later may succeed.
    Transient(Transient),
    /// Reading the events failed, an event failed its verification or could
    /// not be decoded, or an upcaster failed, and trying again will not help.
    Permanent(Permanent),
    /// The projection panicked, with the given message. It is permanent, since
    /// it would panic again, also together with a failure that may pass.
    Panicked {
        /// The value the projection panicked with, if it is text.
        message: String,
    },
    /// The projection itself failed, with the category its error names.
    Projection {
        /// The category of the error.
        category: Option<Category>,
        /// The error of the projection, which a caller can downcast to its
        /// type.
        error: Box<dyn Error + Send + Sync + 'static>,
    },
    /// Rolling back a transaction failed after another failure, which the
    /// failure of the rollback is reported alongside, never instead of. Its
    /// category is the one of the failure that made it roll back.
    RolledBack {
        /// The failure that made the transaction roll back.
        cause: Box<ProjectionError>,
        /// The failure of the rollback.
        rollback: Box<ProjectionError>,
    },
}

impl ProjectionError {
    /// Wraps an error of a projection, keeping its category.
    pub(crate) fn of<E: Error + Categorized + Send + Sync + 'static>(error: E) -> Self {
        Self::Projection {
            category: error.category(),
            error: Box::new(error),
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
        }
    }

    /// Tells whether the projection panicked, also while it rolled back after
    /// another failure.
    pub(crate) fn has_panicked(&self) -> bool {
        match self {
            Self::Panicked { .. } => true,
            Self::RolledBack { cause, rollback } => cause.has_panicked() || rollback.has_panicked(),
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
            Self::Panicked { message } => write!(
                formatter,
                "{}: the projection panicked: {message}",
                Category::Permanent
            ),
            Self::Projection { error, .. } => error.fmt(formatter),
            Self::RolledBack { cause, rollback } => {
                write!(formatter, "{cause}, and rolling back failed: {rollback}")
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
            Self::RolledBack { cause, .. } => Some(cause.as_ref()),
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
            Self::RolledBack { rollback, .. } if rollback.has_panicked() => rollback.category(),
            Self::RolledBack { cause, .. } => cause.category(),
        }
    }
}
