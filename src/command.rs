use std::{any, error::Error, fmt};

use crate::{Categorized, Category, Failure, NotARevision, Permanent, parse_revision};

#[cfg(test)]
mod tests;

/// A command, which knows the subject it acts on, and the conditions under
/// which its events may be written.
///
/// The preconditions are where optimistic concurrency, idempotency and
/// uniqueness live. Every command declares at least one, and the kit adds none
/// of its own:
///
/// ```
/// use architecturekit::{Command, Precondition};
///
/// #[derive(Clone)]
/// struct AcquireBook {
///     book_id: String,
///     title: String,
/// }
///
/// impl Command for AcquireBook {
///     fn subject(&self) -> String {
///         format!("/books/{}", self.book_id)
///     }
///
///     fn preconditions(&self) -> Vec<Precondition> {
///         vec![Precondition::on_state_read()]
///     }
/// }
/// ```
pub trait Command {
    /// Returns the subject the command acts on.
    fn subject(&self) -> String;

    /// Returns the conditions under which the events of the command may be
    /// written, at least one of them.
    fn preconditions(&self) -> Vec<Precondition>;
}

/// A condition that must hold for the events of a command to be written.
///
/// Every command declares at least one, so that writing without any check is
/// always a decision, never an oversight. Create one with
/// [`on_pristine_subject`](Precondition::on_pristine_subject),
/// [`on_populated_subject`](Precondition::on_populated_subject),
/// [`on_event_id`](Precondition::on_event_id),
/// [`on_state_read`](Precondition::on_state_read), or
/// [`unconditionally`](Precondition::unconditionally), or with
/// [`require`](Precondition::require) for any other precondition of the client
/// SDK, such as an EventQL query.
#[derive(Debug)]
pub struct Precondition {
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    Required(eventsourcingdb::Precondition),
    // The ID is kept apart as well, since it usually comes from the caller and
    // is checked before anything is read.
    OnEventId {
        event_id: String,
        database: eventsourcingdb::Precondition,
    },
    OnStateRead,
    Unconditionally,
}

impl Precondition {
    /// Makes a precondition of the client SDK one of the command, for one that
    /// the kit has no function of its own for, such as an EventQL query:
    ///
    /// ```
    /// use architecturekit::Precondition;
    ///
    /// let precondition = Precondition::require(eventsourcingdb::Precondition::is_eventql_query_true(
    ///     "FROM e IN events WHERE e.type == 'io.eventsourcingdb.library.book-acquired' PROJECT INTO COUNT() < 1000",
    /// ));
    /// ```
    #[must_use]
    pub fn require(precondition: eventsourcingdb::Precondition) -> Self {
        Self {
            kind: Kind::Required(precondition),
        }
    }

    /// Lets the events be written only if the subject has no events yet, which
    /// is what a command that creates something usually declares. It is the
    /// same as [`require`](Precondition::require) with
    /// `eventsourcingdb::Precondition::is_subject_pristine`.
    #[must_use]
    pub fn on_pristine_subject(subject: impl Into<String>) -> Self {
        Self::require(eventsourcingdb::Precondition::is_subject_pristine(
            subject, None,
        ))
    }

    /// Lets the events be written only if the subject has events already, for
    /// a command that acts on something that has to exist. It is the same as
    /// [`require`](Precondition::require) with
    /// `eventsourcingdb::Precondition::is_subject_populated`.
    ///
    /// It needs EventSourcingDB 1.2 or later, which added this precondition.
    /// An older database refuses the write, which then fails with a permanent
    /// failure.
    #[must_use]
    pub fn on_populated_subject(subject: impl Into<String>) -> Self {
        Self::require(eventsourcingdb::Precondition::is_subject_populated(
            subject, None,
        ))
    }

    /// Lets the events be written only if the last event of the subject is the
    /// one with the given ID, for a command that was decided on a revision the
    /// caller hands over.
    ///
    /// The ID usually comes from the caller, so it is checked with
    /// [`parse_revision`] before anything is read, rather than left to the
    /// database, which would refuse it as a malformed request. For the
    /// revision of the state that the command was decided on, use
    /// [`on_state_read`](Precondition::on_state_read) instead.
    #[must_use]
    pub fn on_event_id(subject: impl Into<String>, event_id: impl Into<String>) -> Self {
        let event_id = event_id.into();
        let database =
            eventsourcingdb::Precondition::is_subject_on_event_id(subject, event_id.clone(), None);

        Self {
            kind: Kind::OnEventId { event_id, database },
        }
    }

    /// Lets the events be written only if nothing has been written to the
    /// subject since the state the command was decided on was read. For a
    /// subject without events, it requires the subject to still be pristine.
    ///
    /// Use it when the command decides on the state that is read for it,
    /// rather than on a revision the caller hands over.
    #[must_use]
    pub fn on_state_read() -> Self {
        Self {
            kind: Kind::OnStateRead,
        }
    }

    /// Writes the events whatever has been written to the subject in the
    /// meantime. It can not be combined with other preconditions.
    #[must_use]
    pub fn unconditionally() -> Self {
        Self {
            kind: Kind::Unconditionally,
        }
    }

    /// Returns the precondition of the client SDK that the database checks for
    /// this one: the one [`require`](Precondition::require) made it from, which
    /// is how [`on_pristine_subject`](Precondition::on_pristine_subject) and
    /// [`on_populated_subject`](Precondition::on_populated_subject) make
    /// theirs, or the revision check of
    /// [`on_event_id`](Precondition::on_event_id). For any other, it returns
    /// `None`.
    #[must_use]
    pub fn database(&self) -> Option<&eventsourcingdb::Precondition> {
        match &self.kind {
            Kind::Required(database) | Kind::OnEventId { database, .. } => Some(database),
            Kind::OnStateRead | Kind::Unconditionally => None,
        }
    }

    /// Tells whether this precondition was made with
    /// [`on_state_read`](Precondition::on_state_read).
    #[must_use]
    pub fn is_on_state_read(&self) -> bool {
        matches!(self.kind, Kind::OnStateRead)
    }

    /// Tells whether this precondition was made with
    /// [`unconditionally`](Precondition::unconditionally).
    #[must_use]
    pub fn is_unconditional(&self) -> bool {
        matches!(self.kind, Kind::Unconditionally)
    }
}

/// Checks that a command declares its preconditions in a way they can be
/// honored, before anything is read, as executing a command does.
///
/// # Errors
///
/// Fails with a permanent failure if the command declares none at all, or
/// [`Precondition::unconditionally`] together with others. If nothing of that
/// is wrong, it fails with [`NotARevision`] for the first ID of
/// [`Precondition::on_event_id`] that is not a revision, so that a mistake in
/// the code is not hidden behind one of the caller.
pub fn check_preconditions<C: Command + ?Sized>(command: &C) -> Result<(), PreconditionsError> {
    check(&command.preconditions(), any::type_name::<C>())
}

fn check(declared: &[Precondition], command: &str) -> Result<(), PreconditionsError> {
    let refuse = |message: String| {
        Err(PreconditionsError::Permanent(Permanent::Other(
            Failure::new(message, None),
        )))
    };

    if declared.is_empty() {
        return refuse(format!(
            "{command} declares no preconditions, use Precondition::unconditionally to write without any",
        ));
    }
    if declared.len() > 1 && declared.iter().any(Precondition::is_unconditional) {
        return refuse(format!(
            "{command} combines Precondition::unconditionally with other preconditions",
        ));
    }

    for precondition in declared {
        if let Kind::OnEventId { event_id, .. } = &precondition.kind {
            let _ = parse_revision(event_id).map_err(PreconditionsError::NotARevision)?;
        }
    }

    Ok(())
}

/// Why a command declares its preconditions in a way they can not be honored
/// (see [`check_preconditions`]).
#[derive(Debug)]
pub enum PreconditionsError {
    /// The command declares none, or
    /// [`Precondition::unconditionally`] together with others, which is a
    /// mistake in the code.
    Permanent(Permanent),
    /// An ID of [`Precondition::on_event_id`] is not a revision, which is
    /// usually a mistake of the caller.
    NotARevision(NotARevision),
}

impl fmt::Display for PreconditionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Permanent(failure) => failure.fmt(formatter),
            Self::NotARevision(refusal) => refusal.fmt(formatter),
        }
    }
}

impl Error for PreconditionsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Permanent(failure) => failure.source(),
            Self::NotARevision(refusal) => refusal.source(),
        }
    }
}

impl Categorized for PreconditionsError {
    fn category(&self) -> Option<Category> {
        match self {
            Self::Permanent(failure) => failure.category(),
            Self::NotARevision(refusal) => refusal.category(),
        }
    }
}
