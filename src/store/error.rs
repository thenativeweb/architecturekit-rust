use std::{error::Error, fmt};

use crate::{
    Categorized, Category, Failure, NotARevision, Permanent, PreconditionsError, Transient,
};

use super::failure::{Sorted, WriteSorted};

/// Shows a write whose outcome is unknown, in the words of ArchitectureKit for
/// Go.
pub(super) fn fmt_outcome_unknown(
    failure: &Failure,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    write!(formatter, "outcome unknown: {}", failure.message())
}

pub(super) fn source_of_outcome_unknown(failure: &Failure) -> Option<&(dyn Error + 'static)> {
    let cause: &(dyn Error + 'static) = failure.cause()?;
    Some(cause)
}

/// Why loading a state failed.
#[derive(Debug)]
pub enum LoadError {
    /// Trying again later may succeed, for example once the database can be
    /// reached again.
    Transient(Transient),
    /// Trying again will not help, for example since an event failed its
    /// verification, or its data does not match its type.
    Permanent(Permanent),
}

impl From<Sorted> for LoadError {
    fn from(sorted: Sorted) -> Self {
        match sorted {
            Sorted::Transient(failure) => Self::Transient(failure),
            Sorted::Permanent(failure) => Self::Permanent(failure),
        }
    }
}

impl From<Permanent> for LoadError {
    fn from(failure: Permanent) -> Self {
        Self::Permanent(failure)
    }
}

impl fmt::Display for LoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient(failure) => failure.fmt(formatter),
            Self::Permanent(failure) => failure.fmt(formatter),
        }
    }
}

impl Error for LoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transient(failure) => failure.source(),
            Self::Permanent(failure) => failure.source(),
        }
    }
}

impl Categorized for LoadError {
    fn category(&self) -> Option<Category> {
        match self {
            Self::Transient(failure) => failure.category(),
            Self::Permanent(failure) => failure.category(),
        }
    }
}

/// Why writing events failed.
#[derive(Debug)]
pub enum WriteError {
    /// The write certainly stored nothing, and trying it again later may
    /// succeed. A precondition that did not hold is a
    /// [`Transient::Conflict`].
    Transient(Transient),
    /// The write certainly stored nothing, and trying it again will not help,
    /// for example since it declares its preconditions wrongly, or since an
    /// event does not match its schema.
    Permanent(Permanent),
    /// The write failed in a way that leaves open whether the database stored
    /// the events, so trying it again unchanged may store them twice. Find out
    /// first, for example by reading the subject, or try again only with a
    /// precondition that refuses the same events a second time, such as
    /// [`Precondition::on_event_id`](crate::Precondition::on_event_id). It
    /// belongs to no category.
    OutcomeUnknown(Failure),
    /// An ID of [`Precondition::on_event_id`](crate::Precondition::on_event_id)
    /// is not a revision, so nothing was written. It belongs to no category.
    NotARevision(NotARevision),
}

impl From<WriteSorted> for WriteError {
    fn from(sorted: WriteSorted) -> Self {
        match sorted {
            WriteSorted::Sorted(Sorted::Transient(failure)) => Self::Transient(failure),
            WriteSorted::Sorted(Sorted::Permanent(failure)) => Self::Permanent(failure),
            WriteSorted::OutcomeUnknown(failure) => Self::OutcomeUnknown(failure),
        }
    }
}

impl From<PreconditionsError> for WriteError {
    fn from(refusal: PreconditionsError) -> Self {
        match refusal {
            PreconditionsError::Permanent(failure) => Self::Permanent(failure),
            PreconditionsError::NotARevision(refusal) => Self::NotARevision(refusal),
        }
    }
}

impl From<Permanent> for WriteError {
    fn from(failure: Permanent) -> Self {
        Self::Permanent(failure)
    }
}

impl fmt::Display for WriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient(failure) => failure.fmt(formatter),
            Self::Permanent(failure) => failure.fmt(formatter),
            Self::OutcomeUnknown(failure) => fmt_outcome_unknown(failure, formatter),
            Self::NotARevision(refusal) => refusal.fmt(formatter),
        }
    }
}

impl Error for WriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transient(failure) => failure.source(),
            Self::Permanent(failure) => failure.source(),
            Self::OutcomeUnknown(failure) => source_of_outcome_unknown(failure),
            Self::NotARevision(refusal) => refusal.source(),
        }
    }
}

impl Categorized for WriteError {
    fn category(&self) -> Option<Category> {
        match self {
            Self::Transient(failure) => failure.category(),
            Self::Permanent(failure) => failure.category(),
            Self::OutcomeUnknown(_) | Self::NotARevision(_) => None,
        }
    }
}

/// Why registering the schemas of events failed.
#[derive(Debug)]
pub enum RegisterSchemasError {
    /// Trying again later may succeed, for example once the database can be
    /// reached again.
    Transient(Transient),
    /// Trying again will not help, for example since a registered schema
    /// differs from the given one, which can not change.
    Permanent(Permanent),
}

impl From<Sorted> for RegisterSchemasError {
    fn from(sorted: Sorted) -> Self {
        match sorted {
            Sorted::Transient(failure) => Self::Transient(failure),
            Sorted::Permanent(failure) => Self::Permanent(failure),
        }
    }
}

impl From<Permanent> for RegisterSchemasError {
    fn from(failure: Permanent) -> Self {
        Self::Permanent(failure)
    }
}

impl fmt::Display for RegisterSchemasError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient(failure) => failure.fmt(formatter),
            Self::Permanent(failure) => failure.fmt(formatter),
        }
    }
}

impl Error for RegisterSchemasError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transient(failure) => failure.source(),
            Self::Permanent(failure) => failure.source(),
        }
    }
}

impl Categorized for RegisterSchemasError {
    fn category(&self) -> Option<Category> {
        match self {
            Self::Transient(failure) => failure.category(),
            Self::Permanent(failure) => failure.category(),
        }
    }
}

/// Why executing a command failed, where `D` is the error of its decider.
///
/// It has exactly the cases executing can end in, so that a `match` without a
/// catch-all arm handles all of them, in particular
/// [`OutcomeUnknown`](ExecuteError::OutcomeUnknown), which must not be tried
/// again blindly. To ask for the category instead, as ArchitectureKit for Go
/// recommends, use [`Categorized::category`].
#[derive(Debug)]
pub enum ExecuteError<D> {
    /// The decider refused the command, or failed while it decided, for
    /// example while loading something else. Its category is the one the error
    /// of the decider names.
    Decider(D),
    /// Nothing was written, and trying again later may succeed. A precondition
    /// that did not hold is a [`Transient::Conflict`].
    Transient(Transient),
    /// Nothing was written, and trying again will not help.
    Permanent(Permanent),
    /// The write failed in a way that leaves open whether the database stored
    /// the events, so trying it again unchanged may store them twice. Find out
    /// first, or try again only with a precondition that refuses the same
    /// events a second time, such as
    /// [`Precondition::on_event_id`](crate::Precondition::on_event_id) with the
    /// revision the command was decided on. The kit never tries it again by
    /// itself, not even with conflict retries. It belongs to no category.
    OutcomeUnknown(Failure),
    /// An ID of [`Precondition::on_event_id`](crate::Precondition::on_event_id)
    /// is not a revision, so nothing was read or written. It belongs to no
    /// category.
    NotARevision(NotARevision),
}

impl<D> From<LoadError> for ExecuteError<D> {
    fn from(failure: LoadError) -> Self {
        match failure {
            LoadError::Transient(failure) => Self::Transient(failure),
            LoadError::Permanent(failure) => Self::Permanent(failure),
        }
    }
}

impl<D> From<WriteError> for ExecuteError<D> {
    fn from(failure: WriteError) -> Self {
        match failure {
            WriteError::Transient(failure) => Self::Transient(failure),
            WriteError::Permanent(failure) => Self::Permanent(failure),
            WriteError::OutcomeUnknown(failure) => Self::OutcomeUnknown(failure),
            WriteError::NotARevision(refusal) => Self::NotARevision(refusal),
        }
    }
}

impl<D> From<PreconditionsError> for ExecuteError<D> {
    fn from(refusal: PreconditionsError) -> Self {
        match refusal {
            PreconditionsError::Permanent(failure) => Self::Permanent(failure),
            PreconditionsError::NotARevision(refusal) => Self::NotARevision(refusal),
        }
    }
}

impl<D> From<Permanent> for ExecuteError<D> {
    fn from(failure: Permanent) -> Self {
        Self::Permanent(failure)
    }
}

impl<D: fmt::Display> fmt::Display for ExecuteError<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decider(failure) => failure.fmt(formatter),
            Self::Transient(failure) => failure.fmt(formatter),
            Self::Permanent(failure) => failure.fmt(formatter),
            Self::OutcomeUnknown(failure) => fmt_outcome_unknown(failure, formatter),
            Self::NotARevision(refusal) => refusal.fmt(formatter),
        }
    }
}

impl<D: Error + 'static> Error for ExecuteError<D> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decider(failure) => failure.source(),
            Self::Transient(failure) => failure.source(),
            Self::Permanent(failure) => failure.source(),
            Self::OutcomeUnknown(failure) => source_of_outcome_unknown(failure),
            Self::NotARevision(refusal) => refusal.source(),
        }
    }
}

impl<D: Categorized> Categorized for ExecuteError<D> {
    fn category(&self) -> Option<Category> {
        match self {
            Self::Decider(failure) => failure.category(),
            Self::Transient(failure) => failure.category(),
            Self::Permanent(failure) => failure.category(),
            Self::OutcomeUnknown(_) | Self::NotARevision(_) => None,
        }
    }
}
