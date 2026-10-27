use std::{error::Error as StdError, fmt};

use crate::{
    Categorized, Category, EmptyRange, Failure, NotARevision, Permanent, PreconditionsError,
    ReadOptionsError, Transient,
};

use super::error::{
    ExecuteError, LoadError, ReadError, RegisterSchemasError, WriteError, fmt_outcome_unknown,
    source_of_outcome_unknown,
};

/// Any failure of architecturekit, where `D` is the error of a decider, so that
/// a function that calls several operations can pass their failures on with
/// `?`.
///
/// Every error of an operation turns into it. It holds all the cases of those,
/// so a `match` on it needs a catch-all arm, and new cases may come with new
/// operations. Where the cases of a single operation matter, match on its own
/// error before it turns into this one.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error<D> {
    /// See [`ExecuteError::Decider`].
    Decider(D),
    /// A failure of the category [`Category::Transient`].
    Transient(Transient),
    /// A failure of the category [`Category::Permanent`].
    Permanent(Permanent),
    /// See [`ExecuteError::OutcomeUnknown`].
    OutcomeUnknown(Failure),
    /// See [`ExecuteError::NotARevision`].
    NotARevision(NotARevision),
    /// See [`ReadError::EmptyRange`].
    EmptyRange(EmptyRange),
}

impl<D> From<ExecuteError<D>> for Error<D> {
    fn from(failure: ExecuteError<D>) -> Self {
        match failure {
            ExecuteError::Decider(failure) => Self::Decider(failure),
            ExecuteError::Transient(failure) => Self::Transient(failure),
            ExecuteError::Permanent(failure) => Self::Permanent(failure),
            ExecuteError::OutcomeUnknown(failure) => Self::OutcomeUnknown(failure),
            ExecuteError::NotARevision(refusal) => Self::NotARevision(refusal),
        }
    }
}

impl<D> From<LoadError> for Error<D> {
    fn from(failure: LoadError) -> Self {
        ExecuteError::from(failure).into()
    }
}

impl<D> From<WriteError> for Error<D> {
    fn from(failure: WriteError) -> Self {
        ExecuteError::from(failure).into()
    }
}

impl<D> From<RegisterSchemasError> for Error<D> {
    fn from(failure: RegisterSchemasError) -> Self {
        match failure {
            RegisterSchemasError::Transient(failure) => Self::Transient(failure),
            RegisterSchemasError::Permanent(failure) => Self::Permanent(failure),
        }
    }
}

impl<D> From<ReadError> for Error<D> {
    fn from(failure: ReadError) -> Self {
        match failure {
            ReadError::Transient(failure) => Self::Transient(failure),
            ReadError::Permanent(failure) => Self::Permanent(failure),
            ReadError::NotARevision(refusal) => Self::NotARevision(refusal),
            ReadError::EmptyRange(refusal) => Self::EmptyRange(refusal),
        }
    }
}

impl<D> From<ReadOptionsError> for Error<D> {
    fn from(refusal: ReadOptionsError) -> Self {
        ReadError::from(refusal).into()
    }
}

impl<D> From<PreconditionsError> for Error<D> {
    fn from(refusal: PreconditionsError) -> Self {
        ExecuteError::from(refusal).into()
    }
}

impl<D: fmt::Display> fmt::Display for Error<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decider(failure) => failure.fmt(formatter),
            Self::Transient(failure) => failure.fmt(formatter),
            Self::Permanent(failure) => failure.fmt(formatter),
            Self::OutcomeUnknown(failure) => fmt_outcome_unknown(failure, formatter),
            Self::NotARevision(refusal) => refusal.fmt(formatter),
            Self::EmptyRange(refusal) => refusal.fmt(formatter),
        }
    }
}

impl<D: StdError + 'static> StdError for Error<D> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Decider(failure) => failure.source(),
            Self::Transient(failure) => failure.source(),
            Self::Permanent(failure) => failure.source(),
            Self::OutcomeUnknown(failure) => source_of_outcome_unknown(failure),
            Self::NotARevision(refusal) => refusal.source(),
            Self::EmptyRange(refusal) => refusal.source(),
        }
    }
}

impl<D: Categorized> Categorized for Error<D> {
    fn category(&self) -> Option<Category> {
        match self {
            Self::Decider(failure) => failure.category(),
            Self::Transient(failure) => failure.category(),
            Self::Permanent(failure) => failure.category(),
            Self::OutcomeUnknown(_) | Self::NotARevision(_) | Self::EmptyRange(_) => None,
        }
    }
}
