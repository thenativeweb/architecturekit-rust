use std::{error::Error, fmt};

#[cfg(test)]
mod tests;

/// What a failure means for its caller, on two axes: whether it is a matter of
/// the domain or of the machinery, and whether trying again unchanged could
/// help.
///
/// Ask for the category rather than for a concrete failure, so that new
/// failures do not break existing code. The categories are closed on purpose:
/// they answer these two questions and no others, so a new one would come with
/// a new major version, rather than fall into a catch-all arm unnoticed.
///
/// A failure that the two questions do not fit belongs to no category, so its
/// [`Categorized::category`] is `None`, and the failure itself says what it is:
///
/// - A write whose outcome is unknown may have stored its events, so it must
///   not be tried again blindly.
/// - A text of the caller that can not be read as a revision or a subject,
///   values of the caller that can not compose a subject, and bounds of a read
///   that leave no room for an event, mean that the request is wrong.
/// - A view that has not caught up with a write tells that the write succeeded,
///   but its result is not visible yet.
/// - A run of a projection that stopped because its token was cancelled did
///   not fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Category {
    /// A business rule refused the command. The caller has to change what it
    /// asks for; asking again will not help.
    Domain,
    /// The same attempt may succeed later, unchanged. A write that failed with
    /// it has certainly stored nothing, so trying it again can not store its
    /// events twice.
    Transient,
    /// Trying again will not help, and something is wrong with the code, the
    /// data or the configuration.
    Permanent,
}

impl fmt::Display for Category {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Domain => "domain rule violated",
            Self::Transient => "transient failure",
            Self::Permanent => "permanent failure",
        })
    }
}

/// An error that names its [`Category`].
///
/// The errors of architecturekit name theirs. The error of a decider names its
/// own, so that a failure while loading something within the decision, which
/// is a matter of the machinery, is told apart from a refusal, which is a
/// matter of the domain. Every error type names its category itself, since no
/// category fits every error: an error type that only refuses names
/// [`Category::Domain`], and the error of a view in a database names
/// [`Category::Transient`] for a failure that may pass.
///
/// ```
/// use architecturekit::{Categorized, Category};
///
/// #[derive(Debug)]
/// enum BorrowRefused {
///     AlreadyBorrowed,
/// }
///
/// impl Categorized for BorrowRefused {
///     fn category(&self) -> Option<Category> {
///         Some(Category::Domain)
///     }
/// }
/// ```
pub trait Categorized {
    /// Returns the category of the error, or `None` if it belongs to none.
    fn category(&self) -> Option<Category>;
}

/// A business rule that refused a command, with a message for the caller.
///
/// It is the error a decider returns when it refuses a command and needs
/// nothing more than a message. Its category is [`Category::Domain`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainError {
    message: String,
}

impl DomainError {
    /// Creates a domain error with the given message, such as
    /// `DomainError::new(format!("book {id} is already borrowed"))`.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DomainError {}

impl Categorized for DomainError {
    fn category(&self) -> Option<Category> {
        Some(Category::Domain)
    }
}

/// What failed, as in `reading "/books/42"`, and the error that caused it, if
/// there is one, such as the error of the client SDK.
#[derive(Debug)]
pub struct Failure {
    message: String,
    cause: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl Failure {
    pub(crate) fn new(
        message: impl Into<String>,
        cause: Option<Box<dyn Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            message: message.into(),
            cause,
        }
    }

    /// Returns what failed.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the error that caused the failure, if there is one.
    #[must_use]
    pub fn cause(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        self.cause.as_deref()
    }
}

/// A failure of the category [`Category::Transient`]: trying again later,
/// unchanged, may succeed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Transient {
    /// A precondition of the write did not hold, since the state it was
    /// decided on has moved on.
    Conflict(Failure),
    /// Any other transient failure, such as a database that can not be
    /// reached, or one that asks to be asked again later.
    Other(Failure),
}

impl Transient {
    fn failure(&self) -> &Failure {
        match self {
            Self::Conflict(failure) | Self::Other(failure) => failure,
        }
    }
}

impl fmt::Display for Transient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict(failure) => write!(
                formatter,
                "{}: a precondition did not hold: {}",
                Category::Transient,
                failure.message,
            ),
            Self::Other(failure) => {
                write!(formatter, "{}: {}", Category::Transient, failure.message)
            }
        }
    }
}

impl Error for Transient {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let cause: &(dyn Error + 'static) = self.failure().cause.as_deref()?;
        Some(cause)
    }
}

impl Categorized for Transient {
    fn category(&self) -> Option<Category> {
        Some(Category::Transient)
    }
}

/// A failure of the category [`Category::Permanent`]: trying again will not
/// help.
#[derive(Debug)]
#[non_exhaustive]
pub enum Permanent {
    /// An event that was read failed its verification: its hash does not
    /// match its content, or its signature is missing or does not match the
    /// verification key. It may point to a security incident rather than a
    /// mistake, which is why it can be told apart.
    Unverified(Failure),
    /// Any other permanent failure, such as data that can not be encoded, a
    /// command that declares its preconditions wrongly, or a database that
    /// does not support what a request needs.
    Other(Failure),
}

impl Permanent {
    fn failure(&self) -> &Failure {
        match self {
            Self::Unverified(failure) | Self::Other(failure) => failure,
        }
    }
}

impl fmt::Display for Permanent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unverified(failure) => write!(
                formatter,
                "{}: an event could not be verified: {}",
                Category::Permanent,
                failure.message,
            ),
            Self::Other(failure) => {
                write!(formatter, "{}: {}", Category::Permanent, failure.message)
            }
        }
    }
}

impl Error for Permanent {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let cause: &(dyn Error + 'static) = self.failure().cause.as_deref()?;
        Some(cause)
    }
}

impl Categorized for Permanent {
    fn category(&self) -> Option<Category> {
        Some(Category::Permanent)
    }
}

/// An error that can not occur, such as the one of reading a view in memory,
/// belongs to no category.
impl Categorized for std::convert::Infallible {
    fn category(&self) -> Option<Category> {
        match *self {}
    }
}
