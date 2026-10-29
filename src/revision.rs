use std::{error::Error, fmt, future::Future, str::FromStr, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::{Categorized, Category, Failure, Permanent};

#[cfg(test)]
pub(crate) mod tests;

/// The largest event ID EventSourcingDB hands out, since it numbers its
/// events with signed 64-bit integers.
const LARGEST_REVISION: u64 = (1 << 63) - 1;

/// A revision, which is the ID of an event of EventSourcingDB: a number from 0
/// to 2^63-1, since the database numbers its events with signed 64-bit
/// integers, as one ascending sequence across all subjects. Revisions sort as
/// numbers, so that 10 comes after 9, and a value that is no event ID can not
/// be a revision at all.
///
/// A revision that comes from outside, such as an event ID in a request, is
/// read from its text with `parse`, or from a number with `try_from`. A `u32`
/// always fits, so a revision is made from one with `from`, as a test does. A
/// revision is written as its decimal, also by serde, which writes and reads
/// it as a string, as the database does:
///
/// ```
/// use architecturekit::Revision;
///
/// let revision: Revision = "42".parse()?;
///
/// assert_eq!(revision, Revision::from(42));
/// assert_eq!(revision.get(), 42);
/// assert_eq!(revision.to_string(), "42");
/// assert!(Revision::from(9) < Revision::from(10));
/// assert!("4 2".parse::<Revision>().is_err());
/// # Ok::<(), architecturekit::NotARevision>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    /// Returns the revision as a number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Every `u32` is an event ID, so this never fails.
impl From<u32> for Revision {
    fn from(number: u32) -> Self {
        Self(u64::from(number))
    }
}

/// Takes a number as a revision.
///
/// # Errors
///
/// Fails with [`NotARevision`] for a number beyond 2^63-1.
impl TryFrom<u64> for Revision {
    type Error = NotARevision;

    fn try_from(number: u64) -> Result<Self, NotARevision> {
        if number > LARGEST_REVISION {
            return Err(NotARevision {
                revision: number.to_string(),
            });
        }

        Ok(Self(number))
    }
}

/// Reads a revision from its decimal, written with digits only, possibly with
/// leading zeros.
///
/// # Errors
///
/// Fails with [`NotARevision`] for anything else, including the empty string,
/// a sign, spaces, and a number beyond 2^63-1.
impl FromStr for Revision {
    type Err = NotARevision;

    fn from_str(revision: &str) -> Result<Self, NotARevision> {
        let refused = || NotARevision {
            revision: revision.to_owned(),
        };

        // u64::from_str would take a leading plus sign as well.
        if revision.is_empty() || !revision.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(refused());
        }

        match revision.parse::<u64>() {
            Ok(number) if number <= LARGEST_REVISION => Ok(Self(number)),
            _ => Err(refused()),
        }
    }
}

/// Writes the revision as its decimal.
impl fmt::Display for Revision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

/// Writes the revision as a string of its decimal, as the database writes an
/// event ID.
impl Serialize for Revision {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// Reads the revision from a string, as [`parse`](str::parse) does, and fails
/// with the text of [`NotARevision`] for a string that is not one.
impl<'de> Deserialize<'de> for Revision {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(RevisionVisitor)
    }
}

/// Reads a revision from a string for serde.
struct RevisionVisitor;

impl de::Visitor<'_> for RevisionVisitor {
    type Value = Revision;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a revision, which is a string of a decimal from 0 to 2^63-1")
    }

    fn visit_str<E: de::Error>(self, revision: &str) -> Result<Revision, E> {
        revision.parse().map_err(E::custom)
    }
}

/// A value that was meant to be a revision, but is not one.
///
/// It usually comes from the caller, such as an event ID in a request, so it
/// belongs to no category: its [`Categorized::category`] is `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotARevision {
    revision: String,
}

impl NotARevision {
    /// Returns the value that is not a revision.
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }
}

impl fmt::Display for NotARevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "not a revision: {:?}", self.revision)
    }
}

impl Error for NotARevision {}

impl Categorized for NotARevision {
    fn category(&self) -> Option<Category> {
        None
    }
}

/// Reads the ID of a stored event as a revision. The database never writes an
/// ID that is not one, so such an event fails permanently.
pub(crate) fn id_of(event: &eventsourcingdb::Event) -> Result<Revision, Permanent> {
    event.id().parse().map_err(|refusal: NotARevision| {
        Permanent::Other(Failure::new(
            format!(
                "event {:?} on {:?} has an ID that is not a revision",
                event.id(),
                event.subject(),
            ),
            Some(Box::new(refusal)),
        ))
    })
}

/// A view that knows how far its projection has come, which lets a reader
/// wait for a revision instead of polling.
/// [`InMemoryView`](crate::InMemoryView) is one.
///
/// A revision is the ID of the last event a projection has seen. Because the
/// database hands out event IDs as one ascending sequence across all streams,
/// a reader can name the revision it needs and wait for it. This is the
/// counterpart of a precondition on the reading side: a command says "I
/// decided on this revision", a query says "I want to see at least this
/// revision". Together they let a caller read its own writes without guessing
/// how long a projection takes (see [`wait_for_written`]).
///
/// Waiting ends when the future is dropped, so a reader limits it with
/// `tokio::time::timeout`, or stops it with `tokio::select!`.
pub trait Revisioned: Send + Sync {
    /// The error with which asking the view fails, such as for a view that
    /// keeps its revision in a database.
    type Error: Error + Categorized + Send + Sync + 'static;

    /// Returns the last event the view has seen, or `None` while it has seen
    /// none.
    ///
    /// # Errors
    ///
    /// Fails if the revision could not be read, so that a reader fails rather
    /// than taking the view for one that has seen nothing.
    fn revision(&self) -> impl Future<Output = Result<Option<Revision>, Self::Error>> + Send;

    /// Returns once the view has reached the revision, at once if it has
    /// already.
    ///
    /// # Errors
    ///
    /// Fails if the view could not be asked.
    fn wait_for(&self, revision: Revision) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Returns the highest event ID among written events, which is the revision a
/// caller has to see before its own write shows up in a read model, or `None`
/// if nothing was written. The order of the events does not matter, and an ID
/// that is not a revision is skipped.
#[must_use]
pub fn revision_of(events: &[eventsourcingdb::Event]) -> Option<Revision> {
    events
        .iter()
        .filter_map(|event| event.id().parse::<Revision>().ok())
        .max()
}

/// Waits until the view has seen the events that
/// [`Store::execute`](crate::Store::execute) or
/// [`Store::write`](crate::Store::write) returned, for a step on the server
/// that builds on what was just written, such as one that reads from the view
/// what a command has changed:
///
/// ```no_run
/// # use std::time::Duration;
/// # use architecturekit::{InMemoryView, NotCaughtUp, wait_for_written};
/// # async fn example(
/// #     books: InMemoryView<String, String>,
/// #     written: Vec<eventsourcingdb::Event>,
/// # ) {
/// match wait_for_written(&books, &written, Duration::from_secs(5)).await {
///     Ok(()) => {
///         // Read what the command has changed.
///     }
///     Err(NotCaughtUp::TimedOut(_)) => {
///         // The command has succeeded, so answer with success all the same,
///         // and with the revision, for which the caller can wait itself.
///     }
///     Err(NotCaughtUp::View(error)) => {
///         // The view could not be asked.
///     }
/// }
/// # }
/// ```
///
/// It waits with [`Revisioned::wait_for`] for [`revision_of`] the events, for
/// at most the timeout, and returns once the view has reached it, and at once
/// if no events were written, since there is nothing to wait for then.
///
/// The write has succeeded in any case, and trying it again would store the
/// events twice, which is why [`NotCaughtUp`] belongs to no category.
///
/// # Errors
///
/// Fails with [`NotCaughtUp::TimedOut`] if the view has not caught up within
/// the timeout, and with [`NotCaughtUp::View`] if the view could not be asked.
///
/// # Panics
///
/// Panics for a timeout of zero, before it looks at the events, since a view
/// that has to catch up with a write that has just happened takes some time,
/// and waiting no time at all would fail almost always.
pub async fn wait_for_written<V: Revisioned + ?Sized>(
    view: &V,
    written: &[eventsourcingdb::Event],
    timeout: Duration,
) -> Result<(), NotCaughtUp<V::Error>> {
    assert!(
        !timeout.is_zero(),
        "architecturekit: wait_for_written needs a timeout that is positive, not {timeout:?}",
    );

    let Some(revision) = revision_of(written) else {
        return Ok(());
    };

    match tokio::time::timeout(timeout, view.wait_for(revision)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(NotCaughtUp::View(error)),
        Err(_) => Err(NotCaughtUp::TimedOut(timeout)),
    }
}

/// Why a view did not catch up with written events (see
/// [`wait_for_written`]). The events were written, so the write has
/// succeeded, and only the view lags behind.
///
/// It belongs to no category. It is not transient, since trying the write
/// again would store its events twice, and not permanent, since the view may
/// still catch up. So it is answered as the success that the write is, with
/// the revision of the events, for which the caller can wait itself: over
/// HTTP with `CommandAnswer::with_result` of the module `httpapi`, which logs
/// that the view lags behind. It deliberately does not turn into an
/// `HttpError` with `?`, which would answer a command that has succeeded as a
/// failure.
#[derive(Debug)]
pub enum NotCaughtUp<E> {
    /// The view did not catch up within the timeout.
    TimedOut(Duration),
    /// The view could not be asked. Its error keeps its category.
    View(E),
}

impl<E> fmt::Display for NotCaughtUp<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut(timeout) => write!(
                formatter,
                "not caught up: the events were written, but the view did not catch up within {timeout:?}",
            ),
            Self::View(_) => formatter.write_str(
                "not caught up: the events were written, but the view could not be asked",
            ),
        }
    }
}

impl<E: Error + 'static> Error for NotCaughtUp<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::TimedOut(_) => None,
            Self::View(error) => Some(error),
        }
    }
}

impl<E> Categorized for NotCaughtUp<E> {
    fn category(&self) -> Option<Category> {
        None
    }
}
