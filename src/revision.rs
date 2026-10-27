use std::{cmp::Ordering, error::Error, fmt, future::Future, time::Duration};

use crate::{Categorized, Category};

#[cfg(test)]
pub(crate) mod tests;

/// The largest event ID EventSourcingDB hands out, since it numbers its
/// events with signed 64-bit integers.
const LARGEST_REVISION: u64 = (1 << 63) - 1;

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

/// Reads a revision, which is an event ID of EventSourcingDB, as a number.
///
/// A revision is a decimal number from 0 to 2^63-1, written with digits only,
/// possibly with leading zeros. The database numbers its events with signed
/// 64-bit integers, so a larger number is no event ID it could ever hand out.
///
/// ```
/// use architecturekit::parse_revision;
///
/// assert_eq!(parse_revision("42"), Ok(42));
/// assert!(parse_revision("").is_err());
/// ```
///
/// # Errors
///
/// Fails with [`NotARevision`] for anything else, including the empty string,
/// a sign, and spaces.
pub fn parse_revision(revision: &str) -> Result<u64, NotARevision> {
    let refused = || NotARevision {
        revision: revision.to_owned(),
    };

    // u64::from_str would take a leading plus sign as well.
    if revision.is_empty() || !revision.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(refused());
    }

    match revision.parse::<u64>() {
        Ok(number) if number <= LARGEST_REVISION => Ok(number),
        _ => Err(refused()),
    }
}

/// Compares two revisions as numbers, since event IDs are decimal strings,
/// and "10" sorts before "9" as text.
///
/// The empty string is a revision that is not set, as of a view that has seen
/// nothing yet, and comes before every other one. It can not be taken for 0,
/// since "0" is a real event ID.
///
/// # Errors
///
/// Fails with [`NotARevision`] if either side is neither empty nor a revision,
/// as [`parse_revision`] reads it. Both sides are read before either is
/// compared.
pub fn compare_revisions(left: &str, right: &str) -> Result<Ordering, NotARevision> {
    let left = revision_number(left)?;
    let right = revision_number(right)?;

    Ok(left.cmp(&right))
}

/// Reads a revision that may be unset, as `None` for the empty string, which
/// comes before every revision that is set.
pub(crate) fn revision_number(revision: &str) -> Result<Option<u64>, NotARevision> {
    if revision.is_empty() {
        return Ok(None);
    }

    parse_revision(revision).map(Some)
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
    fn revision(&self) -> impl Future<Output = Result<Option<String>, Self::Error>> + Send;

    /// Returns once the view has reached the revision, at once if it has
    /// already. The empty revision counts as reached by every view.
    ///
    /// # Errors
    ///
    /// Fails for a revision that is not one, and if the view could not be
    /// asked.
    fn wait_for(&self, revision: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Returns the highest event ID among written events, which is the revision a
/// caller has to see before its own write shows up in a read model, or `None`
/// if nothing was written. The order of the events does not matter, and an ID
/// that is not a revision is skipped.
#[must_use]
pub fn revision_of(events: &[eventsourcingdb::Event]) -> Option<String> {
    events
        .iter()
        .filter_map(|event| Some((parse_revision(event.id()).ok()?, event.id())))
        .max_by_key(|(number, _)| *number)
        .map(|(_, id)| id.to_owned())
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

    match tokio::time::timeout(timeout, view.wait_for(&revision)).await {
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
/// still catch up. So it is usually answered as the success that the write is,
/// with the revision of the events, for which the caller can wait itself.
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
