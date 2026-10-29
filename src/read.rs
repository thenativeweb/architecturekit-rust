use std::{error::Error, fmt};

use eventsourcingdb::request_options::{
    Bound, BoundType, Ordering, ReadEventMissingStrategy, ReadEventsOptions,
    ReadFromLatestEventOptions,
};

use crate::{Categorized, Category, NotARevision, parse_revision};

#[cfg(test)]
mod tests;

/// The largest event ID plus one, which no read can reach, so that a range
/// without an upper bound ends there.
const END_OF_REVISIONS: u64 = 1 << 63;

/// What a read or a projection reads: a subject, and whether the subjects below
/// it belong to it. Create it with [`tree`](Subjects::tree) or
/// [`exact`](Subjects::exact), so that every read says which of the two it
/// means, rather than relying on a default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subjects {
    subject: String,
    recursive: bool,
}

impl Subjects {
    /// The given subject together with every subject below it, which is what a
    /// projection usually reads: every book below `/books`, or everything below
    /// `/`. It is what the database calls reading recursively.
    ///
    /// # Panics
    ///
    /// A subject that does not start with a slash is a programming error, so
    /// this panics.
    #[must_use]
    pub fn tree(subject: impl Into<String>) -> Self {
        Self::of(subject.into(), true)
    }

    /// The given subject alone, without the subjects below it.
    ///
    /// # Panics
    ///
    /// A subject that does not start with a slash is a programming error, so
    /// this panics.
    #[must_use]
    pub fn exact(subject: impl Into<String>) -> Self {
        Self::of(subject.into(), false)
    }

    fn of(subject: String, recursive: bool) -> Self {
        require_leading_slash(&subject);

        Self { subject, recursive }
    }

    /// Returns the subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Tells whether the subjects below the subject belong to it.
    #[must_use]
    pub fn is_tree(&self) -> bool {
        self.recursive
    }
}

/// Panics for a subject that does not start with a slash.
fn require_leading_slash(subject: &str) {
    assert!(
        subject.starts_with('/'),
        "architecturekit: a subject starts with a slash, which {subject:?} does not"
    );
}

/// Narrows down which events [`Store::read`](crate::Store::read) hands out, or
/// turns around the order it hands them out in. A read has one lower bound, one
/// upper bound, and one order, so options that contradict each other can not
/// be written.
///
/// The options are those of a read of EventSourcingDB, which they cover in
/// full, so they are closed on purpose: a new option of the database comes with
/// a new major version of the client SDK, and so of the kit:
///
/// ```
/// use architecturekit::{LowerBound, Order, ReadOptions};
///
/// let next_page = ReadOptions {
///     lower_bound: LowerBound::AfterEvent("41".to_owned()),
///     ..ReadOptions::default()
/// };
///
/// let newest_first = ReadOptions {
///     order: Order::NewestFirst,
///     ..ReadOptions::default()
/// };
/// ```
///
/// An ID of a bound is a string, as everywhere else in the kit. One that is not
/// the ID of an event, such as an empty one, makes reading fail with
/// [`NotARevision`], and bounds that leave no room for an event make it fail
/// with [`EmptyRange`] (see [`check_read_options`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {
    /// Where the events start, at the first one unless it says otherwise.
    pub lower_bound: LowerBound,
    /// Where the events end, at the last one unless it says otherwise.
    pub upper_bound: UpperBound,
    /// The order of the events, oldest first unless it says otherwise.
    pub order: Order,
}

/// Where the events of a read start.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum LowerBound {
    /// From the first event on.
    #[default]
    Unbounded,
    /// From the event with the given ID on, including it.
    FromEvent(String),
    /// After the event with the given ID, leaving it out. Hand over the ID of
    /// the last event of a page to read the next one.
    AfterEvent(String),
    /// From the latest event of the given type on the given subject on,
    /// including it, as [`State::from_latest`](crate::State::from_latest) does
    /// for a state. The database looks for it on that subject alone, not below
    /// it, and the subject does not have to be one of those that are read. If
    /// the latest event of the type comes after the upper bound, the database
    /// refuses the read, which then fails permanently, since trying again
    /// never helps.
    ///
    /// The database reads from the latest event of a type only oldest first,
    /// so it contradicts [`Order::NewestFirst`], and a subject that does not
    /// start with a slash is a programming error, so reading panics for either.
    FromLatestEvent {
        /// The subject to look for the event on.
        subject: String,
        /// The type of the event.
        event_type: String,
        /// What to read if the subject has no event of the type yet.
        if_missing: IfLatestEventIsMissing,
    },
}

/// What [`LowerBound::FromLatestEvent`] reads if the subject has no event of
/// the type yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IfLatestEventIsMissing {
    /// The events as if there were no lower bound, so from the first one on, or
    /// up to the upper bound, if there is one.
    ReadEverything,
    /// No event at all.
    ReadNothing,
}

/// Where the events of a read end.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum UpperBound {
    /// At the last event.
    #[default]
    Unbounded,
    /// Up to the event with the given ID, including it.
    UpToEvent(String),
    /// Before the event with the given ID, leaving it out.
    BeforeEvent(String),
}

/// The order in which a read hands out the events.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Order {
    /// The oldest event first.
    #[default]
    OldestFirst,
    /// The newest event first. The bounds stay what they are, so together
    /// with [`UpperBound::BeforeEvent`], it reads the events before the given
    /// one, newest first, which pages backwards. A page that ends with the
    /// event `0` is the last one, since nothing lies before it.
    NewestFirst,
}

/// Checks the options of a read as [`Store::read`](crate::Store::read) checks
/// them before it asks the database. Use it to check bounds that come from a
/// request before a side effect that must not happen for a read that is
/// refused, such as recording who reads. The database may still refuse a read
/// that passes, such as one from a latest event that comes after the upper
/// bound.
///
/// # Errors
///
/// Fails with [`NotARevision`] for the first ID of a bound that is not the ID
/// of an event, the lower bound first, and otherwise with [`EmptyRange`] if
/// the bounds leave no room for an event.
///
/// # Panics
///
/// Panics for [`LowerBound::FromLatestEvent`] together with
/// [`Order::NewestFirst`], and for a subject of it that does not start with a
/// slash, before it checks any ID, so that a mistake in the code is not hidden
/// behind one of the caller.
pub fn check_read_options(options: &ReadOptions) -> Result<(), ReadOptionsError> {
    require_consistent(options);

    // The range holds the IDs from first on, and up to, but not including,
    // end. FromEvent(n) starts it at n and AfterEvent(n) at n+1, and
    // UpToEvent(n) ends it after n and BeforeEvent(n) at n. Counting the end
    // that way needs no number below 0 for BeforeEvent("0"), and a revision
    // ends at 2^63-1, so n+1 always fits.
    let first = match &options.lower_bound {
        LowerBound::FromEvent(id) => parse_revision(id)?,
        LowerBound::AfterEvent(id) => parse_revision(id)? + 1,
        LowerBound::Unbounded | LowerBound::FromLatestEvent { .. } => 0,
    };
    let end = match &options.upper_bound {
        UpperBound::UpToEvent(id) => parse_revision(id)? + 1,
        UpperBound::BeforeEvent(id) => parse_revision(id)?,
        UpperBound::Unbounded => END_OF_REVISIONS,
    };

    if first >= end {
        return Err(ReadOptionsError::EmptyRange(EmptyRange {
            range: describe_range(&options.lower_bound, &options.upper_bound),
        }));
    }

    Ok(())
}

/// Panics for options that contradict each other, which their types can not
/// rule out.
fn require_consistent(options: &ReadOptions) {
    if let LowerBound::FromLatestEvent {
        subject,
        event_type,
        ..
    } = &options.lower_bound
    {
        require_leading_slash(subject);
        assert!(
            options.order == Order::OldestFirst,
            "architecturekit: the database reads from the latest event of a type only oldest first, \
             but got FromLatestEvent({subject:?}, {event_type:?}) and NewestFirst"
        );
    }
}

/// Names the bounds of a range by their values only, since they usually come
/// from a request, whose caller knows neither the options of the read nor the
/// subjects: from "2" up to "1", from "1" and before "1", or after "0" and
/// before "1", before "0" without a lower bound, and after the largest
/// revision without an upper bound, the only range that is empty then. A
/// latest event of a type has no ID, so the lower bound is not known then, and
/// only an upper bound before "0" makes a range empty.
fn describe_range(lower_bound: &LowerBound, upper_bound: &UpperBound) -> String {
    let ending = match upper_bound {
        UpperBound::Unbounded => {
            let (LowerBound::FromEvent(id) | LowerBound::AfterEvent(id)) = lower_bound else {
                unreachable!("a range without bounds is never empty");
            };
            return format!("after {id:?}");
        }
        UpperBound::UpToEvent(id) => format!("up to {id:?}"),
        UpperBound::BeforeEvent(id) => format!("before {id:?}"),
    };

    match (lower_bound, upper_bound) {
        (LowerBound::Unbounded | LowerBound::FromLatestEvent { .. }, _) => ending,
        (LowerBound::AfterEvent(id), _) => format!("after {id:?} and {ending}"),
        // From one up to another reads naturally without "and".
        (LowerBound::FromEvent(id), UpperBound::UpToEvent(_)) => format!("from {id:?} {ending}"),
        (LowerBound::FromEvent(id), _) => format!("from {id:?} and {ending}"),
    }
}

/// Turns the options of a read of the given subjects into those of the client
/// SDK.
pub(crate) fn database_options<'a>(
    subjects: &'a Subjects,
    options: &'a ReadOptions,
) -> ReadEventsOptions<'a> {
    let bound = |bound_type, id: &'a String| Bound {
        bound_type,
        id: id.as_str(),
    };

    let (lower_bound, from_latest_event) = match &options.lower_bound {
        LowerBound::Unbounded => (None, None),
        LowerBound::FromEvent(id) => (Some(bound(BoundType::Inclusive, id)), None),
        LowerBound::AfterEvent(id) => (Some(bound(BoundType::Exclusive, id)), None),
        LowerBound::FromLatestEvent {
            subject,
            event_type,
            if_missing,
        } => (
            None,
            Some(ReadFromLatestEventOptions {
                if_event_is_missing: match if_missing {
                    IfLatestEventIsMissing::ReadEverything => {
                        ReadEventMissingStrategy::ReadEverything
                    }
                    IfLatestEventIsMissing::ReadNothing => ReadEventMissingStrategy::ReadNothing,
                },
                subject,
                ty: event_type,
            }),
        ),
    };
    let upper_bound = match &options.upper_bound {
        UpperBound::Unbounded => None,
        UpperBound::UpToEvent(id) => Some(bound(BoundType::Inclusive, id)),
        UpperBound::BeforeEvent(id) => Some(bound(BoundType::Exclusive, id)),
    };

    ReadEventsOptions {
        from_latest_event,
        lower_bound,
        order: match options.order {
            Order::OldestFirst => None,
            Order::NewestFirst => Some(Ordering::Antichronological),
        },
        recursive: subjects.recursive,
        upper_bound,
    }
}

/// The bounds of a read leave no room for an event, such as one before `"0"`,
/// one after `"0"` and before `"1"`, or one after the largest revision,
/// `2^63-1`. No event can ever lie in such a range, so it is a mistake of
/// whoever chose the bounds, which usually come from a request, and it belongs
/// to no category. A range that is only empty for now, such as the one after
/// the last event written so far, is no such mistake, since events can still
/// come there, so reading hands out no events for it, without an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptyRange {
    range: String,
}

impl fmt::Display for EmptyRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "empty range: no event can lie {}", self.range)
    }
}

impl Error for EmptyRange {}

impl Categorized for EmptyRange {
    fn category(&self) -> Option<Category> {
        None
    }
}

/// Why the options of a read are refused (see [`check_read_options`]). It has
/// exactly the cases the options can be refused for, as the error of every
/// operation does, so that a `match` handles all of them; a new one comes with
/// a new option, and so with a new major version.
#[derive(Debug)]
pub enum ReadOptionsError {
    /// An ID of a bound is not the ID of an event.
    NotARevision(NotARevision),
    /// The bounds leave no room for an event.
    EmptyRange(EmptyRange),
}

impl From<NotARevision> for ReadOptionsError {
    fn from(refusal: NotARevision) -> Self {
        Self::NotARevision(refusal)
    }
}

impl fmt::Display for ReadOptionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotARevision(refusal) => refusal.fmt(formatter),
            Self::EmptyRange(refusal) => refusal.fmt(formatter),
        }
    }
}

impl Error for ReadOptionsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NotARevision(refusal) => refusal.source(),
            Self::EmptyRange(refusal) => refusal.source(),
        }
    }
}

impl Categorized for ReadOptionsError {
    fn category(&self) -> Option<Category> {
        None
    }
}
