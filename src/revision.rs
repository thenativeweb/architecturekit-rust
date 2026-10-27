use std::{cmp::Ordering, error::Error, fmt};

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
fn revision_number(revision: &str) -> Result<Option<u64>, NotARevision> {
    if revision.is_empty() {
        return Ok(None);
    }

    parse_revision(revision).map(Some)
}
