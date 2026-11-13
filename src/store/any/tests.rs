// The tests of the store check that the errors of its operations turn into the
// failure of the kit. These tests check that the failures a function meets
// between operations do as well, through `?`: a permanent or a transient
// failure, such as the one of decoding an event, and a change of a view in
// memory that was refused, each with its category and its text.

use super::Error;
use crate::{Categorized, Category, DomainError, Failure, Permanent, Transient, ViewError};

/// Passes the failure on with `?`, as a function that calls several
/// operations does.
fn passed_on<F>(failure: F) -> Error<DomainError>
where
    Error<DomainError>: From<F>,
{
    fn call<F>(failed: Result<(), F>) -> Result<(), Error<DomainError>>
    where
        Error<DomainError>: From<F>,
    {
        failed?;
        Ok(())
    }

    call(Err(failure)).expect_err("the failure is passed on")
}

fn permanent() -> Permanent {
    Permanent::Other(Failure::new("decoding event 7", None))
}

fn transient() -> Transient {
    Transient::Other(Failure::new("reading the subject /books/42", None))
}

fn conflict() -> Transient {
    Transient::Conflict(Failure::new("writing to the subject /books/42", None))
}

#[test]
fn passes_on_a_permanent_failure() {
    let failure = passed_on(permanent());

    assert!(
        matches!(failure, Error::Permanent(Permanent::Other(_))),
        "{failure:?}"
    );
    assert_eq!(failure.category(), Some(Category::Permanent));
    assert_eq!(failure.to_string(), "permanent failure: decoding event 7");
}

#[test]
fn passes_on_a_transient_failure_and_a_conflict_as_such() {
    let failure = passed_on(transient());
    let refusal = passed_on(conflict());

    assert!(
        matches!(failure, Error::Transient(Transient::Other(_))),
        "{failure:?}"
    );
    assert!(
        matches!(refusal, Error::Transient(Transient::Conflict(_))),
        "{refusal:?}"
    );
    assert_eq!(failure.category(), Some(Category::Transient));
    assert_eq!(
        failure.to_string(),
        "transient failure: reading the subject /books/42"
    );
}

#[test]
fn passes_on_a_refused_change_of_a_view_as_the_permanent_failure_it_holds() {
    let failure = passed_on(ViewError::Permanent(permanent()));

    assert!(
        matches!(failure, Error::Permanent(Permanent::Other(_))),
        "{failure:?}"
    );
    assert_eq!(failure.category(), Some(Category::Permanent));
    assert_eq!(failure.to_string(), "permanent failure: decoding event 7");
}
