// These tests check the categories of failures and their texts: Category has
// the text of each category, and every failure of a category begins with it.

use std::error::Error;

use crate::{
    Categorized, Category, DomainError, Failure, NotARevision, Permanent, Revision, Transient,
};

#[derive(Debug)]
struct Cause;

impl std::fmt::Display for Cause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("connection refused")
    }
}

impl Error for Cause {}

fn failure() -> Failure {
    Failure::new(r#"reading "/books/42""#, Some(Box::new(Cause)))
}

#[test]
fn an_unverified_event_is_permanent() {
    let unverified = Permanent::Unverified(failure());

    assert_eq!(unverified.category(), Some(Category::Permanent));
    assert_ne!(
        unverified.category(),
        Some(Category::Transient),
        "an unverified event must not be retried",
    );
}

#[test]
fn category_texts() {
    // An error that is written for the caller reaches them with the text of
    // its category, so the text names what is wrong rather than the crate it
    // comes from.
    for (category, text) in [
        (Category::Domain, "domain rule violated"),
        (Category::Transient, "transient failure"),
        (Category::Permanent, "permanent failure"),
    ] {
        assert_eq!(category.to_string(), text);
    }

    for (failure, text) in [
        (
            Transient::Conflict(failure()).to_string(),
            r#"transient failure: a precondition did not hold: reading "/books/42""#,
        ),
        (
            Transient::Other(failure()).to_string(),
            r#"transient failure: reading "/books/42""#,
        ),
        (
            Permanent::Unverified(failure()).to_string(),
            r#"permanent failure: an event could not be verified: reading "/books/42""#,
        ),
        (
            Permanent::Other(failure()).to_string(),
            r#"permanent failure: reading "/books/42""#,
        ),
        (
            "x".parse::<Revision>()
                .expect_err("x is not a revision")
                .to_string(),
            r#"not a revision: "x""#,
        ),
    ] {
        assert_eq!(failure, text);
    }
}

#[test]
fn a_failure_reaches_its_cause_through_source() {
    // The text of a failure leaves its cause out, since an error reporter
    // that follows source would otherwise show it twice.
    let failures: [Box<dyn Error>; 4] = [
        Box::new(Transient::Other(failure())),
        Box::new(Transient::Conflict(failure())),
        Box::new(Permanent::Other(failure())),
        Box::new(Permanent::Unverified(failure())),
    ];

    for failure in failures {
        let cause = failure.source().expect("the failure has a cause");

        assert_eq!(cause.to_string(), "connection refused");
        assert!(
            !failure.to_string().contains("connection refused"),
            "{failure}"
        );
    }

    assert!(
        Transient::Other(Failure::new("reading", None))
            .source()
            .is_none()
    );
}

#[test]
fn every_error_names_its_category() {
    assert_eq!(
        DomainError::new("refused").category(),
        Some(Category::Domain)
    );
    assert_eq!(
        Transient::Conflict(failure()).category(),
        Some(Category::Transient)
    );
    assert_eq!(
        Transient::Other(failure()).category(),
        Some(Category::Transient)
    );
    assert_eq!(
        Permanent::Other(failure()).category(),
        Some(Category::Permanent)
    );
    assert_eq!(
        "x".parse::<Revision>()
            .map_err(|refusal: NotARevision| refusal.category()),
        Err(None),
        "a value that is not a revision belongs to no category",
    );
}

#[test]
fn a_domain_error_is_its_message() {
    let refusal = DomainError::new(format!("book {} is already borrowed", 42));

    assert_eq!(refusal.to_string(), "book 42 is already borrowed");
    assert!(refusal.source().is_none());
}
