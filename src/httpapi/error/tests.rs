// These tests mirror TestStatusFor, TestCategoryTexts and
// TestStatusForAnEmptyRange of ArchitectureKit for Go, and the parts of
// TestAnsweringRefusals, TestAnsweringAValueThatIsNotARevision and
// TestAnsweringAnEmptyRange that need no route. Some do not apply to Rust:
//
// - A failure is a value of its own, so there is no mark for one that came
//   before the command ran, and a handler that fails before it runs a command
//   answers what it fails with.
// - A request that the caller cancels is dropped, so it gets no answer, and
//   there is no 499, nor a deadline of a context to answer with 503.
// - The log is the one of the log crate, so there is no logger of an API to
//   fall back from, and its entries name the status, but neither the method
//   nor the route, which a handler does not know when it answers.

use std::{convert::Infallible, error::Error, fmt, time::Duration};

use bytes::Bytes;
use http::{Response, StatusCode, header};
use log::Level;

use super::HttpError;
use crate::{
    Categorized, Category, DomainError, ExecuteError, Failure, LoadError, LowerBound, NotCaughtUp,
    Permanent, ReadOptions, Transient, UpperBound, ViewError, check_read_options,
    httpapi::logs::captured, parse_revision,
};

/// An error of a category the test names, as an application has one.
#[derive(Debug)]
struct Refused(Option<Category>);

impl fmt::Display for Refused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the shelf is full")
    }
}

impl Error for Refused {}

impl Categorized for Refused {
    fn category(&self) -> Option<Category> {
        self.0
    }
}

fn transient() -> Transient {
    Transient::Other(Failure::new("reading the subject /books/42", None))
}

fn conflict() -> Transient {
    Transient::Conflict(Failure::new("writing to the subject /books/42", None))
}

fn permanent() -> Permanent {
    Permanent::Other(Failure::new("reading the subject /books/42", None))
}

fn outcome_unknown(cause: Option<Transient>) -> ExecuteError<DomainError> {
    ExecuteError::OutcomeUnknown(Failure::new(
        "writing to the subject /books/42",
        cause.map(Into::into),
    ))
}

fn empty_range() -> HttpError {
    check_read_options(&ReadOptions {
        lower_bound: LowerBound::FromEvent("5".to_owned()),
        upper_bound: UpperBound::BeforeEvent("3".to_owned()),
        ..ReadOptions::default()
    })
    .expect_err("the range is empty")
    .into()
}

fn not_a_revision() -> HttpError {
    parse_revision("abc")
        .expect_err("abc is no revision")
        .into()
}

/// Every kind of failure, with the status it is answered with.
fn every_kind() -> Vec<(&'static str, HttpError, StatusCode)> {
    vec![
        (
            "a caller who could not be determined",
            HttpError::unauthorized("the token has expired"),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "a caller who is not allowed",
            HttpError::forbidden("only an admin may do this"),
            StatusCode::FORBIDDEN,
        ),
        (
            "something that does not exist",
            HttpError::not_found("ticket 7"),
            StatusCode::NOT_FOUND,
        ),
        (
            "a malformed request",
            HttpError::malformed("empty body"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "a body that is too large",
            HttpError::too_large("at most 1048576 bytes are read"),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            "a body of another type",
            HttpError::unsupported_media_type("Content-Type is required"),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            "a broken business rule",
            ExecuteError::Decider(DomainError::new("the book is lent out")).into(),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "an unknown outcome",
            outcome_unknown(None).into(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            "a view that did not catch up",
            NotCaughtUp::<Infallible>::TimedOut(Duration::from_secs(5)).into(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            "a conflict",
            ExecuteError::<DomainError>::Transient(conflict()).into(),
            StatusCode::CONFLICT,
        ),
        (
            "a transient failure",
            ExecuteError::<DomainError>::Transient(transient()).into(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            "a permanent failure",
            ExecuteError::<DomainError>::Permanent(permanent()).into(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            "a value that is not a revision",
            not_a_revision(),
            StatusCode::BAD_REQUEST,
        ),
        ("an empty range", empty_range(), StatusCode::BAD_REQUEST),
        (
            "an error without a category",
            HttpError::of(Refused(None)),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ]
}

#[test]
fn maps_every_category() {
    for (name, error, status) in every_kind() {
        assert_eq!(error.status(), status, "{name}");
    }
}

#[test]
fn maps_an_error_of_an_application_by_its_category() {
    for (category, status) in [
        (Some(Category::Domain), StatusCode::UNPROCESSABLE_ENTITY),
        (Some(Category::Transient), StatusCode::SERVICE_UNAVAILABLE),
        (Some(Category::Permanent), StatusCode::INTERNAL_SERVER_ERROR),
        (None, StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        assert_eq!(
            HttpError::of(Refused(category)).status(),
            status,
            "{category:?}"
        );
        assert_eq!(
            HttpError::from(ExecuteError::Decider(Refused(category))).status(),
            status,
            "a decider that fails with {category:?}"
        );
    }
}

#[test]
fn prefers_conflict_over_its_category() {
    // A conflict is transient, but 409 says more than 503.
    let error = HttpError::from(ExecuteError::<DomainError>::Transient(conflict()));

    assert_eq!(error.status(), StatusCode::CONFLICT);
}

#[test]
fn maps_an_unknown_outcome_to_500_also_if_it_wraps_a_status_that_invites_trying_again() {
    let error = HttpError::from(outcome_unknown(Some(transient())));

    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn maps_a_view_that_did_not_catch_up_to_500_also_if_it_wraps_a_status_that_invites_trying_again() {
    let error = HttpError::from(NotCaughtUp::View(transient()));

    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn maps_what_wait_for_written_returns_once_the_time_has_run_out_to_500_not_to_503() {
    let error = HttpError::from(NotCaughtUp::<Infallible>::TimedOut(Duration::from_millis(
        50,
    )));

    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn maps_a_value_that_is_not_a_revision_to_400_unless_it_is_a_permanent_failure() {
    assert_eq!(not_a_revision().status(), StatusCode::BAD_REQUEST);

    // A view that is handed an event ID that is not a revision is the
    // server's mistake.
    let refusal = parse_revision("abc").expect_err("abc is no revision");
    assert_eq!(
        HttpError::of(ViewError::NotARevision(refusal)).status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn maps_an_empty_range_to_400() {
    assert_eq!(empty_range().status(), StatusCode::BAD_REQUEST);
}

#[test]
fn maps_a_failure_of_the_database_by_its_category() {
    assert_eq!(
        HttpError::from(LoadError::Transient(transient())).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        HttpError::from(LoadError::Permanent(permanent())).status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

/// Answers the failure, and returns the answer with what was logged meanwhile.
fn answered(error: HttpError) -> (Response<Bytes>, Vec<(Level, String)>) {
    captured(|| Response::<Bytes>::from(error))
}

fn message_of(response: &Response<Bytes>) -> String {
    let body: serde_json::Value =
        serde_json::from_slice(response.body()).expect("the answer is JSON");
    body["message"]
        .as_str()
        .expect("the answer has a message")
        .to_owned()
}

#[test]
fn explains_a_failure_the_caller_can_fix_with_its_own_text_without_logging_it() {
    for (name, error, message) in [
        (
            "a broken business rule",
            ExecuteError::Decider(DomainError::new("the book is lent out")).into(),
            "the book is lent out",
        ),
        (
            "a caller who is not allowed",
            HttpError::forbidden("only an admin may do this"),
            "forbidden: only an admin may do this",
        ),
        (
            "something that does not exist",
            HttpError::not_found("ticket 7"),
            "not found: ticket 7",
        ),
        (
            "a malformed request",
            HttpError::malformed("empty body"),
            "malformed request: empty body",
        ),
        (
            "a body that is too large",
            HttpError::too_large("at most 1048576 bytes are read"),
            "request body too large: at most 1048576 bytes are read",
        ),
        (
            "a body of another type",
            HttpError::unsupported_media_type("Content-Type is required"),
            "unsupported media type: Content-Type is required",
        ),
        (
            "a value that is not a revision",
            not_a_revision(),
            r#"not a revision: "abc""#,
        ),
        (
            "an empty range",
            empty_range(),
            r#"empty range: no event can lie from "5" and before "3""#,
        ),
    ] {
        let (response, logged) = answered(error);

        assert_eq!(message_of(&response), message, "{name}");
        assert_eq!(
            logged,
            [],
            "{name}: a failure the caller can fix is not logged"
        );
    }
}

#[test]
fn answers_a_refusal_with_a_fixed_text_and_logs_the_details_as_information() {
    for (name, error, message, cause) in [
        (
            "a caller who could not be determined",
            HttpError::unauthorized("the token has expired"),
            "unauthorized",
            "the token has expired",
        ),
        (
            "a conflict",
            ExecuteError::<DomainError>::Transient(conflict()).into(),
            "conflict: the data has changed since it was read",
            "/books/42",
        ),
    ] {
        let (response, logged) = answered(error);

        assert_eq!(message_of(&response), message, "{name}");
        assert_eq!(logged.len(), 1, "{name}: {logged:?}");
        assert_eq!(logged[0].0, Level::Info, "{name}");
        assert!(
            logged[0].1.contains(cause),
            "{name}: the details have to be logged: {logged:?}"
        );
    }
}

#[test]
fn answers_an_internal_failure_with_a_fixed_text_and_logs_it_as_an_error() {
    for (name, error, message) in [
        (
            "a transient failure",
            HttpError::from(ExecuteError::<DomainError>::Transient(transient())),
            "internal server error",
        ),
        (
            "a permanent failure",
            ExecuteError::<DomainError>::Permanent(permanent()).into(),
            "internal server error",
        ),
        (
            "an error without a category",
            HttpError::of(Refused(None)),
            "internal server error",
        ),
        (
            "an unknown outcome",
            outcome_unknown(Some(transient())).into(),
            "outcome unknown: the request may have succeeded",
        ),
        (
            "a view that did not catch up",
            NotCaughtUp::View(transient()).into(),
            "the request succeeded, but its result is not visible yet",
        ),
    ] {
        let status = error.status();
        let (response, logged) = answered(error);

        assert_eq!(message_of(&response), message, "{name}");
        assert_eq!(logged.len(), 1, "{name}: {logged:?}");
        assert_eq!(logged[0].0, Level::Error, "{name}");
        assert!(
            logged[0].1.contains(&status.as_u16().to_string()),
            "{name}: the status has to be logged: {logged:?}"
        );
        assert!(
            logged[0].1.contains("/books/42") || logged[0].1.contains("the shelf is full"),
            "{name}: the cause has to be logged: {logged:?}"
        );
    }
}

#[test]
fn answers_as_json_that_is_never_cached() {
    let (response, _) = answered(HttpError::not_found("ticket 7"));

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        response.body(),
        &Bytes::from_static(b"{\"message\":\"not found: ticket 7\"}\n")
    );
}

#[test]
fn keeps_the_cause_as_its_source() {
    let error = HttpError::from(ExecuteError::Decider(DomainError::new(
        "the book is lent out",
    )));

    assert_eq!(
        error
            .source()
            .expect("the error of the decider is the source")
            .to_string(),
        "the book is lent out"
    );
    assert_eq!(error.to_string(), "the book is lent out");
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn answers_through_axum_as_it_answers_otherwise() {
    use axum::response::IntoResponse;

    let response = HttpError::forbidden("only an admin may do this").into_response();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body can be read"),
        Bytes::from_static(b"{\"message\":\"forbidden: only an admin may do this\"}\n")
    );
}

#[test]
fn passes_on_the_error_of_a_view_in_memory() {
    fn read() -> Result<u8, HttpError> {
        let found: Result<u8, Infallible> = Ok(42);
        Ok(found?)
    }

    assert_eq!(read().expect("reading never fails"), 42);
}
