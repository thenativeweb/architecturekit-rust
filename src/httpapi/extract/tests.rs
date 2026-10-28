// These tests check the extractors for axum that read a body: the order of
// the checks, the limit of the kit and that of a layer, and a body that can
// not be read. What a body has to look like is part of the tests of the
// neutral functions. They also check the answer to a panic, which a layer of
// tower-http catches. The stack of a panic is not logged, since the hook of
// Rust prints it, as long as it is not replaced.

use std::{any::Any, io};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{FromRequest, Request},
    routing::post,
};
use futures_util::stream;
use http::StatusCode;
use log::Level;
use serde::Deserialize;
use serde_json::{Value, json};
use tower::ServiceExt;
use tower_http::catch_panic::CatchPanicLayer;

use super::{NoBody, Strict, answer_panic};
use crate::httpapi::{HttpError, MAX_REQUEST_BODY, logs::captured};

#[derive(Debug, Deserialize, PartialEq)]
struct Preview {
    #[serde(rename = "customerId")]
    customer_id: String,
}

fn request(content_type: Option<&str>, body: Body) -> Request {
    let mut request = Request::builder().method("POST").uri("/previews");
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }

    request.body(body).expect("the request is valid")
}

/// A body that fails when it is read, which shows whether it was read.
fn unreadable() -> Body {
    Body::from_stream(stream::once(async {
        Err::<Bytes, _>(io::Error::other("the connection broke"))
    }))
}

async fn strict(request: Request) -> Result<Preview, HttpError> {
    Strict::<Preview>::from_request(request, &())
        .await
        .map(|Strict(preview)| preview)
}

#[tokio::test]
async fn decodes_a_body_that_fits() {
    let preview = strict(request(
        Some("application/json"),
        Body::from(r#"{"customerId":"42"}"#),
    ))
    .await
    .expect("the body fits");

    assert_eq!(preview.customer_id, "42");
}

#[tokio::test]
async fn rejects_a_body_that_does_not_claim_to_be_json_before_it_reads_it() {
    let failure = strict(request(Some("text/plain"), unreadable()))
        .await
        .expect_err("not JSON");

    assert_eq!(failure.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn rejects_a_body_over_the_limit() {
    let body = format!(r#"{{"customerId":"{}"}}"#, "a".repeat(MAX_REQUEST_BODY));

    let failure = strict(request(Some("application/json"), Body::from(body)))
        .await
        .expect_err("the body is too large");

    assert_eq!(failure.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        failure.message(),
        "request body too large: at most 1048576 bytes are read"
    );
}

#[tokio::test]
async fn rejects_a_body_over_the_limit_of_a_layer() {
    // A layer such as RequestBodyLimitLayer of tower-http cuts the body off at
    // a limit of its own, below that of the kit.
    let limited = Body::new(http_body_util::Limited::new(
        Body::from(r#"{"customerId":"42"}"#),
        5,
    ));

    let failure = strict(request(Some("application/json"), limited))
        .await
        .expect_err("the body is over the limit of the layer");

    assert_eq!(failure.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        failure.message(),
        "request body too large: the body exceeds the limit of the server"
    );
}

#[tokio::test]
async fn rejects_a_body_that_can_not_be_read_without_the_words_of_the_body() {
    let failure = strict(request(Some("application/json"), unreadable()))
        .await
        .expect_err("the body can not be read");

    assert_eq!(failure.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        failure.message(),
        "malformed request: the body could not be read"
    );
}

#[tokio::test]
async fn takes_an_empty_body_for_a_route_that_takes_no_body() {
    for body in ["", "{}"] {
        let taken = NoBody::from_request(request(None, Body::from(body)), &()).await;

        assert!(taken.is_ok(), "{body:?} has to be taken");
    }
}

#[tokio::test]
async fn refuses_a_body_for_a_route_that_takes_no_body() {
    let failure = NoBody::from_request(request(None, Body::from(r#"{"quantity":3}"#)), &())
        .await
        .expect_err("the route takes no body");

    assert_eq!(failure.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn refuses_a_request_from_another_origin_before_it_reads_the_body() {
    let mut request = request(None, unreadable());
    let _ = request
        .headers_mut()
        .insert("sec-fetch-site", "cross-site".parse().expect("a header"));

    let failure = NoBody::from_request(request, &())
        .await
        .expect_err("another origin");

    assert_eq!(failure.status(), StatusCode::FORBIDDEN);
}

#[test]
fn answers_a_panic_with_500_and_logs_it_as_an_error() {
    let payloads: [(Box<dyn Any + Send>, &str); 3] = [
        (Box::new("the shelf is gone"), "the shelf is gone"),
        (Box::new(String::from("book 42 is gone")), "book 42 is gone"),
        (Box::new(42), "a value that is no text"),
    ];

    for (payload, message) in payloads {
        let (answer, logged) = captured(|| answer_panic(payload));

        assert_eq!(answer.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            logged,
            vec![(
                Level::Error,
                format!(
                    "architecturekit: internal failure, answered with 500: panic while handling the request: {message}"
                ),
            )]
        );
    }
}

/// Borrows a book, and panics, as a handler with a bug does.
async fn borrow_book() -> &'static str {
    tokio::task::yield_now().await;
    panic!("the shelf is gone")
}

#[tokio::test]
async fn answers_a_panic_of_a_handler_with_the_layer_of_tower_http() {
    let router = Router::new()
        .route("/books/borrow", post(borrow_book))
        .layer(CatchPanicLayer::custom(answer_panic));

    let answer = router
        .oneshot(
            Request::post("/books/borrow")
                .body(Body::empty())
                .expect("the request is valid"),
        )
        .await
        .expect("the router answers");

    assert_eq!(answer.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(answer.headers()["cache-control"], "no-store");
    let body = axum::body::to_bytes(answer.into_body(), usize::MAX)
        .await
        .expect("the body can be read");
    let body: Value = serde_json::from_slice(&body).expect("the answer is JSON");
    assert_eq!(body, json!({ "message": "internal server error" }));
}
