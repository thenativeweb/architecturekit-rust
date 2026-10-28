// These tests check commands over HTTP, with a route of axum that the tests
// build from the building blocks of the kit. A route that writes runs against
// a database in a container of its own, any other one against a store whose
// database can not be reached, which it never reaches.

use std::{future, io, sync::Arc};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{FromRequestParts, Path, Request, State},
    routing::post,
};
use eventsourcingdb::Client;
use futures_util::stream;
use http::{StatusCode, request::Parts};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tower::ServiceExt;

use crate::{
    Command, Decider, DomainError, Event, Events, Precondition, Store,
    httpapi::{CommandAnswer, HttpError, MAX_REQUEST_BODY, Strict},
    testing::Database,
};

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.counter-incremented")]
struct Incremented {
    by: u32,
}

#[derive(Debug, Events)]
enum CounterEvent {
    Incremented(Incremented),
}

#[derive(Clone, Debug, Default)]
struct Counter {
    total: u32,
}

impl crate::State for Counter {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        let CounterEvent::Incremented(incremented) = event;
        Self {
            total: self.total + incremented.by,
        }
    }
}

#[derive(Clone, Debug)]
struct Increment {
    counter_id: String,
    by: u32,
    only_if_new: bool,
}

impl Command for Increment {
    fn subject(&self) -> String {
        format!("/counters/{}", self.counter_id)
    }

    fn preconditions(&self) -> Vec<Precondition> {
        if self.only_if_new {
            return vec![Precondition::require(
                eventsourcingdb::Precondition::is_subject_pristine(self.subject(), None),
            )];
        }

        vec![Precondition::on_state_read()]
    }
}

/// Increments a counter, which must not exceed 10, and writes nothing for an
/// increment by 0.
fn counter_decider() -> Decider<Increment, Counter, DomainError> {
    Decider::new(|command: Increment, counter: Counter| async move {
        if command.by == 0 {
            return Ok(Vec::new());
        }
        if counter.total + command.by > 10 {
            return Err(DomainError::new("the counter would exceed 10"));
        }

        Ok(vec![CounterEvent::Incremented(Incremented {
            by: command.by,
        })])
    })
}

/// The user of a request, which the header `x-user` names, as an extractor
/// of an application determines it.
struct Caller;

impl<S: Send + Sync> FromRequestParts<S> for Caller {
    type Rejection = HttpError;

    fn from_request_parts(
        parts: &mut Parts,
        _: &S,
    ) -> impl Future<Output = Result<Self, HttpError>> + Send {
        future::ready(if parts.headers.contains_key("x-user") {
            Ok(Self)
        } else {
            Err(HttpError::unauthorized("the header x-user is missing"))
        })
    }
}

#[derive(Deserialize)]
struct IncrementRequest {
    by: u32,
    note: Option<String>,
}

#[derive(Clone)]
struct App {
    store: Arc<Store>,
    increment: Arc<Decider<Increment, Counter, DomainError>>,
}

/// Increments a counter, and answers with its ID next to the revision.
async fn increment(
    State(app): State<App>,
    Path(counter_id): Path<String>,
    _: Caller,
    Strict(request): Strict<IncrementRequest>,
) -> Result<CommandAnswer, HttpError> {
    if request.by > 100 {
        return Err(HttpError::malformed("by has to be at most 100"));
    }
    let _ = request.note;

    let command = Increment {
        counter_id: counter_id.clone(),
        by: request.by,
        only_if_new: false,
    };
    let written = app.store.execute(&app.increment, command).await?;

    Ok(CommandAnswer::new(&written).with(json!({ "id": counter_id })))
}

/// Creates a counter, which must not exist yet.
async fn create(
    State(app): State<App>,
    Path(counter_id): Path<String>,
    _: Caller,
    Strict(request): Strict<IncrementRequest>,
) -> Result<CommandAnswer, HttpError> {
    let command = Increment {
        counter_id,
        by: request.by,
        only_if_new: true,
    };
    let written = app.store.execute(&app.increment, command).await?;

    Ok(CommandAnswer::new(&written))
}

fn router(store: Store) -> Router {
    let app = App {
        store: Arc::new(store),
        increment: Arc::new(counter_decider()),
    };

    Router::new()
        .route("/counters/{id}/increment", post(increment))
        .route("/counters/{id}/create", post(create))
        .with_state(app)
}

/// A store whose database can not be reached.
async fn unreachable_store() -> Store {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the address binds");
    let address = listener.local_addr().expect("the address is known");
    drop(listener);

    let url = format!("http://{address}")
        .parse()
        .expect("the address is a URL");
    Store::builder(Client::new(url, "secret"), "https://thenativeweb.io").build()
}

async fn database_store(database: &Database) -> Store {
    database
        .store("https://thenativeweb.io", CounterEvent::schemas())
        .await
}

fn request_to(path: &str, content_type: Option<&str>, has_user: bool, body: Body) -> Request {
    let mut request = Request::builder().method("POST").uri(path);
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    if has_user {
        request = request.header("x-user", "jane");
    }

    request.body(body).expect("the request is valid")
}

fn json_to(path: &str, body: &str) -> Request {
    request_to(
        path,
        Some("application/json"),
        true,
        Body::from(body.to_owned()),
    )
}

/// Sends the request to the router, and returns the status and the body of
/// the answer.
async fn send(router: Router, request: Request) -> (StatusCode, Value) {
    let response = router.oneshot(request).await.expect("the router answers");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body can be read");

    (
        status,
        serde_json::from_slice(&body).expect("the answer is JSON"),
    )
}

/// A body that fails when it is read, which shows whether it was read.
fn unreadable() -> Body {
    Body::from_stream(stream::once(async {
        Err::<Bytes, _>(io::Error::other("the connection broke"))
    }))
}

#[tokio::test]
async fn a_request_without_a_user_is_unauthorized_before_its_body_is_read() {
    let router = router(unreachable_store().await);

    let (status, body) = send(
        router,
        request_to(
            "/counters/1/increment",
            Some("application/json"),
            false,
            unreadable(),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, json!({ "message": "unauthorized" }));
}

#[tokio::test]
async fn the_content_type_is_required() {
    for content_type in [None, Some("text/plain")] {
        let router = router(unreachable_store().await);

        let (status, _) = send(
            router,
            request_to(
                "/counters/1/increment",
                content_type,
                true,
                Body::from(r#"{"by":1}"#),
            ),
        )
        .await;

        assert_eq!(
            status,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{content_type:?}"
        );
    }
}

#[tokio::test]
async fn malformed_unknown_and_ambiguous_json_is_rejected() {
    for (label, body, message) in [
        (
            "malformed JSON",
            r#"{"by":"#,
            "malformed request: invalid JSON",
        ),
        (
            "an unknown field",
            r#"{"by":1,"bye":2}"#,
            r#"malformed request: unknown field "bye""#,
        ),
        (
            "a name that occurs twice",
            r#"{"by":1,"by":2}"#,
            r#"malformed request: duplicate field "by""#,
        ),
        (
            "data after the value",
            r#"{"by":1} {}"#,
            "malformed request: data after the JSON value",
        ),
    ] {
        let router = router(unreachable_store().await);

        let (status, answer) = send(router, json_to("/counters/1/increment", body)).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}");
        assert_eq!(answer, json!({ "message": message }), "{label}");
    }
}

#[tokio::test]
async fn a_command_that_cannot_be_built_is_rejected() {
    let router = router(unreachable_store().await);

    let (status, body) = send(router, json_to("/counters/1/increment", r#"{"by":101}"#)).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({ "message": "malformed request: by has to be at most 100" })
    );
}

#[tokio::test]
async fn a_body_over_the_limit_is_rejected() {
    let router = router(unreachable_store().await);
    let body = format!(r#"{{"by":1,"note":"{}"}}"#, "a".repeat(MAX_REQUEST_BODY));

    let (status, _) = send(router, json_to("/counters/1/increment", &body)).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn an_unreadable_body_is_rejected() {
    let router = router(unreachable_store().await);

    let (status, body) = send(
        router,
        request_to(
            "/counters/1/increment",
            Some("application/json"),
            true,
            unreadable(),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({ "message": "malformed request: the body could not be read" })
    );
}

#[tokio::test]
async fn an_unreachable_store_is_an_internal_failure() {
    let router = router(unreachable_store().await);

    let (status, body) = send(router, json_to("/counters/1/increment", r#"{"by":1}"#)).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({ "message": "internal server error" }));
}

#[tokio::test]
async fn executes_the_command_and_answers_with_the_revision_and_the_fields() {
    let database = Database::isolated().await;
    let router = router(database_store(&database).await);

    let (status, body) = send(router, json_to("/counters/7/increment", r#"{"by":2}"#)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "id": "7", "revision": "0" }));
}

#[tokio::test]
async fn answers_with_an_empty_revision_if_nothing_was_written() {
    let database = Database::isolated().await;
    let router = router(database_store(&database).await);

    let (status, body) = send(router, json_to("/counters/7/create", r#"{"by":0}"#)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "revision": "" }));
}

#[tokio::test]
async fn a_broken_business_rule_is_explained_with_422() {
    let database = Database::isolated().await;
    let router = router(database_store(&database).await);

    let (status, body) = send(router, json_to("/counters/7/increment", r#"{"by":11}"#)).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body, json!({ "message": "the counter would exceed 10" }));
}

#[tokio::test]
async fn a_conflict_is_answered_with_409() {
    let database = Database::isolated().await;
    let router = router(database_store(&database).await);

    let (status, _) = send(router.clone(), json_to("/counters/7/create", r#"{"by":1}"#)).await;
    assert_eq!(status, StatusCode::OK, "the counter is created");

    let (status, body) = send(router, json_to("/counters/7/create", r#"{"by":1}"#)).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body,
        json!({ "message": "conflict: the data has changed since it was read" })
    );
}
