// These tests mirror TestQuery, TestQueryMethod, TestRevisioned,
// TestIfNoneMatch, TestIfNoneMatchWithABody, TestTagsOfCallers, TestAwait,
// TestAwaiting and TestVarying of ArchitectureKit for Go, with routes of axum
// that the tests build from the building blocks of the kit, over a view in
// memory. Some do not apply to Rust:
//
// - What Go checks about wiring a query does not apply, since a route is an
//   ordinary handler of axum: a nil API, function, view or option, an option
//   given twice, and a pattern without a method, or with another one.
// - A request that is over is dropped, so there is no context of a request to
//   wait or to read the revision within.
// - The tag spells out a query as JSON rather than by reflection, so what Go
//   checks about spelling out values, such as unexported fields and cycles,
//   does not apply. A query that can not be encoded goes without a tag.

use std::{
    future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{FromRequestParts, Request, State},
};
use chrono::DateTime;
use futures_util::TryStreamExt;
use http::{HeaderMap, Method, StatusCode, header, request::Parts};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::{QueryAnswer, REVISION, Reading, WAIT_FOR_REVISION};
use crate::{
    Envelope, InMemoryView, KeyedView, Revisioned, Transient, View,
    httpapi::{HttpError, Strict, query},
};

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Book {
    id: String,
    shelf: String,
}

fn event(id: &str) -> Envelope<()> {
    Envelope {
        id: id.to_owned(),
        time: DateTime::UNIX_EPOCH,
        source: "https://thenativeweb.io".to_owned(),
        subject: "/books".to_owned(),
        event_type: "io.thenativeweb.test.book-shelved".to_owned(),
        trace_parent: None,
        trace_state: None,
        part: 0,
        data: (),
    }
}

/// Shelves a book, and records the event as seen.
fn shelve(books: &InMemoryView<String, Book>, event_id: &str, id: &str, shelf: &str) {
    let _ = books
        .insert(
            &event(event_id),
            Book {
                id: id.to_owned(),
                shelf: shelf.to_owned(),
            },
        )
        .expect("the book is shelved");
    books.seen(event_id);
}

/// The user of a request, which the header `x-user` names.
#[derive(Serialize)]
struct Caller(String);

impl<S: Send + Sync> FromRequestParts<S> for Caller {
    type Rejection = HttpError;

    fn from_request_parts(
        parts: &mut Parts,
        _: &S,
    ) -> impl Future<Output = Result<Self, HttpError>> + Send {
        future::ready(
            parts
                .headers
                .get("x-user")
                .and_then(|user| user.to_str().ok())
                .map(|user| Self(user.to_owned()))
                .ok_or_else(|| HttpError::unauthorized("the header x-user is missing")),
        )
    }
}

#[derive(Clone)]
struct App {
    books: Arc<InMemoryView<String, Book>>,
    wait: Duration,
    answered: Arc<AtomicUsize>,
}

#[derive(Deserialize, Serialize)]
struct BooksOnShelf {
    shelf: String,
}

#[derive(Deserialize, Serialize)]
struct BookById {
    id: String,
}

/// The books on a shelf, as the caller asks for them, which is what the tag
/// depends on.
#[derive(Serialize)]
struct AskedFor<'q> {
    caller: &'q str,
    query: &'q BooksOnShelf,
}

async fn books_on(app: &App, shelf: &str) -> Vec<Book> {
    let _ = app.answered.fetch_add(1, Ordering::SeqCst);
    let Ok(books) = app
        .books
        .all()
        .try_filter(|book| future::ready(book.shelf == shelf))
        .try_collect()
        .await;

    books
}

async fn books_on_shelf(
    State(app): State<App>,
    caller: Caller,
    reading: Reading,
    Strict(query): Strict<BooksOnShelf>,
) -> Result<QueryAnswer<Vec<Book>>, HttpError> {
    let asked_for = AskedFor {
        caller: &caller.0,
        query: &query,
    };

    reading
        .revisioned(&*app.books, &asked_for, app.wait, || async {
            Ok(QueryAnswer::new(books_on(&app, &query.shelf).await))
        })
        .await
}

/// Answers the books on a shelf as of the day that the header `x-day` names,
/// which the answer depends on besides the revision and the query.
async fn books_on_shelf_today(
    State(app): State<App>,
    _: Caller,
    headers: HeaderMap,
    reading: Reading,
    Strict(query): Strict<BooksOnShelf>,
) -> Result<QueryAnswer<Vec<Book>>, HttpError> {
    let today = headers
        .get("x-day")
        .and_then(|day| day.to_str().ok())
        .unwrap_or("Tuesday")
        .to_owned();

    reading
        .varying(today)
        .revisioned(&*app.books, &query, app.wait, || async {
            Ok(QueryAnswer::new(books_on(&app, &query.shelf).await))
        })
        .await
}

async fn awaited_books_on_shelf(
    State(app): State<App>,
    _: Caller,
    reading: Reading,
    Strict(query): Strict<BooksOnShelf>,
) -> Result<QueryAnswer<Vec<Book>>, HttpError> {
    reading.awaiting(&*app.books, app.wait).await?;

    Ok(QueryAnswer::new(books_on(&app, &query.shelf).await))
}

async fn book_by_id(
    State(app): State<App>,
    _: Caller,
    Strict(query): Strict<BookById>,
) -> Result<QueryAnswer<Book>, HttpError> {
    let Ok(book) = app.books.get(&query.id).await;

    Ok(QueryAnswer::found(book))
}

async fn revisioned_book_by_id(
    State(app): State<App>,
    _: Caller,
    reading: Reading,
    Strict(query): Strict<BookById>,
) -> Result<QueryAnswer<Book>, HttpError> {
    reading
        .revisioned(&*app.books, &query, app.wait, || async {
            let Ok(book) = app.books.get(&query.id).await;
            Ok(QueryAnswer::found(book))
        })
        .await
}

fn app_with(wait: Duration) -> App {
    let books = Arc::new(InMemoryView::new(|book: &Book| book.id.clone()));
    shelve(&books, "1", "42", "A");

    App {
        books,
        wait,
        answered: Arc::new(AtomicUsize::new(0)),
    }
}

fn router(app: App) -> Router {
    Router::new()
        .route("/books/on-shelf", query(books_on_shelf))
        .route("/books/on-shelf-today", query(books_on_shelf_today))
        .route("/books/awaited", query(awaited_books_on_shelf))
        .route("/books/by-id", query(book_by_id))
        .route("/books/revisioned-by-id", query(revisioned_book_by_id))
        .with_state(app)
}

fn asking(path: &str, body: &str, headers: &[(&str, &str)]) -> Request {
    let mut request = Request::builder()
        .method(Method::QUERY)
        .uri(path)
        .header("content-type", "application/json")
        .header("x-user", "jane");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }

    request
        .body(Body::from(body.to_owned()))
        .expect("the request is valid")
}

/// Sends the request, and returns the status, the headers and the body of the
/// answer.
async fn send(router: &Router, request: Request) -> (StatusCode, HeaderMap, Bytes) {
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers");
    let (parts, body) = response.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("the body can be read");

    (parts.status, parts.headers, body)
}

fn json_of(body: &Bytes) -> Value {
    serde_json::from_slice(body).expect("the answer is JSON")
}

#[tokio::test]
async fn answers_with_the_result() {
    let router = router(app_with(Duration::ZERO));

    let (status, headers, body) =
        send(&router, asking("/books/by-id", r#"{"id":"42"}"#, &[])).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_of(&body), json!({ "id": "42", "shelf": "A" }));
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert!(
        headers.get(REVISION).is_none(),
        "a plain query carries no revision"
    );
}

#[tokio::test]
async fn without_items_is_not_found() {
    let router = router(app_with(Duration::ZERO));

    let (status, _, body) = send(&router, asking("/books/by-id", r#"{"id":"23"}"#, &[])).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_of(&body), json!({ "message": "not found" }));
}

#[tokio::test]
async fn needs_a_user() {
    let router = router(app_with(Duration::ZERO));
    let mut request = asking("/books/by-id", r#"{"id":"42"}"#, &[]);
    let _ = request.headers_mut().remove("x-user");

    let (status, _, _) = send(&router, request).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn answers_no_other_method() {
    let router = router(app_with(Duration::ZERO));

    for method in [Method::GET, Method::POST, Method::PUT] {
        let mut request = asking("/books/by-id", r#"{"id":"42"}"#, &[]);
        *request.method_mut() = method.clone();

        let (status, headers, _) = send(&router, request).await;

        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{method}");
        assert_eq!(headers[header::ALLOW], "QUERY", "{method}");
    }
}

#[tokio::test]
async fn a_query_without_a_wanted_revision_answers_at_once_with_the_revision_and_a_tag() {
    let router = router(app_with(Duration::from_secs(10)));

    let (status, headers, body) =
        send(&router, asking("/books/on-shelf", r#"{"shelf":"A"}"#, &[])).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_of(&body), json!([{ "id": "42", "shelf": "A" }]));
    assert_eq!(headers[REVISION], "1");
    assert_eq!(headers[header::CACHE_CONTROL], "private, no-cache");
    let tag = headers[header::ETAG].to_str().expect("the tag is text");
    assert!(
        tag.starts_with("\"1-") && tag.ends_with('"'),
        "the tag names the revision: {tag}"
    );
}

#[tokio::test]
async fn a_query_waits_for_the_revision_it_was_asked_for() {
    let app = app_with(Duration::from_secs(10));
    let router = router(app.clone());

    let shelving = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        shelve(&app.books, "2", "23", "A");
    });

    let (status, headers, body) = send(
        &router,
        asking(
            "/books/on-shelf",
            r#"{"shelf":"A"}"#,
            &[("wait-for-revision", "2")],
        ),
    )
    .await;
    shelving.await.expect("the book is shelved");

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[REVISION], "2");
    assert_eq!(
        json_of(&body),
        json!([{ "id": "42", "shelf": "A" }, { "id": "23", "shelf": "A" }])
    );
}

#[tokio::test]
async fn a_query_answers_with_what_it_has_when_the_wait_runs_out() {
    let router = router(app_with(Duration::from_millis(50)));

    let (status, headers, _) = send(
        &router,
        asking(
            "/books/on-shelf",
            r#"{"shelf":"A"}"#,
            &[("wait-for-revision", "9")],
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[REVISION], "1");
}

#[tokio::test]
async fn a_query_answers_at_once_with_no_wait_at_all() {
    let router = router(app_with(Duration::ZERO));
    let started = Instant::now();

    let (status, _, _) = send(
        &router,
        asking(
            "/books/on-shelf",
            r#"{"shelf":"A"}"#,
            &[("wait-for-revision", "9")],
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn a_wanted_revision_that_is_not_one_is_refused() {
    let router = router(app_with(Duration::from_secs(10)));

    for path in ["/books/on-shelf", "/books/awaited"] {
        let (status, _, body) = send(
            &router,
            asking(path, r#"{"shelf":"A"}"#, &[("wait-for-revision", "abc")]),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(
            json_of(&body),
            json!({ "message": r#"malformed request: not a revision: "abc""# }),
            "{path}"
        );
    }
}

#[tokio::test]
async fn nobody_can_make_the_server_wait_without_being_let_in() {
    let router = router(app_with(Duration::from_secs(10)));

    for path in ["/books/on-shelf", "/books/awaited"] {
        let mut request = asking(path, r#"{"shelf":"A"}"#, &[("wait-for-revision", "9")]);
        let _ = request.headers_mut().remove("x-user");
        let started = Instant::now();

        let (status, _, _) = send(&router, request).await;

        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{path}: the caller has to be refused before anything waits"
        );
    }
}

/// Asks for the books on shelf A, and returns the tag of the answer.
async fn tag_of_shelf_a(router: &Router) -> String {
    let (_, headers, _) = send(router, asking("/books/on-shelf", r#"{"shelf":"A"}"#, &[])).await;

    headers[header::ETAG]
        .to_str()
        .expect("the tag is text")
        .to_owned()
}

#[tokio::test]
async fn a_known_revision_is_answered_with_not_modified_without_answering_the_query() {
    let app = app_with(Duration::ZERO);
    let router = router(app.clone());
    let tag = tag_of_shelf_a(&router).await;
    let answered = app.answered.load(Ordering::SeqCst);

    let (status, headers, body) = send(
        &router,
        asking(
            "/books/on-shelf",
            r#"{"shelf":"A"}"#,
            &[("if-none-match", &tag)],
        ),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty(), "a 304 has no body");
    assert!(headers.get(header::CONTENT_TYPE).is_none());
    assert_eq!(headers[REVISION], "1");
    assert_eq!(headers[header::ETAG], tag.as_str());
    assert_eq!(headers[header::CACHE_CONTROL], "private, no-cache");
    assert_eq!(
        app.answered.load(Ordering::SeqCst),
        answered,
        "the query must not be answered"
    );
}

#[tokio::test]
async fn answers_if_none_match_by_the_rules_of_rfc_9110() {
    let router = router(app_with(Duration::ZERO));
    let tag = tag_of_shelf_a(&router).await;

    for (label, header, status) in [
        ("the tag", tag.clone(), StatusCode::NOT_MODIFIED),
        (
            "the tag as a weak one",
            format!("W/{tag}"),
            StatusCode::NOT_MODIFIED,
        ),
        (
            "the tag in a list",
            format!(r#""other", {tag}"#),
            StatusCode::NOT_MODIFIED,
        ),
        ("any tag", "*".to_owned(), StatusCode::NOT_MODIFIED),
        ("another tag", r#""1-0""#.to_owned(), StatusCode::OK),
        (
            "a tag without quotes",
            tag.trim_matches('"').to_owned(),
            StatusCode::OK,
        ),
    ] {
        let (answered, _, _) = send(
            &router,
            asking(
                "/books/on-shelf",
                r#"{"shelf":"A"}"#,
                &[("if-none-match", &header)],
            ),
        )
        .await;

        assert_eq!(answered, status, "{label}");
    }
}

#[tokio::test]
async fn a_new_revision_changes_the_tag() {
    let app = app_with(Duration::ZERO);
    let router = router(app.clone());
    let before = tag_of_shelf_a(&router).await;

    shelve(&app.books, "2", "23", "B");
    let (status, _, _) = send(
        &router,
        asking(
            "/books/on-shelf",
            r#"{"shelf":"A"}"#,
            &[("if-none-match", &before)],
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_ne!(tag_of_shelf_a(&router).await, before);
}

#[tokio::test]
async fn one_query_does_not_match_the_tag_of_another() {
    let router = router(app_with(Duration::ZERO));
    let tag = tag_of_shelf_a(&router).await;

    for (label, path, body) in [
        ("another value", "/books/on-shelf", r#"{"shelf":"B"}"#),
        (
            "another query string",
            "/books/on-shelf?page=2",
            r#"{"shelf":"A"}"#,
        ),
        (
            "another resource",
            "/books/on-shelf-today",
            r#"{"shelf":"A"}"#,
        ),
    ] {
        let (status, headers, _) =
            send(&router, asking(path, body, &[("if-none-match", &tag)])).await;

        assert_eq!(status, StatusCode::OK, "{label}");
        assert_ne!(headers[header::ETAG], tag.as_str(), "{label}");
    }
}

#[tokio::test]
async fn a_query_that_asks_the_same_in_other_words_is_answered_with_not_modified() {
    let router = router(app_with(Duration::ZERO));
    let tag = tag_of_shelf_a(&router).await;

    let (status, _, _) = send(
        &router,
        asking(
            "/books/on-shelf",
            "{ \"shelf\" : \"A\" }\n",
            &[("if-none-match", &tag)],
        ),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn callers_who_ask_for_their_own_get_tags_of_their_own() {
    let router = router(app_with(Duration::ZERO));
    let tag_of_jane = tag_of_shelf_a(&router).await;

    let mut request = asking(
        "/books/on-shelf",
        r#"{"shelf":"A"}"#,
        &[("if-none-match", &tag_of_jane)],
    );
    let _ = request
        .headers_mut()
        .insert("x-user", "john".parse().expect("a header"));
    let (status, headers, _) = send(&router, request).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "a caller who sends the tag of another gets an answer of their own"
    );
    assert_ne!(headers[header::ETAG], tag_of_jane.as_str());
}

#[tokio::test]
async fn a_view_that_has_seen_nothing_carries_no_tag() {
    let app = App {
        books: Arc::new(InMemoryView::new(|book: &Book| book.id.clone())),
        wait: Duration::ZERO,
        answered: Arc::new(AtomicUsize::new(0)),
    };
    let router = router(app);

    let (status, headers, body) = send(
        &router,
        asking(
            "/books/on-shelf",
            r#"{"shelf":"A"}"#,
            &[("if-none-match", "*")],
        ),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "an answer without a tag is answered in full"
    );
    assert_eq!(json_of(&body), json!([]));
    assert!(headers.get(REVISION).is_none());
    assert!(headers.get(header::ETAG).is_none());
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
}

#[tokio::test]
async fn a_failing_query_carries_no_revision() {
    let router = router(app_with(Duration::ZERO));

    let (status, headers, _) = send(
        &router,
        asking("/books/revisioned-by-id", r#"{"id":"23"}"#, &[]),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(headers.get(REVISION).is_none());
    assert!(headers.get(header::ETAG).is_none());
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
}

#[tokio::test]
async fn an_answer_that_depends_on_more_than_the_revision_varies_its_tag() {
    let router = router(app_with(Duration::ZERO));
    let tag_on = |day: &'static str| {
        let router = router.clone();
        async move {
            let (_, headers, _) = send(
                &router,
                asking(
                    "/books/on-shelf-today",
                    r#"{"shelf":"A"}"#,
                    &[("x-day", day)],
                ),
            )
            .await;
            headers[header::ETAG].clone()
        }
    };

    assert_eq!(
        tag_on("Tuesday").await,
        tag_on("Tuesday").await,
        "the same day gives the same tag"
    );
    assert_ne!(
        tag_on("Tuesday").await,
        tag_on("Wednesday").await,
        "another day gives another tag"
    );
}

#[tokio::test]
async fn awaiting_waits_for_the_revision_it_was_asked_for_and_tags_nothing() {
    let app = app_with(Duration::from_secs(10));
    let router = router(app.clone());

    let shelving = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        shelve(&app.books, "2", "23", "A");
    });

    let (status, headers, body) = send(
        &router,
        asking(
            "/books/awaited",
            r#"{"shelf":"A"}"#,
            &[("wait-for-revision", "2"), ("if-none-match", "*")],
        ),
    )
    .await;
    shelving.await.expect("the book is shelved");

    assert_eq!(
        status,
        StatusCode::OK,
        "awaiting is never answered with 304"
    );
    assert_eq!(json_of(&body).as_array().map(Vec::len), Some(2));
    assert!(headers.get(REVISION).is_none());
    assert!(headers.get(header::ETAG).is_none());
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
}

/// A view that fails to read its revision, as a view in a database may.
struct Broken;

impl Revisioned for Broken {
    type Error = Transient;

    fn revision(&self) -> impl Future<Output = Result<Option<String>, Transient>> + Send {
        future::ready(Err(Transient::Other(crate::Failure::new(
            "the connection broke",
            None,
        ))))
    }

    fn wait_for(&self, _: &str) -> impl Future<Output = Result<(), Transient>> + Send {
        future::ready(Err(Transient::Other(crate::Failure::new(
            "the connection broke",
            None,
        ))))
    }
}

fn parts_with(headers: &[(&str, &str)]) -> Parts {
    let mut request = http::Request::builder().method(Method::QUERY).uri("/books");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }

    request
        .body(())
        .expect("the request is valid")
        .into_parts()
        .0
}

#[tokio::test]
async fn a_view_that_fails_to_read_its_revision_is_answered_by_its_category_without_answering() {
    let answered = AtomicUsize::new(0);

    let failure = Reading::of(&parts_with(&[]))
        .revisioned(&Broken, &json!({}), Duration::ZERO, || async {
            let _ = answered.fetch_add(1, Ordering::SeqCst);
            Ok(QueryAnswer::new(()))
        })
        .await
        .expect_err("the revision can not be read");

    assert_eq!(failure.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(answered.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn awaiting_answers_a_failure_while_waiting_as_every_other_error() {
    let failure = Reading::of(&parts_with(&[("wait-for-revision", "9")]))
        .awaiting(&Broken, Duration::from_secs(10))
        .await
        .expect_err("the view fails while it waits");

    assert_eq!(failure.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn awaiting_ignores_a_request_that_asks_for_nothing() {
    for headers in [vec![], vec![("wait-for-revision", "")]] {
        let waited = Reading::of(&parts_with(&headers))
            .awaiting(&Broken, Duration::from_secs(10))
            .await;

        assert!(waited.is_ok(), "{headers:?}");
    }
}

/// A query that refuses to be encoded.
struct Unencodable;

impl Serialize for Unencodable {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("refusing to be encoded"))
    }
}

#[tokio::test]
async fn a_query_that_can_not_be_encoded_goes_without_a_tag() {
    let books = InMemoryView::new(|book: &Book| book.id.clone());
    shelve(&books, "1", "42", "A");

    let answer = Reading::of(&parts_with(&[("if-none-match", "*")]))
        .revisioned(&books, &Unencodable, Duration::ZERO, || async {
            Ok(QueryAnswer::new(()))
        })
        .await
        .expect("the query is answered");
    let response = http::Response::<Bytes>::from(answer);

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[REVISION], "1");
    assert!(response.headers().get(header::ETAG).is_none());
    assert_eq!(
        response.headers()[header::CACHE_CONTROL],
        "private, no-cache"
    );
}

#[test]
fn names_the_headers_as_arch_kit_for_go_does() {
    assert_eq!(WAIT_FOR_REVISION.as_str(), "wait-for-revision");
    assert_eq!(REVISION.as_str(), "revision");
}
