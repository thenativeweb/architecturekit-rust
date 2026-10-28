use std::{any::Any, convert::Infallible, error::Error, future};

use axum::{
    RequestExt,
    extract::{FromRequest, FromRequestParts, Request},
    handler::Handler,
    http::{HeaderValue, Method, StatusCode, header, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{MethodRouter, any},
};
use futures_util::StreamExt;
use serde::de::DeserializeOwned;

use super::{
    HttpError, MAX_REQUEST_BODY, Reading,
    body::{body_of, no_body, require_json, require_same_origin, too_large},
};

#[cfg(test)]
mod tests;

/// The body of a request as JSON of the given type, read strictly (see
/// [`body_of`]). It needs the feature `axum`.
///
/// ```
/// # use axum::extract::{Path, State};
/// # use serde::Deserialize;
/// use architecturekit::httpapi::{HttpError, Strict};
///
/// #[derive(Deserialize)]
/// struct BorrowRequest {
///     until: String,
/// }
///
/// async fn borrow_book(
///     Path(book_id): Path<String>,
///     Strict(request): Strict<BorrowRequest>,
/// ) -> Result<String, HttpError> {
///     Ok(format!("{book_id} until {}", request.until))
/// }
/// ```
///
/// It checks that the request claims to be `application/json` before it reads
/// the body, reads at most [`MAX_REQUEST_BODY`] bytes, or fewer if a layer
/// such as `DefaultBodyLimit` of axum sets a lower limit, and refuses the
/// request with an [`HttpError`] otherwise. As it reads the body, it comes
/// last among the extractors of a handler, after the one for the user, so
/// that an unknown caller is refused before the body is read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Strict<T>(pub T);

impl<S, T> FromRequest<S> for Strict<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request(request: Request, _: &S) -> Result<Self, HttpError> {
        require_json(request.headers())?;

        let (parts, body) = request.with_limited_body().into_parts();
        let body = read(body).await?;

        body_of(&parts, &body).map(Strict)
    }
}

/// A route that takes no body, which accepts a body that is empty or `{}`,
/// and refuses a request from another origin (see [`no_body`]). It needs the
/// feature `axum`.
///
/// A request without a body needs no preflight, so a page of another origin
/// could send it in the name of the user. That is why a route that changes
/// something without a body takes this extractor, and comes from the same
/// origin, unless its method is GET, HEAD or OPTIONS.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoBody;

impl<S: Send + Sync> FromRequest<S> for NoBody {
    type Rejection = HttpError;

    async fn from_request(request: Request, _: &S) -> Result<Self, HttpError> {
        let (parts, body) = request.with_limited_body().into_parts();
        require_same_origin(&parts)?;

        let body = read(body).await?;

        no_body(&parts, &body).map(|()| NoBody)
    }
}

/// Reads a body in full, but no more than [`MAX_REQUEST_BODY`] bytes.
async fn read(body: axum::body::Body) -> Result<Vec<u8>, HttpError> {
    let mut chunks = body.into_data_stream();
    let mut read = Vec::new();

    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|failure| {
            if exceeds_a_limit(&failure) {
                HttpError::too_large("the body exceeds the limit of the server")
            } else {
                HttpError::malformed("the body could not be read")
            }
        })?;
        if read.len() + chunk.len() > MAX_REQUEST_BODY {
            return Err(too_large());
        }
        read.extend_from_slice(&chunk);
    }

    Ok(read)
}

/// Returns whether reading a body failed since a layer limits its length, as
/// `DefaultBodyLimit` of axum and `RequestBodyLimitLayer` of tower-http do.
fn exceeds_a_limit(failure: &axum::Error) -> bool {
    let mut cause: Option<&(dyn Error + 'static)> = Some(failure);
    while let Some(error) = cause {
        if error.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        cause = error.source();
    }

    false
}

/// Takes what a query reads from the request (see [`Reading::of`]). It needs
/// the feature `axum`.
impl<S: Send + Sync> FromRequestParts<S> for Reading {
    type Rejection = Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        _: &S,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        future::ready(Ok(Self::of(parts)))
    }
}

/// Routes the method QUERY to the handler, as `get` and `post` of axum route
/// theirs, and answers every other method with 405. A query asks with QUERY,
/// which sends its parameters as a body, as POST does, but changes nothing, as
/// GET does. It needs the feature `axum`.
///
/// ```
/// # use axum::Router;
/// # async fn books_on_shelf() -> &'static str { "[]" }
/// use architecturekit::httpapi::query;
///
/// let router: Router = Router::new().route("/books/on-shelf", query(books_on_shelf));
/// ```
///
/// axum names no filter for QUERY before version 0.9, so the route takes
/// every method, and answers every other one itself, with the method it
/// allows in the header `Allow`.
pub fn query<H, T, S>(handler: H) -> MethodRouter<S>
where
    H: Handler<T, S>,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    any(handler).layer(middleware::from_fn(only_query))
}

/// Lets a request with the method QUERY through, and answers any other one
/// with 405.
async fn only_query(request: Request, next: Next) -> Response {
    if request.method() == Method::QUERY {
        return next.run(request).await;
    }

    let mut response = StatusCode::METHOD_NOT_ALLOWED.into_response();
    let _ = response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("QUERY"));

    response
}

/// Answers a panic of a handler with 500 and the fixed text
/// `internal server error`, and logs it as an error. A handler of axum belongs
/// to the application, so the kit catches no panic itself: use this with the
/// layer `CatchPanicLayer` of tower-http. It needs the feature `axum`.
///
/// ```
/// # use axum::{Router, routing::post};
/// # async fn borrow_book() {}
/// use architecturekit::httpapi::answer_panic;
/// use tower_http::catch_panic::CatchPanicLayer;
///
/// let router: Router = Router::new()
///     .route("/books/borrow", post(borrow_book))
///     .layer(CatchPanicLayer::custom(answer_panic));
/// ```
#[must_use]
pub fn answer_panic(payload: Box<dyn Any + Send + 'static>) -> Response {
    let message = payload
        .downcast::<String>()
        .map(|message| *message)
        .or_else(|payload| {
            payload
                .downcast::<&str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_else(|_| "a value that is no text".to_owned());

    HttpError::internal(format!("panic while handling the request: {message}")).into_response()
}
