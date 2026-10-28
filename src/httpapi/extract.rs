use std::error::Error;

use axum::{
    RequestExt,
    extract::{FromRequest, Request},
};
use futures_util::StreamExt;
use serde::de::DeserializeOwned;

use super::{
    HttpError, MAX_REQUEST_BODY,
    body::{body_of, no_body, require_json, require_same_origin, too_large},
};

#[cfg(test)]
mod tests;

/// The body of a request as JSON of the given type, read strictly, as
/// ArchitectureKit for Go reads it (see [`body_of`]). It
/// needs the feature `axum`.
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
/// and refuses a request from another origin, as ArchitectureKit for Go does
/// (see [`no_body`]). It needs the feature `axum`.
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
