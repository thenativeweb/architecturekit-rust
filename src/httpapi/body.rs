use http::{HeaderMap, HeaderValue, Method, header, request::Parts};
use serde::de::DeserializeOwned;

use super::{HttpError, decoding::decode_strictly};

#[cfg(test)]
mod tests;

/// The largest request body that is read, which is 1 MiB. The body is read
/// into memory in full before it is decoded, so it needs an upper bound.
pub const MAX_REQUEST_BODY: usize = 1 << 20;

/// Reads a request body as JSON of the given type, strictly, as
/// ArchitectureKit for Go reads it, for a framework the kit has no extractor
/// for. With axum, use [`Strict`](super::Strict).
///
/// # Errors
///
/// Fails with 415 if the request does not claim to be `application/json`,
/// with 413 if the body is larger than [`MAX_REQUEST_BODY`], and with 400 if it
/// does not fit the type, which says what is wrong in words of its own, such
/// as `malformed request: "items.1.bookId" must be a string`:
///
/// - an empty body, JSON that is not valid, or anything but whitespace after
///   the value;
/// - a name that occurs twice in an object, or a field the type does not
///   know, at any depth;
/// - a value of the wrong kind, a number out of range, or a list of the
///   wrong length.
pub fn body_of<T: DeserializeOwned>(parts: &Parts, body: &[u8]) -> Result<T, HttpError> {
    require_json(&parts.headers)?;
    if body.len() > MAX_REQUEST_BODY {
        return Err(too_large());
    }

    decode_strictly(body).map_err(HttpError::malformed)
}

/// Checks a request to a route that takes no body, as ArchitectureKit for Go
/// checks it, for a framework the kit has no extractor for. With axum, use
/// [`NoBody`](super::NoBody).
///
/// # Errors
///
/// Fails with 403 if the request comes from another origin, since without a
/// body, a request needs no preflight, so a page of another origin could send
/// it in the name of the user, and with 400 if the body is neither empty nor
/// `{}`. A request with the method GET, HEAD or OPTIONS is accepted from any
/// origin.
pub fn no_body(parts: &Parts, body: &[u8]) -> Result<(), HttpError> {
    require_same_origin(parts)?;

    let is_empty_object = || {
        serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(body)
            .is_ok_and(|members| members.is_empty())
    };
    if body.is_empty() || is_empty_object() {
        return Ok(());
    }

    Err(HttpError::malformed(
        "this route takes no body, so the body has to be empty, or {}",
    ))
}

/// Refuses a request that does not claim to be `application/json`.
pub(crate) fn require_json(headers: &HeaderMap) -> Result<(), HttpError> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Err(HttpError::unsupported_media_type(
            "Content-Type is required",
        ));
    };
    let text = String::from_utf8_lossy(value.as_bytes());
    let Some(media_type) = media_type_of(&text) else {
        return Err(HttpError::unsupported_media_type(format!(
            "{text:?} could not be parsed"
        )));
    };
    if media_type != "application/json" {
        return Err(HttpError::unsupported_media_type(format!(
            "{media_type:?} is not application/json"
        )));
    }

    Ok(())
}

/// Refuses a request from another origin, as the cross-origin protection of
/// Go's `net/http` does: a request with the method GET, HEAD or OPTIONS passes,
/// one with a `Sec-Fetch-Site` header passes only for `same-origin` or `none`,
/// and one without passes if it has no `Origin` header, or one whose host is
/// the host of the request.
pub(crate) fn require_same_origin(parts: &Parts) -> Result<(), HttpError> {
    if matches!(parts.method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return Ok(());
    }

    let refused =
        || HttpError::forbidden("a request without a body is not accepted from another origin");

    if let Some(site) = parts.headers.get("sec-fetch-site") {
        return if site == "same-origin" || site == "none" {
            Ok(())
        } else {
            Err(refused())
        };
    }

    let Some(origin) = parts.headers.get(header::ORIGIN) else {
        return Ok(());
    };
    let host = parts
        .headers
        .get(header::HOST)
        .map(HeaderValue::as_bytes)
        .or_else(|| {
            parts
                .uri
                .authority()
                .map(|authority| authority.as_str().as_bytes())
        });
    let origin_host = origin
        .to_str()
        .ok()
        .and_then(|origin| origin.split_once("://"))
        .map(|(_, rest)| rest.split('/').next().unwrap_or_default().as_bytes());

    match (origin_host, host) {
        (Some(origin_host), Some(host)) if origin_host == host => Ok(()),
        _ => Err(refused()),
    }
}

/// The failure of a body that is larger than the kit reads.
pub(crate) fn too_large() -> HttpError {
    HttpError::too_large(format!("at most {MAX_REQUEST_BODY} bytes are read"))
}

/// Reads the type of a media type such as `application/json; charset=utf-8`,
/// in lower case, or `None` if it is not one.
fn media_type_of(value: &str) -> Option<String> {
    let media_type = value.split(';').next()?.trim();
    let (kind, subtype) = media_type.split_once('/')?;
    let is_token = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
    };

    (is_token(kind) && is_token(subtype)).then(|| media_type.to_ascii_lowercase())
}
