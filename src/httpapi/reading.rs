use std::{fmt, future::Future, time::Duration};

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Response, StatusCode, header, request::Parts};
use serde::Serialize;

use super::{HttpError, answer::json};
use crate::{Revisioned, parse_revision};

#[cfg(all(test, feature = "axum"))]
mod tests;

/// The header in which a caller names the revision a query waits for, such as
/// the one that the answer to a command named (see
/// [`CommandAnswer`](super::CommandAnswer)).
pub const WAIT_FOR_REVISION: HeaderName = HeaderName::from_static("wait-for-revision");

/// The header in which the answer to a query names the revision of the view
/// it was read from.
pub const REVISION: HeaderName = HeaderName::from_static("revision");

/// How long a query waits for a revision, unless it says otherwise.
pub const DEFAULT_WAIT: Duration = Duration::from_secs(5);

/// How a query reads a view: the revision the caller waits for, with the
/// header `Wait-For-Revision`, the answers the caller holds already, with
/// `If-None-Match`, and the resource it asks for.
/// It is an extractor of axum with the feature `axum`, and comes from
/// [`Reading::of`] otherwise.
///
/// A handler first builds its query, from the user and the body, and then
/// lets the reading wait and answer, so that nobody can make the server wait
/// without being allowed to ask:
///
/// ```
/// # use std::time::Duration;
/// # use architecturekit::{InMemoryView, KeyedView};
/// # use architecturekit::httpapi::{DEFAULT_WAIT, HttpError, QueryAnswer, Reading};
/// # use serde::Serialize;
/// # #[derive(Clone, Serialize)] struct Book { id: String }
/// # #[derive(Serialize)] struct BookById { id: String }
/// async fn book_by_id(
///     books: &InMemoryView<String, Book>,
///     reading: Reading,
///     query: BookById,
/// ) -> Result<QueryAnswer<Book>, HttpError> {
///     reading
///         .revisioned(books, &query, DEFAULT_WAIT, || async {
///             let Ok(book) = books.get(&query.id).await;
///             Ok(QueryAnswer::found(book))
///         })
///         .await
/// }
/// ```
#[derive(Clone, Debug)]
pub struct Reading {
    wanted: Option<HeaderValue>,
    tags: Vec<HeaderValue>,
    resource: String,
    varies: Option<String>,
}

impl Reading {
    /// Takes what a query reads from the parts of its request.
    #[must_use]
    pub fn of(parts: &Parts) -> Self {
        Self {
            wanted: parts.headers.get(&WAIT_FOR_REVISION).cloned(),
            tags: parts
                .headers
                .get_all(header::IF_NONE_MATCH)
                .iter()
                .cloned()
                .collect(),
            resource: format!(
                "{}?{}",
                parts.uri.path(),
                parts.uri.query().unwrap_or_default()
            ),
            varies: None,
        }
    }

    /// Adds a value that the answer depends on besides the revision of the
    /// view and the query, such as the current day for an answer that counts
    /// the days until something is due. It goes into the tag of the answer
    /// (see [`revisioned`](Reading::revisioned)).
    #[must_use]
    pub fn varying(self, value: impl fmt::Display) -> Self {
        Self {
            varies: Some(value.to_string()),
            ..self
        }
    }

    /// Waits until the view has reached the revision the caller waits for,
    /// but no longer than the given time, and tags nothing. A request without
    /// `Wait-For-Revision` does not wait at all, and one whose wait runs out
    /// is answered with what the view holds, rather than with an error.
    ///
    /// # Errors
    ///
    /// Fails with 400 if the caller waits for a value that is not a revision,
    /// and by its category if the view fails while it waits.
    pub async fn awaiting<V: Revisioned + ?Sized>(
        &self,
        view: &V,
        wait: Duration,
    ) -> Result<(), HttpError> {
        let Some(wanted) = &self.wanted else {
            return Ok(());
        };
        if wanted.is_empty() {
            return Ok(());
        }
        let wanted = String::from_utf8_lossy(wanted.as_bytes());
        let _ = parse_revision(&wanted).map_err(HttpError::malformed)?;

        match tokio::time::timeout(wait, view.wait_for(&wanted)).await {
            Ok(Err(failure)) => Err(HttpError::of(failure)),
            Ok(Ok(())) | Err(_) => Ok(()),
        }
    }

    /// Waits as [`awaiting`](Reading::awaiting) does, and then answers with
    /// the revision of the view: the answer names it in the header
    /// `Revision`, carries a tag of the revision and the query as `ETag`, and
    /// is cached only privately, and checked every time. A caller that holds the tag already, in
    /// `If-None-Match`, is answered with 304, without asking the function for
    /// the answer.
    ///
    /// The tag covers the path and the query string of the request, the
    /// query as it encodes to JSON, and the value given to
    /// [`varying`](Reading::varying), so the query has to hold everything the
    /// answer depends on, such as the user who asks, if callers get answers
    /// of their own. A query that can not be encoded, and a view that has
    /// seen nothing yet, go without a tag, and their answer is not cached.
    ///
    /// # Errors
    ///
    /// Fails as [`awaiting`](Reading::awaiting) does, by its category if the
    /// view can not read its revision, and with what the function fails with.
    pub async fn revisioned<V, Q, T, F, A>(
        &self,
        view: &V,
        query: &Q,
        wait: Duration,
        answer: F,
    ) -> Result<QueryAnswer<T>, HttpError>
    where
        V: Revisioned + ?Sized,
        Q: Serialize + ?Sized,
        F: FnOnce() -> A,
        A: Future<Output = Result<QueryAnswer<T>, HttpError>>,
    {
        self.awaiting(view, wait).await?;

        let revision = view.revision().await.map_err(HttpError::of)?;
        let Some(revision) = revision.filter(|revision| !revision.is_empty()) else {
            return answer().await;
        };

        let tag = self.tag_of(&revision, query);
        if let Some(tag) = &tag
            && self.holds(tag)
        {
            return Ok(QueryAnswer {
                kind: Kind::Unchanged {
                    revision,
                    tag: tag.clone(),
                },
            });
        }

        Ok(answer().await?.tagged(revision, tag))
    }

    /// Returns the tag of an answer at the given revision, or `None` if the
    /// query can not be encoded.
    fn tag_of<Q: Serialize + ?Sized>(&self, revision: &str, query: &Q) -> Option<String> {
        let query = serde_json::to_vec(query).ok()?;

        let mut hash = Fnv64a::default();
        hash.write(self.resource.as_bytes());
        hash.write(b"\0");
        hash.write(&query);
        if let Some(varies) = &self.varies {
            hash.write(b"\0");
            hash.write(varies.as_bytes());
        }

        Some(format!("\"{revision}-{:x}\"", hash.0))
    }

    /// Returns whether the caller holds the given tag already, by the rules
    /// of RFC 9110 for `If-None-Match`: a list of tags, weak or strong, which
    /// are compared weakly, or `*` for any.
    fn holds(&self, tag: &str) -> bool {
        let joined: Vec<String> = self
            .tags
            .iter()
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
            .collect();
        let mut list = joined.join(",");

        loop {
            let rest = list.trim_start_matches([' ', '\t', ',']);
            if rest.starts_with('*') {
                return true;
            }

            let Some(quoted) = rest.strip_prefix("W/").unwrap_or(rest).strip_prefix('"') else {
                return false;
            };
            let Some((opaque, after)) = quoted.split_once('"') else {
                return false;
            };
            if format!("\"{opaque}\"") == tag {
                return true;
            }

            list = after.to_owned();
        }
    }
}

/// The 64-bit hash FNV-1a, which tags the answers.
struct Fnv64a(u64);

impl Default for Fnv64a {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv64a {
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }
}

/// The answer to a query: the result as JSON, which is never cached, unless
/// [`Reading::revisioned`] tags it.
///
/// A query that looks up a single item, and finds none, answers with
/// [`found`](QueryAnswer::found), which is 404 with the text `not found`. To
/// say what was not found, return [`HttpError::not_found`] instead.
#[derive(Debug)]
#[must_use]
pub struct QueryAnswer<T> {
    kind: Kind<T>,
}

#[derive(Debug)]
enum Kind<T> {
    Found {
        result: T,
        revision: Option<(String, Option<String>)>,
    },
    Missing,
    Unchanged {
        revision: String,
        tag: String,
    },
}

impl<T> QueryAnswer<T> {
    /// The answer with the given result.
    pub fn new(result: T) -> Self {
        Self {
            kind: Kind::Found {
                result,
                revision: None,
            },
        }
    }

    /// The answer with the item that a query looked up, which is 404 with the
    /// text `not found` if there is none.
    pub fn found(item: Option<T>) -> Self {
        item.map_or(
            Self {
                kind: Kind::Missing,
            },
            Self::new,
        )
    }

    /// Adds the revision of the view and the tag of the answer to a result.
    /// An answer that found nothing carries neither, as any failure.
    fn tagged(self, revision: String, tag: Option<String>) -> Self {
        match self.kind {
            Kind::Found { result, .. } => Self {
                kind: Kind::Found {
                    result,
                    revision: Some((revision, tag)),
                },
            },
            kind => Self { kind },
        }
    }
}

/// Answers the query.
impl<T: Serialize> From<QueryAnswer<T>> for Response<Bytes> {
    fn from(answer: QueryAnswer<T>) -> Self {
        match answer.kind {
            Kind::Found { result, revision } => {
                let value = match serde_json::to_value(result) {
                    Ok(value) => value,
                    Err(failure) => return HttpError::internal(failure).into(),
                };
                let mut response = json(StatusCode::OK, &value);
                if let Some((revision, tag)) = revision {
                    mark_revision(&mut response, &revision, tag.as_deref());
                }

                response
            }
            Kind::Missing => HttpError::nothing_found().into(),
            Kind::Unchanged { revision, tag } => {
                let mut response = Response::new(Bytes::new());
                *response.status_mut() = StatusCode::NOT_MODIFIED;
                mark_revision(&mut response, &revision, Some(&tag));

                response
            }
        }
    }
}

#[cfg(feature = "axum")]
impl<T: Serialize> axum::response::IntoResponse for QueryAnswer<T> {
    fn into_response(self) -> axum::response::Response {
        Response::<Bytes>::from(self).map(axum::body::Body::from)
    }
}

/// Names the revision and the tag of an answer, which may be cached
/// privately, but has to be checked every time.
fn mark_revision(response: &mut Response<Bytes>, revision: &str, tag: Option<&str>) {
    let headers = response.headers_mut();
    let _ = headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    if let Ok(revision) = HeaderValue::from_str(revision) {
        let _ = headers.insert(REVISION, revision);
    }
    if let Some(tag) = tag.and_then(|tag| HeaderValue::from_str(tag).ok()) {
        let _ = headers.insert(header::ETAG, tag);
    }
}
