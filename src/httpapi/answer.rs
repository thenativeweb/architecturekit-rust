use std::error::Error;

use bytes::Bytes;
use http::{HeaderValue, Response, StatusCode, header};
use serde::Serialize;
use serde_json::{Map, Value};

use super::{HttpError, error::Chain};
use crate::revision_of;

#[cfg(test)]
mod tests;

/// The answer to a command that has succeeded, as ArchitectureKit for Go
/// answers it: the revision it wrote, which is the ID of the last event, and
/// which a caller hands to a query to read its own writes.
///
/// ```json
/// {"revision":"42"}
/// ```
///
/// The revision is empty if the command wrote nothing. A handler returns it
/// with the events that [`Store::execute`](crate::Store::execute) wrote:
///
/// ```
/// # use architecturekit::{Decider, DomainError, Store, httpapi::{CommandAnswer, HttpError}};
/// # async fn borrow_book(
/// #     store: &Store,
/// #     borrow: &Decider<BorrowBook, Book, DomainError>,
/// #     command: BorrowBook,
/// # ) -> Result<CommandAnswer, HttpError> {
/// let written = store.execute(borrow, command).await?;
///
/// Ok(CommandAnswer::new(&written))
/// # }
/// # #[derive(Clone)] struct BorrowBook;
/// # impl architecturekit::Command for BorrowBook {
/// #     fn subject(&self) -> String { "/books/42".to_owned() }
/// #     fn preconditions(&self) -> Vec<architecturekit::Precondition> { Vec::new() }
/// # }
/// # #[derive(Clone, Default)] struct Book;
/// # #[derive(architecturekit::Events)] enum BookEvent { Borrowed(BookBorrowed) }
/// # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// # #[event(type = "io.thenativeweb.library.book-borrowed")]
/// # struct BookBorrowed {}
/// # impl architecturekit::State for Book {
/// #     type Event = BookEvent;
/// #     fn evolve(self, _: &BookEvent) -> Self { self }
/// # }
/// ```
///
/// The answer is JSON, and never cached.
#[derive(Debug)]
#[must_use]
pub struct CommandAnswer {
    revision: String,
    fields: Fields,
}

/// The fields that come next to the revision.
#[derive(Debug)]
enum Fields {
    None,
    Added(Value),
    Unencodable(serde_json::Error),
    Incomplete(Box<dyn Error + Send + Sync + 'static>),
}

impl CommandAnswer {
    /// The answer to a command that wrote the given events.
    pub fn new(written: &[eventsourcingdb::Event]) -> Self {
        Self {
            revision: revision_of(written).unwrap_or_default(),
            fields: Fields::None,
        }
    }

    /// Returns the revision the command wrote, which is empty if it wrote
    /// nothing.
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Adds further fields next to the revision, such as the ID of what the
    /// command created, as `Adding` does in ArchitectureKit for Go. The fields
    /// are a value that encodes to a JSON object, usually a struct with
    /// `#[derive(Serialize)]`. A value that encodes to `null` adds no fields,
    /// and a later call replaces the fields of an earlier one.
    ///
    /// The kit adds the revision itself, so the fields must not hold one. A
    /// value that holds a revision, that does not encode to a JSON object, or
    /// that can not be encoded at all is a mistake of the program, so the
    /// answer is 500, which is logged, although the events have been written.
    pub fn with(self, fields: impl Serialize) -> Self {
        let fields = match serde_json::to_value(fields) {
            Ok(value) => Fields::Added(value),
            Err(failure) => Fields::Unencodable(failure),
        };

        Self { fields, ..self }
    }

    /// Adds the fields that a function returns, which may fail after the
    /// command has succeeded, as `Adding` in ArchitectureKit for Go allows.
    /// If it failed, the events are written all the same, so the answer stays
    /// a success with the revision, which the caller needs to read its own
    /// writes, and must not take for a reason to send the command again. The
    /// failure is logged as an error.
    pub fn with_result<F, E>(self, fields: Result<F, E>) -> Self
    where
        F: Serialize,
        E: Into<Box<dyn Error + Send + Sync + 'static>>,
    {
        match fields {
            Ok(fields) => self.with(fields),
            Err(failure) => Self {
                fields: Fields::Incomplete(failure.into()),
                ..self
            },
        }
    }
}

/// Answers the command, and logs a failure of its fields.
impl From<CommandAnswer> for Response<Bytes> {
    fn from(answer: CommandAnswer) -> Self {
        let mut body = match answer.fields {
            Fields::None | Fields::Added(Value::Null) => Map::new(),
            Fields::Added(Value::Object(fields)) if fields.contains_key("revision") => {
                return HttpError::internal(
                    "the fields of the answer to a command hold a revision, which the kit adds itself",
                )
                .into();
            }
            Fields::Added(Value::Object(fields)) => fields,
            Fields::Added(other) => {
                return HttpError::internal(format!(
                    "the fields of the answer to a command have to be a JSON object, not {other}"
                ))
                .into();
            }
            Fields::Unencodable(failure) => return HttpError::internal(failure).into(),
            Fields::Incomplete(failure) => {
                log::error!(
                    "architecturekit: incomplete answer, the command has succeeded, but its fields could not be added: {}",
                    Chain(Some(&*failure)),
                );
                Map::new()
            }
        };
        let _ = body.insert("revision".to_owned(), Value::String(answer.revision));

        json(StatusCode::OK, &Value::Object(body))
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for CommandAnswer {
    fn into_response(self) -> axum::response::Response {
        Response::<Bytes>::from(self).map(axum::body::Body::from)
    }
}

/// An answer with the given status and value as JSON, followed by a line
/// break, which is never cached.
pub(super) fn json(status: StatusCode, value: &Value) -> Response<Bytes> {
    let mut response = Response::new(Bytes::from(format!("{value}\n")));
    *response.status_mut() = status;

    let headers = response.headers_mut();
    let _ = headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let _ = headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));

    response
}
