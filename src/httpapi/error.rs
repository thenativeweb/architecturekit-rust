use std::{convert::Infallible, error::Error, fmt};

use bytes::Bytes;
use http::{Response, StatusCode};

use super::answer::json;
use crate::{
    Categorized, Category, EmptyRange, ExecuteError, LoadError, NotARevision, NotASubject,
    PreconditionsError, ReadError, ReadOptionsError, RegisterSchemasError, SubjectValuesError,
    Transient, WriteError,
};

#[cfg(test)]
mod tests;

/// The cause of a failure, which is logged rather than told to the caller.
type Cause = Box<dyn Error + Send + Sync + 'static>;

/// How a failure is logged when it is answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Logged {
    /// Not at all, since the caller is told what is wrong.
    Not,
    /// As information, since the server did not fail.
    AsRefusal,
    /// As an error, since the server failed.
    AsFailure,
}

/// A failure of a request, with the status and the message it is answered
/// with. Every error of the kit turns into it with `?`, and so does every
/// other error through [`HttpError::of`], by its category.
///
/// A failure the caller can fix, such as a broken business rule or a malformed
/// request, is answered with its own text, and not logged. Any other failure
/// may name internals, so it is answered with a fixed text, and its cause is
/// logged: a refusal with 401 or 409 as information, since the server did not
/// fail, and a failure of the server, with 500 or 503, as an error.
///
/// | Failure | Status | Message |
/// |---|---|---|
/// | [`unauthorized`](HttpError::unauthorized) | 401 | `unauthorized` |
/// | [`forbidden`](HttpError::forbidden) | 403 | `forbidden: …` |
/// | [`not_found`](HttpError::not_found) | 404 | `not found: …` |
/// | [`malformed`](HttpError::malformed), a value that is not a revision or not a subject, values that can not compose a subject, an empty range | 400 | its own |
/// | [`too_large`](HttpError::too_large) | 413 | `request body too large: …` |
/// | [`unsupported_media_type`](HttpError::unsupported_media_type) | 415 | `unsupported media type: …` |
/// | a failure of the category [`Category::Domain`] | 422 | its own |
/// | [`Transient::Conflict`] | 409 | `conflict: the data has changed since it was read` |
/// | a failure of the category [`Category::Transient`] | 503 | `internal server error` |
/// | an unknown outcome of a write | 500 | `outcome unknown: the request may have succeeded` |
/// | any other failure | 500 | `internal server error` |
///
/// The answer is JSON, such as `{"message":"unauthorized"}`, and it is never
/// cached. A write whose outcome is unknown keeps 500, so that a caller does
/// not try again, which would store the events twice.
///
/// A view that did not catch up with a write, which
/// [`wait_for_written`](crate::wait_for_written) reports as
/// [`NotCaughtUp`](crate::NotCaughtUp), does not turn into it with `?`: the
/// command has
/// succeeded, so it is answered as the success it is, with
/// [`CommandAnswer::with_result`](super::CommandAnswer::with_result).
#[derive(Debug)]
pub struct HttpError {
    status: StatusCode,
    message: String,
    cause: Option<Cause>,
    logged: Logged,
}

impl HttpError {
    /// A caller who could not be determined, answered with 401 and the fixed
    /// text `unauthorized`, since the cause may name internals, such as the key
    /// a token failed to verify with. The cause is logged as information.
    pub fn unauthorized(cause: impl Into<Cause>) -> Self {
        Self::refused(StatusCode::UNAUTHORIZED, "unauthorized", cause.into())
    }

    /// A caller who is known, but not allowed to do this, answered with 403
    /// and `forbidden: ` followed by the reason.
    pub fn forbidden(reason: impl fmt::Display) -> Self {
        Self::told(StatusCode::FORBIDDEN, format!("forbidden: {reason}"))
    }

    /// Something that does not exist, answered with 404 and `not found: `
    /// followed by what was not found, such as `ticket 7`.
    pub fn not_found(what: impl fmt::Display) -> Self {
        Self::told(StatusCode::NOT_FOUND, format!("not found: {what}"))
    }

    /// A request that can not be understood, answered with 400 and
    /// `malformed request: ` followed by the reason.
    pub fn malformed(reason: impl fmt::Display) -> Self {
        Self::told(
            StatusCode::BAD_REQUEST,
            format!("malformed request: {reason}"),
        )
    }

    /// A request body that is too large, answered with 413 and
    /// `request body too large: ` followed by the reason.
    pub fn too_large(reason: impl fmt::Display) -> Self {
        Self::told(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("request body too large: {reason}"),
        )
    }

    /// A request body of a type the route does not take, answered with 415
    /// and `unsupported media type: ` followed by the reason.
    pub fn unsupported_media_type(reason: impl fmt::Display) -> Self {
        Self::told(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported media type: {reason}"),
        )
    }

    /// Answers any error by its category: one of [`Category::Domain`], such as
    /// a broken business rule, with 422 and its own text, one of
    /// [`Category::Transient`] with 503, and any other with 500, both with a
    /// fixed text. Use it for an error of your own, such as the one of a view
    /// in a database.
    pub fn of<E: Error + Categorized + Send + Sync + 'static>(error: E) -> Self {
        match error.category() {
            Some(Category::Domain) => {
                let message = error.to_string();
                Self {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    message,
                    cause: Some(Box::new(error)),
                    logged: Logged::Not,
                }
            }
            Some(Category::Transient) => Self::failed(
                StatusCode::SERVICE_UNAVAILABLE,
                "internal server error",
                Box::new(error),
            ),
            Some(Category::Permanent) | None => Self::failed(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error",
                Box::new(error),
            ),
        }
    }

    /// Returns the status the failure is answered with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns the message the failure is answered with.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// An item that a query looked up, and did not find, answered with 404 and
    /// the fixed text `not found`, since nothing says what was not found. It is
    /// logged as information.
    pub(crate) fn nothing_found() -> Self {
        Self::refused(
            StatusCode::NOT_FOUND,
            "not found",
            "nothing was found".into(),
        )
    }

    /// A mistake of the program, answered with 500 and a fixed text, and
    /// logged as an error.
    pub(crate) fn internal(cause: impl Into<Cause>) -> Self {
        Self::failed(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal server error",
            cause.into(),
        )
    }

    /// A failure whose message the caller is told, and which is not logged.
    fn told(status: StatusCode, message: String) -> Self {
        Self {
            status,
            message,
            cause: None,
            logged: Logged::Not,
        }
    }

    /// A refusal with a fixed message, whose cause is logged as information.
    fn refused(status: StatusCode, message: &str, cause: Cause) -> Self {
        Self {
            status,
            message: message.to_owned(),
            cause: Some(cause),
            logged: Logged::AsRefusal,
        }
    }

    /// A failure of the server with a fixed message, whose cause is logged as
    /// an error.
    fn failed(status: StatusCode, message: &str, cause: Cause) -> Self {
        Self {
            status,
            message: message.to_owned(),
            cause: Some(cause),
            logged: Logged::AsFailure,
        }
    }

    /// Logs the failure, as it says it is logged.
    fn log(&self) {
        let cause = Chain(self.cause.as_deref());

        match self.logged {
            Logged::Not => {}
            Logged::AsRefusal => log::info!(
                "architecturekit: request refused with {}: {cause}",
                self.status.as_u16(),
            ),
            Logged::AsFailure => log::error!(
                "architecturekit: internal failure, answered with {}: {cause}",
                self.status.as_u16(),
            ),
        }
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for HttpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let cause: &(dyn Error + 'static) = self.cause.as_deref()?;
        Some(cause)
    }
}

/// Answers the failure, with the status, and the message as JSON, which is
/// never cached, and logs it as it says it is logged.
impl From<HttpError> for Response<Bytes> {
    fn from(error: HttpError) -> Self {
        error.log();

        json(
            error.status,
            &serde_json::json!({ "message": error.message }),
        )
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        Response::<Bytes>::from(self).map(axum::body::Body::from)
    }
}

/// Answers every failure of the kit, which all turn into this one: a failure
/// of the decider by its category (see [`HttpError::of`]), a conflict with
/// 409, a write whose outcome is unknown with 500 and a text of its own, an
/// empty range with 400 and its own text, and any other failure by its
/// category.
impl<D: Error + Categorized + Send + Sync + 'static> From<crate::Error<D>> for HttpError {
    fn from(error: crate::Error<D>) -> Self {
        match error {
            crate::Error::Decider(error) => Self::of(error),
            crate::Error::Transient(Transient::Conflict(failure)) => Self::refused(
                StatusCode::CONFLICT,
                "conflict: the data has changed since it was read",
                Box::new(Transient::Conflict(failure)),
            ),
            crate::Error::Transient(failure) => Self::of(failure),
            crate::Error::Permanent(failure) => Self::of(failure),
            error @ crate::Error::OutcomeUnknown(_) => Self::failed(
                StatusCode::INTERNAL_SERVER_ERROR,
                "outcome unknown: the request may have succeeded",
                Box::new(error),
            ),
            crate::Error::EmptyRange(refusal) => refusal.into(),
        }
    }
}

impl<D: Error + Categorized + Send + Sync + 'static> From<ExecuteError<D>> for HttpError {
    fn from(error: ExecuteError<D>) -> Self {
        crate::Error::from(error).into()
    }
}

/// Turns a failure of an operation without a decider into an [`HttpError`],
/// through the failure of the kit that every one of them turns into.
macro_rules! through_the_failure_of_the_kit {
    ($($failure:ty),*) => {$(
        impl From<$failure> for HttpError {
            fn from(error: $failure) -> Self {
                crate::Error::<Infallible>::from(error).into()
            }
        }
    )*};
}

through_the_failure_of_the_kit!(
    LoadError,
    WriteError,
    RegisterSchemasError,
    ReadError,
    ReadOptionsError,
    PreconditionsError
);

/// Answers a value that is not a revision with 400 and its own text, since it
/// comes from the caller, such as a revision in a header.
impl From<NotARevision> for HttpError {
    fn from(refusal: NotARevision) -> Self {
        Self::told(StatusCode::BAD_REQUEST, refusal.to_string())
    }
}

/// Answers a value that is not a subject with 400 and its own text, since it
/// comes from the caller, such as a subject in a request.
impl From<NotASubject> for HttpError {
    fn from(refusal: NotASubject) -> Self {
        Self::told(StatusCode::BAD_REQUEST, refusal.to_string())
    }
}

/// Answers values that can not compose a subject with 400 and their own text,
/// since they come from the caller, such as an ID in the path of a request
/// that [`SubjectScheme::try_build`](crate::SubjectScheme::try_build) refuses.
impl From<SubjectValuesError> for HttpError {
    fn from(refusal: SubjectValuesError) -> Self {
        Self::told(StatusCode::BAD_REQUEST, refusal.to_string())
    }
}

/// Answers an empty range with 400 and its own text, since it comes from the
/// caller, such as the bounds of a read.
impl From<EmptyRange> for HttpError {
    fn from(refusal: EmptyRange) -> Self {
        Self::told(StatusCode::BAD_REQUEST, refusal.to_string())
    }
}

/// Lets `?` pass on the error of a view in memory, which never occurs.
impl From<Infallible> for HttpError {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

/// An error followed by its sources, as a log shows it.
pub(super) struct Chain<'e>(pub(super) Option<&'e (dyn Error + Send + Sync + 'static)>);

impl fmt::Display for Chain<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(error) = self.0 else {
            return formatter.write_str("no cause");
        };

        write!(formatter, "{error}")?;
        let mut source = error.source();
        while let Some(cause) = source {
            write!(formatter, ": {cause}")?;
            source = cause.source();
        }

        Ok(())
    }
}
