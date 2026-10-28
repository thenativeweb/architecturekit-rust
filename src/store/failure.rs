//! How a failure of the client is sorted into a category.

use eventsourcingdb::error::{ClientError, DbApiError, code};
use status::{CONFLICT, OK, REQUEST_TIMEOUT, TOO_MANY_REQUESTS, UNAUTHORIZED};

use crate::{Failure, Permanent, Transient};

/// The statuses the rules name, as numbers, so that this does not depend on the
/// HTTP crate the client uses.
mod status {
    pub(super) const OK: u16 = 200;
    pub(super) const REQUEST_TIMEOUT: u16 = 408;
    pub(super) const CONFLICT: u16 = 409;
    pub(super) const TOO_MANY_REQUESTS: u16 = 429;
    pub(super) const UNAUTHORIZED: u16 = 401;
}

/// The codes with which the database refuses a request for now: it is shutting
/// down, its store refuses to be written for the time being, or it waits for
/// somebody to do something, such as to free space on the disk, to restart it
/// after its store failed, or to give it a license that covers more. Trying the
/// same request again later may succeed, so they are transient. A store that
/// has reached the capacity of its license belongs with them: trying again
/// after reading the state anew, as for a conflict, would never help.
const TRANSIENT_CODES: [&str; 8] = [
    code::SERVER_SHUTTING_DOWN,
    code::STORE_READ_ONLY,
    code::INDEXES_REBUILDING,
    code::RESTORE_IN_PROGRESS,
    code::DISK_FULL,
    code::STORE_FAILED,
    code::LICENSE_INVALID,
    code::CAPACITY_EXCEEDED,
];

/// A failure of the client, sorted into a category.
#[derive(Debug)]
pub(crate) enum Sorted {
    Transient(Transient),
    Permanent(Permanent),
}

/// A failure of a write, which either is sorted into a category, since it
/// certainly stored nothing, or leaves its outcome open.
#[derive(Debug)]
pub(crate) enum WriteSorted {
    Sorted(Sorted),
    OutcomeUnknown(Failure),
}

fn failure(message: String, cause: ClientError) -> Failure {
    Failure::new(message, Some(Box::new(cause)))
}

fn transient(message: String, cause: ClientError) -> Sorted {
    Sorted::Transient(Transient::Other(failure(message, cause)))
}

fn permanent(message: String, cause: ClientError) -> Sorted {
    Sorted::Permanent(Permanent::Other(failure(message, cause)))
}

fn answer_of(error: &ClientError) -> Option<&DbApiError> {
    match error {
        ClientError::DbApiError(answer) => Some(answer),
        _ => None,
    }
}

/// Sorts a failure the database reported into a category, by what its answer
/// means. The rules apply to writing, and to reading as well, apart from a 409
/// without a code, which `read_failure` sorts before it gets here. A write gets
/// here only if it certainly stored nothing (see `write_failure`).
///
/// The client refuses a request before it sends it if the database does not
/// support what the request needs, such as a write with a recursive
/// precondition to a database that offers just the v1 API. Only upgrading the
/// database helps then, so it is permanent, and the message says so. So is a
/// request the client refuses itself, since it is malformed: an empty event
/// type, a schema that is not one, or a URL that can not be parsed.
///
/// An error the database reports with a code is sorted by its code alone:
/// precondition-violated is a conflict, the codes in `TRANSIENT_CODES` are
/// transient, and every other code means that the request itself is wrong,
/// which is permanent, including a code the kit does not know.
///
/// A failure without a code is sorted by its status: without an answer, the
/// database is unreachable or the connection broke, which is transient, and so
/// is an answer that does not come from an EventSourcingDB, such as one of a
/// proxy in front of it while the database restarts. An error within a stream
/// (200 without a code), 408, 429 and 5xx are transient. A 409 is a conflict,
/// unless its description says that an event does not match its schema, which
/// is permanent. Every other status means that the request itself is wrong,
/// which is permanent, and a rejected API token is named.
pub(crate) fn database_failure(error: ClientError, doing: &str) -> Sorted {
    let answer = match &error {
        ClientError::IncompatibleServer => {
            return permanent(
                format!(
                    "{doing}: the database does not support what the request needs, and has to be upgraded"
                ),
                error,
            );
        }
        ClientError::InvalidEventType
        | ClientError::JsonSchemaError
        | ClientError::URLParseError(_) => {
            return permanent(doing.to_owned(), error);
        }
        ClientError::InvalidServerHeader => {
            return transient(
                format!("{doing}: the answer does not come from an EventSourcingDB"),
                error,
            );
        }
        ClientError::DbApiError(answer) => answer,
        _ => return transient(doing.to_owned(), error),
    };

    if !answer.code.is_empty() {
        let code = answer.code.as_str();
        return if code == code::PRECONDITION_VIOLATED {
            Sorted::Transient(Transient::Conflict(failure(doing.to_owned(), error)))
        } else if TRANSIENT_CODES.contains(&code) {
            transient(doing.to_owned(), error)
        } else {
            permanent(doing.to_owned(), error)
        };
    }

    match answer.status.as_u16() {
        OK | REQUEST_TIMEOUT | TOO_MANY_REQUESTS | 500.. => transient(doing.to_owned(), error),
        CONFLICT if answer.description.starts_with("schema conflict") => {
            permanent(doing.to_owned(), error)
        }
        CONFLICT => Sorted::Transient(Transient::Conflict(failure(doing.to_owned(), error))),
        UNAUTHORIZED => permanent(
            format!("{doing}: the database rejected the API token"),
            error,
        ),
        _ => permanent(doing.to_owned(), error),
    }
}

/// Sorts the failure of a read. Reading has no preconditions, so a 409 without
/// a code, which only a database that offers just the v1 API sends, is a
/// refusal of the request like any other, which is permanent. Everything else
/// is sorted as `database_failure` sorts it.
///
/// A read that is dropped before it is complete returns nothing at all, so a
/// state built from part of the history can never reach a decision.
pub(crate) fn read_failure(error: ClientError, doing: &str) -> Sorted {
    if answer_of(&error)
        .is_some_and(|answer| answer.code.is_empty() && answer.status.as_u16() == CONFLICT)
    {
        return permanent(doing.to_owned(), error);
    }

    database_failure(error, doing)
}

/// Sorts the failure of a write. If the client reports that the write may have
/// been stored nonetheless, its outcome is unknown, which the client decides
/// by the same rule in every SDK of EventSourcingDB: a write was certainly not
/// stored only if the client could not connect at all, if it did not send the
/// write, or if the database refused it. A write that was certainly not stored
/// keeps the category that `database_failure` sorts it into, so that trying it
/// again unchanged can not store the events twice.
pub(crate) fn write_failure(error: ClientError, doing: &str) -> WriteSorted {
    let ClientError::OutcomeUnknown(inner) = error else {
        return WriteSorted::Sorted(database_failure(error, doing));
    };

    let message = if matches!(*inner, ClientError::InvalidServerHeader) {
        format!("{doing}: the answer does not come from an EventSourcingDB")
    } else {
        doing.to_owned()
    };

    WriteSorted::OutcomeUnknown(failure(message, *inner))
}

/// Tells whether the database refused to register a schema because the event
/// type has one already, or because stored events of the type do not match
/// it, which only the schemas registered afterwards tell apart. A database that
/// offers just the v1 API answers both with 409, and any other 409 of it is
/// taken for one of them, too.
pub(crate) fn is_schema_conflict(refusal: &ClientError) -> bool {
    answer_of(refusal).is_some_and(|answer| match answer.code.as_str() {
        code::EVENT_SCHEMA_ALREADY_REGISTERED | code::EVENT_SCHEMA_VIOLATED => true,
        "" => answer.status.as_u16() == CONFLICT,
        _ => false,
    })
}

/// Reports that the database refused the schema of an event type, for example
/// because stored events of the type do not match it, which is permanent,
/// whatever the reason.
pub(crate) fn schema_refusal(event_type: &str, refusal: ClientError) -> Permanent {
    Permanent::Other(failure(
        format!("the database refused the schema of {event_type:?}"),
        refusal,
    ))
}
