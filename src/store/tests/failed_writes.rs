// These tests check failed writes, and a stall while waiting for the answer,
// for executing and for writing, which fail alike. The failures come from a
// fake (see fake), since the client SDK does not let a test make them itself.

use std::time::Duration;

use eventsourcingdb::{Client, error::ClientError};

use super::{
    cause_of,
    commands::{Increment, KitWrite, SOURCE, chain, counter_decider},
    fake::{Answer, Api, Fake, Reply, dead_client},
};
use crate::{
    Categorized, Category, DomainError, Error, ExecuteError, Precondition, Store, Subject,
    Transient,
};

const READ_EVENTS: &str = "/api/v1/read-events";
const WRITE_EVENTS: &str = "/api/v1/write-events";

fn unconditionally() -> Vec<Precondition> {
    vec![Precondition::unconditionally()]
}

/// Writes a remark with the given note to /test in the given way, through a
/// database that refuses it.
async fn refused(write: KitWrite, client: Client, note: &str) -> Error<DomainError> {
    let store = Store::builder(client, SOURCE).build();

    write
        .remark(&store, &Subject::new("/test"), note, unconditionally)
        .await
        .expect_err("the database refuses the write")
}

/// Asserts that a write failed with an unknown outcome, which belongs to no
/// category.
fn assert_outcome_unknown(failure: &Error<DomainError>, name: &str) {
    assert!(
        matches!(failure, Error::OutcomeUnknown(_)),
        "{name}: {failure:?}"
    );
    assert_eq!(
        failure.category(),
        None,
        "{name}: an unknown outcome belongs to no category"
    );
}

#[tokio::test]
async fn keeps_its_category_if_the_database_can_not_be_reached() {
    for write in KitWrite::ALL {
        // Executing reads before it writes, so its database has to answer the
        // read, and go away before the write.
        let client = match write {
            KitWrite::Execute => {
                Fake::new(Api::V1)
                    .vanishing_after(READ_EVENTS)
                    .client()
                    .await
            }
            KitWrite::Write => dead_client().await,
        };

        let failure = refused(write, client, "a remark").await;

        assert!(
            matches!(failure, Error::Transient(_)),
            "{write:?}: trying again can not store the events twice: {failure:?}"
        );
        assert!(
            failure.to_string().starts_with(&format!(
                "transient failure: {}",
                write.doing(&Subject::new("/test"))
            )),
            "{write:?}: {failure}"
        );
        assert!(
            chain(&failure)
                .to_lowercase()
                .contains("connection refused"),
            "{write:?}: {}",
            chain(&failure)
        );
    }
}

#[tokio::test]
async fn leaves_the_outcome_unknown_if_the_database_hung_up_while_the_request_was_sent() {
    // The client reports a write as certainly not stored only if it could not
    // connect, by the same rule in every SDK, so a request that was cut off
    // after connecting, but before all of it was sent, leaves the outcome
    // open, although the database can not have stored it.
    for write in KitWrite::ALL {
        let client = Fake::new(Api::V1)
            .route(WRITE_EVENTS, Answer::HangUpOnHeaders)
            .client()
            .await;

        let failure = refused(write, client, &"x".repeat(16 << 20)).await;

        assert_outcome_unknown(&failure, &format!("{write:?}"));
    }
}

#[tokio::test]
async fn keeps_its_category_if_the_database_refused_it_before_writing() {
    for (name, status, reason, category) in [
        (
            "a bad request is permanent",
            400,
            "bad request",
            Category::Permanent,
        ),
        (
            "a rejected API token is permanent",
            401,
            "unauthorized",
            Category::Permanent,
        ),
        (
            "a forbidden request is permanent",
            403,
            "forbidden",
            Category::Permanent,
        ),
        (
            "an unknown route is permanent",
            404,
            "not found",
            Category::Permanent,
        ),
        (
            "a failed precondition is a conflict",
            409,
            "state conflict: precondition failed",
            Category::Transient,
        ),
        (
            "a schema violation is permanent",
            409,
            "schema conflict: event does not match",
            Category::Permanent,
        ),
        (
            "a request that is too large is permanent",
            413,
            "too large",
            Category::Permanent,
        ),
        (
            "a media type that is not supported is permanent",
            415,
            "unsupported",
            Category::Permanent,
        ),
        (
            "a request that can not be processed is permanent",
            422,
            "unprocessable",
            Category::Permanent,
        ),
        (
            "too many requests are transient",
            429,
            "slow down",
            Category::Transient,
        ),
        (
            "a database that shuts down is transient",
            503,
            "server is shutting down",
            Category::Transient,
        ),
        (
            "a database without a valid license is transient",
            507,
            "insufficient storage",
            Category::Transient,
        ),
    ] {
        for write in KitWrite::ALL {
            let client = Fake::new(Api::V1)
                .route(WRITE_EVENTS, Answer::Reply(Reply::status(status, reason)))
                .client()
                .await;

            let failure = refused(write, client, "a remark").await;

            assert_eq!(
                failure.category(),
                Some(category),
                "{write:?}, {name}: {failure:?}"
            );
            assert_eq!(
                matches!(failure, Error::Transient(Transient::Conflict(_))),
                name.ends_with("is a conflict"),
                "{write:?}, {name}: {failure:?}"
            );
            let Some(ClientError::DbApiError(answer)) = cause_of(&failure) else {
                panic!("{write:?}, {name}: the source has to be the answer, got {failure:?}");
            };
            assert_eq!(answer.status, status, "{write:?}, {name}");
            assert_eq!(answer.description, reason, "{write:?}, {name}");
        }
    }
}

#[tokio::test]
async fn leaves_the_outcome_unknown_if_the_request_has_left_but_no_answer_says_that_nothing_was_written()
 {
    const WRITTEN: &str = r#"[{"specversion":"1.0","id":"0","source":"https://thenativeweb.io"}]"#;

    // An answer that does not come from an EventSourcingDB, such as one of a
    // proxy in front of it, names that, whatever its status.
    for (name, answer, detail) in [
        ("the connection breaks", Answer::HangUp, None),
        (
            "the answer is cut off",
            Answer::CutOff(Reply::status(200, WRITTEN)),
            None,
        ),
        (
            "the answer can not be decoded",
            Answer::Reply(Reply::status(200, r#"{"these are":"no events"}"#)),
            None,
        ),
        (
            "the database fails with 500, which may follow storing the events",
            Answer::Reply(Reply::status(
                500,
                "failed to write events: failed to flush the write-ahead log",
            )),
            Some("failed to flush the write-ahead log"),
        ),
        (
            "the answer is 502, which the database never sends",
            Answer::Reply(Reply::status(502, "bad gateway")),
            Some("bad gateway"),
        ),
        (
            "the answer is 504, which the database never sends",
            Answer::Reply(Reply::status(504, "gateway timeout")),
            Some("gateway timeout"),
        ),
        (
            "the answer does not come from an EventSourcingDB, such as a 502 of a proxy",
            Answer::Reply(Reply::foreign(502)),
            Some("the answer does not come from an EventSourcingDB"),
        ),
        (
            "the answer does not come from an EventSourcingDB, such as a 504 of a proxy",
            Answer::Reply(Reply::foreign(504)),
            Some("the answer does not come from an EventSourcingDB"),
        ),
        (
            "the answer does not come from an EventSourcingDB, although it reports success",
            Answer::Reply(Reply::foreign(200)),
            Some("the answer does not come from an EventSourcingDB"),
        ),
    ] {
        for write in KitWrite::ALL {
            let client = Fake::new(Api::V1)
                .route(WRITE_EVENTS, answer.clone())
                .client()
                .await;

            let failure = refused(write, client, "a remark").await;

            assert_outcome_unknown(&failure, &format!("{write:?}, {name}"));
            assert!(
                failure.to_string().starts_with(&format!(
                    "outcome unknown: {}",
                    write.doing(&Subject::new("/test"))
                )),
                "{write:?}, {name}: the message names the write after the outcome: {failure}"
            );
            if let Some(detail) = detail {
                assert!(
                    chain(&failure).contains(detail),
                    "{write:?}, {name}: {}",
                    chain(&failure)
                );
            }
        }
    }
}

#[tokio::test]
async fn keeps_the_error_of_the_client_reachable_when_the_outcome_is_unknown() {
    for write in KitWrite::ALL {
        let refusing = Fake::new(Api::V1)
            .route(WRITE_EVENTS, Answer::Reply(Reply::status(500, "failed")))
            .client()
            .await;
        // A proxy that reports success is no EventSourcingDB to the client.
        let proxied = Fake::new(Api::V1)
            .route(WRITE_EVENTS, Answer::Reply(Reply::foreign(200)))
            .client()
            .await;

        let failure = refused(write, refusing, "a remark").await;
        let Some(ClientError::DbApiError(answer)) = cause_of(&failure) else {
            panic!("{write:?}: the source has to be the answer, got {failure:?}");
        };
        assert_eq!(answer.status, 500, "{write:?}");
        assert_eq!(answer.description, "failed", "{write:?}");

        let failure = refused(write, proxied, "a remark").await;
        assert_outcome_unknown(&failure, &format!("{write:?}"));
        assert!(
            matches!(cause_of(&failure), Some(ClientError::InvalidServerHeader)),
            "{write:?}: {failure:?}"
        );
    }
}

#[tokio::test]
async fn follows_a_redirect_of_a_proxy_as_the_client_does() {
    // The client follows 301, 302, and 303 with a GET, which the database
    // refuses before writing. It does not send the body once more for 307 and
    // 308, and reports the redirect itself as an answer that does not come
    // from an EventSourcingDB, so whether the proxy passed the write on is
    // unknown.
    for (status, is_unknown) in [
        (301, false),
        (302, false),
        (303, false),
        (307, true),
        (308, true),
    ] {
        for write in KitWrite::ALL {
            let client = Fake::new(Api::V1)
                .route(
                    WRITE_EVENTS,
                    Answer::Reply(Reply::redirect(status, "/moved/write-events")),
                )
                .route(
                    "/moved/write-events",
                    Answer::ToPost(Reply::status(500, "failed")),
                )
                .client()
                .await;

            let failure = refused(write, client, "a remark").await;

            if is_unknown {
                assert_outcome_unknown(&failure, &format!("{write:?}, {status}"));
                assert!(
                    matches!(cause_of(&failure), Some(ClientError::InvalidServerHeader)),
                    "{write:?}, {status}: the redirect does not come from an EventSourcingDB: {failure:?}"
                );
            } else {
                let Some(ClientError::DbApiError(answer)) = cause_of(&failure) else {
                    panic!("{write:?}, {status}: the source has to be the answer, got {failure:?}");
                };
                assert!(
                    matches!(failure, Error::Permanent(_)),
                    "{write:?}, {status}: the database refused the GET before writing: {failure:?}"
                );
                assert_eq!(answer.status, 405, "{write:?}, {status}");
            }
        }
    }
}

#[tokio::test]
async fn is_never_tried_again_with_an_unknown_outcome_not_even_by_a_store_that_decides_again_on_conflicts()
 {
    let (url, requests) = Fake::new(Api::V1)
        .route(WRITE_EVENTS, Answer::HangUp)
        .start()
        .await;
    let store = Store::builder(Client::new(url, "secret"), SOURCE)
        .conflict_retries(3)
        .build();

    let failure = store
        .execute(
            &counter_decider(),
            Increment::new("/test", 1).on_state_read(),
        )
        .await
        .expect_err("the connection breaks");

    assert!(
        matches!(failure, ExecuteError::OutcomeUnknown(_)),
        "{failure:?}"
    );
    assert_eq!(
        requests.to(WRITE_EVENTS),
        1,
        "trying again may store the events twice"
    );
}

#[tokio::test]
async fn leaves_the_outcome_unknown_after_a_stall_while_waiting_for_the_answer() {
    // The client ends a request on which nothing has arrived for its stall
    // timeout. A write that waits that long for its answer has left, so
    // whether the database stored the events is unknown.
    for write in KitWrite::ALL {
        let (url, _) = Fake::new(Api::V1)
            .route(WRITE_EVENTS, Answer::Stall(Duration::from_secs(5)))
            .start()
            .await;
        let client = Client::builder(url, "secret")
            .stall_timeout(Duration::from_millis(200))
            .build();

        let failure = refused(write, client, "a remark").await;

        assert_outcome_unknown(&failure, &format!("{write:?}"));
        assert!(
            matches!(cause_of(&failure), Some(ClientError::ConnectionStalled)),
            "{write:?}: the source is the stall: {failure:?}"
        );
        assert!(chain(&failure).contains("stalled"), "{write:?}");
    }
}
