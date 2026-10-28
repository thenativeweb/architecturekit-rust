// These tests mirror TestRespond, the parts of TestRouteAnswers that need no
// route, and the rules of Adding in ArchitectureKit for Go. What concerns
// wiring does not apply: there is no function to be nil, and a later call of
// with replaces the fields of an earlier one, rather than panicking. A value
// that is nil in Go is None or the unit in Rust, and serde_json encodes NaN
// as null, rather than failing on it.

use bytes::Bytes;
use http::{Response, StatusCode, header};
use log::Level;
use serde::{Serialize, Serializer};
use serde_json::{Value, json};

use super::CommandAnswer;
use crate::{httpapi::logs::captured, projection::tests::stored};

fn written(ids: &[&str]) -> Vec<eventsourcingdb::Event> {
    ids.iter()
        .map(|id| stored(id, "io.thenativeweb.test.incremented", json!({ "by": 1 })))
        .collect()
}

fn answered(answer: CommandAnswer) -> (Response<Bytes>, Vec<(Level, String)>) {
    captured(|| Response::<Bytes>::from(answer))
}

fn body_of(response: &Response<Bytes>) -> Value {
    serde_json::from_slice(response.body()).expect("the answer is JSON")
}

#[test]
fn reports_the_revision_on_success() {
    let (response, logged) = answered(CommandAnswer::new(&written(&["7", "9", "8"])));

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_of(&response), json!({ "revision": "9" }));
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert!(response.body().ends_with(b"\n"));
    assert_eq!(logged, []);
}

#[test]
fn reports_an_empty_revision_if_nothing_was_written() {
    let answer = CommandAnswer::new(&[]);
    assert_eq!(answer.revision(), "");

    let (response, _) = answered(answer);

    assert_eq!(body_of(&response), json!({ "revision": "" }));
}

#[derive(Serialize)]
struct Created {
    id: String,
}

#[test]
fn answers_with_the_fields_next_to_the_revision() {
    let answer = CommandAnswer::new(&written(&["1"])).with(Created {
        id: "42".to_owned(),
    });

    let (response, logged) = answered(answer);

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_of(&response), json!({ "id": "42", "revision": "1" }));
    assert_eq!(logged, []);
}

#[test]
fn adds_no_fields_for_a_value_that_encodes_to_null() {
    for answer in [
        CommandAnswer::new(&written(&["1"])).with(()),
        CommandAnswer::new(&written(&["1"])).with(None::<Created>),
    ] {
        let (response, _) = answered(answer);

        assert_eq!(body_of(&response), json!({ "revision": "1" }));
    }
}

#[test]
fn replaces_the_fields_of_an_earlier_call() {
    let answer = CommandAnswer::new(&written(&["1"]))
        .with(json!({ "id": "23" }))
        .with(json!({ "id": "42" }));

    let (response, _) = answered(answer);

    assert_eq!(body_of(&response), json!({ "id": "42", "revision": "1" }));
}

/// A value that refuses to be encoded.
struct Unencodable;

impl Serialize for Unencodable {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("refusing to be encoded"))
    }
}

#[test]
fn answers_with_500_for_fields_that_are_a_mistake_of_the_program_and_logs_why() {
    for (name, answer, cause) in [
        (
            "fields that hold a revision",
            CommandAnswer::new(&written(&["1"])).with(json!({ "revision": "2" })),
            "hold a revision",
        ),
        (
            "fields that are no object",
            CommandAnswer::new(&written(&["1"])).with(json!(["42"])),
            "have to be a JSON object",
        ),
        (
            "fields that can not be encoded",
            CommandAnswer::new(&written(&["1"])).with(Unencodable),
            "refusing to be encoded",
        ),
    ] {
        let (response, logged) = answered(answer);

        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{name}"
        );
        assert_eq!(
            body_of(&response),
            json!({ "message": "internal server error" }),
            "{name}"
        );
        assert_eq!(logged.len(), 1, "{name}: {logged:?}");
        assert_eq!(logged[0].0, Level::Error, "{name}");
        assert!(logged[0].1.contains(cause), "{name}: {logged:?}");
    }
}

#[test]
fn answers_as_a_success_when_the_fields_fail_after_the_command_has_succeeded() {
    let answer = CommandAnswer::new(&written(&["1"]))
        .with_result(Err::<Created, _>("the customer could not be looked up"));

    let (response, logged) = answered(answer);

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_of(&response), json!({ "revision": "1" }));
    assert_eq!(logged.len(), 1, "{logged:?}");
    assert_eq!(logged[0].0, Level::Error);
    assert!(logged[0].1.contains("incomplete answer"), "{logged:?}");
    assert!(
        logged[0].1.contains("the customer could not be looked up"),
        "{logged:?}"
    );
}

#[test]
fn answers_with_the_fields_when_the_function_succeeds() {
    let answer = CommandAnswer::new(&written(&["1"])).with_result(Ok::<_, String>(Created {
        id: "42".to_owned(),
    }));

    let (response, _) = answered(answer);

    assert_eq!(body_of(&response), json!({ "id": "42", "revision": "1" }));
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn answers_through_axum_as_it_answers_otherwise() {
    use axum::response::IntoResponse;

    let response = CommandAnswer::new(&written(&["3"])).into_response();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body can be read"),
        Bytes::from_static(b"{\"revision\":\"3\"}\n")
    );
}
