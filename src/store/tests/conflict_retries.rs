// These tests mirror TestWithConflictRetries, TestResolvePreconditions and
// TestViolatedPrecondition of ArchitectureKit for Go.
//
// A negative number of retries can not be written in Rust, so "panics on a
// negative number of retries" has nothing to check. In place of "stops
// deciding again once the context has ended", a test drops the future while
// the decider decides. The client SDK reads the details of an answer as a JSON
// object, so the cases of TestViolatedPrecondition with details of another
// shape, or with details that are not JSON at all, never reach the kit.
//
// A database that offers the v2 API names the precondition that did not hold,
// and one that offers just the v1 API does not, so some of the tests expect
// different numbers of decisions from each (see assert_decisions_by_api).

use std::{
    future, ptr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use eventsourcingdb::error::{ClientError, code};
use serde_json::{Value, json};
use tokio::sync::Notify;

use super::{
    Counter, CounterEvent, Incremented, cause_of,
    commands::{Increment, SOURCE, counter_decider, interfering, total_in},
    database::{self, subject_for},
    fake::{Answer, Api, Fake, Reply},
    store,
};
use crate::{
    Decider, DomainError, EventOn, ExecuteError, Failure, Permanent, Precondition, Store,
    Transient, WriteError,
    command::{resolve_preconditions, violated_precondition},
};

const V2_WRITE_EVENTS: &str = "/api/v2/data/write-events";

async fn retrying(retries: usize) -> Store {
    Store::builder(database::client().await, SOURCE)
        .conflict_retries(retries)
        .build()
}

/// Asserts that executing reported a conflict after the given number of
/// decisions, which depends on the API the database offers. A database that
/// offers the v2 API reports the conflict with the status 200, and names the
/// precondition that did not hold, by its index among the declared ones, in
/// the details, so there the decisions have to be `through_v2`, and the index
/// `violated`. A database that offers just the v1 API answers with 409 and
/// names none, so there they have to be `through_v1`.
fn assert_decisions_by_api(
    failure: &ExecuteError<DomainError>,
    decisions: usize,
    through_v2: usize,
    through_v1: usize,
    violated: u64,
) {
    assert!(
        matches!(failure, ExecuteError::Transient(Transient::Conflict(_))),
        "{failure:?}"
    );
    let Some(ClientError::DbApiError(answer)) = cause_of(failure) else {
        panic!("the source has to be the answer of the database, got {failure:?}");
    };
    assert_eq!(answer.code, code::PRECONDITION_VIOLATED);

    if answer.status == 409 {
        assert!(
            answer.details.is_empty(),
            "a database that offers just the v1 API names no precondition"
        );
        assert_eq!(decisions, through_v1, "through the v1 API");
        return;
    }

    assert_eq!(answer.status, 200);
    assert_eq!(
        Value::Object(answer.details.clone()),
        json!({ "precondition": violated })
    );
    assert_eq!(decisions, through_v2, "through the v2 API");
}

#[tokio::test]
async fn decides_again_on_a_conflict_and_writes_then() {
    let store = retrying(2).await;
    let subject = subject_for("retries-writes");
    let (decider, decisions) = interfering(1).await;

    let written = store
        .execute(&decider, Increment::new(&subject, 1).on_state_read())
        .await
        .expect("the second decision is written");

    assert_eq!(written.len(), 1);
    assert_eq!(decisions.load(Ordering::SeqCst), 2);
    assert_eq!(
        total_in(&store, &subject).await,
        101,
        "the second decision builds on what was written in between"
    );
}

#[tokio::test]
async fn decides_on_the_new_state() {
    let (decider, decisions) = interfering(1).await;

    let failure = retrying(2)
        .await
        .execute(
            &decider,
            Increment::new(&subject_for("retries-new-state"), 1)
                .up_to(50)
                .on_state_read(),
        )
        .await
        .expect_err("what was written in between exceeds the limit");

    assert!(matches!(failure, ExecuteError::Decider(_)), "{failure:?}");
    assert_eq!(decisions.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reports_the_conflict_once_the_retries_are_used_up() {
    let (decider, decisions) = interfering(10).await;

    let failure = retrying(2)
        .await
        .execute(
            &decider,
            Increment::new(&subject_for("retries-used-up"), 1).on_state_read(),
        )
        .await
        .expect_err("somebody else writes every time");

    assert!(
        matches!(failure, ExecuteError::Transient(Transient::Conflict(_))),
        "{failure:?}"
    );
    assert_eq!(
        decisions.load(Ordering::SeqCst),
        3,
        "one decision and two more"
    );
}

#[tokio::test]
async fn reports_a_conflict_right_away_without_retries() {
    let (decider, decisions) = interfering(1).await;

    let failure = store()
        .await
        .execute(
            &decider,
            Increment::new(&subject_for("retries-none"), 1).on_state_read(),
        )
        .await
        .expect_err("somebody else writes in between");

    assert!(
        matches!(failure, ExecuteError::Transient(Transient::Conflict(_))),
        "{failure:?}"
    );
    assert_eq!(decisions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn never_decides_again_on_a_revision_the_caller_hands_over() {
    let store = retrying(2).await;
    let subject = subject_for("retries-revision");

    let seeded = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");

    let (decider, decisions) = interfering(1).await;
    let failure = store
        .execute(
            &decider,
            Increment::new(&subject, 1).requiring_event_id(seeded[0].id()),
        )
        .await
        .expect_err("somebody else writes in between");

    assert!(
        matches!(failure, ExecuteError::Transient(Transient::Conflict(_))),
        "the caller has to learn about the conflict: {failure:?}"
    );
    assert_eq!(decisions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reports_an_outdated_revision_of_the_caller_next_to_on_state_read_at_once_through_the_v2_api()
 {
    let store = retrying(2).await;
    let subject = subject_for("retries-outdated");

    let seeded = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");
    let _ = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");

    // The decider never interferes, so on_state_read holds every time, and
    // only the revision of the caller, which the second event outdated, does
    // not.
    let (decider, decisions) = interfering(0).await;
    let (on, outdated) = (subject.clone(), seeded[0].id().to_owned());
    let failure = store
        .execute(
            &decider,
            Increment::new(&subject, 1).declaring(move || {
                vec![
                    Precondition::on_state_read(),
                    Precondition::on_event_id(on.clone(), outdated.clone()),
                ]
            }),
        )
        .await
        .expect_err("the revision of the caller is outdated");

    assert_decisions_by_api(&failure, decisions.load(Ordering::SeqCst), 1, 3, 1);
    assert_eq!(total_in(&store, &subject).await, 2, "nothing was written");
}

#[tokio::test]
async fn reports_a_precondition_made_with_require_next_to_on_state_read_at_once_through_the_v2_api()
{
    let store = retrying(2).await;
    let subject = subject_for("retries-required");

    let _ = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");

    // The query requires the subject to have no events, as a query may require
    // an ISBN not to be taken, so deciding again can not make it hold.
    let query =
        format!("FROM e IN events WHERE e.subject == '{subject}' PROJECT INTO COUNT() == 0");
    let (decider, decisions) = interfering(0).await;
    let failure = store
        .execute(
            &decider,
            Increment::new(&subject, 1).declaring(move || {
                vec![
                    Precondition::on_state_read(),
                    Precondition::require(eventsourcingdb::Precondition::is_eventql_query_true(
                        query.clone(),
                    )),
                ]
            }),
        )
        .await
        .expect_err("the subject has events");

    assert_decisions_by_api(&failure, decisions.load(Ordering::SeqCst), 1, 3, 1);
    assert_eq!(total_in(&store, &subject).await, 1, "nothing was written");
}

#[tokio::test]
async fn decides_again_through_either_api_if_on_state_read_did_not_hold_among_preconditions_on_several_subjects_that_hold()
 {
    let store = retrying(2).await;
    let subject = subject_for("retries-several");
    let other = format!("{subject}/other");

    let _ = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");
    let on_other = store
        .execute(&counter_decider(), Increment::new(&other, 1))
        .await
        .expect("the command is executed");

    // on_state_read is checked as the same kind as on_event_id, since the
    // subject has an event, and the two are told apart by their index.
    let (decider, decisions) = interfering(1).await;
    let on_other = on_other[0].id().to_owned();
    let written = store
        .execute(
            &decider,
            Increment::new(&subject, 1).declaring(move || {
                vec![
                    Precondition::on_event_id(other.clone(), on_other.clone()),
                    Precondition::on_state_read(),
                    Precondition::on_populated_subject(other.clone()),
                ]
            }),
        )
        .await
        .expect("the second decision is written");

    assert_eq!(written.len(), 1);
    assert_eq!(decisions.load(Ordering::SeqCst), 2);
    assert_eq!(
        total_in(&store, &subject).await,
        102,
        "the second decision builds on what was written in between"
    );
}

#[tokio::test]
async fn uses_up_the_retries_through_either_api_if_on_state_read_does_not_hold_among_other_preconditions()
 {
    let store = retrying(2).await;
    let subject = subject_for("retries-among-others");

    let _ = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");

    let (decider, decisions) = interfering(10).await;
    let on = subject.clone();
    let failure = store
        .execute(
            &decider,
            Increment::new(&subject, 1).declaring(move || {
                vec![
                    Precondition::on_populated_subject(on.clone()),
                    Precondition::on_state_read(),
                ]
            }),
        )
        .await
        .expect_err("somebody else writes every time");

    assert_decisions_by_api(&failure, decisions.load(Ordering::SeqCst), 3, 3, 1);
}

#[tokio::test]
async fn decides_again_only_if_the_database_names_on_state_read_or_no_precondition() {
    for (name, details, expected) in [
        ("on_state_read", json!({ "precondition": 0 }), 3),
        (
            "the revision of the caller",
            json!({ "precondition": 1 }),
            1,
        ),
        (
            "a precondition made with require",
            json!({ "precondition": 2 }),
            1,
        ),
        ("no precondition", json!({}), 3),
        (
            "one beyond the declared preconditions",
            json!({ "precondition": 3 }),
            3,
        ),
    ] {
        let client = Fake::new(Api::V2)
            .route(
                V2_WRITE_EVENTS,
                Answer::Reply(Reply::error_with_details(
                    code::PRECONDITION_VIOLATED,
                    "a precondition does not hold",
                    &details,
                )),
            )
            .client()
            .await;
        let store = Store::builder(client, SOURCE).conflict_retries(2).build();
        let (decider, decisions) = interfering(0).await;

        let failure = store
            .execute(
                &decider,
                Increment::new("/test", 1).declaring(|| {
                    vec![
                        Precondition::on_state_read(),
                        Precondition::on_event_id("/test", "0"),
                        Precondition::require(
                            eventsourcingdb::Precondition::is_eventql_query_true(
                                "FROM e IN events PROJECT INTO COUNT() == 0",
                            ),
                        ),
                    ]
                }),
            )
            .await
            .expect_err(name);

        assert!(
            matches!(failure, ExecuteError::Transient(Transient::Conflict(_))),
            "{name}: the category stays the same: {failure:?}"
        );
        assert_eq!(decisions.load(Ordering::SeqCst), expected, "{name}");
        let Some(ClientError::DbApiError(answer)) = cause_of(&failure) else {
            panic!("{name}: the source has to be the answer of the database, got {failure:?}");
        };
        assert_eq!(
            Value::Object(answer.details.clone()),
            details,
            "{name}: the app can read the details itself"
        );
    }
}

#[tokio::test]
async fn stops_deciding_once_the_future_is_dropped() {
    let store = retrying(2).await;
    let subject = subject_for("retries-dropped");
    let decisions = Arc::new(AtomicUsize::new(0));
    let deciding = Arc::new(Notify::new());

    let (counted, notified) = (Arc::clone(&decisions), Arc::clone(&deciding));
    let deciding_forever = Decider::<_, _, DomainError>::new(move |_: Increment, _: Counter| {
        let _ = counted.fetch_add(1, Ordering::SeqCst);
        notified.notify_one();
        future::pending::<Result<Vec<CounterEvent>, DomainError>>()
    });

    // Executing is dropped once the decider has begun to decide.
    tokio::select! {
        executed = store.execute(&deciding_forever, Increment::new(&subject, 1).on_state_read()) => {
            panic!("the decider never decides, got {executed:?}");
        }
        () = deciding.notified() => {}
    }

    assert_eq!(decisions.load(Ordering::SeqCst), 1, "nothing decides again");
    assert_eq!(total_in(&store, &subject).await, 0, "nothing was written");
}

// Resolving the declared preconditions, and naming the one that did not hold.

fn json_of(precondition: &eventsourcingdb::Precondition) -> Value {
    serde_json::to_value(precondition).expect("a precondition of the client SDK is JSON")
}

fn resolved(subject: &str, declared: Vec<Precondition>, last_event_id: Option<&str>) -> Vec<Value> {
    resolve_preconditions(subject, declared, last_event_id)
        .iter()
        .map(json_of)
        .collect()
}

/// The preconditions of a command on /books/42 that checks several subjects,
/// three of them as the same kind of the database, isSubjectOnEventId, which
/// only their index tells apart: `on_state_read`, once an event was read, and
/// both of `on_event_id`.
fn several_of_a_kind() -> Vec<Precondition> {
    vec![
        Precondition::on_event_id("/books/23", "3"),
        Precondition::on_state_read(),
        Precondition::on_event_id("/books/42", "5"),
        Precondition::on_pristine_subject("/books/7"),
        Precondition::require(eventsourcingdb::Precondition::is_eventql_query_true(
            "FROM e IN events PROJECT INTO COUNT() == 0",
        )),
    ]
}

#[test]
fn fills_in_on_state_read_with_the_last_event_read() {
    assert_eq!(
        resolved("/books/42", vec![Precondition::on_state_read()], Some("7")),
        [json_of(
            &eventsourcingdb::Precondition::is_subject_on_event_id("/books/42", "7", None)
        )]
    );
}

#[test]
fn fills_in_on_state_read_on_a_subject_without_events_with_a_pristine_check() {
    assert_eq!(
        resolved("/books/42", vec![Precondition::on_state_read()], None),
        [json_of(
            &eventsourcingdb::Precondition::is_subject_pristine("/books/42", None)
        )]
    );
}

#[test]
fn passes_required_preconditions_on_as_they_are_in_order() {
    let first = || eventsourcingdb::Precondition::is_subject_populated("/books/1", None);
    let second = || {
        eventsourcingdb::Precondition::is_eventql_query_true("FROM e IN events PROJECT INTO true")
    };

    let resolved = resolved(
        "/books/42",
        vec![
            Precondition::require(first()),
            Precondition::on_state_read(),
            Precondition::require(second()),
        ],
        Some("7"),
    );

    assert_eq!(resolved.len(), 3);
    assert_eq!(resolved[0], json_of(&first()));
    assert_eq!(resolved[2], json_of(&second()));
}

#[test]
fn resolves_on_event_id_into_the_revision_check_of_the_database() {
    assert_eq!(
        resolved(
            "/books/42",
            vec![
                Precondition::on_event_id("/books/23", "5"),
                Precondition::on_state_read(),
            ],
            Some("7"),
        ),
        [
            json_of(&eventsourcingdb::Precondition::is_subject_on_event_id(
                "/books/23",
                "5",
                None
            )),
            json_of(&eventsourcingdb::Precondition::is_subject_on_event_id(
                "/books/42",
                "7",
                None
            )),
        ]
    );
}

#[test]
fn checks_nothing_for_unconditionally() {
    assert_eq!(
        resolved(
            "/books/42",
            vec![Precondition::unconditionally()],
            Some("7")
        ),
        Vec::<Value>::new()
    );
}

#[test]
fn resolves_every_declared_precondition_at_the_index_it_is_declared_at_adding_none() {
    // The database names a precondition that did not hold by its index in the
    // write, so that index has to be the one of the declared precondition (see
    // violated_precondition).
    let expected: Vec<Value> = several_of_a_kind()
        .iter()
        .map(|precondition| match precondition.database() {
            Some(database) => json_of(database),
            None => json_of(&eventsourcingdb::Precondition::is_subject_on_event_id(
                "/books/42",
                "7",
                None,
            )),
        })
        .collect();

    assert_eq!(
        resolved("/books/42", several_of_a_kind(), Some("7")),
        expected
    );
}

/// Returns the failure of a write that a fake refuses as a database that
/// offers the v2 API does, with an error of the given code and details.
async fn failure_with(code: &str, details: &Value) -> Failure {
    let client = Fake::new(Api::V2)
        .route(
            V2_WRITE_EVENTS,
            Answer::Reply(Reply::error_with_details(
                code,
                "/preconditions/1: precondition isSubjectOnEventId does not hold",
                details,
            )),
        )
        .client()
        .await;

    failure_of_write(client).await
}

/// Returns the failure of a write that the database of the client refuses.
async fn failure_of_write(client: eventsourcingdb::Client) -> Failure {
    let refusal = Store::builder(client, SOURCE)
        .build()
        .write(
            vec![EventOn::new("/books/42", &Incremented { by: 1 })],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect_err("the database refuses the write");

    match refusal {
        WriteError::Transient(Transient::Conflict(failure) | Transient::Other(failure))
        | WriteError::Permanent(Permanent::Unverified(failure) | Permanent::Other(failure))
        | WriteError::OutcomeUnknown(failure) => failure,
        WriteError::NotARevision(refusal) => panic!("the write was not sent: {refusal:?}"),
    }
}

#[tokio::test]
async fn is_the_declared_precondition_at_the_index_the_details_name() {
    let declared = several_of_a_kind();

    for (index, expected) in declared.iter().enumerate() {
        let conflict = failure_with(
            code::PRECONDITION_VIOLATED,
            &json!({ "precondition": index }),
        )
        .await;

        let violated = violated_precondition(&conflict, &declared);

        assert!(
            violated.is_some_and(|violated| ptr::eq(violated, expected)),
            "index {index}: {violated:?}"
        );
    }
}

#[tokio::test]
async fn tells_on_state_read_from_the_others_of_the_same_kind_on_its_own_subject_and_on_another_one()
 {
    let declared = several_of_a_kind();

    for (index, is_on_state_read) in [(1, true), (0, false), (2, false)] {
        let conflict = failure_with(
            code::PRECONDITION_VIOLATED,
            &json!({ "precondition": index }),
        )
        .await;

        let violated =
            violated_precondition(&conflict, &declared).expect("the details name a precondition");

        assert_eq!(
            violated.is_on_state_read(),
            is_on_state_read,
            "index {index}"
        );
    }
}

#[tokio::test]
async fn is_none_for_details_that_name_no_declared_precondition() {
    let declared = several_of_a_kind();

    for details in [
        // An index beyond the declared preconditions.
        json!({ "precondition": 5 }),
        json!({ "precondition": -1 }),
        json!({ "precondition": 1_099_511_627_776_u64 }),
        // No index.
        json!({}),
        json!({ "precondition": null }),
        json!({ "event": 1 }),
        // An index that can not be read.
        json!({ "precondition": "1" }),
        json!({ "precondition": 1.5 }),
        json!({ "precondition": true }),
    ] {
        let conflict = failure_with(code::PRECONDITION_VIOLATED, &details).await;

        assert!(
            violated_precondition(&conflict, &declared).is_none(),
            "details {details}"
        );
    }
}

#[tokio::test]
async fn is_none_for_a_database_that_offers_just_the_v1_api_which_names_none() {
    // The client gives the 409 the code of the v2 API, but there are no
    // details to give it.
    let client = Fake::new(Api::V1)
        .route(
            "/api/v1/write-events",
            Answer::Reply(Reply::status(409, "state conflict: precondition failed")),
        )
        .client()
        .await;

    let conflict = failure_of_write(client).await;

    assert!(violated_precondition(&conflict, &several_of_a_kind()).is_none());
}

#[tokio::test]
async fn is_none_for_another_code_even_if_its_details_name_a_precondition() {
    // A query that is malformed is named by its index as well, but it is a
    // mistake of the request, not a precondition that did not hold.
    let refusal = failure_with(code::QUERY_MALFORMED, &json!({ "precondition": 4 })).await;

    assert!(violated_precondition(&refusal, &several_of_a_kind()).is_none());
}

#[test]
fn is_none_for_a_failure_without_an_answer() {
    let declared = several_of_a_kind();

    for failure in [
        Failure::new("connection refused", None),
        Failure::new(
            "connection refused",
            Some(Box::new(std::io::Error::other("connection refused"))),
        ),
    ] {
        assert!(
            violated_precondition(&failure, &declared).is_none(),
            "{failure:?}"
        );
    }
}
