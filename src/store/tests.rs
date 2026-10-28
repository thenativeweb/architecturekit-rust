// These tests check the parts of the store that need no execute: sorting the
// failures of the database through load and write, write, register_schemas,
// the verification of signatures, from_latest and ignored events with a
// database, and that a subject scheme agrees with the database on every
// character. The tests of execute, its conflicts and its retries, of failed
// writes, which write and execute share, and of the state cache are in the
// modules execute, conflict_retries, failed_writes and state_cache.
//
// The answers of the database come either from a real EventSourcingDB in a
// container of each test's own (see database), or from a fake that answers the
// routes a test gives it as the test tells it (see fake), since the client SDK
// does not let a test make the error of an answer itself.

mod commands;
mod conflict_retries;
mod database;
mod execute;
mod failed_writes;
mod fake;
mod projections;
mod read;
mod runs;
mod state_cache;
mod views;

use std::{collections::HashMap, error::Error};

use eventsourcingdb::error::{ClientError, code};
use serde::{Deserialize, Serialize};

use crate::{
    Categorized, Category, Event, EventOn, Events, LoadError, Permanent, Precondition,
    RegisterSchemasError, State, Store, SubjectScheme, Transient, WriteError,
};

use database::{Database, subject_for};
use fake::{Api, Reply, refusing};

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.incremented")]
struct Incremented {
    by: i64,
}

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.reset")]
struct Reset {}

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.annotated")]
struct Annotated {
    note: String,
}

/// An event of a type the counter has no variant for.
#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.unrelated")]
struct Unrelated {}

#[derive(Debug, Events)]
enum CounterEvent {
    Incremented(Incremented),
    Reset(Reset),
    Annotated(Annotated),
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Counter {
    total: i64,
}

impl State for Counter {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        match event {
            CounterEvent::Incremented(incremented) => Counter {
                total: self.total + incremented.by,
            },
            CounterEvent::Reset(_) => Counter { total: 0 },
            CounterEvent::Annotated(_) => self,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct CounterFromLatest(Counter);

impl State for CounterFromLatest {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        CounterFromLatest(self.0.evolve(event))
    }

    fn from_latest() -> Option<&'static str> {
        Some(Reset::EVENT_TYPE)
    }
}

async fn write_unconditionally(store: &Store, events: Vec<EventOn>) {
    let _ = store
        .write(events, vec![Precondition::unconditionally()])
        .await
        .expect("the events are written");
}

// Loading.

#[tokio::test]
async fn loads_the_state_of_a_subject_from_its_events() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("loads");

    write_unconditionally(
        &store,
        vec![
            EventOn::new(&subject, &Incremented { by: 3 }),
            EventOn::new(&subject, &Incremented { by: 4 }),
        ],
    )
    .await;

    let counter: Counter = store.load(&subject).await.expect("the state loads");
    assert_eq!(counter.total, 7);
}

#[tokio::test]
async fn loads_a_pristine_subject_as_the_default_state() {
    let counter: Counter = Database::start()
        .await
        .store()
        .await
        .load(&subject_for("pristine"))
        .await
        .expect("the state loads");

    assert_eq!(counter, Counter::default());
}

#[tokio::test]
async fn loads_a_subject_with_an_ignored_event() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("ignored");

    write_unconditionally(
        &store,
        vec![
            EventOn::new(&subject, &Incremented { by: 3 }),
            EventOn::new(
                &subject,
                &Annotated {
                    note: "a remark".to_owned(),
                },
            ),
        ],
    )
    .await;

    let counter: Counter = store.load(&subject).await.expect("the state loads");
    assert_eq!(counter.total, 3);
}

#[tokio::test]
async fn fails_permanently_on_an_event_the_state_has_no_rule_for() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("no-rule");

    write_unconditionally(&store, vec![EventOn::new(&subject, &Unrelated {})]).await;

    let failure = store
        .load::<Counter>(&subject)
        .await
        .expect_err("the state has no rule for it");
    assert!(matches!(failure, LoadError::Permanent(_)), "{failure:?}");
    assert!(
        failure.to_string().contains("no rule for event type"),
        "{failure}"
    );
}

#[tokio::test]
async fn reads_from_the_latest_event_of_that_type() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("from-latest");

    // The counter has no rule for the first event, so reading it would fail.
    // Starting from the latest reset skips it.
    write_unconditionally(
        &store,
        vec![
            EventOn::new(&subject, &Unrelated {}),
            EventOn::new(&subject, &Reset {}),
            EventOn::new(&subject, &Incremented { by: 2 }),
            EventOn::new(&subject, &Reset {}),
            EventOn::new(&subject, &Incremented { by: 5 }),
        ],
    )
    .await;

    let counter: CounterFromLatest = store
        .load(&subject)
        .await
        .expect("the state loads from the latest reset");
    assert_eq!(counter.0.total, 5);
}

#[tokio::test]
async fn reads_everything_if_the_type_is_missing() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("from-latest-missing");

    write_unconditionally(
        &store,
        vec![
            EventOn::new(&subject, &Incremented { by: 3 }),
            EventOn::new(&subject, &Incremented { by: 4 }),
        ],
    )
    .await;

    let counter: CounterFromLatest = store.load(&subject).await.expect("the state loads");
    assert_eq!(counter.0.total, 7);
}

// Verifying.

#[tokio::test]
async fn verifies_the_signatures_of_the_events_it_reads() {
    let database = Database::start().await;
    let key = database.verification_key();
    let store = database
        .store_with(|builder| builder.signature_verification(key))
        .await;
    let subject = subject_for("signed");

    write_unconditionally(&store, vec![EventOn::new(&subject, &Incremented { by: 1 })]).await;

    let counter: Counter = store.load(&subject).await.expect("the signatures match");
    assert_eq!(counter.total, 1);
}

#[tokio::test]
async fn refuses_an_event_whose_signature_does_not_match_the_key() {
    let other_key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key();
    let database = Database::start().await;
    let store = database
        .store_with(|builder| builder.signature_verification(other_key))
        .await;
    let subject = subject_for("signed-otherwise");

    write_unconditionally(&store, vec![EventOn::new(&subject, &Incremented { by: 1 })]).await;

    let failure = store
        .load::<Counter>(&subject)
        .await
        .expect_err("the signature does not match");
    assert!(
        matches!(failure, LoadError::Permanent(Permanent::Unverified(_))),
        "{failure:?}",
    );
    assert_eq!(failure.category(), Some(Category::Permanent));
}

#[test]
#[should_panic(expected = "without_hash_verification contradicts signature_verification")]
fn refuses_to_skip_hashes_while_checking_signatures() {
    let client =
        eventsourcingdb::Client::new("http://localhost:3000".parse().expect("a URL"), "secret");
    let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key();

    let _ = Store::builder(client, "https://thenativeweb.io")
        .without_hash_verification()
        .signature_verification(key)
        .build();
}

// Writing.

#[tokio::test]
async fn writes_events_to_several_subjects_at_once() {
    let database = Database::start().await;
    let store = database.store().await;
    let first = subject_for("several-first");
    let second = subject_for("several-second");

    let written = store
        .write(
            vec![
                EventOn::new(&first, &Incremented { by: 1 }),
                EventOn::new(&second, &Incremented { by: 2 }),
            ],
            vec![Precondition::on_pristine_subject(&first)],
        )
        .await
        .expect("the events are written");

    assert_eq!(written.len(), 2);
    assert_eq!(written[0].source(), "https://thenativeweb.io");
    assert_eq!(store.load::<Counter>(&first).await.expect("loads").total, 1);
    assert_eq!(
        store.load::<Counter>(&second).await.expect("loads").total,
        2
    );
}

#[tokio::test]
async fn reports_a_precondition_that_did_not_hold_as_a_conflict_and_writes_nothing() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("conflict");

    write_unconditionally(&store, vec![EventOn::new(&subject, &Incremented { by: 1 })]).await;

    let failure = store
        .write(
            vec![EventOn::new(&subject, &Incremented { by: 2 })],
            vec![Precondition::on_pristine_subject(&subject)],
        )
        .await
        .expect_err("the subject is not pristine");

    assert!(
        matches!(failure, WriteError::Transient(Transient::Conflict(_))),
        "{failure:?}",
    );
    assert_eq!(
        store.load::<Counter>(&subject).await.expect("loads").total,
        1
    );
}

#[tokio::test]
async fn refuses_the_preconditions_of_a_write_before_writing() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("refused-preconditions");

    for (name, preconditions, message) in [
        (
            "none",
            vec![],
            "a write declares no preconditions, use Precondition::unconditionally to write without any",
        ),
        (
            "on_state_read",
            vec![Precondition::on_state_read()],
            "a write reads no state, so Precondition::on_state_read has nothing to guard",
        ),
        (
            "unconditionally with another one",
            vec![
                Precondition::unconditionally(),
                Precondition::on_pristine_subject(&subject),
            ],
            "a write combines Precondition::unconditionally with other preconditions",
        ),
    ] {
        let failure = store
            .write(
                vec![EventOn::new(&subject, &Incremented { by: 1 })],
                preconditions,
            )
            .await
            .expect_err(name);

        assert!(
            matches!(failure, WriteError::Permanent(_)),
            "{name}: {failure:?}"
        );
        assert!(failure.to_string().contains(message), "{name}: {failure}");
    }

    assert_eq!(
        store.load::<Counter>(&subject).await.expect("loads").total,
        0,
        "nothing was written"
    );
}

#[tokio::test]
async fn refuses_an_event_id_that_is_not_a_revision_without_writing() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("not-a-revision");

    let failure = store
        .write(
            vec![EventOn::new(&subject, &Incremented { by: 1 })],
            vec![Precondition::on_event_id(&subject, "latest")],
        )
        .await
        .expect_err("latest is not a revision");

    assert!(
        matches!(failure, WriteError::NotARevision(_)),
        "{failure:?}"
    );
    assert_eq!(failure.category(), None);
    assert_eq!(
        store.load::<Counter>(&subject).await.expect("loads").total,
        0,
        "nothing was written"
    );
}

#[tokio::test]
async fn writes_nothing_for_no_events_but_checks_the_preconditions_first() {
    let database = Database::start().await;
    let store = database.store().await;

    let written = store
        .write(vec![], vec![Precondition::unconditionally()])
        .await
        .expect("no events are nothing to write");
    assert_eq!(written, Vec::new());

    let failure = store
        .write(vec![], vec![])
        .await
        .expect_err("a write declares its preconditions");
    assert!(matches!(failure, WriteError::Permanent(_)), "{failure:?}");
}

#[tokio::test]
async fn refuses_an_event_without_a_subject() {
    let failure = Database::start()
        .await
        .store()
        .await
        .write(
            vec![EventOn::new("", &Incremented { by: 1 })],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect_err("an event needs a subject");

    assert_eq!(
        failure.to_string(),
        "permanent failure: event 0 of a write needs a subject",
    );
}

#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.keyed")]
struct Keyed {
    #[serde(skip_serializing_if = "Option::is_none")]
    by_position: Option<HashMap<(i32, i32), String>>,
}

impl crate::Schema for (i32, i32) {
    fn schema() -> serde_json::Value {
        serde_json::json!({})
    }
}

#[tokio::test]
async fn refuses_an_event_whose_data_can_not_be_encoded_without_writing() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("unencodable");

    // serde_json writes the keys of a map as strings, which a tuple is not.
    let failure = store
        .write(
            vec![EventOn::new(
                &subject,
                &Keyed {
                    by_position: Some(HashMap::from([((1, 2), "x".to_owned())])),
                },
            )],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect_err("the data can not be encoded");

    assert!(matches!(failure, WriteError::Permanent(_)), "{failure:?}");
    assert!(
        failure
            .to_string()
            .contains(r#"refusing to write an event of type "io.thenativeweb.test.keyed""#),
        "{failure}",
    );
    assert!(
        failure.source().is_some(),
        "the error of serde is the cause"
    );
}

#[tokio::test]
async fn agrees_with_the_database_on_every_character() {
    let database = Database::start().await;
    let store = database.store().await;
    let base = subject_for("characters");
    let scheme = SubjectScheme::new(&format!("{base}/{{value}}"));

    // The characters that check accepts go into a single value, since the
    // database takes a subject only if it takes every character in it, and
    // writing an event takes far longer than having one refused.
    let mut accepted = String::new();

    let characters =
        (0..128_u8)
            .map(char::from)
            .chain(['\u{00fc}', '\u{212a}', '\u{ff21}', '\u{1f600}']);
    for character in characters {
        // A slash separates segments, so the database takes it, but the
        // subject would no longer follow the pattern.
        if character == '/' {
            continue;
        }

        let value = format!("a{character}b");
        if scheme.check(&[&value]).is_ok() {
            accepted.push(character);
            continue;
        }

        // One write each, since the database refuses a write as a whole.
        let failure = store
            .write(
                vec![EventOn::new(
                    format!("{base}/{value}"),
                    &Incremented { by: 1 },
                )],
                vec![Precondition::unconditionally()],
            )
            .await
            .expect_err("the database refuses what check refuses");
        assert_malformed_subject(&failure, &value);
    }

    let values = [accepted, "_".to_owned(), "-".to_owned()];
    let events = values
        .iter()
        .map(|value| EventOn::new(scheme.build(&[value]), &Incremented { by: 1 }))
        .collect();
    let _ = store
        .write(events, vec![Precondition::unconditionally()])
        .await
        .expect("the database takes every value that check accepts");

    for value in &values {
        let counter: Counter = store
            .load(&scheme.build(&[value]))
            .await
            .expect("the database reads the subject");
        assert_eq!(counter.total, 1, "{value:?}");
    }
}

/// Asserts that the database refused a write for the given subject as
/// malformed: with the code subject-malformed, or, from a database that offers
/// just the v1 API, which reports it without a code, with a description that
/// says so.
fn assert_malformed_subject(failure: &WriteError, value: &str) {
    assert!(
        matches!(failure, WriteError::Permanent(_)),
        "{value:?}: {failure:?}"
    );

    let Some(ClientError::DbApiError(answer)) = cause_of(failure) else {
        panic!("{value:?}: the cause has to be the answer of the database, got {failure:?}");
    };
    if answer.code.is_empty() {
        assert!(
            answer.description.contains("malformed subject"),
            "{value:?}: {answer:?}"
        );
    } else {
        assert_eq!(answer.code, code::SUBJECT_MALFORMED, "{value:?}");
    }
}

/// Returns the error of the client that a failure wraps, if there is one.
fn cause_of(failure: &dyn Error) -> Option<&ClientError> {
    failure.source()?.downcast_ref::<ClientError>()
}

// Registering schemas.

#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.titled")]
struct Titled {
    title: String,
}

/// The same event type as Titled, with another shape.
#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.titled")]
struct TitledOtherwise {
    headline: String,
}

#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.registered")]
struct Registered {
    name: String,
}

#[tokio::test]
async fn registers_the_schemas_of_events_and_checks_them_on_every_start() {
    let database = Database::start().await;
    let store = database.store().await;
    let schemas = || [crate::EventSchema::of::<Registered>()];

    store
        .register_schemas(schemas())
        .await
        .expect("the schemas are registered");
    store
        .register_schemas(schemas())
        .await
        .expect("registering them again checks them");
}

#[tokio::test]
async fn fails_permanently_for_a_schema_that_differs_from_the_registered_one() {
    let database = Database::start().await;
    let store = database.store().await;

    store
        .register_schemas([crate::EventSchema::of::<Titled>()])
        .await
        .expect("the first schema is registered");

    let failure = store
        .register_schemas([crate::EventSchema::of::<TitledOtherwise>()])
        .await
        .expect_err("a registered schema can not change");

    assert!(
        matches!(failure, RegisterSchemasError::Permanent(_)),
        "{failure:?}"
    );
    assert!(
        failure
            .to_string()
            .contains("differs from the registered one, which cannot change"),
        "{failure}",
    );
}

#[tokio::test]
async fn refuses_two_different_schemas_for_the_same_event_type() {
    let failure = Database::start()
        .await
        .store()
        .await
        .register_schemas([
            crate::EventSchema::of::<Titled>(),
            crate::EventSchema::of::<TitledOtherwise>(),
        ])
        .await
        .expect_err("one event type has one schema");

    assert_eq!(
        failure.to_string(),
        r#"permanent failure: event type "io.thenativeweb.test.titled" has two different schemas"#,
    );
}

#[tokio::test]
async fn registers_the_same_schema_given_twice_once() {
    Database::start()
        .await
        .store()
        .await
        .register_schemas([
            crate::EventSchema::of::<Registered>(),
            crate::EventSchema::of::<Registered>(),
        ])
        .await
        .expect("the same schema twice is one schema");
}

// Sorting failures, with a fake database.

const READ_EVENTS: &str = "/api/v1/read-events";
const WRITE_EVENTS: &str = "/api/v1/write-events";

async fn load_from(reply: Reply) -> LoadError {
    let store = Store::builder(
        refusing(Api::V1, READ_EVENTS, reply).await,
        "https://thenativeweb.io",
    )
    .build();
    store
        .load::<Counter>("/test")
        .await
        .expect_err("the fake refuses")
}

async fn write_to(api: Api, path: &'static str, reply: Reply) -> WriteError {
    let store = Store::builder(refusing(api, path, reply).await, "https://thenativeweb.io").build();
    store
        .write(
            vec![EventOn::new("/test", &Incremented { by: 1 })],
            vec![Precondition::on_pristine_subject("/test")],
        )
        .await
        .expect_err("the fake refuses")
}

#[tokio::test]
async fn sorts_failures_by_status_when_reading_and_writing() {
    for (name, status, category) in [
        ("a bad request is permanent", 400, Category::Permanent),
        (
            "a rejected API token is permanent",
            401,
            Category::Permanent,
        ),
        (
            "a request that is too large is permanent",
            413,
            Category::Permanent,
        ),
        ("too many requests are transient", 429, Category::Transient),
        (
            "an unavailable database is transient",
            503,
            Category::Transient,
        ),
    ] {
        let read = load_from(Reply::status(status, "refused")).await;
        assert_eq!(
            read.category(),
            Some(category),
            "{name}, when reading: {read:?}"
        );

        let written = write_to(Api::V1, WRITE_EVENTS, Reply::status(status, "refused")).await;
        assert_eq!(
            written.category(),
            Some(category),
            "{name}, when writing: {written:?}"
        );
        assert!(
            !matches!(written, WriteError::Transient(Transient::Conflict(_))),
            "{name}: only 409 is a conflict",
        );
    }
}

#[tokio::test]
async fn sorts_any_other_status_of_500_and_above_as_transient_when_reading() {
    for status in [500, 502, 504] {
        let read = load_from(Reply::status(status, "failed")).await;
        assert_eq!(
            read.category(),
            Some(Category::Transient),
            "{status}: {read:?}"
        );
    }
}

#[tokio::test]
async fn sorts_409_as_permanent_on_every_read_since_reading_has_no_preconditions() {
    let read = load_from(Reply::status(409, "state conflict: precondition failed")).await;

    assert!(matches!(read, LoadError::Permanent(_)), "{read:?}");
}

#[tokio::test]
async fn sorts_409_by_its_reason_when_writing() {
    let conflict = write_to(
        Api::V1,
        WRITE_EVENTS,
        Reply::status(409, "state conflict: precondition failed"),
    )
    .await;
    assert!(
        matches!(conflict, WriteError::Transient(Transient::Conflict(_))),
        "{conflict:?}",
    );

    let schema = write_to(
        Api::V1,
        WRITE_EVENTS,
        Reply::status(409, "schema conflict: event is invalid"),
    )
    .await;
    assert!(matches!(schema, WriteError::Permanent(_)), "{schema:?}");
}

#[tokio::test]
async fn names_a_rejected_api_token() {
    let read = load_from(Reply::status(401, "unauthorized")).await;

    assert!(
        read.to_string()
            .contains("the database rejected the API token"),
        "{read}"
    );
}

#[tokio::test]
async fn treats_an_answer_that_does_not_come_from_an_eventsourcingdb_as_transient_when_reading() {
    let read = load_from(Reply::foreign(502)).await;

    assert_eq!(read.category(), Some(Category::Transient), "{read:?}");
}

#[tokio::test]
async fn leaves_the_outcome_of_a_write_open_that_failed_after_it_may_have_been_stored() {
    // A 500, or an answer of a proxy in front of the database, may come after
    // the events were stored, so the client leaves the outcome open.
    for reply in [Reply::status(500, "failed"), Reply::foreign(502)] {
        let written = write_to(Api::V1, WRITE_EVENTS, reply.clone()).await;

        assert!(
            matches!(written, WriteError::OutcomeUnknown(_)),
            "{reply:?}: {written:?}"
        );
        assert_eq!(written.category(), None, "{reply:?}");
        assert!(
            written.to_string().starts_with("outcome unknown: "),
            "{written}"
        );
    }
}

#[tokio::test]
async fn wraps_the_error_of_the_client_so_that_source_reaches_the_answer_of_the_database() {
    let read = load_from(Reply::status(503, "shutting down")).await;

    let Some(ClientError::DbApiError(answer)) = cause_of(&read) else {
        panic!("the cause has to be the answer of the database, got {read:?}");
    };
    assert_eq!(answer.status, 503);
    assert_eq!(answer.description, "shutting down");
}

#[tokio::test]
async fn sorts_failures_of_the_v2_api_by_their_code() {
    const V2_WRITE_EVENTS: &str = "/api/v2/data/write-events";

    let conflict = write_to(
        Api::V2,
        V2_WRITE_EVENTS,
        Reply::error_with_code(code::PRECONDITION_VIOLATED, "precondition does not hold"),
    )
    .await;
    assert!(
        matches!(conflict, WriteError::Transient(Transient::Conflict(_))),
        "{conflict:?}",
    );

    for transient in [
        code::SERVER_SHUTTING_DOWN,
        code::STORE_READ_ONLY,
        code::INDEXES_REBUILDING,
        code::RESTORE_IN_PROGRESS,
        code::DISK_FULL,
        code::STORE_FAILED,
        code::LICENSE_INVALID,
        code::CAPACITY_EXCEEDED,
    ] {
        let written = write_to(
            Api::V2,
            V2_WRITE_EVENTS,
            Reply::error_with_code(transient, "not now"),
        )
        .await;
        assert!(
            matches!(written, WriteError::Transient(Transient::Other(_))),
            "{transient}: {written:?}"
        );
    }

    for permanent in [
        code::SUBJECT_MALFORMED,
        code::EVENT_SCHEMA_VIOLATED,
        "https://docs.eventsourcingdb.io/errors/from-the-future",
    ] {
        let written = write_to(
            Api::V2,
            V2_WRITE_EVENTS,
            Reply::error_with_code(permanent, "wrong"),
        )
        .await;
        assert!(
            matches!(written, WriteError::Permanent(_)),
            "{permanent}: {written:?}"
        );
    }
}

#[tokio::test]
async fn sorts_failures_by_status_when_reading_the_registered_schemas() {
    let store = Store::builder(
        refusing(
            Api::V1,
            "/api/v1/read-event-types",
            Reply::status(503, "unavailable"),
        )
        .await,
        "https://thenativeweb.io",
    )
    .build();

    let failure = store
        .register_schemas([crate::EventSchema::of::<Registered>()])
        .await
        .expect_err("the fake refuses");

    assert_eq!(failure.category(), Some(Category::Transient), "{failure:?}");
}
