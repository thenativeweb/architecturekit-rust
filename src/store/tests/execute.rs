// These tests mirror TestExecute and TestConflict of ArchitectureKit for Go,
// the parts of TestOnEventID and TestEncodingEvents that execute commands, and
// TestAWriteThatTheDatabaseDoesNotSupport, for executing and for writing.
//
// Some of them do not apply to Rust. A decider decides on variants of the
// events of its state, which has a rule for each of them, and none of them can
// be nil, so "refuses an event its state has no rule for and writes nothing",
// "writes none of the events if one has no rule", "checks the rules before the
// encoding like the test fixture", and the four tests of nil events have
// nothing to check. Whether the kit hands the database the same JSON as the
// client does for the events themselves has nothing to compare either: the
// client takes the data as a serde_json::Value, which the kit makes from the
// event, as anyone would. A future that is dropped returns nothing at all, so
// there is no error of an ended context that a mistake in the code could hide
// behind. Two cases of reporting a mistake in the code before an ID that is not
// a revision can not be written in Rust: a zero Precondition, and a
// requirement of nothing. Execute and the test fixture check the
// preconditions and encode the events with the same functions, so their errors
// are the same, and the tests of the fixture check its wording.

use std::{
    collections::HashMap,
    future,
    sync::{Arc, LazyLock},
};

use eventsourcingdb::{Client, PreconditionOptions, error::ClientError};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    Counter, CounterEvent, Incremented, Keyed, cause_of,
    commands::{
        Increment, KitWrite, SOURCE, chain, counter_decider, emitting, event_types_in, total_in,
        write_past_the_kit,
    },
    database::{Database, subject_for},
    fake::{Api, Fake, Requests, dead_client},
};
use crate::{
    Categorized, Category, Decider, DomainError, Error, Event, EventOn, Events, ExecuteError,
    Precondition, State, Store, Transient, UpcastEvent, Upcasters, WriteError, check_preconditions,
    revision::tests::NOT_REVISIONS,
};

/// A store of a fake that answers every request with an empty result, and the
/// requests it receives, so that a test can tell that it was not asked.
async fn unasked() -> (Store, Requests) {
    let (url, requests) = Fake::new(Api::V1).start().await;

    (
        Store::builder(Client::new(url, "secret"), SOURCE).build(),
        requests,
    )
}

#[tokio::test]
async fn writes_events_and_returns_them() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-writes");

    let written = store
        .execute(&counter_decider(), Increment::new(&subject, 3))
        .await
        .expect("the command is executed");

    assert_eq!(written.len(), 1);
    assert!(
        !written[0].id().is_empty(),
        "a written event carries its ID"
    );
    assert_eq!(written[0].ty(), Incremented::EVENT_TYPE);
    assert_eq!(total_in(&store, &subject).await, 3);
}

#[tokio::test]
async fn appends_to_an_existing_subject() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-appends");

    for by in [1, 2, 4] {
        let _ = store
            .execute(&counter_decider(), Increment::new(&subject, by))
            .await
            .expect("the command is executed");
    }

    assert_eq!(total_in(&store, &subject).await, 7);
}

#[tokio::test]
async fn returns_the_error_of_the_decider_without_writing() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-refuses");

    let _ = store
        .execute(&counter_decider(), Increment::new(&subject, 5).up_to(10))
        .await
        .expect("the limit is not exceeded");

    let failure = store
        .execute(&counter_decider(), Increment::new(&subject, 8).up_to(10))
        .await
        .expect_err("the limit would be exceeded");

    let ExecuteError::Decider(refusal) = &failure else {
        panic!("the decider refuses the command, got {failure:?}");
    };
    assert_eq!(refusal.to_string(), "limit of 10 would be exceeded");
    assert_eq!(failure.category(), Some(Category::Domain));
    assert_eq!(
        total_in(&store, &subject).await,
        5,
        "a refused command writes nothing"
    );
}

#[tokio::test]
async fn writes_nothing_when_the_decider_decides_on_no_events() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-nothing");

    let written = store
        .execute(&counter_decider(), Increment::new(&subject, 0))
        .await
        .expect("no events are nothing to write");

    assert_eq!(written, Vec::new());
    assert_eq!(total_in(&store, &subject).await, 0);
}

#[tokio::test]
async fn appends_blindly_when_unconditional() {
    let database = Database::start().await;
    let store = Arc::new(database.store().await);
    let decider = Arc::new(counter_decider());
    let subject = subject_for("execute-blindly");

    // Concurrent commands that write unconditionally all succeed. That is what
    // Precondition::unconditionally is for, not an accident. Each of them runs
    // on a task of its own, as with a web server.
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let store = Arc::clone(&store);
            let decider = Arc::clone(&decider);
            let command = Increment::new(&subject, 1);

            tokio::spawn(async move { store.execute(&decider, command).await })
        })
        .collect();
    for task in tasks {
        let _ = task
            .await
            .expect("the task ends")
            .expect("the command is executed");
    }

    assert_eq!(total_in(&store, &subject).await, 4);
}

#[tokio::test]
async fn admits_only_one_with_a_pristine_precondition() {
    let database = Database::start().await;
    let store = Arc::new(database.store().await);
    let decider = Arc::new(counter_decider());
    let subject = subject_for("execute-pristine");

    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let store = Arc::clone(&store);
            let decider = Arc::clone(&decider);
            let command = Increment::new(&subject, 1).pristine();

            tokio::spawn(async move { store.execute(&decider, command).await })
        })
        .collect();

    let (mut succeeded, mut conflicted) = (0, 0);
    for task in tasks {
        match task.await.expect("the task ends") {
            Ok(_) => succeeded += 1,
            Err(ExecuteError::Transient(Transient::Conflict(_))) => conflicted += 1,
            Err(failure) => panic!("a command failed unexpectedly: {failure:?}"),
        }
    }

    assert_eq!(succeeded, 1, "exactly one command succeeds");
    assert_eq!(conflicted, 7, "the others conflict");
    assert_eq!(total_in(&store, &subject).await, 1);
}

#[tokio::test]
async fn detects_stale_reads_with_a_revision_precondition() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-stale");

    let written = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");
    let stale = written[0].id().to_owned();

    // Somebody else writes in between, so the view of the caller is outdated.
    let _ = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");

    let failure = store
        .execute(
            &counter_decider(),
            Increment::new(&subject, 1).requiring_event_id(&stale),
        )
        .await
        .expect_err("the revision is outdated");

    assert!(
        matches!(failure, ExecuteError::Transient(Transient::Conflict(_))),
        "{failure:?}"
    );
    assert_eq!(
        failure.category(),
        Some(Category::Transient),
        "a conflict is transient"
    );
}

#[tokio::test]
async fn reports_a_conflict_rather_than_trying_again() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-conflict");

    // A pristine precondition on a populated subject can never hold, so this
    // shows that the kit reports instead of trying again.
    let _ = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect("the command is executed");

    let failure = store
        .execute(&counter_decider(), Increment::new(&subject, 1).pristine())
        .await
        .expect_err("the subject is not pristine");

    assert!(
        matches!(failure, ExecuteError::Transient(Transient::Conflict(_))),
        "{failure:?}"
    );
    assert_eq!(
        total_in(&store, &subject).await,
        1,
        "nothing more was written"
    );
}

#[tokio::test]
async fn fails_on_an_event_type_without_a_rule() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-no-rule");

    write_past_the_kit(
        &database,
        &subject,
        "io.thenativeweb.test.unexpected",
        json!({ "x": 1 }),
    )
    .await;

    let failure = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect_err("the state has no rule for the event");

    assert!(
        matches!(failure, ExecuteError::Permanent(_)),
        "a missing rule is permanent: {failure:?}"
    );
    assert!(
        failure
            .to_string()
            .contains("io.thenativeweb.test.unexpected"),
        "the failure names the event type: {failure}"
    );
}

#[tokio::test]
async fn writes_an_event_its_state_ignores() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-ignored");

    let written = store
        .execute(
            &emitting(|| {
                vec![
                    CounterEvent::Incremented(Incremented { by: 3 }),
                    CounterEvent::Annotated(super::Annotated {
                        note: "a remark".to_owned(),
                    }),
                ]
            }),
            Increment::new(&subject, 1),
        )
        .await
        .expect("the command is executed");

    assert_eq!(written.len(), 2);
    assert_eq!(
        event_types_in(&database, &subject).await,
        [Incremented::EVENT_TYPE, super::Annotated::EVENT_TYPE]
    );
}

#[tokio::test]
async fn reports_an_unreachable_database() {
    let store = Store::builder(dead_client().await, SOURCE).build();

    let failure = store
        .execute(&counter_decider(), Increment::new("/test/unreachable", 1))
        .await
        .expect_err("the database can not be reached");

    assert!(
        matches!(failure, ExecuteError::Transient(Transient::Other(_))),
        "an unreachable database is transient: {failure:?}"
    );
    assert!(failure.to_string().contains("reading"), "{failure}");
}

#[tokio::test]
async fn reports_technical_write_failures() {
    let database = Database::start().await;
    // The source has to be a valid URI, or the database refuses the write.
    // Reading still works, so this reaches the write.
    let store = Store::builder(database.client().await, "not a valid uri").build();

    let failure = store
        .execute(
            &counter_decider(),
            Increment::new(&subject_for("execute-technical"), 1),
        )
        .await
        .expect_err("the source is invalid");

    assert!(
        matches!(failure, ExecuteError::Permanent(_)),
        "an invalid source is permanent, and no conflict: {failure:?}"
    );
}

#[tokio::test]
async fn fails_when_stored_data_does_not_match_the_rule() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-mismatch");

    // The annotated type has no schema, so the database lets a field of
    // another type through, and the state trips over it when reading.
    write_past_the_kit(
        &database,
        &subject,
        super::Annotated::EVENT_TYPE,
        json!({ "note": 42 }),
    )
    .await;

    let failure = store
        .execute(&counter_decider(), Increment::new(&subject, 1))
        .await
        .expect_err("the data does not match");

    assert!(matches!(failure, ExecuteError::Permanent(_)), "{failure:?}");
    assert!(failure.to_string().contains("decoding"), "{failure}");
}

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.labelled")]
struct Labelled {
    label: String,
}

#[derive(Debug, Events)]
enum LabelEvent {
    Labelled(Labelled),
}

/// A state that only labels are written for.
#[derive(Clone, Debug, Default)]
struct Labels;

impl State for Labels {
    type Event = LabelEvent;

    fn evolve(self, _: &LabelEvent) -> Self {
        self
    }
}

#[tokio::test]
async fn reports_a_schema_violation_as_permanent() {
    let database = Database::start().await;
    let store = database.store().await;

    // The schema asks for a label that is not empty, which the type of the
    // event can not express, so it is registered past the kit.
    let _ = database
        .client()
        .await
        .register_event_schema(
            Labelled::EVENT_TYPE,
            &json!({
                "type": "object",
                "properties": { "label": { "type": "string", "minLength": 1 } },
                "required": ["label"],
                "additionalProperties": false,
            }),
        )
        .await
        .expect("the schema is registered");
    let labelling = Decider::<_, _, DomainError>::new(|_: Increment, _: Labels| {
        future::ready(Ok(vec![LabelEvent::Labelled(Labelled {
            label: String::new(),
        })]))
    });

    let failure = store
        .execute(
            &labelling,
            Increment::new(&subject_for("execute-schema"), 1),
        )
        .await
        .expect_err("the label is empty");

    assert!(
        matches!(failure, ExecuteError::Permanent(_)),
        "a schema violation is permanent, and no conflict: {failure:?}"
    );
    let Some(ClientError::DbApiError(answer)) = cause_of(&failure) else {
        panic!("the cause has to be the answer of the database, got {failure:?}");
    };
    assert_eq!(
        answer.code,
        eventsourcingdb::error::code::EVENT_SCHEMA_VIOLATED
    );
    assert!(
        chain(&failure).contains(&answer.description),
        "the failure carries the description of the database: {failure}"
    );
}

static FAILING_UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
    Upcasters::new().upcast("io.thenativeweb.test.outdated", |_| {
        Err::<Vec<UpcastEvent>, _>("this one cannot be migrated")
    })
});

/// A counter whose upcaster of an outdated event refuses it.
#[derive(Clone, Debug, Default)]
struct Migrating(Counter);

impl State for Migrating {
    type Event = CounterEvent;

    fn evolve(self, event: &CounterEvent) -> Self {
        Migrating(self.0.evolve(event))
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&FAILING_UPCASTERS)
    }
}

#[tokio::test]
async fn reports_a_failing_upcaster() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-upcaster");

    write_past_the_kit(
        &database,
        &subject,
        "io.thenativeweb.test.outdated",
        json!({ "whatever": true }),
    )
    .await;
    let decider = Decider::<_, _, DomainError>::new(|_: Increment, _: Migrating| {
        future::ready(Ok(vec![CounterEvent::Incremented(Incremented { by: 1 })]))
    });

    let failure = store
        .execute(&decider, Increment::new(&subject, 1))
        .await
        .expect_err("the upcaster refuses");

    assert!(
        matches!(failure, ExecuteError::Permanent(_)),
        "a failing upcaster is permanent: {failure:?}"
    );
    assert!(chain(&failure).contains("cannot be migrated"), "{failure}");
}

// Event IDs of the caller.

#[tokio::test]
async fn refuses_an_event_id_that_is_not_a_revision_before_reading_or_writing() {
    for (name, id) in [("an empty one", "")].into_iter().chain(NOT_REVISIONS) {
        let (store, requests) = unasked().await;
        let on_event_id = move || vec![Precondition::on_event_id("/books/42", id)];

        let executed = store
            .execute(
                &counter_decider(),
                Increment::new("/books/42", 1).declaring(on_event_id),
            )
            .await
            .expect_err(name);
        let written = store
            .write(
                vec![EventOn::new("/books/42", &Incremented { by: 1 })],
                on_event_id(),
            )
            .await
            .expect_err(name);

        assert!(
            matches!(executed, ExecuteError::NotARevision(_)),
            "{name}: {executed:?}"
        );
        assert!(
            matches!(written, WriteError::NotARevision(_)),
            "{name}: {written:?}"
        );
        assert_eq!(
            executed.category(),
            None,
            "{name}: the ID is the mistake of the caller"
        );
        assert_eq!(
            executed.to_string(),
            format!("not a revision: {id:?}"),
            "{name}: the text is for the caller, who knows neither the subject nor the command",
        );
        assert_eq!(written.to_string(), executed.to_string(), "{name}");
        assert_eq!(requests.count(), 0, "{name}: the database is not asked");
    }
}

#[tokio::test]
async fn refuses_it_with_the_same_error_as_check_preconditions_and_write() {
    let (store, _) = unasked().await;
    let on_event_id = || vec![Precondition::on_event_id("/books/42", "abc")];
    let command = Increment::new("/books/42", 1).declaring(on_event_id);

    let executed = store
        .execute(&counter_decider(), command.clone())
        .await
        .expect_err("abc is not a revision");
    let checked = check_preconditions(&command).expect_err("abc is not a revision");
    let written = store
        .write(Vec::new(), on_event_id())
        .await
        .expect_err("write checks the preconditions also for no events");

    assert!(
        matches!(executed, ExecuteError::NotARevision(_)),
        "{executed:?}"
    );
    assert_eq!(executed.to_string(), checked.to_string());
    assert_eq!(executed.to_string(), written.to_string());
}

#[tokio::test]
async fn refuses_the_first_id_that_is_not_a_revision() {
    let (store, requests) = unasked().await;
    let preconditions = || {
        vec![
            Precondition::on_event_id("/books/42", "7"),
            Precondition::on_populated_subject("/books/42"),
            Precondition::on_event_id("/books/23", "abc"),
            Precondition::on_event_id("/books/42", "xyz"),
        ]
    };

    let executed = store
        .execute(
            &counter_decider(),
            Increment::new("/books/42", 1).declaring(preconditions),
        )
        .await
        .expect_err("abc is not a revision");
    let written = store
        .write(
            vec![EventOn::new("/books/42", &Incremented { by: 1 })],
            preconditions(),
        )
        .await
        .expect_err("abc is not a revision");

    assert_eq!(executed.to_string(), r#"not a revision: "abc""#);
    assert_eq!(written.to_string(), r#"not a revision: "abc""#);
    assert_eq!(requests.count(), 0, "the database is not asked");
}

#[tokio::test]
async fn reports_a_mistake_in_the_code_before_an_id_that_is_not_a_revision() {
    let (store, requests) = unasked().await;
    let combined = || {
        vec![
            Precondition::on_event_id("/books/42", "abc"),
            Precondition::unconditionally(),
        ]
    };

    let executed = store
        .execute(
            &counter_decider(),
            Increment::new("/books/42", 1).declaring(combined),
        )
        .await
        .expect_err("unconditionally is combined with another precondition");
    let failures: [(&str, Error<DomainError>, &str); 3] = [
        (
            "execute, with unconditionally combined with it",
            executed.into(),
            "combines Precondition::unconditionally with other preconditions",
        ),
        (
            "write, with unconditionally combined with it",
            store
                .write(
                    vec![EventOn::new("/books/42", &Incremented { by: 1 })],
                    combined(),
                )
                .await
                .expect_err("unconditionally is combined with another precondition")
                .into(),
            "combines Precondition::unconditionally with other preconditions",
        ),
        (
            "write, with on_state_read after it",
            store
                .write(
                    vec![EventOn::new("/books/42", &Incremented { by: 1 })],
                    vec![
                        Precondition::on_event_id("/books/42", "abc"),
                        Precondition::on_state_read(),
                    ],
                )
                .await
                .expect_err("a write reads no state")
                .into(),
            "on_state_read has nothing to guard",
        ),
    ];

    for (name, failure, says) in failures {
        assert!(
            matches!(failure, Error::Permanent(_)),
            "{name}: {failure:?}"
        );
        assert!(failure.to_string().contains(says), "{name}: {failure}");
    }
    assert_eq!(requests.count(), 0, "the database is not asked");
}

#[tokio::test]
async fn lets_the_database_check_an_id_that_is_a_revision() {
    let database = Database::start().await;
    let store = database.store().await;
    let subject = subject_for("execute-revision");
    let on = |event_id: &str| {
        let subject = subject.clone();
        let event_id = event_id.to_owned();
        move || vec![Precondition::on_event_id(subject.clone(), event_id.clone())]
    };

    let seeded = store
        .write(
            vec![
                EventOn::new(&subject, &Incremented { by: 1 }),
                EventOn::new(&subject, &Incremented { by: 2 }),
            ],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the events are written");

    for (name, event_id) in [
        ("a revision that is not the last event", seeded[0].id()),
        ("the largest revision", "9223372036854775807"),
    ] {
        let failure = store
            .execute(
                &counter_decider(),
                Increment::new(&subject, 1).declaring(on(event_id)),
            )
            .await
            .expect_err(name);
        assert!(
            matches!(failure, ExecuteError::Transient(Transient::Conflict(_))),
            "{name}: {failure:?}"
        );
    }

    let failure = store
        .write(
            vec![EventOn::new(&subject, &Incremented { by: 1 })],
            on(seeded[0].id())(),
        )
        .await
        .expect_err("a revision that is not the last event conflicts");
    assert!(
        matches!(failure, WriteError::Transient(Transient::Conflict(_))),
        "{failure:?}"
    );

    let executed = store
        .execute(
            &counter_decider(),
            Increment::new(&subject, 4).declaring(on(seeded[1].id())),
        )
        .await
        .expect("the last event is the one to be on");
    let _ = store
        .write(
            vec![EventOn::new(&subject, &Incremented { by: 8 })],
            on(executed[0].id())(),
        )
        .await
        .expect("the last event is the one to be on");

    assert_eq!(total_in(&store, &subject).await, 15);
}

#[tokio::test]
async fn leaves_require_with_the_revision_check_of_the_client_sdk_unchecked() {
    let database = Database::start().await;
    let store = database.store().await;
    let command = Increment::new(&subject_for("execute-required"), 1).requiring_event_id("abc");

    let failure = store
        .execute(&counter_decider(), command.clone())
        .await
        .expect_err("the database refuses the ID as a malformed request");

    assert!(
        matches!(failure, ExecuteError::Permanent(_)),
        "the kit has not looked at the ID: {failure:?}"
    );
    check_preconditions(&command).expect("the kit leaves the ID to the database");
}

// Encoding events.

/// An event whose data refuses to be encoded.
#[derive(Debug, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.failing-to-encode")]
struct FailingToEncode {}

impl Serialize for FailingToEncode {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("refusing to be encoded"))
    }
}

#[derive(Events)]
enum EncodingEvent {
    Incremented(Incremented),
    Keyed(Keyed),
    FailingToEncode(FailingToEncode),
}

/// Counts increments, and ignores all other events of these tests, so that
/// executing gets as far as encoding them.
#[derive(Clone, Debug, Default)]
struct Encoding(Counter);

impl State for Encoding {
    type Event = EncodingEvent;

    fn evolve(self, event: &EncodingEvent) -> Self {
        match event {
            EncodingEvent::Incremented(incremented) => Encoding(Counter {
                total: self.0.total + incremented.by,
            }),
            EncodingEvent::Keyed(_) | EncodingEvent::FailingToEncode(_) => self,
        }
    }
}

type Unencodable = (
    &'static str,
    &'static str,
    fn() -> EncodingEvent,
    &'static str,
);

/// Events whose data `serde_json` can not encode, with their types, and with
/// what `serde_json` says about them.
const UNENCODABLE: [Unencodable; 2] = [
    (
        "a map whose keys are no strings",
        Keyed::EVENT_TYPE,
        || {
            EncodingEvent::Keyed(Keyed {
                by_position: Some(HashMap::from([((1, 2), "x".to_owned())])),
            })
        },
        "key must be a string",
    ),
    (
        "data that refuses to be encoded",
        FailingToEncode::EVENT_TYPE,
        || EncodingEvent::FailingToEncode(FailingToEncode {}),
        "refusing to be encoded",
    ),
];

fn encoding(
    events: impl Fn() -> Vec<EncodingEvent> + Send + Sync + 'static,
) -> Decider<Increment, Encoding, DomainError> {
    Decider::new(move |_: Increment, _: Encoding| future::ready(Ok(events())))
}

#[tokio::test]
async fn refuses_data_that_can_not_be_encoded_as_permanent_and_writes_nothing() {
    let database = Database::start().await;
    let store = database.store().await;

    for (name, event_type, event, says) in UNENCODABLE {
        let subject = subject_for("execute-unencodable");

        let failure = store
            .execute(
                &encoding(move || vec![event()]),
                Increment::new(&subject, 1),
            )
            .await
            .expect_err(name);

        assert!(
            matches!(failure, ExecuteError::Permanent(_)),
            "{name}: trying again would fail the same way: {failure:?}"
        );
        assert_eq!(
            failure.to_string(),
            format!(
                "permanent failure: refusing to write an event of type {event_type:?} to {subject:?}, \
                 since its data can not be encoded as JSON"
            ),
            "{name}: the failure names the event type and the subject",
        );
        assert!(chain(&failure).contains(says), "{name}: {failure}");
        assert_eq!(
            event_types_in(&database, &subject).await,
            Vec::<String>::new(),
            "{name}: nothing was written"
        );
    }
}

#[tokio::test]
async fn writes_none_of_the_events_if_the_second_can_not_be_encoded() {
    let database = Database::start().await;
    let store = database.store().await;

    for (name, event_type, event, _) in UNENCODABLE {
        let subject = subject_for("execute-second-unencodable");

        let failure = store
            .execute(
                &encoding(move || vec![EncodingEvent::Incremented(Incremented { by: 1 }), event()]),
                Increment::new(&subject, 1),
            )
            .await
            .expect_err(name);

        assert!(
            matches!(failure, ExecuteError::Permanent(_)),
            "{name}: {failure:?}"
        );
        assert!(
            failure
                .to_string()
                .contains(&format!("{event_type:?} to {subject:?}")),
            "{name}: {failure}"
        );
        assert_eq!(
            event_types_in(&database, &subject).await,
            Vec::<String>::new(),
            "{name}: not even the first event was written"
        );
    }
}

// Writes that the database does not support.

/// Lets the events be written only if neither the subject nor any subject
/// below it has events yet, which only a database that offers the v2 API can
/// check.
fn recursively_pristine(subject: &str) -> impl Fn() -> Vec<Precondition> + Send + Sync + 'static {
    let subject = subject.to_owned();
    move || {
        vec![Precondition::require(
            eventsourcingdb::Precondition::is_subject_pristine(
                subject.clone(),
                Some(PreconditionOptions { recursive: true }),
            ),
        )]
    }
}

/// Asserts that a write failed since the database does not support what it
/// needs: permanently, since only upgrading the database helps, and not with
/// an unknown outcome, since the client never sent it.
fn assert_not_supported(failure: &Error<DomainError>, name: &str) {
    assert!(
        matches!(failure, Error::Permanent(_)),
        "{name}: only upgrading the database helps: {failure:?}"
    );
    assert!(
        matches!(cause_of(failure), Some(ClientError::IncompatibleServer)),
        "{name}: the source is the error of the client: {failure:?}"
    );
    assert!(
        failure.to_string().contains(
            "the database does not support what the request needs, and has to be upgraded"
        ),
        "{name}: {failure}"
    );
}

#[tokio::test]
async fn fails_permanently_without_sending_a_write_the_database_does_not_support() {
    for write in KitWrite::ALL {
        let (url, requests) = Fake::new(Api::V1).start().await;
        let store = Store::builder(Client::new(url, "secret"), SOURCE).build();

        let failure = write
            .remark(&store, "/test", "a remark", recursively_pristine("/test"))
            .await
            .expect_err("the database offers just the v1 API");

        assert_not_supported(&failure, &format!("{write:?}"));
        assert_eq!(
            requests.to("/api/v1/write-events"),
            0,
            "{write:?}: the client does not send the write"
        );
    }
}

/// Tells whether the shared database offers just the v1 API, by how it
/// refuses an event for a malformed subject: one that offers the v2 API names
/// every error with a code, while one that offers just the v1 API reports this
/// one without a code. The event is written past the store, so that nothing of
/// the kit decides.
async fn offers_just_the_v1_api(database: &Database) -> bool {
    let refusal = database
        .client()
        .await
        .write_events(
            vec![super::commands::candidate(
                "malformed",
                Incremented::EVENT_TYPE,
                json!({ "by": 1 }),
            )],
            Vec::new(),
        )
        .await
        .expect_err("the database refuses a malformed subject");

    let ClientError::DbApiError(answer) = refusal else {
        panic!("the database has to refuse a malformed subject, got {refusal:?}");
    };
    answer.code.is_empty()
}

#[tokio::test]
async fn writes_with_a_recursive_precondition_if_the_database_supports_it() {
    let database = Database::start().await;
    let store = database.store().await;
    let offers_just_the_v1_api = offers_just_the_v1_api(&database).await;

    for write in KitWrite::ALL {
        let subject = subject_for("execute-recursive");

        let written = write
            .remark(&store, &subject, "a remark", recursively_pristine(&subject))
            .await;

        if offers_just_the_v1_api {
            assert_not_supported(
                &written.expect_err("the database offers just the v1 API"),
                &format!("{write:?}"),
            );
            assert_eq!(
                event_types_in(&database, &subject).await,
                Vec::<String>::new(),
                "{write:?}: nothing was written"
            );
        } else {
            let _ = written.expect("the database offers the v2 API");
            assert_eq!(
                event_types_in(&database, &subject).await.len(),
                1,
                "{write:?}"
            );
        }
    }
}
