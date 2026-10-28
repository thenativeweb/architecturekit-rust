// These tests check preconditions, the preconditions of subjects, and
// check_preconditions. The tests that need execute, and the resolving of
// preconditions, which only execute uses, come with the store. A Precondition
// can only be made by its functions, so there is neither a zero value nor a
// requirement of nothing; the other mistakes are checked here directly, as
// execute checks them before it reads.
//
// The preconditions of the client SDK can not be compared with ==, so the
// tests compare them by the JSON the client sends for them.

use eventsourcingdb::PreconditionOptions;
use serde_json::Value;

use crate::{
    Categorized, Category, Command, Precondition, PreconditionsError, check_preconditions,
};

/// Returns the JSON the client sends for the precondition the database checks
/// for a precondition, or `None` if there is none.
fn database_json(precondition: &Precondition) -> Option<Value> {
    precondition.database().map(|database| {
        serde_json::to_value(database).expect("a precondition of the client SDK is JSON")
    })
}

fn json_of(database: &eventsourcingdb::Precondition) -> Value {
    serde_json::to_value(database).expect("a precondition of the client SDK is JSON")
}

#[derive(Clone)]
struct Increment {
    preconditions: fn() -> Vec<Precondition>,
}

impl Command for Increment {
    fn subject(&self) -> String {
        "/test/valid".to_owned()
    }

    fn preconditions(&self) -> Vec<Precondition> {
        (self.preconditions)()
    }
}

#[test]
fn tells_how_it_was_made() {
    let pristine = eventsourcingdb::Precondition::is_subject_pristine("/books/42", None);
    let expected = json_of(&pristine);

    let required = Precondition::require(pristine);
    assert_eq!(database_json(&required), Some(expected));
    assert!(
        !required.is_on_state_read(),
        "a requirement is neither on the state read nor unconditional"
    );
    assert!(
        !required.is_unconditional(),
        "a requirement is neither on the state read nor unconditional"
    );

    assert!(
        Precondition::on_state_read().database().is_none(),
        "on_state_read must only be on the state read"
    );
    assert!(
        Precondition::on_state_read().is_on_state_read(),
        "on_state_read must only be on the state read"
    );
    assert!(
        !Precondition::on_state_read().is_unconditional(),
        "on_state_read must only be on the state read"
    );

    assert!(
        Precondition::unconditionally().database().is_none(),
        "unconditionally must only be unconditional"
    );
    assert!(
        Precondition::unconditionally().is_unconditional(),
        "unconditionally must only be unconditional"
    );
    assert!(
        !Precondition::unconditionally().is_on_state_read(),
        "unconditionally must only be unconditional"
    );
}

#[test]
fn are_the_preconditions_of_the_client_sdk_that_require_takes() {
    for (name, made, database) in [
        (
            "on_pristine_subject",
            Precondition::on_pristine_subject("/books/42"),
            eventsourcingdb::Precondition::is_subject_pristine("/books/42", None),
        ),
        (
            "on_populated_subject",
            Precondition::on_populated_subject("/books/42"),
            eventsourcingdb::Precondition::is_subject_populated("/books/42", None),
        ),
    ] {
        let expected = json_of(&database);

        assert_eq!(
            database_json(&Precondition::require(database)),
            Some(expected.clone()),
            "{name}"
        );
        assert_eq!(database_json(&made), Some(expected), "{name}");
        assert!(!made.is_on_state_read(), "{name}");
        assert!(!made.is_unconditional(), "{name}");
    }
}

#[test]
fn turns_on_event_id_into_the_revision_check_of_the_database() {
    let made = Precondition::on_event_id("/books/23", "5");

    assert_eq!(
        database_json(&made),
        Some(json_of(
            &eventsourcingdb::Precondition::is_subject_on_event_id("/books/23", "5", None,)
        )),
    );
    assert!(!made.is_on_state_read());
    assert!(!made.is_unconditional());
}

#[test]
fn guard_the_subject_they_name() {
    for (left, right) in [
        (
            Precondition::on_pristine_subject("/books/23"),
            Precondition::on_pristine_subject("/books/42"),
        ),
        (
            Precondition::on_populated_subject("/books/23"),
            Precondition::on_populated_subject("/books/42"),
        ),
        (
            Precondition::on_event_id("/books/23", "7"),
            Precondition::on_event_id("/books/42", "7"),
        ),
        (
            Precondition::on_event_id("/books/42", "6"),
            Precondition::on_event_id("/books/42", "7"),
        ),
    ] {
        assert_ne!(
            database_json(&left),
            database_json(&right),
            "{left:?} and {right:?}"
        );
    }
}

#[test]
fn accepts_what_execute_accepts() {
    let accepted: [fn() -> Vec<Precondition>; 5] = [
        || vec![Precondition::unconditionally()],
        || vec![Precondition::on_state_read()],
        || vec![Precondition::on_pristine_subject("/test/valid")],
        || vec![Precondition::on_event_id("/test/valid", "0")],
        || {
            vec![
                Precondition::require(eventsourcingdb::Precondition::is_subject_populated(
                    "/test/valid",
                    None,
                )),
                Precondition::on_state_read(),
            ]
        },
    ];

    for preconditions in accepted {
        assert!(check_preconditions(&Increment { preconditions }).is_ok());
    }
}

#[test]
fn refuses_a_command_without_preconditions_or_with_unconditionally_among_others() {
    type Case = (&'static str, fn() -> Vec<Precondition>, &'static str);

    let refused: [Case; 2] = [
        (
            "none",
            Vec::new,
            "declares no preconditions, use Precondition::unconditionally to write without any",
        ),
        (
            "unconditionally combined with another one",
            || {
                vec![
                    Precondition::unconditionally(),
                    Precondition::on_state_read(),
                ]
            },
            "combines Precondition::unconditionally with other preconditions",
        ),
    ];

    for (name, preconditions, message) in refused {
        let refusal = check_preconditions(&Increment { preconditions }).expect_err(name);

        assert!(
            matches!(refusal, PreconditionsError::Permanent(_)),
            "{name}: {refusal:?}"
        );
        assert_eq!(refusal.category(), Some(Category::Permanent), "{name}");
        assert_eq!(
            refusal.to_string(),
            format!("permanent failure: architecturekit::command::tests::Increment {message}"),
            "{name}",
        );
    }
}

#[test]
fn refuses_an_event_id_of_the_caller_that_is_not_a_revision() {
    let refusal = check_preconditions(&Increment {
        preconditions: || vec![Precondition::on_event_id("/test/valid", "")],
    })
    .expect_err("the empty string is not a revision");

    assert!(
        matches!(refusal, PreconditionsError::NotARevision(_)),
        "{refusal:?}"
    );
    assert_eq!(refusal.category(), None);
    assert_eq!(refusal.to_string(), r#"not a revision: """#);
}

#[test]
fn names_a_mistake_in_the_code_before_one_of_the_caller() {
    let refusal = check_preconditions(&Increment {
        preconditions: || {
            vec![
                Precondition::on_event_id("/test/valid", "not a revision"),
                Precondition::unconditionally(),
            ]
        },
    })
    .expect_err("unconditionally is combined with another one");

    assert!(
        matches!(refusal, PreconditionsError::Permanent(_)),
        "{refusal:?}"
    );
}

// Comparing preconditions, which the test fixture relies on to compare the
// preconditions that a command declares.

fn pristine(subject: &str, options: Option<PreconditionOptions>) -> Precondition {
    Precondition::require(eventsourcingdb::Precondition::is_subject_pristine(
        subject, options,
    ))
}

fn populated(subject: &str, options: Option<PreconditionOptions>) -> Precondition {
    Precondition::require(eventsourcingdb::Precondition::is_subject_populated(
        subject, options,
    ))
}

fn on_revision(subject: &str, options: Option<PreconditionOptions>) -> Precondition {
    Precondition::require(eventsourcingdb::Precondition::is_subject_on_event_id(
        subject, "9", options,
    ))
}

fn recursive() -> PreconditionOptions {
    PreconditionOptions { recursive: true }
}

fn query(query: &str) -> Precondition {
    Precondition::require(eventsourcingdb::Precondition::is_eventql_query_true(query))
}

#[test]
fn equals_a_precondition_that_checks_the_same() {
    assert_eq!(
        Precondition::on_pristine_subject("/account/1"),
        pristine("/account/1", None)
    );
    assert_eq!(
        Precondition::on_populated_subject("/account/2"),
        populated("/account/2", None)
    );
    assert_eq!(
        Precondition::on_event_id("/account/3", "9"),
        Precondition::on_event_id("/account/3", "9")
    );
    assert_eq!(
        query("FROM e IN events PROJECT INTO true"),
        query("FROM e IN events PROJECT INTO true")
    );
    assert_eq!(Precondition::on_state_read(), Precondition::on_state_read());
    assert_eq!(
        Precondition::unconditionally(),
        Precondition::unconditionally()
    );
}

#[test]
fn differs_from_a_precondition_on_another_subject_revision_or_query() {
    assert_ne!(
        Precondition::on_pristine_subject("/account/1"),
        Precondition::on_pristine_subject("/account/other")
    );
    assert_ne!(
        Precondition::on_event_id("/account/3", "9"),
        Precondition::on_event_id("/account/3", "10")
    );
    assert_ne!(
        query("FROM e IN events PROJECT INTO true"),
        query("FROM e IN events PROJECT INTO false")
    );
}

#[test]
fn tells_a_pristine_from_a_populated_subject() {
    assert_ne!(pristine("/account/1", None), populated("/account/1", None));
}

#[test]
fn tells_a_recursive_subject_check_from_one_of_the_subject_alone() {
    assert_ne!(
        pristine("/account/1", None),
        pristine("/account/1", Some(recursive()))
    );
    assert_ne!(
        populated("/account/2", None),
        populated("/account/2", Some(recursive()))
    );
    assert_ne!(
        on_revision("/account/3", None),
        on_revision("/account/3", Some(recursive()))
    );
}

#[test]
fn takes_options_that_leave_a_subject_check_to_the_subject_alone_as_none() {
    // They mean the same to the database, but the client keeps them, so the
    // JSON differs from one built without them.
    let alone = || Some(PreconditionOptions { recursive: false });

    assert_eq!(
        pristine("/account/1", alone()),
        pristine("/account/1", None)
    );
    assert_eq!(
        populated("/account/2", alone()),
        populated("/account/2", None)
    );
    assert_eq!(
        on_revision("/account/3", alone()),
        on_revision("/account/3", None)
    );
}

#[test]
fn tells_on_event_id_from_the_revision_check_it_stands_for() {
    // Require leaves the ID unchecked, so the two behave differently.
    assert_ne!(
        Precondition::on_event_id("/account/3", "9"),
        on_revision("/account/3", None)
    );
}

#[test]
fn tells_the_kinds_of_the_kit_apart() {
    let kinds = [
        Precondition::on_state_read(),
        Precondition::unconditionally(),
        Precondition::on_pristine_subject("/account/1"),
        Precondition::on_event_id("/account/1", "9"),
    ];

    for (left_index, left) in kinds.iter().enumerate() {
        for (right_index, right) in kinds.iter().enumerate() {
            assert_eq!(
                left == right,
                left_index == right_index,
                "{left:?} and {right:?}"
            );
        }
    }
}
