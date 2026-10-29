// These tests check the test fixture for deciders: the given events, stored
// or not, the history, every assertion that follows when, matching events, an
// event that can not be encoded, and the check of preconditions in when.
// Comparing preconditions is part of the tests of command.
//
// A decider decides on variants of the events of its state, which has a rule
// for each of them, so an event without a rule can not be written. An
// assertion fails by panicking, which the tests catch to read its message.

#![expect(
    unused_results,
    reason = "the assertions return the decision to be chained, which a test leaves unused"
)]

use std::{
    collections::HashMap,
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    Categorized, Category, Command, Decider, Event, Events, ExecuteError, Precondition, Revision,
    State, UpcastEvent, Upcasters, check_preconditions,
    testing::{given, given_stored},
};

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "test.account.opened")]
struct Opened {
    owner: String,
}

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "test.account.closed")]
struct Closed {}

/// An event whose data refuses to be encoded. The account ignores it.
#[derive(Debug, Deserialize, Event)]
#[event(type = "test.account.unencodable")]
struct Unencodable {}

impl Serialize for Unencodable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("refusing to be encoded"))
    }
}

/// An event whose data can be encoded only while it holds no positions, since
/// `serde_json` writes the keys of a map as strings, which a tuple is not. The
/// account ignores it.
#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "test.account.positioned")]
struct Positioned {
    #[serde(skip_serializing_if = "Option::is_none")]
    by_position: Option<HashMap<(i32, i32), String>>,
}

#[derive(Debug, Events)]
enum AccountEvent {
    Opened(Opened),
    Closed(Closed),
    Unencodable(Unencodable),
    Positioned(Positioned),
}

fn positioned(by_position: Option<HashMap<(i32, i32), String>>) -> AccountEvent {
    AccountEvent::Positioned(Positioned { by_position })
}

fn opened(owner: &str) -> AccountEvent {
    AccountEvent::Opened(Opened {
        owner: owner.to_owned(),
    })
}

static ACCOUNT_UPCASTERS: std::sync::LazyLock<Upcasters> = std::sync::LazyLock::new(|| {
    // An older type, reachable only through its upcaster.
    Upcasters::new().upcast("test.account.opened.v1", |event: UpcastEvent| {
        Ok::<_, serde_json::Error>(vec![
            event
                .with_type("test.account.opened")
                .with_data(json!({ "owner": "from the old shape" })),
        ])
    })
});

#[derive(Clone, Debug, Default, PartialEq)]
struct Account {
    is_open: bool,
    owner: String,
}

impl State for Account {
    type Event = AccountEvent;

    fn evolve(self, event: &AccountEvent) -> Self {
        match event {
            AccountEvent::Opened(opened) => Account {
                is_open: true,
                owner: opened.owner.clone(),
            },
            AccountEvent::Closed(_) => Account {
                is_open: false,
                ..self
            },
            AccountEvent::Unencodable(_) | AccountEvent::Positioned(_) => self,
        }
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&ACCOUNT_UPCASTERS)
    }
}

/// Opens the account for an owner, under the preconditions the function
/// makes, which is `on_state_read` unless a test needs others.
#[derive(Clone)]
struct Open {
    owner: &'static str,
    declaring: Declaring,
}

fn open(owner: &'static str) -> Open {
    Open {
        owner,
        declaring: || vec![Precondition::on_state_read()],
    }
}

impl Open {
    fn declaring(mut self, preconditions: Declaring) -> Self {
        self.declaring = preconditions;
        self
    }
}

impl Command for Open {
    fn subject(&self) -> String {
        "/account/1".to_owned()
    }

    fn preconditions(&self) -> Vec<Precondition> {
        (self.declaring)()
    }
}

/// The refusals of the account.
#[derive(Debug, PartialEq)]
enum AccountError {
    AlreadyOpen { owner: String },
    OwnerBanned(String),
    NoOwner,
}

impl Categorized for AccountError {
    fn category(&self) -> Option<Category> {
        match self {
            Self::NoOwner => Some(Category::Permanent),
            Self::AlreadyOpen { .. } | Self::OwnerBanned(_) => Some(Category::Domain),
        }
    }
}

fn decider() -> Decider<Open, Account, AccountError> {
    Decider::new(|command: Open, current: Account| async move {
        if current.is_open {
            return Err(AccountError::AlreadyOpen {
                owner: current.owner,
            });
        }

        match command.owner {
            "mallory" => Err(AccountError::OwnerBanned(command.owner.to_owned())),
            "" => Err(AccountError::NoOwner),
            // Nothing to do, and nothing wrong either.
            "nobody" => Ok(Vec::new()),
            owner => Ok(vec![opened(owner)]),
        }
    })
}

/// Emits exactly the events the function makes, whatever the command, which
/// is how the tests reach the encoding.
fn emitting(events: fn() -> Vec<AccountEvent>) -> Decider<Open, Account, AccountError> {
    Decider::new(move |_: Open, _: Account| std::future::ready(Ok(events())))
}

/// The preconditions a command declares, made anew for every command.
type Declaring = fn() -> Vec<Precondition>;

/// An assertion on a decision, under the name a failure reports it by.
type NamedAssertion<'decision> = (&'static str, Box<dyn Fn() + 'decision>);

/// Runs an assertion and returns the message it failed with.
fn failure_of(assertion: impl FnOnce()) -> String {
    let Err(payload) = panic::catch_unwind(AssertUnwindSafe(assertion)) else {
        panic!("the assertion has to fail");
    };

    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .expect("an assertion fails with a message")
}

fn assert_failure(assertion: impl FnOnce(), containing: &str) {
    let message = failure_of(assertion);
    assert!(
        message.contains(containing),
        "expected a failure containing {containing:?}, got {message:?}"
    );
}

// Given and when.

#[tokio::test]
async fn decides_on_the_state_given() {
    given(&decider(), [])
        .when(open("golo"))
        .await
        .then_events([opened("golo")]);
}

#[tokio::test]
async fn folds_the_history_in_order() {
    given(
        &decider(),
        [opened("golo"), AccountEvent::Closed(Closed {})],
    )
    .when(open("jane"))
    .await
    .then_events([opened("jane")]);
}

#[tokio::test]
async fn fails_on_a_history_that_can_not_be_encoded() {
    // Replaying a history encodes every event, as reading it from the database
    // would decode it.
    let message = failure_of(|| {
        let _ = given(&decider(), [AccountEvent::Unencodable(Unencodable {})]);
    });

    assert!(message.starts_with("given: "), "{message}");
    assert!(message.contains("refusing to be encoded"), "{message}");
}

#[tokio::test]
async fn runs_the_upcasters_of_a_stored_history() {
    // The old type has no Rust type any more, so only the stored shape can
    // carry it. This is exactly what typed events can not express.
    let old = UpcastEvent::new("test.account.opened.v1", json!({ "whatever": true }));

    given_stored(&decider(), [old])
        .when(open("jane"))
        .await
        .then_state(|state| {
            assert_eq!(
                state.owner, "from the old shape",
                "the upcaster did not run"
            );
        })
        .then_failed(AccountError::AlreadyOpen {
            owner: "from the old shape".to_owned(),
        });
}

#[tokio::test]
async fn fails_on_a_stored_event_without_a_rule() {
    let message = failure_of(|| {
        let _ = given_stored(
            &decider(),
            [UpcastEvent::new("test.account.unheard-of", json!({}))],
        );
    });

    assert!(message.starts_with("given stored: "), "{message}");
}

// Nothing.

#[tokio::test]
async fn passes_then_nothing_when_there_is_nothing_to_do() {
    given(&decider(), [])
        .when(open("nobody"))
        .await
        .then_nothing();
}

#[tokio::test]
async fn fails_then_nothing_when_something_happened_or_failed() {
    let something = given(&decider(), []).when(open("golo")).await;
    assert_failure(
        || {
            let _ = something.then_nothing();
        },
        "1 event(s)",
    );

    let failed = given(&decider(), []).when(open("")).await;
    assert_failure(
        || {
            let _ = failed.then_nothing();
        },
        "expected nothing to happen, got error: NoOwner",
    );
}

// Failing.

#[tokio::test]
async fn matches_the_error_of_the_decider() {
    given(&decider(), [opened("golo")])
        .when(open("jane"))
        .await
        .then_failed(AccountError::AlreadyOpen {
            owner: "golo".to_owned(),
        });
}

#[tokio::test]
async fn tells_the_errors_of_a_decider_apart() {
    given(&decider(), [])
        .when(open("mallory"))
        .await
        .then_failed(AccountError::OwnerBanned("mallory".to_owned()));

    let decision = given(&decider(), [opened("golo")]).when(open("jane")).await;
    assert_eq!(
        failure_of(|| {
            let _ = decision.then_failed(AccountError::OwnerBanned("jane".to_owned()));
        }),
        r#"expected an error matching OwnerBanned("jane"), got AlreadyOpen { owner: "golo" }"#,
    );
}

#[tokio::test]
async fn fails_then_failed_when_nothing_failed() {
    let decision = given(&decider(), []).when(open("golo")).await;

    assert_eq!(
        failure_of(|| {
            let _ = decision.then_failed(AccountError::NoOwner);
        }),
        r#"expected an error matching NoOwner, got 1 event(s): ["test.account.opened"]"#,
    );
}

#[tokio::test]
async fn hands_out_the_result_for_what_then_failed_does_not_compare() {
    // The category of an error, which then_failed does not compare.
    let decision = given(&decider(), []).when(open("")).await;
    let Err(ExecuteError::Decider(refusal)) = decision.result() else {
        panic!("the decider refuses the command");
    };
    assert_eq!(refusal.category(), Some(Category::Permanent));

    // A refusal of the kit, which the decider never sees.
    let decision = given(&decider(), [])
        .when(open("golo").declaring(|| {
            vec![
                Precondition::unconditionally(),
                Precondition::on_state_read(),
            ]
        }))
        .await;
    assert!(
        matches!(decision.result(), Err(ExecuteError::Permanent(_))),
        "{decision:?}"
    );

    let decision = given(&decider(), []).when(open("golo")).await;
    let Ok(events) = decision.result() else {
        panic!("the decider decides on events");
    };
    assert_eq!(events.len(), 1);
}

// The state.

#[tokio::test]
async fn checks_the_state_the_command_was_decided_on() {
    let mut checked = false;

    given(&decider(), [opened("golo")])
        .when(open("jane"))
        .await
        .then_state(|state| {
            checked = true;
            assert_eq!(
                *state,
                Account {
                    is_open: true,
                    owner: "golo".to_owned()
                }
            );
        });

    assert!(checked, "then_state did not run its check");
}

// Matching events.

fn is_opened(event: &AccountEvent) -> bool {
    matches!(event, AccountEvent::Opened(_))
}

fn is_closed(event: &AccountEvent) -> bool {
    matches!(event, AccountEvent::Closed(_))
}

#[tokio::test]
async fn passes_the_matchers_when_the_events_match() {
    given(&emitting(|| vec![opened("golo"), opened("jane")]), [])
        .when(open("golo"))
        .await
        .then_some_event(is_opened)
        .then_every_event(is_opened)
        .then_no_event(is_closed);
}

#[tokio::test]
async fn fails_the_matchers_when_nothing_matches() {
    let decision = given(&decider(), []).when(open("golo")).await;
    assert_failure(
        || {
            let _ = decision.then_some_event(is_closed);
        },
        "no event matched",
    );
    assert_failure(
        || {
            let _ = decision.then_no_event(is_opened);
        },
        "event 0 matched although it should not",
    );

    let mixed = given(
        &emitting(|| vec![opened("golo"), AccountEvent::Closed(Closed {})]),
        [],
    )
    .when(open("golo"))
    .await;
    assert_failure(
        || {
            let _ = mixed.then_every_event(is_opened);
        },
        "event 1 did not match",
    );

    let empty = given(&decider(), []).when(open("nobody")).await;
    assert_failure(
        || {
            let _ = empty.then_every_event(is_opened);
        },
        "got none",
    );
}

#[tokio::test]
async fn fails_the_matchers_on_an_error() {
    let decision = given(&decider(), []).when(open("")).await;

    let assertions: [NamedAssertion<'_>; 3] = [
        (
            "then_some_event",
            Box::new(|| {
                let _ = decision.then_some_event(|_| true);
            }),
        ),
        (
            "then_every_event",
            Box::new(|| {
                let _ = decision.then_every_event(|_| true);
            }),
        ),
        (
            "then_no_event",
            Box::new(|| {
                let _ = decision.then_no_event(|_| true);
            }),
        ),
    ];

    for (name, assertion) in assertions {
        let message = failure_of(assertion);
        assert!(message.contains("got error: NoOwner"), "{name}: {message}");
    }
}

// Expecting events.

#[tokio::test]
async fn fails_then_events_on_an_error() {
    let decision = given(&decider(), []).when(open("")).await;

    assert_failure(
        || {
            let _ = decision.then_events([opened("golo")]);
        },
        "expected events, got error: NoOwner",
    );
}

#[tokio::test]
async fn fails_then_events_on_the_wrong_count() {
    let decision = given(&decider(), []).when(open("golo")).await;

    assert_failure(
        || {
            let _ = decision.then_events([opened("golo"), AccountEvent::Closed(Closed {})]);
        },
        "expected 2 event(s), got 1",
    );
}

#[tokio::test]
async fn fails_then_events_on_the_wrong_type() {
    let decision = given(&decider(), []).when(open("golo")).await;

    assert_failure(
        || {
            let _ = decision.then_events([AccountEvent::Closed(Closed {})]);
        },
        r#"want "test.account.closed""#,
    );
}

#[tokio::test]
async fn fails_then_events_on_the_wrong_data() {
    let decision = given(&decider(), []).when(open("golo")).await;

    assert_eq!(
        failure_of(|| {
            let _ = decision.then_events([opened("someone-else")]);
        }),
        r#"event 0: got {"owner":"golo"}, want {"owner":"someone-else"}"#,
    );
}

#[tokio::test]
async fn fails_then_events_when_the_expected_event_can_not_be_encoded() {
    // An empty map of positions can be encoded, a filled one can not, so the
    // decided event passes, and only the expected one fails.
    let decision = given(&emitting(|| vec![positioned(None)]), [])
        .when(open("golo"))
        .await;

    assert_failure(
        || {
            let _ =
                decision.then_events([positioned(Some(HashMap::from([((1, 2), "x".to_owned())])))]);
        },
        "event 0: key must be a string",
    );
}

#[tokio::test]
async fn says_so_when_nothing_happened_at_all() {
    let decision = given(&decider(), []).when(open("nobody")).await;

    assert_failure(
        || {
            let _ = decision.then_events([opened("golo")]);
        },
        "no events",
    );
}

// Events whose data can not be encoded.

const ENCODING_REFUSAL: &str = "permanent failure: refusing to write an event of type \
    \"test.account.unencodable\" to \"/account/1\", since its data can not be encoded as JSON: \
    refusing to be encoded";

#[tokio::test]
async fn refuses_an_event_that_can_not_be_encoded_as_a_permanent_failure() {
    let decision = given(
        &emitting(|| vec![AccountEvent::Unencodable(Unencodable {})]),
        [],
    )
    .when(open("golo"))
    .await;

    assert!(
        matches!(decision.result(), Err(ExecuteError::Permanent(_))),
        "{decision:?}"
    );
    assert_eq!(
        failure_of(|| {
            let _ = decision.then_nothing();
        }),
        format!("expected nothing to happen, got error: {ENCODING_REFUSAL}"),
    );
}

#[tokio::test]
async fn refuses_all_events_if_the_second_can_not_be_encoded() {
    let decision = given(
        &emitting(|| vec![opened("golo"), AccountEvent::Unencodable(Unencodable {})]),
        [],
    )
    .when(open("golo"))
    .await;

    let assertions: [NamedAssertion<'_>; 3] = [
        (
            "then_events",
            Box::new(|| {
                let _ = decision.then_events([opened("golo")]);
            }),
        ),
        (
            "then_nothing",
            Box::new(|| {
                let _ = decision.then_nothing();
            }),
        ),
        (
            "then_some_event",
            Box::new(|| {
                let _ = decision.then_some_event(|_| true);
            }),
        ),
    ];

    for (name, assertion) in assertions {
        let message = failure_of(assertion);
        assert!(
            message.ends_with(&format!("got error: {ENCODING_REFUSAL}")),
            "{name}: {message}"
        );
    }
}

#[tokio::test]
async fn lets_events_through_whose_data_can_be_encoded() {
    given(
        &emitting(|| vec![opened("golo"), AccountEvent::Closed(Closed {})]),
        [],
    )
    .when(open("golo"))
    .await
    .then_events([opened("golo"), AccountEvent::Closed(Closed {})]);
}

// Checking the preconditions.

#[tokio::test]
async fn refuses_preconditions_that_can_not_be_honored_with_the_error_of_execute() {
    let cases: [(&str, Declaring, &str); 2] = [
        ("none", Vec::new, "declares no preconditions"),
        (
            "unconditionally with another one",
            || {
                vec![
                    Precondition::unconditionally(),
                    Precondition::on_state_read(),
                ]
            },
            "combines Precondition::unconditionally with other preconditions",
        ),
    ];

    for (name, declaring, cause) in cases {
        let command = open("golo").declaring(declaring);
        let refusal = check_preconditions(&command).expect_err(name);

        let decision = given(&decider(), []).when(command).await;

        assert!(
            matches!(decision.result(), Err(ExecuteError::Permanent(_))),
            "{name}: {decision:?}"
        );
        let message = failure_of(|| {
            let _ = decision.then_events([opened("golo")]);
        });
        assert_eq!(
            message,
            format!("expected events, got error: {refusal}"),
            "{name}"
        );
        assert!(message.contains(cause), "{name}: {message}");
    }
}

#[tokio::test]
async fn does_not_let_the_decider_decide_on_a_command_it_refuses() {
    let decisions = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&decisions);
    let counting = Decider::<Open, Account, AccountError>::new(move |_, _| {
        let _ = counted.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(Vec::new()))
    });

    let _ = given(&counting, [])
        .when(open("golo").declaring(Vec::new))
        .await;

    assert_eq!(decisions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn lets_the_decider_decide_on_every_valid_declaration() {
    let declarations: [Declaring; 5] = [
        || vec![Precondition::on_state_read()],
        || vec![Precondition::unconditionally()],
        || vec![Precondition::on_pristine_subject("/account/1")],
        || vec![Precondition::on_event_id("/account/1", Revision::from(7))],
        || {
            vec![
                Precondition::on_state_read(),
                Precondition::on_populated_subject("/account/2"),
                Precondition::require(eventsourcingdb::Precondition::is_eventql_query_true(
                    "FROM e IN events PROJECT INTO true",
                )),
            ]
        },
    ];

    for declaring in declarations {
        given(&decider(), [])
            .when(open("golo").declaring(declaring))
            .await
            .then_events([opened("golo")]);
    }
}

// Comparing the declared preconditions.

#[tokio::test]
async fn compares_the_declared_preconditions_of_every_kind() {
    given(&decider(), [])
        .when(open("golo").declaring(|| {
            vec![
                Precondition::on_pristine_subject("/account/1"),
                Precondition::on_populated_subject("/account/2"),
                Precondition::on_event_id("/account/3", Revision::from(9)),
                Precondition::require(eventsourcingdb::Precondition::is_eventql_query_true(
                    "FROM e IN events PROJECT INTO true",
                )),
                Precondition::on_state_read(),
            ]
        }))
        .await
        .then_preconditions([
            Precondition::on_pristine_subject("/account/1"),
            Precondition::on_populated_subject("/account/2"),
            Precondition::on_event_id("/account/3", Revision::from(9)),
            Precondition::require(eventsourcingdb::Precondition::is_eventql_query_true(
                "FROM e IN events PROJECT INTO true",
            )),
            Precondition::on_state_read(),
        ]);
}

#[tokio::test]
async fn compares_an_unconditional_command() {
    given(&decider(), [])
        .when(open("golo").declaring(|| vec![Precondition::unconditionally()]))
        .await
        .then_preconditions([Precondition::unconditionally()]);
}

#[tokio::test]
async fn reports_what_a_command_declares_even_if_it_is_refused() {
    // Executing refuses a command without preconditions, but the fixture
    // reports what it sees.
    given(&decider(), [])
        .when(open("golo").declaring(Vec::new))
        .await
        .then_preconditions([]);
}

#[tokio::test]
async fn fails_then_preconditions_on_the_wrong_count() {
    let decision = given(&decider(), [])
        .when(open("golo").declaring(Vec::new))
        .await;

    assert_eq!(
        failure_of(|| {
            let _ = decision.then_preconditions([Precondition::on_state_read()]);
        }),
        "expected 1 precondition(s), got 0: []",
    );
}

#[tokio::test]
async fn fails_then_preconditions_on_the_wrong_content() {
    let decision = given(&decider(), [])
        .when(open("golo").declaring(|| vec![Precondition::on_pristine_subject("/account/1")]))
        .await;

    assert_failure(
        || {
            let _ =
                decision.then_preconditions([Precondition::on_pristine_subject("/account/other")]);
        },
        "/account/other",
    );
    assert_eq!(
        failure_of(|| {
            let _ = decision.then_preconditions([Precondition::on_populated_subject("/account/1")]);
        }),
        r#"precondition 0: got Precondition::require({"payload":{"subject":"/account/1"},"type":"isSubjectPristine"}), want Precondition::require({"payload":{"subject":"/account/1"},"type":"isSubjectPopulated"})"#,
    );
}

#[tokio::test]
async fn tells_a_recursive_subject_check_from_one_of_the_subject_alone() {
    let decision = given(&decider(), [])
        .when(open("golo").declaring(|| {
            vec![Precondition::require(
                eventsourcingdb::Precondition::is_subject_pristine(
                    "/account/1",
                    Some(eventsourcingdb::PreconditionOptions { recursive: true }),
                ),
            )]
        }))
        .await;

    assert_failure(
        || {
            let _ = decision.then_preconditions([Precondition::on_pristine_subject("/account/1")]);
        },
        "recursive",
    );
}
