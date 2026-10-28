// These tests mirror TestStoredEvent, TestStoredEvents, TestStoredEventsAt,
// TestProject and TestProjectTransactional of ArchitectureKit for Go. Some do
// not apply to Rust: a projection is transactional by its trait, so project
// can not be handed one, and the views hand out a Vec, which a test compares
// with assert_eq!, so TestItemsOf and TestExpectItems have nothing to check,
// nor does TestExpectMode, since a projection names its kind by its trait.
// Since project returns its failure rather than failing the test, the tests
// check the failure where Go checks what the test reported.

use std::{
    error::Error,
    fmt,
    sync::{Arc, LazyLock, Mutex},
};

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    Categorized, Category, Envelope, Event, Events, InMemoryView, Projection, ProjectionError,
    Transaction, TransactionalProjection, UpcastEvent, Upcasters, ViewError,
    testing::{StoredEvent, project, project_transactional, stored_events, stored_events_at},
};

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "test.account.opened")]
struct Opened {
    owner: String,
}

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "test.account.closed")]
struct Closed {}

/// An event whose data refuses to be encoded.
#[derive(Debug, Deserialize, Event)]
#[event(type = "test.account.unencodable")]
struct Unencodable {}

impl Serialize for Unencodable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("refusing to be encoded"))
    }
}

#[derive(Debug, Events)]
enum AccountEvent {
    Opened(Opened),
    Closed(Closed),
    Unencodable(Unencodable),
}

/// The events of an account that a projection of owners cares about, which
/// leaves out every other type.
#[derive(Debug, Events)]
enum Opening {
    Opened(Opened),
}

fn opened(owner: &str) -> AccountEvent {
    AccountEvent::Opened(Opened {
        owner: owner.to_owned(),
    })
}

fn closed() -> AccountEvent {
    AccountEvent::Closed(Closed {})
}

fn owner_view() -> InMemoryView<String, String> {
    InMemoryView::new(String::clone)
}

/// The failure of a projection of a test.
#[derive(Debug)]
struct Refused(&'static str);

impl fmt::Display for Refused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for Refused {}

impl Categorized for Refused {
    fn category(&self) -> Option<Category> {
        Some(Category::Permanent)
    }
}

impl From<ViewError> for Refused {
    fn from(_: ViewError) -> Self {
        Self("the view refused")
    }
}

/// A projection of the stored events, which decodes the openings itself, and
/// refuses one it can not decode.
struct Owners {
    view: InMemoryView<String, String>,
}

impl Projection for Owners {
    type Event = eventsourcingdb::Event;
    type Error = Refused;

    async fn apply(&self, event: Envelope<eventsourcingdb::Event>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        if event.event_type != Opened::EVENT_TYPE {
            return Ok(());
        }

        let opened: Opened = serde_json::from_value(event.data.data().clone())
            .map_err(|_| Refused("the opening can not be decoded"))?;
        let _ = self.view.insert(&event, opened.owner)?;

        Ok(())
    }
}

/// A projection of typed openings, which records what it has seen.
struct TypedOwners {
    view: InMemoryView<String, String>,
    seen: Mutex<Vec<String>>,
}

impl TypedOwners {
    fn new() -> Self {
        Self {
            view: owner_view(),
            seen: Mutex::new(Vec::new()),
        }
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().expect("not poisoned").clone()
    }
}

/// An older version wrote the openings of two accounts as one event, which
/// the upcasters split.
static OWNER_UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
    Upcasters::new().upcast("test.account.opened-twice", |event| {
        Ok::<_, serde_json::Error>(vec![
            UpcastEvent::clone(&event)
                .with_type(Opened::EVENT_TYPE)
                .with_data(json!({ "owner": "golo" })),
            event
                .with_type(Opened::EVENT_TYPE)
                .with_data(json!({ "owner": "jane" })),
        ])
    })
});

impl Projection for TypedOwners {
    type Event = Opening;
    type Error = ViewError;

    async fn apply(&self, event: Envelope<Opening>) -> Result<(), ViewError> {
        tokio::task::yield_now().await;
        let Opening::Opened(opened) = &event.data;
        let _ = self.view.insert(&event, opened.owner.clone())?;

        Ok(())
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&OWNER_UPCASTERS)
    }

    fn seen(&self, event_id: &str) {
        self.seen
            .lock()
            .expect("not poisoned")
            .push(event_id.to_owned());
    }
}

#[test]
fn carries_what_a_database_would() {
    let stored = StoredEvent::new("/account/1", "7", &opened("golo"));

    assert_eq!(stored.subject, "/account/1");
    assert_eq!(stored.id, "7");
    assert_eq!(stored.event_type, Opened::EVENT_TYPE);
    assert_eq!(stored.data, json!({ "owner": "golo" }));
    assert_eq!(stored.source, "https://architecturekit.test");

    let event: eventsourcingdb::Event = stored.into();
    assert_eq!(
        event.specversion(),
        "1.0",
        "the stored shape has to look complete"
    );
    assert_eq!(
        event.datacontenttype(),
        "application/json",
        "the stored shape has to look complete"
    );
}

#[test]
#[should_panic(
    expected = r#"architecturekit: StoredEvent::new can not encode an event of type "test.account.unencodable": refusing to be encoded"#
)]
fn panics_on_an_event_that_can_not_be_encoded() {
    let _ = StoredEvent::new(
        "/account/1",
        "0",
        &AccountEvent::Unencodable(Unencodable {}),
    );
}

#[test]
fn turns_into_the_event_of_the_client_sdk_with_every_field() {
    let noon: DateTime<Utc> = "2026-03-02T12:00:00Z".parse().expect("a time");
    let stored = StoredEvent {
        time: noon,
        event_type: "test.account.invented".to_owned(),
        data: json!("not an object"),
        trace_parent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned()),
        trace_state: Some("vendor=value".to_owned()),
        ..StoredEvent::new("/account/1", "7", &opened("golo"))
    };

    let event: eventsourcingdb::Event = stored.into();

    assert_eq!(event.id(), "7");
    assert_eq!(*event.time(), noon);
    assert_eq!(event.source(), "https://architecturekit.test");
    assert_eq!(event.subject(), "/account/1");
    assert_eq!(event.ty(), "test.account.invented");
    assert_eq!(
        event.data(),
        &json!("not an object"),
        "data the projection can not read has to arrive as it is"
    );
    assert_eq!(
        event.traceparent(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    assert_eq!(event.tracestate(), Some("vendor=value"));
}

#[test]
fn numbers_from_zero() {
    let stored = stored_events("/account/1", [opened("golo"), closed(), opened("jane")]);

    let ids: Vec<&str> = stored.iter().map(|event| event.id.as_str()).collect();
    assert_eq!(ids, ["0", "1", "2"]);
}

#[test]
fn numbers_from_the_given_id_and_times_a_minute_apart() {
    let noon: DateTime<Utc> = "2026-03-02T12:00:00Z".parse().expect("a time");

    let stored = stored_events_at(
        "/account/1",
        7,
        noon,
        [opened("golo"), closed(), opened("jane")],
    );

    let shapes: Vec<(&str, DateTime<Utc>, &str)> = stored
        .iter()
        .map(|event| (event.id.as_str(), event.time, event.subject.as_str()))
        .collect();
    assert_eq!(
        shapes,
        [
            ("7", noon, "/account/1"),
            ("8", noon + TimeDelta::minutes(1), "/account/1"),
            ("9", noon + TimeDelta::minutes(2), "/account/1"),
        ]
    );
    assert_eq!(
        stored[1].event_type,
        Closed::EVENT_TYPE,
        "the events keep their order and types"
    );
    assert_eq!(stored[2].data, json!({ "owner": "jane" }));
}

#[tokio::test]
async fn drives_a_projection() {
    let owners = Owners { view: owner_view() };

    project(
        &owners,
        &stored_events("/account/1", [opened("golo"), closed(), opened("jane")]),
    )
    .await
    .expect("the events are applied");

    assert_eq!(owners.view.all(), ["golo", "jane"]);
}

#[tokio::test]
async fn reports_a_refusal() {
    let owners = Owners { view: owner_view() };

    // Data that does not match the event makes the projection refuse.
    let mut refused = StoredEvent::new("/account/1", "0", &opened("golo"));
    refused.data = json!({ "owner": 42 });

    let failure = project(&owners, &[refused])
        .await
        .expect_err("the projection refuses the event");

    assert_eq!(failure.to_string(), "the opening can not be decoded");
    assert!(
        matches!(&failure, ProjectionError::Projection { error, .. } if error.is::<Refused>()),
        "the error of the projection has to be kept: {failure:?}"
    );
}

#[tokio::test]
async fn drives_a_typed_projection() {
    let owners = TypedOwners::new();

    project(
        &owners,
        &stored_events("/account/1", [opened("golo"), closed(), opened("jane")]),
    )
    .await
    .expect("the events are applied");

    assert_eq!(owners.view.all(), ["golo", "jane"]);
}

#[tokio::test]
async fn reports_data_a_typed_projection_can_not_read() {
    let owners = TypedOwners::new();
    let mut broken = StoredEvent::new("/account/1", "0", &opened("golo"));
    broken.data = json!("not an object");

    let failure = project(&owners, &[broken])
        .await
        .expect_err("the data can not be decoded");

    assert_eq!(failure.category(), Some(Category::Permanent));
    assert!(owners.seen().is_empty(), "a failed event is not seen");
}

#[tokio::test]
async fn takes_the_way_of_a_run() {
    // The upcasters run, an event an upcaster splits reaches the projection
    // part by part, an event of a type the projection does not know is left
    // out, and the projection learns of every event.
    let owners = TypedOwners::new();
    let mut invented = StoredEvent::new("/account/2", "1", &opened("john"));
    "test.account.invented".clone_into(&mut invented.event_type);
    let twice = StoredEvent {
        event_type: "test.account.opened-twice".to_owned(),
        ..StoredEvent::new("/account/3", "2", &opened("ignored"))
    };

    project(&owners, &[invented, twice])
        .await
        .expect("the events are applied");

    assert_eq!(owners.view.all(), ["golo", "jane"]);
    assert_eq!(owners.seen(), ["1", "2"]);
}

/// What a transaction writes down that happens to it, shared with the
/// projection that began it.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn write(&self, entry: impl Into<String>) {
        self.0.lock().expect("not poisoned").push(entry.into());
    }

    fn entries(&self) -> Vec<String> {
        self.0.lock().expect("not poisoned").clone()
    }
}

/// What a transactional projection of owners fails at, if anything.
#[derive(Clone, Copy, Default)]
struct Trouble {
    begin: Option<&'static str>,
    commit: Option<&'static str>,
    rollback: Option<&'static str>,
}

/// A transactional projection of owners, whose transactions write down what
/// happens to them, and refuse everything but openings.
#[derive(Default)]
struct OwnerTable {
    log: Log,
    trouble: Trouble,
}

struct OwnerTransaction {
    log: Log,
    trouble: Trouble,
}

impl TransactionalProjection for OwnerTable {
    type Event = AccountEvent;
    type Error = Refused;
    type Transaction = OwnerTransaction;

    async fn checkpoint(&self) -> Result<Option<String>, Refused> {
        tokio::task::yield_now().await;
        Ok(None)
    }

    async fn begin(&self) -> Result<OwnerTransaction, Refused> {
        tokio::task::yield_now().await;
        if let Some(failure) = self.trouble.begin {
            return Err(Refused(failure));
        }
        self.log.write("begin");

        Ok(OwnerTransaction {
            log: self.log.clone(),
            trouble: self.trouble,
        })
    }
}

impl Transaction for OwnerTransaction {
    type Event = AccountEvent;
    type Error = Refused;

    async fn apply(&mut self, event: Envelope<AccountEvent>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        if !matches!(event.data, AccountEvent::Opened(_)) {
            return Err(Refused("only openings, please"));
        }
        self.log.write(format!("apply {}", event.id));

        Ok(())
    }

    async fn commit(self, last_event_id: &str) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.write(format!("commit {last_event_id}"));
        self.trouble
            .commit
            .map_or(Ok(()), |failure| Err(Refused(failure)))
    }

    async fn rollback(self) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        self.log.write("rollback");
        self.trouble
            .rollback
            .map_or(Ok(()), |failure| Err(Refused(failure)))
    }
}

#[tokio::test]
async fn applies_within_one_transaction() {
    let table = OwnerTable::default();

    project_transactional(
        &table,
        &stored_events("/account/1", [opened("golo"), opened("jane")]),
    )
    .await
    .expect("the events are applied");

    assert_eq!(
        table.log.entries(),
        ["begin", "apply 0", "apply 1", "commit 1"]
    );
}

#[tokio::test]
async fn begins_nothing_without_events() {
    let table = OwnerTable::default();

    project_transactional(&table, &[])
        .await
        .expect("nothing is applied");

    assert_eq!(table.log.entries(), Vec::<String>::new());
}

#[tokio::test]
async fn rolls_back_on_a_refusal() {
    let table = OwnerTable {
        trouble: Trouble {
            rollback: Some("rollback did not work either"),
            ..Trouble::default()
        },
        ..OwnerTable::default()
    };

    let failure = project_transactional(
        &table,
        &stored_events("/account/1", [opened("golo"), closed()]),
    )
    .await
    .expect_err("the closing is refused");

    assert!(
        matches!(failure, ProjectionError::RolledBack { .. }),
        "the failure of the rollback has to come alongside its cause: {failure:?}"
    );
    assert_eq!(
        failure.to_string(),
        "only openings, please, and rolling back failed: rollback did not work either"
    );
    assert_eq!(table.log.entries(), ["begin", "apply 0", "rollback"]);
}

#[tokio::test]
async fn reports_a_failing_begin() {
    let table = OwnerTable {
        trouble: Trouble {
            begin: Some("no connection"),
            ..Trouble::default()
        },
        ..OwnerTable::default()
    };

    let failure = project_transactional(&table, &stored_events("/account/1", [opened("golo")]))
        .await
        .expect_err("the transaction can not be begun");

    assert_eq!(failure.to_string(), "no connection");
    assert_eq!(table.log.entries(), Vec::<String>::new());
}

#[tokio::test]
async fn reports_a_failing_commit() {
    let table = OwnerTable {
        trouble: Trouble {
            commit: Some("disk full"),
            ..Trouble::default()
        },
        ..OwnerTable::default()
    };

    let failure = project_transactional(&table, &stored_events("/account/1", [opened("golo")]))
        .await
        .expect_err("the transaction can not be committed");

    assert_eq!(failure.to_string(), "disk full");
}

/// A transactional projection of typed openings, which keeps what a
/// transaction applies until it commits.
#[derive(Default)]
struct TypedOwnerTable {
    committed: Log,
}

struct TypedOwnerTransaction {
    committed: Log,
    pending: Vec<String>,
}

impl TransactionalProjection for TypedOwnerTable {
    type Event = Opening;
    type Error = Refused;
    type Transaction = TypedOwnerTransaction;

    async fn checkpoint(&self) -> Result<Option<String>, Refused> {
        tokio::task::yield_now().await;
        Ok(None)
    }

    async fn begin(&self) -> Result<TypedOwnerTransaction, Refused> {
        tokio::task::yield_now().await;
        Ok(TypedOwnerTransaction {
            committed: self.committed.clone(),
            pending: Vec::new(),
        })
    }
}

impl Transaction for TypedOwnerTransaction {
    type Event = Opening;
    type Error = Refused;

    async fn apply(&mut self, event: Envelope<Opening>) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        let Opening::Opened(opened) = event.data;
        self.pending.push(opened.owner);

        Ok(())
    }

    async fn commit(self, _: &str) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        for owner in self.pending {
            self.committed.write(owner);
        }

        Ok(())
    }

    async fn rollback(self) -> Result<(), Refused> {
        tokio::task::yield_now().await;
        Ok(())
    }
}

#[tokio::test]
async fn drives_a_typed_transaction() {
    let table = TypedOwnerTable::default();

    project_transactional(
        &table,
        &stored_events("/account/1", [opened("golo"), closed(), opened("jane")]),
    )
    .await
    .expect("the events are applied");

    assert_eq!(table.committed.entries(), ["golo", "jane"]);
}
