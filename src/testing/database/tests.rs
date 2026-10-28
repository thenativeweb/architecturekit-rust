// These tests check the isolated database: it hands out a store that writes
// and reads, registers the schemas first, and fails the test for a schema it
// refuses. The options of a store come from its builder.

use std::{panic::AssertUnwindSafe, pin::pin};

use futures_util::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};

use super::Database;
use crate::{Event, EventOn, EventSchema, Events, Precondition, State};

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.entry-written")]
struct EntryWritten {
    text: String,
}

#[derive(Debug, Events)]
enum JournalEvent {
    EntryWritten(EntryWritten),
}

#[derive(Clone, Debug, Default)]
struct Journal {
    entries: Vec<String>,
}

impl State for Journal {
    type Event = JournalEvent;

    fn evolve(mut self, event: &JournalEvent) -> Self {
        let JournalEvent::EntryWritten(written) = event;
        self.entries.push(written.text.clone());
        self
    }
}

const SOURCE: &str = "https://thenativeweb.io";

#[tokio::test]
async fn hands_out_a_store_that_writes_and_reads() {
    let database = Database::isolated().await;
    let store = database.store(SOURCE, JournalEvent::schemas()).await;

    let _ = store
        .write(
            vec![EventOn::new(
                "/journals/1",
                &EntryWritten {
                    text: "an entry".to_owned(),
                },
            )],
            vec![Precondition::on_pristine_subject("/journals/1")],
        )
        .await
        .expect("the entry is written");

    let journal: Journal = store.load("/journals/1").await.expect("the journal loads");
    assert_eq!(journal.entries, ["an entry"]);
}

#[tokio::test]
async fn registers_the_schemas() {
    let database = Database::isolated().await;
    let _ = database.store(SOURCE, JournalEvent::schemas()).await;

    // Only all event types are read, as the store reads them, since reading a
    // single one can make EventSourcingDB 1.2.0 stop answering writes.
    let client = database.client().await;
    let event_types = client
        .read_event_types()
        .await
        .expect("the event types can be read");
    let mut event_types = pin!(event_types);
    let mut registered = None;
    while let Some(event_type) = event_types.next().await {
        let event_type = event_type.expect("the event type can be read");
        if event_type.ty == EntryWritten::EVENT_TYPE {
            registered = event_type.schema;
        }
    }

    assert_eq!(
        registered.as_ref(),
        Some(EventSchema::of::<EntryWritten>().schema())
    );
}

#[tokio::test]
async fn fails_the_test_for_a_schema_the_database_refuses() {
    let database = Database::isolated().await;

    // A registered schema can not change, so the one of the event, which
    // differs, is refused.
    let _ = database
        .client()
        .await
        .register_event_schema(
            EntryWritten::EVENT_TYPE,
            &serde_json::json!({ "type": "object", "properties": {} }),
        )
        .await
        .expect("another schema is registered");

    let failure = AssertUnwindSafe(database.store(SOURCE, JournalEvent::schemas()))
        .catch_unwind()
        .await
        .expect_err("the database refuses the schema");

    let message = failure
        .downcast_ref::<String>()
        .expect("the failure has a message");
    assert!(
        message.starts_with("registering the schemas: "),
        "{message}"
    );
}

#[tokio::test]
async fn is_a_database_of_the_test_s_own_which_is_stopped_once_dropped() {
    let first = Database::isolated().await;
    let second = Database::isolated().await;

    let _ = first
        .store(SOURCE, JournalEvent::schemas())
        .await
        .write(
            vec![EventOn::new(
                "/journals/1",
                &EntryWritten {
                    text: "only in the first".to_owned(),
                },
            )],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the entry is written");
    let journal: Journal = second
        .store(SOURCE, JournalEvent::schemas())
        .await
        .load("/journals/1")
        .await
        .expect("the journal loads");
    assert!(
        journal.entries.is_empty(),
        "a database does not see the events of another one"
    );

    let client = first.client().await;
    drop(first);
    assert!(
        client.ping().await.is_err(),
        "the database has to be stopped once it is dropped"
    );
}
