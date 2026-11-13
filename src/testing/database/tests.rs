// These tests check the isolated database: it hands out a store that writes
// and reads, registers the schemas first, and fails the test for a schema it
// refuses. The options of a store come from its builder.
//
// They also check the shared database: the tests of a program get the same
// one, it outlives the runtime that started it, and the reaper removes it once
// the program has ended, which a second run of this program shows. It starts
// at most once, also if the first caller stops waiting, and a failed start
// fails every caller with the same message.

use std::{
    error::Error,
    fmt, io,
    panic::AssertUnwindSafe,
    pin::pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use eventsourcingdb::Client;
use futures_util::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::{Database, Failure, StartedOnce, store, store_with};
use crate::{Event, EventOn, EventSchema, Events, Precondition, State, Subject};

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
                Subject::new("/journals/1"),
                &EntryWritten {
                    text: "an entry".to_owned(),
                },
            )],
            vec![Precondition::on_pristine_subject(Subject::new(
                "/journals/1",
            ))],
        )
        .await
        .expect("the entry is written");

    let journal: Journal = store
        .load(&Subject::new("/journals/1"))
        .await
        .expect("the journal loads");
    assert_eq!(journal.entries, ["an entry"]);
}

#[tokio::test]
async fn registers_the_schemas() {
    let database = Database::isolated().await;
    let _ = database.store(SOURCE, JournalEvent::schemas()).await;

    // Only all event types are read, as the store reads them, since reading a
    // single one can make EventSourcingDB 1.2.0 stop answering writes.
    let client = database.client();
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
                Subject::new("/journals/1"),
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
        .load(&Subject::new("/journals/1"))
        .await
        .expect("the journal loads");
    assert!(
        journal.entries.is_empty(),
        "a database does not see the events of another one"
    );

    let client = first.client();
    drop(first);
    assert!(
        client.ping().await.is_err(),
        "the database has to be stopped once it is dropped"
    );
}

#[tokio::test]
async fn hands_out_the_address_and_the_token_that_a_client_needs() {
    let database = Database::isolated().await;

    let client = Client::new(database.url().clone(), database.api_token());

    client
        .verify_api_token()
        .await
        .expect("the address and the token reach the database");
}

#[tokio::test]
async fn hands_out_a_store_with_the_options_of_its_builder() {
    let database = Database::isolated().await;
    let configured = AtomicBool::new(false);

    let store = database
        .store_with(SOURCE, JournalEvent::schemas(), |builder| {
            configured.store(true, Ordering::SeqCst);
            builder.state_cache(10)
        })
        .await;

    assert!(
        configured.load(Ordering::SeqCst),
        "the builder is handed over"
    );
    let journal: Journal = store
        .load(&unique_subject())
        .await
        .expect("the store reads from the database");
    assert_eq!(journal.entries, Vec::<String>::new());
}

/// Returns a subject that no other test writes to, since the tests of a
/// program share the shared database.
fn unique_subject() -> Subject {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    Subject::new(&format!(
        "/journals/{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ))
}

#[tokio::test]
async fn shares_one_database_between_the_tests_of_a_program() {
    let first = Database::shared().await;
    let second = Database::shared().await;

    assert!(
        std::ptr::eq(first, second),
        "both ask for the same database"
    );
    first
        .client()
        .verify_api_token()
        .await
        .expect("the shared database answers");
}

#[test]
fn keeps_the_shared_database_beyond_the_runtime_that_started_it() {
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime starts")
    }

    let url = runtime().block_on(async { Database::shared().await.url().clone() });

    // The runtime that asked first has ended by now, along with everything
    // that belonged to it.
    runtime().block_on(async {
        let database = Database::shared().await;
        assert_eq!(database.url(), &url);
        database
            .client()
            .verify_api_token()
            .await
            .expect("the shared database still answers");
    });
}

// The shared database is a static that every test of the program may ask for
// first, so the way it starts once is checked with a start of its own, which
// waits for the test, and needs no Docker.

#[tokio::test]
async fn starts_once_and_hands_the_outcome_to_a_caller_after_one_that_stopped_waiting() {
    static STARTED: StartedOnce<u32> = StartedOnce::new("architecturekit-test-started-once");
    static STARTS: AtomicUsize = AtomicUsize::new(0);

    let (release, released) = oneshot::channel::<()>();
    let first = STARTED.get(move || async move {
        let _ = STARTS.fetch_add(1, Ordering::SeqCst);
        released.await.expect("the test releases the start");
        Ok((42, ()))
    });
    assert!(
        first.now_or_never().is_none(),
        "the start waits for the test, so the first caller stops waiting before it ends"
    );
    release
        .send(())
        .expect("the start is still there, although nobody waits for it");

    let second = STARTED
        .get(|| async {
            let _ = STARTS.fetch_add(1, Ordering::SeqCst);
            Ok((23, ()))
        })
        .await;

    assert_eq!(
        second,
        Ok(&42),
        "the later caller gets what the first start brought"
    );
    assert_eq!(STARTS.load(Ordering::SeqCst), 1, "it starts only once");
}

/// A failure to start a container, with its cause.
#[derive(Debug)]
struct NotStarted(io::Error);

impl fmt::Display for NotStarted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("starting the container")
    }
}

impl Error for NotStarted {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[tokio::test]
async fn fails_every_caller_with_the_same_failure_and_its_causes_once_starting_has_failed() {
    static STARTED: StartedOnce<u32> = StartedOnce::new("architecturekit-test-started-once");
    static STARTS: AtomicUsize = AtomicUsize::new(0);

    let first = STARTED
        .get(|| async {
            let _ = STARTS.fetch_add(1, Ordering::SeqCst);
            Err::<(u32, ()), Failure>(Box::new(NotStarted(io::Error::other(
                "Docker is not running",
            ))))
        })
        .await;
    let second = STARTED
        .get(|| async {
            let _ = STARTS.fetch_add(1, Ordering::SeqCst);
            Ok((23, ()))
        })
        .await;

    assert_eq!(
        first,
        Err("starting the container: Docker is not running".to_owned())
    );
    assert_eq!(second, first, "a later caller gets the same failure");
    assert_eq!(
        STARTS.load(Ordering::SeqCst),
        1,
        "a failed start is not tried again"
    );
}

#[tokio::test]
async fn fails_every_caller_if_the_start_panics() {
    static STARTED: StartedOnce<u32> = StartedOnce::new("architecturekit-test-started-once");

    let first = STARTED
        .get::<(), _>(|| async { panic!("the image is broken") })
        .await;
    let second = STARTED.get(|| async { Ok((23, ())) }).await;

    assert_eq!(
        first,
        Err(
            "the thread architecturekit-test-started-once panicked: the image is broken".to_owned()
        )
    );
    assert_eq!(second, first, "a later caller gets the same failure");
}

#[tokio::test]
async fn hands_out_a_store_on_the_shared_database() {
    let subject = unique_subject();
    let store = store(SOURCE, JournalEvent::schemas()).await;

    let _ = store
        .write(
            vec![EventOn::new(
                subject.clone(),
                &EntryWritten {
                    text: "a shared entry".to_owned(),
                },
            )],
            vec![Precondition::on_pristine_subject(subject.clone())],
        )
        .await
        .expect("the entry is written");

    let configured = AtomicBool::new(false);
    let journal: Journal = store_with(SOURCE, JournalEvent::schemas(), |builder| {
        configured.store(true, Ordering::SeqCst);
        builder
    })
    .await
    .load(&subject)
    .await
    .expect("the journal loads");
    assert_eq!(journal.entries, ["a shared entry"]);
    assert!(
        configured.load(Ordering::SeqCst),
        "the builder is handed over"
    );
}

/// The environment variable that turns the next test into the program whose
/// shared database the test after it watches being removed.
const PRINTS_THE_SHARED_DATABASE: &str = "ARCHITECTUREKIT_PRINTS_THE_SHARED_DATABASE";

#[tokio::test]
#[ignore = "run by removes_the_shared_database_once_the_program_ends"]
async fn prints_the_address_of_the_shared_database() {
    if std::env::var(PRINTS_THE_SHARED_DATABASE).is_err() {
        return;
    }

    println!("shared database: {}", Database::shared().await.url());
}

#[tokio::test]
async fn removes_the_shared_database_once_the_program_ends() {
    let program = std::env::current_exe().expect("the test program can be found");
    let output = std::process::Command::new(program)
        .args([
            "testing::database::tests::prints_the_address_of_the_shared_database",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env(PRINTS_THE_SHARED_DATABASE, "1")
        .output()
        .expect("the test program runs");
    let printed = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{printed}");

    let url = printed
        .lines()
        .find_map(|line| line.strip_prefix("shared database: "))
        .unwrap_or_else(|| panic!("the address is printed: {printed}"));
    let client = Client::new(url.parse().expect("the address is a URL"), "unused");

    // The reaper waits a few seconds after the program has gone, in case it
    // connects again, and removes the container then.
    let deadline = Instant::now() + Duration::from_secs(60);
    while client.ping().await.is_ok() {
        assert!(
            Instant::now() < deadline,
            "the shared database has to be removed once its program has ended"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
