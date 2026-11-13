// These tests check the isolated database: it hands out a store that writes
// and reads, registers the schemas first, and fails the test for a schema it
// refuses. The options of a store come from its builder. It carries the label
// of the session of the program, as the shared database does.
//
// They also check the shared database: the tests of a program get the same
// one, it outlives the runtime that started it, and the reaper removes it once
// the program has ended, also if the program ends while the database is still
// starting, which second runs of this program show. It starts at most once,
// also if the first caller stops waiting, and a failed start fails every
// caller with the same message.

use std::{
    collections::HashMap,
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
use testcontainers::bollard::{
    Docker,
    models::ContainerSummary,
    query_parameters::{ListContainersOptionsBuilder, RemoveContainerOptionsBuilder},
};
use tokio::sync::oneshot;

use super::{
    Database, Failure, SESSION, StartedOnce, new_session, reaper::SESSION_LABEL, store, store_with,
};
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

/// Returns the containers, running or not, that carry the label of the given
/// session.
async fn containers_of(session: &str) -> Vec<ContainerSummary> {
    let label = format!("{SESSION_LABEL}={session}");
    let filters = HashMap::from([("label", vec![label.as_str()])]);
    let options = ListContainersOptionsBuilder::new()
        .all(true)
        .filters(&filters)
        .build();

    Docker::connect_with_defaults()
        .expect("Docker can be reached")
        .list_containers(Some(options))
        .await
        .expect("Docker lists the containers")
}

/// Removes the given containers, so that a test leaves nothing behind, even
/// if the code it checks does.
async fn remove(containers: &[ContainerSummary]) {
    let docker = Docker::connect_with_defaults().expect("Docker can be reached");
    for id in containers
        .iter()
        .filter_map(|container| container.id.as_deref())
    {
        let options = RemoveContainerOptionsBuilder::new()
            .force(true)
            .v(true)
            .build();
        let _ = docker.remove_container(id, Some(options)).await;
    }
}

/// Returns the ID of the container of an isolated database: the one with the
/// label of the session of the program that publishes the port of the
/// database.
async fn id_of(database: &Database) -> Option<String> {
    let port = database.url().port();
    containers_of(&SESSION)
        .await
        .into_iter()
        .find(|container| {
            container
                .ports
                .iter()
                .flatten()
                .any(|published| published.public_port == port)
        })
        .and_then(|container| container.id)
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

    // Docker tells whether the container is gone, rather than a ping of its
    // address, which Docker Desktop may still answer for a moment after the
    // container has been removed.
    let id = id_of(&first)
        .await
        .expect("Docker shows the container of the database");
    drop(first);
    assert!(
        containers_of(&SESSION)
            .await
            .iter()
            .all(|container| container.id.as_ref() != Some(&id)),
        "the database has to be stopped once it is dropped"
    );
}

#[test]
fn a_session_is_128_bits_as_lowercase_hexadecimal_digits_and_differs_every_time() {
    let session = new_session();

    assert_eq!(session.len(), 32, "{session}");
    assert!(
        session
            .chars()
            .all(|digit| digit.is_ascii_digit() || ('a'..='f').contains(&digit)),
        "{session}"
    );
    assert_ne!(new_session(), session);
}

#[tokio::test]
async fn labels_an_isolated_database_with_the_session_of_the_program() {
    let database = Database::isolated().await;

    assert!(
        id_of(&database).await.is_some(),
        "the container of the database carries the label of the session"
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

/// Runs the given test of this program as a program of its own, with the given
/// environment variable set, which turns the test on, and returns the session
/// that it prints.
fn session_printed_by(test: &str, variable: &str) -> String {
    let program = std::env::current_exe().expect("the test program can be found");
    let output = std::process::Command::new(program)
        .args([test, "--exact", "--ignored", "--nocapture"])
        .env(variable, "1")
        .output()
        .expect("the test program runs");
    let printed = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{printed}{}",
        String::from_utf8_lossy(&output.stderr)
    );

    printed
        .lines()
        .find_map(|line| line.strip_prefix("session: "))
        .unwrap_or_else(|| panic!("the session is printed: {printed}"))
        .to_owned()
}

/// Waits until Docker shows no container with the label of the given session
/// any longer, for 60 seconds at most, and returns the containers that are
/// left then, after removing them, so that the test leaves nothing behind even
/// if the code it checks does. The reaper waits a few seconds after the
/// program has gone, in case it connects again, and removes the containers
/// then. Docker tells whether they are gone, rather than a ping of their
/// address, which Docker Desktop may still answer for a while after they have
/// been removed.
async fn left_behind_by(session: &str) -> Vec<ContainerSummary> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut left_behind = containers_of(session).await;
    while !left_behind.is_empty() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        left_behind = containers_of(session).await;
    }
    remove(&left_behind).await;

    left_behind
}

/// The environment variable that turns the next test into the program whose
/// shared database the test after it watches being removed.
const PRINTS_THE_SESSION: &str = "ARCHITECTUREKIT_PRINTS_THE_SESSION";

#[tokio::test]
#[ignore = "run by removes_the_shared_database_once_the_program_ends"]
async fn prints_the_session_once_the_shared_database_has_started() {
    if std::env::var(PRINTS_THE_SESSION).is_err() {
        return;
    }

    let _ = Database::shared().await;
    assert!(
        !containers_of(&SESSION).await.is_empty(),
        "the shared database carries the label of the session"
    );
    println!("session: {}", *SESSION);
}

#[tokio::test]
async fn removes_the_shared_database_once_the_program_ends() {
    let session = session_printed_by(
        "testing::database::tests::prints_the_session_once_the_shared_database_has_started",
        PRINTS_THE_SESSION,
    );

    assert!(
        left_behind_by(&session).await.is_empty(),
        "the shared database has to be removed once its program has ended"
    );
}

/// The environment variable that turns the next test into a program that ends
/// while its shared database is still starting.
const ENDS_WHILE_THE_SHARED_DATABASE_STARTS: &str =
    "ARCHITECTUREKIT_ENDS_WHILE_THE_SHARED_DATABASE_STARTS";

#[tokio::test]
#[ignore = "run by leaves_nothing_behind_if_the_program_ends_while_the_shared_database_starts"]
async fn ends_while_the_shared_database_starts() {
    if std::env::var(ENDS_WHILE_THE_SHARED_DATABASE_STARTS).is_err() {
        return;
    }

    println!("session: {}", *SESSION);
    // The program ends as soon as Docker shows the container, which is long
    // before the database in it answers.
    let started = tokio::spawn(Database::shared());
    loop {
        if !containers_of(&SESSION).await.is_empty() {
            std::process::exit(0);
        }
        if started.is_finished() {
            let _ = started.await.expect("the shared database starts");
            panic!("the shared database has started without the label of the session");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn leaves_nothing_behind_if_the_program_ends_while_the_shared_database_starts() {
    let session = session_printed_by(
        "testing::database::tests::ends_while_the_shared_database_starts",
        ENDS_WHILE_THE_SHARED_DATABASE_STARTS,
    );

    assert!(
        left_behind_by(&session).await.is_empty(),
        "the shared database has to be removed, although its program ended while it was still starting"
    );
}
