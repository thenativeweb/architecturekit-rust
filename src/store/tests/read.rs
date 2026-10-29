// These tests check reading: its bounds, the newest event first, reading from
// the latest event, IDs that are not revisions, and empty ranges against the
// database. They show that Store::read refuses what check_read_options refuses
// before it asks the database, whose own tests cover every range. Every test
// has a database of its own, so the IDs of its events start at "0".
//
// A stream that the caller drops stops reading, as a dropped future does, so
// "stops when the caller stops" reads part of a stream only.

use std::pin::pin;

use eventsourcingdb::{Event as StoredEvent, error::ClientError};
use futures_util::StreamExt;
use serde_json::json;

use super::{
    Incremented, Reset, cause_of,
    commands::SOURCE,
    database::Database,
    fake::{Answer, Api, Fake, Reply, dead_client},
};
use crate::{
    Categorized, Category, Event, EventOn, IfLatestEventIsMissing, LowerBound, Order, Permanent,
    Precondition, ReadError, ReadOptions, Revision, Store, Subject, Subjects, UpperBound,
    read::{DatabaseBounds, database_options},
};

const READ_EVENTS: &str = "/api/v1/read-events";

/// Writes increments by the given amounts to the subject, and returns their
/// IDs.
async fn write(store: &Store, subject: &str, amounts: &[i64]) -> Vec<String> {
    let subject = Subject::new(subject);
    let written = store
        .write(
            amounts
                .iter()
                .map(|by| EventOn::new(subject.clone(), &Incremented { by: *by }))
                .collect(),
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the events are written");

    written.iter().map(|event| event.id().to_owned()).collect()
}

async fn write_reset(store: &Store, subject: &str) -> String {
    let written = store
        .write(
            vec![EventOn::new(Subject::new(subject), &Reset {})],
            vec![Precondition::unconditionally()],
        )
        .await
        .expect("the reset is written");

    written[0].id().to_owned()
}

/// Reads all events the store hands out, or the failure it ends with.
async fn read(
    store: &Store,
    subjects: &Subjects,
    options: &ReadOptions,
) -> Result<Vec<StoredEvent>, ReadError> {
    let events = store.read(subjects, options).await?;
    let mut events = pin!(events);

    let mut read = Vec::new();
    while let Some(event) = events.next().await {
        read.push(event?);
    }

    Ok(read)
}

fn ids(events: &[StoredEvent]) -> Vec<&str> {
    events.iter().map(StoredEvent::id).collect()
}

fn bounded(lower_bound: LowerBound, upper_bound: UpperBound) -> ReadOptions {
    ReadOptions {
        lower_bound,
        upper_bound,
        ..ReadOptions::default()
    }
}

/// Reads an event ID that the database has written, or that a test spells
/// out, as a revision.
fn revision(id: &str) -> Revision {
    id.parse().expect("the ID is a revision")
}

fn after(id: &str) -> LowerBound {
    LowerBound::AfterEvent(revision(id))
}

fn from(id: &str) -> LowerBound {
    LowerBound::FromEvent(revision(id))
}

fn up_to(id: &str) -> UpperBound {
    UpperBound::UpToEvent(revision(id))
}

fn before(id: &str) -> UpperBound {
    UpperBound::BeforeEvent(revision(id))
}

fn from_latest_reset(subject: &str, if_missing: IfLatestEventIsMissing) -> LowerBound {
    LowerBound::FromLatestEvent {
        subject: Subject::new(subject),
        event_type: Reset::EVENT_TYPE.to_owned(),
        if_missing,
    }
}

// Reading.

#[tokio::test]
async fn reads_the_events_of_a_subject_in_order() {
    let database = Database::start().await;
    let store = database.store().await;
    let ids_written = write(&store, "/books/42", &[1, 2, 3]).await;

    let events = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &ReadOptions::default(),
    )
    .await
    .expect("the events are read");

    assert_eq!(ids(&events), ids_written);
    assert_eq!(events[0].data(), &json!({ "by": 1 }));
}

#[tokio::test]
async fn reads_an_exact_subject_without_the_subjects_below_it() {
    let database = Database::start().await;
    let store = database.store().await;
    let on_book = write(&store, "/books/42", &[1]).await;
    let _ = write(&store, "/books/42/notes", &[2]).await;

    let events = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &ReadOptions::default(),
    )
    .await
    .expect("the events are read");

    assert_eq!(ids(&events), on_book);
}

#[tokio::test]
async fn reads_a_subject_tree_with_the_subjects_below_it() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1]).await;
    let _ = write(&store, "/books/42/notes", &[2]).await;
    let _ = write(&store, "/readers/7", &[3]).await;

    let events = read(
        &store,
        &Subjects::tree(Subject::new("/books")),
        &ReadOptions::default(),
    )
    .await
    .expect("the events are read");

    let subjects: Vec<&str> = events.iter().map(StoredEvent::subject).collect();
    assert_eq!(subjects, ["/books/42", "/books/42/notes"]);
}

#[tokio::test]
async fn stops_when_the_caller_stops() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1, 2, 3]).await;

    let subjects = Subjects::exact(Subject::new("/books/42"));
    let options = ReadOptions::default();
    let first = store
        .read(&subjects, &options)
        .await
        .expect("the events are read")
        .take(1)
        .collect::<Vec<_>>()
        .await;

    assert_eq!(first.len(), 1);
    let all = read(&store, &subjects, &options)
        .await
        .expect("the store reads again");
    assert_eq!(all.len(), 3);
}

/// An event as EventSourcingDB hands it out through the v1 API, with a hash
/// that does not match its content.
const TAMPERED_EVENT: &str = r#"{"type":"event","payload":{"specversion":"1.0","id":"0","time":"2026-01-01T00:00:00Z","source":"https://thenativeweb.io","subject":"/books/42","type":"io.thenativeweb.test.incremented","datacontenttype":"application/json","data":{"by":1},"hash":"0000000000000000000000000000000000000000000000000000000000000001","predecessorhash":"0000000000000000000000000000000000000000000000000000000000000000"}}"#;

#[tokio::test]
async fn refuses_an_event_whose_hash_does_not_match() {
    let client = Fake::new(Api::V1)
        .route(
            READ_EVENTS,
            Answer::Reply(Reply::status(200, &format!("{TAMPERED_EVENT}\n"))),
        )
        .client()
        .await;
    let store = Store::builder(client, SOURCE).build();

    let failure = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &ReadOptions::default(),
    )
    .await
    .expect_err("the hash does not match");

    assert!(
        matches!(failure, ReadError::Permanent(Permanent::Unverified(_))),
        "{failure:?}"
    );
}

#[tokio::test]
async fn ends_with_the_first_failure() {
    let client = Fake::new(Api::V1)
        .route(
            READ_EVENTS,
            Answer::Reply(Reply::status(
                200,
                &format!("{TAMPERED_EVENT}\n{TAMPERED_EVENT}\n"),
            )),
        )
        .client()
        .await;
    let store = Store::builder(client, SOURCE).build();
    let subjects = Subjects::exact(Subject::new("/books/42"));
    let options = ReadOptions::default();

    let items = store
        .read(&subjects, &options)
        .await
        .expect("the read begins")
        .collect::<Vec<_>>()
        .await;

    assert_eq!(items.len(), 1, "{items:?}");
    assert!(items[0].is_err(), "{items:?}");
}

#[tokio::test]
async fn sorts_a_rejected_api_token_as_permanent() {
    let client = Fake::new(Api::V1)
        .route(
            READ_EVENTS,
            Answer::Reply(Reply::status(401, "unauthorized")),
        )
        .client()
        .await;
    let store = Store::builder(client, SOURCE).build();

    let failure = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &ReadOptions::default(),
    )
    .await
    .expect_err("the token is rejected");

    assert!(matches!(failure, ReadError::Permanent(_)), "{failure:?}");
    assert!(
        failure
            .to_string()
            .contains(r#"reading "/books/42": the database rejected the API token"#),
        "{failure}"
    );
}

#[tokio::test]
async fn sorts_an_unreachable_database_as_transient() {
    let store = Store::builder(dead_client().await, SOURCE).build();

    let failure = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &ReadOptions::default(),
    )
    .await
    .expect_err("the database can not be reached");

    assert_eq!(failure.category(), Some(Category::Transient), "{failure:?}");
}

#[tokio::test]
async fn sorts_an_answer_that_does_not_come_from_an_eventsourcingdb_as_transient() {
    // A proxy in front of the database answers on its behalf, for example
    // while the database restarts, whatever its status says, so trying again
    // may succeed.
    for status in [403, 404, 502] {
        let client = Fake::new(Api::V1)
            .route(
                READ_EVENTS,
                Answer::Reply(Reply {
                    is_from_eventsourcingdb: false,
                    ..Reply::status(status, "from a proxy")
                }),
            )
            .client()
            .await;
        let store = Store::builder(client, SOURCE).build();

        let failure = read(
            &store,
            &Subjects::exact(Subject::new("/books/42")),
            &ReadOptions::default(),
        )
        .await
        .expect_err("the answer does not come from an EventSourcingDB");

        assert!(
            matches!(failure, ReadError::Transient(_)),
            "{status}: {failure:?}"
        );
        assert!(
            failure.to_string().contains(
                r#"reading "/books/42": the answer does not come from an EventSourcingDB"#
            ),
            "{status}: {failure}"
        );
        assert!(
            matches!(cause_of(&failure), Some(ClientError::InvalidServerHeader)),
            "{status}: {failure:?}"
        );
    }
}

// Bounds.

#[tokio::test]
async fn reads_within_the_bounds() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1, 2, 3, 4, 5]).await;
    let subjects = Subjects::exact(Subject::new("/books/42"));

    for (name, options, expected) in [
        (
            "FromEvent includes the event with the given ID",
            bounded(from("1"), UpperBound::Unbounded),
            vec!["1", "2", "3", "4"],
        ),
        (
            "AfterEvent leaves out the event with the given ID",
            bounded(after("1"), UpperBound::Unbounded),
            vec!["2", "3", "4"],
        ),
        (
            "UpToEvent includes the event with the given ID",
            bounded(LowerBound::Unbounded, up_to("1")),
            vec!["0", "1"],
        ),
        (
            "BeforeEvent leaves out the event with the given ID",
            bounded(LowerBound::Unbounded, before("1")),
            vec!["0"],
        ),
        (
            "a lower and an upper bound read what lies between them",
            bounded(after("0"), before("4")),
            vec!["1", "2", "3"],
        ),
        (
            "an inclusive lower and upper bound read them as well",
            bounded(from("1"), up_to("3")),
            vec!["1", "2", "3"],
        ),
        (
            "the highest possible ID is an ID",
            bounded(LowerBound::Unbounded, up_to("9223372036854775807")),
            vec!["0", "1", "2", "3", "4"],
        ),
    ] {
        let events = read(&store, &subjects, &options).await.expect(name);

        assert_eq!(ids(&events), expected, "{name}");
    }
}

#[tokio::test]
async fn takes_a_bound_that_is_not_an_event_of_the_subjects_that_are_read() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1]).await;
    let other = write(&store, "/books/23", &[2]).await;
    let later = write(&store, "/books/42", &[3]).await;

    let events = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &bounded(from(&other[0]), UpperBound::Unbounded),
    )
    .await
    .expect("the events are read");

    assert_eq!(ids(&events), later);
}

#[tokio::test]
async fn refuses_options_that_check_read_options_refuses_before_the_database_is_asked() {
    let (url, requests) = Fake::new(Api::V1).start().await;
    let store = Store::builder(eventsourcingdb::Client::new(url, "secret"), SOURCE).build();
    let subjects = Subjects::exact(Subject::new("/books/42"));

    let empty = read(
        &store,
        &subjects,
        &bounded(LowerBound::Unbounded, before("0")),
    )
    .await
    .expect_err("no event can lie before 0");
    assert!(matches!(empty, ReadError::EmptyRange(_)), "{empty:?}");
    assert_eq!(
        empty.to_string(),
        r#"empty range: no event can lie before "0""#
    );
    assert_eq!(empty.category(), None);

    assert_eq!(requests.count(), 0, "the database is not asked");

    // Bounds that leave room for an event, and the latest event of a type,
    // which has no ID, go to the database.
    let _ = read(&store, &subjects, &bounded(from("0"), up_to("5")))
        .await
        .expect("the bounds are fine");
    let _ = read(
        &store,
        &subjects,
        &bounded(
            from_latest_reset("/books/42", IfLatestEventIsMissing::ReadEverything),
            UpperBound::Unbounded,
        ),
    )
    .await
    .expect("the latest event has no ID to check");
    assert!(requests.to(READ_EVENTS) >= 2, "the database is asked");
}

// Newest first.

#[tokio::test]
async fn hands_out_the_newest_event_first_within_the_bounds() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1, 2, 3]).await;
    let _ = write(&store, "/books/42/notes", &[4]).await;

    let newest_first = |lower_bound, upper_bound| ReadOptions {
        lower_bound,
        upper_bound,
        order: Order::NewestFirst,
    };

    let all = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &newest_first(LowerBound::Unbounded, UpperBound::Unbounded),
    )
    .await
    .expect("the events are read");
    assert_eq!(ids(&all), ["2", "1", "0"]);

    let bounded = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &newest_first(LowerBound::Unbounded, before("2")),
    )
    .await
    .expect("the events are read");
    assert_eq!(ids(&bounded), ["1", "0"], "the bounds stay what they are");

    let tree = read(
        &store,
        &Subjects::tree(Subject::new("/books")),
        &newest_first(LowerBound::Unbounded, UpperBound::Unbounded),
    )
    .await
    .expect("the events are read");
    assert_eq!(ids(&tree), ["3", "2", "1", "0"]);
}

// The latest event of a type.

#[tokio::test]
async fn reads_from_the_latest_event_of_the_type_and_includes_it() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1]).await;
    let _ = write_reset(&store, "/books/42").await;
    let _ = write(&store, "/books/42", &[2]).await;
    let latest = write_reset(&store, "/books/42").await;
    let after_it = write(&store, "/books/42", &[3]).await;

    let events = read(
        &store,
        &Subjects::exact(Subject::new("/books/42")),
        &bounded(
            from_latest_reset("/books/42", IfLatestEventIsMissing::ReadNothing),
            UpperBound::Unbounded,
        ),
    )
    .await
    .expect("the events are read");

    assert_eq!(ids(&events), [latest.as_str(), after_it[0].as_str()]);
}

#[tokio::test]
async fn reads_everything_or_nothing_if_the_latest_event_is_missing() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1, 2]).await;
    let subjects = Subjects::exact(Subject::new("/books/42"));

    let everything = read(
        &store,
        &subjects,
        &bounded(
            from_latest_reset("/books/42", IfLatestEventIsMissing::ReadEverything),
            UpperBound::Unbounded,
        ),
    )
    .await
    .expect("the events are read");
    assert_eq!(ids(&everything), ["0", "1"]);

    let up_to_one = read(
        &store,
        &subjects,
        &bounded(
            from_latest_reset("/books/42", IfLatestEventIsMissing::ReadEverything),
            up_to("0"),
        ),
    )
    .await
    .expect("the events are read");
    assert_eq!(ids(&up_to_one), ["0"], "up to the upper bound");

    let nothing = read(
        &store,
        &subjects,
        &bounded(
            from_latest_reset("/books/42", IfLatestEventIsMissing::ReadNothing),
            UpperBound::Unbounded,
        ),
    )
    .await
    .expect("the events are read");
    assert_eq!(ids(&nothing), Vec::<&str>::new());
}

#[tokio::test]
async fn looks_for_the_latest_event_on_the_given_subject_alone() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1]).await;
    let _ = write_reset(&store, "/books/42/notes").await;
    let _ = write(&store, "/books/42", &[2]).await;

    let events = read(
        &store,
        &Subjects::tree(Subject::new("/books/42")),
        &bounded(
            from_latest_reset("/books/42", IfLatestEventIsMissing::ReadNothing),
            UpperBound::Unbounded,
        ),
    )
    .await
    .expect("the events are read");

    assert_eq!(
        ids(&events),
        Vec::<&str>::new(),
        "a reset below the subject is not looked for"
    );
}

#[tokio::test]
async fn reads_a_subject_tree_from_the_latest_event_of_a_subject_in_it() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/23", &[1]).await;
    let latest = write_reset(&store, "/books/42").await;
    let after_it = write(&store, "/books/23", &[2]).await;

    let events = read(
        &store,
        &Subjects::tree(Subject::new("/books")),
        &bounded(
            from_latest_reset("/books/42", IfLatestEventIsMissing::ReadNothing),
            UpperBound::Unbounded,
        ),
    )
    .await
    .expect("the events are read");

    assert_eq!(ids(&events), [latest.as_str(), after_it[0].as_str()]);
}

#[tokio::test]
async fn goes_with_an_upper_bound_and_is_refused_if_the_latest_event_comes_after_it() {
    let database = Database::start().await;
    let store = database.store().await;
    let _ = write(&store, "/books/42", &[1]).await;
    let latest = write_reset(&store, "/books/42").await;
    let _ = write(&store, "/books/42", &[2, 3]).await;
    let subjects = Subjects::exact(Subject::new("/books/42"));
    let from_latest = || from_latest_reset("/books/42", IfLatestEventIsMissing::ReadNothing);

    let events = read(&store, &subjects, &bounded(from_latest(), up_to("2")))
        .await
        .expect("the events are read");
    assert_eq!(ids(&events), [latest.as_str(), "2"]);

    let failure = read(&store, &subjects, &bounded(from_latest(), before("1")))
        .await
        .expect_err("the latest event comes after the upper bound");
    assert!(matches!(failure, ReadError::Permanent(_)), "{failure:?}");
    assert!(
        matches!(cause_of(&failure), Some(ClientError::DbApiError(_))),
        "the database refused the read: {failure:?}"
    );
}

// Ranges against the database.

#[tokio::test]
async fn hands_out_no_events_for_a_range_that_is_only_empty_for_now_but_the_events_that_come_there_later()
 {
    let database = Database::start().await;
    let store = database.store().await;
    let written = write(&store, "/books/42", &[1, 2, 3, 4, 5]).await;
    let subjects = Subjects::exact(Subject::new("/books/42"));
    let after_the_last = bounded(after(&written[4]), UpperBound::Unbounded);

    let now = read(&store, &subjects, &after_the_last)
        .await
        .expect("events can still come after the last one");
    assert_eq!(ids(&now), Vec::<&str>::new());

    let later = write(&store, "/books/42", &[6]).await;
    let then = read(&store, &subjects, &after_the_last)
        .await
        .expect("the events are read");
    assert_eq!(ids(&then), later);
}

#[tokio::test]
async fn refuses_exactly_the_bounds_the_database_would_refuse() {
    // Without the check, the database answers bounds that leave no room for
    // an event with 400, so the kit has to refuse the same ones, and no
    // others. The largest revision is left out, since EventSourcingDB 1.2.0
    // counts past it.
    let database = Database::start().await;
    let store = database.store().await;
    let client = database.client().await;
    let _ = write(&store, "/books/42", &[1, 2, 3, 4, 5]).await;
    let subjects = Subjects::exact(Subject::new("/books/42"));

    let mut lower_bounds = vec![
        LowerBound::Unbounded,
        from_latest_reset("/books/42", IfLatestEventIsMissing::ReadEverything),
    ];
    let mut upper_bounds = vec![UpperBound::Unbounded];
    for id in ["0", "1", "2", "4"] {
        lower_bounds.extend([from(id), after(id)]);
        upper_bounds.extend([up_to(id), before(id)]);
    }

    for lower_bound in &lower_bounds {
        for upper_bound in &upper_bounds {
            let options = bounded(lower_bound.clone(), upper_bound.clone());

            let is_refused_by_the_kit = crate::check_read_options(&options).is_err();
            let bounds = DatabaseBounds::of(&options);
            let is_refused_by_the_database = match client
                .read_events(
                    subjects.subject().as_str(),
                    Some(database_options(&subjects, &options, &bounds)),
                )
                .await
            {
                Ok(events) => events.any(|event| async move { event.is_err() }).await,
                Err(_) => true,
            };

            assert_eq!(
                is_refused_by_the_kit, is_refused_by_the_database,
                "{options:?}"
            );
        }
    }
}
