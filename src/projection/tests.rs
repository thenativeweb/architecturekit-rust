// These tests check typed projections through Projected, which turns a stored
// event into what a projection gets. A projection names its events as an enum
// or as the stored event, and is resumable by its trait.

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use std::error::Error;

use crate::{
    Categorized, Category, Envelope, Event, Events, Failure, Permanent, Projected, ProjectionError,
    Revision, State, Subject, Transient, UpcastEvent, Upcasters, replay_stored,
};

#[derive(Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.book-acquired")]
struct BookAcquired {
    title: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.book-borrowed")]
struct BookBorrowed {
    reader: String,
}

#[derive(Debug, PartialEq, Events)]
enum BookEvent {
    Acquired(BookAcquired),
    Borrowed(BookBorrowed),
}

/// A stored event, as the database hands it out.
pub(crate) fn stored(id: &str, event_type: &str, data: Value) -> eventsourcingdb::Event {
    let mut event = json!({
        "specversion": "1.0",
        "id": id,
        "time": "2026-10-27T12:00:00Z",
        "source": "https://thenativeweb.io",
        "subject": "/books/42",
        "type": event_type,
        "datacontenttype": "application/json",
        "hash": "0".repeat(64),
        "predecessorhash": "0".repeat(64),
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    });
    event["data"] = data;

    serde_json::from_value(event).expect("a stored event is JSON")
}

#[test]
fn hands_over_the_metadata_and_the_decoded_data() {
    let event = stored("7", BookAcquired::EVENT_TYPE, json!({ "title": "2001" }));

    let envelopes = BookEvent::project(&event, None).expect("the event is projected");

    assert_eq!(
        envelopes,
        [Envelope {
            id: Revision::from(7),
            time: *event.time(),
            source: "https://thenativeweb.io".to_owned(),
            subject: Subject::new("/books/42"),
            event_type: BookAcquired::EVENT_TYPE.to_owned(),
            trace_parent: Some(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned()
            ),
            trace_state: None,
            part: 0,
            data: BookEvent::Acquired(BookAcquired {
                title: "2001".to_owned()
            }),
        }]
    );
}

#[test]
fn hands_each_event_its_variant() {
    let borrowed = stored("1", BookBorrowed::EVENT_TYPE, json!({ "reader": "Jane" }));

    let envelopes = BookEvent::project(&borrowed, None).expect("the event is projected");

    assert_eq!(
        envelopes[0].data,
        BookEvent::Borrowed(BookBorrowed {
            reader: "Jane".to_owned()
        })
    );
}

#[test]
fn leaves_out_events_of_types_without_a_variant() {
    let other = stored("1", "io.thenativeweb.test.reader-registered", json!({}));

    assert_eq!(
        BookEvent::project(&other, None).expect("the event is left out"),
        []
    );
}

#[test]
fn fails_permanently_on_data_that_does_not_match() {
    let event = stored("3", BookAcquired::EVENT_TYPE, json!({ "title": 42 }));

    let failure = BookEvent::project(&event, None).expect_err("the data does not match");

    assert_eq!(failure.category(), Some(Category::Permanent));
    assert_eq!(
        failure.to_string(),
        r#"permanent failure: decoding event 3 of type "io.thenativeweb.test.book-acquired""#
    );
}

static UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
    Upcasters::new()
        .upcast("io.thenativeweb.test.book-acquired.v1", |event| {
            Ok::<_, serde_json::Error>(vec![
                event
                    .with_type(BookAcquired::EVENT_TYPE)
                    .with_data(json!({ "title": "from the old shape" })),
            ])
        })
        .upcast("io.thenativeweb.test.book-acquired-and-borrowed", |event| {
            Ok::<_, serde_json::Error>(vec![
                UpcastEvent::clone(&event)
                    .with_type(BookAcquired::EVENT_TYPE)
                    .with_data(json!({ "title": "2001" })),
                event
                    .with_type(BookBorrowed::EVENT_TYPE)
                    .with_data(json!({ "reader": "Jane" })),
            ])
        })
        .upcast("io.thenativeweb.test.book-lost", |_| {
            Err::<Vec<UpcastEvent>, _>("this one cannot be migrated")
        })
});

#[test]
fn runs_the_upcasters() {
    let old = stored(
        "1",
        "io.thenativeweb.test.book-acquired.v1",
        json!({ "whatever": true }),
    );

    let envelopes = BookEvent::project(&old, Some(&UPCASTERS)).expect("the event is projected");

    assert_eq!(envelopes[0].event_type, BookAcquired::EVENT_TYPE);
    assert_eq!(
        envelopes[0].data,
        BookEvent::Acquired(BookAcquired {
            title: "from the old shape".to_owned()
        })
    );
}

#[test]
fn hands_out_every_event_an_upcaster_produces_with_its_part() {
    let split = stored(
        "5",
        "io.thenativeweb.test.book-acquired-and-borrowed",
        json!({}),
    );

    let envelopes = BookEvent::project(&split, Some(&UPCASTERS)).expect("the event is projected");

    let parts: Vec<(Revision, usize)> = envelopes
        .iter()
        .map(|envelope| (envelope.id, envelope.part))
        .collect();
    assert_eq!(parts, [(Revision::from(5), 0), (Revision::from(5), 1)]);
    assert!(matches!(envelopes[1].data, BookEvent::Borrowed(_)));
}

#[test]
fn reports_a_failing_upcaster_as_permanent() {
    let lost = stored("2", "io.thenativeweb.test.book-lost", json!({}));

    let failure = BookEvent::project(&lost, Some(&UPCASTERS)).expect_err("the upcaster fails");

    assert_eq!(failure.category(), Some(Category::Permanent));
}

#[derive(Clone, Debug, Default)]
struct Shelf {
    titles: Vec<String>,
}

impl State for Shelf {
    type Event = BookEvent;

    fn evolve(mut self, event: &BookEvent) -> Self {
        if let BookEvent::Acquired(acquired) = event {
            self.titles.push(acquired.title.clone());
        }
        self
    }

    fn upcasters() -> Option<&'static Upcasters> {
        Some(&UPCASTERS)
    }
}

#[test]
fn sees_what_a_state_with_the_same_upcasters_sees() {
    let history = [
        stored("1", "io.thenativeweb.test.book-acquired.v1", json!({})),
        stored(
            "2",
            "io.thenativeweb.test.book-acquired-and-borrowed",
            json!({}),
        ),
    ];

    let shelf: Shelf = replay_stored(
        history
            .iter()
            .map(|event| UpcastEvent::from_stored(event).expect("the ID is a revision")),
    )
    .expect("the state is replayed");
    let projected: Vec<String> = history
        .iter()
        .flat_map(|event| BookEvent::project(event, Some(&UPCASTERS)).expect("projected"))
        .filter_map(|envelope| match envelope.data {
            BookEvent::Acquired(acquired) => Some(acquired.title),
            BookEvent::Borrowed(_) => None,
        })
        .collect();

    assert_eq!(projected, shelf.titles);
}

#[test]
fn hands_a_projection_of_stored_events_every_event_as_it_is_stored() {
    let event = stored(
        "4",
        "io.thenativeweb.test.book-acquired.v1",
        json!({ "whatever": true }),
    );

    let envelopes =
        eventsourcingdb::Event::project(&event, Some(&UPCASTERS)).expect("the event is projected");

    assert_eq!(envelopes.len(), 1);
    assert_eq!(
        envelopes[0].data, event,
        "no upcaster runs, and the hash stays"
    );
    assert_eq!(
        envelopes[0].event_type,
        "io.thenativeweb.test.book-acquired.v1"
    );
}

#[test]
fn fails_permanently_for_an_event_whose_id_is_not_a_revision() {
    let event = stored("x", BookAcquired::EVENT_TYPE, json!({ "title": "2001" }));

    for (name, failure) in [
        (
            "an enum of events",
            BookEvent::project(&event, None).map(drop),
        ),
        (
            "stored events",
            eventsourcingdb::Event::project(&event, None).map(drop),
        ),
    ] {
        let failure = failure.expect_err(name);

        assert!(matches!(failure, Permanent::Other(_)), "{name}");
        assert_eq!(
            failure.to_string(),
            r#"permanent failure: event "x" on "/books/42" has an ID that is not a revision"#,
            "{name}"
        );
        assert_eq!(
            failure.source().map(ToString::to_string),
            Some(r#"not a revision: "x""#.to_owned()),
            "{name}"
        );
    }
}

#[test]
fn fails_permanently_for_an_event_whose_subject_is_not_one() {
    let mut event = serde_json::to_value(stored(
        "7",
        BookAcquired::EVENT_TYPE,
        json!({ "title": "2001" }),
    ))
    .expect("a stored event is JSON");
    event["subject"] = json!("books/42");
    let event: eventsourcingdb::Event =
        serde_json::from_value(event).expect("the client SDK reads it");

    for (name, failure) in [
        (
            "an enum of events",
            BookEvent::project(&event, None).map(drop),
        ),
        (
            "stored events",
            eventsourcingdb::Event::project(&event, None).map(drop),
        ),
    ] {
        let failure = failure.expect_err(name);

        assert!(matches!(failure, Permanent::Other(_)), "{name}");
        assert_eq!(
            failure.to_string(),
            r#"permanent failure: event "7" is on "books/42", which is not a subject"#,
            "{name}"
        );
        assert_eq!(
            failure.source().map(ToString::to_string),
            Some(r#"not a subject: "books/42", which does not start with a slash"#.to_owned()),
            "{name}"
        );
    }
}

#[test]
fn names_the_category_of_every_failure() {
    let failure = || Failure::new("failed", None);
    let panicked = ProjectionError::Panicked {
        message: "boom".to_owned(),
    };

    for (name, error, category, has_panicked) in [
        (
            "transient",
            ProjectionError::Transient(Transient::Other(failure())),
            Some(Category::Transient),
            false,
        ),
        (
            "permanent",
            ProjectionError::Permanent(Permanent::Other(failure())),
            Some(Category::Permanent),
            false,
        ),
        (
            "of the projection",
            ProjectionError::of(crate::DomainError::new("refused")),
            Some(Category::Domain),
            false,
        ),
        (
            "panicked",
            ProjectionError::Panicked {
                message: "boom".to_owned(),
            },
            Some(Category::Permanent),
            true,
        ),
        (
            "rolling back failed after a transient failure",
            ProjectionError::RollbackFailed {
                cause: Box::new(ProjectionError::Transient(Transient::Other(failure()))),
                error: Box::new(ProjectionError::Permanent(Permanent::Other(failure()))),
            },
            Some(Category::Transient),
            false,
        ),
        (
            "rolling back panicked after a transient failure",
            ProjectionError::RollbackFailed {
                cause: Box::new(ProjectionError::Transient(Transient::Other(failure()))),
                error: Box::new(panicked),
            },
            Some(Category::Permanent),
            true,
        ),
    ] {
        assert_eq!(error.category(), category, "{name}");
        assert_eq!(error.has_panicked(), has_panicked, "{name}");
    }
}

#[test]
fn reports_a_panic_with_its_message() {
    let text = ProjectionError::panicked(&"boom");
    let formatted = ProjectionError::panicked(&String::from("boom"));
    let other = ProjectionError::panicked(&42);

    assert_eq!(
        text.to_string(),
        "permanent failure: the projection panicked: boom"
    );
    assert_eq!(formatted.to_string(), text.to_string());
    assert_eq!(
        other.to_string(),
        "permanent failure: the projection panicked: a value that is no text"
    );
}
