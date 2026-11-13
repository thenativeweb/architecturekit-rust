// These tests check projection_fn, which turns a function into a projection:
// it applies every event with the function, hands on the error the function
// returns, takes typed events as well as stored ones, and brings along neither
// upcasters nor a record of what it has seen. Its type names the events first
// and the function last, as projection_fn does.

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use super::{ProjectionFn, projection_fn};
use crate::{DomainError, Envelope, Event, Events, Projection, Subject};

#[derive(Debug, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.book-acquired")]
struct BookAcquired {
    title: String,
}

#[derive(Debug, Events)]
enum BookEvent {
    Acquired(BookAcquired),
}

fn acquired(id: u32, title: &str) -> Envelope<BookEvent> {
    Envelope::new(
        Subject::new("/books/42"),
        id,
        BookEvent::Acquired(BookAcquired {
            title: title.to_owned(),
        }),
    )
}

#[tokio::test]
async fn applies_every_event_with_the_function() {
    let titles = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&titles);
    let projection = projection_fn(move |event: Envelope<BookEvent>| {
        let seen = Arc::clone(&seen);
        async move {
            let BookEvent::Acquired(acquired) = event.data;
            seen.lock()
                .expect("the titles are not poisoned")
                .push(acquired.title);
            Ok::<(), Infallible>(())
        }
    });

    let Ok(()) = projection.apply(acquired(0, "2001")).await;
    let Ok(()) = projection.apply(acquired(1, "2010")).await;

    assert_eq!(
        *titles.lock().expect("the titles are not poisoned"),
        ["2001", "2010"]
    );
}

#[tokio::test]
async fn hands_on_the_error_of_the_function() {
    let projection = projection_fn(|_: Envelope<BookEvent>| async {
        Err(DomainError::new("the shelf is full"))
    });

    let failure = projection
        .apply(acquired(0, "2001"))
        .await
        .expect_err("the function fails");

    assert_eq!(failure.to_string(), "the shelf is full");
}

#[test]
fn takes_the_events_as_they_are_stored() {
    fn assert_projection_of_stored_events<P>(_: &P)
    where
        P: Projection<Event = eventsourcingdb::Event>,
    {
    }

    let projection = projection_fn(|event: Envelope<eventsourcingdb::Event>| async move {
        let _ = (event.event_type, event.data);
        Ok::<(), Infallible>(())
    });

    assert_projection_of_stored_events(&projection);
}

#[test]
fn names_the_events_before_the_function() {
    struct Library<F> {
        catalog: ProjectionFn<BookEvent, F>,
    }

    let library = Library {
        catalog: projection_fn(|_: Envelope<BookEvent>| async { Ok::<(), Infallible>(()) }),
    };

    assert_eq!(format!("{:?}", library.catalog), "ProjectionFn { .. }");
}

#[test]
fn brings_along_no_upcasters() {
    fn upcasters_of<P: Projection>(_: &P) -> bool {
        P::upcasters().is_some()
    }

    let projection = projection_fn(|_: Envelope<BookEvent>| async { Ok::<(), Infallible>(()) });

    assert!(!upcasters_of(&projection), "a function has no upcasters");
}

#[test]
fn describes_itself_without_the_function() {
    let projection = projection_fn(|_: Envelope<BookEvent>| async { Ok::<(), Infallible>(()) });

    assert_eq!(format!("{projection:?}"), "ProjectionFn { .. }");
}
