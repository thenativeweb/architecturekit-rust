use std::any;

use serde_json::Value;

use crate::{Event, Failure, Permanent, UpcastEvent, Upcasters};

#[cfg(test)]
mod tests;

/// The events a state evolves by, as one enum with a variant per event type.
///
/// Derive it for an enum whose every variant holds exactly one [`Event`]:
///
/// ```
/// use architecturekit::{Event, Events};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Event)]
/// #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// struct BookAcquired {
///     title: String,
/// }
///
/// #[derive(Serialize, Deserialize, Event)]
/// #[event(type = "io.eventsourcingdb.library.book-borrowed")]
/// struct BookBorrowed {
///     reader: String,
/// }
///
/// #[derive(Events)]
/// enum BookEvent {
///     Acquired(BookAcquired),
///     Borrowed(BookBorrowed),
/// }
///
/// assert!(BookEvent::has_rule("io.eventsourcingdb.library.book-borrowed"));
/// ```
///
/// The enum is the closed set of event types the state knows. An event of any
/// other type in a subject the state reads makes reading fail, since it points
/// to a missing variant or a wrong subject, and a decider can only return
/// events the state knows, which the compiler checks. An event that belongs to
/// the subject but matters for no decision is a variant as well, whose arm in
/// [`State::evolve`] returns the state unchanged.
///
/// Two variants of the same event type fail to compile.
pub trait Events: Sized {
    /// The event types of the variants, in the order they are declared.
    const EVENT_TYPES: &'static [&'static str];

    /// Returns the schemas of the events of all variants, for registration
    /// with the database.
    fn schemas() -> Vec<EventSchema>;

    /// Returns the event type of this event.
    fn event_type(&self) -> &'static str;

    /// Encodes the data of this event as JSON.
    ///
    /// # Errors
    ///
    /// Fails if serde can not write the data.
    fn to_data(&self) -> Result<Value, serde_json::Error>;

    /// Decodes an event of the given type from its data, or returns `None` if
    /// the type is none of the variants.
    fn from_data(event_type: &str, data: Value) -> Option<Result<Self, serde_json::Error>>;

    /// Tells whether one of the variants has the given event type, so that the
    /// state can read events of that type. Upcasters play no part, since a
    /// decider returns events of current types: an older type that only an
    /// upcaster knows has no rule.
    #[must_use]
    fn has_rule(event_type: &str) -> bool {
        Self::EVENT_TYPES.contains(&event_type)
    }
}

/// The schema of an event type, for registration with the database.
#[derive(Clone, Debug, PartialEq)]
pub struct EventSchema {
    event_type: &'static str,
    schema: Value,
}

impl EventSchema {
    /// Returns the event type and the schema of the event `E`.
    #[must_use]
    pub fn of<E: Event>() -> Self {
        Self {
            event_type: E::EVENT_TYPE,
            schema: E::schema(),
        }
    }

    /// Returns the event type.
    #[must_use]
    pub fn event_type(&self) -> &'static str {
        self.event_type
    }

    /// Returns the JSON schema of the data of the event type.
    #[must_use]
    pub fn schema(&self) -> &Value {
        &self.schema
    }
}

/// The state a command decides on, together with the rule that builds it from
/// events.
///
/// It is deliberately not called a projection: a projection builds a read
/// model for the query side, whereas this is the write side, holding just
/// enough state for a decision. Every read starts from [`Default`], and
/// [`evolve`](State::evolve) takes the state one event further:
///
/// ```
/// # use architecturekit::{Event, Events};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, Event)]
/// # #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// # struct BookAcquired { title: String }
/// # #[derive(Serialize, Deserialize, Event)]
/// # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
/// # struct BookBorrowed { reader: String }
/// # #[derive(Events)]
/// # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
/// use architecturekit::{State, replay};
///
/// #[derive(Clone, Default)]
/// struct Book {
///     is_acquired: bool,
///     is_borrowed: bool,
/// }
///
/// impl State for Book {
///     type Event = BookEvent;
///
///     fn evolve(self, event: &BookEvent) -> Self {
///         match event {
///             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
///             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
///         }
///     }
/// }
///
/// let book: Book = replay([BookEvent::Acquired(BookAcquired { title: "2001".to_owned() })])?;
/// assert!(book.is_acquired);
/// # Ok::<(), architecturekit::Permanent>(())
/// ```
///
/// A state has one way of reading per type. To read the same data in two
/// ways, wrap it in a second type.
///
/// A state is a value that can be sent to another task and holds no
/// references, so that the store can cache it (see
/// [`StoreBuilder::state_cache`](crate::StoreBuilder::state_cache)), and a
/// decider can take it into a future that runs on any thread.
pub trait State: Default + Clone + Send + 'static {
    /// The events the state evolves by.
    type Event: Events;

    /// Takes the state one event further. It runs again every time the state
    /// is read, so it must not read the clock or anything else that changes:
    /// whatever a decision depends on belongs into the event.
    #[must_use]
    fn evolve(self, event: &Self::Event) -> Self;

    /// Returns the upcasters that the stored events run through before
    /// [`evolve`](State::evolve) sees them, so that events of an older type
    /// arrive in their current shape (see [`Upcasters`]). The default is none.
    #[must_use]
    fn upcasters() -> Option<&'static Upcasters> {
        None
    }

    /// Returns the event type from whose latest event onwards the state is
    /// read, instead of from the first event, so that deciding on a long
    /// stream does not read all of it again every time. If the subject has no
    /// event of that type, the state is read from the first event, as usual.
    /// The default is `None`.
    ///
    /// This yields the right state only if the arm of
    /// [`evolve`](State::evolve) for that type does not depend on the state
    /// before it, that is, if an event of that type carries everything the
    /// state needs from the events before it. The database looks for the type
    /// under which an event is stored, so a type that only an upcaster
    /// produces is not found.
    ///
    /// A type that is none of the variants of [`Event`](State::Event) is a
    /// programming error, so reading the state panics.
    #[must_use]
    fn from_latest() -> Option<&'static str> {
        None
    }
}

/// Returns the event type a state starts from, if any, after checking that the
/// state has a rule for it.
pub(crate) fn from_latest_of<S: State>() -> Option<&'static str> {
    let event_type = S::from_latest()?;
    assert!(
        S::Event::has_rule(event_type),
        "architecturekit: event type {event_type:?} has no rule on the state {}, so it can not start from it",
        any::type_name::<S>(),
    );

    Some(event_type)
}

/// Returns the index from which a history is read: the latest event of the
/// type the state starts from, or the first event.
fn start_of<T>(
    history: &[T],
    event_type_of: impl Fn(&T) -> &str,
    from_latest: Option<&str>,
) -> usize {
    from_latest
        .and_then(|from_latest| {
            history
                .iter()
                .rposition(|event| event_type_of(event) == from_latest)
        })
        .unwrap_or(0)
}

/// Folds a sequence of events into a state, starting from [`Default`]. It is
/// meant for tests, where the history is at hand as typed events.
///
/// Every event is written as JSON and read back before the state evolves by
/// it, exactly as executing a command and reading the state again would, so a
/// field that does not survive that, such as one serde skips, shows up in a
/// test rather than in production. If the state starts from the latest event
/// of a type (see [`State::from_latest`]), so does this.
///
/// # Errors
///
/// Fails permanently if an event can not be written or read back.
pub fn replay<S: State>(history: impl IntoIterator<Item = S::Event>) -> Result<S, Permanent> {
    let history: Vec<S::Event> = history.into_iter().collect();
    let start = start_of(&history, |event| event.event_type(), from_latest_of::<S>());

    history[start..]
        .iter()
        .try_fold(S::default(), |current, event| evolve_by(current, event))
}

/// Folds stored events into a state, running the upcasters on the way,
/// exactly as reading from the database would. It is meant for tests of
/// upcasters, which need the shape of an older version. It takes the events
/// as the database stores them, so that the upcasters see their subject, ID,
/// time and source. If the state starts from the latest event of a type (see
/// [`State::from_latest`]), so does this, by the type under which an event is
/// stored, as the database does.
///
/// With the feature `testing`, a test builds such events as `StoredEvent`s,
/// which turn into events of the client SDK with `into`:
///
/// ```
/// # use std::sync::LazyLock;
/// # use architecturekit::{Event, Events, State, Upcasters};
/// # use serde::{Deserialize, Serialize};
/// # use serde_json::json;
/// # #[derive(Serialize, Deserialize, Event)]
/// # #[event(type = "io.eventsourcingdb.library.book-acquired.v2")]
/// # struct BookAcquired { title: String, author: String }
/// # #[derive(Events)]
/// # enum BookEvent { Acquired(BookAcquired) }
/// # static UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
/// #     Upcasters::new().upcast("io.eventsourcingdb.library.book-acquired.v1", |event| {
/// #         let title = event.data()["title"].clone();
/// #         Ok::<_, std::convert::Infallible>(vec![
/// #             event
/// #                 .with_type("io.eventsourcingdb.library.book-acquired.v2")
/// #                 .with_data(json!({ "title": title, "author": "unknown" })),
/// #         ])
/// #     })
/// # });
/// # #[derive(Clone, Default)]
/// # struct Book { author: String }
/// # impl State for Book {
/// #     type Event = BookEvent;
/// #     fn evolve(self, event: &BookEvent) -> Self {
/// #         let BookEvent::Acquired(acquired) = event;
/// #         Book { author: acquired.author.clone() }
/// #     }
/// #     fn upcasters() -> Option<&'static Upcasters> { Some(&UPCASTERS) }
/// # }
/// # #[cfg(feature = "testing")]
/// # {
/// use architecturekit::{Subject, replay_stored, testing::StoredEvent};
///
/// // v1 had no author.
/// let book: Book = replay_stored([StoredEvent::raw(
///     Subject::new("/books/42"),
///     0,
///     "io.eventsourcingdb.library.book-acquired.v1",
///     json!({ "title": "2001" }),
/// )
/// .into()])?;
/// assert_eq!(book.author, "unknown");
/// # }
/// # Ok::<(), architecturekit::Permanent>(())
/// ```
///
/// # Errors
///
/// Fails permanently if an upcaster fails, if an event has a type the state
/// has no rule for, if its data does not match its type, or if its ID or
/// subject is not one, neither of which the database ever writes.
pub fn replay_stored<S: State>(
    history: impl IntoIterator<Item = eventsourcingdb::Event>,
) -> Result<S, Permanent> {
    let history: Vec<eventsourcingdb::Event> = history.into_iter().collect();
    let start = start_of(&history, eventsourcingdb::Event::ty, from_latest_of::<S>());

    history[start..]
        .iter()
        .try_fold(S::default(), |current, stored| {
            evolve_by_stored(current, UpcastEvent::from_stored(stored)?)
        })
}

/// Takes a state a single event further, by the same rules as [`replay`]. It
/// starts from the given state instead of [`Default`], and leaves that as it
/// is, so that the state before and after the event are both at hand, for
/// example for a history that tells what an event changed.
///
/// # Errors
///
/// Fails permanently if the event can not be written or read back.
pub fn step<S: State>(current: &S, event: &S::Event) -> Result<S, Permanent> {
    evolve_by(current.clone(), event)
}

/// Takes a state a single stored event further, running the upcasters on the
/// way, exactly as reading from the database would. It takes the event as the
/// database stores it, such as one that a projection of stored events gets,
/// so that the upcasters see its subject, ID, time and source. If the
/// upcasters turn the event into several, the state evolves by all of them.
/// Otherwise, it behaves like [`step`].
///
/// A projection that tells what each event changed keeps the state before an
/// event, and steps it. The projection gets the events of every subject, but
/// the state has rules only for the events of its own subjects, and an event
/// of a type without a rule makes `step_stored` fail, and with it the
/// projection. So it steps only the events of the subjects of the state, which
/// a [`SubjectScheme`](crate::SubjectScheme) tells apart from the others.
/// Asking [`Events::has_rule`] instead would skip events of an older type as
/// well, since it knows nothing of upcasters:
///
/// ```
/// use std::{collections::HashMap, future, sync::{LazyLock, Mutex}};
///
/// use architecturekit::{
///     Envelope, Events, State, SubjectScheme, eventsourcingdb, projection_fn, step_stored,
/// };
/// # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
/// # struct BookBorrowed {}
/// # #[derive(Events)]
/// # enum BookEvent { Borrowed(BookBorrowed) }
/// # #[derive(Clone, Default)]
/// # struct Book { is_borrowed: bool }
/// # impl State for Book {
/// #     type Event = BookEvent;
/// #     fn evolve(self, _: &BookEvent) -> Self { Book { is_borrowed: true } }
/// # }
///
/// static BOOKS: LazyLock<SubjectScheme> = LazyLock::new(|| SubjectScheme::new("/books/{book_id}"));
///
/// let books: Mutex<HashMap<String, Book>> = Mutex::default();
/// let history = projection_fn(move |event: Envelope<eventsourcingdb::Event>| {
///     let Some(book_id) = BOOKS.value(&event.subject, "book_id") else {
///         // An event of another subject, such as one of a reader.
///         return future::ready(Ok(()));
///     };
///
///     let mut books = books.lock().unwrap();
///     let before = books.get(book_id).cloned().unwrap_or_default();
///     let after = step_stored(&before, &event.data).map(|after| {
///         if !before.is_borrowed && after.is_borrowed {
///             // The book was borrowed by this event.
///         }
///         books.insert(book_id.to_owned(), after);
///     });
///
///     future::ready(after)
/// });
/// ```
///
/// # Errors
///
/// Fails permanently if an upcaster fails, if an event has a type the state
/// has no rule for, if its data does not match its type, or if its ID or
/// subject is not one, neither of which the database ever writes.
pub fn step_stored<S: State>(current: &S, stored: &eventsourcingdb::Event) -> Result<S, Permanent> {
    evolve_by_stored(current.clone(), UpcastEvent::from_stored(stored)?)
}

/// Takes a state further by a typed event, after writing it as JSON and
/// reading it back.
fn evolve_by<S: State>(current: S, event: &S::Event) -> Result<S, Permanent> {
    let event_type = event.event_type();
    let data = event.to_data().map_err(|cause| {
        Permanent::Other(Failure::new(
            format!("encoding {event_type:?}"),
            Some(Box::new(cause)),
        ))
    })?;
    let decoded = decode::<S::Event>(event_type, data)?;

    Ok(current.evolve(&decoded))
}

/// Takes a state further by a stored event, after running it through the
/// upcasters of the state.
pub(crate) fn evolve_by_stored<S: State>(current: S, stored: UpcastEvent) -> Result<S, Permanent> {
    let upcasted = match S::upcasters() {
        Some(upcasters) => upcasters.apply(stored)?,
        None => vec![stored],
    };

    upcasted.into_iter().try_fold(current, |current, event| {
        let event_type = event.event_type().to_owned();
        let decoded = decode::<S::Event>(&event_type, event.into_data())?;
        Ok(current.evolve(&decoded))
    })
}

/// Reads an event of the given type from its data.
fn decode<E: Events>(event_type: &str, data: Value) -> Result<E, Permanent> {
    match E::from_data(event_type, data) {
        None => Err(Permanent::Other(Failure::new(
            format!("no rule for event type {event_type:?}"),
            None,
        ))),
        Some(Err(cause)) => Err(Permanent::Other(Failure::new(
            format!("decoding {event_type:?}"),
            Some(Box::new(cause)),
        ))),
        Some(Ok(event)) => Ok(event),
    }
}
