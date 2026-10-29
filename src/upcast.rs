use std::{collections::HashMap, error::Error, fmt};

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{Failure, Permanent, Revision, revision::id_of};

#[cfg(test)]
mod tests;

/// Bounds a chain of upcasters, so that an upcaster which does not change the
/// event type fails loudly instead of looping forever.
const MAX_UPCAST_STEPS: usize = 16;

/// A stored event as upcasters see it: its type and its data, which an
/// upcaster changes, and its subject, ID, time and source, which it reads.
///
/// It is not an event of the client SDK, since an upcasted event is never
/// stored: it has no hash and no signature of its own, and it is never
/// written back.
#[derive(Clone, Debug, PartialEq)]
pub struct UpcastEvent {
    event_type: String,
    data: Value,
    subject: String,
    id: Revision,
    time: DateTime<Utc>,
    source: String,
}

impl UpcastEvent {
    /// Creates a stored event of the given type with the given data, with the
    /// ID 0, without a subject or a source, at the start of 1970. It is meant
    /// for tests of upcasters, together with
    /// [`replay_stored`](crate::replay_stored) and
    /// [`step_stored`](crate::step_stored).
    #[must_use]
    pub fn new(event_type: impl Into<String>, data: Value) -> Self {
        Self {
            event_type: event_type.into(),
            data,
            subject: String::new(),
            id: Revision::from(0),
            time: DateTime::UNIX_EPOCH,
            source: String::new(),
        }
    }

    /// Takes an event of the client SDK as stored, for the upcasters to see.
    /// An event whose ID is not a revision, which the database never writes,
    /// fails permanently.
    pub(crate) fn from_stored(event: &eventsourcingdb::Event) -> Result<Self, Permanent> {
        Ok(Self {
            event_type: event.ty().to_owned(),
            data: event.data().clone(),
            subject: event.subject().to_owned(),
            id: id_of(event)?,
            time: *event.time(),
            source: event.source().to_owned(),
        })
    }

    /// Returns the event with another type, as an upcaster does for the next
    /// version.
    #[must_use]
    pub fn with_type(mut self, event_type: impl Into<String>) -> Self {
        self.event_type = event_type.into();
        self
    }

    /// Returns the event with other data, as an upcaster does for the next
    /// version.
    #[must_use]
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = data;
        self
    }

    /// Returns the type of the event.
    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Returns the data of the event.
    #[must_use]
    pub fn data(&self) -> &Value {
        &self.data
    }

    /// Returns the data of the event, giving up the event.
    #[must_use]
    pub fn into_data(self) -> Value {
        self.data
    }

    /// Returns the subject the event was stored in.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the ID the event was stored under.
    #[must_use]
    pub fn id(&self) -> Revision {
        self.id
    }

    /// Returns the time the event was stored at.
    #[must_use]
    pub fn time(&self) -> &DateTime<Utc> {
        &self.time
    }

    /// Returns the source of the event.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
}

type Upcaster = Box<
    dyn Fn(UpcastEvent) -> Result<Vec<UpcastEvent>, Box<dyn Error + Send + Sync + 'static>>
        + Send
        + Sync,
>;

/// The set of upcasters for the event types of an application, which translate
/// stored events of an older type into a newer shape, before any state or
/// projection sees them.
///
/// Upcasting belongs to the event types, not to whoever reads them, so the set
/// is made once and shared: every state and every projection that reads these
/// events refers to it. That way the write side and the read side always see
/// the same events:
///
/// ```
/// use std::sync::LazyLock;
///
/// use architecturekit::Upcasters;
/// use serde_json::json;
///
/// static UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
///     Upcasters::new().upcast("io.eventsourcingdb.library.book-acquired.v1", |event| {
///         // v1 had no author.
///         let title = event.data()["title"].clone();
///
///         Ok::<_, std::convert::Infallible>(vec![
///             event
///                 .with_type("io.eventsourcingdb.library.book-acquired.v2")
///                 .with_data(json!({ "title": title, "author": "unknown" })),
///         ])
///     })
/// });
/// ```
#[derive(Default)]
pub struct Upcasters {
    by_type: HashMap<String, Upcaster>,
}

impl Upcasters {
    /// Creates an empty set of upcasters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Translates stored events of the given type into a newer shape, before
    /// any state or projection sees them. The result is never written back.
    ///
    /// Upcasters are chained: if the result carries a type that has an
    /// upcaster of its own, that one runs too, so only one step per version is
    /// needed instead of one per pair of versions. A chain ends after 16 steps:
    /// an event that still has an upcaster then makes reading fail permanently,
    /// which catches an upcaster that keeps its event type and would otherwise
    /// run forever. So does an error that an upcaster returns.
    ///
    /// # Panics
    ///
    /// Registering the same event type twice is a programming error, so this
    /// panics while the set is being built, rather than silently overwriting
    /// an upcaster.
    #[must_use]
    pub fn upcast<F, E>(mut self, from: impl Into<String>, upcast: F) -> Self
    where
        F: Fn(UpcastEvent) -> Result<Vec<UpcastEvent>, E> + Send + Sync + 'static,
        E: Into<Box<dyn Error + Send + Sync + 'static>>,
    {
        let from = from.into();
        assert!(
            !self.by_type.contains_key(&from),
            "architecturekit: event type {from:?} already has an upcaster",
        );

        let upcaster: Upcaster = Box::new(move |event| upcast(event).map_err(Into::into));
        let _ = self.by_type.insert(from, upcaster);

        self
    }

    /// Runs the chain for one stored event and returns what the states or
    /// projections should see. An event without an upcaster passes through
    /// untouched.
    pub(crate) fn apply(&self, event: UpcastEvent) -> Result<Vec<UpcastEvent>, Permanent> {
        self.apply_with_budget(event, MAX_UPCAST_STEPS)
    }

    fn apply_with_budget(
        &self,
        event: UpcastEvent,
        budget: usize,
    ) -> Result<Vec<UpcastEvent>, Permanent> {
        let Some(upcast) = self.by_type.get(&event.event_type) else {
            return Ok(vec![event]);
        };

        if budget == 0 {
            return Err(Permanent::Other(Failure::new(
                format!(
                    "upcasting {:?} exceeded {MAX_UPCAST_STEPS} steps, check for an upcaster that keeps its event type",
                    event.event_type,
                ),
                None,
            )));
        }

        let event_type = event.event_type.clone();
        let produced = upcast(event).map_err(|cause| {
            Permanent::Other(Failure::new(
                format!("upcasting {event_type:?}"),
                Some(cause),
            ))
        })?;

        let mut result = Vec::new();
        for next in produced {
            result.extend(self.apply_with_budget(next, budget - 1)?);
        }

        Ok(result)
    }
}

impl fmt::Debug for Upcasters {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut event_types: Vec<&String> = self.by_type.keys().collect();
        event_types.sort();

        formatter
            .debug_struct("Upcasters")
            .field("event_types", &event_types)
            .finish()
    }
}
