use serde::{Serialize, de::DeserializeOwned};

use crate::Schema;

/// The data of an event, which describes what has happened.
///
/// An event names its event type, and describes its data with a JSON schema,
/// which the store registers with EventSourcingDB, so that the database
/// rejects events whose data does not match it (see [`Schema`]).
///
/// Derive it, together with serde's `Serialize` and `Deserialize`, and name
/// the event type with the `event` attribute:
///
/// ```
/// use architecturekit::Event;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Event)]
/// #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// struct BookAcquired {
///     title: String,
///     author: String,
///     isbn: String,
/// }
///
/// assert_eq!(BookAcquired::EVENT_TYPE, "io.eventsourcingdb.library.book-acquired");
/// ```
///
/// The derive also derives [`Schema`] for the event, from the fields that serde
/// writes. An event whose schema can not be derived, or that describes its
/// schema differently, implements both traits by hand instead.
///
/// An event type names exactly one shape of data, which is why an event can
/// not be generic:
///
/// ```compile_fail
/// use architecturekit::Event;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Event)]
/// #[event(type = "io.eventsourcingdb.library.item-added")]
/// struct ItemAdded<T> {
///     item: T,
/// }
/// ```
pub trait Event: Serialize + DeserializeOwned + Schema {
    /// The type of the event, such as
    /// `io.eventsourcingdb.library.book-acquired`.
    const EVENT_TYPE: &'static str;
}
