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
/// writes. To say more than the fields can, such as that at least one of them
/// is there, name a function with `schema_with`, which gets the derived schema
/// and returns the one that is registered:
///
/// ```
/// use architecturekit::{Event, Schema};
/// use serde::{Deserialize, Serialize};
/// use serde_json::{Value, json};
///
/// #[derive(Serialize, Deserialize, Event)]
/// #[event(
///     type = "io.eventsourcingdb.library.book-corrected",
///     schema_with = "at_least_one_field"
/// )]
/// struct BookCorrected {
///     #[serde(skip_serializing_if = "Option::is_none")]
///     title: Option<String>,
///     #[serde(skip_serializing_if = "Option::is_none")]
///     author: Option<String>,
/// }
///
/// fn at_least_one_field(mut derived: Value) -> Value {
///     derived["minProperties"] = json!(1);
///     derived
/// }
///
/// assert_eq!(BookCorrected::schema()["minProperties"], 1);
/// ```
///
/// A function that leaves the derived schema aside replaces it, for example
/// with one read from a file with `include_str!`. An event whose schema can
/// not be derived at all implements both traits by hand instead.
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
