use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    rc::Rc,
    sync::Arc,
};

use chrono::{DateTime, TimeZone};
use serde_json::{Value, json};

#[cfg(test)]
mod tests;

/// The JSON schema of a type, which describes exactly what serde writes for it.
///
/// The store registers the schema of every event with EventSourcingDB, which
/// from then on rejects events whose data does not match it. A registered
/// schema can not change, so the rules by which a schema is derived do not
/// change either: a type derives the same schema in every version of the
/// kit.
///
/// [`Event`](crate::Event) derives the schema of an event. Derive `Schema` for
/// a type within the data of an event, such as a nested struct:
///
/// ```
/// use architecturekit::{Event, Schema};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Schema)]
/// struct Address {
///     city: String,
/// }
///
/// #[derive(Serialize, Deserialize, Event)]
/// #[event(type = "io.eventsourcingdb.library.reader-registered")]
/// #[serde(rename_all = "camelCase")]
/// struct ReaderRegistered {
///     full_name: String,
///     address: Address,
///     #[serde(skip_serializing_if = "Option::is_none")]
///     nickname: Option<String>,
/// }
///
/// assert_eq!(
///     ReaderRegistered::schema(),
///     serde_json::json!({
///         "type": "object",
///         "properties": {
///             "fullName": { "type": "string" },
///             "address": {
///                 "type": "object",
///                 "properties": { "city": { "type": "string" } },
///                 "required": ["city"],
///                 "additionalProperties": false
///             },
///             "nickname": { "type": "string" }
///         },
///         "required": ["fullName", "address"],
///         "additionalProperties": false
///     })
/// );
/// ```
///
/// The rules:
///
/// - A struct with named fields is an object with the fields serde writes:
///   named as `rename` and `rename_all` say, and without the fields marked with
///   `skip` or `skip_serializing`. A field with `skip_serializing_if` is
///   optional, all others are required, and no other fields are allowed. A
///   newtype struct, and a struct marked with `transparent`, is described like
///   the value it wraps.
/// - A `bool` is a boolean, an integer type an integer, an `f32` or an `f64` a
///   number, and a `String`, a `str` or a `char` a string.
/// - A `Vec`, a `VecDeque`, a slice, a `HashSet` and a `BTreeSet` is an array,
///   and an array `[T; N]` an array of exactly `N` items. A `HashMap` and a
///   `BTreeMap` is an object whose values all have the same schema.
/// - An `Option` may also be null, unless it is the type of an optional field,
///   which serde leaves out instead of writing null. A `Box`, an `Rc`, an
///   `Arc`, a `Cow` and a reference are described like what they hold.
/// - A chrono `DateTime` is a string in the date-time format, a
///   `serde_json::Number` a number, and a `serde_json::Value` allows any value.
///
/// A value that may be null is an `Option`, such as an `Option<Vec<_>>` or an
/// `Option<HashMap<_, _>>`, since serde never writes a `Vec` or a `HashMap` as
/// null. A `Vec<u8>` is an array of integers, since that is what serde writes
/// for it.
///
/// The schema of an enum, a union, a unit struct, a tuple struct with several
/// fields, a struct marked with `tag`, `content`, `into` or `remote`, and a
/// field marked with `flatten`, `serialize_with` or `with` can not be derived,
/// since serde writes them differently than their types describe. Deriving it
/// fails to compile; implement `Schema` by hand instead. The same applies to a
/// recursive type, whose schema panics on deriving it.
///
/// EventSourcingDB currently reads every number in the data of an event as a
/// 64-bit float, which holds an integer exactly only if its magnitude is at
/// most 2^53-1. A larger integer is stored changed, without an error. To keep a
/// 64-bit integer exact, write it as a string, in a type that describes itself
/// as a string.
///
/// Implementing `Schema` by hand is how a type describes itself further, for
/// example with a pattern:
///
/// ```
/// use architecturekit::Schema;
/// use serde::{Deserialize, Serialize};
/// use serde_json::{Value, json};
///
/// #[derive(Serialize, Deserialize)]
/// struct Country(String);
///
/// impl Schema for Country {
///     fn schema() -> Value {
///         json!({ "type": "string", "pattern": "^[A-Z]{2}$" })
///     }
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not describe its JSON schema",
    note = "derive `Schema` for it, or implement `Schema` by hand"
)]
pub trait Schema {
    /// Returns the JSON schema of the type.
    #[must_use]
    fn schema() -> Value;

    /// Returns the JSON schema of the type as the type of an optional field,
    /// which serde leaves out instead of writing null. It is the same as
    /// [`schema`](Schema::schema), except for an [`Option`], which does not
    /// allow null then.
    #[must_use]
    fn schema_without_null() -> Value {
        Self::schema()
    }
}

macro_rules! described_as {
    ($schema:tt => $($described:ty),+ $(,)?) => {
        $(
            impl Schema for $described {
                fn schema() -> Value {
                    json!($schema)
                }
            }
        )+
    };
}

described_as!({ "type": "boolean" } => bool);
described_as!({ "type": "string" } => String, str, char);
described_as!({ "type": "integer" } => i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);
described_as!({ "type": "number" } => f32, f64, serde_json::Number);
described_as!({} => Value);

impl<Tz: TimeZone> Schema for DateTime<Tz> {
    fn schema() -> Value {
        json!({ "type": "string", "format": "date-time" })
    }
}

impl<T: Schema> Schema for Option<T> {
    fn schema() -> Value {
        json!({ "anyOf": [T::schema(), { "type": "null" }] })
    }

    fn schema_without_null() -> Value {
        T::schema()
    }
}

fn array_of<T: Schema>() -> Value {
    json!({ "type": "array", "items": T::schema() })
}

macro_rules! described_as_array {
    ($($collection:ident),+) => {
        $(
            impl<T: Schema> Schema for $collection<T> {
                fn schema() -> Value {
                    array_of::<T>()
                }
            }
        )+
    };
}

described_as_array!(Vec, VecDeque, BTreeSet);

impl<T: Schema, S> Schema for HashSet<T, S> {
    fn schema() -> Value {
        array_of::<T>()
    }
}

impl<T: Schema> Schema for [T] {
    fn schema() -> Value {
        array_of::<T>()
    }
}

impl<T: Schema, const N: usize> Schema for [T; N] {
    fn schema() -> Value {
        json!({ "type": "array", "items": T::schema(), "minItems": N, "maxItems": N })
    }
}

fn map_of<V: Schema>() -> Value {
    json!({ "type": "object", "additionalProperties": V::schema() })
}

impl<K, V: Schema, S> Schema for HashMap<K, V, S> {
    fn schema() -> Value {
        map_of::<V>()
    }
}

impl<K, V: Schema> Schema for BTreeMap<K, V> {
    fn schema() -> Value {
        map_of::<V>()
    }
}

impl Schema for serde_json::Map<String, Value> {
    fn schema() -> Value {
        json!({ "type": "object", "additionalProperties": {} })
    }
}

macro_rules! described_like_what_it_holds {
    ($($holder:ident),+) => {
        $(
            impl<T: Schema + ?Sized> Schema for $holder<T> {
                fn schema() -> Value {
                    T::schema()
                }

                fn schema_without_null() -> Value {
                    T::schema_without_null()
                }
            }
        )+
    };
}

described_like_what_it_holds!(Box, Rc, Arc);

impl<T: Schema + ?Sized> Schema for &T {
    fn schema() -> Value {
        T::schema()
    }

    fn schema_without_null() -> Value {
        T::schema_without_null()
    }
}

impl<T: Schema + ToOwned + ?Sized> Schema for Cow<'_, T> {
    fn schema() -> Value {
        T::schema()
    }

    fn schema_without_null() -> Value {
        T::schema_without_null()
    }
}
