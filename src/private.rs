//! What the derive macros expand to. Not part of the public API.

use std::cell::RefCell;

pub use serde_json::{Error as JsonError, Value};
use serde_json::{Map, json};

use crate::Event;

/// Writes the data of an event as JSON, for the derive of `Events`.
///
/// # Errors
///
/// Fails if serde can not write the data.
pub fn to_data<E: Event>(event: &E) -> Result<Value, JsonError> {
    serde_json::to_value(event)
}

/// Reads the data of an event from JSON, for the derive of `Events`.
///
/// # Errors
///
/// Fails if the data does not match the event.
pub fn from_data<E: Event>(data: Value) -> Result<E, JsonError> {
    serde_json::from_value(data)
}

/// Tells whether two strings are the same, at compile time, so that the derive
/// of `Events` can refuse two variants of the same event type while compiling.
#[must_use]
pub const fn same_str(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }

    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }

    true
}

thread_local! {
    /// The types whose schemas are being derived on this thread, innermost
    /// last, to tell a recursive type from one that merely nests others.
    static DERIVING: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

/// Derives the schema of the type named `type_name` with `describe`, and
/// panics if the type is already being derived, since then it is recursive.
#[must_use]
pub fn derive(type_name: &'static str, describe: impl FnOnce() -> Value) -> Value {
    struct Deriving;

    impl Drop for Deriving {
        fn drop(&mut self) {
            DERIVING.with_borrow_mut(|stack| {
                let _ = stack.pop();
            });
        }
    }

    let is_recursive = DERIVING.with_borrow_mut(|stack| {
        if stack.contains(&type_name) {
            return true;
        }
        stack.push(type_name);
        false
    });
    assert!(
        !is_recursive,
        "architecturekit: {type_name} is recursive, so its schema can not be derived; implement Schema for it by hand",
    );

    let _deriving = Deriving;
    describe()
}

/// Describes a struct as an object with the given fields, each with its name,
/// its schema, and whether it is optional, in the order serde writes them.
#[must_use]
pub fn object(type_name: &'static str, fields: Vec<(&'static str, Value, bool)>) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for (name, schema, is_optional) in fields {
        assert!(
            properties.insert(name.to_owned(), schema).is_none(),
            "architecturekit: {type_name} writes the field {name} more than once, so its schema can not be derived",
        );
        if !is_optional {
            required.push(Value::from(name));
        }
    }

    let mut schema = json!({
        "type": "object",
        "properties": properties,
        "additionalProperties": false,
    });
    if !required.is_empty() {
        schema["required"] = Value::Array(required);
    }

    schema
}
