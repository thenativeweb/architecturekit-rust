// These tests mirror TestDeriveSchema, TestDeriveSchemaPanics, TestSchemaOf
// and TestDeriveSchemaAgreesWithEncodingJSON of ArchitectureKit for Go, under
// the same sentences, and expect the same schemas for equivalent types. The
// cases that describe what only Go has do not apply to Rust:
//
// - "makes a field with the string option a string": serde has no string
//   option, and the schema of a field marked with serialize_with can not be
//   derived.
// - "describes a []byte as a string, which may be null": serde writes a
//   Vec<u8> as an array of integers (see
//   describes_a_vec_of_bytes_as_an_array_of_integers).
// - "describes a type with MarshalText as a string", "describes a type with
//   AppendText as a string", and "describes a slice that encodes itself as
//   text as a string, which is never null": a type that writes itself as text
//   implements Schema by hand.
// - "describes a json.Number as a number, and as a string with the string
//   option": see describes_a_serde_json_number_as_a_number.
// - "allows any value for an interface with a Schema function": serde can not
//   write a trait object.
// - "describes a field type by a Schema function with a pointer receiver":
//   Rust has no pointer receivers.
// - "leaves out an embedded unexported type that is not a struct", "lifts the
//   fields of an embedded struct", "makes the fields of an embedded pointer
//   optional", "keeps an embedded struct with a tag as a field", "lets the
//   least deeply embedded field win", "lets the only tagged field win among
//   equally deep ones", "leaves out a name that equally deep fields share", and
//   "leaves out the fields of a struct embedded twice at the same depth", and
//   their counterparts in TestDeriveSchemaAgreesWithEncodingJSON: Rust has no
//   embedding, and the schema of a field marked with flatten can not be
//   derived (see the refusals, which are doctests).
// - "does not call the own Schema function of the type itself": a Rust type
//   has exactly one Schema.
// - "returns a schema that belongs to the caller" and "returns the shape
//   encoding/json decodes into": schema returns a serde_json::Value of its own.
// - TestDeriveSchemaPanics, except for recursive types: what Go refuses at run
//   time, the derive macros refuse at compile time (see the refusals), or
//   serde can not write in the first place.

// Most types here exist only for their schemas, so they are never built.
#![allow(dead_code)]

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    rc::Rc,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Event, Schema};

/// Asserts that a schema is the expected one, which compares the order of
/// `required` and `anyOf` as well, but not the order of object keys, since the
/// database compares schemas by their JSON form with sorted keys.
fn assert_schema<T: Schema + ?Sized>(expected: &str) {
    let expected: Value = serde_json::from_str(expected).expect("the expected schema is JSON");
    assert_eq!(T::schema(), expected);
}

#[derive(Serialize, Deserialize)]
struct Country(String);

impl Schema for Country {
    fn schema() -> Value {
        json!({ "type": "string", "pattern": "^[A-Z]{2}$" })
    }
}

#[test]
fn describes_a_struct_as_an_object_with_its_fields() {
    #[derive(Serialize, Schema)]
    struct Book {
        title: String,
        pages: i64,
    }

    assert_schema::<Book>(
        r#"{"additionalProperties":false,"properties":{"pages":{"type":"integer"},"title":{"type":"string"}},"required":["title","pages"],"type":"object"}"#,
    );
}

#[test]
fn describes_the_basic_types() {
    #[derive(Serialize, Schema)]
    struct Basics {
        flag: bool,
        small: i8,
        big: u64,
        ratio: f32,
        amount: f64,
    }

    assert_schema::<Basics>(
        r#"{"additionalProperties":false,"properties":{"amount":{"type":"number"},"big":{"type":"integer"},"flag":{"type":"boolean"},"ratio":{"type":"number"},"small":{"type":"integer"}},"required":["flag","small","big","ratio","amount"],"type":"object"}"#,
    );
}

#[test]
fn names_a_field_without_a_rename_like_the_field() {
    #[derive(Serialize, Schema)]
    struct Item {
        title: String,
        r#type: String,
    }

    assert_schema::<Item>(
        r#"{"additionalProperties":false,"properties":{"title":{"type":"string"},"type":{"type":"string"}},"required":["title","type"],"type":"object"}"#,
    );
}

#[test]
fn names_fields_as_rename_and_rename_all_say() {
    #[derive(Serialize, Schema)]
    #[serde(rename_all = "camelCase")]
    struct Reader {
        full_name: String,
        #[serde(rename = "e-mail")]
        email_address: String,
        #[serde(rename(serialize = "joinedAt", deserialize = "joined"))]
        joined: String,
    }

    assert_schema::<Reader>(
        r#"{"additionalProperties":false,"properties":{"e-mail":{"type":"string"},"fullName":{"type":"string"},"joinedAt":{"type":"string"}},"required":["fullName","e-mail","joinedAt"],"type":"object"}"#,
    );
}

#[test]
fn leaves_out_the_fields_serde_skips() {
    #[derive(Serialize, Schema)]
    struct Item {
        title: String,
        #[serde(skip)]
        internal: String,
        #[serde(skip_serializing)]
        hidden: String,
    }

    assert_schema::<Item>(
        r#"{"additionalProperties":false,"properties":{"title":{"type":"string"}},"required":["title"],"type":"object"}"#,
    );
}

#[test]
fn makes_fields_with_skip_serializing_if_optional() {
    #[derive(Serialize, Schema)]
    struct Book {
        title: String,
        #[serde(skip_serializing_if = "String::is_empty")]
        subtitle: String,
        #[serde(skip_serializing_if = "is_zero")]
        edition: i64,
    }

    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde passes a reference"
    )]
    fn is_zero(value: &i64) -> bool {
        *value == 0
    }

    assert_schema::<Book>(
        r#"{"additionalProperties":false,"properties":{"edition":{"type":"integer"},"subtitle":{"type":"string"},"title":{"type":"string"}},"required":["title"],"type":"object"}"#,
    );
}

#[test]
fn leaves_out_required_for_an_object_without_required_fields() {
    #[derive(Serialize, Schema)]
    struct Note {
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    }

    assert_schema::<Note>(
        r#"{"additionalProperties":false,"properties":{"text":{"type":"string"}},"type":"object"}"#,
    );
}

#[test]
fn describes_an_empty_struct_as_an_object_without_fields() {
    #[derive(Serialize, Schema)]
    struct Empty {}

    assert_schema::<Empty>(r#"{"additionalProperties":false,"properties":{},"type":"object"}"#);
}

#[test]
fn describes_a_vec_as_an_array_which_is_never_null() {
    #[derive(Serialize, Schema)]
    struct Book {
        tags: Vec<String>,
    }

    assert_schema::<Book>(
        r#"{"additionalProperties":false,"properties":{"tags":{"items":{"type":"string"},"type":"array"}},"required":["tags"],"type":"object"}"#,
    );
}

#[test]
fn describes_an_option_of_a_vec_as_an_array_which_may_be_null() {
    #[derive(Serialize, Schema)]
    struct Book {
        tags: Option<Vec<String>>,
    }

    assert_schema::<Book>(
        r#"{"additionalProperties":false,"properties":{"tags":{"anyOf":[{"items":{"type":"string"},"type":"array"},{"type":"null"}]}},"required":["tags"],"type":"object"}"#,
    );
}

#[test]
fn never_allows_null_for_an_optional_option() {
    #[derive(Serialize, Schema)]
    struct Book {
        #[serde(skip_serializing_if = "Option::is_none")]
        tags: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        counts: Option<HashMap<String, i64>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        checked: Option<DateTime<Utc>>,
    }

    assert_schema::<Book>(
        r#"{"additionalProperties":false,"properties":{"checked":{"format":"date-time","type":"string"},"counts":{"additionalProperties":{"type":"integer"},"type":"object"},"note":{"type":"string"},"tags":{"items":{"type":"string"},"type":"array"}},"type":"object"}"#,
    );
}

#[test]
fn removes_one_null_for_an_optional_option_of_an_option() {
    #[derive(Serialize, Schema)]
    struct Book {
        #[serde(skip_serializing_if = "Option::is_none")]
        #[expect(clippy::option_option, reason = "this is what the test is about")]
        note: Option<Option<String>>,
    }

    assert_schema::<Book>(
        r#"{"additionalProperties":false,"properties":{"note":{"anyOf":[{"type":"string"},{"type":"null"}]}},"type":"object"}"#,
    );
}

#[test]
fn describes_a_vec_of_bytes_as_an_array_of_integers() {
    #[derive(Serialize, Schema)]
    struct Blob {
        data: Vec<u8>,
    }

    assert_schema::<Blob>(
        r#"{"additionalProperties":false,"properties":{"data":{"items":{"type":"integer"},"type":"array"}},"required":["data"],"type":"object"}"#,
    );
}

#[test]
fn describes_an_array_as_an_array_of_exactly_its_length() {
    #[derive(Serialize, Schema)]
    struct Results {
        scores: [i64; 3],
    }

    assert_schema::<Results>(
        r#"{"additionalProperties":false,"properties":{"scores":{"items":{"type":"integer"},"maxItems":3,"minItems":3,"type":"array"}},"required":["scores"],"type":"object"}"#,
    );
}

#[test]
fn describes_sets_and_other_sequences_as_arrays() {
    assert_schema::<VecDeque<String>>(r#"{"items":{"type":"string"},"type":"array"}"#);
    assert_schema::<HashSet<String>>(r#"{"items":{"type":"string"},"type":"array"}"#);
    assert_schema::<BTreeSet<String>>(r#"{"items":{"type":"string"},"type":"array"}"#);
    assert_schema::<[String]>(r#"{"items":{"type":"string"},"type":"array"}"#);
}

#[test]
fn describes_a_map_as_an_object_which_is_never_null() {
    #[derive(Serialize, Schema)]
    struct Stock {
        counts: HashMap<i32, String>,
        shelves: BTreeMap<String, i64>,
    }

    assert_schema::<Stock>(
        r#"{"additionalProperties":false,"properties":{"counts":{"additionalProperties":{"type":"string"},"type":"object"},"shelves":{"additionalProperties":{"type":"integer"},"type":"object"}},"required":["counts","shelves"],"type":"object"}"#,
    );
}

#[test]
fn describes_an_option_of_a_map_as_an_object_which_may_be_null() {
    #[derive(Serialize, Schema)]
    struct Stock {
        counts: Option<HashMap<i32, String>>,
    }

    assert_schema::<Stock>(
        r#"{"additionalProperties":false,"properties":{"counts":{"anyOf":[{"additionalProperties":{"type":"string"},"type":"object"},{"type":"null"}]}},"required":["counts"],"type":"object"}"#,
    );
}

#[test]
fn describes_an_option_as_what_it_holds_which_may_be_null() {
    #[derive(Serialize, Schema)]
    struct Book {
        edition: Option<i64>,
        #[expect(clippy::option_option, reason = "this is what the test is about")]
        note: Option<Option<String>>,
        home: Option<Country>,
    }

    assert_schema::<Book>(
        r#"{"additionalProperties":false,"properties":{"edition":{"anyOf":[{"type":"integer"},{"type":"null"}]},"home":{"anyOf":[{"pattern":"^[A-Z]{2}$","type":"string"},{"type":"null"}]},"note":{"anyOf":[{"anyOf":[{"type":"string"},{"type":"null"}]},{"type":"null"}]}},"required":["edition","note","home"],"type":"object"}"#,
    );
}

#[test]
fn describes_a_box_an_rc_an_arc_and_a_cow_like_what_they_hold() {
    assert_schema::<Box<String>>(r#"{"type":"string"}"#);
    assert_schema::<Rc<i64>>(r#"{"type":"integer"}"#);
    assert_schema::<Arc<bool>>(r#"{"type":"boolean"}"#);
    assert_schema::<Cow<'static, str>>(r#"{"type":"string"}"#);
    assert_schema::<Box<[String]>>(r#"{"items":{"type":"string"},"type":"array"}"#);
    assert_schema::<&'static Country>(r#"{"pattern":"^[A-Z]{2}$","type":"string"}"#);
}

#[test]
fn describes_a_time_as_a_date_time_string() {
    #[derive(Serialize, Schema)]
    struct Loan {
        at: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    }

    assert_schema::<Loan>(
        r#"{"additionalProperties":false,"properties":{"at":{"format":"date-time","type":"string"},"until":{"anyOf":[{"format":"date-time","type":"string"},{"type":"null"}]}},"required":["at","until"],"type":"object"}"#,
    );
}

#[test]
fn describes_a_serde_json_number_as_a_number() {
    assert_schema::<serde_json::Number>(r#"{"type":"number"}"#);
}

#[test]
fn allows_any_value_for_a_serde_json_value() {
    #[derive(Serialize, Schema)]
    struct Shape {
        anything: Value,
        details: serde_json::Map<String, Value>,
    }

    assert_schema::<Shape>(
        r#"{"additionalProperties":false,"properties":{"anything":{},"details":{"additionalProperties":{},"type":"object"}},"required":["anything","details"],"type":"object"}"#,
    );
}

#[test]
fn describes_a_field_type_by_its_own_schema() {
    #[derive(Serialize, Schema)]
    struct Reader {
        country: Country,
    }

    assert_schema::<Reader>(
        r#"{"additionalProperties":false,"properties":{"country":{"pattern":"^[A-Z]{2}$","type":"string"}},"required":["country"],"type":"object"}"#,
    );
}

#[test]
fn describes_a_nested_struct_as_an_object() {
    #[derive(Serialize, Schema)]
    struct Address {
        city: String,
    }

    #[derive(Serialize, Schema)]
    struct Reader {
        address: Address,
    }

    assert_schema::<Reader>(
        r#"{"additionalProperties":false,"properties":{"address":{"additionalProperties":false,"properties":{"city":{"type":"string"}},"required":["city"],"type":"object"}},"required":["address"],"type":"object"}"#,
    );
}

#[test]
fn describes_a_newtype_struct_and_a_transparent_struct_like_the_value_they_wrap() {
    #[derive(Serialize, Schema)]
    struct Isbn(String);

    #[derive(Serialize, Schema)]
    #[serde(transparent)]
    struct Pages {
        count: i64,
    }

    assert_schema::<Isbn>(r#"{"type":"string"}"#);
    assert_schema::<Pages>(r#"{"type":"integer"}"#);
}

#[test]
fn derives_the_schema_of_a_generic_type_for_its_arguments() {
    #[derive(Serialize, Schema)]
    struct Page<T> {
        items: Vec<T>,
    }

    assert_schema::<Page<Country>>(
        r#"{"additionalProperties":false,"properties":{"items":{"items":{"pattern":"^[A-Z]{2}$","type":"string"},"type":"array"}},"required":["items"],"type":"object"}"#,
    );
}

#[test]
#[should_panic(expected = "is recursive, so its schema can not be derived")]
fn panics_for_a_recursive_type() {
    #[derive(Serialize, Schema)]
    struct Chapter {
        sections: Vec<Chapter>,
    }

    let _ = Chapter::schema();
}

#[test]
fn derives_a_type_again_after_a_recursive_one_panicked() {
    #[derive(Serialize, Schema)]
    struct Chapter {
        sections: Vec<Chapter>,
    }

    #[derive(Serialize, Schema)]
    struct Book {
        title: String,
    }

    assert!(std::panic::catch_unwind(Chapter::schema).is_err());
    assert_schema::<Book>(
        r#"{"additionalProperties":false,"properties":{"title":{"type":"string"}},"required":["title"],"type":"object"}"#,
    );
}

#[test]
fn derives_a_type_that_nests_the_same_type_twice() {
    #[derive(Serialize, Schema)]
    struct Address {
        city: String,
    }

    #[derive(Serialize, Schema)]
    struct Loan {
        from: Address,
        to: Address,
    }

    let address = Address::schema();
    assert_eq!(Loan::schema()["properties"]["from"], address);
    assert_eq!(Loan::schema()["properties"]["to"], address);
}

#[test]
#[should_panic(expected = "writes the field title more than once")]
fn panics_for_a_field_name_that_two_fields_share() {
    #[derive(Serialize, Schema)]
    struct Book {
        title: String,
        #[serde(rename = "title")]
        headline: String,
    }

    let _ = Book::schema();
}

#[test]
fn derives_the_schema_of_an_event() {
    #[derive(Serialize, Deserialize, Event)]
    #[event(type = "io.eventsourcingdb.library.book-acquired")]
    struct BookAcquired {
        title: String,
        author: String,
        isbn: String,
    }

    assert_eq!(
        BookAcquired::EVENT_TYPE,
        "io.eventsourcingdb.library.book-acquired"
    );
    assert_schema::<BookAcquired>(
        r#"{"additionalProperties":false,"properties":{"author":{"type":"string"},"isbn":{"type":"string"},"title":{"type":"string"}},"required":["title","author","isbn"],"type":"object"}"#,
    );
}

#[test]
fn returns_the_own_schema_of_an_event_implemented_by_hand() {
    #[derive(Serialize, Deserialize)]
    struct ReaderMoved {
        country: String,
    }

    impl Schema for ReaderMoved {
        fn schema() -> Value {
            json!({ "type": "object", "required": ["country"] })
        }
    }

    impl Event for ReaderMoved {
        const EVENT_TYPE: &'static str = "io.eventsourcingdb.library.reader-moved";
    }

    assert_schema::<ReaderMoved>(r#"{"required":["country"],"type":"object"}"#);
}

#[test]
fn agrees_with_serde_json_on_names_and_options() {
    #[derive(Serialize, Schema)]
    #[serde(rename_all = "camelCase")]
    struct Reader {
        full_name: String,
        #[serde(rename = "e-mail")]
        email_address: String,
        r#type: String,
        #[serde(skip)]
        internal: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        nickname: Option<String>,
        middle_name: Option<String>,
    }

    let schema = Reader::schema();
    let properties = schema["properties"]
        .as_object()
        .expect("properties is an object");
    let required: Vec<&str> = schema["required"]
        .as_array()
        .expect("required is an array")
        .iter()
        .map(|name| name.as_str().expect("a required name is a string"))
        .collect();

    for nickname in [None, Some("Lin".to_owned())] {
        let reader = Reader {
            full_name: "Ada Lovelace".to_owned(),
            email_address: "ada@example.com".to_owned(),
            r#type: "member".to_owned(),
            internal: "never written".to_owned(),
            nickname,
            middle_name: None,
        };
        let written = serde_json::to_value(&reader).expect("the reader can be written");
        let written = written.as_object().expect("serde writes an object");

        for name in written.keys() {
            assert!(
                properties.contains_key(name),
                "serde writes {name}, which the schema does not name"
            );
        }
        for name in &required {
            assert!(
                written.contains_key(*name),
                "the schema requires {name}, which serde does not write"
            );
        }
    }
}
