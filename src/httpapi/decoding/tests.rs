// These tests check how a body is decoded, with the types of Rust:
//
// - serde tells the names of fields apart by their case, so a name in another
//   case is an unknown field rather than the same one, or a name that occurs
//   twice.
// - A slice of bytes is a list of numbers.
// - A failure is told in words, and its cause, which serde gives as text, is
//   not kept as an error of its own.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, de};

use super::decode_strictly;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PreviewRequest {
    customer_id: Option<String>,
    quantity: Option<u32>,
    is_gift: Option<bool>,
    items: Option<Vec<PreviewItem>>,
    delivery: Option<Delivery>,
    position: Option<(i32, i32)>,
    kind: Option<Kind>,
    count: Option<Even>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PreviewItem {
    book_id: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Delivery {
    address: String,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Fiction,
    Poetry,
}

/// A number that decodes itself, and refuses an odd one in words of its own.
#[derive(Debug, PartialEq)]
struct Even(u32);

impl<'de> Deserialize<'de> for Even {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let number = u32::deserialize(deserializer)?;
        if number % 2 == 1 {
            return Err(de::Error::custom("an even number is needed"));
        }

        Ok(Self(number))
    }
}

fn text_of<T: for<'de> Deserialize<'de> + std::fmt::Debug>(body: &str) -> String {
    decode_strictly::<T>(body.as_bytes()).expect_err(body)
}

#[test]
fn decodes_a_body_that_fits() {
    let decoded: PreviewRequest =
        decode_strictly(br#"{"customerId":"42","quantity":3}"#).expect("the body fits");

    assert_eq!(decoded.customer_id.as_deref(), Some("42"));
    assert_eq!(decoded.quantity, Some(3));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the table holds every case, one after another"
)]
fn rejects_json_that_does_not_fit_and_says_why_in_words_of_its_own() {
    for (label, body, text) in [
        ("an empty body", "", "empty body"),
        ("a body of nothing but whitespace", " \t\r\n", "empty body"),
        ("not JSON at all", "not json", "invalid JSON"),
        (
            "JSON that ends too early",
            r#"{"customerId":"42""#,
            "invalid JSON",
        ),
        ("JSON without a value", r#"{"customerId":}"#, "invalid JSON"),
        (
            "JSON with a comma too many",
            r#"{"customerId":"42",}"#,
            "invalid JSON",
        ),
        (
            "JSON with single quotes",
            "{'customerId':'42'}",
            "invalid JSON",
        ),
        (
            "JSON after a character that is no whitespace in JSON",
            "\u{c}{}",
            "invalid JSON",
        ),
        (
            "an unknown field",
            r#"{"customerId":"42","quantiy":3}"#,
            r#"unknown field "quantiy""#,
        ),
        (
            "a nested unknown field",
            r#"{"delivery":{"address":"Main Street 1","adress":"Main Street 1"}}"#,
            r#"unknown field "adress""#,
        ),
        (
            "an unknown field in an object in a list",
            r#"{"items":[{"bookId":"1","isbn":"2"}]}"#,
            r#"unknown field "isbn""#,
        ),
        (
            "an unknown field with a quote in its name",
            r#"{"quan\"tity":3}"#,
            r#"unknown field "quan\"tity""#,
        ),
        (
            "a string where a number belongs",
            r#"{"quantity":"three"}"#,
            r#""quantity" must be a number"#,
        ),
        (
            "a number where a string belongs",
            r#"{"customerId":42}"#,
            r#""customerId" must be a string"#,
        ),
        (
            "a string where a boolean belongs",
            r#"{"isGift":"yes"}"#,
            r#""isGift" must be a boolean"#,
        ),
        (
            "an object where a list belongs",
            r#"{"items":{}}"#,
            r#""items" must be an array"#,
        ),
        (
            "a list where an object belongs",
            r#"{"delivery":[]}"#,
            r#""delivery" must be an object"#,
        ),
        (
            "a list with a value for every field where an object belongs",
            r#"{"delivery":["Main Street 1"]}"#,
            r#""delivery" must be an object"#,
        ),
        (
            "a list with a value for every field where an object in a list belongs",
            r#"{"items":[{"bookId":"1"},["2"]]}"#,
            r#""items.1" must be an object"#,
        ),
        (
            "a nested value of the wrong kind",
            r#"{"delivery":{"address":1}}"#,
            r#""delivery.address" must be a string"#,
        ),
        (
            "a value of the wrong kind in a list",
            r#"{"items":[{"bookId":"1"},{"bookId":2}]}"#,
            r#""items.1.bookId" must be a string"#,
        ),
        (
            "a list where the body has to be an object",
            "[]",
            "the body must be an object",
        ),
        (
            "a number too large for its field",
            r#"{"quantity":1e400}"#,
            r#""quantity" is out of range"#,
        ),
        (
            "a negative number for a field that holds none",
            r#"{"quantity":-1}"#,
            r#""quantity" is out of range"#,
        ),
        (
            "a fraction where an integer belongs",
            r#"{"quantity":1.5}"#,
            r#""quantity" must be an integer"#,
        ),
        (
            "an unknown field before a value of the wrong kind",
            r#"{"quantiy":3,"quantity":"three"}"#,
            r#"unknown field "quantiy""#,
        ),
        (
            "a value of the wrong kind before an unknown field",
            r#"{"quantity":"three","quantiy":3}"#,
            r#""quantity" must be a number"#,
        ),
        (
            "a value of the wrong kind in JSON that ends too early",
            r#"{"quantity":"three""#,
            "invalid JSON",
        ),
        (
            "an unknown field before a name that occurs twice",
            r#"{"quantiy":3,"customerId":"42","customerId":"43"}"#,
            r#"duplicate field "customerId""#,
        ),
        (
            "a name that occurs twice with values of the wrong kind",
            r#"{"quantity":"three","quantity":"four"}"#,
            r#"duplicate field "quantity""#,
        ),
        (
            "a field that is missing",
            r#"{"delivery":{}}"#,
            r#""delivery.address" is missing"#,
        ),
        (
            "a value that is none of the variants",
            r#"{"kind":"drama"}"#,
            r#""kind" must be one of "fiction", "poetry""#,
        ),
    ] {
        assert_eq!(text_of::<PreviewRequest>(body), text, "{label}");
    }
}

#[test]
fn says_which_kind_the_body_as_a_whole_has_to_be_or_why_it_does_not_fit() {
    assert_eq!(
        text_of::<PreviewRequest>("[]"),
        "the body must be an object"
    );
    assert_eq!(text_of::<PreviewRequest>("1"), "the body must be an object");
    assert_eq!(
        text_of::<PreviewRequest>(r#""42""#),
        "the body must be an object"
    );
    assert_eq!(
        text_of::<PreviewRequest>("true"),
        "the body must be an object"
    );
    assert_eq!(
        text_of::<HashMap<String, u32>>("[]"),
        "the body must be an object"
    );
    assert_eq!(
        text_of::<Vec<PreviewItem>>("{}"),
        "the body must be an array"
    );
    assert_eq!(text_of::<String>("42"), "the body must be a string");
    assert_eq!(text_of::<u32>("true"), "the body must be a number");
    assert_eq!(text_of::<u32>("1e400"), "the body is out of range");
    assert_eq!(text_of::<u32>("1.5"), "the body must be an integer");
    assert_eq!(
        text_of::<DateTime<Utc>>(r#""tomorrow""#),
        r#"the body must be a time such as "2026-10-05T12:00:00Z""#
    );
    assert_eq!(
        text_of::<HashMap<u32, u32>>(r#"{"x":1}"#),
        "the keys of the body must be numbers"
    );
}

#[test]
fn refuses_a_body_of_null() {
    assert_eq!(
        text_of::<PreviewRequest>("null"),
        "the body must be an object"
    );
    assert_eq!(
        text_of::<PreviewRequest>(" \t\r\nnull \t\r\n"),
        "the body must be an object"
    );
}

#[test]
fn still_takes_null_as_the_body_for_an_option() {
    let decoded: Option<PreviewRequest> = decode_strictly(b"null").expect("null is none");

    assert_eq!(decoded, None);
}

#[test]
fn refuses_a_list_whose_length_differs_from_that_of_a_tuple_and_says_how_long_it_has_to_be() {
    assert_eq!(
        text_of::<PreviewRequest>(r#"{"position":[1]}"#),
        r#""position" must have 2 elements"#
    );
    assert_eq!(
        text_of::<PreviewRequest>(r#"{"position":[1,2,3]}"#),
        r#""position" has too many elements"#
    );

    let decoded: PreviewRequest =
        decode_strictly(br#"{"position":[1,2]}"#).expect("a list of the same length fits");
    assert_eq!(decoded.position, Some((1, 2)));
}

#[test]
fn refuses_a_list_as_the_body_rather_than_taking_its_values_for_the_fields_in_their_order() {
    #[derive(Debug, Deserialize)]
    struct AcquireBook {
        title: String,
        author: String,
    }

    let decoded: AcquireBook = decode_strictly(br#"{"title":"2001","author":"Arthur C. Clarke"}"#)
        .expect("an object fits");
    assert_eq!(decoded.title, "2001");
    assert_eq!(decoded.author, "Arthur C. Clarke");

    assert_eq!(
        text_of::<AcquireBook>(r#"["2001","Arthur C. Clarke"]"#),
        "the body must be an object"
    );
    assert_eq!(
        text_of::<Option<AcquireBook>>(r#"["2001","Arthur C. Clarke"]"#),
        "the body must be an object"
    );
}

#[test]
fn refuses_a_list_where_an_object_belongs_in_a_map_a_newtype_and_a_variant() {
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[expect(dead_code, reason = "the fields are only decoded")]
    struct Dispatch {
        by_warehouse: Option<HashMap<String, Delivery>>,
        parcel: Option<Parcel>,
        carrier: Option<Carrier>,
    }

    #[derive(Debug, Deserialize)]
    #[expect(dead_code, reason = "the field is only decoded")]
    struct Parcel(Delivery);

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[expect(dead_code, reason = "the fields are only decoded")]
    enum Carrier {
        Courier { name: String, phone: String },
        Pickup(Delivery),
    }

    for (label, body, text) in [
        (
            "a value of a map",
            r#"{"byWarehouse":{"north":["Main Street 1"]}}"#,
            r#""byWarehouse.north" must be an object"#,
        ),
        (
            "a newtype that holds an object",
            r#"{"parcel":["Main Street 1"]}"#,
            r#""parcel" must be an object"#,
        ),
        (
            "a variant with named fields",
            r#"{"carrier":{"courier":["Jane","555-0100"]}}"#,
            r#""carrier.courier" must be an object"#,
        ),
        (
            "a variant that holds an object",
            r#"{"carrier":{"pickup":["Main Street 1"]}}"#,
            r#""carrier.pickup" must be an object"#,
        ),
    ] {
        assert_eq!(text_of::<Dispatch>(body), text, "{label}");
    }
}

#[test]
fn says_that_an_enum_with_a_tag_has_to_be_an_object() {
    #[derive(Debug, Deserialize)]
    #[serde(tag = "kind", content = "data", rename_all = "camelCase")]
    #[expect(dead_code, reason = "the fields are only decoded")]
    enum Shipment {
        Book { title: String },
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "kind", rename_all = "camelCase")]
    #[expect(dead_code, reason = "the fields are only decoded")]
    enum Lending {
        Book { title: String },
    }

    for (label, text) in [
        (
            "a list for an enum with its content in a field of its own",
            text_of::<Shipment>(r#"["book",{"title":"2001"}]"#),
        ),
        (
            "a number for an enum with its content in a field of its own",
            text_of::<Shipment>("42"),
        ),
        (
            "a number for an enum with its content beside the tag",
            text_of::<Lending>("42"),
        ),
    ] {
        assert_eq!(text, "the body must be an object", "{label}");
    }
}

#[test]
fn still_takes_a_list_where_a_list_belongs() {
    #[derive(Debug, Deserialize, PartialEq)]
    struct Corner(i32, i32);

    #[derive(Debug, Deserialize, PartialEq)]
    struct Isbns(Vec<String>);

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    enum Leg {
        Transfer(String, String),
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct Route {
        position: (i32, i32),
        corner: Corner,
        stops: Vec<Delivery>,
        sizes: [u32; 2],
        isbns: Isbns,
        leg: Leg,
    }

    let decoded: Route = decode_strictly(
        br#"{
            "position": [1, 2],
            "corner": [3, 4],
            "stops": [{"address": "Main Street 1"}],
            "sizes": [5, 6],
            "isbns": ["978-0-345-41346-6"],
            "leg": {"transfer": ["Main Street 1", "Station Road 2"]}
        }"#,
    )
    .expect("a list fits where a list belongs");

    assert_eq!(
        decoded,
        Route {
            position: (1, 2),
            corner: Corner(3, 4),
            stops: vec![Delivery {
                address: "Main Street 1".to_owned(),
            }],
            sizes: [5, 6],
            isbns: Isbns(vec!["978-0-345-41346-6".to_owned()]),
            leg: Leg::Transfer("Main Street 1".to_owned(), "Station Road 2".to_owned()),
        }
    );
}

#[test]
fn says_which_value_is_no_time() {
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[expect(dead_code, reason = "the field is only decoded")]
    struct Lending {
        due_on: DateTime<Utc>,
    }

    assert_eq!(
        text_of::<Lending>(r#"{"dueOn":"tomorrow"}"#),
        r#""dueOn" must be a time such as "2026-10-05T12:00:00Z""#
    );
    assert_eq!(
        text_of::<Lending>(r#"{"dueOn":42}"#),
        r#""dueOn" must be a time such as "2026-10-05T12:00:00Z""#
    );
}

#[test]
fn keeps_the_text_of_the_error_of_a_type_that_decodes_itself() {
    assert_eq!(
        text_of::<PreviewRequest>(r#"{"count":3}"#),
        "an even number is needed"
    );
}

#[test]
fn says_which_keys_of_a_map_do_not_fit_their_type() {
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[expect(dead_code, reason = "the fields are only decoded")]
    struct Indexes {
        by_availability: Option<HashMap<bool, String>>,
        by_number: Option<HashMap<u32, String>>,
    }

    assert_eq!(
        text_of::<Indexes>(r#"{"byAvailability":{"yes":"a"}}"#),
        r#"the keys of "byAvailability" must be booleans"#
    );
    assert_eq!(
        text_of::<Indexes>(r#"{"byNumber":{"x":"a"}}"#),
        r#"the keys of "byNumber" must be numbers"#
    );
}

#[test]
fn says_that_a_value_can_not_be_decoded_where_it_has_no_words_of_its_own() {
    #[derive(Debug, Deserialize)]
    #[expect(dead_code, reason = "the field is only decoded")]
    struct Marked {
        marker: (),
    }

    assert_eq!(
        text_of::<Marked>(r#"{"marker":1}"#),
        r#""marker" can not be decoded"#
    );
}

#[test]
fn names_a_value_by_the_names_in_its_path_even_if_they_hold_a_dot() {
    #[derive(Debug, Deserialize)]
    #[expect(dead_code, reason = "the field is only decoded")]
    struct Dotted {
        #[serde(rename = "a.b")]
        field: u32,
    }

    assert_eq!(
        text_of::<Dotted>(r#"{"a.b":"x"}"#),
        r#""a.b" must be a number"#
    );
}

#[test]
fn rejects_anything_but_whitespace_after_the_value() {
    for (label, body) in [
        ("a second value", "{} {}"),
        ("a word", "{} x"),
        ("a comma", "{},"),
    ] {
        assert_eq!(
            text_of::<PreviewRequest>(body),
            "data after the JSON value",
            "{label}"
        );
    }
}

#[test]
fn accepts_whitespace_after_the_value() {
    let decoded: PreviewRequest = decode_strictly(b"{} \t\r\n").expect("whitespace is fine");

    assert_eq!(decoded.quantity, None);
}

#[test]
fn rejects_a_name_that_occurs_twice() {
    for (label, body, text) in [
        (
            "at the top",
            r#"{"customerId":"42","customerId":"42"}"#,
            r#"duplicate field "customerId""#,
        ),
        (
            "in a nested object",
            r#"{"delivery":{"address":"a","address":"b"}}"#,
            r#"duplicate field "address""#,
        ),
        (
            "in an object in a list",
            r#"{"items":[{"bookId":"1","bookId":"2"}]}"#,
            r#"duplicate field "bookId""#,
        ),
        (
            "in a field the type does not know",
            r#"{"extra":{"a":1,"a":2}}"#,
            r#"duplicate field "a""#,
        ),
    ] {
        assert_eq!(text_of::<PreviewRequest>(body), text, "{label}");
    }
}

#[test]
fn tells_names_apart_by_their_case() {
    assert_eq!(
        text_of::<PreviewRequest>(r#"{"QUANTITY":3}"#),
        r#"unknown field "QUANTITY""#
    );
}
