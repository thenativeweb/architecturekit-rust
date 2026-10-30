//! Decodes a request body strictly, and says what is wrong with one that does
//! not fit in words of its own, rather than in those of serde, which name the
//! types of Rust, such as `u32`, and positions in the text that a caller does
//! not need.

use std::{collections::HashSet, fmt};

use serde::de::{self, DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor};

mod by_name;
#[cfg(test)]
mod tests;

use by_name::ByName;

/// A time as the text for a value that is no time shows it.
const EXAMPLE_TIME: &str = "2026-10-05T12:00:00Z";

/// The texts of chrono for a time that can not be parsed, which tell a value
/// that has to be a time apart from any other one.
const TIME_FAILURES: [&str; 7] = [
    "input contains invalid characters",
    "premature end of input",
    "trailing input",
    "input is out of range",
    "no possible date and time matching input",
    "input is not enough for unique date and time",
    "bad or unsupported format string",
];

/// Decodes a body strictly: it refuses an empty body, JSON that is not valid,
/// anything but whitespace after the value, a name that occurs twice in an
/// object, a field the type does not know, and a list where an object
/// belongs. What is wrong is told in words of its own, such as
/// `"items.1.bookId" must be a string`.
pub(crate) fn decode_strictly<T: DeserializeOwned>(body: &[u8]) -> Result<T, String> {
    if body.iter().all(|byte| is_whitespace(*byte)) {
        return Err("empty body".to_owned());
    }
    scan(body)?;

    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let mut unknown = None;
    let mut record = |path: serde_ignored::Path<'_>| {
        if unknown.is_none() {
            unknown = Some(last_name_of(&path));
        }
    };
    // ByName refuses a list where a struct belongs. It wraps the reader of the
    // JSON itself, beneath the wrappers that note the path and the unknown
    // fields, so that a list it refuses is named by its path like any other
    // failure.
    let decoded: Result<T, _> = serde_path_to_error::deserialize(serde_ignored::Deserializer::new(
        ByName(&mut deserializer),
        &mut record,
    ));

    // An unknown field that came before a value that did not fit is told
    // first, as it comes first in the body. Decoding ends at the value, so a
    // field after it is never seen.
    if let Some(name) = unknown {
        return Err(format!("unknown field {name:?}"));
    }

    decoded.map_err(|failure| describe(failure.path(), failure.inner()))
}

/// Reads the body once as JSON of any shape, and refuses JSON that is not
/// valid, data after the value, and a name that occurs twice in an object,
/// before the body is decoded into its type, so that these come first. A
/// number that no type can hold is left to the decoding, which says which
/// value it is.
fn scan(body: &[u8]) -> Result<(), String> {
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let mut duplicate = None;

    let scanned = Scan {
        duplicate: &mut duplicate,
    }
    .deserialize(&mut deserializer);

    if let Some(name) = duplicate {
        return Err(format!("duplicate field {name:?}"));
    }
    match scanned {
        Ok(()) => deserializer
            .end()
            .map_err(|_| "data after the JSON value".to_owned()),
        Err(failure) if failure.to_string().starts_with("number out of range") => Ok(()),
        Err(_) => Err("invalid JSON".to_owned()),
    }
}

/// Runs over a value of any shape, and ends at the first name that occurs
/// twice in an object, which it notes.
struct Scan<'d> {
    duplicate: &'d mut Option<String>,
}

impl<'de> DeserializeSeed<'de> for Scan<'_> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Scan<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<(), A::Error> {
        while items
            .next_element_seed(Scan {
                duplicate: &mut *self.duplicate,
            })?
            .is_some()
        {}

        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut members: A) -> Result<(), A::Error> {
        let mut names = HashSet::new();
        while let Some(name) = members.next_key::<String>()? {
            if !names.insert(name.clone()) {
                *self.duplicate = Some(name);
                return Err(de::Error::custom("a name occurs twice"));
            }
            members.next_value_seed(Scan {
                duplicate: &mut *self.duplicate,
            })?;
        }

        Ok(())
    }
}

/// Whitespace as JSON knows it.
fn is_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

/// Returns the name of the field an ignored path ends with, or the index of
/// the item.
fn last_name_of(path: &serde_ignored::Path<'_>) -> String {
    match path {
        serde_ignored::Path::Root => String::new(),
        serde_ignored::Path::Map { key, .. } => key.clone(),
        serde_ignored::Path::Seq { index, .. } => index.to_string(),
        serde_ignored::Path::Some { parent }
        | serde_ignored::Path::NewtypeStruct { parent }
        | serde_ignored::Path::NewtypeVariant { parent } => last_name_of(parent),
    }
}

/// Returns the names in a path, with the index of an item as its name.
fn names_in(path: &serde_path_to_error::Path) -> Vec<String> {
    path.iter()
        .filter_map(|segment| match segment {
            serde_path_to_error::Segment::Map { key } => Some(key.clone()),
            serde_path_to_error::Segment::Seq { index } => Some(index.to_string()),
            serde_path_to_error::Segment::Enum { variant } => Some(variant.clone()),
            serde_path_to_error::Segment::Unknown => None,
        })
        .collect()
}

/// Names a value by its path, with a dot between the names, in quotes, or as
/// the body.
fn subject_of(names: &[String]) -> String {
    if names.is_empty() {
        return "the body".to_owned();
    }

    format!("{:?}", names.join("."))
}

/// Says what is wrong with a value that did not fit its type, in words of its
/// own where serde says enough, that it can not be decoded where serde says
/// too little, and in the words of the type, where the type decodes itself.
fn describe(path: &serde_path_to_error::Path, failure: &serde_json::Error) -> String {
    let names = names_in(path);
    let subject = subject_of(&names);
    let reason = reason_of(failure);

    // A key of a map that does not fit leaves the path at a segment that
    // serde does not know.
    if matches!(
        path.iter().next_back(),
        Some(serde_path_to_error::Segment::Unknown)
    ) {
        return describe_key(&subject, &reason);
    }

    if let Some(field) = between_backticks(&reason, "missing field `") {
        let mut names = names;
        names.push(field.to_owned());
        return format!("{} is missing", subject_of(&names));
    }
    if reason.starts_with("number out of range") {
        return format!("{subject} is out of range");
    }
    if reason.starts_with("trailing characters") {
        return format!("{subject} has too many elements");
    }
    if TIME_FAILURES.iter().any(|text| reason.starts_with(text)) {
        return format!("{subject} must be a time such as {EXAMPLE_TIME:?}");
    }
    if let Some(variants) = reason.strip_prefix("unknown variant `") {
        return format!("{subject} must be one of {}", variants_of(variants));
    }
    if let Some((unexpected, expected)) = reason
        .strip_prefix("invalid type: ")
        .and_then(|rest| rest.split_once(", expected "))
        && let Some(text) = describe_type(&subject, unexpected, expected)
    {
        return text;
    }
    if let Some((_, expected)) = reason
        .strip_prefix("invalid value: ")
        .and_then(|rest| rest.split_once(", expected "))
        && is_integer(expected)
    {
        return format!("{subject} is out of range");
    }
    if let Some((_, expected)) = reason
        .strip_prefix("invalid length ")
        .and_then(|rest| rest.split_once(", expected "))
        && let Some(text) = describe_length(&subject, expected)
    {
        return text;
    }

    // What serde says in words of its own, which name the types of Rust, is
    // left out. Any other failure comes from a type that decodes itself, and
    // its text is kept as it is.
    if SERDE_FAILURES
        .iter()
        .any(|prefix| reason.starts_with(prefix))
    {
        return format!("{subject} can not be decoded");
    }

    reason
}

/// The beginnings of the failures that serde reports in words of its own.
const SERDE_FAILURES: [&str; 7] = [
    "invalid type: ",
    "invalid value: ",
    "invalid length ",
    "unknown variant ",
    "unknown field ",
    "missing field ",
    "duplicate field ",
];

/// Says which kind of value the keys of a map have to be.
fn describe_key(subject: &str, reason: &str) -> String {
    if reason.contains("expected key to be a number") {
        return format!("the keys of {subject} must be numbers");
    }

    let expected = reason
        .split_once(", expected ")
        .map_or("", |(_, expected)| expected);
    if is_time(expected) || TIME_FAILURES.iter().any(|text| reason.starts_with(text)) {
        return format!("the keys of {subject} must be times such as {EXAMPLE_TIME:?}");
    }

    match kind_of(expected) {
        Some("a number") => format!("the keys of {subject} must be numbers"),
        Some("a boolean") => format!("the keys of {subject} must be booleans"),
        _ => format!("{subject} can not be decoded"),
    }
}

/// Returns whether serde expected a time, as chrono describes one.
fn is_time(expected: &str) -> bool {
    expected.contains("formatted date and time string")
}

/// Says which kind of value a value of the wrong kind has to be.
fn describe_type(subject: &str, unexpected: &str, expected: &str) -> Option<String> {
    if is_time(expected) {
        return Some(format!("{subject} must be a time such as {EXAMPLE_TIME:?}"));
    }
    if is_integer(expected) && unexpected.starts_with("floating point") {
        return Some(format!("{subject} must be an integer"));
    }

    kind_of(expected).map(|kind| format!("{subject} must be {kind}"))
}

/// Says how many elements a list of the wrong length has to have.
fn describe_length(subject: &str, expected: &str) -> Option<String> {
    if expected.starts_with("struct ") {
        return Some(format!("{subject} must be an object"));
    }

    let count = expected
        .strip_prefix("a tuple of size ")
        .or_else(|| expected.strip_prefix("an array of length "))
        .or_else(|| {
            expected.rsplit_once(" with ").map(|(_, count)| {
                count
                    .trim_end_matches(" elements")
                    .trim_end_matches(" element")
            })
        })?
        .parse::<usize>()
        .ok()?;

    Some(match count {
        0 => format!("{subject} must have no elements"),
        1 => format!("{subject} must have 1 element"),
        count => format!("{subject} must have {count} elements"),
    })
}

/// Names the kind of JSON value that serde expected, in words of its own.
fn kind_of(expected: &str) -> Option<&'static str> {
    if is_integer(expected) || matches!(expected, "f32" | "f64") {
        return Some("a number");
    }
    if matches!(
        expected,
        "a string" | "a borrowed string" | "a character" | "string"
    ) {
        return Some("a string");
    }
    if expected == "a boolean" {
        return Some("a boolean");
    }
    if expected == "a sequence"
        || expected.starts_with("a tuple of size ")
        || expected.starts_with("an array of length ")
        || expected.starts_with("tuple struct ")
    {
        return Some("an array");
    }
    // An enum with a tag is an object in JSON, whether its content stands
    // beside the tag or in a field of its own.
    if expected == "a map"
        || expected.starts_with("struct ")
        || expected.starts_with("adjacently tagged enum ")
        || expected.starts_with("internally tagged enum ")
    {
        return Some("an object");
    }

    None
}

/// Returns whether serde expected an integer of a primitive type.
fn is_integer(expected: &str) -> bool {
    matches!(
        expected,
        "u8" | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
    )
}

/// Lists the variants serde expected, in quotes, from a text such as
/// "`Drama`, expected `Fiction` or `Poetry`".
fn variants_of(text: &str) -> String {
    let expected = text
        .split_once("expected ")
        .map_or("", |(_, expected)| expected);
    let variants: Vec<String> = expected
        .split('`')
        .skip(1)
        .step_by(2)
        .map(|variant| format!("{variant:?}"))
        .collect();

    variants.join(", ")
}

/// Returns the text between the given prefix and the next backtick.
fn between_backticks<'r>(text: &'r str, prefix: &str) -> Option<&'r str> {
    text.strip_prefix(prefix)?.split('`').next()
}

/// Returns the reason serde gives for a failure, without the position in the
/// text, which a caller does not need.
fn reason_of(failure: &serde_json::Error) -> String {
    let text = failure.to_string();
    let position = format!(" at line {} column {}", failure.line(), failure.column());

    text.strip_suffix(&position).unwrap_or(&text).to_owned()
}
