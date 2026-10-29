//! What the derive macros and the types of the kit refuse, each as a doctest
//! that must not compile. The module exists only while rustdoc collects
//! doctests.

/// An event needs its event type:
///
/// ```compile_fail
/// #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// struct BookAcquired {
///     title: String,
/// }
/// ```
///
/// And the event type must not be empty:
///
/// ```compile_fail
/// #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// #[event(type = "")]
/// struct BookAcquired {
///     title: String,
/// }
/// ```
pub struct EventType;

/// serde writes an enum in several shapes:
///
/// ```compile_fail
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// enum Format {
///     Hardcover,
///     Paperback,
/// }
/// ```
pub struct Enum;

/// serde writes a unit struct as null:
///
/// ```compile_fail
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// struct Marker;
/// ```
pub struct UnitStruct;

/// serde writes a tuple struct with several fields as an array of mixed values:
///
/// ```compile_fail
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// struct Range(i64, String);
/// ```
pub struct TupleStruct;

/// serde writes a transparent struct as its only field:
///
/// ```compile_fail
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// #[serde(transparent)]
/// struct Pages {
///     count: i64,
///     unit: String,
/// }
/// ```
pub struct TransparentStruct;

/// serde writes the fields of a flattened field in place of the field:
///
/// ```compile_fail
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// struct Audit {
///     by: String,
/// }
///
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// struct Book {
///     title: String,
///     #[serde(flatten)]
///     audit: Audit,
/// }
/// ```
pub struct Flatten;

/// A field that serde writes with a function of its own is not written as its
/// type describes:
///
/// ```compile_fail
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// struct Book {
///     #[serde(serialize_with = "serialize_pages")]
///     pages: i64,
/// }
///
/// fn serialize_pages<S: serde::Serializer>(pages: &i64, serializer: S) -> Result<S::Ok, S::Error> {
///     serializer.serialize_str(&pages.to_string())
/// }
/// ```
pub struct SerializeWith;

/// serde writes a struct with a tag with a further field:
///
/// ```compile_fail
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// #[serde(tag = "kind")]
/// struct Book {
///     title: String,
/// }
/// ```
pub struct Tag;

/// serde writes a struct with `into` as the other type:
///
/// ```compile_fail
/// #[derive(Clone, serde::Serialize, architecturekit::Schema)]
/// #[serde(into = "String")]
/// struct Isbn {
///     digits: String,
/// }
///
/// impl From<Isbn> for String {
///     fn from(isbn: Isbn) -> Self {
///         isbn.digits
///     }
/// }
/// ```
pub struct Into;

/// The events of a state are an enum, with one variant per event type:
///
/// ```compile_fail
/// #[derive(architecturekit::Events)]
/// struct BookEvent {
///     title: String,
/// }
/// ```
///
/// And every variant holds exactly one event:
///
/// ```compile_fail
/// #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// struct BookAcquired {
///     title: String,
/// }
///
/// #[derive(architecturekit::Events)]
/// enum BookEvent {
///     Acquired(BookAcquired),
///     Lost,
/// }
/// ```
///
/// ```compile_fail
/// #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// struct BookAcquired {
///     title: String,
/// }
///
/// #[derive(architecturekit::Events)]
/// enum BookEvent {
///     Acquired { event: BookAcquired },
/// }
/// ```
pub struct EventsOfAState;

/// Two variants of the same event type would make reading ambiguous:
///
/// ```compile_fail
/// #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// struct BookAcquired {
///     title: String,
/// }
///
/// #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// struct BookBought {
///     title: String,
/// }
///
/// #[derive(architecturekit::Events)]
/// enum BookEvent {
///     Acquired(BookAcquired),
///     Bought(BookBought),
/// }
/// ```
pub struct DuplicateEventTypes;

/// The variants of an enum of events name the event types of a state, so it
/// can not be generic:
///
/// ```compile_fail
/// #[derive(architecturekit::Events)]
/// enum Wrapped<T: architecturekit::Event> {
///     Event(T),
/// }
/// ```
pub struct GenericEvents;

/// A field needs a type that describes its schema:
///
/// ```compile_fail,E0277
/// #[derive(serde::Serialize)]
/// struct Address {
///     city: String,
/// }
///
/// #[derive(serde::Serialize, architecturekit::Schema)]
/// struct Reader {
///     address: Address,
/// }
/// ```
pub struct FieldWithoutSchema;

/// An error type names its category itself, since no category fits every
/// error:
///
/// ```compile_fail
/// #[derive(Debug)]
/// struct BorrowRefused;
///
/// impl architecturekit::Categorized for BorrowRefused {}
/// ```
pub struct CategoryOfAnError;

/// The options of a projection may gain fields, so they are built with their
/// functions, not with a literal, not even with the rest from the default:
///
/// ```compile_fail
/// let options = architecturekit::ProjectionOptions {
///     name: Some("book list".to_owned()),
///     ..Default::default()
/// };
/// ```
pub struct LiteralOptions;

/// An envelope may gain fields, so it is built with `Envelope::new`, not with
/// a literal:
///
/// ```compile_fail
/// let envelope = architecturekit::Envelope {
///     id: architecturekit::Revision::from(0),
///     time: chrono::DateTime::UNIX_EPOCH,
///     source: "https://library.eventsourcingdb.io".to_owned(),
///     subject: "/books/42".to_owned(),
///     event_type: "io.eventsourcingdb.library.book-acquired".to_owned(),
///     trace_parent: None,
///     trace_state: None,
///     part: 0,
///     data: (),
/// };
/// ```
pub struct LiteralEnvelope;

/// A failure of a projection may gain fields, so a pattern names the fields it
/// needs, and leaves the others to `..`:
///
/// ```compile_fail
/// fn message_of(failure: &architecturekit::ProjectionError) -> Option<&str> {
///     match failure {
///         architecturekit::ProjectionError::Panicked { message } => Some(message),
///         _ => None,
///     }
/// }
/// ```
pub struct PatternWithoutRest;
