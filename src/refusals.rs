//! What the derive macros refuse, each as a doctest that must not compile. The
//! module exists only while rustdoc collects doctests.

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
