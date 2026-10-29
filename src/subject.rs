use std::{borrow::Borrow, collections::HashMap, error::Error, fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::{Categorized, Category, Failure, Permanent};

#[cfg(test)]
mod tests;

/// The characters that EventSourcingDB allows in a segment of a subject, for
/// the messages of errors and panics.
const SUBJECT_CHARACTERS: &str = "A-Z, a-z, 0-9, underscores, and hyphens";

/// A subject of EventSourcingDB, which is a path such as `/books/42`: it starts
/// with a slash, `/` alone is the root, and its segments are separated by
/// single slashes, none of them empty, each made only of the characters A-Z,
/// a-z, 0-9, underscores, and hyphens. A single slash at the end is taken, and
/// left out, as the database does, so `/books/42/` is `/books/42`. A value
/// that is no such path can not be a subject at all.
///
/// A subject that is written in the code is made with
/// [`new`](Subject::new), which panics for one that is not a subject. One that
/// comes from outside, such as a path in a request, is read with `parse`,
/// which refuses it with [`NotASubject`], and one that is made of values from
/// outside is built with [`SubjectScheme::try_build`]:
///
/// ```
/// use architecturekit::Subject;
///
/// let book = Subject::new("/books/42");
/// assert_eq!(book.as_str(), "/books/42");
///
/// let read: Subject = "/books/42/".parse()?;
/// assert_eq!(read, book);
///
/// assert!("books/42".parse::<Subject>().is_err());
/// # Ok::<(), architecturekit::NotASubject>(())
/// ```
///
/// serde writes and reads a subject as a string.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Subject(String);

impl Subject {
    /// Takes a subject that is written in the code.
    ///
    /// # Panics
    ///
    /// A subject that is not one is a programming error, so this panics. For a
    /// subject that comes from outside, use `parse` instead.
    #[must_use]
    #[track_caller]
    pub fn new(subject: &str) -> Self {
        match subject.parse() {
            Ok(subject) => subject,
            Err(refusal) => panic!("architecturekit: {refusal}"),
        }
    }

    /// Returns the root, `/`, which every subject lies under.
    #[must_use]
    pub fn root() -> Self {
        Self("/".to_owned())
    }

    /// Returns the subject as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Reads a subject, and leaves out a single slash at its end.
///
/// # Errors
///
/// Fails with [`NotASubject`] for a value that does not start with a slash,
/// that has an empty segment, or a segment with a character the database does
/// not allow in a subject.
impl FromStr for Subject {
    type Err = NotASubject;

    fn from_str(subject: &str) -> Result<Self, NotASubject> {
        let refused = |reason| NotASubject {
            subject: subject.to_owned(),
            reason,
        };

        let Some(rest) = subject.strip_prefix('/') else {
            return Err(refused(Reason::NoLeadingSlash));
        };
        if rest.is_empty() {
            return Ok(Self::root());
        }

        let rest = rest.strip_suffix('/').unwrap_or(rest);
        for segment in rest.split('/') {
            if segment.is_empty() {
                return Err(refused(Reason::EmptySegment));
            }
            if !has_only_subject_characters(segment) {
                return Err(refused(Reason::Characters));
            }
        }

        Ok(Self(format!("/{rest}")))
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(&self.0)
    }
}

impl AsRef<str> for Subject {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for Subject {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<Subject> for String {
    fn from(subject: Subject) -> Self {
        subject.0
    }
}

/// Writes the subject as a string.
impl Serialize for Subject {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// Reads the subject from a string, as [`parse`](str::parse) does, and fails
/// with the text of [`NotASubject`] for a string that is not one.
impl<'de> Deserialize<'de> for Subject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(SubjectVisitor)
    }
}

/// Reads a subject from a string for serde.
struct SubjectVisitor;

impl de::Visitor<'_> for SubjectVisitor {
    type Value = Subject;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a subject, which is a string such as \"/books/42\"")
    }

    fn visit_str<E: de::Error>(self, subject: &str) -> Result<Subject, E> {
        subject.parse().map_err(E::custom)
    }
}

/// A value that was meant to be a subject, but is not one, such as `books/42`
/// or `/books//42`.
///
/// It usually comes from the caller, such as a path in a request, so it
/// belongs to no category: its [`Categorized::category`] is `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotASubject {
    subject: String,
    reason: Reason,
}

/// Why a value is not a subject.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    NoLeadingSlash,
    EmptySegment,
    Characters,
}

impl NotASubject {
    /// Returns the value that is not a subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

impl fmt::Display for NotASubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "not a subject: {:?}, ", self.subject)?;

        match self.reason {
            Reason::NoLeadingSlash => formatter.write_str("which does not start with a slash"),
            Reason::EmptySegment => formatter.write_str("which has an empty segment"),
            Reason::Characters => write!(
                formatter,
                "whose segments may only contain {SUBJECT_CHARACTERS}"
            ),
        }
    }
}

impl Error for NotASubject {}

impl Categorized for NotASubject {
    fn category(&self) -> Option<Category> {
        None
    }
}

/// Reads the subject of a stored event. The database never writes a subject
/// that is not one, so such an event fails permanently.
pub(crate) fn subject_of(event: &eventsourcingdb::Event) -> Result<Subject, Permanent> {
    event.subject().parse().map_err(|refusal: NotASubject| {
        Permanent::Other(Failure::new(
            format!(
                "event {:?} is on {:?}, which is not a subject",
                event.id(),
                event.subject(),
            ),
            Some(Box::new(refusal)),
        ))
    })
}

/// How a subject is composed: literal segments and placeholders in braces, as
/// in `/tenant/{tenant}/workshop/{workshop}`.
///
/// A scheme works in both directions. [`build`](SubjectScheme::build) composes
/// a subject from values, and [`match_subject`](SubjectScheme::match_subject)
/// takes one apart again, or [`value`](SubjectScheme::value) takes a single
/// value out of it, which a projection needs to recover the ID of an aggregate
/// that the events themselves do not carry:
///
/// ```
/// use architecturekit::SubjectScheme;
///
/// let workshops = SubjectScheme::new("/tenant/{tenant}/workshop/{workshop}");
///
/// let subject = workshops.build(&["acme", "event-sourcing"]);
/// assert_eq!(subject.as_str(), "/tenant/acme/workshop/event-sourcing");
///
/// assert_eq!(workshops.value(&subject, "workshop"), Some("event-sourcing"));
/// ```
///
/// EventSourcingDB allows only ASCII letters and digits, underscores, and
/// hyphens in a segment of a subject (see [`Subject`]), so this is what the
/// literal segments of a pattern and the values of its placeholders may
/// contain. The names of its
/// placeholders follow the same rule, so that a typo, such as a brace too many,
/// is not taken for a name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectScheme {
    pattern: String,
    segments: Vec<Segment>,
    placeholders: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Placeholder,
}

impl SubjectScheme {
    /// Parses a pattern.
    ///
    /// # Panics
    ///
    /// A malformed pattern, including one with a literal segment or a
    /// placeholder name that contains a character EventSourcingDB does not
    /// allow in a subject, is a programming error, so this panics while the
    /// scheme is being built.
    #[must_use]
    pub fn new(pattern: &str) -> Self {
        let Some(rest) = pattern.strip_prefix('/') else {
            panic!("architecturekit: subject pattern {pattern:?} must start with a slash");
        };

        let mut segments = Vec::new();
        let mut placeholders: Vec<String> = Vec::new();

        for segment in rest.split('/') {
            assert!(
                !segment.is_empty(),
                "architecturekit: subject pattern {pattern:?} has an empty segment",
            );

            let Some(inner) = segment.strip_prefix('{') else {
                assert!(
                    !segment.contains(['{', '}']),
                    "architecturekit: segment {segment:?} in {pattern:?} is malformed",
                );
                assert!(
                    has_only_subject_characters(segment),
                    "architecturekit: segment {segment:?} in {pattern:?} may only contain {SUBJECT_CHARACTERS}",
                );
                segments.push(Segment::Literal(segment.to_owned()));
                continue;
            };

            // A placeholder is a whole segment in a single pair of braces, so a
            // brace anywhere else, as in "{id}}" or "{{id}}", is a mistake.
            let name = match inner.strip_suffix('}') {
                Some(name) if !name.contains(['{', '}']) => name,
                _ => panic!("architecturekit: segment {segment:?} in {pattern:?} is malformed"),
            };

            assert!(
                !name.is_empty(),
                "architecturekit: pattern {pattern:?} has an unnamed placeholder",
            );
            assert!(
                has_only_subject_characters(name),
                "architecturekit: placeholder {name:?} in {pattern:?} may only contain {SUBJECT_CHARACTERS}",
            );
            assert!(
                !placeholders.iter().any(|existing| existing == name),
                "architecturekit: pattern {pattern:?} uses the placeholder {name:?} twice",
            );

            placeholders.push(name.to_owned());
            segments.push(Segment::Placeholder);
        }

        Self {
            pattern: pattern.to_owned(),
            segments,
            placeholders,
        }
    }

    /// Returns the pattern the scheme was built from.
    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Returns the subject that every subject of the scheme lies under: the
    /// literal segments before the first placeholder, as `/tenant` for
    /// `/tenant/{tenant}/workshop/{workshop}`, or `/` if the pattern starts
    /// with a placeholder. Reading or observing recursively from it yields the
    /// events of all subjects the scheme describes, and possibly of others
    /// under the same root, which [`match_subject`](SubjectScheme::match_subject)
    /// tells apart.
    #[must_use]
    pub fn root(&self) -> Subject {
        let literals: Vec<&str> = self
            .segments
            .iter()
            .map_while(|segment| match segment {
                Segment::Literal(literal) => Some(literal.as_str()),
                Segment::Placeholder => None,
            })
            .collect();

        // The literals are checked when the scheme is built, and the root has
        // none, so this is a subject.
        Subject(format!("/{}", literals.join("/")))
    }

    /// Returns the placeholder names, in the order
    /// [`build`](SubjectScheme::build) expects their values.
    #[must_use]
    pub fn placeholders(&self) -> &[String] {
        &self.placeholders
    }

    /// Composes a subject. The values fill the placeholders in the order they
    /// appear in the pattern.
    ///
    /// # Panics
    ///
    /// A wrong number of values, or one that [`check`](SubjectScheme::check)
    /// refuses, such as an empty one or one with a slash or a dot, is a
    /// programming error, so this panics. Values that come from outside, such
    /// as an ID in a request, may well be like that, so build from them with
    /// [`try_build`](SubjectScheme::try_build) instead.
    #[must_use]
    pub fn build<V: AsRef<str>>(&self, values: &[V]) -> Subject {
        if let Err(message) = self.check_values(values, true) {
            panic!("{message}");
        }

        self.compose(values)
    }

    /// Composes a subject from values that come from outside, such as an ID in
    /// a request, as [`build`](SubjectScheme::build) does, but refuses values
    /// that [`check`](SubjectScheme::check) refuses, rather than panicking:
    ///
    /// ```
    /// use architecturekit::SubjectScheme;
    ///
    /// let books = SubjectScheme::new("/books/{book_id}");
    ///
    /// assert_eq!(books.try_build(&["42"])?.as_str(), "/books/42");
    /// assert!(books.try_build(&["a.b"]).is_err());
    /// # Ok::<(), architecturekit::SubjectValuesError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Fails with what [`check`](SubjectScheme::check) fails with.
    pub fn try_build<V: AsRef<str>>(&self, values: &[V]) -> Result<Subject, SubjectValuesError> {
        self.check(values)?;

        Ok(self.compose(values))
    }

    /// Composes a subject from values that have been checked.
    fn compose<V: AsRef<str>>(&self, values: &[V]) -> Subject {
        let mut subject = String::new();
        let mut next = 0;

        for segment in &self.segments {
            subject.push('/');
            match segment {
                Segment::Literal(literal) => subject.push_str(literal),
                Segment::Placeholder => {
                    subject.push_str(values[next].as_ref());
                    next += 1;
                }
            }
        }

        // The literals are checked when the scheme is built, and the values
        // before they are composed, so this is a subject.
        Subject(subject)
    }

    /// Tells whether the values can compose a subject: one per placeholder,
    /// none of them empty, and each made only of the characters EventSourcingDB
    /// allows in a segment of a subject, which are A-Z, a-z, 0-9, underscores,
    /// and hyphens. It is what [`build`](SubjectScheme::build) insists on, as
    /// an error rather than a panic, for values that come from outside.
    ///
    /// The error may reach the caller of an API, so it names the placeholder
    /// of a value, but neither the crate nor the pattern, as in: value for
    /// "id" must not be empty. The panic of `build` names both, since it is
    /// for the developer.
    ///
    /// # Errors
    ///
    /// Fails if the values can not compose a subject, as described above.
    pub fn check<V: AsRef<str>>(&self, values: &[V]) -> Result<(), SubjectValuesError> {
        self.check_values(values, false).map_err(SubjectValuesError)
    }

    /// Tells whether the values can compose a subject, for `check`, or for
    /// `build` if `is_for_developer` is set, in which case the message names
    /// the crate and the pattern as well.
    fn check_values<V: AsRef<str>>(
        &self,
        values: &[V],
        is_for_developer: bool,
    ) -> Result<(), String> {
        let (wanted, got) = (self.placeholders.len(), values.len());
        if wanted != got {
            return Err(if is_for_developer {
                format!(
                    "architecturekit: pattern {:?} needs {wanted} value(s), got {got}",
                    self.pattern,
                )
            } else {
                format!("the subject needs {wanted} value(s), got {got}")
            });
        }

        let (prefix, in_pattern) = if is_for_developer {
            ("architecturekit: ", format!(" in {:?}", self.pattern))
        } else {
            ("", String::new())
        };

        for (value, placeholder) in values.iter().zip(&self.placeholders) {
            let value = value.as_ref();
            if value.is_empty() {
                return Err(format!(
                    "{prefix}value for {placeholder:?}{in_pattern} must not be empty"
                ));
            }
            if !has_only_subject_characters(value) {
                return Err(format!(
                    "{prefix}value {value:?} for {placeholder:?}{in_pattern} may only contain {SUBJECT_CHARACTERS}"
                ));
            }
        }

        Ok(())
    }

    /// Takes a subject apart, into the value of every placeholder, by its
    /// name. It returns `None` if the subject does not follow the pattern,
    /// which is an ordinary case for a projection reading recursively across
    /// several schemes. A subject with a value that
    /// [`check`](SubjectScheme::check) refuses does not follow it either, so
    /// that this takes apart only what [`build`](SubjectScheme::build)
    /// composes.
    ///
    /// It is called `match_subject` rather than `match`, which is a keyword in
    /// Rust. To take a single value out of a subject, use
    /// [`value`](SubjectScheme::value), which panics for a name the pattern
    /// does not have. It takes a [`Subject`] as well as text.
    #[must_use]
    pub fn match_subject<'scheme, 'subject, S>(
        &'scheme self,
        subject: &'subject S,
    ) -> Option<HashMap<&'scheme str, &'subject str>>
    where
        S: AsRef<str> + ?Sized,
    {
        let parts: Vec<&str> = subject.as_ref().strip_prefix('/')?.split('/').collect();
        if parts.len() != self.segments.len() {
            return None;
        }

        let mut placeholders = self.placeholders.iter();
        let mut values = Vec::with_capacity(self.placeholders.len());

        for (segment, part) in self.segments.iter().zip(parts) {
            match segment {
                Segment::Literal(literal) => {
                    if part != literal {
                        return None;
                    }
                }
                Segment::Placeholder => {
                    if part.is_empty() || !has_only_subject_characters(part) {
                        return None;
                    }
                    values.push((placeholders.next()?.as_str(), part));
                }
            }
        }

        Some(values.into_iter().collect())
    }

    /// Takes the value of a single placeholder out of a subject. It returns
    /// `None` if the subject does not follow the pattern, as
    /// [`match_subject`](SubjectScheme::match_subject) does. To take several
    /// values out of the same subject, use `match_subject`.
    ///
    /// # Panics
    ///
    /// A placeholder that the pattern does not have, such as one with a typo,
    /// is a programming error, so this panics, whatever the subject is.
    #[must_use]
    pub fn value<'subject, S>(
        &self,
        subject: &'subject S,
        placeholder: &str,
    ) -> Option<&'subject str>
    where
        S: AsRef<str> + ?Sized,
    {
        assert!(
            self.placeholders.iter().any(|name| name == placeholder),
            "architecturekit: pattern {:?} has no placeholder {placeholder:?}",
            self.pattern,
        );

        self.match_subject(subject)
            .and_then(|values| values.get(placeholder).copied())
    }
}

/// The values that [`SubjectScheme::check`] refuses, with a message that may
/// reach the caller of an API.
///
/// They usually come from the caller, such as an ID in a request, so it
/// belongs to no category: its [`Categorized::category`] is `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectValuesError(String);

impl fmt::Display for SubjectValuesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for SubjectValuesError {}

impl Categorized for SubjectValuesError {
    fn category(&self) -> Option<Category> {
        None
    }
}

/// Tells whether a segment of a subject contains only characters that
/// EventSourcingDB allows there. It is true for an empty segment, which the
/// callers refuse on their own.
fn has_only_subject_characters(segment: &str) -> bool {
    segment
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '-')
}
