use std::{collections::HashMap, error::Error, fmt};

#[cfg(test)]
mod tests;

/// The characters that EventSourcingDB allows in a segment of a subject, for
/// the messages of errors and panics.
const SUBJECT_CHARACTERS: &str = "A-Z, a-z, 0-9, underscores, and hyphens";

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
/// let subject = workshops.build(&["acme", "go-cqrs"]);
/// assert_eq!(subject, "/tenant/acme/workshop/go-cqrs");
///
/// assert_eq!(workshops.value(&subject, "workshop"), Some("go-cqrs"));
/// ```
///
/// EventSourcingDB allows only ASCII letters and digits, underscores, and
/// hyphens in a segment of a subject, so this is what the literal segments of
/// a pattern and the values of its placeholders may contain. The names of its
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
    pub fn root(&self) -> String {
        let literals: Vec<&str> = self
            .segments
            .iter()
            .map_while(|segment| match segment {
                Segment::Literal(literal) => Some(literal.as_str()),
                Segment::Placeholder => None,
            })
            .collect();

        format!("/{}", literals.join("/"))
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
    /// as an ID in a request, may well be like that, so check them first.
    #[must_use]
    pub fn build<V: AsRef<str>>(&self, values: &[V]) -> String {
        if let Err(message) = self.check_values(values, true) {
            panic!("{message}");
        }

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

        subject
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
    /// does not have.
    #[must_use]
    pub fn match_subject<'scheme, 'subject>(
        &'scheme self,
        subject: &'subject str,
    ) -> Option<HashMap<&'scheme str, &'subject str>> {
        let parts: Vec<&str> = subject.strip_prefix('/')?.split('/').collect();
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
    pub fn value<'subject>(
        &self,
        subject: &'subject str,
        placeholder: &str,
    ) -> Option<&'subject str> {
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectValuesError(String);

impl fmt::Display for SubjectValuesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for SubjectValuesError {}

/// Tells whether a segment of a subject contains only characters that
/// EventSourcingDB allows there. It is true for an empty segment, which the
/// callers refuse on their own.
fn has_only_subject_characters(segment: &str) -> bool {
    segment
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '-')
}
