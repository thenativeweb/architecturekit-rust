// These tests check SubjectScheme and its values. "agrees with the database
// on every character" needs write, so it comes with the store. Messages quote
// values the way Rust does, with {:?}.

use std::{
    collections::HashMap,
    panic::{UnwindSafe, catch_unwind},
};

use crate::SubjectScheme;

/// The characters that EventSourcingDB allows in a segment of a subject,
/// written out rather than taken from the code under test.
const DATABASE_CHARACTERS: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";

/// Every ASCII character, and some beyond: letters outside ASCII, characters
/// that look like the ones the database allows, and the replacement
/// character.
fn some_characters() -> Vec<char> {
    (0..128_u8)
        .map(char::from)
        .chain([
            '\u{0080}',
            '\u{00fc}',
            '\u{00df}',
            '\u{00e9}',
            '\u{212a}',
            '\u{2010}',
            '\u{ff11}',
            '\u{ff21}',
            '\u{ff3f}',
            '\u{1f600}',
            '\u{fffd}',
        ])
        .collect()
}

/// Runs a function that is expected to panic, and returns the message of its
/// panic, or `None` if it did not panic.
fn panic_message(function: impl FnOnce() + UnwindSafe) -> Option<String> {
    let payload = catch_unwind(function).err()?;
    payload.downcast_ref::<String>().cloned().or_else(|| {
        payload
            .downcast_ref::<&str>()
            .map(|message| (*message).to_owned())
    })
}

fn workshops() -> SubjectScheme {
    SubjectScheme::new("/tenant/{tenant}/workshop/{workshop}")
}

#[test]
fn builds_and_matches() {
    let scheme = workshops();

    let subject = scheme.build(&["acme", "es-cqrs"]);
    assert_eq!(subject, "/tenant/acme/workshop/es-cqrs");

    let values = scheme.match_subject(&subject).expect("the subject matches");
    assert_eq!(values["tenant"], "acme");
    assert_eq!(values["workshop"], "es-cqrs");
}

#[test]
fn without_placeholders() {
    let scheme = SubjectScheme::new("/system/health");

    assert_eq!(scheme.build::<&str>(&[]), "/system/health");
    assert!(
        scheme.match_subject("/system/health").is_some(),
        "should match itself"
    );
}

#[test]
fn reports_its_placeholders() {
    assert_eq!(workshops().placeholders(), ["tenant", "workshop"]);
}

#[test]
fn rejects_subjects_that_do_not_fit() {
    let scheme = workshops();

    for (name, subject) in [
        ("no leading slash", "tenant/acme/workshop/es-cqrs"),
        ("too few segments", "/tenant/acme/workshop"),
        ("too many segments", "/tenant/acme/workshop/es-cqrs/more"),
        ("literal does not match", "/client/acme/workshop/es-cqrs"),
        ("empty placeholder value", "/tenant/acme/workshop/"),
        (
            "value with a character the database refuses",
            "/tenant/acme/workshop/es.cqrs",
        ),
    ] {
        assert!(
            scheme.match_subject(subject).is_none(),
            "{name}: {subject:?} should not match"
        );
    }
}

#[test]
fn panics_on_malformed_patterns() {
    for (name, pattern) in [
        ("no leading slash", "workshop/{id}"),
        ("empty segment", "/workshop//{id}"),
        ("unclosed placeholder", "/workshop/{id"),
        ("unnamed placeholder", "/workshop/{}"),
        ("duplicate placeholder", "/a/{id}/b/{id}"),
        ("malformed segment", "/workshop/pre{id}"),
        ("dot in a literal", "/books.v2/{book}"),
        ("space in a literal", "/my books/{book}"),
        ("umlaut in a literal", "/b\u{00fc}cher/{book}"),
        ("dots as a literal", "/../{book}"),
        ("closing brace too many", "/{a}}"),
        ("placeholder in braces twice", "/{{a}}"),
        ("opening brace in a placeholder", "/{a{b}"),
        ("space in a placeholder", "/{a b}"),
        ("umlaut in a placeholder", "/{\u{00e4}}"),
        ("dot as a placeholder", "/{.}"),
        ("asterisk as a placeholder", "/x/{*}"),
    ] {
        assert!(
            panic_message(|| drop(SubjectScheme::new(pattern))).is_some(),
            "{name}: {pattern:?} should panic",
        );
    }
}

#[test]
fn names_what_is_wrong_with_a_literal() {
    assert_eq!(
        panic_message(|| drop(SubjectScheme::new("/books.v2/{book}"))).as_deref(),
        Some(
            r#"architecturekit: segment "books.v2" in "/books.v2/{book}" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#
        ),
    );

    // A brace means a placeholder that is not the whole segment, which says
    // more than the characters.
    assert_eq!(
        panic_message(|| drop(SubjectScheme::new("/workshop/pre{id}"))).as_deref(),
        Some(r#"architecturekit: segment "pre{id}" in "/workshop/pre{id}" is malformed"#),
    );
}

#[test]
fn names_what_is_wrong_with_a_placeholder() {
    for (pattern, expected) in [
        (
            "/{a b}",
            r#"architecturekit: placeholder "a b" in "/{a b}" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#,
        ),
        (
            "/{\u{00e4}}",
            "architecturekit: placeholder \"\u{00e4}\" in \"/{\u{00e4}}\" may only contain A-Z, a-z, 0-9, underscores, and hyphens",
        ),
        (
            "/{.}",
            r#"architecturekit: placeholder "." in "/{.}" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#,
        ),
        (
            "/x/{*}",
            r#"architecturekit: placeholder "*" in "/x/{*}" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#,
        ),
        (
            "/books/{book.id}",
            r#"architecturekit: placeholder "book.id" in "/books/{book.id}" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#,
        ),
        // A brace that does not open or close the segment means a mistake in
        // the braces, which says more than the characters.
        (
            "/{a}}",
            r#"architecturekit: segment "{a}}" in "/{a}}" is malformed"#,
        ),
        (
            "/{{a}}",
            r#"architecturekit: segment "{{a}}" in "/{{a}}" is malformed"#,
        ),
        (
            "/{a{b}",
            r#"architecturekit: segment "{a{b}" in "/{a{b}" is malformed"#,
        ),
        (
            "/{a}{b}",
            r#"architecturekit: segment "{a}{b}" in "/{a}{b}" is malformed"#,
        ),
        (
            "/{}}",
            r#"architecturekit: segment "{}}" in "/{}}" is malformed"#,
        ),
        (
            "/{{}",
            r#"architecturekit: segment "{{}" in "/{{}" is malformed"#,
        ),
        (
            "/{a}b",
            r#"architecturekit: segment "{a}b" in "/{a}b" is malformed"#,
        ),
        ("/{", r#"architecturekit: segment "{" in "/{" is malformed"#),
        ("/}", r#"architecturekit: segment "}" in "/}" is malformed"#),
        (
            "/a}",
            r#"architecturekit: segment "a}" in "/a}" is malformed"#,
        ),
        (
            "/{}",
            r#"architecturekit: pattern "/{}" has an unnamed placeholder"#,
        ),
    ] {
        assert_eq!(
            panic_message(|| drop(SubjectScheme::new(pattern))).as_deref(),
            Some(expected),
            "{pattern:?}",
        );
    }
}

#[test]
fn accepts_placeholder_names_with_every_character_the_database_allows() {
    let scheme =
        SubjectScheme::new("/tenant/{Tenant_2}/agreements/{license-agreement}/{_}/{-}/{0}");

    assert_eq!(
        scheme.placeholders(),
        ["Tenant_2", "license-agreement", "_", "-", "0"]
    );

    let subject = scheme.build(&["acme", "42", "a", "b", "c"]);
    assert_eq!(subject, "/tenant/acme/agreements/42/a/b/c");

    let values = scheme.match_subject(&subject).expect("the subject matches");
    assert_eq!(
        values,
        HashMap::from([
            ("Tenant_2", "acme"),
            ("license-agreement", "42"),
            ("_", "a"),
            ("-", "b"),
            ("0", "c"),
        ]),
    );

    assert_eq!(scheme.value(&subject, "license-agreement"), Some("42"));

    let _ = SubjectScheme::new(&format!("/{{{DATABASE_CHARACTERS}}}"));
}

#[test]
fn accepts_the_placeholder_names_that_applications_use() {
    for name in [
        "user",
        "todo",
        "customer",
        "license-agreement",
        "master-agreement",
        "migration-ticket",
        "trial-license",
        "instance",
        "hour",
        "access",
        "key",
    ] {
        let scheme = SubjectScheme::new(&format!("/things/{{{name}}}"));

        assert_eq!(
            scheme.value(&scheme.build(&["42"]), name),
            Some("42"),
            "{name}"
        );
    }
}

#[test]
fn accepts_literals_with_every_character_the_database_allows() {
    let scheme = SubjectScheme::new("/Tenant_2/{tenant}/work-shop/{workshop}");

    let subject = scheme.build(&["acme", "42"]);
    assert_eq!(subject, "/Tenant_2/acme/work-shop/42");

    let values = scheme.match_subject(&subject).expect("the subject matches");
    assert_eq!(
        values,
        HashMap::from([("tenant", "acme"), ("workshop", "42")])
    );
    assert_eq!(scheme.root(), "/Tenant_2");
}

#[test]
fn panics_on_bad_values() {
    let scheme = SubjectScheme::new("/workshop/{workshop}");

    for (name, values) in [
        ("too few", vec![]),
        ("too many", vec!["a", "b"]),
        ("empty", vec![""]),
        ("contains a slash", vec!["with/slash"]),
    ] {
        assert!(
            panic_message(|| drop(scheme.build(&values))).is_some(),
            "{name} should panic",
        );
    }
}

#[test]
fn checks_values() {
    let scheme = SubjectScheme::new("/workshop/{workshop}");

    assert!(scheme.check(&["42"]).is_ok(), "values that fit should pass");

    for (name, values) in [
        ("too few", vec![]),
        ("too many", vec!["a", "b"]),
        ("empty", vec![""]),
        ("contains a slash", vec!["with/slash"]),
    ] {
        assert!(scheme.check(&values).is_err(), "{name} should be refused");
    }
}

#[test]
fn names_the_placeholder_of_a_bad_value() {
    let error = workshops()
        .check(&["acme", ""])
        .expect_err("an empty value is refused");

    assert!(error.to_string().contains(r#""workshop""#), "{error}");
}

#[test]
fn says_which_characters_a_value_may_contain() {
    let scheme = workshops();

    assert_eq!(
        scheme
            .check(&["acme", "es.cqrs"])
            .expect_err("a dot is refused")
            .to_string(),
        r#"value "es.cqrs" for "workshop" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#,
    );
    assert_eq!(
        panic_message(|| drop(scheme.build(&["acme", "es.cqrs"]))).as_deref(),
        Some(concat!(
            r#"architecturekit: value "es.cqrs" for "workshop" in "/tenant/{tenant}/workshop/{workshop}" "#,
            "may only contain A-Z, a-z, 0-9, underscores, and hyphens",
        )),
    );
}

#[test]
fn names_only_the_placeholder_when_it_checks_but_the_crate_and_the_pattern_when_it_builds() {
    // What check returns may reach the caller of an API, while what build
    // panics with is for the developer.
    let scheme = workshops();

    for (name, values, checked, built) in [
        (
            "too few values",
            vec!["acme"],
            "the subject needs 2 value(s), got 1",
            r#"architecturekit: pattern "/tenant/{tenant}/workshop/{workshop}" needs 2 value(s), got 1"#,
        ),
        (
            "too many values",
            vec!["acme", "es-cqrs", "more"],
            "the subject needs 2 value(s), got 3",
            r#"architecturekit: pattern "/tenant/{tenant}/workshop/{workshop}" needs 2 value(s), got 3"#,
        ),
        (
            "an empty value",
            vec!["", "es-cqrs"],
            r#"value for "tenant" must not be empty"#,
            r#"architecturekit: value for "tenant" in "/tenant/{tenant}/workshop/{workshop}" must not be empty"#,
        ),
        (
            "a value with a character the database does not allow",
            vec!["acme", "es/cqrs"],
            r#"value "es/cqrs" for "workshop" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#,
            concat!(
                r#"architecturekit: value "es/cqrs" for "workshop" in "/tenant/{tenant}/workshop/{workshop}" "#,
                "may only contain A-Z, a-z, 0-9, underscores, and hyphens",
            ),
        ),
    ] {
        let error = scheme.check(&values).expect_err(name);
        assert_eq!(error.to_string(), checked, "{name}");
        assert!(
            std::error::Error::source(&error).is_none(),
            "{name}: the error wraps nothing"
        );
        assert_eq!(
            panic_message(|| drop(scheme.build(&values))).as_deref(),
            Some(built),
            "{name}"
        );
    }
}

#[test]
fn refuses_values_with_characters_the_database_does_not_allow() {
    let scheme = SubjectScheme::new("/workshop/{workshop}");

    for (name, value) in [
        ("dot", "a.b"),
        ("only a dot", "."),
        ("only dots", ".."),
        ("space", "with space"),
        ("umlaut", "\u{00fc}mlaut"),
        ("colon", "a:b"),
        ("at sign", "a@b"),
        ("tilde", "a~b"),
        ("asterisk", "*"),
        ("question mark", "a?b"),
        ("hash", "a#b"),
        ("encoded slash", "a%2Fb"),
        ("comma", "a,b"),
        ("line feed", "a\nb"),
        ("null character", "a\0b"),
    ] {
        assert_eq!(
            scheme.check(&[value]).expect_err(name).to_string(),
            format!(
                r#"value {value:?} for "workshop" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#
            ),
            "{name}",
        );
        assert_eq!(
            panic_message(|| drop(scheme.build(&[value]))),
            Some(format!(
                r#"architecturekit: value {value:?} for "workshop" in "/workshop/{{workshop}}" may only contain A-Z, a-z, 0-9, underscores, and hyphens"#
            )),
            "{name}",
        );
    }
}

#[test]
fn accepts_values_with_characters_the_database_allows() {
    let scheme = SubjectScheme::new("/workshop/{workshop}");

    for value in [
        "UPPER",
        "lower",
        "MiXeD",
        "0123456789",
        "a_b",
        "a-b",
        "_",
        "-",
        "-_-",
        DATABASE_CHARACTERS,
    ] {
        assert!(scheme.check(&[value]).is_ok(), "{value:?} should pass");

        let subject = scheme.build(&[value]);
        assert_eq!(subject, format!("/workshop/{value}"));

        let values = scheme.match_subject(&subject).expect("the subject matches");
        assert_eq!(values["workshop"], value);
    }
}

#[test]
fn allows_exactly_the_characters_the_database_allows() {
    let scheme = SubjectScheme::new("/workshop/{workshop}");

    for character in some_characters() {
        // In the middle, so that neither the first nor the last character alone
        // decides.
        let value = format!("a{character}b");
        let is_allowed = DATABASE_CHARACTERS.contains(character);

        assert_eq!(
            scheme.check(&[&value]).is_ok(),
            is_allowed,
            "check of {value:?}"
        );
        assert_eq!(
            panic_message(|| drop(scheme.build(&[&value]))).is_none(),
            is_allowed,
            "build of {value:?}",
        );
        assert_eq!(
            scheme
                .match_subject(&format!("/workshop/{value}"))
                .is_some(),
            is_allowed,
            "match of {value:?}",
        );

        // A slash in a literal separates two segments rather than being part of
        // one.
        if character == '/' {
            continue;
        }

        let literal = format!("/{value}/{{workshop}}");
        assert_eq!(
            panic_message(|| drop(SubjectScheme::new(&literal))).is_none(),
            is_allowed,
            "literal {value:?}",
        );

        let placeholder = format!("/workshop/{{{value}}}");
        assert_eq!(
            panic_message(|| drop(SubjectScheme::new(&placeholder))).is_none(),
            is_allowed,
            "placeholder {value:?}",
        );
    }
}

#[test]
fn reports_its_root() {
    for (pattern, root) in [
        ("/instances/{instance}", "/instances"),
        ("/tenant/{tenant}/workshop/{workshop}", "/tenant"),
        ("/system/tenants/{tenant}", "/system/tenants"),
        ("/{tenant}/orders/{order}", "/"),
        ("/system/health", "/system/health"),
    ] {
        assert_eq!(SubjectScheme::new(pattern).root(), root, "{pattern}");
    }
}

#[test]
fn builds_every_subject_under_its_root() {
    // A recursive read from a subject covers that subject and every one below
    // it, so this is what makes reading from the root complete.
    for scheme in [
        workshops(),
        SubjectScheme::new("/{tenant}/orders/{order}"),
        SubjectScheme::new("/system/health"),
    ] {
        let values = vec!["value"; scheme.placeholders().len()];
        let subject = scheme.build(&values);
        let root = scheme.root();

        let is_under = subject == root || root == "/" || subject.starts_with(&format!("{root}/"));
        assert!(is_under, "{subject:?} is not under {root:?}");
    }
}

#[test]
fn reports_its_pattern() {
    let pattern = "/tenant/{tenant}/workshop/{workshop}";

    assert_eq!(SubjectScheme::new(pattern).pattern(), pattern);
}

#[test]
fn takes_the_value_of_a_placeholder_out_of_a_subject() {
    let scheme = workshops();

    assert_eq!(
        scheme.value("/tenant/acme/workshop/es-cqrs", "tenant"),
        Some("acme")
    );
    assert_eq!(
        scheme.value("/tenant/acme/workshop/es-cqrs", "workshop"),
        Some("es-cqrs")
    );
}

#[test]
fn reports_none_for_a_subject_that_does_not_follow_the_pattern_as_match_subject_does() {
    let scheme = workshops();

    for subject in [
        "tenant/acme/workshop/es-cqrs",
        "/tenant/acme/workshop",
        "/client/acme/workshop/es-cqrs",
        "/tenant/acme/workshop/es.cqrs",
    ] {
        assert!(scheme.match_subject(subject).is_none(), "{subject:?}");
        assert_eq!(scheme.value(subject, "tenant"), None, "{subject:?}");
    }
}

#[test]
fn panics_for_a_placeholder_the_pattern_does_not_have() {
    let scheme = workshops();
    let message = r#"architecturekit: pattern "/tenant/{tenant}/workshop/{workshop}" has no placeholder "tennant""#;

    assert_eq!(
        panic_message(|| {
            let _ = scheme.value("/tenant/acme/workshop/es-cqrs", "tennant");
        })
        .as_deref(),
        Some(message),
    );

    // The mistake is in the code, so it is found whatever the subject is.
    assert_eq!(
        panic_message(|| {
            let _ = scheme.value("/client/acme", "tennant");
        })
        .as_deref(),
        Some(message),
    );
}

#[test]
fn panics_for_a_literal_segment_or_a_placeholder_in_braces_neither_of_which_is_the_name_of_one() {
    let books = SubjectScheme::new("/books/{book}");

    for name in ["books", "{book}", ""] {
        assert_eq!(
            panic_message(|| {
                let _ = books.value("/books/42", name);
            }),
            Some(format!(
                r#"architecturekit: pattern "/books/{{book}}" has no placeholder {name:?}"#
            )),
        );
    }
}
