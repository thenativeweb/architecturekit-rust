// These tests mirror TestCompareRevisions and TestParseRevision of
// ArchitectureKit for Go. The other tests of revision_test.go belong to views
// and projections, so they come with the reading side.

use std::cmp::Ordering;

use crate::{compare_revisions, parse_revision};

/// Values that are not revisions, other than the empty one, which
/// `compare_revisions` takes for a view that has seen nothing.
const NOT_REVISIONS: [(&str, &str); 10] = [
    ("letters", "abc"),
    ("a fraction", "1.5"),
    ("a negative number", "-1"),
    ("a leading space", " 1"),
    ("a trailing space", "1 "),
    ("a hexadecimal number", "0x10"),
    ("a plus sign", "+1"),
    ("one beyond the largest event ID", "9223372036854775808"),
    ("the largest 64-bit number", "18446744073709551615"),
    ("a number beyond 64 bits", "99999999999999999999"),
];

#[test]
fn revisions_are_compared_as_numbers() {
    for (name, left, right, expected) in [
        ("empty vs empty", "", "", Ordering::Equal),
        ("0 vs 0", "0", "0", Ordering::Equal),
        ("7 vs 7", "7", "7", Ordering::Equal),
        ("empty vs 0", "", "0", Ordering::Less),
        ("0 vs empty", "0", "", Ordering::Greater),
        ("1 vs 2", "1", "2", Ordering::Less),
        ("2 vs 1", "2", "1", Ordering::Greater),
        // The trap this exists for: as text, "10" sorts before "9".
        ("9 vs 10", "9", "10", Ordering::Less),
        ("10 vs 9", "10", "9", Ordering::Greater),
        ("100 vs 99", "100", "99", Ordering::Greater),
        (
            "the largest vs the one below",
            "9223372036854775807",
            "9223372036854775806",
            Ordering::Greater,
        ),
        ("leading zeros", "002", "2", Ordering::Equal),
    ] {
        assert_eq!(compare_revisions(left, right), Ok(expected), "{name}");
    }
}

#[test]
fn something_that_is_not_a_revision_is_refused() {
    for (name, revision) in NOT_REVISIONS {
        assert!(compare_revisions(revision, "1").is_err(), "{name}");

        // Both sides are checked, not only the first.
        assert!(
            compare_revisions("1", revision).is_err(),
            "{name}, on the right"
        );
    }
}

#[test]
fn returns_the_number_of_a_revision() {
    for (name, revision, expected) in [
        ("the first event ID", "0", 0),
        ("a single digit", "7", 7),
        ("several digits", "10", 10),
        ("leading zeros", "002", 2),
        ("the largest event ID", "9223372036854775807", (1 << 63) - 1),
    ] {
        assert_eq!(parse_revision(revision), Ok(expected), "{name}");
    }
}

#[test]
fn refuses_the_empty_string_which_is_no_event_id() {
    let refusal = parse_revision("").expect_err("the empty string is no event ID");

    assert_eq!(refusal.to_string(), r#"not a revision: """#);
    assert_eq!(refusal.revision(), "");
}

#[test]
fn refuses_what_is_not_a_revision_naming_the_value() {
    for (name, revision) in NOT_REVISIONS {
        let refusal = parse_revision(revision).expect_err(name);

        assert_eq!(
            refusal.to_string(),
            format!("not a revision: {revision:?}"),
            "{name}"
        );
        assert_eq!(refusal.revision(), revision, "{name}");
    }
}

#[test]
fn reads_a_revision_by_the_rules_compare_revisions_applies() {
    let values: Vec<&str> = [
        "0",
        "1",
        "2",
        "002",
        "9",
        "10",
        "99",
        "100",
        "9223372036854775806",
        "9223372036854775807",
    ]
    .into_iter()
    .chain(NOT_REVISIONS.map(|(_, revision)| revision))
    .collect();

    for left in &values {
        for right in &values {
            let order = compare_revisions(left, right);

            match (parse_revision(left), parse_revision(right)) {
                (Ok(left_number), Ok(right_number)) => {
                    assert_eq!(
                        order,
                        Ok(left_number.cmp(&right_number)),
                        "comparing {left:?} with {right:?}"
                    );
                }
                _ => assert!(
                    order.is_err(),
                    "compare_revisions accepts {left:?} and {right:?}, which parse_revision refuses",
                ),
            }
        }
    }
}
