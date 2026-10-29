// These tests check check_read_options, which Store::read applies before it
// asks the database: every combination of bounds, empty ranges, also at the
// largest revision and from the latest event, and contradicting options. The
// tests of the store show that it does.
//
// The options of a read are an enum per bound and order, so two lower bounds,
// two upper bounds, two orders, and an IfLatestEventIsMissing that is neither
// of its values can not be written.

use crate::{
    Categorized, IfLatestEventIsMissing, LowerBound, Order, ReadOptions, ReadOptionsError,
    Revision, Subject, Subjects, UpperBound, check_read_options,
};

const LARGEST_REVISION: &str = "9223372036854775807";

/// Reads a revision that a test spells out.
fn revision(id: &str) -> Revision {
    id.parse().expect("the test spells out a revision")
}

fn from(id: &str) -> LowerBound {
    LowerBound::FromEvent(revision(id))
}

fn after(id: &str) -> LowerBound {
    LowerBound::AfterEvent(revision(id))
}

fn up_to(id: &str) -> UpperBound {
    UpperBound::UpToEvent(revision(id))
}

fn before(id: &str) -> UpperBound {
    UpperBound::BeforeEvent(revision(id))
}

fn bounded(lower_bound: LowerBound, upper_bound: UpperBound) -> ReadOptions {
    ReadOptions {
        lower_bound,
        upper_bound,
        ..ReadOptions::default()
    }
}

fn from_latest(if_missing: IfLatestEventIsMissing) -> LowerBound {
    LowerBound::FromLatestEvent {
        subject: Subject::new("/books/42"),
        event_type: "io.thenativeweb.test.reset".to_owned(),
        if_missing,
    }
}

/// Returns the text `check_read_options` refuses the options with, or `None` if
/// it accepts them.
fn refusal_of(options: &ReadOptions) -> Option<String> {
    check_read_options(options)
        .err()
        .map(|refusal| refusal.to_string())
}

/// Every combination of the four kinds of bounds, alone and together, in
/// arrangements that leave room for an event and ones that do not, with the
/// text of the refusal, if there is one.
#[expect(
    clippy::too_many_lines,
    reason = "the table holds every combination of bounds"
)]
fn bounded_reads() -> Vec<(&'static str, ReadOptions, Option<&'static str>)> {
    use LowerBound::Unbounded as NoLowerBound;
    use UpperBound::Unbounded as NoUpperBound;

    vec![
        // A lower bound alone leaves room, since events can still come, unless
        // it is after the largest revision, which no event can ever come after.
        ("FromEvent alone", bounded(from("2"), NoUpperBound), None),
        ("AfterEvent alone", bounded(after("2"), NoUpperBound), None),
        (
            "AfterEvent of the largest revision alone",
            bounded(after(LARGEST_REVISION), NoUpperBound),
            Some(r#"empty range: no event can lie after "9223372036854775807""#),
        ),
        // An upper bound alone leaves no room only before the first event.
        (
            "UpToEvent of the first event",
            bounded(NoLowerBound, up_to("0")),
            None,
        ),
        (
            "BeforeEvent of the second event",
            bounded(NoLowerBound, before("1")),
            None,
        ),
        (
            "BeforeEvent of the first event",
            bounded(NoLowerBound, before("0")),
            Some(r#"empty range: no event can lie before "0""#),
        ),
        (
            "FromEvent and UpToEvent of the same event",
            bounded(from("1"), up_to("1")),
            None,
        ),
        (
            "FromEvent and UpToEvent in order",
            bounded(from("1"), up_to("2")),
            None,
        ),
        (
            "FromEvent and UpToEvent the wrong way round",
            bounded(from("2"), up_to("1")),
            Some(r#"empty range: no event can lie from "2" up to "1""#),
        ),
        (
            "FromEvent and BeforeEvent of the next event",
            bounded(from("1"), before("2")),
            None,
        ),
        (
            "FromEvent and BeforeEvent of the same event",
            bounded(from("1"), before("1")),
            Some(r#"empty range: no event can lie from "1" and before "1""#),
        ),
        (
            "FromEvent and BeforeEvent the wrong way round",
            bounded(from("2"), before("1")),
            Some(r#"empty range: no event can lie from "2" and before "1""#),
        ),
        (
            "FromEvent and BeforeEvent of the first event",
            bounded(from("0"), before("0")),
            Some(r#"empty range: no event can lie from "0" and before "0""#),
        ),
        (
            "AfterEvent and UpToEvent of the next event",
            bounded(after("0"), up_to("1")),
            None,
        ),
        (
            "AfterEvent and UpToEvent of the same event",
            bounded(after("1"), up_to("1")),
            Some(r#"empty range: no event can lie after "1" and up to "1""#),
        ),
        (
            "AfterEvent and UpToEvent the wrong way round",
            bounded(after("2"), up_to("1")),
            Some(r#"empty range: no event can lie after "2" and up to "1""#),
        ),
        (
            "AfterEvent and BeforeEvent with one event between them",
            bounded(after("0"), before("2")),
            None,
        ),
        (
            "AfterEvent and BeforeEvent of neighboring events",
            bounded(after("0"), before("1")),
            Some(r#"empty range: no event can lie after "0" and before "1""#),
        ),
        (
            "AfterEvent and BeforeEvent of the same event",
            bounded(after("1"), before("1")),
            Some(r#"empty range: no event can lie after "1" and before "1""#),
        ),
        (
            "AfterEvent and BeforeEvent the wrong way round",
            bounded(after("5"), before("0")),
            Some(r#"empty range: no event can lie after "5" and before "0""#),
        ),
        (
            "IDs read with leading zeros, which are named as their numbers",
            bounded(after("007"), before("8")),
            Some(r#"empty range: no event can lie after "7" and before "8""#),
        ),
    ]
}

#[test]
fn refuses_exactly_the_bounds_that_leave_no_room_for_an_event() {
    for (name, options, refusal) in bounded_reads() {
        assert_eq!(refusal_of(&options).as_deref(), refusal, "{name}");

        let refused = check_read_options(&options);
        if let Err(refused) = refused {
            assert!(
                matches!(refused, ReadOptionsError::EmptyRange(_)),
                "{name}: {refused:?}"
            );
            assert_eq!(
                refused.category(),
                None,
                "{name}: it belongs to no category"
            );
        }
    }
}

#[test]
fn refuses_the_same_ranges_newest_first_since_the_order_does_not_change_the_bounds() {
    for (name, options, refusal) in bounded_reads() {
        let newest_first = ReadOptions {
            order: Order::NewestFirst,
            ..options
        };

        assert_eq!(refusal_of(&newest_first).as_deref(), refusal, "{name}");
    }
}

#[test]
fn refuses_empty_ranges_at_the_largest_revision() {
    // A revision ends at 2^63-1, so the range after the largest one starts at
    // 2^63, which still fits the numbers the bounds are compared as, but which
    // no event can ever reach.
    for (name, options, refusal) in [
        (
            "AfterEvent alone",
            bounded(after(LARGEST_REVISION), UpperBound::Unbounded),
            Some(r#"empty range: no event can lie after "9223372036854775807""#),
        ),
        (
            "AfterEvent of the one before alone",
            bounded(after("9223372036854775806"), UpperBound::Unbounded),
            None,
        ),
        (
            "FromEvent alone",
            bounded(from(LARGEST_REVISION), UpperBound::Unbounded),
            None,
        ),
        (
            "UpToEvent alone",
            bounded(LowerBound::Unbounded, up_to(LARGEST_REVISION)),
            None,
        ),
        (
            "BeforeEvent alone",
            bounded(LowerBound::Unbounded, before(LARGEST_REVISION)),
            None,
        ),
        (
            "FromEvent and UpToEvent of it",
            bounded(from(LARGEST_REVISION), up_to(LARGEST_REVISION)),
            None,
        ),
        (
            "AfterEvent of the one before and UpToEvent of it",
            bounded(after("9223372036854775806"), up_to(LARGEST_REVISION)),
            None,
        ),
        (
            "FromEvent of the first event and UpToEvent of it",
            bounded(from("0"), up_to(LARGEST_REVISION)),
            None,
        ),
        (
            "AfterEvent and UpToEvent of it",
            bounded(after(LARGEST_REVISION), up_to(LARGEST_REVISION)),
            Some(
                r#"empty range: no event can lie after "9223372036854775807" and up to "9223372036854775807""#,
            ),
        ),
        (
            "AfterEvent and BeforeEvent of it",
            bounded(after(LARGEST_REVISION), before(LARGEST_REVISION)),
            Some(
                r#"empty range: no event can lie after "9223372036854775807" and before "9223372036854775807""#,
            ),
        ),
        (
            "FromEvent and BeforeEvent of it",
            bounded(from(LARGEST_REVISION), before(LARGEST_REVISION)),
            Some(
                r#"empty range: no event can lie from "9223372036854775807" and before "9223372036854775807""#,
            ),
        ),
        (
            "AfterEvent of it and BeforeEvent of the first event",
            bounded(after(LARGEST_REVISION), before("0")),
            Some(r#"empty range: no event can lie after "9223372036854775807" and before "0""#),
        ),
    ] {
        assert_eq!(refusal_of(&options).as_deref(), refusal, "{name}");
    }
}

#[test]
fn refuses_only_an_upper_bound_before_the_first_event_from_the_latest_event() {
    // The lower bound is only known once the database has found the latest
    // event of the type, so only an upper bound that leaves no room on its own
    // is refused.
    let latest = || from_latest(IfLatestEventIsMissing::ReadEverything);

    assert_eq!(
        refusal_of(&bounded(latest(), before("0"))).as_deref(),
        Some(r#"empty range: no event can lie before "0""#)
    );
    for upper_bound in [up_to("0"), before("1"), UpperBound::Unbounded] {
        assert_eq!(
            refusal_of(&bounded(latest(), upper_bound.clone())),
            None,
            "{upper_bound:?}"
        );
    }
}

#[test]
fn accepts_the_options_that_leave_room_for_an_event() {
    for options in [
        ReadOptions::default(),
        bounded(from("0"), up_to("5")),
        bounded(from_latest(IfLatestEventIsMissing::ReadNothing), up_to("5")),
        ReadOptions {
            order: Order::NewestFirst,
            ..bounded(after("1"), before("5"))
        },
    ] {
        assert_eq!(refusal_of(&options), None, "{options:?}");
    }
}

#[test]
#[should_panic(
    expected = r#"architecturekit: the database reads from the latest event of a type only oldest first, but got FromLatestEvent("/books/42", "io.thenativeweb.test.reset") and NewestFirst"#
)]
fn panics_for_the_latest_event_of_a_type_newest_first() {
    let _ = check_read_options(&ReadOptions {
        lower_bound: from_latest(IfLatestEventIsMissing::ReadEverything),
        order: Order::NewestFirst,
        ..ReadOptions::default()
    });
}

#[test]
#[should_panic(expected = "only oldest first")]
fn panics_before_it_checks_the_range() {
    let _ = check_read_options(&ReadOptions {
        lower_bound: from_latest(IfLatestEventIsMissing::ReadEverything),
        upper_bound: before("0"),
        order: Order::NewestFirst,
    });
}

#[test]
fn tells_a_subject_tree_from_an_exact_subject() {
    let tree = Subjects::tree(Subject::new("/books"));
    let exact = Subjects::exact(Subject::new("/books"));

    assert_eq!(tree.subject(), &Subject::new("/books"));
    assert!(tree.is_tree());
    assert!(!exact.is_tree());
    assert_ne!(tree, exact);
}
