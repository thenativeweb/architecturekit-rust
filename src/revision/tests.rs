// These tests check Revision, revision_of, and wait_for_written. The tests of
// the revision of a view in memory come with the view. Waiting ends when its
// future is dropped.

use std::{
    collections::HashSet,
    error::Error,
    fmt,
    future::{self, Future},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde_json::json;

use crate::{
    Categorized, Category, InMemoryView, NotARevision, NotCaughtUp, Revision, Revisioned,
    projection::tests::stored, revision_of, wait_for_written,
};

/// Values that are not revisions, other than the empty one.
pub(crate) const NOT_REVISIONS: [(&str, &str); 10] = [
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

/// The largest event ID the database hands out.
const LARGEST: u64 = (1 << 63) - 1;

#[test]
fn reads_a_revision_from_its_decimal() {
    for (name, revision, expected) in [
        ("the first event ID", "0", 0),
        ("a single digit", "7", 7),
        ("several digits", "10", 10),
        ("leading zeros", "002", 2),
        ("the largest event ID", "9223372036854775807", LARGEST),
    ] {
        assert_eq!(
            revision.parse::<Revision>().map(Revision::get),
            Ok(expected),
            "{name}"
        );
    }
}

#[test]
fn refuses_the_empty_string_which_is_no_event_id() {
    let refusal = ""
        .parse::<Revision>()
        .expect_err("the empty string is no event ID");

    assert_eq!(refusal.to_string(), r#"not a revision: """#);
    assert_eq!(refusal.revision(), "");
}

#[test]
fn refuses_what_is_not_a_revision_naming_the_value() {
    for (name, revision) in NOT_REVISIONS {
        let refusal = revision.parse::<Revision>().expect_err(name);

        assert_eq!(
            refusal.to_string(),
            format!("not a revision: {revision:?}"),
            "{name}"
        );
        assert_eq!(refusal.revision(), revision, "{name}");
    }
}

#[test]
fn a_value_that_is_not_a_revision_belongs_to_no_category() {
    let refusal = "abc"
        .parse::<Revision>()
        .expect_err("letters are no event ID");

    assert_eq!(refusal.category(), None);
}

#[test]
fn takes_every_u32_as_a_revision() {
    for number in [0, 1, 42, u32::MAX] {
        assert_eq!(Revision::from(number).get(), u64::from(number));
    }
}

#[test]
fn takes_a_u64_up_to_the_largest_event_id() {
    for number in [0, 1, u64::from(u32::MAX) + 1, LARGEST - 1, LARGEST] {
        assert_eq!(
            Revision::try_from(number).map(Revision::get),
            Ok(number),
            "{number}"
        );
    }
}

#[test]
fn refuses_a_u64_beyond_the_largest_event_id_naming_the_value() {
    for number in [LARGEST + 1, u64::MAX] {
        let refusal = Revision::try_from(number).expect_err("the database never hands it out");

        assert_eq!(refusal.revision(), number.to_string());
        assert_eq!(refusal.to_string(), format!("not a revision: \"{number}\""));
    }
}

#[test]
fn writes_a_revision_as_its_decimal_without_leading_zeros() {
    for (revision, expected) in [
        ("0", "0"),
        ("7", "7"),
        ("002", "2"),
        ("9223372036854775807", "9223372036854775807"),
    ] {
        let revision: Revision = revision.parse().expect("it is a revision");

        assert_eq!(revision.to_string(), expected);
    }
}

#[test]
fn writes_a_revision_with_the_width_and_alignment_of_a_number() {
    assert_eq!(format!("{:>4}", Revision::from(42)), "  42");
    assert_eq!(format!("{:<4}|", Revision::from(42)), "42  |");
}

#[test]
fn revisions_sort_as_numbers_not_as_text() {
    let revision = |text: &str| text.parse::<Revision>().expect("it is a revision");

    // The trap this exists for: as text, "10" sorts before "9".
    assert!(revision("9") < revision("10"));
    assert!(revision("100") > revision("99"));
    assert!(revision("9223372036854775806") < revision("9223372036854775807"));
    assert_eq!(revision("002"), revision("2"));

    let mut revisions: Vec<Revision> = ["10", "9", "100", "0", "99"]
        .into_iter()
        .map(revision)
        .collect();
    revisions.sort();

    assert_eq!(
        revisions,
        [0, 9, 10, 99, 100].map(Revision::from),
        "revisions sort by their numbers"
    );
}

#[test]
fn revisions_of_the_same_number_are_the_same_whatever_their_text() {
    let revisions: HashSet<Revision> = ["2", "02", "002"]
        .into_iter()
        .map(|text| text.parse().expect("it is a revision"))
        .collect();

    assert_eq!(revisions.len(), 1);
}

#[test]
fn a_revision_reads_back_what_it_writes() {
    for number in [0, 1, 9, 10, 4_294_967_296, LARGEST] {
        let revision = Revision::try_from(number).expect("it is a revision");

        assert_eq!(revision.to_string().parse(), Ok(revision), "{number}");
    }
}

#[test]
fn serde_writes_a_revision_as_a_string_of_its_decimal() {
    assert_eq!(
        serde_json::to_value(Revision::from(42)).expect("a revision is JSON"),
        json!("42")
    );
    assert_eq!(
        serde_json::to_string(&Some(Revision::from(7))).expect("a revision is JSON"),
        r#""7""#
    );
}

#[test]
fn serde_reads_a_revision_from_a_string_by_the_rules_of_parse() {
    for (text, expected) in [
        ("0", 0),
        ("42", 42),
        ("007", 7),
        ("9223372036854775807", LARGEST),
    ] {
        let revision: Revision =
            serde_json::from_value(json!(text)).expect("the string is a revision");

        assert_eq!(revision.get(), expected, "{text}");
    }
}

#[test]
fn serde_reads_back_what_it_writes() {
    for number in [0, 41, LARGEST] {
        let revision = Revision::try_from(number).expect("it is a revision");
        let written = serde_json::to_string(&revision).expect("a revision is JSON");

        assert_eq!(
            serde_json::from_str::<Revision>(&written).expect("it reads back"),
            revision
        );
    }
}

#[test]
fn serde_refuses_a_string_that_is_not_a_revision_with_its_text() {
    for (name, revision) in [("an empty one", "")].into_iter().chain(NOT_REVISIONS) {
        let failure = serde_json::from_value::<Revision>(json!(revision)).expect_err(name);

        assert_eq!(
            failure.to_string(),
            format!("not a revision: {revision:?}"),
            "{name}"
        );
    }
}

#[test]
fn serde_refuses_a_number_since_the_database_writes_event_ids_as_strings() {
    let failure =
        serde_json::from_value::<Revision>(json!(42)).expect_err("a revision is a string");

    assert_eq!(
        failure.to_string(),
        "invalid type: integer `42`, expected a revision, which is a string of a decimal from 0 to 2^63-1"
    );
}

#[test]
fn a_refusal_names_the_value_it_refuses() {
    let refusal: NotARevision = "soon".parse::<Revision>().expect_err("soon is no event ID");

    assert_eq!(refusal.revision(), "soon");
    assert!(refusal.source().is_none(), "it wraps nothing else");
}

/// Stored events with the given IDs.
fn written(ids: &[&str]) -> Vec<eventsourcingdb::Event> {
    ids.iter()
        .map(|id| stored(id, "io.thenativeweb.test.book-acquired", json!({})))
        .collect()
}

#[test]
fn the_revision_of_a_write_is_its_highest_event_id() {
    for (name, ids, want) in [
        ("nothing written", &[][..], None),
        ("one event", &["7"][..], Some(7)),
        ("two events", &["7", "8"][..], Some(8)),
        // Order is not assumed, and numbers decide, not text.
        ("out of order", &["10", "9"][..], Some(10)),
        ("nonsense is skipped", &["x", "3"][..], Some(3)),
    ] {
        assert_eq!(
            revision_of(&written(ids)),
            want.map(Revision::from),
            "{name}"
        );
    }
}

/// Returns a view that has seen the event with the given ID.
fn view_at(revision: u32) -> Arc<InMemoryView<String, String>> {
    let view = Arc::new(InMemoryView::new(String::clone));
    view.seen(Revision::from(revision));

    view
}

/// An error of a view that carries more than its text.
#[derive(Debug, PartialEq)]
struct BrokenView {
    reason: &'static str,
    category: Option<Category>,
}

impl fmt::Display for BrokenView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.reason)
    }
}

impl Error for BrokenView {}

impl Categorized for BrokenView {
    fn category(&self) -> Option<Category> {
        self.category
    }
}

/// A view whose `wait_for` answers at once with what the given function
/// returns, and records the revisions it was asked for.
struct WaitingView<F> {
    wait_for: F,
    asked: Mutex<Vec<Revision>>,
}

impl<F> WaitingView<F>
where
    F: Fn() -> Result<(), BrokenView> + Send + Sync,
{
    fn new(wait_for: F) -> Self {
        Self {
            wait_for,
            asked: Mutex::new(Vec::new()),
        }
    }

    fn asked(&self) -> Vec<Revision> {
        self.asked
            .lock()
            .expect("no test panics while asking")
            .clone()
    }
}

impl<F> Revisioned for WaitingView<F>
where
    F: Fn() -> Result<(), BrokenView> + Send + Sync,
{
    type Error = BrokenView;

    fn revision(&self) -> impl Future<Output = Result<Option<Revision>, BrokenView>> + Send {
        future::ready(Ok(None))
    }

    fn wait_for(&self, revision: Revision) -> impl Future<Output = Result<(), BrokenView>> + Send {
        self.asked
            .lock()
            .expect("no test panics while asking")
            .push(revision);

        future::ready((self.wait_for)())
    }
}

#[tokio::test]
async fn returns_at_once_if_the_view_has_seen_the_events_already() {
    for revision in [5, 6] {
        assert!(
            matches!(
                wait_for_written(
                    &*view_at(revision),
                    &written(&["5"]),
                    Duration::from_secs(3600)
                )
                .await,
                Ok(())
            ),
            "a view at {revision}"
        );
    }
}

#[tokio::test]
async fn returns_although_the_time_has_run_out_if_the_view_has_seen_the_events_already() {
    let waited = wait_for_written(&*view_at(5), &written(&["5"]), Duration::from_nanos(1)).await;

    assert!(matches!(waited, Ok(())), "{waited:?}");
}

#[tokio::test]
async fn waits_for_the_highest_id_among_the_events() {
    let view = WaitingView::new(|| Ok(()));

    let waited = wait_for_written(
        &view,
        &written(&["9", "10", "2"]),
        Duration::from_secs(3600),
    )
    .await;

    assert!(matches!(waited, Ok(())), "{waited:?}");
    assert_eq!(view.asked(), [Revision::from(10)]);
}

#[tokio::test]
async fn returns_once_the_view_has_caught_up() {
    let view = view_at(4);

    let catching_up = {
        let view = Arc::clone(&view);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            view.seen(Revision::from(5));
        })
    };

    let waited = wait_for_written(&*view, &written(&["5"]), Duration::from_secs(5)).await;

    assert!(matches!(waited, Ok(())), "{waited:?}");
    catching_up.await.expect("the view catches up");
}

#[tokio::test]
async fn returns_without_waiting_if_no_events_were_written() {
    let view = WaitingView::new(|| {
        Err(BrokenView {
            reason: "the view must not be asked",
            category: None,
        })
    });

    let waited = wait_for_written(&view, &[], Duration::from_secs(3600)).await;

    assert!(matches!(waited, Ok(())), "{waited:?}");
    assert_eq!(
        view.asked(),
        Vec::<Revision>::new(),
        "the view must not be asked"
    );
}

#[tokio::test]
async fn fails_with_not_caught_up_if_the_view_does_not_catch_up_in_time() {
    let started = Instant::now();

    let waited = wait_for_written(&*view_at(4), &written(&["5"]), Duration::from_millis(50)).await;

    let Err(failure @ NotCaughtUp::TimedOut(timeout)) = waited else {
        panic!("the view has not caught up: {waited:?}");
    };
    assert_eq!(timeout, Duration::from_millis(50));
    assert_eq!(
        failure.to_string(),
        "not caught up: the events were written, but the view did not catch up within 50ms"
    );
    assert!(failure.source().is_none(), "it wraps nothing else");
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "it has to wait for the full time"
    );
}

#[tokio::test]
async fn belongs_to_no_category_if_the_view_does_not_catch_up_in_time_since_the_write_has_succeeded()
 {
    let waited = wait_for_written(&*view_at(4), &written(&["5"]), Duration::from_millis(20)).await;

    let failure = waited.expect_err("the view has not caught up");
    assert_eq!(
        failure.category(),
        None,
        "trying the write again would store its events twice, and the view may still catch up"
    );
}

#[tokio::test]
async fn names_the_timeout_the_way_a_duration_prints() {
    let waited =
        wait_for_written(&*view_at(4), &written(&["5"]), Duration::from_micros(1500)).await;

    assert_eq!(
        waited.expect_err("the view has not caught up").to_string(),
        "not caught up: the events were written, but the view did not catch up within 1.5ms"
    );
}

#[tokio::test]
async fn fails_with_not_caught_up_that_wraps_any_other_error_of_the_view() {
    let view = WaitingView::new(|| {
        Err(BrokenView {
            reason: "the index is gone",
            category: None,
        })
    });

    let waited = wait_for_written(&view, &written(&["5"]), Duration::from_millis(20)).await;

    let Err(failure @ NotCaughtUp::View(_)) = waited else {
        panic!("the view could not be asked: {waited:?}");
    };
    assert_eq!(
        failure.to_string(),
        "not caught up: the events were written, but the view could not be asked"
    );
    assert_eq!(
        failure
            .source()
            .expect("the error of the view is the source")
            .to_string(),
        "the index is gone"
    );
    assert!(
        matches!(&failure, NotCaughtUp::View(broken) if broken.reason == "the index is gone"),
        "the error of the view has to be kept as it is"
    );
}

#[tokio::test]
async fn keeps_the_category_of_an_error_of_the_view_behind_not_caught_up() {
    // A view in a database may fail to read its revision for now.
    let view = WaitingView::new(|| {
        Err(BrokenView {
            reason: "polling the revision: connection refused",
            category: Some(Category::Transient),
        })
    });

    let waited = wait_for_written(&view, &written(&["5"]), Duration::from_secs(3600)).await;

    let failure = waited.expect_err("the view could not be asked");
    assert_eq!(
        failure.category(),
        None,
        "the write has succeeded, so it must not look like a failed one"
    );
    let NotCaughtUp::View(cause) = failure else {
        panic!("the view could not be asked: {failure:?}");
    };
    assert_eq!(cause.category(), Some(Category::Transient));
}

#[tokio::test]
async fn fails_with_not_caught_up_that_wraps_an_error_of_the_view_that_looks_like_a_timeout_if_the_time_has_not_run_out()
 {
    // A view backed by a database may time out on its own.
    let view = WaitingView::new(|| {
        Err(BrokenView {
            reason: "reading the revision: timed out",
            category: None,
        })
    });

    let waited = wait_for_written(&view, &written(&["5"]), Duration::from_secs(3600)).await;

    assert!(
        matches!(waited, Err(NotCaughtUp::View(_))),
        "the error of the view is not the timeout: {waited:?}"
    );
}

#[tokio::test]
#[should_panic(
    expected = "architecturekit: wait_for_written needs a timeout that is positive, not 0ns"
)]
async fn panics_for_a_timeout_that_is_not_positive_before_it_looks_at_the_events() {
    let _ = wait_for_written(&*view_at(5), &[], Duration::ZERO).await;
}
