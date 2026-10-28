// These tests check comparing and parsing revisions, revision_of, and
// wait_for_written. The tests of the revision of a view in memory come with
// the view. Waiting ends when its future is dropped.

use std::{
    cmp::Ordering,
    error::Error,
    fmt,
    future::{self, Future},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde_json::json;

use crate::{
    Categorized, Category, InMemoryView, NotCaughtUp, Revisioned, compare_revisions,
    parse_revision, projection::tests::stored, revision_of, wait_for_written,
};

/// Values that are not revisions, other than the empty one, which
/// `compare_revisions` takes for a view that has seen nothing.
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
        ("one event", &["7"][..], Some("7")),
        ("two events", &["7", "8"][..], Some("8")),
        // Order is not assumed, and numbers decide, not text.
        ("out of order", &["10", "9"][..], Some("10")),
        ("nonsense is skipped", &["x", "3"][..], Some("3")),
    ] {
        assert_eq!(revision_of(&written(ids)).as_deref(), want, "{name}");
    }
}

/// Returns a view that has seen the event with the given ID.
fn view_at(revision: &str) -> Arc<InMemoryView<String, String>> {
    let view = Arc::new(InMemoryView::new(String::clone));
    view.seen(revision);

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
    asked: Mutex<Vec<String>>,
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

    fn asked(&self) -> Vec<String> {
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

    fn revision(&self) -> impl Future<Output = Result<Option<String>, BrokenView>> + Send {
        future::ready(Ok(None))
    }

    fn wait_for(&self, revision: &str) -> impl Future<Output = Result<(), BrokenView>> + Send {
        self.asked
            .lock()
            .expect("no test panics while asking")
            .push(revision.to_owned());

        future::ready((self.wait_for)())
    }
}

#[tokio::test]
async fn returns_at_once_if_the_view_has_seen_the_events_already() {
    for revision in ["5", "6"] {
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
    let waited = wait_for_written(&*view_at("5"), &written(&["5"]), Duration::from_nanos(1)).await;

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
    assert_eq!(view.asked(), ["10"]);
}

#[tokio::test]
async fn returns_once_the_view_has_caught_up() {
    let view = view_at("4");

    let catching_up = {
        let view = Arc::clone(&view);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            view.seen("5");
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
        Vec::<String>::new(),
        "the view must not be asked"
    );
}

#[tokio::test]
async fn fails_with_not_caught_up_if_the_view_does_not_catch_up_in_time() {
    let started = Instant::now();

    let waited =
        wait_for_written(&*view_at("4"), &written(&["5"]), Duration::from_millis(50)).await;

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
    let waited =
        wait_for_written(&*view_at("4"), &written(&["5"]), Duration::from_millis(20)).await;

    let failure = waited.expect_err("the view has not caught up");
    assert_eq!(
        failure.category(),
        None,
        "trying the write again would store its events twice, and the view may still catch up"
    );
}

#[tokio::test]
async fn names_the_timeout_the_way_a_duration_prints() {
    let waited = wait_for_written(
        &*view_at("4"),
        &written(&["5"]),
        Duration::from_micros(1500),
    )
    .await;

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
    let _ = wait_for_written(&*view_at("5"), &[], Duration::ZERO).await;
}
