// These tests mirror TestInMemoryView, TestInMemoryIndex,
// TestInMemoryIndexValues, TestOutcome, TestCloneWith, TestSplitEvents and
// TestInMemoryViewRevision of ArchitectureKit for Go, under the same sentences.
// Some do not apply to Rust:
//
// - A function or an option can not be nil, and an option can not be given
//   twice, since the field of the revision is given with the view.
// - Reading hands out a Vec of clones, so there is no sequence that could go
//   on after its caller stopped.
// - A change always works on a clone of the item, which replaces the item
//   once the change is accepted, so CloneWith has nothing to switch on, and
//   the compiler rules out a reader that reads while a change writes.
// - The numbers of the outcomes and the zero outcome have no counterpart,
//   since an Outcome is an enum, and an error comes without one.
// - Waiting ends when its future is dropped, rather than with a context.
// - TestKeyedView comes with the traits of the views that serve HTTP.
// - The tests of RevisionIn against a database are part of the tests of the
//   store.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::Duration,
};

use chrono::DateTime;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    Categorized, Category, Envelope, Event, Events, InMemoryIndex, InMemoryView, Outcome,
    Projected, Revisioned, UpcastEvent, Upcasters, ViewError, projection::tests::stored,
};

/// An item with a key, a value to index, and a field for its revision.
#[derive(Clone, Debug, Default, PartialEq)]
struct Book {
    id: String,
    shelf: String,
    title: String,
    revision: String,
}

impl Book {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            ..Self::default()
        }
    }

    fn on(mut self, place: &str) -> Self {
        place.clone_into(&mut self.shelf);
        self
    }

    fn titled(mut self, title: &str) -> Self {
        title.clone_into(&mut self.title);
        self
    }
}

fn book_view() -> InMemoryView<String, Book> {
    InMemoryView::with_revision_in(|book: &Book| book.id.clone(), |book| &mut book.revision)
}

/// An event with the given ID, as a projection gets it, without data.
fn event(id: &str) -> Envelope<()> {
    part_of(id, 0)
}

/// A part of an event with the given ID that an upcaster split.
fn part_of(id: &str, part: usize) -> Envelope<()> {
    Envelope {
        id: id.to_owned(),
        time: DateTime::UNIX_EPOCH,
        source: "https://thenativeweb.io".to_owned(),
        subject: "/books/42".to_owned(),
        event_type: "io.thenativeweb.test.book-acquired".to_owned(),
        trace_parent: None,
        trace_state: None,
        part,
        data: (),
    }
}

fn must_insert<T>(view: &InMemoryView<String, T>, event_id: &str, item: T)
where
    T: Clone + Send + Sync + 'static,
{
    assert_eq!(
        view.insert(&event(event_id), item),
        Ok(Outcome::Added),
        "the item has to be added"
    );
}

fn must_get(view: &InMemoryView<String, Book>, id: &str) -> Book {
    view.get(id)
        .unwrap_or_else(|| panic!("book {id:?} is missing"))
}

fn ids_in(view: &InMemoryView<String, Book>) -> Vec<String> {
    ids_of(&view.all())
}

fn ids_of(books: &[Book]) -> Vec<String> {
    books.iter().map(|book| book.id.clone()).collect()
}

/// Returns the revision of a view in memory, which is ready at once.
fn current_revision<K, T>(view: &InMemoryView<K, T>) -> Option<String>
where
    K: Clone + std::fmt::Debug + Eq + std::hash::Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    view.revision()
        .now_or_never()
        .expect("a view in memory knows its revision at once")
        .expect("a view in memory never fails to read its revision")
}

impl PartialEq for ViewError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

#[test]
fn inserts_and_gets_items() {
    let view = book_view();

    assert_eq!(
        view.insert(&event("1"), Book::new("42").titled("2001")),
        Ok(Outcome::Added)
    );

    let got = must_get(&view, "42");
    assert_eq!(got.title, "2001");
    assert_eq!(got.revision, "1");

    assert_eq!(view.get("23"), None);
}

#[test]
fn hands_out_items_in_the_order_of_insertion() {
    let view = book_view();
    must_insert(&view, "1", Book::new("a"));
    must_insert(&view, "2", Book::new("b"));
    must_insert(&view, "3", Book::new("c"));

    assert_eq!(view.delete("b", &event("4")), Ok(Outcome::Applied));
    must_insert(&view, "5", Book::new("b"));

    // A key that was deleted and inserted again moves to the end.
    assert_eq!(ids_in(&view), ["a", "c", "b"]);
}

#[test]
fn hands_out_a_copy() {
    let view = book_view();
    must_insert(&view, "1", Book::new("a"));

    let items = view.all();
    must_insert(&view, "2", Book::new("b"));

    // Writing while a query holds its result must not change what it holds.
    assert_eq!(
        ids_of(&items),
        ["a"],
        "a query result must not change underneath"
    );
}

#[test]
fn skips_an_insert_that_is_not_newer() {
    let view = book_view();
    must_insert(&view, "5", Book::new("42").titled("first"));

    // The same event, applied a second time, and an older one.
    assert_eq!(
        view.insert(&event("5"), Book::new("42").titled("again")),
        Ok(Outcome::AlreadyApplied),
        "the same event has to be skipped"
    );
    assert_eq!(
        view.insert(&event("3"), Book::new("42").titled("older")),
        Ok(Outcome::AlreadyApplied),
        "an older event has to be skipped"
    );

    let got = must_get(&view, "42");
    assert_eq!(got.title, "first");
    assert_eq!(got.revision, "5");
}

#[test]
fn refuses_a_second_item_with_the_same_key() {
    let view = book_view();
    must_insert(&view, "1", Book::new("42"));

    let failure = view
        .insert(&event("2"), Book::new("42"))
        .expect_err("a taken key is refused");

    assert!(
        matches!(failure, ViewError::Permanent(_)),
        "a taken key is permanent"
    );
    assert_eq!(failure.category(), Some(Category::Permanent));
    assert_eq!(
        failure.to_string(),
        r#"permanent failure: event 2 inserts an item with the key "42", which is already taken"#
    );
}

#[test]
fn updates_an_item() {
    let view = book_view();
    must_insert(&view, "1", Book::new("42").titled("draft"));

    let outcome = view.update("42", &event("2"), |book| {
        "final".clone_into(&mut book.title);
        "made up by the handler".clone_into(&mut book.revision);
    });
    assert_eq!(outcome, Ok(Outcome::Applied));

    // The view sets the revision, whatever the handler wrote.
    let got = must_get(&view, "42");
    assert_eq!(got.title, "final");
    assert_eq!(got.revision, "2");
}

#[test]
fn skips_an_update_that_is_not_newer() {
    let view = book_view();
    must_insert(&view, "2", Book::new("42").titled("current"));

    let outcome = view.update("42", &event("2"), |book| {
        "replayed".clone_into(&mut book.title);
    });
    assert_eq!(outcome, Ok(Outcome::AlreadyApplied));

    assert_eq!(must_get(&view, "42").title, "current");
}

#[test]
fn changes_nothing_for_a_missing_item() {
    let view = book_view();

    assert_eq!(
        view.update("42", &event("1"), |book| "x".clone_into(&mut book.title)),
        Ok(Outcome::Missing),
        "updating a missing book"
    );
    assert_eq!(
        view.delete("42", &event("1")),
        Ok(Outcome::Missing),
        "deleting a missing book"
    );
}

#[test]
fn refuses_to_change_the_key() {
    let view = book_view();
    must_insert(&view, "1", Book::new("42").titled("kept"));

    let failure = view
        .update("42", &event("2"), |book| {
            "23".clone_into(&mut book.id);
            "moved".clone_into(&mut book.title);
        })
        .expect_err("changing the key is refused");

    assert!(
        matches!(failure, ViewError::Permanent(_)),
        "changing the key is permanent"
    );
    assert_eq!(
        failure.to_string(),
        r#"permanent failure: event 2 changes the key of an item from "42" to "23""#
    );
    assert_eq!(
        must_get(&view, "42").title,
        "kept",
        "a refused change must not be kept"
    );
    assert_eq!(view.get("23"), None, "a refused change must not be kept");
}

#[test]
fn upserts() {
    let view = book_view();
    let shelve = |title: &'static str| {
        move |book: &mut Book| {
            "42".clone_into(&mut book.id);
            title.clone_into(&mut book.title);
            book.shelf = format!("{}|{title}", book.shelf);
        }
    };

    assert_eq!(
        view.upsert("42", &event("1"), shelve("new")),
        Ok(Outcome::Added),
        "a missing book is added"
    );
    let got = must_get(&view, "42");
    assert_eq!(got.title, "new", "a missing book is added");
    assert_eq!(got.shelf, "|new", "a missing book starts from the default");
    assert_eq!(got.revision, "1", "a missing book is added with the event");

    assert_eq!(
        view.upsert("42", &event("2"), shelve("renamed")),
        Ok(Outcome::Applied),
        "an existing book is changed"
    );
    let got = must_get(&view, "42");
    assert_eq!(got.title, "renamed", "an existing book is changed");
    assert_eq!(
        got.shelf, "|new|renamed",
        "an existing book is changed, not replaced"
    );
    assert_eq!(
        got.revision, "2",
        "an existing book is changed with the event"
    );

    assert_eq!(
        view.upsert("42", &event("2"), shelve("replayed")),
        Ok(Outcome::AlreadyApplied),
        "an event that is not newer is skipped"
    );
    assert_eq!(
        must_get(&view, "42").title,
        "renamed",
        "an event that is not newer is skipped"
    );
}

#[test]
fn refuses_to_upsert_an_item_under_another_key() {
    let view = book_view();

    let failure = view
        .upsert("42", &event("1"), |book| {
            "no key set".clone_into(&mut book.title);
        })
        .expect_err("a missing key is refused");

    assert!(
        matches!(failure, ViewError::Permanent(_)),
        "a missing key is permanent"
    );
    assert_eq!(
        failure.to_string(),
        r#"permanent failure: event 1 upserts an item with the key "" under the key "42""#
    );
    assert_eq!(
        ids_in(&view),
        Vec::<String>::new(),
        "a refused item must not be added"
    );

    must_insert(&view, "2", Book::new("42"));
    let failure = view
        .upsert("42", &event("3"), |book| "23".clone_into(&mut book.id))
        .expect_err("changing the key of an existing item is refused");
    assert!(
        matches!(failure, ViewError::Permanent(_)),
        "changing the key of an existing item is permanent"
    );
}

#[test]
fn deletes_an_item() {
    let view = book_view();
    must_insert(&view, "5", Book::new("42"));

    assert_eq!(
        view.delete("42", &event("3")),
        Ok(Outcome::AlreadyApplied),
        "an older event must not delete"
    );
    assert_eq!(view.delete("42", &event("6")), Ok(Outcome::Applied));

    assert_eq!(view.get("42"), None, "the book is still there");
}

#[test]
fn updates_and_deletes_where_items_match() {
    let view = book_view();
    must_insert(&view, "1", Book::new("a").on("left"));
    must_insert(&view, "2", Book::new("b").on("right"));
    must_insert(&view, "9", Book::new("c").on("left"));

    let on_the_left = |book: &Book| book.shelf == "left";

    // c has already seen a newer event, so only a is changed.
    let changed = view.update_where(on_the_left, &event("5"), |book| {
        "left".clone_into(&mut book.title);
    });
    assert_eq!(changed, Ok(1));
    let got = must_get(&view, "a");
    assert_eq!(got.title, "left");
    assert_eq!(got.revision, "5");

    assert_eq!(view.delete_where(on_the_left, &event("10")), Ok(2));
    assert_eq!(ids_in(&view), ["b"]);
}

#[test]
fn moves_the_revision_with_a_change_that_does_nothing() {
    let view = book_view();
    must_insert(&view, "1", Book::new("42").titled("kept"));

    assert_eq!(
        view.update("42", &event("3"), |_| {}),
        Ok(Outcome::Applied),
        "a change that does nothing is still applied"
    );

    let got = must_get(&view, "42");
    assert_eq!(got.title, "kept");
    assert_eq!(got.revision, "3", "the field has to follow the event");

    assert_eq!(
        view.update("42", &event("2"), |book| "older"
            .clone_into(&mut book.title)),
        Ok(Outcome::AlreadyApplied),
        "the revision the view keeps has to follow the event as well"
    );
    assert_eq!(must_get(&view, "42").title, "kept");
}

#[test]
fn keeps_revisions_without_a_field() {
    let view = InMemoryView::new(|book: &Book| book.id.clone());
    must_insert(&view, "5", Book::new("42").titled("current"));

    assert_eq!(
        view.update("42", &event("5"), |book| {
            "replayed".clone_into(&mut book.title);
        }),
        Ok(Outcome::AlreadyApplied),
        "the replayed event has to be skipped"
    );
    assert_eq!(
        must_get(&view, "42").revision,
        "",
        "without a field, the item carries no revision"
    );
}

/// A view with an index and two books on different shelves, which records
/// whether a function it handed over ran.
struct ViewFixture {
    view: InMemoryView<String, Book>,
    by_shelf: InMemoryIndex<String, Book, String>,
    ran: Cell<bool>,
}

/// Everything a change may touch: the items with their revisions, the
/// revision of the view, and what the index finds on every shelf.
#[derive(Debug, PartialEq)]
struct ViewState {
    items: Vec<Book>,
    revision: Option<String>,
    shelves: HashMap<&'static str, Vec<String>>,
}

impl ViewFixture {
    fn new() -> Self {
        let view = book_view();
        let by_shelf = view.index(|book| book.shelf.clone());

        must_insert(&view, "1", Book::new("a").on("left").titled("kept"));
        must_insert(&view, "2", Book::new("b").on("right").titled("kept"));
        view.seen("2");

        Self {
            view,
            by_shelf,
            ran: Cell::new(false),
        }
    }

    /// Returns a change that sets the given key, so that an upsert can add an
    /// item with it, and records that it ran.
    fn change<'f>(&'f self, key: &'f str) -> impl FnMut(&mut Book) + 'f {
        move |book| {
            self.ran.set(true);
            key.clone_into(&mut book.id);
            "changed".clone_into(&mut book.title);
        }
    }

    /// Returns a match for the books on the given shelf, which records that
    /// it ran.
    fn is_on<'f>(&'f self, place: &'f str) -> impl FnMut(&Book) -> bool + 'f {
        move |book| {
            self.ran.set(true);
            book.shelf == place
        }
    }

    fn state(&self) -> ViewState {
        ViewState {
            items: self.view.all(),
            revision: current_revision(&self.view),
            shelves: ["left", "right", "nowhere"]
                .into_iter()
                .map(|shelf| (shelf, ids_of(&self.by_shelf.lookup(shelf))))
                .collect(),
        }
    }
}

/// The item a change is about, by its key and its shelf. The fixture has a
/// book with the key on the shelf, or none.
#[derive(Clone, Copy, Debug)]
struct ViewTarget {
    key: &'static str,
    shelf: &'static str,
}

const ON_A_FREE_KEY: ViewTarget = ViewTarget {
    key: "c",
    shelf: "nowhere",
};

const ON_A_TAKEN_KEY: ViewTarget = ViewTarget {
    key: "a",
    shelf: "left",
};

/// What a change hands out: an outcome, or a count.
#[derive(Debug, PartialEq)]
enum Done {
    Outcome(Outcome),
    Count(usize),
}

/// All functions that change a view.
#[derive(Clone, Copy, Debug)]
enum ViewChange {
    Insert,
    Upsert,
    Update,
    Delete,
    UpdateWhere,
    DeleteWhere,
    IndexUpdate,
    IndexDelete,
}

const VIEW_CHANGES: [ViewChange; 8] = [
    ViewChange::Insert,
    ViewChange::Upsert,
    ViewChange::Update,
    ViewChange::Delete,
    ViewChange::UpdateWhere,
    ViewChange::DeleteWhere,
    ViewChange::IndexUpdate,
    ViewChange::IndexDelete,
];

impl ViewChange {
    /// Applies the change to the target with the given event ID.
    fn apply(
        self,
        fixture: &ViewFixture,
        target: ViewTarget,
        event_id: &str,
    ) -> Result<Done, ViewError> {
        let event = event(event_id);
        let (view, by_shelf) = (&fixture.view, &fixture.by_shelf);

        match self {
            Self::Insert => view
                .insert(&event, Book::new(target.key).on(target.shelf))
                .map(Done::Outcome),
            Self::Upsert => view
                .upsert(target.key, &event, fixture.change(target.key))
                .map(Done::Outcome),
            Self::Update => view
                .update(target.key, &event, fixture.change(target.key))
                .map(Done::Outcome),
            Self::Delete => view.delete(target.key, &event).map(Done::Outcome),
            Self::UpdateWhere => view
                .update_where(
                    fixture.is_on(target.shelf),
                    &event,
                    fixture.change(target.key),
                )
                .map(Done::Count),
            Self::DeleteWhere => view
                .delete_where(fixture.is_on(target.shelf), &event)
                .map(Done::Count),
            Self::IndexUpdate => by_shelf
                .update(target.shelf, &event, fixture.change(target.key))
                .map(Done::Count),
            Self::IndexDelete => by_shelf.delete(target.shelf, &event).map(Done::Count),
        }
    }
}

#[test]
fn refuses_an_event_id_that_is_empty_or_not_a_revision_and_changes_nothing() {
    for event_id in ["", "abc", "-1", "1.5", " 1", "9223372036854775808"] {
        for target in [ON_A_FREE_KEY, ON_A_TAKEN_KEY] {
            for change in VIEW_CHANGES {
                let case = format!("{change:?} with {event_id:?} on {target:?}");
                let fixture = ViewFixture::new();
                let before = fixture.state();

                let failure = change.apply(&fixture, target, event_id).expect_err(&case);

                match &failure {
                    ViewError::NotARevision(refusal) => {
                        assert_eq!(refusal.revision(), event_id, "{case}");
                    }
                    ViewError::Permanent(_) => panic!("{case}: {failure}"),
                }
                assert_eq!(failure.category(), Some(Category::Permanent), "{case}");
                assert_eq!(
                    failure.to_string(),
                    format!(
                        "permanent failure: not a revision: an operation on a view needs the ID of the event it applies, not {event_id:?}"
                    ),
                    "{case}"
                );
                assert!(
                    !fixture.ran.get(),
                    "{case}: a function that was handed over must not run"
                );
                assert_eq!(
                    fixture.state(),
                    before,
                    "{case}: the view has to stay as it is"
                );
            }
        }
    }
}

#[test]
fn accepts_every_revision_as_an_event_id() {
    use Done::{Count, Outcome as Of};
    use Outcome::{Added, AlreadyApplied, Applied, Missing};

    // What each function hands out on a free key, and on a taken key for an
    // event that is older than the item, or newer. None stands for a
    // permanent failure.
    let wants = |change| -> [Option<Done>; 3] {
        match change {
            ViewChange::Insert => [Some(Of(Added)), Some(Of(AlreadyApplied)), None],
            ViewChange::Upsert => [Some(Of(Added)), Some(Of(AlreadyApplied)), Some(Of(Applied))],
            ViewChange::Update | ViewChange::Delete => [
                Some(Of(Missing)),
                Some(Of(AlreadyApplied)),
                Some(Of(Applied)),
            ],
            ViewChange::UpdateWhere
            | ViewChange::DeleteWhere
            | ViewChange::IndexUpdate
            | ViewChange::IndexDelete => [Some(Count(0)), Some(Count(0)), Some(Count(1))],
        }
    };

    // The smallest event ID is older than every item of the fixture, the
    // largest one newer.
    let (smallest, largest) = ("0", "9223372036854775807");

    for change in VIEW_CHANGES {
        let [on_free, older_on_taken, newer_on_taken] = wants(change);
        let cases = [
            (
                "the smallest one on a free key",
                ON_A_FREE_KEY,
                smallest,
                &on_free,
            ),
            (
                "the largest one on a free key",
                ON_A_FREE_KEY,
                largest,
                &on_free,
            ),
            (
                "the smallest one on a taken key",
                ON_A_TAKEN_KEY,
                smallest,
                &older_on_taken,
            ),
            (
                "the largest one on a taken key",
                ON_A_TAKEN_KEY,
                largest,
                &newer_on_taken,
            ),
        ];

        for (name, target, event_id, want) in cases {
            let case = format!("{change:?} with {name}");
            let result = change.apply(&ViewFixture::new(), target, event_id);

            match want {
                Some(want) => assert_eq!(result.as_ref(), Ok(want), "{case}"),
                None => assert!(
                    matches!(result, Err(ViewError::Permanent(_))),
                    "{case}: the event ID itself is fine, but {result:?}"
                ),
            }
        }
    }
}

#[test]
fn refuses_an_event_id_that_is_not_a_revision_where_it_is_given_not_at_the_next_change() {
    let view = book_view();

    assert!(
        matches!(
            view.insert(&event("abc"), Book::new("42").titled("draft")),
            Err(ViewError::NotARevision(_))
        ),
        "the insert has to be refused"
    );

    // The book never got in, so the next event about it finds nothing to
    // compare with, rather than an ID that is not a revision.
    assert_eq!(
        view.update("42", &event("7"), |book| "final"
            .clone_into(&mut book.title)),
        Ok(Outcome::Missing),
        "the next change must not fail"
    );

    must_insert(&view, "5", Book::new("42").titled("draft"));

    assert_eq!(
        view.update("42", &event("7"), |book| "final"
            .clone_into(&mut book.title)),
        Ok(Outcome::Applied)
    );
    assert_eq!(
        must_get(&view, "42"),
        Book {
            id: "42".to_owned(),
            title: "final".to_owned(),
            revision: "7".to_owned(),
            ..Book::default()
        }
    );
}

#[test]
fn reports_a_failure_among_several_items() {
    let view = book_view();
    must_insert(&view, "1", Book::new("a").on("left"));
    must_insert(&view, "2", Book::new("b").on("left"));

    let changed = view.update_where(
        |book| book.shelf == "left",
        &event("3"),
        |book| "moved".clone_into(&mut book.id),
    );

    assert!(
        matches!(changed, Err(ViewError::Permanent(_))),
        "the change of a key has to be refused"
    );
}

#[test]
fn stays_in_order_when_many_items_are_deleted() {
    let view = book_view();
    for i in 0..40 {
        must_insert(&view, &i.to_string(), Book::new(&format!("book-{i:02}")));
    }

    // Deleting most items makes the view drop them from its order.
    for i in 0..30 {
        assert_eq!(
            view.delete(format!("book-{i:02}").as_str(), &event("100")),
            Ok(Outcome::Applied)
        );
    }
    must_insert(&view, "101", Book::new("book-00"));

    let want: Vec<String> = (30..40)
        .map(|i| format!("book-{i}"))
        .chain(["book-00".to_owned()])
        .collect();
    assert_eq!(ids_in(&view), want);
}

#[test]
fn looks_up_items_by_value() {
    let view = book_view();
    must_insert(&view, "1", Book::new("a").on("left"));

    // An index that is added later knows the items that are already there.
    let by_shelf = view.index(|book| book.shelf.clone());

    must_insert(&view, "2", Book::new("b").on("right"));
    must_insert(&view, "3", Book::new("c").on("left"));

    assert_eq!(ids_of(&by_shelf.lookup("left")), ["a", "c"]);
    assert_eq!(by_shelf.lookup("nowhere"), Vec::<Book>::new());
}

#[test]
fn follows_a_changed_value() {
    let view = book_view();
    let by_shelf = view.index(|book| book.shelf.clone());
    must_insert(&view, "1", Book::new("a").on("left"));

    assert_eq!(
        view.update("a", &event("2"), |book| "right".clone_into(&mut book.shelf)),
        Ok(Outcome::Applied)
    );

    assert_eq!(by_shelf.lookup("left"), Vec::<Book>::new());
    assert_eq!(ids_of(&by_shelf.lookup("right")), ["a"]);
}

#[test]
fn hands_out_a_copy_of_what_an_index_finds() {
    let view = book_view();
    let by_shelf = view.index(|book| book.shelf.clone());
    must_insert(&view, "1", Book::new("a").on("left"));

    let items = by_shelf.lookup("left");
    must_insert(&view, "2", Book::new("b").on("left"));

    assert_eq!(
        ids_of(&items),
        ["a"],
        "a query result must not change underneath"
    );
}

#[test]
fn updates_and_deletes_by_value() {
    let view = book_view();
    let by_shelf = view.index(|book| book.shelf.clone());
    must_insert(&view, "1", Book::new("a").on("left"));
    must_insert(&view, "2", Book::new("b").on("right"));
    must_insert(&view, "9", Book::new("c").on("left"));

    assert_eq!(
        by_shelf.update("left", &event("5"), |book| {
            "left".clone_into(&mut book.title);
        }),
        Ok(1),
        "only a is changed"
    );

    assert_eq!(by_shelf.delete("left", &event("10")), Ok(2));

    assert_eq!(by_shelf.lookup("left"), Vec::<Book>::new());
    assert_eq!(ids_in(&view), ["b"]);
}

#[test]
fn forgets_a_value_once_no_item_has_it() {
    // An index over a value that changes would otherwise grow by one entry
    // for every value it has ever seen, although none of them finds an item.
    let view = book_view();
    let by_shelf = view.index(|book| book.shelf.clone());
    let sizes = || -> HashMap<String, usize> {
        by_shelf
            .index
            .keys()
            .iter()
            .map(|(value, keys)| (value.clone(), keys.len()))
            .collect()
    };

    must_insert(&view, "1", Book::new("a").on("new"));
    must_insert(&view, "2", Book::new("b").on("new"));

    assert_eq!(
        view.update("a", &event("3"), |book| "returned"
            .clone_into(&mut book.shelf)),
        Ok(Outcome::Applied)
    );
    assert_eq!(
        sizes(),
        HashMap::from([("new".to_owned(), 1), ("returned".to_owned(), 1)]),
        "a value is kept while an item has it"
    );

    assert_eq!(
        view.update("b", &event("4"), |book| "returned"
            .clone_into(&mut book.shelf)),
        Ok(Outcome::Applied)
    );
    assert_eq!(
        sizes(),
        HashMap::from([("returned".to_owned(), 2)]),
        "a value that an update left behind is kept"
    );

    assert_eq!(view.delete("a", &event("5")), Ok(Outcome::Applied));
    assert_eq!(view.delete("b", &event("6")), Ok(Outcome::Applied));
    assert_eq!(
        sizes(),
        HashMap::new(),
        "a value that a delete left behind is kept"
    );
}

#[test]
fn names_itself() {
    assert_eq!(Outcome::Applied.to_string(), "applied");
    assert_eq!(Outcome::Missing.to_string(), "missing");
    assert_eq!(Outcome::AlreadyApplied.to_string(), "already applied");
    assert_eq!(Outcome::Added.to_string(), "added");
}

/// An item that holds a list and a map, which a change writes into.
#[derive(Clone, Debug, Default, PartialEq)]
struct ShelfItem {
    id: String,
    book_ids: Vec<String>,
    labels: HashMap<String, String>,
}

impl ShelfItem {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            book_ids: vec!["42".to_owned()],
            labels: HashMap::from([("genre".to_owned(), "fiction".to_owned())]),
        }
    }
}

fn shelf_view() -> InMemoryView<String, ShelfItem> {
    InMemoryView::new(|item: &ShelfItem| item.id.clone())
}

/// Writes into the list and the map of a shelf, rather than replacing them.
fn rearrange(item: &mut ShelfItem) {
    "23".clone_into(&mut item.book_ids[0]);
    let _ = item.labels.insert("genre".to_owned(), "poetry".to_owned());
}

#[test]
fn hands_a_clone_to_every_change_of_an_existing_item() {
    type Change = fn(&InMemoryView<String, ShelfItem>) -> Result<(), ViewError>;
    let changes: [(&str, Change); 4] = [
        ("update", |view| {
            view.update("a", &event("2"), rearrange).map(drop)
        }),
        ("upsert", |view| {
            view.upsert("a", &event("2"), rearrange).map(drop)
        }),
        ("update_where", |view| {
            view.update_where(|_| true, &event("2"), rearrange)
                .map(drop)
        }),
        ("index update", |view| {
            let by_id = view.index(|item| item.id.clone());
            by_id.update("a", &event("2"), rearrange).map(drop)
        }),
    ];

    for (name, change) in changes {
        let view = shelf_view();
        must_insert(&view, "1", ShelfItem::new("a"));

        let got = view.get("a").expect("the shelf is there");
        let items = view.all();

        change(&view).unwrap_or_else(|failure| panic!("{name}: {failure}"));

        // What the readers got before the change must stay as it was.
        assert_eq!(got, ShelfItem::new("a"), "{name}: an item that was read");
        assert_eq!(
            items,
            [ShelfItem::new("a")],
            "{name}: an item that was listed"
        );

        let rearranged = view.get("a").expect("the shelf is there");
        assert_eq!(rearranged.book_ids, ["23"], "{name}");
        assert_eq!(rearranged.labels["genre"], "poetry", "{name}");
    }
}

#[test]
fn keeps_an_index_on_data_that_a_change_writes_into() {
    let view = shelf_view();
    let by_genre = view.index(|item| item.labels["genre"].clone());
    must_insert(&view, "1", ShelfItem::new("a"));

    assert_eq!(
        view.update("a", &event("2"), rearrange),
        Ok(Outcome::Applied)
    );

    assert_eq!(
        by_genre.lookup("fiction"),
        Vec::<ShelfItem>::new(),
        "the index must not find the shelf under its old value"
    );
    assert_eq!(
        by_genre.lookup("poetry").len(),
        1,
        "the index has to find the shelf under its new value"
    );
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.book-acquired")]
struct BookAcquired {
    title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.book-shelved")]
struct BookShelved {
    shelf: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(type = "io.thenativeweb.test.book-discarded")]
struct BookDiscarded {}

/// The events of a book, as a newer version of an application writes them.
#[derive(Clone, Debug, PartialEq, Events)]
enum BookEvent {
    Acquired(BookAcquired),
    Shelved(BookShelved),
    Discarded(BookDiscarded),
}

// An older version of the application wrote a single event for what are two
// events now, which the upcasters split. The last one is a mistake, since a
// book is acquired only once.
const ACQUIRED_AND_SHELVED: &str = "io.thenativeweb.test.book-acquired-and-shelved";
const SHELVED_AND_DISCARDED: &str = "io.thenativeweb.test.book-shelved-and-discarded";
const REPLACED: &str = "io.thenativeweb.test.book-replaced";
const ACQUIRED_TWICE: &str = "io.thenativeweb.test.book-acquired-twice";

/// Splits a stored event into one event per given type. Each is a copy of the
/// stored event, with its ID and its data, of which every type decodes the
/// fields it knows.
fn split_into(
    event_types: [&'static str; 2],
) -> impl Fn(UpcastEvent) -> Result<Vec<UpcastEvent>, serde_json::Error> + Send + Sync + 'static {
    move |event| {
        Ok(event_types
            .iter()
            .map(|event_type| UpcastEvent::clone(&event).with_type(*event_type))
            .collect())
    }
}

static BOOK_UPCASTERS: LazyLock<Upcasters> = LazyLock::new(|| {
    Upcasters::new()
        .upcast(
            ACQUIRED_AND_SHELVED,
            split_into([BookAcquired::EVENT_TYPE, BookShelved::EVENT_TYPE]),
        )
        .upcast(
            SHELVED_AND_DISCARDED,
            split_into([BookShelved::EVENT_TYPE, BookDiscarded::EVENT_TYPE]),
        )
        .upcast(
            REPLACED,
            split_into([BookDiscarded::EVENT_TYPE, BookAcquired::EVENT_TYPE]),
        )
        .upcast(
            ACQUIRED_TWICE,
            split_into([BookAcquired::EVENT_TYPE, BookAcquired::EVENT_TYPE]),
        )
});

/// A stored event about the book 42.
fn stored_book(
    event_id: &str,
    event_type: &str,
    data: serde_json::Value,
) -> eventsourcingdb::Event {
    stored(event_id, event_type, data)
}

/// The ways to add a book.
#[derive(Clone, Copy, Debug)]
enum Acquisition {
    Insert,
    Upsert,
}

/// The ways to change a book.
#[derive(Clone, Copy, Debug)]
enum Change {
    Update,
    Upsert,
    UpdateWhere,
    IndexUpdate,
}

const CHANGES: [Change; 4] = [
    Change::Update,
    Change::Upsert,
    Change::UpdateWhere,
    Change::IndexUpdate,
];

/// The ways to remove a book.
#[derive(Clone, Copy, Debug)]
enum Removal {
    Delete,
    DeleteWhere,
    IndexDelete,
}

/// A view of books with an index by ID, so that the index can stand in for
/// the view, and the handlers of a projection that apply the events of books
/// to it in the given ways. It counts how often a change of a book runs, and
/// how many books are removed, and it keeps what every acquisition of a book
/// reported, so that a test sees whether a part was applied again.
struct BookCatalog {
    view: InMemoryView<String, Book>,
    by_id: InMemoryIndex<String, Book, String>,
    acquisition: Acquisition,
    change: Change,
    removal: Removal,
    changes: Cell<usize>,
    removed: Cell<usize>,
    acquired: RefCell<Vec<Option<Outcome>>>,
}

impl BookCatalog {
    fn new(acquisition: Acquisition, change: Change, removal: Removal) -> Self {
        let view = book_view();
        let by_id = view.index(|book| book.id.clone());

        Self {
            view,
            by_id,
            acquisition,
            change,
            removal,
            changes: Cell::new(0),
            removed: Cell::new(0),
            acquired: RefCell::new(Vec::new()),
        }
    }

    /// Applies stored events as a projection does, with every part the given
    /// upcasters split them into.
    fn apply(
        &self,
        upcasters: Option<&Upcasters>,
        events: &[eventsourcingdb::Event],
    ) -> Result<(), ViewError> {
        for stored in events {
            for event in BookEvent::project(stored, upcasters).expect("the event is projected") {
                self.handle(&event)?;
            }
        }

        Ok(())
    }

    fn handle(&self, event: &Envelope<BookEvent>) -> Result<(), ViewError> {
        let id = event.subject.trim_start_matches("/books/");

        match &event.data {
            BookEvent::Acquired(acquired) => {
                let outcome = self.acquire(event, id, &acquired.title);
                self.acquired
                    .borrow_mut()
                    .push(outcome.as_ref().ok().copied());
                outcome.map(drop)
            }
            BookEvent::Shelved(shelved) => self.shelve(event, id, |book| {
                self.changes.set(self.changes.get() + 1);
                shelved.shelf.clone_into(&mut book.shelf);
            }),
            BookEvent::Discarded(_) => {
                let removed = self.discard(event, id)?;
                self.removed.set(self.removed.get() + removed);
                Ok(())
            }
        }
    }

    fn acquire(
        &self,
        event: &Envelope<BookEvent>,
        id: &str,
        title: &str,
    ) -> Result<Outcome, ViewError> {
        match self.acquisition {
            Acquisition::Insert => self.view.insert(event, Book::new(id).titled(title)),
            Acquisition::Upsert => self.view.upsert(id, event, |book| {
                self.changes.set(self.changes.get() + 1);
                id.clone_into(&mut book.id);
                title.clone_into(&mut book.title);
            }),
        }
    }

    fn shelve(
        &self,
        event: &Envelope<BookEvent>,
        id: &str,
        mut change: impl FnMut(&mut Book),
    ) -> Result<(), ViewError> {
        match self.change {
            Change::Update => self.view.update(id, event, change).map(drop),
            Change::Upsert => self
                .view
                .upsert(id, event, |book| {
                    id.clone_into(&mut book.id);
                    change(book);
                })
                .map(drop),
            Change::UpdateWhere => self
                .view
                .update_where(|book| book.id == id, event, change)
                .map(drop),
            Change::IndexUpdate => self.by_id.update(id, event, change).map(drop),
        }
    }

    fn discard(&self, event: &Envelope<BookEvent>, id: &str) -> Result<usize, ViewError> {
        match self.removal {
            Removal::Delete => Ok(usize::from(
                self.view.delete(id, event)? == Outcome::Applied,
            )),
            Removal::DeleteWhere => self.view.delete_where(|book| book.id == id, event),
            Removal::IndexDelete => self.by_id.delete(id, event),
        }
    }
}

fn dune(shelf: &str, revision: &str) -> Book {
    Book {
        id: "42".to_owned(),
        shelf: shelf.to_owned(),
        title: "Dune".to_owned(),
        revision: revision.to_owned(),
    }
}

fn acquired_and_shelved() -> eventsourcingdb::Event {
    stored_book(
        "0",
        ACQUIRED_AND_SHELVED,
        json!({ "title": "Dune", "shelf": "A3" }),
    )
}

#[test]
fn applies_a_later_part_that_changes_the_item_an_earlier_part_inserted() {
    for change in CHANGES {
        let catalog = BookCatalog::new(Acquisition::Insert, change, Removal::Delete);

        catalog
            .apply(Some(&BOOK_UPCASTERS), &[acquired_and_shelved()])
            .unwrap_or_else(|failure| panic!("{change:?}: {failure}"));

        assert_eq!(
            must_get(&catalog.view, "42"),
            dune("A3", "0"),
            "{change:?}: the second part has to change the item"
        );
        assert_eq!(catalog.changes.get(), 1, "{change:?}");
        assert_eq!(
            *catalog.acquired.borrow(),
            [Some(Outcome::Added)],
            "{change:?}: the first part has to add the item"
        );
    }
}

#[test]
fn changes_nothing_when_the_stored_event_is_applied_again() {
    for change in CHANGES {
        let catalog = BookCatalog::new(Acquisition::Insert, change, Removal::Delete);

        catalog
            .apply(
                Some(&BOOK_UPCASTERS),
                &[acquired_and_shelved(), acquired_and_shelved()],
            )
            .unwrap_or_else(|failure| panic!("{change:?}: {failure}"));

        assert_eq!(must_get(&catalog.view, "42"), dune("A3", "0"), "{change:?}");
        assert_eq!(
            catalog.changes.get(),
            1,
            "{change:?}: no part may change the item a second time"
        );
        assert_eq!(
            *catalog.acquired.borrow(),
            [Some(Outcome::Added), Some(Outcome::AlreadyApplied)],
            "{change:?}: no part may add the item a second time"
        );
    }
}

#[test]
fn applies_a_later_stored_event_after_a_split_one() {
    for change in CHANGES {
        let catalog = BookCatalog::new(Acquisition::Insert, change, Removal::Delete);

        catalog
            .apply(
                Some(&BOOK_UPCASTERS),
                &[
                    acquired_and_shelved(),
                    stored_book("1", BookShelved::EVENT_TYPE, json!({ "shelf": "B7" })),
                ],
            )
            .unwrap_or_else(|failure| panic!("{change:?}: {failure}"));

        assert_eq!(
            must_get(&catalog.view, "42"),
            dune("B7", "1"),
            "{change:?}: a later event has to be newer than every part of an earlier one"
        );
        assert_eq!(catalog.changes.get(), 2, "{change:?}");
    }
}

#[test]
fn skips_a_change_of_the_same_event_without_a_part_which_counts_as_part_0() {
    let catalog = BookCatalog::new(Acquisition::Insert, Change::Update, Removal::Delete);
    catalog
        .apply(Some(&BOOK_UPCASTERS), &[acquired_and_shelved()])
        .expect("the event is applied");

    assert_eq!(
        catalog
            .view
            .update("42", &event("0"), |book| "C1".clone_into(&mut book.shelf)),
        Ok(Outcome::AlreadyApplied)
    );
    assert_eq!(
        catalog
            .view
            .update("42", &event("1"), |book| "C1".clone_into(&mut book.shelf)),
        Ok(Outcome::Applied),
        "a later event without a part has to be applied"
    );
}

#[test]
fn removes_an_item_in_a_later_part() {
    for removal in [Removal::Delete, Removal::DeleteWhere, Removal::IndexDelete] {
        let catalog = BookCatalog::new(Acquisition::Insert, Change::Update, removal);
        let shelved_and_discarded =
            stored_book("1", SHELVED_AND_DISCARDED, json!({ "shelf": "B7" }));

        catalog
            .apply(
                Some(&BOOK_UPCASTERS),
                &[acquired_and_shelved(), shelved_and_discarded.clone()],
            )
            .unwrap_or_else(|failure| panic!("{removal:?}: {failure}"));

        assert_eq!(
            ids_in(&catalog.view),
            Vec::<String>::new(),
            "{removal:?}: the second part has to remove the item the first one changed"
        );
        assert_eq!(catalog.removed.get(), 1, "{removal:?}");
        assert_eq!(catalog.changes.get(), 2, "{removal:?}");

        catalog
            .apply(Some(&BOOK_UPCASTERS), &[shelved_and_discarded])
            .unwrap_or_else(|failure| panic!("{removal:?}: {failure}"));

        assert_eq!(ids_in(&catalog.view), Vec::<String>::new(), "{removal:?}");
        assert_eq!(
            catalog.removed.get(),
            1,
            "{removal:?}: applying the stored event again must not remove anything"
        );
        assert_eq!(
            catalog.changes.get(),
            2,
            "{removal:?}: applying the stored event again must not change anything"
        );
    }
}

#[test]
fn adds_an_item_in_a_later_part_and_keeps_it_when_the_stored_event_is_applied_again() {
    for acquisition in [Acquisition::Insert, Acquisition::Upsert] {
        let catalog = BookCatalog::new(acquisition, Change::Update, Removal::Delete);
        let replaced = stored_book("1", REPLACED, json!({ "title": "Dune, second copy" }));

        catalog
            .apply(
                Some(&BOOK_UPCASTERS),
                &[
                    stored_book("0", BookAcquired::EVENT_TYPE, json!({ "title": "Dune" })),
                    replaced.clone(),
                ],
            )
            .unwrap_or_else(|failure| panic!("{acquisition:?}: {failure}"));

        let want = Book {
            id: "42".to_owned(),
            title: "Dune, second copy".to_owned(),
            revision: "1".to_owned(),
            ..Book::default()
        };
        assert_eq!(
            must_get(&catalog.view, "42"),
            want,
            "{acquisition:?}: the first part has to remove the old item, and the second one has to add the new one"
        );
        assert_eq!(catalog.removed.get(), 1, "{acquisition:?}");
        assert_eq!(
            *catalog.acquired.borrow(),
            [Some(Outcome::Added), Some(Outcome::Added)],
            "{acquisition:?}: the second part has to add the item anew"
        );
        let changes = catalog.changes.get();

        catalog
            .apply(Some(&BOOK_UPCASTERS), &[replaced])
            .unwrap_or_else(|failure| {
                panic!("{acquisition:?}: applying the stored event again has to skip both parts: {failure}")
            });

        assert_eq!(must_get(&catalog.view, "42"), want, "{acquisition:?}");
        assert_eq!(catalog.removed.get(), 1, "{acquisition:?}");
        assert_eq!(
            catalog.changes.get(),
            changes,
            "{acquisition:?}: applying the stored event again must not change anything"
        );
        assert_eq!(
            *catalog.acquired.borrow(),
            [
                Some(Outcome::Added),
                Some(Outcome::Added),
                Some(Outcome::AlreadyApplied)
            ],
            "{acquisition:?}: applying the stored event again must not add anything"
        );
    }
}

#[test]
fn refuses_a_later_part_that_inserts_an_item_whose_key_an_earlier_part_took() {
    let catalog = BookCatalog::new(Acquisition::Insert, Change::Update, Removal::Delete);

    let failure = catalog
        .apply(
            Some(&BOOK_UPCASTERS),
            &[stored_book("0", ACQUIRED_TWICE, json!({ "title": "Dune" }))],
        )
        .expect_err("the second part is refused");

    assert!(
        matches!(failure, ViewError::Permanent(_)),
        "a second item with the same key is a mistake, also within one stored event"
    );
    assert!(failure.to_string().contains("already taken"), "{failure}");
    assert_eq!(
        *catalog.acquired.borrow(),
        [Some(Outcome::Added), None],
        "the second part has to fail without an outcome"
    );
}

#[test]
fn upserts_in_a_later_part_the_item_an_earlier_part_added_and_skips_both_when_the_stored_event_is_applied_again()
 {
    let catalog = BookCatalog::new(Acquisition::Upsert, Change::Update, Removal::Delete);
    let acquired_twice = stored_book("0", ACQUIRED_TWICE, json!({ "title": "Dune" }));

    catalog
        .apply(
            Some(&BOOK_UPCASTERS),
            &[acquired_twice.clone(), acquired_twice],
        )
        .expect("the events are applied");

    assert_eq!(
        *catalog.acquired.borrow(),
        [
            Some(Outcome::Added),
            Some(Outcome::Applied),
            Some(Outcome::AlreadyApplied),
            Some(Outcome::AlreadyApplied),
        ],
        "the first part has to add the item, and the second one has to change it"
    );
    assert_eq!(catalog.changes.get(), 2);
    assert_eq!(must_get(&catalog.view, "42"), dune("", "0"));
}

#[test]
fn treats_every_event_as_part_0_without_upcasters() {
    let catalog = BookCatalog::new(Acquisition::Insert, Change::Update, Removal::Delete);

    catalog
        .apply(
            None,
            &[
                stored_book("0", BookAcquired::EVENT_TYPE, json!({ "title": "Dune" })),
                stored_book("1", BookShelved::EVENT_TYPE, json!({ "shelf": "A3" })),
                stored_book("1", BookShelved::EVENT_TYPE, json!({ "shelf": "B7" })),
                stored_book(
                    "0",
                    BookAcquired::EVENT_TYPE,
                    json!({ "title": "Dune, again" }),
                ),
            ],
        )
        .expect("the events are applied");

    assert_eq!(
        must_get(&catalog.view, "42"),
        dune("A3", "1"),
        "an event applied again has to be skipped, as before"
    );
    assert_eq!(catalog.changes.get(), 1);
    assert_eq!(
        *catalog.acquired.borrow(),
        [Some(Outcome::Added), Some(Outcome::AlreadyApplied)]
    );
}

#[test]
fn applies_every_part_of_an_event_in_order() {
    // Parts are told apart by their position, so a later part of the same
    // event is newer than an earlier one, and an earlier one is skipped.
    let view = book_view();
    must_insert(&view, "3", Book::new("42"));

    assert_eq!(
        view.update("42", &part_of("3", 1), |book| "A3"
            .clone_into(&mut book.shelf)),
        Ok(Outcome::Applied)
    );
    assert_eq!(
        view.update("42", &part_of("3", 0), |book| "B7"
            .clone_into(&mut book.shelf)),
        Ok(Outcome::AlreadyApplied)
    );
    assert_eq!(
        must_get(&view, "42"),
        Book {
            id: "42".to_owned(),
            shelf: "A3".to_owned(),
            revision: "3".to_owned(),
            ..Book::default()
        },
        "the revision of an item stays the ID of the event, without the part"
    );
}

fn number_view() -> Arc<InMemoryView<u64, u64>> {
    Arc::new(InMemoryView::new(|number: &u64| *number))
}

#[test]
fn a_fresh_view_has_seen_nothing() {
    assert_eq!(current_revision(&number_view()), None);
}

#[test]
fn revision_never_fails() {
    let view = number_view();
    view.seen("5");

    assert_eq!(current_revision(&view), Some("5".to_owned()));
}

#[test]
fn seen_moves_the_revision_forward_only() {
    let view = number_view();

    view.seen("5");
    assert_eq!(current_revision(&view), Some("5".to_owned()));

    // An event that arrives twice, or out of order after a restart, must not
    // pull the revision back. Each is checked on its own, since the event that
    // arrives twice would move a revision that went back forward again.
    view.seen("3");
    assert_eq!(
        current_revision(&view),
        Some("5".to_owned()),
        "an older event"
    );
    view.seen("5");
    assert_eq!(
        current_revision(&view),
        Some("5".to_owned()),
        "the same event"
    );

    view.seen("12");
    assert_eq!(current_revision(&view), Some("12".to_owned()));
}

#[test]
fn seen_ignores_what_is_not_a_revision() {
    let view = number_view();
    view.seen("5");
    view.seen("nonsense");
    view.seen("");

    assert_eq!(current_revision(&view), Some("5".to_owned()));
}

#[tokio::test]
async fn waiting_for_a_revision_already_reached_returns_at_once() {
    let view = number_view();
    view.seen("10");

    for (name, revision) in [
        ("the empty revision", ""),
        ("an older revision", "9"),
        ("the same revision", "10"),
    ] {
        assert_eq!(
            view.wait_for(revision).now_or_never(),
            Some(Ok(())),
            "{name}"
        );
    }
}

#[tokio::test]
async fn waiting_returns_when_the_revision_arrives() {
    let view = number_view();

    let waiting = {
        let view = Arc::clone(&view);
        tokio::spawn(async move { view.wait_for("3").await })
    };

    // Everything below the wanted revision leaves the waiter waiting.
    view.seen("1");
    view.seen("2");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "returned too early");

    view.seen("3");

    let waited = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("it returns")
        .expect("it does not panic");
    assert_eq!(waited, Ok(()));
}

#[tokio::test]
async fn waiting_ends_when_its_future_is_dropped() {
    let view = number_view();

    let waited = tokio::time::timeout(Duration::from_millis(50), view.wait_for("1")).await;

    assert!(waited.is_err(), "the view has not seen the revision");
}

#[tokio::test]
async fn waiting_for_something_that_is_not_a_revision_fails() {
    let view = number_view();

    let refusal = view
        .wait_for("soon")
        .await
        .expect_err("soon is not a revision");

    assert_eq!(refusal.revision(), "soon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_waiters_are_all_woken_up() {
    let view = number_view();

    let waiters: Vec<_> = (0..10)
        .map(|_| {
            let view = Arc::clone(&view);
            tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(10), view.wait_for("4")).await
            })
        })
        .collect();

    // Several steps, so that the waiters go around more than once.
    for id in ["1", "2", "3", "4"] {
        view.seen(id);
    }

    for waiter in waiters {
        assert_eq!(
            waiter
                .await
                .expect("a waiter does not panic")
                .expect("a waiter is woken up"),
            Ok(()),
            "a waiter failed"
        );
    }
}
