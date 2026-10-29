// The tests of the store check the state cache through load and execute.
// These tests check what those can not show reliably: which of two states of
// the same subject the cache keeps, and which subject it evicts.

use super::StateCache;
use crate::Revision;

#[derive(Clone, Debug, PartialEq)]
struct Count(u32);

#[derive(Clone, Debug, PartialEq)]
struct Sum(u32);

#[test]
fn holds_nothing_for_a_subject_it_was_not_given() {
    let cache = StateCache::new(10);

    assert_eq!(cache.get::<Count>("/books/42"), None);
}

#[test]
fn hands_out_a_state_with_the_id_of_the_last_event_it_was_built_from() {
    let cache = StateCache::new(10);

    cache.put("/books/42", Count(2), Revision::from(7));

    assert_eq!(
        cache.get::<Count>("/books/42"),
        Some((Count(2), Revision::from(7)))
    );
}

#[test]
fn keeps_states_of_different_types_on_the_same_subject_apart() {
    let cache = StateCache::new(10);

    cache.put("/books/42", Count(2), Revision::from(7));
    cache.put("/books/42", Sum(9), Revision::from(7));

    assert_eq!(
        cache.get::<Count>("/books/42"),
        Some((Count(2), Revision::from(7)))
    );
    assert_eq!(
        cache.get::<Sum>("/books/42"),
        Some((Sum(9), Revision::from(7)))
    );
}

#[test]
fn keeps_the_state_built_from_the_later_event() {
    let cache = StateCache::new(10);

    // Two commands on the same subject may finish in any order, and the one
    // that read less must not replace the one that read more. The IDs are
    // compared as numbers, so 10 is later than 9.
    cache.put("/books/42", Count(10), Revision::from(10));
    cache.put("/books/42", Count(9), Revision::from(9));
    assert_eq!(
        cache.get::<Count>("/books/42"),
        Some((Count(10), Revision::from(10)))
    );

    cache.put("/books/42", Count(11), Revision::from(10));
    assert_eq!(
        cache.get::<Count>("/books/42"),
        Some((Count(11), Revision::from(10))),
        "a state built from the same event replaces the cached one"
    );

    cache.put("/books/42", Count(12), Revision::from(12));
    assert_eq!(
        cache.get::<Count>("/books/42"),
        Some((Count(12), Revision::from(12)))
    );
}

#[test]
fn evicts_the_subject_used_least_recently() {
    let cache = StateCache::new(2);

    cache.put("/books/1", Count(1), Revision::from(1));
    cache.put("/books/2", Count(2), Revision::from(2));
    let _ = cache.get::<Count>("/books/1");
    cache.put("/books/3", Count(3), Revision::from(3));

    assert!(cache.get::<Count>("/books/1").is_some(), "it was used last");
    assert_eq!(
        cache.get::<Count>("/books/2"),
        None,
        "it was used least recently"
    );
    assert!(cache.get::<Count>("/books/3").is_some());
}

#[test]
fn counts_a_subject_that_is_given_again_once() {
    let cache = StateCache::new(2);

    cache.put("/books/1", Count(1), Revision::from(1));
    cache.put("/books/2", Count(2), Revision::from(2));
    cache.put("/books/1", Count(3), Revision::from(3));
    cache.put("/books/2", Count(4), Revision::from(4));

    assert_eq!(
        cache.get::<Count>("/books/1"),
        Some((Count(3), Revision::from(3)))
    );
    assert_eq!(
        cache.get::<Count>("/books/2"),
        Some((Count(4), Revision::from(4)))
    );
}

#[test]
fn counts_a_state_of_another_type_as_another_subject() {
    let cache = StateCache::new(1);

    cache.put("/books/42", Count(2), Revision::from(7));
    cache.put("/books/42", Sum(9), Revision::from(7));

    assert_eq!(cache.get::<Count>("/books/42"), None);
    assert_eq!(
        cache.get::<Sum>("/books/42"),
        Some((Sum(9), Revision::from(7)))
    );
}
