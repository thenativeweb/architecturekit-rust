// These tests check the steps of QueryExt. They follow the sentences of the
// tests of the query package of ArchitectureKit for Go, without those of
// Where, Select, Skip, Take, and Collect, which futures brings along:
//
// - A step hands on an error of the view and ends there, without calling a
//   key, a comparison, or a predicate for it.
// - A step reads no further than it needs, so an error that comes after its
//   answer goes unnoticed.

use std::{
    cmp::Ordering,
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
};

use futures_util::{Stream, StreamExt, TryStreamExt, future, stream};

use super::{QueryExt, SingleError};
use crate::{Categorized, Category};

/// An error of a view, such as a cursor that loses its connection.
#[derive(Debug, PartialEq, Eq)]
struct Broken;

impl fmt::Display for Broken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the view broke")
    }
}

impl Error for Broken {}

impl Categorized for Broken {
    fn category(&self) -> Option<Category> {
        Some(Category::Transient)
    }
}

/// An item with a key to sort by and a value that tells equal keys apart.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Book {
    title: &'static str,
    shelf: u32,
}

const fn book(title: &'static str, shelf: u32) -> Book {
    Book { title, shelf }
}

/// A view that hands out the given elements and counts how many of them were
/// read, so that a test can tell how far a step read.
fn counted(
    elements: Vec<Result<Book, Broken>>,
) -> (
    impl Stream<Item = Result<Book, Broken>> + Send,
    Arc<AtomicUsize>,
) {
    let read = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&read);
    let items = stream::iter(elements).inspect(move |_| {
        let _ = counter.fetch_add(1, AtomicOrdering::SeqCst);
    });

    (items, read)
}

fn books(elements: Vec<Result<Book, Broken>>) -> impl Stream<Item = Result<Book, Broken>> + Send {
    stream::iter(elements)
}

mod order_by {
    use super::*;

    #[tokio::test]
    async fn sorts_ascending() {
        let sorted: Vec<Book> = books(vec![Ok(book("c", 1)), Ok(book("a", 2)), Ok(book("b", 3))])
            .order_by(|book| book.title)
            .try_collect()
            .await
            .expect("the view does not fail");

        assert_eq!(sorted, vec![book("a", 2), book("b", 3), book("c", 1)]);
    }

    #[tokio::test]
    async fn hands_on_an_error_alone() {
        let elements: Vec<Result<Book, Broken>> =
            books(vec![Ok(book("b", 1)), Err(Broken), Ok(book("a", 2))])
                .order_by(|book| book.title)
                .collect()
                .await;

        assert_eq!(elements, vec![Err(Broken)]);
    }

    #[tokio::test]
    async fn reads_nothing_before_it_is_read() {
        let (items, read) = counted(vec![Ok(book("b", 1)), Ok(book("a", 2))]);

        let sorted = items.order_by(|book| book.title);
        assert_eq!(read.load(AtomicOrdering::SeqCst), 0);

        let first = sorted.take(1).try_collect::<Vec<Book>>().await;
        assert_eq!(first, Ok(vec![book("a", 2)]));
        assert_eq!(read.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn stops_when_the_caller_does() {
        let mut sorted = Box::pin(
            books(vec![Ok(book("c", 1)), Ok(book("a", 2)), Ok(book("b", 3))])
                .order_by(|book| book.title),
        );

        let first = sorted.next().await;
        drop(sorted);

        assert_eq!(first, Some(Ok(book("a", 2))));
    }
}

mod order_by_descending {
    use super::*;

    #[tokio::test]
    async fn sorts_the_other_way() {
        let sorted: Vec<Book> = books(vec![Ok(book("a", 1)), Ok(book("c", 2)), Ok(book("b", 3))])
            .order_by_descending(|book| book.title)
            .try_collect()
            .await
            .expect("the view does not fail");

        assert_eq!(sorted, vec![book("c", 2), book("b", 3), book("a", 1)]);
    }

    #[tokio::test]
    async fn is_stable() {
        let sorted: Vec<Book> = books(vec![Ok(book("a", 1)), Ok(book("b", 2)), Ok(book("a", 3))])
            .order_by_descending(|book| book.title)
            .try_collect()
            .await
            .expect("the view does not fail");

        assert_eq!(sorted, vec![book("b", 2), book("a", 1), book("a", 3)]);
    }
}

mod order_by_with {
    use super::*;

    #[tokio::test]
    async fn sorts_on_several_fields() {
        let sorted: Vec<Book> = books(vec![Ok(book("b", 2)), Ok(book("a", 2)), Ok(book("c", 1))])
            .order_by_with(|left, right| {
                left.shelf
                    .cmp(&right.shelf)
                    .then(left.title.cmp(right.title))
            })
            .try_collect()
            .await
            .expect("the view does not fail");

        assert_eq!(sorted, vec![book("c", 1), book("a", 2), book("b", 2)]);
    }

    #[tokio::test]
    async fn is_stable() {
        let sorted: Vec<Book> = books(vec![Ok(book("b", 1)), Ok(book("a", 1)), Ok(book("c", 0))])
            .order_by_with(|left, right| left.shelf.cmp(&right.shelf))
            .try_collect()
            .await
            .expect("the view does not fail");

        assert_eq!(sorted, vec![book("c", 0), book("b", 1), book("a", 1)]);
    }

    #[tokio::test]
    async fn is_stable_for_more_than_a_handful_of_items() {
        // Sorting algorithms switch strategies above a few dozen elements, so
        // stability is checked well beyond that.
        let titles: Vec<&'static str> = (0..200)
            .map(|index| &*Box::leak(format!("{index:03}").into_boxed_str()))
            .collect();
        let elements: Vec<Result<Book, Broken>> = titles
            .iter()
            .enumerate()
            .map(|(index, title)| Ok(book(title, u32::from(index % 3 == 0))))
            .collect();

        let sorted: Vec<Book> = books(elements)
            .order_by_with(|left, right| left.shelf.cmp(&right.shelf))
            .try_collect()
            .await
            .expect("the view does not fail");

        assert_eq!(sorted.len(), titles.len());
        let shelf = |shelf: u32| -> Vec<&str> {
            sorted
                .iter()
                .filter(|book| book.shelf == shelf)
                .map(|book| book.title)
                .collect()
        };
        assert!(shelf(0).is_sorted(), "the books keep their order");
        assert!(shelf(1).is_sorted(), "the books keep their order");
        assert!(sorted.iter().map(|book| book.shelf).is_sorted());
    }

    #[tokio::test]
    async fn composes_with_the_other_steps() {
        let page: Vec<&str> = books(vec![
            Ok(book("e", 1)),
            Ok(book("d", 2)),
            Ok(book("c", 1)),
            Ok(book("b", 2)),
            Ok(book("a", 1)),
        ])
        .try_filter(|book| future::ready(book.shelf == 1))
        .order_by_with(|left, right| left.title.cmp(right.title))
        .skip(1)
        .take(1)
        .map_ok(|book| book.title)
        .try_collect()
        .await
        .expect("the view does not fail");

        assert_eq!(page, vec!["c"]);
    }

    #[tokio::test]
    async fn does_not_compare_an_error() {
        let elements: Vec<Result<Book, Broken>> = books(vec![Ok(book("a", 1)), Err(Broken)])
            .order_by_with(|_, _| Ordering::Equal)
            .collect()
            .await;

        assert_eq!(elements, vec![Err(Broken)]);
    }
}

mod first {
    use super::*;

    #[tokio::test]
    async fn reports_whether_there_was_one() {
        let found = books(vec![Ok(book("a", 1)), Ok(book("b", 2))])
            .first()
            .await;
        assert_eq!(found, Ok(Some(book("a", 1))));

        let nothing = books(vec![]).first().await;
        assert_eq!(nothing, Ok(None));
    }

    #[tokio::test]
    async fn returns_an_error_before_the_first_item() {
        let failure = books(vec![Err(Broken), Ok(book("a", 1))]).first().await;

        assert_eq!(failure, Err(Broken));
    }

    #[tokio::test]
    async fn does_not_read_an_error_after_the_first_item() {
        let (items, read) = counted(vec![Ok(book("a", 1)), Err(Broken)]);

        let found = items.first().await;

        assert_eq!(found, Ok(Some(book("a", 1))));
        assert_eq!(read.load(AtomicOrdering::SeqCst), 1);
    }
}

mod single {
    use super::*;

    #[tokio::test]
    async fn says_what_it_found_without_the_name_of_the_kit() {
        assert_eq!(SingleError::<Broken>::NoItems.to_string(), "no items");
        assert_eq!(
            SingleError::<Broken>::TooManyItems.to_string(),
            "more than one item"
        );
        assert_eq!(SingleError::View(Broken).to_string(), "the view broke");
    }

    #[tokio::test]
    async fn insists_on_exactly_one() {
        let found = books(vec![Ok(book("a", 1))]).single().await;
        assert_eq!(found, Ok(book("a", 1)));

        let nothing = books(vec![]).single().await;
        assert_eq!(nothing, Err(SingleError::NoItems));

        let several = books(vec![Ok(book("a", 1)), Ok(book("b", 2))])
            .single()
            .await;
        assert_eq!(several, Err(SingleError::TooManyItems));
    }

    #[tokio::test]
    async fn returns_an_error_before_the_first_item() {
        let failure = books(vec![Err(Broken), Ok(book("a", 1))]).single().await;

        assert_eq!(failure, Err(SingleError::View(Broken)));
    }

    #[tokio::test]
    async fn returns_an_error_after_the_one_item() {
        let failure = books(vec![Ok(book("a", 1)), Err(Broken)]).single().await;

        assert_eq!(failure, Err(SingleError::View(Broken)));
    }

    #[tokio::test]
    async fn does_not_read_an_error_after_the_second_item() {
        let (items, read) = counted(vec![Ok(book("a", 1)), Ok(book("b", 2)), Err(Broken)]);

        let several = items.single().await;

        assert_eq!(several, Err(SingleError::TooManyItems));
        assert_eq!(read.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn takes_the_category_of_the_view_and_none_of_its_own() {
        assert_eq!(
            SingleError::View(Broken).category(),
            Some(Category::Transient)
        );
        assert_eq!(SingleError::<Broken>::NoItems.category(), None);
        assert_eq!(SingleError::<Broken>::TooManyItems.category(), None);
    }

    #[tokio::test]
    async fn names_the_error_of_the_view_as_its_source() {
        let failure = SingleError::View(Broken);
        let source = failure
            .source()
            .expect("the error of the view is the source");

        assert_eq!(source.to_string(), "the view broke");
        assert!(SingleError::<Broken>::NoItems.source().is_none());
    }
}

mod try_count {
    use super::*;

    #[tokio::test]
    async fn counts_everything() {
        let count = books(vec![Ok(book("a", 1)), Ok(book("b", 2)), Ok(book("c", 3))])
            .try_count()
            .await;

        assert_eq!(count, Ok(3));
    }

    #[tokio::test]
    async fn returns_an_error_without_a_count() {
        let failure = books(vec![Ok(book("a", 1)), Err(Broken), Ok(book("c", 3))])
            .try_count()
            .await;

        assert_eq!(failure, Err(Broken));
    }
}

mod steps {
    use super::*;

    #[tokio::test]
    async fn compose() {
        let found = books(vec![Ok(book("b", 1)), Ok(book("a", 2)), Ok(book("c", 1))])
            .try_filter(|book| future::ready(book.shelf == 1))
            .order_by(|book| book.title)
            .first()
            .await;

        assert_eq!(found, Ok(Some(book("b", 1))));
    }

    #[tokio::test]
    async fn carry_an_error_of_a_view_through_to_the_result() {
        let failure = books(vec![Ok(book("b", 1)), Err(Broken)])
            .try_filter(|_| future::ready(true))
            .order_by(|book| book.title)
            .single()
            .await;

        assert_eq!(failure, Err(SingleError::View(Broken)));
    }

    #[tokio::test]
    async fn read_no_further_than_the_result_needs() {
        let (items, read) = counted(vec![Ok(book("a", 1)), Ok(book("b", 2)), Err(Broken)]);

        let found = items.try_filter(|_| future::ready(true)).first().await;

        assert_eq!(found, Ok(Some(book("a", 1))));
        assert_eq!(read.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn answer_whether_any_item_matches_with_futures() {
        // try_any of futures behaves as Any of ArchitectureKit for Go does, so
        // the kit adds no step of its own for it.
        let (items, read) = counted(vec![Ok(book("a", 1)), Ok(book("b", 2)), Err(Broken)]);

        let any = items.try_any(|book| future::ready(book.title == "a")).await;

        assert_eq!(any, Ok(true));
        assert_eq!(read.load(AtomicOrdering::SeqCst), 1);
    }
}
