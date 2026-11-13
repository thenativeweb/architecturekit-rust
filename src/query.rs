//! Steps of a query over the items of a view, beyond those that the `futures`
//! crate brings along.
//!
//! [`View::all`](crate::View::all) hands out the items of a view as a stream,
//! in which every element is an item or an error. Filtering, transforming, and
//! paging come from `TryStreamExt` and `StreamExt` of the `futures` crate, such
//! as `try_filter`, `map_ok`, `skip`, `take`, and `try_collect`, and so does
//! `try_any`, which tells whether any item matches. [`QueryExt`] adds what those
//! lack: ordering, and getting the first item, the single item, or the number
//! of items.
//!
//! ```
//! use architecturekit::{InMemoryView, View, query::QueryExt};
//! use futures_util::{StreamExt, TryStreamExt, future};
//! # #[derive(Clone, Default)]
//! # struct Book { id: String, title: String, is_borrowed: bool }
//! # async fn example(catalog: InMemoryView<String, Book>) {
//!
//! let page: Vec<Book> = catalog
//!     .all()
//!     .try_filter(|book| future::ready(!book.is_borrowed))
//!     .order_by(|book| book.title.clone())
//!     .skip(20)
//!     .take(10)
//!     .try_collect()
//!     .await
//!     .unwrap_or_else(|never| match never {});
//! # }
//! ```
//!
//! A view that fails while its items are read, such as one in a database that
//! loses its connection, hands out the error as an element of its own, and
//! ends there. Every step hands it on, without calling a key, a comparison, or
//! a predicate for it, and a step that turns the stream into a result returns
//! it, unless the step has its answer before the error comes, as
//! [`first`](QueryExt::first) and [`single`](QueryExt::single) may.

use std::{
    cmp::{Ordering, Reverse},
    error::Error,
    fmt,
    future::Future,
    iter,
    pin::pin,
};

use futures_util::{Stream, StreamExt, TryStreamExt, future::Either, stream};

use crate::{Categorized, Category};

#[cfg(test)]
mod tests;

/// The steps of a query that [`QueryExt`] adds to every stream of items and
/// errors, such as the one that [`View::all`](crate::View::all) hands out.
///
/// Import it to use them, next to `TryStreamExt` and `StreamExt` of the
/// `futures` crate for the other steps.
pub trait QueryExt<T, E>: Stream<Item = Result<T, E>> + Sized {
    /// Sorts the items by a key, and hands them back as a stream, so that
    /// `skip` and `take` can follow. The sort is stable, so items with equal
    /// keys keep their order, and the key is computed once per item.
    ///
    /// Sorting needs every item, so filter before it. The items are read once
    /// the stream is, not before. If reading them fails, the stream hands out
    /// the error alone, and none of the items read before it, since they are
    /// not all there are.
    fn order_by<K, F>(self, key: F) -> impl Stream<Item = Result<T, E>> + Send
    where
        Self: Send,
        T: Send,
        E: Send,
        K: Ord,
        F: FnMut(&T) -> K + Send,
    {
        sorted(self, move |items| items.sort_by_cached_key(key))
    }

    /// Sorts the items by a key the other way round, as
    /// [`order_by`](QueryExt::order_by) does otherwise. Items with equal keys
    /// keep their order.
    fn order_by_descending<K, F>(self, key: F) -> impl Stream<Item = Result<T, E>> + Send
    where
        Self: Send,
        T: Send,
        E: Send,
        K: Ord,
        F: FnMut(&T) -> K + Send,
    {
        let mut key = key;
        sorted(self, move |items| {
            items.sort_by_cached_key(|item| Reverse(key(item)));
        })
    }

    /// Sorts the items by a comparison, such as one on several fields, which
    /// [`Ordering::then`] and [`Ordering::then_with`] compose:
    ///
    /// ```
    /// # use architecturekit::query::QueryExt;
    /// # use futures_util::{Stream, TryStreamExt};
    /// # use std::convert::Infallible;
    /// # struct ToDo { due: u32, title: String }
    /// # async fn example(to_dos: impl Stream<Item = Result<ToDo, Infallible>> + Send) {
    /// let sorted: Vec<ToDo> = to_dos
    ///     .order_by_with(|left, right| {
    ///         left.due.cmp(&right.due).then_with(|| left.title.cmp(&right.title))
    ///     })
    ///     .try_collect()
    ///     .await
    ///     .unwrap_or_else(|never| match never {});
    /// # }
    /// ```
    ///
    /// The sort is stable, and otherwise behaves as
    /// [`order_by`](QueryExt::order_by) does.
    fn order_by_with<F>(self, compare: F) -> impl Stream<Item = Result<T, E>> + Send
    where
        Self: Send,
        T: Send,
        E: Send,
        F: FnMut(&T, &T) -> Ordering + Send,
    {
        sorted(self, move |items| items.sort_by(compare))
    }

    /// Returns the first item, or `None` if there is none. It reads no further
    /// than that, so an error that would come after the first item goes
    /// unnoticed, while one before it is returned.
    fn first(self) -> impl Future<Output = Result<Option<T>, E>> + Send
    where
        Self: Send,
        T: Send,
        E: Send,
    {
        async move { pin!(self).next().await.transpose() }
    }

    /// Returns the one item that a query expected, and fails with
    /// [`SingleError::NoItems`] if there is none, or with
    /// [`SingleError::TooManyItems`] if there are several.
    ///
    /// It reads up to the second item. So an error that comes after the first
    /// item, but before a second one or the end, is returned, since the item
    /// can only be called the single one once the stream has ended. An error
    /// after a second item goes unnoticed, since there are too many either way.
    ///
    /// With the feature `http`, `?` answers a missing item with 404, and
    /// several with 500, logged as an error, since the caller can not fix
    /// them. Whether several items are a mistake in the data or a race, only
    /// the application knows.
    fn single(self) -> impl Future<Output = Result<T, SingleError<E>>> + Send
    where
        Self: Send,
        T: Send,
        E: Send,
    {
        async move {
            let mut items = pin!(self);
            let found = match items.next().await {
                None => return Err(SingleError::NoItems),
                Some(Err(error)) => return Err(SingleError::View(error)),
                Some(Ok(item)) => item,
            };

            match items.next().await {
                None => Ok(found),
                Some(Err(error)) => Err(SingleError::View(error)),
                Some(Ok(_)) => Err(SingleError::TooManyItems),
            }
        }
    }

    /// Counts the items, reading all of them. If that fails, it returns the
    /// error, and no count, since the items read before it are not all there
    /// are.
    ///
    /// It is called `try_count`, since the `count` of `StreamExt` counts the
    /// errors as well.
    fn try_count(self) -> impl Future<Output = Result<usize, E>> + Send
    where
        Self: Send,
        T: Send,
        E: Send,
    {
        async move {
            let mut items = pin!(self);
            let mut count = 0;
            while let Some(item) = items.next().await {
                let _ = item?;
                count += 1;
            }

            Ok(count)
        }
    }
}

impl<S, T, E> QueryExt<T, E> for S where S: Stream<Item = Result<T, E>> {}

/// Collects the items, sorts them, and hands them out again, or the error
/// alone if reading them failed. It reads nothing before it is read itself.
fn sorted<S, T, E>(
    items: S,
    sort: impl FnOnce(&mut Vec<T>) + Send,
) -> impl Stream<Item = Result<T, E>> + Send
where
    S: Stream<Item = Result<T, E>> + Send,
    T: Send,
    E: Send,
{
    stream::once(async move {
        match items.try_collect::<Vec<T>>().await {
            Ok(mut collected) => {
                sort(&mut collected);
                Either::Left(stream::iter(collected.into_iter().map(Ok)))
            }
            Err(error) => Either::Right(stream::iter(iter::once(Err(error)))),
        }
    })
    .flatten()
}

/// The failure of [`single`](QueryExt::single), which expected exactly one
/// item.
///
/// Its texts name neither the kit nor the view, so that an application may
/// hand them on to a caller.
#[derive(Debug, PartialEq, Eq)]
pub enum SingleError<E> {
    /// The query found no item.
    NoItems,
    /// The query found more than one item.
    TooManyItems,
    /// The view failed while its items were read.
    View(E),
}

impl<E: fmt::Display> fmt::Display for SingleError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoItems => formatter.write_str("no items"),
            Self::TooManyItems => formatter.write_str("more than one item"),
            Self::View(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for SingleError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NoItems | Self::TooManyItems => None,
            // The text names the error of the view already, so this passes on
            // its source.
            Self::View(error) => error.source(),
        }
    }
}

/// Takes the category of the error of the view. A missing item and several
/// items belong to none, since only the application knows what they mean:
/// whether a missing item is expected, and whether several are a mistake in
/// the data or a race.
impl<E: Categorized> Categorized for SingleError<E> {
    fn category(&self) -> Option<Category> {
        match self {
            Self::NoItems | Self::TooManyItems => None,
            Self::View(error) => error.category(),
        }
    }
}
