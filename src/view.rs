use std::{
    borrow::Borrow,
    collections::HashMap,
    convert::Infallible,
    error::Error,
    fmt::{self, Debug},
    future::{self, Future},
    hash::Hash,
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use futures_util::{Stream, stream};
use tokio::sync::watch;

use crate::{Categorized, Category, Envelope, Failure, Permanent, Revision, Revisioned};

mod index;
#[cfg(test)]
mod tests;

pub use index::InMemoryIndex;

/// A collection of items that a query reads, wherever they live, in memory or
/// in a database. [`InMemoryView`] is one.
///
/// Reading is asynchronous and may fail, as it does for a view in a database,
/// so that code that reads a view works the same, whatever is behind it. A view
/// in memory never fails, and names [`Infallible`] as its error.
pub trait View<T>: Send + Sync {
    /// The error with which reading fails.
    type Error: Error + Categorized + Send + Sync + 'static;

    /// Hands out every item, as a stream, so that a view in a database can
    /// hand on the items as it reads them, rather than reading all of them
    /// first. Every element is an item or an error, since such a view may also
    /// fail halfway, for example when its connection breaks, and it ends after
    /// an error.
    ///
    /// The steps of a query come from `TryStreamExt` of the `futures` crate,
    /// such as `try_filter`, `map_ok`, `try_next` and `try_collect`. To order
    /// the items, collect them first.
    fn all(&self) -> impl Stream<Item = Result<T, Self::Error>> + Send;
}

/// A view that finds a single item by its key, without reading all of them.
/// [`InMemoryView`] is one, for the type of its keys, and for every type that
/// a key can be borrowed as, such as `str` for a `String`.
pub trait KeyedView<K: ?Sized, T>: View<T> {
    /// Returns the item with the given key, or `None` if there is none.
    fn get(&self, key: &K) -> impl Future<Output = Result<Option<T>, Self::Error>> + Send;
}

/// Holds the items of a read model in memory, each under a key of its own. It
/// is the one view the kit ships, and because it keeps no checkpoint, its
/// projection is rebuilt on every start.
///
/// It is read through [`View`] and [`KeyedView`], asynchronously, as a view in
/// a database is, so that code that reads a view works the same for both. It
/// never fails while it is read, so its error is [`Infallible`], and a reader
/// needs no handling for it:
///
/// ```
/// use architecturekit::{InMemoryView, KeyedView, View};
/// use futures_util::TryStreamExt;
///
/// #[derive(Clone, Debug, Default, PartialEq)]
/// struct Book {
///     id: String,
///     title: String,
/// }
///
/// # let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
/// # runtime.block_on(async {
/// let books = InMemoryView::new(|book: &Book| book.id.clone());
///
/// let Ok(book) = books.get("42").await;
/// assert_eq!(book, None);
///
/// let Ok(all) = books.all().try_collect::<Vec<_>>().await;
/// assert!(all.is_empty());
/// # });
/// ```
///
/// Every item carries a revision of its own, which is the ID of the last event
/// that changed it. An event that is not newer than the item it is about is
/// skipped, so that applying the same event twice changes nothing, as long as
/// the item is still there. The view forgets the revision of an item it
/// removes, so an event that adds the item, applied again after a later event
/// has removed it, adds it again. A projection that is rebuilt applies every
/// event once, in order, so that does not happen to a view in memory. Apart
/// from that, the view has a revision as a whole, which is the last event it
/// has seen at all (see [`seen`](InMemoryView::seen)).
///
/// Every function that changes the view takes the event it applies. The ID of
/// an event is a [`Revision`], so the view compares IDs as numbers: event 10
/// is newer than event 9.
///
/// An upcaster may split a stored event into several events, its parts, which
/// all carry the ID of the stored event, and each its position as
/// [`Envelope::part`]. Next to the revision of an item, the view keeps the part
/// of the last change, and for the same event ID, a later part counts as newer
/// than an earlier one. So every part of a split event is applied, in order,
/// even if several parts change the same item, and applying the stored event
/// again still changes nothing. The revision of an item stays the ID of the
/// event, without the part.
///
/// Reading hands out clones of the items, which a later change does not
/// reach, and a change works on a clone of the item, which replaces the item
/// only once the change is accepted. Every function that changes the view runs
/// the functions it is given, such as a change, while it holds the lock of the
/// view. So these functions must not call any function of the view or of its
/// indexes, not even [`get`](KeyedView::get) or
/// [`all`](View::all), since that blocks forever. A panic in one of
/// them leaves the view usable for those who read it.
///
/// This is the stored shape, not the answer to a query. Use `TryStreamExt` of
/// the `futures` crate on [`View::all`] to filter, order and project it into
/// whatever an answer needs.
pub struct InMemoryView<K, T> {
    shared: Arc<Shared<K, T>>,
}

/// Takes the key of an item.
type KeyOf<K, T> = Box<dyn Fn(&T) -> K + Send + Sync>;

/// Returns the field of an item that holds its revision.
type RevisionIn<T> = Box<dyn Fn(&mut T) -> &mut Option<Revision> + Send + Sync>;

/// What a view shares with its indexes.
struct Shared<K, T> {
    key_of: KeyOf<K, T>,
    revision_in: Option<RevisionIn<T>>,
    entries: RwLock<Entries<K, T>>,

    // How far the projection has come, which waiting readers watch.
    revision: watch::Sender<Option<Revision>>,
}

/// The items of a view. `order` keeps the keys in the order in which they
/// were inserted, and may still hold keys that have been deleted since. An
/// entry knows its position in `order`, so that a key that was deleted and
/// inserted again is only handed out at its new position.
struct Entries<K, T> {
    items: HashMap<K, Entry<T>>,
    order: Vec<K>,
    indexes: Vec<Arc<dyn Indexer<K, T>>>,
}

/// An item together with the event that changed it last: `revision` is the ID
/// of that event, and `part` is its part, if an upcaster split it. `position`
/// is where the key of the item is in the order of the view.
struct Entry<T> {
    item: T,
    revision: Revision,
    part: usize,
    position: usize,
}

/// Keeps a secondary index up to date. The view calls it with every item it
/// adds and every item it removes, and a change is a removal of the old item
/// followed by an addition of the new one.
trait Indexer<K, T>: Send + Sync {
    fn add(&self, key: &K, item: &T);
    fn remove(&self, key: &K, item: &T);
}

/// The event a change of the view applies: `id` is its ID, which becomes the
/// revision of the item, and `part` is its part, if an upcaster split it.
struct AppliedEvent {
    id: Revision,
    part: usize,
}

impl AppliedEvent {
    /// Takes the event a change of the view applies.
    fn of<E>(event: &Envelope<E>) -> Self {
        Self {
            id: event.id,
            part: event.part,
        }
    }

    /// Returns whether this part of an event is newer than the last change of
    /// an item. An event with a higher ID is newer, whatever its part, and for
    /// the same ID, a later part is newer than an earlier one.
    fn is_newer_than<T>(&self, entry: &Entry<T>) -> bool {
        if self.id == entry.revision {
            return self.part > entry.part;
        }

        self.id > entry.revision
    }
}

impl<K, T> InMemoryView<K, T>
where
    K: Clone + Debug + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Creates an empty view, which takes the key of an item from the given
    /// function, and keeps the revisions of its items to itself.
    #[must_use]
    pub fn new(key_of: impl Fn(&T) -> K + Send + Sync + 'static) -> Self {
        Self::build(Box::new(key_of), None)
    }

    /// Creates an empty view, which takes the key of an item from the given
    /// function, and keeps the revision of every item in a field of the item,
    /// so that it travels with the item, for example to a caller that hands it
    /// over to a command later. The second function returns that field. The
    /// view sets the field on every change, so a projection does not.
    ///
    /// ```
    /// use architecturekit::InMemoryView;
    ///
    /// use architecturekit::Revision;
    ///
    /// #[derive(Clone, Debug, Default, PartialEq)]
    /// struct Book {
    ///     id: String,
    ///     revision: Option<Revision>,
    /// }
    ///
    /// let books = InMemoryView::with_revision_in(
    ///     |book: &Book| book.id.clone(),
    ///     |book| &mut book.revision,
    /// );
    /// # drop(books);
    /// ```
    ///
    /// The revision of an item fits a precondition on the last event of a
    /// subject, such as
    /// [`Precondition::on_event_id`](crate::Precondition::on_event_id), only
    /// if the item stands for exactly one subject, and the projection applies
    /// every event type of that subject to the item. Otherwise the two drift
    /// apart as soon as an event lands in the subject that the view does not
    /// apply to the item, and every command with the revision of the item
    /// fails with a conflict, until an event changes the item again. For an
    /// event type the item does not change for, such as one the state ignores,
    /// call [`update`](InMemoryView::update) with a change that does nothing,
    /// which only moves the revision on. For an item that gathers several
    /// subjects, use
    /// [`Precondition::on_state_read`](crate::Precondition::on_state_read)
    /// instead.
    #[must_use]
    pub fn with_revision_in(
        key_of: impl Fn(&T) -> K + Send + Sync + 'static,
        field: impl Fn(&mut T) -> &mut Option<Revision> + Send + Sync + 'static,
    ) -> Self {
        Self::build(Box::new(key_of), Some(Box::new(field)))
    }

    fn build(key_of: KeyOf<K, T>, revision_in: Option<RevisionIn<T>>) -> Self {
        Self {
            shared: Arc::new(Shared {
                key_of,
                revision_in,
                entries: RwLock::new(Entries {
                    items: HashMap::new(),
                    order: Vec::new(),
                    indexes: Vec::new(),
                }),
                revision: watch::Sender::new(None),
            }),
        }
    }

    /// Records an event as processed, which moves the revision of the view
    /// on, and wakes up the readers waiting for it (see [`Revisioned`]). A
    /// projection calls it from [`Projection::seen`](crate::Projection::seen)
    /// for every event it has seen, also for one it ignores. Events may
    /// arrive more than once and out of order after a restart, so an event
    /// the view has already passed does not move the revision backwards.
    pub fn seen(&self, event_id: Revision) {
        let _ = self.shared.revision.send_if_modified(|seen| {
            if seen.is_some_and(|seen| seen >= event_id) {
                return false;
            }

            *seen = Some(event_id);
            true
        });
    }

    /// Adds an item for the given event, and reports what it did:
    /// [`Outcome::Added`] if it added the item, and
    /// [`Outcome::AlreadyApplied`] if it skipped the event, since an item with
    /// the same key has seen it, or a newer one. The latter is not an error,
    /// since it happens when events are applied a second time. To add an item
    /// or change an existing one, use [`upsert`](InMemoryView::upsert).
    ///
    /// # Errors
    ///
    /// Fails with [`ViewError::Permanent`] if the event is newer than an item
    /// with the same key, since a second item under that key points to a
    /// mistake in the events or the key. A later part of the same event counts
    /// as newer, so two parts of a split event that insert an item with the
    /// same key fail as well.
    pub fn insert<E>(&self, event: &Envelope<E>, item: T) -> Result<Outcome, ViewError> {
        let event = AppliedEvent::of(event);

        let mut entries = self.shared.write();
        let key = (self.shared.key_of)(&item);

        if let Some(entry) = entries.items.get(&key) {
            if !event.is_newer_than(entry) {
                return Ok(Outcome::AlreadyApplied);
            }

            return Err(ViewError::permanent(format!(
                "event {} inserts an item with the key {key:?}, which is already taken",
                event.id,
            )));
        }

        self.shared.add(&mut entries, key, item, &event);

        Ok(Outcome::Added)
    }

    /// Changes the item with the given key, or adds one if there is none, and
    /// reports what it did: [`Outcome::Added`] if it added the item,
    /// [`Outcome::Applied`] if it changed the existing one, and
    /// [`Outcome::AlreadyApplied`] if it skipped the event, since the existing
    /// item has seen it, or a newer one. The change starts from the existing
    /// item, or from the default of the item, so a single function describes
    /// both, and it has to set the fields that make up the key.
    ///
    /// Like with [`update`](InMemoryView::update), the change runs while the
    /// view is locked, so it must not call any function of the view.
    ///
    /// # Errors
    ///
    /// Fails with [`ViewError::Permanent`] if the key of the item, after the
    /// change, differs from the given one.
    pub fn upsert<E, Q>(
        &self,
        key: &Q,
        event: &Envelope<E>,
        change: impl FnOnce(&mut T),
    ) -> Result<Outcome, ViewError>
    where
        K: Borrow<Q>,
        Q: Debug + Eq + Hash + ?Sized,
        T: Default,
    {
        let event = AppliedEvent::of(event);

        let mut entries = self.shared.write();
        if entries.items.contains_key(key) {
            return self.shared.change(&mut entries, key, &event, change);
        }

        let mut item = T::default();
        change(&mut item);

        let item_key = (self.shared.key_of)(&item);
        if item_key.borrow() != key {
            return Err(ViewError::permanent(format!(
                "event {} upserts an item with the key {item_key:?} under the key {key:?}",
                event.id,
            )));
        }

        self.shared.add(&mut entries, item_key, item, &event);

        Ok(Outcome::Added)
    }

    /// Changes the item with the given key, and reports what it did:
    /// [`Outcome::Applied`] if it changed the item, [`Outcome::Missing`] if
    /// there is no such item, and [`Outcome::AlreadyApplied`] if the event is
    /// not newer than the item. Neither of the latter is an error, since both
    /// happen when events are applied a second time: a later event may have
    /// deleted the item already. A projection for which a missing item means
    /// that something is wrong says so itself.
    ///
    /// The change gets a clone of the item, which replaces the item once the
    /// change is accepted. A change that does nothing still moves the revision
    /// of the item on.
    ///
    /// The change runs while the view is locked, so it must not call any
    /// function of the view, not even [`get`](KeyedView::get) or
    /// [`all`](View::all), since that blocks forever. To copy data
    /// from another item of the view into this one, read it before, and use it
    /// in the change.
    ///
    /// # Errors
    ///
    /// Fails with [`ViewError::Permanent`] if the change changes the key of
    /// the item, which leaves the item as it was.
    pub fn update<E, Q>(
        &self,
        key: &Q,
        event: &Envelope<E>,
        change: impl FnOnce(&mut T),
    ) -> Result<Outcome, ViewError>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let event = AppliedEvent::of(event);

        self.shared
            .change(&mut self.shared.write(), key, &event, change)
    }

    /// Removes the item with the given key, and reports what it did, like
    /// [`update`](InMemoryView::update): [`Outcome::Applied`] if it removed
    /// the item, [`Outcome::Missing`] if there is no such item, and
    /// [`Outcome::AlreadyApplied`] if the event is not newer than the item.
    ///
    /// The revision of the item goes with it, so an older event that adds the
    /// item again, such as one that is applied a second time, adds it again.
    /// Removing never fails.
    pub fn delete<E, Q>(&self, key: &Q, event: &Envelope<E>) -> Outcome
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let event = AppliedEvent::of(event);

        self.shared.write().delete(key, &event)
    }

    /// Changes every matching item for which the event is newer, and reports
    /// how many it changed. Like with [`update`](InMemoryView::update), both
    /// the match and the change run while the view is locked, so neither of
    /// them may call any function of the view.
    ///
    /// # Errors
    ///
    /// Fails with [`ViewError::Permanent`] once the change changes the key of
    /// an item, which keeps the items changed before.
    pub fn update_where<E>(
        &self,
        matches: impl FnMut(&T) -> bool,
        event: &Envelope<E>,
        change: impl FnMut(&mut T),
    ) -> Result<usize, ViewError> {
        let event = AppliedEvent::of(event);

        let mut entries = self.shared.write();
        let keys = entries.keys_where(matches);

        self.shared.change_all(&mut entries, keys, &event, change)
    }

    /// Removes every matching item for which the event is newer, and reports
    /// how many it removed. Like with
    /// [`update_where`](InMemoryView::update_where), the match runs while the
    /// view is locked, so it must not call any function of the view. Removing
    /// never fails.
    pub fn delete_where<E>(&self, matches: impl FnMut(&T) -> bool, event: &Envelope<E>) -> usize {
        let event = AppliedEvent::of(event);

        let mut entries = self.shared.write();
        let keys = entries.keys_where(matches);

        entries.delete_all(&keys, &event)
    }
}

impl<K, T> Shared<K, T>
where
    K: Clone + Debug + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    // A panic in a function the view was given leaves the lock poisoned. The
    // view goes on all the same, since a change works on a clone, which never
    // replaces the item if it panics.
    fn read(&self) -> RwLockReadGuard<'_, Entries<K, T>> {
        self.entries.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, Entries<K, T>> {
        self.entries.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Writes the ID of the event into the field of the item, if the view
    /// keeps the revision there.
    fn set_revision(&self, item: &mut T, event: &AppliedEvent) {
        if let Some(field) = &self.revision_in {
            *field(item) = Some(event.id);
        }
    }

    /// Stores a new item, whose key is free.
    fn add(&self, entries: &mut Entries<K, T>, key: K, mut item: T, event: &AppliedEvent) {
        self.set_revision(&mut item, event);

        for index in &entries.indexes {
            index.add(&key, &item);
        }

        let position = entries.order.len();
        entries.order.push(key.clone());
        let _ = entries.items.insert(
            key,
            Entry {
                item,
                revision: event.id,
                part: event.part,
                position,
            },
        );
    }

    /// Applies a change to one item, if the event is newer.
    fn change<Q>(
        &self,
        entries: &mut Entries<K, T>,
        key: &Q,
        event: &AppliedEvent,
        change: impl FnOnce(&mut T),
    ) -> Result<Outcome, ViewError>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let Some((key, entry)) = entries.items.get_key_value(key) else {
            return Ok(Outcome::Missing);
        };
        if !event.is_newer_than(entry) {
            return Ok(Outcome::AlreadyApplied);
        }

        let mut changed = entry.item.clone();
        change(&mut changed);

        let changed_key = (self.key_of)(&changed);
        if changed_key != *key {
            return Err(ViewError::permanent(format!(
                "event {} changes the key of an item from {key:?} to {changed_key:?}",
                event.id,
            )));
        }

        self.set_revision(&mut changed, event);

        for index in &entries.indexes {
            index.remove(&changed_key, &entry.item);
            index.add(&changed_key, &changed);
        }

        let entry = entries
            .items
            .get_mut::<K>(&changed_key)
            .expect("the item is still there, since the view is locked");
        entry.item = changed;
        entry.revision = event.id;
        entry.part = event.part;

        Ok(Outcome::Applied)
    }

    /// Applies a change to the items with the given keys, and counts the ones
    /// it changed.
    fn change_all(
        &self,
        entries: &mut Entries<K, T>,
        keys: Vec<K>,
        event: &AppliedEvent,
        mut change: impl FnMut(&mut T),
    ) -> Result<usize, ViewError> {
        let mut changed = 0;
        for key in keys {
            if self.change(entries, &key, event, &mut change)? == Outcome::Applied {
                changed += 1;
            }
        }

        Ok(changed)
    }
}

impl<K: Eq + Hash, T> Entries<K, T> {
    /// Removes one item, if the event is newer.
    fn delete<Q>(&mut self, key: &Q, event: &AppliedEvent) -> Outcome
    where
        K: Borrow<Q> + Clone,
        Q: Eq + Hash + ?Sized,
    {
        let Some((key, entry)) = self.items.get_key_value(key) else {
            return Outcome::Missing;
        };
        if !event.is_newer_than(entry) {
            return Outcome::AlreadyApplied;
        }

        for index in &self.indexes {
            index.remove(key, &entry.item);
        }

        let key = key.clone();
        let _ = self.items.remove::<K>(&key);
        self.compact();

        Outcome::Applied
    }

    /// Removes the items with the given keys, and counts the ones it removed.
    fn delete_all(&mut self, keys: &[K], event: &AppliedEvent) -> usize
    where
        K: Clone,
    {
        keys.iter()
            .filter(|key| self.delete(*key, event) == Outcome::Applied)
            .count()
    }

    /// Returns the items with their keys, in the order of the view.
    fn listed(&self) -> impl Iterator<Item = (&K, &Entry<T>)> {
        self.order.iter().enumerate().filter_map(|(position, key)| {
            self.items
                .get_key_value(key)
                .filter(|(_, entry)| entry.position == position)
        })
    }

    /// Returns the keys of the matching items, in the order of the view.
    fn keys_where(&self, mut matches: impl FnMut(&T) -> bool) -> Vec<K>
    where
        K: Clone,
    {
        self.listed()
            .filter(|(_, entry)| matches(&entry.item))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Drops the keys of deleted items from the order once they make up most
    /// of it, so that deleting stays cheap, and the order does not grow
    /// without bounds.
    fn compact(&mut self) {
        if self.order.len() < 2 * self.items.len() + 16 {
            return;
        }

        let mut order = Vec::with_capacity(self.items.len());
        for (position, key) in self.order.drain(..).enumerate() {
            if let Some(entry) = self.items.get_mut(&key)
                && entry.position == position
            {
                entry.position = order.len();
                order.push(key);
            }
        }

        self.order = order;
    }
}

impl<K, T> View<T> for InMemoryView<K, T>
where
    K: Clone + Debug + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    type Error = Infallible;

    /// Hands out clones of every item, in the order in which they were
    /// inserted, where a key that was deleted and inserted again comes at its
    /// new position. The items are cloned when `all` is called, so a query
    /// can take its time without holding up the projection that feeds the
    /// view.
    fn all(&self) -> impl Stream<Item = Result<T, Infallible>> + Send {
        let items: Vec<T> = self
            .shared
            .read()
            .listed()
            .map(|(_, entry)| entry.item.clone())
            .collect();

        stream::iter(items.into_iter().map(Ok))
    }
}

impl<K, Q, T> KeyedView<Q, T> for InMemoryView<K, T>
where
    K: Borrow<Q> + Clone + Debug + Eq + Hash + Send + Sync + 'static,
    Q: Eq + Hash + ?Sized,
    T: Clone + Send + Sync + 'static,
{
    /// Returns a clone of the item with the given key, or `None` if there is
    /// none.
    fn get(&self, key: &Q) -> impl Future<Output = Result<Option<T>, Infallible>> + Send {
        future::ready(Ok(self
            .shared
            .read()
            .items
            .get(key)
            .map(|entry| entry.item.clone())))
    }
}

impl<K, T> Revisioned for InMemoryView<K, T>
where
    K: Clone + Debug + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// A view in memory keeps its revision in memory, so asking it never
    /// fails.
    type Error = Infallible;

    /// Returns the last event the view has seen.
    fn revision(&self) -> impl Future<Output = Result<Option<Revision>, Infallible>> + Send {
        future::ready(Ok(*self.shared.revision.borrow()))
    }

    async fn wait_for(&self, revision: Revision) -> Result<(), Infallible> {
        let mut seen = self.shared.revision.subscribe();
        drop(
            seen.wait_for(|seen| seen.is_some_and(|seen| seen >= revision))
                .await
                .expect("the view keeps its revision while it is borrowed"),
        );

        Ok(())
    }
}

impl<K, T> Debug for InMemoryView<K, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries = self
            .shared
            .entries
            .read()
            .unwrap_or_else(PoisonError::into_inner);

        formatter
            .debug_struct("InMemoryView")
            .field("items", &entries.items.len())
            .field("indexes", &entries.indexes.len())
            .field(
                "revision",
                &self.shared.revision.borrow().map(|seen| seen.to_string()),
            )
            .finish_non_exhaustive()
    }
}

/// What a change of an [`InMemoryView`] did to the one item it concerns. A
/// change of several items, such as [`InMemoryView::update_where`] or
/// [`InMemoryIndex::update`](crate::InMemoryIndex::update), reports how many
/// it changed instead, since each of them may fare differently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The change was applied to an item.
    Applied,
    /// There is no item to apply the change to.
    Missing,
    /// The item has seen the event, or a newer one, so the change was
    /// skipped.
    AlreadyApplied,
    /// The item was added.
    Added,
}

impl fmt::Display for Outcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Applied => "applied",
            Self::Missing => "missing",
            Self::AlreadyApplied => "already applied",
            Self::Added => "added",
        })
    }
}

/// Why a change of an [`InMemoryView`] was refused. It leaves the view as it
/// is, and belongs to the category [`Category::Permanent`], since applying the
/// same event again will fail again.
#[derive(Debug)]
pub enum ViewError {
    /// The event would leave the view with two items under the same key, or
    /// with an item under a key other than its own, which points to a mistake
    /// in the events or the key.
    Permanent(Permanent),
}

impl ViewError {
    fn permanent(message: String) -> Self {
        Self::Permanent(Permanent::Other(Failure::new(message, None)))
    }
}

impl fmt::Display for ViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Permanent(failure) => fmt::Display::fmt(failure, formatter),
        }
    }
}

impl Error for ViewError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Permanent(failure) => failure.source(),
        }
    }
}

impl Categorized for ViewError {
    fn category(&self) -> Option<Category> {
        Some(Category::Permanent)
    }
}
