use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    hash::Hash,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use super::{AppliedEvent, Entries, InMemoryView, Indexer, Shared, ViewError};
use crate::Envelope;

/// A secondary index of an [`InMemoryView`], created with
/// [`InMemoryView::index`], which finds the items by a value taken from them,
/// such as the shelf they stand on. Several items may share a value.
pub struct InMemoryIndex<K, T, V> {
    view: Arc<Shared<K, T>>,
    pub(super) index: Arc<Index<K, T, V>>,
}

/// The keys of the items by their value, which the view keeps up to date.
pub(super) struct Index<K, T, V> {
    value_of: Box<dyn Fn(&T) -> V + Send + Sync>,

    // Only changed while the view is locked for writing, so the lock of its
    // own never waits for long.
    keys: Mutex<HashMap<V, HashSet<K>>>,
}

impl<K, T> InMemoryView<K, T>
where
    K: Clone + Debug + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Adds a secondary index to the view, which finds the items by the value
    /// the given function takes from them, such as the user they belong to.
    /// Several items may share a value. The index is kept up to date with
    /// every change, also when the value of an item changes.
    ///
    /// ```
    /// use architecturekit::InMemoryView;
    ///
    /// #[derive(Clone, Debug, Default, PartialEq)]
    /// struct Book {
    ///     id: String,
    ///     shelf: String,
    /// }
    ///
    /// let books = InMemoryView::new(|book: &Book| book.id.clone());
    /// let by_shelf = books.index(|book| book.shelf.clone());
    ///
    /// assert!(by_shelf.lookup("A3").is_empty());
    /// ```
    ///
    /// Add indexes before the view is used, since adding one reads every
    /// item. The function runs while the view is locked, so it must not call
    /// any function of the view or of its indexes.
    pub fn index<V>(
        &self,
        value_of: impl Fn(&T) -> V + Send + Sync + 'static,
    ) -> InMemoryIndex<K, T, V>
    where
        V: Eq + Hash + Send + Sync + 'static,
    {
        let index = Arc::new(Index {
            value_of: Box::new(value_of),
            keys: Mutex::new(HashMap::new()),
        });

        let mut entries = self.shared.write();
        for (key, entry) in &entries.items {
            index.add(key, &entry.item);
        }
        let indexer: Arc<dyn Indexer<K, T>> = index.clone();
        entries.indexes.push(indexer);

        InMemoryIndex {
            view: Arc::clone(&self.shared),
            index,
        }
    }
}

impl<K, T, V> InMemoryIndex<K, T, V>
where
    K: Clone + Debug + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    V: Eq + Hash + Send + Sync + 'static,
{
    /// Returns clones of the items with the given value, in the order of the
    /// view.
    pub fn lookup<Q>(&self, value: &Q) -> Vec<T>
    where
        V: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let entries = self.view.read();

        self.keys_of(&entries, value)
            .iter()
            .map(|key| entries.items[key].item.clone())
            .collect()
    }

    /// Changes every item with the given value for which the event is newer,
    /// and reports how many it changed, like
    /// [`InMemoryView::update_where`]. The change runs while the view is
    /// locked, so it must not call any function of the view or of its
    /// indexes, not even [`lookup`](InMemoryIndex::lookup).
    ///
    /// # Errors
    ///
    /// Fails with [`ViewError::NotARevision`] if the ID of the event is not a
    /// revision, and with [`ViewError::Permanent`] once the change changes the
    /// key of an item, which keeps the items changed before.
    pub fn update<E, Q>(
        &self,
        value: &Q,
        event: &Envelope<E>,
        change: impl FnMut(&mut T),
    ) -> Result<usize, ViewError>
    where
        V: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let event = AppliedEvent::of(event)?;

        let mut entries = self.view.write();
        let keys = self.keys_of(&entries, value);

        self.view.change_all(&mut entries, keys, &event, change)
    }

    /// Removes every item with the given value for which the event is newer,
    /// and reports how many it removed, like [`InMemoryView::delete_where`].
    ///
    /// # Errors
    ///
    /// Fails with [`ViewError::NotARevision`] if the ID of the event is not a
    /// revision.
    pub fn delete<E, Q>(&self, value: &Q, event: &Envelope<E>) -> Result<usize, ViewError>
    where
        V: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let event = AppliedEvent::of(event)?;

        let mut entries = self.view.write();
        let keys = self.keys_of(&entries, value);

        Ok(entries.delete_all(&keys, &event))
    }

    /// Returns the keys of the items with the given value, in the order of the
    /// view.
    fn keys_of<Q>(&self, entries: &Entries<K, T>, value: &Q) -> Vec<K>
    where
        V: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut keys: Vec<(usize, K)> = self
            .index
            .keys()
            .get(value)
            .into_iter()
            .flatten()
            .filter_map(|key| Some((entries.items.get(key)?.position, key.clone())))
            .collect();
        keys.sort_unstable_by_key(|(position, _)| *position);

        keys.into_iter().map(|(_, key)| key).collect()
    }
}

impl<K, T, V> Index<K, T, V> {
    // Nothing that runs while the lock is held can panic but the hashing of a
    // key or a value, which leaves the keys as they were before.
    pub(super) fn keys(&self) -> MutexGuard<'_, HashMap<V, HashSet<K>>> {
        self.keys.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<K, T, V> Indexer<K, T> for Index<K, T, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Eq + Hash + Send + Sync,
{
    fn add(&self, key: &K, item: &T) {
        let value = (self.value_of)(item);

        let _ = self.keys().entry(value).or_default().insert(key.clone());
    }

    // A value that no item has any more is forgotten, so that an index over a
    // value that changes does not grow by one entry for every value it has
    // ever seen.
    fn remove(&self, key: &K, item: &T) {
        let value = (self.value_of)(item);

        let mut keys = self.keys();
        if let Some(keys_of_value) = keys.get_mut(&value) {
            let _ = keys_of_value.remove(key);
            if keys_of_value.is_empty() {
                let _ = keys.remove(&value);
            }
        }
    }
}

impl<K, T, V> Debug for InMemoryIndex<K, T, V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryIndex")
            .field("values", &self.index.keys().len())
            .finish_non_exhaustive()
    }
}
