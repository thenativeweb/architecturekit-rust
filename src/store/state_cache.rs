//! The states of the most recently used subjects.

use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
    sync::{Mutex, PoisonError},
};

use crate::{Revision, Subject};

#[cfg(test)]
mod tests;

/// Names a cached state. Besides the subject, the type of the state is part of
/// the key, since states of two types fold the events of the same subject into
/// something else.
type Key = (TypeId, Subject);

#[derive(Debug)]
struct Entry {
    state: Box<dyn Any + Send>,
    last_event_id: Revision,
    used: u64,
}

#[derive(Debug, Default)]
struct Entries {
    by_key: HashMap<Key, Entry>,
    // The keys by when they were used last, so that the first one is the one
    // to evict.
    by_use: BTreeMap<u64, Key>,
    uses: u64,
}

impl Entries {
    /// Marks the entry of the key as the one used most recently.
    fn touch(&mut self, key: &Key) {
        let Some(entry) = self.by_key.get_mut(key) else {
            return;
        };

        self.uses += 1;
        let _ = self.by_use.remove(&entry.used);
        entry.used = self.uses;
        let _ = self.by_use.insert(entry.used, key.clone());
    }
}

/// Holds the states of the most recently used subjects, together with the ID
/// of the last event each of them was built from.
///
/// Only what was read is cached, never what a command has written: without a
/// precondition, somebody else may have written in between, and reading from
/// the last event read onwards picks that up, as well as the events of the
/// command itself.
#[derive(Debug)]
pub(super) struct StateCache {
    max_subjects: usize,
    entries: Mutex<Entries>,
}

impl StateCache {
    /// Creates a cache for the given number of subjects, which the builder
    /// hands over only if it is at least 1.
    pub(super) fn new(max_subjects: usize) -> Self {
        Self {
            max_subjects,
            entries: Mutex::new(Entries::default()),
        }
    }

    /// Returns a copy of the cached state of the type on the subject, together
    /// with the ID of the last event it was built from, so that the caller can
    /// not change what the cache holds.
    pub(super) fn get<S: Clone + 'static>(&self, subject: &Subject) -> Option<(S, Revision)> {
        let mut entries = self.lock();
        let key = (TypeId::of::<S>(), subject.clone());

        entries.touch(&key);
        let entry = entries.by_key.get(&key)?;
        let state = entry
            .state
            .downcast_ref::<S>()
            .expect("the type of the state is part of the key");

        Some((state.clone(), entry.last_event_id))
    }

    /// Caches a state, unless a state built from a later event is cached
    /// already: two commands on the same subject may finish in any order, and
    /// the one that read less must not replace the one that read more. Once
    /// there are more subjects than the cache holds, it evicts the one used
    /// least recently.
    pub(super) fn put<S: Send + 'static>(
        &self,
        subject: &Subject,
        state: S,
        last_event_id: Revision,
    ) {
        let mut entries = self.lock();
        let key = (TypeId::of::<S>(), subject.clone());

        if let Some(entry) = entries.by_key.get_mut(&key) {
            if last_event_id >= entry.last_event_id {
                entry.state = Box::new(state);
                entry.last_event_id = last_event_id;
            }

            entries.touch(&key);
            return;
        }

        let _ = entries.by_key.insert(
            key.clone(),
            Entry {
                state: Box::new(state),
                last_event_id,
                used: 0,
            },
        );
        entries.touch(&key);

        if entries.by_key.len() > self.max_subjects
            && let Some((_, oldest)) = entries.by_use.pop_first()
        {
            let _ = entries.by_key.remove(&oldest);
        }
    }

    /// Locks the entries. Nothing that runs while they are locked can leave
    /// them half changed, apart from cloning a state, which changes nothing in
    /// the cache, so a lock that a panic poisoned is taken all the same.
    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
