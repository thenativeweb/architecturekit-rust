use std::{collections::HashMap, future::Future, pin::pin, sync::Arc, time::Duration};

use ed25519_dalek::VerifyingKey;
use eventsourcingdb::{
    Client, EventCandidate,
    error::ClientError,
    request_options::{
        Bound, BoundType, ReadEventMissingStrategy, ReadEventsOptions, ReadFromLatestEventOptions,
    },
};
use futures_util::{Stream, StreamExt, future};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::Reconnect;
use crate::{
    Command, Decider, Event, EventSchema, Events, Failure, Permanent, Precondition, ReadOptions,
    Revision, State, Subject, Subjects, Transient, UpcastEvent,
    command::{
        checked_preconditions, resolve_preconditions, violated_precondition, write_preconditions,
    },
    read::{DatabaseBounds, check_read_options, database_options},
    state::{evolve_by_stored, from_latest_of},
};

mod any;
mod error;
mod failure;
pub(crate) mod projecting;

use projecting::Reconnecting;
mod state_cache;
#[cfg(test)]
mod tests;

pub use any::Error;
pub use error::{ExecuteError, LoadError, ReadError, RegisterSchemasError, WriteError};
use failure::{database_failure, is_schema_conflict, read_failure, schema_refusal, write_failure};
use state_cache::StateCache;

/// Reads from and writes to EventSourcingDB, through the client SDK.
///
/// Create one with [`Store::builder`], from a client and the source to use for
/// all events it writes:
///
/// ```no_run
/// use architecturekit::{Store, eventsourcingdb::Client};
///
/// let client = Client::new("http://localhost:3000".parse().unwrap(), "secret");
/// let store = Store::builder(client, "https://library.eventsourcingdb.io").build();
/// ```
///
/// The store checks the hash of every event it reads, before any upcaster or
/// state sees it, and an event whose hash does not match its content makes
/// reading fail with [`Permanent::Unverified`]. To turn this off, use
/// [`StoreBuilder::without_hash_verification`].
#[derive(Debug)]
pub struct Store {
    client: Arc<Client>,
    source: String,
    verification: Verification,
    conflict_retries: usize,
    state_cache: Option<StateCache>,
    reconnecting: Reconnecting,
}

#[derive(Clone, Debug)]
enum Verification {
    Hash,
    Nothing,
    Signature(VerifyingKey),
}

/// Builds a [`Store`], which [`Store::builder`] creates.
#[derive(Debug)]
pub struct StoreBuilder {
    client: Client,
    source: String,
    skips_hashes: bool,
    verification_key: Option<VerifyingKey>,
    conflict_retries: usize,
    max_cached_subjects: usize,
    reconnecting: Reconnecting,
}

impl StoreBuilder {
    /// Turns off checking the hash of every event the store reads. Checking a
    /// hash takes about a microsecond per event, so there is rarely a reason
    /// to, and an event that was changed after it had been written goes
    /// unnoticed then.
    #[must_use]
    pub fn without_hash_verification(mut self) -> Self {
        self.skips_hashes = true;
        self
    }

    /// Checks the signature of every event the store reads, besides its hash,
    /// against the verification key of the database. An event without a
    /// signature, or with one that does not match the key, makes reading fail
    /// with [`Permanent::Unverified`].
    ///
    /// The database signs events only if it runs with a signing key, and it
    /// signs them when handing them out, with the key it has at that moment.
    /// After the key is rotated, hand over the new verification key.
    #[must_use]
    pub fn signature_verification(mut self, verification_key: VerifyingKey) -> Self {
        self.verification_key = Some(verification_key);
        self
    }

    /// Sets how long the projections started on the store wait before they
    /// read again after reading failed, which includes a stream of events that
    /// ended. The delay starts at the initial delay, doubles with every attempt
    /// in a row, and never exceeds the maximum delay. It starts over once a
    /// projection has made progress, or has followed the events for longer than
    /// the initial delay, even if no event arrived, as when a load balancer
    /// ends long-lived connections regularly. Without this, the delays are 1
    /// second and 1 minute.
    ///
    /// # Panics
    ///
    /// An initial delay of zero is a programming error, since the projections
    /// would then read again without any pause, and so is a maximum delay below
    /// the initial delay. Both panic.
    #[must_use]
    pub fn reconnect_delays(mut self, initial_delay: Duration, max_delay: Duration) -> Self {
        assert!(
            !initial_delay.is_zero(),
            "architecturekit: reconnect_delays needs an initial delay that is positive, not {initial_delay:?}"
        );
        assert!(
            max_delay >= initial_delay,
            "architecturekit: reconnect_delays needs a maximum delay that is not below the initial delay of {initial_delay:?}, not {max_delay:?}"
        );

        self.reconnecting.initial_delay = initial_delay;
        self.reconnecting.max_delay = max_delay;
        self
    }

    /// Calls the given function whenever a projection started on the store is
    /// about to read again, before it waits, with what it reads again for. A
    /// panic of the function ends the run of the projection, as a panic of the
    /// projection does.
    #[must_use]
    pub fn reconnect_observer(
        mut self,
        observe: impl Fn(&Reconnect) + Send + Sync + 'static,
    ) -> Self {
        self.reconnecting.observer = Some(Arc::new(observe));
        self
    }

    /// Keeps the states of the most recently used subjects in memory, up to the
    /// given number, so that the next command on one of them reads only the
    /// events written since. A number of 0 keeps none, which is the same as
    /// leaving it out, so that a configuration can turn the cache off.
    ///
    /// The cache is correct with several processes writing to the same
    /// subjects, since every read still reads all events after the ones it
    /// has cached. It tells states apart by their type and their subject, and
    /// it hands out a clone of the cached state, so that the caller can not
    /// change what the cache holds.
    #[must_use]
    pub fn state_cache(mut self, max_subjects: usize) -> Self {
        self.max_cached_subjects = max_subjects;
        self
    }

    /// Has [`Store::execute`] decide again, up to the given number of times,
    /// if a command could not be written because something else was written to
    /// its subject after the state had been read. It then reads the state anew
    /// and the decider decides on it, as if the command had arrived a moment
    /// later. Without this, a conflict is reported right away.
    ///
    /// This applies only to commands whose preconditions include
    /// [`Precondition::on_state_read`], since only then can the conflict come
    /// from the state that was read. A command that checks only a revision the
    /// caller hands over is never decided again: the caller has to learn about
    /// the conflict, and deciding again would fail the same way. A command that
    /// checks [`on_state_read`](Precondition::on_state_read) together with any
    /// other precondition, such as one made with
    /// [`require`](Precondition::require), is decided again only if
    /// [`on_state_read`](Precondition::on_state_read) did not hold. A database
    /// that offers the v2 API names the precondition that did not hold, so if
    /// it is another one, such as an outdated revision of the caller, the
    /// conflict is reported at once. A database that offers just the v1 API,
    /// such as EventSourcingDB 1.2, does not name it, so then the command is
    /// decided again whenever [`on_state_read`](Precondition::on_state_read) is
    /// among its preconditions, and if another one did not hold, every attempt
    /// fails the same way until the retries are used up. On the same subject, a
    /// revision of the caller and [`on_state_read`](Precondition::on_state_read)
    /// need not both be checked: if the revision of the caller holds, so does
    /// the other.
    ///
    /// Once the retries are used up, or once the database names another
    /// precondition, the conflict is reported as a [`Transient::Conflict`]
    /// either way, whose source is the answer of the database, so that its
    /// details can be read. [`Store::write`] never decides again, since it
    /// decides nothing. Nor is a write tried again whose outcome is unknown,
    /// since its events may have been stored (see
    /// [`ExecuteError::OutcomeUnknown`]).
    #[must_use]
    pub fn conflict_retries(mut self, retries: usize) -> Self {
        self.conflict_retries = retries;
        self
    }

    /// Builds the store.
    ///
    /// # Panics
    ///
    /// Turning off checking the hashes contradicts checking the signatures,
    /// which checks the hashes as well, so asking for both is a programming
    /// error, and this panics.
    #[must_use]
    pub fn build(self) -> Store {
        let verification = match (self.skips_hashes, self.verification_key) {
            (true, Some(_)) => panic!(
                "architecturekit: without_hash_verification contradicts signature_verification, which checks the hash as well"
            ),
            (true, None) => Verification::Nothing,
            (false, Some(key)) => Verification::Signature(key),
            (false, None) => Verification::Hash,
        };

        Store {
            client: Arc::new(self.client),
            source: self.source,
            verification,
            conflict_retries: self.conflict_retries,
            state_cache: (self.max_cached_subjects > 0)
                .then(|| StateCache::new(self.max_cached_subjects)),
            reconnecting: self.reconnecting,
        }
    }
}

/// An event together with the subject it is written to, for [`Store::write`].
#[derive(Debug)]
pub struct EventOn {
    subject: Subject,
    event_type: &'static str,
    data: Result<Value, serde_json::Error>,
}

impl EventOn {
    /// Puts an event on a subject. Its data is written as JSON right away, and
    /// data that can not be written makes [`Store::write`] fail permanently.
    #[must_use]
    pub fn new<E: Event>(subject: Subject, event: &E) -> Self {
        Self {
            subject,
            event_type: E::EVENT_TYPE,
            data: serde_json::to_value(event),
        }
    }
}

impl Store {
    /// Creates a builder for a store that reads and writes with the given
    /// client, and writes its events with the given source.
    #[must_use]
    pub fn builder(client: Client, source: impl Into<String>) -> StoreBuilder {
        StoreBuilder {
            client,
            source: source.into(),
            skips_hashes: false,
            verification_key: None,
            conflict_retries: 0,
            max_cached_subjects: 0,
            reconnecting: Reconnecting::default(),
        }
    }

    /// Reads the state of a subject, from the events of the subject alone,
    /// starting from [`Default`], or from the latest event of the type the
    /// state names with [`State::from_latest`]. Use it for a query that needs
    /// the state of a single subject, rather than a view across many.
    ///
    /// Every event is verified before the upcasters of the state, and then the
    /// state, see it. Dropping the future before the state is read completely
    /// returns nothing, so part of a state never reaches a caller.
    ///
    /// # Errors
    ///
    /// Fails transiently if the database can not be reached, and permanently
    /// if an event fails its verification, if the state has no rule for an
    /// event, if the data of an event does not match its type, if the ID of
    /// an event is not a revision or its subject not a subject, neither of
    /// which the database ever writes, or if the database refuses the request.
    ///
    /// # Panics
    ///
    /// A state that starts from an event type it has no rule for is a
    /// programming error, so this panics (see [`State::from_latest`]).
    pub async fn load<S: State>(&self, subject: &Subject) -> Result<S, LoadError> {
        Ok(self.fold::<S>(subject).await?.0)
    }

    /// Reads the state of a subject, and returns it together with the ID of the
    /// last event read, or `None` for a subject without events.
    ///
    /// With a state cache, it continues from the cached state, and reads only
    /// the events after the one that state was built from.
    pub(crate) async fn fold<S: State>(
        &self,
        subject: &Subject,
    ) -> Result<(S, Option<Revision>), LoadError> {
        let doing = format!("reading {:?}", subject.as_str());
        let from_latest = from_latest_of::<S>();
        let cached = self
            .state_cache
            .as_ref()
            .and_then(|cache| cache.get::<S>(subject));
        let (mut current, cached_event_id) = match cached {
            Some((state, last_event_id)) => (state, Some(last_event_id)),
            None => (S::default(), None),
        };

        let after = cached_event_id.map(|last_event_id| last_event_id.to_string());
        let options = match &after {
            Some(last_event_id) => ReadEventsOptions {
                lower_bound: Some(Bound {
                    bound_type: BoundType::Exclusive,
                    id: last_event_id,
                }),
                ..ReadEventsOptions::default()
            },
            None => ReadEventsOptions {
                from_latest_event: from_latest.map(|event_type| ReadFromLatestEventOptions {
                    if_event_is_missing: ReadEventMissingStrategy::ReadEverything,
                    subject: subject.as_str(),
                    ty: event_type,
                }),
                ..ReadEventsOptions::default()
            },
        };

        let events = self
            .client
            .read_events(subject.as_str(), Some(options))
            .await
            .map_err(|error| LoadError::from(read_failure(error, &doing)))?;
        let mut events = pin!(events);

        let mut last_event_id = cached_event_id;

        while let Some(event) = events.next().await {
            let event = event.map_err(|error| LoadError::from(read_failure(error, &doing)))?;
            self.verify(&event)?;

            let event = UpcastEvent::from_stored(&event)?;
            last_event_id = Some(event.id());
            current = evolve_by_stored(current, event)?;
        }

        if let (Some(cache), Some(last_event_id)) = (&self.state_cache, last_event_id) {
            cache.put(subject, current.clone(), last_event_id);
        }

        Ok((current, last_event_id))
    }

    /// Hands out the events of the given subjects as they are stored, for a read
    /// that neither [`load`](Store::load) nor a projection fits: one page of a
    /// long stream, the events up to a certain one, or a single event. Every
    /// event is verified like everything else the store reads, before the
    /// caller sees it. Neither upcasters nor states apply, since there is no
    /// state.
    ///
    /// Without options, it hands out every event of the subjects, oldest
    /// first. The bounds and the order of [`ReadOptions`] narrow that down.
    /// The stream ends with the first failure, and dropping it stops reading.
    ///
    /// # Errors
    ///
    /// Fails before the database is asked, as [`check_read_options`] does,
    /// with [`ReadError::EmptyRange`] for bounds that leave no room for an
    /// event. Otherwise it fails, or the stream ends, with
    /// [`ReadError::Transient`] if the database can not be reached, and with
    /// [`ReadError::Permanent`] if an event fails its verification or the
    /// database refuses the read.
    ///
    /// # Panics
    ///
    /// Panics for options that contradict each other (see
    /// [`LowerBound::FromLatestEvent`](crate::LowerBound::FromLatestEvent)).
    pub async fn read<'a>(
        &'a self,
        subjects: &'a Subjects,
        options: &'a ReadOptions,
    ) -> Result<impl Stream<Item = Result<eventsourcingdb::Event, ReadError>> + 'a, ReadError> {
        check_read_options(options)?;

        let doing = format!("reading {:?}", subjects.subject().as_str());
        let bounds = DatabaseBounds::of(options);

        // A read that the database refuses at once fails here, rather than as
        // the first element of the stream.
        let events = self
            .client
            .read_events(
                subjects.subject().as_str(),
                Some(database_options(subjects, options, &bounds)),
            )
            .await
            .map_err(|error| ReadError::from(read_failure(error, &doing)))?;

        Ok(events
            .map(move |event| {
                let event = event.map_err(|error| ReadError::from(read_failure(error, &doing)))?;
                self.verify(&event)?;

                Ok(event)
            })
            // The stream ends with the first failure.
            .scan(false, |has_failed, event| {
                if *has_failed {
                    return future::ready(None);
                }
                *has_failed = event.is_err();

                future::ready(Some(event))
            }))
    }

    /// Checks an event the store has read: its hash, unless the store was
    /// built without, and its signature as well, if it was built with a
    /// verification key. It runs on the event as stored, before any upcaster
    /// changes it.
    fn verify(&self, event: &eventsourcingdb::Event) -> Result<(), Permanent> {
        verify(&self.verification, event)
    }

    /// Executes a command: reads the state of its subject, lets the decider
    /// decide on it, and writes the events it decided on to the subject, under
    /// the preconditions the command declares. It returns the events as the
    /// database recorded them, or none if the decider decided on none.
    ///
    /// [`Precondition::on_state_read`] is filled in with the last event read,
    /// so that the events are only written if the state they were decided on
    /// still holds. A conflict is reported, and the caller decides what to do
    /// about it, unless the store was built with
    /// [`StoreBuilder::conflict_retries`] and the conflict may come from the
    /// state that was read, in which case it decides again first.
    ///
    /// The decider decides on variants of the events of the state, so the
    /// state has a rule for every event that is written, and can read the
    /// subject for the next command. If the data of one of the events can not
    /// be encoded as JSON, none of them is written, and executing fails
    /// permanently, naming the event type, the subject and the reason, since
    /// trying again would fail the same way.
    ///
    /// Dropping the future before the write has begun writes nothing, also
    /// while the decider decides. Once the write has begun, it runs on a task
    /// of its own, as with [`Store::write`], so that it is finished even if the
    /// caller stops waiting for it.
    ///
    /// # Errors
    ///
    /// Fails with [`ExecuteError::Decider`] if the decider refuses the command,
    /// with [`ExecuteError::Transient`], including a [`Transient::Conflict`]
    /// for a precondition that did not hold, or with
    /// [`ExecuteError::Permanent`] if nothing was written, and with
    /// [`ExecuteError::OutcomeUnknown`] if the events may have been stored
    /// nonetheless. The preconditions are checked before anything is read, as
    /// [`check_preconditions`](crate::check_preconditions) checks them.
    ///
    /// # Panics
    ///
    /// A state that starts from an event type it has no rule for is a
    /// programming error, so this panics (see [`State::from_latest`]).
    pub async fn execute<C, S, D>(
        &self,
        decider: &Decider<C, S, D>,
        command: C,
    ) -> Result<Vec<eventsourcingdb::Event>, ExecuteError<D>>
    where
        C: Command + Clone,
        S: State,
    {
        let declared = checked_preconditions(&command)?;
        let retries = if declared.iter().any(Precondition::is_on_state_read) {
            self.conflict_retries
        } else {
            0
        };

        let mut attempt = 0;
        loop {
            let conflict = match self.execute_once(decider, command.clone()).await {
                Err(ExecuteError::Transient(Transient::Conflict(conflict)))
                    if attempt < retries =>
                {
                    conflict
                }
                executed => return executed,
            };

            // Deciding again only helps if the state that was read has
            // changed. If the database names another precondition as the one
            // that did not hold, every attempt would fail the same way.
            if violated_precondition(&conflict, &declared)
                .is_some_and(|violated| !violated.is_on_state_read())
            {
                return Err(ExecuteError::Transient(Transient::Conflict(conflict)));
            }

            attempt += 1;
        }
    }

    /// Reads the state, decides, and writes, once. The preconditions of the
    /// client SDK can not be copied, so every attempt asks the command for
    /// them anew.
    async fn execute_once<C: Command, S: State, D>(
        &self,
        decider: &Decider<C, S, D>,
        command: C,
    ) -> Result<Vec<eventsourcingdb::Event>, ExecuteError<D>> {
        let subject = command.subject();
        let declared = command.preconditions();

        let (state, last_event_id) = self.fold::<S>(&subject).await?;

        let events = decider
            .decide(command, state)
            .await
            .map_err(ExecuteError::Decider)?;
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // None of the events is written if the data of one of them can not be
        // encoded.
        let candidates = events
            .into_iter()
            .map(|event| self.candidate(subject.clone(), event.event_type(), event.to_data()))
            .collect::<Result<Vec<_>, _>>()?;

        let preconditions = resolve_preconditions(&subject, declared, last_event_id);
        let doing = format!("writing {:?}", subject.as_str());

        Ok(self
            .write_candidates(candidates, preconditions, doing)
            .await?)
    }

    /// Writes events to one or several subjects at once, so that either all of
    /// them are written or none, under the given preconditions. Use it for
    /// what is not a decision on the state of a single subject, such as a
    /// result derived from many of them.
    ///
    /// Like a command, a write declares at least one precondition, such as
    /// [`Precondition::on_pristine_subject`], or
    /// [`Precondition::unconditionally`] to write without any.
    /// [`Precondition::on_state_read`] has nothing to guard, since a write
    /// reads no state. Writing no events writes nothing, but the preconditions
    /// are checked first.
    ///
    /// Once the write has begun, it runs on a task of its own, so that it is
    /// finished even if the caller stops waiting for it: a write cut off in
    /// the middle would leave open whether the events were stored. If it fails
    /// after its caller has stopped waiting, the failure is logged.
    ///
    /// A write knows no state, so every state that reads one of the subjects
    /// needs a variant for each event type written to it, or reading fails
    /// afterwards.
    ///
    /// # Errors
    ///
    /// Fails with [`WriteError::Transient`], including a
    /// [`Transient::Conflict`] for a precondition that did not hold, or with
    /// [`WriteError::Permanent`] if the write certainly stored nothing, and
    /// with [`WriteError::OutcomeUnknown`] if it may have been stored
    /// nonetheless.
    pub async fn write(
        &self,
        events: Vec<EventOn>,
        preconditions: Vec<Precondition>,
    ) -> Result<Vec<eventsourcingdb::Event>, WriteError> {
        let preconditions = write_preconditions(preconditions)?;
        let Some(first) = events.first() else {
            return Ok(Vec::new());
        };

        let doing = format!(
            "writing {} events, the first to {:?}",
            events.len(),
            first.subject.as_str()
        );
        let candidates = events
            .into_iter()
            .map(|event| self.candidate(event.subject, event.event_type, event.data))
            .collect::<Result<Vec<_>, _>>()?;

        self.write_candidates(candidates, preconditions, doing)
            .await
    }

    /// Turns the encoded data of an event of the given type into what the
    /// client writes to the subject, with the source of the store. Data that
    /// could not be encoded fails permanently, naming the event type, the
    /// subject and the reason. Execute and write both encode with it.
    fn candidate(
        &self,
        subject: Subject,
        event_type: &str,
        data: Result<Value, serde_json::Error>,
    ) -> Result<EventCandidate, Permanent> {
        let data = data.map_err(|cause| unencodable(event_type, subject.as_str(), cause))?;

        Ok(EventCandidate {
            data,
            source: self.source.clone(),
            subject: subject.into(),
            ty: event_type.to_owned(),
            traceinfo: None,
        })
    }

    /// Writes the candidates under the given preconditions, on a task of its
    /// own, and returns them as the database recorded them.
    pub(crate) async fn write_candidates(
        &self,
        candidates: Vec<EventCandidate>,
        preconditions: Vec<eventsourcingdb::Precondition>,
        doing: String,
    ) -> Result<Vec<eventsourcingdb::Event>, WriteError> {
        let client = Arc::clone(&self.client);
        let written = detached(doing.clone(), async move {
            client.write_events(candidates, preconditions).await
        })
        .await;

        match written {
            Some(Ok(written)) => Ok(written),
            Some(Err(error)) => Err(write_failure(error, &doing).into()),
            None => Err(WriteError::OutcomeUnknown(Failure::new(
                format!("{doing}: the write ended without an answer"),
                None,
            ))),
        }
    }

    /// Registers the schemas of the given events with the database, such as
    /// those of `BookEvent::schemas()`. It is meant to be called on every
    /// start: for an event type the database knows already, it checks that the
    /// registered schema is the given one.
    ///
    /// A registered schema can not change. If it differs from the given one,
    /// or if the database refuses the given one, for example because stored
    /// events of the type do not match it, this fails permanently. To change
    /// the shape of an event, introduce a new event type and an upcaster
    /// instead. A registration that has begun runs on a task of its own, as a
    /// write does.
    ///
    /// # Errors
    ///
    /// Fails transiently if the database can not be reached, and permanently
    /// if a schema differs from the registered one, if the same event type
    /// comes with two different schemas, or if the database refuses a schema.
    pub async fn register_schemas(
        &self,
        schemas: impl IntoIterator<Item = EventSchema>,
    ) -> Result<(), RegisterSchemasError> {
        let given = collect_schemas(schemas)?;
        let mut registered = self.read_registered_schemas().await?;

        for schema in given {
            let event_type = schema.event_type();

            if !registered.contains_key(event_type) {
                let doing = format!("registering schema for {event_type:?}");
                let client = Arc::clone(&self.client);
                let value = schema.schema().clone();
                let refusal = match detached(doing.clone(), async move {
                    client
                        .register_event_schema(event_type, &value)
                        .await
                        .map(|_| ())
                })
                .await
                {
                    Some(Ok(())) => continue,
                    Some(Err(refusal)) => refusal,
                    None => {
                        return Err(RegisterSchemasError::Transient(Transient::Other(
                            Failure::new(
                                format!("{doing}: the registration ended without an answer"),
                                None,
                            ),
                        )));
                    }
                };

                if !is_schema_conflict(&refusal) {
                    return Err(database_failure(refusal, &doing).into());
                }

                // Either another instance has registered the schema in the
                // meantime, or the database refused it, for example because
                // stored events of the type do not match it. Only the
                // registered schemas tell the two apart.
                registered = self.read_registered_schemas().await?;
                if !registered.contains_key(event_type) {
                    return Err(schema_refusal(event_type, refusal).into());
                }
            }

            if registered.get(event_type) != Some(schema.schema()) {
                return Err(Permanent::Other(Failure::new(
                    format!(
                        "the schema of {event_type:?} differs from the registered one, which cannot change; \
                         introduce a new event type and an upcaster instead",
                    ),
                    None,
                ))
                .into());
            }
        }

        Ok(())
    }

    /// Reads the schemas the database holds, by event type. It reads all event
    /// types to the end, and deliberately does not ask for a single one: after
    /// reading a single event type that further event types follow,
    /// EventSourcingDB 1.2.0 stops answering writes.
    async fn read_registered_schemas(
        &self,
    ) -> Result<HashMap<String, Value>, RegisterSchemasError> {
        let doing = "reading the registered schemas";
        let event_types = self
            .client
            .read_event_types()
            .await
            .map_err(|error| RegisterSchemasError::from(read_failure(error, doing)))?;
        let mut event_types = pin!(event_types);

        let mut registered = HashMap::new();
        while let Some(event_type) = event_types.next().await {
            let event_type = event_type
                .map_err(|error| RegisterSchemasError::from(read_failure(error, doing)))?;
            if let Some(schema) = event_type.schema {
                let _ = registered.insert(event_type.ty, schema);
            }
        }

        Ok(registered)
    }
}

/// Checks an event that was read: its hash, unless the store was built without,
/// and its signature as well, if it was built with a verification key. It runs
/// on the event as stored, before any upcaster changes it.
fn verify(verification: &Verification, event: &eventsourcingdb::Event) -> Result<(), Permanent> {
    let verified = match verification {
        Verification::Signature(key) => event.verify_signature(key),
        Verification::Hash => event.verify_hash(),
        Verification::Nothing => Ok(()),
    };

    verified.map_err(|cause| {
        Permanent::Unverified(Failure::new(
            format!("event {} on {:?}", event.id(), event.subject()),
            Some(Box::new(cause)),
        ))
    })
}

/// Collects the given schemas, leaving out event types that appear more than
/// once with the same schema.
fn collect_schemas(
    schemas: impl IntoIterator<Item = EventSchema>,
) -> Result<Vec<EventSchema>, Permanent> {
    let mut collected: Vec<EventSchema> = Vec::new();

    for schema in schemas {
        match collected
            .iter()
            .find(|earlier| earlier.event_type() == schema.event_type())
        {
            Some(earlier) if earlier.schema() == schema.schema() => {}
            Some(_) => {
                return Err(Permanent::Other(Failure::new(
                    format!(
                        "event type {:?} has two different schemas",
                        schema.event_type()
                    ),
                    None,
                )));
            }
            None => collected.push(schema),
        }
    }

    Ok(collected)
}

/// Refuses to write an event whose data can not be encoded as JSON, naming its
/// type, the subject, and the reason, since trying again would fail the same
/// way. Executing, writing, and the test fixture refuse with it alike.
pub(crate) fn unencodable(event_type: &str, subject: &str, cause: serde_json::Error) -> Permanent {
    Permanent::Other(Failure::new(
        format!(
            "refusing to write an event of type {event_type:?} to {subject:?}, since its data can not be encoded as JSON",
        ),
        Some(Box::new(cause)),
    ))
}

/// Runs work against the database on a task of its own, so that it is
/// finished even if the caller stops waiting for it, and returns its result,
/// or `None` if the task ended without one. If the work fails after its caller
/// has stopped waiting, the failure is logged, since nobody else learns of it.
async fn detached<T: Send + 'static>(
    doing: String,
    work: impl Future<Output = Result<T, ClientError>> + Send + 'static,
) -> Option<Result<T, ClientError>> {
    let (sender, receiver) = oneshot::channel();

    drop(tokio::spawn(async move {
        let result = work.await;
        if let Err(Err(error)) = sender.send(result) {
            log::error!(
                "architecturekit: {doing} failed after its caller had stopped waiting: {error}"
            );
        }
    }));

    receiver.await.ok()
}
