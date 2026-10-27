use std::{collections::HashMap, future::Future, pin::pin, sync::Arc};

use ed25519_dalek::VerifyingKey;
use eventsourcingdb::{
    Client, EventCandidate,
    error::ClientError,
    request_options::{ReadEventMissingStrategy, ReadEventsOptions, ReadFromLatestEventOptions},
};
use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::{
    Event, EventSchema, Failure, Permanent, Precondition, State, Transient, UpcastEvent,
    command::write_preconditions,
    state::{evolve_by_stored, from_latest_of},
};

mod error;
mod failure;
#[cfg(test)]
mod tests;

pub use error::{LoadError, RegisterSchemasError, WriteError};
use failure::{database_failure, is_schema_conflict, read_failure, schema_refusal, write_failure};

/// Reads from and writes to EventSourcingDB, through the client SDK.
///
/// Create one with [`Store::builder`], from a client and the source to use for
/// all events it writes:
///
/// ```no_run
/// use architecturekit::Store;
/// use eventsourcingdb::Client;
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
}

#[derive(Debug)]
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
        }
    }
}

/// An event together with the subject it is written to, for [`Store::write`].
#[derive(Debug)]
pub struct EventOn {
    subject: String,
    event_type: &'static str,
    data: Result<Value, serde_json::Error>,
}

impl EventOn {
    /// Puts an event on a subject. Its data is written as JSON right away, and
    /// data that can not be written makes [`Store::write`] fail permanently.
    #[must_use]
    pub fn new<E: Event>(subject: impl Into<String>, event: &E) -> Self {
        Self {
            subject: subject.into(),
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
    /// event, if the data of an event does not match its type, or if the
    /// database refuses the request.
    ///
    /// # Panics
    ///
    /// A state that starts from an event type it has no rule for is a
    /// programming error, so this panics (see [`State::from_latest`]).
    pub async fn load<S: State>(&self, subject: &str) -> Result<S, LoadError> {
        Ok(self.fold::<S>(subject).await?.0)
    }

    /// Reads the state of a subject, and returns it together with the ID of the
    /// last event read, or `None` for a subject without events.
    pub(crate) async fn fold<S: State>(
        &self,
        subject: &str,
    ) -> Result<(S, Option<String>), LoadError> {
        let doing = format!("reading {subject:?}");
        let options = ReadEventsOptions {
            from_latest_event: from_latest_of::<S>().map(|event_type| ReadFromLatestEventOptions {
                if_event_is_missing: ReadEventMissingStrategy::ReadEverything,
                subject,
                ty: event_type,
            }),
            ..ReadEventsOptions::default()
        };

        let events = self
            .client
            .read_events(subject, Some(options))
            .await
            .map_err(|error| LoadError::from(read_failure(error, &doing)))?;
        let mut events = pin!(events);

        let mut current = S::default();
        let mut last_event_id = None;

        while let Some(event) = events.next().await {
            let event = event.map_err(|error| LoadError::from(read_failure(error, &doing)))?;
            self.verify(&event)?;

            last_event_id = Some(event.id().to_owned());
            current = evolve_by_stored(current, UpcastEvent::from_stored(&event))?;
        }

        Ok((current, last_event_id))
    }

    /// Checks an event the store has read: its hash, unless the store was
    /// built without, and its signature as well, if it was built with a
    /// verification key. It runs on the event as stored, before any upcaster
    /// changes it.
    fn verify(&self, event: &eventsourcingdb::Event) -> Result<(), Permanent> {
        let verified = match &self.verification {
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
    /// [`WriteError::Permanent`] if the write certainly stored nothing, with
    /// [`WriteError::OutcomeUnknown`] if it may have been stored nonetheless,
    /// and with [`WriteError::NotARevision`] for an ID of
    /// [`Precondition::on_event_id`] that is not a revision.
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
            first.subject
        );
        let candidates = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| self.candidate_for(index, event))
            .collect::<Result<Vec<_>, _>>()?;

        self.write_candidates(candidates, preconditions, doing)
            .await
    }

    /// Turns an event into what the client writes, with the source of the
    /// store. Data that can not be written fails permanently, naming the event
    /// type, the subject and the reason.
    fn candidate_for(&self, index: usize, event: EventOn) -> Result<EventCandidate, Permanent> {
        if event.subject.is_empty() {
            return Err(Permanent::Other(Failure::new(
                format!("event {index} of a write needs a subject"),
                None,
            )));
        }

        let data = event.data.map_err(|cause| {
            Permanent::Other(Failure::new(
                format!(
                    "refusing to write an event of type {:?} to {:?}, since its data can not be encoded as JSON",
                    event.event_type, event.subject,
                ),
                Some(Box::new(cause)),
            ))
        })?;

        Ok(EventCandidate {
            data,
            source: self.source.clone(),
            subject: event.subject,
            ty: event.event_type.to_owned(),
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
    /// types to the end, and deliberately does not ask for a single one, as
    /// ArchitectureKit for Go does not: after reading a single event type that
    /// further event types follow, EventSourcingDB 1.2.0 stops answering
    /// writes.
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
