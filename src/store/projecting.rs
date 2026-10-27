//! Drives projections, as ArchitectureKit for Go does: it catches up from the
//! checkpoint of a projection, in batches, verifies every event before the
//! projection applies it, and follows the events as they are written, reading
//! again after a failure that may pass.

use std::{
    fmt,
    future::{self, Future},
    ops::Deref,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};

use eventsourcingdb::{
    Client,
    error::ClientError,
    request_options::{Bound, BoundType, ObserveEventsOptions, ReadEventsOptions},
};
use futures_util::{FutureExt, Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use super::{
    Store, Verification,
    failure::{Sorted, read_failure},
    verify,
};
use crate::{
    Categorized, Category, Failure, Permanent, Projected, Projection, ProjectionError,
    ProjectionOptions, ProjectionRun, Reconnect, ResumableProjection, Subjects, Transaction,
    TransactionalProjection, Transient,
};

#[cfg(test)]
mod tests;

/// What driving needs of a projection, which hides the three kinds of
/// projections behind one shape.
pub(crate) trait Writer: Send {
    /// Returns where to go on from.
    fn checkpoint(
        &mut self,
    ) -> impl Future<Output = Result<Option<String>, ProjectionError>> + Send;

    /// Begins a batch.
    fn begin(&mut self) -> impl Future<Output = Result<(), ProjectionError>> + Send;

    /// Applies a stored event, which has been verified.
    fn apply(
        &mut self,
        event: &eventsourcingdb::Event,
    ) -> impl Future<Output = Result<(), ProjectionError>> + Send;

    /// Ends a batch, whose last event has the given ID.
    fn commit(
        &mut self,
        last_event_id: &str,
    ) -> impl Future<Output = Result<(), ProjectionError>> + Send;

    /// Undoes a batch that has begun.
    fn rollback(&mut self) -> impl Future<Output = Result<(), ProjectionError>> + Send;

    /// Returns how many events make a batch while catching up, at least one.
    fn batch_size(&self) -> usize;
}

/// Hands a stored event to a projection, as the envelopes it gets.
async fn project<E: Projected, F: Future<Output = Result<(), ProjectionError>>>(
    event: &eventsourcingdb::Event,
    upcasters: Option<&crate::Upcasters>,
    mut apply: impl FnMut(crate::Envelope<E>) -> F,
) -> Result<(), ProjectionError> {
    for envelope in E::project(event, upcasters)? {
        apply(envelope).await?;
    }

    Ok(())
}

/// Keeps no checkpoint of its own, so every start reads from the first event.
/// Within a run, it remembers the last event it applied, so that catching up
/// again after a lost stream does not apply any event twice.
pub(crate) struct RebuildWriter<R> {
    projection: R,
    last_applied: Option<String>,
}

impl<R> RebuildWriter<R> {
    pub(crate) fn new(projection: R) -> Self {
        Self {
            projection,
            last_applied: None,
        }
    }
}

impl<R> Writer for RebuildWriter<R>
where
    R: Deref + Send + Sync,
    R::Target: Projection,
{
    fn checkpoint(
        &mut self,
    ) -> impl Future<Output = Result<Option<String>, ProjectionError>> + Send {
        future::ready(Ok(self.last_applied.clone()))
    }

    fn begin(&mut self) -> impl Future<Output = Result<(), ProjectionError>> + Send {
        future::ready(Ok(()))
    }

    async fn apply(&mut self, event: &eventsourcingdb::Event) -> Result<(), ProjectionError> {
        let projection = &*self.projection;
        project(
            event,
            <R::Target as Projection>::upcasters(),
            |envelope| async move {
                projection
                    .apply(envelope)
                    .await
                    .map_err(ProjectionError::of)
            },
        )
        .await?;
        projection.seen(event.id());
        self.last_applied = Some(event.id().to_owned());

        Ok(())
    }

    fn commit(&mut self, _: &str) -> impl Future<Output = Result<(), ProjectionError>> + Send {
        future::ready(Ok(()))
    }

    // There is nothing to undo, since nothing was begun.
    fn rollback(&mut self) -> impl Future<Output = Result<(), ProjectionError>> + Send {
        future::ready(Ok(()))
    }

    fn batch_size(&self) -> usize {
        1
    }
}

/// Saves the checkpoint after the data, not with it. Within a run, it
/// remembers the last event it applied, as [`RebuildWriter`] does, so that
/// catching up again after a lost stream goes on from there, rather than from
/// a checkpoint that a batch cut short did not get to save.
pub(crate) struct ResumableWriter<R> {
    projection: R,
    last_applied: Option<String>,
}

impl<R> ResumableWriter<R> {
    pub(crate) fn new(projection: R) -> Self {
        Self {
            projection,
            last_applied: None,
        }
    }
}

impl<R> Writer for ResumableWriter<R>
where
    R: Deref + Send + Sync,
    R::Target: ResumableProjection,
{
    async fn checkpoint(&mut self) -> Result<Option<String>, ProjectionError> {
        if self.last_applied.is_some() {
            return Ok(self.last_applied.clone());
        }

        self.projection
            .checkpoint()
            .await
            .map_err(ProjectionError::of)
    }

    fn begin(&mut self) -> impl Future<Output = Result<(), ProjectionError>> + Send {
        future::ready(Ok(()))
    }

    async fn apply(&mut self, event: &eventsourcingdb::Event) -> Result<(), ProjectionError> {
        let projection = &*self.projection;
        project(
            event,
            <R::Target as Projection>::upcasters(),
            |envelope| async move {
                projection
                    .apply(envelope)
                    .await
                    .map_err(ProjectionError::of)
            },
        )
        .await?;
        projection.seen(event.id());
        self.last_applied = Some(event.id().to_owned());

        Ok(())
    }

    async fn commit(&mut self, last_event_id: &str) -> Result<(), ProjectionError> {
        self.projection
            .save_checkpoint(last_event_id)
            .await
            .map_err(ProjectionError::of)
    }

    // There is nothing to undo: the data is written already, and only the
    // checkpoint is missing. That is what makes this kind at-least-once
    // across a crash.
    fn rollback(&mut self) -> impl Future<Output = Result<(), ProjectionError>> + Send {
        future::ready(Ok(()))
    }

    fn batch_size(&self) -> usize {
        self.projection.catch_up_batch_size().max(1)
    }
}

/// The events of the transactional projection a pointer points to.
type TransactionalEvent<R> = <<R as Deref>::Target as TransactionalProjection>::Event;

/// Keeps data and checkpoint in step, within the transactions of the
/// projection.
pub(crate) struct TransactionalWriter<R>
where
    R: Deref,
    R::Target: TransactionalProjection,
{
    projection: R,
    transaction: Option<<R::Target as TransactionalProjection>::Transaction>,
}

impl<R> TransactionalWriter<R>
where
    R: Deref,
    R::Target: TransactionalProjection,
{
    pub(crate) fn new(projection: R) -> Self {
        Self {
            projection,
            transaction: None,
        }
    }
}

impl<R> Writer for TransactionalWriter<R>
where
    R: Deref + Send + Sync,
    R::Target: TransactionalProjection,
{
    async fn checkpoint(&mut self) -> Result<Option<String>, ProjectionError> {
        self.projection
            .checkpoint()
            .await
            .map_err(ProjectionError::of)
    }

    async fn begin(&mut self) -> Result<(), ProjectionError> {
        self.transaction = Some(self.projection.begin().await.map_err(ProjectionError::of)?);

        Ok(())
    }

    // A panic is turned into an error right where it happens, so that driving
    // rolls the transaction back, as for an error, instead of leaving it open.
    async fn apply(&mut self, event: &eventsourcingdb::Event) -> Result<(), ProjectionError> {
        let transaction = self
            .transaction
            .as_mut()
            .expect("a transaction has begun before an event is applied");

        for envelope in TransactionalEvent::<R>::project(
            event,
            <R::Target as TransactionalProjection>::upcasters(),
        )? {
            AssertUnwindSafe(transaction.apply(envelope))
                .catch_unwind()
                .await
                .map_err(|payload| ProjectionError::panicked(&*payload))?
                .map_err(ProjectionError::of)?;
        }

        Ok(())
    }

    async fn commit(&mut self, last_event_id: &str) -> Result<(), ProjectionError> {
        let transaction = self
            .transaction
            .take()
            .expect("a transaction has begun before it is committed");

        transaction
            .commit(last_event_id)
            .await
            .map_err(ProjectionError::of)
    }

    // A panic is turned into an error as well, so that driving reports it
    // alongside the failure that made it roll back, never instead of it.
    async fn rollback(&mut self) -> Result<(), ProjectionError> {
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };

        AssertUnwindSafe(transaction.rollback())
            .catch_unwind()
            .await
            .map_err(|payload| ProjectionError::panicked(&*payload))?
            .map_err(ProjectionError::of)
    }

    fn batch_size(&self) -> usize {
        self.projection.catch_up_batch_size().max(1)
    }
}

/// Turns a failure of reading into one of a projection.
fn read_failed(error: ClientError) -> ProjectionError {
    match read_failure(error, "reading events") {
        Sorted::Transient(failure) => ProjectionError::Transient(failure),
        Sorted::Permanent(failure) => ProjectionError::Permanent(failure),
    }
}

/// Rolls back a batch after the given failure, and reports a failing rollback
/// alongside the failure, never instead of it.
async fn roll_back<W: Writer>(writer: &mut W, cause: ProjectionError) -> ProjectionError {
    match writer.rollback().await {
        Ok(()) => cause,
        Err(rollback) => ProjectionError::RolledBack {
            cause: Box::new(cause),
            rollback: Box::new(rollback),
        },
    }
}

/// Applies the events in batches of the given size, and returns the ID of the
/// last one it applied, together with how it ended. Every event is verified
/// before it is applied, and one that fails ends the batch like an event the
/// projection refuses. Every committed batch is reported with the ID of its
/// last event.
///
/// Once the token is cancelled, it stops waiting for the next event, rolls an
/// open batch back, and returns without a failure, so the caller tells a
/// stream that was cut short from one that ended by the token. An event that
/// is being applied when the token is cancelled is applied to the end.
pub(crate) async fn drive<W: Writer>(
    writer: &mut W,
    events: impl Stream<Item = Result<eventsourcingdb::Event, ClientError>>,
    verify: impl Fn(&eventsourcingdb::Event) -> Result<(), Permanent>,
    batch_size: usize,
    token: &CancellationToken,
    mut committed: impl FnMut(&str),
) -> (Option<String>, Result<(), ProjectionError>) {
    let mut events = std::pin::pin!(events);
    let mut last_event_id = None;
    let mut in_batch = 0;
    let mut is_open = false;

    loop {
        let next = tokio::select! {
            biased;
            () = token.cancelled() => None,
            next = events.next() => Some(next),
        };
        let Some(next) = next else {
            if is_open && let Err(failure) = writer.rollback().await {
                return (last_event_id, Err(failure));
            }
            return (last_event_id, Ok(()));
        };
        let Some(event) = next else {
            break;
        };

        let checked = event
            .map_err(read_failed)
            .and_then(|event| verify(&event).map(|()| event).map_err(Into::into));
        let event = match checked {
            Ok(event) => event,
            Err(failure) if is_open => {
                return (last_event_id, Err(roll_back(writer, failure).await));
            }
            Err(failure) => return (last_event_id, Err(failure)),
        };

        if !is_open {
            if let Err(failure) = writer.begin().await {
                return (last_event_id, Err(failure));
            }
            is_open = true;
        }

        if let Err(failure) = writer.apply(&event).await {
            return (last_event_id, Err(roll_back(writer, failure).await));
        }
        last_event_id = Some(event.id().to_owned());
        in_batch += 1;
        if in_batch < batch_size {
            continue;
        }

        let id = event.id();
        if let Err(failure) = writer.commit(id).await {
            return (last_event_id, Err(failure));
        }
        committed(id);
        is_open = false;
        in_batch = 0;
    }

    if is_open && let Some(id) = &last_event_id {
        if let Err(failure) = writer.commit(id).await {
            return (last_event_id, Err(failure));
        }
        committed(id);
    }

    (last_event_id, Ok(()))
}

/// Turns a checkpoint into a lower bound that leaves out the event it names,
/// since that one has been applied already.
pub(crate) fn bound_after(checkpoint: Option<&str>) -> Option<Bound<'_>> {
    checkpoint.map(|id| Bound {
        bound_type: BoundType::Exclusive,
        id,
    })
}

/// A function that learns of every attempt of a projection to read again.
pub(crate) type Observer = Arc<dyn Fn(&Reconnect) + Send + Sync>;

/// How the projections of a store read again after reading failed (see
/// [`StoreBuilder::reconnect_delays`](crate::StoreBuilder::reconnect_delays)).
#[derive(Clone)]
pub(crate) struct Reconnecting {
    pub(crate) initial_delay: Duration,
    pub(crate) max_delay: Duration,
    pub(crate) observer: Option<Observer>,
}

impl Default for Reconnecting {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            observer: None,
        }
    }
}

impl fmt::Debug for Reconnecting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reconnecting")
            .field("initial_delay", &self.initial_delay)
            .field("max_delay", &self.max_delay)
            .field("has_observer", &self.observer.is_some())
            .finish()
    }
}

/// What a projection needs of a store to read, which a run takes onto a task
/// of its own.
#[derive(Clone)]
pub(crate) struct Reader {
    client: Arc<Client>,
    verification: Verification,
}

impl Reader {
    /// Reads the events stored so far from the checkpoint of the writer on,
    /// and applies them, in batches of the given size. It returns where it
    /// stopped, which is the checkpoint if there was nothing to apply.
    pub(crate) async fn catch_up<W: Writer>(
        &self,
        subjects: &Subjects,
        writer: &mut W,
        batch_size: usize,
        token: &CancellationToken,
        committed: impl FnMut(&str),
    ) -> (Option<String>, Result<(), ProjectionError>) {
        let checkpoint = match writer.checkpoint().await {
            Ok(checkpoint) => checkpoint,
            Err(failure) => return (None, Err(failure)),
        };

        let options = ReadEventsOptions {
            lower_bound: bound_after(checkpoint.as_deref()),
            recursive: subjects.is_tree(),
            ..ReadEventsOptions::default()
        };
        // Waiting for the database to answer stops as soon as the token is
        // cancelled, as waiting for the next event does.
        let requested = tokio::select! {
            biased;
            () = token.cancelled() => None,
            requested = self.client.read_events(subjects.subject(), Some(options)) => Some(requested),
        };
        let events = match requested {
            None => return (checkpoint.clone(), Ok(())),
            Some(Ok(events)) => events,
            Some(Err(error)) => return (checkpoint.clone(), Err(read_failed(error))),
        };

        let (last_event_id, result) = drive(
            writer,
            events,
            |event| verify(&self.verification, event),
            batch_size,
            token,
            committed,
        )
        .await;

        (last_event_id.or_else(|| checkpoint.clone()), result)
    }

    /// Catches up from the checkpoint, and then follows the events as they are
    /// written, until reading fails, which includes a stream that ends, since
    /// the database never ends one on its own, or until the token is
    /// cancelled, which ends it without a failure. It returns how long it
    /// followed the events, which is zero if it did not catch up.
    ///
    /// Once the projection follows the events, it commits every event on its
    /// own, since a batch would only be committed once the next events arrive,
    /// which may take long.
    async fn follow<W: Writer>(
        &self,
        subjects: &Subjects,
        writer: &mut W,
        batch_size: usize,
        token: &CancellationToken,
        run: &ProjectionRun,
    ) -> (Duration, Result<(), ProjectionError>) {
        let (last_event_id, caught_up) = self
            .catch_up(subjects, writer, batch_size, token, |id| run.committed(id))
            .await;
        if let Err(failure) = caught_up {
            return (Duration::ZERO, Err(failure));
        }
        // A catch-up that the token cut short must not look complete.
        if token.is_cancelled() {
            return (Duration::ZERO, Ok(()));
        }

        let caught_up_at = Instant::now();
        run.caught_up_now();

        let options = ObserveEventsOptions {
            lower_bound: bound_after(last_event_id.as_deref()),
            recursive: subjects.is_tree(),
            ..ObserveEventsOptions::default()
        };
        let requested = tokio::select! {
            biased;
            () = token.cancelled() => None,
            requested = self.client.observe_events(subjects.subject(), Some(options)) => Some(requested),
        };
        let events = match requested {
            None => return (caught_up_at.elapsed(), Ok(())),
            Some(Ok(events)) => events,
            Some(Err(error)) => return (caught_up_at.elapsed(), Err(read_failed(error))),
        };
        let (_, followed) = drive(
            writer,
            events,
            |event| verify(&self.verification, event),
            1,
            token,
            |id| run.committed(id),
        )
        .await;

        let ended = match followed {
            Err(failure) => Err(failure),
            Ok(()) if token.is_cancelled() => Ok(()),
            Ok(()) => Err(ProjectionError::Transient(Transient::Other(Failure::new(
                "reading events: the stream of events ended",
                None,
            )))),
        };

        (caught_up_at.elapsed(), ended)
    }

    /// Catches up and then follows the events, until the token is cancelled or
    /// a failure that trying again will not fix, and returns that failure. A
    /// failure that may pass, and is no panic, is tried again, from the
    /// checkpoint on, after a delay that grows with every attempt in a row.
    async fn run<W: Writer>(
        &self,
        subjects: &Subjects,
        writer: &mut W,
        reconnecting: &Reconnecting,
        token: &CancellationToken,
        run: &ProjectionRun,
    ) -> Option<ProjectionError> {
        let batch_size = writer.batch_size();
        let mut delay = reconnecting.initial_delay;
        let mut attempt = 0;

        loop {
            let checkpoint_before = writer.checkpoint().await.ok().flatten();
            let (live_for, followed) = self.follow(subjects, writer, batch_size, token, run).await;
            if token.is_cancelled() {
                return None;
            }

            // The run only ends without a failure through its token.
            let failure = followed.err()?;
            // A panic ends the run even together with a failure that may
            // pass, since trying again would only panic again.
            if failure.has_panicked() || failure.category() != Some(Category::Transient) {
                return Some(failure);
            }
            let failure = Arc::new(failure);
            run.disrupted(Arc::clone(&failure));

            // A run that got somewhere was healthy until it ended, and so was
            // one that followed the events for longer than the initial delay,
            // even if no event arrived, so the next failure is tried again
            // quickly again. One that fails before it catches up, or soon
            // after, is given ever more time.
            let checkpoint_after = writer.checkpoint().await.ok().flatten();
            if checkpoint_after != checkpoint_before || live_for > reconnecting.initial_delay {
                delay = reconnecting.initial_delay;
                attempt = 0;
            }
            attempt += 1;

            if let Some(observe) = &reconnecting.observer {
                let reconnect = Reconnect {
                    projection: run.name().map(ToOwned::to_owned),
                    subject: subjects.subject().to_owned(),
                    error: Arc::clone(&failure),
                    delay,
                    attempt,
                };
                // A panic of the observer ends the run, as one of the projection
                // does, since the run catches every panic on the way.
                observe(&reconnect);
            }

            tokio::select! {
                () = token.cancelled() => return None,
                () = tokio::time::sleep(delay) => {}
            }
            delay = (delay * 2).min(reconnecting.max_delay);
        }
    }
}

impl Store {
    /// Returns what a projection needs of the store to read.
    pub(crate) fn reader(&self) -> Reader {
        Reader {
            client: Arc::clone(&self.client),
            verification: self.verification.clone(),
        }
    }

    /// Catches up once, and turns a panic on the way, such as one in the
    /// projection, into the error a run ends with.
    async fn catch_up_once<W: Writer>(
        &self,
        subjects: &Subjects,
        mut writer: W,
    ) -> Result<(), ProjectionError> {
        let batch_size = writer.batch_size();
        let reader = self.reader();
        let token = CancellationToken::new();

        AssertUnwindSafe(reader.catch_up(subjects, &mut writer, batch_size, &token, |_| {}))
            .catch_unwind()
            .await
            .map_or_else(
                |payload| Err(ProjectionError::panicked(&*payload)),
                |(_, result)| result,
            )
    }

    /// Starts a run on a task of its own, which drives the writer the given
    /// function makes, and turns a panic on the way into the failure the run
    /// ends with, so that it ends the run rather than the task.
    fn start<W: Writer + 'static>(
        &self,
        subjects: Subjects,
        mut writer: W,
        token: CancellationToken,
        options: ProjectionOptions,
    ) -> ProjectionRun {
        let run = ProjectionRun::new(options);
        let reader = self.reader();
        let reconnecting = self.reconnecting.clone();

        let progress = run.clone();
        drop(tokio::spawn(async move {
            let failure = AssertUnwindSafe(reader.run(
                &subjects,
                &mut writer,
                &reconnecting,
                &token,
                &progress,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|payload| Some(ProjectionError::panicked(&*payload)));

            progress.stop(failure);
        }));

        run
    }

    /// Applies the events of the subjects that are stored so far to a
    /// projection, from the first one on, and returns. Use it to build a read
    /// model once, for a batch job or in a test, instead of following the
    /// events as they are written. Dropping the future stops it, so a read
    /// model that is built only in part never looks complete.
    ///
    /// # Errors
    ///
    /// Fails with the first failure, of reading, of verifying an event, or of
    /// the projection, including a panic of the projection, which is
    /// [`ProjectionError::Panicked`].
    pub async fn catch_up_projection<P: Projection>(
        &self,
        subjects: &Subjects,
        projection: &P,
    ) -> Result<(), ProjectionError> {
        self.catch_up_once(subjects, RebuildWriter::new(projection))
            .await
    }

    /// Applies the events of the subjects that are stored so far to a
    /// resumable projection, from its checkpoint on, saving the checkpoint
    /// after every batch, and returns. See
    /// [`catch_up_projection`](Store::catch_up_projection).
    ///
    /// # Errors
    ///
    /// Fails as [`catch_up_projection`](Store::catch_up_projection) does, and
    /// if the checkpoint can not be read or saved.
    pub async fn catch_up_resumable_projection<P: ResumableProjection>(
        &self,
        subjects: &Subjects,
        projection: &P,
    ) -> Result<(), ProjectionError> {
        self.catch_up_once(subjects, ResumableWriter::new(projection))
            .await
    }

    /// Applies the events of the subjects that are stored so far to a
    /// transactional projection, from its checkpoint on, one transaction per
    /// batch, and returns. See
    /// [`catch_up_projection`](Store::catch_up_projection).
    ///
    /// # Errors
    ///
    /// Fails as [`catch_up_projection`](Store::catch_up_projection) does, and
    /// if the checkpoint can not be read, or a transaction can not be begun or
    /// committed. A transaction that fails is rolled back, and a failure of
    /// the rollback is reported alongside the failure that caused it.
    pub async fn catch_up_transactional_projection<P: TransactionalProjection>(
        &self,
        subjects: &Subjects,
        projection: &P,
    ) -> Result<(), ProjectionError> {
        self.catch_up_once(subjects, TransactionalWriter::new(projection))
            .await
    }

    /// Starts a projection on a task of its own, which applies the events of
    /// the subjects stored so far, from the first one on, and then follows
    /// them as they are written, until the token is cancelled, or until a
    /// failure that trying again will not fix. It returns the run, which tells
    /// how far the projection has got (see [`ProjectionRun`]).
    ///
    /// Reading that fails in a way that may pass, which includes a stream of
    /// events that ends, and a failure of the projection of the category
    /// [`Category::Transient`], is tried again from the last event applied on,
    /// after a delay (see
    /// [`StoreBuilder::reconnect_delays`](crate::StoreBuilder::reconnect_delays)).
    /// Any other failure ends the run, and so does a panic of the projection,
    /// as [`ProjectionError::Panicked`], rather than the task. Cancelling the
    /// token stops waiting for the next event at once, while an event that is
    /// being applied is applied to the end, and the run then stops without a
    /// failure.
    ///
    /// It needs a Tokio runtime with the time driver, as the client SDK does.
    ///
    /// # Panics
    ///
    /// A name of the options that is empty is a programming error, so this
    /// panics.
    pub fn start_projection<P: Projection>(
        &self,
        subjects: Subjects,
        projection: Arc<P>,
        token: CancellationToken,
        options: ProjectionOptions,
    ) -> ProjectionRun {
        self.start(subjects, RebuildWriter::new(projection), token, options)
    }

    /// Starts a resumable projection as
    /// [`start_projection`](Store::start_projection) does, from its checkpoint
    /// on. While it catches up, it saves the checkpoint after every batch, and
    /// once it follows the events, after every event.
    ///
    /// # Panics
    ///
    /// As [`start_projection`](Store::start_projection) does.
    pub fn start_resumable_projection<P: ResumableProjection>(
        &self,
        subjects: Subjects,
        projection: Arc<P>,
        token: CancellationToken,
        options: ProjectionOptions,
    ) -> ProjectionRun {
        self.start(subjects, ResumableWriter::new(projection), token, options)
    }

    /// Starts a transactional projection as
    /// [`start_projection`](Store::start_projection) does, from its checkpoint
    /// on, one transaction per batch while it catches up, and one per event
    /// once it follows the events.
    ///
    /// # Panics
    ///
    /// As [`start_projection`](Store::start_projection) does.
    pub fn start_transactional_projection<P: TransactionalProjection>(
        &self,
        subjects: Subjects,
        projection: Arc<P>,
        token: CancellationToken,
        options: ProjectionOptions,
    ) -> ProjectionRun {
        self.start(
            subjects,
            TransactionalWriter::new(projection),
            token,
            options,
        )
    }
}
