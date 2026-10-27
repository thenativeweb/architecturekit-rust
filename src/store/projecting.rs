//! Drives projections, as ArchitectureKit for Go does: it catches up from the
//! checkpoint of a projection, in batches, and verifies every event before the
//! projection applies it.

use std::{
    future::{self, Future},
    panic::AssertUnwindSafe,
};

use eventsourcingdb::{
    error::ClientError,
    request_options::{Bound, BoundType, ReadEventsOptions},
};
use futures_util::{FutureExt, Stream, StreamExt};

use super::{
    Store,
    failure::{Sorted, read_failure},
};
use crate::{
    Permanent, Projected, Projection, ProjectionError, ResumableProjection, Subjects, Transaction,
    TransactionalProjection,
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
pub(crate) struct RebuildWriter<'p, P> {
    projection: &'p P,
    last_applied: Option<String>,
}

impl<'p, P> RebuildWriter<'p, P> {
    pub(crate) fn new(projection: &'p P) -> Self {
        Self {
            projection,
            last_applied: None,
        }
    }
}

impl<P: Projection> Writer for RebuildWriter<'_, P> {
    fn checkpoint(
        &mut self,
    ) -> impl Future<Output = Result<Option<String>, ProjectionError>> + Send {
        future::ready(Ok(self.last_applied.clone()))
    }

    fn begin(&mut self) -> impl Future<Output = Result<(), ProjectionError>> + Send {
        future::ready(Ok(()))
    }

    async fn apply(&mut self, event: &eventsourcingdb::Event) -> Result<(), ProjectionError> {
        let projection = self.projection;
        project(event, P::upcasters(), |envelope| async move {
            projection
                .apply(envelope)
                .await
                .map_err(ProjectionError::of)
        })
        .await?;
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
pub(crate) struct ResumableWriter<'p, P> {
    projection: &'p P,
    last_applied: Option<String>,
}

impl<'p, P> ResumableWriter<'p, P> {
    pub(crate) fn new(projection: &'p P) -> Self {
        Self {
            projection,
            last_applied: None,
        }
    }
}

impl<P: ResumableProjection> Writer for ResumableWriter<'_, P> {
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
        let projection = self.projection;
        project(event, P::upcasters(), |envelope| async move {
            projection
                .apply(envelope)
                .await
                .map_err(ProjectionError::of)
        })
        .await?;
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

/// Keeps data and checkpoint in step, within the transactions of the
/// projection.
pub(crate) struct TransactionalWriter<'p, P: TransactionalProjection> {
    projection: &'p P,
    transaction: Option<P::Transaction>,
}

impl<'p, P: TransactionalProjection> TransactionalWriter<'p, P> {
    pub(crate) fn new(projection: &'p P) -> Self {
        Self {
            projection,
            transaction: None,
        }
    }
}

impl<P: TransactionalProjection> Writer for TransactionalWriter<'_, P> {
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

        for envelope in P::Event::project(event, P::upcasters())? {
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
pub(crate) async fn drive<W: Writer>(
    writer: &mut W,
    events: impl Stream<Item = Result<eventsourcingdb::Event, ClientError>>,
    verify: impl Fn(&eventsourcingdb::Event) -> Result<(), Permanent>,
    batch_size: usize,
    mut committed: impl FnMut(&str),
) -> (Option<String>, Result<(), ProjectionError>) {
    let mut events = std::pin::pin!(events);
    let mut last_event_id = None;
    let mut in_batch = 0;
    let mut is_open = false;

    while let Some(event) = events.next().await {
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

impl Store {
    /// Reads the events stored so far from the checkpoint of the writer on,
    /// and applies them, in batches of the given size. It returns where it
    /// stopped, which is the checkpoint if there was nothing to apply.
    pub(crate) async fn catch_up<W: Writer>(
        &self,
        subjects: &Subjects,
        writer: &mut W,
        batch_size: usize,
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
        let events = match self
            .client
            .read_events(subjects.subject(), Some(options))
            .await
        {
            Ok(events) => events,
            Err(error) => return (checkpoint.clone(), Err(read_failed(error))),
        };

        let (last_event_id, result) = drive(
            writer,
            events,
            |event| self.verify(event),
            batch_size,
            committed,
        )
        .await;

        (last_event_id.or(checkpoint), result)
    }

    /// Catches up once, and turns a panic on the way, such as one in the
    /// projection, into the error a run ends with.
    async fn catch_up_once<W: Writer>(
        &self,
        subjects: &Subjects,
        mut writer: W,
    ) -> Result<(), ProjectionError> {
        let batch_size = writer.batch_size();

        AssertUnwindSafe(self.catch_up(subjects, &mut writer, batch_size, |_| {}))
            .catch_unwind()
            .await
            .map_or_else(
                |payload| Err(ProjectionError::panicked(&*payload)),
                |(_, result)| result,
            )
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
}
