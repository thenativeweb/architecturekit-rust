use std::{error::Error, fmt, future::Future, marker::PhantomData};

use super::{Envelope, Projected, Projection};
use crate::Categorized;

#[cfg(test)]
mod tests;

/// Turns a function into a [`Projection`], for a projection that needs no type
/// of its own, such as one that logs or counts. The function gets every event
/// as an [`Envelope`], typed by an enum with `#[derive(Events)]`, or as it is
/// stored, and returns a future of the result:
///
/// ```
/// use std::convert::Infallible;
///
/// use architecturekit::{Envelope, projection_fn};
///
/// let log = projection_fn(|event: Envelope<architecturekit::eventsourcingdb::Event>| async move {
///     println!("{} {}", event.subject, event.event_type);
///     Ok::<(), Infallible>(())
/// });
/// ```
///
/// A function brings along neither upcasters nor a record of what it has seen
/// (see [`Projection::upcasters`] and [`Projection::seen`]). So a view that it
/// fills never reaches a revision, and waiting for one, with the header
/// `Wait-For-Revision` or with [`wait_for_written`](crate::wait_for_written),
/// runs into the timeout. A projection that needs upcasters or a revision has
/// a type of its own.
pub fn projection_fn<E, F, Fut, Err>(apply: F) -> ProjectionFn<E, F>
where
    E: Projected,
    F: Fn(Envelope<E>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Err>> + Send,
    Err: Error + Categorized + Send + Sync + 'static,
{
    ProjectionFn {
        events: PhantomData,
        apply,
    }
}

/// A function that [`projection_fn`] has turned into a [`Projection`].
pub struct ProjectionFn<E, F> {
    events: PhantomData<fn() -> E>,
    apply: F,
}

impl<E, F> fmt::Debug for ProjectionFn<E, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionFn")
            .finish_non_exhaustive()
    }
}

impl<E, F, Fut, Err> Projection for ProjectionFn<E, F>
where
    E: Projected,
    F: Fn(Envelope<E>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Err>> + Send,
    Err: Error + Categorized + Send + Sync + 'static,
{
    type Event = E;
    type Error = Err;

    fn apply(&self, event: Envelope<E>) -> impl Future<Output = Result<(), Err>> + Send {
        (self.apply)(event)
    }
}
