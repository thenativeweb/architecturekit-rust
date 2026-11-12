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
/// (see [`Projection::upcasters`] and [`Projection::seen`]). A projection that
/// needs either has a type of its own.
pub fn projection_fn<E, F, Fut, Err>(apply: F) -> ProjectionFn<F, E>
where
    E: Projected,
    F: Fn(Envelope<E>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Err>> + Send,
    Err: Error + Categorized + Send + Sync + 'static,
{
    ProjectionFn {
        apply,
        events: PhantomData,
    }
}

/// A function that [`projection_fn`] has turned into a [`Projection`].
pub struct ProjectionFn<F, E> {
    apply: F,
    events: PhantomData<fn() -> E>,
}

impl<F, E> fmt::Debug for ProjectionFn<F, E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionFn")
            .finish_non_exhaustive()
    }
}

impl<F, E, Fut, Err> Projection for ProjectionFn<F, E>
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
