use std::{any, fmt, future::Future, pin::Pin};

use crate::State;

#[cfg(test)]
mod tests;

type Decision<S, D> = Result<Vec<<S as State>::Event>, D>;
type Decide<C, S, D> =
    dyn Fn(C, S) -> Pin<Box<dyn Future<Output = Decision<S, D>> + Send>> + Send + Sync;

/// A decision on a command, given the state of its subject: the events to
/// write, or an error if the command is refused.
///
/// Create one from a function that receives the command and the state, and
/// returns the events in a future, so that it can load what else it needs to
/// decide, such as the state of another subject:
///
/// ```
/// # use architecturekit::{Event, Events, State};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, Event)]
/// # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
/// # struct BookBorrowed { reader: String }
/// # #[derive(Events)]
/// # enum BookEvent { Borrowed(BookBorrowed) }
/// # #[derive(Clone, Default)]
/// # struct Book { is_borrowed: bool }
/// # impl State for Book {
/// #     type Event = BookEvent;
/// #     fn evolve(self, event: &BookEvent) -> Self { Book { is_borrowed: true } }
/// # }
/// # #[derive(Clone)]
/// # struct BorrowBook { book_id: String, reader: String }
/// use architecturekit::{Decider, DomainError};
///
/// let borrow_book = Decider::new(|cmd: BorrowBook, book: Book| async move {
///     if book.is_borrowed {
///         return Err(DomainError::new(format!("book {} is already borrowed", cmd.book_id)));
///     }
///
///     Ok(vec![BookEvent::Borrowed(BookBorrowed { reader: cmd.reader })])
/// });
/// ```
///
/// The types of the command, the state and the error come from the function,
/// so that none of them has to be named. A decider that never refuses names
/// its error type, as in `Decider::<_, _, DomainError>::new(…)`.
///
/// The function takes the command and the state rather than references to
/// them, so that its future can be sent across threads, which a web server
/// needs.
pub struct Decider<C, S: State, D> {
    decide: Box<Decide<C, S, D>>,
}

impl<C, S: State, D> Decider<C, S, D> {
    /// Creates a decider from a function that decides on a command, given the
    /// state of its subject.
    #[must_use]
    pub fn new<F, Fut>(decide: F) -> Self
    where
        F: Fn(C, S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Decision<S, D>> + Send + 'static,
    {
        Self {
            decide: Box::new(move |command, state| Box::pin(decide(command, state))),
        }
    }

    /// Decides on a command, given the state of its subject, with the function
    /// the decider was created with, and returns what that function returns.
    ///
    /// # Errors
    ///
    /// Fails with the error of the function, if it refuses the command.
    pub async fn decide(&self, command: C, state: S) -> Decision<S, D> {
        (self.decide)(command, state).await
    }
}

impl<C, S: State, D> fmt::Debug for Decider<C, S, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Decider")
            .field("command", &any::type_name::<C>())
            .field("state", &any::type_name::<S>())
            .field("error", &any::type_name::<D>())
            .finish_non_exhaustive()
    }
}
