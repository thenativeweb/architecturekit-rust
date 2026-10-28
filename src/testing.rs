//! Given-when-then for deciders, driving projections with stored events, and
//! a database in a container for the tests that need a real one. The module needs the feature `testing`, and
//! [`Database`] the feature `testcontainers`, which turns on `testing` as well,
//! so that a test that only checks a decider builds without Docker.
//!
//! A decider decides asynchronously, so a test waits for its decision:
//!
//! ```
//! # use architecturekit::{Command, Event, Events, Precondition, State};
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Serialize, Deserialize, Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String }
//! # #[derive(Serialize, Deserialize, Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! # #[derive(Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_borrowed: bool }
//! # impl State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => self,
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true },
//! #         }
//! #     }
//! # }
//! # #[derive(Clone)]
//! # struct BorrowBook { book_id: String, reader: String }
//! # impl Command for BorrowBook {
//! #     fn subject(&self) -> String { format!("/books/{}", self.book_id) }
//! #     fn preconditions(&self) -> Vec<Precondition> { vec![Precondition::on_state_read()] }
//! # }
//! # #[derive(Debug, PartialEq)]
//! # enum BorrowError { AlreadyBorrowed }
//! use architecturekit::{Decider, testing::given};
//!
//! let borrow_book = Decider::new(|cmd: BorrowBook, book: Book| async move {
//!     if book.is_borrowed {
//!         return Err(BorrowError::AlreadyBorrowed);
//!     }
//!
//!     Ok(vec![BookEvent::Borrowed(BookBorrowed { reader: cmd.reader })])
//! });
//! # let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! # runtime.block_on(async {
//!
//! // In a #[tokio::test]:
//! given(&borrow_book, [BookEvent::Acquired(BookAcquired { title: "2001".to_owned() })])
//!     .when(BorrowBook { book_id: "42".to_owned(), reader: "Jane".to_owned() })
//!     .await
//!     .then_events([BookEvent::Borrowed(BookBorrowed { reader: "Jane".to_owned() })])
//!     .then_preconditions([Precondition::on_state_read()]);
//! # });
//! ```
//!
//! An assertion that does not hold panics with a message that says what was
//! expected and what was decided, as `assert_eq!` does.
//!
//! A projection is driven with events in the shape the database stores them,
//! without one (see [`project`] and [`StoredEvent`]), and what it built is
//! read from its views, such as with `assert_eq!(view.all(), […])`.

#[cfg(feature = "testcontainers")]
mod database;
mod fixture;
mod projection;

#[cfg(feature = "testcontainers")]
pub use database::Database;
pub use fixture::{Decision, Fixture, given, given_stored};
pub use projection::{
    StoredEvent, project, project_transactional, stored_events, stored_events_at,
};
