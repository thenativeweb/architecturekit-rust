//! # architecturekit
//!
//! Building blocks for DDD-based applications with CQRS and event sourcing in Rust, on top of [EventSourcingDB](https://www.eventfoundation.io/products/eventsourcingdb) – a purpose-built database for event sourcing.
//!
//! ArchitectureKit covers both sides of an event-sourced application: commands, events, and the state to decide on for writing, and projections, views, and queries for reading. An optional feature answers commands and queries over HTTP, with handlers for axum.
//!
//! The complete documentation of ArchitectureKit, including preconditions, views, projections, queries, handling errors, testing, and HTTP, is available at **[https://www.eventfoundation.io/docs/architecturekit/rust](https://www.eventfoundation.io/docs/architecturekit/rust)**. The API is documented on [docs.rs](https://docs.rs/architecturekit).
//!
//! ## Getting Started
//!
//! ArchitectureKit needs Rust 1.88 or later, and a running EventSourcingDB. To start one on your machine, see the [quickstart of EventSourcingDB](https://www.eventfoundation.io/docs/eventsourcingdb/quickstart).
//!
//! Add the crate, together with serde for the data of events, and Tokio to run it:
//!
//! ```shell
//! cargo add architecturekit
//! cargo add serde --features derive
//! cargo add tokio --features macros,rt-multi-thread
//! ```
//!
//! ArchitectureKit brings the client SDK for EventSourcingDB along as `architecturekit::eventsourcingdb`, so that their versions always fit, and a new major version of the client SDK comes with a new major version of ArchitectureKit.
//!
//! Create an EventSourcingDB client, and a store with the client and the source to use for all events you write:
//!
//! ```rust
//! use architecturekit::{Store, eventsourcingdb::Client};
//!
//! let client = Client::new("http://localhost:3000".parse()?, "secret");
//! let store = Store::builder(client, "https://library.eventsourcingdb.io").build();
//! # let _ = store;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Define a command, which describes what someone wants to do:
//!
//! ```rust
//! use architecturekit::{Command, Precondition, Subject};
//!
//! #[derive(Clone)]
//! struct AcquireBook {
//!     book_id: String,
//!     title: String,
//!     author: String,
//!     isbn: String,
//! }
//!
//! impl Command for AcquireBook {
//!     fn subject(&self) -> Subject {
//!         Subject::new(&format!("/books/{}", self.book_id))
//!     }
//!
//!     fn preconditions(&self) -> Vec<Precondition> {
//!         vec![Precondition::on_state_read()]
//!     }
//! }
//! ```
//!
//! Define an event, which describes what has happened:
//!
//! ```rust
//! use architecturekit::Event;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Event)]
//! #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! struct BookAcquired {
//!     title: String,
//!     author: String,
//!     isbn: String,
//! }
//! ```
//!
//! Define the state that a command is decided on, and how events change it:
//!
//! ```rust
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String, isbn: String }
//! use architecturekit::{Events, State};
//!
//! #[derive(Events)]
//! enum BookEvent {
//!     Acquired(BookAcquired),
//! }
//!
//! #[derive(Clone, Default)]
//! struct Book {
//!     is_acquired: bool,
//! }
//!
//! impl State for Book {
//!     type Event = BookEvent;
//!
//!     fn evolve(self, event: &BookEvent) -> Self {
//!         match event {
//!             BookEvent::Acquired(_) => Book { is_acquired: true },
//!         }
//!     }
//! }
//! ```
//!
//! Connect the state with the decision made on it:
//!
//! ```rust
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String, isbn: String }
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true },
//! #         }
//! #     }
//! # }
//! # #[derive(Clone)]
//! # struct AcquireBook { book_id: String, title: String, author: String, isbn: String }
//! # impl architecturekit::Command for AcquireBook {
//! #     fn subject(&self) -> architecturekit::Subject {
//! #         architecturekit::Subject::new(&format!("/books/{}", self.book_id))
//! #     }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! use architecturekit::{Decider, DomainError};
//!
//! fn acquire_book() -> Decider<AcquireBook, Book, DomainError> {
//!     Decider::new(|command: AcquireBook, book: Book| async move {
//!         if book.is_acquired {
//!             return Err(DomainError::new(format!(
//!                 "book has already been acquired: {}",
//!                 command.book_id
//!             )));
//!         }
//!
//!         Ok(vec![BookEvent::Acquired(BookAcquired {
//!             title: command.title,
//!             author: command.author,
//!             isbn: command.isbn,
//!         })])
//!     })
//! }
//! ```
//!
//! Execute the command:
//!
//! ```rust,no_run
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String, isbn: String }
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true },
//! #         }
//! #     }
//! # }
//! # #[derive(Clone)]
//! # struct AcquireBook { book_id: String, title: String, author: String, isbn: String }
//! # impl architecturekit::Command for AcquireBook {
//! #     fn subject(&self) -> architecturekit::Subject {
//! #         architecturekit::Subject::new(&format!("/books/{}", self.book_id))
//! #     }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! # fn acquire_book() -> architecturekit::Decider<AcquireBook, Book, architecturekit::DomainError> {
//! #     architecturekit::Decider::new(|command: AcquireBook, _: Book| async move {
//! #         Ok(vec![BookEvent::Acquired(BookAcquired {
//! #             title: command.title,
//! #             author: command.author,
//! #             isbn: command.isbn,
//! #         })])
//! #     })
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = architecturekit::eventsourcingdb::Client::new("http://localhost:3000".parse()?, "secret");
//! # let store = architecturekit::Store::builder(client, "https://library.eventsourcingdb.io").build();
//! let written_events = store
//!     .execute(
//!         &acquire_book(),
//!         AcquireBook {
//!             book_id: "42".to_owned(),
//!             title: "2001 – A Space Odyssey".to_owned(),
//!             author: "Arthur C. Clarke".to_owned(),
//!             isbn: "978-0756906788".to_owned(),
//!         },
//!     )
//!     .await?;
//! # let _ = written_events;
//! # Ok(())
//! # }
//! ```
//!
//! For everything else, see the **[complete documentation](https://www.eventfoundation.io/docs/architecturekit/rust)**.
#![deny(
    ambiguous_negative_literals,
    clippy::pedantic,
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    unsafe_code,
    warnings
)]

// The derive macros name the crate as ::architecturekit, which this makes
// work within the crate's own tests as well.
#[cfg(test)]
extern crate self as architecturekit;

mod command;
mod decider;
mod error;
mod event;
#[cfg(feature = "http")]
pub mod httpapi;
mod projection;
pub mod query;
mod read;
#[cfg(doctest)]
mod refusals;
mod revision;
mod schema;
mod state;
mod store;
mod subject;
#[cfg(feature = "testing")]
pub mod testing;
mod upcast;
mod view;

pub use command::{Command, Precondition, PreconditionsError, check_preconditions};
pub use decider::Decider;
pub use error::{Categorized, Category, DomainError, Failure, Permanent, Transient};
pub use event::Event;
pub use projection::{
    CaughtUpError, Envelope, Phase, Projected, Projection, ProjectionError, ProjectionFn,
    ProjectionOptions, ProjectionRun, ProjectionStatus, Reconnect, ResumableProjection,
    Transaction, TransactionalProjection, decode, projection_fn, wait_caught_up,
};
pub use read::{
    EmptyRange, IfLatestEventIsMissing, LowerBound, Order, ReadOptions, ReadOptionsError, Subjects,
    UpperBound, check_read_options,
};
pub use revision::{
    NotARevision, NotCaughtUp, Revision, Revisioned, revision_of, wait_for_written,
};
pub use schema::Schema;
pub use state::{EventSchema, Events, State, replay, replay_stored, step, step_stored};
pub use store::{
    Error, EventOn, ExecuteError, LoadError, ReadError, RegisterSchemasError, Store, StoreBuilder,
    WriteError,
};
pub use subject::{NotASubject, Subject, SubjectScheme, SubjectValuesError};
pub use upcast::{UpcastEvent, Upcasters};
pub use view::{InMemoryIndex, InMemoryView, KeyedView, Outcome, View, ViewError};

/// Stops a run of a projection from outside (see [`Store::start_projection`]).
pub use tokio_util::sync::CancellationToken;

/// The client SDK for EventSourcingDB, whose types the kit takes and hands out,
/// such as the [`Client`](eventsourcingdb::Client) that [`Store::builder`]
/// takes. Use it through the kit, so that its version always fits the one of
/// the kit: a new major version of the client SDK comes with a new major
/// version of the kit.
pub use eventsourcingdb;

/// The crate of the key that [`StoreBuilder::signature_verification`] takes,
/// in the version the client SDK verifies signatures with.
pub use ed25519_dalek;

#[cfg(feature = "derive")]
pub use architecturekit_macros::{Event, Events, Schema};

#[doc(hidden)]
#[path = "private.rs"]
pub mod __private;
