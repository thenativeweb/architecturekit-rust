//! # architecturekit
//!
//! Building blocks for DDD-based applications with CQRS and event sourcing in Rust, on top of [EventSourcingDB](https://www.eventsourcingdb.io) – a purpose-built database for event sourcing.
//!
//! architecturekit takes care of what such an application needs besides its domain: events whose schemas are derived from their types, states that are replayed from events, commands that are decided on them with optimistic concurrency, projections that keep views up to date, answers to commands and queries over HTTP, and tests for deciders and projections. The API is documented on [docs.rs](https://docs.rs/architecturekit).
//!
//! ## Installation
//!
//! Add architecturekit, together with serde to write and read the data of events, and Tokio to run it all:
//!
//! ```shell
//! cargo add architecturekit
//! cargo add serde --features derive
//! cargo add tokio --features macros,rt-multi-thread
//! ```
//!
//! architecturekit brings the client SDK for EventSourcingDB along as `architecturekit::eventsourcingdb`, and `ed25519_dalek` for verifying signatures as `architecturekit::ed25519_dalek`, so that their versions always fit. A new major version of the client SDK comes with a new major version of architecturekit.
//!
//! architecturekit needs Rust 1.88 or later. Its features are:
//!
//! - `derive`, which is on by default, derives events, their enums, and their schemas.
//! - `http` answers commands and queries over HTTP, with the types of the `http` crate.
//! - `axum` adds extractors and handlers for axum 0.8, and turns on `http`.
//! - `testing` adds helpers to test deciders and projections.
//! - `testcontainers` adds an EventSourcingDB in a container of a test's own, and turns on `testing`.
//!
//! ## Defining Events
//!
//! An event describes what has happened. Derive `Event` for its data, together with serde's `Serialize` and `Deserialize`, and name its event type:
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
//! }
//!
//! #[derive(Serialize, Deserialize, Event)]
//! #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! struct BookBorrowed {
//!     reader: String,
//! }
//! ```
//!
//! The derive also derives the JSON schema of the event from the fields that serde writes. EventSourcingDB rejects events whose data does not match the registered schema, and a registered schema can not change, so the rules of the derive are fixed: the same fields always give the same schema.
//!
//! For a type within the data of an event, such as a nested struct, derive `Schema`. A type whose schema can not be derived, such as an enum, implements `Schema` by hand, and the derive refuses to compile otherwise. For the rules, see the documentation of `Schema` on [docs.rs](https://docs.rs/architecturekit/latest/architecturekit/trait.Schema.html).
//!
//! ## Replaying States
//!
//! A state holds what a command is decided on, such as whether a book has been borrowed. It is replayed from the events of its subject, which an enum lists, with one variant per event type, so that the compiler checks that the state handles every event it may see:
//!
//! ```rust
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! use architecturekit::{Events, State};
//!
//! #[derive(Events)]
//! enum BookEvent {
//!     Acquired(BookAcquired),
//!     Borrowed(BookBorrowed),
//! }
//!
//! #[derive(Clone, Default)]
//! struct Book {
//!     is_acquired: bool,
//!     is_borrowed: bool,
//! }
//!
//! impl State for Book {
//!     type Event = BookEvent;
//!
//!     fn evolve(self, event: &BookEvent) -> Self {
//!         match event {
//!             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//!             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//!         }
//!     }
//! }
//! ```
//!
//! A state starts from its `Default`. An event of a type that the enum does not list is left out, and upcasters turn events of older types into current ones (see `State::upcasters`).
//!
//! ## Deciding on Commands
//!
//! A command names the subject whose state it is decided on, and the preconditions that its events are written with. `Precondition::on_state_read` writes them only if no other event has been written to the subject since its state was read, which is optimistic concurrency. A `Subject` is a path that EventSourcingDB accepts, such as `/books/42`, and a `SubjectScheme` builds subjects from values, and reads values from subjects:
//!
//! ```rust
//! use std::sync::LazyLock;
//!
//! use architecturekit::{Command, Precondition, Subject, SubjectScheme};
//!
//! static BOOKS: LazyLock<SubjectScheme> = LazyLock::new(|| SubjectScheme::new("/books/{book_id}"));
//!
//! #[derive(Clone)]
//! struct BorrowBook {
//!     book: Subject,
//!     reader: String,
//! }
//!
//! impl Command for BorrowBook {
//!     fn subject(&self) -> Subject {
//!         self.book.clone()
//!     }
//!
//!     fn preconditions(&self) -> Vec<Precondition> {
//!         vec![Precondition::on_state_read()]
//!     }
//! }
//! ```
//!
//! A decider takes a command and the state, and either returns the events that the command causes, or refuses it:
//!
//! ```rust
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! use architecturekit::{Decider, DomainError};
//!
//! fn borrow_book() -> Decider<BorrowBook, Book, DomainError> {
//!     Decider::new(|command: BorrowBook, book: Book| async move {
//!         if !book.is_acquired {
//!             return Err(DomainError::new(format!("{} does not exist", command.book)));
//!         }
//!         if book.is_borrowed {
//!             return Err(DomainError::new(format!("{} is already borrowed", command.book)));
//!         }
//!
//!         Ok(vec![BookEvent::Borrowed(BookBorrowed { reader: command.reader })])
//!     })
//! }
//! ```
//!
//! A decider is asynchronous, so that it can load further states from the store. `DomainError` refuses a command with a message. An error type of your own works as well, and implements `Categorized` to say whether it refuses the command or reports a failure.
//!
//! ## Executing Commands
//!
//! The store connects to EventSourcingDB through the client SDK, and names the source of the events that it writes. Register the schemas of the events once at the start, and then execute commands:
//!
//! ```rust,no_run
//! # use architecturekit::Events;
//! #
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! #
//! # fn borrow_book() -> architecturekit::Decider<BorrowBook, Book, architecturekit::DomainError> {
//! #     architecturekit::Decider::new(|command: BorrowBook, book: Book| async move {
//! #         if !book.is_acquired {
//! #             return Err(architecturekit::DomainError::new(format!("{} does not exist", command.book)));
//! #         }
//! #         if book.is_borrowed {
//! #             return Err(architecturekit::DomainError::new(format!("{} is already borrowed", command.book)));
//! #         }
//! #         Ok(vec![BookEvent::Borrowed(BookBorrowed { reader: command.reader })])
//! #     })
//! # }
//! use architecturekit::{Store, eventsourcingdb::Client};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::new("http://localhost:3000".parse()?, "secret");
//!     let store = Store::builder(client, "https://library.eventsourcingdb.io").build();
//!     store.register_schemas(BookEvent::schemas()).await?;
//!
//!     let command = BorrowBook { book: BOOKS.build(&["42"]), reader: "Jane".to_owned() };
//!     let written = store.execute(&borrow_book(), command).await?;
//!     println!("borrowed with {} event(s)", written.len());
//!
//!     Ok(())
//! }
//! ```
//!
//! `execute` loads the state, decides, and writes the events with their preconditions. If another command has written to the subject in the meantime, it fails with a conflict, unless `StoreBuilder::conflict_retries` lets it decide again on the new state. Every error tells by its category what to do about it: a refusal of the decider is final, a transient failure, such as a conflict, may pass when trying again, and a permanent one will not. A write whose outcome is unknown has a variant of its own, since its events may have been stored.
//!
//! ## Projecting Events into Views
//!
//! A projection applies events to views, which queries read. An `InMemoryView` keeps its items in memory, together with the revision of the event that last changed each of them, so that an event that is applied twice changes nothing:
//!
//! ```rust
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! use architecturekit::{Envelope, InMemoryView, Projection, Revision, ViewError};
//! use serde::Serialize;
//!
//! #[derive(Clone, Default, Serialize)]
//! struct ListedBook {
//!     id: String,
//!     title: String,
//!     is_borrowed: bool,
//! }
//!
//! struct BookList {
//!     books: InMemoryView<String, ListedBook>,
//! }
//!
//! impl Projection for BookList {
//!     type Event = BookEvent;
//!     type Error = ViewError;
//!
//!     async fn apply(&self, event: Envelope<BookEvent>) -> Result<(), ViewError> {
//!         let Some(book_id) = BOOKS.value(&event.subject, "book_id") else {
//!             return Ok(());
//!         };
//!
//!         match &event.data {
//!             BookEvent::Acquired(acquired) => {
//!                 let book = ListedBook {
//!                     id: book_id.to_owned(),
//!                     title: acquired.title.clone(),
//!                     is_borrowed: false,
//!                 };
//!                 self.books.insert(&event, book)?;
//!             }
//!             BookEvent::Borrowed(_) => {
//!                 self.books.update(book_id, &event, |book| book.is_borrowed = true)?;
//!             }
//!         }
//!
//!         Ok(())
//!     }
//!
//!     fn seen(&self, event_id: Revision) {
//!         self.books.seen(event_id);
//!     }
//! }
//! ```
//!
//! `seen` tells the view about every event the projection has handled, including those it had nothing to do with, so that a query can wait for a revision. The store starts a run of the projection, which reads the events stored so far, and then follows the events as they are written:
//!
//! ```rust
//! # use architecturekit::InMemoryView;
//! #
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! #
//! # #[derive(Clone, Default, serde::Serialize)]
//! # struct ListedBook { id: String, title: String, is_borrowed: bool }
//! # struct BookList { books: architecturekit::InMemoryView<String, ListedBook> }
//! # impl architecturekit::Projection for BookList {
//! #     type Event = BookEvent;
//! #     type Error = architecturekit::ViewError;
//! #     async fn apply(
//! #         &self,
//! #         event: architecturekit::Envelope<BookEvent>,
//! #     ) -> Result<(), architecturekit::ViewError> {
//! #         let Some(book_id) = BOOKS.value(&event.subject, "book_id") else { return Ok(()); };
//! #         match &event.data {
//! #             BookEvent::Acquired(acquired) => {
//! #                 let book = ListedBook { id: book_id.to_owned(), title: acquired.title.clone(), is_borrowed: false };
//! #                 self.books.insert(&event, book)?;
//! #             }
//! #             BookEvent::Borrowed(_) => {
//! #                 self.books.update(book_id, &event, |book| book.is_borrowed = true)?;
//! #             }
//! #         }
//! #         Ok(())
//! #     }
//! #     fn seen(&self, event_id: architecturekit::Revision) { self.books.seen(event_id); }
//! # }
//! # async fn example(store: architecturekit::Store) -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//!
//! use architecturekit::{CancellationToken, ProjectionOptions, Subjects, wait_caught_up};
//!
//! let book_list = Arc::new(BookList {
//!     books: InMemoryView::new(|book: &ListedBook| book.id.clone()),
//! });
//! let token = CancellationToken::new();
//!
//! let run = store.start_projection(
//!     Subjects::tree(BOOKS.root()),
//!     Arc::clone(&book_list),
//!     token.clone(),
//!     ProjectionOptions::named("book list"),
//! );
//! wait_caught_up(&[&run]).await?;
//! # Ok(())
//! # }
//! ```
//!
//! If reading fails in a way that may pass, the run reconnects and goes on from the last event it applied, and a failure that will not pass ends it, as does cancelling the token. `ProjectionRun::status` tells where it stands. A view in a database of its own implements `ResumableProjection`, which goes on from a checkpoint after a restart, or `TransactionalProjection`, which applies the events and the checkpoint within one transaction.
//!
//! ## Serving Commands and Queries
//!
//! The feature `axum` adds what a web server built with axum needs to answer commands and queries:
//!
//! ```shell
//! cargo add architecturekit --features axum
//! cargo add axum
//! cargo add tower-http --features catch-panic
//! ```
//!
//! A command is an ordinary handler of axum:
//!
//! ```rust
//! # #[cfg(feature = "axum")]
//! # mod example {
//! # use architecturekit::{Decider, DomainError, Store};
//! #
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! #
//! # fn borrow_book() -> architecturekit::Decider<BorrowBook, Book, architecturekit::DomainError> {
//! #     architecturekit::Decider::new(|command: BorrowBook, book: Book| async move {
//! #         if !book.is_acquired {
//! #             return Err(architecturekit::DomainError::new(format!("{} does not exist", command.book)));
//! #         }
//! #         if book.is_borrowed {
//! #             return Err(architecturekit::DomainError::new(format!("{} is already borrowed", command.book)));
//! #         }
//! #         Ok(vec![BookEvent::Borrowed(BookBorrowed { reader: command.reader })])
//! #     })
//! # }
//! #
//! # #[derive(Clone, Default, serde::Serialize)]
//! # struct ListedBook { id: String, title: String, is_borrowed: bool }
//! # struct BookList { books: architecturekit::InMemoryView<String, ListedBook> }
//! # impl architecturekit::Projection for BookList {
//! #     type Event = BookEvent;
//! #     type Error = architecturekit::ViewError;
//! #     async fn apply(
//! #         &self,
//! #         event: architecturekit::Envelope<BookEvent>,
//! #     ) -> Result<(), architecturekit::ViewError> {
//! #         let Some(book_id) = BOOKS.value(&event.subject, "book_id") else { return Ok(()); };
//! #         match &event.data {
//! #             BookEvent::Acquired(acquired) => {
//! #                 let book = ListedBook { id: book_id.to_owned(), title: acquired.title.clone(), is_borrowed: false };
//! #                 self.books.insert(&event, book)?;
//! #             }
//! #             BookEvent::Borrowed(_) => {
//! #                 self.books.update(book_id, &event, |book| book.is_borrowed = true)?;
//! #             }
//! #         }
//! #         Ok(())
//! #     }
//! #     fn seen(&self, event_id: architecturekit::Revision) { self.books.seen(event_id); }
//! # }
//! use std::sync::Arc;
//!
//! use architecturekit::httpapi::{CommandAnswer, HttpError, Strict};
//! use axum::extract::{Path, State};
//! use serde::Deserialize;
//!
//! #[derive(Clone)]
//! struct App {
//!     store: Arc<Store>,
//!     borrow_book: Arc<Decider<BorrowBook, Book, DomainError>>,
//!     book_list: Arc<BookList>,
//! }
//!
//! #[derive(Deserialize)]
//! struct BorrowRequest {
//!     reader: String,
//! }
//!
//! async fn borrow(
//!     State(app): State<App>,
//!     Path(book_id): Path<String>,
//!     Strict(request): Strict<BorrowRequest>,
//! ) -> Result<CommandAnswer, HttpError> {
//!     let command = BorrowBook { book: BOOKS.try_build(&[&book_id])?, reader: request.reader };
//!     let written = app.store.execute(&app.borrow_book, command).await?;
//!
//!     Ok(CommandAnswer::new(&written))
//! }
//! # }
//! ```
//!
//! `Strict` reads the body as JSON, and refuses a body that does not fit, also because of a field that the type does not have, with 400 and what is wrong, such as `{"message":"malformed request: unknown field \"raeder\""}`. `SubjectScheme::try_build` refuses an ID that can not be part of a subject, such as `a.b`, which `?` answers with 400 as well, where `build` would panic. A command that has succeeded is answered with the revision it wrote, such as `{"revision":"42"}`. Every error of architecturekit turns into an `HttpError` with `?`, which answers it by its category:
//!
//! | Failure | Status | Message |
//! |---|---|---|
//! | an ID that can not be part of a subject | 400 | its own |
//! | a refusal of the decider | 422 | its own |
//! | a conflict | 409 | `conflict: the data has changed since it was read` |
//! | another transient failure | 503 | `internal server error` |
//! | a write whose outcome is unknown | 500 | `outcome unknown: the request may have succeeded` |
//! | any other failure | 500 | `internal server error` |
//!
//! A fixed message keeps internals from the caller, and its cause is logged with the `log` crate. Who may send a command is up to the application: it determines the user with an extractor of its own, and refuses with `HttpError::unauthorized` or `HttpError::forbidden`.
//!
//! A query is a handler of axum as well, which reads a view:
//!
//! ```rust
//! # #[cfg(feature = "axum")]
//! # mod example {
//! # use architecturekit::{Decider, DomainError, Store};
//! #
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! #
//! # fn borrow_book() -> architecturekit::Decider<BorrowBook, Book, architecturekit::DomainError> {
//! #     architecturekit::Decider::new(|command: BorrowBook, book: Book| async move {
//! #         if !book.is_acquired {
//! #             return Err(architecturekit::DomainError::new(format!("{} does not exist", command.book)));
//! #         }
//! #         if book.is_borrowed {
//! #             return Err(architecturekit::DomainError::new(format!("{} is already borrowed", command.book)));
//! #         }
//! #         Ok(vec![BookEvent::Borrowed(BookBorrowed { reader: command.reader })])
//! #     })
//! # }
//! #
//! # #[derive(Clone, Default, serde::Serialize)]
//! # struct ListedBook { id: String, title: String, is_borrowed: bool }
//! # struct BookList { books: architecturekit::InMemoryView<String, ListedBook> }
//! # impl architecturekit::Projection for BookList {
//! #     type Event = BookEvent;
//! #     type Error = architecturekit::ViewError;
//! #     async fn apply(
//! #         &self,
//! #         event: architecturekit::Envelope<BookEvent>,
//! #     ) -> Result<(), architecturekit::ViewError> {
//! #         let Some(book_id) = BOOKS.value(&event.subject, "book_id") else { return Ok(()); };
//! #         match &event.data {
//! #             BookEvent::Acquired(acquired) => {
//! #                 let book = ListedBook { id: book_id.to_owned(), title: acquired.title.clone(), is_borrowed: false };
//! #                 self.books.insert(&event, book)?;
//! #             }
//! #             BookEvent::Borrowed(_) => {
//! #                 self.books.update(book_id, &event, |book| book.is_borrowed = true)?;
//! #             }
//! #         }
//! #         Ok(())
//! #     }
//! #     fn seen(&self, event_id: architecturekit::Revision) { self.books.seen(event_id); }
//! # }
//! # use std::sync::Arc;
//! #
//! # use architecturekit::httpapi::{CommandAnswer, HttpError, Strict};
//! # use axum::extract::{Path, State};
//! # use serde::Deserialize;
//! #
//! # #[derive(Clone)]
//! # struct App {
//! #     store: Arc<Store>,
//! #     borrow_book: Arc<Decider<BorrowBook, Book, DomainError>>,
//! #     book_list: Arc<BookList>,
//! # }
//! #
//! # #[derive(Deserialize)]
//! # struct BorrowRequest {
//! #     reader: String,
//! # }
//! #
//! # async fn borrow(
//! #     State(app): State<App>,
//! #     Path(book_id): Path<String>,
//! #     Strict(request): Strict<BorrowRequest>,
//! # ) -> Result<CommandAnswer, HttpError> {
//! #     let command = BorrowBook { book: BOOKS.try_build(&[&book_id])?, reader: request.reader };
//! #     let written = app.store.execute(&app.borrow_book, command).await?;
//! #
//! #     Ok(CommandAnswer::new(&written))
//! # }
//! use architecturekit::KeyedView;
//! use architecturekit::httpapi::{DEFAULT_WAIT, QueryAnswer, Reading};
//! use serde::Serialize;
//!
//! #[derive(Deserialize, Serialize)]
//! struct BookById {
//!     id: String,
//! }
//!
//! async fn book_by_id(
//!     State(app): State<App>,
//!     reading: Reading,
//!     Strict(query): Strict<BookById>,
//! ) -> Result<QueryAnswer<ListedBook>, HttpError> {
//!     let books = &app.book_list.books;
//!
//!     reading
//!         .revisioned(books, &query, DEFAULT_WAIT, || async {
//!             let Ok(book) = books.get(&query.id).await;
//!             Ok(QueryAnswer::found(book))
//!         })
//!         .await
//! }
//! # }
//! ```
//!
//! A query is sent with the HTTP method `QUERY`, with the query as its body, and answered with what the view holds, or with 404 if it holds nothing. A caller that has just sent a command passes its revision in the header `Wait-For-Revision`, and the query waits until the view has seen it, for at most `DEFAULT_WAIT`, so that the caller reads what it has written. The answer names the revision of the view, and carries a tag, so that a caller that holds the answer already gets 304.
//!
//! The router puts it all together, with the probes of an orchestrator such as Kubernetes, and an answer to a handler that panics:
//!
//! ```rust
//! # #[cfg(feature = "axum")]
//! # mod example {
//! # use architecturekit::{Decider, DomainError, Store};
//! #
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! #
//! # fn borrow_book() -> architecturekit::Decider<BorrowBook, Book, architecturekit::DomainError> {
//! #     architecturekit::Decider::new(|command: BorrowBook, book: Book| async move {
//! #         if !book.is_acquired {
//! #             return Err(architecturekit::DomainError::new(format!("{} does not exist", command.book)));
//! #         }
//! #         if book.is_borrowed {
//! #             return Err(architecturekit::DomainError::new(format!("{} is already borrowed", command.book)));
//! #         }
//! #         Ok(vec![BookEvent::Borrowed(BookBorrowed { reader: command.reader })])
//! #     })
//! # }
//! #
//! # #[derive(Clone, Default, serde::Serialize)]
//! # struct ListedBook { id: String, title: String, is_borrowed: bool }
//! # struct BookList { books: architecturekit::InMemoryView<String, ListedBook> }
//! # impl architecturekit::Projection for BookList {
//! #     type Event = BookEvent;
//! #     type Error = architecturekit::ViewError;
//! #     async fn apply(
//! #         &self,
//! #         event: architecturekit::Envelope<BookEvent>,
//! #     ) -> Result<(), architecturekit::ViewError> {
//! #         let Some(book_id) = BOOKS.value(&event.subject, "book_id") else { return Ok(()); };
//! #         match &event.data {
//! #             BookEvent::Acquired(acquired) => {
//! #                 let book = ListedBook { id: book_id.to_owned(), title: acquired.title.clone(), is_borrowed: false };
//! #                 self.books.insert(&event, book)?;
//! #             }
//! #             BookEvent::Borrowed(_) => {
//! #                 self.books.update(book_id, &event, |book| book.is_borrowed = true)?;
//! #             }
//! #         }
//! #         Ok(())
//! #     }
//! #     fn seen(&self, event_id: architecturekit::Revision) { self.books.seen(event_id); }
//! # }
//! # use std::sync::Arc;
//! #
//! # use architecturekit::httpapi::{CommandAnswer, HttpError, Strict};
//! # use axum::extract::{Path, State};
//! # use serde::Deserialize;
//! #
//! # #[derive(Clone)]
//! # struct App {
//! #     store: Arc<Store>,
//! #     borrow_book: Arc<Decider<BorrowBook, Book, DomainError>>,
//! #     book_list: Arc<BookList>,
//! # }
//! #
//! # #[derive(Deserialize)]
//! # struct BorrowRequest {
//! #     reader: String,
//! # }
//! #
//! # async fn borrow(
//! #     State(app): State<App>,
//! #     Path(book_id): Path<String>,
//! #     Strict(request): Strict<BorrowRequest>,
//! # ) -> Result<CommandAnswer, HttpError> {
//! #     let command = BorrowBook { book: BOOKS.try_build(&[&book_id])?, reader: request.reader };
//! #     let written = app.store.execute(&app.borrow_book, command).await?;
//! #
//! #     Ok(CommandAnswer::new(&written))
//! # }
//! # use architecturekit::KeyedView;
//! # use architecturekit::httpapi::{DEFAULT_WAIT, QueryAnswer, Reading};
//! # use serde::Serialize;
//! #
//! # #[derive(Deserialize, Serialize)]
//! # struct BookById {
//! #     id: String,
//! # }
//! #
//! # async fn book_by_id(
//! #     State(app): State<App>,
//! #     reading: Reading,
//! #     Strict(query): Strict<BookById>,
//! # ) -> Result<QueryAnswer<ListedBook>, HttpError> {
//! #     let books = &app.book_list.books;
//! #
//! #     reading
//! #         .revisioned(books, &query, DEFAULT_WAIT, || async {
//! #             let Ok(book) = books.get(&query.id).await;
//! #             Ok(QueryAnswer::found(book))
//! #         })
//! #         .await
//! # }
//! # async fn example(store: Store, run: architecturekit::ProjectionRun, book_list: Arc<BookList>) {
//! use architecturekit::httpapi::{Health, answer_panic, query};
//! use axum::{Router, routing::{get, post}};
//! use tower_http::catch_panic::CatchPanicLayer;
//!
//! let app = App {
//!     store: Arc::new(store),
//!     borrow_book: Arc::new(borrow_book()),
//!     book_list,
//! };
//!
//! let router: Router = Router::new()
//!     .route("/books/{book_id}/borrow", post(borrow))
//!     .route("/books/by-id", query(book_by_id))
//!     .route("/health/ready", get(Health::readiness([run.clone()])))
//!     .route("/health/alive", get(Health::liveness([run])))
//!     .layer(CatchPanicLayer::custom(answer_panic))
//!     .with_state(app);
//! # }
//! # }
//! ```
//!
//! `Health::readiness` answers with 200 once every projection has caught up, and with 503 before, and once one of them has stopped. `Health::liveness` answers with 503 only once a projection has stopped. `answer_panic` answers a handler that panics with 500, and logs the panic. For another web framework, the feature `http` offers the same with the types of the `http` crate, such as `body_of` and `Reading::of`.
//!
//! ## Testing Deciders
//!
//! The feature `testing` adds given-when-then for deciders in `architecturekit::testing`, and the feature `testcontainers` adds an EventSourcingDB in a container of a test's own, which needs Docker. Turn them on for the tests only:
//!
//! ```shell
//! cargo add architecturekit --dev --features testcontainers
//! ```
//!
//! A decider decides asynchronously, so a test runs on Tokio, and waits for the decision:
//!
//! ```rust
//! # use architecturekit::DomainError;
//! #
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! #
//! # fn borrow_book() -> architecturekit::Decider<BorrowBook, Book, architecturekit::DomainError> {
//! #     architecturekit::Decider::new(|command: BorrowBook, book: Book| async move {
//! #         if !book.is_acquired {
//! #             return Err(architecturekit::DomainError::new(format!("{} does not exist", command.book)));
//! #         }
//! #         if book.is_borrowed {
//! #             return Err(architecturekit::DomainError::new(format!("{} is already borrowed", command.book)));
//! #         }
//! #         Ok(vec![BookEvent::Borrowed(BookBorrowed { reader: command.reader })])
//! #     })
//! # }
//! # #[cfg(feature = "testing")]
//! # {
//! #
//! # let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! # runtime.block_on(async {
//! use architecturekit::testing::given;
//!
//! // In a #[tokio::test]:
//! given(
//!     &borrow_book(),
//!     [
//!         BookEvent::Acquired(BookAcquired { title: "2001".to_owned(), author: "Arthur C. Clarke".to_owned() }),
//!         BookEvent::Borrowed(BookBorrowed { reader: "Jane".to_owned() }),
//!     ],
//! )
//! .when(BorrowBook { book: BOOKS.build(&["42"]), reader: "John".to_owned() })
//! .await
//! .then_failed(DomainError::new("/books/42 is already borrowed"));
//! # });
//! # }
//! ```
//!
//! `then_failed` compares the error of the decider, which therefore implements `PartialEq` and `Debug`. `then_events` compares events by their type and the JSON of their data, which is what would be stored, so they need no `PartialEq`, and `then_preconditions` compares the preconditions they are written with.
//!
//! ## Testing Projections
//!
//! The feature `testing` also drives a projection with events in the shape that the database stores them in, without a database, and the same way a run does: the upcasters run, an event of a type that the projection has no variant for is left out, an event that an upcaster splits arrives part by part, and the projection learns of every event in `seen`. A test then reads what the projection built from its views:
//!
//! ```rust
//! # use architecturekit::InMemoryView;
//! #
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-acquired")]
//! # struct BookAcquired { title: String, author: String }
//! # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
//! # #[event(type = "io.eventsourcingdb.library.book-borrowed")]
//! # struct BookBorrowed { reader: String }
//! #
//! # #[derive(architecturekit::Events)]
//! # enum BookEvent { Acquired(BookAcquired), Borrowed(BookBorrowed) }
//! # #[derive(Clone, Default)]
//! # struct Book { is_acquired: bool, is_borrowed: bool }
//! # impl architecturekit::State for Book {
//! #     type Event = BookEvent;
//! #     fn evolve(self, event: &BookEvent) -> Self {
//! #         match event {
//! #             BookEvent::Acquired(_) => Book { is_acquired: true, ..self },
//! #             BookEvent::Borrowed(_) => Book { is_borrowed: true, ..self },
//! #         }
//! #     }
//! # }
//! #
//! # static BOOKS: std::sync::LazyLock<architecturekit::SubjectScheme> =
//! #     std::sync::LazyLock::new(|| architecturekit::SubjectScheme::new("/books/{book_id}"));
//! # #[derive(Clone)]
//! # struct BorrowBook { book: architecturekit::Subject, reader: String }
//! # impl architecturekit::Command for BorrowBook {
//! #     fn subject(&self) -> architecturekit::Subject { self.book.clone() }
//! #     fn preconditions(&self) -> Vec<architecturekit::Precondition> {
//! #         vec![architecturekit::Precondition::on_state_read()]
//! #     }
//! # }
//! #
//! # #[derive(Clone, Default, serde::Serialize)]
//! # struct ListedBook { id: String, title: String, is_borrowed: bool }
//! # struct BookList { books: architecturekit::InMemoryView<String, ListedBook> }
//! # impl architecturekit::Projection for BookList {
//! #     type Event = BookEvent;
//! #     type Error = architecturekit::ViewError;
//! #     async fn apply(
//! #         &self,
//! #         event: architecturekit::Envelope<BookEvent>,
//! #     ) -> Result<(), architecturekit::ViewError> {
//! #         let Some(book_id) = BOOKS.value(&event.subject, "book_id") else { return Ok(()); };
//! #         match &event.data {
//! #             BookEvent::Acquired(acquired) => {
//! #                 let book = ListedBook { id: book_id.to_owned(), title: acquired.title.clone(), is_borrowed: false };
//! #                 self.books.insert(&event, book)?;
//! #             }
//! #             BookEvent::Borrowed(_) => {
//! #                 self.books.update(book_id, &event, |book| book.is_borrowed = true)?;
//! #             }
//! #         }
//! #         Ok(())
//! #     }
//! #     fn seen(&self, event_id: architecturekit::Revision) { self.books.seen(event_id); }
//! # }
//! # #[cfg(feature = "testing")]
//! # {
//! #
//! # let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! # runtime.block_on(async {
//! use architecturekit::{KeyedView, testing::{project, stored_events}};
//!
//! // In a #[tokio::test]:
//! let book_list = BookList { books: InMemoryView::new(|book: &ListedBook| book.id.clone()) };
//!
//! project(
//!     &book_list,
//!     &stored_events(
//!         &BOOKS.build(&["42"]),
//!         [BookEvent::Acquired(BookAcquired { title: "2001".to_owned(), author: "Arthur C. Clarke".to_owned() })],
//!     ),
//! )
//! .await
//! .expect("the events are applied");
//!
//! let Ok(book) = book_list.books.get("42").await;
//! assert_eq!(book.map(|book| book.title), Some("2001".to_owned()));
//! # });
//! # }
//! ```
//!
//! `project` returns the failure of the projection, so that a test can check that a projection refuses an event. The fields of a `StoredEvent` are public, so that a test can give an event a type that the projection does not know, data that it can not read, or a time of its own. `project_transactional` drives a transactional projection within one transaction.
//!
//! ## License
//!
//! architecturekit is licensed under the [MIT License](https://github.com/thenativeweb/architecturekit-rust/blob/main/LICENSE.md).
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
    CaughtUpError, Envelope, Phase, Projected, Projection, ProjectionError, ProjectionOptions,
    ProjectionRun, ProjectionStatus, Reconnect, ResumableProjection, Transaction,
    TransactionalProjection, wait_caught_up,
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
