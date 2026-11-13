# architecturekit

Building blocks for DDD-based applications with CQRS and event sourcing in Rust, on top of [EventSourcingDB](https://www.eventfoundation.io/products/eventsourcingdb) – a purpose-built database for event sourcing.

ArchitectureKit covers both sides of an event-sourced application: commands, events, and the state to decide on for writing, and projections, views, and queries for reading. An optional feature answers commands and queries over HTTP, with handlers for axum.

The complete documentation of ArchitectureKit, including preconditions, views, projections, queries, handling errors, testing, and HTTP, is available at **[https://www.eventfoundation.io/docs/architecturekit/rust](https://www.eventfoundation.io/docs/architecturekit/rust)**. The API is documented on [docs.rs](https://docs.rs/architecturekit).

## Getting Started

ArchitectureKit needs Rust 1.88 or later, and a running EventSourcingDB. To start one on your machine, see the [quickstart of EventSourcingDB](https://www.eventfoundation.io/docs/eventsourcingdb/quickstart).

Add the crate, together with serde for the data of events, and Tokio to run it:

```shell
cargo add architecturekit
cargo add serde --features derive
cargo add tokio --features macros,rt-multi-thread
```

ArchitectureKit brings the client SDK for EventSourcingDB along as `architecturekit::eventsourcingdb`, so that their versions always fit, and a new major version of the client SDK comes with a new major version of ArchitectureKit.

Create an EventSourcingDB client, and a store with the client and the source to use for all events you write:

```rust
use architecturekit::{Store, eventsourcingdb::Client};

let client = Client::new("http://localhost:3000".parse()?, "secret");
let store = Store::builder(client, "https://library.eventsourcingdb.io").build();
```

Define a command, which describes what someone wants to do:

```rust
use architecturekit::{Command, Precondition, Subject};

#[derive(Clone)]
struct AcquireBook {
    book_id: String,
    title: String,
    author: String,
    isbn: String,
}

impl Command for AcquireBook {
    fn subject(&self) -> Subject {
        Subject::new(&format!("/books/{}", self.book_id))
    }

    fn preconditions(&self) -> Vec<Precondition> {
        vec![Precondition::on_state_read()]
    }
}
```

Define an event, which describes what has happened:

```rust
use architecturekit::Event;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Event)]
#[event(type = "io.eventsourcingdb.library.book-acquired")]
struct BookAcquired {
    title: String,
    author: String,
    isbn: String,
}
```

Define the state that a command is decided on, and how events change it:

```rust
use architecturekit::{Events, State};

#[derive(Events)]
enum BookEvent {
    Acquired(BookAcquired),
}

#[derive(Clone, Default)]
struct Book {
    is_acquired: bool,
}

impl State for Book {
    type Event = BookEvent;

    fn evolve(self, event: &BookEvent) -> Self {
        match event {
            BookEvent::Acquired(_) => Book { is_acquired: true },
        }
    }
}
```

Connect the state with the decision made on it:

```rust
use architecturekit::{Decider, DomainError};

fn acquire_book() -> Decider<AcquireBook, Book, DomainError> {
    Decider::new(|command: AcquireBook, book: Book| async move {
        if book.is_acquired {
            return Err(DomainError::new(format!(
                "book has already been acquired: {}",
                command.book_id
            )));
        }

        Ok(vec![BookEvent::Acquired(BookAcquired {
            title: command.title,
            author: command.author,
            isbn: command.isbn,
        })])
    })
}
```

Execute the command:

```rust
let written_events = store
    .execute(
        &acquire_book(),
        AcquireBook {
            book_id: "42".to_owned(),
            title: "2001 – A Space Odyssey".to_owned(),
            author: "Arthur C. Clarke".to_owned(),
            isbn: "978-0756906788".to_owned(),
        },
    )
    .await?;
```

For everything else, see the **[complete documentation](https://www.eventfoundation.io/docs/architecturekit/rust)**.
