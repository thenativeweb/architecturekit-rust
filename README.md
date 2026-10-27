# architecturekit

Building blocks for DDD-based applications with CQRS and event sourcing in Rust, on top of [EventSourcingDB](https://www.eventsourcingdb.io) – a purpose-built database for event sourcing.

architecturekit is the Rust edition of ArchitectureKit, whose concepts are described in the [documentation of ArchitectureKit](https://www.eventfoundation.io/docs/architecturekit) with examples in Go. The Rust edition follows the same concepts under the same names, written the way Rust names things, and it is being built step by step. The API is documented on [docs.rs](https://docs.rs/architecturekit).

## Installation

Add architecturekit, and serde to write and read the data of events:

```shell
cargo add architecturekit
cargo add serde --features derive
```

architecturekit needs Rust 1.88 or later.

## Defining Events

An event describes what has happened. Derive `Event` for its data, together with serde's `Serialize` and `Deserialize`, and name its event type:

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

The derive also derives the JSON schema of the event from the fields that serde writes, by the same rules as ArchitectureKit for Go, so that a Go and a Rust application that write the same event type derive the same schema for it. EventSourcingDB rejects events whose data does not match the registered schema, and a registered schema can not change.

For a type within the data of an event, such as a nested struct, derive `Schema`. A type whose schema can not be derived, such as an enum, implements `Schema` by hand, and the derive refuses to compile otherwise. For the rules, see the documentation of `Schema` on [docs.rs](https://docs.rs/architecturekit/latest/architecturekit/trait.Schema.html).

Where Rust differs from Go, the schema follows what serde writes: a Go pointer, slice or map that may be nil corresponds to an `Option` in Rust, since serde never writes a `Vec` or a `HashMap` as null, and a `Vec<u8>` is an array of integers.

## Testing Deciders

The feature `testing` adds given-when-then for deciders in `architecturekit::testing`, and the feature `testcontainers` adds an EventSourcingDB in a container of a test's own, which needs Docker. Turn them on for the tests only:

```shell
cargo add architecturekit --dev --features testcontainers
```

A decider decides asynchronously, so a test runs on Tokio and waits for the decision:

```rust
use architecturekit::testing::given;

#[tokio::test]
async fn refuses_to_lend_a_book_that_is_lent_out() {
    given(&lend_book(), [BookEvent::Lent(BookLent { reader: "Jane".to_owned() })])
        .when(LendBook { book_id: "42".to_owned(), reader: "John".to_owned() })
        .await
        .then_failed(LendError::AlreadyLent);
}
```

`then_failed` compares the error of the decider, which therefore implements `PartialEq` and `Debug`. Events are compared by their type and the JSON of their data, which is what would be stored, so they need no `PartialEq`.

## License

architecturekit is licensed under the [MIT License](LICENSE.md).
