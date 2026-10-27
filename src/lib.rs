//! Building blocks for DDD-based applications with CQRS and event sourcing, on
//! top of [EventSourcingDB](https://www.eventsourcingdb.io).
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

mod event;
#[cfg(doctest)]
mod refusals;
mod schema;
mod subject;

pub use event::Event;
pub use schema::Schema;
pub use subject::{SubjectScheme, SubjectValuesError};

#[cfg(feature = "derive")]
pub use architecturekit_macros::{Event, Schema};

#[doc(hidden)]
#[path = "private.rs"]
pub mod __private;
