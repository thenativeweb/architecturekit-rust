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

mod command;
mod decider;
mod error;
mod event;
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
pub use revision::{NotARevision, compare_revisions, parse_revision};
pub use schema::Schema;
pub use state::{EventSchema, Events, State, replay, replay_stored, step, step_stored};
pub use store::{
    Error, EventOn, ExecuteError, LoadError, ReadError, RegisterSchemasError, Store, StoreBuilder,
    WriteError,
};
pub use subject::{SubjectScheme, SubjectValuesError};
pub use upcast::{UpcastEvent, Upcasters};

/// Stops a run of a projection from outside, as a `context.Context` does in
/// ArchitectureKit for Go (see [`Store::start_projection`]).
pub use tokio_util::sync::CancellationToken;

#[cfg(feature = "derive")]
pub use architecturekit_macros::{Event, Events, Schema};

#[doc(hidden)]
#[path = "private.rs"]
pub mod __private;
