//! Serving commands and queries over HTTP, with the answers of ArchitectureKit
//! for Go. The module needs the feature `http`, and its parts for axum the
//! feature `axum`, which turns on `http` as well.
//!
//! The kit offers building blocks rather than routes: every route is an
//! ordinary handler of the web framework, which determines its user with an
//! extractor of its own, and answers with what the kit answers. A failure is
//! an [`HttpError`], which every error of the kit turns into, with the status
//! and the message ArchitectureKit for Go answers it with.

mod body;
mod decoding;
mod error;
#[cfg(feature = "axum")]
mod extract;
#[cfg(test)]
mod logs;

pub use body::{MAX_REQUEST_BODY, body_of, no_body};
pub use error::HttpError;
#[cfg(feature = "axum")]
pub use extract::{NoBody, Strict};
