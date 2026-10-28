//! Serving commands and queries over HTTP, with the answers of ArchitectureKit
//! for Go. The module needs the feature `http`, and its parts for axum the
//! feature `axum`, which turns on `http` as well.
//!
//! The kit offers building blocks rather than routes: every route is an
//! ordinary handler of the web framework, which determines its user with an
//! extractor of its own, and answers with what the kit answers. A failure is
//! an [`HttpError`], which every error of the kit turns into, with the status
//! and the message ArchitectureKit for Go answers it with.

mod error;
#[cfg(test)]
mod logs;

pub use error::HttpError;
