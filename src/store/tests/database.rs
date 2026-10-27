//! A real EventSourcingDB in a container of a test's own, as the isolated
//! database of ArchitectureKit for Go. The container stops once the test drops
//! it, so nothing is left behind, which a container that all tests share would
//! not manage: Rust never drops a static, and the client SDK for Rust has no
//! reaper that removes containers once the tests have run.
//!
//! It runs with a signing key, unlike the database of the feature
//! `testcontainers`, so that its events can be verified by their signatures.

use std::{
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use ed25519_dalek::VerifyingKey;
use eventsourcingdb::{Client, container::Container};

use crate::{Store, StoreBuilder};

/// The source of every event the tests write.
pub(super) const SOURCE: &str = "https://thenativeweb.io";

pub(super) struct Database {
    container: Container,
}

impl Database {
    /// Starts a database, with the image tag that `EVENTSOURCINGDB_IMAGE_TAG`
    /// names, as the client SDK reads it.
    pub(super) async fn start() -> Self {
        let image_tag =
            std::env::var("EVENTSOURCINGDB_IMAGE_TAG").unwrap_or_else(|_| "latest".to_owned());

        let container = Container::builder()
            .with_image_tag(&image_tag)
            .with_signing_key()
            .start()
            .await
            .expect("the database starts");

        Self { container }
    }

    pub(super) async fn client(&self) -> Client {
        self.container
            .get_client()
            .await
            .expect("the database has a client")
    }

    pub(super) fn verification_key(&self) -> VerifyingKey {
        *self
            .container
            .get_verification_key()
            .expect("the database has a signing key")
    }

    /// Returns a store on the database that writes events with the source of
    /// the tests.
    pub(super) async fn store(&self) -> Store {
        self.store_with(|builder| builder).await
    }

    /// Returns a store on the database, built as the function says.
    pub(super) async fn store_with(
        &self,
        build: impl FnOnce(StoreBuilder) -> StoreBuilder,
    ) -> Store {
        build(Store::builder(self.client().await, SOURCE)).build()
    }
}

/// Returns a subject below the given name that no other test writes to, which
/// keeps the tests apart in case two of them share a database after all.
pub(super) fn subject_for(name: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);

    format!(
        "/test/{name}/{}-{}",
        process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    )
}
