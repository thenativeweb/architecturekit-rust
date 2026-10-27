//! A real EventSourcingDB in a container, which all tests of a run share, as
//! the isolated database of ArchitectureKit for Go does. Every test writes to
//! subjects of its own, so the tests do not see each other's events.

use std::{
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use eventsourcingdb::{Client, container::Container};
use tokio::sync::OnceCell;

static CONTAINER: OnceCell<Container> = OnceCell::const_new();

/// Returns the shared container, which starts on first use, with the image tag
/// that `EVENTSOURCINGDB_IMAGE_TAG` names, as the client SDK reads it, and with
/// a signing key, so that its events can be verified by their signatures.
pub(super) async fn container() -> &'static Container {
    CONTAINER
        .get_or_init(|| async {
            let image_tag =
                std::env::var("EVENTSOURCINGDB_IMAGE_TAG").unwrap_or_else(|_| "latest".to_owned());

            Container::builder()
                .with_image_tag(&image_tag)
                .with_signing_key()
                .start()
                .await
                .expect("the database starts")
        })
        .await
}

/// Returns a client of the shared database.
pub(super) async fn client() -> Client {
    container()
        .await
        .get_client()
        .await
        .expect("the database has a client")
}

/// Returns a subject that no other test writes to, below the given name.
pub(super) fn subject_for(name: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);

    format!(
        "/test/{name}/{}-{}",
        process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    )
}
