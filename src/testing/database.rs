use std::env;

use eventsourcingdb::{Client, container::Container};

use crate::{EventSchema, Store};

#[cfg(test)]
mod tests;

/// The environment variable that chooses the tag of the image the container
/// runs, as it does for the tests of the client SDK.
const IMAGE_TAG_VARIABLE: &str = "EVENTSOURCINGDB_IMAGE_TAG";

/// An EventSourcingDB in a container of a test's own, for the tests that need
/// a real one. It needs the feature `testcontainers`.
///
/// The container stops once the database is dropped, so nothing is left behind
/// after the test. It has to be dropped within the Tokio runtime that started
/// it, which a database that a test holds in a local variable is.
///
/// The container runs the image `thenativeweb/eventsourcingdb` with the tag
/// that the environment variable `EVENTSOURCINGDB_IMAGE_TAG` names, such as
/// `preview`, or `latest` without it. Docker pulls the image only if it is
/// missing. The database runs without a signing key, so its events carry no
/// signature.
///
/// ```no_run
/// use architecturekit::testing::Database;
/// # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// # #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// # struct BookAcquired { title: String }
/// # #[derive(architecturekit::Events)]
/// # enum BookEvent { Acquired(BookAcquired) }
/// # use architecturekit::Events;
/// # async fn test() {
///
/// // In a #[tokio::test]:
/// let database = Database::isolated().await;
/// let store = database
///     .store("https://library.eventsourcingdb.io", BookEvent::schemas())
///     .await;
/// # }
/// ```
#[derive(Debug)]
pub struct Database {
    container: Container,
}

impl Database {
    /// Starts a database in a container of its own, and waits until it can be
    /// reached. Starting it takes a moment, about a second with the image at
    /// hand.
    ///
    /// # Panics
    ///
    /// Fails the test if the container can not be started, for example since
    /// Docker is not running.
    pub async fn isolated() -> Self {
        let mut builder = Container::builder();
        if let Ok(image_tag) = env::var(IMAGE_TAG_VARIABLE) {
            builder = builder.with_image_tag(&image_tag);
        }

        match builder.start().await {
            Ok(container) => Self { container },
            Err(error) => panic!("starting a database: {error}"),
        }
    }

    /// Returns a client of the database, for a test that reads or writes
    /// events past the store, such as one that writes an event no command
    /// would.
    ///
    /// # Panics
    ///
    /// Fails the test if the address of the container can not be found out.
    pub async fn client(&self) -> Client {
        match self.container.get_client().await {
            Ok(client) => client,
            Err(error) => panic!("connecting to the database: {error}"),
        }
    }

    /// Returns a store on the database that writes events with the given
    /// source, and registers the given schemas first, such as those of
    /// `BookEvent::schemas()`. For a store with options, such as a state
    /// cache, build one with [`Store::builder`] from [`client`](Database::client)
    /// instead.
    ///
    /// # Panics
    ///
    /// Fails the test if the database refuses one of the schemas.
    pub async fn store(
        &self,
        source: impl Into<String>,
        schemas: impl IntoIterator<Item = EventSchema>,
    ) -> Store {
        let store = Store::builder(self.client().await, source).build();
        if let Err(failure) = store.register_schemas(schemas).await {
            panic!("registering the schemas: {failure}");
        }

        store
    }
}
