use std::{
    env,
    future::{self, Future},
    thread,
};

use eventsourcingdb::{Client, container::Container};
use tokio::sync::{OnceCell, oneshot};
use url::Url;

use crate::{EventSchema, Store, StoreBuilder};

mod reaper;
#[cfg(test)]
mod tests;

use reaper::{Failure, Reaper};

/// The environment variable that chooses the tag of the image the container
/// runs, as it does for the tests of the client SDK.
const IMAGE_TAG_VARIABLE: &str = "EVENTSOURCINGDB_IMAGE_TAG";

/// The database that the tests of a program share, once one of them has asked
/// for it.
static SHARED: OnceCell<Database> = OnceCell::const_new();

/// An EventSourcingDB in a container, for the tests that need a real one. It
/// needs the feature `testcontainers`.
///
/// A test gets either a database that the tests of its program share, from
/// [`shared`](Database::shared), or one of its own, from
/// [`isolated`](Database::isolated). Most tests do with the shared one, which
/// starts once rather than for every test, as long as each test writes to
/// subjects of its own, for example with a random ID in them, and reads only
/// from those. A test that reads more than that, such as a projection from
/// `/`, needs a database of its own.
///
/// The container runs the image `thenativeweb/eventsourcingdb` with the tag
/// that the environment variable `EVENTSOURCINGDB_IMAGE_TAG` names, such as
/// `preview`, or `latest` without it. Docker pulls the image only if it is
/// missing. The database runs without a signing key, so its events carry no
/// signature.
///
/// ```no_run
/// use architecturekit::testing;
/// # #[derive(serde::Serialize, serde::Deserialize, architecturekit::Event)]
/// # #[event(type = "io.eventsourcingdb.library.book-acquired")]
/// # struct BookAcquired { title: String }
/// # #[derive(architecturekit::Events)]
/// # enum BookEvent { Acquired(BookAcquired) }
/// # use architecturekit::Events;
/// # async fn test() {
///
/// // In a #[tokio::test]:
/// let store = testing::store("https://library.eventsourcingdb.io", BookEvent::schemas()).await;
/// # }
/// ```
#[derive(Debug)]
pub struct Database {
    url: Url,
    api_token: String,
    /// The container of an isolated database, which stops once the database
    /// is dropped. The shared database keeps its container on a thread of its
    /// own instead, for as long as its program runs.
    _container: Option<Container>,
}

impl Database {
    /// Returns the database that the tests of a program share. The first test
    /// that asks for it starts it, and the others wait for that.
    ///
    /// The database keeps running beyond the test that started it, on a thread
    /// of its own, until the program ends. Then the reaper of Testcontainers,
    /// Ryuk, which runs in a container of its own with access to Docker,
    /// removes it a few seconds later, however the program ends, even if it
    /// is killed. Ryuk is configured as in the other languages of
    /// Testcontainers: the environment variable
    /// `TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED` set to `true` starts it
    /// privileged, `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` names the socket of
    /// Docker where it does not live at `/var/run/docker.sock`, and
    /// `TESTCONTAINERS_RYUK_DISABLED` set to `true` turns it off, which leaves
    /// the database behind.
    ///
    /// # Panics
    ///
    /// Fails the test if the database or Ryuk can not be started, for example
    /// since Docker is not running. The next test that asks tries again.
    pub async fn shared() -> &'static Self {
        SHARED
            .get_or_init(|| async {
                match start_shared().await {
                    Ok(database) => database,
                    Err(error) => panic!("starting the shared database: {error}"),
                }
            })
            .await
    }

    /// Starts a database in a container of the test's own, and waits until it
    /// can be reached. Starting it takes a moment, about a second with the
    /// image at hand.
    ///
    /// The container stops once the database is dropped, so nothing is left
    /// behind after the test. It has to be dropped within the Tokio runtime
    /// that started it, which a database that a test holds in a local variable
    /// is.
    ///
    /// # Panics
    ///
    /// Fails the test if the container can not be started, for example since
    /// Docker is not running.
    pub async fn isolated() -> Self {
        let container = match start_container().await {
            Ok(container) => container,
            Err(error) => panic!("starting a database: {error}"),
        };
        let url = match container.get_base_url().await {
            Ok(url) => url,
            Err(error) => panic!("finding out the address of the database: {error}"),
        };

        Self {
            url,
            api_token: container.get_api_token().to_owned(),
            _container: Some(container),
        }
    }

    /// Returns the address of the database, which a client needs together with
    /// the [`api_token`](Database::api_token), for a test that connects by
    /// itself, such as one that starts a whole server.
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns the API token of the database.
    #[must_use]
    pub fn api_token(&self) -> &str {
        &self.api_token
    }

    /// Returns a client of the database, for a test that reads or writes
    /// events past the store, such as one that writes an event no command
    /// would.
    pub fn client(&self) -> impl Future<Output = Client> + Send {
        // The address and the token are known by now, so nothing is waited
        // for, but the client stays awaited, as it was when it asked the
        // container.
        future::ready(Client::new(self.url.clone(), self.api_token.clone()))
    }

    /// Returns a store on the database that writes events with the given
    /// source, and registers the given schemas first, such as those of
    /// `BookEvent::schemas()`.
    ///
    /// # Panics
    ///
    /// Fails the test if the database refuses one of the schemas.
    pub async fn store(
        &self,
        source: impl Into<String>,
        schemas: impl IntoIterator<Item = EventSchema>,
    ) -> Store {
        self.store_with(source, schemas, |builder| builder).await
    }

    /// Returns a store with options, which the given function sets on its
    /// builder, such as a state cache, and otherwise behaves as
    /// [`store`](Database::store) does:
    ///
    /// ```no_run
    /// # use architecturekit::testing::Database;
    /// # async fn test(database: &Database) {
    /// let store = database
    ///     .store_with("https://library.eventsourcingdb.io", [], |builder| {
    ///         builder.state_cache(1_000)
    ///     })
    ///     .await;
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Fails the test if the database refuses one of the schemas.
    pub async fn store_with(
        &self,
        source: impl Into<String>,
        schemas: impl IntoIterator<Item = EventSchema>,
        configure: impl FnOnce(StoreBuilder) -> StoreBuilder,
    ) -> Store {
        let store = configure(Store::builder(self.client().await, source)).build();
        if let Err(failure) = store.register_schemas(schemas).await {
            panic!("registering the schemas: {failure}");
        }

        store
    }
}

/// Returns a store on the database that the tests of a program share, which
/// writes events with the given source, and registers the given schemas first.
/// It is [`Database::shared`] and [`Database::store`] in one.
///
/// # Panics
///
/// Fails the test if the database can not be started, or refuses one of the
/// schemas.
pub async fn store(
    source: impl Into<String>,
    schemas: impl IntoIterator<Item = EventSchema>,
) -> Store {
    Database::shared().await.store(source, schemas).await
}

/// Returns a store with options on the database that the tests of a program
/// share. It is [`Database::shared`] and [`Database::store_with`] in one.
///
/// # Panics
///
/// Fails the test if the database can not be started, or refuses one of the
/// schemas.
pub async fn store_with(
    source: impl Into<String>,
    schemas: impl IntoIterator<Item = EventSchema>,
    configure: impl FnOnce(StoreBuilder) -> StoreBuilder,
) -> Store {
    Database::shared()
        .await
        .store_with(source, schemas, configure)
        .await
}

/// Starts the container of a database, from the image the environment chooses.
async fn start_container() -> Result<Container, Failure> {
    let mut builder = Container::builder();
    if let Ok(image_tag) = env::var(IMAGE_TAG_VARIABLE) {
        builder = builder.with_image_tag(&image_tag);
    }

    Ok(builder.start().await?)
}

/// Starts the shared database on a thread of its own, with a runtime of its
/// own, so that it outlives the runtime of the test that asked for it first.
async fn start_shared() -> Result<Database, Failure> {
    let (sender, receiver) = oneshot::channel();
    let _ = thread::Builder::new()
        .name("architecturekit-shared-database".to_owned())
        .spawn(move || keep_shared(sender))?;

    receiver
        .await
        .map_err(|_| "the thread of the shared database ended before the database started")?
}

/// Starts the shared database and the reaper that watches it, hands the
/// database over, and keeps both for as long as the program runs.
fn keep_shared(sender: oneshot::Sender<Result<Database, Failure>>) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = sender.send(Err(error.into()));
            return;
        }
    };

    runtime.block_on(async move {
        match start_watched().await {
            Ok((database, kept)) => {
                // If no test waits for the database any longer, it goes, and
                // with it the container, within this runtime.
                if sender.send(Ok(database)).is_ok() {
                    let _kept = kept;
                    future::pending::<()>().await;
                }
            }
            Err(error) => {
                let _ = sender.send(Err(error));
            }
        }
    });
}

/// Starts the reaper and the container of the shared database, and hands the
/// container to the reaper. A container that the reaper does not take on is
/// removed again, since it is dropped within this runtime.
async fn start_watched() -> Result<(Database, (Option<Reaper>, Container)), Failure> {
    let mut reaper = Reaper::start().await?;
    let container = start_container().await?;
    if let Some(reaper) = &mut reaper {
        reaper.watch(&container).await?;
    }

    let database = Database {
        url: container.get_base_url().await?,
        api_token: container.get_api_token().to_owned(),
        _container: None,
    };

    Ok((database, (reaper, container)))
}
