use std::{
    any::Any,
    env,
    future::{self, Future},
    panic::{self, AssertUnwindSafe},
    sync::Once,
    thread,
};

use eventsourcingdb::{Client, container::Container};
use tokio::sync::SetOnce;
use url::Url;

use super::chain;
use crate::{EventSchema, Store, StoreBuilder};

mod reaper;
#[cfg(test)]
mod tests;

use reaper::{Failure, Reaper};

/// The environment variable that chooses the tag of the image the container
/// runs, as it does for the tests of the client SDK.
const IMAGE_TAG_VARIABLE: &str = "EVENTSOURCINGDB_IMAGE_TAG";

/// The database that the tests of a program share, once one of them has asked
/// for it, or why it could not be started.
static SHARED: StartedOnce<Database> = StartedOnce::new("architecturekit-shared-database");

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
    /// that asks for it starts it, and the others wait for that. It starts at
    /// most once per program, even if the test that started it stops waiting
    /// for it, for example since it timed out, and the tests after that get
    /// the same database.
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
    /// since Docker is not running, with the message of the failure and those
    /// of its causes. A failed start is not tried again: every test that asks
    /// for the database later fails with the same message, rather than waiting
    /// for another start that would most likely fail as well.
    pub async fn shared() -> &'static Self {
        match SHARED.get(start_watched).await {
            Ok(database) => database,
            Err(failure) => panic!("starting the shared database: {failure}"),
        }
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
    #[must_use]
    pub fn client(&self) -> Client {
        Client::new(self.url.clone(), self.api_token.clone())
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
        let store = configure(Store::builder(self.client(), source)).build();
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

/// What a thread of its own starts at most once for the whole program, and
/// keeps until the program ends, together with the outcome of starting it,
/// which every caller gets: the value, or why it could not be started.
struct StartedOnce<T> {
    /// The name of the thread.
    name: &'static str,
    started: Once,
    outcome: SetOnce<Result<T, String>>,
}

impl<T: Send + Sync + 'static> StartedOnce<T> {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            started: Once::new(),
            outcome: SetOnce::const_new(),
        }
    }

    /// Starts the thread on the first call, which runs the given start within
    /// a runtime of its own, and waits for the outcome, as every later call
    /// does, whose start is not run. The thread publishes the outcome whether
    /// a caller still waits for it or not, so a caller can stop waiting at any
    /// time, and a later one gets the same outcome. Of what the start hands
    /// out, the thread publishes the value, and keeps the rest for as long as
    /// the program runs.
    ///
    /// # Errors
    ///
    /// Fails with the message of the failure that the start, the runtime, or
    /// the thread failed with, followed by those of its causes.
    async fn get<K, Fut>(
        &'static self,
        start: impl FnOnce() -> Fut + Send + 'static,
    ) -> Result<&'static T, String>
    where
        Fut: Future<Output = Result<(T, K), Failure>>,
    {
        self.started.call_once(|| {
            let spawned = thread::Builder::new()
                .name(self.name.to_owned())
                .spawn(move || {
                    if let Err(panic) = panic::catch_unwind(AssertUnwindSafe(|| self.keep(start))) {
                        self.publish(Err(format!(
                            "the thread {} panicked: {}",
                            self.name,
                            text_of(&*panic)
                        )));
                    }
                });
            if let Err(failure) = spawned {
                self.publish(Err(chain(&failure)));
            }
        });

        self.outcome.wait().await.as_ref().map_err(Clone::clone)
    }

    /// Runs the start within a runtime of its own, publishes its outcome, and
    /// keeps what it has started for as long as the program runs.
    fn keep<K, Fut>(&self, start: impl FnOnce() -> Fut)
    where
        Fut: Future<Output = Result<(T, K), Failure>>,
    {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(failure) => {
                self.publish(Err(chain(&failure)));
                return;
            }
        };

        runtime.block_on(async {
            match start().await {
                Ok((value, kept)) => {
                    self.publish(Ok(value));
                    let _kept = kept;
                    future::pending::<()>().await;
                }
                Err(failure) => self.publish(Err(chain(&*failure))),
            }
        });
    }

    /// Publishes the outcome. Only the one thread publishes, and only once,
    /// or the first caller if the thread can not be started at all.
    fn publish(&self, outcome: Result<T, String>) {
        let _ = self.outcome.set(outcome);
    }
}

/// Returns the text that a thread panicked with.
fn text_of(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("a value that is no text")
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
