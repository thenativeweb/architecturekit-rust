//! The reaper of Testcontainers, Ryuk, which removes the shared database once
//! the program that started it has ended.
//!
//! The reaper runs in a container of its own, with access to Docker. The
//! program connects to it, and names the containers it is to watch. Once the
//! connection ends, which the operating system sees to however the program
//! ends, even if it is killed, the reaper waits a few seconds in case the
//! program connects again, removes the containers, and then itself.

use std::{collections::HashMap, env, error::Error, time::Duration};

use eventsourcingdb::container::Container;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    bollard::{Docker, query_parameters::ListContainersOptionsBuilder},
    core::{IntoContainerPort, Mount, WaitFor},
    runners::AsyncRunner,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

/// A failure while the reaper starts or takes on a container.
pub(super) type Failure = Box<dyn Error + Send + Sync>;

/// The image of the reaper, in the version that the other languages of
/// Testcontainers start, too.
const IMAGE: &str = "testcontainers/ryuk";
const TAG: &str = "0.14.0";

/// The port the reaper listens on, within its container.
const PORT: u16 = 8080;

/// The path of the socket of Docker, which the reaper reaches Docker through.
const SOCKET: &str = "/var/run/docker.sock";

/// The environment variables of Testcontainers that the other languages know as
/// well: one turns the reaper off, one starts it privileged, and one names the
/// socket of Docker, for an environment in which it lives elsewhere.
const DISABLED_VARIABLE: &str = "TESTCONTAINERS_RYUK_DISABLED";
const PRIVILEGED_VARIABLE: &str = "TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED";
const SOCKET_VARIABLE: &str = "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE";

/// How long the reaper may take to acknowledge a container.
const ACKNOWLEDGEMENT_TIMEOUT: Duration = Duration::from_secs(10);

/// A running reaper, and the connection the program keeps to it.
#[derive(Debug)]
pub(super) struct Reaper {
    /// Kept for as long as the program runs, since the reaper removes itself.
    _container: ContainerAsync<GenericImage>,
    connection: TcpStream,
}

impl Reaper {
    /// Starts a reaper, and connects to it, or returns `None` if the
    /// environment variable `TESTCONTAINERS_RYUK_DISABLED` turns it off.
    pub(super) async fn start() -> Result<Option<Self>, Failure> {
        if is_true(DISABLED_VARIABLE) {
            return Ok(None);
        }

        let socket = env::var(SOCKET_VARIABLE).unwrap_or_else(|_| SOCKET.to_owned());
        let container = GenericImage::new(IMAGE, TAG)
            .with_exposed_port(PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("msg=Started"))
            .with_mount(Mount::bind_mount(socket, SOCKET))
            .with_privileged(is_true(PRIVILEGED_VARIABLE))
            .with_host_config_modifier(|config| config.auto_remove = Some(true))
            .start()
            .await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(PORT.tcp()).await?;
        let connection = TcpStream::connect((host.to_string(), port)).await?;

        Ok(Some(Self {
            _container: container,
            connection,
        }))
    }

    /// Hands the container of a database to the reaper, which removes it once
    /// the connection ends, and waits until the reaper has acknowledged it.
    pub(super) async fn watch(&mut self, database: &Container) -> Result<(), Failure> {
        let id = id_of(database).await?;
        self.connection
            .write_all(format!("id={id}\n").as_bytes())
            .await?;

        let mut answer = [0; 4];
        let _ = timeout(
            ACKNOWLEDGEMENT_TIMEOUT,
            self.connection.read_exact(&mut answer),
        )
        .await??;
        if &answer != b"ACK\n" {
            return Err(format!(
                "the reaper answered {:?} rather than acknowledging the container",
                String::from_utf8_lossy(&answer)
            )
            .into());
        }

        Ok(())
    }
}

/// Finds out the ID of the container of a database, which the client SDK does
/// not hand out, as the one container that publishes the port the database is
/// reached on, since no other one can publish it while the database runs.
async fn id_of(database: &Container) -> Result<String, Failure> {
    let port = database.get_mapped_port().await?.to_string();
    let filters = HashMap::from([("publish", vec![port.as_str()])]);
    let options = ListContainersOptionsBuilder::new()
        .filters(&filters)
        .build();

    let containers = Docker::connect_with_defaults()?
        .list_containers(Some(options))
        .await?;
    match containers.as_slice() {
        [container] => container
            .id
            .clone()
            .ok_or_else(|| "Docker names no ID for the container of the database".into()),
        _ => Err(format!(
            "{} containers publish the port {port} of the database, rather than one",
            containers.len()
        )
        .into()),
    }
}

/// Tells whether an environment variable is set to `true` or `1`, as the other
/// languages of Testcontainers read their switches.
fn is_true(variable: &str) -> bool {
    env::var(variable).is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}
