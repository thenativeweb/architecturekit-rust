//! The reaper of Testcontainers, Ryuk, which removes the shared database once
//! the program that started it has ended.
//!
//! The reaper runs in a container of its own, with access to Docker. The
//! program connects to it, and names the label of the containers it is to
//! remove, before it starts any of them. Once the connection ends, which the
//! operating system sees to however the program ends, even if it is killed,
//! the reaper waits a few seconds in case the program connects again, removes
//! the containers that carry the label by then, and then itself.

use std::{env, error::Error, time::Duration};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, Mount, WaitFor},
    runners::AsyncRunner,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

/// A failure while the reaper starts or takes on the session.
pub(super) type Failure = Box<dyn Error + Send + Sync>;

/// The key of the label that marks the containers of a program for the
/// reaper, with the session of the program as its value. It lives in the
/// namespace of the kit, since Testcontainers reserves its own.
pub(super) const SESSION_LABEL: &str = "io.architecturekit.session";

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

/// How long the reaper may take to acknowledge the session.
const ACKNOWLEDGEMENT_TIMEOUT: Duration = Duration::from_secs(10);

/// A running reaper, and the connection the program keeps to it.
#[derive(Debug)]
pub(super) struct Reaper {
    /// Kept for as long as the program runs, since the reaper removes itself.
    _container: ContainerAsync<GenericImage>,
    /// Kept for as long as the program runs, since its end tells the reaper
    /// that the program has ended.
    _connection: TcpStream,
}

impl Reaper {
    /// Starts a reaper, connects to it, and names the given session, so that
    /// the reaper removes every container whose label
    /// `io.architecturekit.session` has the session as its value, once the
    /// connection ends. It returns once the reaper has acknowledged the
    /// session, so a container that starts afterwards is removed however the
    /// program ends, even while the container is still starting. It returns
    /// `None` if the environment variable `TESTCONTAINERS_RYUK_DISABLED` turns
    /// the reaper off.
    pub(super) async fn start(session: &str) -> Result<Option<Self>, Failure> {
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
        let mut connection = TcpStream::connect((host.to_string(), port)).await?;

        connection
            .write_all(format!("label={SESSION_LABEL}={session}\n").as_bytes())
            .await?;
        let mut answer = [0; 4];
        let _ = timeout(ACKNOWLEDGEMENT_TIMEOUT, connection.read_exact(&mut answer)).await??;
        if &answer != b"ACK\n" {
            return Err(format!(
                "the reaper answered {:?} rather than acknowledging the session",
                String::from_utf8_lossy(&answer)
            )
            .into());
        }

        Ok(Some(Self {
            _container: container,
            _connection: connection,
        }))
    }
}

/// Tells whether an environment variable is set to `true` or `1`, as the other
/// languages of Testcontainers read their switches.
fn is_true(variable: &str) -> bool {
    env::var(variable).is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}
