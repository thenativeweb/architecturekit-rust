use std::{error::Error, fmt, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::future;
use tokio::sync::watch;

use crate::{Categorized, Category, ProjectionError, Revision};

/// How a projection is started. Build it with [`named`](ProjectionOptions::named)
/// or [`default`](Default::default), which leaves the projection without a
/// name:
///
/// ```
/// use architecturekit::ProjectionOptions;
///
/// let options = ProjectionOptions::named("book list");
/// assert_eq!(options.name.as_deref(), Some("book list"));
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ProjectionOptions {
    /// The name of the projection, which its run, its status, and the
    /// failures it reports name. A projection without a name is told apart by
    /// its subject.
    pub name: Option<String>,
}

impl ProjectionOptions {
    /// Options for a projection with the given name.
    ///
    /// # Panics
    ///
    /// A name that is empty is a programming error, so starting a projection
    /// with it panics.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
        }
    }
}

/// Where a run of a projection stands.
///
/// The phases are closed on purpose: a caller that judges a run by its phase,
/// as [`Health`](crate::httpapi::Health) does, has to place every one of them,
/// so a new phase would come with a new major version rather than fall into a
/// catch-all arm unnoticed:
///
/// ```
/// use architecturekit::Phase;
///
/// fn is_ready(phase: Phase, has_caught_up: bool) -> bool {
///     match phase {
///         Phase::CatchingUp | Phase::Stopped => false,
///         Phase::Live => true,
///         Phase::Reconnecting => has_caught_up,
///     }
/// }
///
/// assert!(is_ready(Phase::Live, true));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// The projection applies the events stored so far.
    CatchingUp,
    /// The projection follows the events as they are written.
    Live,
    /// Reading failed in a way that may pass, and the projection waits to read
    /// again, from its checkpoint on.
    Reconnecting,
    /// The run has ended, because its token was cancelled, or because of a
    /// failure that trying again will not fix.
    Stopped,
}

/// The status of a run of a projection, as [`ProjectionRun::status`] returns it.
/// It may gain fields, so it can not be built outside the kit.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ProjectionStatus {
    /// Where the run stands.
    pub phase: Phase,
    /// Since when the run is in its phase. For [`Phase::Reconnecting`], it is
    /// when the disruption began, however many attempts it has taken since.
    pub since: DateTime<Utc>,
    /// Why the run reconnects, or why it stopped. It is `None` while the run
    /// catches up or is live, and for a run that stopped because its token was
    /// cancelled.
    pub error: Option<Arc<ProjectionError>>,
    /// How many attempts the run has made to read again since it last caught
    /// up.
    pub attempts: usize,
    /// The ID of the last event the projection committed, if any.
    pub revision: Option<Revision>,
    /// Whether the run has caught up at least once. It stays true once set.
    pub has_caught_up: bool,
}

/// A run of a projection, which follows the events of its subjects until its
/// token is cancelled, or until a failure that trying again will not fix. Start
/// one with [`Store::start_projection`](crate::Store::start_projection) or its
/// counterparts. It can be cloned, and every clone watches the same run.
#[derive(Clone, Debug)]
pub struct ProjectionRun {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    name: Option<String>,
    status: watch::Sender<ProjectionStatus>,
}

impl ProjectionRun {
    /// Creates a run that is catching up.
    ///
    /// # Panics
    ///
    /// A name that is empty is a programming error, so this panics.
    pub(crate) fn new(options: ProjectionOptions) -> Self {
        assert!(
            options.name.as_deref() != Some(""),
            "architecturekit: a projection needs a name that is not empty, or none"
        );

        let (status, _) = watch::channel(ProjectionStatus {
            phase: Phase::CatchingUp,
            since: Utc::now(),
            error: None,
            attempts: 0,
            revision: None,
            has_caught_up: false,
        });

        Self {
            inner: Arc::new(Inner {
                name: options.name,
                status,
            }),
        }
    }

    /// Returns the name of the projection, if it has one.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// Returns the status of the run.
    #[must_use]
    pub fn status(&self) -> ProjectionStatus {
        self.inner.status.borrow().clone()
    }

    /// Returns the failure the run ended with, or `None` while it runs, and if
    /// it stopped because its token was cancelled.
    #[must_use]
    pub fn error(&self) -> Option<Arc<ProjectionError>> {
        let status = self.inner.status.borrow();
        if status.phase == Phase::Stopped {
            return status.error.clone();
        }

        None
    }

    /// Waits until the run has caught up for the first time. A run that stops
    /// before it has caught up never has, so use [`wait_caught_up`] to wait
    /// for it to catch up or fail.
    pub async fn caught_up(&self) {
        self.wait_for(|status| status.has_caught_up).await;
    }

    /// Waits until the run has stopped.
    pub async fn done(&self) {
        self.wait_for(|status| status.phase == Phase::Stopped).await;
    }

    async fn wait_for(&self, holds: impl FnMut(&ProjectionStatus) -> bool) {
        let mut status = self.inner.status.subscribe();
        // The run holds the sender, so waiting can not fail.
        let _ = status.wait_for(holds).await;
    }

    /// Marks the run as live, and as having caught up.
    pub(crate) fn caught_up_now(&self) {
        self.inner.status.send_modify(|status| {
            status.phase = Phase::Live;
            status.since = Utc::now();
            status.error = None;
            status.attempts = 0;
            status.has_caught_up = true;
        });
    }

    /// Marks the run as reconnecting because of the given failure, keeping the
    /// beginning of a disruption that lasts.
    pub(crate) fn disrupted(&self, failure: Arc<ProjectionError>) {
        self.inner.status.send_modify(|status| {
            if status.phase != Phase::Reconnecting {
                status.phase = Phase::Reconnecting;
                status.since = Utc::now();
                status.attempts = 0;
            }
            status.error = Some(failure);
            status.attempts += 1;
        });
    }

    /// Records the ID of the last event the projection committed.
    pub(crate) fn committed(&self, event_id: Revision) {
        self.inner.status.send_modify(|status| {
            status.revision = Some(event_id);
        });
    }

    /// Marks the run as stopped, with the failure it ended with, if any.
    pub(crate) fn stop(&self, failure: Option<ProjectionError>) {
        self.inner.status.send_modify(|status| {
            status.phase = Phase::Stopped;
            status.since = Utc::now();
            status.error = failure.map(Arc::new);
            status.attempts = 0;
        });
    }
}

/// Waits until every given run has caught up, as a service does before it
/// takes requests. A run that has caught up once counts, even if it has ended
/// since. Dropping the future stops waiting.
///
/// # Errors
///
/// Fails as soon as one of the runs has ended before it caught up: with the
/// failure of the first run among the given ones that ended with one, and
/// otherwise, if a run stopped because its token was cancelled, naming the
/// first of those.
pub async fn wait_caught_up(runs: &[&ProjectionRun]) -> Result<(), CaughtUpError> {
    let mut statuses: Vec<_> = runs
        .iter()
        .map(|run| run.inner.status.subscribe())
        .collect();

    loop {
        if let Some(verdict) = caught_up_verdict(runs) {
            return verdict;
        }

        let changes = statuses.iter_mut().map(|status| Box::pin(status.changed()));
        let _ = future::select_all(changes).await;
    }
}

/// Decides whether the runs have caught up, or failed to, or `None` if that
/// is still open.
fn caught_up_verdict(runs: &[&ProjectionRun]) -> Option<Result<(), CaughtUpError>> {
    let mut has_every_run_caught_up = true;
    let mut stopped = None;

    for run in runs {
        let status = run.status();
        if status.has_caught_up {
            continue;
        }
        has_every_run_caught_up = false;
        if status.phase != Phase::Stopped {
            continue;
        }
        if let Some(error) = status.error {
            return Some(Err(CaughtUpError::Failed {
                projection: run.name().map(ToOwned::to_owned),
                error,
            }));
        }
        if stopped.is_none() {
            stopped = Some(*run);
        }
    }

    if has_every_run_caught_up {
        return Some(Ok(()));
    }

    stopped.map(|run| {
        Err(CaughtUpError::Stopped {
            projection: run.name().map(ToOwned::to_owned),
        })
    })
}

/// Why [`wait_caught_up`] failed. It may gain variants and fields, so match it
/// with a catch-all arm, or ask for its category.
#[derive(Debug)]
#[non_exhaustive]
pub enum CaughtUpError {
    /// A run ended with a failure before it caught up.
    #[non_exhaustive]
    Failed {
        /// The name of the projection, if it has one.
        projection: Option<String>,
        /// The failure the run ended with.
        error: Arc<ProjectionError>,
    },
    /// A run stopped before it caught up, because its token was cancelled. It
    /// belongs to no category.
    #[non_exhaustive]
    Stopped {
        /// The name of the projection, if it has one.
        projection: Option<String>,
    },
}

impl fmt::Display for CaughtUpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed {
                projection: Some(name),
                error,
            } => write!(formatter, "projection {name:?}: {error}"),
            Self::Failed {
                projection: None,
                error,
            } => error.fmt(formatter),
            Self::Stopped {
                projection: Some(name),
            } => write!(
                formatter,
                "the projection {name:?} stopped before it caught up"
            ),
            Self::Stopped { projection: None } => {
                write!(formatter, "the projection stopped before it caught up")
            }
        }
    }
}

impl Error for CaughtUpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Failed { error, .. } => Some(error.as_ref()),
            Self::Stopped { .. } => None,
        }
    }
}

impl Categorized for CaughtUpError {
    fn category(&self) -> Option<Category> {
        match self {
            Self::Failed { error, .. } => error.category(),
            Self::Stopped { .. } => None,
        }
    }
}

/// An attempt of a projection to read again, after reading failed, which
/// includes a stream of events that ended, as the observer set with
/// [`StoreBuilder::reconnect_observer`](crate::StoreBuilder::reconnect_observer)
/// receives it. It may gain fields, so it can not be built outside the kit.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Reconnect {
    /// The name of the projection, or `None` if it has none, in which case
    /// [`subject`](Reconnect::subject) tells it apart.
    pub projection: Option<String>,
    /// The subject the projection reads.
    pub subject: String,
    /// Why the projection reads again, such as a stream that broke off, which
    /// is what a stream that ended is, since the database never ends one on
    /// its own.
    pub error: Arc<ProjectionError>,
    /// How long the projection waits before it reads again.
    pub delay: Duration,
    /// The attempts in a row, starting at one. It grows together with the
    /// delay, and starts over when the delay does.
    pub attempt: usize,
}
