use std::collections::HashSet;

use bytes::Bytes;
use http::{Response, StatusCode};
use serde_json::{Map, Value, json};

use super::answer::json;
use crate::{Phase, ProjectionRun, ProjectionStatus};

#[cfg(test)]
mod tests;

/// Answers whether the projections of a service are ready, or alive, for the
/// probes of an orchestrator such as Kubernetes. With the feature `axum`, it
/// is a handler:
///
/// ```
/// # #[cfg(feature = "axum")]
/// # mod example {
/// # use axum::{Router, routing::get};
/// # use architecturekit::ProjectionRun;
/// use architecturekit::httpapi::Health;
///
/// # fn routes(book_list: ProjectionRun) -> Router {
/// Router::new()
///     .route("/health/ready", get(Health::readiness([book_list.clone()])))
///     .route("/health/alive", get(Health::liveness([book_list])))
/// # }
/// # }
/// ```
///
/// It answers with 200 if the service is healthy, and 503 otherwise, with
/// JSON that tells where each projection stands, but not why it failed, and
/// that is never cached:
///
/// ```json
/// {"isReady":true,"projections":{"book list":{"attempts":0,"hasCaughtUp":true,"phase":"live","revision":"42","since":"2026-10-28T12:00:00Z"}}}
/// ```
///
/// It watches the runs it was given, so every answer tells where they stand
/// at that moment.
#[derive(Clone, Debug)]
pub struct Health {
    runs: Vec<(String, ProjectionRun)>,
    verdict: Verdict,
}

/// What a probe asks.
#[derive(Clone, Copy, Debug)]
enum Verdict {
    Ready,
    Alive,
}

impl Health {
    /// Is ready once every projection has caught up, and as long as none has
    /// stopped, which a probe of readiness asks, so that a service takes
    /// requests only once its views hold what is stored. A projection that
    /// reads again after it has caught up stays ready.
    ///
    /// # Panics
    ///
    /// Panics for a run without a name, and for two runs of the same name,
    /// since the answer tells them apart by their names.
    pub fn readiness(runs: impl IntoIterator<Item = ProjectionRun>) -> Self {
        Self::new(runs, Verdict::Ready)
    }

    /// Is alive as long as no projection has stopped, which a probe of
    /// liveness asks, so that an orchestrator restarts a service whose
    /// projection ended with a failure that trying again will not fix.
    ///
    /// # Panics
    ///
    /// Panics as [`readiness`](Health::readiness) does.
    pub fn liveness(runs: impl IntoIterator<Item = ProjectionRun>) -> Self {
        Self::new(runs, Verdict::Alive)
    }

    fn new(runs: impl IntoIterator<Item = ProjectionRun>, verdict: Verdict) -> Self {
        let mut names = HashSet::new();
        let runs = runs
            .into_iter()
            .enumerate()
            .map(|(position, run)| {
                let Some(name) = run.name().map(ToOwned::to_owned) else {
                    panic!(
                        "architecturekit: projection {position} has no name, give it one with ProjectionOptions"
                    );
                };
                assert!(
                    names.insert(name.clone()),
                    "architecturekit: two projections are named {name:?}"
                );

                (name, run)
            })
            .collect();

        Self { runs, verdict }
    }

    /// Answers where the projections stand now.
    #[must_use]
    pub fn answer(&self) -> Response<Bytes> {
        let mut is_healthy = true;
        let mut projections = Map::new();

        for (name, run) in &self.runs {
            let status = run.status();
            is_healthy = is_healthy && self.judge(&status);
            let _ = projections.insert(name.clone(), describe(&status));
        }

        let key = match self.verdict {
            Verdict::Ready => "isReady",
            Verdict::Alive => "isAlive",
        };
        let status = if is_healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };

        json(
            status,
            &json!({ key: is_healthy, "projections": projections }),
        )
    }

    fn judge(&self, status: &ProjectionStatus) -> bool {
        let has_stopped = status.phase == Phase::Stopped;

        match self.verdict {
            Verdict::Ready => status.has_caught_up && !has_stopped,
            Verdict::Alive => !has_stopped,
        }
    }
}

/// Tells where a projection stands, but not why it failed, which may name
/// internals.
fn describe(status: &ProjectionStatus) -> Value {
    let phase = match status.phase {
        Phase::CatchingUp => "catching-up",
        Phase::Live => "live",
        Phase::Reconnecting => "reconnecting",
        Phase::Stopped => "stopped",
    };

    json!({
        "phase": phase,
        "since": status.since,
        "hasCaughtUp": status.has_caught_up,
        "attempts": status.attempts,
        "revision": status.revision.map(|revision| revision.to_string()).unwrap_or_default(),
    })
}

#[cfg(feature = "axum")]
impl<S: Send + Sync + 'static> axum::handler::Handler<(), S> for Health {
    type Future = std::future::Ready<axum::response::Response>;

    fn call(self, _: axum::extract::Request, _: S) -> Self::Future {
        std::future::ready(self.answer().map(axum::body::Body::from))
    }
}
