// These tests mirror TestHealth and TestHealthVerdicts of ArchitectureKit for
// Go. They put the runs into their phases themselves, rather than running
// projections against a database, since how a run gets into a phase is part
// of the tests of the projections. What does not apply: a projection without a
// run, which Rust does not allow, and changing the given runs afterwards,
// which Rust does not allow either, since Health owns them.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::DateTime;
use http::{StatusCode, header};
use serde_json::{Value, json};

use super::{Health, time_of};
use crate::{Phase, ProjectionError, ProjectionOptions, ProjectionRun, ProjectionStatus};

fn run(name: &str) -> ProjectionRun {
    ProjectionRun::new(ProjectionOptions {
        name: Some(name.to_owned()),
    })
}

fn live(name: &str) -> ProjectionRun {
    let run = run(name);
    run.caught_up_now();

    run
}

fn stopped(name: &str) -> ProjectionRun {
    let run = live(name);
    run.stop(None);

    run
}

/// A failure whose text names an internal address, as the failure to reach a
/// database does.
fn unreachable() -> Arc<ProjectionError> {
    Arc::new(ProjectionError::Panicked {
        message: "connection to 127.0.0.1:3000 refused".to_owned(),
    })
}

/// Asks the given health, and returns the status and the body of its answer.
fn ask(health: &Health) -> (StatusCode, Value) {
    let answer = health.answer();
    let body = serde_json::from_slice(answer.body()).expect("the answer is JSON");

    (answer.status(), body)
}

#[test]
fn is_not_ready_while_a_projection_has_never_caught_up_but_alive() {
    let catching_up = run("catalog");
    let reconnecting = run("loans");
    reconnecting.disrupted(unreachable());

    for run in [catching_up, reconnecting] {
        let (status, body) = ask(&Health::readiness([run.clone()]));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["isReady"], false);

        let (status, body) = ask(&Health::liveness([run]));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["isAlive"], true);
    }
}

#[test]
fn is_ready_and_alive_once_every_projection_is_live() {
    let runs = [live("catalog"), live("loans")];

    let (status, body) = ask(&Health::readiness(runs.clone()));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["isReady"], true);
    assert_eq!(body["projections"]["catalog"]["phase"], "live");
    assert_eq!(body["projections"]["catalog"]["hasCaughtUp"], true);

    let (status, body) = ask(&Health::liveness(runs));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["isAlive"], true);
}

#[test]
fn stays_ready_and_alive_while_a_projection_reconnects_after_it_has_caught_up() {
    let run = live("catalog");
    run.disrupted(unreachable());

    let (status, body) = ask(&Health::readiness([run.clone()]));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["isReady"], true);
    assert_eq!(body["projections"]["catalog"]["phase"], "reconnecting");
    assert_eq!(body["projections"]["catalog"]["attempts"], 1);

    let (status, _) = ask(&Health::liveness([run]));
    assert_eq!(status, StatusCode::OK);
}

#[test]
fn is_neither_ready_nor_alive_once_a_projection_has_stopped() {
    let ended = stopped("catalog");
    let failed = live("loans");
    failed.stop(Some(ProjectionError::Panicked {
        message: "the checkpoint is broken".to_owned(),
    }));

    for run in [ended, failed] {
        let (status, body) = ask(&Health::readiness([run.clone()]));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["isReady"], false);

        let (status, body) = ask(&Health::liveness([run]));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["isAlive"], false);
    }
}

#[test]
fn judges_by_every_projection() {
    // The stopped run comes first as well as last, so that judging by the
    // last run alone does not pass.
    let (catalog, loans) = (live("catalog"), stopped("loans"));

    for runs in [
        [catalog.clone(), loans.clone()],
        [loans.clone(), catalog.clone()],
    ] {
        let (status, body) = ask(&Health::readiness(runs.clone()));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["projections"]["catalog"]["phase"], "live");
        assert_eq!(body["projections"]["loans"]["phase"], "stopped");

        let (status, _) = ask(&Health::liveness(runs));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[test]
fn is_ready_and_alive_without_any_projection() {
    let (status, body) = ask(&Health::readiness([]));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "isReady": true, "projections": {} }));

    let (status, body) = ask(&Health::liveness([]));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "isAlive": true, "projections": {} }));
}

#[test]
fn answers_with_json_that_must_not_be_cached() {
    let answer = Health::readiness([]).answer();

    assert_eq!(answer.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(answer.headers()[header::CACHE_CONTROL], "no-store");
}

#[test]
fn tells_where_each_projection_stands_but_not_why() {
    let run = live("catalog");
    run.committed("42");
    run.disrupted(unreachable());
    let since = run.status().since;

    let answer = Health::liveness([run]).answer();
    let body: Value = serde_json::from_slice(answer.body()).expect("the answer is JSON");

    assert_eq!(
        body["projections"]["catalog"],
        json!({
            "phase": "reconnecting",
            "since": time_of(since),
            "hasCaughtUp": true,
            "attempts": 1,
            "revision": "42",
        })
    );
    assert!(
        !String::from_utf8_lossy(answer.body()).contains("127.0.0.1"),
        "the reason may name internal addresses"
    );
}

#[test]
fn tells_a_projection_that_has_committed_nothing_by_an_empty_revision() {
    let (_, body) = ask(&Health::readiness([run("catalog")]));

    assert_eq!(body["projections"]["catalog"]["phase"], "catching-up");
    assert_eq!(body["projections"]["catalog"]["revision"], "");
}

#[test]
fn writes_times_as_rfc_3339_does() {
    let time = UNIX_EPOCH + Duration::new(1_793_188_800, 500_000_000);

    assert_eq!(
        json!(time_of(time)),
        json!("2026-10-28T12:00:00.500Z"),
        "a time is written in UTC"
    );
    assert_eq!(
        time_of(UNIX_EPOCH - Duration::from_secs(1)),
        DateTime::UNIX_EPOCH,
        "a time before 1970 is written as 1970"
    );
}

#[test]
fn keeps_watching_the_projections_it_was_given() {
    let run = run("catalog");
    let health = Health::readiness([run.clone()]);
    assert_eq!(ask(&health).0, StatusCode::SERVICE_UNAVAILABLE);

    run.caught_up_now();
    assert_eq!(ask(&health).0, StatusCode::OK);

    run.stop(None);
    assert_eq!(ask(&health.clone()).0, StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
#[should_panic(
    expected = "architecturekit: projection 1 has no name, give it one with ProjectionOptions"
)]
fn panics_for_a_projection_without_a_name() {
    let _ = Health::liveness([
        run("catalog"),
        ProjectionRun::new(ProjectionOptions::default()),
    ]);
}

#[test]
#[should_panic(expected = r#"architecturekit: two projections are named "catalog""#)]
fn panics_for_two_projections_of_the_same_name() {
    let _ = Health::readiness([run("catalog"), run("catalog")]);
}

#[test]
fn judges_every_phase() {
    let verdicts = [
        (
            "catching up for the first time",
            Phase::CatchingUp,
            false,
            false,
            true,
        ),
        (
            "reconnecting before it has ever caught up",
            Phase::Reconnecting,
            false,
            false,
            true,
        ),
        ("live", Phase::Live, true, true, true),
        (
            "reconnecting after it has caught up",
            Phase::Reconnecting,
            true,
            true,
            true,
        ),
        (
            "stopped before it has ever caught up",
            Phase::Stopped,
            false,
            false,
            false,
        ),
        (
            "stopped after it has caught up",
            Phase::Stopped,
            true,
            false,
            false,
        ),
    ];

    for (name, phase, has_caught_up, is_ready, is_alive) in verdicts {
        let status = ProjectionStatus {
            phase,
            since: SystemTime::now(),
            error: None,
            attempts: 0,
            revision: None,
            has_caught_up,
        };

        assert_eq!(
            Health::readiness([]).judge(&status),
            is_ready,
            "ready: {name}"
        );
        assert_eq!(
            Health::liveness([]).judge(&status),
            is_alive,
            "alive: {name}"
        );
    }
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn answers_as_a_handler_of_axum() {
    use axum::{Router, body::Body, extract::Request, routing::get};
    use tower::ServiceExt;

    let router = Router::new()
        .route("/health/ready", get(Health::readiness([run("catalog")])))
        .route("/health/alive", get(Health::liveness([run("catalog")])));
    let ask = |path: &str| {
        let request = Request::get(path)
            .body(Body::empty())
            .expect("the request is valid");
        router.clone().oneshot(request)
    };

    let ready = ask("/health/ready").await.expect("the router answers");
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(ready.into_body(), usize::MAX)
        .await
        .expect("the body can be read");
    let body: Value = serde_json::from_slice(&body).expect("the answer is JSON");
    assert_eq!(body["isReady"], false);

    let alive = ask("/health/alive").await.expect("the router answers");
    assert_eq!(alive.status(), StatusCode::OK);
}
