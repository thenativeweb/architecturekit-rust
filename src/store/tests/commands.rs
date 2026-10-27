//! The command of the tests, and what they need around it, as the test domain
//! of ArchitectureKit for Go has it: a command that increments the counter of
//! its subject, deciders for it, and ways to read and write past the kit.

use std::{
    error::Error,
    future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use eventsourcingdb::EventCandidate;
use futures_util::StreamExt;
use serde_json::Value;

use super::{Annotated, Counter, CounterEvent, Incremented, database::Database};
use crate::{Command, Decider, DomainError, Event, EventOn, Precondition, Store};

pub(super) use super::database::SOURCE;

type Declaring = Arc<dyn Fn() -> Vec<Precondition> + Send + Sync>;

/// A command that increments the counter of its subject, up to a limit, if it
/// has one. Incrementing by nothing decides on no events.
///
/// It declares the preconditions it is given, or none but
/// [`Precondition::unconditionally`], so that the tests that are not about
/// preconditions need not say so. A precondition can not be copied, but a
/// command has to be, so it holds a function that makes them.
#[derive(Clone)]
pub(super) struct Increment {
    pub(super) subject: String,
    pub(super) by: i64,
    pub(super) limit: Option<i64>,
    declaring: Declaring,
}

impl Increment {
    pub(super) fn new(subject: &str, by: i64) -> Self {
        Self {
            subject: subject.to_owned(),
            by,
            limit: None,
            declaring: Arc::new(|| vec![Precondition::unconditionally()]),
        }
    }

    /// Returns a copy that refuses to exceed the given total.
    pub(super) fn up_to(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Returns a copy that declares the preconditions the given function
    /// makes, which may be none at all.
    pub(super) fn declaring(
        mut self,
        preconditions: impl Fn() -> Vec<Precondition> + Send + Sync + 'static,
    ) -> Self {
        self.declaring = Arc::new(preconditions);
        self
    }

    /// Returns a copy that only writes if the subject is still empty.
    pub(super) fn pristine(self) -> Self {
        let subject = self.subject.clone();
        self.declaring(move || {
            vec![Precondition::require(
                eventsourcingdb::Precondition::is_subject_pristine(subject.clone(), None),
            )]
        })
    }

    /// Returns a copy that only writes if the subject is on the given event,
    /// which it requires with the revision check of the client SDK, so that
    /// the kit leaves the ID to the database.
    pub(super) fn requiring_event_id(self, event_id: &str) -> Self {
        let subject = self.subject.clone();
        let event_id = event_id.to_owned();
        self.declaring(move || {
            vec![Precondition::require(
                eventsourcingdb::Precondition::is_subject_on_event_id(
                    subject.clone(),
                    event_id.clone(),
                    None,
                ),
            )]
        })
    }

    /// Returns a copy that only writes if nothing has been written since the
    /// state was read.
    pub(super) fn on_state_read(self) -> Self {
        self.declaring(|| vec![Precondition::on_state_read()])
    }
}

impl Command for Increment {
    fn subject(&self) -> String {
        self.subject.clone()
    }

    fn preconditions(&self) -> Vec<Precondition> {
        (self.declaring)()
    }
}

/// Increments the counter by what the command says, and refuses to exceed its
/// limit.
pub(super) fn counter_decider() -> Decider<Increment, Counter, DomainError> {
    Decider::new(|command: Increment, current: Counter| async move {
        if command.by == 0 {
            return Ok(Vec::new());
        }
        if let Some(limit) = command.limit
            && current.total + command.by > limit
        {
            return Err(DomainError::new(format!(
                "limit of {limit} would be exceeded"
            )));
        }

        Ok(vec![CounterEvent::Incremented(Incremented {
            by: command.by,
        })])
    })
}

/// Decides on the events the given function makes, whatever the command and
/// the state.
pub(super) fn emitting(
    events: impl Fn() -> Vec<CounterEvent> + Send + Sync + 'static,
) -> Decider<Increment, Counter, DomainError> {
    Decider::new(move |_: Increment, _: Counter| future::ready(Ok(events())))
}

/// Decides on a remark with the given note, whatever the command and the
/// state, which the counter ignores.
pub(super) fn annotating(note: &str) -> Decider<Increment, Counter, DomainError> {
    let note = note.to_owned();
    emitting(move || vec![CounterEvent::Annotated(Annotated { note: note.clone() })])
}

/// Decides as [`counter_decider`] does, but for its first decisions, as many as
/// `times` says, somebody else writes to the subject while it decides, so that
/// writing what it decided conflicts. It returns the decider and how often it
/// has decided so far.
pub(super) async fn interfering(
    database: &Database,
    times: usize,
) -> (Decider<Increment, Counter, DomainError>, Arc<AtomicUsize>) {
    let client = Arc::new(database.client().await);
    let decide = Arc::new(counter_decider());
    let decisions = Arc::new(AtomicUsize::new(0));

    let counted = Arc::clone(&decisions);
    let decider = Decider::new(move |command: Increment, current: Counter| {
        let client = Arc::clone(&client);
        let decide = Arc::clone(&decide);
        let decision = counted.fetch_add(1, Ordering::SeqCst) + 1;

        async move {
            if decision <= times {
                let _ = client
                    .write_events(
                        vec![candidate(
                            &command.subject,
                            Incremented::EVENT_TYPE,
                            serde_json::json!({ "by": 100 }),
                        )],
                        Vec::new(),
                    )
                    .await
                    .expect("somebody else writes");
            }

            decide.decide(command, current).await
        }
    });

    (decider, decisions)
}

/// The two ways the kit writes, which fail alike.
#[derive(Clone, Copy, Debug)]
pub(super) enum KitWrite {
    Execute,
    Write,
}

impl KitWrite {
    pub(super) const ALL: [Self; 2] = [Self::Execute, Self::Write];

    /// Writes a remark with the given note to the subject, under the
    /// preconditions the given function makes: with execute, and a decider
    /// that decides on the remark whatever the command, or with write.
    pub(super) async fn remark(
        self,
        store: &Store,
        subject: &str,
        note: &str,
        preconditions: impl Fn() -> Vec<Precondition> + Send + Sync + 'static,
    ) -> Result<Vec<eventsourcingdb::Event>, crate::Error<DomainError>> {
        match self {
            Self::Execute => Ok(store
                .execute(
                    &annotating(note),
                    Increment::new(subject, 1).declaring(preconditions),
                )
                .await?),
            Self::Write => Ok(store
                .write(
                    vec![EventOn::new(
                        subject,
                        &Annotated {
                            note: note.to_owned(),
                        },
                    )],
                    preconditions(),
                )
                .await?),
        }
    }

    /// Returns how the message of a failure names the write of a single event
    /// to the subject.
    pub(super) fn doing(self, subject: &str) -> String {
        match self {
            Self::Execute => format!("writing {subject:?}"),
            Self::Write => format!("writing 1 events, the first to {subject:?}"),
        }
    }
}

/// An event to write past the kit.
pub(super) fn candidate(subject: &str, event_type: &str, data: Value) -> EventCandidate {
    EventCandidate {
        data,
        source: SOURCE.to_owned(),
        subject: subject.to_owned(),
        ty: event_type.to_owned(),
        traceinfo: None,
    }
}

/// Writes an event of the given type and data to the subject past the kit, as
/// somebody else would.
pub(super) async fn write_past_the_kit(
    database: &Database,
    subject: &str,
    event_type: &str,
    data: Value,
) {
    let _ = database
        .client()
        .await
        .write_events(vec![candidate(subject, event_type, data)], Vec::new())
        .await
        .expect("the event is written past the kit");
}

/// Returns the total of the counter of the subject.
pub(super) async fn total_in(store: &Store, subject: &str) -> i64 {
    store
        .load::<Counter>(subject)
        .await
        .expect("the counter loads")
        .total
}

/// Returns the types of the events of the subject, read straight from the
/// database, past the kit.
pub(super) async fn event_types_in(database: &Database, subject: &str) -> Vec<String> {
    let client = database.client().await;
    let events = client
        .read_events(subject, None)
        .await
        .expect("the subject can be read");
    let mut events = pin!(events);

    let mut event_types = Vec::new();
    while let Some(event) = events.next().await {
        event_types.push(event.expect("the event can be read").ty().to_owned());
    }

    event_types
}

/// Returns the messages of an error and all of its sources, joined as Go joins
/// them, so that a test can look for what a cause says.
pub(super) fn chain(error: &dyn Error) -> String {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(cause) = source {
        messages.push(cause.to_string());
        source = cause.source();
    }

    messages.join(": ")
}
