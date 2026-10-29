use std::{error::Error, fmt};

use serde_json::Value;

use crate::{
    Command, Decider, Events, ExecuteError, Precondition, State, Subject, UpcastEvent,
    check_preconditions, replay, replay_stored, store::unencodable,
};

#[cfg(test)]
mod tests;

/// The state a command will be decided on, which [`given`] or
/// [`given_stored`] builds.
pub struct Fixture<'decider, C, S: State, D> {
    decider: &'decider Decider<C, S, D>,
    state: S,
}

impl<C, S: State, D> fmt::Debug for Fixture<'_, C, S, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Fixture")
            .field("decider", self.decider)
            .finish_non_exhaustive()
    }
}

/// Builds the state a command will be decided on from typed events, which is
/// how a test usually spells out a history. It needs the feature `testing`.
///
/// # Panics
///
/// Fails the test if the state can not be built from the events, for example
/// since one of them can not be encoded as JSON, which
/// [`replay`] needs.
#[track_caller]
pub fn given<C, S: State, D>(
    decider: &Decider<C, S, D>,
    history: impl IntoIterator<Item = S::Event>,
) -> Fixture<'_, C, S, D> {
    let state = match replay(history) {
        Ok(state) => state,
        Err(failure) => panic!("given: {}", chain(&failure)),
    };

    Fixture { decider, state }
}

/// Builds the state a command will be decided on from events in their stored
/// shape, running the upcasters of the state on the way. Use it to test that an
/// older event type still arrives correctly, which typed events can not show.
/// It needs the feature `testing`.
///
/// # Panics
///
/// Fails the test if the state can not be built from the events, for example
/// since it has no rule for one of them, or an upcaster refuses one.
#[track_caller]
pub fn given_stored<C, S: State, D>(
    decider: &Decider<C, S, D>,
    history: impl IntoIterator<Item = UpcastEvent>,
) -> Fixture<'_, C, S, D> {
    let state = match replay_stored(history) {
        Ok(state) => state,
        Err(failure) => panic!("given stored: {}", chain(&failure)),
    };

    Fixture { decider, state }
}

impl<C: Command, S: State, D> Fixture<'_, C, S, D> {
    /// Lets the decider decide on the command, given the state.
    ///
    /// Like [`Store::execute`](crate::Store::execute), it first checks the
    /// preconditions the command declares (see
    /// [`check_preconditions`]), and the decider
    /// does not decide on a command they refuse. It also refuses the events if
    /// the data of one of them can not be encoded as JSON. The decision then
    /// holds the error that executing the command fails with, so that the
    /// assertions that expect events or nothing fail, naming the cause.
    pub async fn when(self, command: C) -> Decision<S, D> {
        let subject = command.subject();
        let preconditions = command.preconditions();

        let decided = match check_preconditions(&command) {
            Ok(()) => self
                .decider
                .decide(command, self.state.clone())
                .await
                .map_err(ExecuteError::Decider),
            Err(refusal) => Err(refusal.into()),
        };

        Decision {
            state: self.state,
            preconditions,
            result: decided.and_then(|events| encode(&subject, events)),
        }
    }
}

/// The events and their data, as executing the command would write them.
struct Decided<E> {
    events: Vec<E>,
    data: Vec<Value>,
}

/// Encodes the data of every event as JSON, as executing does, and refuses all
/// of them with the same error if the data of one of them can not be encoded.
fn encode<E: Events, D>(subject: &Subject, events: Vec<E>) -> Result<Decided<E>, ExecuteError<D>> {
    let data = events
        .iter()
        .map(|event| {
            event
                .to_data()
                .map_err(|cause| unencodable(event.event_type(), subject.as_str(), cause))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Decided { events, data })
}

/// The decision of the decider on the command: events, nothing, or a refusal,
/// which [`Fixture::when`] returns. Every assertion returns the decision again,
/// so that they can be chained, and fails the test if it does not hold.
pub struct Decision<S: State, D> {
    state: S,
    preconditions: Vec<Precondition>,
    result: Result<Decided<S::Event>, ExecuteError<D>>,
}

impl<S: State, D: fmt::Debug> fmt::Debug for Decision<S, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let result = match &self.result {
            Ok(decided) => describe(&decided.events),
            Err(failure) => describe_failure(failure),
        };

        formatter
            .debug_struct("Decision")
            .field("preconditions", &self.preconditions)
            .field("result", &result)
            .finish_non_exhaustive()
    }
}

impl<S: State, D: fmt::Debug> Decision<S, D> {
    /// Returns the events the decider decided on, or the error the command
    /// failed with, for what the assertions do not check, such as the category
    /// of an error, or a refusal of the kit, as in
    /// `assert!(matches!(decision.result(), Err(ExecuteError::Permanent(_))))`.
    ///
    /// # Errors
    ///
    /// Returns the error the command failed with, as executing it would.
    pub fn result(&self) -> Result<&[S::Event], &ExecuteError<D>> {
        self.result
            .as_ref()
            .map(|decided| decided.events.as_slice())
    }

    /// Expects exactly these events, in this order. Two events are the same if
    /// they have the same type and their data is the same JSON, which is what
    /// executing the command would write.
    ///
    /// # Panics
    ///
    /// Fails the test if the events differ, if the command failed, or if one
    /// of the expected events can not be encoded as JSON.
    #[track_caller]
    pub fn then_events(&self, expected: impl IntoIterator<Item = S::Event>) -> &Self {
        let decided = self.decided("expected events");
        let expected: Vec<S::Event> = expected.into_iter().collect();

        assert!(
            decided.events.len() == expected.len(),
            "expected {} event(s), got {}: {}",
            expected.len(),
            decided.events.len(),
            describe(&decided.events),
        );

        for (index, (wanted, (got, got_data))) in expected
            .iter()
            .zip(decided.events.iter().zip(&decided.data))
            .enumerate()
        {
            assert!(
                got.event_type() == wanted.event_type(),
                "event {index}: got type {:?}, want {:?}",
                got.event_type(),
                wanted.event_type(),
            );

            let wanted_data = match wanted.to_data() {
                Ok(data) => data,
                Err(cause) => panic!("event {index}: {cause}"),
            };
            assert!(
                *got_data == wanted_data,
                "event {index}: got {got_data}, want {wanted_data}"
            );
        }

        self
    }

    /// Expects neither events nor a failure, which is what a command does when
    /// it finds there is nothing left to do.
    ///
    /// # Panics
    ///
    /// Fails the test if there are events, or if the command failed.
    #[track_caller]
    pub fn then_nothing(&self) -> &Self {
        let decided = self.decided("expected nothing to happen");

        assert!(
            decided.events.is_empty(),
            "expected nothing to happen, got {}",
            describe(&decided.events)
        );

        self
    }

    /// Expects the decider to have refused the command with the given error.
    /// For anything else about a failure, such as its category, or a refusal
    /// of the kit, use [`result`](Decision::result).
    ///
    /// # Panics
    ///
    /// Fails the test if the decider refused the command with another error,
    /// if the kit refused it, or if the command did not fail.
    #[track_caller]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "a test hands over the error it expects as a value, as the decider returns it"
    )]
    pub fn then_failed(&self, expected: D) -> &Self
    where
        D: PartialEq,
    {
        match &self.result {
            Err(ExecuteError::Decider(refusal)) if *refusal == expected => {}
            Err(failure) => panic!(
                "expected an error matching {expected:?}, got {}",
                describe_failure(failure)
            ),
            Ok(decided) => panic!(
                "expected an error matching {expected:?}, got {}",
                describe(&decided.events)
            ),
        }

        self
    }

    /// Expects at least one event to match.
    ///
    /// # Panics
    ///
    /// Fails the test if no event matches, or if the command failed.
    #[track_caller]
    pub fn then_some_event(&self, matches: impl Fn(&S::Event) -> bool) -> &Self {
        let decided = self.decided("expected events");

        assert!(
            decided.events.iter().any(matches),
            "no event matched, got {}",
            describe(&decided.events)
        );

        self
    }

    /// Expects all events to match, and at least one to be there.
    ///
    /// # Panics
    ///
    /// Fails the test if there are no events, if one of them does not match,
    /// or if the command failed.
    #[track_caller]
    pub fn then_every_event(&self, matches: impl Fn(&S::Event) -> bool) -> &Self {
        let decided = self.decided("expected events");

        assert!(!decided.events.is_empty(), "expected events, got none");
        if let Some(index) = decided.events.iter().position(|event| !matches(event)) {
            panic!("event {index} did not match: {}", describe(&decided.events));
        }

        self
    }

    /// Expects no event to match, which is how a test says that a command did
    /// not do something in particular.
    ///
    /// # Panics
    ///
    /// Fails the test if an event matches, or if the command failed.
    #[track_caller]
    pub fn then_no_event(&self, matches: impl Fn(&S::Event) -> bool) -> &Self {
        let decided = self.decided("expected events");

        if let Some(index) = decided.events.iter().position(matches) {
            panic!(
                "event {index} matched although it should not: {}",
                describe(&decided.events)
            );
        }

        self
    }

    /// Checks the state the command was decided on, which is the state the
    /// fixture built. That makes it useful for testing upcasters.
    pub fn then_state(&self, check: impl FnOnce(&S)) -> &Self {
        check(&self.state);
        self
    }

    /// Expects the command to declare exactly these preconditions, in this
    /// order, as [`Precondition`] compares them.
    ///
    /// # Panics
    ///
    /// Fails the test if the declared preconditions differ.
    #[track_caller]
    pub fn then_preconditions(&self, expected: impl IntoIterator<Item = Precondition>) -> &Self {
        let expected: Vec<Precondition> = expected.into_iter().collect();

        assert!(
            self.preconditions.len() == expected.len(),
            "expected {} precondition(s), got {}: {:?}",
            expected.len(),
            self.preconditions.len(),
            self.preconditions,
        );

        for (index, (got, wanted)) in self.preconditions.iter().zip(&expected).enumerate() {
            assert!(
                got == wanted,
                "precondition {index}: got {got:?}, want {wanted:?}"
            );
        }

        self
    }

    /// Returns the events of the decision, or fails the test, saying what was
    /// expected, if the command failed.
    #[track_caller]
    fn decided(&self, expected: &str) -> &Decided<S::Event> {
        match &self.result {
            Ok(decided) => decided,
            Err(failure) => panic!("{expected}, got error: {}", describe_failure(failure)),
        }
    }
}

/// Describes events by their types, as the messages of the assertions show
/// them.
fn describe<E: Events>(events: &[E]) -> String {
    if events.is_empty() {
        return "no events".to_owned();
    }

    let types: Vec<&str> = events.iter().map(Events::event_type).collect();
    format!("{} event(s): {types:?}", events.len())
}

/// Describes a failure with all of its causes, and the error of the decider as
/// `Debug` shows it, since the decider's error need not be shown otherwise.
fn describe_failure<D: fmt::Debug>(failure: &ExecuteError<D>) -> String {
    match failure {
        ExecuteError::Decider(refusal) => format!("{refusal:?}"),
        ExecuteError::Transient(failure) => chain(failure),
        ExecuteError::Permanent(failure) => chain(failure),
        ExecuteError::OutcomeUnknown(failure) => {
            format!("outcome unknown: {}", failure.message())
        }
    }
}

/// Returns the message of an error together with those of its causes.
fn chain(error: &dyn Error) -> String {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(cause) = source {
        messages.push(cause.to_string());
        source = cause.source();
    }

    messages.join(": ")
}
