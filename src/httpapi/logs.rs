//! Captures what the kit logs while a test runs, so that a test can check
//! that a failure is logged, and how, or that it is not. The logger keeps the
//! records of every thread to that thread, so tests that run side by side do
//! not see each other's records.

use std::{cell::RefCell, sync::Once};

use log::{Level, LevelFilter, Log, Metadata, Record};

thread_local! {
    static RECORDS: RefCell<Vec<(Level, String)>> = const { RefCell::new(Vec::new()) };
}

struct Capture;

impl Log for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("architecturekit")
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            RECORDS.with(|records| {
                records
                    .borrow_mut()
                    .push((record.level(), record.args().to_string()));
            });
        }
    }

    fn flush(&self) {}
}

static CAPTURE: Capture = Capture;
static INSTALL: Once = Once::new();

/// Runs the function, and returns its result with what the kit logged on
/// this thread meanwhile, as its level and its message.
pub(crate) fn captured<R>(run: impl FnOnce() -> R) -> (R, Vec<(Level, String)>) {
    INSTALL.call_once(|| {
        log::set_logger(&CAPTURE).expect("no other logger is set in the tests");
        log::set_max_level(LevelFilter::Trace);
    });

    RECORDS.with(|records| records.borrow_mut().clear());
    let result = run();
    let records = RECORDS.with(RefCell::take);

    (result, records)
}
