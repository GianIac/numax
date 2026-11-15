mod probe;
pub(crate) mod runtime;
mod schema;
mod state;
mod storage;
mod suspicion;

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Error {
    InvalidConfig,
    ClockRegression,
    Overflow,
    Capacity,
    Unauthorized,
    NotReady,
    Persistence,
    InvalidMetadata,
}

fn deadline(now: Duration, timeout: Duration) -> Result<Duration, Error> {
    now.checked_add(timeout).ok_or(Error::Overflow)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    epoch: u64,
    incarnation: u64,
}

impl Version {
    fn refute(self, accused: Self) -> Result<Self, Error> {
        if self.epoch != accused.epoch {
            return Err(Error::Unauthorized);
        }
        Ok(Self {
            epoch: self.epoch,
            incarnation: self
                .incarnation
                .max(accused.incarnation)
                .checked_add(1)
                .ok_or(Error::Overflow)?,
        })
    }
}
