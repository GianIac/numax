use std::collections::BTreeSet;
use std::time::Duration;

use super::{Error, Version, deadline};

#[derive(Debug, Clone, Copy)]
pub(super) struct Timing {
    pub(super) interval: Duration,
    pub(super) direct: Duration,
    pub(super) suspicion_min: Duration,
    pub(super) suspicion_max: Duration,
    pub(super) confirmations: usize,
    pub(super) health_max: u32,
}

impl Timing {
    pub(super) fn validate(self) -> Result<Self, Error> {
        if self.interval.is_zero()
            || self.direct.is_zero()
            || self.direct >= self.interval
            || self.suspicion_min.is_zero()
            || self.suspicion_min > self.suspicion_max
            || self.confirmations > 64
            || self.health_max > 1024
            || self.interval.checked_mul(self.health_max + 1).is_none()
        {
            return Err(Error::InvalidConfig);
        }
        Ok(self)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Health(u32);

impl Health {
    pub(super) fn score(self) -> u32 {
        self.0
    }

    pub(super) fn success(&mut self) {
        self.0 = self.0.saturating_sub(1);
    }

    pub(super) fn failure(&mut self, missing_nack: bool, maximum: u32) {
        self.0 = self
            .0
            .saturating_add(1 + u32::from(missing_nack))
            .min(maximum);
    }

    pub(super) fn self_refutation(&mut self, maximum: u32) {
        self.0 = self.0.saturating_add(1).min(maximum);
    }

    pub(super) fn scale(self, duration: Duration) -> Result<Duration, Error> {
        duration
            .checked_mul(self.0.checked_add(1).ok_or(Error::Overflow)?)
            .ok_or(Error::Overflow)
    }
}

#[derive(Debug, Clone)]
pub(super) struct Suspicion {
    pub(super) version: Version,
    pub(super) timer: u64,
    pub(super) deadline: Duration,
    started: Duration,
    last_time: Duration,
    minimum: Duration,
    maximum: Duration,
    threshold: usize,
    initial_reporter: String,
    reporters: BTreeSet<String>,
}

impl Suspicion {
    pub(super) fn new(
        version: Version,
        timer: u64,
        reporter: String,
        now: Duration,
        timing: Timing,
        available_reporters: usize,
    ) -> Result<Self, Error> {
        timing.validate()?;
        let threshold = timing.confirmations.min(available_reporters);
        let duration = if threshold == 0 {
            timing.suspicion_min
        } else {
            timing.suspicion_max
        };
        Ok(Self {
            version,
            timer,
            deadline: deadline(now, duration)?,
            started: now,
            last_time: now,
            minimum: timing.suspicion_min,
            maximum: timing.suspicion_max,
            threshold,
            initial_reporter: reporter,
            reporters: BTreeSet::new(),
        })
    }

    pub(super) fn confirm(&mut self, reporter: &str, now: Duration) -> Result<bool, Error> {
        if now < self.last_time {
            return Err(Error::ClockRegression);
        }
        self.last_time = now;
        if now >= self.deadline
            || reporter == self.initial_reporter
            || self.reporters.contains(reporter)
            || self.reporters.len() >= self.threshold
        {
            return Ok(false);
        }
        let count = self.reporters.len() + 1;
        let duration = if count == self.threshold {
            self.minimum
        } else {
            let fraction = ((count + 1) as f64).ln() / ((self.threshold + 1) as f64).ln();
            let reduction = self.maximum.saturating_sub(self.minimum).as_secs_f64() * fraction;
            Duration::try_from_secs_f64(self.maximum.as_secs_f64() - reduction)
                .map_err(|_| Error::Overflow)?
                .max(self.minimum)
        };
        let shortened = deadline(self.started, duration)?;
        self.reporters.insert(reporter.to_owned());
        self.deadline = self.deadline.min(shortened);
        Ok(true)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(in crate::membership) fn timing() -> Timing {
        Timing {
            interval: Duration::from_secs(1),
            direct: Duration::from_millis(200),
            suspicion_min: Duration::from_secs(2),
            suspicion_max: Duration::from_secs(6),
            confirmations: 3,
            health_max: 8,
        }
    }

    #[test]
    fn confirmations_shorten_from_original_start_without_duplicate_votes() {
        let mut suspicion = Suspicion::new(
            Version {
                epoch: 7,
                incarnation: 1,
            },
            1,
            "first".into(),
            Duration::ZERO,
            timing(),
            3,
        )
        .unwrap();
        assert_eq!(suspicion.deadline, Duration::from_secs(6));
        assert!(!suspicion.confirm("first", Duration::ZERO).unwrap());
        assert!(suspicion.confirm("second", Duration::from_secs(1)).unwrap());
        assert_eq!(suspicion.deadline, Duration::from_secs(4));
        assert!(!suspicion.confirm("second", Duration::from_secs(2)).unwrap());
        assert_eq!(suspicion.deadline, Duration::from_secs(4));
        assert!(
            suspicion
                .confirm("third", Duration::from_millis(2100))
                .unwrap()
        );
        assert!((suspicion.deadline.as_secs_f64() - 2.830074999).abs() < 0.000001);
        assert!(
            suspicion
                .confirm("fourth", Duration::from_millis(2200))
                .unwrap()
        );
        assert_eq!(suspicion.deadline, Duration::from_secs(2));
        assert!(
            !suspicion
                .confirm("fifth", Duration::from_millis(2300))
                .unwrap()
        );
    }

    #[test]
    fn small_clusters_clamp_confirmations_and_equal_deadline_expires_first() {
        for available in 0..=3 {
            let mut suspicion = Suspicion::new(
                Version {
                    epoch: 1,
                    incarnation: 0,
                },
                1,
                "first".into(),
                Duration::ZERO,
                timing(),
                available,
            )
            .unwrap();
            assert_eq!(suspicion.threshold, available);
            let end = suspicion.deadline;
            assert!(!suspicion.confirm("second", end).unwrap());
            assert_eq!(suspicion.deadline, end);
        }
    }

    #[test]
    fn health_saturates_recovers_and_combines_failed_probe_with_missing_nack() {
        let mut health = Health::default();
        health.success();
        assert_eq!(health.score(), 0);
        health.failure(true, 8);
        assert_eq!(health.score(), 2);
        health.self_refutation(8);
        assert_eq!(
            health.scale(Duration::from_secs(1)).unwrap(),
            Duration::from_secs(4)
        );
        for _ in 0..20 {
            health.failure(false, 8);
        }
        assert_eq!(health.score(), 8);
        for _ in 0..20 {
            health.success();
        }
        assert_eq!(health.score(), 0);
    }

    #[test]
    fn invalid_timing_clock_regression_and_overflow_are_rejected() {
        let mut invalid = timing();
        invalid.direct = invalid.interval;
        assert!(matches!(invalid.validate(), Err(Error::InvalidConfig)));
        assert!(matches!(
            Suspicion::new(
                Version {
                    epoch: 1,
                    incarnation: 0
                },
                1,
                "first".into(),
                Duration::MAX,
                timing(),
                3
            ),
            Err(Error::Overflow)
        ));
        let mut suspicion = Suspicion::new(
            Version {
                epoch: 1,
                incarnation: 0,
            },
            1,
            "first".into(),
            Duration::from_secs(2),
            timing(),
            3,
        )
        .unwrap();
        assert_eq!(
            suspicion.confirm("second", Duration::ZERO),
            Err(Error::ClockRegression)
        );
        assert_eq!(suspicion.deadline, Duration::from_secs(8));
    }
}
