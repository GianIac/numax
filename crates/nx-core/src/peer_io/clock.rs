use async_trait::async_trait;
use std::time::Instant;

#[async_trait]
pub(crate) trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    async fn sleep_until(&self, deadline: Instant);
}

pub(crate) struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    async fn sleep_until(&self, deadline: Instant) {
        tokio::time::sleep_until(deadline.into()).await;
    }
}
