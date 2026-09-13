//! Cooperative request rate limiter.

use std::time::Duration;
use tokio::{
    sync::{Mutex, Semaphore},
    time::Instant,
};

/// Request concurrency and pacing limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimitConfig {
    /// Maximum requests executing concurrently.
    pub max_concurrent: usize,
    /// Minimum delay between request starts.
    pub min_interval: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 10,
            min_interval: Duration::from_millis(20),
        }
    }
}

pub(crate) struct RateLimiter {
    semaphore: Semaphore,
    next: Mutex<Instant>,
    interval: Duration,
}
impl RateLimiter {
    pub(crate) fn new(config: RateLimitConfig) -> Self {
        Self {
            semaphore: Semaphore::new(config.max_concurrent.max(1)),
            next: Mutex::new(Instant::now()),
            interval: config.min_interval,
        }
    }
    pub(crate) async fn wait(&self) -> tokio::sync::SemaphorePermit<'_> {
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("rate limiter remains open");
        let mut next = self.next.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + self.interval;
        permit
    }
}
