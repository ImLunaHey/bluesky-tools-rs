//! Small bounded response cache.

use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

/// Configuration for idempotent query caching.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheConfig {
    /// Maximum stored responses. Zero disables caching.
    pub capacity: usize,
    /// Lifetime of a cached response.
    pub ttl: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 500,
            ttl: Duration::from_secs(30),
        }
    }
}

pub(crate) struct Cache {
    config: CacheConfig,
    values: HashMap<String, (Instant, Value)>,
    order: VecDeque<String>,
}

impl Cache {
    pub(crate) fn new(config: CacheConfig) -> Self {
        Self {
            config,
            values: HashMap::new(),
            order: VecDeque::new(),
        }
    }
    pub(crate) fn get(&mut self, key: &str) -> Option<Value> {
        let (created, value) = self.values.get(key)?;
        if created.elapsed() > self.config.ttl {
            self.values.remove(key);
            return None;
        }
        Some(value.clone())
    }
    pub(crate) fn insert(&mut self, key: String, value: Value) {
        if self.config.capacity == 0 {
            return;
        }
        if !self.values.contains_key(&key) {
            self.order.push_back(key.clone());
        }
        self.values.insert(key, (Instant::now(), value));
        while self.values.len() > self.config.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
    }
    pub(crate) fn clear(&mut self) {
        self.values.clear();
        self.order.clear();
    }
}
