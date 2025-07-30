use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

#[derive(Default)]
pub struct MemoryMetrics {
    entries: HashMap<String, usize>,
}

impl MemoryMetrics {
    pub fn record(&mut self, operator: &str, bytes: usize) {
        *self.entries.entry(operator.to_string()).or_insert(0) += bytes;
    }

    pub fn snapshot(&self) -> HashMap<String, usize> {
        self.entries.clone()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

pub struct LightweightMemoryTracker {
    enabled: AtomicBool,
    metrics: Arc<Mutex<MemoryMetrics>>,
}

impl LightweightMemoryTracker {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            metrics: Arc::new(Mutex::new(MemoryMetrics::default())),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
        self.metrics.lock().unwrap().clear();
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    pub fn record_memory(&self, operator: &str, bytes: usize) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        self.metrics.lock().unwrap().record(operator, bytes);
    }

    pub fn metrics(&self) -> HashMap<String, usize> {
        self.metrics.lock().unwrap().snapshot()
    }

    pub fn reset(&self) {
        self.metrics.lock().unwrap().clear();
    }
}
