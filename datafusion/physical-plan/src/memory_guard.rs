use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_common::DataFusionError;

#[derive(Debug)]
pub struct OperatorMemoryGuard {
    consumer: MemoryConsumer,
    used: AtomicUsize,
}

impl OperatorMemoryGuard {
    pub fn new(name: &str, pool: &Arc<dyn MemoryPool>) -> Self {
        let consumer = MemoryConsumer::new(name).register(pool);
        Self { consumer, used: AtomicUsize::new(0) }
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<MemoryReservation, DataFusionError> {
        if bytes == 0 { return self.consumer.try_reserve(0).map_err(|e| DataFusionError::ResourcesExhausted(e.to_string())); }
        let current = self.used.load(Ordering::Relaxed);
        let next = current.saturating_add(bytes);
        let reservation = self.consumer.try_reserve(bytes).map_err(|e| DataFusionError::ResourcesExhausted(format!("operator memory budget exceeded: {e}")))?;
        self.used.store(next, Ordering::Relaxed);
        Ok(reservation)
    }
}
