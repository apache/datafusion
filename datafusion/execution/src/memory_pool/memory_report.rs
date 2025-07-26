use super::{human_readable_size, MemoryReservation};
use crate::memory_pool::pool::TrackConsumersPool;
use datafusion_expr::Accumulator;
use std::any::Any;

/// Helper trait to provide memory usage breakdowns for debugging.
pub trait ExplainMemory {
    /// Returns a human readable string describing memory usage.
    fn explain_memory(&self) -> String;
}

impl ExplainMemory for MemoryReservation {
    fn explain_memory(&self) -> String {
        format!(
            "{}#{} reserved {}",
            self.consumer().name(),
            self.consumer().id(),
            human_readable_size(self.size())
        )
    }
}

impl<T: Accumulator + ?Sized> ExplainMemory for T {
    fn explain_memory(&self) -> String {
        human_readable_size(self.size())
    }
}

/// Try to downcast a pooled type to [`TrackConsumersPool`] and report
/// the largest consumers. Returns `None` if the pool does not track
/// consumers.
pub fn report_top_consumers(
    pool: &(dyn Any + Send + Sync),
    top: usize,
) -> Option<String> {
    let any = pool;
    if let Some(tracked) = any.downcast_ref::<TrackConsumersPool<crate::memory_pool::pool::GreedyMemoryPool>>() {
        Some(tracked.report_top(top))
    } else if let Some(tracked) = any.downcast_ref::<TrackConsumersPool<crate::memory_pool::pool::FairSpillPool>>() {
        Some(tracked.report_top(top))
    } else if let Some(tracked) = any.downcast_ref::<TrackConsumersPool<crate::memory_pool::pool::UnboundedMemoryPool>>() {
        Some(tracked.report_top(top))
    } else {
        None
    }
}
