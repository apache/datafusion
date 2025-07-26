use super::{human_readable_size, MemoryReservation};
use crate::memory_pool::pool::{
    FairSpillPool, GreedyMemoryPool, TrackConsumersPool, UnboundedMemoryPool,
};
use crate::memory_pool::MemoryPool;
use datafusion_expr::Accumulator;
use std::any::Any;

/// Helper trait to provide memory usage breakdowns for debugging.
///
/// Implemented for [`MemoryReservation`] and any [`Accumulator`] via a blanket
/// implementation that relies on [`Accumulator::size`].
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use datafusion_execution::memory_pool::{ExplainMemory, GreedyMemoryPool, MemoryConsumer};
/// let pool = Arc::new(GreedyMemoryPool::new(1024));
/// let mut reservation = MemoryConsumer::new("example").register(&pool);
/// reservation.try_grow(256).unwrap();
/// println!("{}", reservation.explain_memory());
/// ```
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
        // `Accumulator` requires implementers to provide `size()` which
        // we leverage here to report memory usage.
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
    fn try_report<I: MemoryPool + 'static>(
        pool: &(dyn Any + Send + Sync),
        top: usize,
    ) -> Option<String> {
        pool.downcast_ref::<TrackConsumersPool<I>>()
            .map(|tracked| tracked.report_top(top))
    }

    try_report::<GreedyMemoryPool>(pool, top)
        .or_else(|| try_report::<FairSpillPool>(pool, top))
        .or_else(|| try_report::<UnboundedMemoryPool>(pool, top))
}
