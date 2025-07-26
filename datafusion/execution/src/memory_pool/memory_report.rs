use super::{human_readable_size, MemoryReservation};
use crate::memory_pool::pool::{
    FairSpillPool, GreedyMemoryPool, TrackConsumersPool, UnboundedMemoryPool,
};
use crate::memory_pool::MemoryPool;
use datafusion_common::Result;
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
/// println!("{}", reservation.explain_memory().unwrap());
/// ```
pub trait ExplainMemory {
    /// Returns a human readable string describing memory usage.
    fn explain_memory(&self) -> Result<String>;
}

impl ExplainMemory for MemoryReservation {
    fn explain_memory(&self) -> Result<String> {
        Ok(format!(
            "{}#{} reserved {}",
            self.consumer().name(),
            self.consumer().id(),
            human_readable_size(self.size())
        ))
    }
}

impl<T: Accumulator + ?Sized> ExplainMemory for T {
    fn explain_memory(&self) -> Result<String> {
        // `Accumulator` requires implementers to provide `size()` which
        // we leverage here to report memory usage.
        Ok(human_readable_size(self.size()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::MemoryConsumer;
    use arrow::array::ArrayRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Accumulator;
    use std::sync::Arc;

    #[test]
    fn reservation_explain() -> Result<()> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64));
        let mut r = MemoryConsumer::new("test").register(&pool);
        r.try_grow(10)?;
        let expected = format!(
            "test#{} reserved {}",
            r.consumer().id(),
            human_readable_size(10)
        );
        assert_eq!(r.explain_memory()?, expected);
        Ok(())
    }

    #[derive(Debug)]
    struct DummyAcc(usize);

    impl Accumulator for DummyAcc {
        fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }
        fn evaluate(&mut self) -> Result<ScalarValue> {
            Ok(ScalarValue::UInt64(Some(self.0 as u64)))
        }
        fn size(&self) -> usize {
            self.0
        }
        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            Ok(vec![])
        }
        fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn accumulator_explain() -> Result<()> {
        let acc = DummyAcc(42);
        assert_eq!(acc.explain_memory()?, human_readable_size(42));
        Ok(())
    }
}
