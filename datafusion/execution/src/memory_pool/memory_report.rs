// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(feature = "explain_memory")]
use super::{human_readable_size, MemoryReservation};
use crate::memory_pool::pool::{
    FairSpillPool, GreedyMemoryPool, TrackConsumersPool, UnboundedMemoryPool,
};
use crate::memory_pool::MemoryPool;
use datafusion_common::Result;
use std::any::Any;

/// Helper trait to provide memory usage breakdowns for debugging.
///
/// Implemented for [`MemoryReservation`] and any additional types
/// that need to describe their memory usage.
///
/// # Example
/// ```/// # use std::sync::Arc;
/// # use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
/// # use datafusion_execution::memory_pool::memory_report::ExplainMemory;
/// let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
/// let mut reservation = MemoryConsumer::new("example").register(&pool);
/// reservation.try_grow(256).unwrap();
/// println!("{}", reservation.explain_memory().unwrap());
/// ```
pub trait ExplainMemory {
    /// Returns a human readable string describing memory usage.
    fn explain_memory(&self) -> Result<String>;

    /// Returns the size in bytes this type accounts for
    fn memory_size(&self) -> usize;
}

#[cfg(feature = "explain_memory")]
impl ExplainMemory for MemoryReservation {
    fn explain_memory(&self) -> Result<String> {
        Ok(format!(
            "{}#{} reserved {}",
            self.consumer().name(),
            self.consumer().id(),
            human_readable_size(self.size())
        ))
    }

    fn memory_size(&self) -> usize {
        self.size()
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

#[cfg(all(test, feature = "explain_memory"))]
mod tests {
    use super::*;
    use crate::memory_pool::MemoryConsumer;
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
}
