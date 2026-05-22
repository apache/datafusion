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

//! Statistics computation for physical plans.
//!
//! [`StatisticsArgs`] provides external context to
//! [`ExecutionPlan::statistics_with_args`].

use crate::ExecutionPlan;
use datafusion_common::{Result, Statistics, assert_or_internal_err};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// Per-call memoization cache for statistics computation.
///
/// Keyed by `(plan node pointer address, partition)`. Shared across
/// a single statistics walk via [`StatisticsArgs`].
///
/// The pointer-based key is safe within a single synchronous walk:
/// all `Arc<dyn ExecutionPlan>` nodes are held by the plan tree for
/// the duration of the walk, so addresses cannot be reused.
#[derive(Debug, Default)]
struct StatsCache(HashMap<(usize, Option<usize>), Arc<Statistics>>);

impl StatsCache {
    fn get(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
    ) -> Option<&Arc<Statistics>> {
        let key = (
            plan as *const dyn ExecutionPlan as *const () as usize,
            partition,
        );
        self.0.get(&key)
    }

    fn insert(
        &mut self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
        stats: Arc<Statistics>,
    ) {
        let key = (
            plan as *const dyn ExecutionPlan as *const () as usize,
            partition,
        );
        self.0.insert(key, stats);
    }
}

/// Arguments passed to [`ExecutionPlan::statistics_with_args`] carrying
/// external information that operators can use when computing their
/// statistics.
#[derive(Debug)]
pub struct StatisticsArgs {
    partition: Option<usize>,
    /// Shared memoization cache for the current statistics walk.
    cache: Rc<RefCell<StatsCache>>,
}

impl StatisticsArgs {
    /// Creates new statistics arguments with a fresh cache.
    pub fn new(partition: Option<usize>) -> Self {
        Self {
            partition,
            cache: Rc::new(RefCell::new(StatsCache::default())),
        }
    }

    pub fn partition(&self) -> Option<usize> {
        self.partition
    }

    /// Computes statistics for a child plan, using the shared cache
    /// to avoid redundant subtree walks.
    pub fn compute_child_statistics(
        &self,
        plan: impl AsRef<dyn ExecutionPlan>,
        partition: Option<usize>,
    ) -> Result<Arc<Statistics>> {
        let plan = plan.as_ref();

        if let Some(idx) = partition {
            let partition_count = plan.properties().partitioning.partition_count();
            assert_or_internal_err!(
                idx < partition_count,
                "Invalid partition index: {}, the partition count is {}",
                idx,
                partition_count
            );
        }

        if let Some(cached) = self.cache.borrow().get(plan, partition) {
            return Ok(Arc::clone(cached));
        }

        let child_args = StatisticsArgs {
            partition,
            cache: Rc::clone(&self.cache),
        };
        let result = plan.statistics_with_args(&child_args)?;

        self.cache
            .borrow_mut()
            .insert(plan, partition, Arc::clone(&result));
        Ok(result)
    }
}

#[cfg(all(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::test::exec::StatisticsExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ColumnStatistics, stats::Precision};

    fn make_stats_leaf(num_rows: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let col_stats = vec![ColumnStatistics {
            null_count: Precision::Exact(0),
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        }];
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(num_rows),
                total_byte_size: Precision::Absent,
                column_statistics: col_stats,
            },
            schema,
        ))
    }

    #[test]
    fn coalesce_returns_overall_stats_for_any_partition() {
        let leaf = make_stats_leaf(100);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(leaf));

        let args = StatisticsArgs::new(Some(0));
        let stats = plan.statistics_with_args(&args).unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(100));

        let args_none = StatisticsArgs::new(None);
        let stats_none = plan.statistics_with_args(&args_none).unwrap();
        assert_eq!(stats_none.num_rows, Precision::Exact(100));
    }
}
