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

//! Context for computing statistics in physical plans.
//!
//! [`StatisticsArgs`] provides external context to
//! [`ExecutionPlan::statistics_with_args`].

use crate::ExecutionPlan;
use datafusion_common::Result;
use datafusion_common::{Statistics, assert_or_internal_err};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// Per-call memoization cache for [`compute_statistics`].
///
/// Keyed by `(plan node pointer address, partition)`. Created once per
/// top-level [`compute_statistics`] call and shared across all recursive
/// and operator-internal calls via [`StatisticsArgs`].
///
/// The pointer-based key is safe within a single synchronous
/// `compute_statistics` call: all `Arc<dyn ExecutionPlan>` nodes are held
/// by the plan tree for the duration of the walk, so addresses cannot be
/// reused.
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
    /// Shared memoization cache for the current `compute_statistics` walk.
    cache: Option<Rc<RefCell<StatsCache>>>,
}

impl StatisticsArgs {
    pub fn new(partition: Option<usize>) -> Self {
        Self {
            partition,
            cache: None,
        }
    }

    pub fn partition(&self) -> Option<usize> {
        self.partition
    }

    /// Computes statistics for a child plan, using the shared cache
    /// from the current [`compute_statistics`] walk.
    pub fn compute_child_statistics(
        &self,
        plan: impl AsRef<dyn ExecutionPlan>,
        partition: Option<usize>,
    ) -> Result<Arc<Statistics>> {
        let plan = plan.as_ref();
        match &self.cache {
            Some(cache) => compute_statistics_inner(plan, partition, cache),
            None => compute_statistics(plan, partition),
        }
    }
}

/// Computes statistics for a plan node by calling
/// [`ExecutionPlan::statistics_with_args`] with a shared cache.
///
/// Results are memoized within a single call: operators that call
/// [`StatisticsArgs::compute_child_statistics`] will hit the cache
/// instead of re-walking subtrees.
pub fn compute_statistics(
    plan: &dyn ExecutionPlan,
    partition: Option<usize>,
) -> Result<Arc<Statistics>> {
    let cache = Rc::new(RefCell::new(StatsCache::default()));
    compute_statistics_inner(plan, partition, &cache)
}

fn compute_statistics_inner(
    plan: &dyn ExecutionPlan,
    partition: Option<usize>,
    cache: &Rc<RefCell<StatsCache>>,
) -> Result<Arc<Statistics>> {
    if let Some(idx) = partition {
        let partition_count = plan.properties().partitioning.partition_count();
        assert_or_internal_err!(
            idx < partition_count,
            "Invalid partition index: {}, the partition count is {}",
            idx,
            partition_count
        );
    }

    if let Some(cached) = cache.borrow().get(plan, partition) {
        return Ok(Arc::clone(cached));
    }

    let args = StatisticsArgs {
        partition,
        cache: Some(Rc::clone(cache)),
    };
    let result = plan.statistics_with_args(&args)?;

    cache
        .borrow_mut()
        .insert(plan, partition, Arc::clone(&result));
    Ok(result)
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

        let stats = compute_statistics(plan.as_ref(), Some(0)).unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(100));

        let stats_none = compute_statistics(plan.as_ref(), None).unwrap();
        assert_eq!(stats_none.num_rows, Precision::Exact(100));
    }
}
