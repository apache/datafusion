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

//! Shared bounds for Sort-Merge Join dynamic filter pushdown.
//!
//! This mirrors the correctness model used by `HashJoinExec`'s
//! [`crate::joins::hash_join::shared_bounds`]: a dynamic filter must be built
//! from *complete* information about the side it summarizes, and published
//! **exactly once**. Publishing a partial or mid-stream bound — or advancing a
//! bound as partitions make progress — can incorrectly eliminate valid join
//! results, because the single dynamic filter is shared across all of the
//! (concurrently executing) hash partitions feeding the join.
//!
//! Accordingly, each partition streams its join-key values to the accumulator,
//! which tracks the global `[min, max]` range across all partitions. Only once
//! every partition has been fully consumed is the filter published as a static
//! range predicate `col >= min AND col <= max` (a superset that never prunes a
//! matchable row), after which it is marked complete.

use crate::metrics::Count;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::{BinaryExpr, DynamicFilterPhysicalExpr, lit};
use parking_lot::Mutex;
use std::sync::Arc;

/// Coordinates dynamic filter updates for Sort-Merge Join across multiple partitions.
///
/// The filter summarizes the join keys observed on one side of the join and is
/// pushed down to prune the *other* side. It is published once, after all
/// partitions have reported their data, as a static range predicate. See the
/// module documentation for why mid-stream/advancing bounds are unsafe here.
#[derive(Debug)]
pub(crate) struct SharedSortMergeBoundsAccumulator {
    /// Shared state for all partitions.
    state: Mutex<AccumulatorState>,
    /// Total number of partitions that will report to this accumulator.
    num_partitions: usize,
    /// Dynamic filter to update (once, when all partitions are exhausted).
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Join key expression on the side being filtered.
    on_expr: PhysicalExprRef,
    /// Metric to track number of filter updates.
    metrics: Option<Count>,
}

#[derive(Debug)]
struct AccumulatorState {
    /// Smallest (non-null) join key value observed across all partitions.
    min: Option<ScalarValue>,
    /// Largest (non-null) join key value observed across all partitions.
    max: Option<ScalarValue>,
    /// Number of partitions that have been fully consumed.
    exhausted_count: usize,
    /// Whether the filter has already been published.
    published: bool,
}

impl SharedSortMergeBoundsAccumulator {
    pub fn new(
        num_partitions: usize,
        on_expr: PhysicalExprRef,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
        metrics: Option<Count>,
    ) -> Self {
        Self {
            state: Mutex::new(AccumulatorState {
                min: None,
                max: None,
                exhausted_count: 0,
                published: false,
            }),
            num_partitions,
            dynamic_filter,
            on_expr,
            metrics,
        }
    }

    /// Report a join key value observed by a partition.
    ///
    /// Null keys are ignored: they never participate in equi-join matches, so
    /// they neither widen the bounds nor need to be preserved by the filter.
    pub fn report_head(&self, _partition_id: usize, head: ScalarValue) -> Result<()> {
        if head.is_null() {
            return Ok(());
        }
        let mut state = self.state.lock();
        match &state.min {
            Some(min) if min <= &head => {}
            _ => state.min = Some(head.clone()),
        }
        match &state.max {
            Some(max) if max >= &head => {}
            _ => state.max = Some(head),
        }
        Ok(())
    }

    /// Mark a partition as fully consumed.
    ///
    /// Once every partition has been consumed, the accumulated `[min, max]`
    /// range is published to the dynamic filter exactly once and the filter is
    /// marked complete.
    pub fn mark_exhausted(&self, _partition_id: usize) -> Result<()> {
        let mut state = self.state.lock();
        state.exhausted_count += 1;

        if state.exhausted_count < self.num_partitions || state.published {
            return Ok(());
        }
        state.published = true;

        // Build the final, static predicate from complete information.
        let filter_expr = match (state.min.take(), state.max.take()) {
            (Some(min), Some(max)) => {
                // col >= min AND col <= max
                let lower = Arc::new(BinaryExpr::new(
                    Arc::clone(&self.on_expr),
                    Operator::GtEq,
                    lit(min),
                ));
                let upper = Arc::new(BinaryExpr::new(
                    Arc::clone(&self.on_expr),
                    Operator::LtEq,
                    lit(max),
                ));
                Arc::new(BinaryExpr::new(lower, Operator::And, upper)) as _
            }
            // No non-null keys were observed on this side: for the join types
            // that enable this filter (Inner / LeftSemi / RightSemi) nothing on
            // the other side can match, so prune everything.
            _ => lit(false),
        };

        self.dynamic_filter.update(filter_expr)?;
        self.dynamic_filter.mark_complete();

        if let Some(m) = &self.metrics {
            m.add(1);
        }

        Ok(())
    }
}
