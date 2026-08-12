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

//! Aggregate table for partial aggregation when input is ordered by group keys.
//!
//! See the [`super::common_ordered`] comments for the high-level ideas.
//!
//! This operator handles input that is ordered by group keys:
//! - Fully ordered: `GROUP BY a, b`, input is `ORDER BY a, b`
//! - Partially ordered: `GROUP BY a, b`, input is `ORDER BY a`
//!
//! When a group key combination is exhausted, this table eagerly flushes the
//! completed groups to improve memory efficiency.
//!
//! The implementation is separated from other aggregate tables because this
//! execution path is likely to be optimized further in the future.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

use crate::aggregates::{
    AggregateExec, AggregateMode, aggregate_hash_table::PartialMarker,
};

use super::common_ordered::OrderedAggregateTable;

/// Implementation specific to partial aggregation, where the table stores
/// partial aggregate states and the input rows are raw rows.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, sum(x), count(x)`
/// - Input rows: `k, x`
///
/// See comments at [`OrderedAggregateTable`] for details.
impl OrderedAggregateTable<PartialMarker> {
    pub(in crate::aggregates) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        let input_schema = agg.input().schema();
        Self::new_for_mode(
            agg,
            partition,
            &input_schema,
            output_schema,
            batch_size,
            &agg.input_order_mode,
            &AggregateMode::Partial,
            agg.filter_expr.iter().cloned().collect(),
        )
    }

    /// Aggregates one raw input batch and updates ordering information for any
    /// newly observed groups.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        self.aggregate_evaluated_batch(&evaluated_batch, false)
    }

    /// Emits the next batch of partial state rows for groups proven complete by
    /// the input ordering.
    ///
    /// For example, when the query is `GROUP BY a` and the input is ordered by
    /// `a`, seeing a latest input row with `a = 3` means all groups with `a < 3`
    /// are complete and safe to emit.
    ///
    /// Key steps:
    /// 1. Ask `group_ordering` to decide how many groups can be emitted eagerly.
    /// 2. Remove the emitted groups from `group_ordering`, `GroupValues`, and
    ///    all `GroupsAccumulator`s.
    ///
    /// This may output small batches. Avoiding tiny batches is left to future
    /// ordered-aggregation optimizations.
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        self.next_output_batch_for_mode(false)
    }
}
