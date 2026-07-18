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

use crate::aggregates::PhysicalGroupBy;
use crate::joins::SeededRandomState;
use crate::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory};
use arrow::array::{ArrayRef, RecordBatch};
use datafusion_common::Result;
use datafusion_common::cast::as_uint64_array;
use datafusion_common::hash_utils::create_hashes;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::VecAllocExt;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

const INTERNAL_HASH_COL_PREFIX: &str = "__datafusion_internal_hash";
pub(crate) const HASH_ROWS_COMPUTED: &str = "hash_rows_computed";
pub(crate) const HASH_ROWS_REUSED: &str = "hash_rows_reused";

/// Execution metrics for expression hashing.
///
/// Clones update the same underlying counters, allowing a replacement hasher
/// (for example while merging spilled aggregation state) to preserve the
/// operator's metrics.
#[derive(Debug, Clone)]
pub(crate) struct HashMetrics {
    rows_computed: Count,
    rows_reused: Count,
}

impl HashMetrics {
    pub(crate) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            rows_computed: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter(HASH_ROWS_COMPUTED, partition),
            rows_reused: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter(HASH_ROWS_REUSED, partition),
        }
    }

    fn record_computed(&self, rows: usize) {
        self.rows_computed.add(rows);
    }

    fn record_reused(&self, rows: usize) {
        self.rows_reused.add(rows);
    }
}

pub(crate) struct ExpressionHasher {
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    hash_buffer: Vec<u64>,
    random_state: SeededRandomState,
    metrics: Option<HashMetrics>,
}

impl ExpressionHasher {
    pub(crate) fn new(hash_exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            hash_exprs,
            hash_buffer: vec![],
            random_state: SeededRandomState::with_seed(0),
            metrics: None,
        }
    }

    pub(crate) fn new_with_metrics(
        hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
        metrics: HashMetrics,
    ) -> Self {
        Self {
            metrics: Some(metrics),
            ..Self::new(hash_exprs)
        }
    }

    pub(crate) fn set_metrics(&mut self, metrics: HashMetrics) {
        self.metrics = Some(metrics);
    }

    /// Creates a hasher for another expression list while preserving metrics.
    pub(crate) fn new_for_exprs(&self, hash_exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            metrics: self.metrics.clone(),
            ..Self::new(hash_exprs)
        }
    }

    /// Builds the name for the column that will carry hashes across [`ExecutionPlan`]s.
    ///
    /// In order to avoid recomputation of hashes, some nodes have the capability of computing the
    /// hash once, and propagate it through the plan so that future nodes can reuse them.
    /// Equal expression lists produce the same name, while the expression order is part of the
    /// identity. The name is an identifier within a physical plan and is not stable across versions.
    ///
    /// [`ExecutionPlan`]: crate::ExecutionPlan
    pub(crate) fn internal_hash_col_name(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash_exprs.hash(&mut hasher);
        format!("{INTERNAL_HASH_COL_PREFIX}_{:016x}", hasher.finish())
    }

    pub(crate) fn precomputed<'a>(&self, batch: &'a RecordBatch) -> Option<&'a [u64]> {
        let internal_hash_col_name = self.internal_hash_col_name();
        let hash_column = batch.column_by_name(&internal_hash_col_name)?;
        let hash_array = as_uint64_array(hash_column.as_ref()).ok()?;
        if let Some(metrics) = &self.metrics {
            metrics.record_reused(hash_array.len());
        }
        Some(hash_array.values())
    }

    pub(crate) fn precomputed_group_by<'a>(
        &self,
        group_by: &PhysicalGroupBy,
        batch: &'a RecordBatch,
    ) -> Option<&'a [u64]> {
        if !group_by.is_single() {
            return None;
        }

        self.precomputed(batch)
    }

    pub(crate) fn compute_hashes(&mut self, arrays: &[ArrayRef]) -> Result<&[u64]> {
        let num_rows = arrays.first().map(|array| array.len()).unwrap_or(0);
        self.hash_buffer.clear();
        self.hash_buffer.resize(num_rows, 0);

        create_hashes(
            arrays,
            self.random_state.random_state(),
            &mut self.hash_buffer,
        )?;

        if let Some(metrics) = &self.metrics {
            metrics.record_computed(num_rows);
        }

        Ok(&self.hash_buffer)
    }

    pub(crate) fn compute_hashes_for_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<&[u64]> {
        let arrays = evaluate_expressions_to_arrays(&self.hash_exprs, batch)?;
        self.compute_hashes(&arrays)
    }

    pub(crate) fn allocated_size(&self) -> usize {
        self.hash_exprs.allocated_size() + self.hash_buffer.allocated_size()
    }

    pub(crate) fn clear_shrink(&mut self, capacity: usize) {
        self.hash_buffer.clear();
        self.hash_buffer.shrink_to(capacity);
    }
}
