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

use crate::window::partition_evaluator::PartitionEvaluator;
use crate::{PhysicalExpr, PhysicalSortExpr};
use arrow::compute::kernels::partition::lexicographical_partition_ranges;
use arrow::compute::kernels::sort::SortColumn;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use arrow_schema::DataType;
use datafusion_common::{reverse_sort_options, DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, WindowFrame};
use indexmap::IndexMap;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

/// A window expression that:
/// * knows its resulting field
pub trait WindowExpr: Send + Sync + Debug {
    /// Returns the window expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this window function.
    fn field(&self) -> Result<Field>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "WindowExpr: default name"
    }

    /// expressions that are passed to the WindowAccumulator.
    /// Functions which take a single input argument, such as `sum`, return a single [`datafusion_expr::expr::Expr`],
    /// others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// evaluate the window function arguments against the batch and return
    /// array ref, normally the resulting vec is a single element one.
    fn evaluate_args(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.expressions()
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect()
    }

    /// evaluate the window function values against the batch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// evaluate the window function values against the batch
    fn evaluate_stateful(
        &self,
        _partition_batches: &PartitionBatches,
        _window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        Err(DataFusionError::Internal(format!(
            "evaluate_stateful is not implemented for {}",
            self.name()
        )))
    }

    /// evaluate the partition points given the sort columns; if the sort columns are
    /// empty then the result will be a single element vec of the whole column rows.
    fn evaluate_partition_points(
        &self,
        num_rows: usize,
        partition_columns: &[SortColumn],
    ) -> Result<Vec<Range<usize>>> {
        if partition_columns.is_empty() {
            Ok(vec![Range {
                start: 0,
                end: num_rows,
            }])
        } else {
            Ok(lexicographical_partition_ranges(partition_columns)
                .map_err(DataFusionError::ArrowError)?
                .collect::<Vec<_>>())
        }
    }

    /// expressions that's from the window function's partition by clause, empty if absent
    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>];

    /// expressions that's from the window function's order by clause, empty if absent
    fn order_by(&self) -> &[PhysicalSortExpr];

    /// get order by columns, empty if absent
    fn order_by_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.order_by()
            .iter()
            .map(|e| e.evaluate_to_sort_column(batch))
            .collect::<Result<Vec<SortColumn>>>()
    }

    /// get sort columns that can be used for peer evaluation, empty if absent
    fn sort_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        let order_by_columns = self.order_by_columns(batch)?;
        Ok(order_by_columns)
    }

    /// Get values columns(argument of Window Function)
    /// and order by columns (columns of the ORDER BY expression)used in evaluators
    fn get_values_orderbys(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<(Vec<ArrayRef>, Vec<ArrayRef>)> {
        let values = self.evaluate_args(record_batch)?;
        let order_by_columns = self.order_by_columns(record_batch)?;
        let order_bys: Vec<ArrayRef> =
            order_by_columns.iter().map(|s| s.values.clone()).collect();
        Ok((values, order_bys))
    }

    /// Get the window frame of this [WindowExpr].
    fn get_window_frame(&self) -> &Arc<WindowFrame>;

    /// Return a flag indicating whether this [WindowExpr] can run with
    /// bounded memory.
    fn uses_bounded_memory(&self) -> bool;

    /// Get the reverse expression of this [WindowExpr].
    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>>;
}

/// Reverses the ORDER BY expression, which is useful during equivalent window
/// expression construction. For instance, 'ORDER BY a ASC, NULLS LAST' turns into
/// 'ORDER BY a DESC, NULLS FIRST'.
pub fn reverse_order_bys(order_bys: &[PhysicalSortExpr]) -> Vec<PhysicalSortExpr> {
    order_bys
        .iter()
        .map(|e| PhysicalSortExpr {
            expr: e.expr.clone(),
            options: reverse_sort_options(e.options),
        })
        .collect()
}

#[derive(Debug)]
pub enum WindowFn {
    Builtin(Box<dyn PartitionEvaluator>),
    Aggregate(Box<dyn Accumulator>),
}

/// State for RANK(percent_rank, rank, dense_rank)
/// builtin window function
#[derive(Debug, Clone, Default)]
pub struct RankState {
    /// The last values for rank as these values change, we increase n_rank
    pub last_rank_data: Vec<ScalarValue>,
    /// The index where last_rank_boundary is started
    pub last_rank_boundary: usize,
    /// Rank number kept from the start
    pub n_rank: usize,
}

/// State for 'ROW_NUMBER' builtin window function
#[derive(Debug, Clone, Default)]
pub struct NumRowsState {
    pub n_rows: usize,
}

#[derive(Debug, Clone, Default)]
pub struct NthValueState {
    pub range: Range<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct LeadLagState {
    pub idx: usize,
}

#[derive(Debug, Clone, Default)]
pub enum BuiltinWindowState {
    Rank(RankState),
    NumRows(NumRowsState),
    NthValue(NthValueState),
    LeadLag(LeadLagState),
    #[default]
    Default,
}
#[derive(Debug)]
pub enum WindowFunctionState {
    /// Different Aggregate functions may have different state definitions
    /// In [Accumulator] trait, [fn state(&self) -> Result<Vec<ScalarValue>>] implementation
    /// dictates that.
    AggregateState(Vec<ScalarValue>),
    /// BuiltinWindowState
    BuiltinWindowState(BuiltinWindowState),
}

#[derive(Debug)]
pub struct WindowAggState {
    /// The range that we calculate the window function
    pub window_frame_range: Range<usize>,
    /// The index of the last row that its result is calculated inside the partition record batch buffer.
    pub last_calculated_index: usize,
    /// The offset of the deleted row number
    pub offset_pruned_rows: usize,
    /// State of the window function, required to calculate its result
    // For instance, for ROW_NUMBER we keep the row index counter to generate correct result
    pub window_function_state: WindowFunctionState,
    /// Stores the results calculated by window frame
    pub out_col: ArrayRef,
    /// Keeps track of how many rows should be generated to be in sync with input record_batch.
    // (For each row in the input record batch we need to generate a window result).
    pub n_row_result_missing: usize,
    /// flag indicating whether we have received all data for this partition
    pub is_end: bool,
}

/// State for each unique partition determined according to PARTITION BY column(s)
#[derive(Debug)]
pub struct PartitionBatchState {
    /// The record_batch belonging to current partition
    pub record_batch: RecordBatch,
    /// flag indicating whether we have received all data for this partition
    pub is_end: bool,
}

/// key for IndexMap for each unique partition
/// For instance, if window frame is OVER(PARTITION BY a,b)
/// PartitionKey would consist of unique [a,b] pairs
pub type PartitionKey = Vec<ScalarValue>;

#[derive(Debug)]
pub struct WindowState {
    pub state: WindowAggState,
    pub window_fn: WindowFn,
}
pub type PartitionWindowAggStates = IndexMap<PartitionKey, WindowState>;

/// The IndexMap (i.e. an ordered HashMap) where record batches are separated for each partition.
pub type PartitionBatches = IndexMap<PartitionKey, PartitionBatchState>;

impl WindowAggState {
    pub fn new(
        out_type: &DataType,
        window_function_state: WindowFunctionState,
    ) -> Result<Self> {
        let empty_out_col = ScalarValue::try_from(out_type)?.to_array_of_size(0);
        Ok(Self {
            window_frame_range: Range { start: 0, end: 0 },
            last_calculated_index: 0,
            offset_pruned_rows: 0,
            window_function_state,
            out_col: empty_out_col,
            n_row_result_missing: 0,
            is_end: false,
        })
    }
}
