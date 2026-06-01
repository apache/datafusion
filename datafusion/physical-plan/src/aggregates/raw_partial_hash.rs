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

//! Grouped hash aggregation for simple multi-stage aggregation paths.
//!
//! This module handles the basic grouped two-stage paths:
//!
//! ```text
//! input rows -> GROUP BY hash table -> accumulator state rows
//! state rows -> GROUP BY hash table -> final aggregate rows
//! ```
//!
//! `AggregateExec` keeps finite-memory, ordered, limit, grouping-set,
//! `partial state -> partial state`, and single-stage aggregation on
//! `GroupedHashAggregateStream` for now.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::types::{Float64Type, Int64Type, UInt64Type};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, NullBufferBuilder,
    UInt64Array, new_null_array,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use futures::ready;
use futures::stream::{Stream, StreamExt};

use super::group_values::{
    GroupByMetrics, GroupValues, new_unordered_blocked_group_values,
    new_unordered_group_values, supports_blocked_group_values,
};
use super::row_hash::create_group_accumulator;
use super::{
    AggregateExec, PhysicalGroupBy, aggregate_expressions, evaluate_group_by,
    group_id_array, max_duplicate_ordinal,
};
use crate::metrics::{
    BaselineMetrics, MetricBuilder, MetricCategory, RecordOutput, SpillMetrics,
};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, PhysicalExpr, RecordBatchStream, SendableRecordBatchStream};

#[derive(Debug, Clone)]
enum ExecutionState {
    ReadingInput,
    ProducingOutput,
    Done,
}

struct HashAggregateAccumulator {
    /// Arguments to pass to this accumulator.
    ///
    /// Example: `CORR(x, y)` stores two expressions here, while `SUM(x)` stores one.
    arguments: Vec<Arc<dyn PhysicalExpr>>,

    /// Optional `FILTER` expression for this accumulator.
    ///
    /// Example: `SUM(x) FILTER (WHERE x > 10)` stores the `x > 10` predicate.
    filter: Option<Arc<dyn PhysicalExpr>>,

    /// Accumulator state for all groups for one aggregate expression.
    accumulator: Box<dyn GroupsAccumulator>,
}

struct EvaluatedHashAggregateAccumulator {
    arguments: Vec<ArrayRef>,
    filter: Option<ArrayRef>,
}

struct EvaluatedAggregateBatch {
    /// One entry per grouping set; each entry contains all evaluated group key
    /// arrays for the current input batch.
    grouping_set_args: Vec<Vec<ArrayRef>>,

    /// Evaluated arguments and filters, one entry per aggregate expression.
    accumulator_args: Vec<EvaluatedHashAggregateAccumulator>,
}

#[derive(Debug)]
struct BlockedAvgGroupsAccumulator {
    state_layout: AvgStateLayout,
    block_size: usize,
    counts: Vec<Box<[u64]>>,
    sums: Vec<Box<[f64]>>,
    len: usize,
}

#[derive(Debug, Clone, Copy)]
enum AvgStateLayout {
    CountSumUInt64,
    SumCountInt64,
}

impl BlockedAvgGroupsAccumulator {
    fn new(state_layout: AvgStateLayout, block_size: usize) -> Self {
        assert!(block_size > 0);
        Self {
            state_layout,
            block_size,
            counts: vec![],
            sums: vec![],
            len: 0,
        }
    }

    fn ensure_capacity(&mut self, total_num_groups: usize) {
        let required_blocks = total_num_groups.div_ceil(self.block_size);
        while self.counts.len() < required_blocks {
            self.counts
                .push(vec![0; self.block_size].into_boxed_slice());
            self.sums
                .push(vec![0.0; self.block_size].into_boxed_slice());
        }
        self.len = self.len.max(total_num_groups);
    }

    fn add_value(&mut self, group_index: usize, value: f64) {
        debug_assert!(group_index < self.len);
        let block_idx = group_index / self.block_size;
        let value_idx = group_index % self.block_size;
        self.counts[block_idx][value_idx] += 1;
        self.sums[block_idx][value_idx] += value;
    }

    fn merge_value(&mut self, group_index: usize, count: u64, sum: f64) {
        debug_assert!(group_index < self.len);
        let block_idx = group_index / self.block_size;
        let value_idx = group_index % self.block_size;
        self.counts[block_idx][value_idx] += count;
        self.sums[block_idx][value_idx] += sum;
    }

    fn values_range<T: Copy>(
        blocks: &[Box<[T]>],
        block_size: usize,
        start: usize,
        len: usize,
    ) -> Vec<T> {
        let mut output = Vec::with_capacity(len);
        let mut remaining = len;
        let mut group_id = start;

        while remaining > 0 {
            let block_idx = group_id / block_size;
            let offset = group_id % block_size;
            let take = remaining.min(block_size - offset);
            output.extend_from_slice(&blocks[block_idx][offset..offset + take]);
            remaining -= take;
            group_id += take;
        }

        output
    }

    fn rebuild_from_values(&mut self, counts: &[u64], sums: &[f64]) {
        debug_assert_eq!(counts.len(), sums.len());
        self.counts.clear();
        self.sums.clear();
        self.len = counts.len();

        for chunk in counts.chunks(self.block_size) {
            let mut block = vec![0; self.block_size].into_boxed_slice();
            block[..chunk.len()].copy_from_slice(chunk);
            self.counts.push(block);
        }

        for chunk in sums.chunks(self.block_size) {
            let mut block = vec![0.0; self.block_size].into_boxed_slice();
            block[..chunk.len()].copy_from_slice(chunk);
            self.sums.push(block);
        }
    }

    fn take_all_values(&mut self) -> (Vec<u64>, Vec<f64>) {
        let counts = Self::values_range(&self.counts, self.block_size, 0, self.len);
        let sums = Self::values_range(&self.sums, self.block_size, 0, self.len);
        self.counts.clear();
        self.sums.clear();
        self.len = 0;
        (counts, sums)
    }

    fn take_block_values(&mut self) -> (Vec<u64>, Vec<f64>) {
        let emit_len = self.len.min(self.block_size);

        let mut counts = self.counts.remove(0).into_vec();
        counts.truncate(emit_len);

        let mut sums = self.sums.remove(0).into_vec();
        sums.truncate(emit_len);

        self.len -= emit_len;
        (counts, sums)
    }

    fn take_first_values(&mut self, n: usize) -> (Vec<u64>, Vec<f64>) {
        let n = n.min(self.len);
        let counts = Self::values_range(&self.counts, self.block_size, 0, n);
        let sums = Self::values_range(&self.sums, self.block_size, 0, n);

        let remaining_counts =
            Self::values_range(&self.counts, self.block_size, n, self.len - n);
        let remaining_sums =
            Self::values_range(&self.sums, self.block_size, n, self.len - n);
        self.rebuild_from_values(&remaining_counts, &remaining_sums);

        (counts, sums)
    }

    fn take_values(&mut self, emit_to: EmitTo) -> (Vec<u64>, Vec<f64>) {
        match emit_to {
            EmitTo::All => self.take_all_values(),
            EmitTo::Block => self.take_block_values(),
            EmitTo::First(n) => self.take_first_values(n),
        }
    }

    fn nulls_for_counts(counts: &[u64]) -> Option<arrow::buffer::NullBuffer> {
        let mut nulls = NullBufferBuilder::new(counts.len());
        for count in counts {
            if *count == 0 {
                nulls.append_null();
            } else {
                nulls.append_non_null();
            }
        }
        nulls.finish()
    }

    fn filter_is_valid(opt_filter: Option<&BooleanArray>, row: usize) -> bool {
        opt_filter
            .map(|filter| filter.is_valid(row) && filter.value(row))
            .unwrap_or(true)
    }

    fn count_state_i64(counts: &[u64]) -> Vec<i64> {
        counts.iter().map(|count| *count as i64).collect()
    }
}

impl GroupsAccumulator for BlockedAvgGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<Float64Type>();
        self.ensure_capacity(total_num_groups);

        if values.null_count() == 0 && opt_filter.is_none() {
            for (&group_index, &value) in group_indices.iter().zip(values.values().iter())
            {
                self.add_value(group_index, value);
            }
        } else {
            for (row, &group_index) in group_indices.iter().enumerate() {
                if !Self::filter_is_valid(opt_filter, row) || values.is_null(row) {
                    continue;
                }
                self.add_value(group_index, values.value(row));
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (counts, sums) = self.take_values(emit_to);
        let mut values = Vec::with_capacity(counts.len());
        let mut nulls = NullBufferBuilder::new(counts.len());

        for (count, sum) in counts.into_iter().zip(sums) {
            if count == 0 {
                values.push(0.0);
                nulls.append_null();
            } else {
                values.push(sum / count as f64);
                nulls.append_non_null();
            }
        }

        Ok(Arc::new(Float64Array::new(values.into(), nulls.finish())))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (counts, sums) = self.take_values(emit_to);
        let nulls = Self::nulls_for_counts(&counts);

        match self.state_layout {
            AvgStateLayout::CountSumUInt64 => Ok(vec![
                Arc::new(UInt64Array::new(counts.into(), nulls.clone())) as ArrayRef,
                Arc::new(Float64Array::new(sums.into(), nulls)) as ArrayRef,
            ]),
            AvgStateLayout::SumCountInt64 => Ok(vec![
                Arc::new(Float64Array::new(sums.into(), nulls.clone())) as ArrayRef,
                Arc::new(Int64Array::new(
                    Self::count_state_i64(&counts).into(),
                    nulls,
                )) as ArrayRef,
            ]),
        }
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        self.ensure_capacity(total_num_groups);

        match self.state_layout {
            AvgStateLayout::CountSumUInt64 => {
                let partial_counts = values[0].as_primitive::<UInt64Type>();
                let partial_sums = values[1].as_primitive::<Float64Type>();

                for (row, &group_index) in group_indices.iter().enumerate() {
                    if !Self::filter_is_valid(opt_filter, row)
                        || partial_counts.is_null(row)
                        || partial_sums.is_null(row)
                    {
                        continue;
                    }

                    self.merge_value(
                        group_index,
                        partial_counts.value(row),
                        partial_sums.value(row),
                    );
                }
            }
            AvgStateLayout::SumCountInt64 => {
                let partial_sums = values[0].as_primitive::<Float64Type>();
                let partial_counts = values[1].as_primitive::<Int64Type>();

                for (row, &group_index) in group_indices.iter().enumerate() {
                    if !Self::filter_is_valid(opt_filter, row)
                        || partial_counts.is_null(row)
                        || partial_sums.is_null(row)
                    {
                        continue;
                    }

                    self.merge_value(
                        group_index,
                        partial_counts.value(row) as u64,
                        partial_sums.value(row),
                    );
                }
            }
        }

        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert_eq!(values.len(), 1, "single argument to convert_to_state");
        let values = values[0].as_primitive::<Float64Type>();
        let mut counts = Vec::with_capacity(values.len());
        let mut sums = Vec::with_capacity(values.len());
        let mut nulls = NullBufferBuilder::new(values.len());

        for row in 0..values.len() {
            if Self::filter_is_valid(opt_filter, row) && values.is_valid(row) {
                counts.push(1);
                sums.push(values.value(row));
                nulls.append_non_null();
            } else {
                counts.push(0);
                sums.push(0.0);
                nulls.append_null();
            }
        }

        let nulls = nulls.finish();
        match self.state_layout {
            AvgStateLayout::CountSumUInt64 => Ok(vec![
                Arc::new(UInt64Array::new(counts.into(), nulls.clone())) as ArrayRef,
                Arc::new(Float64Array::new(sums.into(), nulls)) as ArrayRef,
            ]),
            AvgStateLayout::SumCountInt64 => Ok(vec![
                Arc::new(Float64Array::new(sums.into(), nulls.clone())) as ArrayRef,
                Arc::new(Int64Array::new(
                    Self::count_state_i64(&counts).into(),
                    nulls,
                )) as ArrayRef,
            ]),
        }
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.len() * self.block_size * size_of::<u64>()
            + self.sums.len() * self.block_size * size_of::<f64>()
            + self.counts.allocated_size()
            + self.sums.allocated_size()
    }
}

fn blocked_avg_state_layout(
    agg_expr: &Arc<AggregateFunctionExpr>,
) -> Result<Option<AvgStateLayout>> {
    if agg_expr.fun().name() != "avg" || agg_expr.is_distinct() {
        return Ok(None);
    }

    let state_fields = agg_expr.state_fields()?;
    let state_layout = match state_fields.as_slice() {
        [count, sum]
            if matches!(count.data_type(), DataType::UInt64)
                && matches!(sum.data_type(), DataType::Float64)
                && matches!(agg_expr.field().data_type(), DataType::Float64) =>
        {
            Some(AvgStateLayout::CountSumUInt64)
        }
        [sum, count]
            if matches!(sum.data_type(), DataType::Float64)
                && matches!(count.data_type(), DataType::Int64)
                && matches!(agg_expr.field().data_type(), DataType::Float64) =>
        {
            Some(AvgStateLayout::SumCountInt64)
        }
        _ => None,
    };

    Ok(state_layout)
}

fn create_blocked_group_accumulator(
    agg_expr: &Arc<AggregateFunctionExpr>,
    block_size: usize,
) -> Result<Option<Box<dyn GroupsAccumulator>>> {
    let state_layout = blocked_avg_state_layout(agg_expr)?;
    Ok(state_layout.map(|state_layout| {
        Box::new(BlockedAvgGroupsAccumulator::new(state_layout, block_size))
            as Box<dyn GroupsAccumulator>
    }))
}

fn create_blocked_group_accumulators(
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    block_size: usize,
) -> Result<Option<Vec<Box<dyn GroupsAccumulator>>>> {
    let mut accumulators = Vec::with_capacity(aggr_expr.len());
    for agg_expr in aggr_expr {
        let Some(accumulator) = create_blocked_group_accumulator(agg_expr, block_size)?
        else {
            return Ok(None);
        };
        accumulators.push(accumulator);
    }
    Ok(Some(accumulators))
}

pub(crate) fn can_use_blocked_hash_aggregate(agg: &AggregateExec) -> Result<bool> {
    if agg.group_by.has_grouping_set() || agg.group_by.groups().len() != 1 {
        return Ok(false);
    }

    let group_schema = agg.group_by.group_schema(&agg.input().schema())?;
    if !supports_blocked_group_values(&group_schema) {
        return Ok(false);
    }

    for agg_expr in agg.aggr_expr.iter() {
        if blocked_avg_state_layout(agg_expr)?.is_none() {
            return Ok(false);
        }
    }

    Ok(true)
}

impl HashAggregateAccumulator {
    fn new(
        arguments: Vec<Arc<dyn PhysicalExpr>>,
        filter: Option<Arc<dyn PhysicalExpr>>,
        accumulator: Box<dyn GroupsAccumulator>,
    ) -> Self {
        Self {
            arguments,
            filter,
            accumulator,
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<EvaluatedHashAggregateAccumulator> {
        let arguments = self
            .arguments
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .collect::<Result<_>>()?;

        let filter = self
            .filter
            .as_ref()
            .map(|filter| {
                filter
                    .evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .transpose()?;

        Ok(EvaluatedHashAggregateAccumulator { arguments, filter })
    }

    fn update_batch(
        &mut self,
        values: &EvaluatedHashAggregateAccumulator,
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        let filter = values.filter.as_ref().map(|filter| filter.as_boolean());
        self.accumulator.update_batch(
            &values.arguments,
            group_indices,
            filter,
            total_num_groups,
        )
    }

    fn merge_batch(
        &mut self,
        values: &EvaluatedHashAggregateAccumulator,
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        debug_assert!(values.filter.is_none());
        self.accumulator.merge_batch(
            &values.arguments,
            group_indices,
            None,
            total_num_groups,
        )
    }

    fn evaluate_final(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.accumulator.evaluate(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.accumulator.state(emit_to)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.accumulator.supports_convert_to_state()
    }

    fn null_arguments(&self, input_schema: &SchemaRef) -> Result<Vec<ArrayRef>> {
        self.arguments
            .iter()
            .map(|expr| {
                let data_type = expr.data_type(input_schema)?;
                Ok(new_null_array(&data_type, 1))
            })
            .collect()
    }
}

/// Hash table mode that consumes raw input rows and produces partial state.
struct RawPartial;

/// Hash table mode that consumes partial state and produces final values.
struct PartialFinal;

/// Hash table state for grouped raw-partial aggregation.
///
/// This owns the coupled state for:
/// - evaluating group keys,
/// - interning each distinct group,
/// - mapping each input row to its group index,
/// - evaluating aggregate inputs,
/// - updating per-group accumulator state.
struct AggregateHashTable<Mode> {
    /// Grouping and accumulator-specific timing metrics.
    group_by_metrics: GroupByMetrics,

    /// Raw input schema, used to evaluate expressions and synthesize empty
    /// grouping-set rows.
    input_schema: SchemaRef,

    /// Output schema: group columns followed by aggregate state or final values.
    output_schema: SchemaRef,

    /// Maximum rows per emitted output batch.
    batch_size: usize,

    /// True when group values and all accumulators use the same internal block
    /// size and can emit one block per output batch.
    blocked_output: bool,

    /// GROUP BY expressions evaluated for each input batch.
    group_by: Arc<PhysicalGroupBy>,

    /// Interned group keys. Accumulator state is stored separately by group index.
    group_values: Box<dyn GroupValues>,

    /// Group index for each row in the current input batch.
    ///
    /// Each value indexes into `group_values`, and the same index is used by every
    /// accumulator to update that group's aggregate state.
    batch_group_indices: Vec<usize>,

    /// One item per aggregate expression.
    ///
    /// Example: `COUNT(x), SUM(y)` creates two items. Each item owns the input
    /// expressions, optional filter, and accumulator state for all groups.
    accumulators: Vec<HashAggregateAccumulator>,

    /// Full output built once after input is exhausted.
    output_batch: Option<RecordBatch>,

    /// Offset of the next row to slice from `output_batch`.
    output_batch_offset: usize,

    /// True once all output rows have been emitted.
    output_finished: bool,

    _mode: PhantomData<Mode>,
}

impl<Mode> AggregateHashTable<Mode> {
    fn new_with_filters(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
        filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Self> {
        let input_schema = agg.input().schema();
        let aggregate_arguments = aggregate_expressions(
            &agg.aggr_expr,
            &agg.mode,
            agg.group_by.num_group_exprs(),
        )?;

        let group_schema = agg.group_by.group_schema(&input_schema)?;
        let can_use_blocked_output = can_use_blocked_hash_aggregate(agg)?;

        let blocked_group_values = if can_use_blocked_output {
            new_unordered_blocked_group_values(&group_schema, batch_size)?
        } else {
            None
        };
        let blocked_accumulators = if can_use_blocked_output {
            create_blocked_group_accumulators(&agg.aggr_expr, batch_size)?
        } else {
            None
        };

        let (group_values, accumulator_impls, blocked_output) =
            match (blocked_group_values, blocked_accumulators) {
                (Some(group_values), Some(accumulators)) => {
                    (group_values, accumulators, true)
                }
                _ => {
                    let group_values = new_unordered_group_values(group_schema)?;
                    let accumulators = agg
                        .aggr_expr
                        .iter()
                        .map(create_group_accumulator)
                        .collect::<Result<Vec<_>>>()?;
                    (group_values, accumulators, false)
                }
            };

        let accumulators: Vec<_> = aggregate_arguments
            .into_iter()
            .zip(filters)
            .zip(accumulator_impls)
            .map(|((arguments, filter), accumulator)| {
                HashAggregateAccumulator::new(arguments, filter, accumulator)
            })
            .collect();

        Ok(Self {
            group_by_metrics: GroupByMetrics::new(&agg.metrics, partition),
            input_schema,
            output_schema,
            batch_size,
            blocked_output,
            group_by: Arc::clone(&agg.group_by),
            group_values,
            batch_group_indices: Default::default(),
            accumulators,
            output_batch: None,
            output_batch_offset: 0,
            output_finished: false,
            _mode: PhantomData,
        })
    }

    fn evaluate_batch(&self, batch: &RecordBatch) -> Result<EvaluatedAggregateBatch> {
        let timer = self.group_by_metrics.time_calculating_group_ids.timer();
        // outer vec: one per each grouping set
        // inner vec: all group by exprs for the current grouping set
        let grouping_set_args = evaluate_group_by(&self.group_by, batch)?;
        drop(timer);

        let timer = self.group_by_metrics.aggregate_arguments_time.timer();
        // The evaluated args for each accumulator
        let accumulator_args = self
            .accumulators
            .iter()
            .map(|acc| acc.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        drop(timer);

        Ok(EvaluatedAggregateBatch {
            grouping_set_args,
            accumulator_args,
        })
    }

    fn next_output_batch_from(
        &mut self,
        build_output_batch: impl FnOnce(&mut Self) -> Result<Option<RecordBatch>>,
    ) -> Result<Option<RecordBatch>> {
        if self.output_finished {
            return Ok(None);
        }

        if self.output_batch.is_none() {
            self.output_batch = build_output_batch(self)?;
            self.output_batch_offset = 0;
        }

        let Some(batch) = self.output_batch.as_ref() else {
            self.output_finished = true;
            return Ok(None);
        };

        debug_assert!(self.batch_size > 0);
        let output_len = self
            .batch_size
            .max(1)
            .min(batch.num_rows() - self.output_batch_offset);
        let output = batch.slice(self.output_batch_offset, output_len);
        self.output_batch_offset += output_len;

        if self.output_batch_offset == batch.num_rows() {
            self.output_batch = None;
            self.output_batch_offset = 0;
            self.output_finished = true;
        }

        debug_assert!(output.num_rows() > 0);
        debug_assert!(output.num_rows() <= self.batch_size.max(1));
        Ok(Some(output))
    }

    fn next_blocked_output_batch_from(
        &mut self,
        build_output_batch: impl FnOnce(&mut Self) -> Result<Option<RecordBatch>>,
    ) -> Result<Option<RecordBatch>> {
        debug_assert!(self.blocked_output);

        if self.output_finished {
            return Ok(None);
        }

        let output = build_output_batch(self)?;
        if output.is_none() {
            self.output_finished = true;
        }

        Ok(output)
    }

    fn memory_size(&self) -> usize {
        let acc = self
            .accumulators
            .iter()
            .map(|acc| acc.accumulator.size())
            .sum::<usize>();
        let output = self
            .output_batch
            .as_ref()
            .map(RecordBatch::get_array_memory_size)
            .unwrap_or_default();

        acc + self.group_values.size()
            + self.batch_group_indices.allocated_size()
            + output
    }

    fn clear(&mut self) {
        self.group_values.clear_shrink(0);
        self.batch_group_indices.clear();
        self.batch_group_indices.shrink_to(0);
        self.output_batch = None;
        self.output_batch_offset = 0;
        self.output_finished = false;
    }
}

impl AggregateHashTable<RawPartial> {
    fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        let table = Self::new_with_filters(
            agg,
            partition,
            output_schema,
            batch_size,
            agg.filter_expr.iter().cloned().collect(),
        )?;

        if table
            .accumulators
            .iter()
            .all(|acc| acc.supports_convert_to_state())
        {
            let _skipped_aggregation_rows = MetricBuilder::new(&agg.metrics)
                .with_category(MetricCategory::Rows)
                .counter("skipped_aggregation_rows", partition);
        }

        Ok(table)
    }

    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;

        let timer = self.group_by_metrics.aggregation_time.timer();
        for group_values in &evaluated_batch.grouping_set_args {
            self.group_values
                .intern(group_values, &mut self.batch_group_indices)?;
            let group_indices = &self.batch_group_indices;
            let total_num_groups = self.group_values.len();

            for (acc, values) in self
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                acc.update_batch(values, group_indices, total_num_groups)?;
            }
        }
        drop(timer);

        Ok(())
    }

    fn next_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.blocked_output {
            self.next_blocked_output_batch_from(Self::build_blocked_output_batch)
        } else {
            self.next_output_batch_from(Self::build_output_batch)
        }
    }

    fn build_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.init_empty_grouping_sets()?;

        if self.group_values.is_empty() {
            return Ok(None);
        }

        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = self.group_values.emit(EmitTo::All)?;

        for acc in self.accumulators.iter_mut() {
            output.extend(acc.state(EmitTo::All)?);
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
        debug_assert!(batch.num_rows() > 0);
        drop(timer);
        Ok(Some(batch))
    }

    fn build_blocked_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.init_empty_grouping_sets()?;

        if self.group_values.is_empty() {
            return Ok(None);
        }

        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = self.group_values.emit(EmitTo::Block)?;

        for acc in self.accumulators.iter_mut() {
            output.extend(acc.state(EmitTo::Block)?);
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
        debug_assert!(batch.num_rows() > 0);
        debug_assert!(batch.num_rows() <= self.batch_size);
        drop(timer);
        Ok(Some(batch))
    }

    fn init_empty_grouping_sets(&mut self) -> Result<()> {
        if !self.group_by.has_grouping_set() || !self.group_values.is_empty() {
            return Ok(());
        }

        let max_ordinal = max_duplicate_ordinal(self.group_by.groups());
        let mut ordinals: HashMap<&[bool], usize> = HashMap::new();
        let group_schema = self.group_by.group_schema(&self.input_schema)?;
        let n_expr = self.group_by.expr().len();
        let mut any_interned = false;

        for group in self.group_by.groups() {
            let ordinal = {
                let entry = ordinals.entry(group.as_slice()).or_insert(0);
                let ordinal = *entry;
                *entry += 1;
                ordinal
            };

            if !group.iter().all(|&is_null| is_null) {
                continue;
            }

            let mut cols: Vec<ArrayRef> = group_schema
                .fields()
                .iter()
                .take(n_expr)
                .map(|field| new_null_array(field.data_type(), 1))
                .collect();
            cols.push(group_id_array(group, ordinal, max_ordinal, 1)?);

            self.group_values
                .intern(&cols, &mut self.batch_group_indices)?;
            any_interned = true;
        }

        if any_interned {
            let total_groups = self.group_values.len();
            let false_filter = BooleanArray::from(vec![false]);
            for acc in self.accumulators.iter_mut() {
                let null_args = acc.null_arguments(&self.input_schema)?;
                let values = EvaluatedHashAggregateAccumulator {
                    arguments: null_args,
                    filter: Some(Arc::new(false_filter.clone())),
                };
                acc.update_batch(&values, &[0], total_groups)?;
            }
        }

        Ok(())
    }
}

impl AggregateHashTable<PartialFinal> {
    fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_filters(
            agg,
            partition,
            output_schema,
            batch_size,
            vec![None; agg.aggr_expr.len()],
        )
    }

    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;

        let timer = self.group_by_metrics.aggregation_time.timer();
        for group_values in &evaluated_batch.grouping_set_args {
            self.group_values
                .intern(group_values, &mut self.batch_group_indices)?;
            let group_indices = &self.batch_group_indices;
            let total_num_groups = self.group_values.len();

            for (acc, values) in self
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                acc.merge_batch(values, group_indices, total_num_groups)?;
            }
        }
        drop(timer);

        Ok(())
    }

    fn next_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.blocked_output {
            self.next_blocked_output_batch_from(Self::build_blocked_output_batch)
        } else {
            self.next_output_batch_from(Self::build_output_batch)
        }
    }

    fn build_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.group_values.is_empty() {
            return Ok(None);
        }

        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = self.group_values.emit(EmitTo::All)?;

        for acc in self.accumulators.iter_mut() {
            output.push(acc.evaluate_final(EmitTo::All)?);
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
        debug_assert!(batch.num_rows() > 0);
        drop(timer);
        Ok(Some(batch))
    }

    fn build_blocked_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.group_values.is_empty() {
            return Ok(None);
        }

        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = self.group_values.emit(EmitTo::Block)?;

        for acc in self.accumulators.iter_mut() {
            output.push(acc.evaluate_final(EmitTo::Block)?);
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
        debug_assert!(batch.num_rows() > 0);
        debug_assert!(batch.num_rows() <= self.batch_size);
        drop(timer);
        Ok(Some(batch))
    }
}

/// Hash aggregate stream for grouped `AggregateMode::Partial`.
///
/// Input: raw rows
/// Output: partial state (e.g. for avg(x), it's sum(x), count(x))
pub(crate) struct RawPartialHashAggregateStream {
    // ========================================================================
    // PROPERTIES:
    // Initialized once for this input partition.
    // ========================================================================
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    // ========================================================================
    // STATE FLAGS:
    // Control whether the stream is reading input, emitting state, or done.
    // ========================================================================
    exec_state: ExecutionState,

    // ========================================================================
    // STATE BUFFERS:
    //
    // Hold intermediate groups and aggregate state while reading input.
    // Example: `SELECT z, COUNT(x), SUM(y) FROM t GROUP BY z` stores each distinct
    // `z` in `group_values` and keeps one partial-state accumulator for `COUNT(x)`
    // and one for `SUM(y)`.
    // ========================================================================
    /// Hash table and accumulator state for all groups seen so far.
    hash_table: AggregateHashTable<RawPartial>,

    // ========================================================================
    // EXECUTION RESOURCES:
    // Metrics and memory accounting for this stream.
    // ========================================================================
    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,
}

impl RawPartialHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, super::AggregateMode::Partial);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<RawPartial>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("RawPartialHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            exec_state: ExecutionState::ReadingInput,
            hash_table,
            baseline_metrics,
            reservation,
        })
    }
}

impl Stream for RawPartialHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            match &self.exec_state {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let result = self.hash_table.aggregate_batch(&batch);
                            timer.done();

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }

                            // TODO: impl memory-limited aggr, when OOM directly send
                            // partial state to final aggregate stage
                            if let Err(e) =
                                self.reservation.try_resize(self.hash_table.memory_size())
                            {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            let input_schema = self.input.schema();
                            self.input =
                                Box::pin(EmptyRecordBatchStream::new(input_schema));

                            self.exec_state = ExecutionState::ProducingOutput;
                        }
                    }
                }

                ExecutionState::ProducingOutput => {
                    let timer = elapsed_compute.timer();
                    let result = self.hash_table.next_output_batch();
                    timer.done();

                    match result {
                        Ok(Some(batch)) => {
                            let _ = self
                                .reservation
                                .try_resize(self.hash_table.memory_size());
                            debug_assert!(batch.num_rows() > 0);
                            return Poll::Ready(Some(Ok(
                                batch.record_output(&self.baseline_metrics)
                            )));
                        }
                        Ok(None) => {
                            let _ = self
                                .reservation
                                .try_resize(self.hash_table.memory_size());
                            self.exec_state = ExecutionState::Done;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                ExecutionState::Done => {
                    self.hash_table.clear();
                    let _ = self.reservation.try_resize(self.hash_table.memory_size());
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for RawPartialHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// `AggregateMode::FinalPartitioned`.
///
/// Input: partial state, such as `sum(x), count(x)` for `avg(x)`.
/// Output: final values, such as `avg(x)`.
pub(crate) struct PartialFinalHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Controls whether the stream is reading input, emitting output, or done.
    exec_state: ExecutionState,

    /// Hash table and accumulator state for all groups seen so far.
    hash_table: AggregateHashTable<PartialFinal>,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,
}

impl PartialFinalHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            super::AggregateMode::Final | super::AggregateMode::FinalPartitioned
        ));
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<PartialFinal>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("PartialFinalHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            exec_state: ExecutionState::ReadingInput,
            hash_table,
            baseline_metrics,
            reservation,
        })
    }
}

impl Stream for PartialFinalHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            match &self.exec_state {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let result = self.hash_table.aggregate_batch(&batch);
                            timer.done();

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }

                            if let Err(e) =
                                self.reservation.try_resize(self.hash_table.memory_size())
                            {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            let input_schema = self.input.schema();
                            self.input =
                                Box::pin(EmptyRecordBatchStream::new(input_schema));

                            self.exec_state = ExecutionState::ProducingOutput;
                        }
                    }
                }

                ExecutionState::ProducingOutput => {
                    let timer = elapsed_compute.timer();
                    let result = self.hash_table.next_output_batch();
                    timer.done();

                    match result {
                        Ok(Some(batch)) => {
                            let _ = self
                                .reservation
                                .try_resize(self.hash_table.memory_size());
                            debug_assert!(batch.num_rows() > 0);
                            return Poll::Ready(Some(Ok(
                                batch.record_output(&self.baseline_metrics)
                            )));
                        }
                        Ok(None) => {
                            let _ = self
                                .reservation
                                .try_resize(self.hash_table.memory_size());
                            self.exec_state = ExecutionState::Done;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                ExecutionState::Done => {
                    self.hash_table.clear();
                    let _ = self.reservation.try_resize(self.hash_table.memory_size());
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for PartialFinalHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::array::types::Int64Type;
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn blocked_group_values_emit_blocks() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Int64,
            false,
        )]));
        let mut group_values =
            new_unordered_blocked_group_values(&schema, 3)?.expect("blocked Int64");
        let values: ArrayRef =
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70]));
        let mut groups = vec![];

        group_values.intern(&[values], &mut groups)?;

        assert_eq!(groups, vec![0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(group_values.len(), 7);

        let block = group_values.emit(EmitTo::Block)?;
        assert_eq!(
            block[0].as_primitive::<Int64Type>().values().as_ref(),
            &[10, 20, 30]
        );

        let block = group_values.emit(EmitTo::Block)?;
        assert_eq!(
            block[0].as_primitive::<Int64Type>().values().as_ref(),
            &[40, 50, 60]
        );

        let block = group_values.emit(EmitTo::Block)?;
        assert_eq!(
            block[0].as_primitive::<Int64Type>().values().as_ref(),
            &[70]
        );
        assert!(group_values.is_empty());

        Ok(())
    }

    #[test]
    fn blocked_avg_state_emit_blocks() -> Result<()> {
        let mut accumulator =
            BlockedAvgGroupsAccumulator::new(AvgStateLayout::CountSumUInt64, 3);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            Some(4.0),
            Some(5.0),
            Some(6.0),
            Some(7.0),
        ]));
        let group_indices = vec![0, 1, 2, 3, 4, 5, 6];

        accumulator.update_batch(&[values], &group_indices, None, 7)?;

        let state = accumulator.state(EmitTo::Block)?;
        let counts = state[0].as_primitive::<UInt64Type>();
        let sums = state[1].as_primitive::<Float64Type>();
        assert_eq!(counts.values().as_ref(), &[1, 1, 0]);
        assert_eq!(sums.values().as_ref(), &[1.0, 2.0, 0.0]);
        assert!(counts.is_null(2));
        assert!(sums.is_null(2));

        let state = accumulator.state(EmitTo::Block)?;
        assert_eq!(
            state[0].as_primitive::<UInt64Type>().values().as_ref(),
            &[1, 1, 1]
        );
        assert_eq!(
            state[1].as_primitive::<Float64Type>().values().as_ref(),
            &[4.0, 5.0, 6.0]
        );

        let state = accumulator.state(EmitTo::Block)?;
        assert_eq!(
            state[0].as_primitive::<UInt64Type>().values().as_ref(),
            &[1]
        );
        assert_eq!(
            state[1].as_primitive::<Float64Type>().values().as_ref(),
            &[7.0]
        );

        Ok(())
    }

    #[test]
    fn blocked_avg_merge_evaluate_blocks() -> Result<()> {
        let mut accumulator =
            BlockedAvgGroupsAccumulator::new(AvgStateLayout::CountSumUInt64, 2);
        let counts: ArrayRef = Arc::new(UInt64Array::from(vec![2, 1, 3, 4, 5]));
        let sums: ArrayRef =
            Arc::new(Float64Array::from(vec![10.0, 4.0, 9.0, 20.0, 15.0]));
        let group_indices = vec![0, 1, 2, 3, 4];

        accumulator.merge_batch(&[counts, sums], &group_indices, None, 5)?;

        let output = accumulator.evaluate(EmitTo::Block)?;
        assert_eq!(
            output.as_primitive::<Float64Type>().values().as_ref(),
            &[5.0, 4.0]
        );

        let output = accumulator.evaluate(EmitTo::Block)?;
        assert_eq!(
            output.as_primitive::<Float64Type>().values().as_ref(),
            &[3.0, 5.0]
        );

        let output = accumulator.evaluate(EmitTo::Block)?;
        assert_eq!(
            output.as_primitive::<Float64Type>().values().as_ref(),
            &[3.0]
        );

        Ok(())
    }

    #[test]
    fn blocked_avg_spark_state_layout() -> Result<()> {
        let mut accumulator =
            BlockedAvgGroupsAccumulator::new(AvgStateLayout::SumCountInt64, 2);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0]));
        let group_indices = vec![0, 1, 2];

        accumulator.update_batch(&[values], &group_indices, None, 3)?;

        let state = accumulator.state(EmitTo::Block)?;
        assert_eq!(
            state[0].as_primitive::<Float64Type>().values().as_ref(),
            &[2.0, 4.0]
        );
        assert_eq!(
            state[1].as_primitive::<Int64Type>().values().as_ref(),
            &[1, 1]
        );

        let state = accumulator.state(EmitTo::Block)?;
        assert_eq!(
            state[0].as_primitive::<Float64Type>().values().as_ref(),
            &[6.0]
        );
        assert_eq!(state[1].as_primitive::<Int64Type>().values().as_ref(), &[1]);

        Ok(())
    }
}
