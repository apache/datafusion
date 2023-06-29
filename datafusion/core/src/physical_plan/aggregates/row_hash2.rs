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

//! Hash aggregation through row format
//!
//! POC demonstration of GroupByHashApproach

use datafusion_physical_expr::GroupsAccumulator;
use log::info;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use ahash::RandomState;
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_physical_expr::hash_utils::create_hashes;
use futures::ready;
use futures::stream::{Stream, StreamExt};

use crate::physical_plan::aggregates::{
    evaluate_group_by, evaluate_many, evaluate_optional, group_schema, AggregateMode,
    PhysicalGroupBy,
};
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{aggregates, AggregateExpr, PhysicalExpr};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use arrow::array::*;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use hashbrown::raw::RawTable;

#[derive(Debug, Clone)]
/// This object tracks the aggregation phase (input/output)
pub(crate) enum ExecutionState {
    ReadingInput,
    /// When producing output, the remaining rows to output are stored
    /// here and are sliced off as needed in batch_size chunks
    ProducingOutput(RecordBatch),
    Done,
}

use super::AggregateExec;

/// Grouping aggregate
///
/// For each aggregation entry, we use:
/// - [Arrow-row] represents grouping keys for fast hash computation and comparison directly on raw bytes.
/// - [GroupsAccumulator] to store per group aggregates
///
/// The architecture is the following:
///
/// TODO
///
/// [WordAligned]: datafusion_row::layout
pub(crate) struct GroupedHashAggregateStream2 {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: AggregateMode,

    /// Accumulators, one for each `AggregateExpr` in the query
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    /// Arguments expressionf or each accumulator
    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    /// Filter expression to evaluate for each aggregate
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,

    /// Converter for each row
    row_converter: RowConverter,
    group_by: PhysicalGroupBy,

    /// The memory reservation for this grouping
    reservation: MemoryReservation,

    /// Logically maps group values to a group_index `group_states`
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, index into `group_states`)
    map: RawTable<(u64, usize)>,

    /// The actual group by values, stored in arrow Row format
    /// the index of group_by_values is the index
    /// https://github.com/apache/arrow-rs/issues/4466
    group_by_values: Vec<OwnedRow>,

    /// scratch space for the current Batch / Aggregate being
    /// processed. Saved here to avoid reallocations
    current_group_indices: Vec<usize>,

    /// generating input/output?
    exec_state: ExecutionState,

    baseline_metrics: BaselineMetrics,

    random_state: RandomState,
    /// size to be used for resulting RecordBatches
    batch_size: usize,
}

impl GroupedHashAggregateStream2 {
    /// Create a new GroupedHashAggregateStream
    pub fn new(
        agg: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        info!("Creating GroupedHashAggregateStream2");
        let agg_schema = Arc::clone(&agg.schema);
        let agg_group_by = agg.group_by.clone();
        let agg_filter_expr = agg.filter_expr.clone();

        let batch_size = context.session_config().batch_size();
        let input = agg.input.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        let timer = baseline_metrics.elapsed_compute().timer();

        let mut aggregate_exprs = vec![];
        let mut aggregate_arguments = vec![];

        // The expressions to evaluate the batch, one vec of expressions per aggregation.
        // Assuming create_schema() always puts group columns in front of aggregation columns, we set
        // col_idx_base to the group expression count.

        let all_aggregate_expressions = aggregates::aggregate_expressions(
            &agg.aggr_expr,
            &agg.mode,
            agg_group_by.expr.len(),
        )?;
        let filter_expressions = match agg.mode {
            AggregateMode::Partial | AggregateMode::Single => agg_filter_expr,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; agg.aggr_expr.len()]
            }
        };

        for (agg_expr, agg_args) in agg
            .aggr_expr
            .iter()
            .zip(all_aggregate_expressions.into_iter())
        {
            aggregate_exprs.push(agg_expr.clone());
            aggregate_arguments.push(agg_args);
        }

        let accumulators = create_accumulators(aggregate_exprs)?;

        let group_schema = group_schema(&agg_schema, agg_group_by.expr.len());
        let row_converter = RowConverter::new(
            group_schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let name = format!("GroupedHashAggregateStream2[{partition}]");
        let reservation = MemoryConsumer::new(name).register(context.memory_pool());
        let map = RawTable::with_capacity(0);
        let group_by_values = vec![];
        let current_group_indices = vec![];

        timer.done();

        let exec_state = ExecutionState::ReadingInput;

        Ok(GroupedHashAggregateStream2 {
            schema: agg_schema,
            input,
            mode: agg.mode,
            accumulators,
            aggregate_arguments,
            filter_expressions,
            row_converter,
            group_by: agg_group_by,
            reservation,
            map,
            group_by_values,
            current_group_indices,
            exec_state,
            baseline_metrics,
            random_state: Default::default(),
            batch_size,
        })
    }
}

/// Crate a `GroupsAccumulator` for each of the aggregate_exprs to hold the aggregation state
fn create_accumulators(
    aggregate_exprs: Vec<Arc<dyn AggregateExpr>>,
) -> Result<Vec<Box<dyn GroupsAccumulator>>> {
    info!("Creating accumulator for {aggregate_exprs:#?}");
    aggregate_exprs
        .into_iter()
        .map(|agg_expr| agg_expr.create_groups_accumulator())
        .collect()
}

impl Stream for GroupedHashAggregateStream2 {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            let exec_state = self.exec_state.clone();
            match exec_state {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        // new batch to aggregate
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let result = self.group_aggregate_batch(batch);
                            timer.done();

                            // allocate memory
                            // This happens AFTER we actually used the memory, but simplifies the whole accounting and we are OK with
                            // overshooting a bit. Also this means we either store the whole record batch or not.
                            let result = result.and_then(|allocated| {
                                self.reservation.try_grow(allocated)
                            });

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        // inner had error, return to caller
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        // inner is done, producing output
                        None => {
                            let timer = elapsed_compute.timer();
                            match self.create_batch_from_map() {
                                Ok(batch) => {
                                    self.exec_state =
                                        ExecutionState::ProducingOutput(batch)
                                }
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                            timer.done();
                        }
                    }
                }

                ExecutionState::ProducingOutput(batch) => {
                    // slice off a part of the batch, if needed
                    let output_batch = if batch.num_rows() <= self.batch_size {
                        self.exec_state = ExecutionState::Done;
                        batch
                    } else {
                        // output first batch_size rows
                        let num_remaining = batch.num_rows() - self.batch_size;
                        let remaining = batch.slice(self.batch_size, num_remaining);
                        self.exec_state = ExecutionState::ProducingOutput(remaining);
                        batch.slice(0, self.batch_size)
                    };
                    return Poll::Ready(Some(Ok(
                        output_batch.record_output(&self.baseline_metrics)
                    )));
                }

                ExecutionState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for GroupedHashAggregateStream2 {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl GroupedHashAggregateStream2 {
    /// Update self.aggr_state based on the group_by values (result of evalauting the group_by_expressions)
    ///
    /// At the return of this function,
    /// `self.aggr_state.current_group_indices` has the correct
    /// group_index for each row in the group_values
    fn update_group_state(
        &mut self,
        group_values: &[ArrayRef],
        allocated: &mut usize,
    ) -> Result<()> {
        // Convert the group keys into the row format
        let group_rows = self.row_converter.convert_columns(group_values)?;
        let n_rows = group_rows.num_rows();
        // 1.1 construct the key from the group values
        // 1.2 construct the mapping key if it does not exist

        // tracks to which group each of the input rows belongs
        let group_indices = &mut self.current_group_indices;
        group_indices.clear();

        // 1.1 Calculate the group keys for the group values
        let mut batch_hashes = vec![0; n_rows];
        create_hashes(group_values, &self.random_state, &mut batch_hashes)?;

        for (row, hash) in batch_hashes.into_iter().enumerate() {
            let entry = self.map.get_mut(hash, |(_hash, group_idx)| {
                // verify that a group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)

                // TODO update *allocated based on size of the row
                // that was just pushed into
                // aggr_state.group_by_values
                group_rows.row(row) == self.group_by_values[*group_idx].row()
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Add new entry to aggr_state and save newly created index
                    let group_idx = self.group_by_values.len();
                    self.group_by_values.push(group_rows.row(row).owned());

                    // for hasher function, use precomputed hash value
                    self.map.insert_accounted(
                        (hash, group_idx),
                        |(hash, _group_index)| *hash,
                        allocated,
                    );
                    group_idx
                }
            };
            group_indices.push_accounted(group_idx, allocated);
        }
        Ok(())
    }

    /// Perform group-by aggregation for the given [`RecordBatch`].
    ///
    /// If successful, returns the additional amount of memory, in
    /// bytes, that were allocated during this process.
    ///
    fn group_aggregate_batch(&mut self, batch: RecordBatch) -> Result<usize> {
        // Evaluate the grouping expressions:
        let group_by_values = evaluate_group_by(&self.group_by, &batch)?;

        // Keep track of memory allocated:
        let mut allocated = 0usize;

        // Evaluate the aggregation expressions.
        let input_values = evaluate_many(&self.aggregate_arguments, &batch)?;
        // Evalaute the filter expressions, if any, against the inputs
        let filter_values = evaluate_optional(&self.filter_expressions, &batch)?;

        let row_converter_size_pre = self.row_converter.size();
        for group_values in &group_by_values {
            // calculate the group indicies for each input row
            self.update_group_state(group_values, &mut allocated)?;
            let group_indices = &self.current_group_indices;

            // Gather the inputs to call the actual aggregation
            let t = self
                .accumulators
                .iter_mut()
                .zip(input_values.iter())
                .zip(filter_values.iter());

            let total_num_groups = self.group_by_values.len();

            for ((acc, values), opt_filter) in t {
                let acc_size_pre = acc.size();
                let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());

                match self.mode {
                    AggregateMode::Partial | AggregateMode::Single => {
                        acc.update_batch(
                            values,
                            &group_indices,
                            opt_filter,
                            total_num_groups,
                        )?;
                    }
                    AggregateMode::FinalPartitioned | AggregateMode::Final => {
                        // if aggregation is over intermediate states,
                        // use merge
                        acc.merge_batch(
                            values,
                            &group_indices,
                            opt_filter,
                            total_num_groups,
                        )?;
                    }
                }

                allocated += acc.size().saturating_sub(acc_size_pre);
            }
        }
        allocated += self
            .row_converter
            .size()
            .saturating_sub(row_converter_size_pre);

        Ok(allocated)
    }
}

impl GroupedHashAggregateStream2 {
    /// Create an output RecordBatch with all group keys and accumulator states/values
    fn create_batch_from_map(&mut self) -> Result<RecordBatch> {
        if self.group_by_values.is_empty() {
            let schema = self.schema.clone();
            return Ok(RecordBatch::new_empty(schema));
        }

        // First output rows are the groups
        let groups_rows = self.group_by_values.iter().map(|owned_row| owned_row.row());

        let mut output: Vec<ArrayRef> = self.row_converter.convert_rows(groups_rows)?;

        // Next output the accumulators
        for acc in self.accumulators.iter_mut() {
            match self.mode {
                AggregateMode::Partial => output.extend(acc.state()?),
                AggregateMode::Final
                | AggregateMode::FinalPartitioned
                | AggregateMode::Single => output.push(acc.evaluate()?),
            }
        }

        Ok(RecordBatch::try_new(self.schema.clone(), output)?)
    }
}
