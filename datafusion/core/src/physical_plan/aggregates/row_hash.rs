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

use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use ahash::RandomState;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};

use crate::error::Result;
use crate::physical_plan::aggregates::{evaluate, evaluate_many, AggregateMode};
use crate::physical_plan::hash_utils::create_row_hashes;
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{aggregates, AggregateExpr, PhysicalExpr};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};

use arrow::datatypes::Schema;
use arrow::{array::ArrayRef, compute};
use arrow::{
    array::{Array, UInt32Builder},
    error::{ArrowError, Result as ArrowResult},
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_row::layout::RowLayout;
use datafusion_row::writer::{write_row, RowWriter};
use datafusion_row::RowType;
use hashbrown::raw::RawTable;

/*
The architecture is the following:

1. An accumulator has state that is updated on each batch.
2. At the end of the aggregation (e.g. end of batches in a partition), the accumulator converts its state to a RecordBatch of a single row
3. The RecordBatches of all accumulators are merged (`concatenate` in `rust/arrow`) together to a single RecordBatch.
4. The state's RecordBatch is `merge`d to a new state
5. The state is mapped to the final value

Why:

* Accumulators' state can be statically typed, but it is more efficient to transmit data from the accumulators via `Array`
* The `merge` operation must have access to the state of the aggregators because it uses it to correctly merge
* It uses Arrow's native dynamically typed object, `Array`.
* Arrow shines in batch operations and both `merge` and `concatenate` of uniform types are very performant.

Example: average

* the state is `n: u32` and `sum: f64`
* For every batch, we update them accordingly.
* At the end of the accumulation (of a partition), we convert `n` and `sum` to a RecordBatch of 1 row and two columns: `[n, sum]`
* The RecordBatch is (sent back / transmitted over network)
* Once all N record batches arrive, `merge` is performed, which builds a RecordBatch with N rows and 2 columns.
* Finally, `get_value` returns an array with one entry computed from the state
*/
pub(crate) struct GroupedRowHashAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: AggregateMode,
    accumulators: Accumulators,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    group_expr: Vec<Arc<dyn PhysicalExpr>>,

    group_schema: SchemaRef,
    aggr_schema: SchemaRef,
    aggr_layout: RowLayout,
    aggr_buffer_width: usize,

    baseline_metrics: BaselineMetrics,
    random_state: RandomState,
    finished: bool,
}

fn create_separate_schema(schema: &Schema, group_count: usize) -> (SchemaRef, SchemaRef) {
    let (group_fields, aggr_fields) = schema.fields().split_at(group_count);
    (
        Arc::new(Schema::new(group_fields.to_vec())),
        Arc::new(Schema::new(aggr_fields.to_vec())),
    )
}

impl GroupedRowHashAggregateStream {
    /// Create a new GroupedRowHashAggregateStream
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Result<Self> {
        let timer = baseline_metrics.elapsed_compute().timer();

        // The expressions to evaluate the batch, one vec of expressions per aggregation.
        // Assume create_schema() always put group columns in front of aggr columns, we set
        // col_idx_base to group expression count.
        let aggregate_expressions =
            aggregates::aggregate_expressions(&aggr_expr, &mode, group_expr.len())?;

        let (group_schema, aggr_schema) =
            create_separate_schema(&schema, group_expr.len());
        let aggr_layout = RowLayout::new(&aggr_schema, RowType::WordAligned);
        let aggr_buffer_width = aggr_layout.fixed_part_width();
        timer.done();

        Ok(Self {
            schema,
            mode,
            input,
            aggr_expr,
            group_expr,
            group_schema,
            aggr_schema,
            aggr_layout,
            aggr_buffer_width,
            baseline_metrics,
            aggregate_expressions,
            accumulators: Default::default(),
            random_state: Default::default(),
            finished: false,
        })
    }
}

impl Stream for GroupedRowHashAggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.finished {
            return Poll::Ready(None);
        }

        let elapsed_compute = this.baseline_metrics.elapsed_compute();

        loop {
            let result = match ready!(this.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = elapsed_compute.timer();
                    let result = group_aggregate_batch(
                        &this.mode,
                        &this.random_state,
                        &this.group_expr,
                        &this.aggr_expr,
                        &this.group_schema,
                        &this.aggr_schema,
                        &this.aggr_layout,
                        this.aggr_buffer_width,
                        batch,
                        &mut this.accumulators,
                        &this.aggregate_expressions,
                    );

                    timer.done();

                    match result {
                        Ok(_) => continue,
                        Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
                    }
                }
                Some(Err(e)) => Err(e),
                None => {
                    this.finished = true;
                    let timer = this.baseline_metrics.elapsed_compute().timer();
                    let result = create_batch_from_map(
                        &this.mode,
                        &this.accumulators,
                        this.group_expr.len(),
                        &this.schema,
                    )
                    .record_output(&this.baseline_metrics);

                    timer.done();
                    result
                }
            };

            this.finished = true;
            return Poll::Ready(Some(result));
        }
    }
}

impl RecordBatchStream for GroupedRowHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// TODO: Make this a member function of [`GroupedRowHashAggregateStream`]
fn group_aggregate_batch(
    mode: &AggregateMode,
    random_state: &RandomState,
    group_expr: &[Arc<dyn PhysicalExpr>],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    group_schema: &Schema,
    aggr_schema: &Schema,
    aggr_row_layout: &RowLayout,
    aggr_buffer_width: usize,
    batch: RecordBatch,
    accumulators: &mut Accumulators,
    aggregate_expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<()> {
    // evaluate the grouping expressions
    let group_values = evaluate(group_expr, &batch)?;
    let group_rows: Vec<Vec<u8>> = create_group_rows(group_values, group_schema);

    // evaluate the aggregation expressions.
    // We could evaluate them after the `take`, but since we need to evaluate all
    // of them anyways, it is more performant to do it while they are together.
    let aggr_input_values = evaluate_many(aggregate_expressions, &batch)?;

    // 1.1 construct the key from the group values
    // 1.2 construct the mapping key if it does not exist
    // 1.3 add the row' index to `indices`

    // track which entries in `accumulators` have rows in this batch to aggregate
    let mut groups_with_rows = vec![];

    // 1.1 Calculate the group keys for the group values
    let mut batch_hashes = vec![0; batch.num_rows()];
    create_row_hashes(&group_rows, random_state, &mut batch_hashes)?;

    for (row, hash) in batch_hashes.into_iter().enumerate() {
        let Accumulators { map, group_states } = accumulators;

        let entry = map.get_mut(hash, |(_hash, group_idx)| {
            // verify that a group that we are inserting with hash is
            // actually the same key value as the group in
            // existing_idx  (aka group_values @ row)
            let group_state = &group_states[*group_idx];
            group_rows[row] == group_state.group_by_values
        });

        match entry {
            // Existing entry for this group value
            Some((_hash, group_idx)) => {
                let group_state = &mut group_states[*group_idx];
                // 1.3
                if group_state.indices.is_empty() {
                    groups_with_rows.push(*group_idx);
                };
                group_state.indices.push(row as u32); // remember this row
            }
            //  1.2 Need to create new entry
            None => {
                // Add new entry to group_states and save newly created index
                let group_state = RowGroupState {
                    group_by_values: group_rows[row].clone(),
                    aggregation_buffer: Vec::with_capacity(aggr_buffer_width),
                    indices: vec![row as u32], // 1.3
                };
                let group_idx = group_states.len();
                group_states.push(group_state);
                groups_with_rows.push(group_idx);

                // for hasher function, use precomputed hash value
                map.insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);
            }
        };
    }

    // Collect all indices + offsets based on keys in this vec
    let mut batch_indices: UInt32Builder = UInt32Builder::new(0);
    let mut offsets = vec![0];
    let mut offset_so_far = 0;
    for group_idx in groups_with_rows.iter() {
        let indices = &accumulators.group_states[*group_idx].indices;
        batch_indices.append_slice(indices)?;
        offset_so_far += indices.len();
        offsets.push(offset_so_far);
    }
    let batch_indices = batch_indices.finish();

    // `Take` all values based on indices into Arrays
    let values: Vec<Vec<Arc<dyn Array>>> = aggr_input_values
        .iter()
        .map(|array| {
            array
                .iter()
                .map(|array| {
                    compute::take(
                        array.as_ref(),
                        &batch_indices,
                        None, // None: no index check
                    )
                    .unwrap()
                })
                .collect()
            // 2.3
        })
        .collect();

    // 2.1 for each key in this batch
    // 2.2 for each aggregation
    // 2.3 `slice` from each of its arrays the keys' values
    // 2.4 update / merge the accumulator with the values
    // 2.5 clear indices
    groups_with_rows
        .iter()
        .zip(offsets.windows(2))
        .try_for_each(|(group_idx, offsets)| {
            let group_state = &mut accumulators.group_states[*group_idx];
            // 2.2
            group_state
                .accumulator_set
                .iter_mut()
                .zip(values.iter())
                .map(|(accumulator, aggr_array)| {
                    (
                        accumulator,
                        aggr_array
                            .iter()
                            .map(|array| {
                                // 2.3
                                array.slice(offsets[0], offsets[1] - offsets[0])
                            })
                            .collect::<Vec<ArrayRef>>(),
                    )
                })
                .try_for_each(|(accumulator, values)| match mode {
                    AggregateMode::Partial => accumulator.update_batch(&values),
                    AggregateMode::FinalPartitioned | AggregateMode::Final => {
                        // note: the aggregation here is over states, not values, thus the merge
                        accumulator.merge_batch(&values)
                    }
                })
                // 2.5
                .and({
                    group_state.indices.clear();
                    Ok(())
                })
        })?;

    Ok(())
}

/// The state that is built for each output group.
#[derive(Debug)]
struct RowGroupState {
    /// The actual group by values, stored sequentially
    group_by_values: Vec<u8>,

    // Accumulator state, stored sequentially
    aggregation_buffer: Vec<u8>,

    /// scratch space used to collect indices for input rows in a
    /// bach that have values to aggregate. Reset on each batch
    indices: Vec<u32>,
}

/// The state of all the groups
#[derive(Default)]
struct Accumulators {
    /// Logically maps group values to an index in `group_states`
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, index into `group_states`)
    map: RawTable<(u64, usize)>,

    /// State for each group
    group_states: Vec<RowGroupState>,
}

impl std::fmt::Debug for Accumulators {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // hashes are not store inline, so could only get values
        let map_string = "RawTable";
        f.debug_struct("RowAccumulators")
            .field("map", &map_string)
            .field("row_group_states", &self.group_states)
            .finish()
    }
}

/// Create grouping rows
fn create_group_rows(arrays: Vec<ArrayRef>, schema: &Schema) -> Vec<Vec<u8>> {
    let mut writer = RowWriter::new(schema, RowType::Compact);
    let mut results = vec![];
    for cur_row in 0..arrays[0].len() {
        write_row(&mut writer, cur_row, schema, &arrays);
        results.push(writer.get_row().to_vec());
        writer.reset()
    }
    results
}
