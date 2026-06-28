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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, PrimitiveArray, new_null_array};
use arrow::compute::take_arrays;
use arrow::datatypes::{SchemaRef, UInt32Type};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err, internal_err};

use crate::aggregates::group_values::new_group_values;
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::{AggregateExec, group_id_array, max_duplicate_ordinal};
use crate::hash_utils::create_hashes;
use crate::repartition::{
    PARTITIONED_AGGREGATION_NUM_PARTITIONS_KEY, PARTITIONED_AGGREGATION_PARTITION_KEY,
    REPARTITION_RANDOM_STATE,
};

use super::common::{
    AggregateHashTable, AggregateHashTableBuffer, AggregateHashTableState,
    EvaluatedAccumulatorArgs, HashAggregateAccumulator, PartialMarker, PartialSkipMarker,
    emit_to_for_batch_size,
};

/// Methods specific to the aggregate hash table used in the partial aggregation stage.
impl AggregateHashTable<PartialMarker> {
    pub(in crate::aggregates) fn new(
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
            agg.filter_expr.iter().cloned().collect(),
        )
    }

    /// Emits the next batch of aggregated group keys and aggregate states.
    ///
    /// The output batch size is determined by `self.batch_size`.
    ///
    /// Returns `Some(batch)` for each emitted batch, `None` when output is
    /// exhausted, and an internal error if polled in the `Building` state.
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        let output_schema = Arc::clone(&self.output_schema);
        let batch_size = self.batch_size;
        match &mut self.state {
            AggregateHashTableState::Outputting(state) => {
                if state.group_values.is_empty() {
                    self.state = AggregateHashTableState::Done;
                    return Ok(None);
                }

                let emit_to =
                    emit_to_for_batch_size(batch_size, state.group_values.len());
                let timer = self.group_by_metrics.emitting_time.timer();
                let mut output = state.group_values.emit(emit_to)?;

                for acc in state.accumulators.iter_mut() {
                    output.extend(acc.state(emit_to)?);
                }
                let done = state.group_values.is_empty();
                drop(timer);

                let batch = RecordBatch::try_new(output_schema, output)?;
                debug_assert!(batch.num_rows() > 0);
                if done {
                    self.state = AggregateHashTableState::Done;
                }
                Ok(Some(batch))
            }
            AggregateHashTableState::Done => Ok(None),
            AggregateHashTableState::Building(_) => {
                internal_err!("next_output_batch must be called in the outputting state")
            }
            AggregateHashTableState::OutputtingMaterializedFinal(_) => {
                internal_err!(
                    "partial aggregate output should not materialize final output"
                )
            }
        }
    }

    pub(in crate::aggregates) fn can_skip_aggregation(&self) -> bool {
        self.state
            .building()
            .accumulators
            .iter()
            .all(|acc| acc.supports_convert_to_state())
    }

    /// In skip-partial-aggregation optimization, when a decision has been made to skip
    /// partial stage, build a typed hash table only for aggregation state conversion
    /// row-by-row.
    pub(in crate::aggregates) fn partial_skip_table(
        &self,
    ) -> Result<AggregateHashTable<PartialSkipMarker>> {
        let state = self.state.building();
        let group_schema = state.group_by.group_schema(&self.input_schema)?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;
        let accumulators = state
            .accumulators
            .iter()
            .map(HashAggregateAccumulator::empty_like)
            .collect::<Result<Vec<_>>>()?;

        Ok(AggregateHashTable {
            group_by_metrics: self.group_by_metrics.clone(),
            input_schema: Arc::clone(&self.input_schema),
            output_schema: Arc::clone(&self.output_schema),
            batch_size: self.batch_size,
            state: AggregateHashTableState::Building(AggregateHashTableBuffer {
                group_by: Arc::clone(&state.group_by),
                group_values,
                batch_group_indices: Default::default(),
                accumulators,
            }),
            _mode: PhantomData,
        })
    }

    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        let state = self.state.building_mut();

        let _timer = self.group_by_metrics.aggregation_time.timer();
        for group_values in &evaluated_batch.grouping_set_args {
            state
                .group_values
                .intern(group_values, &mut state.batch_group_indices)?;
            let group_indices = &state.batch_group_indices;
            let total_num_groups = state.group_values.len();

            for (acc, values) in state
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                acc.update_batch(values, group_indices, total_num_groups)?;
            }
        }

        Ok(())
    }

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.init_empty_grouping_sets()?;
        self.start_outputting();
        Ok(())
    }

    /// Creates the required empty grouping-set rows when the input is empty.
    ///
    /// For example, this query must still produce one grand-total group even if
    /// `t` has no rows:
    ///
    /// ```sql
    /// SELECT COUNT(v)
    /// FROM t
    /// GROUP BY GROUPING SETS (());
    /// ```
    ///
    /// The synthetic row is filtered out before accumulator update so aggregates
    /// see the same state they would see for an empty input, rather than a real
    /// null-valued row.
    fn init_empty_grouping_sets(&mut self) -> Result<()> {
        let state = self.state.building_mut();
        if !state.group_by.has_grouping_set() || !state.group_values.is_empty() {
            return Ok(());
        }

        let max_ordinal = max_duplicate_ordinal(state.group_by.groups());
        let mut ordinals: HashMap<&[bool], usize> = HashMap::new();
        let group_schema = state.group_by.group_schema(&self.input_schema)?;
        let n_expr = state.group_by.expr().len();
        let mut any_interned = false;

        for group in state.group_by.groups() {
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

            state
                .group_values
                .intern(&cols, &mut state.batch_group_indices)?;
            any_interned = true;
        }

        if any_interned {
            let total_groups = state.group_values.len();
            let false_filter = BooleanArray::from(vec![false]);
            for acc in state.accumulators.iter_mut() {
                let null_args = acc.null_arguments(&self.input_schema)?;
                let values = EvaluatedAccumulatorArgs {
                    arguments: null_args,
                    filter: Some(Arc::new(false_filter.clone())),
                };
                acc.update_batch(&values, &[0], total_groups)?;
            }
        }

        Ok(())
    }
}

type PartitionRange = (usize, Range<usize>);

fn add_partitioned_aggregation_metadata(
    mut batch: RecordBatch,
    partition: usize,
    num_partitions: usize,
) -> RecordBatch {
    let metadata = batch.schema_metadata_mut();
    metadata.insert(
        PARTITIONED_AGGREGATION_PARTITION_KEY.to_string(),
        partition.to_string(),
    );
    metadata.insert(
        PARTITIONED_AGGREGATION_NUM_PARTITIONS_KEY.to_string(),
        num_partitions.to_string(),
    );
    batch
}

/// Hash tables for partial aggregation pre-partitioned by the final hash key.
pub(in crate::aggregates) struct PartitionedPartialHashAggregateTables {
    tables: Vec<AggregateHashTable<PartialMarker>>,
    next_output_partition: usize,
    hashes_buffer: Vec<u64>,
    partition_indices: Vec<Vec<u32>>,
    partition_ranges: Vec<PartitionRange>,
    reordered_indices: Vec<u32>,
}

impl PartitionedPartialHashAggregateTables {
    pub(in crate::aggregates) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: &SchemaRef,
        batch_size: usize,
        num_partitions: usize,
    ) -> Result<Self> {
        if num_partitions == 0 {
            return internal_err!(
                "PartitionedPartialHashAggregateTables requires at least one partition"
            );
        }

        let tables = (0..num_partitions)
            .map(|_| {
                AggregateHashTable::<PartialMarker>::new(
                    agg,
                    partition,
                    Arc::clone(output_schema),
                    batch_size,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            tables,
            next_output_partition: 0,
            hashes_buffer: vec![],
            partition_indices: vec![vec![]; num_partitions],
            partition_ranges: vec![],
            reordered_indices: vec![],
        })
    }

    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.tables[0].evaluate_batch(batch)?;
        assert_eq_or_internal_err!(
            evaluated_batch.grouping_set_args.len(),
            1,
            "partitioned partial aggregation requires a single grouping set"
        );
        let group_values = &evaluated_batch.grouping_set_args[0];

        self.hashes_buffer.clear();
        self.hashes_buffer.resize(batch.num_rows(), 0);
        create_hashes(
            group_values,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut self.hashes_buffer,
        )?;

        for indices in &mut self.partition_indices {
            indices.clear();
        }
        if self.tables.len().is_power_of_two() {
            let mask = self.tables.len() - 1;
            for (row, hash) in self.hashes_buffer.iter().enumerate() {
                self.partition_indices[(*hash as usize) & mask].push(row as u32);
            }
        } else {
            for (row, hash) in self.hashes_buffer.iter().enumerate() {
                let partition = (*hash as usize) % self.tables.len();
                self.partition_indices[partition].push(row as u32);
            }
        }

        partition_grouped_take(
            &mut self.partition_indices,
            &mut self.partition_ranges,
            &mut self.reordered_indices,
        );
        if self.reordered_indices.is_empty() {
            return Ok(());
        }

        let indices_array: PrimitiveArray<UInt32Type> =
            self.reordered_indices.iter().copied().collect();
        let reordered_group_values = take_group_values(group_values, &indices_array)?;
        let reordered_accumulator_args =
            take_accumulator_args(&evaluated_batch.accumulator_args, &indices_array)?;

        for (partition, range) in &self.partition_ranges {
            let partition_group_values =
                slice_group_values(&reordered_group_values, range);
            let partition_accumulator_args =
                slice_accumulator_args(&reordered_accumulator_args, range);
            self.tables[*partition].aggregate_evaluated_batch(
                &partition_group_values,
                &partition_accumulator_args,
            )?;
        }

        Ok(())
    }

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        for table in &mut self.tables {
            table.start_output()?;
        }
        Ok(())
    }

    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        let num_partitions = self.tables.len();
        for _ in 0..num_partitions {
            let partition = self.next_output_partition;
            self.next_output_partition =
                (self.next_output_partition + 1) % num_partitions;

            if let Some(batch) = self.tables[partition].next_output_batch()? {
                return Ok(Some(add_partitioned_aggregation_metadata(
                    batch,
                    partition,
                    num_partitions,
                )));
            }
        }

        Ok(None)
    }

    pub(in crate::aggregates) fn can_skip_aggregation(&self) -> bool {
        self.tables[0].can_skip_aggregation()
    }

    pub(in crate::aggregates) fn partial_skip_table(
        &self,
    ) -> Result<AggregateHashTable<PartialSkipMarker>> {
        self.tables[0].partial_skip_table()
    }

    pub(in crate::aggregates) fn memory_size(&self) -> usize {
        self.tables
            .iter()
            .map(AggregateHashTable::memory_size)
            .sum()
    }

    pub(in crate::aggregates) fn building_group_count(&self) -> usize {
        self.tables
            .iter()
            .map(AggregateHashTable::building_group_count)
            .sum()
    }

    pub(in crate::aggregates) fn is_building(&self) -> bool {
        self.tables.iter().all(AggregateHashTable::is_building)
    }

    pub(in crate::aggregates) fn is_done(&self) -> bool {
        self.tables.iter().all(AggregateHashTable::is_done)
    }
}

impl AggregateHashTable<PartialMarker> {
    fn aggregate_evaluated_batch(
        &mut self,
        group_values: &[ArrayRef],
        accumulator_args: &[EvaluatedAccumulatorArgs],
    ) -> Result<()> {
        let state = self.state.building_mut();

        let _timer = self.group_by_metrics.aggregation_time.timer();
        state
            .group_values
            .intern(group_values, &mut state.batch_group_indices)?;
        let group_indices = &state.batch_group_indices;
        let total_num_groups = state.group_values.len();

        for (acc, values) in state.accumulators.iter_mut().zip(accumulator_args.iter()) {
            acc.update_batch(values, group_indices, total_num_groups)?;
        }

        Ok(())
    }
}

fn partition_grouped_take(
    indices: &mut [Vec<u32>],
    partition_ranges: &mut Vec<PartitionRange>,
    reordered_indices: &mut Vec<u32>,
) {
    partition_ranges.clear();
    reordered_indices.clear();

    for (partition, partition_indices) in indices.iter_mut().enumerate() {
        if partition_indices.is_empty() {
            continue;
        }
        let start = reordered_indices.len();
        reordered_indices.extend_from_slice(partition_indices);
        let len = reordered_indices.len() - start;
        partition_ranges.push((partition, start..start + len));
        partition_indices.clear();
    }
}

fn take_group_values(
    group_values: &[ArrayRef],
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<Vec<ArrayRef>> {
    take_arrays(group_values, indices, None).map_err(Into::into)
}

fn take_accumulator_args(
    values: &[EvaluatedAccumulatorArgs],
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<Vec<EvaluatedAccumulatorArgs>> {
    values
        .iter()
        .map(|value| {
            Ok(EvaluatedAccumulatorArgs {
                arguments: take_arrays(&value.arguments, indices, None)?,
                filter: value
                    .filter
                    .as_ref()
                    .map(|filter| {
                        take_arrays(std::slice::from_ref(filter), indices, None)
                    })
                    .transpose()?
                    .map(|mut filter| filter.remove(0)),
            })
        })
        .collect()
}

fn slice_group_values(group_values: &[ArrayRef], range: &Range<usize>) -> Vec<ArrayRef> {
    group_values
        .iter()
        .map(|value| value.slice(range.start, range.len()))
        .collect()
}

fn slice_accumulator_args(
    values: &[EvaluatedAccumulatorArgs],
    range: &Range<usize>,
) -> Vec<EvaluatedAccumulatorArgs> {
    values
        .iter()
        .map(|value| EvaluatedAccumulatorArgs {
            arguments: slice_group_values(&value.arguments, range),
            filter: value
                .filter
                .as_ref()
                .map(|filter| filter.slice(range.start, range.len())),
        })
        .collect()
}

impl AggregateHashTable<PartialSkipMarker> {
    pub(in crate::aggregates) fn convert_batch_to_state(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let evaluated_batch = self.evaluate_batch(batch)?;

        assert_eq_or_internal_err!(
            evaluated_batch.grouping_set_args.len(),
            1,
            "group_values expected to have single element"
        );
        let mut output = evaluated_batch
            .grouping_set_args
            .into_iter()
            .next()
            .unwrap_or_default();

        let state = self.state.building_mut();
        for (acc, values) in state
            .accumulators
            .iter_mut()
            .zip(evaluated_batch.accumulator_args.iter())
        {
            output.extend(acc.convert_to_state(values)?);
        }

        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            output,
        )?)
    }
}
