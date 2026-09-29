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

//! Flat or blocked storage for the group keys and accumulator states of an
//! aggregate table.
//!
//! Every part of a table chooses its storage on its own, once, when the table
//! is created: the group keys use [`BlockedGroupValues`] when their type
//! supports it (see [`use_blocked_keys`]), and each aggregate uses a
//! [`BlockedGroupsAccumulator`] when it has one. Everything else keeps today's
//! flat [`GroupValues`] and [`GroupsAccumulator`], unchanged, so any mix of
//! flat and blocked parts works, and a table with no blocked part runs exactly
//! the flat code.
//!
//! Mixing needs two things:
//! * Group indices in both layouts: the keys produce their own layout, and
//!   [`GroupKeys`] converts them once per batch for the accumulators that use
//!   the other layout (see [`GroupKeys::indices_for`]).
//! * Aligned output: blocked parts emit one array per block, and flat parts
//!   emit one array that is sliced, without copying, at the same block
//!   boundaries (see [`materialize_batches`]).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_err};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::{
    BlockedEmitTo, BlockedGroupsAccumulator, BlocksIndex, EmitTo, GroupsAccumulator,
};

use crate::aggregates::AggregateExec;
use crate::aggregates::group_values::blocked::{
    BlockedGroupValues, new_blocked_group_values,
};
use crate::aggregates::group_values::{
    AccumulatorPhase, AggregateAccumulatorMetrics, GroupValues, new_group_values,
};
use crate::aggregates::order::GroupOrdering;

use super::common::{AggregateAccumulator, MaterializeAccumulatorFn};

/// Smallest block, in groups. Tables with fewer groups stay in a single,
/// flat block and run exactly the flat hot loops.
const MIN_BLOCK_SIZE: usize = 1 << 18;

/// Number of groups per block for every blocked part of a table.
///
/// At least `batch_size`, so an output batch never spans two blocks.
pub(in crate::aggregates) fn block_size(batch_size: usize) -> usize {
    batch_size.max(MIN_BLOCK_SIZE).next_power_of_two()
}

/// Returns `true` if the group keys of `agg` can be stored in blocks: a single
/// grouping set whose key has a [`BlockedGroupValues`] implementation.
pub(in crate::aggregates) fn use_blocked_keys(
    agg: &AggregateExec,
    group_schema: &SchemaRef,
    block_size: usize,
) -> bool {
    agg.group_by().is_single()
        && new_blocked_group_values(group_schema, block_size).is_some()
}

/// Group indices of one input batch, in the layout of one accumulator.
#[derive(Clone, Copy)]
pub(in crate::aggregates) enum GroupIndices<'a> {
    Flat(&'a [usize]),
    Blocked(&'a [BlocksIndex]),
}

/// Which groups to materialize, in the layout of one part of the table.
#[derive(Clone, Copy, Debug)]
pub(in crate::aggregates) enum Emit {
    Flat(EmitTo),
    Blocked(BlockedEmitTo),
}

enum KeyStorage {
    Flat(Box<dyn GroupValues>),
    Blocked(Box<dyn BlockedGroupValues>),
}

/// Group keys, and the group index of every row of the current input batch
/// in each layout the accumulators need.
pub(in crate::aggregates) struct GroupKeys {
    storage: KeyStorage,
    block_size: usize,
    /// Flat indices: written by flat keys, or converted for flat accumulators
    flat_indices: Vec<usize>,
    /// Blocked indices: written by blocked keys, or converted for blocked
    /// accumulators
    blocked_indices: Vec<BlocksIndex>,
    /// Whether some accumulator uses the layout the keys do not produce
    convert: bool,
}

impl GroupKeys {
    /// Creates blocked keys when `blocked` is set, flat keys otherwise.
    /// `accumulators` are the storages of the table's accumulators.
    pub(super) fn try_new(
        group_schema: SchemaRef,
        group_ordering: &GroupOrdering,
        blocked: bool,
        block_size: usize,
        accumulators: &[&AccumulatorStorage],
    ) -> Result<Self> {
        let storage = if blocked {
            match new_blocked_group_values(&group_schema, block_size) {
                Some(group_values) => KeyStorage::Blocked(group_values),
                None => {
                    return internal_err!(
                        "blocked group values are not supported for {group_schema:?}"
                    );
                }
            }
        } else {
            KeyStorage::Flat(new_group_values(group_schema, group_ordering)?)
        };
        let convert = accumulators
            .iter()
            .any(|acc| matches!(acc, AccumulatorStorage::Blocked(_)) != blocked);
        Ok(Self {
            storage,
            block_size,
            flat_indices: Vec::new(),
            blocked_indices: Vec::new(),
            convert,
        })
    }

    /// Assigns a group to each row of `cols`, see [`GroupValues::intern`],
    /// then converts the indices once for the accumulators using the other
    /// layout.
    pub(super) fn intern(&mut self, cols: &[ArrayRef]) -> Result<()> {
        match &mut self.storage {
            KeyStorage::Flat(group_values) => {
                group_values.intern(cols, &mut self.flat_indices)?;
                if self.convert {
                    BlocksIndex::from_flat_slice(
                        &self.flat_indices,
                        self.block_size,
                        &mut self.blocked_indices,
                    );
                }
            }
            KeyStorage::Blocked(group_values) => {
                group_values.intern(cols, &mut self.blocked_indices)?;
                if self.convert {
                    BlocksIndex::to_flat_slice(
                        &self.blocked_indices,
                        self.block_size,
                        &mut self.flat_indices,
                    );
                }
            }
        }
        Ok(())
    }

    /// Group of each row of the last interned batch, in the layout of
    /// `accumulator`.
    pub(super) fn indices_for(
        &self,
        accumulator: &AccumulatorStorage,
    ) -> GroupIndices<'_> {
        match accumulator {
            AccumulatorStorage::Flat(_) => GroupIndices::Flat(&self.flat_indices),
            AccumulatorStorage::Blocked(_) => {
                GroupIndices::Blocked(&self.blocked_indices)
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        match &self.storage {
            KeyStorage::Flat(group_values) => group_values.len(),
            KeyStorage::Blocked(group_values) => group_values.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match &self.storage {
            KeyStorage::Flat(group_values) => group_values.is_empty(),
            KeyStorage::Blocked(group_values) => group_values.is_empty(),
        }
    }

    /// Memory used by the keys and the scratch indices.
    pub(super) fn size(&self) -> usize {
        let group_values = match &self.storage {
            KeyStorage::Flat(group_values) => group_values.size(),
            KeyStorage::Blocked(group_values) => group_values.size(),
        };
        group_values
            + self.flat_indices.allocated_size()
            + self.blocked_indices.allocated_size()
    }

    /// Clears the keys and releases their memory, see
    /// [`GroupValues::clear_shrink`], and frees the scratch indices.
    pub(super) fn clear_shrink(&mut self, num_rows: usize) {
        match &mut self.storage {
            KeyStorage::Flat(group_values) => group_values.clear_shrink(num_rows),
            KeyStorage::Blocked(group_values) => group_values.clear_shrink(num_rows),
        }
        self.free_indices();
    }

    /// Frees the scratch indices, which are not needed once input is done.
    pub(super) fn free_indices(&mut self) {
        self.flat_indices = Vec::new();
        self.blocked_indices = Vec::new();
    }
}

/// Per-group state of one aggregate expression.
pub(in crate::aggregates) enum AccumulatorStorage {
    Flat(Box<dyn GroupsAccumulator>),
    Blocked(Box<dyn BlockedGroupsAccumulator>),
}

impl AccumulatorStorage {
    pub(super) fn size(&self) -> usize {
        match self {
            Self::Flat(acc) => acc.size(),
            Self::Blocked(acc) => acc.size(),
        }
    }

    /// No group indices, in this accumulator's layout.
    pub(super) fn no_groups(&self) -> GroupIndices<'static> {
        match self {
            Self::Flat(_) => GroupIndices::Flat(&[]),
            Self::Blocked(_) => GroupIndices::Blocked(&[]),
        }
    }

    fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked(_))
    }
}

/// One materialized output batch, and the memory released once all of it has
/// been handed out.
pub(in crate::aggregates) struct MaterializedBatch {
    pub(in crate::aggregates) batch: RecordBatch,
    /// Buffers only this batch holds, plus, for the last batch, the flat
    /// arrays all batches slice (they are released only with the last slice).
    /// Summed over all batches, this is the memory of the output, each buffer
    /// counted once.
    pub(in crate::aggregates) memory_size: usize,
}

/// Emits of a part of the table, as columns indexed `[block][column]`.
fn emit_part(
    blocked: bool,
    emit_to: EmitTo,
    block_size: usize,
    mut emit: impl FnMut(Emit) -> Result<Vec<Vec<ArrayRef>>>,
) -> Result<Vec<Vec<ArrayRef>>> {
    if !blocked {
        return emit(Emit::Flat(emit_to));
    }
    let mut blocks = Vec::new();
    for blocked_emit in BlockedEmitTo::from_emit_to(emit_to, block_size) {
        blocks.extend(emit(Emit::Blocked(blocked_emit))?);
    }
    Ok(blocks)
}

/// Removes the groups selected by `emit_to` from the keys and every
/// accumulator, and returns them as one [`MaterializedBatch`] per block.
///
/// Blocked parts emit one array per block. Flat parts emit a single array,
/// sliced without copying at the same block boundaries. With no blocked part
/// this returns one batch, exactly like the flat code. Blocks are never
/// concatenated.
pub(super) fn materialize_batches(
    keys: &mut GroupKeys,
    accumulators: &mut [AggregateAccumulator],
    emit_to: EmitTo,
    materialize_accumulator_fn: MaterializeAccumulatorFn,
    accumulator_phase: AccumulatorPhase,
    accumulator_metrics: &AggregateAccumulatorMetrics,
    schema: &SchemaRef,
) -> Result<Vec<MaterializedBatch>> {
    let block_size = keys.block_size;
    // One entry per part (keys first): whether it is blocked, and its columns
    // indexed `[block][column]` (a single "block" for flat parts)
    let mut parts = Vec::with_capacity(accumulators.len() + 1);
    let keys_blocked = matches!(keys.storage, KeyStorage::Blocked(_));
    parts.push((
        keys_blocked,
        emit_part(keys_blocked, emit_to, block_size, |emit| {
            match (&mut keys.storage, emit) {
                (KeyStorage::Flat(group_values), Emit::Flat(emit_to)) => {
                    Ok(vec![group_values.emit(emit_to)?])
                }
                (KeyStorage::Blocked(group_values), Emit::Blocked(emit_to)) => {
                    group_values.emit(emit_to)
                }
                (_, emit) => {
                    internal_err!("{emit:?} does not match the group key storage")
                }
            }
        })?,
    ));
    for (idx, acc) in accumulators.iter_mut().enumerate() {
        let blocked = acc.storage().is_blocked();
        let columns = accumulator_metrics.time(idx, accumulator_phase, || {
            emit_part(blocked, emit_to, block_size, |emit| {
                materialize_accumulator_fn(acc, emit)
            })
        })?;
        parts.push((blocked, columns));
    }

    // Row count of every output batch: the blocks of any blocked part (they
    // all have the same boundaries), or one batch if every part is flat
    let batch_lengths: Vec<usize> = match parts.iter().find(|(blocked, _)| *blocked) {
        Some((_, blocks)) => blocks.iter().map(|columns| columns[0].len()).collect(),
        None => vec![parts[0].1[0][0].len()],
    };

    let mut batches: Vec<(Vec<ArrayRef>, usize)> = batch_lengths
        .iter()
        .map(|_| (Vec::with_capacity(schema.fields().len()), 0))
        .collect();
    let mut shared_memory = 0;
    for (blocked, blocks) in parts {
        if blocked {
            if blocks.len() != batch_lengths.len() {
                return internal_err!(
                    "blocked parts emitted {} and {} blocks",
                    blocks.len(),
                    batch_lengths.len()
                );
            }
            for ((columns, memory), block) in batches.iter_mut().zip(blocks) {
                *memory += block
                    .iter()
                    .map(|a| a.get_array_memory_size())
                    .sum::<usize>();
                columns.extend(block);
            }
        } else {
            let [flat] = <[_; 1]>::try_from(blocks).map_err(|blocks| {
                datafusion_common::internal_datafusion_err!(
                    "flat part emitted {} blocks",
                    blocks.len()
                )
            })?;
            shared_memory += flat
                .iter()
                .map(|a| a.get_array_memory_size())
                .sum::<usize>();
            let mut offset = 0;
            for ((columns, _), &len) in batches.iter_mut().zip(&batch_lengths) {
                columns.extend(flat.iter().map(|array| array.slice(offset, len)));
                offset += len;
            }
        }
    }
    if let Some((_, memory)) = batches.last_mut() {
        *memory += shared_memory;
    }

    batches
        .into_iter()
        .map(|(columns, memory_size)| {
            Ok(MaterializedBatch {
                batch: RecordBatch::try_new(Arc::clone(schema), columns)?,
                memory_size,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion_expr::AggregateUDF;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::min_max::min_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;

    use super::*;
    use crate::aggregates::aggregate_hash_table::{AggregateHashTable, SingleMarker};
    use crate::aggregates::{AggregateMode, PhysicalGroupBy, create_schema};
    use crate::test::TestMemoryExec;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
    }

    /// `SELECT <aggregates>(k) FROM t GROUP BY k` in single mode.
    fn aggregate_exec(
        input: Vec<RecordBatch>,
        aggregates: &[Arc<AggregateUDF>],
    ) -> Result<AggregateExec> {
        let schema = schema();
        let aggr_expr = aggregates
            .iter()
            .map(|udaf| {
                AggregateExprBuilder::new(Arc::clone(udaf), vec![col("k", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias(udaf.name())
                    .build()
                    .map(Arc::new)
            })
            .collect::<Result<Vec<_>>>()?;
        let exec = TestMemoryExec::try_new(&[input], Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(col("k", &schema)?, "k".to_string())]),
            aggr_expr.clone(),
            vec![None; aggr_expr.len()],
            exec,
            schema,
        )
    }

    fn single_table(
        agg: &AggregateExec,
        batch_size: usize,
    ) -> Result<AggregateHashTable<SingleMarker>> {
        let state_schema = Arc::new(create_schema(
            &schema(),
            agg.group_by(),
            agg.aggr_expr(),
            AggregateMode::Partial,
        )?);
        AggregateHashTable::<SingleMarker>::new(
            agg,
            0,
            Arc::clone(&agg.schema),
            state_schema,
            batch_size,
        )
    }

    #[test]
    fn each_part_chooses_its_own_storage() -> Result<()> {
        let agg = aggregate_exec(vec![], &[count_udaf(), min_udaf()])?;
        let table = single_table(&agg, 8192)?;
        let state = table.state.building();
        assert!(matches!(state.keys.storage, KeyStorage::Blocked(_)));
        // count has a blocked accumulator, min does not (yet)
        assert!(state.accumulators[0].storage().is_blocked());
        assert!(!state.accumulators[1].storage().is_blocked());
        // min needs flat indices while the keys produce blocked ones
        assert!(state.keys.convert);
        assert_eq!(block_size(8192), MIN_BLOCK_SIZE);
        // The block always holds at least one output batch.
        assert_eq!(block_size(MIN_BLOCK_SIZE + 1), MIN_BLOCK_SIZE * 2);
        Ok(())
    }

    /// Blocked `count` and flat `min` over blocked keys: flat columns are
    /// sliced at the block boundaries, and their memory is counted once.
    #[test]
    fn mixed_storage_output_is_aligned_and_counted_once() -> Result<()> {
        let num_groups = MIN_BLOCK_SIZE + 10;
        let input = RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from_iter_values(0..num_groups as i64))],
        )?;
        let agg = aggregate_exec(vec![], &[count_udaf(), min_udaf()])?;
        let mut table = single_table(&agg, 8192)?;
        table.aggregate_batch(&input)?;

        let batches = table.take_state_batches()?;
        assert_eq!(
            batches
                .iter()
                .map(|b| b.batch.num_rows())
                .collect::<Vec<_>>(),
            vec![MIN_BLOCK_SIZE, 10]
        );
        for b in &batches {
            let keys = b.batch.column(0).as_primitive::<Int64Type>().values();
            let counts = b.batch.column(1).as_primitive::<Int64Type>().values();
            let mins = b.batch.column(2).as_primitive::<Int64Type>().values();
            assert!(counts.iter().all(|&c| c == 1));
            assert_eq!(keys, mins);
        }

        // Keys and counts are owned by each block, the flat mins are shared
        // by both batches and counted once, on the last batch
        let owned = |b: &MaterializedBatch| {
            b.batch.column(0).get_array_memory_size()
                + b.batch.column(1).get_array_memory_size()
        };
        let shared = batches[0].batch.column(2).get_array_memory_size();
        assert_eq!(batches[0].memory_size, owned(&batches[0]));
        assert_eq!(batches[1].memory_size, owned(&batches[1]) + shared);
        Ok(())
    }

    /// Output is materialized one block at a time, and each block's memory is
    /// released once all its slices are handed out.
    #[test]
    fn output_releases_state_block_by_block() -> Result<()> {
        let num_groups = MIN_BLOCK_SIZE + 10;
        let batch_size = 8192;
        let input = RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from_iter_values(0..num_groups as i64))],
        )?;
        let agg = aggregate_exec(vec![], &[count_udaf()])?;
        let mut table = single_table(&agg, batch_size)?;
        table.aggregate_batch(&input)?;
        table.start_output()?;
        let memory_while_building = table.memory_size();

        let mut keys = vec![];
        let mut memory_per_batch = vec![];
        while let Some(batch) = table.next_output_batch()? {
            assert!(batch.num_rows() <= batch_size);
            keys.extend(
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec(),
            );
            assert!(
                batch
                    .column(1)
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .all(|c| *c == 1)
            );
            memory_per_batch.push(table.memory_size());
        }

        keys.sort_unstable();
        assert_eq!(keys, (0..num_groups as i64).collect::<Vec<_>>());
        // While the first block is sliced, the table still holds the second
        // block; once the first block is handed out, only the small second
        // block (10 groups) remains.
        let first_block_batches = MIN_BLOCK_SIZE / batch_size;
        assert!(memory_per_batch[0] <= memory_while_building);
        assert!(
            memory_per_batch[first_block_batches] < memory_while_building / 4,
            "memory after the first block: {}, while building: {memory_while_building}",
            memory_per_batch[first_block_batches]
        );
        assert_eq!(*memory_per_batch.last().unwrap(), 0);
        Ok(())
    }
}
