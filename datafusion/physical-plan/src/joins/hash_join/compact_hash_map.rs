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

//! Adapt hash lookup capacity to the build keys while retaining every row index.

use std::fmt;
use std::mem::size_of;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::{NullEquality, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use hashbrown::HashTable;

use crate::joins::join_hash_map::update_from_iter;
use crate::joins::utils::matchable_join_keys;

/// Bound hash scratch and the number of potentially new hashes per insertion.
const HASH_BUILD_CHUNK_ROWS: usize = 8192;

// Only flat column arrays can be sliced without repeatedly hashing unused
// dictionary values or changing the input batch seen by computed expressions.
fn is_flat_join_type(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Null
                | DataType::Boolean
                | DataType::FixedSizeBinary(_)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Utf8View
                | DataType::BinaryView
        )
}

// Start with one chunk of buckets so low-cardinality builds stay small. On
// growth, try the row-count capacity once to avoid repeated rehashing of unique
// keys. If it cannot fit, continue growing by observed hashes plus the next chunk.
#[expect(clippy::type_complexity)]
pub(super) fn build_compact_hash_map<T>(
    batches: &[RecordBatch],
    on: &[PhysicalExprRef],
    num_rows: usize,
    random_state: &RandomState,
    null_equality: NullEquality,
    reservation: &MemoryReservation,
    peak: &mut usize,
) -> Result<(HashTable<(u64, T)>, Vec<T>)>
where
    T: Copy + Default + TryFrom<usize> + PartialOrd,
    <T as TryFrom<usize>>::Error: fmt::Debug,
{
    let initial_reserved = reservation.size();
    let fixed_bytes = size_of::<HashTable<(u64, T)>>() + size_of::<Vec<T>>();
    let chain_bytes = num_rows.checked_mul(size_of::<T>()).ok_or_else(|| {
        datafusion_common::exec_datafusion_err!("Hash join row-index size overflow")
    })?;
    reservation.try_grow(fixed_bytes + chain_bytes)?;
    *peak = (*peak).max(reservation.size() - initial_reserved);
    let mut next = vec![T::default(); num_rows];
    let mut table = HashTable::new();
    let scratch = reservation.new_empty();
    let max_batch_rows = batches.iter().map(RecordBatch::num_rows).max().unwrap_or(0);
    let chunk_rows = max_batch_rows.min(HASH_BUILD_CHUNK_ROWS);
    let mut hash_chunk_rows = chunk_rows;
    if let Some(batch) = batches.first() {
        for expr in on {
            if !expr.is::<Column>()
                || !is_flat_join_type(&expr.data_type(&batch.schema())?)
            {
                // Slicing a dictionary retains all its values. Hash complex keys
                // and evaluate computed keys once per original batch.
                hash_chunk_rows = max_batch_rows;
                break;
            }
        }
    }
    scratch.try_grow(hash_chunk_rows * size_of::<u64>())?;
    let mut hashes = vec![0; hash_chunk_rows];
    *peak = (*peak).max(reservation.size() - initial_reserved + scratch.size());
    let mut tried_preallocation = false;
    'build: loop {
        let mut offset = 0;
        for batch in batches.iter().rev() {
            for hash_start in (0..batch.num_rows()).step_by(hash_chunk_rows.max(1)).rev()
            {
                let hash_rows = (batch.num_rows() - hash_start).min(hash_chunk_rows);
                let chunk = (hash_rows < batch.num_rows())
                    .then(|| batch.slice(hash_start, hash_rows));
                let keys =
                    evaluate_expressions_to_arrays(on, chunk.as_ref().unwrap_or(batch))?;
                hashes[..hash_rows].fill(0);
                let hashes =
                    create_hashes(&keys, random_state, &mut hashes[..hash_rows])?;
                let valid = matchable_join_keys(&keys, null_equality);
                for start in (0..hash_rows).step_by(HASH_BUILD_CHUNK_ROWS).rev() {
                    let rows = (hash_rows - start).min(HASH_BUILD_CHUNK_ROWS);
                    let hashes = &hashes[start..start + rows];
                    let valid = valid.as_ref().map(|valid| valid.slice(start, rows));
                    let additional = rows - valid.as_ref().map_or(0, |n| n.null_count());
                    if additional > table.capacity() - table.len() {
                        // Keep the old allocation charged during rehash. The fixed-size
                        // allowance covers control-group padding; at least eight elements
                        // also covers hashbrown's minimum bucket sizes.
                        let minimum = (table.len() + additional).max(chunk_rows);
                        let mut entries = if table.capacity() != 0 && !tried_preallocation
                        {
                            tried_preallocation = true;
                            num_rows
                        } else {
                            minimum
                        };
                        let mut bytes = estimate_memory_size::<(u64, T)>(
                            entries.max(8),
                            fixed_bytes,
                        )?;
                        let mut admission = reservation.try_grow(bytes);
                        if admission.is_err() && entries != minimum {
                            entries = minimum;
                            bytes = estimate_memory_size::<(u64, T)>(
                                entries.max(8),
                                fixed_bytes,
                            )?;
                            admission = reservation.try_grow(bytes);
                        }
                        let replay = admission.is_err();
                        if let Err(error) = admission {
                            // If only the replacement fits, drop the partial index
                            // before releasing its charge, then rebuild from the start.
                            if table.capacity() == 0 {
                                return Err(error);
                            }
                            let old_bytes = table.allocation_size();
                            table = HashTable::new();
                            reservation.shrink(old_bytes);
                            reservation.try_grow(bytes)?;
                        }
                        *peak = (*peak)
                            .max(reservation.size() - initial_reserved + scratch.size());
                        let old_bytes = table.allocation_size();
                        table
                            .try_reserve(entries - table.len(), |&(hash, _)| hash)
                            .map_err(|e| {
                                datafusion_common::DataFusionError::ResourcesExhausted(
                                    format!("Hash join table allocation: {e}"),
                                )
                            })?;
                        reservation.shrink(old_bytes + bytes - table.allocation_size());
                        if replay {
                            next.fill(T::default());
                            continue 'build;
                        }
                    }
                    let row_offset = offset + hash_start + start;
                    let iter = hashes
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| valid.as_ref().is_none_or(|n| n.is_valid(*i)))
                        .map(|(i, hash)| (row_offset + i, hash));
                    update_from_iter(&mut table, &mut next, Box::new(iter.rev()), 0);
                }
            }
            offset += batch.num_rows();
        }
        break;
    }
    drop(hashes);
    drop(scratch);
    Ok((table, next))
}

#[cfg(test)]
mod tests;
