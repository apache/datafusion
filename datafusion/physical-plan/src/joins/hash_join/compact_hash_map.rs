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
use std::hash::BuildHasher;
use std::mem::size_of;

use arrow::array::AsArray;
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

use crate::joins::join_hash_map::update_from_iter_inner;
use crate::joins::utils::matchable_join_keys;

/// Bound hash scratch and the number of potentially new hashes per insertion.
const HASH_BUILD_CHUNK_ROWS: usize = 8192;

// Only a capacity hint: every row still goes through the normal build path.
// Sample directly from flat byte columns, without copying values or evaluating
// expressions again. Other key representations keep the existing growth policy.
fn sampled_capacity(
    batches: &[RecordBatch],
    on: &[PhysicalExprRef],
    num_rows: usize,
    random_state: &RandomState,
    reservation: &MemoryReservation,
) -> Option<(usize, usize)> {
    const SAMPLES: usize = 2048;
    if num_rows < 128 * SAMPLES || on.len() != 1 {
        return None;
    }
    let column = on[0].downcast_ref::<Column>()?;
    let data_type = batches.first()?.column(column.index()).data_type();
    if !matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    ) {
        return None;
    }
    // Sparse non-null samples cannot estimate the eligible population safely.
    // Abstain before allocating scratch so the original path and peak are kept.
    if batches
        .iter()
        .any(|batch| batch.column(column.index()).null_count() != 0)
    {
        return None;
    }
    let sample_reservation = reservation.new_empty();
    let sample_bytes = SAMPLES * size_of::<u64>();
    sample_reservation.try_grow(sample_bytes).ok()?;
    let mut samples = Vec::with_capacity(SAMPLES);
    for i in 0..SAMPLES {
        samples.push(random_state.hash_one(i) % num_rows as u64);
    }
    // Sample across the whole input, including within grouped runs of equal
    // values. Repeated row positions must not create false duplicate hashes.
    samples.sort_unstable();
    samples.dedup();
    let mut batch_index = 0;
    let mut batch_start = 0;
    for sample in &mut samples {
        let row = *sample as usize;
        while row >= batch_start + batches[batch_index].num_rows() {
            batch_start += batches[batch_index].num_rows();
            batch_index += 1;
        }
        let array = batches[batch_index].column(column.index());
        let row = row - batch_start;
        let hash = match data_type {
            DataType::Utf8 => random_state.hash_one(array.as_string::<i32>().value(row)),
            DataType::LargeUtf8 => {
                random_state.hash_one(array.as_string::<i64>().value(row))
            }
            DataType::Utf8View => {
                random_state.hash_one(array.as_string_view().value(row))
            }
            DataType::Binary => {
                random_state.hash_one(array.as_binary::<i32>().value(row))
            }
            DataType::LargeBinary => {
                random_state.hash_one(array.as_binary::<i64>().value(row))
            }
            DataType::BinaryView => {
                random_state.hash_one(array.as_binary_view().value(row))
            }
            _ => unreachable!(),
        };
        *sample = hash;
    }
    samples.sort_unstable();
    let mut pairs = 0_u64;
    let mut maximum_frequency = 0;
    let mut singletons = 0;
    let mut distinct = 0;
    let mut triples = 0_u64;
    let mut start = 0;
    while start < samples.len() {
        let mut end = start + 1;
        while end < samples.len() && samples[start] == samples[end] {
            end += 1;
        }
        let count = end - start;
        maximum_frequency = maximum_frequency.max(count);
        let count = count as u64;
        pairs += count * (count - 1) / 2;
        triples += count * count.saturating_sub(1) * count.saturating_sub(2) / 6;
        singletons += usize::from(count == 1);
        distinct += 1;
        start = end;
    }
    // Few repeated pairs give an unstable estimate. A frequent sampled key
    // indicates skew, where collision-based estimates can badly undercount the
    // long tail of unique keys. Abstain for skew, retaining the initial small
    // table and its usual full preallocation on the first observed growth.
    let entries = if singletons <= samples.len() / 32 {
        // Nearly all sampled rows repeat. Retain the bounded starting table
        // and let observed growth catch any unsampled tail.
        distinct * 2
    } else if maximum_frequency > 8
        || (triples >= 8 && triples * samples.len() as u64 > pairs * pairs)
    {
        0
    } else if pairs < 8 {
        num_rows
    } else {
        // Inflate the uniform-key collision estimate to leave growth headroom.
        let sample_count = samples.len() as u64;
        (sample_count * sample_count.saturating_sub(1) / pairs).min(num_rows as u64)
            as usize
    };
    Some((entries, sample_bytes))
}

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

// Start with one chunk so low-cardinality builds stay small. At the first
// growth, a bounded byte-column sample can replace row-count preallocation.
// If a sample is unavailable or cannot fit with its compaction workspace, try
// the row-count bound, then grow by observed hashes plus the next chunk.
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
    let mut sampled_preallocation = false;
    let mut compact_sampled_growth = false;
    // Held capacity is part of the reservation peak, without allocating it yet.
    let mut compaction_headroom = 0;
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
                        let mut entries = if table.capacity() == 0 {
                            minimum
                        } else if !tried_preallocation || sampled_preallocation {
                            // Sample only when the bounded table first outgrows
                            // its capacity. A zero hint or unsupported key keeps
                            // the original row-count preallocation attempt.
                            reservation.shrink(std::mem::take(&mut compaction_headroom));
                            let estimate = if !tried_preallocation {
                                sampled_capacity(
                                    batches,
                                    on,
                                    num_rows,
                                    random_state,
                                    reservation,
                                )
                            } else {
                                None
                            };
                            tried_preallocation = true;
                            sampled_preallocation = false;
                            if let Some((estimate, sample_bytes)) = estimate {
                                *peak = (*peak).max(
                                    reservation.size() - initial_reserved
                                        + scratch.size()
                                        + sample_bytes,
                                );
                                if estimate == 0 {
                                    num_rows
                                } else {
                                    let estimate = estimate
                                        .saturating_add(chunk_rows)
                                        .min(num_rows)
                                        .max(minimum);
                                    sampled_preallocation = estimate < num_rows;
                                    estimate
                                }
                            } else {
                                num_rows
                            }
                        } else {
                            minimum
                        };
                        let mut bytes = estimate_memory_size::<(u64, T)>(
                            entries.max(8),
                            fixed_bytes,
                        )?;
                        // Hints in the full table's bucket tier need no separate
                        // compaction workspace or sampled-growth policy.
                        if sampled_preallocation
                            && estimate_memory_size::<(u64, T)>(
                                num_rows.max(8),
                                fixed_bytes,
                            )
                            .is_ok_and(|full_bytes| bytes == full_bytes)
                        {
                            entries = num_rows;
                            sampled_preallocation = false;
                        }
                        let mut headroom = if sampled_preallocation {
                            // A table at most half full shrinks to at most half its
                            // buckets. Keep that workspace until the hint is
                            // outgrown or compacted, so a sample cannot consume
                            // memory needed to release its excess allocation.
                            ((bytes - fixed_bytes) / 2 + fixed_bytes)
                                .max(estimate_memory_size::<(u64, T)>(8, fixed_bytes)?)
                        } else {
                            0
                        };
                        let mut admission = bytes
                            .checked_add(headroom)
                            .ok_or_else(|| {
                                datafusion_common::exec_datafusion_err!(
                                    "Hash join compaction reservation size overflow"
                                )
                            })
                            .and_then(|bytes| reservation.try_grow(bytes));
                        if admission.is_err() && headroom != 0 {
                            // The full table may fit even when the sampled table
                            // plus guaranteed compaction workspace does not.
                            headroom = 0;
                            sampled_preallocation = false;
                            entries = num_rows;
                            bytes = estimate_memory_size::<(u64, T)>(
                                entries.max(8),
                                fixed_bytes,
                            )?;
                            admission = reservation.try_grow(bytes);
                        }
                        if admission.is_err() && entries != minimum {
                            entries = minimum;
                            bytes = estimate_memory_size::<(u64, T)>(
                                entries.max(8),
                                fixed_bytes,
                            )?;
                            admission = reservation.try_grow(bytes);
                        }
                        compaction_headroom = headroom;
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
                        if sampled_preallocation {
                            compact_sampled_growth = true;
                        } else if entries == num_rows {
                            compact_sampled_growth = false;
                        }
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
                    update_from_iter_inner(&mut table, &mut next, iter.rev(), 0);
                }
            }
            offset += batch.num_rows();
        }
        break;
    }
    drop(hashes);
    drop(scratch);
    // Keep minimum growth after an outgrown sample compact too. A replacement
    // that fit beside the original row-count-sized table also fits beside this
    // smaller table. Successful full preallocation keeps its original threshold.
    let should_compact = if compact_sampled_growth {
        table.capacity() / 2 >= table.len()
    } else {
        table.capacity() / 4 > table.len()
    };
    if should_compact {
        let bytes = estimate_memory_size::<(u64, T)>(table.len().max(8), fixed_bytes)?;
        let additional = bytes.saturating_sub(compaction_headroom);
        if additional == 0 || reservation.try_grow(additional).is_ok() {
            *peak = (*peak).max(reservation.size() - initial_reserved);
            let old_bytes = table.allocation_size();
            table.shrink_to_fit(|&(hash, _)| hash);
            reservation.shrink(old_bytes + bytes - table.allocation_size());
            compaction_headroom = compaction_headroom.saturating_sub(bytes);
        }
    }
    reservation.shrink(compaction_headroom);
    Ok((table, next))
}

#[cfg(test)]
mod tests;
