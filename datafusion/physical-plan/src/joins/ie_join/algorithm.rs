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

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array, UInt64Array};
use arrow::compute::{SortColumn, lexsort_to_indices, sort_to_indices};
use arrow::record_batch::RecordBatch;
use arrow_schema::SortOptions;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::utils::normalize_float_zero;
use datafusion_common::{NullEquality, Result, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;

use crate::joins::JoinOn;
use crate::joins::utils::matchable_join_keys;
use crate::metrics::Gauge;

use super::exec::IEJoinCondition;

/// Rows for one equality-hash group in the two right-side orderings.
#[derive(Debug, Clone)]
pub(super) struct RightGroup {
    pub hash: u64,
    pub first: Range<usize>,
    pub second: Range<usize>,
}

/// Materialized and sorted state used by the IEJoin scan.
pub(super) struct IEJoinData {
    pub left_batch: RecordBatch,
    pub right_batch: RecordBatch,
    pub left_keys: [ArrayRef; 2],
    pub right_keys: [ArrayRef; 2],
    pub left_equality_keys: Vec<ArrayRef>,
    pub right_equality_keys: Vec<ArrayRef>,
    pub left_hashes: Option<UInt64Array>,
    pub left_by_second: Vec<u32>,
    pub right_by_first: Vec<u32>,
    pub right_by_second: Vec<u32>,
    pub right_first_position: Vec<u32>,
    pub right_groups: Vec<RightGroup>,
    pub reservation: MemoryReservation,
}

impl IEJoinData {
    pub fn try_new(
        left_batch: RecordBatch,
        right_batch: RecordBatch,
        conditions: &[IEJoinCondition; 2],
        on: &JoinOn,
        null_equality: NullEquality,
        reservation: MemoryReservation,
        peak_mem_used: &Gauge,
    ) -> Result<Self> {
        let left_keys = evaluate_condition_keys(&left_batch, conditions, true)?;
        let right_keys = evaluate_condition_keys(&right_batch, conditions, false)?;

        let left_equality_exprs = on.iter().map(|(left, _)| left);
        let right_equality_exprs = on.iter().map(|(_, right)| right);
        let left_equality_keys =
            evaluate_expressions_to_arrays(left_equality_exprs, &left_batch)?;
        let right_equality_keys =
            evaluate_expressions_to_arrays(right_equality_exprs, &right_batch)?;

        let key_memory = left_keys
            .iter()
            .chain(right_keys.iter())
            .chain(left_equality_keys.iter())
            .chain(right_equality_keys.iter())
            .try_fold(0_usize, |size, array| {
                size.checked_add(array.get_array_memory_size())
            })
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin key memory size overflow"
                )
            })?;
        reservation.try_grow(key_memory)?;
        peak_mem_used.set_max(reservation.size());

        let left_rows = left_batch.num_rows();
        let right_rows = right_batch.num_rows();
        if left_rows > u32::MAX as usize || right_rows > u32::MAX as usize {
            return internal_err!(
                "IEJoin input partition exceeds the u32 row limit: left={left_rows}, right={right_rows}"
            );
        }
        let has_equality = !on.is_empty();
        let retained_indices = right_rows
            .checked_mul(3)
            .and_then(|right| left_rows.checked_add(right))
            .and_then(|rows| rows.checked_mul(size_of::<u32>()))
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin retained index memory size overflow"
                )
            })?;
        let retained_hashes = if has_equality {
            left_rows.checked_mul(size_of::<u64>())
        } else {
            Some(0)
        }
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "IEJoin retained hash memory size overflow"
            )
        })?;
        let temporary_hashes = if has_equality {
            right_rows.checked_mul(size_of::<u64>())
        } else {
            Some(0)
        }
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "IEJoin temporary hash memory size overflow"
            )
        })?;
        let retained_size =
            retained_indices
                .checked_add(retained_hashes)
                .ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!(
                        "IEJoin memory size overflow"
                    )
                })?;
        // Sorting briefly owns both Arrow's index array and the retained Vec.
        let sort_scratch = left_rows
            .max(right_rows)
            .checked_mul(size_of::<usize>())
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin temporary memory size overflow"
                )
            })?;
        // There are at most two validity bitmaps for each input: equality keys
        // and range keys. Vec<bool> stores one bit per row.
        let validity_word_bits = usize::BITS as usize;
        let validity_scratch = left_rows
            .div_ceil(validity_word_bits)
            .checked_add(right_rows.div_ceil(validity_word_bits))
            .and_then(|words| words.checked_mul(size_of::<usize>()))
            .and_then(|size| size.checked_mul(2))
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin validity memory size overflow"
                )
            })?;
        let temporary_size = sort_scratch
            .checked_add(validity_scratch)
            .and_then(|size| size.checked_add(temporary_hashes))
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin temporary memory size overflow"
                )
            })?;
        let reserved_size =
            retained_size.checked_add(temporary_size).ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin reserved memory size overflow"
                )
            })?;
        reservation.try_grow(reserved_size)?;
        peak_mem_used.set_max(reservation.size());

        let random_state = RandomState::with_seed(0x9e37_79b9_7f4a_7c15);
        let left_hashes = has_equality
            .then(|| make_hashes(&left_equality_keys, left_rows, &random_state))
            .transpose()?;
        let right_hashes = has_equality
            .then(|| make_hashes(&right_equality_keys, right_rows, &random_state))
            .transpose()?;

        let left_matchable = matchable_join_keys(&left_equality_keys, null_equality);
        let right_matchable = matchable_join_keys(&right_equality_keys, null_equality);
        let left_range_matchable =
            matchable_join_keys(&left_keys, NullEquality::NullEqualsNothing);
        let right_range_matchable =
            matchable_join_keys(&right_keys, NullEquality::NullEqualsNothing);

        let left_by_second = sorted_valid_indices(
            left_hashes.as_ref(),
            &left_keys[1],
            condition_sort_options(conditions[1].operator()),
            |row| {
                left_range_matchable
                    .as_ref()
                    .is_none_or(|valid| valid.is_valid(row))
                    && left_matchable
                        .as_ref()
                        .is_none_or(|valid| valid.is_valid(row))
            },
        )?;
        let right_by_first = sorted_valid_indices(
            right_hashes.as_ref(),
            &right_keys[0],
            condition_sort_options(conditions[0].operator()),
            |row| {
                right_range_matchable
                    .as_ref()
                    .is_none_or(|valid| valid.is_valid(row))
                    && right_matchable
                        .as_ref()
                        .is_none_or(|valid| valid.is_valid(row))
            },
        )?;
        let right_by_second = sorted_valid_indices(
            right_hashes.as_ref(),
            &right_keys[1],
            condition_sort_options(conditions[1].operator()),
            |row| {
                right_range_matchable
                    .as_ref()
                    .is_none_or(|valid| valid.is_valid(row))
                    && right_matchable
                        .as_ref()
                        .is_none_or(|valid| valid.is_valid(row))
            },
        )?;

        drop((
            left_matchable,
            right_matchable,
            left_range_matchable,
            right_range_matchable,
        ));

        let mut right_first_position = vec![u32::MAX; right_rows];
        for (position, row) in right_by_first.iter().copied().enumerate() {
            let position = u32::try_from(position).map_err(|_| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin right-side position exceeds u32::MAX"
                )
            })?;
            right_first_position[row as usize] = position;
        }

        let group_count = count_groups(&right_by_first, right_hashes.as_ref());
        let retained_group_size = group_count
            .checked_mul(size_of::<RightGroup>())
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin retained group memory size overflow"
                )
            })?;
        let group_scratch = group_count
            .checked_mul(2)
            .and_then(|groups| groups.checked_mul(size_of::<(u64, Range<usize>)>()))
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin temporary group memory size overflow"
                )
            })?;
        let reusable_scratch =
            sort_scratch.checked_add(validity_scratch).ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin reusable memory size overflow"
                )
            })?;
        let group_phase_size = retained_group_size
            .checked_add(group_scratch)
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin group memory size overflow"
                )
            })?;
        let additional_group_reservation =
            group_phase_size.saturating_sub(reusable_scratch);
        reservation.try_grow(additional_group_reservation)?;
        peak_mem_used.set_max(reservation.size());

        let first_groups =
            group_ranges(&right_by_first, right_hashes.as_ref(), group_count);
        let second_groups =
            group_ranges(&right_by_second, right_hashes.as_ref(), group_count);
        if first_groups.len() != second_groups.len() {
            return internal_err!(
                "IEJoin produced inconsistent right-side equality groups"
            );
        }
        let mut right_groups = Vec::with_capacity(first_groups.len());
        for ((hash, first), (second_hash, second)) in
            first_groups.into_iter().zip(second_groups)
        {
            if hash != second_hash || first.len() != second.len() {
                return internal_err!(
                    "IEJoin produced unequal permutations for equality group {hash}"
                );
            }
            right_groups.push(RightGroup {
                hash,
                first,
                second,
            });
        }
        drop(right_hashes);

        let total_phase_reservation = reserved_size
            .checked_add(additional_group_reservation)
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin total phase memory size overflow"
                )
            })?;
        let final_reservation = retained_size
            .checked_add(retained_group_size)
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin final memory size overflow"
                )
            })?;
        let released_size = total_phase_reservation
            .checked_sub(final_reservation)
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoin reservation accounting underflow"
                )
            })?;
        reservation.shrink(released_size);

        Ok(Self {
            left_batch,
            right_batch,
            left_keys,
            right_keys,
            left_equality_keys,
            right_equality_keys,
            left_hashes,
            left_by_second,
            right_by_first,
            right_by_second,
            right_first_position,
            right_groups,
            reservation,
        })
    }
}

fn evaluate_condition_keys(
    batch: &RecordBatch,
    conditions: &[IEJoinCondition; 2],
    left: bool,
) -> Result<[ArrayRef; 2]> {
    let mut keys = Vec::with_capacity(2);
    for condition in conditions {
        let expr = if left {
            condition.left()
        } else {
            condition.right()
        };
        let values = expr.evaluate(batch)?.into_array(batch.num_rows())?;
        // Arrow's ordering distinguishes -0.0 from +0.0, while SQL range
        // comparisons do not. Normalize once before both sorting and scanning.
        keys.push(normalize_float_zero(&values));
    }
    keys.try_into().map_err(|_| {
        datafusion_common::internal_datafusion_err!(
            "IEJoin requires exactly two condition keys"
        )
    })
}

fn make_hashes(
    keys: &[ArrayRef],
    row_count: usize,
    random_state: &RandomState,
) -> Result<UInt64Array> {
    debug_assert!(!keys.is_empty());
    let mut hashes = vec![0; row_count];
    create_hashes(keys, random_state, &mut hashes)?;
    Ok(UInt64Array::from(hashes))
}

fn condition_sort_options(operator: datafusion_expr::Operator) -> SortOptions {
    use datafusion_expr::Operator::{Gt, GtEq, Lt, LtEq};
    match operator {
        // For L < R, scanning L and R from high to low makes the set of
        // matching right rows grow monotonically. The converse holds for >.
        Lt | LtEq => SortOptions::new(true, false),
        Gt | GtEq => SortOptions::new(false, false),
        _ => unreachable!("IEJoin condition validation rejects non-range operators"),
    }
}

fn sorted_valid_indices(
    hashes: Option<&UInt64Array>,
    key: &ArrayRef,
    key_options: SortOptions,
    valid: impl Fn(usize) -> bool,
) -> Result<Vec<u32>> {
    let indices: UInt32Array = if let Some(hashes) = hashes {
        lexsort_to_indices(
            &[
                SortColumn {
                    values: Arc::new(hashes.clone()),
                    options: Some(SortOptions::new(false, false)),
                },
                SortColumn {
                    values: Arc::clone(key),
                    options: Some(key_options),
                },
            ],
            None,
        )?
    } else {
        sort_to_indices(key.as_ref(), Some(key_options), None)?
    };

    Ok(indices
        .values()
        .iter()
        .copied()
        .filter(|row| valid(*row as usize))
        .collect())
}

fn group_ranges(
    sorted_rows: &[u32],
    hashes: Option<&UInt64Array>,
    group_count: usize,
) -> Vec<(u64, Range<usize>)> {
    if hashes.is_none() {
        return (!sorted_rows.is_empty())
            .then_some((0, 0..sorted_rows.len()))
            .into_iter()
            .collect();
    }
    let hashes = hashes.expect("checked above");
    let mut groups = Vec::with_capacity(group_count);
    let mut start = 0;
    while start < sorted_rows.len() {
        let hash = hashes.value(sorted_rows[start] as usize);
        let mut end = start + 1;
        while end < sorted_rows.len() && hashes.value(sorted_rows[end] as usize) == hash {
            end += 1;
        }
        groups.push((hash, start..end));
        start = end;
    }
    groups
}

fn count_groups(sorted_rows: &[u32], hashes: Option<&UInt64Array>) -> usize {
    let Some(hashes) = hashes else {
        return usize::from(!sorted_rows.is_empty());
    };
    sorted_rows
        .windows(2)
        .filter(|rows| hashes.value(rows[0] as usize) != hashes.value(rows[1] as usize))
        .count()
        + usize::from(!sorted_rows.is_empty())
}

/// A bitmap with a summary bit per data word, allowing scans to skip 4,096
/// candidate positions at a time when the active set is sparse.
pub(super) struct ActiveBitmap {
    words: Vec<u64>,
    summary: Vec<u64>,
}

impl ActiveBitmap {
    pub fn new(len: usize) -> Self {
        let words = len.div_ceil(64);
        Self {
            words: vec![0; words],
            summary: vec![0; words.div_ceil(64)],
        }
    }

    pub fn memory_size(&self) -> usize {
        (self.words.len() + self.summary.len()) * size_of::<u64>()
    }

    pub fn resize_and_clear(&mut self, len: usize) {
        let words = len.div_ceil(64);
        self.words.resize(words, 0);
        self.words.fill(0);
        self.summary.resize(words.div_ceil(64), 0);
        self.summary.fill(0);
    }

    #[inline]
    pub fn insert(&mut self, position: usize) {
        let word = position / 64;
        let bit = position % 64;
        self.words[word] |= 1_u64 << bit;
        self.summary[word / 64] |= 1_u64 << (word % 64);
    }

    pub fn next_set(&self, start: usize, end: usize) -> Option<usize> {
        if start >= end {
            return None;
        }
        let last_word = (end - 1) / 64;
        let mut word_index = start / 64;
        let mut word = self.words[word_index] & (u64::MAX << (start % 64));
        if word_index == last_word && !end.is_multiple_of(64) {
            word &= (1_u64 << (end % 64)) - 1;
        }
        if word != 0 {
            return Some(word_index * 64 + word.trailing_zeros() as usize);
        }

        word_index += 1;
        while word_index <= last_word {
            let summary_index = word_index / 64;
            let mut summary =
                self.summary[summary_index] & (u64::MAX << (word_index % 64));
            if summary_index == last_word / 64 && !(last_word + 1).is_multiple_of(64) {
                summary &= (1_u64 << ((last_word + 1) % 64)) - 1;
            }
            if summary == 0 {
                word_index = (summary_index + 1) * 64;
                continue;
            }
            let found_word = summary_index * 64 + summary.trailing_zeros() as usize;
            let mut found = self.words[found_word];
            if found_word == last_word && !end.is_multiple_of(64) {
                found &= (1_u64 << (end % 64)) - 1;
            }
            if found != 0 {
                return Some(found_word * 64 + found.trailing_zeros() as usize);
            }
            word_index = found_word + 1;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::ActiveBitmap;

    #[test]
    fn active_bitmap_scans_boundaries() {
        let mut bitmap = ActiveBitmap::new(10_000);
        for position in [0, 63, 64, 65, 4095, 4096, 9_999] {
            bitmap.insert(position);
        }

        assert_eq!(bitmap.next_set(0, 10_000), Some(0));
        assert_eq!(bitmap.next_set(1, 10_000), Some(63));
        assert_eq!(bitmap.next_set(64, 65), Some(64));
        assert_eq!(bitmap.next_set(65, 4_096), Some(65));
        assert_eq!(bitmap.next_set(66, 4_096), Some(4_095));
        assert_eq!(bitmap.next_set(4_096, 9_999), Some(4_096));
        assert_eq!(bitmap.next_set(4_097, 9_999), None);
        assert_eq!(bitmap.next_set(9_999, 10_000), Some(9_999));

        bitmap.resize_and_clear(100);
        assert_eq!(bitmap.next_set(0, 100), None);
    }
}
