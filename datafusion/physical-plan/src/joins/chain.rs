// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::Debug;
use std::ops::Sub;

use arrow::datatypes::ArrowNativeType;

use crate::joins::join_hash_map::ProbeOffset;
use crate::joins::join_hash_map::ProbeOffset::{AtRow, MidChain};

/// Traverses the chain of matching indices, collecting results up to the remaining limit.
/// Returns `Some(offset)` if the limit was reached and there are more results to process,
/// or `None` if the chain was fully traversed.
#[inline(always)]
pub(crate) fn traverse_chain<T>(
    next_chain: &[T],
    prob_idx: usize,
    start_chain_idx: T,
    remaining: &mut usize,
    input_indices: &mut Vec<u32>,
    match_indices: &mut Vec<u64>,
    is_last_input: bool,
) -> Option<ProbeOffset>
where
    T: Copy + TryFrom<usize> + PartialOrd + Into<u64> + Sub<Output = T>,
    <T as TryFrom<usize>>::Error: Debug,
    T: ArrowNativeType,
{
    let zero = T::usize_as(0);
    let one = T::usize_as(1);
    let mut match_row_idx = start_chain_idx - one;

    loop {
        match_indices.push(match_row_idx.into());
        input_indices.push(prob_idx as u32);
        *remaining -= 1;

        let next = next_chain[match_row_idx.into() as usize];

        if *remaining == 0 {
            // Limit reached - return offset for next call
            return if is_last_input && next == zero {
                // Finished processing the last input row
                None
            } else if next == zero {
                // `prob_idx` has been processed during this iteration, so the
                // next call should start with the next input row
                Some(AtRow { row: prob_idx + 1 })
            } else {
                Some(MidChain {
                    row: prob_idx,
                    next: next.into(),
                })
            };
        }
        if next == zero {
            // End of chain
            return None;
        }
        match_row_idx = next - one;
    }
}

/// Emits the matches of the probe rows `0..num_rows` from `offset`, at most
/// `limit` of them, for maps that store the build rows of each key next to
/// each other, see `JoinHashMapType::group_rows`. `rows_of` returns the range
/// in `rows` of a probe row's matches, or `None` if the probe row has none.
/// Returns the offset to resume from, as [`traverse_chain`] does.
pub(crate) fn grouped_matched_indices<T>(
    rows: &[T],
    num_rows: usize,
    rows_of: impl Fn(usize) -> Option<(usize, usize)>,
    limit: usize,
    offset: ProbeOffset,
    input_indices: &mut Vec<u32>,
    match_indices: &mut Vec<u64>,
) -> Option<ProbeOffset>
where
    T: Copy + Into<u64>,
{
    // Clear the buffer before producing new results
    input_indices.clear();
    match_indices.clear();
    let mut remaining = limit;

    let (first_row, mut resume_pos) = match offset {
        AtRow { row } => (row, None),
        // Resume at position `next` of the matches of probe row `row`
        MidChain { row, next } => (row, Some(next as usize)),
    };
    for row in first_row..num_rows {
        let resume_pos = resume_pos.take();
        let Some((start, end)) = rows_of(row) else {
            continue;
        };
        let pos = resume_pos.unwrap_or(start);
        let take = (end - pos).min(remaining);
        match_indices.extend(rows[pos..pos + take].iter().map(|&row| row.into()));
        input_indices.extend(std::iter::repeat_n(row as u32, take));
        remaining -= take;
        if remaining == 0 {
            return if pos + take < end {
                Some(MidChain {
                    row,
                    next: (pos + take) as u64,
                })
            } else if row == num_rows - 1 {
                None
            } else {
                Some(AtRow { row: row + 1 })
            };
        }
    }
    None
}
