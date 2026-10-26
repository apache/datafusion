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
