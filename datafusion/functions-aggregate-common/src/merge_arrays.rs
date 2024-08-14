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

use arrow::compute::SortOptions;
use datafusion_common::utils::compare_rows;
use datafusion_common::{exec_err, ScalarValue};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};

/// This is a wrapper struct to be able to correctly merge `ARRAY_AGG` data from
/// multiple partitions using `BinaryHeap`. When used inside `BinaryHeap`, this
/// struct returns smallest `CustomElement`, where smallest is determined by
/// `ordering` values (`Vec<ScalarValue>`) according to `sort_options`.
#[derive(Debug, PartialEq, Eq)]
struct CustomElement<'a> {
    /// Stores the partition this entry came from
    branch_idx: usize,
    /// Values to merge
    value: ScalarValue,
    // Comparison "key"
    ordering: Vec<ScalarValue>,
    /// Options defining the ordering semantics
    sort_options: &'a [SortOptions],
}

impl<'a> CustomElement<'a> {
    fn new(
        branch_idx: usize,
        value: ScalarValue,
        ordering: Vec<ScalarValue>,
        sort_options: &'a [SortOptions],
    ) -> Self {
        Self {
            branch_idx,
            value,
            ordering,
            sort_options,
        }
    }

    fn ordering(
        &self,
        current: &[ScalarValue],
        target: &[ScalarValue],
    ) -> datafusion_common::Result<Ordering> {
        // Calculate ordering according to `sort_options`
        compare_rows(current, target, self.sort_options)
    }
}

// Overwrite ordering implementation such that
// - `self.ordering` values are used for comparison,
// - When used inside `BinaryHeap` it is a min-heap.
impl<'a> Ord for CustomElement<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compares according to custom ordering
        self.ordering(&self.ordering, &other.ordering)
            // Convert max heap to min heap
            .map(|ordering| ordering.reverse())
            // This function return error, when `self.ordering` and `other.ordering`
            // have different types (such as one is `ScalarValue::Int64`, other is `ScalarValue::Float32`)
            // Here this case won't happen, because data from each partition will have same type
            .unwrap()
    }
}

impl<'a> PartialOrd for CustomElement<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// This functions merges `values` array (`&[Vec<ScalarValue>]`) into single array `Vec<ScalarValue>`
/// Merging done according to ordering values stored inside `ordering_values` (`&[Vec<Vec<ScalarValue>>]`)
/// Inner `Vec<ScalarValue>` in the `ordering_values` can be thought as ordering information for the
/// each `ScalarValue` in the `values` array.
/// Desired ordering specified by `sort_options` argument (Should have same size with inner `Vec<ScalarValue>`
/// of the `ordering_values` array).
///
/// As an example
/// values can be \[
///      \[1, 2, 3, 4, 5\],
///      \[1, 2, 3, 4\],
///      \[1, 2, 3, 4, 5, 6\],
/// \]
/// In this case we will be merging three arrays (doesn't have to be same size)
/// and produce a merged array with size 15 (sum of 5+4+6)
/// Merging will be done according to ordering at `ordering_values` vector.
/// As an example `ordering_values` can be [
///      \[(1, a), (2, b), (3, b), (4, a), (5, b) \],
///      \[(1, a), (2, b), (3, b), (4, a) \],
///      \[(1, b), (2, c), (3, d), (4, e), (5, a), (6, b) \],
/// ]
/// For each ScalarValue in the `values` we have a corresponding `Vec<ScalarValue>` (like timestamp of it)
/// for the example above `sort_options` will have size two, that defines ordering requirement of the merge.
/// Inner `Vec<ScalarValue>`s of the `ordering_values` will be compared according `sort_options` (Their sizes should match)
pub fn merge_ordered_arrays(
    // We will merge values into single `Vec<ScalarValue>`.
    values: &mut [VecDeque<ScalarValue>],
    // `values` will be merged according to `ordering_values`.
    // Inner `Vec<ScalarValue>` can be thought as ordering information for the
    // each `ScalarValue` in the values`.
    ordering_values: &mut [VecDeque<Vec<ScalarValue>>],
    // Defines according to which ordering comparisons should be done.
    sort_options: &[SortOptions],
) -> datafusion_common::Result<(Vec<ScalarValue>, Vec<Vec<ScalarValue>>)> {
    // Keep track the most recent data of each branch, in binary heap data structure.
    let mut heap = BinaryHeap::<CustomElement>::new();

    if values.len() != ordering_values.len()
        || values
            .iter()
            .zip(ordering_values.iter())
            .any(|(vals, ordering_vals)| vals.len() != ordering_vals.len())
    {
        return exec_err!(
            "Expects values arguments and/or ordering_values arguments to have same size"
        );
    }
    let n_branch = values.len();
    let mut merged_values = vec![];
    let mut merged_orderings = vec![];
    // Continue iterating the loop until consuming data of all branches.
    loop {
        let minimum = if let Some(minimum) = heap.pop() {
            minimum
        } else {
            // Heap is empty, fill it with the next entries from each branch.
            for branch_idx in 0..n_branch {
                if let Some(orderings) = ordering_values[branch_idx].pop_front() {
                    // Their size should be same, we can safely .unwrap here.
                    let value = values[branch_idx].pop_front().unwrap();
                    // Push the next element to the heap:
                    heap.push(CustomElement::new(
                        branch_idx,
                        value,
                        orderings,
                        sort_options,
                    ));
                }
                // If None, we consumed this branch, skip it.
            }

            // Now we have filled the heap, get the largest entry (this will be
            // the next element in merge).
            if let Some(minimum) = heap.pop() {
                minimum
            } else {
                // Heap is empty, this means that all indices are same with
                // `end_indices`. We have consumed all of the branches, merge
                // is completed, exit from the loop:
                break;
            }
        };
        let CustomElement {
            branch_idx,
            value,
            ordering,
            ..
        } = minimum;
        // Add minimum value in the heap to the result
        merged_values.push(value);
        merged_orderings.push(ordering);

        // If there is an available entry, push next entry in the most
        // recently consumed branch to the heap.
        if let Some(orderings) = ordering_values[branch_idx].pop_front() {
            // Their size should be same, we can safely .unwrap here.
            let value = values[branch_idx].pop_front().unwrap();
            // Push the next element to the heap:
            heap.push(CustomElement::new(
                branch_idx,
                value,
                orderings,
                sort_options,
            ));
        }
    }

    Ok((merged_values, merged_orderings))
}
