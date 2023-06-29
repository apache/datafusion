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

//! Vectorized [`GroupsAccumulator`]

use arrow_array::{ArrayRef, BooleanArray};
use datafusion_common::Result;

/// An implementation of GroupAccumulator is for a single aggregate
/// (e.g. AVG) and stores the state for *all* groups internally
///
/// The logical model is that each group is given a `group_index`
/// assigned and maintained by the hash table.
///
/// group_indexes are contiguous (there aren't gaps), and thus it is
/// expected that each GroupAccumulator will use something like `Vec<..>`
/// to store the group states.
pub trait GroupsAccumulator: Send {
    /// updates the accumulator's state from a vector of arrays:
    ///
    /// * `values`: the input arguments to the accumulator
    /// * `group_indices`:  To which groups do the rows in `values` belong, group id)
    /// * `opt_filter`: if present, only update aggregate state using values[i] if opt_filter[i] is true
    /// * `total_num_groups`: the number of groups (the largest group_index is total_num_groups - 1)
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indicies: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Returns the final aggregate value for each group as a single
    /// `RecordBatch`
    ///
    /// OPEN QUESTION: Should this method take a "batch_size: usize"
    /// and produce a Vec<RecordBatch> as output to avoid 1) requiring
    /// one giant intermediate buffer?
    ///
    /// For example, the `SUM` accumulator maintains a running sum,
    /// and `evaluate` will produce that running sum as its output for
    /// all groups, in group_index order
    ///
    /// This call should be treated as consuming (takes `self`, but it
    /// can not be due to keeping it object save) the accumulator is
    /// free to release / reset it is internal state after this call
    /// and error on any subsequent call.
    fn evaluate(&mut self) -> Result<ArrayRef>;

    /// Returns any intermediate aggregate state used for multi-phase grouping
    ///
    /// For example, AVG returns two arrays:  `SUM` and `COUNT`.
    ///
    /// This call should be treated as consuming (takes `self`, but it
    /// can not be due to keeping it object save) the accumulator is
    /// free to release / reset it is internal state after this call
    /// and error on any subsequent call.
    ///
    /// TODO: consider returning a single Array (which could be a
    /// StructArray) instead
    fn state(&mut self) -> Result<Vec<ArrayRef>>;

    /// merges intermediate state (from `state()`) into this accumulators values
    ///
    /// For some aggregates (such as `SUM`), merge_batch is the same
    /// as `update_batch`, but for some aggregrates (such as `COUNT`)
    /// the operations differ. See [`Self::state`] for more details on how
    /// state is used and merged.
    ///
    /// * `values`: arrays produced from calling `state` previously to the accumulator
    /// * `group_indices`:  To which groups do the rows in `values` belong, group id)
    /// * `opt_filter`: if present, only update aggregate state using values[i] if opt_filter[i] is true
    /// * `total_num_groups`: the number of groups (the largest group_index is total_num_groups - 1)
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indicies: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Amount of memory used to store the state of this
    /// accumulator. This function is called once per batch, so it
    /// should be O(n) to compute
    fn size(&self) -> usize;
}
