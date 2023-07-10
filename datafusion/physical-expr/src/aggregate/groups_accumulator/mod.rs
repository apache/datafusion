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

pub(crate) mod accumulate;
mod adapter;
pub use adapter::GroupsAccumulatorAdapter;

pub(crate) mod bool_op;
pub(crate) mod prim_op;

use arrow_array::{ArrayRef, BooleanArray};
use datafusion_common::Result;

/// `GroupAccumulator` implements a single aggregate (e.g. AVG) and
/// stores the state for *all* groups internally.
///
/// Each group is assigned a `group_index` by the hash table and each
/// accumulator manages the specific state, one per group_index.
///
/// group_indexes are contiguous (there aren't gaps), and thus it is
/// expected that each GroupAccumulator will use something like `Vec<..>`
/// to store the group states.
pub trait GroupsAccumulator: Send {
    /// Updates the accumulator's state from its arguments, encoded as
    /// a vector of [`ArrayRef`]s.
    ///
    /// * `values`: the input arguments to the accumulator
    ///
    /// * `group_indices`: To which groups do the rows in `values`
    /// belong, group id)
    ///
    /// * `opt_filter`: if present, only update aggregate state using
    /// `values[i]` if `opt_filter[i]` is true
    ///
    /// * `total_num_groups`: the number of groups (the largest
    /// group_index is thus `total_num_groups - 1`).
    ///
    /// Note thats subsequent calls to update_batch may have larger
    /// total_num_groups as new groups are seen.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Returns the final aggregate value for each group as a single
    /// `RecordBatch`, resetting the internal state.
    ///
    /// The rows returned *must* be in group_index order: The value
    /// for group_index 0, followed by 1, etc.  Any group_index that
    /// did not have values, should be null.
    ///
    /// For example, a `SUM` accumulator maintains a running sum for
    /// each group, and `evaluate` will produce that running sum as
    /// its output for all groups, in group_index order
    ///
    /// The accumulator should free to release / reset it is internal
    /// state after this call to the same as it was after being
    /// initially created.
    fn evaluate(&mut self) -> Result<ArrayRef>;

    /// Returns the intermediate aggregate state for this accumulator,
    /// used for multi-phase grouping, resetting its internal state.
    ///
    /// The rows returned *must* be in group_index order: The value
    /// for group_index 0, followed by 1, etc.  Any group_index that
    /// did not have values, should be null.
    ///
    /// For example, `AVG` might return two arrays: `SUM` and `COUNT`
    /// but the `MIN` aggregate would just return a single array.
    ///
    /// Note more sophisticated internal state can be passed as
    /// single `StructArray` rather than multiple arrays.
    ///
    /// The accumulator should free to release / reset it is internal
    /// state after this call to the same as it was after being
    /// initially created.
    fn state(&mut self) -> Result<Vec<ArrayRef>>;

    /// Merges intermediate state (the output from [`Self::state`])
    /// into this accumulator's values.
    ///
    /// For some aggregates (such as `SUM`), `merge_batch` is the same
    /// as `update_batch`, but for some aggregrates (such as `COUNT`,
    /// where the partial counts must be summed) the operations
    /// differ. See [`Self::state`] for more details on how state is
    /// used and merged.
    ///
    /// * `values`: arrays produced from calling `state` previously to the accumulator
    ///
    /// Other arguments are the same as for [`Self::update_batch`];
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Amount of memory used to store the state of this accumulator,
    /// in bytes. This function is called once per batch, so it should
    /// be `O(n)` to compute, not `O(num_groups)`
    fn size(&self) -> usize;
}
