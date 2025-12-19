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

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::{Result, not_impl_err};

/// Describes how many rows should be emitted during grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitTo {
    /// Emit all groups
    All,
    /// Emit only the first `n` groups and shift all existing group
    /// indexes down by `n`.
    ///
    /// For example, if `n=10`, group_index `0, 1, ... 9` are emitted
    /// and group indexes `10, 11, 12, ...` become `0, 1, 2, ...`.
    First(usize),
}

impl EmitTo {
    /// Removes the number of rows from `v` required to emit the right
    /// number of rows, returning a `Vec` with elements taken, and the
    /// remaining values in `v`.
    ///
    /// This avoids copying if Self::All
    pub fn take_needed<T>(&self, v: &mut Vec<T>) -> Vec<T> {
        match self {
            Self::All => {
                // Take the entire vector, leave new (empty) vector
                std::mem::take(v)
            }
            Self::First(n) => {
                // get end n+1,.. values into t
                let mut t = v.split_off(*n);
                // leave n+1,.. in v
                std::mem::swap(v, &mut t);
                t
            }
        }
    }
}

/// `GroupsAccumulator` implements a single aggregate (e.g. AVG) and
/// stores the state for *all* groups internally.
///
/// Logically, a [`GroupsAccumulator`] stores a mapping from each group index to
/// the state of the aggregate for that group. For example an implementation for
/// `min` might look like
///
/// ```text
///    ┌─────┐
///    │  0  │───────────▶   100
///    ├─────┤
///    │  1  │───────────▶   200
///    └─────┘
///      ...                 ...
///    ┌─────┐
///    │ N-2 │───────────▶    50
///    ├─────┤
///    │ N-1 │───────────▶   200
///    └─────┘
///
///
///  Logical group      Current Min
///     number          value for that
///                     group
/// ```
///
/// # Notes on Implementing `GroupsAccumulator`
///
/// All aggregates must first implement the simpler [`Accumulator`] trait, which
/// handles state for a single group. Implementing `GroupsAccumulator` is
/// optional and is harder to implement than `Accumulator`, but can be much
/// faster for queries with many group values.  See the [Aggregating Millions of
/// Groups Fast blog] for more background.
///
/// [`NullState`] can help keep the state for groups that have not seen any
/// values and produce the correct output for those groups.
///
/// [`NullState`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/struct.NullState.html
///
/// # Details
/// Each group is assigned a `group_index` by the hash table and each
/// accumulator manages the specific state, one per `group_index`.
///
/// `group_index`es are contiguous (there aren't gaps), and thus it is
/// expected that each `GroupsAccumulator` will use something like `Vec<..>`
/// to store the group states.
///
/// [`Accumulator`]: crate::accumulator::Accumulator
/// [Aggregating Millions of Groups Fast blog]: https://arrow.apache.org/blog/2023/08/05/datafusion_fast_grouping/
pub trait GroupsAccumulator: Send {
    /// Updates the accumulator's state from its arguments, encoded as
    /// a vector of [`ArrayRef`]s.
    ///
    /// * `values`: the input arguments to the accumulator
    ///
    /// * `group_indices`: The group indices to which each row in `values` belongs.
    ///
    /// * `opt_filter`: if present, only update aggregate state using
    ///   `values[i]` if `opt_filter[i]` is true
    ///
    /// * `total_num_groups`: the number of groups (the largest
    ///   group_index is thus `total_num_groups - 1`).
    ///
    /// Note that subsequent calls to update_batch may have larger
    /// total_num_groups as new groups are seen.
    ///
    /// See [`NullState`] to help keep the state for groups that have not seen any
    /// values and produce the correct output for those groups.
    ///
    /// [`NullState`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/struct.NullState.html
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
    /// If `emit_to` is [`EmitTo::All`], the accumulator should
    /// return all groups and release / reset its internal state
    /// equivalent to when it was first created.
    ///
    /// If `emit_to` is [`EmitTo::First`], only the first `n` groups
    /// should be emitted and the state for those first groups
    /// removed. State for the remaining groups must be retained for
    /// future use. The group_indices on subsequent calls to
    /// `update_batch` or `merge_batch` will be shifted down by
    /// `n`. See [`EmitTo::First`] for more details.
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef>;

    /// Returns the intermediate aggregate state for this accumulator,
    /// used for multi-phase grouping, resetting its internal state.
    ///
    /// See [`Accumulator::state`] for more information on multi-phase
    /// aggregation.
    ///
    /// For example, `AVG` might return two arrays: `SUM` and `COUNT`
    /// but the `MIN` aggregate would just return a single array.
    ///
    /// Note more sophisticated internal state can be passed as
    /// single `StructArray` rather than multiple arrays.
    ///
    /// See [`Self::evaluate`] for details on the required output
    /// order and `emit_to`.
    ///
    /// [`Accumulator::state`]: crate::accumulator::Accumulator::state
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

    /// Merges intermediate state (the output from [`Self::state`])
    /// into this accumulator's current state.
    ///
    /// For some aggregates (such as `SUM`), `merge_batch` is the same
    /// as `update_batch`, but for some aggregates (such as `COUNT`,
    /// where the partial counts must be summed) the operations
    /// differ. See [`Self::state`] for more details on how state is
    /// used and merged.
    ///
    /// * `values`: arrays produced from previously calling `state` on other accumulators.
    ///
    /// Other arguments are the same as for [`Self::update_batch`].
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Converts an input batch directly to the intermediate aggregate state.
    ///
    /// This is the equivalent of treating each input row as its own group. It
    /// is invoked when the Partial phase of a multi-phase aggregation is not
    /// reducing the cardinality enough to warrant spending more effort on
    /// pre-aggregation (see `Background` section below), and switches to
    /// passing intermediate state directly on to the next aggregation phase.
    ///
    /// Examples:
    /// * `COUNT`: an array of 1s for each row in the input batch.
    /// * `SUM/MIN/MAX`: the input values themselves.
    ///
    /// # Arguments
    /// * `values`: the input arguments to the accumulator
    /// * `opt_filter`: if present, any row where `opt_filter[i]` is false should be ignored
    ///
    /// # Background
    ///
    /// In a multi-phase aggregation (see [`Accumulator::state`]), the initial
    /// Partial phase reduces the cardinality of the input data as soon as
    /// possible in the plan.
    ///
    /// This strategy is very effective for queries with a small number of
    /// groups, as most of the data is aggregated immediately and only a small
    /// amount of data must be repartitioned (see [`Accumulator::state`] for
    /// background)
    ///
    /// However, for queries with a large number of groups, the Partial phase
    /// often does not reduce the cardinality enough to warrant the memory and
    /// CPU cost of actually performing the aggregation. For such cases, the
    /// HashAggregate operator will dynamically switch to passing intermediate
    /// state directly to the next aggregation phase with minimal processing
    /// using this method.
    ///
    /// [`Accumulator::state`]: crate::accumulator::Accumulator::state
    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    /// Returns `true` if [`Self::convert_to_state`] is implemented to support
    /// intermediate aggregate state conversion.
    fn supports_convert_to_state(&self) -> bool {
        false
    }

    /// Amount of memory used to store the state of this accumulator,
    /// in bytes.
    ///
    /// This function is called once per batch, so it should be `O(n)` to
    /// compute, not `O(num_groups)`
    fn size(&self) -> usize;
}
