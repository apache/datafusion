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

use std::cmp::Ordering;
use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::{assert_ne_or_internal_err, exec_err, not_impl_err, utils::split_vec_min_alloc, Result, assert_or_internal_err};

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
            Self::First(n) => split_vec_min_alloc(v, *n),
        }
    }
}

/// Selects groups for a non-destructive grouped aggregation read.
///
/// Unlike [`EmitTo`], this selection does not remove groups or change their
/// indices. Selections created by [`Self::try_from_indices`] preserve the
/// requested order and support duplicate indices.
///
/// A selection is validated once when it is constructed and can then be reused
/// for the group values and accumulators participating in the same snapshot.
/// Construct a new selection if the number or indexing of those groups changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupSelection<'a> {
  total_num_groups: usize,
  indices: Option<&'a [usize]>,
}

impl<'a> GroupSelection<'a> {
  /// Selects all `total_num_groups` groups in group-index order.
  pub fn all(total_num_groups: usize) -> Self {
    Self {
      total_num_groups,
      indices: None,
    }
  }

  /// Selects groups in the order specified by `indices`.
  ///
  /// Returns an error if an index is not less than `total_num_groups`. Empty
  /// selections are valid, and duplicate indices are preserved.
  pub fn try_from_indices(
    indices: &'a [usize],
    total_num_groups: usize,
  ) -> Result<Self> {
    if let Some(index) = indices.iter().find(|&&index| index >= total_num_groups) {
      return exec_err!(
                "Group index {index} is out of bounds for {total_num_groups} groups"
            );
    }
    Ok(Self {
      total_num_groups,
      indices: Some(indices),
    })
  }

  /// Returns the group count against which this selection was constructed.
  pub fn total_num_groups(self) -> usize {
    self.total_num_groups
  }

  /// Ensures this selection is being applied to the same number of groups
  /// against which it was constructed.
  ///
  /// Preserving-read implementations should call this method with their
  /// stored group count before using [`Self::iter`]. This check is `O(1)`;
  /// the selected indices were already checked by [`Self::try_from_indices`].
  pub fn validate_num_groups(self, actual_num_groups: usize) -> Result<()> {
    if actual_num_groups != self.total_num_groups {
      return exec_err!(
                "Group selection was constructed for {} groups but applied to {actual_num_groups} groups",
                self.total_num_groups
            );
    }
    Ok(())
  }

  /// Returns the number of selected groups.
  pub fn len(self) -> usize {
    self.indices
      .map_or(self.total_num_groups, |indices| indices.len())
  }

  /// Returns `true` if no groups are selected.
  pub fn is_empty(self) -> bool {
    self.len() == 0
  }

  /// Returns the selected group indices in output order.
  pub fn iter(self) -> impl Iterator<Item = usize> + 'a {
    let (all, indices): (_, &'a [usize]) = match self.indices {
      None => (0..self.total_num_groups, &[]),
      Some(indices) => (0..0, indices),
    };
    all.chain(indices.iter().copied())
  }
}

/// Selects groups for a non-destructive grouped aggregation read.
///
/// Unlike [`BlockedEmitTo`], this selection does not remove groups or change their
/// indices. Selections created by [`Self::try_from_indices`] preserve the
/// requested order and support duplicate indices.
///
/// A selection is validated once when it is constructed and can then be reused
/// for the group values and accumulators participating in the same snapshot.
/// Construct a new selection if the number or indexing of those groups changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockedGroupSelection<'a> {
  block_size: usize,
  total_num_groups: usize,
  indices: Option<&'a [BlocksIndex]>,
}

impl<'a> BlockedGroupSelection<'a> {
  /// Selects all `total_num_groups` groups in group-index order.
  pub fn all(total_num_groups: usize, block_size: usize,) -> Self {
    Self {
      total_num_groups,
      indices: None,
      block_size,
    }
  }

  /// Selects groups in the order specified by `indices`.
  ///
  /// Returns an error if an index is not less than `total_num_groups`. Empty
  /// selections are valid, and duplicate indices are preserved.
  pub fn try_from_indices(
    indices: &'a [BlocksIndex],
    total_num_groups: usize,
    block_size: usize,
  ) -> Result<Self> {
    let total_num_groups_parsed = BlocksIndex::from_index_in_fixed_block_size(total_num_groups, block_size);
    if let Some(index) = indices.iter().find(|&&index| index >= total_num_groups_parsed) {
      return exec_err!(
                "Group index {index:?} is out of bounds for {total_num_groups_parsed:?} ({total_num_groups}) groups"
            );
    }
    Ok(Self {
      total_num_groups,
      indices: Some(indices),
      block_size
    })
  }

  /// Returns the group count against which this selection was constructed.
  pub fn total_num_groups(self) -> usize {
    self.total_num_groups
  }

  /// Ensures this selection is being applied to the same number of groups
  /// against which it was constructed.
  ///
  /// Preserving-read implementations should call this method with their
  /// stored group count before using [`Self::iter`]. This check is `O(1)`;
  /// the selected indices were already checked by [`Self::try_from_indices`].
  pub fn validate_num_groups(self, actual_num_groups: usize) -> Result<()> {
    if actual_num_groups != self.total_num_groups {
      return exec_err!(
                "Group selection was constructed for {} groups but applied to {actual_num_groups} groups",
                self.total_num_groups
            );
    }
    Ok(())
  }

  /// Returns the number of selected groups.
  pub fn len(self) -> usize {
    self.indices
      .map_or(self.total_num_groups, |indices| indices.len())
  }

  /// Returns `true` if no groups are selected.
  pub fn is_empty(self) -> bool {
    self.len() == 0
  }

  /// Returns the selected group indices in output order.
  pub fn iter(self) -> impl Iterator<Item = BlocksIndex> + 'a {
    let (all, indices): (_, &'a [BlocksIndex]) = match self.indices {
      None => (0..self.total_num_groups, &[]),
      Some(indices) => (0..0, indices),
    };
    let block_size = self.block_size;
    all.map(move |index| BlocksIndex::from_index_in_fixed_block_size(index, block_size)).chain(indices.iter().copied())
  }

  #[doc(hidden)]
  pub fn indices(&self) -> Option<&[BlocksIndex]> {
    self.indices
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
/// For more background, please also see the [Aggregating Millions of Groups Fast in Apache Arrow DataFusion 28.0.0 blog]
///
/// [Aggregating Millions of Groups Fast in Apache Arrow DataFusion 28.0.0 blog]: https://datafusion.apache.org/blog/2023/08/05/datafusion_fast_grouping
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
pub trait GroupsAccumulator: Send + std::any::Any {
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

    /// Returns final aggregate values without changing the logical state or
    /// group indices.
    ///
    /// Rows are returned in the order specified by `selection`. An empty
    /// selection returns a correctly typed array with no rows.
    ///
    /// This method requires exclusive access because implementations may mutate
    /// internal caches or builders. However, repeated calls and later updates
    /// must observe the same logical accumulator state.
    fn evaluate_preserving(
      &mut self,
      _selection: GroupSelection<'_>,
    ) -> Result<ArrayRef> {
        not_impl_err!("Preserving grouped evaluation is not implemented")
    }

    /// Returns `true` if [`Self::evaluate_preserving`] is implemented.
    fn supports_evaluate_preserving(&self) -> bool {
        false
    }

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

    /// Returns intermediate aggregate state without changing the logical state
    /// or group indices.
    ///
    /// Each returned array has one row per selected group, in the order
    /// specified by `selection`. An empty selection returns the normal number
    /// of correctly typed state arrays, each with no rows.
    ///
    /// This method requires exclusive access because implementations may mutate
    /// internal caches or builders. However, repeated calls and later updates
    /// must observe the same logical accumulator state.
    fn state_preserving(
      &mut self,
      _selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Preserving grouped state is not implemented")
    }

    /// Returns `true` if [`Self::state_preserving`] is implemented.
    fn supports_state_preserving(&self) -> bool {
        false
    }

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
    /// Other arguments are the same as for [`Self::update_batch`], except that
    /// there is no `opt_filter` — aggregate filters are applied during the
    /// partial (update) phase, so by the time intermediate states are merged
    /// no per-row filtering is needed.
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
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
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>>;

    /// Amount of memory used to store the state of this accumulator,
    /// in bytes.
    ///
    /// This function is called once per batch, so it should be `O(n)` to
    /// compute, not `O(num_groups)`
    ///
    /// May be expensive; check the implementation before calling on hot paths.
    fn size(&self) -> usize;
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct BlocksIndex {
  block_index: usize,
  index_in_block: usize,
}

impl BlocksIndex {
  pub const ZERO: Self = Self {
    block_index: 0,
    index_in_block: 0,
  };
  pub const MAX: Self = Self {
    block_index: usize::MAX,
    index_in_block: usize::MAX,
  };

  pub fn new(block_index: usize, index_in_block: usize) -> Self {
    Self {
      block_index,
      index_in_block,
    }
  }

  pub fn new_in_first_block(index_in_block: usize) -> Self {
    // Implementation note:
    // not having From<usize> that will do this instead even when it will be more convenient
    // so we can later change the layout to be a single usize with bit shifts
    Self::new(0, index_in_block)
  }

  pub fn from_index_in_fixed_block_size(index: usize, block_size: usize) -> Self {
    Self::new(index / block_size, index % block_size)
  }

  pub fn into_index_in_fixed_block_size(&self, block_size: usize) -> usize {
    self.block_index * block_size + self.index_in_block
  }

  pub fn block_index(&self) -> usize {
    self.block_index
  }

  pub fn index_in_block(&self) -> usize {
    self.index_in_block
  }

  pub fn next_index_in_block(&self) -> Self {
    self.add_index_in_block(1)
  }

  pub fn add_index_in_block(&self, n: usize) -> Self {
    Self {
      block_index: self.block_index,
      index_in_block: self.index_in_block + n,
    }
  }

  pub fn add_fixed(&self, n: usize, block_size: usize) -> Self {
    let mut new = *self;
    new.add_mut_fixed(n, block_size);
    new
  }

  pub fn add_mut_fixed(&mut self, n: usize, block_size: usize) {
    self.block_index += (self.index_in_block + n) / block_size;
    self.index_in_block = (self.index_in_block + n) % block_size;
  }

  pub fn next_fixed(&self, block_size: usize) -> Self {
    let mut new = *self;
    new.next_mut_fixed(block_size);
    new
  }

  pub fn next_mut_fixed(&mut self, block_size: usize) {
    self.block_index += ((self.index_in_block + 1) == block_size) as usize;
    self.index_in_block = (self.index_in_block + 1) % block_size;
  }

  pub fn prev_fixed(&self, block_size: usize) -> Self {
    let mut new = *self;
    new.prev_mut_fixed(block_size);
    new
  }

  pub fn prev_mut_fixed(&mut self, block_size: usize) {
    self.block_index -= (self.index_in_block == 0) as usize;
    self.index_in_block = self.index_in_block.wrapping_sub(1).min(block_size - 1);
  }

  pub fn prev_block(&self) -> Self {
    Self {
      block_index: self.block_index - 1,
      index_in_block: self.index_in_block
    }
  }

  pub fn prev_block_saturate(&self) -> Self {
    self.block_index.checked_sub(1).map_or_else(
      || {
        Self {
          block_index: 0,
          index_in_block: 0
        }
      },
      |b| {
        Self {
          block_index: b,
          index_in_block: self.index_in_block
        }
      })
  }

  pub fn prev_block_checked(&self) -> Option<Self> {
    self.block_index.checked_sub(1).map(|b| {
      Self {
        block_index: b,
        index_in_block: self.index_in_block
      }
    })
  }

  pub fn sub(self, rhs: Self, batch_size: usize) -> Self {
    if self.index_in_block >= rhs.index_in_block {
      BlocksIndex::new(self.block_index - rhs.block_index, self.index_in_block - rhs.index_in_block)
    } else {
      BlocksIndex::new(self.block_index - rhs.block_index - 1, batch_size - (rhs.index_in_block - self.index_in_block))
    }
  }

  pub fn sub_flat(self, rhs_flat: usize, batch_size: usize) -> Self {
    BlocksIndex::from_index_in_fixed_block_size(self.into_index_in_fixed_block_size(batch_size) - rhs_flat, batch_size)
  }

  pub fn sub_flat_checked(self, rhs_flat: usize, batch_size: usize) -> Option<Self> {
    Some(BlocksIndex::from_index_in_fixed_block_size(self.into_index_in_fixed_block_size(batch_size).checked_sub(rhs_flat)?, batch_size))
  }

  pub fn sub_assign(&mut self, rhs: Self, batch_size: usize) {
    if self.index_in_block >= rhs.index_in_block {
      self.block_index -= rhs.block_index;
      self.index_in_block -= rhs.index_in_block;
    } else {
      self.block_index = self.block_index - rhs.block_index - 1;
      self.index_in_block = batch_size - (rhs.index_in_block - self.index_in_block);
    }
  }
}

impl PartialOrd for BlocksIndex {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for BlocksIndex {
  fn cmp(&self, other: &Self) -> Ordering {
    self.block_index.cmp(&other.block_index).then(self.index_in_block.cmp(&other.index_in_block))
  }
}

/// Describes how many rows should be emitted during grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockedEmitTo {
  All,
  /// Emit next group
  NextBlock,
  /// Emit only the first `n` groups and shift all existing group
  /// indexes down by `n`.
  ///
  /// For example, if `n=10`, group_index `0, 1, ... 9` are emitted
  /// and group indexes `10, 11, 12, ...` become `0, 1, 2, ...`.
  ///
  /// Requirements:
  /// 1. `n` must be smaller than block_size
  /// 2. `n` is not 0
  First(usize),
}

impl BlockedEmitTo {
  pub fn into_emit_to(self, len: usize, block_size: usize) -> Result<impl Iterator<Item=EmitTo> + Clone> {
    let mut to_emit = match self {
      Self::All => len,
      Self::NextBlock => len.min(block_size),
      Self::First(n) => {
        assert_ne_or_internal_err!(n, 0);
        assert_or_internal_err!(n <= len, "n ({n}) must be less than or equal current length ({})", len);
        assert_or_internal_err!(n < block_size, "n ({n}) must be less than current block size ({})", block_size);
        n
      }
    };

    let mut blocks = vec![];

    let emitting_all = len == to_emit;

    while to_emit > block_size {
      blocks.push(EmitTo::First(block_size));
      to_emit -= block_size
    }

    if to_emit > 0 && emitting_all {
      blocks.push(EmitTo::All);
    } else if to_emit > 0 {
      blocks.push(EmitTo::First(to_emit));
    }

    Ok(blocks.into_iter())
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
/// For more background, please also see the [Aggregating Millions of Groups Fast in Apache Arrow DataFusion 28.0.0 blog]
///
/// [Aggregating Millions of Groups Fast in Apache Arrow DataFusion 28.0.0 blog]: https://datafusion.apache.org/blog/2023/08/05/datafusion_fast_grouping
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
pub trait BlockedGroupsAccumulator: Send + std::any::Any {
  fn batch_size(&self) -> usize;

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
    group_indices: &[BlocksIndex],
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
  fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>>;

  /// Returns final aggregate values without changing the logical state or
  /// group indices.
  ///
  /// Rows are returned in the order specified by `selection`. An empty
  /// selection returns a correctly typed array with no rows.
  ///
  /// This method requires exclusive access because implementations may mutate
  /// internal caches or builders. However, repeated calls and later updates
  /// must observe the same logical accumulator state.
  fn evaluate_preserving(
    &mut self,
    _selection: BlockedGroupSelection<'_>,
  ) -> Result<ArrayRef> {
    not_impl_err!("Preserving grouped evaluation is not implemented")
  }

  /// Returns `true` if [`Self::evaluate_preserving`] is implemented.
  fn supports_evaluate_preserving(&self) -> bool {
    false
  }


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
  fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>>;

  /// Returns intermediate aggregate state without changing the logical state
  /// or group indices.
  ///
  /// Each returned array has one row per selected group, in the order
  /// specified by `selection`. An empty selection returns the normal number
  /// of correctly typed state arrays, each with no rows.
  ///
  /// This method requires exclusive access because implementations may mutate
  /// internal caches or builders. However, repeated calls and later updates
  /// must observe the same logical accumulator state.
  fn state_preserving(
    &mut self,
    _selection: BlockedGroupSelection<'_>,
  ) -> Result<Vec<ArrayRef>> {
    not_impl_err!("Preserving grouped state is not implemented")
  }

  /// Returns `true` if [`Self::state_preserving`] is implemented.
  fn supports_state_preserving(&self) -> bool {
    false
  }
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
  /// Other arguments are the same as for [`Self::update_batch`], except that
  /// there is no `opt_filter` — aggregate filters are applied during the
  /// partial (update) phase, so by the time intermediate states are merged
  /// no per-row filtering is needed.
  fn merge_batch(
    &mut self,
    values: &[ArrayRef],
    group_indices: &[BlocksIndex],
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
    values: &[ArrayRef],
    opt_filter: Option<&BooleanArray>,
  ) -> Result<Vec<ArrayRef>>;

  /// Amount of memory used to store the state of this accumulator,
  /// in bytes.
  ///
  /// This function is called once per batch, so it should be `O(n)` to
  /// compute, not `O(num_groups)`
  ///
  /// May be expensive; check the implementation before calling on hot paths.
  fn size(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use super::{EmitTo, GroupSelection};

    /// When `n` is small relative to `len`, the old `split_off(n) + swap` pattern had
    /// two allocation problems:
    ///
    /// 1. The returned Vec kept the original large backing allocation even though it
    ///    only contains `n` elements (wasted capacity on a short-lived value).
    /// 2. `split_off` allocated a fresh Vec for the `len - n` remaining elements,
    ///    even though that side is much larger than `n` — the expensive side to
    ///    allocate.
    ///
    /// `split_vec_min_alloc` fixes both: when `n * 2 <= len` it uses
    /// `drain(0..n).collect()`, allocating only `n` elements for the emitted prefix
    /// and keeping the original large backing in the remaining accumulator.
    #[test]
    fn take_needed_first_small_n_allocates_minimally() {
        let mut v: Vec<i32> = Vec::with_capacity(128);
        v.extend(0..20i32);
        let original_capacity = v.capacity(); // 128

        // n=4, n*2=8 <= len=20 -> drain branch in split_vec_min_alloc
        let emitted = EmitTo::First(4).take_needed(&mut v);

        assert_eq!(emitted, vec![0, 1, 2, 3]);
        assert_eq!(v, (4..20i32).collect::<Vec<_>>());

        // The emitted prefix must NOT carry the original large allocation.
        // Old split_off+swap returned a Vec with capacity=128 for only 4 elements.
        assert!(
            emitted.capacity() <= 4,
            "emitted prefix capacity {} should be ~n=4, not the original {}",
            emitted.capacity(),
            original_capacity,
        );

        // The remaining accumulator must retain the original large allocation so
        // that incoming groups don't immediately force a realloc.
        // Old split_off+swap left the remaining vec with a small fresh allocation.
        assert_eq!(
            v.capacity(),
            original_capacity,
            "remaining vec capacity {} should equal original {}",
            v.capacity(),
            original_capacity,
        );
    }

    #[test]
    fn group_selection_is_validated_once_and_reusable() {
        let values = [10, 20, 30, 40];
        let selected =
            GroupSelection::try_from_indices(&[3, 1, 3], values.len()).unwrap();
        assert_eq!(selected.total_num_groups(), values.len());
        assert_eq!(selected.len(), 3);
        assert_eq!(
            selected
                .iter()
                .map(|index| values[index])
                .collect::<Vec<_>>(),
            vec![40, 20, 40]
        );
        selected.validate_num_groups(values.len()).unwrap();
        let error = selected.validate_num_groups(values.len() - 1).unwrap_err();
        assert!(error.to_string().contains("constructed for 4 groups"));

        let all = GroupSelection::all(values.len());
        assert_eq!(
            all.iter().map(|index| values[index]).collect::<Vec<_>>(),
            values
        );

        let empty = GroupSelection::try_from_indices(&[], values.len()).unwrap();
        assert!(empty.is_empty());
        assert!(empty.iter().next().is_none());

        let error = GroupSelection::try_from_indices(&[4], values.len()).unwrap_err();
        assert!(error.to_string().contains("out of bounds"));
    }
}
