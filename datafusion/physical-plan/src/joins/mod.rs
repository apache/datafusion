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

//! DataFusion Join implementations

use core::fmt;
use std::fmt::{Display, Formatter};

use arrow::array::BooleanBufferBuilder;
pub use asof_join::{AsOfJoinExec, AsOfMatchExpr};
pub use cross_join::CrossJoinExec;
use datafusion_physical_expr::PhysicalExprRef;
/// # Public Only for Internal Use:
/// Exposed for expression serialization tests in `datafusion-proto`. Not part of the
/// supported public API.
/// See the [API health policy] for details.
///
/// [API health policy]: https://datafusion.apache.org/contributor-guide/api-health.html#datafusion-internal-public-apis
#[cfg(any(test, feature = "test_utils"))]
#[doc(hidden)]
pub use hash_join::HashTableLookupExpr;
pub use hash_join::{HashExpr, HashJoinExec, HashJoinExecBuilder, SeededRandomState};
pub use nested_loop_join::{NestedLoopJoinExec, NestedLoopJoinExecBuilder};
use parking_lot::Mutex;
// Note: SortMergeJoin is not used in plans yet
pub use piecewise_merge_join::PiecewiseMergeJoinExec;
pub use sort_merge_join::SortMergeJoinExec;
pub use symmetric_hash_join::SymmetricHashJoinExec;
mod asof_join;
mod chain;
mod cross_join;
mod hash_join;
mod logical_batch;
mod nested_loop_join;
mod piecewise_merge_join;
#[cfg(feature = "proto")]
mod proto;
mod sort_merge_join;
mod stream_join_utils;
mod symmetric_hash_join;
pub mod utils;

mod array_map;
mod join_filter;
/// Hash map implementations for join operations.
///
/// # Public Only for Internal Use:
/// Exposed for hash join tests in `datafusion-proto`. Not part of the supported public
/// API.
/// See the [API health policy] for details.
///
/// [API health policy]: https://datafusion.apache.org/contributor-guide/api-health.html#datafusion-internal-public-apis
#[cfg(any(test, feature = "test_utils"))]
#[doc(hidden)]
pub mod join_hash_map;
#[cfg(not(any(test, feature = "test_utils")))]
mod join_hash_map;

use array_map::ArrayMap;
use utils::JoinHashMapType;

/// The build-side map of a hash join, indexing build rows by join key.
///
/// Under [`NullEquality::NullEqualsNothing`], build rows with a NULL in any
/// join key column can never match a probe row and are omitted from the map.
/// [`Map::is_empty`] and [`Map::num_of_distinct_key`] therefore reflect the
/// *matchable* build rows: the map can be empty even when the build side
/// contains rows.
///
/// [`NullEquality::NullEqualsNothing`]: datafusion_common::NullEquality::NullEqualsNothing
pub enum Map {
    HashMap(Box<dyn JoinHashMapType>),
    ArrayMap(ArrayMap),
}

impl Map {
    /// Returns the number of elements in the map.
    pub fn num_of_distinct_key(&self) -> usize {
        match self {
            Map::HashMap(map) => map.len(),
            Map::ArrayMap(array_map) => array_map.num_of_distinct_key(),
        }
    }

    /// Returns `true` if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.num_of_distinct_key() == 0
    }
}

pub(crate) type MapOffset = (usize, Option<u64>);

#[cfg(test)]
pub mod test_utils;

/// The on clause of the join, as vector of (left, right) columns.
pub type JoinOn = Vec<(PhysicalExprRef, PhysicalExprRef)>;
/// Reference for JoinOn.
pub type JoinOnRef<'a> = &'a [(PhysicalExprRef, PhysicalExprRef)];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Hash join Partitioning mode
pub enum PartitionMode {
    /// Left/right children are partitioned using the left and right keys
    Partitioned,
    /// Left side will collected into one partition
    CollectLeft,
    /// DataFusion optimizer decides which PartitionMode
    /// mode(Partitioned/CollectLeft) is optimal based on statistics. It will
    /// also consider swapping the left and right inputs for the Join
    Auto,
}

impl Display for PartitionMode {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let partition_mode = match self {
            PartitionMode::Partitioned => "Partitioned",
            PartitionMode::CollectLeft => "CollectLeft",
            PartitionMode::Auto => "Auto",
        };
        write!(f, "{partition_mode}")
    }
}

/// Partitioning mode to use for symmetric hash join
#[derive(Hash, Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamJoinPartitionMode {
    /// Left/right children are partitioned using the left and right keys
    Partitioned,
    /// Both sides will collected into one partition
    SinglePartition,
}

/// Shared bitmap for visited left-side indices
type SharedBitmapBuilder = Mutex<BooleanBufferBuilder>;
