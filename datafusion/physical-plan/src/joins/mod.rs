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

use arrow::array::BooleanBufferBuilder;
pub use cross_join::CrossJoinExec;
use datafusion_physical_expr::PhysicalExprRef;
pub use hash_join::{HashExpr, HashJoinExec, HashTableLookupExpr, SeededRandomState};
pub use nested_loop_join::NestedLoopJoinExec;
use parking_lot::Mutex;
// Note: SortMergeJoin is not used in plans yet
pub use piecewise_merge_join::PiecewiseMergeJoinExec;
pub use sort_merge_join::SortMergeJoinExec;
pub use symmetric_hash_join::SymmetricHashJoinExec;
pub mod chain;
mod cross_join;
mod hash_join;
mod nested_loop_join;
mod piecewise_merge_join;
mod sort_merge_join;
mod stream_join_utils;
mod symmetric_hash_join;
pub mod utils;

mod array_map;
mod join_filter;
/// Hash map implementations for join operations.
///
/// Note: This module is public for internal testing purposes only
/// and is not guaranteed to be stable across versions.
pub mod join_hash_map;

use array_map::ArrayMap;
use utils::JoinHashMapType;

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
