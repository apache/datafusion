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

mod cross_join;
mod hash_join;
mod nested_loop_join;
mod sort_merge_join;
pub mod utils;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Partitioning mode to use for hash join
pub enum PartitionMode {
    /// Left/right children are partitioned using the left and right keys
    Partitioned,
    /// Left side will collected into one partition
    CollectLeft,
    /// When set to Auto, DataFusion optimizer will decide which PartitionMode mode(Partitioned/CollectLeft) is optimal based on statistics.
    /// It will also consider swapping the left and right inputs for the Join
    Auto,
}

pub use cross_join::CrossJoinExec;
pub use hash_join::HashJoinExec;
pub use nested_loop_join::NestedLoopJoinExec;

// Note: SortMergeJoin is not used in plans yet
pub use sort_merge_join::SortMergeJoinExec;
