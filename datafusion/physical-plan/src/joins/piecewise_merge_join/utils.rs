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

use datafusion_expr::JoinType;

// Returns boolean for whether the join is a right existence join
pub(super) fn is_right_existence_join(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::RightAnti | JoinType::RightSemi | JoinType::RightMark
    )
}

// Returns boolean for whether the join is an existence join
pub(super) fn is_existence_join(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftMark
            | JoinType::RightMark
    )
}

// Returns boolean for whether the join is a left existence join that is currently
// supported by `PiecewiseMergeJoin`. These do not require swapping the inputs: the
// marked (left) side is already the buffered side, so `ExistencePWMJStream` can track the
// matched suffix and slice the buffered batch at its start.
pub(super) fn is_supported_existence_join(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti)
}

// Returns boolean to check if the join type needs to record
// buffered side matches for classic joins
pub(super) fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Full | JoinType::Left)
}

// Returns boolean for whether or not we need to build the buffered side
// bitmap for marking matched rows on the buffered side.
//
// `LeftSemi`/`LeftAnti` are absent on purpose: `ExistencePWMJStream` only ever marks a
// contiguous suffix of the buffered side, so it tracks the boundary as a single index
// (`BufferedSideData::existence_min_marked`) and needs no bitmap.
pub(super) fn build_visited_indices_map(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Full
            | JoinType::Left
            | JoinType::RightAnti
            | JoinType::RightSemi
            | JoinType::LeftMark
            | JoinType::RightMark
    )
}
