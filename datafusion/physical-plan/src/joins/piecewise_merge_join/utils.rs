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

// Returns boolean for whether the join is a right existence join served by
// `RightExistencePWMJStream`, which reads nothing but a single min/max off the buffered side.
//
// `RightMark` is deliberately excluded even though it is a right existence join: it needs the
// buffered side walked in order and an extra boolean column, so it must not inherit this
// stream's relaxed input requirements if the `try_new` gate is ever loosened.
pub(super) fn is_supported_right_existence_join(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::RightSemi | JoinType::RightAnti)
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

// Returns boolean for whether the join is an existence join that is currently supported by
// `PiecewiseMergeJoin`, which is every one of them except the Mark joins
pub(super) fn is_supported_existence_join(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::RightSemi
            | JoinType::RightAnti
    )
}

// Returns boolean to check if the join type needs to record
// buffered side matches for classic joins
pub(super) fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Full | JoinType::Left)
}
