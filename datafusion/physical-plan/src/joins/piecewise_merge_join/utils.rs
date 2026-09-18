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
// `RightMark` belongs here too: deciding its mark column is the same one-key comparison as
// `RightSemi`/`RightAnti`, just kept instead of used to filter, so it needs no more of the
// buffered side than they do.
pub(super) fn is_supported_right_existence_join(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark
    )
}

// Returns boolean to check if the join type needs to record
// buffered side matches for classic joins
pub(super) fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Full | JoinType::Left)
}
