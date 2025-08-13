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

//! Helpers for reasoning about which sides of a [`JoinType`] preserve
//! their input rows.

use crate::JoinType;

/// For a given [`JoinType`], determine whether each input of the join is
/// preserved for filters applied *after* the join.
///
/// A preserved side guarantees that each row in the join output maps back to a
/// row from the preserved input table. If a table is not preserved, it can
/// produce additional rows containing NULL values. For example:
///
/// * In an [`JoinType::Inner`] join, both sides are preserved because every
///   output row originates from a matching row on each side.
/// * In a [`JoinType::Left`] join, the left side is preserved but the right side
///   is not because the join may output extra rows with NULLs for the right
///   columns when there is no match.
///
/// The returned tuple is `(left_preserved, right_preserved)`.
pub fn lr_is_preserved(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (true, true),
        JoinType::Left => (true, false),
        JoinType::Right => (false, true),
        JoinType::Full => (false, false),
        // For semi/anti joins the non-driving side cannot appear in the output.
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => (true, false),
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => (false, true),
    }
}

/// For a given [`JoinType`], determine whether each input of the join is
/// preserved for filters in the join condition (ON-clause filters).
///
/// Only preserved sides may safely have filters pushed below the join.
///
/// The returned tuple is `(left_preserved, right_preserved)`.
pub fn on_lr_is_preserved(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (true, true),
        JoinType::Left => (false, true),
        JoinType::Right => (true, false),
        JoinType::Full => (false, false),
        JoinType::LeftSemi | JoinType::RightSemi => (true, true),
        JoinType::LeftAnti => (false, true),
        JoinType::RightAnti => (true, false),
        JoinType::LeftMark => (false, true),
        JoinType::RightMark => (true, false),
    }
}
