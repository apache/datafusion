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
//! **`preservation_for_output_filters` answers post‑join output filtering, whereas `preservation_for_on_filters` addresses ON‑clause feasibility.**

use crate::JoinType;

/// For a given [`JoinType`], determine whether each input of the join is
/// preserved for filters applied *after* the join.
///
/// Row preservation means every output row can be traced back to a row from
/// that input. Non‑preserved sides may introduce additional NULL padded rows.
/// The table below visualises the behaviour (`✓` preserved, `✗` not preserved):
///
/// ```text
///                 left right
/// INNER             ✓     ✓
/// LEFT              ✓     ✗
/// RIGHT             ✗     ✓
/// FULL              ✗     ✗
/// LEFT SEMI         ✓     ✗
/// LEFT ANTI         ✓     ✗
/// LEFT MARK         ✓     ✗
/// RIGHT SEMI        ✗     ✓
/// RIGHT ANTI        ✗     ✓
/// RIGHT MARK        ✗     ✓
/// ```
///
/// The returned tuple `(left_preserved, right_preserved)` reports whether each
/// side of the join preserves its input rows under post‑join filtering.
#[inline]
pub const fn preservation_for_output_filters(join_type: JoinType) -> (bool, bool) {
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
/// preserved for filters in the join condition (ON‑clause filters).
///
/// Filters on ON‑clause expressions may only reference sides that are
/// preserved; otherwise pushing the filter below the join could drop rows.
/// This table shows preservation for ON‑clause evaluation (`✓` preserved):
///
/// ```text
///                 left right
/// INNER             ✓     ✓
/// LEFT              ✗     ✓
/// RIGHT             ✓     ✗
/// FULL              ✗     ✗
/// LEFT SEMI         ✓     ✓
/// RIGHT SEMI        ✓     ✓
/// LEFT ANTI         ✗     ✓
/// RIGHT ANTI        ✓     ✗
/// LEFT MARK         ✗     ✓
/// RIGHT MARK        ✓     ✗
/// ```
///
/// The returned tuple `(left_preserved, right_preserved)` reports which sides
/// may safely participate in ON‑clause filtering.
#[inline]
pub const fn preservation_for_on_filters(join_type: JoinType) -> (bool, bool) {
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

#[allow(unused_imports)]
pub(crate) use preservation_for_on_filters as on_lr_is_preserved;
#[allow(unused_imports)]
pub(crate) use preservation_for_output_filters as lr_is_preserved;
