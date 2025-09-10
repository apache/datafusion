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

//! Defines the [`JoinType`], [`JoinConstraint`] and [`JoinSide`] types.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use crate::error::_not_impl_err;
use crate::{DataFusionError, Result};

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum JoinType {
    /// Inner Join - Returns only rows where there is a matching value in both tables based on the join condition.
    /// For example, if joining table A and B on A.id = B.id, only rows where A.id equals B.id will be included.
    /// All columns from both tables are returned for the matching rows. Non-matching rows are excluded entirely.
    Inner,
    /// Left Join - Returns all rows from the left table and matching rows from the right table.
    /// If no match, NULL values are returned for columns from the right table.
    Left,
    /// Right Join - Returns all rows from the right table and matching rows from the left table.
    /// If no match, NULL values are returned for columns from the left table.
    Right,
    /// Full Join (also called Full Outer Join) - Returns all rows from both tables, matching rows where possible.
    /// When a row from either table has no match in the other table, the missing columns are filled with NULL values.
    /// For example, if table A has row X with no match in table B, the result will contain row X with NULL values for all of table B's columns.
    /// This join type preserves all records from both tables, making it useful when you need to see all data regardless of matches.
    Full,
    /// Left Semi Join - Returns rows from the left table that have matching rows in the right table.
    /// Only columns from the left table are returned.
    LeftSemi,
    /// Right Semi Join - Returns rows from the right table that have matching rows in the left table.
    /// Only columns from the right table are returned.
    RightSemi,
    /// Left Anti Join - Returns rows from the left table that do not have a matching row in the right table.
    LeftAnti,
    /// Right Anti Join - Returns rows from the right table that do not have a matching row in the left table.
    RightAnti,
    /// Left Mark join
    ///
    /// Returns one record for each record from the left input. The output contains an additional
    /// column "mark" which is true if there is at least one match in the right input where the
    /// join condition evaluates to true. Otherwise, the mark column is false. For more details see
    /// [1]. This join type is used to decorrelate EXISTS subqueries used inside disjunctive
    /// predicates.
    ///
    /// Note: This we currently do not implement the full null semantics for the mark join described
    /// in [1] which will be needed if we and ANY subqueries. In our version the mark column will
    /// only be true for had a match and false when no match was found, never null.
    ///
    /// [1]: http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    LeftMark,
    /// Right Mark Join
    ///
    /// Same logic as the LeftMark Join above, however it returns a record for each record from the
    /// right input.
    RightMark,
}

impl JoinType {
    pub fn is_outer(self) -> bool {
        self == JoinType::Left || self == JoinType::Right || self == JoinType::Full
    }

    /// Returns the `JoinType` if the (2) inputs were swapped
    ///
    /// Panics if [`Self::supports_swap`] returns false
    pub fn swap(&self) -> JoinType {
        match self {
            JoinType::Inner => JoinType::Inner,
            JoinType::Full => JoinType::Full,
            JoinType::Left => JoinType::Right,
            JoinType::Right => JoinType::Left,
            JoinType::LeftSemi => JoinType::RightSemi,
            JoinType::RightSemi => JoinType::LeftSemi,
            JoinType::LeftAnti => JoinType::RightAnti,
            JoinType::RightAnti => JoinType::LeftAnti,
            JoinType::LeftMark => JoinType::RightMark,
            JoinType::RightMark => JoinType::LeftMark,
        }
    }

    /// Does the join type support swapping inputs?
    pub fn supports_swap(&self) -> bool {
        matches!(
            self,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Full
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
        )
    }

    /// Returns `true` if the left input is preserved in the output of this join
    /// (i.e. the output is a subset of the left input without additional rows).
    pub fn preserves_left(self) -> bool {
        matches!(
            self,
            JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark
        )
    }

    /// Returns `true` if the right input is preserved in the output of this join
    /// (i.e. the output is a subset of the right input without additional rows).
    pub fn preserves_right(self) -> bool {
        matches!(
            self,
            JoinType::Inner
                | JoinType::Right
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::RightMark
        )
    }

    /// Returns `true` if the left input is preserved for the join condition
    /// (`ON` clause) of this join type.
    pub fn on_preserves_left(self) -> bool {
        matches!(
            self,
            JoinType::Inner
                | JoinType::Right
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::RightMark
        )
    }

    /// Returns `true` if the right input is preserved for the join condition
    /// (`ON` clause) of this join type.
    pub fn on_preserves_right(self) -> bool {
        matches!(
            self,
            JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark
        )
    }

    /// Returns which side of the join should receive dynamic filter pushdown
    /// bounds for this join type.
    pub fn dynamic_filter_side(self) -> JoinSide {
        use JoinSide::*;

        let preserves_left = self.preserves_left();
        let preserves_right = self.preserves_right();
        let on_preserves_left = self.on_preserves_left();
        let on_preserves_right = self.on_preserves_right();

        if !preserves_left && !preserves_right {
            None
        } else if !preserves_left && on_preserves_left {
            Left
        } else if !preserves_right && on_preserves_right {
            Right
        } else if preserves_left && preserves_right {
            // Both sides are preserved and participate in the join condition.
            // Either side could receive the dynamic filter; choose the right for
            // a deterministic result.
            Right
        } else {
            // A side is not preserved by the join and its corresponding `ON`
            // clause is also not preserved, so no dynamic filters should be
            // pushed.
            None
        }
    }

    /// Returns `true` if rows from the left input may appear in the output even when
    /// they have no matching rows on the right side of the join.
    ///
    /// This helper is used by join planning code to determine if the left input side
    /// is "preserved" during execution. Dynamic filters can only be pushed to
    /// the non-preserved side of a join without affecting its semantics.
    pub fn preserves_unmatched_left(self) -> bool {
        matches!(
            self,
            JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::LeftMark
        )
    }

    /// Returns `true` if rows from the right input may appear in the output even when
    /// they have no matching rows on the left side of the join.
    ///
    /// This helper is used by join planning code to determine if the right input side
    /// is "preserved" during execution. Dynamic filters can only be pushed to
    /// the non-preserved side of a join without affecting its semantics.
    pub fn preserves_unmatched_right(self) -> bool {
        matches!(
            self,
            JoinType::Right | JoinType::Full | JoinType::RightAnti | JoinType::RightMark
        )
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let join_type = match self {
            JoinType::Inner => "Inner",
            JoinType::Left => "Left",
            JoinType::Right => "Right",
            JoinType::Full => "Full",
            JoinType::LeftSemi => "LeftSemi",
            JoinType::RightSemi => "RightSemi",
            JoinType::LeftAnti => "LeftAnti",
            JoinType::RightAnti => "RightAnti",
            JoinType::LeftMark => "LeftMark",
            JoinType::RightMark => "RightMark",
        };
        write!(f, "{join_type}")
    }
}

impl FromStr for JoinType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_uppercase();
        match s.as_str() {
            "INNER" => Ok(JoinType::Inner),
            "LEFT" => Ok(JoinType::Left),
            "RIGHT" => Ok(JoinType::Right),
            "FULL" => Ok(JoinType::Full),
            "LEFTSEMI" => Ok(JoinType::LeftSemi),
            "RIGHTSEMI" => Ok(JoinType::RightSemi),
            "LEFTANTI" => Ok(JoinType::LeftAnti),
            "RIGHTANTI" => Ok(JoinType::RightAnti),
            "LEFTMARK" => Ok(JoinType::LeftMark),
            "RIGHTMARK" => Ok(JoinType::RightMark),
            _ => _not_impl_err!("The join type {s} does not exist or is not implemented"),
        }
    }
}

/// Join constraint
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum JoinConstraint {
    /// Join ON
    On,
    /// Join USING
    Using,
}

impl Display for JoinSide {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            JoinSide::Left => write!(f, "left"),
            JoinSide::Right => write!(f, "right"),
            JoinSide::None => write!(f, "none"),
        }
    }
}

/// Join side.
/// Stores the referred table side during calculations
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinSide {
    /// Left side of the join
    Left,
    /// Right side of the join
    Right,
    /// Neither side of the join, used for Mark joins where the mark column does not belong to
    /// either side of the join
    None,
}

impl JoinSide {
    /// Inverse the join side
    pub fn negate(&self) -> Self {
        match self {
            JoinSide::Left => JoinSide::Right,
            JoinSide::Right => JoinSide::Left,
            JoinSide::None => JoinSide::None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity)]
pub fn join_type_truth_table_cases() -> Vec<(JoinType, (bool, bool, bool, bool, JoinSide))>
{
    use JoinType::*;
    vec![
        (Inner, (true, true, true, true, JoinSide::Right)),
        (Left, (true, false, false, true, JoinSide::Right)),
        (Right, (false, true, true, false, JoinSide::Left)),
        (Full, (false, false, false, false, JoinSide::None)),
        (LeftSemi, (true, false, true, true, JoinSide::Right)),
        (RightSemi, (false, true, true, true, JoinSide::Left)),
        (LeftAnti, (true, false, false, true, JoinSide::Right)),
        (RightAnti, (false, true, true, false, JoinSide::Left)),
        (LeftMark, (true, false, false, true, JoinSide::Right)),
        (RightMark, (false, true, true, false, JoinSide::Left)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_type_truth_table() {
        for (jt, (pl, pr, on_pl, on_pr, df_side)) in join_type_truth_table_cases() {
            assert_eq!(jt.preserves_left(), pl, "{jt:?} preserves_left");
            assert_eq!(jt.preserves_right(), pr, "{jt:?} preserves_right");
            assert_eq!(jt.on_preserves_left(), on_pl, "{jt:?} on_preserves_left");
            assert_eq!(jt.on_preserves_right(), on_pr, "{jt:?} on_preserves_right");

            let expected_side = if !pl && !pr {
                JoinSide::None
            } else if !pl && on_pl {
                JoinSide::Left
            } else if !pr && on_pr {
                JoinSide::Right
            } else {
                // Both sides are preserved
                JoinSide::Right
            };

            assert_eq!(df_side, expected_side, "{jt:?} case dynamic_filter_side");
            assert_eq!(
                jt.dynamic_filter_side(),
                expected_side,
                "{jt:?} dynamic_filter_side"
            );
        }
    }

    #[test]
    fn unmatched_preservation_helpers_truth_table() {
        use JoinType::*;

        let cases = vec![
            (Inner, (false, false)),
            (Left, (true, false)),
            (Right, (false, true)),
            (Full, (true, true)),
            (LeftSemi, (false, false)),
            (RightSemi, (false, false)),
            (LeftAnti, (true, false)),
            (RightAnti, (false, true)),
            (LeftMark, (true, false)),
            (RightMark, (false, true)),
        ];

        for (jt, (mul, mur)) in cases {
            assert_eq!(
                jt.preserves_unmatched_left(),
                mul,
                "{jt:?} preserves_unmatched_left",
            );
            assert_eq!(
                jt.preserves_unmatched_right(),
                mur,
                "{jt:?} preserves_unmatched_right",
            );
        }
    }
}
