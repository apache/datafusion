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

const LEFT_PRESERVING: &[JoinType] =
    &[JoinType::Left, JoinType::Full, JoinType::LeftMark];

const RIGHT_PRESERVING: &[JoinType] =
    &[JoinType::Right, JoinType::Full, JoinType::RightMark];

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

    /// Returns true if this join type preserves all rows from the specified `side`.
    pub fn preserves(self, side: JoinSide) -> bool {
        match side {
            JoinSide::Left => LEFT_PRESERVING.contains(&self),
            JoinSide::Right => RIGHT_PRESERVING.contains(&self),
            JoinSide::None => false,
        }
    }

    /// Returns true if this join type preserves all rows from its left input.
    ///
    /// For [`JoinType::Left`], [`JoinType::Full`], and [`JoinType::LeftMark`] joins
    /// every row from the left side will appear in the output at least once.
    pub fn preserves_left(self) -> bool {
        self.preserves(JoinSide::Left)
    }

    /// Returns true if this join type preserves all rows from its right input.
    ///
    /// For [`JoinType::Right`], [`JoinType::Full`], and [`JoinType::RightMark`] joins
    /// every row from the right side will appear in the output at least once.
    pub fn preserves_right(self) -> bool {
        self.preserves(JoinSide::Right)
    }

    /// Returns the input side eligible for dynamic filter pushdown.
    ///
    /// The side returned here can have a [`DynamicFilterPhysicalExpr`] pushed
    /// into it, allowing values read from the opposite input to prune rows
    /// before the join executes. When both inputs must be preserved,
    /// dynamic filter pushdown is not supported and [`JoinSide::None`] is
    /// returned.
    ///
    /// If neither input is preserving (for example with [`JoinType::Inner`],
    /// [`JoinType::LeftSemi`], [`JoinType::RightSemi`],
    /// [`JoinType::LeftAnti`], or [`JoinType::RightAnti`]), either side could
    /// in principle receive the pushed filter. DataFusion selects the probe
    /// side: for [`JoinType::LeftSemi`] and [`JoinType::LeftAnti`] this is the
    /// left input, for [`JoinType::RightSemi`] and [`JoinType::RightAnti`] it
    /// is the right input, and for other joins the right input is used by
    /// default as joins typically treat the right as the probe side.
    pub fn dynamic_filter_side(self) -> JoinSide {
        use JoinSide::*;
        let preserves_left = self.preserves_left();
        let preserves_right = self.preserves_right();

        match (preserves_left, preserves_right) {
            (true, true) => None,
            (true, false) => Right,
            (false, true) => Left,
            (false, false) => match self {
                JoinType::LeftSemi | JoinType::LeftAnti => Left,
                JoinType::RightSemi | JoinType::RightAnti => Right,
                _ => Right,
            },
        }
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
mod tests {
    use super::*;

    #[test]
    fn test_join_type_swap() {
        assert_eq!(JoinType::Inner.swap(), JoinType::Inner);
        assert_eq!(JoinType::Left.swap(), JoinType::Right);
        assert_eq!(JoinType::Right.swap(), JoinType::Left);
        assert_eq!(JoinType::Full.swap(), JoinType::Full);
        assert_eq!(JoinType::LeftSemi.swap(), JoinType::RightSemi);
        assert_eq!(JoinType::RightSemi.swap(), JoinType::LeftSemi);
        assert_eq!(JoinType::LeftAnti.swap(), JoinType::RightAnti);
        assert_eq!(JoinType::RightAnti.swap(), JoinType::LeftAnti);
        assert_eq!(JoinType::LeftMark.swap(), JoinType::RightMark);
        assert_eq!(JoinType::RightMark.swap(), JoinType::LeftMark);
    }

    #[test]
    fn test_join_type_supports_swap() {
        use JoinType::*;
        let supported = [
            Inner, Left, Right, Full, LeftSemi, RightSemi, LeftAnti, RightAnti,
        ];
        for jt in supported {
            assert!(jt.supports_swap(), "{jt:?} should support swap");
        }
        let not_supported = [LeftMark, RightMark];
        for jt in not_supported {
            assert!(!jt.supports_swap(), "{jt:?} should not support swap");
        }
    }

    #[test]
    fn test_preserves_sides() {
        use JoinSide::*;

        assert!(JoinType::Left.preserves(Left));
        assert!(JoinType::Full.preserves(Left));
        assert!(JoinType::LeftMark.preserves(Left));
        assert!(!JoinType::LeftSemi.preserves(Left));

        assert!(JoinType::Right.preserves(Right));
        assert!(JoinType::Full.preserves(Right));
        assert!(JoinType::RightMark.preserves(Right));
        assert!(!JoinType::RightSemi.preserves(Right));

        assert!(!JoinType::LeftAnti.preserves(Left));
        assert!(!JoinType::LeftAnti.preserves(Right));
        assert!(!JoinType::RightAnti.preserves(Left));
        assert!(!JoinType::RightAnti.preserves(Right));
    }

    #[test]
    fn test_dynamic_filter_side() {
        use JoinSide::*;

        assert_eq!(JoinType::Inner.dynamic_filter_side(), Right);
        assert_eq!(JoinType::Left.dynamic_filter_side(), Right);
        assert_eq!(JoinType::Right.dynamic_filter_side(), Left);
        assert_eq!(JoinType::Full.dynamic_filter_side(), None);
        assert_eq!(JoinType::LeftSemi.dynamic_filter_side(), Left);
        assert_eq!(JoinType::RightSemi.dynamic_filter_side(), Right);
        assert_eq!(JoinType::LeftAnti.dynamic_filter_side(), Left);
        assert_eq!(JoinType::RightAnti.dynamic_filter_side(), Right);
        assert_eq!(JoinType::LeftMark.dynamic_filter_side(), Right);
        assert_eq!(JoinType::RightMark.dynamic_filter_side(), Left);
    }

    #[test]
    fn test_dynamic_filter_side_preservation_logic() {
        use JoinSide::*;

        for jt in [JoinType::Left, JoinType::LeftMark] {
            assert!(jt.preserves_left());
            assert!(!jt.preserves_right());
            assert_eq!(jt.dynamic_filter_side(), Right);
        }

        for jt in [JoinType::Right, JoinType::RightMark] {
            assert!(!jt.preserves_left());
            assert!(jt.preserves_right());
            assert_eq!(jt.dynamic_filter_side(), Left);
        }

        for jt in [JoinType::Full] {
            assert!(jt.preserves_left());
            assert!(jt.preserves_right());
            assert_eq!(jt.dynamic_filter_side(), None);
        }
    }
}
