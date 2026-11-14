// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Defines the [`JoinType`], [`JoinConstraint`] and [`JoinSide`] types.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use crate::error::_not_impl_err;
use crate::{DataFusionError, Result};

/// Join type
///
/// A join combines rows from two inputs, called the "left" and "right"
/// inputs, according to a join condition. The `JoinType` determines which
/// rows appear in the output and which columns the output contains.
///
/// # Example Inputs
///
/// The examples on each variant use the following two inputs, joined on
/// `left.id = right.id`. Note that `id = 2` appears only in `left`,
/// `id = 4` appears only in `right`, and `id = 3` appears twice in `right`.
///
/// ```text
/// left               right
/// +----+------+      +----+-------+
/// | id | name |      | id | color |
/// +----+------+      +----+-------+
/// | 1  | a    |      | 1  | red   |
/// | 2  | b    |      | 3  | blue  |
/// | 3  | c    |      | 3  | green |
/// +----+------+      | 4  | pink  |
///                    +----+-------+
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum JoinType {
    /// Inner Join
    ///
    /// Returns only the rows where the join condition matches a row from the
    /// left input with a row from the right input. Each left row is combined
    /// with every right row it matches, so a row can appear more than once.
    /// Rows without a match on the other side are excluded. The output
    /// contains all columns from both inputs.
    ///
    /// For the [example inputs](JoinType#example-inputs), `id = 2` and
    /// `id = 4` have no match and are excluded, while `id = 3` appears twice
    /// because it matches two rows from the right input:
    ///
    /// ```text
    /// +----+------+----+-------+
    /// | id | name | id | color |
    /// +----+------+----+-------+
    /// | 1  | a    | 1  | red   |
    /// | 3  | c    | 3  | blue  |
    /// | 3  | c    | 3  | green |
    /// +----+------+----+-------+
    /// ```
    Inner,
    /// Left Join (also called Left Outer Join)
    ///
    /// Returns all rows from the left input. Each left row that matches one
    /// or more right rows is combined with each of them, as in an [`Inner`]
    /// join. A left row with no match is output once, with `NULL` for all
    /// columns from the right input. Right rows with no match are excluded.
    /// The output contains all columns from both inputs.
    ///
    /// For the [example inputs](JoinType#example-inputs), `id = 2` is kept
    /// with `NULL` right columns, while `id = 4` (only in the right input)
    /// is excluded:
    ///
    /// ```text
    /// +----+------+------+-------+
    /// | id | name | id   | color |
    /// +----+------+------+-------+
    /// | 1  | a    | 1    | red   |
    /// | 2  | b    | NULL | NULL  |
    /// | 3  | c    | 3    | blue  |
    /// | 3  | c    | 3    | green |
    /// +----+------+------+-------+
    /// ```
    ///
    /// [`Inner`]: JoinType::Inner
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
    /// For scalar `NOT IN`, DataFusion can plan a null-aware hash mark join where the
    /// mark column is nullable: TRUE for a match, NULL for SQL UNKNOWN, and FALSE
    /// otherwise. Row-valued multi-column `NOT IN` and non-hash residual predicate
    /// null-aware mark semantics are not implemented.
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

    /// Whether each side of the join is preserved for ON-clause filter pushdown.
    ///
    /// It is only correct to push ON-clause filters below a join for preserved
    /// inputs.
    ///
    /// # "Preserved" input definition
    ///
    /// A join side is preserved if the join returns all or a subset of the rows
    /// from that side, such that each output row directly maps to an input row.
    /// If a side is not preserved, the join can produce extra null rows that
    /// don't map to any input row.
    ///
    /// # Return Value
    ///
    /// A tuple of booleans - (left_preserved, right_preserved).
    pub fn on_lr_is_preserved(&self) -> (bool, bool) {
        match self {
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
                | JoinType::LeftMark
                | JoinType::RightMark
        )
    }

    /// Returns true when an empty build side necessarily produces an empty
    /// result for this join type.
    pub fn empty_build_side_produces_empty_result(self) -> bool {
        matches!(
            self,
            JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark
                | JoinType::RightSemi
        )
    }

    /// Returns true when an empty build-side map necessarily produces an empty
    /// result for this join type, even if the build side still contains rows.
    ///
    /// Every output row of these join types requires a matching build row, so
    /// when the map has no matchable keys the result is empty regardless of the
    /// probe side. Note this is a subset of
    /// [`Self::empty_build_side_produces_empty_result`]: an empty build side
    /// yields an empty map, but the map can also be empty when every build row
    /// has a NULL join key under [`NullEquality::NullEqualsNothing`].
    ///
    /// [`NullEquality::NullEqualsNothing`]: crate::NullEquality::NullEqualsNothing
    pub fn empty_map_produces_empty_result(self) -> bool {
        matches!(
            self,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
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
