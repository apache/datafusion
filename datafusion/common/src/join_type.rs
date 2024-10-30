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
    /// Inner Join
    Inner,
    /// Left Join
    Left,
    /// Right Join
    Right,
    /// Full Join
    Full,
    /// Left Semi Join
    LeftSemi,
    /// Right Semi Join
    RightSemi,
    /// Left Anti Join
    LeftAnti,
    /// Right Anti Join
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
    /// [1] http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    LeftMark,
}

impl JoinType {
    pub fn is_outer(self) -> bool {
        self == JoinType::Left || self == JoinType::Right || self == JoinType::Full
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
