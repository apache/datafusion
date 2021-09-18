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

//! Window frame
//!
//! The frame-spec determines which output rows are read by an aggregate window function. The frame-spec consists of four parts:
//! - A frame type - either ROWS, RANGE or GROUPS,
//! - A starting frame boundary,
//! - An ending frame boundary,
//! - An EXCLUDE clause.

use crate::error::{DataFusionError, Result};
use sqlparser::ast;
use std::cmp::Ordering;
use std::convert::{From, TryFrom};
use std::fmt;

/// The frame-spec determines which output rows are read by an aggregate window function.
///
/// The ending frame boundary can be omitted (if the BETWEEN and AND keywords that surround the
/// starting frame boundary are also omitted), in which case the ending frame boundary defaults to
/// CURRENT ROW.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct WindowFrame {
    /// A frame type - either ROWS, RANGE or GROUPS
    pub units: WindowFrameUnits,
    /// A starting frame boundary
    pub start_bound: WindowFrameBound,
    /// An ending frame boundary
    pub end_bound: WindowFrameBound,
}

impl fmt::Display for WindowFrame {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} BETWEEN {} AND {}",
            self.units, self.start_bound, self.end_bound
        )?;
        Ok(())
    }
}

impl TryFrom<ast::WindowFrame> for WindowFrame {
    type Error = DataFusionError;

    fn try_from(value: ast::WindowFrame) -> Result<Self> {
        let start_bound = value.start_bound.into();
        let end_bound = value
            .end_bound
            .map(WindowFrameBound::from)
            .unwrap_or(WindowFrameBound::CurrentRow);

        if let WindowFrameBound::Following(None) = start_bound {
            Err(DataFusionError::Execution(
                "Invalid window frame: start bound cannot be unbounded following"
                    .to_owned(),
            ))
        } else if let WindowFrameBound::Preceding(None) = end_bound {
            Err(DataFusionError::Execution(
                "Invalid window frame: end bound cannot be unbounded preceding"
                    .to_owned(),
            ))
        } else if start_bound > end_bound {
            Err(DataFusionError::Execution(format!(
            "Invalid window frame: start bound ({}) cannot be larger than end bound ({})",
            start_bound, end_bound
        )))
        } else {
            let units = value.units.into();
            if units == WindowFrameUnits::Range {
                for bound in &[start_bound, end_bound] {
                    match bound {
                        WindowFrameBound::Preceding(Some(v))
                        | WindowFrameBound::Following(Some(v))
                            if *v > 0 =>
                        {
                            Err(DataFusionError::NotImplemented(format!(
                                "With WindowFrameUnits={}, the bound cannot be {} PRECEDING or FOLLOWING at the moment",
                                units, v
                            )))
                        }
                        _ => Ok(()),
                    }?;
                }
            }
            Ok(Self {
                units,
                start_bound,
                end_bound,
            })
        }
    }
}

impl Default for WindowFrame {
    fn default() -> Self {
        WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: WindowFrameBound::CurrentRow,
        }
    }
}

/// There are five ways to describe starting and ending frame boundaries:
///
/// 1. UNBOUNDED PRECEDING
/// 2. <expr> PRECEDING
/// 3. CURRENT ROW
/// 4. <expr> FOLLOWING
/// 5. UNBOUNDED FOLLOWING
///
/// in this implementation we'll only allow <expr> to be u64 (i.e. no dynamic boundary)
#[derive(Debug, Clone, Copy, Eq)]
pub enum WindowFrameBound {
    /// 1. UNBOUNDED PRECEDING
    /// The frame boundary is the first row in the partition.
    ///
    /// 2. <expr> PRECEDING
    /// <expr> must be a non-negative constant numeric expression. The boundary is a row that
    /// is <expr> "units" prior to the current row.
    Preceding(Option<u64>),
    /// 3. The current row.
    ///
    /// For RANGE and GROUPS frame types, peers of the current row are also
    /// included in the frame, unless specifically excluded by the EXCLUDE clause.
    /// This is true regardless of whether CURRENT ROW is used as the starting or ending frame
    /// boundary.
    CurrentRow,
    /// 4. This is the same as "<expr> PRECEDING" except that the boundary is <expr> units after the
    /// current rather than before the current row.
    ///
    /// 5. UNBOUNDED FOLLOWING
    /// The frame boundary is the last row in the partition.
    Following(Option<u64>),
}

impl From<ast::WindowFrameBound> for WindowFrameBound {
    fn from(value: ast::WindowFrameBound) -> Self {
        match value {
            ast::WindowFrameBound::Preceding(v) => Self::Preceding(v),
            ast::WindowFrameBound::Following(v) => Self::Following(v),
            ast::WindowFrameBound::CurrentRow => Self::CurrentRow,
        }
    }
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => write!(f, "{} PRECEDING", n),
            WindowFrameBound::Following(Some(n)) => write!(f, "{} FOLLOWING", n),
        }
    }
}

impl PartialEq for WindowFrameBound {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for WindowFrameBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WindowFrameBound {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_rank().cmp(&other.get_rank())
    }
}

impl WindowFrameBound {
    /// get the rank of this window frame bound.
    ///
    /// the rank is a tuple of (u8, u64) because we'll firstly compare the kind and then the value
    /// which requires special handling e.g. with preceding the larger the value the smaller the
    /// rank and also for 0 preceding / following it is the same as current row
    fn get_rank(&self) -> (u8, u64) {
        match self {
            WindowFrameBound::Preceding(None) => (0, 0),
            WindowFrameBound::Following(None) => (4, 0),
            WindowFrameBound::Preceding(Some(0))
            | WindowFrameBound::CurrentRow
            | WindowFrameBound::Following(Some(0)) => (2, 0),
            WindowFrameBound::Preceding(Some(v)) => (1, u64::MAX - *v),
            WindowFrameBound::Following(Some(v)) => (3, *v),
        }
    }
}

/// There are three frame types: ROWS, GROUPS, and RANGE. The frame type determines how the
/// starting and ending boundaries of the frame are measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum WindowFrameUnits {
    /// The ROWS frame type means that the starting and ending boundaries for the frame are
    /// determined by counting individual rows relative to the current row.
    Rows,
    /// The RANGE frame type requires that the ORDER BY clause of the window have exactly one
    /// term. Call that term "X". With the RANGE frame type, the elements of the frame are
    /// determined by computing the value of expression X for all rows in the partition and framing
    /// those rows for which the value of X is within a certain range of the value of X for the
    /// current row.
    Range,
    /// The GROUPS frame type means that the starting and ending boundaries are determine
    /// by counting "groups" relative to the current group. A "group" is a set of rows that all have
    /// equivalent values for all all terms of the window ORDER BY clause.
    Groups,
}

impl fmt::Display for WindowFrameUnits {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}

impl From<ast::WindowFrameUnits> for WindowFrameUnits {
    fn from(value: ast::WindowFrameUnits) -> Self {
        match value {
            ast::WindowFrameUnits::Range => Self::Range,
            ast::WindowFrameUnits::Groups => Self::Groups,
            ast::WindowFrameUnits::Rows => Self::Rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_frame_creation() -> Result<()> {
        let window_frame = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Following(None),
            end_bound: None,
        };
        let result = WindowFrame::try_from(window_frame);
        assert_eq!(
            result.err().unwrap().to_string(),
            "Execution error: Invalid window frame: start bound cannot be unbounded following".to_owned()
        );

        let window_frame = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Preceding(None),
            end_bound: Some(ast::WindowFrameBound::Preceding(None)),
        };
        let result = WindowFrame::try_from(window_frame);
        assert_eq!(
            result.err().unwrap().to_string(),
            "Execution error: Invalid window frame: end bound cannot be unbounded preceding".to_owned()
        );

        let window_frame = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Preceding(Some(1)),
            end_bound: Some(ast::WindowFrameBound::Preceding(Some(2))),
        };
        let result = WindowFrame::try_from(window_frame);
        assert_eq!(
            result.err().unwrap().to_string(),
            "Execution error: Invalid window frame: start bound (1 PRECEDING) cannot be larger than end bound (2 PRECEDING)".to_owned()
        );

        let window_frame = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Preceding(Some(2)),
            end_bound: Some(ast::WindowFrameBound::Preceding(Some(1))),
        };
        let result = WindowFrame::try_from(window_frame);
        assert_eq!(
            result.err().unwrap().to_string(),
            "This feature is not implemented: With WindowFrameUnits=RANGE, the bound cannot be 2 PRECEDING or FOLLOWING at the moment".to_owned()
        );

        let window_frame = ast::WindowFrame {
            units: ast::WindowFrameUnits::Rows,
            start_bound: ast::WindowFrameBound::Preceding(Some(2)),
            end_bound: Some(ast::WindowFrameBound::Preceding(Some(1))),
        };
        let result = WindowFrame::try_from(window_frame);
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_eq() {
        assert_eq!(
            WindowFrameBound::Preceding(Some(0)),
            WindowFrameBound::CurrentRow
        );
        assert_eq!(
            WindowFrameBound::CurrentRow,
            WindowFrameBound::Following(Some(0))
        );
        assert_eq!(
            WindowFrameBound::Following(Some(2)),
            WindowFrameBound::Following(Some(2))
        );
        assert_eq!(
            WindowFrameBound::Following(None),
            WindowFrameBound::Following(None)
        );
        assert_eq!(
            WindowFrameBound::Preceding(Some(2)),
            WindowFrameBound::Preceding(Some(2))
        );
        assert_eq!(
            WindowFrameBound::Preceding(None),
            WindowFrameBound::Preceding(None)
        );
    }

    #[test]
    fn test_ord() {
        assert!(WindowFrameBound::Preceding(Some(1)) < WindowFrameBound::CurrentRow);
        // ! yes this is correct!
        assert!(
            WindowFrameBound::Preceding(Some(2)) < WindowFrameBound::Preceding(Some(1))
        );
        assert!(
            WindowFrameBound::Preceding(Some(u64::MAX))
                < WindowFrameBound::Preceding(Some(u64::MAX - 1))
        );
        assert!(
            WindowFrameBound::Preceding(None)
                < WindowFrameBound::Preceding(Some(1000000))
        );
        assert!(
            WindowFrameBound::Preceding(None)
                < WindowFrameBound::Preceding(Some(u64::MAX))
        );
        assert!(WindowFrameBound::Preceding(None) < WindowFrameBound::Following(Some(0)));
        assert!(
            WindowFrameBound::Preceding(Some(1)) < WindowFrameBound::Following(Some(1))
        );
        assert!(WindowFrameBound::CurrentRow < WindowFrameBound::Following(Some(1)));
        assert!(
            WindowFrameBound::Following(Some(1)) < WindowFrameBound::Following(Some(2))
        );
        assert!(WindowFrameBound::Following(Some(2)) < WindowFrameBound::Following(None));
        assert!(
            WindowFrameBound::Following(Some(u64::MAX))
                < WindowFrameBound::Following(None)
        );
    }
}
