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

//! Window frame module
//!
//! The frame-spec determines which output rows are read by an aggregate window function. The frame-spec consists of four parts:
//! - A frame type - either ROWS, RANGE or GROUPS,
//! - A starting frame boundary,
//! - An ending frame boundary,
//! - An EXCLUDE clause.

use std::fmt::{self, Formatter};
use std::hash::Hash;

use crate::{expr::Sort, lit};

use datafusion_common::{plan_err, sql_err, DataFusionError, Result, ScalarValue};
use sqlparser::ast;
use sqlparser::parser::ParserError::ParserError;

/// The frame specification determines which output rows are read by an aggregate
/// window function. The ending frame boundary can be omitted if the `BETWEEN`
/// and `AND` keywords that surround the starting frame boundary are also omitted,
/// in which case the ending frame boundary defaults to `CURRENT ROW`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct WindowFrame {
    /// Frame type - either `ROWS`, `RANGE` or `GROUPS`
    pub units: WindowFrameUnits,
    /// Starting frame boundary
    pub start_bound: WindowFrameBound,
    /// Ending frame boundary
    pub end_bound: WindowFrameBound,
    /// Flag indicating whether the frame is causal (i.e. computing the result
    /// for the current row doesn't depend on any subsequent rows).
    ///
    /// Example causal window frames:
    /// ```text
    ///                +--------------+
    ///      Future    |              |
    ///         |      |              |
    ///         |      |              |
    ///    Current Row |+------------+|  ---
    ///         |      |              |   |
    ///         |      |              |   |
    ///         |      |              |   |  Window Frame 1
    ///       Past     |              |   |
    ///                |              |   |
    ///                |              |  ---
    ///                +--------------+
    ///
    ///                +--------------+
    ///      Future    |              |
    ///         |      |              |
    ///         |      |              |
    ///    Current Row |+------------+|
    ///         |      |              |
    ///         |      |              | ---
    ///         |      |              |  |
    ///       Past     |              |  |  Window Frame 2
    ///                |              |  |
    ///                |              | ---
    ///                +--------------+
    /// ```
    /// Example non-causal window frame:
    /// ```text
    ///                +--------------+
    ///      Future    |              |
    ///         |      |              |
    ///         |      |              | ---
    ///    Current Row |+------------+|  |
    ///         |      |              |  |  Window Frame 3
    ///         |      |              |  |
    ///         |      |              | ---
    ///       Past     |              |
    ///                |              |
    ///                |              |
    ///                +--------------+
    /// ```
    causal: bool,
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

impl fmt::Debug for WindowFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WindowFrame {{ units: {:?}, start_bound: {:?}, end_bound: {:?}, is_causal: {:?} }}",
            self.units, self.start_bound, self.end_bound, self.causal
        )?;
        Ok(())
    }
}

impl TryFrom<ast::WindowFrame> for WindowFrame {
    type Error = DataFusionError;

    fn try_from(value: ast::WindowFrame) -> Result<Self> {
        let start_bound = value.start_bound.try_into()?;
        let end_bound = match value.end_bound {
            Some(value) => value.try_into()?,
            None => WindowFrameBound::CurrentRow,
        };

        if let WindowFrameBound::Following(val) = &start_bound {
            if val.is_null() {
                plan_err!(
                    "Invalid window frame: start bound cannot be UNBOUNDED FOLLOWING"
                )?
            }
        } else if let WindowFrameBound::Preceding(val) = &end_bound {
            if val.is_null() {
                plan_err!(
                    "Invalid window frame: end bound cannot be UNBOUNDED PRECEDING"
                )?
            }
        };
        let units = value.units.into();
        Ok(Self::new_bounds(units, start_bound, end_bound))
    }
}

impl WindowFrame {
    /// Creates a new, default window frame (with the meaning of default
    /// depending on whether the frame contains an `ORDER BY` clause and this
    /// ordering is strict (i.e. no ties).
    pub fn new(order_by: Option<bool>) -> Self {
        if let Some(strict) = order_by {
            // This window frame covers the table (or partition if `PARTITION BY`
            // is used) from beginning to the `CURRENT ROW` (with same rank). It
            // is used when the `OVER` clause contains an `ORDER BY` clause but
            // no frame.
            Self {
                units: if strict {
                    WindowFrameUnits::Rows
                } else {
                    WindowFrameUnits::Range
                },
                start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                end_bound: WindowFrameBound::CurrentRow,
                causal: strict,
            }
        } else {
            // This window frame covers the whole table (or partition if `PARTITION BY`
            // is used). It is used when the `OVER` clause does not contain an
            // `ORDER BY` clause and there is no frame.
            Self {
                units: WindowFrameUnits::Rows,
                start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                end_bound: WindowFrameBound::Following(ScalarValue::UInt64(None)),
                causal: false,
            }
        }
    }

    /// Get reversed window frame. For example
    /// `3 ROWS PRECEDING AND 2 ROWS FOLLOWING` -->
    /// `2 ROWS PRECEDING AND 3 ROWS FOLLOWING`
    pub fn reverse(&self) -> Self {
        let start_bound = match &self.end_bound {
            WindowFrameBound::Preceding(value) => {
                WindowFrameBound::Following(value.clone())
            }
            WindowFrameBound::Following(value) => {
                WindowFrameBound::Preceding(value.clone())
            }
            WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        };
        let end_bound = match &self.start_bound {
            WindowFrameBound::Preceding(value) => {
                WindowFrameBound::Following(value.clone())
            }
            WindowFrameBound::Following(value) => {
                WindowFrameBound::Preceding(value.clone())
            }
            WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        };
        Self::new_bounds(self.units, start_bound, end_bound)
    }

    /// Get whether window frame is causal
    pub fn is_causal(&self) -> bool {
        self.causal
    }

    /// Initializes window frame from units (type), start bound and end bound.
    pub fn new_bounds(
        units: WindowFrameUnits,
        start_bound: WindowFrameBound,
        end_bound: WindowFrameBound,
    ) -> Self {
        let causal = match units {
            WindowFrameUnits::Rows => match &end_bound {
                WindowFrameBound::Following(value) => {
                    if value.is_null() {
                        // Unbounded following
                        false
                    } else {
                        let zero = ScalarValue::new_zero(&value.data_type());
                        zero.map(|zero| value.eq(&zero)).unwrap_or(false)
                    }
                }
                _ => true,
            },
            WindowFrameUnits::Range | WindowFrameUnits::Groups => match &end_bound {
                WindowFrameBound::Preceding(value) => {
                    if value.is_null() {
                        // Unbounded preceding
                        true
                    } else {
                        let zero = ScalarValue::new_zero(&value.data_type());
                        zero.map(|zero| value.gt(&zero)).unwrap_or(false)
                    }
                }
                _ => false,
            },
        };
        Self {
            units,
            start_bound,
            end_bound,
            causal,
        }
    }

    /// Regularizes the ORDER BY clause of the window frame.
    pub fn regularize_order_bys(&self, order_by: &mut Vec<Sort>) -> Result<()> {
        match self.units {
            // Normally, RANGE frames require an ORDER BY clause with exactly
            // one column. However, an ORDER BY clause may be absent or have
            // more than one column when the start/end bounds are UNBOUNDED or
            // CURRENT ROW.
            WindowFrameUnits::Range if self.free_range() => {
                // If an ORDER BY clause is absent, it is equivalent to an
                // ORDER BY clause with constant value as sort key. If an
                // ORDER BY clause is present but has more than one column,
                // it is unchanged. Note that this follows PostgreSQL behavior.
                if order_by.is_empty() {
                    order_by.push(lit(1u64).sort(true, false));
                }
            }
            WindowFrameUnits::Range if order_by.len() != 1 => {
                return plan_err!("RANGE requires exactly one ORDER BY column");
            }
            WindowFrameUnits::Groups if order_by.is_empty() => {
                return plan_err!("GROUPS requires an ORDER BY clause");
            }
            _ => {}
        }
        Ok(())
    }

    /// Returns whether the window frame can accept multiple ORDER BY expressons.
    pub fn can_accept_multi_orderby(&self) -> bool {
        match self.units {
            WindowFrameUnits::Rows => true,
            WindowFrameUnits::Range => self.free_range(),
            WindowFrameUnits::Groups => true,
        }
    }

    /// Returns whether the window frame is "free range"; i.e. its start/end
    /// bounds are UNBOUNDED or CURRENT ROW.
    fn free_range(&self) -> bool {
        (self.start_bound.is_unbounded()
            || self.start_bound == WindowFrameBound::CurrentRow)
            && (self.end_bound.is_unbounded()
                || self.end_bound == WindowFrameBound::CurrentRow)
    }
}

/// There are five ways to describe starting and ending frame boundaries:
///
/// 1. UNBOUNDED PRECEDING
/// 2. `<expr>` PRECEDING
/// 3. CURRENT ROW
/// 4. `<expr>` FOLLOWING
/// 5. UNBOUNDED FOLLOWING
///
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum WindowFrameBound {
    /// 1. UNBOUNDED PRECEDING
    ///    The frame boundary is the first row in the partition.
    ///
    /// 2. `<expr>` PRECEDING
    ///    `<expr>` must be a non-negative constant numeric expression. The boundary is a row that
    ///    is `<expr>` "units" prior to the current row.
    Preceding(ScalarValue),
    /// 3. The current row.
    ///
    /// For RANGE and GROUPS frame types, peers of the current row are also
    /// included in the frame, unless specifically excluded by the EXCLUDE clause.
    /// This is true regardless of whether CURRENT ROW is used as the starting or ending frame
    /// boundary.
    CurrentRow,
    /// 4. This is the same as "`<expr>` PRECEDING" except that the boundary is `<expr>` units after the
    ///    current rather than before the current row.
    ///
    /// 5. UNBOUNDED FOLLOWING
    ///    The frame boundary is the last row in the partition.
    Following(ScalarValue),
}

impl WindowFrameBound {
    pub fn is_unbounded(&self) -> bool {
        match self {
            WindowFrameBound::Preceding(elem) => elem.is_null(),
            WindowFrameBound::CurrentRow => false,
            WindowFrameBound::Following(elem) => elem.is_null(),
        }
    }
}

impl TryFrom<ast::WindowFrameBound> for WindowFrameBound {
    type Error = DataFusionError;

    fn try_from(value: ast::WindowFrameBound) -> Result<Self> {
        Ok(match value {
            ast::WindowFrameBound::Preceding(Some(v)) => {
                Self::Preceding(convert_frame_bound_to_scalar_value(*v)?)
            }
            ast::WindowFrameBound::Preceding(None) => Self::Preceding(ScalarValue::Null),
            ast::WindowFrameBound::Following(Some(v)) => {
                Self::Following(convert_frame_bound_to_scalar_value(*v)?)
            }
            ast::WindowFrameBound::Following(None) => Self::Following(ScalarValue::Null),
            ast::WindowFrameBound::CurrentRow => Self::CurrentRow,
        })
    }
}

pub fn convert_frame_bound_to_scalar_value(v: ast::Expr) -> Result<ScalarValue> {
    Ok(ScalarValue::Utf8(Some(match v {
        ast::Expr::Value(ast::Value::Number(value, false))
        | ast::Expr::Value(ast::Value::SingleQuotedString(value)) => value,
        ast::Expr::Interval(ast::Interval {
            value,
            leading_field,
            ..
        }) => {
            let result = match *value {
                ast::Expr::Value(ast::Value::SingleQuotedString(item)) => item,
                e => {
                    return sql_err!(ParserError(format!(
                        "INTERVAL expression cannot be {e:?}"
                    )));
                }
            };
            if let Some(leading_field) = leading_field {
                format!("{result} {leading_field}")
            } else {
                result
            }
        }
        _ => plan_err!(
            "Invalid window frame: frame offsets must be non negative integers"
        )?,
    })))
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFrameBound::Preceding(n) => {
                if n.is_null() {
                    f.write_str("UNBOUNDED PRECEDING")
                } else {
                    write!(f, "{n} PRECEDING")
                }
            }
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Following(n) => {
                if n.is_null() {
                    f.write_str("UNBOUNDED FOLLOWING")
                } else {
                    write!(f, "{n} FOLLOWING")
                }
            }
        }
    }
}

/// There are three frame types: ROWS, GROUPS, and RANGE. The frame type determines how the
/// starting and ending boundaries of the frame are measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
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
        let err = WindowFrame::try_from(window_frame).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: Invalid window frame: start bound cannot be UNBOUNDED FOLLOWING".to_owned()
        );

        let window_frame = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Preceding(None),
            end_bound: Some(ast::WindowFrameBound::Preceding(None)),
        };
        let err = WindowFrame::try_from(window_frame).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: Invalid window frame: end bound cannot be UNBOUNDED PRECEDING".to_owned()
        );

        let window_frame = ast::WindowFrame {
            units: ast::WindowFrameUnits::Rows,
            start_bound: ast::WindowFrameBound::Preceding(Some(Box::new(
                ast::Expr::Value(ast::Value::Number("2".to_string(), false)),
            ))),
            end_bound: Some(ast::WindowFrameBound::Preceding(Some(Box::new(
                ast::Expr::Value(ast::Value::Number("1".to_string(), false)),
            )))),
        };
        let result = WindowFrame::try_from(window_frame);
        assert!(result.is_ok());
        Ok(())
    }
}
