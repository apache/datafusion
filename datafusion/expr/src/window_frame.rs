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

use datafusion_common::{DataFusionError, Result, ScalarValue};
use sqlparser::ast;
use sqlparser::parser::ParserError::ParserError;
use std::convert::{From, TryFrom};
use std::fmt;
use std::hash::Hash;

/// The frame-spec determines which output rows are read by an aggregate window function.
///
/// The ending frame boundary can be omitted (if the BETWEEN and AND keywords that surround the
/// starting frame boundary are also omitted), in which case the ending frame boundary defaults to
/// CURRENT ROW.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let start_bound = value.start_bound.try_into()?;
        let end_bound = match value.end_bound {
            Some(value) => value.try_into()?,
            None => WindowFrameBound::CurrentRow,
        };

        if let WindowFrameBound::Following(ScalarValue::Utf8(None)) = start_bound {
            Err(DataFusionError::Execution(
                "Invalid window frame: start bound cannot be unbounded following"
                    .to_owned(),
            ))
        } else if let WindowFrameBound::Preceding(ScalarValue::Utf8(None)) = end_bound {
            Err(DataFusionError::Execution(
                "Invalid window frame: end bound cannot be unbounded preceding"
                    .to_owned(),
            ))
        } else {
            let units = value.units.into();
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
            start_bound: WindowFrameBound::Preceding(ScalarValue::Utf8(None)),
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameBound {
    /// 1. UNBOUNDED PRECEDING
    /// The frame boundary is the first row in the partition.
    ///
    /// 2. <expr> PRECEDING
    /// <expr> must be a non-negative constant numeric expression. The boundary is a row that
    /// is <expr> "units" prior to the current row.
    Preceding(ScalarValue),
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
    Following(ScalarValue),
}

impl TryFrom<ast::WindowFrameBound> for WindowFrameBound {
    type Error = DataFusionError;

    fn try_from(value: ast::WindowFrameBound) -> Result<Self> {
        Ok(match value {
            ast::WindowFrameBound::Preceding(Some(v)) => {
                Self::Preceding(convert_frame_bound_to_scalar_value(*v)?)
            }
            ast::WindowFrameBound::Preceding(None) => {
                Self::Preceding(ScalarValue::Utf8(None))
            }
            ast::WindowFrameBound::Following(Some(v)) => {
                Self::Following(convert_frame_bound_to_scalar_value(*v)?)
            }
            ast::WindowFrameBound::Following(None) => {
                Self::Following(ScalarValue::Utf8(None))
            }
            ast::WindowFrameBound::CurrentRow => Self::CurrentRow,
        })
    }
}

pub fn convert_frame_bound_to_scalar_value(v: ast::Expr) -> Result<ScalarValue> {
    Ok(ScalarValue::Utf8(Some(match v {
        ast::Expr::Value(ast::Value::Number(value, false))
        | ast::Expr::Value(ast::Value::SingleQuotedString(value)) => value,
        ast::Expr::Interval {
            value,
            leading_field,
            ..
        } => {
            let result = match *value {
                ast::Expr::Value(ast::Value::SingleQuotedString(item)) => item,
                e => {
                    let msg = format!("INTERVAL expression cannot be {:?}", e);
                    return Err(DataFusionError::SQL(ParserError(msg)));
                }
            };
            if let Some(leading_field) = leading_field {
                format!("{} {}", result, leading_field)
            } else {
                result
            }
        }
        e => {
            let msg = format!("Window frame bound cannot be {:?}", e);
            return Err(DataFusionError::Internal(msg));
        }
    })))
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFrameBound::Preceding(ScalarValue::Utf8(None)) => {
                f.write_str("UNBOUNDED PRECEDING")
            }
            WindowFrameBound::Preceding(n) => write!(f, "{} PRECEDING", n),
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Following(ScalarValue::Utf8(None)) => {
                f.write_str("UNBOUNDED FOLLOWING")
            }
            WindowFrameBound::Following(n) => write!(f, "{} FOLLOWING", n),
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
            err.to_string(),
            "Execution error: Invalid window frame: start bound cannot be unbounded following".to_owned()
        );

        let window_frame = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Preceding(None),
            end_bound: Some(ast::WindowFrameBound::Preceding(None)),
        };
        let err = WindowFrame::try_from(window_frame).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Execution error: Invalid window frame: end bound cannot be unbounded preceding".to_owned()
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
