// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Window frame module
//!
//! The frame-spec determines which output rows are read by an aggregate window function. The frame-spec consists of four parts:
//! - A frame type - either ROWS, RANGE or GROUPS,
//! - A starting frame boundary,
//! - An ending frame boundary,
//! - An EXCLUDE clause.

use crate::{expr::Sort, lit};
use std::fmt::{self, Formatter};
use std::hash::Hash;

use datafusion_common::{Result, ScalarValue, plan_err};
#[cfg(feature = "sql")]
use sqlparser::ast::{self, ExprKind, Literal};
#[cfg(feature = "sql")]
use sqlparser::dialect::AstExt;

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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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

#[cfg(feature = "sql")]
impl<X: AstExt> TryFrom<ast::WindowFrame<'_, X>> for WindowFrame {
    type Error = datafusion_common::error::DataFusionError;

    fn try_from(value: ast::WindowFrame<'_, X>) -> Result<Self> {
        let start_bound = WindowFrameBound::try_parse(value.start, &value.units)?;
        let end_bound = match value.end {
            Some(bound) => WindowFrameBound::try_parse(bound, &value.units)?,
            None => WindowFrameBound::CurrentRow,
        };

        if let WindowFrameBound::Following(val) = &start_bound {
            if val.is_null() {
                plan_err!(
                    "Invalid window frame: start bound cannot be UNBOUNDED FOLLOWING"
                )?
            }
        } else if let WindowFrameBound::Preceding(val) = &end_bound
            && val.is_null()
        {
            plan_err!("Invalid window frame: end bound cannot be UNBOUNDED PRECEDING")?
        }

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
                start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
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
            // For a row whose ORDER BY value is NULL or NaN, an offset bound
            // ends the RANGE frame at its last peer, which may be a later row.
            WindowFrameUnits::Range => matches!(
                &end_bound,
                WindowFrameBound::Preceding(value) if value.is_null()
            ),
            WindowFrameUnits::Groups => match &end_bound {
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
            WindowFrameUnits::Range if self.free_range() && order_by.is_empty() => {
                // If an ORDER BY clause is absent, it is equivalent to an
                // ORDER BY clause with constant value as sort key. If an
                // ORDER BY clause is present but has more than one column,
                // it is unchanged. Note that this follows PostgreSQL behavior.
                order_by.push(lit(1u64).sort(true, false));
            }
            WindowFrameUnits::Range if self.free_range() => {}
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

    /// Returns whether the window frame can accept multiple ORDER BY expressions.
    pub fn can_accept_multi_orderby(&self) -> bool {
        match self.units {
            WindowFrameUnits::Rows => true,
            WindowFrameUnits::Range => self.free_range(),
            WindowFrameUnits::Groups => true,
        }
    }

    /// Returns whether the window frame is "free range"; i.e. its start/end
    /// bounds are UNBOUNDED or CURRENT ROW.
    ///
    /// This inspects only the bounds, not the frame units, so it returns `true`
    /// for ROWS and GROUPS frames with such bounds as well. For RANGE frames,
    /// such bounds are located by comparing order key values, whereas a finite
    /// offset bound (e.g. `5 PRECEDING`) has to be computed arithmetically from
    /// the current row's order key value.
    pub fn free_range(&self) -> bool {
        (self.start_bound.is_unbounded()
            || self.start_bound == WindowFrameBound::CurrentRow)
            && (self.end_bound.is_unbounded()
                || self.end_bound == WindowFrameBound::CurrentRow)
    }

    /// Is the window frame ever-expanding (it always grows in the superset sense).
    /// Useful when understanding if set-monotonicity properties of functions can
    /// be exploited.
    pub fn is_ever_expanding(&self) -> bool {
        self.start_bound.is_unbounded()
    }
}

/// There are five ways to describe starting and ending frame boundaries:
///
/// 1. UNBOUNDED PRECEDING
/// 2. `<expr>` PRECEDING
/// 3. CURRENT ROW
/// 4. `<expr>` FOLLOWING
/// 5. UNBOUNDED FOLLOWING
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

impl WindowFrameBound {
    #[cfg(feature = "sql")]
    fn try_parse<X: AstExt>(
        value: ast::FrameBound<'_, X>,
        units: &ast::FrameUnits,
    ) -> Result<Self> {
        Ok(match value {
            ast::FrameBound::Preceding(v) => {
                Self::Preceding(convert_frame_bound_to_scalar_value(v, units)?)
            }
            ast::FrameBound::UnboundedPreceding => {
                Self::Preceding(ScalarValue::UInt64(None))
            }
            ast::FrameBound::Following(v) => {
                Self::Following(convert_frame_bound_to_scalar_value(v, units)?)
            }
            ast::FrameBound::UnboundedFollowing => {
                Self::Following(ScalarValue::UInt64(None))
            }
            ast::FrameBound::CurrentRow => Self::CurrentRow,
        })
    }
}

/// The text of a string literal, for an interval's value.
#[cfg(feature = "sql")]
fn interval_text<X: AstExt>(value: ast::Expr<'_, X>) -> Result<String> {
    use datafusion_common::exec_err;
    match value.kind {
        ExprKind::Literal(Literal::String(item)) => Ok(item.value().into_owned()),
        e => exec_err!("INTERVAL expression cannot be {e:?}"),
    }
}

#[cfg(feature = "sql")]
fn convert_frame_bound_to_scalar_value<X: AstExt>(
    v: ast::Expr<'_, X>,
    units: &ast::FrameUnits,
) -> Result<ScalarValue> {
    use arrow::datatypes::DataType;
    match units {
        // For ROWS and GROUPS we are sure that the ScalarValue must be a non-negative integer ...
        ast::FrameUnits::Rows | ast::FrameUnits::Groups => match v.kind {
            ExprKind::Literal(Literal::Number(value)) => Ok(
                ScalarValue::try_from_string(value.into_owned(), &DataType::UInt64)?,
            ),
            // A string is read as an interval with no unit, as if it were
            // written `INTERVAL 'value'`.
            ExprKind::Literal(Literal::String(value)) => {
                Ok(ScalarValue::try_from_string(
                    value.value().into_owned(),
                    &DataType::UInt64,
                )?)
            }
            ExprKind::Interval(interval)
                if interval.leading_field.is_none()
                    && interval.leading_precision.is_none()
                    && interval.last_field.is_none()
                    && interval.fractional_seconds_precision.is_none() =>
            {
                let value = interval_text(interval.value)?;
                Ok(ScalarValue::try_from_string(value, &DataType::UInt64)?)
            }
            _ => plan_err!(
                "Invalid window frame: frame offsets for ROWS / GROUPS must be non negative integers"
            ),
        },
        // ... instead for RANGE it could be anything depending on the type of the ORDER BY clause,
        // so we use a ScalarValue::Utf8.
        ast::FrameUnits::Range => Ok(ScalarValue::Utf8(Some(match v.kind {
            ExprKind::Literal(Literal::Number(value)) => value.into_owned(),
            ExprKind::Literal(Literal::String(value)) => value.value().into_owned(),
            ExprKind::Interval(interval) => {
                let leading_field = interval.leading_field.clone();
                let result = interval_text(interval.value)?;
                if let Some(leading_field) = leading_field {
                    format!("{result} {}", leading_field.to_ascii_uppercase())
                } else {
                    result
                }
            }
            _ => plan_err!(
                "Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"
            )?,
        }))),
    }
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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
    /// equivalent values for all terms of the window ORDER BY clause.
    Groups,
}

impl fmt::Display for WindowFrameUnits {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}

#[cfg(feature = "sql")]
impl From<ast::FrameUnits> for WindowFrameUnits {
    fn from(value: ast::FrameUnits) -> Self {
        match value {
            ast::FrameUnits::Range => Self::Range,
            ast::FrameUnits::Groups => Self::Groups,
            ast::FrameUnits::Rows => Self::Rows,
        }
    }
}

#[cfg(all(test, feature = "sql"))]
mod tests {
    use super::*;
    use sqlparser::dialect::{Builtin, DuckDb};

    /// The frame of `f() OVER (frame)`.
    fn frame(sql: &str) -> ast::WindowFrame<'_, Builtin> {
        let expr = sqlparser::parse_expr::<DuckDb>(sql).unwrap();
        let ExprKind::Function {
            options: Some(options),
            ..
        } = expr.kind
        else {
            panic!("not a call: {sql}");
        };
        match options.over {
            Some(ast::WindowOver::Spec(spec)) => spec.frame.unwrap(),
            other => panic!("no window frame: {other:?}"),
        }
    }

    /// The start bound of `f() OVER (units bound)`.
    fn bound(units: &str, bound: &str) -> Result<WindowFrameBound> {
        let sql = format!("f() OVER (ORDER BY a {units} {bound})");
        let frame = frame(&sql);
        WindowFrameBound::try_parse(frame.start.clone(), &frame.units)
    }

    #[test]
    fn test_window_frame_creation() -> Result<()> {
        let err = WindowFrame::try_from(frame(
            "f() OVER (ORDER BY a RANGE UNBOUNDED FOLLOWING)",
        ))
        .unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: Invalid window frame: start bound cannot be UNBOUNDED FOLLOWING".to_owned()
        );

        let err = WindowFrame::try_from(frame(
            "f() OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING)",
        ))
        .unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: Invalid window frame: end bound cannot be UNBOUNDED PRECEDING".to_owned()
        );

        let window_frame = WindowFrame::try_from(frame(
            "f() OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)",
        ))?;
        assert_eq!(window_frame.units, WindowFrameUnits::Rows);
        assert_eq!(
            window_frame.start_bound,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2)))
        );
        assert_eq!(
            window_frame.end_bound,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1)))
        );

        Ok(())
    }

    #[test]
    fn test_window_frame_bound_creation() -> Result<()> {
        for units in ["ROWS", "GROUPS", "RANGE"] {
            assert_eq!(
                bound(units, "UNBOUNDED PRECEDING")?,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None))
            );
        }
        for units in ["ROWS", "GROUPS"] {
            assert_eq!(
                bound(units, "42 PRECEDING")?,
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(42)))
            );
            assert_eq!(
                bound(units, "INTERVAL '1' DAY PRECEDING")
                    .unwrap_err()
                    .strip_backtrace(),
                "Error during planning: Invalid window frame: frame offsets for ROWS / GROUPS must be non negative integers"
            );
        }
        assert_eq!(
            bound("RANGE", "42 PRECEDING")?,
            WindowFrameBound::Preceding(ScalarValue::Utf8(Some("42".to_string())))
        );
        assert_eq!(
            bound("RANGE", "INTERVAL '1' DAY PRECEDING")?,
            WindowFrameBound::Preceding(ScalarValue::Utf8(Some("1 DAY".to_string())))
        );
        Ok(())
    }
}
