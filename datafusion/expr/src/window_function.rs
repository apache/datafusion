use datafusion_common::ScalarValue;

use crate::{expr::WindowFunction, BuiltInWindowFunction, Expr, Literal};

/// Create an expression to represent the `row_number` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn row_number() -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::RowNumber, vec![])
}

/// Create an expression to represent the `rank` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn rank() -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::Rank, vec![])
}

/// Create an expression to represent the `dense_rank` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn dense_rank() -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::DenseRank, vec![])
}

/// Create an expression to represent the `percent_rank` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn percent_rank() -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::PercentRank, vec![])
}

/// Create an expression to represent the `cume_dist` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn cume_dist() -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::CumeDist, vec![])
}

/// Create an expression to represent the `ntile` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn ntile(arg: Expr) -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::Ntile, vec![arg])
}

/// Create an expression to represent the `lag` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn lag(
    arg: Expr,
    shift_offset: Option<i64>,
    default_value: Option<ScalarValue>,
) -> WindowFunction {
    let shift_offset_lit = shift_offset
        .map(|v| v.lit())
        .unwrap_or(ScalarValue::Null.lit());
    let default_lit = default_value.unwrap_or(ScalarValue::Null).lit();
    WindowFunction::new(
        BuiltInWindowFunction::Lag,
        vec![arg, shift_offset_lit, default_lit],
    )
}

/// Create an expression to represent the `lead` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn lead(
    arg: Expr,
    shift_offset: Option<i64>,
    default_value: Option<ScalarValue>,
) -> WindowFunction {
    let shift_offset_lit = shift_offset
        .map(|v| v.lit())
        .unwrap_or(ScalarValue::Null.lit());
    let default_lit = default_value.unwrap_or(ScalarValue::Null).lit();
    WindowFunction::new(
        BuiltInWindowFunction::Lead,
        vec![arg, shift_offset_lit, default_lit],
    )
}

/// Create an expression to represent the `first_value` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn first_value(arg: Expr) -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::FirstValue, vec![arg])
}

/// Create an expression to represent the `last_value` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn last_value(arg: Expr) -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::LastValue, vec![arg])
}

/// Create an expression to represent the `nth_value` window function
///
/// Note: call [`WindowFunction::build]` to create an [`Expr`]
pub fn nth_value(arg: Expr, n: i64) -> WindowFunction {
    WindowFunction::new(BuiltInWindowFunction::NthValue, vec![arg, n.lit()])
}
