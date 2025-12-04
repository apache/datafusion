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

//! `lead` and `lag` window function implementations

use crate::utils::{get_scalar_value_from_args, get_signed_integer};
use arrow::datatypes::FieldRef;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::{arrow_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion_doc::window_doc_sections::DOC_SECTION_ANALYTICAL;
use datafusion_expr::{
    Documentation, LimitEffect, Literal, PartitionEvaluator, ReversedUDWF, Signature,
    TypeSignature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::expr::ExpressionArgs;
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_physical_expr::expressions;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::any::Any;
use std::cmp::min;
use std::collections::VecDeque;
use std::hash::Hash;
use std::ops::{Neg, Range};
use std::sync::{Arc, LazyLock};

get_or_init_udwf!(
    Lag,
    lag,
    "Returns the row value that precedes the current row by a specified \
    offset within partition. If no such row exists, then returns the \
    default value.",
    WindowShift::lag
);
get_or_init_udwf!(
    Lead,
    lead,
    "Returns the value from a row that follows the current row by a \
    specified offset within the partition. If no such row exists, then \
    returns the default value.",
    WindowShift::lead
);

/// Create an expression to represent the `lag` window function
///
/// returns value evaluated at the row that is offset rows before the current row within the partition;
/// if there is no such row, instead return default (which must be of the same type as value).
/// Both offset and default are evaluated with respect to the current row.
/// If omitted, offset defaults to 1 and default to null
pub fn lag(
    arg: datafusion_expr::Expr,
    shift_offset: Option<i64>,
    default_value: Option<ScalarValue>,
) -> datafusion_expr::Expr {
    let shift_offset_lit = shift_offset
        .map(|v| v.lit())
        .unwrap_or(ScalarValue::Null.lit());
    let default_lit = default_value.unwrap_or(ScalarValue::Null).lit();

    lag_udwf().call(vec![arg, shift_offset_lit, default_lit])
}

/// Create an expression to represent the `lead` window function
///
/// returns value evaluated at the row that is offset rows after the current row within the partition;
/// if there is no such row, instead return default (which must be of the same type as value).
/// Both offset and default are evaluated with respect to the current row.
/// If omitted, offset defaults to 1 and default to null
pub fn lead(
    arg: datafusion_expr::Expr,
    shift_offset: Option<i64>,
    default_value: Option<ScalarValue>,
) -> datafusion_expr::Expr {
    let shift_offset_lit = shift_offset
        .map(|v| v.lit())
        .unwrap_or(ScalarValue::Null.lit());
    let default_lit = default_value.unwrap_or(ScalarValue::Null).lit();

    lead_udwf().call(vec![arg, shift_offset_lit, default_lit])
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum WindowShiftKind {
    Lag,
    Lead,
}

impl WindowShiftKind {
    fn name(&self) -> &'static str {
        match self {
            WindowShiftKind::Lag => "lag",
            WindowShiftKind::Lead => "lead",
        }
    }

    /// In [`WindowShiftEvaluator`] a positive offset is used to signal
    /// computation of `lag()`. So here we negate the input offset
    /// value when computing `lead()`.
    fn shift_offset(&self, value: Option<i64>) -> i64 {
        match self {
            WindowShiftKind::Lag => value.unwrap_or(1),
            WindowShiftKind::Lead => value.map(|v| v.neg()).unwrap_or(-1),
        }
    }
}

/// window shift expression
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct WindowShift {
    signature: Signature,
    kind: WindowShiftKind,
}

impl WindowShift {
    fn new(kind: WindowShiftKind) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(1),
                    TypeSignature::Any(2),
                    TypeSignature::Any(3),
                ],
                Volatility::Immutable,
            )
            .with_parameter_names(vec![
                "expr".to_string(),
                "offset".to_string(),
                "default".to_string(),
            ])
            .expect("valid parameter names for lead/lag"),
            kind,
        }
    }

    pub fn lag() -> Self {
        Self::new(WindowShiftKind::Lag)
    }

    pub fn lead() -> Self {
        Self::new(WindowShiftKind::Lead)
    }

    pub fn kind(&self) -> &WindowShiftKind {
        &self.kind
    }
}

static LAG_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(DOC_SECTION_ANALYTICAL, "Returns value evaluated at the row that is offset rows before the \
            current row within the partition; if there is no such row, instead return default \
            (which must be of the same type as value).", "lag(expression, offset, default)")
        .with_argument("expression", "Expression to operate on")
        .with_argument("offset", "Integer. Specifies how many rows back \
        the value of expression should be retrieved. Defaults to 1.")
        .with_argument("default", "The default value if the offset is \
        not within the partition. Must be of the same type as expression.")
        .with_sql_example(r#"
```sql
-- Example usage of the lag window function:
SELECT employee_id,
    salary,
    lag(salary, 1, 0) OVER (ORDER BY employee_id) AS prev_salary
FROM employees;

+-------------+--------+-------------+
| employee_id | salary | prev_salary |
+-------------+--------+-------------+
| 1           | 30000  | 0           |
| 2           | 50000  | 30000       |
| 3           | 70000  | 50000       |
| 4           | 60000  | 70000       |
+-------------+--------+-------------+
```
"#)
        .build()
});

fn get_lag_doc() -> &'static Documentation {
    &LAG_DOCUMENTATION
}

static LEAD_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(DOC_SECTION_ANALYTICAL,
            "Returns value evaluated at the row that is offset rows after the \
            current row within the partition; if there is no such row, instead return default \
            (which must be of the same type as value).",
        "lead(expression, offset, default)")
        .with_argument("expression", "Expression to operate on")
        .with_argument("offset", "Integer. Specifies how many rows \
        forward the value of expression should be retrieved. Defaults to 1.")
        .with_argument("default", "The default value if the offset is \
        not within the partition. Must be of the same type as expression.")
        .with_sql_example(r#"
```sql
-- Example usage of lead window function:
SELECT
    employee_id,
    department,
    salary,
    lead(salary, 1, 0) OVER (PARTITION BY department ORDER BY salary) AS next_salary
FROM employees;

+-------------+-------------+--------+--------------+
| employee_id | department  | salary | next_salary  |
+-------------+-------------+--------+--------------+
| 1           | Sales       | 30000  | 50000        |
| 2           | Sales       | 50000  | 70000        |
| 3           | Sales       | 70000  | 0            |
| 4           | Engineering | 40000  | 60000        |
| 5           | Engineering | 60000  | 0            |
+-------------+-------------+--------+--------------+
```
"#)
        .build()
});

fn get_lead_doc() -> &'static Documentation {
    &LEAD_DOCUMENTATION
}

impl WindowUDFImpl for WindowShift {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Handles the case where `NULL` expression is passed as an
    /// argument to `lead`/`lag`. The type is refined depending
    /// on the default value argument.
    ///
    /// For more details see: <https://github.com/apache/datafusion/issues/12717>
    fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        parse_expr(expr_args.input_exprs(), expr_args.input_fields())
            .into_iter()
            .collect::<Vec<_>>()
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let shift_offset =
            get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 1)?
                .map(|v| get_signed_integer(&v))
                .map_or(Ok(None), |v| v.map(Some))
                .map(|n| self.kind.shift_offset(n))
                .map(|offset| {
                    if partition_evaluator_args.is_reversed() {
                        -offset
                    } else {
                        offset
                    }
                })?;
        let default_value = parse_default_value(
            partition_evaluator_args.input_exprs(),
            partition_evaluator_args.input_fields(),
        )?;

        Ok(Box::new(WindowShiftEvaluator {
            shift_offset,
            default_value,
            ignore_nulls: partition_evaluator_args.ignore_nulls(),
            non_null_offsets: VecDeque::new(),
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let return_field = parse_expr_field(field_args.input_fields())?;

        Ok(return_field
            .as_ref()
            .clone()
            .with_name(field_args.name())
            .into())
    }

    fn reverse_expr(&self) -> ReversedUDWF {
        match self.kind {
            WindowShiftKind::Lag => ReversedUDWF::Reversed(lag_udwf()),
            WindowShiftKind::Lead => ReversedUDWF::Reversed(lead_udwf()),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        match self.kind {
            WindowShiftKind::Lag => Some(get_lag_doc()),
            WindowShiftKind::Lead => Some(get_lead_doc()),
        }
    }

    fn limit_effect(&self, args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        if self.kind == WindowShiftKind::Lag {
            return LimitEffect::None;
        }
        match args {
            [_, expr, ..] => {
                let Some(lit) = expr.as_any().downcast_ref::<expressions::Literal>()
                else {
                    return LimitEffect::Unknown;
                };
                let ScalarValue::Int64(Some(amount)) = lit.value() else {
                    return LimitEffect::Unknown; // we should only get int64 from the parser
                };
                LimitEffect::Relative((*amount).max(0) as usize)
            }
            [_] => LimitEffect::Relative(1), // default value
            _ => LimitEffect::Unknown,       // invalid arguments
        }
    }
}

/// When `lead`/`lag` is evaluated on a `NULL` expression we attempt to
/// refine it by matching it with the type of the default value.
///
/// For e.g. in `lead(NULL, 1, false)` the generic `ScalarValue::Null`
/// is refined into `ScalarValue::Boolean(None)`. Only the type is
/// refined, the expression value remains `NULL`.
///
/// When the window function is evaluated with `NULL` expression
/// this guarantees that the type matches with that of the default
/// value.
///
/// For more details see: <https://github.com/apache/datafusion/issues/12717>
fn parse_expr(
    input_exprs: &[Arc<dyn PhysicalExpr>],
    input_fields: &[FieldRef],
) -> Result<Arc<dyn PhysicalExpr>> {
    assert!(!input_exprs.is_empty());
    assert!(!input_fields.is_empty());

    let expr = Arc::clone(input_exprs.first().unwrap());
    let expr_field = input_fields.first().unwrap();

    // Handles the most common case where NULL is unexpected
    if !expr_field.data_type().is_null() {
        return Ok(expr);
    }

    let default_value = get_scalar_value_from_args(input_exprs, 2)?;
    default_value.map_or(Ok(expr), |value| {
        ScalarValue::try_from(&value.data_type())
            .map(|v| Arc::new(expressions::Literal::new(v)) as Arc<dyn PhysicalExpr>)
    })
}

static NULL_FIELD: LazyLock<FieldRef> =
    LazyLock::new(|| Field::new("value", DataType::Null, true).into());

/// Returns the field of the default value(if provided) when the
/// expression is `NULL`.
///
/// Otherwise, returns the expression field unchanged.
fn parse_expr_field(input_fields: &[FieldRef]) -> Result<FieldRef> {
    assert!(!input_fields.is_empty());
    let expr_field = input_fields.first().unwrap_or(&NULL_FIELD);

    // Handles the most common case where NULL is unexpected
    if !expr_field.data_type().is_null() {
        return Ok(expr_field.as_ref().clone().with_nullable(true).into());
    }

    let default_value_field = input_fields.get(2).unwrap_or(&NULL_FIELD);
    Ok(default_value_field
        .as_ref()
        .clone()
        .with_nullable(true)
        .into())
}

/// Handles type coercion and null value refinement for default value
/// argument depending on the data type of the input expression.
fn parse_default_value(
    input_exprs: &[Arc<dyn PhysicalExpr>],
    input_types: &[FieldRef],
) -> Result<ScalarValue> {
    let expr_field = parse_expr_field(input_types)?;
    let unparsed = get_scalar_value_from_args(input_exprs, 2)?;

    unparsed
        .filter(|v| !v.data_type().is_null())
        .map(|v| v.cast_to(expr_field.data_type()))
        .unwrap_or_else(|| ScalarValue::try_from(expr_field.data_type()))
}

#[derive(Debug)]
struct WindowShiftEvaluator {
    shift_offset: i64,
    default_value: ScalarValue,
    ignore_nulls: bool,
    // VecDeque contains offset values that between non-null entries
    non_null_offsets: VecDeque<usize>,
}

impl WindowShiftEvaluator {
    fn is_lag(&self) -> bool {
        // Mode is LAG, when shift_offset is positive
        self.shift_offset > 0
    }
}

// implement ignore null for evaluate_all
fn evaluate_all_with_ignore_null(
    array: &ArrayRef,
    offset: i64,
    default_value: &ScalarValue,
    is_lag: bool,
) -> Result<ArrayRef, DataFusionError> {
    let valid_indices: Vec<usize> =
        array.nulls().unwrap().valid_indices().collect::<Vec<_>>();
    let direction = !is_lag;
    let new_array_results: Result<Vec<_>, DataFusionError> = (0..array.len())
        .map(|id| {
            let result_index = match valid_indices.binary_search(&id) {
                Ok(pos) => if direction {
                    pos.checked_add(offset as usize)
                } else {
                    pos.checked_sub(offset.unsigned_abs() as usize)
                }
                .and_then(|new_pos| {
                    if new_pos < valid_indices.len() {
                        Some(valid_indices[new_pos])
                    } else {
                        None
                    }
                }),
                Err(pos) => if direction {
                    pos.checked_add(offset as usize)
                } else if pos > 0 {
                    pos.checked_sub(offset.unsigned_abs() as usize)
                } else {
                    None
                }
                .and_then(|new_pos| {
                    if new_pos < valid_indices.len() {
                        Some(valid_indices[new_pos])
                    } else {
                        None
                    }
                }),
            };

            match result_index {
                Some(index) => ScalarValue::try_from_array(array, index),
                None => Ok(default_value.clone()),
            }
        })
        .collect();

    let new_array = new_array_results?;
    ScalarValue::iter_to_array(new_array)
}
// TODO: change the original arrow::compute::kernels::window::shift impl to support an optional default value
fn shift_with_default_value(
    array: &ArrayRef,
    offset: i64,
    default_value: &ScalarValue,
) -> Result<ArrayRef> {
    use datafusion_common::arrow::compute::concat;

    let value_len = array.len() as i64;
    if offset == 0 {
        Ok(Arc::clone(array))
    } else if offset == i64::MIN || offset.abs() >= value_len {
        default_value.to_array_of_size(value_len as usize)
    } else {
        let slice_offset = (-offset).clamp(0, value_len) as usize;
        let length = array.len() - offset.unsigned_abs() as usize;
        let slice = array.slice(slice_offset, length);

        // Generate array with remaining `null` items
        let nulls = offset.unsigned_abs() as usize;
        let default_values = default_value.to_array_of_size(nulls)?;

        // Concatenate both arrays, add nulls after if shift > 0 else before
        if offset > 0 {
            concat(&[default_values.as_ref(), slice.as_ref()])
                .map_err(|e| arrow_datafusion_err!(e))
        } else {
            concat(&[slice.as_ref(), default_values.as_ref()])
                .map_err(|e| arrow_datafusion_err!(e))
        }
    }
}

impl PartitionEvaluator for WindowShiftEvaluator {
    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        if self.is_lag() {
            let start = if self.non_null_offsets.len() == self.shift_offset as usize {
                // How many rows needed previous than the current row to get necessary lag result
                let offset: usize = self.non_null_offsets.iter().sum();
                idx.saturating_sub(offset)
            } else if !self.ignore_nulls {
                let offset = self.shift_offset as usize;
                idx.saturating_sub(offset)
            } else {
                0
            };
            let end = idx + 1;
            Ok(Range { start, end })
        } else {
            let end = if self.non_null_offsets.len() == (-self.shift_offset) as usize {
                // How many rows needed further than the current row to get necessary lead result
                let offset: usize = self.non_null_offsets.iter().sum();
                min(idx + offset + 1, n_rows)
            } else if !self.ignore_nulls {
                let offset = (-self.shift_offset) as usize;
                min(idx + offset, n_rows)
            } else {
                n_rows
            };
            Ok(Range { start: idx, end })
        }
    }

    fn is_causal(&self) -> bool {
        // Lagging windows are causal by definition:
        self.is_lag()
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        let array = &values[0];
        let len = array.len();

        // LAG mode
        let i = if self.is_lag() {
            (range.end as i64 - self.shift_offset - 1) as usize
        } else {
            // LEAD mode
            (range.start as i64 - self.shift_offset) as usize
        };

        let mut idx: Option<usize> = if i < len { Some(i) } else { None };

        // LAG with IGNORE NULLS calculated as the current row index - offset, but only for non-NULL rows
        // If current row index points to NULL value the row is NOT counted
        if self.ignore_nulls && self.is_lag() {
            // LAG when NULLS are ignored.
            // Find the nonNULL row index that shifted by offset comparing to current row index
            idx = if self.non_null_offsets.len() == self.shift_offset as usize {
                let total_offset: usize = self.non_null_offsets.iter().sum();
                Some(range.end - 1 - total_offset)
            } else {
                None
            };

            // Keep track of offset values between non-null entries
            if array.is_valid(range.end - 1) {
                // Non-null add new offset
                self.non_null_offsets.push_back(1);
                if self.non_null_offsets.len() > self.shift_offset as usize {
                    // WE do not need to keep track of more than `lag number of offset` values.
                    self.non_null_offsets.pop_front();
                }
            } else if !self.non_null_offsets.is_empty() {
                // Entry is null, increment offset value of the last entry.
                let end_idx = self.non_null_offsets.len() - 1;
                self.non_null_offsets[end_idx] += 1;
            }
        } else if self.ignore_nulls && !self.is_lag() {
            // LEAD when NULLS are ignored.
            // Stores the necessary non-null entry number further than the current row.
            let non_null_row_count = (-self.shift_offset) as usize;

            if self.non_null_offsets.is_empty() {
                // When empty, fill non_null offsets with the data further than the current row.
                let mut offset_val = 1;
                for idx in range.start + 1..range.end {
                    if array.is_valid(idx) {
                        self.non_null_offsets.push_back(offset_val);
                        offset_val = 1;
                    } else {
                        offset_val += 1;
                    }
                    // It is enough to keep track of `non_null_row_count + 1` non-null offset.
                    // further data is unnecessary for the result.
                    if self.non_null_offsets.len() == non_null_row_count + 1 {
                        break;
                    }
                }
            } else if range.end < len && array.is_valid(range.end) {
                // Update `non_null_offsets` with the new end data.
                if array.is_valid(range.end) {
                    // When non-null, append a new offset.
                    self.non_null_offsets.push_back(1);
                } else {
                    // When null, increment offset count of the last entry
                    let last_idx = self.non_null_offsets.len() - 1;
                    self.non_null_offsets[last_idx] += 1;
                }
            }

            // Find the nonNULL row index that shifted by offset comparing to current row index
            idx = if self.non_null_offsets.len() >= non_null_row_count {
                let total_offset: usize =
                    self.non_null_offsets.iter().take(non_null_row_count).sum();
                Some(range.start + total_offset)
            } else {
                None
            };
            // Prune `self.non_null_offsets` from the start. so that at next iteration
            // start of the `self.non_null_offsets` matches with current row.
            if !self.non_null_offsets.is_empty() {
                self.non_null_offsets[0] -= 1;
                if self.non_null_offsets[0] == 0 {
                    // When offset is 0. Remove it.
                    self.non_null_offsets.pop_front();
                }
            }
        }

        // Set the default value if
        // - index is out of window bounds
        // OR
        // - ignore nulls mode and current value is null and is within window bounds
        // .unwrap() is safe here as there is a none check in front
        #[expect(clippy::unnecessary_unwrap)]
        if !(idx.is_none() || (self.ignore_nulls && array.is_null(idx.unwrap()))) {
            ScalarValue::try_from_array(array, idx.unwrap())
        } else {
            Ok(self.default_value.clone())
        }
    }

    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        _num_rows: usize,
    ) -> Result<ArrayRef> {
        // LEAD, LAG window functions take single column, values will have size 1
        let value = &values[0];
        if !self.ignore_nulls {
            shift_with_default_value(value, self.shift_offset, &self.default_value)
        } else {
            evaluate_all_with_ignore_null(
                value,
                self.shift_offset,
                &self.default_value,
                self.is_lag(),
            )
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use datafusion_common::cast::as_int32_array;
    use datafusion_physical_expr::expressions::{Column, Literal};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    fn test_i32_result(
        expr: WindowShift,
        partition_evaluator_args: PartitionEvaluatorArgs,
        expected: Int32Array,
    ) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let values = vec![arr];
        let num_rows = values.len();
        let result = expr
            .partition_evaluator(partition_evaluator_args)?
            .evaluate_all(&values, num_rows)?;
        let result = as_int32_array(&result)?;
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn lead_lag_get_range() -> Result<()> {
        // LAG(2)
        let lag_fn = WindowShiftEvaluator {
            shift_offset: 2,
            default_value: ScalarValue::Null,
            ignore_nulls: false,
            non_null_offsets: Default::default(),
        };
        assert_eq!(lag_fn.get_range(6, 10)?, Range { start: 4, end: 7 });
        assert_eq!(lag_fn.get_range(0, 10)?, Range { start: 0, end: 1 });

        // LAG(2 ignore nulls)
        let lag_fn = WindowShiftEvaluator {
            shift_offset: 2,
            default_value: ScalarValue::Null,
            ignore_nulls: true,
            // models data received [<Some>, <Some>, <Some>, NULL, <Some>, NULL, <current row>, ...]
            non_null_offsets: vec![2, 2].into(), // [1, 1, 2, 2] actually, just last 2 is used
        };
        assert_eq!(lag_fn.get_range(6, 10)?, Range { start: 2, end: 7 });

        // LEAD(2)
        let lead_fn = WindowShiftEvaluator {
            shift_offset: -2,
            default_value: ScalarValue::Null,
            ignore_nulls: false,
            non_null_offsets: Default::default(),
        };
        assert_eq!(lead_fn.get_range(6, 10)?, Range { start: 6, end: 8 });
        assert_eq!(lead_fn.get_range(9, 10)?, Range { start: 9, end: 10 });

        // LEAD(2 ignore nulls)
        let lead_fn = WindowShiftEvaluator {
            shift_offset: -2,
            default_value: ScalarValue::Null,
            ignore_nulls: true,
            // models data received [..., <current row>, NULL, <Some>, NULL, <Some>, ..]
            non_null_offsets: vec![2, 2].into(),
        };
        assert_eq!(lead_fn.get_range(4, 10)?, Range { start: 4, end: 9 });

        Ok(())
    }

    #[test]
    fn test_lead_window_shift() -> Result<()> {
        let expr = Arc::new(Column::new("c3", 0)) as Arc<dyn PhysicalExpr>;

        test_i32_result(
            WindowShift::lead(),
            PartitionEvaluatorArgs::new(
                &[expr],
                &[Field::new("f", DataType::Int32, true).into()],
                false,
                false,
            ),
            [
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
                Some(8),
                None,
            ]
            .iter()
            .collect::<Int32Array>(),
        )
    }

    #[test]
    fn test_lag_window_shift() -> Result<()> {
        let expr = Arc::new(Column::new("c3", 0)) as Arc<dyn PhysicalExpr>;

        test_i32_result(
            WindowShift::lag(),
            PartitionEvaluatorArgs::new(
                &[expr],
                &[Field::new("f", DataType::Int32, true).into()],
                false,
                false,
            ),
            [
                None,
                Some(1),
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
            ]
            .iter()
            .collect::<Int32Array>(),
        )
    }

    #[test]
    fn test_lag_with_default() -> Result<()> {
        let expr = Arc::new(Column::new("c3", 0)) as Arc<dyn PhysicalExpr>;
        let shift_offset =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let default_value = Arc::new(Literal::new(ScalarValue::Int32(Some(100))))
            as Arc<dyn PhysicalExpr>;

        let input_exprs = &[expr, shift_offset, default_value];
        let input_fields = [DataType::Int32, DataType::Int32, DataType::Int32]
            .into_iter()
            .map(|d| Field::new("f", d, true))
            .map(Arc::new)
            .collect::<Vec<_>>();

        test_i32_result(
            WindowShift::lag(),
            PartitionEvaluatorArgs::new(input_exprs, &input_fields, false, false),
            [
                Some(100),
                Some(1),
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
            ]
            .iter()
            .collect::<Int32Array>(),
        )
    }
}
