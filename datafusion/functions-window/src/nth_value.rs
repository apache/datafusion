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

//! `nth_value` window function implementation

use crate::utils::{get_scalar_value_from_args, get_signed_integer};

use arrow::datatypes::FieldRef;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::window_doc_sections::DOC_SECTION_ANALYTICAL;
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::{
    Documentation, Literal, PartitionEvaluator, ReversedUDWF, Signature, TypeSignature,
    Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use field::WindowUDFFieldArgs;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::Range;
use std::sync::LazyLock;

get_or_init_udwf!(
    First,
    first_value,
    "returns the first value in the window frame",
    NthValue::first
);
get_or_init_udwf!(
    Last,
    last_value,
    "returns the last value in the window frame",
    NthValue::last
);
get_or_init_udwf!(
    NthValue,
    nth_value,
    "returns the nth value in the window frame",
    NthValue::nth
);

/// Create an expression to represent the `first_value` window function
///
pub fn first_value(arg: datafusion_expr::Expr) -> datafusion_expr::Expr {
    first_value_udwf().call(vec![arg])
}

/// Create an expression to represent the `last_value` window function
///
pub fn last_value(arg: datafusion_expr::Expr) -> datafusion_expr::Expr {
    last_value_udwf().call(vec![arg])
}

/// Create an expression to represent the `nth_value` window function
///
pub fn nth_value(arg: datafusion_expr::Expr, n: i64) -> datafusion_expr::Expr {
    nth_value_udwf().call(vec![arg, n.lit()])
}

/// Tag to differentiate special use cases of the NTH_VALUE built-in window function.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum NthValueKind {
    First,
    Last,
    Nth,
}

impl NthValueKind {
    fn name(&self) -> &'static str {
        match self {
            NthValueKind::First => "first_value",
            NthValueKind::Last => "last_value",
            NthValueKind::Nth => "nth_value",
        }
    }
}

#[derive(Debug)]
pub struct NthValue {
    signature: Signature,
    kind: NthValueKind,
}

impl NthValue {
    /// Create a new `nth_value` function
    pub fn new(kind: NthValueKind) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(0),
                    TypeSignature::Any(1),
                    TypeSignature::Any(2),
                ],
                Volatility::Immutable,
            ),
            kind,
        }
    }

    pub fn first() -> Self {
        Self::new(NthValueKind::First)
    }

    pub fn last() -> Self {
        Self::new(NthValueKind::Last)
    }
    pub fn nth() -> Self {
        Self::new(NthValueKind::Nth)
    }
}

static FIRST_VALUE_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns value evaluated at the row that is the first row of the window \
            frame.",
        "first_value(expression)",
    )
    .with_argument("expression", "Expression to operate on")
        .with_sql_example(r#"```sql
    --Example usage of the first_value window function:
    SELECT department,
           employee_id,
           salary,
           first_value(salary) OVER (PARTITION BY department ORDER BY salary DESC) AS top_salary
    FROM employees;
```

```sql
+-------------+-------------+--------+------------+
| department  | employee_id | salary | top_salary |
+-------------+-------------+--------+------------+
| Sales       | 1           | 70000  | 70000      |
| Sales       | 2           | 50000  | 70000      |
| Sales       | 3           | 30000  | 70000      |
| Engineering | 4           | 90000  | 90000      |
| Engineering | 5           | 80000  | 90000      |
+-------------+-------------+--------+------------+
```"#)
    .build()
});

fn get_first_value_doc() -> &'static Documentation {
    &FIRST_VALUE_DOCUMENTATION
}

static LAST_VALUE_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns value evaluated at the row that is the last row of the window \
            frame.",
        "last_value(expression)",
    )
    .with_argument("expression", "Expression to operate on")
        .with_sql_example(r#"```sql
-- SQL example of last_value:
SELECT department,
       employee_id,
       salary,
       last_value(salary) OVER (PARTITION BY department ORDER BY salary) AS running_last_salary
FROM employees;
```

```sql
+-------------+-------------+--------+---------------------+
| department  | employee_id | salary | running_last_salary |
+-------------+-------------+--------+---------------------+
| Sales       | 1           | 30000  | 30000               |
| Sales       | 2           | 50000  | 50000               |
| Sales       | 3           | 70000  | 70000               |
| Engineering | 4           | 40000  | 40000               |
| Engineering | 5           | 60000  | 60000               |
+-------------+-------------+--------+---------------------+
```"#)
    .build()
});

fn get_last_value_doc() -> &'static Documentation {
    &LAST_VALUE_DOCUMENTATION
}

static NTH_VALUE_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns the value evaluated at the nth row of the window frame \
         (counting from 1). Returns NULL if no such row exists.",
        "nth_value(expression, n)",
    )
    .with_argument(
        "expression",
        "The column from which to retrieve the nth value.",
    )
    .with_argument(
        "n",
        "Integer. Specifies the row number (starting from 1) in the window frame.",
    )
    .with_sql_example(
        r#"```sql
-- Sample employees table:
CREATE TABLE employees (id INT, salary INT);
INSERT INTO employees (id, salary) VALUES
(1, 30000),
(2, 40000),
(3, 50000),
(4, 60000),
(5, 70000);

-- Example usage of nth_value:
SELECT nth_value(salary, 2) OVER (
  ORDER BY salary
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS nth_value
FROM employees;
```

```text
+-----------+
| nth_value |
+-----------+
| 40000     |
| 40000     |
| 40000     |
| 40000     |
| 40000     |
+-----------+
```"#,
    )
    .build()
});

fn get_nth_value_doc() -> &'static Documentation {
    &NTH_VALUE_DOCUMENTATION
}

impl WindowUDFImpl for NthValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let state = NthValueState {
            finalized_result: None,
            kind: self.kind,
        };

        if !matches!(self.kind, NthValueKind::Nth) {
            return Ok(Box::new(NthValueEvaluator {
                state,
                ignore_nulls: partition_evaluator_args.ignore_nulls(),
                n: 0,
            }));
        }

        let n =
            match get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 1)
                .map_err(|_e| {
                    exec_datafusion_err!(
                "Expected a signed integer literal for the second argument of nth_value")
                })?
                .map(get_signed_integer)
            {
                Some(Ok(n)) => {
                    if partition_evaluator_args.is_reversed() {
                        -n
                    } else {
                        n
                    }
                }
                _ => {
                    return exec_err!(
                "Expected a signed integer literal for the second argument of nth_value"
            )
                }
            };

        Ok(Box::new(NthValueEvaluator {
            state,
            ignore_nulls: partition_evaluator_args.ignore_nulls(),
            n,
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let return_type = field_args
            .input_fields()
            .first()
            .map(|f| f.data_type())
            .cloned()
            .unwrap_or(DataType::Null);

        Ok(Field::new(field_args.name(), return_type, true).into())
    }

    fn reverse_expr(&self) -> ReversedUDWF {
        match self.kind {
            NthValueKind::First => ReversedUDWF::Reversed(last_value_udwf()),
            NthValueKind::Last => ReversedUDWF::Reversed(first_value_udwf()),
            NthValueKind::Nth => ReversedUDWF::Reversed(nth_value_udwf()),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        match self.kind {
            NthValueKind::First => Some(get_first_value_doc()),
            NthValueKind::Last => Some(get_last_value_doc()),
            NthValueKind::Nth => Some(get_nth_value_doc()),
        }
    }

    fn equals(&self, other: &dyn WindowUDFImpl) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        let Self { signature, kind } = self;
        signature == &other.signature && kind == &other.kind
    }

    fn hash_value(&self) -> u64 {
        let Self { signature, kind } = self;
        let mut hasher = DefaultHasher::new();
        std::any::type_name::<Self>().hash(&mut hasher);
        signature.hash(&mut hasher);
        kind.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone)]
pub struct NthValueState {
    // In certain cases, we can finalize the result early. Consider this usage:
    // ```
    //  FIRST_VALUE(increasing_col) OVER window AS my_first_value
    //  WINDOW (ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS window
    // ```
    // The result will always be the first entry in the table. We can store such
    // early-finalizing results and then just reuse them as necessary. This opens
    // opportunities to prune our datasets.
    pub finalized_result: Option<ScalarValue>,
    pub kind: NthValueKind,
}

#[derive(Debug)]
pub(crate) struct NthValueEvaluator {
    state: NthValueState,
    ignore_nulls: bool,
    n: i64,
}

impl PartitionEvaluator for NthValueEvaluator {
    /// When the window frame has a fixed beginning (e.g UNBOUNDED PRECEDING),
    /// for some functions such as FIRST_VALUE, LAST_VALUE and NTH_VALUE, we
    /// can memoize the result.  Once result is calculated, it will always stay
    /// same. Hence, we do not need to keep past data as we process the entire
    /// dataset.
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        let out = &state.out_col;
        let size = out.len();
        let mut buffer_size = 1;
        // Decide if we arrived at a final result yet:
        let (is_prunable, is_reverse_direction) = match self.state.kind {
            NthValueKind::First => {
                let n_range =
                    state.window_frame_range.end - state.window_frame_range.start;
                (n_range > 0 && size > 0, false)
            }
            NthValueKind::Last => (true, true),
            NthValueKind::Nth => {
                let n_range =
                    state.window_frame_range.end - state.window_frame_range.start;
                match self.n.cmp(&0) {
                    Ordering::Greater => (
                        n_range >= (self.n as usize) && size > (self.n as usize),
                        false,
                    ),
                    Ordering::Less => {
                        let reverse_index = (-self.n) as usize;
                        buffer_size = reverse_index;
                        // Negative index represents reverse direction.
                        (n_range >= reverse_index, true)
                    }
                    Ordering::Equal => (false, false),
                }
            }
        };
        // Do not memoize results when nulls are ignored.
        if is_prunable && !self.ignore_nulls {
            if self.state.finalized_result.is_none() && !is_reverse_direction {
                let result = ScalarValue::try_from_array(out, size - 1)?;
                self.state.finalized_result = Some(result);
            }
            state.window_frame_range.start =
                state.window_frame_range.end.saturating_sub(buffer_size);
        }
        Ok(())
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        if let Some(ref result) = self.state.finalized_result {
            Ok(result.clone())
        } else {
            // FIRST_VALUE, LAST_VALUE, NTH_VALUE window functions take a single column, values will have size 1.
            let arr = &values[0];
            let n_range = range.end - range.start;
            if n_range == 0 {
                // We produce None if the window is empty.
                return ScalarValue::try_from(arr.data_type());
            }

            // If null values exist and need to be ignored, extract the valid indices.
            let valid_indices = if self.ignore_nulls {
                // Calculate valid indices, inside the window frame boundaries.
                let slice = arr.slice(range.start, n_range);
                match slice.nulls() {
                    Some(nulls) => {
                        let valid_indices = nulls
                            .valid_indices()
                            .map(|idx| {
                                // Add offset `range.start` to valid indices, to point correct index in the original arr.
                                idx + range.start
                            })
                            .collect::<Vec<_>>();
                        if valid_indices.is_empty() {
                            // If all values are null, return directly.
                            return ScalarValue::try_from(arr.data_type());
                        }
                        Some(valid_indices)
                    }
                    None => None,
                }
            } else {
                None
            };
            match self.state.kind {
                NthValueKind::First => {
                    if let Some(valid_indices) = &valid_indices {
                        ScalarValue::try_from_array(arr, valid_indices[0])
                    } else {
                        ScalarValue::try_from_array(arr, range.start)
                    }
                }
                NthValueKind::Last => {
                    if let Some(valid_indices) = &valid_indices {
                        ScalarValue::try_from_array(
                            arr,
                            valid_indices[valid_indices.len() - 1],
                        )
                    } else {
                        ScalarValue::try_from_array(arr, range.end - 1)
                    }
                }
                NthValueKind::Nth => {
                    match self.n.cmp(&0) {
                        Ordering::Greater => {
                            // SQL indices are not 0-based.
                            let index = (self.n as usize) - 1;
                            if index >= n_range {
                                // Outside the range, return NULL:
                                ScalarValue::try_from(arr.data_type())
                            } else if let Some(valid_indices) = valid_indices {
                                if index >= valid_indices.len() {
                                    return ScalarValue::try_from(arr.data_type());
                                }
                                ScalarValue::try_from_array(&arr, valid_indices[index])
                            } else {
                                ScalarValue::try_from_array(arr, range.start + index)
                            }
                        }
                        Ordering::Less => {
                            let reverse_index = (-self.n) as usize;
                            if n_range < reverse_index {
                                // Outside the range, return NULL:
                                ScalarValue::try_from(arr.data_type())
                            } else if let Some(valid_indices) = valid_indices {
                                if reverse_index > valid_indices.len() {
                                    return ScalarValue::try_from(arr.data_type());
                                }
                                let new_index =
                                    valid_indices[valid_indices.len() - reverse_index];
                                ScalarValue::try_from_array(&arr, new_index)
                            } else {
                                ScalarValue::try_from_array(
                                    arr,
                                    range.start + n_range - reverse_index,
                                )
                            }
                        }
                        Ordering::Equal => ScalarValue::try_from(arr.data_type()),
                    }
                }
            }
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn uses_window_frame(&self) -> bool {
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
    use std::sync::Arc;

    fn test_i32_result(
        expr: NthValue,
        partition_evaluator_args: PartitionEvaluatorArgs,
        expected: Int32Array,
    ) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let values = vec![arr];
        let mut ranges: Vec<Range<usize>> = vec![];
        for i in 0..8 {
            ranges.push(Range {
                start: 0,
                end: i + 1,
            })
        }
        let mut evaluator = expr.partition_evaluator(partition_evaluator_args)?;
        let result = ranges
            .iter()
            .map(|range| evaluator.evaluate(&values, range))
            .collect::<Result<Vec<ScalarValue>>>()?;
        let result = ScalarValue::iter_to_array(result.into_iter())?;
        let result = as_int32_array(&result)?;
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn first_value() -> Result<()> {
        let expr = Arc::new(Column::new("c3", 0)) as Arc<dyn PhysicalExpr>;
        test_i32_result(
            NthValue::first(),
            PartitionEvaluatorArgs::new(
                &[expr],
                &[Field::new("f", DataType::Int32, true).into()],
                false,
                false,
            ),
            Int32Array::from(vec![1; 8]).iter().collect::<Int32Array>(),
        )
    }

    #[test]
    fn last_value() -> Result<()> {
        let expr = Arc::new(Column::new("c3", 0)) as Arc<dyn PhysicalExpr>;
        test_i32_result(
            NthValue::last(),
            PartitionEvaluatorArgs::new(
                &[expr],
                &[Field::new("f", DataType::Int32, true).into()],
                false,
                false,
            ),
            Int32Array::from(vec![
                Some(1),
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
                Some(8),
            ]),
        )
    }

    #[test]
    fn nth_value_1() -> Result<()> {
        let expr = Arc::new(Column::new("c3", 0)) as Arc<dyn PhysicalExpr>;
        let n_value =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;

        test_i32_result(
            NthValue::nth(),
            PartitionEvaluatorArgs::new(
                &[expr, n_value],
                &[Field::new("f", DataType::Int32, true).into()],
                false,
                false,
            ),
            Int32Array::from(vec![1; 8]),
        )?;
        Ok(())
    }

    #[test]
    fn nth_value_2() -> Result<()> {
        let expr = Arc::new(Column::new("c3", 0)) as Arc<dyn PhysicalExpr>;
        let n_value =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;

        test_i32_result(
            NthValue::nth(),
            PartitionEvaluatorArgs::new(
                &[expr, n_value],
                &[Field::new("f", DataType::Int32, true).into()],
                false,
                false,
            ),
            Int32Array::from(vec![
                None,
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
            ]),
        )?;
        Ok(())
    }
}
