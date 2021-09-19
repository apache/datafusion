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

use std::{any::Any, convert::TryInto, sync::Arc};

use arrow::array::*;
use arrow::compute;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{DataFusionError, Result};
use crate::logical_plan::Operator;
use crate::physical_plan::expressions::try_cast;
use crate::physical_plan::{ColumnarValue, PhysicalExpr};
use crate::scalar::ScalarValue;

use super::coercion::{
    eq_coercion, like_coercion, numerical_coercion, order_coercion, string_coercion,
};
use arrow::scalar::Scalar;

/// Binary expression
#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl BinaryExpr {
    /// Create new binary expression
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self { left, op, right }
    }

    /// Get the left side of the binary expression
    pub fn left(&self) -> &Arc<dyn PhysicalExpr> {
        &self.left
    }

    /// Get the right side of the binary expression
    pub fn right(&self) -> &Arc<dyn PhysicalExpr> {
        &self.right
    }

    /// Get the operator for this binary expression
    pub fn op(&self) -> &Operator {
        &self.op
    }
}

impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

/// Invoke a boolean kernel on a pair of arrays
macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:expr) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

fn to_arrow_comparison(op: &Operator) -> compute::comparison::Operator {
    match op {
        Operator::Eq => compute::comparison::Operator::Eq,
        Operator::NotEq => compute::comparison::Operator::Neq,
        Operator::Lt => compute::comparison::Operator::Lt,
        Operator::LtEq => compute::comparison::Operator::LtEq,
        Operator::Gt => compute::comparison::Operator::Gt,
        Operator::GtEq => compute::comparison::Operator::GtEq,
        _ => unreachable!(),
    }
}

fn to_arrow_arithmetics(op: &Operator) -> compute::arithmetics::Operator {
    match op {
        Operator::Plus => compute::arithmetics::Operator::Add,
        Operator::Minus => compute::arithmetics::Operator::Subtract,
        Operator::Multiply => compute::arithmetics::Operator::Multiply,
        Operator::Divide => compute::arithmetics::Operator::Divide,
        Operator::Modulo => compute::arithmetics::Operator::Remainder,
        _ => unreachable!(),
    }
}

#[inline]
fn evaluate_regex<O: Offset>(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
    Ok(compute::regex_match::regex_match::<O>(
        lhs.as_any().downcast_ref().unwrap(),
        rhs.as_any().downcast_ref().unwrap(),
    )?)
}

fn evaluate(lhs: &dyn Array, op: &Operator, rhs: &dyn Array) -> Result<Arc<dyn Array>> {
    use Operator::*;
    if matches!(op, Plus | Minus | Divide | Multiply | Modulo) {
        let op = to_arrow_arithmetics(op);
        Ok(compute::arithmetics::arithmetic(lhs, op, rhs).map(|x| x.into())?)
    } else if matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq) {
        let op = to_arrow_comparison(op);
        Ok(compute::comparison::compare(lhs, rhs, op).map(Arc::new)?)
    } else if matches!(op, Or) {
        boolean_op!(lhs, rhs, compute::boolean_kleene::or)
    } else if matches!(op, And) {
        boolean_op!(lhs, rhs, compute::boolean_kleene::and)
    } else {
        match (lhs.data_type(), op, rhs.data_type()) {
            (DataType::Utf8, Like, DataType::Utf8) => {
                Ok(compute::like::like_utf8::<i32>(
                    lhs.as_any().downcast_ref().unwrap(),
                    rhs.as_any().downcast_ref().unwrap(),
                )
                .map(Arc::new)?)
            }
            (DataType::LargeUtf8, Like, DataType::LargeUtf8) => {
                Ok(compute::like::like_utf8::<i64>(
                    lhs.as_any().downcast_ref().unwrap(),
                    rhs.as_any().downcast_ref().unwrap(),
                )
                .map(Arc::new)?)
            }
            (DataType::Utf8, NotLike, DataType::Utf8) => {
                Ok(compute::like::nlike_utf8::<i32>(
                    lhs.as_any().downcast_ref().unwrap(),
                    rhs.as_any().downcast_ref().unwrap(),
                )
                .map(Arc::new)?)
            }
            (DataType::LargeUtf8, NotLike, DataType::LargeUtf8) => {
                Ok(compute::like::nlike_utf8::<i64>(
                    lhs.as_any().downcast_ref().unwrap(),
                    rhs.as_any().downcast_ref().unwrap(),
                )
                .map(Arc::new)?)
            }
            (DataType::Utf8, RegexMatch, DataType::Utf8) => {
                Ok(Arc::new(evaluate_regex::<i32>(lhs, rhs)?))
            }
            (DataType::Utf8, RegexIMatch, DataType::Utf8) => {
                todo!();
            }
            (DataType::Utf8, RegexNotMatch, DataType::Utf8) => {
                let re = evaluate_regex::<i32>(lhs, rhs)?;
                Ok(Arc::new(compute::boolean::not(&re)))
            }
            (DataType::Utf8, RegexNotIMatch, DataType::Utf8) => {
                todo!();
            }
            (DataType::LargeUtf8, RegexMatch, DataType::LargeUtf8) => {
                Ok(Arc::new(evaluate_regex::<i64>(lhs, rhs)?))
            }
            (DataType::LargeUtf8, RegexIMatch, DataType::LargeUtf8) => {
                todo!();
            }
            (DataType::LargeUtf8, RegexNotMatch, DataType::LargeUtf8) => {
                let re = evaluate_regex::<i64>(lhs, rhs)?;
                Ok(Arc::new(compute::boolean::not(&re)))
            }
            (DataType::LargeUtf8, RegexNotIMatch, DataType::LargeUtf8) => {
                todo!();
            }
            (lhs, op, rhs) => Err(DataFusionError::Internal(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                op, lhs, rhs
            ))),
        }
    }
}

macro_rules! dyn_compute_scalar {
    ($lhs:expr, $op:expr, $rhs:expr, $ty:ty) => {{
        Arc::new(compute::arithmetics::arithmetic_primitive_scalar::<$ty>(
            $lhs.as_any().downcast_ref().unwrap(),
            $op,
            &$rhs.clone().try_into().unwrap(),
        )?)
    }};
}

#[inline]
fn evaluate_regex_scalar<O: Offset>(
    values: &dyn Array,
    regex: &ScalarValue,
) -> Result<BooleanArray> {
    let values = values.as_any().downcast_ref().unwrap();
    let regex = match regex {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.as_str(),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Regex pattern is not a valid string, got: {:?}",
                regex,
            )));
        }
    };
    Ok(compute::regex_match::regex_match_scalar::<O>(
        values, regex,
    )?)
}

fn evaluate_scalar(
    lhs: &dyn Array,
    op: &Operator,
    rhs: &ScalarValue,
) -> Result<Option<Arc<dyn Array>>> {
    use Operator::*;
    if matches!(op, Plus | Minus | Divide | Multiply | Modulo) {
        let op = to_arrow_arithmetics(op);
        Ok(match lhs.data_type() {
            DataType::Int8 => Some(dyn_compute_scalar!(lhs, op, rhs, i8)),
            DataType::Int16 => Some(dyn_compute_scalar!(lhs, op, rhs, i16)),
            DataType::Int32 => Some(dyn_compute_scalar!(lhs, op, rhs, i32)),
            DataType::Int64 => Some(dyn_compute_scalar!(lhs, op, rhs, i64)),
            DataType::UInt8 => Some(dyn_compute_scalar!(lhs, op, rhs, u8)),
            DataType::UInt16 => Some(dyn_compute_scalar!(lhs, op, rhs, u16)),
            DataType::UInt32 => Some(dyn_compute_scalar!(lhs, op, rhs, u32)),
            DataType::UInt64 => Some(dyn_compute_scalar!(lhs, op, rhs, u64)),
            DataType::Float32 => Some(dyn_compute_scalar!(lhs, op, rhs, f32)),
            DataType::Float64 => Some(dyn_compute_scalar!(lhs, op, rhs, f64)),
            _ => None, // fall back to default comparison below
        })
    } else if matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq) {
        let op = to_arrow_comparison(op);
        let rhs: Result<Box<dyn Scalar>> = rhs.try_into();
        match rhs {
            Ok(rhs) => {
                let arr = compute::comparison::compare_scalar(lhs, &*rhs, op)?;
                Ok(Some(Arc::new(arr) as Arc<dyn Array>))
            }
            Err(_) => {
                // fall back to default comparison below
                Ok(None)
            }
        }
    } else if matches!(op, Or) {
        // TODO: optimize scalar Or
        Ok(None)
    } else if matches!(op, And) {
        // TODO: optimize scalar And
        Ok(None)
    } else {
        match (lhs.data_type(), op) {
            (DataType::Utf8, RegexMatch) => {
                Ok(Some(Arc::new(evaluate_regex_scalar::<i32>(lhs, rhs)?)))
            }
            (DataType::Utf8, RegexIMatch) => {
                todo!();
            }
            (DataType::Utf8, RegexNotMatch) => Ok(Some(Arc::new(compute::boolean::not(
                &evaluate_regex_scalar::<i32>(lhs, rhs)?,
            )))),
            (DataType::Utf8, RegexNotIMatch) => {
                todo!();
            }
            (DataType::LargeUtf8, RegexMatch) => {
                Ok(Some(Arc::new(evaluate_regex_scalar::<i64>(lhs, rhs)?)))
            }
            (DataType::LargeUtf8, RegexIMatch) => {
                todo!();
            }
            (DataType::LargeUtf8, RegexNotMatch) => Ok(Some(Arc::new(
                compute::boolean::not(&evaluate_regex_scalar::<i64>(lhs, rhs)?),
            ))),
            (DataType::LargeUtf8, RegexNotIMatch) => {
                todo!();
            }

            _ => Ok(None),
        }
    }
}

fn evaluate_inverse_scalar(
    lhs: &ScalarValue,
    op: &Operator,
    rhs: &dyn Array,
) -> Result<Option<Arc<dyn Array>>> {
    use Operator::*;
    match op {
        Lt => evaluate_scalar(rhs, &Gt, lhs),
        Gt => evaluate_scalar(rhs, &Lt, lhs),
        GtEq => evaluate_scalar(rhs, &LtEq, lhs),
        LtEq => evaluate_scalar(rhs, &GtEq, lhs),
        Eq => evaluate_scalar(rhs, &Eq, lhs),
        NotEq => evaluate_scalar(rhs, &NotEq, lhs),
        Plus => evaluate_scalar(rhs, &Plus, lhs),
        Multiply => evaluate_scalar(rhs, &Multiply, lhs),
        _ => Ok(None),
    }
}

/// Coercion rules for all binary operators. Returns the output type
/// of applying `op` to an argument of `lhs_type` and `rhs_type`.
fn common_binary_type(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // This result MUST be compatible with `binary_coerce`
    let result = match op {
        Operator::And | Operator::Or => match (lhs_type, rhs_type) {
            // logical binary boolean operators can only be evaluated in bools
            (DataType::Boolean, DataType::Boolean) => Some(DataType::Boolean),
            _ => None,
        },
        // logical equality operators have their own rules, and always return a boolean
        Operator::Eq | Operator::NotEq => eq_coercion(lhs_type, rhs_type),
        // "like" operators operate on strings and always return a boolean
        Operator::Like | Operator::NotLike => like_coercion(lhs_type, rhs_type),
        // order-comparison operators have their own rules
        Operator::Lt | Operator::Gt | Operator::GtEq | Operator::LtEq => {
            order_coercion(lhs_type, rhs_type)
        }
        // for math expressions, the final value of the coercion is also the return type
        // because coercion favours higher information types
        Operator::Plus
        | Operator::Minus
        | Operator::Modulo
        | Operator::Divide
        | Operator::Multiply => numerical_coercion(lhs_type, rhs_type),
        Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch => string_coercion(lhs_type, rhs_type),
    };

    // re-write the error message of failed coercions to include the operator's information
    match result {
        None => Err(DataFusionError::Plan(
            format!(
                "'{:?} {} {:?}' can't be evaluated because there isn't a common type to coerce the types to",
                lhs_type, op, rhs_type
            ),
        )),
        Some(t) => Ok(t)
    }
}

/// Returns the return type of a binary operator or an error when the binary operator cannot
/// perform the computation between the argument's types, even after type coercion.
///
/// This function makes some assumptions about the underlying available computations.
pub fn binary_operator_data_type(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // validate that it is possible to perform the operation on incoming types.
    // (or the return datatype cannot be infered)
    let common_type = common_binary_type(lhs_type, op, rhs_type)?;

    match op {
        // operators that return a boolean
        Operator::Eq
        | Operator::NotEq
        | Operator::And
        | Operator::Or
        | Operator::Like
        | Operator::NotLike
        | Operator::Lt
        | Operator::Gt
        | Operator::GtEq
        | Operator::LtEq
        | Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch => Ok(DataType::Boolean),
        // math operations return the same value as the common coerced type
        Operator::Plus
        | Operator::Minus
        | Operator::Divide
        | Operator::Multiply
        | Operator::Modulo => Ok(common_type),
    }
}

impl PhysicalExpr for BinaryExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        binary_operator_data_type(
            &self.left.data_type(input_schema)?,
            &self.op,
            &self.right.data_type(input_schema)?,
        )
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let left_value = self.left.evaluate(batch)?;
        let right_value = self.right.evaluate(batch)?;
        let left_data_type = left_value.data_type();
        let right_data_type = right_value.data_type();

        if left_data_type != right_data_type {
            return Err(DataFusionError::Internal(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                self.op, left_data_type, right_data_type
            )));
        }

        let scalar_result = match (&left_value, &right_value) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) => {
                evaluate_scalar(array.as_ref(), &self.op, scalar)
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(array)) => {
                evaluate_inverse_scalar(scalar, &self.op, array.as_ref())
            }
            (_, _) => Ok(None),
        }?;

        if let Some(result) = scalar_result {
            return Ok(ColumnarValue::Array(result));
        }

        // if both arrays or both literals - extract arrays and continue execution
        let (left, right) = (
            left_value.into_array(batch.num_rows()),
            right_value.into_array(batch.num_rows()),
        );

        let result = evaluate(left.as_ref(), &self.op, right.as_ref());
        result.map(|a| ColumnarValue::Array(a))
    }
}

/// return two physical expressions that are optionally coerced to a
/// common type that the binary operator supports.
fn binary_cast(
    lhs: Arc<dyn PhysicalExpr>,
    op: &Operator,
    rhs: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    let lhs_type = &lhs.data_type(input_schema)?;
    let rhs_type = &rhs.data_type(input_schema)?;

    let cast_type = common_binary_type(lhs_type, op, rhs_type)?;

    Ok((
        try_cast(lhs, input_schema, cast_type.clone())?,
        try_cast(rhs, input_schema, cast_type)?,
    ))
}

/// Create a binary expression whose arguments are correctly coerced.
/// This function errors if it is not possible to coerce the arguments
/// to computational types supported by the operator.
pub fn binary(
    lhs: Arc<dyn PhysicalExpr>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let (l, r) = binary_cast(lhs, &op, rhs, input_schema)?;
    Ok(Arc::new(BinaryExpr::new(l, op, r)))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::*;
    use arrow::{array::*, types::NativeType};

    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::col;

    // Create a binary expression without coercion. Used here when we do not want to coerce the expressions
    // to valid types. Usage can result in an execution (after plan) error.
    fn binary_simple(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, op, r))
    }

    #[test]
    fn binary_comparison() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
        let b = Int32Array::from_slice(&[1, 2, 4, 8, 16]);

        // expression: "a < b"
        let lt = binary_simple(col("a", &schema)?, Operator::Lt, col("b", &schema)?);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        let result = lt.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.len(), 5);

        let expected = vec![false, false, true, true, true];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for (i, &expected_item) in expected.iter().enumerate().take(5) {
            assert_eq!(result.value(i), expected_item);
        }

        Ok(())
    }

    #[test]
    fn binary_nested() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(&[2, 4, 6, 8, 10]);
        let b = Int32Array::from_slice(&[2, 5, 4, 8, 8]);

        // expression: "a < b OR a == b"
        let expr = binary_simple(
            binary_simple(col("a", &schema)?, Operator::Lt, col("b", &schema)?),
            Operator::Or,
            binary_simple(col("a", &schema)?, Operator::Eq, col("b", &schema)?),
        );
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        assert_eq!("a@0 < b@1 OR a@0 = b@1", format!("{}", expr));

        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.len(), 5);

        let expected = vec![true, true, false, true, false];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for (i, &expected_item) in expected.iter().enumerate().take(5) {
            assert_eq!(result.value(i), expected_item);
        }

        Ok(())
    }

    // runs an end-to-end test of physical type coercion:
    // 1. construct a record batch with two columns of type A and B
    //  (*_ARRAY is the Rust Arrow array type, and *_TYPE is the DataType of the elements)
    // 2. construct a physical expression of A OP B
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type C
    // 5. verify that the results of evaluation are $VEC
    macro_rules! test_coercion {
        ($A_ARRAY:ident, $B_ARRAY:ident, $OP:expr, $C_ARRAY:ident) => {{
            let schema = Schema::new(vec![
                Field::new("a", $A_ARRAY.data_type().clone(), false),
                Field::new("b", $B_ARRAY.data_type().clone(), false),
            ]);
            // verify that we can construct the expression
            let expression =
                binary(col("a", &schema)?, $OP, col("b", &schema)?, &schema)?;
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new($A_ARRAY), Arc::new($B_ARRAY)],
            )?;

            // verify that the expression's type is correct
            assert_eq!(&expression.data_type(&schema)?, $C_ARRAY.data_type());

            // compute
            let result = expression.evaluate(&batch)?.into_array(batch.num_rows());

            // verify that the array's data_type is correct
            assert_eq!($C_ARRAY, result.as_ref());
        }};
    }

    #[test]
    fn test_type_coersion() -> Result<()> {
        let a = Int32Array::from_slice(&[1, 2]);
        let b = UInt32Array::from_slice(&[1, 2]);
        let c = Int32Array::from_slice(&[2, 4]);
        test_coercion!(a, b, Operator::Plus, c);

        let a = Int32Array::from_slice(&[1]);
        let b = UInt32Array::from_slice(&[1]);
        let c = Int32Array::from_slice(&[2]);
        test_coercion!(a, b, Operator::Plus, c);

        let a = Int32Array::from_slice(&[1]);
        let b = UInt16Array::from_slice(&[1]);
        let c = Int32Array::from_slice(&[2]);
        test_coercion!(a, b, Operator::Plus, c);

        let a = Float32Array::from_slice(&[1.0]);
        let b = UInt16Array::from_slice(&[1]);
        let c = Float32Array::from_slice(&[2.0]);
        test_coercion!(a, b, Operator::Plus, c);

        let a = Float32Array::from_slice(&[1.0]);
        let b = UInt16Array::from_slice(&[1]);
        let c = Float32Array::from_slice(&[1.0]);
        test_coercion!(a, b, Operator::Multiply, c);

        let a = Utf8Array::<i32>::from_slice(&["hello world"]);
        let b = Utf8Array::<i32>::from_slice(&["%hello%"]);
        let c = BooleanArray::from_slice(&[true]);
        test_coercion!(a, b, Operator::Like, c);

        let a = Utf8Array::<i32>::from_slice(&["1994-12-13"]);
        let b = Int32Array::from_slice(&[9112]).to(DataType::Date32);
        let c = BooleanArray::from_slice(&[true]);
        test_coercion!(a, b, Operator::Eq, c);

        let a = Utf8Array::<i32>::from_slice(&["1994-12-13", "1995-01-26"]);
        let b = Int32Array::from_slice(&[9113, 9154]).to(DataType::Date32);
        let c = BooleanArray::from_slice(&[true, false]);
        test_coercion!(a, b, Operator::Lt, c);

        let a =
            Utf8Array::<i32>::from_slice(&["1994-12-13T12:34:56", "1995-01-26T01:23:45"]);
        let b =
            Int64Array::from_slice(&[787322096000, 791083425000]).to(DataType::Date64);
        let c = BooleanArray::from_slice(&[true, true]);
        test_coercion!(a, b, Operator::Eq, c);

        let a =
            Utf8Array::<i32>::from_slice(&["1994-12-13T12:34:56", "1995-01-26T01:23:45"]);
        let b =
            Int64Array::from_slice(&[787322096001, 791083424999]).to(DataType::Date64);
        let c = BooleanArray::from_slice(&[true, false]);
        test_coercion!(a, b, Operator::Lt, c);

        let a = Utf8Array::<i32>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i32>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[true, false, true, false, false]);
        test_coercion!(a, b, Operator::RegexMatch, c);

        // FIXME: https://github.com/apache/arrow-datafusion/issues/1035
        // let a = Utf8Array::<i32>::from_slice(["abc"; 5]);
        // let b = Utf8Array::<i32>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        // let c = BooleanArray::from_slice(&[true, true, true, true, false]);
        // test_coercion!(a, b, Operator::RegexIMatch, c);

        let a = Utf8Array::<i32>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i32>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[false, true, false, true, true]);
        test_coercion!(a, b, Operator::RegexNotMatch, c);

        // FIXME: https://github.com/apache/arrow-datafusion/issues/1035
        // let a = Utf8Array::<i32>::from_slice(["abc"; 5]);
        // let b = Utf8Array::<i32>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        // let c = BooleanArray::from_slice(&[false, false, false, false, true]);
        // test_coercion!(a, b, Operator::RegexNotIMatch, c);

        let a = Utf8Array::<i64>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i64>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[true, false, true, false, false]);
        test_coercion!(a, b, Operator::RegexMatch, c);

        // FIXME: https://github.com/apache/arrow-datafusion/issues/1035
        // let a = Utf8Array::<i64>::from_slice(["abc"; 5]);
        // let b = Utf8Array::<i64>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        // let c = BooleanArray::from_slice(&[true, true, true, true, false]);
        // test_coercion!(a, b, Operator::RegexIMatch, c);

        let a = Utf8Array::<i64>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i64>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[false, true, false, true, true]);
        test_coercion!(a, b, Operator::RegexNotMatch, c);

        // FIXME: https://github.com/apache/arrow-datafusion/issues/1035
        // let a = Utf8Array::<i64>::from_slice(["abc"; 5]);
        // let b = Utf8Array::<i64>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        // let c = BooleanArray::from_slice(&[false, false, false, false, true]);
        // test_coercion!(a, b, Operator::RegexNotIMatch, c);
        Ok(())
    }

    // Note it would be nice to use the same test_coercion macro as
    // above, but sadly the type of the values of the dictionary are
    // not encoded in the rust type of the DictionaryArray. Thus there
    // is no way at the time of this writing to create a dictionary
    // array using the `From` trait
    #[test]
    fn test_dictionary_type_to_array_coersion() -> Result<()> {
        // Test string  a string dictionary

        let data = vec![Some("one"), None, Some("three"), Some("four")];

        let mut dict_array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
        dict_array.try_extend(data)?;
        let dict_array = dict_array.into_arc();

        let str_array =
            Utf8Array::<i32>::from(&[Some("not one"), Some("two"), None, Some("four")]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict", dict_array.data_type().clone(), true),
            Field::new("str", str_array.data_type().clone(), true),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![dict_array, Arc::new(str_array)])?;

        let expected = BooleanArray::from(&[Some(false), None, None, Some(true)]);

        // Test 1: dict = str

        // verify that we can construct the expression
        let expression = binary(
            col("dict", &schema)?,
            Operator::Eq,
            col("str", &schema)?,
            &schema,
        )?;
        assert_eq!(expression.data_type(&schema)?, DataType::Boolean);

        // evaluate and verify the result type matched
        let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.data_type(), &DataType::Boolean);

        // verify that the result itself is correct
        assert_eq!(expected, result.as_ref());

        // Test 2: now test the other direction
        // str = dict

        // verify that we can construct the expression
        let expression = binary(
            col("str", &schema)?,
            Operator::Eq,
            col("dict", &schema)?,
            &schema,
        )?;
        assert_eq!(expression.data_type(&schema)?, DataType::Boolean);

        // evaluate and verify the result type matched
        let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.data_type(), &DataType::Boolean);

        // verify that the result itself is correct
        assert_eq!(expected, result.as_ref());

        Ok(())
    }

    #[test]
    fn plus_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
        let b = Int32Array::from_slice(&[1, 2, 4, 8, 16]);

        apply_arithmetic::<i32>(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
            Operator::Plus,
            Int32Array::from_slice(&[2, 4, 7, 12, 21]),
        )?;

        Ok(())
    }

    #[test]
    fn minus_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[1, 2, 4, 8, 16]));
        let b = Arc::new(Int32Array::from_slice(&[1, 2, 3, 4, 5]));

        apply_arithmetic::<i32>(
            schema.clone(),
            vec![a.clone(), b.clone()],
            Operator::Minus,
            Int32Array::from_slice(&[0, 0, 1, 4, 11]),
        )?;

        // should handle have negative values in result (for signed)
        apply_arithmetic::<i32>(
            schema,
            vec![b, a],
            Operator::Minus,
            Int32Array::from_slice(&[0, 0, -1, -4, -11]),
        )?;

        Ok(())
    }

    #[test]
    fn multiply_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[4, 8, 16, 32, 64]));
        let b = Arc::new(Int32Array::from_slice(&[2, 4, 8, 16, 32]));

        apply_arithmetic::<i32>(
            schema,
            vec![a, b],
            Operator::Multiply,
            Int32Array::from_slice(&[8, 32, 128, 512, 2048]),
        )?;

        Ok(())
    }

    #[test]
    fn divide_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[8, 32, 128, 512, 2048]));
        let b = Arc::new(Int32Array::from_slice(&[2, 4, 8, 16, 32]));

        apply_arithmetic::<i32>(
            schema,
            vec![a, b],
            Operator::Divide,
            Int32Array::from_slice(&[4, 8, 16, 32, 64]),
        )?;

        Ok(())
    }

    fn apply_arithmetic<T: NativeType>(
        schema: Arc<Schema>,
        data: Vec<Arc<dyn Array>>,
        op: Operator,
        expected: PrimitiveArray<T>,
    ) -> Result<()> {
        let arithmetic_op = binary_simple(col("a", &schema)?, op, col("b", &schema)?);
        let batch = RecordBatch::try_new(schema, data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_eq!(expected, result.as_ref());
        Ok(())
    }

    fn apply_logic_op(
        schema: Arc<Schema>,
        left: BooleanArray,
        right: BooleanArray,
        op: Operator,
        expected: BooleanArray,
    ) -> Result<()> {
        let arithmetic_op = binary_simple(col("a", &schema)?, op, col("b", &schema)?);
        let data: Vec<ArrayRef> = vec![Arc::new(left), Arc::new(right)];
        let batch = RecordBatch::try_new(schema, data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_eq!(expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn modulus_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[8, 32, 128, 512, 2048]));
        let b = Arc::new(Int32Array::from_slice(&[2, 4, 7, 14, 32]));

        apply_arithmetic::<i32>(
            schema,
            vec![a, b],
            Operator::Modulo,
            Int32Array::from_slice(&[0, 0, 2, 8, 0]),
        )?;

        Ok(())
    }

    #[test]
    fn and_with_nulls_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let a = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let b = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ]);

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            None,
        ]);
        apply_logic_op(Arc::new(schema), a, b, Operator::And, expected)?;

        Ok(())
    }

    #[test]
    fn or_with_nulls_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let a = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let b = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ]);

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            None,
        ]);
        apply_logic_op(Arc::new(schema), a, b, Operator::Or, expected)?;

        Ok(())
    }

    #[test]
    fn test_coersion_error() -> Result<()> {
        let expr =
            common_binary_type(&DataType::Float32, &Operator::Plus, &DataType::Utf8);

        if let Err(DataFusionError::Plan(e)) = expr {
            assert_eq!(e, "'Float32 + Utf8' can't be evaluated because there isn't a common type to coerce the types to");
            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "Coercion should have returned an DataFusionError::Internal".to_string(),
            ))
        }
    }
}
