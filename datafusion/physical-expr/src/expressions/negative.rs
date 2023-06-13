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

//! Negation (-) expression

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::kernels::arithmetic::negate;
use arrow::{
    array::{
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
    },
    datatypes::{DataType, IntervalUnit, Schema},
    record_batch::RecordBatch,
};

use crate::physical_expr::down_cast_any_ref;
use crate::PhysicalExpr;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{
    type_coercion::{is_interval, is_null, is_signed_numeric},
    ColumnarValue,
};

/// Invoke a compute kernel on array(s)
macro_rules! compute_op {
    // invoke unary operator
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&operand)?))
    }};
}

/// Negative expression
#[derive(Debug, Hash)]
pub struct NegativeExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

impl NegativeExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for NegativeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(- {})", self.arg)
    }
}

impl PhysicalExpr for NegativeExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let result: Result<ArrayRef> = match array.data_type() {
                    DataType::Int8 => compute_op!(array, negate, Int8Array),
                    DataType::Int16 => compute_op!(array, negate, Int16Array),
                    DataType::Int32 => compute_op!(array, negate, Int32Array),
                    DataType::Int64 => compute_op!(array, negate, Int64Array),
                    DataType::Float32 => compute_op!(array, negate, Float32Array),
                    DataType::Float64 => compute_op!(array, negate, Float64Array),
                    DataType::Interval(IntervalUnit::YearMonth) => compute_op!(array, negate, IntervalYearMonthArray),
                    DataType::Interval(IntervalUnit::DayTime) => compute_op!(array, negate, IntervalDayTimeArray),
                    DataType::Interval(IntervalUnit::MonthDayNano) => compute_op!(array, negate, IntervalMonthDayNanoArray),
                    _ => Err(DataFusionError::Internal(format!(
                        "(- '{:?}') can't be evaluated because the expression's type is {:?}, not signed numeric",
                        self,
                        array.data_type(),
                    ))),
                };
                result.map(|a| ColumnarValue::Array(a))
            }
            ColumnarValue::Scalar(scalar) => {
                Ok(ColumnarValue::Scalar((scalar.arithmetic_negate())?))
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NegativeExpr::new(children[0].clone())))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for NegativeExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg))
            .unwrap_or(false)
    }
}

/// Creates a unary expression NEGATIVE
///
/// # Errors
///
/// This function errors when the argument's type is not signed numeric
pub fn negative(
    arg: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let data_type = arg.data_type(input_schema)?;
    if is_null(&data_type) {
        Ok(arg)
    } else if !is_signed_numeric(&data_type) && !is_interval(&data_type) {
        Err(DataFusionError::Internal(
            format!("Can't create negative physical expr for (- '{arg:?}'), the type of child expr is {data_type}, not signed numeric"),
        ))
    } else {
        Ok(Arc::new(NegativeExpr::new(arg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    #[allow(unused_imports)]
    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow_schema::DataType::{Float32, Float64, Int16, Int32, Int64, Int8};
    use arrow_schema::IntervalUnit::{DayTime, MonthDayNano, YearMonth};
    use datafusion_common::{cast::as_primitive_array, Result};
    use paste::paste;

    macro_rules! test_array_negative_op {
        ($DATA_TY:tt, $($VALUE:expr),*   ) => {
            let schema = Schema::new(vec![Field::new("a", DataType::$DATA_TY, true)]);
            let expr = negative(col("a", &schema)?, &schema)?;
            assert_eq!(expr.data_type(&schema)?, DataType::$DATA_TY);
            assert!(expr.nullable(&schema)?);
            let mut arr = Vec::new();
            let mut arr_expected = Vec::new();
            $(
                arr.push(Some($VALUE));
                arr_expected.push(Some(-$VALUE));
            )+
            arr.push(None);
            arr_expected.push(None);
            let input = paste!{[<$DATA_TY Array>]::from(arr)};
            let expected = &paste!{[<$DATA_TY Array>]::from(arr_expected)};
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)])?;
            let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
            let result =
                as_primitive_array(&result).expect(format!("failed to downcast to {:?}Array", $DATA_TY).as_str());
            assert_eq!(result, expected);
        };
    }

    macro_rules! test_array_negative_op_intervals {
        ($DATA_TY:tt, $($VALUE:expr),*   ) => {
            let schema = Schema::new(vec![Field::new("a", DataType::Interval(IntervalUnit::$DATA_TY), true)]);
            let expr = negative(col("a", &schema)?, &schema)?;
            assert_eq!(expr.data_type(&schema)?, DataType::Interval(IntervalUnit::$DATA_TY));
            assert!(expr.nullable(&schema)?);
            let mut arr = Vec::new();
            let mut arr_expected = Vec::new();
            $(
                arr.push(Some($VALUE));
                arr_expected.push(Some(-$VALUE));
            )+
            arr.push(None);
            arr_expected.push(None);
            let input = paste!{[<Interval $DATA_TY Array>]::from(arr)};
            let expected = &paste!{[<Interval $DATA_TY Array>]::from(arr_expected)};
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)])?;
            let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
            let result =
                as_primitive_array(&result).expect(format!("failed to downcast to {:?}Array", $DATA_TY).as_str());
            assert_eq!(result, expected);
        };
    }

    #[test]
    fn array_negative_op() -> Result<()> {
        test_array_negative_op!(Int8, 2i8, 1i8);
        test_array_negative_op!(Int16, 234i16, 123i16);
        test_array_negative_op!(Int32, 2345i32, 1234i32);
        test_array_negative_op!(Int64, 23456i64, 12345i64);
        test_array_negative_op!(Float32, 2345.0f32, 1234.0f32);
        test_array_negative_op!(Float64, 23456.0f64, 12345.0f64);
        test_array_negative_op_intervals!(YearMonth, 2345i32, 1234i32);
        test_array_negative_op_intervals!(DayTime, 23456i64, 12345i64);
        test_array_negative_op_intervals!(MonthDayNano, 234567i128, 123456i128);
        Ok(())
    }
}
