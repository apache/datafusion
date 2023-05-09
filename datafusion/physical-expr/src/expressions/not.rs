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

//! Not expression

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::physical_expr::down_cast_any_ref;
use crate::PhysicalExpr;
use arrow::datatypes::{
    DataType, Int16Type, Int32Type, Int64Type, Int8Type, Schema, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{cast::as_boolean_array, DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

/// Not expression
#[derive(Debug)]
pub struct NotExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

impl NotExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl fmt::Display for NotExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NOT {}", self.arg)
    }
}

impl PhysicalExpr for NotExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        // Ok(DataType::Boolean)
        let data_type = self.arg.data_type(input_schema)?;
        match data_type {
            DataType::Boolean => Ok(DataType::Boolean),
            DataType::UInt8 => Ok(DataType::UInt8),
            DataType::UInt16 => Ok(DataType::UInt16),
            DataType::UInt32 => Ok(DataType::UInt32),
            DataType::UInt64 => Ok(DataType::UInt64),
            DataType::Int8 => Ok(DataType::Int8),
            DataType::Int16 => Ok(DataType::Int16),
            DataType::Int32 => Ok(DataType::Int32),
            DataType::Int64 => Ok(DataType::Int64),
            DataType::Null => Ok(DataType::Null),
            _ => Err(DataFusionError::Plan(format!(
                "NOT or BITWISE_NOT not supported for datatype: '{data_type:?}'"
            ))),
        }
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let evaluate_arg = self.arg.evaluate(batch)?;
        match evaluate_arg {
            ColumnarValue::Array(array) => {
                match array.data_type() {
                    DataType::Boolean => {
                        let array = as_boolean_array(&array)?;
                        Ok(ColumnarValue::Array(Arc::new(
                            arrow::compute::kernels::boolean::not(array)?,
                        )))
                    },
                    DataType::UInt8 => expr_array_not!(&array, UInt8Type),
                    DataType::UInt16 => expr_array_not!(&array, UInt16Type),
                    DataType::UInt32 => expr_array_not!(&array, UInt32Type),
                    DataType::UInt64 => expr_array_not!(&array, UInt64Type),
                    DataType::Int8 => expr_array_not!(&array, Int8Type),
                    DataType::Int16 => expr_array_not!(&array, Int16Type),
                    DataType::Int32 => expr_array_not!(&array, Int32Type),
                    DataType::Int64 => expr_array_not!(&array, Int64Type),
                    _ => Err(DataFusionError::Internal(format!(
                        "NOT or Bitwise_not can't be evaluated because the expression's typs is {:?}, not boolean or integer",
                        array.data_type(),
                    )))
                }
            }
            ColumnarValue::Scalar(scalar) => {
                match scalar {
                    ScalarValue::Boolean(v) => expr_not!(v, Boolean),
                    ScalarValue::Int8(v) => expr_not!(v, Int8),
                    ScalarValue::Int16(v) => expr_not!(v, Int16),
                    ScalarValue::Int32(v) => expr_not!(v, Int32),
                    ScalarValue::Int64(v) => expr_not!(v, Int64),
                    ScalarValue::UInt8(v) => expr_not!(v, UInt8),
                    ScalarValue::UInt16(v) => expr_not!(v, UInt16),
                    ScalarValue::UInt32(v) => expr_not!(v, UInt32),
                    ScalarValue::UInt64(v) => expr_not!(v, UInt64),
                    ScalarValue::Null => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
                    _ => {
                        Err(DataFusionError::Internal(format!(
                            "NOT/BITWISE_NOT '{:?}' can't be evaluated because the expression's type is {:?}, not boolean or NULL or Integer",
                            self.arg, scalar.get_datatype(),
                        )))
                    }
                }
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
        Ok(Arc::new(NotExpr::new(children[0].clone())))
    }
}

impl PartialEq<dyn Any> for NotExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg))
            .unwrap_or(false)
    }
}

/// Creates a unary expression NOT
pub fn not(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(NotExpr::new(arg)))
}

/// Create a unary expression BITWISE_NOT
pub fn bitwise_not(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(NotExpr::new(arg)))
}

macro_rules! expr_not {
    ($VALUE:expr, $SCALAR_TY:ident) => {
        match $VALUE {
            Some(v) => Ok(ColumnarValue::Scalar(ScalarValue::$SCALAR_TY(Some(!v)))),
            None => Ok(ColumnarValue::Scalar(ScalarValue::$SCALAR_TY(None))),
        }
    };
}

macro_rules! expr_array_not {
    ($ARRAY:expr, $PRIMITIVE_TY:ident) => {
        Ok(ColumnarValue::Array(Arc::new(
            arrow::compute::kernels::bitwise::bitwise_not(as_primitive_array::<
                $PRIMITIVE_TY,
            >($ARRAY)?)?,
        )))
    };
}

use expr_array_not;
use expr_not;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit};
    use arrow::{array::*, datatypes::*};
    use arrow_schema::DataType::{
        Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8,
    };
    use datafusion_common::{
        cast::{as_boolean_array, as_primitive_array},
        Result,
    };
    use paste::paste;

    #[test]
    fn neg_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);

        let expr = not(col("a", &schema)?)?;
        assert_eq!(expr.data_type(&schema)?, DataType::Boolean);
        assert!(expr.nullable(&schema)?);

        let input = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let expected = &BooleanArray::from(vec![Some(false), None, Some(true)]);

        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)])?;

        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result =
            as_boolean_array(&result).expect("failed to downcast to BooleanArray");
        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn scalar_bitwise_not_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::UInt8, true)]));
        let dummy_input = Arc::new(UInt8Array::from(vec![Some(1u8)]));
        let dummy_batch = RecordBatch::try_new(schema, vec![dummy_input])?;

        let expr = bitwise_not(lit(0u8))?;
        match expr.evaluate(&dummy_batch)? {
            ColumnarValue::Scalar(v) => assert_eq!(v, ScalarValue::UInt8(Some(255))),
            _ => unreachable!("should be ColumnarValue::Scalar datatype"),
        }

        let expr = bitwise_not(lit(1u32))?;
        match expr.evaluate(&dummy_batch)? {
            ColumnarValue::Scalar(v) => {
                assert_eq!(v, ScalarValue::UInt32(Some(u32::MAX - 1)))
            }
            _ => unreachable!("should be ColumnarValue::Scalar datatype"),
        }

        let expr = bitwise_not(lit(ScalarValue::UInt16(None)))?;
        match expr.evaluate(&dummy_batch)? {
            ColumnarValue::Scalar(v) => assert_eq!(v, ScalarValue::UInt16(None)),
            _ => unreachable!("should be ColumnarValue::Scalar datatype"),
        }

        let expr = bitwise_not(lit(3i8))?;
        match expr.evaluate(&dummy_batch)? {
            // 3i8: 0000 0011 => !3i8: 1111 1100 = -4
            ColumnarValue::Scalar(v) => assert_eq!(v, ScalarValue::Int8(Some(-4i8))),
            _ => unreachable!("should be ColumnarValue::Scalar datatype"),
        }

        Ok(())
    }

    macro_rules! test_array_bitwise_not_op {
        ($DATA_TY:tt, $($VALUE:expr),*   ) => {
            let schema = Schema::new(vec![Field::new("a", DataType::$DATA_TY, true)]);
            let expr = not(col("a", &schema)?)?;
            assert_eq!(expr.data_type(&schema)?, DataType::$DATA_TY);
            assert!(expr.nullable(&schema)?);
            let mut arr = Vec::new();
            let mut arr_expected = Vec::new();
            $(
                arr.push(Some($VALUE));
                arr_expected.push(Some(!$VALUE));
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

    #[test]
    fn array_bitwise_not_op() -> Result<()> {
        test_array_bitwise_not_op!(UInt8, 0x1, 0xF2);
        test_array_bitwise_not_op!(UInt16, 32u16, 255u16);
        test_array_bitwise_not_op!(UInt32, 144u32, 166u32);
        test_array_bitwise_not_op!(UInt64, 123u64, 321u64);
        test_array_bitwise_not_op!(Int8, -1i8, 1i8);
        test_array_bitwise_not_op!(Int16, -123i16, 123i16);
        test_array_bitwise_not_op!(Int32, -1234i32, 1234i32);
        test_array_bitwise_not_op!(Int64, -12345i64, 12345i64);
        Ok(())
    }
}
