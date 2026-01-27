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

use arrow::array::*;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `negative` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#negative>
///
/// Returns the negation of input (equivalent to unary minus)
/// Returns NULL if input is NULL, returns NaN if input is NaN.
///
/// TODOs:
///  - Spark's ANSI-compliant dialect, when off (i.e. `spark.sql.ansi.enabled=false`),
///    negating the minimal value of a signed integer wraps around.
///    For example: negative(i32::MIN) returns i32::MIN (wraps instead of error).
///    This is the current implementation (legacy mode only).
///  - Spark's ANSI mode (when `spark.sql.ansi.enabled=true`) should throw an
///    ARITHMETIC_OVERFLOW error on integer overflow instead of wrapping.
///    This is not yet implemented - all operations currently use wrapping behavior.
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNegative {
    signature: Signature,
}

impl Default for SparkNegative {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkNegative {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkNegative {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "negative"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_negative(&args.args)
    }
}

/// Helper macro to generate wrapping negation for array types
macro_rules! wrapping_negative_array {
    ($INPUT:expr, $ARRAY_TYPE:ident) => {{
        let array = $INPUT
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected {}, got different type",
                    stringify!($ARRAY_TYPE)
                ))
            })?;
        let result: $ARRAY_TYPE = array.unary(|x| x.wrapping_neg());
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

/// Helper macro to generate simple negation for floating point array types
macro_rules! simple_negative_array {
    ($INPUT:expr, $ARRAY_TYPE:ident) => {{
        let array = $INPUT
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected {}, got different type",
                    stringify!($ARRAY_TYPE)
                ))
            })?;
        let result: $ARRAY_TYPE = array.unary(|x| -x);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

/// Helper macro to generate wrapping negation for scalar types
macro_rules! wrapping_negative_scalar {
    ($INPUT:ident, $SCALAR_TYPE:ident) => {{
        let result = $INPUT.wrapping_neg();
        Ok(ColumnarValue::Scalar(ScalarValue::$SCALAR_TYPE(Some(
            result,
        ))))
    }};
}

/// Helper macro to generate wrapping negation for decimal scalar types
macro_rules! wrapping_negative_decimal_scalar {
    ($INPUT:ident, $PRECISION:expr, $SCALE:expr, $SCALAR_TYPE:ident) => {{
        let result = $INPUT.wrapping_neg();
        Ok(ColumnarValue::Scalar(ScalarValue::$SCALAR_TYPE(
            Some(result),
            $PRECISION,
            $SCALE,
        )))
    }};
}

/// Core implementation of Spark's negative function
fn spark_negative(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [arg] = take_function_args("negative", args)?;

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Ok(args[0].clone()),

            // Signed integers - use wrapping negation (Spark legacy mode behavior)
            DataType::Int8 => wrapping_negative_array!(array, Int8Array),
            DataType::Int16 => wrapping_negative_array!(array, Int16Array),
            DataType::Int32 => wrapping_negative_array!(array, Int32Array),
            DataType::Int64 => wrapping_negative_array!(array, Int64Array),

            // Floating point - simple negation (no overflow possible)
            DataType::Float16 => simple_negative_array!(array, Float16Array),
            DataType::Float32 => simple_negative_array!(array, Float32Array),
            DataType::Float64 => simple_negative_array!(array, Float64Array),

            // Decimal types - wrapping negation
            DataType::Decimal32(_, _) => {
                wrapping_negative_array!(array, Decimal32Array)
            }
            DataType::Decima64(_, _) => {
                wrapping_negative_array!(array, Decimal64Array)
            }
            DataType::Decimal128(_, _) => {
                wrapping_negative_array!(array, Decimal128Array)
            }
            DataType::Decimal256(_, _) => {
                wrapping_negative_array!(array, Decimal256Array)
            }

            dt => internal_err!("Not supported datatype for Spark NEGATIVE: {dt}"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Null => Ok(args[0].clone()),
            sv if sv.is_null() => Ok(args[0].clone()),

            // Signed integers - wrapping negation
            ScalarValue::Int8(Some(v)) => wrapping_negative_scalar!(v, Int8),
            ScalarValue::Int16(Some(v)) => wrapping_negative_scalar!(v, Int16),
            ScalarValue::Int32(Some(v)) => wrapping_negative_scalar!(v, Int32),
            ScalarValue::Int64(Some(v)) => wrapping_negative_scalar!(v, Int64),

            // Floating point - simple negation
            ScalarValue::Float16(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float16(Some(-v))))
            }
            ScalarValue::Float32(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(-v))))
            }
            ScalarValue::Float64(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(-v))))
            }

            // Decimal types - wrapping negation
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                wrapping_negative_decimal_scalar!(v, *precision, *scale, Decimal128)
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                wrapping_negative_decimal_scalar!(v, *precision, *scale, Decimal256)
            }

            dt => internal_err!("Not supported datatype for Spark NEGATIVE: {dt}"),
        },
    }
}
