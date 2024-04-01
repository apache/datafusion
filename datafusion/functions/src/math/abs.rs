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

//! math expressions

use arrow::array::Decimal128Array;
use arrow::array::Decimal256Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int8Array;
use arrow::datatypes::DataType;
use datafusion_common::{exec_err, not_impl_err};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;

use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::error::ArrowError;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

type MathArrayFunction = fn(&Vec<ArrayRef>) -> Result<ArrayRef>;

macro_rules! make_abs_function {
    ($ARRAY_TYPE:ident) => {{
        |args: &Vec<ArrayRef>| {
            let array = downcast_arg!(&args[0], "abs arg", $ARRAY_TYPE);
            let res: $ARRAY_TYPE = array.unary(|x| x.abs());
            Ok(Arc::new(res) as ArrayRef)
        }
    }};
}

macro_rules! make_try_abs_function {
    ($ARRAY_TYPE:ident) => {{
        |args: &Vec<ArrayRef>| {
            let array = downcast_arg!(&args[0], "abs arg", $ARRAY_TYPE);
            let res: $ARRAY_TYPE = array.try_unary(|x| {
                x.checked_abs().ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "{} overflow on abs({})",
                        stringify!($ARRAY_TYPE),
                        x
                    ))
                })
            })?;
            Ok(Arc::new(res) as ArrayRef)
        }
    }};
}

macro_rules! make_decimal_abs_function {
    ($ARRAY_TYPE:ident) => {{
        |args: &Vec<ArrayRef>| {
            let array = downcast_arg!(&args[0], "abs arg", $ARRAY_TYPE);
            let res: $ARRAY_TYPE = array
                .unary(|x| x.wrapping_abs())
                .with_data_type(args[0].data_type().clone());
            Ok(Arc::new(res) as ArrayRef)
        }
    }};
}

/// Abs SQL function
/// Return different implementations based on input datatype to reduce branches during execution
fn create_abs_function(input_data_type: &DataType) -> Result<MathArrayFunction> {
    match input_data_type {
        DataType::Float32 => Ok(make_abs_function!(Float32Array)),
        DataType::Float64 => Ok(make_abs_function!(Float64Array)),

        // Types that may overflow, such as abs(-128_i8).
        DataType::Int8 => Ok(make_try_abs_function!(Int8Array)),
        DataType::Int16 => Ok(make_try_abs_function!(Int16Array)),
        DataType::Int32 => Ok(make_try_abs_function!(Int32Array)),
        DataType::Int64 => Ok(make_try_abs_function!(Int64Array)),

        // Types of results are the same as the input.
        DataType::Null
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(|args: &Vec<ArrayRef>| Ok(args[0].clone())),

        // Decimal types
        DataType::Decimal128(_, _) => Ok(make_decimal_abs_function!(Decimal128Array)),
        DataType::Decimal256(_, _) => Ok(make_decimal_abs_function!(Decimal256Array)),

        other => not_impl_err!("Unsupported data type {other:?} for function abs"),
    }
}
#[derive(Debug)]
pub struct AbsFunc {
    signature: Signature,
}

impl Default for AbsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AbsFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AbsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            DataType::Int8 => Ok(DataType::Int8),
            DataType::Int16 => Ok(DataType::Int16),
            DataType::Int32 => Ok(DataType::Int32),
            DataType::Int64 => Ok(DataType::Int64),
            DataType::Null => Ok(DataType::Null),
            DataType::UInt8 => Ok(DataType::UInt8),
            DataType::UInt16 => Ok(DataType::UInt16),
            DataType::UInt32 => Ok(DataType::UInt32),
            DataType::UInt64 => Ok(DataType::UInt64),
            DataType::Decimal128(precision, scale) => {
                Ok(DataType::Decimal128(precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                Ok(DataType::Decimal256(precision, scale))
            }
            _ => not_impl_err!(
                "Unsupported data type {} for function abs",
                arg_types[0].to_string()
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        if args.len() != 1 {
            return exec_err!("abs function requires 1 argument, got {}", args.len());
        }

        let input_data_type = args[0].data_type();
        let abs_fun = create_abs_function(input_data_type)?;

        let arr = abs_fun(&args)?;
        Ok(ColumnarValue::Array(arr))
    }
}
