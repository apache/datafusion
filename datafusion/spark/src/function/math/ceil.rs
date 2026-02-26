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

use arrow::array::{
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;
// spark semantics

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $RESULT =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC() as i64);
                Ok(Arc::new(res))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
}

pub fn spark_ceil(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result =
                    downcast_compute_op!(array, "ceil", ceil, Float32Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result =
                    downcast_compute_op!(array, "ceil", ceil, Float64Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Int8 => {
                let input = array.as_any().downcast_ref::<Int8Array>().unwrap();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int16 => {
                let input = array.as_any().downcast_ref::<Int16Array>().unwrap();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int32 => {
                let input = array.as_any().downcast_ref::<Int32Array>().unwrap();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int64 => {
                // Optimization: Int64 -> Int64 doesn't need conversion, just return same array
                Ok(ColumnarValue::Array(Arc::clone(array)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function ceil",
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Int8(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x as i64),
            ))),
            ScalarValue::Int16(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x as i64),
            ))),
            ScalarValue::Int32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(*a))),
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ceil",
                value.data_type(),
            ))),
        },
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCiel {
    signature: Signature,
}

impl Default for SparkCiel {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCiel {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCiel {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion_common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        spark_ceil(&args.args)
    }
}
