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

//! Math function: `tanh()`.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{ArrayRef, Float32Array, Float64Array};
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::Volatility;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature};

#[derive(Debug)]
pub struct TanhFunc {
    signature: Signature,
}

impl TanhFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Float64, DataType::Float32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TanhFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "tanh"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = &arg_types[0];

        match arg_type {
            DataType::Float64 => Ok(DataType::Float64),
            DataType::Float32 => Ok(DataType::Float32),

            // For other types (possible values null/int), use Float 64
            _ => Ok(DataType::Float64),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => Arc::new(make_function_scalar_inputs_return_type!(
                &args[0],
                self.name(),
                Float64Array,
                Float64Array,
                { f64::tanh }
            )),
            DataType::Float32 => Arc::new(make_function_scalar_inputs_return_type!(
                &args[0],
                self.name(),
                Float32Array,
                Float32Array,
                { f32::tanh }
            )),
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                )
            }
        };
        Ok(ColumnarValue::Array(arr))
    }
}
