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

//! Math function: `isnan()`.

use arrow::datatypes::DataType;
use datafusion_common::{exec_err, DataFusionError, Result, plan_datafusion_err};
use datafusion_expr::ColumnarValue;

use arrow::array::{ArrayRef, BooleanArray, Float32Array, Float64Array};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility, utils::generate_signature_error_msg};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct IsNanFunc {
    signature: Signature,
}

impl IsNanFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature:
            Signature::one_of(
                vec![Exact(vec![Float32]), Exact(vec![Float64])],
                Volatility::Immutable,
            )
        }
    }
}

impl ScalarUDFImpl for IsNanFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "isnan"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(plan_datafusion_err!(
                "{}",
                generate_signature_error_msg(
                    self.name(),
                    self.signature().clone(),
                    arg_types,
                )
            ));
        }

        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => {
                Arc::new(make_function_scalar_inputs_return_type!(
                                &args[0],
                                self.name(),
                                Float64Array,
                                BooleanArray,
                                { f64::is_nan }
                            ))
            }
            DataType::Float32 => {
                Arc::new(make_function_scalar_inputs_return_type!(
                            &args[0],
                            self.name(),
                            Float32Array,
                            BooleanArray,
                            { f32::is_nan }
                        ))
            }
            other => return exec_err!("Unsupported data type {other:?} for function {}", self.name()),
        };
        Ok(ColumnarValue::Array(arr))
    }
}
