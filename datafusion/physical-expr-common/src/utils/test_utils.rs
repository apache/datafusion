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

use std::{any::Any, sync::Arc};

use datafusion_common::{exec_err, DataFusionError, Result};

use arrow::{
    array::{ArrayRef, Float32Array, Float64Array},
    datatypes::DataType,
};

use datafusion_expr::{
    ColumnarValue, FuncMonotonicity, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone)]
pub struct TestScalarUDF {
    pub signature: Signature,
}

impl Default for TestScalarUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl TestScalarUDF {
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

impl ScalarUDFImpl for TestScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "test-scalar-udf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = &arg_types[0];

        match arg_type {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        }
    }

    fn monotonicity(&self) -> Result<Option<FuncMonotonicity>> {
        Ok(Some(vec![Some(true)]))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => Arc::new({
                let arg = &args[0].as_any().downcast_ref::<Float64Array>().ok_or_else(
                    || {
                        DataFusionError::Internal(format!(
                            "could not cast {} to {}",
                            self.name(),
                            std::any::type_name::<Float64Array>()
                        ))
                    },
                )?;

                arg.iter()
                    .map(|a| a.map(f64::floor))
                    .collect::<Float64Array>()
            }),
            DataType::Float32 => Arc::new({
                let arg = &args[0].as_any().downcast_ref::<Float32Array>().ok_or_else(
                    || {
                        DataFusionError::Internal(format!(
                            "could not cast {} to {}",
                            self.name(),
                            std::any::type_name::<Float32Array>()
                        ))
                    },
                )?;

                arg.iter()
                    .map(|a| a.map(f32::floor))
                    .collect::<Float32Array>()
            }),
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                );
            }
        };
        Ok(ColumnarValue::Array(arr))
    }
}
