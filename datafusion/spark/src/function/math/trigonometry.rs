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

use crate::function::error_utils::unsupported_data_type_exec_err;
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float64Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

static CSC_FUNCTION_NAME: &str = "csc";

/// <https://spark.apache.org/docs/latest/api/sql/index.html#csc>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCsc {
    signature: Signature,
}

impl Default for SparkCsc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCsc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCsc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        CSC_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), &args.args)?;
        spark_csc(arg)
    }
}

fn spark_csc(arg: &ColumnarValue) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float64(value.map(|x| 1.0 / x.sin())),
        )),
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(
                array
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(|x| 1.0 / x.sin()),
            ) as ArrayRef)),
            other => Err(unsupported_data_type_exec_err(
                CSC_FUNCTION_NAME,
                format!("{}", DataType::Float64).as_str(),
                other,
            )),
        },
        other => Err(unsupported_data_type_exec_err(
            CSC_FUNCTION_NAME,
            format!("{}", DataType::Float64).as_str(),
            &other.data_type(),
        )),
    }
}

static SEC_FUNCTION_NAME: &str = "sec";

/// <https://spark.apache.org/docs/latest/api/sql/index.html#sec>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSec {
    signature: Signature,
}

impl Default for SparkSec {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSec {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        SEC_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), &args.args)?;
        spark_sec(arg)
    }
}

fn spark_sec(arg: &ColumnarValue) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float64(value.map(|x| 1.0 / x.cos())),
        )),
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(
                array
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(|x| 1.0 / x.cos()),
            ) as ArrayRef)),
            other => Err(unsupported_data_type_exec_err(
                SEC_FUNCTION_NAME,
                format!("{}", DataType::Float64).as_str(),
                other,
            )),
        },
        other => Err(unsupported_data_type_exec_err(
            SEC_FUNCTION_NAME,
            format!("{}", DataType::Float64).as_str(),
            &other.data_type(),
        )),
    }
}
