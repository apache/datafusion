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

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};

#[derive(Debug)]
pub struct SparkExpm1 {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkExpm1 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkExpm1 {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["spark_expm1".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkExpm1 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "expm1"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(invalid_arg_count_exec_err("expm1", (1, 1), args.args.len()));
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(
                ColumnarValue::Scalar(ScalarValue::Float64(value.map(|x| x.exp_m1()))),
            ),
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(
                    array
                        .as_primitive::<Float64Type>()
                        .unary::<_, Float64Type>(|x| x.exp_m1()),
                )
                    as ArrayRef)),
                other => Err(unsupported_data_type_exec_err(
                    "expm1",
                    format!("{}", DataType::Float64).as_str(),
                    other,
                )),
            },
            other => Err(unsupported_data_type_exec_err(
                "expm1",
                format!("{}", DataType::Float64).as_str(),
                &other.data_type(),
            )),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err("expm1", (1, 1), arg_types.len()));
        }
        if arg_types[0].is_numeric() {
            Ok(vec![DataType::Float64])
        } else {
            Err(unsupported_data_type_exec_err(
                "expm1",
                "Numeric Type",
                &arg_types[0],
            ))
        }
    }
}
