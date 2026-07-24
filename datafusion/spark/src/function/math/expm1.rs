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
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use std::sync::Arc;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#expm1>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkExpm1 {
    signature: Signature,
}

impl Default for SparkExpm1 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkExpm1 {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkExpm1 {
    fn name(&self) -> &str {
        "expm1"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Spark's `expm1` is a null-intolerant `UnaryMathExpression`: the result is NULL
        // exactly when the input is NULL, so propagate the child's nullability instead of
        // defaulting to always-nullable. See #19144.
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Float64,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;
        match arg {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expm1_nullability() {
        let expm1 = SparkExpm1::new();
        for nullable in [true, false] {
            let field = Arc::new(Field::new("c", DataType::Float64, nullable));
            let out = expm1
                .return_field_from_args(ReturnFieldArgs {
                    arg_fields: &[field],
                    scalar_arguments: &[None],
                })
                .unwrap();
            assert_eq!(out.data_type(), &DataType::Float64);
            assert_eq!(out.is_nullable(), nullable);
        }
    }
}
