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
use datafusion_common::internal_err;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
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
        internal_err!(
            "SparkCsc: return_type() is not used; return_field_from_args() is implemented"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        let out_dt = DataType::Float64;
        let out_nullable = input_field.is_nullable();

        Ok(Arc::new(Field::new(self.name(), out_dt, out_nullable)))
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
        internal_err!(
            "SparkSec: return_type() is not used; return_field_from_args() is implemented"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        let out_dt = DataType::Float64;
        let out_nullable = input_field.is_nullable();

        Ok(Arc::new(Field::new(self.name(), out_dt, out_nullable)))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csc_nullability() {
        let csc = SparkCsc::new();

        // --- non-nullable input ---
        let non_nullable_f64 = Arc::new(Field::new("c", DataType::Float64, false));
        let out_non_null = csc
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_f64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(!out_non_null.is_nullable());
        assert_eq!(out_non_null.data_type(), &DataType::Float64);

        // --- nullable input ---
        let nullable_f64 = Arc::new(Field::new("c", DataType::Float64, true));
        let out_nullable = csc
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_f64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(out_nullable.is_nullable());
        assert_eq!(out_nullable.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_sec_nullability() {
        let sec = SparkSec::new();

        // --- non-nullable input ---
        let non_nullable_f64 = Arc::new(Field::new("c", DataType::Float64, false));
        let out_non_null = sec
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_f64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(!out_non_null.is_nullable());
        assert_eq!(out_non_null.data_type(), &DataType::Float64);

        // --- nullable input ---
        let nullable_f64 = Arc::new(Field::new("c", DataType::Float64, true));
        let out_nullable = sec
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_f64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(out_nullable.is_nullable());
        assert_eq!(out_nullable.data_type(), &DataType::Float64);
    }
}
