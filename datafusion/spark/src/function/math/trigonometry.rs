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

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float64Type};
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
    aliases: Vec<String>,
}

impl Default for SparkCsc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCsc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
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
        spark_csc(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                CSC_FUNCTION_NAME,
                (1, 1),
                arg_types.len(),
            ));
        }
        if arg_types[0].is_numeric() {
            Ok(vec![DataType::Float64])
        } else {
            Err(unsupported_data_type_exec_err(
                CSC_FUNCTION_NAME,
                "Numeric Type",
                &arg_types[0],
            ))
        }
    }
}

fn spark_csc(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return Err(invalid_arg_count_exec_err(
            CSC_FUNCTION_NAME,
            (1, 1),
            args.len(),
        ));
    }
    match &args[0] {
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

#[cfg(test)]
mod tests {
    use crate::function::math::trigonometry::{spark_csc, SparkCsc};
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{Array, Float64Array};
    use arrow::datatypes::DataType::Float64;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::f64::consts::PI;
    use std::sync::Arc;

    macro_rules! test_trig_float64_invoke {
        ($FUNC: expr, $INPUT:expr, $EXPECTED:expr) => {
            test_scalar_function!(
                $FUNC,
                vec![ColumnarValue::Scalar(ScalarValue::Float64($INPUT))],
                $EXPECTED,
                f64,
                Float64,
                Float64Array
            );
        };
    }

    #[test]
    fn test_csc_invoke() {
        test_trig_float64_invoke!(SparkCsc::new(), Some(0f64), Ok(Some(f64::INFINITY)));
    }

    #[test]
    fn test_csc_array() {
        let input = Float64Array::from(vec![1f64, 0f64, -1f64]);
        let expected = Float64Array::from(vec![
            1.1883951057781212,
            f64::INFINITY,
            -1.1883951057781212,
        ]);
        let args = ColumnarValue::Array(Arc::new(input));

        if let Ok(ColumnarValue::Array(result_array)) = spark_csc(&[args]) {
            let output = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(output, &expected);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_csc_scalar() {
        let input = ScalarValue::Float64(Some(PI / 2.0));
        let expected = ScalarValue::Float64(Some(1.0));
        let args = ColumnarValue::Scalar(input);

        if let Ok(ColumnarValue::Scalar(result_scalar)) = spark_csc(&[args]) {
            assert_eq!(result_scalar, expected);
        } else {
            panic!("Expected scalar result");
        }
    }
}
