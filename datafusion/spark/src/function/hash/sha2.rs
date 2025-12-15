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

extern crate datafusion_functions;

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};
use crate::function::math::hex::spark_sha2_hex;
use arrow::array::{ArrayRef, AsArray, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type};
use datafusion_common::{
    Result, ScalarValue, exec_err, internal_datafusion_err, internal_err,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
pub use datafusion_functions::crypto::basic::{sha224, sha256, sha384, sha512};
use std::any::Any;
use std::sync::Arc;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#sha2>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSha2 {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkSha2 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSha2 {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkSha2 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types: Vec<_> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();

        let data_type = if arg_types[1].is_null() {
            DataType::Null
        } else {
            match arg_types[0] {
                DataType::Utf8View
                | DataType::LargeUtf8
                | DataType::Utf8
                | DataType::Binary
                | DataType::BinaryView
                | DataType::LargeBinary => DataType::Utf8,
                DataType::Null => DataType::Null,
                _ => {
                    return exec_err!(
                        "{} function can only accept strings or binary arrays.",
                        self.name()
                    );
                }
            }
        };

        let nullable = args.arg_fields.iter().any(|f| f.is_nullable())
            || args
                .scalar_arguments
                .iter()
                .any(|scalar| scalar.is_some_and(|s| s.is_null()));

        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 2] = args.args.try_into().map_err(|_| {
            internal_datafusion_err!("Expected 2 arguments for function sha2")
        })?;

        sha2(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (2, 2),
                arg_types.len(),
            ));
        }
        let expr_type = match &arg_types[0] {
            DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Utf8
            | DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary
            | DataType::Null => Ok(arg_types[0].clone()),
            _ => Err(unsupported_data_type_exec_err(
                self.name(),
                "String, Binary",
                &arg_types[0],
            )),
        }?;
        let bit_length_type = if arg_types[1].is_numeric() {
            Ok(DataType::Int32)
        } else if arg_types[1].is_null() {
            Ok(DataType::Null)
        } else {
            Err(unsupported_data_type_exec_err(
                self.name(),
                "Numeric Type",
                &arg_types[1],
            ))
        }?;

        Ok(vec![expr_type, bit_length_type])
    }
}

pub fn sha2(args: [ColumnarValue; 2]) -> Result<ColumnarValue> {
    match args {
        [
            ColumnarValue::Scalar(ScalarValue::Utf8(expr_arg)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(bit_length_arg))),
        ] => compute_sha2(
            bit_length_arg,
            &[ColumnarValue::from(ScalarValue::Utf8(expr_arg))],
        ),
        [
            ColumnarValue::Array(expr_arg),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(bit_length_arg))),
        ] => compute_sha2(bit_length_arg, &[ColumnarValue::from(expr_arg)]),
        [
            ColumnarValue::Scalar(ScalarValue::Utf8(expr_arg)),
            ColumnarValue::Array(bit_length_arg),
        ] => {
            let arr: StringArray = bit_length_arg
                .as_primitive::<Int32Type>()
                .iter()
                .map(|bit_length| {
                    match sha2([
                        ColumnarValue::Scalar(ScalarValue::Utf8(expr_arg.clone())),
                        ColumnarValue::Scalar(ScalarValue::Int32(bit_length)),
                    ])
                    .unwrap()
                    {
                        ColumnarValue::Scalar(ScalarValue::Utf8(str)) => str,
                        ColumnarValue::Array(arr) => arr
                            .as_string::<i32>()
                            .iter()
                            .map(|str| str.unwrap().to_string())
                            .next(), // first element
                        _ => unreachable!(),
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(arr) as ArrayRef))
        }
        [
            ColumnarValue::Array(expr_arg),
            ColumnarValue::Array(bit_length_arg),
        ] => {
            let expr_iter = expr_arg.as_string::<i32>().iter();
            let bit_length_iter = bit_length_arg.as_primitive::<Int32Type>().iter();
            let arr: StringArray = expr_iter
                .zip(bit_length_iter)
                .map(|(expr, bit_length)| {
                    match sha2([
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                            expr.unwrap().to_string(),
                        ))),
                        ColumnarValue::Scalar(ScalarValue::Int32(bit_length)),
                    ])
                    .unwrap()
                    {
                        ColumnarValue::Scalar(ScalarValue::Utf8(str)) => str,
                        ColumnarValue::Array(arr) => arr
                            .as_string::<i32>()
                            .iter()
                            .map(|str| str.unwrap().to_string())
                            .next(), // first element
                        _ => unreachable!(),
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(arr) as ArrayRef))
        }
        _ => exec_err!("Unsupported argument types for sha2 function"),
    }
}

fn compute_sha2(
    bit_length_arg: i32,
    expr_arg: &[ColumnarValue],
) -> Result<ColumnarValue> {
    match bit_length_arg {
        0 | 256 => sha256(expr_arg),
        224 => sha224(expr_arg),
        384 => sha384(expr_arg),
        512 => sha512(expr_arg),
        _ => {
            // Return null for unsupported bit lengths instead of error, because spark sha2 does not
            // error out for this.
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
    }
    .map(|hashed| spark_sha2_hex(&[hashed]).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_sha2_nullability() -> Result<()> {
        let func = SparkSha2::new();

        let non_nullable_expr: FieldRef =
            Arc::new(Field::new("expr", DataType::Binary, false));
        let non_nullable_bit_length: FieldRef =
            Arc::new(Field::new("bit_length", DataType::Int32, false));

        let out = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[
                Arc::clone(&non_nullable_expr),
                Arc::clone(&non_nullable_bit_length),
            ],
            scalar_arguments: &[None, None],
        })?;
        assert!(!out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Utf8);

        let nullable_expr: FieldRef =
            Arc::new(Field::new("expr", DataType::Binary, true));
        let out = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[
                Arc::clone(&nullable_expr),
                Arc::clone(&non_nullable_bit_length),
            ],
            scalar_arguments: &[None, None],
        })?;
        assert!(out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Utf8);

        let nullable_bit_length: FieldRef =
            Arc::new(Field::new("bit_length", DataType::Int32, true));
        let out = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[
                Arc::clone(&non_nullable_expr),
                Arc::clone(&nullable_bit_length),
            ],
            scalar_arguments: &[None, None],
        })?;
        assert!(out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Utf8);

        let null_bit_length: FieldRef =
            Arc::new(Field::new("bit_length", DataType::Null, true));
        let out = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[non_nullable_expr, null_bit_length],
            scalar_arguments: &[None, None],
        })?;
        assert!(out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Null);

        let null_scalar = ScalarValue::Int32(None);
        let out = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[
                nullable_expr,
                Arc::new(Field::new("bit_length", DataType::Int32, false)),
            ],
            scalar_arguments: &[None, Some(&null_scalar)],
        })?;
        assert!(out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Utf8);

        Ok(())
    }
}
