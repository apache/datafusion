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

use arrow::array::ArrayRef;
use arrow::array::GenericStringBuilder;
use arrow::datatypes::DataType::Int64;
use arrow::datatypes::DataType::Utf8;
use arrow::datatypes::{DataType, Field, FieldRef};
use std::{any::Any, sync::Arc};

use datafusion_common::{Result, ScalarValue, cast::as_int64_array, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// Spark-compatible `char` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#char>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CharFunc {
    signature: Signature,
}

impl Default for CharFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CharFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CharFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion_common::internal_err!(
            "return_type should not be called, use return_field_from_args instead"
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_chr(&args.args)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), Utf8, nullable)))
    }
}

/// Returns the ASCII character having the binary equivalent to the input expression.
/// E.g., chr(65) = 'A'.
/// Compatible with Apache Spark's Chr function
fn spark_chr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = args[0].clone();
    match array {
        ColumnarValue::Array(array) => {
            let array = chr(&[array])?;
            Ok(ColumnarValue::Array(array))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => {
            if value < 0 {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "".to_string(),
                ))))
            } else {
                match core::char::from_u32((value % 256) as u32) {
                    Some(ch) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        ch.to_string(),
                    )))),
                    None => {
                        exec_err!("requested character was incompatible for encoding.")
                    }
                }
            }
        }
        _ => exec_err!("The argument must be an Int64 array or scalar."),
    }
}

fn chr(args: &[ArrayRef]) -> Result<ArrayRef> {
    let integer_array = as_int64_array(&args[0])?;

    let mut builder = GenericStringBuilder::<i32>::with_capacity(
        integer_array.len(),
        integer_array.len(),
    );

    for integer_opt in integer_array {
        match integer_opt {
            Some(integer) => {
                if integer < 0 {
                    builder.append_value(""); // empty string for negative numbers.
                } else {
                    match core::char::from_u32((integer % 256) as u32) {
                        Some(ch) => builder.append_value(ch.to_string()),
                        None => {
                            return exec_err!(
                                "requested character not compatible for encoding."
                            );
                        }
                    }
                }
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[test]
fn test_char_nullability() -> Result<()> {
    use arrow::datatypes::{DataType::Utf8, Field, FieldRef};
    use datafusion_expr::ReturnFieldArgs;
    use std::sync::Arc;

    let func = CharFunc::new();

    let nullable_field: FieldRef = Arc::new(Field::new("col", Int64, true));

    let out_nullable = func.return_field_from_args(ReturnFieldArgs {
        arg_fields: &[nullable_field],
        scalar_arguments: &[None],
    })?;

    assert!(
        out_nullable.is_nullable(),
        "char(col) should be nullable when input column is nullable"
    );
    assert_eq!(
        out_nullable.data_type(),
        &Utf8,
        "char always returns Utf8 regardless of input type"
    );

    let non_nullable_field: FieldRef = Arc::new(Field::new("col", Int64, false));

    let out_non_nullable = func.return_field_from_args(ReturnFieldArgs {
        arg_fields: &[non_nullable_field],
        scalar_arguments: &[None],
    })?;

    assert!(
        !out_non_nullable.is_nullable(),
        "char(col) should NOT be nullable when input column is NOT nullable"
    );
    assert_eq!(
        out_non_nullable.data_type(),
        &Utf8,
        "char always returns Utf8 regardless of input type"
    );

    Ok(())
}
