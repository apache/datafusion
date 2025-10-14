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

use arrow::array::Array;
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::string::concat::ConcatFunc;
use std::{any::Any, sync::Arc};

/// Spark-compatible `concat` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#concat>
///
/// Concatenates multiple input strings into a single string.
/// Returns NULL if any input is NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConcat {
    signature: Signature,
}

impl Default for SparkConcat {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcat {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::UserDefined, TypeSignature::Nullary],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkConcat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_concat(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Accept any string types, including zero arguments
        Ok(arg_types.to_vec())
    }
}

/// Concatenates strings, returning NULL if any input is NULL
/// This is a Spark-specific wrapper around DataFusion's concat that returns NULL
/// if any argument is NULL (Spark behavior), whereas DataFusion's concat ignores NULLs.
fn spark_concat(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let ScalarFunctionArgs {
        args: arg_values,
        arg_fields,
        number_rows,
        return_field,
        config_options,
    } = args;

    if arg_values.is_empty() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            Some(String::new()),
        )));
    }

    // Check if all arguments are scalars
    let all_scalars = arg_values
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));

    if all_scalars {
        // For scalars, check if any is NULL
        for arg in &arg_values {
            if let ColumnarValue::Scalar(scalar) = arg {
                if scalar.is_null() {
                    // Return NULL if any argument is NULL (Spark behavior)
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
            }
        }
        // No NULLs found, delegate to DataFusion's concat
        let concat_func = ConcatFunc::new();
        let func_args = ScalarFunctionArgs {
            args: arg_values,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        };
        concat_func.invoke_with_args(func_args)
    } else {
        // For arrays, we need to check each row for NULLs and return NULL for that row
        // Get array length
        let array_len = arg_values
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .unwrap_or(number_rows);

        // Convert all scalars to arrays
        let arrays: Result<Vec<_>> = arg_values
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(array_len),
            })
            .collect();
        let arrays = arrays?;

        // Check for NULL values in each row
        let mut null_mask = vec![false; array_len];
        for array in &arrays {
            for (i, null_flag) in null_mask.iter_mut().enumerate().take(array_len) {
                if array.is_null(i) {
                    *null_flag = true;
                }
            }
        }

        // Delegate to DataFusion's concat
        let concat_func = ConcatFunc::new();
        let func_args = ScalarFunctionArgs {
            args: arg_values,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        };

        let result = concat_func.invoke_with_args(func_args)?;

        // Apply NULL mask to the result
        match result {
            ColumnarValue::Array(array) => {
                let return_type = array.data_type();
                let mut builder: Box<dyn arrow::array::ArrayBuilder> = match return_type {
                    DataType::Utf8 => {
                        let string_array = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .unwrap();
                        let mut builder =
                            arrow::array::StringBuilder::with_capacity(array_len, 0);
                        for (i, &is_null) in null_mask.iter().enumerate().take(array_len)
                        {
                            if is_null || string_array.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(string_array.value(i));
                            }
                        }
                        Box::new(builder)
                    }
                    DataType::LargeUtf8 => {
                        let string_array = array
                            .as_any()
                            .downcast_ref::<arrow::array::LargeStringArray>()
                            .unwrap();
                        let mut builder =
                            arrow::array::LargeStringBuilder::with_capacity(array_len, 0);
                        for (i, &is_null) in null_mask.iter().enumerate().take(array_len)
                        {
                            if is_null || string_array.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(string_array.value(i));
                            }
                        }
                        Box::new(builder)
                    }
                    DataType::Utf8View => {
                        let string_array = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringViewArray>()
                            .unwrap();
                        let mut builder =
                            arrow::array::StringViewBuilder::with_capacity(array_len);
                        for (i, &is_null) in null_mask.iter().enumerate().take(array_len)
                        {
                            if is_null || string_array.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(string_array.value(i));
                            }
                        }
                        Box::new(builder)
                    }
                    _ => {
                        return datafusion_common::exec_err!(
                            "Unsupported return type for concat: {:?}",
                            return_type
                        );
                    }
                };

                Ok(ColumnarValue::Array(builder.finish()))
            }
            other => Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;
    use datafusion_common::Result;
    use datafusion_expr::ColumnarValue;

    #[test]
    fn test_concat_basic() -> Result<()> {
        test_scalar_function!(
            SparkConcat::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("Spark".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("SQL".to_string()))),
            ],
            Ok(Some("SparkSQL")),
            &str,
            DataType::Utf8,
            StringArray
        );
        Ok(())
    }

    #[test]
    fn test_concat_with_null() -> Result<()> {
        test_scalar_function!(
            SparkConcat::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("Spark".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("SQL".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            DataType::Utf8,
            StringArray
        );
        Ok(())
    }
}
