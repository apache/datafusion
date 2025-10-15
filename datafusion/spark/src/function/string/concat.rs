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
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::string::concat::ConcatFunc;
use std::any::Any;
use std::sync::Arc;

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

    // Handle zero-argument case: return empty string
    if arg_values.is_empty() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            Some(String::new()),
        )));
    }

    // Step 1: Check for NULL mask in incoming args
    let null_mask = compute_null_mask(&arg_values, number_rows)?;

    // If all scalars and any is NULL, return NULL immediately
    if null_mask.is_none() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
    }

    // Step 2: Delegate to DataFusion's concat
    let concat_func = ConcatFunc::new();
    let func_args = ScalarFunctionArgs {
        args: arg_values,
        arg_fields,
        number_rows,
        return_field,
        config_options,
    };
    let result = concat_func.invoke_with_args(func_args)?;

    // Step 3: Apply NULL mask to result
    apply_null_mask(result, null_mask)
}

/// Compute NULL mask for the arguments using NullBuffer::union
/// Returns None if all scalars and any is NULL, or an Option<NullBuffer>
/// representing the combined null mask for incoming arrays
fn compute_null_mask(
    args: &[ColumnarValue],
    number_rows: usize,
) -> Result<Option<Option<NullBuffer>>> {
    // Check if all arguments are scalars
    let all_scalars = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));

    if all_scalars {
        // For scalars, check if any is NULL
        for arg in args {
            if let ColumnarValue::Scalar(scalar) = arg {
                if scalar.is_null() {
                    // Return None to indicate all values should be NULL
                    return Ok(None);
                }
            }
        }
        // No NULLs in scalars
        Ok(Some(None))
    } else {
        // For arrays, compute NULL mask for each row using NullBuffer::union
        let array_len = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .unwrap_or(number_rows);

        // Convert all scalars to arrays for uniform processing
        let arrays: Result<Vec<_>> = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(array_len),
            })
            .collect();
        let arrays = arrays?;

        // Use NullBuffer::union to combine all null buffers
        let combined_nulls = arrays
            .iter()
            .map(|arr| arr.nulls())
            .fold(None, |acc, nulls| NullBuffer::union(acc.as_ref(), nulls));

        Ok(Some(combined_nulls))
    }
}

/// Apply NULL mask to the result using NullBuffer::union
fn apply_null_mask(
    result: ColumnarValue,
    null_mask: Option<Option<NullBuffer>>,
) -> Result<ColumnarValue> {
    match (result, null_mask) {
        // Scalar with NULL mask means return NULL
        (ColumnarValue::Scalar(_), None) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        // Scalar without NULL mask, return as-is
        (scalar @ ColumnarValue::Scalar(_), Some(None)) => Ok(scalar),
        // Array with NULL mask - use NullBuffer::union to combine nulls
        (ColumnarValue::Array(array), Some(Some(null_mask))) => {
            // Combine the result's existing nulls with our computed null mask
            let combined_nulls = NullBuffer::union(array.nulls(), Some(&null_mask));

            // Create new array with combined nulls
            let new_array = array
                .into_data()
                .into_builder()
                .nulls(combined_nulls)
                .build()?;

            Ok(ColumnarValue::Array(Arc::new(arrow::array::make_array(
                new_array,
            ))))
        }
        // Array without NULL mask, return as-is
        (array @ ColumnarValue::Array(_), _) => Ok(array),
        // Shouldn't happen
        (scalar, _) => Ok(scalar),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;
    use datafusion_common::Result;

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
