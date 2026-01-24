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

use arrow::datatypes::{DataType, Field};
use datafusion_common::arrow::datatypes::FieldRef;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ReturnFieldArgs;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::string::concat::ConcatFunc;
use std::any::Any;
use std::sync::Arc;

use crate::function::null_utils::{
    NullMaskResolution, apply_null_mask, compute_null_mask,
};

/// Spark-compatible `concat` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#concat>
///
/// Concatenates multiple input strings into a single string.
/// Returns NULL if any input is NULL.
///
/// Differences with DataFusion concat:
/// - Support 0 arguments
/// - Return NULL if any input is NULL
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_concat(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Accept any string types, including zero arguments
        Ok(arg_types.to_vec())
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion_common::internal_err!(
            "return_type should not be called for Spark concat"
        )
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        // Spark semantics: concat returns NULL if ANY input is NULL
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new("concat", DataType::Utf8, nullable)))
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
    if matches!(null_mask, NullMaskResolution::ReturnNull) {
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
    }

    // Step 2: Delegate to DataFusion's concat
    let concat_func = ConcatFunc::new();
    let return_type = return_field.data_type().clone();
    let func_args = ScalarFunctionArgs {
        args: arg_values,
        arg_fields,
        number_rows,
        return_field,
        config_options,
    };
    let result = concat_func.invoke_with_args(func_args)?;

    // Step 3: Apply NULL mask to result
    apply_null_mask(result, null_mask, &return_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::Result;
    use datafusion_expr::ReturnFieldArgs;
    use std::sync::Arc;

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
    #[test]
    fn test_spark_concat_return_field_non_nullable() -> Result<()> {
        let func = SparkConcat::new();

        let fields = vec![
            Arc::new(Field::new("a", DataType::Utf8, false)),
            Arc::new(Field::new("b", DataType::Utf8, false)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &[],
        };

        let field = func.return_field_from_args(args)?;

        assert!(
            !field.is_nullable(),
            "Expected concat result to be non-nullable when all inputs are non-nullable"
        );

        Ok(())
    }
    #[test]
    fn test_spark_concat_return_field_nullable() -> Result<()> {
        let func = SparkConcat::new();

        let fields = vec![
            Arc::new(Field::new("a", DataType::Utf8, false)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &[],
        };

        let field = func.return_field_from_args(args)?;

        assert!(
            field.is_nullable(),
            "Expected concat result to be nullable when any input is nullable"
        );

        Ok(())
    }
}
