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

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArrayType, PrimitiveArray, StringArrayType,
};
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type};
use datafusion_common::exec_err;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use std::sync::Arc;

/// Spark-compatible `length` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#length>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkLengthFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkLengthFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkLengthFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Utf8View,
                    DataType::Utf8,
                    DataType::LargeUtf8,
                    DataType::Binary,
                    DataType::LargeBinary,
                    DataType::BinaryView,
                ],
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("character_length"),
                String::from("char_length"),
                String::from("len"),
            ],
        }
    }
}

impl ScalarUDFImpl for SparkLengthFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> datafusion_common::Result<DataType> {
        datafusion_common::internal_err!(
            "return_type should not be called, use return_field_from_args instead"
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function(spark_length, vec![])(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion_common::Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        // spark length always returns Int32
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, nullable)))
    }
}

fn spark_length(args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            character_length::<_>(&string_array)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            character_length::<_>(&string_array)
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            character_length::<_>(&string_array)
        }
        DataType::Binary => {
            let binary_array = args[0].as_binary::<i32>();
            byte_length::<_>(&binary_array)
        }
        DataType::LargeBinary => {
            let binary_array = args[0].as_binary::<i64>();
            byte_length::<_>(&binary_array)
        }
        DataType::BinaryView => {
            let binary_array = args[0].as_binary_view();
            byte_length::<_>(&binary_array)
        }
        other => exec_err!("Unsupported data type {other:?} for function `length`"),
    }
}

fn character_length<'a, V>(array: &V) -> datafusion_common::Result<ArrayRef>
where
    V: StringArrayType<'a>,
{
    // String characters are variable length encoded in UTF-8, counting the
    // number of chars requires expensive decoding, however checking if the
    // string is ASCII only is relatively cheap.
    // If strings are ASCII only, count bytes instead.
    let is_array_ascii_only = array.is_ascii();
    let nulls = array.nulls().cloned();
    let array = {
        if is_array_ascii_only {
            let values: Vec<_> = (0..array.len())
                .map(|i| {
                    // Safety: we are iterating with array.len() so the index is always valid
                    let value = unsafe { array.value_unchecked(i) };
                    value.len() as i32
                })
                .collect();
            PrimitiveArray::<Int32Type>::new(values.into(), nulls)
        } else {
            let values: Vec<_> = (0..array.len())
                .map(|i| {
                    // Safety: we are iterating with array.len() so the index is always valid
                    if array.is_null(i) {
                        i32::default()
                    } else {
                        let value = unsafe { array.value_unchecked(i) };
                        if value.is_empty() {
                            i32::default()
                        } else if value.is_ascii() {
                            value.len() as i32
                        } else {
                            value.chars().count() as i32
                        }
                    }
                })
                .collect();
            PrimitiveArray::<Int32Type>::new(values.into(), nulls)
        }
    };

    Ok(Arc::new(array))
}

fn byte_length<'a, V>(array: &V) -> datafusion_common::Result<ArrayRef>
where
    V: BinaryArrayType<'a>,
{
    let nulls = array.nulls().cloned();
    let values: Vec<_> = (0..array.len())
        .map(|i| {
            // Safety: we are iterating with array.len() so the index is always valid
            let value = unsafe { array.value_unchecked(i) };
            value.len() as i32
        })
        .collect();
    Ok(Arc::new(PrimitiveArray::<Int32Type>::new(
        values.into(),
        nulls,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType::Int32;
    use arrow::datatypes::{Field, FieldRef};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarUDFImpl};

    macro_rules! test_spark_length_string {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_scalar_function!(
                SparkLengthFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_scalar_function!(
                SparkLengthFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_scalar_function!(
                SparkLengthFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );
        };
    }

    macro_rules! test_spark_length_binary {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_scalar_function!(
                SparkLengthFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Binary($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_scalar_function!(
                SparkLengthFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeBinary($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_scalar_function!(
                SparkLengthFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::BinaryView($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_spark_length_string!(Some(String::from("chars")), Ok(Some(5)));
        test_spark_length_string!(Some(String::from("josé")), Ok(Some(4)));
        // test long strings (more than 12 bytes for StringView)
        test_spark_length_string!(Some(String::from("joséjoséjoséjosé")), Ok(Some(16)));
        test_spark_length_string!(Some(String::from("")), Ok(Some(0)));
        test_spark_length_string!(None, Ok(None));

        test_spark_length_binary!(Some(String::from("chars").into_bytes()), Ok(Some(5)));
        test_spark_length_binary!(Some(String::from("josé").into_bytes()), Ok(Some(5)));
        // test long strings (more than 12 bytes for BinaryView)
        test_spark_length_binary!(
            Some(String::from("joséjoséjoséjosé").into_bytes()),
            Ok(Some(20))
        );
        test_spark_length_binary!(Some(String::from("").into_bytes()), Ok(Some(0)));
        test_spark_length_binary!(None, Ok(None));

        Ok(())
    }

    #[test]
    fn test_spark_length_nullability() -> Result<()> {
        let func = SparkLengthFunc::new();

        let nullable_field: FieldRef = Arc::new(Field::new("col", DataType::Utf8, true));

        let out_nullable = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[nullable_field],
            scalar_arguments: &[None],
        })?;

        assert!(
            out_nullable.is_nullable(),
            "length(col) should be nullable when child is nullable"
        );

        let non_nullable_field: FieldRef =
            Arc::new(Field::new("col", DataType::Utf8, false));

        let out_non_nullable = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[non_nullable_field],
            scalar_arguments: &[None],
        })?;

        assert!(
            !out_non_nullable.is_nullable(),
            "length(col) should NOT be nullable when child is NOT nullable"
        );

        Ok(())
    }
}
