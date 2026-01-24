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

use std::any::Any;
use std::sync::Arc;

use crate::utils::utf8_to_str_type;
use arrow::array::{
    ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringArrayType, StringViewArray,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{LargeUtf8, Utf8, Utf8View};
use datafusion_common::cast::as_int64_array;
use datafusion_common::types::{NativeType, logical_int64, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{ColumnarValue, Documentation, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns a string with an input string repeated a specified number.",
    syntax_example = "repeat(str, n)",
    sql_example = r#"```sql
> select repeat('data', 3);
+-------------------------------+
| repeat(Utf8("data"),Int64(3)) |
+-------------------------------+
| datadatadata                  |
+-------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "n",
        description = "Number of times to repeat the input string."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RepeatFunc {
    signature: Signature,
}

impl Default for RepeatFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RepeatFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    // Accept all integer types but cast them to i64
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int64,
                    ),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RepeatFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "repeat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "repeat")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [string_arg, count_arg] = take_function_args(self.name(), args.args)?;

        match (&string_arg, &count_arg) {
            (
                ColumnarValue::Scalar(string_scalar),
                ColumnarValue::Scalar(count_scalar),
            ) => {
                if string_scalar.is_null() || count_scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }

                let count = match count_scalar {
                    ScalarValue::Int64(Some(n)) => *n,
                    _ => {
                        return internal_err!(
                            "Unexpected data type {:?} for repeat count",
                            count_scalar.data_type()
                        );
                    }
                };

                let repeated = match string_scalar {
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)) => {
                        if count <= 0 {
                            String::new()
                        } else {
                            let result_len = s.len().saturating_mul(count as usize);
                            if result_len > i32::MAX as usize {
                                return exec_err!(
                                    "string size overflow on repeat, max size is {}, but got {}",
                                    i32::MAX,
                                    result_len
                                );
                            }
                            s.repeat(count as usize)
                        }
                    }
                    _ => {
                        return internal_err!(
                            "Unexpected data type {:?} for function repeat",
                            string_scalar.data_type()
                        );
                    }
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(repeated))))
            }
            _ => {
                let string_array = string_arg.to_array(args.number_rows)?;
                let count_array = count_arg.to_array(args.number_rows)?;
                Ok(ColumnarValue::Array(repeat(&[string_array, count_array])?))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Repeats string the specified number of times.
/// repeat('Pg', 4) = 'PgPgPgPg'
fn repeat(args: &[ArrayRef]) -> Result<ArrayRef> {
    let number_array = as_int64_array(&args[1])?;
    match args[0].data_type() {
        Utf8View => {
            let string_view_array = args[0].as_string_view();
            repeat_impl::<i32, &StringViewArray>(
                &string_view_array,
                number_array,
                i32::MAX as usize,
            )
        }
        Utf8 => {
            let string_array = args[0].as_string::<i32>();
            repeat_impl::<i32, &GenericStringArray<i32>>(
                &string_array,
                number_array,
                i32::MAX as usize,
            )
        }
        LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            repeat_impl::<i64, &GenericStringArray<i64>>(
                &string_array,
                number_array,
                i64::MAX as usize,
            )
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function repeat. \
        Expected Utf8, Utf8View or LargeUtf8."
        ),
    }
}

fn repeat_impl<'a, T, S>(
    string_array: &S,
    number_array: &Int64Array,
    max_str_len: usize,
) -> Result<ArrayRef>
where
    T: OffsetSizeTrait,
    S: StringArrayType<'a> + 'a,
{
    use arrow::array::Array;
    let mut total_capacity = 0;
    let mut max_item_capacity = 0;
    string_array.iter().zip(number_array.iter()).try_for_each(
        |(string, number)| -> Result<(), DataFusionError> {
            match (string, number) {
                (Some(string), Some(number)) if number >= 0 => {
                    let item_capacity = string.len() * number as usize;
                    if item_capacity > max_str_len {
                        return exec_err!(
                            "string size overflow on repeat, max size is {}, but got {}",
                            max_str_len,
                            number as usize * string.len()
                        );
                    }
                    total_capacity += item_capacity;
                    max_item_capacity = max_item_capacity.max(item_capacity);
                }
                _ => (),
            }
            Ok(())
        },
    )?;

    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), total_capacity);

    // Reusable buffer to avoid allocations in string.repeat()
    let mut buffer = Vec::<u8>::with_capacity(max_item_capacity);

    // Helper function to repeat a string into a buffer using doubling strategy
    #[inline]
    fn repeat_to_buffer(buffer: &mut Vec<u8>, string: &str, count: i64) {
        buffer.clear();
        if count > 0 && !string.is_empty() {
            let count = count as usize;
            let src = string.as_bytes();
            buffer.extend_from_slice(src);
            while buffer.len() < src.len() * count {
                let copy_len = buffer.len().min(src.len() * count - buffer.len());
                buffer.extend_from_within(..copy_len);
            }
        }
    }

    // no nulls in either array
    if string_array.null_count() == 0 && number_array.null_count() == 0 {
        for i in 0..string_array.len() {
            // SAFETY: null_count() == 0 guarantees no nulls
            let string = unsafe { string_array.value_unchecked(i) };
            let count = number_array.value(i);
            if count >= 0 {
                repeat_to_buffer(&mut buffer, string, count);
                // SAFETY: buffer contains valid UTF-8 since we only copy from a valid &str
                builder.append_value(unsafe { std::str::from_utf8_unchecked(&buffer) });
            } else {
                builder.append_value("");
            }
        }
    } else {
        //  handle nulls
        for (string, number) in string_array.iter().zip(number_array.iter()) {
            match (string, number) {
                (Some(string), Some(number)) if number >= 0 => {
                    repeat_to_buffer(&mut buffer, string, number);
                    // SAFETY: buffer contains valid UTF-8 since we only copy from a valid &str
                    builder
                        .append_value(unsafe { std::str::from_utf8_unchecked(&buffer) });
                }
                (Some(_), Some(_)) => builder.append_value(""),
                _ => builder.append_null(),
            }
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::ScalarValue;
    use datafusion_common::{Result, exec_err};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::repeat::RepeatFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(Some("PgPgPgPg")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(Some("PgPgPgPg")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1073741824))),
            ],
            exec_err!(
                "string size overflow on repeat, max size is {}, but got {}",
                i32::MAX,
                2usize * 1073741824
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
