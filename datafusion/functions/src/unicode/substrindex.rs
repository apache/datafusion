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

use arrow::array::{
    ArrayAccessor, ArrayIter, ArrayRef, ArrowPrimitiveType, AsArray, OffsetSizeTrait,
    PrimitiveArray, StringBuilder,
};
use arrow::datatypes::{DataType, Int32Type, Int64Type};

use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct SubstrIndexFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SubstrIndexFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SubstrIndexFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Utf8View, Int64]),
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("substring_index")],
        }
    }
}

impl ScalarUDFImpl for SubstrIndexFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "substr_index"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "substr_index")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(substr_index, vec![])(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Returns the substring from str before count occurrences of the delimiter delim. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
/// SUBSTRING_INDEX('www.apache.org', '.', 1) = www
/// SUBSTRING_INDEX('www.apache.org', '.', 2) = www.apache
/// SUBSTRING_INDEX('www.apache.org', '.', -2) = apache.org
/// SUBSTRING_INDEX('www.apache.org', '.', -1) = org
fn substr_index(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "substr_index was called with {} arguments. It requires 3.",
            args.len()
        );
    }

    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            let delimiter_array = args[1].as_string::<i32>();
            let count_array: &PrimitiveArray<Int64Type> = args[2].as_primitive();
            substr_index_general::<Int32Type, _, _>(
                string_array,
                delimiter_array,
                count_array,
            )
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            let delimiter_array = args[1].as_string::<i64>();
            let count_array: &PrimitiveArray<Int64Type> = args[2].as_primitive();
            substr_index_general::<Int64Type, _, _>(
                string_array,
                delimiter_array,
                count_array,
            )
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            let delimiter_array = args[1].as_string_view();
            let count_array: &PrimitiveArray<Int64Type> = args[2].as_primitive();
            substr_index_general::<Int32Type, _, _>(
                string_array,
                delimiter_array,
                count_array,
            )
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function substr_index")
        }
    }
}

pub fn substr_index_general<
    'a,
    T: ArrowPrimitiveType,
    V: ArrayAccessor<Item = &'a str>,
    P: ArrayAccessor<Item = i64>,
>(
    string_array: V,
    delimiter_array: V,
    count_array: P,
) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    let mut builder = StringBuilder::new();
    let string_iter = ArrayIter::new(string_array);
    let delimiter_array_iter = ArrayIter::new(delimiter_array);
    let count_array_iter = ArrayIter::new(count_array);
    string_iter
        .zip(delimiter_array_iter)
        .zip(count_array_iter)
        .for_each(|((string, delimiter), n)| match (string, delimiter, n) {
            (Some(string), Some(delimiter), Some(n)) => {
                // In MySQL, these cases will return an empty string.
                if n == 0 || string.is_empty() || delimiter.is_empty() {
                    builder.append_value("");
                    return;
                }

                let occurrences = usize::try_from(n.unsigned_abs()).unwrap_or(usize::MAX);
                let length = if n > 0 {
                    let split = string.split(delimiter);
                    split
                        .take(occurrences)
                        .map(|s| s.len() + delimiter.len())
                        .sum::<usize>()
                        - delimiter.len()
                } else {
                    let split = string.rsplit(delimiter);
                    split
                        .take(occurrences)
                        .map(|s| s.len() + delimiter.len())
                        .sum::<usize>()
                        - delimiter.len()
                };
                if n > 0 {
                    match string.get(..length) {
                        Some(substring) => builder.append_value(substring),
                        None => builder.append_null(),
                    }
                } else {
                    match string.get(string.len().saturating_sub(length)..) {
                        Some(substring) => builder.append_value(substring),
                        None => builder.append_null(),
                    }
                }
            }
            _ => builder.append_null(),
        });

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::substrindex::SubstrIndexFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SubstrIndexFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("www")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("www.apache")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(-2i64)),
            ],
            Ok(Some("apache.org")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(-1i64)),
            ],
            Ok(Some("org")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from("")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
