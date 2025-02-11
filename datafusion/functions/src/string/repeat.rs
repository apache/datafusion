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
use std::sync::{Arc, OnceLock};

use crate::strings::StringArrayType;
use crate::utils::{make_scalar_function, utf8_to_str_type};
use arrow::array::{
    ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringViewArray,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Int64, LargeUtf8, Utf8, Utf8View};
use datafusion_common::cast::as_int64_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

#[derive(Debug)]
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
            signature: Signature::one_of(
                vec![
                    // Planner attempts coercion to the target type starting with the most preferred candidate.
                    // For example, given input `(Utf8View, Int64)`, it first tries coercing to `(Utf8View, Int64)`.
                    // If that fails, it proceeds to `(Utf8, Int64)`.
                    TypeSignature::Exact(vec![Utf8View, Int64]),
                    TypeSignature::Exact(vec![Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, Int64]),
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(repeat, vec![])(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_repeat_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_repeat_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description(
                "Returns a string with an input string repeated a specified number.",
            )
            .with_syntax_example("repeat(str, n)")
            .with_sql_example(
                r#"```sql
> select repeat('data', 3);
+-------------------------------+
| repeat(Utf8("data"),Int64(3)) |
+-------------------------------+
| datadatadata                  |
+-------------------------------+
```"#,
            )
            .with_standard_argument("str", Some("String"))
            .with_argument("n", "Number of times to repeat the input string.")
            .build()
            .unwrap()
    })
}

/// Repeats string the specified number of times.
/// repeat('Pg', 4) = 'PgPgPgPg'
fn repeat(args: &[ArrayRef]) -> Result<ArrayRef> {
    let number_array = as_int64_array(&args[1])?;
    match args[0].data_type() {
        Utf8View => {
            let string_view_array = args[0].as_string_view();
            repeat_impl::<i32, &StringViewArray>(string_view_array, number_array)
        }
        Utf8 => {
            let string_array = args[0].as_string::<i32>();
            repeat_impl::<i32, &GenericStringArray<i32>>(string_array, number_array)
        }
        LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            repeat_impl::<i64, &GenericStringArray<i64>>(string_array, number_array)
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function repeat. \
        Expected Utf8, Utf8View or LargeUtf8."
        ),
    }
}

fn repeat_impl<'a, T, S>(string_array: S, number_array: &Int64Array) -> Result<ArrayRef>
where
    T: OffsetSizeTrait,
    S: StringArrayType<'a>,
{
    let mut builder: GenericStringBuilder<T> = GenericStringBuilder::new();
    string_array
        .iter()
        .zip(number_array.iter())
        .for_each(|(string, number)| match (string, number) {
            (Some(string), Some(number)) if number >= 0 => {
                builder.append_value(string.repeat(number as usize))
            }
            (Some(_), Some(_)) => builder.append_value(""),
            _ => builder.append_null(),
        });
    let array = builder.finish();

    Ok(Arc::new(array) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::repeat::RepeatFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            RepeatFunc::new(),
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
