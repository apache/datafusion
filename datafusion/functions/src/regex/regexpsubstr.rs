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

//! Regex expressions
use arrow::array::{
    Array, ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, OffsetSizeTrait,
};
use arrow::datatypes::{DataType, Int64Type};
use arrow::error::ArrowError;
use datafusion_common::plan_err;
use datafusion_common::ScalarValue;
use datafusion_common::{
    cast::as_generic_string_array, internal_err, DataFusionError, Result,
};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_REGEX;
use datafusion_expr::{ColumnarValue, Documentation, ScalarFunctionArgs, TypeSignature};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use regex::Regex;
use std::any::Any;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct RegexpSubstrFunc {
    signature: Signature,
}

impl Default for RegexpSubstrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpSubstrFunc {
    pub fn new() -> Self {
        use DataType::{Int64, LargeUtf8, Utf8};
        Self {
            signature: Signature::one_of(
                vec![
                    // Planner attempts coercion to the target type starting with the most preferred candidate.
                    // For example, given input `(Utf8View, Utf8)`, it first tries coercing to `(Utf8, Utf8)`.
                    // If that fails, it proceeds to `(LargeUtf8, Utf8)`.
                    TypeSignature::Exact(vec![Utf8, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64, Int64]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64, Int64, Utf8]),
                    TypeSignature::Exact(vec![
                        LargeUtf8, LargeUtf8, Int64, Int64, LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64, Int64, Utf8, Int64]),
                    TypeSignature::Exact(vec![
                        LargeUtf8, LargeUtf8, Int64, Int64, LargeUtf8, Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpSubstrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let len = args
            .args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let inferred_length = len.unwrap_or(1);
        let args = args
            .args
            .iter()
            .map(|arg| arg.to_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = regexp_subst_func(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_regexp_substr_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_regexp_substr_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_REGEX,
            "Returns the substring that matches a [regular expression](https://docs.rs/regex/latest/regex/#syntax) within a string.",
            "regexp_substr(str, regexp[, position[, occurrence[, flags[, group_num]]]])")
            .with_sql_example(r#"```sql
            > select regexp_substr('Köln', '[a-zA-Z]ö[a-zA-Z]{2}');
            +---------------------------------------------------------+
            | regexp_substr(Utf8("Köln"),Utf8("[a-zA-Z]ö[a-zA-Z]{2}")) |
            +---------------------------------------------------------+
            | Köln                                                    |
            +---------------------------------------------------------+
            SELECT regexp_substr('aBc', '(b|d)', 1, 1, 'i');
            +---------------------------------------------------+
            | regexp_substr(Utf8("aBc"),Utf8("(b|d)"), Int32(1), Int32(1), Utf8("i")) |
            +---------------------------------------------------+
            | B                                                 |
            +---------------------------------------------------+
```
Additional examples can be found [here](https://docs.snowflake.com/en/sql-reference/functions/regexp_substr#examples)
"#)
            .with_standard_argument("str", Some("String"))
            .with_argument("regexp", "Regular expression to match against.
            Can be a constant, column, or function.")
            .with_argument("position", "Number of characters from the beginning of the string where the function starts searching for matches. Default: 1")
            .with_argument("occurrence", "Specifies the first occurrence of the pattern from which to start returning matches.. Default: 1")
            .with_argument("flags",
                           r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **c**: case-sensitive: letters match upper or lower case. Default flag
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **e**: extract submatches (for Snowflake compatibility)
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?"#)
            .with_argument("group_num", "Specifies which group to extract. Groups are specified by using parentheses in the regular expression.")
            .build()
    })
}

fn regexp_subst_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => regexp_substr::<i32>(args),
        DataType::LargeUtf8 => regexp_substr::<i64>(args),
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_substr")
        }
    }
}
pub fn regexp_substr<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args_len = args.len();
    let get_int_arg = |index: usize, name: &str| -> Result<Option<i64>> {
        if args_len > index {
            let arg = args[index].as_primitive::<Int64Type>();
            if arg.is_empty() {
                return plan_err!(
                    "regexp_substr() requires the {:?} argument to be an integer",
                    name
                );
            }
            Ok(Some(arg.value(0)))
        } else {
            Ok(None)
        }
    };

    let values = as_generic_string_array::<T>(&args[0])?;
    let regex = Some(as_generic_string_array::<T>(&args[1])?.value(0));
    let start = get_int_arg(2, "position")?;
    let occurrence = get_int_arg(3, "occurrence")?;
    let flags = if args_len > 4 {
        let flags = args[4].as_string::<T>();
        if flags.iter().any(|s| s == Some("g")) {
            return plan_err!("regexp_substr() does not support the \"global\" option");
        }
        Some(flags.value(0))
    } else {
        None
    };

    let group_num = get_int_arg(5, "group_num")?;

    let result =
        regexp_substr_inner::<T>(values, regex, start, occurrence, flags, group_num)?;
    Ok(Arc::new(result))
}

fn regexp_substr_inner<T: OffsetSizeTrait>(
    values: &GenericStringArray<T>,
    regex: Option<&str>,
    start: Option<i64>,
    occurrence: Option<i64>,
    flags: Option<&str>,
    group_num: Option<i64>,
) -> Result<ArrayRef> {
    let regex = match regex {
        None | Some("") => {
            return Ok(Arc::new(GenericStringArray::<T>::new_null(values.len())))
        }
        Some(regex) => regex,
    };
    let regex = compile_regex(regex, flags)?;
    let mut builder = GenericStringBuilder::<T>::new();

    values.iter().try_for_each(|value| {
        match value {
            Some(value) => {
                // Skip characters from the beginning
                let cleaned_value = if let Some(start) = start {
                    if start < 1 {
                        return Err(DataFusionError::from(ArrowError::ComputeError(
                            "regexp_count() requires start to be 1 based".to_string(),
                        )));
                    }
                    value.chars().skip(start as usize - 1).collect()
                } else {
                    value.to_string()
                };

                let matches =
                    get_matches(cleaned_value.as_str(), &regex, occurrence, group_num);

                if matches.is_empty() {
                    builder.append_null();
                } else {
                    // Return only first substring that matches the pattern
                    if let Some(first_match) = matches.first() {
                        builder.append_value(first_match);
                    }
                }
            }
            _ => builder.append_null(),
        }
        Ok(())
    })?;
    Ok(Arc::new(builder.finish()))
}

fn get_matches(
    value: &str,
    regex: &Regex,
    occurrence: Option<i64>,
    group_num: Option<i64>,
) -> Vec<String> {
    let mut matches = Vec::new();
    let occurrence = occurrence.unwrap_or(1) as usize;

    for caps in regex.captures_iter(value) {
        match group_num {
            Some(group_num) => {
                if let Some(m) = caps.get(group_num as usize) {
                    matches.push(m.as_str().to_string());
                }
            }
            None => {
                let mut iter = caps.iter();
                if caps.len() > 1 {
                    iter.next();
                }
                for m in iter.flatten() {
                    matches.push(m.as_str().to_string());
                }
            }
        }
    }

    if matches.len() > occurrence {
        matches = matches.split_off(occurrence - 1);
    }
    matches
}
fn compile_regex(regex: &str, flags: Option<&str>) -> Result<Regex, ArrowError> {
    let pattern = match flags {
        None | Some("") => regex.to_string(),
        Some(flags) => {
            if flags.contains("g") {
                return Err(ArrowError::ComputeError(
                    "regexp_substr() does not support global flag".to_string(),
                ));
            }
            // Case-sensitive enabled by default
            let flags = flags.replace("c", "");
            format!("(?{}){}", flags, regex)
        }
    };

    Regex::new(&pattern).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Regular expression did not compile: {}",
            pattern
        ))
    })
}

#[cfg(test)]
mod tests {
    use crate::regex::regexpsubstr::{regexp_substr, RegexpSubstrFunc};
    use arrow::array::{Array, ArrayRef, Int64Array, LargeStringArray, StringArray};
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_regexp_substr() {
        let values = [
            "Hellooo Woorld",
            "How are you doing today floor?",
            "the quick brown fox jumps over the lazy dog door",
            "PACK MY BOX WITH FIVE DOZEN LIQUOR JUGS",
        ];
        let regex = ["\\b\\S*o\\S*\\b", "(..or)"];
        let expected = [
            ["Hellooo", "How", "brown", ""],
            ["Woor", "loor", "door", ""],
        ];

        // Scalar
        values.iter().enumerate().for_each(|(pos, &value)| {
            regex.iter().enumerate().for_each(|(rpos, regex)| {
                let expected = expected.get(rpos).unwrap().get(pos).unwrap().to_string();

                // Utf8, LargeUtf8
                for (data_type, scalar) in &[
                    (
                        DataType::Utf8,
                        ScalarValue::Utf8 as fn(Option<String>) -> ScalarValue,
                    ),
                    (
                        DataType::LargeUtf8,
                        ScalarValue::LargeUtf8 as fn(Option<String>) -> ScalarValue,
                    ),
                ] {
                    let result =
                        RegexpSubstrFunc::new().invoke_with_args(ScalarFunctionArgs {
                            args: vec![
                                ColumnarValue::Scalar(scalar(Some(value.to_string()))),
                                ColumnarValue::Scalar(scalar(Some(regex.to_string()))),
                            ],
                            number_rows: 1,
                            return_type: data_type,
                        });
                    match result {
                        Ok(ColumnarValue::Scalar(
                               ScalarValue::Utf8(ref res) | ScalarValue::LargeUtf8(ref res),
                           )) => {
                            if res.is_some() {
                                assert_eq!(
                                    res.as_ref().unwrap(),
                                    &expected.to_string(),
                                    "regexp_substr scalar test failed"
                                );
                            } else {
                                assert_eq!(
                                    "", expected,
                                    "regexp_substr scalar utf8 test failed"
                                )
                            }
                        }
                        _ => panic!("Unexpected result"),
                    }
                }
            });
        });

        // Array (column)
        regex.iter().enumerate().for_each(|(rpos, regex)| {
            // Utf8, LargeUtf8
            for data_type in &[DataType::Utf8, DataType::LargeUtf8] {
                let (array_values, regex) = match data_type {
                    DataType::Utf8 => (
                        Arc::new(StringArray::from(
                            values.iter().map(|v| v.to_string()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        ScalarValue::Utf8(Some(regex.to_string())),
                    ),
                    DataType::LargeUtf8 => (
                        Arc::new(LargeStringArray::from(
                            values.iter().map(|v| v.to_string()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        ScalarValue::LargeUtf8(Some(regex.to_string())),
                    ),
                    _ => unreachable!(),
                };
                let result =
                    RegexpSubstrFunc::new().invoke_with_args(ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Array(Arc::new(array_values)),
                            ColumnarValue::Scalar(regex),
                        ],
                        number_rows: 1,
                        return_type: data_type,
                    });
                match result {
                    Ok(ColumnarValue::Array(array)) => {
                        let expected = expected
                            .get(rpos)
                            .unwrap()
                            .iter()
                            .map(|v| {
                                if v.is_empty() {
                                    return None;
                                }
                                Some(v.to_string())
                            })
                            .collect::<Vec<Option<String>>>();

                        assert_eq!(array.data_type(), data_type, "wrong array datatype");
                        match data_type {
                            DataType::Utf8 => {
                                let array =
                                    array.as_any().downcast_ref::<StringArray>().unwrap();
                                let expected = StringArray::from(expected);
                                assert_eq!(
                                    array, &expected,
                                    "regexp_substr array Utf8 test failed"
                                );
                            }
                            DataType::LargeUtf8 => {
                                let array = array
                                    .as_any()
                                    .downcast_ref::<LargeStringArray>()
                                    .unwrap();
                                let expected = LargeStringArray::from(expected);
                                assert_eq!(
                                    array, &expected,
                                    "regexp_substr array LargeUtf8 test failed"
                                );
                            }
                            _ => unreachable!(),
                        };
                    }
                    _ => panic!("Unexpected result"),
                }
            }
        });
    }

    #[test]
    fn test_regexp_substr_with_params() {
        let values = [
            "",
            "aabca aabca",
            "abc abc",
            "Abcab abc",
            "abCab cabc",
            "ab",
        ];
        let regex = "abc";
        let position = 1;
        let occurrence = 1;
        let flags = "i";
        let group_num = 0;
        let expected = ["", "abc", "abc", "Abc", "abC", ""];

        // Scalar
        values.iter().enumerate().for_each(|(pos, &value)| {
            let expected = expected.get(pos).cloned().unwrap();
            // Utf8, LargeUtf8
            for (data_type, scalar) in &[
                (
                    DataType::Utf8,
                    ScalarValue::Utf8 as fn(Option<String>) -> ScalarValue,
                ),
                (
                    DataType::LargeUtf8,
                    ScalarValue::LargeUtf8 as fn(Option<String>) -> ScalarValue,
                ),
            ] {
                let result =
                    RegexpSubstrFunc::new().invoke_with_args(ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Scalar(scalar(Some(value.to_string()))),
                            ColumnarValue::Scalar(scalar(Some(regex.to_string()))),
                            ColumnarValue::Scalar(ScalarValue::Int64(Some(position))),
                            ColumnarValue::Scalar(ScalarValue::Int64(Some(occurrence))),
                            ColumnarValue::Scalar(scalar(Some(flags.to_string()))),
                            ColumnarValue::Scalar(ScalarValue::Int64(Some(group_num))),
                        ],
                        number_rows: 1,
                        return_type: data_type,
                    });
                match result {
                    Ok(ColumnarValue::Scalar(
                           ScalarValue::Utf8(ref res) | ScalarValue::LargeUtf8(ref res),
                       )) => {
                        if res.is_some() {
                            assert_eq!(
                                res.as_ref().unwrap(),
                                &expected.to_string(),
                                "regexp_substr scalar test failed"
                            );
                        } else {
                            assert_eq!(
                                "", expected,
                                "regexp_substr scalar utf8 test failed"
                            )
                        }
                    }
                    _ => panic!("Unexpected result"),
                }
            }
        });
    }

    #[test]
    fn test_unsupported_global_flag_regexp_substr() {
        let values = StringArray::from(vec!["abc"]);
        let patterns = StringArray::from(vec!["^(a)"]);
        let position = Int64Array::from(vec![1]);
        let occurrence = Int64Array::from(vec![1]);
        let flags = StringArray::from(vec!["g"]);

        let re_err = regexp_substr::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(position),
            Arc::new(occurrence),
            Arc::new(flags),
        ])
            .expect_err("unsupported flag should have failed");

        assert_eq!(re_err.strip_backtrace(), "Error during planning: regexp_substr() does not support the \"global\" option");
    }
}