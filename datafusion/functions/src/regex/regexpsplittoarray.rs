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

use crate::regex::utils::{compile_and_cache_regex, compile_regex};
use crate::strings::StringArrayType;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, AsArray, Datum, GenericStringBuilder, LargeStringBuilder,
    ListBuilder, OffsetSizeTrait, StringBuilder,
};
use arrow::datatypes::DataType::{LargeUtf8, Utf8};
use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;
use datafusion_common::{exec_err, DataFusionError, ScalarValue};
use datafusion_expr::TypeSignature::{Exact, Uniform};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

use datafusion_expr::scalar_doc_sections::DOC_SECTION_REGEX;
use itertools::izip;
use regex::Regex;

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_regexp_split_to_array_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_REGEX)
            .with_description("Returns the number of matches that a [regular expression](https://docs.rs/regex/latest/regex/#syntax) has in a string.")
            .with_syntax_example("regexp_split_to_array(str, regexp[, flags])")
            .with_sql_example(r#"```sql
> select regexp_split_to_array('abcAbAbc', 'abc', 'i');
+---------------------------------------------------------------+
| regexp_split_to_array(Utf8("abcAbAbc"),Utf8("abc"),Utf8("i")) |
+---------------------------------------------------------------+
| ,Ab,
+---------------------------------------------------------------+
```"#)
            .with_standard_argument("str", Some("String"))
            .with_standard_argument("regexp",Some("Regular"))
            .with_argument("flags",
                           r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?"#)
            .build()
            .unwrap()
    })
}

#[derive(Debug)]
pub struct RegexpSplitToArrayFunc {
    signature: Signature,
}

impl Default for RegexpSplitToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpSplitToArrayFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Uniform(2, vec![Utf8View, LargeUtf8, Utf8]),
                    Exact(vec![Utf8View, Utf8View]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![Utf8View, Utf8View, Utf8View]),
                    Exact(vec![LargeUtf8, LargeUtf8, LargeUtf8]),
                    Exact(vec![Utf8, Utf8, Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpSplitToArrayFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_split_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        use DataType::*;

        Ok(match &arg_types[0] {
            Null => Null,
            other => List(Arc::new(Field::new("item", other.clone(), true))),
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .map(|arg| arg.clone().into_array(inferred_length))
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        let result = regexp_split_to_array_func(&args)?;
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(result))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_regexp_split_to_array_doc())
    }
}

pub fn regexp_split_to_array_func(
    args: &[ArrayRef],
) -> Result<ArrayRef, DataFusionError> {
    let args_len = args.len();
    if !(2..=3).contains(&args_len) {
        return exec_err!("regexp_count was called with {args_len} arguments. It requires 2 or 3 arguments.");
    }

    let values = &args[0];

    regexp_split_to_array(
        values,
        &args[1],
        if args_len > 2 { Some(&args[2]) } else { None },
    )
    .map_err(|e| e.into())
}

pub fn regexp_split_to_array(
    values: &dyn Array,
    regex_array: &dyn Datum,
    flags_array: Option<&dyn Datum>,
) -> Result<ArrayRef, ArrowError> {
    let (regex_array, is_regex_scalar) = regex_array.get();
    let (flags_array, is_flags_scalar) = flags_array.map_or((None, true), |flags| {
        let (flags, is_flags_scalar) = flags.get();
        (Some(flags), is_flags_scalar)
    });

    match (values.data_type(), regex_array.data_type(), flags_array) {
        (Utf8, Utf8, None) => regexp_split_to_array_inner(
            values.as_string::<i32>(),
            regex_array.as_string::<i32>(),
            is_regex_scalar,
            None,
            is_flags_scalar,
            ListBuilder::new(StringBuilder::new()),
        ),
        (Utf8, Utf8, Some(flags_array)) if *flags_array.data_type() == Utf8 => regexp_split_to_array_inner(
            values.as_string::<i32>(),
            regex_array.as_string::<i32>(),
            is_regex_scalar,
            Some(flags_array.as_string::<i32>()),
            is_flags_scalar,
            ListBuilder::new(StringBuilder::new()),
        ),
        (LargeUtf8, LargeUtf8, None) => regexp_split_to_array_inner(
            values.as_string::<i64>(),
            regex_array.as_string::<i64>(),
            is_regex_scalar,
            None,
            is_flags_scalar,
            ListBuilder::new(LargeStringBuilder::new()),
        ),
        (LargeUtf8, LargeUtf8, Some(flags_array)) if *flags_array.data_type() == LargeUtf8 => regexp_split_to_array_inner(
            values.as_string::<i64>(),
            regex_array.as_string::<i64>(),
            is_regex_scalar,
            Some(flags_array.as_string::<i64>()),
            is_flags_scalar,
            ListBuilder::new(LargeStringBuilder::new()),
        ),
        _ => Err(ArrowError::ComputeError(
            "regexp_split_to_array() expected the input arrays to be of type Utf8, LargeUtf8, or Utf8View and the data types of the values, regex_array, and flags_array to match".to_string(),
        )),
    }
}

pub fn regexp_split_to_array_inner<'a, S, T>(
    values: S,
    regex_array: S,
    is_regex_scalar: bool,
    flags_array: Option<S>,
    is_flags_scalar: bool,
    mut list_array: ListBuilder<GenericStringBuilder<T>>,
) -> Result<ArrayRef, ArrowError>
where
    S: StringArrayType<'a>,
    T: OffsetSizeTrait,
{
    let (regex_scalar, is_regex_scalar) = if is_regex_scalar || regex_array.len() == 1 {
        (Some(regex_array.value(0)), true)
    } else {
        (None, false)
    };

    let (flags_array, flags_scalar, is_flags_scalar) =
        if let Some(flags_array) = flags_array {
            if is_flags_scalar || flags_array.len() == 1 {
                (None, Some(flags_array.value(0)), true)
            } else {
                (Some(flags_array), None, false)
            }
        } else {
            (None, None, true)
        };

    let mut regex_cache = HashMap::new();

    let result = match (is_regex_scalar, is_flags_scalar) {
        (true, true) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    values.iter().for_each(|value| {
                        get_splitted_array_no_regex(&mut list_array, value)
                    });
                    return Ok(Arc::new(list_array.finish()));
                }
                Some(regex) => regex,
            };

            let pattern = compile_regex(regex, flags_scalar)?;
            values
                .iter()
                .for_each(|value| get_splitted_array(&mut list_array, value, &pattern));
            Ok(Arc::new(list_array.finish()))
        }
        (true, false) => {
            let regex = match regex_scalar {
                None => {
                    values.iter().for_each(|value| {
                        get_splitted_array_no_regex(&mut list_array, value)
                    });
                    return Ok(Arc::new(list_array.finish()));
                }
                Some(regex) => regex,
            };

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            values
                .iter()
                .zip(flags_array.iter())
                .for_each(|(value, flags)| {
                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache).ok();
                    get_splitted_array(&mut list_array, value, &pattern.unwrap())
                });

            Ok(Arc::new(list_array.finish()))
        }
        (false, true) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            values
                .iter()
                .zip(regex_array.iter())
                .for_each(|(value, regex)| {
                    let regex = regex.unwrap_or("");
                    let pattern =
                        compile_and_cache_regex(regex, flags_scalar, &mut regex_cache)
                            .ok();
                    get_splitted_array(&mut list_array, value, &pattern.unwrap());
                });
            Ok(Arc::new(list_array.finish()))
        }
        (false, false) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            izip!(values.iter(), regex_array.iter(), flags_array.iter()).for_each(
                |(value, regex, flags)| {
                    let regex = regex.unwrap_or("");
                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache).ok();
                    get_splitted_array(&mut list_array, value, &pattern.unwrap());
                },
            );
            Ok(Arc::new(list_array.finish()))
        }
    };
    result.map(|r| r as Arc<dyn Array>)
}

fn get_splitted_array_no_regex<T: OffsetSizeTrait>(
    list_array: &mut ListBuilder<GenericStringBuilder<T>>,
    value: Option<&str>,
) {
    match value {
        None => list_array.values().append_null(),
        Some(value) => value.chars().map(|c| c.to_string()).for_each(|v| {
            list_array.values().append_value(v);
        }),
    };
    list_array.append(true);
}

fn get_splitted_array<T: OffsetSizeTrait>(
    list_array: &mut ListBuilder<GenericStringBuilder<T>>,
    value: Option<&str>,
    pattern: &Regex,
) {
    match value {
        None => list_array.values().append_null(),
        Some(value) => pattern.split(value).map(|s| s.to_string()).for_each(|v| {
            list_array.values().append_value(v);
        }),
    };
    list_array.append(true);
}

#[cfg(test)]
mod tests {
    use crate::regex::regexpsplittoarray::RegexpSplitToArrayFunc;
    use arrow::array::{Array, ListBuilder, StringArray, StringBuilder};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    #[test]
    fn test_case_sensitive_regexp_split_to_array_scalar() {
        let values = ["", "aabca", "abcabc", "abcAbcab", "abcabcabc"];
        let regex = "abc";
        let expected = [
            vec![""],
            vec!["a", "a"],
            vec!["", "", ""],
            vec!["", "Abcab"],
            vec!["", "", "", ""],
        ];

        values.iter().enumerate().for_each(|(pos, &v)| {
            // utf8
            let v_sv = ScalarValue::Utf8(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8(Some(regex.to_string()));
            let expected: StringArray = expected.get(pos).cloned().unwrap().into();

            let re = RegexpSplitToArrayFunc::new()
                .invoke(&[ColumnarValue::Scalar(v_sv), ColumnarValue::Scalar(regex_sv)]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::List(v))) => {
                    assert_eq!(v.len(), 1, "regexp_split_to_array scalar test failed");

                    assert_eq!(
                        v.value(0).as_any().downcast_ref::<StringArray>().unwrap(),
                        &expected,
                        "regexp_split_to_array scalar test failed"
                    );
                }
                _ => panic!("Unexpected result"),
            }
        });
    }

    #[test]
    fn test_get_splitted_array_no_regex() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        let value = Some("hello");
        super::get_splitted_array_no_regex(&mut builder, value);
        let list_array = builder.finish();

        let binding = list_array.value(0);
        let string_array = binding
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(string_array.len(), 5);
        assert_eq!(string_array.value(0), "h");
        assert_eq!(string_array.value(1), "e");
        assert_eq!(string_array.value(2), "l");
        assert_eq!(string_array.value(3), "l");
        assert_eq!(string_array.value(4), "o");
    }

    #[test]
    fn test_get_splitted_array() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        let value = Some("hello");

        let pattern = regex::Regex::new(r"l").unwrap();
        super::get_splitted_array(&mut builder, value, &pattern);
        let list_array = builder.finish();

        let binding = list_array.value(0);
        let string_array = binding.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "he");
        assert_eq!(string_array.value(1), "");
        assert_eq!(string_array.value(2), "o");
    }
}
