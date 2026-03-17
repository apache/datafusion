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

use crate::regex::compile_and_cache_regex;
use datafusion_macros::user_doc;
use arrow::array::{Array, ArrayRef, AsArray, GenericByteViewArray, PrimitiveArray, StringBuilder, StringArrayType, StringViewBuilder};
use arrow::datatypes::StringViewType;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Int64, LargeUtf8, Utf8, Utf8View};
use arrow::datatypes::Int64Type;
use datafusion_common::{Result, ScalarValue, arrow_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature::Exact, Volatility,
};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = r#"Extracts the first match of a [regular expression](https://docs.rs/regex/latest/regex/#syntax) in a string, returning a specific capture group.
If the regex did not match, or the specified group did not match, an empty string is returned."#,
    syntax_example = "regexp_extract(str, regexp, idx)",
    sql_example = r#"```sql
> select regexp_extract('2024-03-16', '(\d{4})-(\d{2})-(\d{2})', 1);
+------------------------------------------------------------------+
| regexp_extract(Utf8("2024-03-16"),Utf8("(\d{4})-(\d{2})-(\d{2})"),Int64(1)) |
+------------------------------------------------------------------+
| 2024                                                             |
+------------------------------------------------------------------+
> select regexp_extract('2024-03-16', '(\d{4})-(\d{2})-(\d{2})', 0);
+------------------------------------------------------------------+
| regexp_extract(Utf8("2024-03-16"),Utf8("(\d{4})-(\d{2})-(\d{2})"),Int64(0)) |
+------------------------------------------------------------------+
| 2024-03-16                                                       |
+------------------------------------------------------------------+
> select regexp_extract('no digits here', '(\d+)', 1);
+------------------------------------------------------------------+
| regexp_extract(Utf8("no digits here"),Utf8("(\d+)"),Int64(1))    |
+------------------------------------------------------------------+
|                                                                  |
+------------------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/regexp.rs)
"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to match against. Can be a constant, column, or function."
    ),
    argument(
        name = "idx",
        description = r#"The capture group index to return. Index **0** returns the entire matched string. Index **N** returns the Nth capture group `(...)`.
If the group index exceeds the number of groups in the pattern, an empty string is returned."#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpExtractFunc {
    signature: Signature,
}

impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Utf8View, Int64]),
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Null => Ok(DataType::Null),
            Utf8View => Ok(Utf8View),
            LargeUtf8 => Ok(LargeUtf8),
            _ => Ok(Utf8),
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;

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
            .map(|arg| arg.to_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = regexp_extract_func(&args);
        if is_scalar {
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

pub fn regexp_extract_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "regexp_extract requires exactly 3 arguments, got {}",
            args.len()
        );
    }

    let idx_array = args[2].as_primitive::<Int64Type>();

    match args[0].data_type() {
        Utf8View => regexp_extract_inner_view(
            args[0].as_string_view(),
            args[1].as_string_view(),
            idx_array,
        ),
        Utf8 => regexp_extract_inner(
            &args[0].as_string::<i32>(),
            &args[1].as_string::<i32>(),
            idx_array,
        ),
        LargeUtf8 => regexp_extract_inner(
            &args[0].as_string::<i64>(),
            &args[1].as_string::<i64>(),
            idx_array,
        ),
        other => exec_err!("regexp_extract does not support type {other:?}"),
    }
}


fn regexp_extract_inner<'a, S>(
    values: &S,
    regex_array: &S,
    idx_array: &PrimitiveArray<Int64Type>,
) -> Result<ArrayRef>
where
    S: StringArrayType<'a>,
{
    let is_regex_scalar = regex_array.len() == 1;
    let is_idx_scalar = idx_array.len() == 1;
    let mut regex_cache = HashMap::new();
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);

    for i in 0..values.len() {
        let pattern_i = if is_regex_scalar { 0 } else { i };
        let idx_i = if is_idx_scalar { 0 } else { i };

        if values.is_null(i) || regex_array.is_null(pattern_i) || idx_array.is_null(idx_i) {
            builder.append_null();
            continue;
        }

        let extracted = process_row(
            values.value(i),
            regex_array.value(pattern_i),
            idx_array.value(idx_i),
            &mut regex_cache,
        )?;
        builder.append_value(extracted);
    }

    Ok(Arc::new(builder.finish()))
}

fn regexp_extract_inner_view<'a>(
    values: &'a GenericByteViewArray<StringViewType>,
    regex_array: &'a GenericByteViewArray<StringViewType>,
    idx_array: &PrimitiveArray<Int64Type>,
) -> Result<ArrayRef> {
    let is_regex_scalar = regex_array.len() == 1;
    let is_idx_scalar = idx_array.len() == 1;
    let mut regex_cache = HashMap::new();
    let mut builder = StringViewBuilder::with_capacity(values.len());

    for i in 0..values.len() {
        let pattern_i = if is_regex_scalar { 0 } else { i };
        let idx_i = if is_idx_scalar { 0 } else { i };

        if values.is_null(i) || regex_array.is_null(pattern_i) || idx_array.is_null(idx_i) {
            builder.append_null();
            continue;
        }

        let extracted = process_row(
            values.value(i),
            regex_array.value(pattern_i),
            idx_array.value(idx_i),
            &mut regex_cache,
        )?;
        builder.append_value(extracted);
    }

    Ok(Arc::new(builder.finish()))
}

fn process_row<'a>(
    value: &'a str,
    pattern: &'a str,
    idx: i64,
    regex_cache: &mut HashMap<(&'a str, Option<&'a str>), regex::Regex>,
) -> Result<&'a str> {
    if idx < 0 {
        return exec_err!("regexp_extract requires a non-negative idx, got {idx}");
    }
    if pattern.is_empty() {
        return Ok("");
    }
    let re = compile_and_cache_regex(pattern, None, regex_cache)
        .map_err(|e| arrow_datafusion_err!(e))?;
    Ok(extract_group(value, re, idx as usize))
}

fn extract_group<'a>(value: &'a str, re: &regex::Regex, idx: usize) -> &'a str {
    match re.captures(value) {
        None => "",
        Some(caps) => caps.get(idx).map_or("", |m| m.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::regexp_extract_func;
    use arrow::array::{Array, Int64Array, StringArray};
    use std::sync::Arc;

    fn run(values: &[Option<&str>], pattern: &str, idx: i64) -> Vec<Option<String>> {
        let values = Arc::new(StringArray::from(values.to_vec()));
        let patterns = Arc::new(StringArray::from(vec![pattern; values.len()]));
        let idxs = Arc::new(Int64Array::from(vec![idx; values.len()]));
        let result = regexp_extract_func(&[values, patterns, idxs]).unwrap();
        result
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.to_string()))
            .collect()
    }

    #[test]
    fn test_basic_group() {
        let result = run(&[Some("2024-03-16")], r"(\d{4})-(\d{2})-(\d{2})", 1);
        assert_eq!(result, vec![Some("2024".to_string())]);
    }

    #[test]
    fn test_idx_zero_returns_whole_match() {
        let result = run(&[Some("foo123bar")], r"\w+", 0);
        assert_eq!(result, vec![Some("foo123bar".to_string())]);
    }

    #[test]
    fn test_no_match_returns_empty_string() {
        let result = run(&[Some("abc")], r"(\d+)", 1);
        assert_eq!(result, vec![Some("".to_string())]);
    }

    #[test]
    fn test_null_input_returns_null() {
        let result = run(&[None], r"(\d+)", 1);
        assert_eq!(result, vec![None]);
    }

    #[test]
    fn test_empty_pattern_returns_empty_string() {
        let result = run(&[Some("abc")], "", 0);
        assert_eq!(result, vec![Some("".to_string())]);
    }

    #[test]
    fn test_idx_out_of_range_returns_empty_string() {
        let result = run(&[Some("abc")], r"(a)(b)", 5);
        assert_eq!(result, vec![Some("".to_string())]);
    }

    #[test]
    fn test_negative_idx_returns_error() {
        let values = Arc::new(StringArray::from(vec!["abc"]));
        let patterns = Arc::new(StringArray::from(vec!["(a)"]));
        let idxs = Arc::new(Int64Array::from(vec![-1i64]));
        let err = regexp_extract_func(&[values, patterns, idxs])
            .expect_err("negative idx should fail");
        assert!(err.to_string().contains("non-negative idx"));
    }

    #[test]
    fn test_multiple_groups() {
        let result = run(&[Some("2024-03")], r"(\d{4})-(\d{2})", 2);
        assert_eq!(result, vec![Some("03".to_string())]);
    }

    #[test]
    fn test_idx_as_column() {
        let values = Arc::new(StringArray::from(vec!["2024-03-16", "2025-01-01"]));
        let patterns = Arc::new(StringArray::from(vec![
            r"(\d{4})-(\d{2})-(\d{2})",
            r"(\d{4})-(\d{2})-(\d{2})",
        ]));
        let idxs = Arc::new(Int64Array::from(vec![1i64, 3i64]));
        let result = regexp_extract_func(&[values, patterns, idxs]).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.to_string()))
            .collect::<Vec<_>>();
        assert_eq!(result, vec![Some("2024".to_string()), Some("01".to_string())]);
    }

    #[test]
    fn test_batch_mixed_nulls() {
        let result = run(&[Some("abc"), None, Some("123")], r"([a-z]+)", 1);
        assert_eq!(
            result,
            vec![Some("abc".to_string()), None, Some("".to_string())]
        );
    }
}
