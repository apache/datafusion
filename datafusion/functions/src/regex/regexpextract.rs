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
use arrow::array::{Array, ArrayRef, Int32Array, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::exec_err;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use regex::Regex;
use std::any::Any;
use std::sync::Arc;

// See https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html
// See https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/regexpExpressions.scala#L863

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Extract the first string in the `str` that match the `regexp` expression and corresponding to the regex group index",
    syntax_example = "regexp_extract(str, regexp[, idx])",
    sql_example = r#"```sql
            > SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1);
            +---------------------------------------------------------+
            | 100                                                   |
            +---------------------------------------------------------+
            > SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 2);
            +---------------------------------------------------------+
            | 200                                                   |
            +---------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/regexp.rs)
"#,
    argument(name = "str", description = "Column or column name"),
    argument(
        name = "regexp",
        description = r#"a string representing a regular expression. The regex string should be a
          Java regular expression.<br><br>
          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser, see the unescaping rules at <a href="https://spark.apache.org/docs/latest/sql-ref-literals.html#string-literal">String Literal</a>.
          For example, to match "\abc", a regular expression for `regexp` can be "^\\abc$".<br><br>
          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".<br><br>
          It's recommended to use a raw string literal (with the `r` prefix) to avoid escaping
          special characters in the pattern string if exists."#
    ),
    argument(
        name = "idx",
        description = r#"an integer expression that representing the group index. The regex maybe contains
          multiple groups. `idx` indicates which regex group to extract. The group index should
          be non-negative. The minimum value of `idx` is 0, which means matching the entire
          regular expression. If `idx` is not specified, the default group index value is 1.
          This parameter is optional; when omitted the function defaults to extracting the first
          capture group (idx=1), matching Spark's behavior."#
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
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    // Spark Catalyst Expression: RegExpExtract(subject, regexp, idx)
                    // where idx defaults to 1 when omitted.
                    // See: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/regexpExpressions.scala
                    //
                    // 2-arg form: regexp_extract(str, regexp) — idx defaults to 1
                    // Matches Spark's: def this(s: Expression, r: Expression) = this(s, r, Literal(1))
                    TypeSignature::Exact(vec![Utf8, Utf8]),
                    // 3-arg form: regexp_extract(str, regexp, idx)
                    TypeSignature::Exact(vec![Utf8, Utf8, Int32]),
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
        use DataType::*;
        // Spark's RegExpExtract always returns StringType
        match arg_types.len() {
            2 | 3 => match arg_types[0] {
                Utf8 => Ok(Utf8),
                _ => exec_err!("regexp_extract only supports Utf8 for arg0"),
            },
            _ => exec_err!(
                "regexp_extract expects 2 or 3 arguments, got {}",
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;

        if args.len() != 2 && args.len() != 3 {
            return exec_err!("regexp_extract expects 2 or 3 arguments");
        }

        // DataFusion passes either scalars or arrays. Convert to arrays.
        let len = args
            .iter()
            .map(|v| match v {
                ColumnarValue::Array(a) => a.len(),
                ColumnarValue::Scalar(_) => 1,
            })
            .max()
            .unwrap_or(1);

        let a0 = args[0].to_array(len)?;
        let a1 = args[1].to_array(len)?;

        // Spark Catalyst: def this(s, r) = this(s, r, Literal(1))
        // When idx is omitted, default to group index 1.
        let a2 = if args.len() == 3 {
            args[2].to_array(len)?
        } else {
            // Default idx = 1, matching Spark's behavior
            Arc::new(Int32Array::from(vec![1; len])) as ArrayRef
        };

        let out: ArrayRef = regexp_extract(&[a0, a1, a2])?;
        Ok(ColumnarValue::Array(out))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Helper to build args for tests and external callers.
pub fn regexp_extract(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("regexp_extract expects 3 arguments");
    }

    match args[0].data_type() {
        // TODO: DataType::Utf8View => regexp_extract_utf8_view(args),
        DataType::Utf8 => regexp_extract_utf8(args),
        // TODO: DataType::LargeUtf8 => regexp_extract_large_utf8(args),
        other => exec_err!("regexp_extract unsupported input type {other:?}"),
    }
}

fn regexp_extract_utf8(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = &args[0];
    let pattern = &args[1];
    let index = &args[2];

    let values_array = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("arg0 must be Utf8".to_string()))?;
    let pattern_array = pattern
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("arg1 must be Utf8".to_string()))?;
    let index_array = index
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFusionError::Execution("arg2 must be Int32".to_string()))?;

    let mut out = StringBuilder::new();

    // Now iterate over the values, pattern, and index arrays and extract the matches

    for i in 0..values_array.len() {
        if values_array.is_null(i) || pattern_array.is_null(i) || index_array.is_null(i) {
            out.append_null();
            continue;
        }

        let value = values_array.value(i);
        let pattern = pattern_array.value(i);
        let index = index_array.value(i);

        if index < 0 {
            return exec_err!(
                "The value of idx in regexp_extract must be non-negative, but got {}",
                index
            );
        }

        let group_index = index as usize;

        let regex =
            Regex::new(pattern).map_err(|e| ArrowError::ComputeError(e.to_string()))?;

        // Validate group index doesn't exceed the number of capture groups
        let num_groups = regex.captures_len();
        if group_index >= num_groups {
            return exec_err!(
                "Regex group index {} exceeds the number of groups {} in pattern '{}'",
                group_index,
                num_groups - 1,
                pattern
            );
        }

        if let Some(cap) = regex.captures(value) {
            // cap.get() returns None for unmatched optional groups like (b)?
            // Spark returns empty string in that case
            match cap.get(group_index) {
                Some(m) => out.append_value(m.as_str()),
                None => out.append_value(""),
            }
        } else {
            // No match at all — Spark returns empty string, not null
            out.append_value("");
        }
    }

    Ok(Arc::new(out.finish()))
}

#[cfg(test)]
mod tests {
    use super::regexp_extract;
    use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
    use std::sync::Arc;

    /// Helper: call regexp_extract with single-element arrays and return the result string.
    fn extract_one(value: &str, pattern: &str, idx: i32) -> Option<String> {
        let values = Arc::new(StringArray::from(vec![value]));
        let patterns = Arc::new(StringArray::from(vec![pattern]));
        let indices = Arc::new(Int32Array::from(vec![idx]));
        let result = regexp_extract(&[values, patterns, indices]).unwrap();
        let arr = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_string())
        }
    }

    // -----------------------------------------------------------------------
    // Tests derived from the PySpark regexp_extract documentation:
    // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html
    // -----------------------------------------------------------------------

    /// PySpark example 1:
    ///   regexp_extract('100-200', r'(\d+)-(\d+)', 1) → '100'
    #[test]
    fn test_spark_example_extract_first_group() {
        assert_eq!(
            extract_one("100-200", r"(\d+)-(\d+)", 1),
            Some("100".to_string())
        );
    }

    /// Also verify extracting the second capture group:
    ///   regexp_extract('100-200', r'(\d+)-(\d+)', 2) → '200'
    #[test]
    fn test_spark_example_extract_second_group() {
        assert_eq!(
            extract_one("100-200", r"(\d+)-(\d+)", 2),
            Some("200".to_string())
        );
    }

    /// PySpark example 2 — no match returns empty string:
    ///   regexp_extract('foo', r'(\d+)', 1) → ''
    #[test]
    fn test_spark_example_no_match_returns_empty() {
        assert_eq!(extract_one("foo", r"(\d+)", 1), Some("".to_string()));
    }

    /// PySpark example 3 — unmatched optional group returns empty string:
    ///   regexp_extract('aaaac', '(a+)(b)?(c)', 2) → ''
    #[test]
    fn test_spark_example_optional_group_not_matched() {
        assert_eq!(
            extract_one("aaaac", r"(a+)(b)?(c)", 2),
            Some("".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // Additional coverage
    // -----------------------------------------------------------------------

    /// idx=0 returns the entire match.
    #[test]
    fn test_idx_zero_returns_entire_match() {
        assert_eq!(
            extract_one("100-200", r"(\d+)-(\d+)", 0),
            Some("100-200".to_string())
        );
    }

    /// Verify extracting groups from partially-matching optional groups.
    ///   regexp_extract('aaaac', '(a+)(b)?(c)', 1) → 'aaaa'
    #[test]
    fn test_optional_group_extract_first() {
        assert_eq!(
            extract_one("aaaac", r"(a+)(b)?(c)", 1),
            Some("aaaa".to_string())
        );
    }

    ///   regexp_extract('aaaac', '(a+)(b)?(c)', 3) → 'c'
    #[test]
    fn test_optional_group_extract_third() {
        assert_eq!(
            extract_one("aaaac", r"(a+)(b)?(c)", 3),
            Some("c".to_string())
        );
    }

    /// Negative group index should error.
    #[test]
    fn test_negative_index_errors() {
        let values = Arc::new(StringArray::from(vec!["abc"]));
        let patterns = Arc::new(StringArray::from(vec!["(a)"]));
        let indices = Arc::new(Int32Array::from(vec![-1]));
        let err = regexp_extract(&[values, patterns, indices])
            .expect_err("negative index should error");
        let msg = err.to_string();
        assert!(
            msg.contains("non-negative"),
            "unexpected error message: {msg}"
        );
    }

    /// Group index exceeding the number of groups should error.
    #[test]
    fn test_group_index_out_of_bounds() {
        let values = Arc::new(StringArray::from(vec!["abc"]));
        let patterns = Arc::new(StringArray::from(vec!["(a)"]));
        // pattern has 1 capture group, so valid indices are 0 and 1; 2 is out of bounds
        let indices = Arc::new(Int32Array::from(vec![2]));
        let err = regexp_extract(&[values, patterns, indices])
            .expect_err("out-of-bounds index should error");
        let msg = err.to_string();
        assert!(msg.contains("exceeds"), "unexpected error message: {msg}");
    }

    /// Multiple rows processed in a single batch.
    #[test]
    fn test_batch_multiple_rows() {
        let values = Arc::new(StringArray::from(vec![
            "100-200",
            "foo",
            "aaaac",
            "hello-world",
        ]));
        let patterns = Arc::new(StringArray::from(vec![
            r"(\d+)-(\d+)",
            r"(\d+)",
            r"(a+)(b)?(c)",
            r"(\w+)-(\w+)",
        ]));
        let indices = Arc::new(Int32Array::from(vec![1, 1, 2, 2]));

        let result = regexp_extract(&[values, patterns, indices]).unwrap();
        let arr = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");

        assert_eq!(arr.value(0), "100"); // first group of '100-200'
        assert_eq!(arr.value(1), ""); // no match in 'foo'
        assert_eq!(arr.value(2), ""); // optional (b)? not matched
        assert_eq!(arr.value(3), "world"); // second group of 'hello-world'
    }

    /// Null input produces null output.
    #[test]
    fn test_null_input_produces_null() {
        let values = Arc::new(StringArray::from(vec![None as Option<&str>]));
        let patterns = Arc::new(StringArray::from(vec![Some(r"(\d+)")]));
        let indices = Arc::new(Int32Array::from(vec![1]));

        let result = regexp_extract(&[values, patterns, indices]).unwrap();
        let arr = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");

        assert!(arr.is_null(0));
    }

    /// Empty string input matches empty pattern group 0.
    #[test]
    fn test_empty_string_with_matching_pattern() {
        // Pattern .* matches empty string; group 0 is the entire match ""
        assert_eq!(extract_one("", ".*", 0), Some("".to_string()));
    }

    // -----------------------------------------------------------------------
    // Tests for 2-arg form (idx defaults to 1, matching Spark's Catalyst
    // `def this(s, r) = this(s, r, Literal(1))`)
    // -----------------------------------------------------------------------

    /// Helper: call regexp_extract via invoke_with_args with 2 args (no idx),
    /// verifying that idx defaults to 1.
    fn extract_two_arg(value: &str, pattern: &str) -> Option<String> {
        use super::RegexpExtractFunc;
        use arrow::datatypes::Field;
        use datafusion_common::config::ConfigOptions;
        use datafusion_expr::ColumnarValue;
        use datafusion_expr::ScalarUDFImpl;

        let func = RegexpExtractFunc::new();
        let values =
            ColumnarValue::Array(Arc::new(StringArray::from(vec![value])) as ArrayRef);
        let patterns =
            ColumnarValue::Array(Arc::new(StringArray::from(vec![pattern])) as ArrayRef);

        let result = func
            .invoke_with_args(datafusion_expr::ScalarFunctionArgs {
                args: vec![values, patterns],
                arg_fields: vec![
                    Arc::new(Field::new("str", arrow::datatypes::DataType::Utf8, true)),
                    Arc::new(Field::new(
                        "regexp",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    )),
                ],
                number_rows: 1,
                return_field: Arc::new(Field::new(
                    "result",
                    arrow::datatypes::DataType::Utf8,
                    true,
                )),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("expected StringArray");
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0).to_string())
                }
            }
            _ => panic!("expected Array"),
        }
    }

    /// 2-arg form: regexp_extract('100-200', r'(\d+)-(\d+)') → '100'
    /// (idx defaults to 1, extracting first capture group)
    #[test]
    fn test_two_arg_defaults_to_group_1() {
        assert_eq!(
            extract_two_arg("100-200", r"(\d+)-(\d+)"),
            Some("100".to_string())
        );
    }

    /// 2-arg form with no match returns empty string (same as Spark).
    #[test]
    fn test_two_arg_no_match_returns_empty() {
        assert_eq!(extract_two_arg("foo", r"(\d+)"), Some("".to_string()));
    }

    /// 2-arg form extracts first group from a single-group pattern.
    #[test]
    fn test_two_arg_single_group() {
        assert_eq!(
            extract_two_arg("hello world", r"(\w+)"),
            Some("hello".to_string())
        );
    }

    /// Multiple rows with the same pattern and group index — the common case
    /// where a single regex is applied across an entire column.
    #[test]
    fn test_batch_same_pattern() {
        let values = Arc::new(StringArray::from(vec![
            "2024-01-15",
            "2023-12-25",
            "no-date-here",
            "2025-06-30",
            "",
        ]));
        // Same pattern repeated for every row
        let patterns = Arc::new(StringArray::from(vec![
            r"(\d{4})-(\d{2})-(\d{2})";
            5
        ]));
        // Same group index (1 = year) for every row
        let indices = Arc::new(Int32Array::from(vec![1; 5]));

        let result = regexp_extract(&[values, patterns, indices]).unwrap();
        let arr = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");

        assert_eq!(arr.value(0), "2024");
        assert_eq!(arr.value(1), "2023");
        assert_eq!(arr.value(2), ""); // no match → empty string
        assert_eq!(arr.value(3), "2025");
        assert_eq!(arr.value(4), ""); // empty input → no match → empty string
    }
}
