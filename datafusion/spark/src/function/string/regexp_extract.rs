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

use std::sync::Arc;

use arrow::array::{Array, ArrayAccessor, ArrayRef, AsArray, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion_common::types::{logical_int32, logical_int64, logical_string, NativeType};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use regex::Regex;

const FUNCTION_NAME: &str = "regexp_extract";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpExtractFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        let string = Coercion::new_exact(TypeSignatureClass::Native(logical_string()));
        let int64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Native(logical_int32())],
            NativeType::Int64,
        );

        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![string.clone(), string.clone()]),
                    TypeSignature::Coercible(vec![
                        string.clone(),
                        string.clone(),
                        int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn name(&self) -> &str {
        FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(regexp_extract_fn, vec![])(&args.args)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion_common::internal_err!(
            "return_type should not be called for Spark regexp_extract"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new(FUNCTION_NAME, DataType::Utf8, nullable)))
    }
}

fn regexp_extract_fn(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 && args.len() != 3 {
        return exec_err!(
            "{FUNCTION_NAME} requires 2 or 3 arguments, got {}",
            args.len()
        );
    }

    let default_idx: ArrayRef =
        Arc::new(Int64Array::from(vec![1i64; args[0].len()]));
    let idx_array = if args.len() == 3 {
        args[2].as_primitive::<Int64Type>()
    } else {
        default_idx.as_primitive::<Int64Type>()
    };

    match (args[0].data_type(), args[1].data_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            regexp_extract_string(args[0].as_string::<i32>(), args[1].as_string::<i32>(), idx_array)
        }
        (DataType::LargeUtf8, DataType::Utf8) => {
            regexp_extract_string(args[0].as_string::<i64>(), args[1].as_string::<i32>(), idx_array)
        }
        (DataType::Utf8View, DataType::Utf8) => {
            regexp_extract_string(args[0].as_string_view(), args[1].as_string::<i32>(), idx_array)
        }
        (DataType::Utf8, DataType::LargeUtf8) => {
            regexp_extract_string(args[0].as_string::<i32>(), args[1].as_string::<i64>(), idx_array)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            regexp_extract_string(args[0].as_string::<i64>(), args[1].as_string::<i64>(), idx_array)
        }
        (DataType::Utf8View, DataType::LargeUtf8) => {
            regexp_extract_string(args[0].as_string_view(), args[1].as_string::<i64>(), idx_array)
        }
        (DataType::Utf8, DataType::Utf8View) => {
            regexp_extract_string(args[0].as_string::<i32>(), args[1].as_string_view(), idx_array)
        }
        (DataType::LargeUtf8, DataType::Utf8View) => {
            regexp_extract_string(args[0].as_string::<i64>(), args[1].as_string_view(), idx_array)
        }
        (DataType::Utf8View, DataType::Utf8View) => {
            regexp_extract_string(args[0].as_string_view(), args[1].as_string_view(), idx_array)
        }
        (other_str, other_pat) => exec_err!(
            "Unsupported data types {other_str:?}, {other_pat:?} for {FUNCTION_NAME}"
        ),
    }
}

fn regexp_extract_string<'a, S, P>(
    str_array: S,
    pattern_array: P,
    idx_array: &arrow::array::PrimitiveArray<Int64Type>,
) -> Result<ArrayRef>
where
    S: ArrayAccessor<Item = &'a str> + Array,
    P: ArrayAccessor<Item = &'a str> + Array,
{
    let mut builder = StringBuilder::new();

    for row in 0..str_array.len() {
        if str_array.is_null(row)
            || pattern_array.is_null(row)
            || idx_array.is_null(row)
        {
            builder.append_null();
            continue;
        }

        let value = str_array.value(row);
        let pattern = pattern_array.value(row);
        let idx = idx_array.value(row);

        if idx < 0 {
            return exec_err!(
                "{FUNCTION_NAME} requires a non-negative group index, got {idx}"
            );
        }
        let idx = idx as usize;

        let re = Regex::new(pattern).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "{FUNCTION_NAME} invalid regex pattern '{pattern}': {e}"
            ))
        })?;

        let group_count = re.captures_len() - 1;
        if idx > group_count {
            return exec_err!(
                "{FUNCTION_NAME} group index {idx} exceeds the number of groups ({group_count}) in pattern '{pattern}'"
            );
        }

        match re.captures(value) {
            Some(caps) => {
                let result = caps.get(idx).map(|m| m.as_str()).unwrap_or("");
                builder.append_value(result);
            }
            None => {
                builder.append_value("");
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int64Array, LargeStringArray, StringArray, StringViewArray,
    };

    fn run_regexp_extract(args: Vec<ArrayRef>) -> Result<ArrayRef> {
        regexp_extract_fn(&args)
    }

    fn string_result(result: &ArrayRef) -> Vec<Option<&str>> {
        let arr = result.as_string::<i32>();
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    // --- Basic matching ---

    #[test]
    fn test_basic_group_extraction() {
        let strs = Arc::new(StringArray::from(vec!["100-200", "abc-def"])) as ArrayRef;
        let pats =
            Arc::new(StringArray::from(vec!["(\\d+)-(\\d+)", "(\\w+)-(\\w+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("100"), Some("def")]);
    }

    #[test]
    fn test_group_zero_returns_entire_match() {
        let strs = Arc::new(StringArray::from(vec!["hello world"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(hello) (world)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("hello world")]);
    }

    #[test]
    fn test_group_index_out_of_range_returns_error() {
        let strs = Arc::new(StringArray::from(vec!["abc"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(a)(b)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![5])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("exceeds the number of groups"),
            "unexpected error: {err}"
        );
    }

    // --- No match ---

    #[test]
    fn test_no_match_returns_empty() {
        let strs = Arc::new(StringArray::from(vec!["abc"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["\\d+"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("")]);
    }

    // --- Null handling ---

    #[test]
    fn test_null_str_returns_null() {
        let strs = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(\\w+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![None]);
    }

    #[test]
    fn test_null_pattern_returns_null() {
        let strs = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![None]);
    }

    #[test]
    fn test_null_idx_returns_null() {
        let strs = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(\\w+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![None]);
    }

    #[test]
    fn test_all_nulls_returns_null() {
        let strs = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![None]);
    }

    // --- Negative index ---

    #[test]
    fn test_negative_index_returns_error() {
        let strs = Arc::new(StringArray::from(vec!["abc"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(a)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![-1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("non-negative"), "unexpected error: {err}");
    }

    // --- Invalid regex ---

    #[test]
    fn test_invalid_regex_returns_error() {
        let strs = Arc::new(StringArray::from(vec!["abc"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["[invalid"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid regex"), "unexpected error: {err}");
    }

    // --- Empty strings ---

    #[test]
    fn test_empty_string_input() {
        let strs = Arc::new(StringArray::from(vec![""])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(\\w+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("")]);
    }

    #[test]
    fn test_empty_pattern_matches_empty() {
        let strs = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec![""])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("")]);
    }

    // --- Unicode ---

    #[test]
    fn test_unicode_input() {
        let strs = Arc::new(StringArray::from(vec!["日本語123テスト"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(\\d+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("123")]);
    }

    #[test]
    fn test_unicode_pattern() {
        let strs = Arc::new(StringArray::from(vec!["café latte"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(café)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("café")]);
    }

    // --- Multiple rows ---

    #[test]
    fn test_multiple_rows_mixed() {
        let strs = Arc::new(StringArray::from(vec![
            Some("abc123"),
            None,
            Some("no-match"),
            Some("xyz789"),
        ])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec![
            Some("([a-z]+)(\\d+)"),
            Some("(\\w+)"),
            Some("(\\d+)"),
            Some("([a-z]+)(\\d+)"),
        ])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![
            Some(2),
            Some(1),
            Some(0),
            Some(1),
        ])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(
            string_result(&result),
            vec![Some("123"), None, Some(""), Some("xyz")]
        );
    }

    // --- LargeUtf8 ---

    #[test]
    fn test_large_utf8() {
        let strs = Arc::new(LargeStringArray::from(vec!["hello-world"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(\\w+)-(\\w+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![2])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("world")]);
    }

    // --- Utf8View ---

    #[test]
    fn test_utf8_view() {
        let strs = Arc::new(StringViewArray::from(vec!["foo123bar"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(\\d+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("123")]);
    }

    // --- Wrong number of arguments ---

    #[test]
    fn test_wrong_arg_count() {
        let strs = Arc::new(StringArray::from(vec!["abc"])) as ArrayRef;

        let result = run_regexp_extract(vec![strs]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("2 or 3 arguments"),
            "unexpected error: {err}"
        );
    }

    // --- Default idx ---

    #[test]
    fn test_two_args_defaults_idx_to_one() {
        let strs = Arc::new(StringArray::from(vec!["abc123"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["([a-z]+)(\\d+)"])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats]).unwrap();
        assert_eq!(string_result(&result), vec![Some("abc")]);
    }

    // --- Group index boundary ---

    #[test]
    fn test_idx_equals_group_count_is_valid() {
        let strs = Arc::new(StringArray::from(vec!["abc"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(a)(b)(c)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![3])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("c")]);
    }

    #[test]
    fn test_idx_one_past_group_count_returns_error() {
        let strs = Arc::new(StringArray::from(vec!["abc"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["(a)(b)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![3])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("exceeds the number of groups"),
            "unexpected error: {err}"
        );
    }

    // --- Pattern with no groups ---

    #[test]
    fn test_no_groups_idx_zero() {
        let strs = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![0])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("hello")]);
    }

    #[test]
    fn test_no_groups_idx_one_returns_error() {
        let strs = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("exceeds the number of groups"),
            "unexpected error: {err}"
        );
    }

    // --- Nested groups ---

    #[test]
    fn test_nested_groups() {
        let strs = Arc::new(StringArray::from(vec!["abc123"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["((\\w+)(\\d+))"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("abc123")]);
    }

    #[test]
    fn test_nested_groups_inner() {
        let strs = Arc::new(StringArray::from(vec!["abc123"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["((\\w+?)(\\d+))"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![3])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("123")]);
    }

    // --- Mixed type combinations ---

    #[test]
    fn test_large_utf8_str_with_utf8_view_pattern() {
        let strs = Arc::new(LargeStringArray::from(vec!["test456end"])) as ArrayRef;
        let pats = Arc::new(StringViewArray::from(vec!["(\\d+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("456")]);
    }

    #[test]
    fn test_utf8_view_str_with_large_utf8_pattern() {
        let strs = Arc::new(StringViewArray::from(vec!["key=value"])) as ArrayRef;
        let pats = Arc::new(LargeStringArray::from(vec!["(\\w+)=(\\w+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![2])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("value")]);
    }

    // --- Match but group didn't participate ---

    #[test]
    fn test_optional_group_not_captured() {
        let strs = Arc::new(StringArray::from(vec!["ac"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["a(b)?c"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("")]);
    }

    // --- Special regex characters ---

    #[test]
    fn test_special_characters_in_input() {
        let strs = Arc::new(StringArray::from(vec!["price is $100.50"])) as ArrayRef;
        let pats = Arc::new(StringArray::from(vec!["\\$(\\d+\\.\\d+)"])) as ArrayRef;
        let idxs = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

        let result = run_regexp_extract(vec![strs, pats, idxs]).unwrap();
        assert_eq!(string_result(&result), vec![Some("100.50")]);
    }
}

#[cfg(test)]
mod dataframe_tests {
    use std::sync::Arc;

    use arrow::array::{Array, AsArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use datafusion_expr::ScalarUDF;

    use super::RegexpExtractFunc;

    fn spark_ctx() -> SessionContext {
        SessionContext::new()
    }

    fn regexp_extract_udf() -> ScalarUDF {
        ScalarUDF::new_from_impl(RegexpExtractFunc::new())
    }

    async fn collect_strings(df: DataFrame) -> Vec<Option<String>> {
        let batches = df.collect().await.unwrap();
        let mut results = Vec::new();
        for batch in &batches {
            let col = batch.column(0).as_string::<i32>();
            for i in 0..col.len() {
                if col.is_null(i) {
                    results.push(None);
                } else {
                    results.push(Some(col.value(i).to_string()));
                }
            }
        }
        results
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("str", DataType::Utf8, true),
            Field::new("pattern", DataType::Utf8, true),
            Field::new("idx", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("abc123"),
                    Some("abc123"),
                    Some("key=value"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some("([a-z]+)(\\d+)"),
                    Some("([a-z]+)(\\d+)"),
                    Some("(\\w+)=(\\w+)"),
                    Some("(\\w+)"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(2),
                    Some(1),
                ])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_basic_extraction() {
        let ctx = spark_ctx();
        let udf = regexp_extract_udf();

        let df = ctx.read_batch(test_batch()).unwrap();
        let df = df
            .select(vec![udf.call(vec![col("str"), col("pattern"), col("idx")])])
            .unwrap();

        let result = collect_strings(df).await;
        assert_eq!(
            result,
            vec![
                Some("abc".to_string()),
                Some("123".to_string()),
                Some("value".to_string()),
                None,
            ]
        );
    }

    #[tokio::test]
    async fn test_literal_pattern_and_idx() {
        let ctx = spark_ctx();
        let udf = regexp_extract_udf();

        let schema = Arc::new(Schema::new(vec![
            Field::new("str", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "100-200",
                "foo",
                "abc-def",
            ]))],
        )
        .unwrap();

        let df = ctx.read_batch(batch).unwrap();
        let df = df
            .select(vec![udf.call(vec![
                col("str"),
                lit("(\\d+)-(\\d+)"),
                lit(1i64),
            ])])
            .unwrap();

        let result = collect_strings(df).await;
        assert_eq!(
            result,
            vec![
                Some("100".to_string()),
                Some("".to_string()),
                Some("".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn test_group_zero_entire_match() {
        let ctx = spark_ctx();
        let udf = regexp_extract_udf();

        let schema = Arc::new(Schema::new(vec![
            Field::new("str", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["hello world"]))],
        )
        .unwrap();

        let df = ctx.read_batch(batch).unwrap();
        let df = df
            .select(vec![udf.call(vec![
                col("str"),
                lit("(hello) (world)"),
                lit(0i64),
            ])])
            .unwrap();

        let result = collect_strings(df).await;
        assert_eq!(result, vec![Some("hello world".to_string())]);
    }

    #[tokio::test]
    async fn test_unicode_extraction() {
        let ctx = spark_ctx();
        let udf = regexp_extract_udf();

        let schema = Arc::new(Schema::new(vec![
            Field::new("str", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["日本語123テスト"]))],
        )
        .unwrap();

        let df = ctx.read_batch(batch).unwrap();
        let df = df
            .select(vec![udf.call(vec![col("str"), lit("(\\d+)"), lit(1i64)])])
            .unwrap();

        let result = collect_strings(df).await;
        assert_eq!(result, vec![Some("123".to_string())]);
    }

    #[tokio::test]
    async fn test_with_filter() {
        let ctx = spark_ctx();
        let udf = regexp_extract_udf();

        let schema = Arc::new(Schema::new(vec![
            Field::new("log", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "ERROR: code=404",
                "INFO: code=200",
                "ERROR: code=500",
            ]))],
        )
        .unwrap();

        let df = ctx.read_batch(batch).unwrap();
        let extract_expr =
            udf.call(vec![col("log"), lit("code=(\\d+)"), lit(1i64)]);
        let df = df
            .filter(col("log").like(lit("ERROR%")))
            .unwrap()
            .select(vec![extract_expr])
            .unwrap();

        let result = collect_strings(df).await;
        assert_eq!(
            result,
            vec![Some("404".to_string()), Some("500".to_string())]
        );
    }
}
