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
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, MapBuilder, MapFieldNames, StringBuilder, StringArrayType};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

use crate::function::map::utils::map_type_from_key_value_types;

/// Spark-compatible `str_to_map` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#str_to_map>
///
/// Creates a map from a string by splitting on delimiters.
/// str_to_map(text[, pairDelim[, keyValueDelim]]) -> Map<String, String>
///
/// - text: The input string
/// - pairDelim: Delimiter between key-value pairs (default: ',')
/// - keyValueDelim: Delimiter between key and value (default: ':')
///
/// # Duplicate Key Handling
/// Uses EXCEPTION behavior (Spark 3.0+ default): errors on duplicate keys.
/// See `spark.sql.mapKeyDedupPolicy`:
/// <https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4502-L4511>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStrToMap {
    signature: Signature,
}

impl Default for SparkStrToMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStrToMap {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // str_to_map(text)
                    TypeSignature::String(1),
                    // str_to_map(text, pairDelim)
                    TypeSignature::String(2),
                    // str_to_map(text, pairDelim, keyValueDelim)
                    TypeSignature::String(3),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkStrToMap {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "str_to_map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        let map_type = map_type_from_key_value_types(&DataType::Utf8, &DataType::Utf8);
        Ok(Arc::new(Field::new(self.name(), map_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays: Vec<ArrayRef> = ColumnarValue::values_to_arrays(&args.args)?;
        let result = str_to_map_inner(&arrays)?;
        Ok(ColumnarValue::Array(result))
    }
}

fn str_to_map_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        1 => match args[0].data_type() {
            DataType::Utf8 => {
                str_to_map_impl(as_string_array(&args[0])?, None, None)
            }
            DataType::LargeUtf8 => {
                str_to_map_impl(as_large_string_array(&args[0])?, None, None)
            }
            DataType::Utf8View => {
                str_to_map_impl(as_string_view_array(&args[0])?, None, None)
            }
            other => exec_err!(
                "Unsupported data type {other:?} for str_to_map, \
                expected Utf8, LargeUtf8, or Utf8View"
            ),
        },
        2 => match (args[0].data_type(), args[1].data_type()) {
            (DataType::Utf8, DataType::Utf8) => str_to_map_impl(
                as_string_array(&args[0])?,
                Some(as_string_array(&args[1])?),
                None,
            ),
            (DataType::LargeUtf8, DataType::LargeUtf8) => str_to_map_impl(
                as_large_string_array(&args[0])?,
                Some(as_large_string_array(&args[1])?),
                None,
            ),
            (DataType::Utf8View, DataType::Utf8View) => str_to_map_impl(
                as_string_view_array(&args[0])?,
                Some(as_string_view_array(&args[1])?),
                None,
            ),
            (t1, t2) => exec_err!(
                "Unsupported data types ({t1:?}, {t2:?}) for str_to_map, \
                expected matching Utf8, LargeUtf8, or Utf8View"
            ),
        },
        3 => match (
            args[0].data_type(),
            args[1].data_type(),
            args[2].data_type(),
        ) {
            (DataType::Utf8, DataType::Utf8, DataType::Utf8) => str_to_map_impl(
                as_string_array(&args[0])?,
                Some(as_string_array(&args[1])?),
                Some(as_string_array(&args[2])?),
            ),
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => {
                str_to_map_impl(
                    as_large_string_array(&args[0])?,
                    Some(as_large_string_array(&args[1])?),
                    Some(as_large_string_array(&args[2])?),
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::Utf8View) => {
                str_to_map_impl(
                    as_string_view_array(&args[0])?,
                    Some(as_string_view_array(&args[1])?),
                    Some(as_string_view_array(&args[2])?),
                )
            }
            (t1, t2, t3) => exec_err!(
                "Unsupported data types ({t1:?}, {t2:?}, {t3:?}) for str_to_map, \
                expected matching Utf8, LargeUtf8, or Utf8View"
            ),
        },
        n => exec_err!("str_to_map expects 1-3 arguments, got {n}"),
    }
}

fn str_to_map_impl<'a, V: StringArrayType<'a> + Copy>(
    text_array: V,
    pair_delim_array: Option<V>,
    kv_delim_array: Option<V>,
) -> Result<ArrayRef> {
    let num_rows = text_array.len();
    // Use field names matching map_type_from_key_value_types: "key" and "value"
    let field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut map_builder = MapBuilder::new(
        Some(field_names),
        StringBuilder::new(),
        StringBuilder::new(),
    );

    for row_idx in 0..num_rows {
        if text_array.is_null(row_idx)
            || pair_delim_array.is_some_and(|a| a.is_null(row_idx))
            || kv_delim_array.is_some_and(|a| a.is_null(row_idx))
        {
            map_builder.append(false)?;
            continue;
        }

        // Per-row delimiter extraction
        let pair_delim = pair_delim_array.map_or(",", |a| a.value(row_idx));
        let kv_delim = kv_delim_array.map_or(":", |a| a.value(row_idx));

        let text = text_array.value(row_idx);
        if text.is_empty() {
            // Empty string -> map with empty key and NULL value (Spark behavior)
            map_builder.keys().append_value("");
            map_builder.values().append_null();
            map_builder.append(true)?;
            continue;
        }

        let mut seen_keys = HashSet::new();
        for pair in text.split(pair_delim) {
            if pair.is_empty() {
                continue;
            }

            let mut kv_iter = pair.splitn(2, kv_delim);
            let key = kv_iter.next().unwrap_or("");
            let value = kv_iter.next();

            // EXCEPTION policy: error on duplicate keys (Spark 3.0+ default)
            if !seen_keys.insert(key) {
                return exec_err!(
                    "Duplicate map key '{key}' was found, please check the input data. \
                    If you want to remove the duplicates, you can set \
                    spark.sql.mapKeyDedupPolicy to LAST_WIN"
                );
            }

            map_builder.keys().append_value(key);
            match value {
                Some(v) => map_builder.values().append_value(v),
                None => map_builder.values().append_null(),
            }
        }
        map_builder.append(true)?;
    }

    Ok(Arc::new(map_builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, MapArray, StringArray};

    // Table-driven tests for str_to_map
    // Test cases derived from Spark ComplexTypeSuite:
    // https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ComplexTypeSuite.scala#L525-L618
    #[test]
    fn test_str_to_map_cases() {
        struct TestCase {
            name: &'static str,
            inputs: Vec<Option<&'static str>>,
            pair_delim: Option<&'static str>,
            kv_delim: Option<&'static str>,
            expected: Vec<Option<Vec<(&'static str, Option<&'static str>)>>>,
        }

        let cases = vec![
            TestCase {
                name: "s0: basic default delimiters",
                inputs: vec![Some("a:1,b:2,c:3")],
                pair_delim: None,
                kv_delim: None,
                expected: vec![Some(vec![
                    ("a", Some("1")),
                    ("b", Some("2")),
                    ("c", Some("3")),
                ])],
            },
            TestCase {
                name: "s1: preserve spaces in values",
                inputs: vec![Some("a: ,b:2")],
                pair_delim: None,
                kv_delim: None,
                expected: vec![Some(vec![("a", Some(" ")), ("b", Some("2"))])],
            },
            TestCase {
                name: "s2: custom kv delimiter '='",
                inputs: vec![Some("a=1,b=2,c=3")],
                pair_delim: Some(","),
                kv_delim: Some("="),
                expected: vec![Some(vec![
                    ("a", Some("1")),
                    ("b", Some("2")),
                    ("c", Some("3")),
                ])],
            },
            TestCase {
                name: "s3: empty string",
                inputs: vec![Some("")],
                pair_delim: Some(","),
                kv_delim: Some("="),
                expected: vec![Some(vec![("", None)])],
            },
            TestCase {
                name: "s4: custom pair delimiter '_'",
                inputs: vec![Some("a:1_b:2_c:3")],
                pair_delim: Some("_"),
                kv_delim: Some(":"),
                expected: vec![Some(vec![
                    ("a", Some("1")),
                    ("b", Some("2")),
                    ("c", Some("3")),
                ])],
            },
            TestCase {
                name: "s5: single key no value",
                inputs: vec![Some("a")],
                pair_delim: None,
                kv_delim: None,
                expected: vec![Some(vec![("a", None)])],
            },
            TestCase {
                name: "s6: custom delimiters '&' and '='",
                inputs: vec![Some("a=1&b=2&c=3")],
                pair_delim: Some("&"),
                kv_delim: Some("="),
                expected: vec![Some(vec![
                    ("a", Some("1")),
                    ("b", Some("2")),
                    ("c", Some("3")),
                ])],
            },
            TestCase {
                name: "null input returns null",
                inputs: vec![None],
                pair_delim: None,
                kv_delim: None,
                expected: vec![None],
            },
            TestCase {
                name: "multi-row",
                inputs: vec![Some("a:1,b:2"), Some("x:9"), None],
                pair_delim: None,
                kv_delim: None,
                expected: vec![
                    Some(vec![("a", Some("1")), ("b", Some("2"))]),
                    Some(vec![("x", Some("9"))]),
                    None,
                ],
            },
        ];

        for case in cases {
            let text: ArrayRef = Arc::new(StringArray::from(case.inputs));
            let args: Vec<ArrayRef> = match (case.pair_delim, case.kv_delim) {
                (Some(p), Some(k)) => vec![
                    text.clone(),
                    Arc::new(StringArray::from(vec![p; text.len()])),
                    Arc::new(StringArray::from(vec![k; text.len()])),
                ],
                _ => vec![text],
            };

            let result = str_to_map_inner(&args).unwrap();
            let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();

            assert_eq!(map_array.len(), case.expected.len(), "case: {}", case.name);

            for (row_idx, expected_row) in case.expected.iter().enumerate() {
                match expected_row {
                    None => {
                        assert!(
                            map_array.is_null(row_idx),
                            "case: {} row {} expected NULL",
                            case.name,
                            row_idx
                        );
                    }
                    Some(expected_entries) => {
                        assert!(
                            !map_array.is_null(row_idx),
                            "case: {} row {} unexpected NULL",
                            case.name,
                            row_idx
                        );
                        let entries = get_map_entries(map_array, row_idx);
                        let expected: Vec<(String, Option<String>)> = expected_entries
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.map(|s| s.to_string())))
                            .collect();
                        assert_eq!(
                            entries, expected,
                            "case: {} row {}",
                            case.name, row_idx
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_duplicate_keys_exception() {
        let text: ArrayRef = Arc::new(StringArray::from(vec!["a:1,b:2,a:3"]));
        let result = str_to_map_inner(&[text]);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Duplicate map key"),
            "expected duplicate key error, got: {err_msg}"
        );
    }

    #[test]
    fn test_per_row_delimiters() {
        // Each row has its own delimiters
        let text: ArrayRef =
            Arc::new(StringArray::from(vec![Some("a=1,b=2"), Some("x#9")]));
        let pair_delim: ArrayRef =
            Arc::new(StringArray::from(vec![Some(","), Some(",")]));
        let kv_delim: ArrayRef = Arc::new(StringArray::from(vec![Some("="), Some("#")]));

        let result = str_to_map_inner(&[text, pair_delim, kv_delim]).unwrap();
        let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();

        assert_eq!(map_array.len(), 2);

        // Row 0: "a=1,b=2" with pair=",", kv="="
        let entries0 = get_map_entries(map_array, 0);
        assert_eq!(
            entries0,
            vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2".to_string())),
            ]
        );

        // Row 1: "x#9" with pair=",", kv="#"
        let entries1 = get_map_entries(map_array, 1);
        assert_eq!(entries1, vec![("x".to_string(), Some("9".to_string()))]);
    }

    fn get_map_entries(
        map_array: &MapArray,
        row: usize,
    ) -> Vec<(String, Option<String>)> {
        if map_array.is_null(row) {
            return vec![];
        }
        let start = map_array.value_offsets()[row] as usize;
        let end = map_array.value_offsets()[row + 1] as usize;
        let keys = map_array
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = map_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        (start..end)
            .map(|i| {
                let key = keys.value(i).to_string();
                let value = if values.is_null(i) {
                    None
                } else {
                    Some(values.value(i).to_string())
                };
                (key, value)
            })
            .collect()
    }
}
