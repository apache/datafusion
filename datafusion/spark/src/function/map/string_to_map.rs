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

use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use crate::function::map::utils::{
    map_from_keys_values_offsets_nulls, map_type_from_key_value_types,
};

/// Spark-compatible `string_to_map` expression
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
/// Currently uses LAST_WIN behavior (last value wins for duplicate keys).
///
/// TODO: Support `spark.sql.mapKeyDedupPolicy` config (EXCEPTION vs LAST_WIN).
/// Spark 3.0+ defaults to EXCEPTION. See:
/// <https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4502-L4511>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStringToMap {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkStringToMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStringToMap {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // string_to_map(text)
                    TypeSignature::String(1),
                    // string_to_map(text, pairDelim)
                    TypeSignature::String(2),
                    // string_to_map(text, pairDelim, keyValueDelim)
                    TypeSignature::String(3),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("str_to_map")],
        }
    }
}

impl ScalarUDFImpl for SparkStringToMap {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "string_to_map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
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
        make_scalar_function(string_to_map_inner, vec![])(&args.args)
    }
}

fn string_to_map_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let text_array = &args[0];

    // Get delimiters with defaults
    let pair_delim = if args.len() > 1 {
        extract_delimiter_from_string_array(&args[1])?
    } else {
        ",".to_string()
    };

    let kv_delim = if args.len() > 2 {
        extract_delimiter_from_string_array(&args[2])?
    } else {
        ":".to_string()
    };

    // Process each row
    let text_array = text_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expected StringArray for text argument".to_string(),
            )
        })?;

    let num_rows = text_array.len();
    let mut keys_builder = StringBuilder::new();
    let mut values_builder = StringBuilder::new();
    let mut offsets: Vec<i32> = vec![0];
    let mut current_offset: i32 = 0;
    let mut null_buffer = vec![true; num_rows];

    for row_idx in 0..num_rows {
        if text_array.is_null(row_idx) {
            null_buffer[row_idx] = false;
            offsets.push(current_offset);
            continue;
        }

        let text = text_array.value(row_idx);
        if text.is_empty() {
            // Empty string -> map with empty key and NULL value (Spark behavior)
            keys_builder.append_value("");
            values_builder.append_null();
            current_offset += 1;
            offsets.push(current_offset);
            continue;
        }

        let mut count = 0;
        // Split text into key-value pairs using pair_delim.
        // Example: "a:1,b:2" with pair_delim="," -> ["a:1", "b:2"]
        for pair in text.split(&pair_delim) {
            // Skip empty pairs (e.g., from "a:1,,b:2" -> ["a:1", "", "b:2"])
            if pair.is_empty() {
                continue;
            }

            // Split each pair into key and value using kv_delim.
            // splitn(2, ...) ensures we only split on the FIRST delimiter.
            // Example: "a:1:2" with kv_delim=":" -> ["a", "1:2"] (value keeps extra colons)
            //
            // kv[0] = key (always present)
            // kv[1] = value (may not exist if no delimiter found)
            //
            // Examples:
            //   "a:1"   -> kv = ["a", "1"]   -> key="a", value=Some("1")
            //   "a"     -> kv = ["a"]        -> key="a", value=None
            //   "a:"    -> kv = ["a", ""]    -> key="a", value=Some("")
            //   ":1"    -> kv = ["", "1"]    -> key="",  value=Some("1")
            let mut kv_iter = pair.splitn(2, &kv_delim);
            let key = kv_iter.next().unwrap_or("");
            let value = kv_iter.next();

            keys_builder.append_value(key);
            if let Some(v) = value {
                values_builder.append_value(v);
            } else {
                values_builder.append_null();
            }
            count += 1;
        }

        current_offset += count;
        offsets.push(current_offset);
    }

    let keys_array: ArrayRef = Arc::new(keys_builder.finish());
    let values_array: ArrayRef = Arc::new(values_builder.finish());

    // Create null buffer
    let null_buffer = NullBuffer::from(null_buffer);

    map_from_keys_values_offsets_nulls(
        &keys_array,
        &values_array,
        &offsets,
        &offsets,
        Some(&null_buffer),
        Some(&null_buffer),
    )
}

/// Extract delimiter value from [`StringArray`].
fn extract_delimiter_from_string_array(array: &ArrayRef) -> Result<String> {
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expected StringArray for delimiter".to_string(),
            )
        })?;

    assert!(!string_array.is_empty(), "Delimiter array should not be empty");

    // In columnar execution, scalar delimiter is expanded to array to match batch size.
    // All elements are the same, so we just take the first element.
    Ok(string_array.value(0).to_string())
}


#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::MapArray;

    /// Helper to extract keys and values from MapArray for assertions
    fn get_map_entries(map_array: &MapArray, row: usize) -> Vec<(String, Option<String>)> {
        if map_array.is_null(row) {
            return vec![];
        }
        let start = map_array.value_offsets()[row] as usize;
        let end = map_array.value_offsets()[row + 1] as usize;
        let keys = map_array.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let values = map_array.values().as_any().downcast_ref::<StringArray>().unwrap();

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

    #[test]
    fn test_extract_delimiter_from_string_array() {
        // Normal case - single element
        let delim: ArrayRef = Arc::new(StringArray::from(vec!["&"]));
        let result = extract_delimiter_from_string_array(&delim).unwrap();
        assert_eq!(result, "&");

        // Multi-char delimiter
        let delim: ArrayRef = Arc::new(StringArray::from(vec!["&&"]));
        let result = extract_delimiter_from_string_array(&delim).unwrap();
        assert_eq!(result, "&&");

        // Expanded scalar case - multiple elements (all same value).
        // This happens when the scalar delimiter is expanded to match batch size
        let delim: ArrayRef = Arc::new(StringArray::from(vec!["=", "=", "="]));
        let result = extract_delimiter_from_string_array(&delim).unwrap();
        assert_eq!(result, "=");
    }

    // Table-driven tests for string_to_map
    // Test cases derived from Spark ComplexTypeSuite:
    // https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ComplexTypeSuite.scala#L525-L618
    #[test]
    fn test_string_to_map_cases() {
        struct TestCase {
            name: &'static str,
            input: Option<&'static str>,  // None = NULL input
            pair_delim: Option<&'static str>,
            kv_delim: Option<&'static str>,
            expected: Option<Vec<(&'static str, Option<&'static str>)>>,  // None = NULL output
        }

        let cases = vec![
            TestCase {
                name: "s0: basic default delimiters",
                input: Some("a:1,b:2,c:3"),
                pair_delim: None,
                kv_delim: None,
                expected: Some(vec![("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]),
            },
            TestCase {
                name: "s1: preserve spaces in values",
                input: Some("a: ,b:2"),
                pair_delim: None,
                kv_delim: None,
                expected: Some(vec![("a", Some(" ")), ("b", Some("2"))]),
            },
            TestCase {
                name: "s2: custom kv delimiter '='",
                input: Some("a=1,b=2,c=3"),
                pair_delim: Some(","),
                kv_delim: Some("="),
                expected: Some(vec![("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]),
            },
            TestCase {
                name: "s3: empty string",
                input: Some(""),
                pair_delim: Some(","),
                kv_delim: Some("="),
                expected: Some(vec![("", None)]),
            },
            TestCase {
                name: "s4: custom pair delimiter '_'",
                input: Some("a:1_b:2_c:3"),
                pair_delim: Some("_"),
                kv_delim: Some(":"),
                expected: Some(vec![("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]),
            },
            TestCase {
                name: "s5: single key no value",
                input: Some("a"),
                pair_delim: None,
                kv_delim: None,
                expected: Some(vec![("a", None)]),
            },
            TestCase {
                name: "s6: custom delimiters '&' and '='",
                input: Some("a=1&b=2&c=3"),
                pair_delim: Some("&"),
                kv_delim: Some("="),
                expected: Some(vec![("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]),
            },
            TestCase {
                name: "null input returns null",
                input: None,
                pair_delim: None,
                kv_delim: None,
                expected: None,
            },
        ];

        for case in cases {
            let text: ArrayRef = Arc::new(StringArray::from(vec![case.input]));
            let args: Vec<ArrayRef> = match (case.pair_delim, case.kv_delim) {
                (Some(p), Some(k)) => vec![
                    text,
                    Arc::new(StringArray::from(vec![p])),
                    Arc::new(StringArray::from(vec![k])),
                ],
                _ => vec![text],
            };

            let result = string_to_map_inner(&args).unwrap();
            let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();

            assert_eq!(map_array.len(), 1, "case: {}", case.name);

            match case.expected {
                None => {
                    // Expected NULL output
                    assert!(map_array.is_null(0), "case: {} expected NULL", case.name);
                }
                Some(expected_entries) => {
                    assert!(!map_array.is_null(0), "case: {} unexpected NULL", case.name);
                    let entries = get_map_entries(map_array, 0);
                    let expected: Vec<(String, Option<String>)> = expected_entries
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.map(|s| s.to_string())))
                        .collect();
                    assert_eq!(entries, expected, "case: {}", case.name);
                }
            }
        }
    }

    // Multi-row test showing Arrow array structure
    // Input: ["a:1,b:2", "x:9", NULL]
    //
    // Arrow MapArray internal structure:
    //   keys:    ["a", "b", "x"]  (flat array of all keys)
    //   values:  ["1", "2", "9"]  (flat array of all values)
    //   offsets: [0, 2, 3, 3]    (marks boundaries between rows)
    //
    // How to read offsets:
    //   Row 0: keys[0..2] = ["a", "b"], values[0..2] = ["1", "2"] -> {a: 1, b: 2}
    //   Row 1: keys[2..3] = ["x"],      values[2..3] = ["9"]      -> {x: 9}
    //   Row 2: NULL (offset unchanged: 3..3 = empty)
    #[test]
    fn test_multi_row_array_structure() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a:1,b:2"),
            Some("x:9"),
            None,
        ]));

        let result = string_to_map_inner(&[text]).unwrap();
        let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();

        // 3 rows in output
        assert_eq!(map_array.len(), 3);

        // Row 0: {a: 1, b: 2}
        assert!(!map_array.is_null(0));
        let entries = get_map_entries(map_array, 0);
        assert_eq!(entries, vec![
            ("a".to_string(), Some("1".to_string())),
            ("b".to_string(), Some("2".to_string())),
        ]);

        // Row 1: {x: 9}
        assert!(!map_array.is_null(1));
        let entries = get_map_entries(map_array, 1);
        assert_eq!(entries, vec![("x".to_string(), Some("9".to_string()))]);

        // Row 2: NULL
        assert!(map_array.is_null(2));
    }
}