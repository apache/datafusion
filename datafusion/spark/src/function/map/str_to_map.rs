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

use arrow::array::{
    Array, ArrayRef, MapBuilder, MapFieldNames, StringArrayType, StringBuilder,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{
    as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

use crate::function::map::utils::map_type_from_key_value_types;

const DEFAULT_PAIR_DELIM: &str = ",";
const DEFAULT_KV_DELIM: &str = ":";

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
///
/// TODO: Support configurable `spark.sql.mapKeyDedupPolicy` (LAST_WIN) in a follow-up PR.
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
            DataType::Utf8 => str_to_map_impl(as_string_array(&args[0])?, None, None),
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

    // Precompute combined null buffer from all input arrays.
    // NullBuffer::union performs a bitmap-level AND, which is more efficient
    // than checking per-row nullability inline.
    let text_nulls = text_array.nulls().cloned();
    let pair_nulls = pair_delim_array.and_then(|a| a.nulls().cloned());
    let kv_nulls = kv_delim_array.and_then(|a| a.nulls().cloned());
    let combined_nulls = [text_nulls.as_ref(), pair_nulls.as_ref(), kv_nulls.as_ref()]
        .into_iter()
        .fold(None, |acc, nulls| NullBuffer::union(acc.as_ref(), nulls));

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

    let mut seen_keys = HashSet::new();
    for row_idx in 0..num_rows {
        if combined_nulls.as_ref().is_some_and(|n| n.is_null(row_idx)) {
            map_builder.append(false)?;
            continue;
        }

        // Per-row delimiter extraction
        let pair_delim =
            pair_delim_array.map_or(DEFAULT_PAIR_DELIM, |a| a.value(row_idx));
        let kv_delim = kv_delim_array.map_or(DEFAULT_KV_DELIM, |a| a.value(row_idx));

        let text = text_array.value(row_idx);
        if text.is_empty() {
            // Empty string -> map with empty key and NULL value (Spark behavior)
            map_builder.keys().append_value("");
            map_builder.values().append_null();
            map_builder.append(true)?;
            continue;
        }

        seen_keys.clear();
        for pair in text.split(pair_delim) {
            if pair.is_empty() {
                continue;
            }

            let mut kv_iter = pair.splitn(2, kv_delim);
            let key = kv_iter.next().unwrap_or("");
            let value = kv_iter.next();

            // TODO: Support LAST_WIN policy via spark.sql.mapKeyDedupPolicy config
            // EXCEPTION policy: error on duplicate keys (Spark 3.0+ default)
            if !seen_keys.insert(key) {
                return exec_err!(
                    "Duplicate map key '{key}' was found, please check the input data. \
                    If you want to remove the duplicated keys, you can set \
                    spark.sql.mapKeyDedupPolicy to \"LAST_WIN\" so that the key \
                    inserted at last takes precedence."
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
