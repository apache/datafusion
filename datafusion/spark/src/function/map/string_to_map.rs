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
        get_scalar_string(&args[1])?
    } else {
        ",".to_string()
    };

    let kv_delim = if args.len() > 2 {
        get_scalar_string(&args[2])?
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
    let mut null_buffer = vec![true; num_rows];

    for row_idx in 0..num_rows {
        if text_array.is_null(row_idx) {
            null_buffer[row_idx] = false;
            offsets.push(*offsets.last().unwrap());
            continue;
        }

        let text = text_array.value(row_idx);
        if text.is_empty() {
            // Empty string -> map with empty key and NULL value (Spark behavior)
            keys_builder.append_value("");
            values_builder.append_null();
            offsets.push(offsets.last().unwrap() + 1);
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

        offsets.push(offsets.last().unwrap() + count);
    }

    let keys_array: ArrayRef = Arc::new(keys_builder.finish());
    let values_array: ArrayRef = Arc::new(values_builder.finish());

    // Create null buffer
    let null_buffer = arrow::buffer::NullBuffer::from(null_buffer);

    map_from_keys_values_offsets_nulls(
        &keys_array,
        &values_array,
        &offsets,
        &offsets,
        Some(&null_buffer),
        Some(&null_buffer),
    )
}

/// Extract scalar string value from array (assumes all values are the same)
fn get_scalar_string(array: &ArrayRef) -> Result<String> {
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expected StringArray for delimiter".to_string(),
            )
        })?;

    if string_array.len() == 0 {
        return Ok(",".to_string());
    }

    Ok(string_array.value(0).to_string())
}
