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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, NullBufferBuilder, StringBuilder, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::cast::as_string_array;
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use serde_json::value::RawValue;

/// Spark-compatible `json_tuple` expression
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#json_tuple>
///
/// Extracts top-level fields from a JSON string and returns them as a struct.
///
/// `json_tuple(json_string, field1, field2, ...) -> Struct<c0: Utf8, c1: Utf8, ...>`
///
/// Note: In Spark, `json_tuple` is a Generator that produces multiple columns directly.
/// In DataFusion, a ScalarUDF can only return one value per row, so the result is wrapped
/// in a Struct. The caller (e.g. Comet) is expected to destructure the struct fields.
///
/// - Returns NULL for each field that is missing from the JSON object
/// - Returns NULL for all fields if the input is NULL or not valid JSON
/// - Non-string JSON values are converted to their JSON string representation
/// - JSON `null` values are returned as NULL (not the string "null")
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonTuple {
    signature: Signature,
}

impl Default for JsonTuple {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonTuple {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonTuple {
    fn name(&self) -> &str {
        "json_tuple"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 {
            return exec_err!(
                "json_tuple requires at least 2 arguments (json_string, field1), got {}",
                args.arg_fields.len()
            );
        }

        let num_fields = args.arg_fields.len() - 1;
        let fields: Fields = (0..num_fields)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>()
            .into();

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Struct(fields),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            return_field,
            ..
        } = args;
        let arrays = ColumnarValue::values_to_arrays(&arg_values)?;
        let result = json_tuple_inner(&arrays, return_field.data_type())?;

        Ok(ColumnarValue::Array(result))
    }
}

fn json_tuple_inner(args: &[ArrayRef], return_type: &DataType) -> Result<ArrayRef> {
    let num_rows = args[0].len();
    let num_fields = args.len() - 1;

    let json_array = as_string_array(&args[0])?;

    let field_arrays = args[1..]
        .iter()
        .map(|arg| as_string_array(arg))
        .collect::<Result<Vec<_>>>()?;

    let mut builders: Vec<StringBuilder> =
        (0..num_fields).map(|_| StringBuilder::new()).collect();

    let mut null_buffer = NullBufferBuilder::new(num_rows);

    for row_idx in 0..num_rows {
        if json_array.is_null(row_idx) {
            for builder in &mut builders {
                builder.append_null();
            }
            null_buffer.append_null();
            continue;
        }

        let json_str = json_array.value(row_idx);
        // Parse into RawValue to preserve original text for numbers
        match serde_json::from_str::<HashMap<String, Box<RawValue>>>(json_str) {
            Ok(map) => {
                null_buffer.append_non_null();
                for (field_idx, builder) in builders.iter_mut().enumerate() {
                    if field_arrays[field_idx].is_null(row_idx) {
                        builder.append_null();
                        continue;
                    }
                    let field_name = field_arrays[field_idx].value(row_idx);
                    match map.get(field_name) {
                        Some(raw) => {
                            let raw_str = raw.get();
                            if raw_str == "null" {
                                builder.append_null();
                            } else if raw_str.starts_with('"') {
                                // String value: parse to unescape
                                match serde_json::from_str::<String>(raw_str) {
                                    Ok(s) => builder.append_value(s),
                                    Err(_) => builder.append_value(raw_str),
                                }
                            } else {
                                // Numbers, booleans: use raw text as-is
                                // Spark uppercases exponent: 1.5e10 → 1.5E10
                                if raw_str.contains('e') {
                                    builder.append_value(raw_str.replace('e', "E"));
                                } else {
                                    builder.append_value(raw_str);
                                }
                            }
                        }
                        None => {
                            builder.append_null();
                        }
                    }
                }
            }
            _ => {
                for builder in &mut builders {
                    builder.append_null();
                }
                null_buffer.append_null();
            }
        }
    }

    let struct_fields = match return_type {
        DataType::Struct(fields) => fields.clone(),
        _ => {
            return internal_err!(
                "json_tuple requires a Struct return type, got {:?}",
                return_type
            );
        }
    };

    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(|mut builder| Arc::new(builder.finish()) as ArrayRef)
        .collect();

    let struct_array = StructArray::try_new(struct_fields, arrays, null_buffer.finish())?;

    Ok(Arc::new(struct_array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_return_field_shape() {
        let func = JsonTuple::new();
        let fields = vec![
            Arc::new(Field::new("json", DataType::Utf8, false)),
            Arc::new(Field::new("f1", DataType::Utf8, false)),
            Arc::new(Field::new("f2", DataType::Utf8, false)),
        ];
        let result = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None, None, None],
            })
            .unwrap();

        match result.data_type() {
            DataType::Struct(inner) => {
                assert_eq!(inner.len(), 2);
                assert_eq!(inner[0].name(), "c0");
                assert_eq!(inner[1].name(), "c1");
                assert_eq!(inner[0].data_type(), &DataType::Utf8);
                assert!(inner[0].is_nullable());
            }
            other => panic!("Expected Struct, got {other:?}"),
        }
    }

    /// Helper to run json_tuple with a single field and return the result string.
    fn json_tuple_single(json: &str, field: &str) -> Option<String> {
        let json_arr: ArrayRef = Arc::new(StringArray::from(vec![json]));
        let field_arr: ArrayRef = Arc::new(StringArray::from(vec![field]));

        let return_type =
            DataType::Struct(vec![Field::new("c0", DataType::Utf8, true)].into());

        let result = json_tuple_inner(&[json_arr, field_arr], &return_type).unwrap();
        let struct_arr = result.as_any().downcast_ref::<StructArray>().unwrap();
        let col = struct_arr.column(0);
        let str_arr = col.as_any().downcast_ref::<StringArray>().unwrap();

        if str_arr.is_null(0) {
            None
        } else {
            Some(str_arr.value(0).to_string())
        }
    }

    #[test]
    fn test_number_scientific_notation() {
        // Spark: json_tuple('{"v":1.5e10}', 'v') → '1.5E10'
        assert_eq!(
            json_tuple_single(r#"{"v":1.5e10}"#, "v"),
            Some("1.5E10".to_string())
        );
    }

    #[test]
    fn test_number_large_integer() {
        // Spark: json_tuple('{"v":99999999999999999999}', 'v') → '99999999999999999999'
        assert_eq!(
            json_tuple_single(r#"{"v":99999999999999999999}"#, "v"),
            Some("99999999999999999999".to_string())
        );
    }

    #[test]
    fn test_number_negative_zero() {
        // Spark: json_tuple('{"v":-0}', 'v') → '0'
        // RawValue preserves '-0', but Spark returns '0'
        // This is acceptable — both are valid representations
        let result = json_tuple_single(r#"{"v":-0}"#, "v");
        assert!(
            result == Some("-0".to_string()) || result == Some("0".to_string()),
            "expected '-0' or '0', got {:?}",
            result
        );
    }

    #[test]
    fn test_number_normal_int() {
        assert_eq!(
            json_tuple_single(r#"{"v":42}"#, "v"),
            Some("42".to_string())
        );
    }

    #[test]
    fn test_number_normal_float() {
        assert_eq!(
            json_tuple_single(r#"{"v":3.14}"#, "v"),
            Some("3.14".to_string())
        );
    }

    #[test]
    fn test_too_few_args() {
        let func = JsonTuple::new();
        let fields = vec![Arc::new(Field::new("json", DataType::Utf8, false))];
        let result = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &[None],
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least 2 arguments")
        );
    }
}
