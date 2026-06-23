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

use arrow::array::{ArrayRef, StructArray};
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_common::{exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use parquet_variant_compute::{VariantArray, VariantArrayBuilder, VariantType};

use crate::shared::try_field_as_variant_array;

/// Normalizes a Variant value into a canonical binary form.
///
/// The primary transformation is sorting all object keys alphabetically
/// at every nesting level. This ensures that two variants representing
/// the same logical data (e.g. `{"b":1,"a":2}` vs `{"a":2,"b":1}`)
/// produce identical binary representations, enabling correct equality
/// comparisons, hashing, GROUP BY, and DISTINCT operations.
///
/// Primitive values and list element ordering are preserved as-is.
///
/// ## Limitations
///
/// The `parquet-variant` crate's `VariantBuilder` already sorts object
/// keys in `ObjectBuilder::finish()` and performs a full logical
/// (decode + re-encode) copy when appending `Variant::Object` values.
/// This means normalization works correctly but:
///
/// - **No in-place binary normalization**: Unlike DuckDB's implementation
///   which manipulates the binary buffer directly with varint encoding,
///   we must fully decode each variant and re-encode it through the
///   builder. This is simpler but involves more allocation.
///
/// - **No "already normalized" fast path**: There is no API to inspect
///   whether a variant's object keys are already sorted, so we always
///   pay the full rebuild cost even for already-normalized inputs.
///
/// - **No `append_value_bytes` for normalized output**: The builder's
///   `append_value_bytes` copies raw bytes (skipping re-encoding), which
///   would preserve unsorted key order. We must use `append_variant`
///   (logical copy) to get sorting, which is slower for large variants.
///
/// - **Metadata dictionary is fully rebuilt**: Each output variant gets
///   a fresh metadata dictionary constructed by the builder. DuckDB
///   shares and incrementally builds a single dictionary across the
///   entire vector, which is more memory-efficient for repeated keys.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantNormalizeUdf {
    signature: Signature,
}

impl Default for VariantNormalizeUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VariantNormalizeUdf {
    fn name(&self) -> &str {
        "variant_normalize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, false),
        ])))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let data_type = self.return_type(&[])?;
        Ok(Arc::new(
            Field::new(self.name(), data_type, true).with_extension_type(VariantType),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let variant_field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("expected 1 argument"))?;

        try_field_as_variant_array(variant_field.as_ref())?;

        let [variant_arg] = args.args.as_slice() else {
            return exec_err!("expected 1 argument");
        };

        let out = match variant_arg {
            ColumnarValue::Scalar(scalar_variant) => {
                let ScalarValue::Struct(struct_array) = scalar_variant else {
                    return exec_err!("expected variant struct");
                };

                let variant_array = VariantArray::try_new(struct_array.as_ref())?;
                let mut builder = VariantArrayBuilder::new(1);

                if variant_array.is_null(0) {
                    builder.append_null();
                } else {
                    // append_variant performs a logical copy through ObjectBuilder,
                    // which sorts keys in finish(). This normalizes recursively.
                    builder.append_variant(variant_array.value(0));
                }

                let result: StructArray = builder.build().into();
                ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(result)))
            }
            ColumnarValue::Array(arr) => {
                let variant_array = VariantArray::try_new(arr.as_ref())?;
                let mut builder = VariantArrayBuilder::new(variant_array.len());

                for i in 0..variant_array.len() {
                    if variant_array.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_variant(variant_array.value(i));
                    }
                }

                let result: StructArray = builder.build().into();
                ColumnarValue::Array(Arc::new(result) as ArrayRef)
            }
        };

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet_variant::{Variant, VariantBuilder};
    use parquet_variant_compute::VariantType;

    use crate::shared::{
        build_variant_array_from_json, build_variant_array_from_json_array,
    };

    fn invoke_scalar(json: &serde_json::Value) -> VariantArray {
        let input = build_variant_array_from_json(json);
        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = VariantNormalizeUdf::default();
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[],
            })
            .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(s)) => {
                VariantArray::try_new(s.as_ref()).unwrap()
            }
            _ => panic!("expected scalar struct"),
        }
    }

    fn invoke_array(jsons: &[Option<serde_json::Value>]) -> VariantArray {
        let input = build_variant_array_from_json_array(jsons);
        let input: StructArray = input.into();
        let variant_input = Arc::new(input) as ArrayRef;

        let udf = VariantNormalizeUdf::default();
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[],
            })
            .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(arr) => VariantArray::try_new(arr.as_ref()).unwrap(),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_primitive_passthrough() {
        let result = invoke_scalar(&serde_json::json!(42));
        assert_eq!(result.value(0), Variant::from(42u8));
    }

    #[test]
    fn test_null_passthrough() {
        let result = invoke_scalar(&serde_json::json!(null));
        assert_eq!(result.value(0), Variant::from(()));
    }

    #[test]
    fn test_string_passthrough() {
        let result = invoke_scalar(&serde_json::json!("hello"));
        assert_eq!(result.value(0), Variant::from("hello"));
    }

    #[test]
    fn test_object_keys_sorted() {
        // JSON object with keys in non-alphabetical order
        let result = invoke_scalar(&serde_json::json!({"z": 1, "a": 2, "m": 3}));
        let variant = result.value(0);
        let obj = variant.as_object().unwrap();

        let keys: Vec<&str> = obj.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
    }

    #[test]
    fn test_nested_object_keys_sorted() {
        let result = invoke_scalar(&serde_json::json!({
            "z": {"c": 1, "a": 2},
            "a": {"z": 3, "b": 4}
        }));
        let variant = result.value(0);
        let obj = variant.as_object().unwrap();

        // Top-level keys sorted
        let keys: Vec<&str> = obj.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["a", "z"]);

        // Nested keys sorted
        let val_a = obj.get("a").unwrap();
        let inner_a = val_a.as_object().unwrap();
        let inner_a_keys: Vec<&str> = inner_a.iter().map(|(k, _)| k).collect();
        assert_eq!(inner_a_keys, vec!["b", "z"]);

        let val_z = obj.get("z").unwrap();
        let inner_z = val_z.as_object().unwrap();
        let inner_z_keys: Vec<&str> = inner_z.iter().map(|(k, _)| k).collect();
        assert_eq!(inner_z_keys, vec!["a", "c"]);
    }

    #[test]
    fn test_list_preserved_object_within_sorted() {
        let result = invoke_scalar(&serde_json::json!([
            {"b": 1, "a": 2},
            {"d": 3, "c": 4}
        ]));
        let variant = result.value(0);
        let list = variant.as_list().unwrap();

        // List order preserved
        assert_eq!(list.len(), 2);

        // But object keys within are sorted
        let val0 = list.get(0).unwrap();
        let obj0 = val0.as_object().unwrap();
        let keys0: Vec<&str> = obj0.iter().map(|(k, _)| k).collect();
        assert_eq!(keys0, vec!["a", "b"]);

        let val1 = list.get(1).unwrap();
        let obj1 = val1.as_object().unwrap();
        let keys1: Vec<&str> = obj1.iter().map(|(k, _)| k).collect();
        assert_eq!(keys1, vec!["c", "d"]);
    }

    #[test]
    fn test_deeply_nested() {
        let result = invoke_scalar(&serde_json::json!({
            "z": {
                "y": {
                    "b": [{"d": 1, "c": 2}],
                    "a": "leaf"
                }
            }
        }));
        let variant = result.value(0);

        // Navigate: z -> y -> a (should be sorted before b)
        let obj = variant.as_object().unwrap();
        let z_val = obj.get("z").unwrap();
        let z = z_val.as_object().unwrap();
        let y_val = z.get("y").unwrap();
        let y = y_val.as_object().unwrap();

        let y_keys: Vec<&str> = y.iter().map(|(k, _)| k).collect();
        assert_eq!(y_keys, vec!["a", "b"]);

        // The list element's object should also be sorted
        let b_val = y.get("b").unwrap();
        let b_list = b_val.as_list().unwrap();
        let elem0 = b_list.get(0).unwrap();
        let inner_obj = elem0.as_object().unwrap();
        let inner_keys: Vec<&str> = inner_obj.iter().map(|(k, _)| k).collect();
        assert_eq!(inner_keys, vec!["c", "d"]);
    }

    #[test]
    fn test_columnar_with_nulls() {
        let result = invoke_array(&[
            Some(serde_json::json!({"b": 1, "a": 2})),
            None,
            Some(serde_json::json!(42)),
            Some(serde_json::json!({"z": "last", "a": "first"})),
        ]);

        assert_eq!(result.len(), 4);

        // Row 0: object with sorted keys
        assert!(!result.is_null(0));
        let v0 = result.value(0);
        let obj = v0.as_object().unwrap();
        let keys: Vec<&str> = obj.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["a", "b"]);

        // Row 1: null preserved
        assert!(result.is_null(1));

        // Row 2: primitive preserved
        assert!(!result.is_null(2));
        assert_eq!(result.value(2), Variant::from(42u8));

        // Row 3: object with sorted keys
        assert!(!result.is_null(3));
        let v3 = result.value(3);
        let obj = v3.as_object().unwrap();
        let keys: Vec<&str> = obj.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["a", "z"]);
    }

    #[test]
    fn test_idempotent() {
        // Normalizing an already-normalized variant should produce the same result
        let result1 = invoke_scalar(&serde_json::json!({"z": 1, "a": 2}));
        let variant1 = result1.value(0);

        // Re-normalize by feeding result through again
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant(variant1.clone());
        let intermediate: StructArray = builder.build().into();
        let variant_input = ScalarValue::Struct(Arc::new(intermediate));

        let udf = VariantNormalizeUdf::default();
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[],
            })
            .unwrap();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result2 = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Struct(s)) = result2 else {
            panic!("expected scalar struct");
        };
        let result2 = VariantArray::try_new(s.as_ref()).unwrap();
        let variant2 = result2.value(0);

        assert_eq!(variant1, variant2);
    }

    #[test]
    fn test_return_field_has_variant_extension() {
        let udf = VariantNormalizeUdf::default();
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[arg_field],
                scalar_arguments: &[],
            })
            .unwrap();

        assert!(matches!(return_field.extension_type(), VariantType));
    }

    #[test]
    fn test_manually_constructed_unsorted_variant() {
        // Build a variant with explicitly unsorted keys using VariantBuilder directly.
        // VariantBuilder.finish() sorts the metadata dictionary, but
        // ObjectBuilder.finish() sorts the field order too, so we verify
        // round-tripping through our UDF produces the expected sorted output.
        let mut builder = VariantBuilder::new();
        let mut obj = builder.new_object();
        obj.insert("zebra", 1i8);
        obj.insert("apple", 2i8);
        obj.insert("mango", 3i8);
        obj.finish();
        let (metadata, value) = builder.finish();

        let variant = Variant::try_new(&metadata, &value).unwrap();
        let obj = variant.as_object().unwrap();
        let keys: Vec<&str> = obj.iter().map(|(k, _)| k).collect();
        // ObjectBuilder::finish() already sorts, so even the source is sorted
        assert_eq!(keys, vec!["apple", "mango", "zebra"]);

        // Feed through our UDF and confirm same result
        let mut arr_builder = VariantArrayBuilder::new(1);
        arr_builder.append_variant(variant);
        let input: StructArray = arr_builder.build().into();

        let udf = VariantNormalizeUdf::default();
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[],
            })
            .unwrap();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(input)))],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Struct(s)) = result else {
            panic!("expected scalar struct");
        };
        let result = VariantArray::try_new(s.as_ref()).unwrap();
        let result_val = result.value(0);
        let result_obj = result_val.as_object().unwrap();
        let result_keys: Vec<&str> = result_obj.iter().map(|(k, _)| k).collect();
        assert_eq!(result_keys, vec!["apple", "mango", "zebra"]);
    }
}
