use std::sync::Arc;

use arrow::array::{ArrayRef, ListBuilder, StringBuilder};
use arrow_schema::{DataType, Field};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_common::{exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use parquet_variant::Variant;
use parquet_variant::VariantPath;
use parquet_variant_compute::{
    GetOptions, VariantArray, variant_get as compute_variant_get,
};

use crate::shared::{try_field_as_variant_array, try_parse_string_scalar};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantObjectKeys {
    signature: Signature,
}

impl Default for VariantObjectKeys {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![TypeSignature::Any(1), TypeSignature::Any(2)]),
                Volatility::Immutable,
            ),
        }
    }
}

fn append_keys_from_variant(
    v_opt: Option<Variant>,
    builder: &mut ListBuilder<StringBuilder>,
) {
    match v_opt {
        Some(Variant::Object(obj)) => {
            for (key, _) in obj.iter() {
                builder.values().append_value(key);
            }
            builder.append(true);
        }
        _ => {
            builder.append_null();
        }
    }
}

impl ScalarUDFImpl for VariantObjectKeys {
    fn name(&self) -> &str {
        "variant_object_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (variant_arg, path) = match args.args.as_slice() {
            [variant_arg] => (variant_arg, None),
            [variant_arg, path_arg] => {
                let ColumnarValue::Scalar(path_scalar) = path_arg else {
                    return exec_err!("expected scalar value for path");
                };
                let path = try_parse_string_scalar(path_scalar)?.ok_or_else(|| {
                    exec_datafusion_err!("expected non-null string for path")
                })?;
                (variant_arg, Some(path))
            }
            _ => return exec_err!("expected 1 or 2 arguments"),
        };

        let variant_field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("expected 1 argument field type"))?;

        try_field_as_variant_array(variant_field.as_ref())?;

        let out = match variant_arg {
            ColumnarValue::Scalar(scalar_variant) => {
                let ScalarValue::Struct(struct_arr) = scalar_variant else {
                    return exec_err!("expected variant scalar value");
                };
                let arr: ArrayRef = Arc::clone(struct_arr) as ArrayRef;

                let arr = if let Some(path_str) = path {
                    compute_variant_get(
                        &arr,
                        GetOptions::new_with_path(VariantPath::try_from(
                            path_str.as_str(),
                        )?),
                    )?
                } else {
                    arr
                };

                let variant_array = VariantArray::try_new(arr.as_ref())?;

                let mut builder = ListBuilder::new(StringBuilder::new());
                let v_opt = variant_array.iter().next().flatten();
                append_keys_from_variant(v_opt, &mut builder);

                let list_array = builder.finish();
                ColumnarValue::Scalar(ScalarValue::List(Arc::new(list_array)))
            }
            ColumnarValue::Array(variant_array) => {
                let arr = if let Some(path_str) = path {
                    compute_variant_get(
                        variant_array,
                        GetOptions::new_with_path(VariantPath::try_from(
                            path_str.as_str(),
                        )?),
                    )?
                } else {
                    Arc::clone(variant_array)
                };

                let variant_array = VariantArray::try_new(arr.as_ref())?;
                let mut builder = ListBuilder::new(StringBuilder::new());

                for v_opt in variant_array.iter() {
                    append_keys_from_variant(v_opt, &mut builder);
                }

                let list_array = builder.finish();
                ColumnarValue::Array(Arc::new(list_array) as ArrayRef)
            }
        };

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ListArray, StructArray};
    use arrow_schema::Fields;
    use parquet_variant_compute::VariantType;

    use crate::shared::{
        build_variant_array_from_json, build_variant_array_from_json_array,
    };

    use super::*;

    fn make_args(variant_input: ColumnarValue, path: Option<&str>) -> ScalarFunctionArgs {
        let return_field = Arc::new(Field::new(
            "result",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ));
        let mut arg_fields: Vec<Arc<Field>> = vec![Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        )];
        let mut args = vec![variant_input];

        if let Some(p) = path {
            arg_fields.push(Arc::new(Field::new("path", DataType::Utf8, true)));
            args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                p.to_string(),
            ))));
        }

        ScalarFunctionArgs {
            args,
            return_field,
            arg_fields,
            number_rows: Default::default(),
            config_options: Default::default(),
        }
    }

    fn scalar_from_json(json: &serde_json::Value) -> ColumnarValue {
        let input = build_variant_array_from_json(json);
        ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(input.into())))
    }

    fn extract_keys_from_list(list_array: &ListArray, idx: usize) -> Vec<String> {
        let values = list_array.value(idx);
        let string_array = values
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let mut keys: Vec<String> = (0..string_array.len())
            .map(|i| string_array.value(i).to_string())
            .collect();
        keys.sort();
        keys
    }

    #[test]
    fn test_scalar_object() {
        let udf = VariantObjectKeys::default();
        let args = make_args(
            scalar_from_json(&serde_json::json!({"a": 1, "b": 2, "c": 3})),
            None,
        );
        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Scalar(ScalarValue::List(list_array)) = result else {
            panic!("expected List scalar")
        };

        assert!(!list_array.is_null(0));
        assert_eq!(extract_keys_from_list(&list_array, 0), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_scalar_non_object() {
        let udf = VariantObjectKeys::default();
        let args = make_args(scalar_from_json(&serde_json::json!([1, 2, 3])), None);
        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Scalar(ScalarValue::List(list_array)) = result else {
            panic!("expected List scalar")
        };
        assert!(list_array.is_null(0));
    }

    #[test]
    fn test_scalar_empty_object() {
        let udf = VariantObjectKeys::default();
        let args = make_args(scalar_from_json(&serde_json::json!({})), None);
        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Scalar(ScalarValue::List(list_array)) = result else {
            panic!("expected List scalar")
        };
        assert!(!list_array.is_null(0));
        assert_eq!(list_array.value(0).len(), 0);
    }

    #[test]
    fn test_scalar_with_path() {
        let udf = VariantObjectKeys::default();
        let args = make_args(
            scalar_from_json(&serde_json::json!({
                "user": {"name": "Alice", "age": 30},
                "scores": [1, 2]
            })),
            Some("user"),
        );
        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Scalar(ScalarValue::List(list_array)) = result else {
            panic!("expected List scalar")
        };
        assert!(!list_array.is_null(0));
        assert_eq!(extract_keys_from_list(&list_array, 0), vec!["age", "name"]);
    }

    #[test]
    fn test_scalar_with_path_non_object() {
        let udf = VariantObjectKeys::default();
        let args = make_args(
            scalar_from_json(&serde_json::json!({"items": [1, 2, 3]})),
            Some("items"),
        );
        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Scalar(ScalarValue::List(list_array)) = result else {
            panic!("expected List scalar")
        };
        assert!(list_array.is_null(0));
    }

    #[test]
    fn test_scalar_with_path_missing() {
        let udf = VariantObjectKeys::default();
        let args = make_args(
            scalar_from_json(&serde_json::json!({"a": 1})),
            Some("nonexistent"),
        );
        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Scalar(ScalarValue::List(list_array)) = result else {
            panic!("expected List scalar")
        };
        assert!(list_array.is_null(0));
    }

    #[test]
    fn test_columnar() {
        let input = build_variant_array_from_json_array(&[
            Some(serde_json::json!({"x": 1, "y": 2})),
            Some(serde_json::json!([1, 2, 3])),
            None,
            Some(serde_json::json!({"name": "Alice"})),
        ]);

        let input: StructArray = input.into();
        let variant_input = Arc::new(input) as ArrayRef;

        let udf = VariantObjectKeys::default();
        let args = make_args(ColumnarValue::Array(variant_input), None);
        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Array(array) = result else {
            panic!("expected array result")
        };

        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_array.len(), 4);

        // {"x": 1, "y": 2} -> ["x", "y"]
        assert!(!list_array.is_null(0));
        assert_eq!(extract_keys_from_list(list_array, 0), vec!["x", "y"]);

        // [1, 2, 3] -> null
        assert!(list_array.is_null(1));

        // null -> null
        assert!(list_array.is_null(2));

        // {"name": "Alice"} -> ["name"]
        assert!(!list_array.is_null(3));
        assert_eq!(extract_keys_from_list(list_array, 3), vec!["name"]);
    }

    #[test]
    fn test_columnar_with_path() {
        let input = build_variant_array_from_json_array(&[
            Some(serde_json::json!({"info": {"a": 1, "b": 2}})),
            Some(serde_json::json!({"info": [1, 2]})),
            Some(serde_json::json!({"other": 1})),
        ]);

        let input: StructArray = input.into();
        let variant_input = Arc::new(input) as ArrayRef;

        let udf = VariantObjectKeys::default();
        let args = make_args(ColumnarValue::Array(variant_input), Some("info"));
        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Array(array) = result else {
            panic!("expected array result")
        };

        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_array.len(), 3);

        // {"info": {"a": 1, "b": 2}} with path "info" -> ["a", "b"]
        assert!(!list_array.is_null(0));
        assert_eq!(extract_keys_from_list(list_array, 0), vec!["a", "b"]);

        // {"info": [1, 2]} with path "info" -> null (list, not object)
        assert!(list_array.is_null(1));

        // {"other": 1} with path "info" -> null (path doesn't exist)
        assert!(list_array.is_null(2));
    }
}
