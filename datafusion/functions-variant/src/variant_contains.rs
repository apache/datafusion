use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_common::{exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use parquet_variant::{Variant, VariantPath};
use parquet_variant_compute::VariantArray;

use crate::shared::{
    path_from_scalar, try_field_as_variant_array, try_parse_string_columnar,
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantContainsUdf {
    signature: Signature,
}

impl Default for VariantContainsUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

fn variant_contains(
    variant: Option<&Variant<'_, '_>>,
    path: &VariantPath<'_>,
) -> Option<bool> {
    variant.map(|value| value.get_path(path).is_some())
}

impl ScalarUDFImpl for VariantContainsUdf {
    fn name(&self) -> &str {
        "variant_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (variant_arg, path_arg) = match args.args.as_slice() {
            [variant_arg, path_arg] => (variant_arg, path_arg),
            _ => return exec_err!("expected 2 arguments"),
        };

        let variant_field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("expected argument field"))?;

        try_field_as_variant_array(variant_field.as_ref())?;

        match (variant_arg, path_arg) {
            (ColumnarValue::Array(variant_array), ColumnarValue::Scalar(path_scalar)) => {
                if path_scalar.is_null() {
                    return exec_err!("path argument must be non-null");
                }

                let path = path_from_scalar(path_scalar)?;
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                let values = variant_array
                    .iter()
                    .map(|variant| variant_contains(variant.as_ref(), &path))
                    .collect::<Vec<_>>();

                Ok(ColumnarValue::Array(
                    Arc::new(BooleanArray::from(values)) as ArrayRef
                ))
            }
            (
                ColumnarValue::Scalar(scalar_variant),
                ColumnarValue::Scalar(path_scalar),
            ) => {
                let ScalarValue::Struct(variant_array) = scalar_variant else {
                    return exec_err!("expected struct array");
                };

                if path_scalar.is_null() {
                    return exec_err!("path argument must be non-null");
                }

                let path = path_from_scalar(path_scalar)?;
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                let variant = variant_array.iter().next().flatten();
                let value = variant_contains(variant.as_ref(), &path);

                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(value)))
            }
            (ColumnarValue::Array(variant_array), ColumnarValue::Array(paths)) => {
                if variant_array.len() != paths.len() {
                    return exec_err!(
                        "expected variant array and paths to be of same length"
                    );
                }

                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                let paths = try_parse_string_columnar(paths)?;

                let values = variant_array
                    .iter()
                    .zip(paths)
                    .map(|(maybe_variant, path_str)| {
                        let path_str = path_str.ok_or_else(|| {
                            exec_datafusion_err!("path argument must be non-null")
                        })?;
                        let path = VariantPath::try_from(path_str)
                            .map_err(Into::<DataFusionError>::into)?;

                        Ok(variant_contains(maybe_variant.as_ref(), &path))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(ColumnarValue::Array(
                    Arc::new(BooleanArray::from(values)) as ArrayRef
                ))
            }
            (ColumnarValue::Scalar(scalar_variant), ColumnarValue::Array(paths)) => {
                let ScalarValue::Struct(variant_array) = scalar_variant else {
                    return exec_err!("expected struct array");
                };

                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                let variant = variant_array.iter().next().flatten();
                let paths = try_parse_string_columnar(paths)?;

                let values = paths
                    .into_iter()
                    .map(|path_str| {
                        let path_str = path_str.ok_or_else(|| {
                            exec_datafusion_err!("path argument must be non-null")
                        })?;
                        let path = VariantPath::try_from(path_str)
                            .map_err(Into::<DataFusionError>::into)?;

                        Ok(variant_contains(variant.as_ref(), &path))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(ColumnarValue::Array(
                    Arc::new(BooleanArray::from(values)) as ArrayRef
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
    use arrow_schema::{Field, Fields};
    use parquet_variant_compute::VariantType;

    use crate::shared::{build_variant_array_from_json_array, variant_scalar_from_json};

    use super::*;

    fn arg_fields() -> Vec<Arc<Field>> {
        vec![
            Arc::new(
                Field::new("input", DataType::Struct(Fields::empty()), true)
                    .with_extension_type(VariantType),
            ),
            Arc::new(Field::new("path", DataType::Utf8, true)),
        ]
    }

    #[test]
    fn test_scalar_existing_path_returns_true() {
        let udf = VariantContainsUdf::default();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(variant_scalar_from_json(serde_json::json!({
                    "a": {"b": null}
                }))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a.b".to_string()))),
            ],
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            arg_fields: arg_fields(),
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Boolean(Some(value))) = result else {
            panic!("expected boolean scalar")
        };

        assert!(value);
    }

    #[test]
    fn test_scalar_missing_path_returns_false() {
        let udf = VariantContainsUdf::default();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(variant_scalar_from_json(serde_json::json!({
                    "a": 1
                }))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a.b".to_string()))),
            ],
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            arg_fields: arg_fields(),
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Boolean(Some(value))) = result else {
            panic!("expected boolean scalar")
        };

        assert!(!value);
    }

    #[test]
    fn test_array_paths_and_null_variant() {
        let udf = VariantContainsUdf::default();
        let input = build_variant_array_from_json_array(&[
            Some(serde_json::json!({"a": 1})),
            Some(serde_json::json!({"a": null})),
            None,
        ]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(
                    Arc::new(arrow::array::StructArray::from(input)) as ArrayRef
                ),
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                ])) as ArrayRef),
            ],
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            arg_fields: arg_fields(),
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(values) = result else {
            panic!("expected boolean array")
        };

        let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            values.into_iter().collect::<Vec<_>>(),
            vec![Some(true), Some(true), None]
        );
    }

    #[test]
    fn test_array_variant_scalar_path() {
        let udf = VariantContainsUdf::default();
        let input = build_variant_array_from_json_array(&[
            Some(serde_json::json!({"a": 1})),
            Some(serde_json::json!({"b": 1})),
        ]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(
                    Arc::new(arrow::array::StructArray::from(input)) as ArrayRef
                ),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
            ],
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            arg_fields: arg_fields(),
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(values) = result else {
            panic!("expected boolean array")
        };

        let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            values.into_iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false)]
        );
    }
}
