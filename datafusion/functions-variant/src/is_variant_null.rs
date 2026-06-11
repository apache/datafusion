use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow_schema::DataType;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_common::{exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use parquet_variant::Variant;
use parquet_variant_compute::VariantArray;

use crate::shared::{try_field_as_variant_array, try_parse_variant_scalar};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct IsVariantNullUdf {
    signature: Signature,
}

impl Default for IsVariantNullUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsVariantNullUdf {
    fn name(&self) -> &str {
        "is_variant_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let variant_field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("expected 1 argument field type"))?;

        try_field_as_variant_array(variant_field.as_ref())?;

        let [variant_arg] = args.args.as_slice() else {
            return exec_err!("expected 1 argument");
        };

        let out = match variant_arg {
            ColumnarValue::Scalar(scalar_variant) => {
                let variant_array = try_parse_variant_scalar(scalar_variant)?;
                let variant = variant_array.value(0);
                let is_variant_null = variant == Variant::Null;

                ColumnarValue::Scalar(ScalarValue::Boolean(Some(is_variant_null)))
            }
            ColumnarValue::Array(variant_array) => {
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;

                let out: BooleanArray = variant_array
                    .iter()
                    .map(|v| v.map(|v| v == Variant::Null))
                    .collect::<Vec<_>>()
                    .into();

                ColumnarValue::Array(Arc::new(out) as ArrayRef)
            }
        };

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::StructArray;
    use arrow_schema::{Field, Fields};
    use parquet_variant_compute::VariantType;

    use crate::shared::{
        build_variant_array_from_json, build_variant_array_from_json_array,
    };

    use super::*;

    #[test]
    fn test_scalar() {
        let expected_json = serde_json::json!(null);
        let input = build_variant_array_from_json(&expected_json);

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = IsVariantNullUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result else {
            panic!("expected valid bool")
        };

        assert!(b);
    }

    #[test]
    fn test_columnar() {
        let input = build_variant_array_from_json_array(&[
            Some(serde_json::json!(null)),
            Some(serde_json::json!(null)),
            Some(serde_json::json!("null")), // this is a ShortString('null')
            Some(serde_json::json!({
                "name": "norm"
            })),
        ]);

        let input: StructArray = input.into();

        let variant_input = Arc::new(input) as ArrayRef;

        let udf = IsVariantNullUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();

        let ColumnarValue::Array(boolean_array) = result else {
            panic!("expected valid bool")
        };

        let bool_array = boolean_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        let bool_array = bool_array.into_iter().collect::<Vec<_>>();

        assert_eq!(
            bool_array,
            vec![Some(true), Some(true), Some(false), Some(false),]
        );
    }
}
