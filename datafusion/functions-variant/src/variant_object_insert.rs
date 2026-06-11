use std::sync::Arc;

use arrow::array::StructArray;
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use parquet_variant::{Variant, VariantBuilder};
use parquet_variant_compute::{VariantArray, VariantType};

use crate::shared::{ensure, try_parse_string_scalar, try_parse_variant_scalar};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantObjectInsert {
    signature: Signature,
}

impl Default for VariantObjectInsert {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VariantObjectInsert {
    fn name(&self) -> &str {
        "variant_object_insert"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "implemented return_field_from_args instead".into(),
        ))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, false),
        ]));

        Ok(Arc::new(
            Field::new(self.name(), data_type, true).with_extension_type(VariantType),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let argument_fields = args.arg_fields;
        let argument_values = args.args;

        ensure(
            argument_fields.len() == argument_values.len(),
            "argument fields and values must be of same length",
        )?;

        let [variant_object_to_update, key, value] = argument_values.as_slice() else {
            return exec_err!("expected 3 arguments");
        };

        {
            let variant_object_to_update_ext =
                matches!(argument_fields[0].extension_type(), VariantType);
            let value_ext = matches!(argument_fields[2].extension_type(), VariantType);

            ensure(
                variant_object_to_update_ext && value_ext,
                "expected extension type of VariantType",
            )?;
        }

        let key = {
            let ColumnarValue::Scalar(key) = key else {
                return exec_err!("expected scalar value for key");
            };

            try_parse_string_scalar(key)?.ok_or_else(|| {
                DataFusionError::Execution("expected non null string".into())
            })?
        };

        let value_array = {
            let ColumnarValue::Scalar(value) = value else {
                return exec_err!("expected scalar value for value");
            };

            try_parse_variant_scalar(value)?
        };
        let value = value_array.value(0);

        match variant_object_to_update {
            ColumnarValue::Scalar(scalar_variant_object_to_update) => {
                let variant_object =
                    try_parse_variant_scalar(scalar_variant_object_to_update)?;
                let variant_object = variant_object.value(0);
                let Variant::Object(variant_object) = variant_object else {
                    return exec_err!("expected variant object");
                };

                let mut v = VariantBuilder::new();
                let mut o = v.new_object();

                o.extend(variant_object.iter());
                o.insert(key, value);

                o.finish();

                let (m, v) = v.finish();
                let v = Variant::try_new(m.as_ref(), v.as_ref())?;

                let out: StructArray = VariantArray::from_iter([v]).into();

                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(out))))
            }
            ColumnarValue::Array(variant_objects_to_update) => {
                let variant_objects = VariantArray::try_new(&variant_objects_to_update)?;

                // grr lifetimes...
                let buffers = variant_objects
                    .iter()
                    .map(|v_opt| {
                        v_opt
                            .map(|variant_object| {
                                let Variant::Object(variant_object) = variant_object
                                else {
                                    return exec_err!("expected variant object");
                                };

                                let mut v = VariantBuilder::new();
                                let mut o = v.new_object();

                                o.extend(variant_object.iter());
                                o.insert(key, value.clone());

                                o.finish();

                                Ok(v.finish())
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;

                let variants = buffers
                    .iter()
                    .map(|opt| {
                        opt.as_ref()
                            .map(|(m, v)| Variant::new(m.as_ref(), v.as_ref()))
                    })
                    .collect::<Vec<_>>();

                let out: StructArray = VariantArray::from_iter(variants).into();

                Ok(ColumnarValue::Array(Arc::new(out)))
            }
        }
    }
}
