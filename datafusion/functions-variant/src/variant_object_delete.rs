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
pub struct VariantObjectDelete {
    signature: Signature,
}

impl Default for VariantObjectDelete {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VariantObjectDelete {
    fn name(&self) -> &str {
        "variant_object_delete"
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

        let [variant_object_to_update, key_to_delete] = argument_values.as_slice() else {
            return exec_err!("expected 2 arguments");
        };

        ensure(
            matches!(argument_fields[0].extension_type(), VariantType),
            "expected extension type of VariantType for variant object argument",
        )?;

        let key = {
            let ColumnarValue::Scalar(key) = key_to_delete else {
                return exec_err!("expected scalar value for key");
            };

            try_parse_string_scalar(key)?.ok_or_else(|| {
                DataFusionError::Execution("expected non null string".into())
            })?
        };

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

                for (k, val) in variant_object.iter() {
                    if k != key.as_str() {
                        o.insert(k, val);
                    }
                }

                o.finish();

                let (m, v) = v.finish();
                let v = Variant::try_new(m.as_ref(), v.as_ref())?;

                let out: StructArray = VariantArray::from_iter([v]).into();

                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(out))))
            }
            ColumnarValue::Array(variant_objects_to_update) => {
                let variant_objects = VariantArray::try_new(&variant_objects_to_update)?;

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

                                for (k, val) in variant_object.iter() {
                                    if k != key.as_str() {
                                        o.insert(k, val);
                                    }
                                }

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
