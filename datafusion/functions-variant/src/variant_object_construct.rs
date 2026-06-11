use std::sync::Arc;

use arrow::array::StructArray;
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use parquet_variant::{Variant, VariantBuilder};
use parquet_variant_compute::{VariantArray, VariantType};

use crate::shared::{ensure, try_parse_string_scalar, try_parse_variant_scalar};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantObjectConstruct {
    signature: Signature,
}

impl Default for VariantObjectConstruct {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VariantObjectConstruct {
    fn name(&self) -> &str {
        "variant_object_construct"
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
        // validate arguments
        let argument_fields = args.arg_fields;
        let argument_values = args.args;

        ensure(
            argument_fields.len() == argument_values.len(),
            "argument fields and values must be of same length",
        )?;

        ensure(
            argument_fields.len() & 1 == 0,
            "list of arguments must be (key, value) pair",
        )?;

        let (_key_fields, value_fields): (Vec<_>, Vec<_>) = argument_fields
            .into_iter()
            .enumerate()
            .partition(|(i, _)| i & 1 == 0);

        let all_value_fields_have_variant_ext = value_fields
            .iter()
            .all(|(_, v)| matches!(v.extension_type(), VariantType));

        ensure(
            all_value_fields_have_variant_ext,
            "expected all values in (key, value) to have a Variant ext type",
        )?;

        // sometimes field metadata is super redundant. Why check if the key fields are String
        // when you can just try to parse out the ScalarValues?

        let all_arguments_scalar = argument_values
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)));

        // for now, let's just handle the scalar case
        ensure(
            all_arguments_scalar,
            "all arguments must be scalar, todo: how do array arguments look like?",
        )?;

        let (key_values, variant_values): (Vec<_>, Vec<_>) = argument_values
            .into_iter()
            .enumerate()
            .partition(|(i, _)| i & 1 == 0);

        let object_keys = key_values
            .into_iter()
            .map(|(_, v)| {
                let ColumnarValue::Scalar(sv) = v else {
                    unreachable!()
                };

                try_parse_string_scalar(&sv).and_then(|opt| {
                    opt.cloned()
                        .ok_or_else(|| exec_datafusion_err!("expected non null string"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let object_values = variant_values
            .into_iter()
            .map(|(_, v)| {
                let ColumnarValue::Scalar(sv) = v else {
                    unreachable!()
                };

                try_parse_variant_scalar(&sv)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // note, should we have the ability to configure behavior for duplicate keys?

        let mut v = VariantBuilder::new();
        let mut o = v.new_object();

        for (k, v) in object_keys.iter().zip(object_values) {
            let v = v.value(0);
            o.try_insert(k, v)?;
        }

        o.finish();

        let (m, v) = v.finish();

        let v = Variant::new(m.as_ref(), v.as_ref());

        let out: StructArray = VariantArray::from_iter([v]).into();

        Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(out))))
    }
}
