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

use arrow::array::{Array, ArrayRef, AsArray, StructArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use parquet_variant::Variant;
use parquet_variant_compute::{VariantArray, VariantArrayBuilder, cast_to_variant};

use crate::shared::{try_parse_binary_columnar, try_parse_binary_scalar};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CastToVariantUdf {
    signature: Signature,
}

impl Default for CastToVariantUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl CastToVariantUdf {
    fn canonical_variant_data_type() -> DataType {
        VariantArrayBuilder::new(0).build().data_type().clone()
    }

    fn canonical_return_field(name: &str) -> Field {
        VariantArrayBuilder::new(0)
            .build()
            .field(name.to_string())
            .with_nullable(true)
    }

    fn append_variant_or_null(
        builder: &mut VariantArrayBuilder,
        metadata: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<()> {
        match (metadata, value) {
            (Some(m), Some(v)) if !m.is_empty() && !v.is_empty() => {
                builder.append_variant(Variant::try_new(m, v)?);
            }
            _ => builder.append_null(),
        }

        Ok(())
    }

    fn from_metadata_value(
        metadata_argument: &ColumnarValue,
        variant_argument: &ColumnarValue,
    ) -> Result<ColumnarValue> {
        let out = match (metadata_argument, variant_argument) {
            (ColumnarValue::Array(metadata_array), ColumnarValue::Array(value_array)) => {
                if metadata_array.len() != value_array.len() {
                    return exec_err!(
                        "expected metadata array to be of same length as variant array"
                    );
                }

                let metadata_array = try_parse_binary_columnar(metadata_array)?;
                let value_array = try_parse_binary_columnar(value_array)?;

                let mut builder = VariantArrayBuilder::new(metadata_array.len());
                for (m, v) in metadata_array.into_iter().zip(value_array) {
                    Self::append_variant_or_null(&mut builder, m, v)?;
                }
                let out: StructArray = builder.build().into();

                ColumnarValue::Array(Arc::new(out) as ArrayRef)
            }
            (
                ColumnarValue::Scalar(metadata_value),
                ColumnarValue::Array(value_array),
            ) => {
                let metadata = try_parse_binary_scalar(metadata_value)?;
                let value_array = try_parse_binary_columnar(value_array)?;

                let mut builder = VariantArrayBuilder::new(value_array.len());
                for v in value_array {
                    Self::append_variant_or_null(
                        &mut builder,
                        metadata.map(|m| m.as_slice()),
                        v,
                    )?;
                }
                let arr: StructArray = builder.build().into();

                ColumnarValue::Array(Arc::new(arr) as ArrayRef)
            }
            (
                ColumnarValue::Scalar(metadata_value),
                ColumnarValue::Scalar(value_scalar),
            ) => {
                let metadata = try_parse_binary_scalar(metadata_value)?;
                let value = try_parse_binary_scalar(value_scalar)?;

                match (metadata, value) {
                    (Some(m), Some(v)) if !m.is_empty() && !v.is_empty() => {
                        let mut b = VariantArrayBuilder::new(1);
                        b.append_variant(Variant::try_new(m.as_slice(), v.as_slice())?);
                        let arr: StructArray = b.build().into();

                        ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(arr)))
                    }
                    _ => ColumnarValue::Scalar(ScalarValue::Null),
                }
            }
            (
                ColumnarValue::Array(metadata_array),
                ColumnarValue::Scalar(value_scalar),
            ) => {
                let metadata_array = try_parse_binary_columnar(metadata_array)?;
                let value = try_parse_binary_scalar(value_scalar)?;

                let mut builder = VariantArrayBuilder::new(metadata_array.len());
                for m in metadata_array {
                    Self::append_variant_or_null(
                        &mut builder,
                        m,
                        value.map(|v| v.as_slice()),
                    )?;
                }
                let arr: StructArray = builder.build().into();

                ColumnarValue::Array(Arc::new(arr))
            }
        };

        Ok(out)
    }

    fn from_array(array: &ArrayRef) -> Result<ColumnarValue> {
        // If the array is already a Variant array, pass it through unchanged
        if let Some(struct_array) = array.as_struct_opt()
            && VariantArray::try_new(struct_array).is_ok()
        {
            return Ok(ColumnarValue::Array(Arc::clone(array)));
        }

        let variant_array = cast_to_variant(array.as_ref())?;
        let struct_array: StructArray = variant_array.into();

        Ok(ColumnarValue::Array(Arc::new(struct_array)))
    }

    fn from_scalar_value(scalar_value: &ScalarValue) -> Result<ColumnarValue> {
        if let ScalarValue::Struct(struct_array) = scalar_value
            && VariantArray::try_new(struct_array.as_ref()).is_ok()
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::clone(
                struct_array,
            ))));
        }

        let array = scalar_value.to_array_of_size(1)?;
        let variant_array = cast_to_variant(array.as_ref())?;
        let struct_array: StructArray = variant_array.into();

        Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(
            struct_array,
        ))))
    }
}

impl ScalarUDFImpl for CastToVariantUdf {
    fn name(&self) -> &str {
        "cast_to_variant"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Self::canonical_variant_data_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        Ok(Arc::new(Self::canonical_return_field(self.name())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.as_slice() {
            [metadata_value, variant_value] => {
                Self::from_metadata_value(metadata_value, variant_value)
            }
            [ColumnarValue::Scalar(scalar_value)] => {
                Self::from_scalar_value(scalar_value)
            }
            [ColumnarValue::Array(array)] => Self::from_array(array),
            _ => exec_err!("unrecognized argument"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Fields;
    use parquet_variant_compute::VariantType;

    #[test]
    fn test_return_field_extension_type() {
        let udf = CastToVariantUdf::default();
        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));

        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(matches!(return_field.extension_type(), VariantType));
        assert_eq!(
            return_field.data_type(),
            &DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::BinaryView, false),
                Field::new("value", DataType::BinaryView, false),
            ]))
        );
    }
}
