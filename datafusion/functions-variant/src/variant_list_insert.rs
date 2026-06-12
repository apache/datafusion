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

use arrow::array::StructArray;
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use parquet_variant::{Variant, VariantBuilder};
use parquet_variant_compute::{VariantArray, VariantType};

use crate::shared::{ensure, try_parse_variant_scalar};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantListInsert {
    signature: Signature,
}

impl Default for VariantListInsert {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VariantListInsert {
    fn name(&self) -> &str {
        "variant_list_insert"
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

        let [variant_list_to_update, element_to_append] = argument_values.as_slice()
        else {
            return exec_err!("expected 2 arguments");
        };

        let all_arguments_variant_field = argument_fields
            .iter()
            .all(|f| matches!(f.extension_type(), VariantType));

        ensure(
            all_arguments_variant_field,
            "expected all arguments to have the VariantType extension",
        )?;

        match (variant_list_to_update, element_to_append) {
            (
                ColumnarValue::Scalar(scalar_variant_list),
                ColumnarValue::Scalar(scalar_element_to_append),
            ) => {
                let variant_list = try_parse_variant_scalar(scalar_variant_list)?;
                let variant_list = variant_list.value(0);

                let variant_to_insert =
                    try_parse_variant_scalar(scalar_element_to_append)?;
                let variant_to_insert = variant_to_insert.value(0);

                let out: StructArray = {
                    let (m, v) = create_variant_list_with_new_elements(
                        variant_list,
                        [variant_to_insert].into_iter(),
                    )?;

                    VariantArray::from_iter([Variant::new(m.as_ref(), v.as_ref())]).into()
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(out))))
            }
            (
                ColumnarValue::Array(variant_lists),
                ColumnarValue::Scalar(scalar_element_to_append),
            ) => {
                let variant_array = VariantArray::try_new(&variant_lists)?;

                let variant_to_insert =
                    try_parse_variant_scalar(scalar_element_to_append)?;
                let variant_to_insert = variant_to_insert.value(0);

                // note: grr lifetimes...
                let buffers = variant_array
                    .iter()
                    .map(|v| {
                        v.map(|v| {
                            create_variant_list_with_new_elements(
                                v,
                                [variant_to_insert.clone()].into_iter(),
                            )
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

                Ok(ColumnarValue::Array(Arc::new(out) as _))
            }

            (
                ColumnarValue::Array(variant_lists),
                ColumnarValue::Array(elements_to_append),
            ) => {
                let variant_lists = VariantArray::try_new(&variant_lists)?;
                let elements_to_append = VariantArray::try_new(&elements_to_append)?;

                ensure(
                    variant_lists.len() == elements_to_append.len(),
                    "expected arguments to have the same length",
                )?;

                let mut buffers = Vec::with_capacity(variant_lists.len());

                for (variant_list_to_update, element_to_append) in
                    variant_lists.iter().zip(elements_to_append.iter())
                {
                    match (variant_list_to_update, element_to_append) {
                        (Some(variant_list), Some(element_to_append)) => {
                            let (m, v) = create_variant_list_with_new_elements(
                                variant_list,
                                [element_to_append].into_iter(),
                            )?;

                            buffers.push(Some((m, v)));
                        }
                        (None, None) => buffers.push(None),
                        (Some(variant_list), None) => {
                            let mut v = VariantBuilder::new();
                            v.append_value(variant_list);

                            buffers.push(Some(v.finish()))
                        }
                        (None, Some(element_to_append)) => {
                            let mut v = VariantBuilder::new();
                            let mut l = v.new_list();

                            l.append_value(element_to_append);
                            l.finish();

                            buffers.push(Some(v.finish()));
                        }
                    };
                }

                let variants = buffers
                    .iter()
                    .map(|opt| {
                        opt.as_ref()
                            .map(|(m, v)| Variant::new(m.as_ref(), v.as_ref()))
                    })
                    .collect::<Vec<_>>();

                let out: StructArray = VariantArray::from_iter(variants).into();

                Ok(ColumnarValue::Array(Arc::new(out) as _))
            }
            (ColumnarValue::Scalar(_), ColumnarValue::Array(_)) => {
                exec_err!("unsupported argument")
            }
        }
    }
}

// note: I wonder if we can abstract this away
// it would be good to profile and see if this pocket of code is slow
fn create_variant_list_with_new_elements<'m, 'v>(
    variant_list: Variant,
    elements_to_insert: impl Iterator<Item = Variant<'m, 'v>>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let Variant::List(variant_list) = variant_list else {
        return exec_err!("expected variant list");
    };

    // note: I wonder if we can abstract this away
    // it would be good to profile and see if this pocket of code is slow
    let mut v = VariantBuilder::new();
    let mut l = v.new_list();

    l.extend(variant_list.iter());

    for v in elements_to_insert {
        l.append_value(v);
    }

    l.finish();

    Ok(v.finish())
}
