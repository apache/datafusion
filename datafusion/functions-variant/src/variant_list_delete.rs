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
pub struct VariantListDelete {
    signature: Signature,
}

impl Default for VariantListDelete {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

// just a helper to parse index elements
fn try_parse_index_scalar(scalar: &ScalarValue) -> Result<usize> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(*v as usize),
        ScalarValue::Int16(Some(v)) => Ok(*v as usize),
        ScalarValue::Int32(Some(v)) => Ok(*v as usize),
        ScalarValue::Int64(Some(v)) => Ok(*v as usize),
        _ => exec_err!("expected non-null integer scalar for index"),
    }
}

fn delete_list_element(
    variant_list: Variant,
    index: usize,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let Variant::List(variant_list) = variant_list else {
        return exec_err!("expected variant list");
    };

    if index >= variant_list.len() {
        return exec_err!(
            "index {} oob for list of length {}",
            index,
            variant_list.len()
        );
    }

    let mut v = VariantBuilder::new();
    let mut l = v.new_list();

    for (i, val) in variant_list.iter().enumerate() {
        if i != index {
            l.append_value(val);
        }
    }

    l.finish();

    Ok(v.finish())
}

impl ScalarUDFImpl for VariantListDelete {
    fn name(&self) -> &str {
        "variant_list_delete"
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

        let [variant_list_to_update, index_to_delete] = argument_values.as_slice() else {
            return exec_err!("expected 2 arguments");
        };

        ensure(
            matches!(argument_fields[0].extension_type(), VariantType),
            "expected extension type of VariantType for variant list argument",
        )?;

        let index = {
            let ColumnarValue::Scalar(index) = index_to_delete else {
                return exec_err!("expected scalar value for index");
            };

            try_parse_index_scalar(index)?
        };

        match variant_list_to_update {
            ColumnarValue::Scalar(scalar_variant_list) => {
                let variant_list = try_parse_variant_scalar(scalar_variant_list)?;
                let variant_list = variant_list.value(0);

                let (m, v) = delete_list_element(variant_list, index)?;
                // safety: delete_list_element uses the list builder to construct a valid variant ahead of time
                let v = Variant::new(m.as_ref(), v.as_ref());

                let out: StructArray = VariantArray::from_iter([v]).into();

                Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(out))))
            }
            ColumnarValue::Array(variant_lists) => {
                let variant_array = VariantArray::try_new(&variant_lists)?;

                let buffers = variant_array
                    .iter()
                    .map(|v_opt| {
                        v_opt
                            .map(|variant_list| delete_list_element(variant_list, index))
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
