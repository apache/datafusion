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

use arrow::array::StringViewArray;
use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use parquet_variant_compute::VariantArray;

use crate::shared::try_field_as_variant_array;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantPretty {
    signature: Signature,
}

impl Default for VariantPretty {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VariantPretty {
    fn name(&self) -> &str {
        "variant_pretty"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty argument, expected 1 argument"))?;

        try_field_as_variant_array(field.as_ref())?;

        let arg = args
            .args
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty argument, expected 1 argument"))?;

        let out = match arg {
            ColumnarValue::Scalar(scalar) => {
                let ScalarValue::Struct(variant_array) = scalar else {
                    return exec_err!("Unsupported data type: {}", scalar.data_type());
                };

                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                let v = variant_array.value(0);

                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(format!("{v:?}"))))
            }
            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Struct(_) => {
                    let variant_array = VariantArray::try_new(arr.as_ref())?;

                    let out = variant_array
                        .iter()
                        .map(|v| v.map(|v| format!("{v:?}")))
                        .collect::<Vec<_>>();

                    let out: StringViewArray = out.into();

                    ColumnarValue::Array(Arc::new(out))
                }
                unsupported => return exec_err!("Invalid data type: {unsupported}"),
            },
        };

        Ok(out)
    }
}
