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

// https://docs.databricks.com/gcp/en/sql/language-manual/functions/to_json

use std::sync::Arc;

use arrow::array::StringViewArray;
use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;

use crate::shared::try_field_as_variant_array;

/// Returns a JSON string from a VariantArray
///
/// ## Arguments
/// - expr: a DataType::Struct expression that represents a VariantArray
/// - options: an optional MAP (note, it seems arrow-rs' parquet-variant is pretty restrictive about the options)
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantToJsonUdf {
    signature: Signature,
}

impl Default for VariantToJsonUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![TypeSignature::Any(1), TypeSignature::Any(2)]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for VariantToJsonUdf {
    fn name(&self) -> &str {
        "variant_to_json"
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

                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v.to_json_string()?)))
            }
            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Struct(_) => {
                    let variant_array = VariantArray::try_new(arr.as_ref())?;

                    let out: StringViewArray = variant_array
                        .iter()
                        .map(|v| v.map(|v| v.to_json_string()).transpose())
                        .collect::<Result<Vec<_>, _>>()?
                        .into();

                    ColumnarValue::Array(Arc::new(out))
                }
                unsupported => return exec_err!("Invalid data type: {unsupported}"),
            },
        };

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{Field, Fields};
    use parquet_variant_compute::VariantType;
    use serde_json::Value;

    use crate::shared::build_variant_array_from_json;

    use super::*;

    #[test]
    fn test_scalar_primitive() {
        let expected_json = serde_json::json!("norm");
        let input = build_variant_array_from_json(&expected_json);

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = VariantToJsonUdf::default();
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

        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(j))) = result else {
            panic!("expected valid json string")
        };

        assert_eq!(j.as_str(), r#""norm""#);
    }

    #[test]
    fn test_variant_to_json_udf_scalar_complex() {
        let expected_json = serde_json::json!({
            "name": "norm",
            "age": 50,
            "list": [false, true, ()]
        });

        let input = build_variant_array_from_json(&expected_json);

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = VariantToJsonUdf::default();

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

        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(j))) = result else {
            panic!("expected valid json string")
        };

        let json: Value = serde_json::from_str(j.as_str()).unwrap();
        assert_eq!(json, expected_json);
    }
}
