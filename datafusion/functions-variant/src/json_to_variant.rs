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

// https://docs.databricks.com/gcp/en/sql/language-manual/functions/parse_json

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, LargeStringArray, StringArray, StringViewArray, StructArray,
};
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature,
};
use parquet_variant_compute::{VariantArrayBuilder, VariantType};
use parquet_variant_json::JsonToVariant as JsonToVariantExt;

use crate::shared::{try_field_as_string, try_parse_string_scalar};

/// Returns a Variant from a JSON string
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct JsonToVariantUdf {
    signature: Signature,
}

impl Default for JsonToVariantUdf {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                ),
                datafusion_expr::Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonToVariantUdf {
    fn name(&self) -> &str {
        "json_to_variant"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, false),
        ])))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let data_type = self.return_type(
            args.arg_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        Ok(Arc::new(
            Field::new(self.name(), data_type, true).with_extension_type(VariantType),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty argument, expected 1 argument"))?;

        try_field_as_string(arg_field.as_ref())?;

        let arg = args
            .args
            .first()
            .ok_or_else(|| exec_datafusion_err!("empty argument, expected 1 argument"))?;

        let out = match arg {
            ColumnarValue::Scalar(scalar_value) => {
                let json_str = try_parse_string_scalar(scalar_value)?;

                let mut builder = VariantArrayBuilder::new(1);

                match json_str {
                    Some(json_str) => builder.append_json(json_str.as_str())?,
                    None => builder.append_null(),
                }

                let struct_array: StructArray = builder.build().into();
                ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(struct_array)))
            }
            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Utf8 => ColumnarValue::Array(from_utf8_arr(arr)?),
                DataType::LargeUtf8 => ColumnarValue::Array(from_large_utf8_arr(arr)?),
                DataType::Utf8View => ColumnarValue::Array(from_utf8view_arr(arr)?),
                _ => return exec_err!("Invalid data type {}", arr.data_type()),
            },
        };

        Ok(out)
    }
}

macro_rules! define_from_string_array {
    ($fn_name:ident, $array_type:ty) => {
        pub(crate) fn $fn_name(arr: &ArrayRef) -> Result<ArrayRef> {
            let arr = arr.as_any().downcast_ref::<$array_type>().ok_or(
                exec_datafusion_err!("Unable to downcast array as expected by type."),
            )?;

            let mut builder = VariantArrayBuilder::new(arr.len());

            for v in arr {
                match v {
                    Some(json_str) => builder.append_json(json_str)?,
                    None => builder.append_null(),
                }
            }

            let variant_array: StructArray = builder.build().into();

            Ok(Arc::new(variant_array) as ArrayRef)
        }
    };
}

define_from_string_array!(from_utf8_arr, StringArray);
define_from_string_array!(from_utf8view_arr, StringViewArray);
define_from_string_array!(from_large_utf8_arr, LargeStringArray);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};
    use parquet_variant::{Variant, VariantBuilder};
    use parquet_variant_compute::VariantArray;

    #[test]
    fn test_json_to_variant_udf_scalar_none() {
        let json_input = ScalarValue::Utf8(None);

        let udf = JsonToVariantUdf::default();
        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));

        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[],
            })
            .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(json_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(sv)) => {
                assert!(sv.is_null(0), "expected null struct");
            }
            _ => panic!("Expected null struct array result"),
        }
    }

    #[test]
    fn test_json_to_variant_udf_scalar_null() {
        let json_input = ScalarValue::Utf8(Some("null".into()));

        let udf = JsonToVariantUdf::default();
        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[],
            })
            .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(json_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(v)) => {
                let variant_array = VariantArray::try_new(v.as_ref()).unwrap();
                let variant = variant_array.value(0);
                assert_eq!(variant, Variant::from(()));
            }
            _ => panic!("Expected scalar BinaryView result"),
        }
    }

    #[test]
    fn test_json_to_variant_udf_scalar_complex() {
        let json_input =
            ScalarValue::Utf8(Some(r#"{"key": 123, "data": [4, 5, "str"]}"#.to_string()));

        let udf = JsonToVariantUdf::default();

        let (expected_m, expected_v) = {
            let mut variant_builder = VariantBuilder::new();
            let mut object_builder = variant_builder.new_object();

            object_builder.insert("key", 123_u8);

            let mut inner_array_builder = object_builder.new_list("data");

            inner_array_builder.append_value(4u8);
            inner_array_builder.append_value(5u8);
            inner_array_builder.append_value("str");

            inner_array_builder.finish();

            object_builder.finish();

            variant_builder.finish()
        };

        let expected_variant = Variant::try_new(&expected_m, &expected_v).unwrap();

        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[],
            })
            .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(json_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(v)) => {
                let variant_array = VariantArray::try_new(v.as_ref()).unwrap();
                let variant = variant_array.value(0);
                assert_eq!(variant, expected_variant);
            }
            _ => panic!("Expected scalar BinaryView result"),
        }
    }

    #[test]
    fn test_json_to_variant_udf_scalar_primitive() {
        let json_input = ScalarValue::Utf8(Some("123".to_string()));

        let udf = JsonToVariantUdf::default();
        let arg_field = Arc::new(Field::new("input", DataType::Utf8, true));
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &[],
            })
            .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(json_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Struct(v)) => {
                let variant_array = VariantArray::try_new(v.as_ref()).unwrap();
                let variant = variant_array.value(0);
                assert_eq!(variant, Variant::from(123_u8));
            }
            _ => panic!("Expected scalar BinaryView result"),
        }
    }
}
