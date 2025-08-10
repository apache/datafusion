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

use std::any::Any;
use std::sync::Arc;

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};
use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use arrow::{
    array::{as_dictionary_array, as_largestring_array, as_string_array},
    datatypes::Int32Type,
};
use datafusion_common::cast::as_string_view_array;
use datafusion_common::{
    cast::{as_binary_array, as_fixed_size_binary_array, as_int64_array},
    exec_err, DataFusionError,
};
use datafusion_expr::Signature;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use std::fmt::Write;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#hex>
#[derive(Debug)]
pub struct SparkHex {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkHex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkHex {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkHex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        spark_hex(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(
        &self,
        arg_types: &[DataType],
    ) -> datafusion_common::Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err("hex", (1, 1), arg_types.len()));
        }
        match &arg_types[0] {
            DataType::Int64
            | DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary => Ok(vec![arg_types[0].clone()]),
            DataType::Dictionary(key_type, value_type) => match value_type.as_ref() {
                DataType::Int64
                | DataType::Utf8
                | DataType::Utf8View
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary => Ok(vec![arg_types[0].clone()]),
                other => {
                    if other.is_numeric() {
                        Ok(vec![DataType::Dictionary(
                            key_type.clone(),
                            Box::new(DataType::Int64),
                        )])
                    } else {
                        Err(unsupported_data_type_exec_err(
                            "hex",
                            "Numeric, String, or Binary",
                            &arg_types[0],
                        ))
                    }
                }
            },
            other => {
                if other.is_numeric() {
                    Ok(vec![DataType::Int64])
                } else {
                    Err(unsupported_data_type_exec_err(
                        "hex",
                        "Numeric, String, or Binary",
                        &arg_types[0],
                    ))
                }
            }
        }
    }
}

fn hex_int64(num: i64) -> String {
    format!("{num:X}")
}

#[inline(always)]
fn hex_encode<T: AsRef<[u8]>>(data: T, lower_case: bool) -> String {
    let mut s = String::with_capacity(data.as_ref().len() * 2);
    if lower_case {
        for b in data.as_ref() {
            // Writing to a string never errors, so we can unwrap here.
            write!(&mut s, "{b:02x}").unwrap();
        }
    } else {
        for b in data.as_ref() {
            // Writing to a string never errors, so we can unwrap here.
            write!(&mut s, "{b:02X}").unwrap();
        }
    }
    s
}

#[inline(always)]
fn hex_bytes<T: AsRef<[u8]>>(
    bytes: T,
    lowercase: bool,
) -> Result<String, std::fmt::Error> {
    let hex_string = hex_encode(bytes, lowercase);
    Ok(hex_string)
}

/// Spark-compatible `hex` function
pub fn spark_hex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    compute_hex(args, false)
}

/// Spark-compatible `sha2` function
pub fn spark_sha2_hex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    compute_hex(args, true)
}

pub fn compute_hex(
    args: &[ColumnarValue],
    lowercase: bool,
) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "hex expects exactly one argument".to_string(),
        ));
    }

    let input = match &args[0] {
        ColumnarValue::Scalar(value) => ColumnarValue::Array(value.to_array()?),
        ColumnarValue::Array(_) => args[0].clone(),
    };

    match &input {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 => {
                let array = as_int64_array(array)?;

                let hexed_array: StringArray =
                    array.iter().map(|v| v.map(hex_int64)).collect();

                Ok(ColumnarValue::Array(Arc::new(hexed_array)))
            }
            DataType::Utf8 => {
                let array = as_string_array(array);

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(|b| hex_bytes(b, lowercase)).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array)?;

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(|b| hex_bytes(b, lowercase)).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::LargeUtf8 => {
                let array = as_largestring_array(array);

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(|b| hex_bytes(b, lowercase)).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::Binary => {
                let array = as_binary_array(array)?;

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(|b| hex_bytes(b, lowercase)).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::FixedSizeBinary(_) => {
                let array = as_fixed_size_binary_array(array)?;

                let hexed: StringArray = array
                    .iter()
                    .map(|v| v.map(|b| hex_bytes(b, lowercase)).transpose())
                    .collect::<Result<_, _>>()?;

                Ok(ColumnarValue::Array(Arc::new(hexed)))
            }
            DataType::Dictionary(_, value_type) => {
                let dict = as_dictionary_array::<Int32Type>(&array);

                let values = match **value_type {
                    DataType::Int64 => as_int64_array(dict.values())?
                        .iter()
                        .map(|v| v.map(hex_int64))
                        .collect::<Vec<_>>(),
                    DataType::Utf8 => as_string_array(dict.values())
                        .iter()
                        .map(|v| v.map(|b| hex_bytes(b, lowercase)).transpose())
                        .collect::<Result<_, _>>()?,
                    DataType::Binary => as_binary_array(dict.values())?
                        .iter()
                        .map(|v| v.map(|b| hex_bytes(b, lowercase)).transpose())
                        .collect::<Result<_, _>>()?,
                    _ => exec_err!(
                        "hex got an unexpected argument type: {:?}",
                        array.data_type()
                    )?,
                };

                let new_values: Vec<Option<String>> = dict
                    .keys()
                    .iter()
                    .map(|key| key.map(|k| values[k as usize].clone()).unwrap_or(None))
                    .collect();

                let string_array_values = StringArray::from(new_values);

                Ok(ColumnarValue::Array(Arc::new(string_array_values)))
            }
            _ => exec_err!(
                "hex got an unexpected argument type: {:?}",
                array.data_type()
            ),
        },
        _ => exec_err!("native hex does not support scalar values at this time"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::{
        array::{
            as_string_array, BinaryDictionaryBuilder, PrimitiveDictionaryBuilder,
            StringBuilder, StringDictionaryBuilder,
        },
        datatypes::{Int32Type, Int64Type},
    };
    use datafusion_expr::ColumnarValue;

    #[test]
    fn test_dictionary_hex_utf8() {
        let mut input_builder = StringDictionaryBuilder::<Int32Type>::new();
        input_builder.append_value("hi");
        input_builder.append_value("bye");
        input_builder.append_null();
        input_builder.append_value("rust");
        let input = input_builder.finish();

        let mut string_builder = StringBuilder::new();
        string_builder.append_value("6869");
        string_builder.append_value("627965");
        string_builder.append_null();
        string_builder.append_value("72757374");
        let expected = string_builder.finish();

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_string_array(&result);

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_dictionary_hex_int64() {
        let mut input_builder = PrimitiveDictionaryBuilder::<Int32Type, Int64Type>::new();
        input_builder.append_value(1);
        input_builder.append_value(2);
        input_builder.append_null();
        input_builder.append_value(3);
        let input = input_builder.finish();

        let mut string_builder = StringBuilder::new();
        string_builder.append_value("1");
        string_builder.append_value("2");
        string_builder.append_null();
        string_builder.append_value("3");
        let expected = string_builder.finish();

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_string_array(&result);

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_dictionary_hex_binary() {
        let mut input_builder = BinaryDictionaryBuilder::<Int32Type>::new();
        input_builder.append_value("1");
        input_builder.append_value("j");
        input_builder.append_null();
        input_builder.append_value("3");
        let input = input_builder.finish();

        let mut expected_builder = StringBuilder::new();
        expected_builder.append_value("31");
        expected_builder.append_value("6A");
        expected_builder.append_null();
        expected_builder.append_value("33");
        let expected = expected_builder.finish();

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_string_array(&result);

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_hex_int64() {
        let num = 1234;
        let hexed = super::hex_int64(num);
        assert_eq!(hexed, "4D2".to_string());

        let num = -1;
        let hexed = super::hex_int64(num);
        assert_eq!(hexed, "FFFFFFFFFFFFFFFF".to_string());
    }

    #[test]
    fn test_spark_hex_int64() {
        let int_array = Int64Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let columnar_value = ColumnarValue::Array(Arc::new(int_array));

        let result = super::spark_hex(&[columnar_value]).unwrap();
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let string_array = as_string_array(&result);
        let expected_array = StringArray::from(vec![
            Some("1".to_string()),
            Some("2".to_string()),
            None,
            Some("3".to_string()),
        ]);

        assert_eq!(string_array, &expected_array);
    }
}
