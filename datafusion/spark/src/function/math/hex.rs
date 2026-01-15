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
use std::str::from_utf8_unchecked;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use arrow::{
    array::{as_dictionary_array, as_largestring_array, as_string_array},
    datatypes::Int32Type,
};
use datafusion_common::cast::as_large_binary_array;
use datafusion_common::cast::as_string_view_array;
use datafusion_common::types::{NativeType, logical_int64, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    DataFusionError,
    cast::{as_binary_array, as_fixed_size_binary_array, as_int64_array},
    exec_err,
};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
/// <https://spark.apache.org/docs/latest/api/sql/index.html#hex>
#[derive(Debug, PartialEq, Eq, Hash)]
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
        let int64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Int64,
        );

        let string = Coercion::new_exact(TypeSignatureClass::Native(logical_string()));

        let binary = Coercion::new_exact(TypeSignatureClass::Binary);

        let variants = vec![
            // accepts numeric types
            TypeSignature::Coercible(vec![int64]),
            // accepts string types (Utf8, Utf8View, LargeUtf8)
            TypeSignature::Coercible(vec![string]),
            // accepts binary types (Binary, FixedSizeBinary, LargeBinary)
            TypeSignature::Coercible(vec![binary]),
        ];

        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
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

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(match &arg_types[0] {
            DataType::Dictionary(key_type, _) => {
                DataType::Dictionary(key_type.clone(), Box::new(DataType::Utf8))
            }
            _ => DataType::Utf8,
        })
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
}

/// Hex encoding lookup tables for fast byte-to-hex conversion
const HEX_CHARS_LOWER: &[u8; 16] = b"0123456789abcdef";
const HEX_CHARS_UPPER: &[u8; 16] = b"0123456789ABCDEF";

#[inline]
fn hex_int64(num: i64, buffer: &mut [u8; 16]) -> &[u8] {
    if num == 0 {
        return b"0";
    }

    let mut n = num as u64;
    let mut i = 16;
    while n != 0 {
        i -= 1;
        buffer[i] = HEX_CHARS_UPPER[(n & 0xF) as usize];
        n >>= 4;
    }
    &buffer[i..]
}

/// Generic hex encoding for byte array types
fn hex_encode_bytes<'a, I, T>(
    iter: I,
    lowercase: bool,
    len: usize,
) -> Result<ColumnarValue, DataFusionError>
where
    I: Iterator<Item = Option<T>>,
    T: AsRef<[u8]> + 'a,
{
    let mut builder = StringBuilder::with_capacity(len, len * 64);
    let mut buffer = Vec::with_capacity(64);
    let hex_chars = if lowercase {
        HEX_CHARS_LOWER
    } else {
        HEX_CHARS_UPPER
    };

    for v in iter {
        if let Some(b) = v {
            buffer.clear();
            let bytes = b.as_ref();
            for &byte in bytes {
                buffer.push(hex_chars[(byte >> 4) as usize]);
                buffer.push(hex_chars[(byte & 0x0f) as usize]);
            }
            // SAFETY: buffer contains only ASCII hex digests, which are valid UTF-8
            unsafe {
                builder.append_value(from_utf8_unchecked(&buffer));
            }
        } else {
            builder.append_null();
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

/// Generic hex encoding for int64 type
fn hex_encode_int64<I>(iter: I, len: usize) -> Result<ColumnarValue, DataFusionError>
where
    I: Iterator<Item = Option<i64>>,
{
    let mut builder = StringBuilder::with_capacity(len, len * 16);

    for v in iter {
        if let Some(num) = v {
            let mut temp = [0u8; 16];
            let slice = hex_int64(num, &mut temp);
            // SAFETY: slice contains only ASCII hex digests, which are valid UTF-8
            unsafe {
                builder.append_value(from_utf8_unchecked(slice));
            }
        } else {
            builder.append_null();
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
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
    let input = match take_function_args("hex", args)? {
        [ColumnarValue::Scalar(value)] => ColumnarValue::Array(value.to_array()?),
        [ColumnarValue::Array(arr)] => ColumnarValue::Array(Arc::clone(arr)),
    };

    match &input {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 => {
                let array = as_int64_array(array)?;
                hex_encode_int64(array.iter(), array.len())
            }
            DataType::Utf8 => {
                let array = as_string_array(array);
                hex_encode_bytes(array.iter(), lowercase, array.len())
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array)?;
                hex_encode_bytes(array.iter(), lowercase, array.len())
            }
            DataType::LargeUtf8 => {
                let array = as_largestring_array(array);
                hex_encode_bytes(array.iter(), lowercase, array.len())
            }
            DataType::Binary => {
                let array = as_binary_array(array)?;
                hex_encode_bytes(array.iter(), lowercase, array.len())
            }
            DataType::LargeBinary => {
                let array = as_large_binary_array(array)?;
                hex_encode_bytes(array.iter(), lowercase, array.len())
            }
            DataType::FixedSizeBinary(_) => {
                let array = as_fixed_size_binary_array(array)?;
                hex_encode_bytes(array.iter(), lowercase, array.len())
            }
            DataType::Dictionary(_, _) => {
                let dict = as_dictionary_array::<Int32Type>(&array);
                let dict_values = dict.values();

                let encoded_values: ColumnarValue = match dict_values.data_type() {
                    DataType::Int64 => {
                        let arr = as_int64_array(dict_values)?;
                        hex_encode_int64(arr.iter(), arr.len())?
                    }
                    DataType::Utf8 => {
                        let arr = as_string_array(dict_values);
                        hex_encode_bytes(arr.iter(), lowercase, arr.len())?
                    }
                    DataType::Binary => {
                        let arr = as_binary_array(dict_values)?;
                        hex_encode_bytes(arr.iter(), lowercase, arr.len())?
                    }
                    _ => {
                        return exec_err!(
                            "hex got an unexpected argument type: {}",
                            dict_values.data_type()
                        );
                    }
                };

                let encoded_values_array: ArrayRef = match encoded_values {
                    ColumnarValue::Array(a) => a,
                    ColumnarValue::Scalar(s) => Arc::new(s.to_array()?),
                };

                let new_dict = dict.with_values(encoded_values_array);
                Ok(ColumnarValue::Array(Arc::new(new_dict)))
            }
            _ => exec_err!("hex got an unexpected argument type: {}", array.data_type()),
        },
        _ => exec_err!("native hex does not support scalar values at this time"),
    }
}

#[cfg(test)]
mod test {
    use std::str::from_utf8_unchecked;
    use std::sync::Arc;

    use arrow::array::{DictionaryArray, Int32Array, Int64Array, StringArray};
    use arrow::{
        array::{
            BinaryDictionaryBuilder, PrimitiveDictionaryBuilder, StringDictionaryBuilder,
            as_string_array,
        },
        datatypes::{Int32Type, Int64Type},
    };
    use datafusion_common::cast::as_dictionary_array;
    use datafusion_expr::ColumnarValue;

    #[test]
    fn test_dictionary_hex_utf8() {
        let mut input_builder = StringDictionaryBuilder::<Int32Type>::new();
        input_builder.append_value("hi");
        input_builder.append_value("bye");
        input_builder.append_null();
        input_builder.append_value("rust");
        let input = input_builder.finish();

        let mut expected_builder = StringDictionaryBuilder::<Int32Type>::new();
        expected_builder.append_value("6869");
        expected_builder.append_value("627965");
        expected_builder.append_null();
        expected_builder.append_value("72757374");
        let expected = expected_builder.finish();

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_dictionary_array(&result).unwrap();

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

        let mut expected_builder = StringDictionaryBuilder::<Int32Type>::new();
        expected_builder.append_value("1");
        expected_builder.append_value("2");
        expected_builder.append_null();
        expected_builder.append_value("3");
        let expected = expected_builder.finish();

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_dictionary_array(&result).unwrap();

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

        let mut expected_builder = StringDictionaryBuilder::<Int32Type>::new();
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

        let result = as_dictionary_array(&result).unwrap();

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_hex_int64() {
        let test_cases = vec![(1234, "4D2"), (-1, "FFFFFFFFFFFFFFFF")];

        for (num, expected) in test_cases {
            let mut cache = [0u8; 16];
            let slice = super::hex_int64(num, &mut cache);

            unsafe {
                let result = from_utf8_unchecked(slice);
                assert_eq!(expected, result);
            }
        }
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

    #[test]
    fn test_dict_values_null() {
        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let vals = Int64Array::from(vec![Some(32), None]);
        // [32, null, null]
        let dict = DictionaryArray::new(keys, Arc::new(vals));

        let columnar_value = ColumnarValue::Array(Arc::new(dict));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_dictionary_array(&result).unwrap();

        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let vals = StringArray::from(vec![Some("20"), None]);
        let expected = DictionaryArray::new(keys, Arc::new(vals));

        assert_eq!(&expected, result);
    }
}
