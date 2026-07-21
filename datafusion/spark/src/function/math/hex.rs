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

use std::str::from_utf8_unchecked;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, NullBufferBuilder, StringArray, StringBuilder};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::DataType;
use arrow::{
    array::{as_dictionary_array, as_largestring_array, as_string_array},
    datatypes::Int32Type,
};
use datafusion_common::cast::as_large_binary_array;
use datafusion_common::cast::as_string_view_array;
use datafusion_common::types::{NativeType, logical_int64, logical_string};
use datafusion_common::utils::hex::{HexCase, encode_bytes_into, encode_u64};
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    DataFusionError,
    cast::{as_binary_array, as_fixed_size_binary_array, as_int64_array},
    exec_datafusion_err, exec_err,
};
use datafusion_expr::{
    Coercion, ColumnarValue, EncodingPreservation, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
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

        let binary = Coercion::new_exact(TypeSignatureClass::Binary)
            .with_encoding_preservation(EncodingPreservation::dictionary());

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

/// Generic hex encoding for byte array types
fn hex_encode_bytes<'a, I, T>(
    iter: I,
    lowercase: bool,
    len: usize,
) -> Result<ArrayRef, DataFusionError>
where
    I: Iterator<Item = Option<T>>,
    T: AsRef<[u8]> + 'a,
{
    let case = if lowercase {
        HexCase::Lower
    } else {
        HexCase::Upper
    };

    // Write hex digits directly into one growing value buffer, tracking offsets
    // ourselves. Each input byte becomes exactly two output bytes, so there is
    // no per-row `String`/`StringBuilder` copy — the hex digits are written once
    // into the final buffer.
    let mut values: Vec<u8> = Vec::with_capacity(len * 64);
    let mut offsets: Vec<i32> = Vec::with_capacity(len + 1);
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(len);

    for v in iter {
        if let Some(b) = v {
            let bytes = b.as_ref();
            let additional = bytes
                .len()
                .checked_mul(2)
                .ok_or_else(|| exec_datafusion_err!("hex output size overflow"))?;
            values.try_reserve(additional).map_err(|e| {
                exec_datafusion_err!(
                    "failed to reserve {additional} bytes for hex output: {e}"
                )
            })?;
            encode_bytes_into(bytes, case, &mut values);
            nulls.append_non_null();
        } else {
            nulls.append_null();
        }
        offsets.push(
            i32::try_from(values.len()).map_err(|_| {
                exec_datafusion_err!("hex output exceeds i32 offset range")
            })?,
        );
    }

    // SAFETY: the value buffer contains only ASCII hex digits (valid UTF-8) and
    // the offsets are monotonically increasing and end at `values.len()`, so the
    // array invariants hold. This mirrors the previous `from_utf8_unchecked`
    // path and avoids a redundant UTF-8 validation pass over the whole buffer.
    let array = unsafe {
        StringArray::new_unchecked(
            OffsetBuffer::new(offsets.into()),
            Buffer::from_vec(values),
            nulls.finish(),
        )
    };
    Ok(Arc::new(array))
}

/// Generic hex encoding for int64 type
fn hex_encode_int64(
    iter: impl Iterator<Item = Option<i64>>,
    len: usize,
) -> Result<ArrayRef, DataFusionError> {
    let mut builder = StringBuilder::with_capacity(len, len * 16);

    for v in iter {
        if let Some(num) = v {
            let mut temp = [0u8; 16];
            let slice = encode_u64(num as u64, HexCase::Upper, &mut temp);
            // SAFETY: slice contains only ASCII hex digests, which are valid UTF-8
            unsafe {
                builder.append_value(from_utf8_unchecked(slice));
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
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
                Ok(ColumnarValue::Array(hex_encode_int64(
                    array.iter(),
                    array.len(),
                )?))
            }
            DataType::Utf8 => {
                let array = as_string_array(array);
                Ok(ColumnarValue::Array(hex_encode_bytes(
                    array.iter(),
                    lowercase,
                    array.len(),
                )?))
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(
                    array.iter(),
                    lowercase,
                    array.len(),
                )?))
            }
            DataType::LargeUtf8 => {
                let array = as_largestring_array(array);
                Ok(ColumnarValue::Array(hex_encode_bytes(
                    array.iter(),
                    lowercase,
                    array.len(),
                )?))
            }
            DataType::Binary => {
                let array = as_binary_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(
                    array.iter(),
                    lowercase,
                    array.len(),
                )?))
            }
            DataType::LargeBinary => {
                let array = as_large_binary_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(
                    array.iter(),
                    lowercase,
                    array.len(),
                )?))
            }
            DataType::FixedSizeBinary(_) => {
                let array = as_fixed_size_binary_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(
                    array.iter(),
                    lowercase,
                    array.len(),
                )?))
            }
            DataType::Dictionary(key_type, _) => {
                if **key_type != DataType::Int32 {
                    return exec_err!(
                        "hex only supports Int32 dictionary keys, get: {}",
                        key_type
                    );
                }

                let dict = as_dictionary_array::<Int32Type>(&array);
                let dict_values = dict.values();

                let encoded_values = match dict_values.data_type() {
                    DataType::Int64 => {
                        let arr = as_int64_array(dict_values)?;
                        hex_encode_int64(arr.iter(), arr.len())?
                    }
                    DataType::Utf8 => {
                        let arr = as_string_array(dict_values);
                        hex_encode_bytes(arr.iter(), lowercase, arr.len())?
                    }
                    DataType::LargeUtf8 => {
                        let arr = as_largestring_array(dict_values);
                        hex_encode_bytes(arr.iter(), lowercase, arr.len())?
                    }
                    DataType::Utf8View => {
                        let arr = as_string_view_array(dict_values)?;
                        hex_encode_bytes(arr.iter(), lowercase, arr.len())?
                    }
                    DataType::Binary => {
                        let arr = as_binary_array(dict_values)?;
                        hex_encode_bytes(arr.iter(), lowercase, arr.len())?
                    }
                    DataType::LargeBinary => {
                        let arr = as_large_binary_array(dict_values)?;
                        hex_encode_bytes(arr.iter(), lowercase, arr.len())?
                    }
                    DataType::FixedSizeBinary(_) => {
                        let arr = as_fixed_size_binary_array(dict_values)?;
                        hex_encode_bytes(arr.iter(), lowercase, arr.len())?
                    }
                    _ => {
                        return exec_err!(
                            "hex got an unexpected argument type: {}",
                            dict_values.data_type()
                        );
                    }
                };

                let new_dict = dict.with_values(encoded_values);
                Ok(ColumnarValue::Array(Arc::new(new_dict)))
            }
            _ => exec_err!("hex got an unexpected argument type: {}", array.data_type()),
        },
        _ => exec_err!("native hex does not support scalar values at this time"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        BinaryArray, DictionaryArray, Int32Array, Int64Array, StringArray,
    };
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
        let cases = vec![
            (0_i64, "0"),
            (1, "1"),
            (15, "F"),
            (16, "10"),
            (255, "FF"),
            (256, "100"),
            (1234, "4D2"),
            (i64::MAX, "7FFFFFFFFFFFFFFF"),
            (i64::MIN, "8000000000000000"),
            (-1, "FFFFFFFFFFFFFFFF"),
        ];

        let arr =
            super::hex_encode_int64(cases.iter().map(|(n, _)| Some(*n)), cases.len())
                .unwrap();
        let arr = as_string_array(&arr);
        for (i, (num, expected)) in cases.iter().enumerate() {
            assert_eq!(*expected, arr.value(i), "hex({num})");
        }
    }

    #[test]
    fn test_spark_hex_binary_round_trip_all_bytes() {
        // Single-row binary input containing every byte value, encoded in
        // a single column. Catches per-byte regressions in the bytes path.
        let payload: Vec<u8> = (0u8..=255).collect();
        let bin_array = BinaryArray::from(vec![Some(payload.as_slice())]);

        let result =
            super::spark_hex(&[ColumnarValue::Array(Arc::new(bin_array))]).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };
        let strings = as_string_array(&array);
        let mut expected = String::with_capacity(512);
        for byte in 0u8..=255 {
            use std::fmt::Write;
            write!(expected, "{byte:02X}").unwrap();
        }
        assert_eq!(strings.value(0), expected);
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
