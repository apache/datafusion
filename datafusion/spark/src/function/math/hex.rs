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

use arrow::array::{
    Array, ArrayAccessor, ArrayRef, Int64Array, StringViewArray, as_dictionary_array,
    as_largestring_array, as_string_array, make_view,
};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Int32Type};
use datafusion_common::cast::as_binary_view_array;
use datafusion_common::cast::as_large_binary_array;
use datafusion_common::cast::as_string_view_array;
use datafusion_common::types::{NativeType, logical_int64, logical_string};
use datafusion_common::utils::hex::{
    HexCase, ToHex, encode_bytes, encode_bytes_into, encode_bytes_to_slice,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    DataFusionError, ScalarValue,
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
                DataType::Dictionary(key_type.clone(), Box::new(DataType::Utf8View))
            }
            _ => DataType::Utf8View,
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

/// Arrow `StringView` inlines values of at most this many bytes. Hex of at most
/// 6 input bytes (or 12 hex digits for integers) takes this path and never
/// touches the data buffer.
const HEX_INLINE_LEN: usize = 12;

/// Append a `StringView` for `bytes` encoded as hex.
///
/// Short encodings (`<= 12` bytes) are inlined in the view. Longer encodings
/// are written once into `data` and referenced by offset.
#[inline]
fn push_hex_bytes_view(
    views: &mut Vec<u128>,
    data: &mut Vec<u8>,
    bytes: &[u8],
    case: HexCase,
) -> Result<(), DataFusionError> {
    let hex_len = bytes
        .len()
        .checked_mul(2)
        .ok_or_else(|| exec_datafusion_err!("hex output size overflow"))?;

    if hex_len <= HEX_INLINE_LEN {
        let mut tmp = [0u8; HEX_INLINE_LEN];
        encode_bytes_to_slice(bytes, case, &mut tmp[..hex_len])?;
        views.push(make_view(&tmp[..hex_len], 0, 0));
        return Ok(());
    }

    let offset = data.len();
    let offset_u32 = u32::try_from(offset)
        .map_err(|_| exec_datafusion_err!("hex output exceeds u32 offset range"))?;
    data.try_reserve(hex_len).map_err(|e| {
        exec_datafusion_err!("failed to reserve {hex_len} bytes for hex output: {e}")
    })?;
    encode_bytes_into(bytes, case, data);
    views.push(make_view(&data[offset..], 0, offset_u32));
    Ok(())
}

#[inline]
fn push_hex_int_view(
    views: &mut Vec<u128>,
    data: &mut Vec<u8>,
    num: i64,
    hex_buffer: &mut [u8; 16],
) -> Result<(), DataFusionError> {
    let hex = num.write_hex(HexCase::Upper, hex_buffer);
    if hex.len() <= HEX_INLINE_LEN {
        views.push(make_view(hex, 0, 0));
        return Ok(());
    }

    let offset = data.len();
    let offset_u32 = u32::try_from(offset)
        .map_err(|_| exec_datafusion_err!("hex output exceeds u32 offset range"))?;
    data.extend_from_slice(hex);
    views.push(make_view(hex, 0, offset_u32));
    Ok(())
}

fn finish_hex_string_view(
    views: Vec<u128>,
    data: Vec<u8>,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    let buffers = if data.is_empty() {
        vec![]
    } else {
        vec![Buffer::from_vec(data)]
    };
    // SAFETY: every view is produced by `make_view` from ASCII hex digits
    // (valid UTF-8). Inlined views copy those digits into the view itself;
    // out-of-line views have a prefix/length that matches the slice written
    // into `buffers[0]` at the recorded offset.
    unsafe {
        Arc::new(StringViewArray::new_unchecked(
            ScalarBuffer::from(views),
            buffers,
            nulls,
        ))
    }
}

/// Generic hex encoding for byte array types. Returns a `Utf8View` array.
fn hex_encode_bytes<'a, A, T>(
    array: &A,
    lowercase: bool,
) -> Result<ArrayRef, DataFusionError>
where
    A: ArrayAccessor<Item = &'a T>,
    T: AsRef<[u8]> + ?Sized + 'a,
{
    let case = if lowercase {
        HexCase::Lower
    } else {
        HexCase::Upper
    };
    let len = array.len();
    let nulls = array.nulls().cloned();

    let mut views = Vec::with_capacity(len);
    // Only encodings longer than the inline limit land here.
    let mut data = Vec::new();

    if let Some(ref nulls) = nulls {
        for i in 0..len {
            if nulls.is_valid(i) {
                // SAFETY: `i` is in bounds and the validity buffer marks it valid.
                let bytes = unsafe { array.value_unchecked(i) }.as_ref();
                push_hex_bytes_view(&mut views, &mut data, bytes, case)?;
            } else {
                views.push(make_view(b"", 0, 0));
            }
        }
    } else {
        for i in 0..len {
            // SAFETY: `i` is in bounds and no null buffer means every value is valid.
            let bytes = unsafe { array.value_unchecked(i) }.as_ref();
            push_hex_bytes_view(&mut views, &mut data, bytes, case)?;
        }
    }

    Ok(finish_hex_string_view(views, data, nulls))
}

/// Hex encoding for int64. Returns a `Utf8View` array and reuses the input
/// null buffer so nulls stay a pointer clone rather than a per-row rebuild.
fn hex_encode_int64(array: &Int64Array) -> Result<ArrayRef, DataFusionError> {
    let len = array.len();
    let nulls = array.nulls().cloned();
    let mut views = Vec::with_capacity(len);
    let mut data = Vec::new();
    let mut hex_buffer = [0u8; 16];

    if let Some(ref nulls) = nulls {
        for (i, &num) in array.values().iter().enumerate() {
            if nulls.is_valid(i) {
                push_hex_int_view(&mut views, &mut data, num, &mut hex_buffer)?;
            } else {
                views.push(make_view(b"", 0, 0));
            }
        }
    } else {
        for &num in array.values() {
            push_hex_int_view(&mut views, &mut data, num, &mut hex_buffer)?;
        }
    }

    Ok(finish_hex_string_view(views, data, nulls))
}

fn hex_scalar(
    scalar: &ScalarValue,
    lowercase: bool,
) -> Result<ColumnarValue, DataFusionError> {
    if scalar.is_null() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None)));
    }

    let case = if lowercase {
        HexCase::Lower
    } else {
        HexCase::Upper
    };

    let encoded = match scalar {
        ScalarValue::Int64(Some(n)) => {
            let mut buf = [0u8; 16];
            let hex = n.write_hex(HexCase::Upper, &mut buf);
            // SAFETY: `write_hex` emits only ASCII hex digits.
            unsafe { from_utf8_unchecked(hex).to_string() }
        }
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => encode_bytes(s.as_bytes(), case),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => encode_bytes(b, case),
        other => {
            return exec_err!(
                "hex got an unexpected argument type: {}",
                other.data_type()
            );
        }
    };

    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(encoded))))
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
    let [input] = take_function_args("hex", args)?;

    match input {
        ColumnarValue::Scalar(scalar) => hex_scalar(scalar, lowercase),
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 => {
                let array = as_int64_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_int64(array)?))
            }
            DataType::Utf8 => {
                let array = as_string_array(array);
                Ok(ColumnarValue::Array(hex_encode_bytes(&array, lowercase)?))
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(&array, lowercase)?))
            }
            DataType::LargeUtf8 => {
                let array = as_largestring_array(array);
                Ok(ColumnarValue::Array(hex_encode_bytes(&array, lowercase)?))
            }
            DataType::Binary => {
                let array = as_binary_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(&array, lowercase)?))
            }
            DataType::BinaryView => {
                let array = as_binary_view_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(&array, lowercase)?))
            }
            DataType::LargeBinary => {
                let array = as_large_binary_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(&array, lowercase)?))
            }
            DataType::FixedSizeBinary(_) => {
                let array = as_fixed_size_binary_array(array)?;
                Ok(ColumnarValue::Array(hex_encode_bytes(&array, lowercase)?))
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
                        hex_encode_int64(arr)?
                    }
                    DataType::Utf8 => {
                        let arr = as_string_array(dict_values);
                        hex_encode_bytes(&arr, lowercase)?
                    }
                    DataType::LargeUtf8 => {
                        let arr = as_largestring_array(dict_values);
                        hex_encode_bytes(&arr, lowercase)?
                    }
                    DataType::Utf8View => {
                        let arr = as_string_view_array(dict_values)?;
                        hex_encode_bytes(&arr, lowercase)?
                    }
                    DataType::Binary => {
                        let arr = as_binary_array(dict_values)?;
                        hex_encode_bytes(&arr, lowercase)?
                    }
                    DataType::BinaryView => {
                        let arr = as_binary_view_array(dict_values)?;
                        hex_encode_bytes(&arr, lowercase)?
                    }
                    DataType::LargeBinary => {
                        let arr = as_large_binary_array(dict_values)?;
                        hex_encode_bytes(&arr, lowercase)?
                    }
                    DataType::FixedSizeBinary(_) => {
                        let arr = as_fixed_size_binary_array(dict_values)?;
                        hex_encode_bytes(&arr, lowercase)?
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
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Array, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeBinaryBuilder,
        Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow::{
        array::{
            BinaryDictionaryBuilder, PrimitiveDictionaryBuilder, StringDictionaryBuilder,
        },
        datatypes::{Int32Type, Int64Type},
    };
    use datafusion_common::ScalarValue;
    use datafusion_common::cast::{as_dictionary_array, as_string_view_array};
    use datafusion_expr::ColumnarValue;

    fn utf8view_dict(
        keys: Int32Array,
        values: Vec<Option<&str>>,
    ) -> DictionaryArray<Int32Type> {
        DictionaryArray::new(keys, Arc::new(StringViewArray::from(values)))
    }

    #[test]
    fn test_dictionary_hex_utf8() {
        let mut input_builder = StringDictionaryBuilder::<Int32Type>::new();
        input_builder.append_value("hi");
        input_builder.append_value("bye");
        input_builder.append_null();
        input_builder.append_value("rust");
        let input = input_builder.finish();

        let expected = utf8view_dict(
            input.keys().clone(),
            vec![Some("6869"), Some("627965"), Some("72757374")],
        );

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
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

        let expected =
            utf8view_dict(input.keys().clone(), vec![Some("1"), Some("2"), Some("3")]);

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
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

        let expected = utf8view_dict(
            input.keys().clone(),
            vec![Some("31"), Some("6A"), Some("33")],
        );

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
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

        let input =
            Int64Array::from(cases.iter().map(|(n, _)| Some(*n)).collect::<Vec<_>>());
        let arr = super::hex_encode_int64(&input).unwrap();
        let arr = as_string_view_array(&arr).unwrap();
        for (i, (num, expected)) in cases.iter().enumerate() {
            assert_eq!(*expected, arr.value(i), "hex({num})");
        }
        // Values with more than 12 hex digits go out-of-line; the rest are inlined.
        assert!(!arr.data_buffers().is_empty());
    }

    #[test]
    fn test_hex_encode_bytes_lowercase() {
        // Every in-repo caller of `hex_encode_bytes` goes through `spark_hex`,
        // which always passes `lowercase = false`. The `lowercase = true` path
        // is reachable only via `spark_sha2_hex`, which has no in-workspace
        // caller, so it otherwise has no coverage. Drive it directly here.
        let input = StringArray::from(vec![Some("hi"), Some("bye"), None, Some("rust")]);
        let input_ref = &input;
        let result = super::hex_encode_bytes(&input_ref, true).unwrap();
        let result = as_string_view_array(&result).unwrap();

        let expected = StringViewArray::from(vec![
            Some("6869"),
            Some("627965"),
            None,
            Some("72757374"),
        ]);
        assert_eq!(result, &expected);
        // 2–8 hex digits all fit in the StringView inline prefix.
        assert!(result.data_buffers().is_empty());
    }

    #[test]
    fn test_spark_hex_binary_round_trip_all_bytes() {
        // Single-row binary input containing every byte value, encoded in
        // a single column. Catches per-byte regressions in the bytes path.
        let payload: Vec<u8> = (0u8..=255).collect();
        let bin_array = BinaryArray::from(vec![Some(payload.as_slice())]);

        let result =
            super::spark_hex(&[ColumnarValue::Array(Arc::new(bin_array))]).unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("Expected array")
        };
        let strings = as_string_view_array(&array).unwrap();
        let mut expected = String::with_capacity(512);
        for byte in 0u8..=255 {
            use std::fmt::Write;
            write!(expected, "{byte:02X}").unwrap();
        }
        assert_eq!(strings.value(0), expected);
        // 512 hex digits cannot be inlined.
        assert!(!strings.data_buffers().is_empty());
    }

    #[test]
    fn test_spark_hex_binary_no_nulls() {
        let input = BinaryArray::from(vec![
            b"".as_slice(),
            b"\x00\x7f\x80\xff".as_slice(),
            b"DataFusion".as_slice(),
        ]);

        let result = super::spark_hex(&[ColumnarValue::Array(Arc::new(input))]).unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("Expected array")
        };
        let strings = as_string_view_array(&array).unwrap();

        assert_eq!(strings.nulls(), None);
        assert_eq!(
            strings,
            &StringViewArray::from(vec!["", "007F80FF", "44617461467573696F6E"])
        );
        // "" and "007F80FF" inline; "44617461467573696F6E" (20 bytes) does not.
        assert!(!strings.data_buffers().is_empty());
    }

    #[test]
    fn test_spark_hex_binary_reuses_input_nulls() {
        let input = BinaryArray::from(vec![
            Some(b"skip".as_slice()),
            None,
            Some(b"\x00\xff".as_slice()),
            Some(b"hex".as_slice()),
            None,
        ])
        .slice(1, 4);
        let input_nulls = input.nulls().unwrap().clone();

        let result = super::spark_hex(&[ColumnarValue::Array(Arc::new(input))]).unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("Expected array")
        };
        let strings = as_string_view_array(&array).unwrap();
        let output_nulls = strings.nulls().unwrap();

        assert_eq!(output_nulls, &input_nulls);
        assert!(output_nulls.inner().ptr_eq(input_nulls.inner()));
        assert_eq!(
            strings,
            &StringViewArray::from(vec![None, Some("00FF"), Some("686578"), None])
        );
        // All non-null encodings here are ≤ 12 bytes, so nothing is buffered.
        assert!(strings.data_buffers().is_empty());
    }

    #[test]
    fn test_spark_hex_int64() {
        let int_array = Int64Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let columnar_value = ColumnarValue::Array(Arc::new(int_array));

        let result = super::spark_hex(&[columnar_value]).unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
        };

        let string_array = as_string_view_array(&result).unwrap();
        let expected_array = StringViewArray::from(vec![
            Some("1".to_string()),
            Some("2".to_string()),
            None,
            Some("3".to_string()),
        ]);

        assert_eq!(string_array, &expected_array);
        assert!(string_array.data_buffers().is_empty());
    }

    #[test]
    fn test_dict_values_null() {
        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let vals = Int64Array::from(vec![Some(32), None]);
        // [32, null, null]
        let dict = DictionaryArray::new(keys, Arc::new(vals));

        let columnar_value = ColumnarValue::Array(Arc::new(dict));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
        };

        let result = as_dictionary_array(&result).unwrap();

        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let expected = utf8view_dict(keys, vec![Some("20"), None]);

        assert_eq!(&expected, result);
    }

    #[test]
    fn test_dict_binary_values_null() {
        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let vals = BinaryArray::from(vec![Some(b"hi".as_slice()), None]);
        // [b"hi", null, null]
        let dict = DictionaryArray::new(keys, Arc::new(vals));

        let result = super::spark_hex(&[ColumnarValue::Array(Arc::new(dict))]).unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
        };
        let result = as_dictionary_array(&result).unwrap();

        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let expected = utf8view_dict(keys, vec![Some("6869"), None]);

        assert_eq!(&expected, result);
    }

    #[test]
    fn test_spark_hex_scalar() {
        let result = super::spark_hex(&[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "Spark SQL".to_string(),
        )))])
        .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => {
                assert_eq!(s, "537061726B2053514C");
            }
            other => panic!("expected Utf8View scalar, got {other:?}"),
        }

        let result =
            super::spark_hex(&[ColumnarValue::Scalar(ScalarValue::Int64(Some(1234)))])
                .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => {
                assert_eq!(s, "4D2");
            }
            other => panic!("expected Utf8View scalar, got {other:?}"),
        }

        let result =
            super::spark_hex(&[ColumnarValue::Scalar(ScalarValue::Utf8(None))]).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => {}
            other => panic!("expected null Utf8View scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_return_type_is_utf8view() {
        let hex = super::SparkHex::new();
        use arrow::datatypes::DataType;
        use datafusion_expr::ScalarUDFImpl;

        for input in [
            DataType::Int64,
            DataType::Utf8,
            DataType::Utf8View,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::BinaryView,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(2),
        ] {
            assert_eq!(
                hex.return_type(std::slice::from_ref(&input)).unwrap(),
                DataType::Utf8View,
                "hex({input})"
            );
        }
        assert_eq!(
            hex.return_type(&[DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Binary),
            )])
            .unwrap(),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View),)
        );
    }

    #[test]
    fn test_int64_reuses_input_nulls() {
        let input = Int64Array::from(vec![Some(1), None, Some(-1), None]);
        let input_nulls = input.nulls().unwrap().clone();
        let result = super::hex_encode_int64(&input).unwrap();
        let strings = as_string_view_array(&result).unwrap();
        let output_nulls = strings.nulls().unwrap();

        assert_eq!(output_nulls, &input_nulls);
        assert!(output_nulls.inner().ptr_eq(input_nulls.inner()));
        assert_eq!(
            strings,
            &StringViewArray::from(vec![Some("1"), None, Some("FFFFFFFFFFFFFFFF"), None])
        );
        // -1 is 16 hex digits, so it is stored out-of-line.
        assert!(!strings.data_buffers().is_empty());
    }

    #[test]
    fn test_hex_int64_inline_boundary() {
        // 12 hex digits is the StringView inline limit; 13 digits go out-of-line.
        let inline_max = 0x0000_FFFF_FFFF_FFFFi64;
        let outline_min = 0x0001_0000_0000_0000i64;

        let inline_only = Int64Array::from(vec![0, 1, inline_max]);
        let inline_arr = super::hex_encode_int64(&inline_only).unwrap();
        let inline_arr = as_string_view_array(&inline_arr).unwrap();
        assert_eq!(
            inline_arr,
            &StringViewArray::from(vec!["0", "1", "FFFFFFFFFFFF"])
        );
        assert!(inline_arr.data_buffers().is_empty());

        let mixed = Int64Array::from(vec![inline_max, outline_min]);
        let mixed_arr = super::hex_encode_int64(&mixed).unwrap();
        let mixed_arr = as_string_view_array(&mixed_arr).unwrap();
        assert_eq!(
            mixed_arr,
            &StringViewArray::from(vec!["FFFFFFFFFFFF", "1000000000000"])
        );
        assert!(!mixed_arr.data_buffers().is_empty());
    }

    #[test]
    fn test_spark_hex_utf8view_and_large_utf8() {
        let view = StringViewArray::from(vec![Some("hi"), None, Some("foobar")]);
        let result = super::spark_hex(&[ColumnarValue::Array(Arc::new(view))]).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };
        let strings = as_string_view_array(&array).unwrap();
        assert_eq!(
            strings,
            &StringViewArray::from(vec![Some("6869"), None, Some("666F6F626172")])
        );
        assert!(strings.data_buffers().is_empty());

        let large = LargeStringArray::from(vec![Some("hi"), None, Some("foobarb")]);
        let result = super::spark_hex(&[ColumnarValue::Array(Arc::new(large))]).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };
        let strings = as_string_view_array(&array).unwrap();
        assert_eq!(
            strings,
            &StringViewArray::from(vec![Some("6869"), None, Some("666F6F62617262")])
        );
        // "foobarb" is 14 hex digits, so it is stored out-of-line.
        assert!(!strings.data_buffers().is_empty());
    }

    #[test]
    fn test_spark_hex_binary_view_and_fixed_size() {
        let view = BinaryViewArray::from(vec![
            Some(b"hi".as_slice()),
            None,
            Some(b"\x00\xff".as_slice()),
        ]);
        let result = super::spark_hex(&[ColumnarValue::Array(Arc::new(view))]).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };
        let strings = as_string_view_array(&array).unwrap();
        assert_eq!(
            strings,
            &StringViewArray::from(vec![Some("6869"), None, Some("00FF")])
        );

        let mut fixed_builder = FixedSizeBinaryBuilder::new(2);
        fixed_builder.append_value(b"ab").unwrap();
        fixed_builder.append_null();
        fixed_builder.append_value(b"cd").unwrap();
        let fixed = fixed_builder.finish();
        let result = super::spark_hex(&[ColumnarValue::Array(Arc::new(fixed))]).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };
        let strings = as_string_view_array(&array).unwrap();
        assert_eq!(
            strings,
            &StringViewArray::from(vec![Some("6162"), None, Some("6364")])
        );
    }

    #[test]
    fn test_spark_hex_scalar_binary_types() {
        let result = super::spark_hex(&[ColumnarValue::Scalar(ScalarValue::Binary(
            Some(b"SQL".to_vec()),
        ))])
        .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => {
                assert_eq!(s, "53514C");
            }
            other => panic!("expected Utf8View scalar, got {other:?}"),
        }

        let result = super::spark_hex(&[ColumnarValue::Scalar(ScalarValue::BinaryView(
            Some(b"SQL".to_vec()),
        ))])
        .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => {
                assert_eq!(s, "53514C");
            }
            other => panic!("expected Utf8View scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_spark_hex_unexpected_type() {
        let input = ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2, 3])));
        let err = super::spark_hex(&[input]).unwrap_err();
        assert!(
            err.to_string().contains("unexpected argument type"),
            "unexpected error: {err}"
        );
    }
}
