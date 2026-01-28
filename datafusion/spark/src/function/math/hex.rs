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
use arrow::downcast_dictionary_array;
use datafusion_common::cast::{
    as_binary_array, as_binary_view_array, as_fixed_size_binary_array, as_int64_array,
    as_large_binary_array, as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::types::{NativeType, logical_int64, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#hex>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkHex {
    signature: Signature,
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match &arg_types[0] {
            DataType::Dictionary(key_type, _) => {
                DataType::Dictionary(key_type.clone(), Box::new(DataType::Utf8))
            }
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(compute_hex, vec![Hint::AcceptsSingular])(&args.args)
    }
}

fn compute_hex(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("hex", args)?;
    downcast_dictionary_array! {
        array => {
            let values = hex_values(array.values())?;
            Ok(Arc::new(array.with_values(values)))
        },
        _ => {
            hex_values(array)
        }
    }
}

fn hex_values(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Int64 => {
            let array = as_int64_array(array)?;
            hex_encode_int64(array.iter())
        }
        DataType::Utf8 => {
            let array = as_string_array(array)?;
            hex_encode_bytes(array.iter())
        }
        DataType::Utf8View => {
            let array = as_string_view_array(array)?;
            hex_encode_bytes(array.iter())
        }
        DataType::LargeUtf8 => {
            let array = as_large_string_array(array)?;
            hex_encode_bytes(array.iter())
        }
        DataType::Binary => {
            let array = as_binary_array(array)?;
            hex_encode_bytes(array.iter())
        }
        DataType::LargeBinary => {
            let array = as_large_binary_array(array)?;
            hex_encode_bytes(array.iter())
        }
        DataType::BinaryView => {
            let array = as_binary_view_array(array)?;
            hex_encode_bytes(array.iter())
        }
        DataType::FixedSizeBinary(_) => {
            let array = as_fixed_size_binary_array(array)?;
            hex_encode_bytes(array.iter())
        }
        dt => internal_err!("Unexpected data type for hex: {dt}"),
    }
}

/// Hex encoding lookup tables for fast byte-to-hex conversion
const HEX_CHARS_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// Generic hex encoding for byte array types
fn hex_encode_bytes<'a, I, T>(iter: I) -> Result<ArrayRef>
where
    I: ExactSizeIterator<Item = Option<T>>,
    T: AsRef<[u8]> + 'a,
{
    let mut builder = StringBuilder::with_capacity(iter.len(), iter.len() * 64);
    let mut buffer = Vec::with_capacity(64);

    for v in iter {
        if let Some(b) = v {
            buffer.clear();
            let bytes = b.as_ref();
            for &byte in bytes {
                buffer.push(HEX_CHARS_UPPER[(byte >> 4) as usize]);
                buffer.push(HEX_CHARS_UPPER[(byte & 0x0f) as usize]);
            }
            // SAFETY: buffer contains only ASCII hex digests, which are valid UTF-8
            unsafe {
                builder.append_value(from_utf8_unchecked(&buffer));
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

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

/// Generic hex encoding for int64 type
fn hex_encode_int64(
    iter: impl ExactSizeIterator<Item = Option<i64>>,
) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(iter.len(), iter.len() * 16);

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

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod test {
    use std::str::from_utf8_unchecked;
    use std::sync::Arc;

    use arrow::array::{DictionaryArray, Int32Array, Int64Array, StringArray};

    use datafusion_common::cast::as_dictionary_array;

    use super::*;

    #[test]
    fn test_hex_int64() {
        let test_cases = vec![(1234, "4D2"), (-1, "FFFFFFFFFFFFFFFF")];

        for (num, expected) in test_cases {
            let mut cache = [0u8; 16];
            let slice = hex_int64(num, &mut cache);

            let result = unsafe { from_utf8_unchecked(slice) };
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn test_dict_values_null() {
        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let vals = Int64Array::from(vec![Some(32), None]);
        // [32, null, null]
        let dict = Arc::new(DictionaryArray::new(keys.clone(), Arc::new(vals)));

        let result = compute_hex(&[dict]).unwrap();
        let result = as_dictionary_array(&result).unwrap();

        let vals = StringArray::from(vec![Some("20"), None]);
        let expected = DictionaryArray::new(keys, Arc::new(vals));

        assert_eq!(&expected, result);
    }
}
