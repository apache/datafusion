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

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::utils::hex::{HexCase, encode_u64};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

/// Converts the number to its equivalent hexadecimal representation.
/// to_hex(2147483647) = '7fffffff'
fn to_hex_array<T: ArrowPrimitiveType>(array: &ArrayRef) -> Result<ArrayRef>
where
    T::Native: ToHex,
{
    let integer_array = as_primitive_array::<T>(array)?;
    let len = integer_array.len();

    // Max hex string length: 16 chars for u64/i64
    let max_hex_len = T::Native::get_byte_width() * 2;

    // Pre-allocate buffers - avoid the builder API overhead
    let mut offsets: Vec<i32> = Vec::with_capacity(len + 1);
    let mut values: Vec<u8> = Vec::with_capacity(len * max_hex_len);

    // Reusable buffer for hex conversion
    let mut hex_buffer = [0u8; 16];

    // Start with offset 0
    offsets.push(0);

    // Process all values directly (including null slots - we write empty strings for nulls)
    // The null bitmap will mark which entries are actually null
    for value in integer_array.values() {
        values.extend_from_slice(value.write_hex(&mut hex_buffer));
        offsets.push(values.len() as i32);
    }

    // Copy null bitmap from input (nulls pass through unchanged)
    let nulls = integer_array.nulls().cloned();

    // SAFETY: offsets are valid (monotonically increasing, last value equals values.len())
    // and values contains valid UTF-8 (only ASCII hex digits)
    let offsets =
        unsafe { OffsetBuffer::new_unchecked(Buffer::from_vec(offsets).into()) };
    let result = StringArray::new(offsets, Buffer::from_vec(values), nulls);

    Ok(Arc::new(result) as ArrayRef)
}

#[inline]
fn to_hex_scalar<T: ToHex>(value: T) -> String {
    let mut hex_buffer = [0u8; 16];
    let hex = value.write_hex(&mut hex_buffer);
    // SAFETY: hex holds only ASCII hex digits.
    unsafe { std::str::from_utf8_unchecked(hex).to_string() }
}

/// Trait for converting integer types to hexadecimal in a buffer
trait ToHex: ArrowNativeType {
    /// Writes the hex representation into `buf` and returns the written
    /// subslice. Digits are right-aligned in `buf` with leading zeros trimmed.
    fn write_hex(self, buf: &mut [u8; 16]) -> &[u8];
}

/// Signed values use their two's complement representation, matching a cast to
/// the corresponding unsigned type.
macro_rules! impl_to_hex_signed {
    ($ty:ty) => {
        impl ToHex for $ty {
            #[inline]
            fn write_hex(self, buf: &mut [u8; 16]) -> &[u8] {
                encode_u64(self as i64 as u64, HexCase::Lower, buf)
            }
        }
    };
}

macro_rules! impl_to_hex_unsigned {
    ($ty:ty) => {
        impl ToHex for $ty {
            #[inline]
            fn write_hex(self, buf: &mut [u8; 16]) -> &[u8] {
                encode_u64(self as u64, HexCase::Lower, buf)
            }
        }
    };
}

impl_to_hex_signed!(i8);
impl_to_hex_signed!(i16);
impl_to_hex_signed!(i32);
impl_to_hex_signed!(i64);
impl_to_hex_unsigned!(u8);
impl_to_hex_unsigned!(u16);
impl_to_hex_unsigned!(u32);
impl_to_hex_unsigned!(u64);

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Converts an integer to a hexadecimal string.",
    syntax_example = "to_hex(int)",
    sql_example = r#"```sql
> select to_hex(12345689);
+-------------------------+
| to_hex(Int64(12345689)) |
+-------------------------+
| bc6159                  |
+-------------------------+
```"#,
    standard_argument(name = "int", prefix = "Integer")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToHexFunc {
    signature: Signature,
}

impl Default for ToHexFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToHexFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Integer)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ToHexFunc {
    fn name(&self) -> &str {
        "to_hex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = &args.args[0];

        match arg {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(to_hex_scalar(*v)))),
            ),
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(v))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(to_hex_scalar(*v)))),
            ),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(to_hex_scalar(*v)))),
            ),
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(to_hex_scalar(*v)))),
            ),
            ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(to_hex_scalar(*v)))),
            ),
            ColumnarValue::Scalar(ScalarValue::UInt16(Some(v))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(to_hex_scalar(*v)))),
            ),
            ColumnarValue::Scalar(ScalarValue::Int8(Some(v))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(to_hex_scalar(*v)))),
            ),
            ColumnarValue::Scalar(ScalarValue::UInt8(Some(v))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(to_hex_scalar(*v)))),
            ),

            // NULL scalars
            ColumnarValue::Scalar(s) if s.is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }

            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Int64 => {
                    Ok(ColumnarValue::Array(to_hex_array::<Int64Type>(array)?))
                }
                DataType::UInt64 => {
                    Ok(ColumnarValue::Array(to_hex_array::<UInt64Type>(array)?))
                }
                DataType::Int32 => {
                    Ok(ColumnarValue::Array(to_hex_array::<Int32Type>(array)?))
                }
                DataType::UInt32 => {
                    Ok(ColumnarValue::Array(to_hex_array::<UInt32Type>(array)?))
                }
                DataType::Int16 => {
                    Ok(ColumnarValue::Array(to_hex_array::<Int16Type>(array)?))
                }
                DataType::UInt16 => {
                    Ok(ColumnarValue::Array(to_hex_array::<UInt16Type>(array)?))
                }
                DataType::Int8 => {
                    Ok(ColumnarValue::Array(to_hex_array::<Int8Type>(array)?))
                }
                DataType::UInt8 => {
                    Ok(ColumnarValue::Array(to_hex_array::<UInt8Type>(array)?))
                }
                other => exec_err!("Unsupported data type {other:?} for function to_hex"),
            },

            other => internal_err!(
                "Unexpected argument type {:?} for function to_hex",
                other.data_type()
            ),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Int8Array, Int16Array, Int32Array, Int64Array, StringArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use datafusion_common::cast::as_string_array;

    use super::*;

    macro_rules! test_to_hex_type {
        // Default test with standard input/output
        ($name:ident, $arrow_type:ty, $array_type:ty) => {
            test_to_hex_type!(
                $name,
                $arrow_type,
                $array_type,
                vec![Some(100), Some(0), None],
                vec![Some("64"), Some("0"), None]
            );
        };

        // Custom test with custom input/output (eg: positive number)
        ($name:ident, $arrow_type:ty, $array_type:ty, $input:expr, $expected:expr) => {
            #[test]
            fn $name() -> Result<()> {
                let input = $input;
                let expected = $expected;

                let array = <$array_type>::from(input);
                let array_ref: ArrayRef = Arc::new(array);
                let hex_result = to_hex_array::<$arrow_type>(&array_ref)?;
                let hex_array = as_string_array(&hex_result)?;
                let expected_array = StringArray::from(expected);

                assert_eq!(&expected_array, hex_array);
                Ok(())
            }
        };
    }

    test_to_hex_type!(
        to_hex_int8,
        Int8Type,
        Int8Array,
        vec![Some(100), Some(0), None, Some(-1)],
        vec![Some("64"), Some("0"), None, Some("ffffffffffffffff")]
    );
    test_to_hex_type!(
        to_hex_int16,
        Int16Type,
        Int16Array,
        vec![Some(100), Some(0), None, Some(-1)],
        vec![Some("64"), Some("0"), None, Some("ffffffffffffffff")]
    );
    test_to_hex_type!(
        to_hex_int32,
        Int32Type,
        Int32Array,
        vec![Some(100), Some(0), None, Some(-1)],
        vec![Some("64"), Some("0"), None, Some("ffffffffffffffff")]
    );
    test_to_hex_type!(
        to_hex_int64,
        Int64Type,
        Int64Array,
        vec![Some(100), Some(0), None, Some(-1)],
        vec![Some("64"), Some("0"), None, Some("ffffffffffffffff")]
    );

    test_to_hex_type!(to_hex_uint8, UInt8Type, UInt8Array);
    test_to_hex_type!(to_hex_uint16, UInt16Type, UInt16Array);
    test_to_hex_type!(to_hex_uint32, UInt32Type, UInt32Array);
    test_to_hex_type!(to_hex_uint64, UInt64Type, UInt64Array);

    test_to_hex_type!(
        to_hex_large_signed,
        Int64Type,
        Int64Array,
        vec![Some(i64::MAX), Some(i64::MIN)],
        vec![Some("7fffffffffffffff"), Some("8000000000000000")]
    );

    test_to_hex_type!(
        to_hex_large_unsigned,
        UInt64Type,
        UInt64Array,
        vec![Some(u64::MAX), Some(u64::MIN)],
        vec![Some("ffffffffffffffff"), Some("0")]
    );
}
