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

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

/// Hex lookup table for fast conversion
const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Converts the number to its equivalent hexadecimal representation.
/// to_hex(2147483647) = '7fffffff'
fn to_hex<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: ToHex,
{
    let integer_array = as_primitive_array::<T>(&args[0])?;
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
        let hex_len = value.write_hex_to_buffer(&mut hex_buffer);
        values.extend_from_slice(&hex_buffer[16 - hex_len..]);
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

/// Trait for converting integer types to hexadecimal in a buffer
trait ToHex: ArrowNativeType {
    /// Write hex representation to buffer and return the number of hex digits written.
    /// The hex digits are written right-aligned in the buffer (starting from position 16 - len).
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize;
}

/// Write unsigned value to hex buffer and return the number of digits written.
/// Digits are written right-aligned in the buffer.
#[inline]
fn write_unsigned_hex_to_buffer(value: u64, buffer: &mut [u8; 16]) -> usize {
    if value == 0 {
        buffer[15] = b'0';
        return 1;
    }

    // Write hex digits from right to left
    let mut pos = 16;
    let mut v = value;
    while v > 0 {
        pos -= 1;
        buffer[pos] = HEX_CHARS[(v & 0xf) as usize];
        v >>= 4;
    }

    16 - pos
}

/// Write signed value to hex buffer (two's complement for negative) and return digit count
#[inline]
fn write_signed_hex_to_buffer(value: i64, buffer: &mut [u8; 16]) -> usize {
    // For negative values, use two's complement representation (same as casting to u64)
    write_unsigned_hex_to_buffer(value as u64, buffer)
}

impl ToHex for i8 {
    #[inline]
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize {
        write_signed_hex_to_buffer(self as i64, buffer)
    }
}

impl ToHex for i16 {
    #[inline]
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize {
        write_signed_hex_to_buffer(self as i64, buffer)
    }
}

impl ToHex for i32 {
    #[inline]
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize {
        write_signed_hex_to_buffer(self as i64, buffer)
    }
}

impl ToHex for i64 {
    #[inline]
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize {
        write_signed_hex_to_buffer(self, buffer)
    }
}

impl ToHex for u8 {
    #[inline]
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize {
        write_unsigned_hex_to_buffer(self as u64, buffer)
    }
}

impl ToHex for u16 {
    #[inline]
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize {
        write_unsigned_hex_to_buffer(self as u64, buffer)
    }
}

impl ToHex for u32 {
    #[inline]
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize {
        write_unsigned_hex_to_buffer(self as u64, buffer)
    }
}

impl ToHex for u64 {
    #[inline]
    fn write_hex_to_buffer(self, buffer: &mut [u8; 16]) -> usize {
        write_unsigned_hex_to_buffer(self, buffer)
    }
}

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        match args.args[0].data_type() {
            DataType::Null => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            DataType::Int64 => {
                make_scalar_function(to_hex::<Int64Type>, vec![])(&args.args)
            }
            DataType::UInt64 => {
                make_scalar_function(to_hex::<UInt64Type>, vec![])(&args.args)
            }
            DataType::Int32 => {
                make_scalar_function(to_hex::<Int32Type>, vec![])(&args.args)
            }
            DataType::UInt32 => {
                make_scalar_function(to_hex::<UInt32Type>, vec![])(&args.args)
            }
            DataType::Int16 => {
                make_scalar_function(to_hex::<Int16Type>, vec![])(&args.args)
            }
            DataType::UInt16 => {
                make_scalar_function(to_hex::<UInt16Type>, vec![])(&args.args)
            }
            DataType::Int8 => {
                make_scalar_function(to_hex::<Int8Type>, vec![])(&args.args)
            }
            DataType::UInt8 => {
                make_scalar_function(to_hex::<UInt8Type>, vec![])(&args.args)
            }
            other => exec_err!("Unsupported data type {other:?} for function to_hex"),
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
                let array_ref = Arc::new(array);
                let hex_result = to_hex::<$arrow_type>(&[array_ref])?;
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
