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
use arrow::array::{ArrayRef, GenericStringBuilder};
use arrow::datatypes::DataType::{
    Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Utf8,
};
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::Result;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{exec_err, plan_err};

use datafusion_expr::{ColumnarValue, Documentation};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::TypeSignature::Exact;
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

    // Max hex string length: 16 chars for u64/i64
    let max_hex_len = T::Native::get_byte_width() * 2;
    let mut result = GenericStringBuilder::<i32>::with_capacity(
        integer_array.len(),
        integer_array.len() * max_hex_len,
    );

    // Reusable buffer to avoid allocations - sized for max possible hex output
    let mut buffer = [0u8; 16];

    for integer in integer_array {
        if let Some(value) = integer {
            let hex_str = value.to_hex(&mut buffer);
            result.append_value(hex_str);
        } else {
            result.append_null();
        }
    }

    let result = result.finish();

    Ok(Arc::new(result) as ArrayRef)
}

/// Trait for converting integer types to hexadecimal strings
trait ToHex: ArrowNativeType {
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str;
}

/// Write unsigned value to hex buffer and return the string slice
#[inline]
fn write_unsigned_hex(value: u64, buffer: &mut [u8; 16]) -> &str {
    if value == 0 {
        buffer[0] = b'0';
        // SAFETY: "0" is valid UTF-8
        return unsafe { std::str::from_utf8_unchecked(&buffer[..1]) };
    }

    // Write hex digits from right to left
    let mut pos = 16;
    let mut v = value;
    while v > 0 {
        pos -= 1;
        buffer[pos] = HEX_CHARS[(v & 0xf) as usize];
        v >>= 4;
    }

    // SAFETY: HEX_CHARS contains only ASCII hex digits which are valid UTF-8
    unsafe { std::str::from_utf8_unchecked(&buffer[pos..]) }
}

/// Write signed value to hex buffer (two's complement for negative) and return the string slice
#[inline]
fn write_signed_hex(value: i64, buffer: &mut [u8; 16]) -> &str {
    // For negative values, use two's complement representation (same as casting to u64)
    write_unsigned_hex(value as u64, buffer)
}

impl ToHex for i8 {
    #[inline]
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str {
        write_signed_hex(self as i64, buffer)
    }
}

impl ToHex for i16 {
    #[inline]
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str {
        write_signed_hex(self as i64, buffer)
    }
}

impl ToHex for i32 {
    #[inline]
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str {
        write_signed_hex(self as i64, buffer)
    }
}

impl ToHex for i64 {
    #[inline]
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str {
        write_signed_hex(self, buffer)
    }
}

impl ToHex for u8 {
    #[inline]
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str {
        write_unsigned_hex(self as u64, buffer)
    }
}

impl ToHex for u16 {
    #[inline]
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str {
        write_unsigned_hex(self as u64, buffer)
    }
}

impl ToHex for u32 {
    #[inline]
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str {
        write_unsigned_hex(self as u64, buffer)
    }
}

impl ToHex for u64 {
    #[inline]
    fn to_hex(self, buffer: &mut [u8; 16]) -> &str {
        write_unsigned_hex(self, buffer)
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
            signature: Signature::one_of(
                vec![
                    Exact(vec![Int8]),
                    Exact(vec![Int16]),
                    Exact(vec![Int32]),
                    Exact(vec![Int64]),
                    Exact(vec![UInt8]),
                    Exact(vec![UInt16]),
                    Exact(vec![UInt32]),
                    Exact(vec![UInt64]),
                ],
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Utf8,
            _ => {
                return plan_err!("The to_hex function can only accept integers.");
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args[0].data_type() {
            Int64 => make_scalar_function(to_hex::<Int64Type>, vec![])(&args.args),
            UInt64 => make_scalar_function(to_hex::<UInt64Type>, vec![])(&args.args),
            Int32 => make_scalar_function(to_hex::<Int32Type>, vec![])(&args.args),
            UInt32 => make_scalar_function(to_hex::<UInt32Type>, vec![])(&args.args),
            Int16 => make_scalar_function(to_hex::<Int16Type>, vec![])(&args.args),
            UInt16 => make_scalar_function(to_hex::<UInt16Type>, vec![])(&args.args),
            Int8 => make_scalar_function(to_hex::<Int8Type>, vec![])(&args.args),
            UInt8 => make_scalar_function(to_hex::<UInt8Type>, vec![])(&args.args),
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
