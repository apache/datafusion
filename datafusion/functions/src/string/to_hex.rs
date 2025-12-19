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
use std::fmt::Write;
use std::sync::Arc;

use crate::utils::make_scalar_function;
use arrow::array::{ArrayRef, GenericStringBuilder};
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

/// Converts the number to its equivalent hexadecimal representation.
/// to_hex(2147483647) = '7fffffff'
fn to_hex<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: std::fmt::LowerHex,
{
    let integer_array = as_primitive_array::<T>(&args[0])?;

    let mut result = GenericStringBuilder::<i32>::with_capacity(
        integer_array.len(),
        // * 8 to convert to bits, / 4 bits per hex char
        integer_array.len() * (T::Native::get_byte_width() * 8 / 4),
    );

    for integer in integer_array {
        if let Some(value) = integer {
            if let Some(value_usize) = value.to_usize() {
                write!(result, "{value_usize:x}")?;
            } else if let Some(value_isize) = value.to_isize() {
                write!(result, "{value_isize:x}")?;
            } else {
                return exec_err!(
                    "Unsupported data type {integer:?} for function to_hex"
                );
            }
            result.append_value("");
        } else {
            result.append_null();
        }
    }

    let result = result.finish();

    Ok(Arc::new(result) as ArrayRef)
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
