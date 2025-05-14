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

use arrow::array::{ArrayRef, GenericStringBuilder, OffsetSizeTrait};
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int32Type, Int64Type,
};

use crate::utils::make_scalar_function;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::Result;
use datafusion_common::{exec_err, plan_err};

use datafusion_expr::{ColumnarValue, Documentation};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

/// Converts the number to its equivalent hexadecimal representation.
/// to_hex(2147483647) = '7fffffff'
pub fn to_hex<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
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
#[derive(Debug)]
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
        use DataType::*;
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
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
        use DataType::*;

        Ok(match arg_types[0] {
            Int8 | Int16 | Int32 | Int64 => Utf8,
            _ => {
                return plan_err!("The to_hex function can only accept integers.");
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args[0].data_type() {
            DataType::Int32 => {
                make_scalar_function(to_hex::<Int32Type>, vec![])(&args.args)
            }
            DataType::Int64 => {
                make_scalar_function(to_hex::<Int64Type>, vec![])(&args.args)
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
    use arrow::array::{Int32Array, StringArray};

    use datafusion_common::cast::as_string_array;

    use super::*;

    #[test]
    // Test to_hex function for zero
    fn to_hex_zero() -> Result<()> {
        let array = vec![0].into_iter().collect::<Int32Array>();
        let array_ref = Arc::new(array);
        let hex_value_arc = to_hex::<Int32Type>(&[array_ref])?;
        let hex_value = as_string_array(&hex_value_arc)?;
        let expected = StringArray::from(vec![Some("0")]);
        assert_eq!(&expected, hex_value);

        Ok(())
    }

    #[test]
    // Test to_hex function for positive number
    fn to_hex_positive_number() -> Result<()> {
        let array = vec![100].into_iter().collect::<Int32Array>();
        let array_ref = Arc::new(array);
        let hex_value_arc = to_hex::<Int32Type>(&[array_ref])?;
        let hex_value = as_string_array(&hex_value_arc)?;
        let expected = StringArray::from(vec![Some("64")]);
        assert_eq!(&expected, hex_value);

        Ok(())
    }

    #[test]
    // Test to_hex function for negative number
    fn to_hex_negative_number() -> Result<()> {
        let array = vec![-1].into_iter().collect::<Int32Array>();
        let array_ref = Arc::new(array);
        let hex_value_arc = to_hex::<Int32Type>(&[array_ref])?;
        let hex_value = as_string_array(&hex_value_arc)?;
        let expected = StringArray::from(vec![Some("ffffffffffffffff")]);
        assert_eq!(&expected, hex_value);

        Ok(())
    }
}
