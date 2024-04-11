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

use arrow::array::{
    ArrayRef, ArrowPrimitiveType, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_int_type};

#[derive(Debug)]
pub struct StrposFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl StrposFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![Utf8, LargeUtf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("instr"), String::from("position")],
        }
    }
}

impl ScalarUDFImpl for StrposFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "strpos"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "strpos/instr/position")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(strpos::<Int32Type>, vec![])(args),
            DataType::LargeUtf8 => {
                make_scalar_function(strpos::<Int64Type>, vec![])(args)
            }
            other => exec_err!("Unsupported data type {other:?} for function strpos"),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)
/// strpos('high', 'ig') = 2
/// The implementation uses UTF-8 code points as characters
fn strpos<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    let string_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[0])?;

    let substring_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[1])?;

    let result = string_array
        .iter()
        .zip(substring_array.iter())
        .map(|(string, substring)| match (string, substring) {
            (Some(string), Some(substring)) => {
                // the find method returns the byte index of the substring
                // Next, we count the number of the chars until that byte
                T::Native::from_usize(
                    string
                        .find(substring)
                        .map(|x| string[..x].chars().count() + 1)
                        .unwrap_or(0),
                )
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}
