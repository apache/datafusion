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
pub(super) struct FindInSetFunc {
    signature: Signature,
}

impl FindInSetFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Utf8, Utf8]), Exact(vec![LargeUtf8, LargeUtf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FindInSetFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "find_in_set"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "find_in_set")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(find_in_set::<Int32Type>, vec![])(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(find_in_set::<Int64Type>, vec![])(args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function find_in_set")
            }
        }
    }
}

///Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings
///A string list is a string composed of substrings separated by , characters.
pub fn find_in_set<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    if args.len() != 2 {
        return exec_err!(
            "find_in_set was called with {} arguments. It requires 2.",
            args.len()
        );
    }

    let str_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[0])?;
    let str_list_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[1])?;

    let result = str_array
        .iter()
        .zip(str_list_array.iter())
        .map(|(string, str_list)| match (string, str_list) {
            (Some(string), Some(str_list)) => {
                let mut res = 0;
                let str_set: Vec<&str> = str_list.split(',').collect();
                for (idx, str) in str_set.iter().enumerate() {
                    if str == &string {
                        res = idx + 1;
                        break;
                    }
                }
                T::Native::from_usize(res)
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();
    Ok(Arc::new(result) as ArrayRef)
}
