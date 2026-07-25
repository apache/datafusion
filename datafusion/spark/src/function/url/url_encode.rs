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

use arrow::array::{
    Array, ArrayRef, LargeStringBuilder, StringBuilder, StringViewBuilder,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use url::form_urlencoded::byte_serialize;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UrlEncode {
    signature: Signature,
}

impl Default for UrlEncode {
    fn default() -> Self {
        Self::new()
    }
}

impl UrlEncode {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UrlEncode {
    fn name(&self) -> &str {
        "url_encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!(
                "{} expects 1 argument, but got {}",
                self.name(),
                arg_types.len()
            );
        }
        // As the type signature is already checked, we can safely return the type of the first argument
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_url_encode, vec![])(&args)
    }
}

/// Core implementation of URL encoding function.
///
/// # Arguments
///
/// * `args` - A slice containing exactly one ArrayRef with the strings to encode
///
/// # Returns
///
/// * `Ok(ArrayRef)` - A new array of the same type containing encoded strings
/// * `Err(DataFusionError)` - If invalid arguments are provided
///
fn spark_url_encode(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("`url_encode` expects 1 argument");
    }

    // The percent-encoded form of each value is assembled in a single scratch buffer
    // reused across rows, rather than allocating a `String` per row.
    macro_rules! encode_all {
        ($array:expr, $builder:expr) => {{
            let array = $array;
            let mut builder = $builder;
            let mut encoded = String::new();
            for value in array.iter() {
                match value {
                    Some(value) => {
                        encoded.clear();
                        encoded.extend(byte_serialize(value.as_bytes()));
                        builder.append_value(&encoded);
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }};
    }

    match &args[0].data_type() {
        DataType::Utf8 => {
            let array = as_string_array(&args[0])?;
            let builder =
                StringBuilder::with_capacity(array.len(), array.value_data().len());
            encode_all!(array, builder)
        }
        DataType::LargeUtf8 => {
            let array = as_large_string_array(&args[0])?;
            let builder =
                LargeStringBuilder::with_capacity(array.len(), array.value_data().len());
            encode_all!(array, builder)
        }
        DataType::Utf8View => {
            let array = as_string_view_array(&args[0])?;
            let builder = StringViewBuilder::with_capacity(array.len());
            encode_all!(array, builder)
        }
        other => exec_err!("`url_encode`: Expr must be STRING, got {other:?}"),
    }
}
