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

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion_common::types::{NativeType, logical_int64};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use std::sync::Arc;

/// Spark-compatible `bin` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#bin>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBin {
    signature: Signature,
}

impl Default for SparkBin {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBin {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Coercible(vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_int64()),
                    vec![TypeSignatureClass::Numeric],
                    NativeType::Int64,
                )])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkBin {
    fn name(&self) -> &str {
        "bin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Utf8,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_bin_inner, vec![])(&args.args)
    }
}

fn spark_bin_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("bin", arg)?;
    match &array.data_type() {
        DataType::Int64 => {
            let array = array.as_primitive::<Int64Type>();
            let len = array.len();
            // Most values are small, so 8 digits per row is a reasonable estimate;
            // the buffer grows on its own for wider ones.
            let mut builder = StringBuilder::with_capacity(len, len * 8);
            // Digits are rendered into this stack buffer, so no row allocates.
            let mut digits = [0u8; MAX_BIN_DIGITS];
            for value in array.iter() {
                match value {
                    Some(value) => builder.append_value(spark_bin(value, &mut digits)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        data_type => {
            internal_err!("bin does not support: {data_type}")
        }
    }
}

/// An `i64` renders as at most 64 binary digits.
const MAX_BIN_DIGITS: usize = 64;

/// Renders `value` as binary, right-aligned in `digits`, and returns the digits written.
///
/// Negative values render as their two's-complement bit pattern, matching `{:b}`.
fn spark_bin(value: i64, digits: &mut [u8; MAX_BIN_DIGITS]) -> &str {
    let mut pos = MAX_BIN_DIGITS;
    let mut remaining = value as u64;
    // `while` alone would produce an empty string for zero.
    loop {
        pos -= 1;
        digits[pos] = b'0' + (remaining & 1) as u8;
        remaining >>= 1;
        if remaining == 0 {
            break;
        }
    }
    // SAFETY: every byte written above is an ASCII '0' or '1'.
    unsafe { std::str::from_utf8_unchecked(&digits[pos..]) }
}
