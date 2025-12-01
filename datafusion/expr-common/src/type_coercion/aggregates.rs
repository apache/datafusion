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

use crate::signature::TypeSignature;
use arrow::datatypes::{DataType, FieldRef};

use datafusion_common::{Result, internal_err, plan_err};

// TODO: remove usage of these (INTEGERS and NUMERICS) in favour of signatures
//       see https://github.com/apache/datafusion/issues/18092
pub static INTEGERS: &[DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
];

pub static NUMERICS: &[DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Float32,
    DataType::Float64,
];

/// Validate the length of `input_fields` matches the `signature` for `agg_fun`.
///
/// This method DOES NOT validate the argument fields - only that (at least one,
/// in the case of [`TypeSignature::OneOf`]) signature matches the desired
/// number of input types.
pub fn check_arg_count(
    func_name: &str,
    input_fields: &[FieldRef],
    signature: &TypeSignature,
) -> Result<()> {
    match signature {
        TypeSignature::Uniform(agg_count, _) | TypeSignature::Any(agg_count) => {
            if input_fields.len() != *agg_count {
                return plan_err!(
                    "The function {func_name} expects {:?} arguments, but {:?} were provided",
                    agg_count,
                    input_fields.len()
                );
            }
        }
        TypeSignature::Exact(types) => {
            if types.len() != input_fields.len() {
                return plan_err!(
                    "The function {func_name} expects {:?} arguments, but {:?} were provided",
                    types.len(),
                    input_fields.len()
                );
            }
        }
        TypeSignature::OneOf(variants) => {
            let ok = variants
                .iter()
                .any(|v| check_arg_count(func_name, input_fields, v).is_ok());
            if !ok {
                return plan_err!(
                    "The function {func_name} does not accept {:?} function arguments.",
                    input_fields.len()
                );
            }
        }
        TypeSignature::VariadicAny => {
            if input_fields.is_empty() {
                return plan_err!(
                    "The function {func_name} expects at least one argument"
                );
            }
        }
        TypeSignature::UserDefined
        | TypeSignature::Numeric(_)
        | TypeSignature::Coercible(_) => {
            // User-defined signature is validated in `coerce_types`
            // Numeric and Coercible signature is validated in `get_valid_types`
        }
        _ => {
            return internal_err!(
                "Aggregate functions do not support this {signature:?}"
            );
        }
    }
    Ok(())
}
