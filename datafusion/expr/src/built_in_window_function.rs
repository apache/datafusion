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

//! Built-in functions module contains all the built-in functions definitions.

use std::fmt;
use std::str::FromStr;

use crate::type_coercion::functions::data_types;
use crate::utils;
use crate::{Signature, Volatility};
use datafusion_common::{plan_datafusion_err, plan_err, DataFusionError, Result};

use arrow::datatypes::DataType;

use strum_macros::EnumIter;

impl fmt::Display for BuiltInWindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// A [window function] built in to DataFusion
///
/// [Window Function]: https://en.wikipedia.org/wiki/Window_function_(SQL)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, EnumIter)]
pub enum BuiltInWindowFunction {
    /// returns value evaluated at the row that is the first row of the window frame
    FirstValue,
    /// Returns value evaluated at the row that is the last row of the window frame
    LastValue,
    /// Returns value evaluated at the row that is the nth row of the window frame (counting from 1); returns null if no such row
    NthValue,
}

impl BuiltInWindowFunction {
    pub fn name(&self) -> &str {
        use BuiltInWindowFunction::*;
        match self {
            FirstValue => "first_value",
            LastValue => "last_value",
            NthValue => "NTH_VALUE",
        }
    }
}

impl FromStr for BuiltInWindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltInWindowFunction> {
        Ok(match name.to_uppercase().as_str() {
            "FIRST_VALUE" => BuiltInWindowFunction::FirstValue,
            "LAST_VALUE" => BuiltInWindowFunction::LastValue,
            "NTH_VALUE" => BuiltInWindowFunction::NthValue,
            _ => return plan_err!("There is no built-in window function named {name}"),
        })
    }
}

/// Returns the datatype of the built-in window function
impl BuiltInWindowFunction {
    pub fn return_type(&self, input_expr_types: &[DataType]) -> Result<DataType> {
        // Note that this function *must* return the same type that the respective physical expression returns
        // or the execution panics.

        // Verify that this is a valid set of data types for this function
        data_types(input_expr_types, &self.signature())
            // Original errors are all related to wrong function signature
            // Aggregate them for better error message
            .map_err(|_| {
                plan_datafusion_err!(
                    "{}",
                    utils::generate_signature_error_msg(
                        &format!("{self}"),
                        self.signature(),
                        input_expr_types,
                    )
                )
            })?;

        match self {
            BuiltInWindowFunction::FirstValue
            | BuiltInWindowFunction::LastValue
            | BuiltInWindowFunction::NthValue => Ok(input_expr_types[0].clone()),
        }
    }

    /// The signatures supported by the built-in window function `fun`.
    pub fn signature(&self) -> Signature {
        // Note: The physical expression must accept the type returned by this function or the execution panics.
        match self {
            BuiltInWindowFunction::FirstValue | BuiltInWindowFunction::LastValue => {
                Signature::any(1, Volatility::Immutable)
            }
            BuiltInWindowFunction::NthValue => Signature::any(2, Volatility::Immutable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;
    #[test]
    // Test for BuiltInWindowFunction's Display and from_str() implementations.
    // For each variant in BuiltInWindowFunction, it converts the variant to a string
    // and then back to a variant. The test asserts that the original variant and
    // the reconstructed variant are the same. This assertion is also necessary for
    // function suggestion. See https://github.com/apache/datafusion/issues/8082
    fn test_display_and_from_str() {
        for func_original in BuiltInWindowFunction::iter() {
            let func_name = func_original.to_string();
            let func_from_str = BuiltInWindowFunction::from_str(&func_name).unwrap();
            assert_eq!(func_from_str, func_original);
        }
    }
}
