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

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

use crate::type_coercion::functions::data_types;
use crate::{FuncMonotonicity, Signature, Volatility};

use arrow::datatypes::DataType;
use datafusion_common::{plan_err, DataFusionError, Result};

use strum::IntoEnumIterator;
use strum_macros::EnumIter;

/// Enum of all built-in scalar functions
// Contributor's guide for adding new scalar functions
// https://arrow.apache.org/datafusion/contributor-guide/index.html#how-to-add-a-new-scalar-function
#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumIter, Copy)]
pub enum BuiltinScalarFunction {
    // math functions
    /// coalesce
    Coalesce,
}

/// Maps the sql function name to `BuiltinScalarFunction`
fn name_to_function() -> &'static HashMap<&'static str, BuiltinScalarFunction> {
    static NAME_TO_FUNCTION_LOCK: OnceLock<HashMap<&'static str, BuiltinScalarFunction>> =
        OnceLock::new();
    NAME_TO_FUNCTION_LOCK.get_or_init(|| {
        let mut map = HashMap::new();
        BuiltinScalarFunction::iter().for_each(|func| {
            func.aliases().iter().for_each(|&a| {
                map.insert(a, func);
            });
        });
        map
    })
}

/// Maps `BuiltinScalarFunction` --> canonical sql function
/// First alias in the array is used to display function names
fn function_to_name() -> &'static HashMap<BuiltinScalarFunction, &'static str> {
    static FUNCTION_TO_NAME_LOCK: OnceLock<HashMap<BuiltinScalarFunction, &'static str>> =
        OnceLock::new();
    FUNCTION_TO_NAME_LOCK.get_or_init(|| {
        let mut map = HashMap::new();
        BuiltinScalarFunction::iter().for_each(|func| {
            map.insert(func, *func.aliases().first().unwrap_or(&"NO_ALIAS"));
        });
        map
    })
}

impl BuiltinScalarFunction {
    /// an allowlist of functions to take zero arguments, so that they will get special treatment
    /// while executing.
    #[deprecated(
        since = "32.0.0",
        note = "please use TypeSignature::supports_zero_argument instead"
    )]
    pub fn supports_zero_argument(&self) -> bool {
        self.signature().type_signature.supports_zero_argument()
    }

    /// Returns the name of this function
    pub fn name(&self) -> &str {
        // .unwrap is safe here because compiler makes sure the map will have matches for each BuiltinScalarFunction
        function_to_name().get(self).unwrap()
    }

    /// Returns the [Volatility] of the builtin function.
    pub fn volatility(&self) -> Volatility {
        match self {
            // Immutable scalar builtins
            BuiltinScalarFunction::Coalesce => Volatility::Immutable,
        }
    }

    /// Returns the output [`DataType`] of this function
    ///
    /// This method should be invoked only after `input_expr_types` have been validated
    /// against the function's `TypeSignature` using `type_coercion::functions::data_types()`.
    ///
    /// This method will:
    /// 1. Perform additional checks on `input_expr_types` that are beyond the scope of `TypeSignature` validation.
    /// 2. Deduce the output `DataType` based on the provided `input_expr_types`.
    pub fn return_type(self, input_expr_types: &[DataType]) -> Result<DataType> {
        // Note that this function *must* return the same type that the respective physical expression returns
        // or the execution panics.

        // the return type of the built in function.
        // Some built-in functions' return type depends on the incoming type.
        match self {
            BuiltinScalarFunction::Coalesce => {
                // COALESCE has multiple args and they might get coerced, get a preview of this
                let coerced_types = data_types(input_expr_types, &self.signature());
                coerced_types.map(|types| types[0].clone())
            }
        }
    }

    /// Return the argument [`Signature`] supported by this function
    pub fn signature(&self) -> Signature {
        // note: the physical expression must accept the type returned by this function or the execution panics.

        // for now, the list is small, as we do not have many built-in functions.
        match self {
            BuiltinScalarFunction::Coalesce => {
                Signature::variadic_equal(self.volatility())
            }
        }
    }

    /// This function specifies monotonicity behaviors for built-in scalar functions.
    /// The list can be extended, only mathematical and datetime functions are
    /// considered for the initial implementation of this feature.
    pub fn monotonicity(&self) -> Option<FuncMonotonicity> {
        None
    }

    /// Returns all names that can be used to call this function
    pub fn aliases(&self) -> &'static [&'static str] {
        match self {
            // conditional functions
            BuiltinScalarFunction::Coalesce => &["coalesce"],
        }
    }
}

impl fmt::Display for BuiltinScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for BuiltinScalarFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltinScalarFunction> {
        if let Some(func) = name_to_function().get(name) {
            Ok(*func)
        } else {
            plan_err!("There is no built-in function named {name}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test for BuiltinScalarFunction's Display and from_str() implementations.
    // For each variant in BuiltinScalarFunction, it converts the variant to a string
    // and then back to a variant. The test asserts that the original variant and
    // the reconstructed variant are the same. This assertion is also necessary for
    // function suggestion. See https://github.com/apache/arrow-datafusion/issues/8082
    fn test_display_and_from_str() {
        for (_, func_original) in name_to_function().iter() {
            let func_name = func_original.to_string();
            let func_from_str = BuiltinScalarFunction::from_str(&func_name).unwrap();
            assert_eq!(func_from_str, *func_original);
        }
    }

    #[test]
    fn test_coalesce_return_types() {
        let coalesce = BuiltinScalarFunction::Coalesce;
        let return_type = coalesce
            .return_type(&[DataType::Date32, DataType::Date32])
            .unwrap();
        assert_eq!(return_type, DataType::Date32);
    }

    #[test]
    fn test_coalesce_return_types_dictionary() {
        let coalesce = BuiltinScalarFunction::Coalesce;
        let return_type = coalesce
            .return_type(&[
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                DataType::Utf8,
            ])
            .unwrap();
        assert_eq!(
            return_type,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
    }
}
