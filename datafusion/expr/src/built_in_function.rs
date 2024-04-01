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
use crate::{FuncMonotonicity, Signature, TypeSignature, Volatility};

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
    /// cbrt
    Cbrt,
    /// ceil
    Ceil,
    /// coalesce
    Coalesce,
    /// cos
    Cos,
    /// cos
    Cosh,
    /// degrees
    Degrees,
    /// exp
    Exp,
    /// factorial
    Factorial,
    /// floor
    Floor,
    /// gcd, Greatest common divisor
    Gcd,
    /// lcm, Least common multiple
    Lcm,
    /// iszero
    Iszero,
    /// log, same as log10
    Log,
    /// nanvl
    Nanvl,
    /// pi
    Pi,
    /// power
    Power,
    /// radians
    Radians,
    /// round
    Round,
    /// signum
    Signum,
    /// sin
    Sin,
    /// sinh
    Sinh,
    /// sqrt
    Sqrt,
    /// trunc
    Trunc,
    /// cot
    Cot,

    // string functions
    /// concat
    Concat,
    /// concat_ws
    ConcatWithSeparator,
    /// ends_with
    EndsWith,
    /// initcap
    InitCap,
    /// random
    Random,
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
            BuiltinScalarFunction::Ceil => Volatility::Immutable,
            BuiltinScalarFunction::Coalesce => Volatility::Immutable,
            BuiltinScalarFunction::Cos => Volatility::Immutable,
            BuiltinScalarFunction::Cosh => Volatility::Immutable,
            BuiltinScalarFunction::Degrees => Volatility::Immutable,
            BuiltinScalarFunction::Exp => Volatility::Immutable,
            BuiltinScalarFunction::Factorial => Volatility::Immutable,
            BuiltinScalarFunction::Floor => Volatility::Immutable,
            BuiltinScalarFunction::Gcd => Volatility::Immutable,
            BuiltinScalarFunction::Iszero => Volatility::Immutable,
            BuiltinScalarFunction::Lcm => Volatility::Immutable,
            BuiltinScalarFunction::Log => Volatility::Immutable,
            BuiltinScalarFunction::Nanvl => Volatility::Immutable,
            BuiltinScalarFunction::Pi => Volatility::Immutable,
            BuiltinScalarFunction::Power => Volatility::Immutable,
            BuiltinScalarFunction::Round => Volatility::Immutable,
            BuiltinScalarFunction::Signum => Volatility::Immutable,
            BuiltinScalarFunction::Sin => Volatility::Immutable,
            BuiltinScalarFunction::Sinh => Volatility::Immutable,
            BuiltinScalarFunction::Sqrt => Volatility::Immutable,
            BuiltinScalarFunction::Cbrt => Volatility::Immutable,
            BuiltinScalarFunction::Cot => Volatility::Immutable,
            BuiltinScalarFunction::Trunc => Volatility::Immutable,
            BuiltinScalarFunction::Concat => Volatility::Immutable,
            BuiltinScalarFunction::ConcatWithSeparator => Volatility::Immutable,
            BuiltinScalarFunction::EndsWith => Volatility::Immutable,
            BuiltinScalarFunction::InitCap => Volatility::Immutable,
            BuiltinScalarFunction::Radians => Volatility::Immutable,

            // Volatile builtin functions
            BuiltinScalarFunction::Random => Volatility::Volatile,
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
        use DataType::*;

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
            BuiltinScalarFunction::Concat => Ok(Utf8),
            BuiltinScalarFunction::ConcatWithSeparator => Ok(Utf8),
            BuiltinScalarFunction::InitCap => {
                utf8_to_str_type(&input_expr_types[0], "initcap")
            }
            BuiltinScalarFunction::Pi => Ok(Float64),
            BuiltinScalarFunction::Random => Ok(Float64),
            BuiltinScalarFunction::EndsWith => Ok(Boolean),

            BuiltinScalarFunction::Factorial
            | BuiltinScalarFunction::Gcd
            | BuiltinScalarFunction::Lcm => Ok(Int64),

            BuiltinScalarFunction::Power => match &input_expr_types[0] {
                Int64 => Ok(Int64),
                _ => Ok(Float64),
            },

            BuiltinScalarFunction::Log => match &input_expr_types[0] {
                Float32 => Ok(Float32),
                _ => Ok(Float64),
            },

            BuiltinScalarFunction::Nanvl => match &input_expr_types[0] {
                Float32 => Ok(Float32),
                _ => Ok(Float64),
            },

            BuiltinScalarFunction::Iszero => Ok(Boolean),

            BuiltinScalarFunction::Ceil
            | BuiltinScalarFunction::Cos
            | BuiltinScalarFunction::Cosh
            | BuiltinScalarFunction::Degrees
            | BuiltinScalarFunction::Exp
            | BuiltinScalarFunction::Floor
            | BuiltinScalarFunction::Radians
            | BuiltinScalarFunction::Round
            | BuiltinScalarFunction::Signum
            | BuiltinScalarFunction::Sin
            | BuiltinScalarFunction::Sinh
            | BuiltinScalarFunction::Sqrt
            | BuiltinScalarFunction::Cbrt
            | BuiltinScalarFunction::Trunc
            | BuiltinScalarFunction::Cot => match input_expr_types[0] {
                Float32 => Ok(Float32),
                _ => Ok(Float64),
            },
        }
    }

    /// Return the argument [`Signature`] supported by this function
    pub fn signature(&self) -> Signature {
        use DataType::*;
        use TypeSignature::*;
        // note: the physical expression must accept the type returned by this function or the execution panics.

        // for now, the list is small, as we do not have many built-in functions.
        match self {
            BuiltinScalarFunction::Concat
            | BuiltinScalarFunction::ConcatWithSeparator => {
                Signature::variadic(vec![Utf8], self.volatility())
            }
            BuiltinScalarFunction::Coalesce => {
                Signature::variadic_equal(self.volatility())
            }
            BuiltinScalarFunction::InitCap => {
                Signature::uniform(1, vec![Utf8, LargeUtf8], self.volatility())
            }

            BuiltinScalarFunction::EndsWith => Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![Utf8, LargeUtf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                ],
                self.volatility(),
            ),
            BuiltinScalarFunction::Pi => Signature::exact(vec![], self.volatility()),
            BuiltinScalarFunction::Random => Signature::exact(vec![], self.volatility()),
            BuiltinScalarFunction::Power => Signature::one_of(
                vec![Exact(vec![Int64, Int64]), Exact(vec![Float64, Float64])],
                self.volatility(),
            ),
            BuiltinScalarFunction::Round => Signature::one_of(
                vec![
                    Exact(vec![Float64, Int64]),
                    Exact(vec![Float32, Int64]),
                    Exact(vec![Float64]),
                    Exact(vec![Float32]),
                ],
                self.volatility(),
            ),
            BuiltinScalarFunction::Trunc => Signature::one_of(
                vec![
                    Exact(vec![Float32, Int64]),
                    Exact(vec![Float64, Int64]),
                    Exact(vec![Float64]),
                    Exact(vec![Float32]),
                ],
                self.volatility(),
            ),

            BuiltinScalarFunction::Log => Signature::one_of(
                vec![
                    Exact(vec![Float32]),
                    Exact(vec![Float64]),
                    Exact(vec![Float32, Float32]),
                    Exact(vec![Float64, Float64]),
                ],
                self.volatility(),
            ),
            BuiltinScalarFunction::Nanvl => Signature::one_of(
                vec![Exact(vec![Float32, Float32]), Exact(vec![Float64, Float64])],
                self.volatility(),
            ),
            BuiltinScalarFunction::Factorial => {
                Signature::uniform(1, vec![Int64], self.volatility())
            }
            BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => {
                Signature::uniform(2, vec![Int64], self.volatility())
            }
            BuiltinScalarFunction::Cbrt
            | BuiltinScalarFunction::Ceil
            | BuiltinScalarFunction::Cos
            | BuiltinScalarFunction::Cosh
            | BuiltinScalarFunction::Degrees
            | BuiltinScalarFunction::Exp
            | BuiltinScalarFunction::Floor
            | BuiltinScalarFunction::Radians
            | BuiltinScalarFunction::Signum
            | BuiltinScalarFunction::Sin
            | BuiltinScalarFunction::Sinh
            | BuiltinScalarFunction::Sqrt
            | BuiltinScalarFunction::Cot => {
                // math expressions expect 1 argument of type f64 or f32
                // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
                // return the best approximation for it (in f64).
                // We accept f32 because in this case it is clear that the best approximation
                // will be as good as the number of digits in the number
                Signature::uniform(1, vec![Float64, Float32], self.volatility())
            }
            BuiltinScalarFunction::Iszero => Signature::one_of(
                vec![Exact(vec![Float32]), Exact(vec![Float64])],
                self.volatility(),
            ),
        }
    }

    /// This function specifies monotonicity behaviors for built-in scalar functions.
    /// The list can be extended, only mathematical and datetime functions are
    /// considered for the initial implementation of this feature.
    pub fn monotonicity(&self) -> Option<FuncMonotonicity> {
        if matches!(
            &self,
            BuiltinScalarFunction::Ceil
                | BuiltinScalarFunction::Degrees
                | BuiltinScalarFunction::Exp
                | BuiltinScalarFunction::Factorial
                | BuiltinScalarFunction::Floor
                | BuiltinScalarFunction::Radians
                | BuiltinScalarFunction::Round
                | BuiltinScalarFunction::Signum
                | BuiltinScalarFunction::Sinh
                | BuiltinScalarFunction::Sqrt
                | BuiltinScalarFunction::Cbrt
                | BuiltinScalarFunction::Trunc
                | BuiltinScalarFunction::Pi
        ) {
            Some(vec![Some(true)])
        } else if *self == BuiltinScalarFunction::Log {
            Some(vec![Some(true), Some(false)])
        } else {
            None
        }
    }

    /// Returns all names that can be used to call this function
    pub fn aliases(&self) -> &'static [&'static str] {
        match self {
            BuiltinScalarFunction::Cbrt => &["cbrt"],
            BuiltinScalarFunction::Ceil => &["ceil"],
            BuiltinScalarFunction::Cos => &["cos"],
            BuiltinScalarFunction::Cot => &["cot"],
            BuiltinScalarFunction::Cosh => &["cosh"],
            BuiltinScalarFunction::Degrees => &["degrees"],
            BuiltinScalarFunction::Exp => &["exp"],
            BuiltinScalarFunction::Factorial => &["factorial"],
            BuiltinScalarFunction::Floor => &["floor"],
            BuiltinScalarFunction::Gcd => &["gcd"],
            BuiltinScalarFunction::Iszero => &["iszero"],
            BuiltinScalarFunction::Lcm => &["lcm"],
            BuiltinScalarFunction::Log => &["log"],
            BuiltinScalarFunction::Nanvl => &["nanvl"],
            BuiltinScalarFunction::Pi => &["pi"],
            BuiltinScalarFunction::Power => &["power", "pow"],
            BuiltinScalarFunction::Radians => &["radians"],
            BuiltinScalarFunction::Random => &["random"],
            BuiltinScalarFunction::Round => &["round"],
            BuiltinScalarFunction::Signum => &["signum"],
            BuiltinScalarFunction::Sin => &["sin"],
            BuiltinScalarFunction::Sinh => &["sinh"],
            BuiltinScalarFunction::Sqrt => &["sqrt"],
            BuiltinScalarFunction::Trunc => &["trunc"],

            // conditional functions
            BuiltinScalarFunction::Coalesce => &["coalesce"],

            BuiltinScalarFunction::Concat => &["concat"],
            BuiltinScalarFunction::ConcatWithSeparator => &["concat_ws"],
            BuiltinScalarFunction::EndsWith => &["ends_with"],
            BuiltinScalarFunction::InitCap => &["initcap"],
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

/// Creates a function to identify the optimal return type of a string function given
/// the type of its first argument.
///
/// If the input type is `LargeUtf8` or `LargeBinary` the return type is
/// `$largeUtf8Type`,
///
/// If the input type is `Utf8` or `Binary` the return type is `$utf8Type`,
macro_rules! get_optimal_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                // LargeBinary inputs are automatically coerced to Utf8
                DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                // Binary inputs are automatically coerced to Utf8
                DataType::Utf8 | DataType::Binary => $utf8Type,
                DataType::Null => DataType::Null,
                DataType::Dictionary(_, value_type) => match **value_type {
                    DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                    DataType::Utf8 | DataType::Binary => $utf8Type,
                    DataType::Null => DataType::Null,
                    _ => {
                        return plan_err!(
                            "The {} function can only accept strings, but got {:?}.",
                            name.to_uppercase(),
                            **value_type
                        );
                    }
                },
                data_type => {
                    return plan_err!(
                        "The {} function can only accept strings, but got {:?}.",
                        name.to_uppercase(),
                        data_type
                    );
                }
            })
        }
    };
}

// `utf8_to_str_type`: returns either a Utf8 or LargeUtf8 based on the input type size.
get_optimal_return_type!(utf8_to_str_type, DataType::LargeUtf8, DataType::Utf8);

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
}
