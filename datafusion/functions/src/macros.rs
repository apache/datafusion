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

/// macro that exports a list of function names as:
/// 1. individual functions in an `expr_fn` module
/// 2. a single function that returns a list of all functions
///
/// Equivalent to
/// ```text
/// pub mod expr_fn {
///     use super::*;
///     /// Return encode(arg)
///     pub fn encode(args: Vec<Expr>) -> Expr {
///         super::encode().call(args)
///     }
///  ...
/// /// Return a list of all functions in this package
/// pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
///     vec![
///       encode(),
///       decode()
///    ]
/// }
/// ```
///
/// Exported functions accept:
/// - `Vec<Expr>` argument (single argument followed by a comma)
/// - Variable number of `Expr` arguments (zero or more arguments, must be without commas)
/// - Functions that require config (marked with `@config` prefix)
#[macro_export]
macro_rules! export_functions {
    ($(($FUNC:ident, $DOC:expr, $($arg:tt)*)),*) => {
        $(
            // switch to single-function cases below
            $crate::export_functions!(single $FUNC, $DOC, $($arg)*);
        )*
    };

    // function that requires config (marked with @config)
    (single $FUNC:ident, $DOC:expr, @config) => {
        #[doc = $DOC]
        pub fn $FUNC() -> datafusion_expr::Expr {
            use datafusion_common::config::ConfigOptions;
            super::$FUNC(&ConfigOptions::default()).call(vec![])
        }
    };

    // single vector argument (a single argument followed by a comma)
    (single $FUNC:ident, $DOC:expr, $arg:ident,) => {
        #[doc = $DOC]
        pub fn $FUNC($arg: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr {
            super::$FUNC().call($arg)
        }
    };

    // variadic arguments (zero or more arguments, without commas)
    (single $FUNC:ident, $DOC:expr, $($arg:ident)*) => {
        #[doc = $DOC]
        pub fn $FUNC($($arg: datafusion_expr::Expr),*) -> datafusion_expr::Expr {
            super::$FUNC().call(vec![$($arg),*])
        }
    };
}

/// Creates a singleton `ScalarUDF` of the `$UDF` function and a function
/// named `$NAME` which returns that singleton. Optionally use a custom constructor
/// `$CTOR` which defaults to `$UDF::new()` if not specified.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
#[macro_export]
macro_rules! make_udf_function {
    ($UDF:ty, $NAME:ident, $CTOR:expr) => {
        #[doc = concat!("Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ", stringify!($NAME))]
        pub fn $NAME() -> std::sync::Arc<datafusion_expr::ScalarUDF> {
            // Singleton instance of the function
            static INSTANCE: std::sync::LazyLock<
                std::sync::Arc<datafusion_expr::ScalarUDF>,
            > = std::sync::LazyLock::new(|| {
                std::sync::Arc::new(datafusion_expr::ScalarUDF::new_from_impl(
                    ($CTOR)(),
                ))
            });
            std::sync::Arc::clone(&INSTANCE)
        }
    };
    ($UDF:ty, $NAME:ident) => {
        make_udf_function!($UDF, $NAME, <$UDF>::new);
    };
}

/// Creates a singleton `ScalarUDF` of the `$UDF` function and a function
/// named `$NAME` which returns that singleton. The function takes a
/// configuration argument of type `$CONFIG_TYPE` to create the UDF.
#[macro_export]
macro_rules! make_udf_function_with_config {
    ($UDF:ty, $NAME:ident) => {
        #[doc = concat!("Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ", stringify!($NAME))]
        pub fn $NAME(config: &datafusion_common::config::ConfigOptions) -> std::sync::Arc<datafusion_expr::ScalarUDF> {
            std::sync::Arc::new(datafusion_expr::ScalarUDF::new_from_impl(
                <$UDF>::new_with_config(&config),
            ))
        }
    };
}

/// Macro creates a sub module if the feature is not enabled
///
/// The rationale for providing stub functions is to help users to configure datafusion
/// properly (so they get an error telling them why a function is not available)
/// instead of getting a cryptic "no function found" message at runtime.
macro_rules! make_stub_package {
    ($name:ident, $feature:literal) => {
        #[cfg(not(feature = $feature))]
        #[doc = concat!("Disabled. Enable via feature flag `", $feature, "`")]
        pub mod $name {
            use datafusion_expr::ScalarUDF;
            use log::debug;
            use std::sync::Arc;

            /// Returns an empty list of functions when the feature is not enabled
            pub fn functions() -> Vec<Arc<ScalarUDF>> {
                debug!("{} functions disabled", stringify!($name));
                vec![]
            }
        }
    };
}

/// Downcast a named argument to a specific array type, returning an internal error
/// if the cast fails
///
/// $ARG: ArrayRef
/// $NAME: name of the argument (for error messages)
/// $ARRAY_TYPE: the type of array to cast the argument to
#[macro_export]
macro_rules! downcast_named_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "could not cast {} to {}",
                $NAME,
                std::any::type_name::<$ARRAY_TYPE>()
            )
        })?
    }};
}

/// Downcast an argument to a specific array type, returning an internal error
/// if the cast fails
///
/// $ARG: ArrayRef
/// $ARRAY_TYPE: the type of array to cast the argument to
#[macro_export]
macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $crate::downcast_named_arg!($ARG, "", $ARRAY_TYPE)
    }};
}

/// Macro to create a unary math UDF.
///
/// A unary math function takes an argument of type Float32 or Float64,
/// applies a unary floating function to the argument, and returns a value of the same type.
///
/// $UDF: the name of the UDF struct that implements `ScalarUDFImpl`
/// $NAME: the name of the function
/// $UNARY_FUNC: the unary function to apply to the argument
/// $OUTPUT_ORDERING: the output ordering calculation method of the function
/// $GET_DOC: the function to get the documentation of the UDF
macro_rules! make_math_unary_udf {
    ($UDF:ident, $NAME:ident, $UNARY_FUNC:ident, $OUTPUT_ORDERING:expr, $EVALUATE_BOUNDS:expr, $GET_DOC:expr) => {
        $crate::make_udf_function!($NAME::$UDF, $NAME);

        mod $NAME {
            use std::any::Any;
            use std::sync::Arc;

            use arrow::array::{ArrayRef, AsArray};
            use arrow::datatypes::{DataType, Float32Type, Float64Type};
            use datafusion_common::{exec_err, Result};
            use datafusion_expr::interval_arithmetic::Interval;
            use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
            use datafusion_expr::{
                ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl,
                Signature, Volatility,
            };

            #[derive(Debug, PartialEq, Eq, Hash)]
            pub struct $UDF {
                signature: Signature,
            }

            impl $UDF {
                pub fn new() -> Self {
                    use DataType::*;
                    Self {
                        signature: Signature::uniform(
                            1,
                            vec![Float64, Float32],
                            Volatility::Immutable,
                        ),
                    }
                }
            }

            impl ScalarUDFImpl for $UDF {
                fn as_any(&self) -> &dyn Any {
                    self
                }
                fn name(&self) -> &str {
                    stringify!($NAME)
                }

                fn signature(&self) -> &Signature {
                    &self.signature
                }

                fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                    let arg_type = &arg_types[0];

                    match arg_type {
                        DataType::Float32 => Ok(DataType::Float32),
                        // For other types (possible values float64/null/int), use Float64
                        _ => Ok(DataType::Float64),
                    }
                }

                fn output_ordering(
                    &self,
                    input: &[ExprProperties],
                ) -> Result<SortProperties> {
                    $OUTPUT_ORDERING(input)
                }

                fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
                    $EVALUATE_BOUNDS(inputs)
                }

                fn invoke_with_args(
                    &self,
                    args: ScalarFunctionArgs,
                ) -> Result<ColumnarValue> {
                    let args = ColumnarValue::values_to_arrays(&args.args)?;
                    let arr: ArrayRef = match args[0].data_type() {
                        DataType::Float64 => Arc::new(
                            args[0]
                                .as_primitive::<Float64Type>()
                                .unary::<_, Float64Type>(|x: f64| f64::$UNARY_FUNC(x)),
                        ) as ArrayRef,
                        DataType::Float32 => Arc::new(
                            args[0]
                                .as_primitive::<Float32Type>()
                                .unary::<_, Float32Type>(|x: f32| f32::$UNARY_FUNC(x)),
                        ) as ArrayRef,
                        other => {
                            return exec_err!(
                                "Unsupported data type {other:?} for function {}",
                                self.name()
                            )
                        }
                    };

                    Ok(ColumnarValue::Array(arr))
                }

                fn documentation(&self) -> Option<&Documentation> {
                    Some($GET_DOC())
                }
            }
        }
    };
}

/// Macro to create a binary math UDF.
///
/// A binary math function takes two arguments of types Float32 or Float64,
/// applies a binary floating function to the argument, and returns a value of the same type.
///
/// $UDF: the name of the UDF struct that implements `ScalarUDFImpl`
/// $NAME: the name of the function
/// $BINARY_FUNC: the binary function to apply to the argument
/// $OUTPUT_ORDERING: the output ordering calculation method of the function
/// $GET_DOC: the function to get the documentation of the UDF
macro_rules! make_math_binary_udf {
    ($UDF:ident, $NAME:ident, $BINARY_FUNC:ident, $OUTPUT_ORDERING:expr, $GET_DOC:expr) => {
        $crate::make_udf_function!($NAME::$UDF, $NAME);

        mod $NAME {
            use std::any::Any;
            use std::sync::Arc;

            use arrow::array::{ArrayRef, AsArray};
            use arrow::datatypes::{DataType, Float32Type, Float64Type};
            use datafusion_common::{exec_err, Result};
            use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
            use datafusion_expr::TypeSignature;
            use datafusion_expr::{
                ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl,
                Signature, Volatility,
            };

            #[derive(Debug, PartialEq, Eq, Hash)]
            pub struct $UDF {
                signature: Signature,
            }

            impl $UDF {
                pub fn new() -> Self {
                    use DataType::*;
                    Self {
                        signature: Signature::one_of(
                            vec![
                                TypeSignature::Exact(vec![Float32, Float32]),
                                TypeSignature::Exact(vec![Float64, Float64]),
                            ],
                            Volatility::Immutable,
                        ),
                    }
                }
            }

            impl ScalarUDFImpl for $UDF {
                fn as_any(&self) -> &dyn Any {
                    self
                }
                fn name(&self) -> &str {
                    stringify!($NAME)
                }

                fn signature(&self) -> &Signature {
                    &self.signature
                }

                fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                    let arg_type = &arg_types[0];

                    match arg_type {
                        DataType::Float32 => Ok(DataType::Float32),
                        // For other types (possible values float64/null/int), use Float64
                        _ => Ok(DataType::Float64),
                    }
                }

                fn output_ordering(
                    &self,
                    input: &[ExprProperties],
                ) -> Result<SortProperties> {
                    $OUTPUT_ORDERING(input)
                }

                fn invoke_with_args(
                    &self,
                    args: ScalarFunctionArgs,
                ) -> Result<ColumnarValue> {
                    let args = ColumnarValue::values_to_arrays(&args.args)?;
                    let arr: ArrayRef = match args[0].data_type() {
                        DataType::Float64 => {
                            let y = args[0].as_primitive::<Float64Type>();
                            let x = args[1].as_primitive::<Float64Type>();
                            let result = arrow::compute::binary::<_, _, _, Float64Type>(
                                y,
                                x,
                                |y, x| f64::$BINARY_FUNC(y, x),
                            )?;
                            Arc::new(result) as _
                        }
                        DataType::Float32 => {
                            let y = args[0].as_primitive::<Float32Type>();
                            let x = args[1].as_primitive::<Float32Type>();
                            let result = arrow::compute::binary::<_, _, _, Float32Type>(
                                y,
                                x,
                                |y, x| f32::$BINARY_FUNC(y, x),
                            )?;
                            Arc::new(result) as _
                        }
                        other => {
                            return exec_err!(
                                "Unsupported data type {other:?} for function {}",
                                self.name()
                            )
                        }
                    };

                    Ok(ColumnarValue::Array(arr))
                }

                fn documentation(&self) -> Option<&Documentation> {
                    Some($GET_DOC())
                }
            }
        }
    };
}
