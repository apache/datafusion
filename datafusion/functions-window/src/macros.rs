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

/// Lazily initializes a user-defined window function exactly once
/// when called concurrently. Repeated calls return a reference to the
/// same instance.
///
/// # Parameters
///
/// * `$UDWF`: The struct which defines the [`Signature`](datafusion_expr::Signature)
///     of the user-defined window function.
/// * `$OUT_FN_NAME`: The basename to generate a unique function name like
///     `$OUT_FN_NAME_udwf`.
/// * `$DOC`: Description of user-defined window function.
/// * (optional) `$CTOR`: When none provided it automatically resolves
///     to `$UDWF::default()` (default constructor). To customize
///     pass a different constructor.
///
/// # Example
///
/// ```
/// use std::any::Any;
/// use datafusion_common::arrow::datatypes::{DataType, Field};
/// use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
///
/// use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// use datafusion_functions_window::get_or_init_udwf;
///
/// #[derive(Debug)]
/// struct AddOne {
///     signature: Signature,
/// }
///
/// impl Default for AddOne {
///     fn default() -> Self {
///         Self {
///             signature: Signature::numeric(1, Volatility::Immutable),
///         }
///     }
/// }
///
/// impl WindowUDFImpl for AddOne {
///     fn as_any(&self) -> &dyn Any {
///         self
///     }
///     fn name(&self) -> &str {
///         "add_one"
///     }
///     fn signature(&self) -> &Signature {
///         &self.signature
///     }
///     fn partition_evaluator(
///         &self,
///     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
///         unimplemented!("unnecessary for doc test")
///     }
///     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
///         Ok(Field::new(field_args.name(), DataType::Int64, false))
///     }
/// }
///
/// /// This creates `add_one_udwf()` from `AddOne`.
/// get_or_init_udwf!(AddOne, add_one, "Adds one to each row value in window partition.");
/// ```
#[macro_export]
macro_rules! get_or_init_udwf {
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr) => {
        get_or_init_udwf!($UDWF, $OUT_FN_NAME, $DOC, $UDWF::default);
    };

    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr, $CTOR:path) => {
        paste::paste! {
            #[doc = concat!(" Singleton instance of [`", stringify!($OUT_FN_NAME), "`], ensures the user-defined")]
            #[doc = concat!(" window function is only created once.")]
            #[allow(non_upper_case_globals)]
            static [<STATIC_ $UDWF>]: std::sync::OnceLock<std::sync::Arc<datafusion_expr::WindowUDF>> =
                std::sync::OnceLock::new();

            #[doc = concat!(" Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`", stringify!($OUT_FN_NAME), "`].")]
            #[doc = ""]
            #[doc = concat!(" ", $DOC)]
            pub fn [<$OUT_FN_NAME _udwf>]() -> std::sync::Arc<datafusion_expr::WindowUDF> {
                [<STATIC_ $UDWF>]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion_expr::WindowUDF::from($CTOR()))
                    })
                    .clone()
            }
        }
    };
}

/// Create a [`WindowFunction`] expression that exposes a fluent API
/// which you can use to build more complex expressions and contains
/// additional [`ExprFunctionExt`] methods for configuring user-defined
/// window functions.
///
/// [`WindowFunction`]: datafusion_expr::Expr::WindowFunction
/// [`ExprFunctionExt`]: datafusion_expr::expr_fn::ExprFunctionExt
///
/// # Parameters
///
/// * `$UDWF`: The struct which defines the [`Signature`] of the
///     user-defined window function.
/// * `$OUT_FN_NAME`: The basename to generate a unique function name like
///     `$OUT_FN_NAME_udwf`.
/// * `$DOC`: Description of user-defined window function.
/// * (optional) `[$($PARAM:ident),+]`: An array of 1 or more parameters
///     for the generated function. The type of parameters is [`Expr`].
///     This is unnecessary for functions which take no arguments.
///
/// [`Signature`]: datafusion_expr::Signature
/// [`Expr`]: datafusion_expr::Expr
///
/// # Example
///
/// 1. Function with zero parameters
/// ```
/// use std::any::Any;
/// use datafusion_common::arrow::datatypes::{DataType, Field};
/// use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
/// use datafusion_functions_window::{create_udwf_expr, get_or_init_udwf};
/// use datafusion_functions_window_common::field::WindowUDFFieldArgs;
///
/// #[derive(Debug)]
/// struct RowNumber {
///     signature: Signature,
/// }
///
/// # get_or_init_udwf!(
/// #     RowNumber,
/// #     row_number,
/// #     "Returns a unique row number for each row in window partition beginning at 1."
/// # );
/// // Creates `row_number()` API which has no parameters
/// create_udwf_expr!(
///     RowNumber,
///     row_number,
///     "Returns a unique row number for each row in window partition beginning at 1."
/// );
///
/// # impl Default for RowNumber {
/// #     fn default() -> Self {
/// #         Self {
/// #             signature: Signature::any(0, Volatility::Immutable),
/// #         }
/// #     }
/// # }
///
/// # impl WindowUDFImpl for RowNumber {
/// #     fn as_any(&self) -> &dyn Any {
/// #         self
/// #     }
/// #     fn name(&self) -> &str {
/// #         "row_number"
/// #     }
/// #     fn signature(&self) -> &Signature {
/// #         &self.signature
/// #     }
/// #     fn partition_evaluator(
/// #         &self,
/// #     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
/// #         unimplemented!()
/// #     }
/// #     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
/// #         Ok(Field::new(field_args.name(), DataType::UInt64, false))
/// #     }
/// # }
///
/// ```

#[macro_export]
macro_rules! create_udwf_expr {
    // zero arguments
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr) => {
        paste::paste! {
            #[doc = " Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for"]
            #[doc = concat!(" [`", stringify!($UDWF), "`] user-defined window function.")]
            #[doc = ""]
            #[doc = concat!(" ", $DOC)]
            pub fn $OUT_FN_NAME() -> datafusion_expr::Expr {
                [<$OUT_FN_NAME _udwf>]().call(vec![])
            }
       }
    };

    // 1 or more arguments
    ($UDWF:ident, $OUT_FN_NAME:ident, [$($PARAM:ident),+], $DOC:expr) => {
        paste::paste! {
            #[doc = " Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for"]
            #[doc = concat!(" [`", stringify!($UDWF), "`] user-defined window function.")]
            #[doc = ""]
            #[doc = concat!(" ", $DOC)]
            pub fn $OUT_FN_NAME(
                $($PARAM: datafusion_expr::Expr),+
            ) -> datafusion_expr::Expr {
                [<$OUT_FN_NAME _udwf>]()
                    .call(vec![$($PARAM),+])
            }
       }
    };
}
