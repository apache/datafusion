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

/// Creates external API functions for an array UDF. Specifically, creates
///
/// 1. Single `HigherOrderUDF` instance
///
/// Creates a singleton `HigherOrderUDF` of the `$UDF` function named `STATIC_$(UDF)` and a
/// function named `$HIGHER_ORDER_UDF_FUNC` which returns that function named `STATIC_$(UDF)`.
///
/// This is used to ensure creating the list of `HigherOrderUDF` only happens once.
///
/// # 2. `expr_fn` style function
///
/// These are functions that create an `Expr` that invokes the UDF, used
/// primarily to programmatically create expressions.
///
/// For example:
/// ```text
/// pub fn array_to_string(delimiter: Expr) -> Expr {
/// ...
/// }
/// ```
/// # Arguments
/// * `UDF`: name of the [`HigherOrderUDF`]
/// * `EXPR_FN`: name of the expr_fn function to be created
/// * `arg`: 0 or more named arguments for the function
/// * `DOC`: documentation string for the function
/// * `HIGHER_ORDER_UDF_FUNC`: name of the function to create (just) the `HigherOrderUDF`
/// * (optional) `$CTOR`: Pass a custom constructor. When omitted it
///   automatically resolves to `$UDF::new()`.
///
/// [`HigherOrderUDF`]: datafusion_expr::HigherOrderUDF
macro_rules! make_higher_order_function_expr_and_func {
    ($UDF:ident, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $HIGHER_ORDER_UDF_FN:ident) => {
        make_higher_order_function_expr_and_func!($UDF, $EXPR_FN, $($arg)*, $DOC, $HIGHER_ORDER_UDF_FN, $UDF::new);
    };
    ($UDF:ident, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $HIGHER_ORDER_UDF_FN:ident, $CTOR:path) => {
        // "fluent expr_fn" style function
        #[doc = $DOC]
        pub fn $EXPR_FN($($arg: datafusion_expr::Expr),*) -> datafusion_expr::Expr {
            datafusion_expr::Expr::HigherOrderFunction(datafusion_expr::expr::HigherOrderFunction::new(
                $HIGHER_ORDER_UDF_FN(),
                vec![$($arg),*],
            ))
        }
        create_higher_order!($UDF, $HIGHER_ORDER_UDF_FN, $CTOR);
    };
    ($UDF:ident, $EXPR_FN:ident, $DOC:expr, $HIGHER_ORDER_UDF_FN:ident) => {
        make_higher_order_function_expr_and_func!($UDF, $EXPR_FN, $DOC, $HIGHER_ORDER_UDF_FN, $UDF::new);
    };
    ($UDF:ident, $EXPR_FN:ident, $DOC:expr, $HIGHER_ORDER_UDF_FN:ident, $CTOR:path) => {
        // "fluent expr_fn" style function
        #[doc = $DOC]
        pub fn $EXPR_FN(arg: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr {
            datafusion_expr::Expr::HigherOrderFunction(datafusion_expr::expr::HigherOrderFunction::new(
                $HIGHER_ORDER_UDF_FN(),
                arg,
            ))
        }
        create_higher_order!($UDF, $HIGHER_ORDER_UDF_FN, $CTOR);
    };
}

/// Creates a singleton `HigherOrderUDF` of the `$UDF` function named `STATIC_$(UDF)` and a
/// function named `$HIGHER_ORDER_UDF_FUNC` which returns that function named `STATIC_$(UDF)`.
///
/// This is used to ensure creating the list of `HigherOrderUDF` only happens once.
///
/// # Arguments
/// * `UDF`: name of the [`HigherOrderUDF`]
/// * `HIGHER_ORDER_UDF_FUNC`: name of the function to create (just) the `HigherOrderUDF`
/// * (optional) `$CTOR`: Pass a custom constructor. When omitted it
///   automatically resolves to `$UDF::new()`.
///
/// [`HigherOrderUDF`]: datafusion_expr::HigherOrderUDF
macro_rules! create_higher_order {
    ($UDF:ident, $HIGHER_ORDER_UDF_FN:ident) => {
        create_higher_order!($UDF, $HIGHER_ORDER_UDF_FN, $UDF::new);
    };
    ($UDF:ident, $HIGHER_ORDER_UDF_FN:ident, $CTOR:path) => {
        #[doc = concat!("HigherOrderFunction that returns a [`HigherOrderUDF`](datafusion_expr::HigherOrderUDF) for ")]
        #[doc = stringify!($UDF)]
        pub fn $HIGHER_ORDER_UDF_FN() -> std::sync::Arc<dyn datafusion_expr::HigherOrderUDF> {
            // Singleton instance of [`$UDF`], ensures the UDF is only created once
            static INSTANCE: std::sync::LazyLock<std::sync::Arc<dyn datafusion_expr::HigherOrderUDF>> =
                std::sync::LazyLock::new(|| {
                    std::sync::Arc::new($CTOR())
                });
            std::sync::Arc::clone(&INSTANCE)
        }
    };
}
