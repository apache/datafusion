// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// Creates external API functions for an array UDF. Specifically, creates
///
/// 1. Single `ScalarUDF` instance
///
/// Creates a singleton `ScalarUDF` of the `$UDF` function named `STATIC_$(UDF)` and a
/// function named `$SCALAR_UDF_FUNC` which returns that function named `STATIC_$(UDF)`.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
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
/// * `UDF`: name of the [`ScalarUDFImpl`]
/// * `EXPR_FN`: name of the expr_fn function to be created
/// * `arg`: 0 or more named arguments for the function
/// * `DOC`: documentation string for the function
/// * `SCALAR_UDF_FUNC`: name of the function to create (just) the `ScalarUDF`
/// * (optional) `$CTOR`: Pass a custom constructor. When omitted it
///   automatically resolves to `$UDF::new()`.
///
/// [`ScalarUDFImpl`]: datafusion_expr::ScalarUDFImpl
macro_rules! make_udf_expr_and_func {
    ($UDF:ident, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $SCALAR_UDF_FN:ident) => {
        make_udf_expr_and_func!($UDF, $EXPR_FN, $($arg)*, $DOC, $SCALAR_UDF_FN, $UDF::new);
    };
    ($UDF:ident, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $SCALAR_UDF_FN:ident, $CTOR:path) => {
            // "fluent expr_fn" style function
            #[doc = $DOC]
            pub fn $EXPR_FN($($arg: datafusion_expr::Expr),*) -> datafusion_expr::Expr {
                datafusion_expr::Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                    $SCALAR_UDF_FN(),
                    vec![$($arg),*],
                ))
            }
            create_func!($UDF, $SCALAR_UDF_FN, $CTOR);
    };
    ($UDF:ident, $EXPR_FN:ident, $DOC:expr, $SCALAR_UDF_FN:ident) => {
        make_udf_expr_and_func!($UDF, $EXPR_FN, $DOC, $SCALAR_UDF_FN, $UDF::new);
    };
    ($UDF:ident, $EXPR_FN:ident, $DOC:expr, $SCALAR_UDF_FN:ident, $CTOR:path) => {
            // "fluent expr_fn" style function
            #[doc = $DOC]
            pub fn $EXPR_FN(arg: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr {
                datafusion_expr::Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                    $SCALAR_UDF_FN(),
                    arg,
                ))
            }
            create_func!($UDF, $SCALAR_UDF_FN, $CTOR);
    };
}

/// Creates a singleton `ScalarUDF` of the `$UDF` function named `STATIC_$(UDF)` and a
/// function named `$SCALAR_UDF_FUNC` which returns that function named `STATIC_$(UDF)`.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
///
/// # Arguments
/// * `UDF`: name of the [`ScalarUDFImpl`]
/// * `SCALAR_UDF_FUNC`: name of the function to create (just) the `ScalarUDF`
/// * (optional) `$CTOR`: Pass a custom constructor. When omitted it
///   automatically resolves to `$UDF::new()`.
///
/// [`ScalarUDFImpl`]: datafusion_expr::ScalarUDFImpl
macro_rules! create_func {
    ($UDF:ident, $SCALAR_UDF_FN:ident) => {
        create_func!($UDF, $SCALAR_UDF_FN, $UDF::new);
    };
    ($UDF:ident, $SCALAR_UDF_FN:ident, $CTOR:path) => {
            #[doc = concat!("ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for ")]
            #[doc = stringify!($UDF)]
            pub fn $SCALAR_UDF_FN() -> std::sync::Arc<datafusion_expr::ScalarUDF> {
                // Singleton instance of [`$UDF`], ensures the UDF is only created once
                static INSTANCE: std::sync::LazyLock<std::sync::Arc<datafusion_expr::ScalarUDF>> =
                    std::sync::LazyLock::new(|| {
                        std::sync::Arc::new(datafusion_expr::ScalarUDF::new_from_impl(
                            $CTOR(),
                        ))
                    });
                std::sync::Arc::clone(&INSTANCE)
            }
    };
}
