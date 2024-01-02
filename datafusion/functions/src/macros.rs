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
macro_rules! export_functions {
    ($($name:ident),*) => {
        pub mod expr_fn {
            $(
                #[doc = concat!("Call the `", $name, "`function")]
                /// Return $name(arg)
                pub fn $name(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr {
                    super::$name().call(args)
                }
            )*
        }

        /// Return a list of all functions in this package
        pub fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>> {
            vec![
                $(
                    $name(),
                )*
            ]
        }
    };
}

/// Creates a singleton `ScalarUDF` of the `$UDF` function named `$GNAME` and a
/// function named `$NAME` which returns that function named $NAME.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
macro_rules! make_udf_function {
    ($UDF:ty, $GNAME:ident, $NAME:ident) => {
        /// Singleton instance of the function
        static $GNAME: std::sync::OnceLock<std::sync::Arc<datafusion_expr::ScalarUDF>> =
            std::sync::OnceLock::new();

        /// Return a [`ScalarUDF`] for [`$UDF`]
        fn $NAME() -> std::sync::Arc<datafusion_expr::ScalarUDF> {
            $GNAME
                .get_or_init(|| {
                    std::sync::Arc::new(datafusion_expr::ScalarUDF::new_from_impl(
                        <$UDF>::new(),
                    ))
                })
                .clone()
        }
    };
}

/// Macro creates the named module if the feature is enabled
/// otherwise creates a stub
///
/// Which returns:
///
/// 1. The list of actual function implementation when the relevant
/// feature is activated,
///
/// 2. A list of stub function when the feature is not activated that produce
/// a runtime error (and explain what feature flag is needed to activate them).
///
/// The rationale for providing stub functions is to help users to configure datafusion
/// properly (so they get an error telling them why a function is not available)
/// instead of getting a cryptic "no function found" message at runtime.

macro_rules! make_package {
    ($name:ident, $feature:literal, $DOC:expr) => {
        #[cfg(feature = $feature)]
        #[doc = $DOC ]
        #[doc = concat!("Enabled via feature flag `", $feature, "`")]
        pub mod $name;

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
