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

/// macro that exports listed names as individual functions in an `expr_fn` module
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
            use super::*;
            $(
                /// Return $name(arg)
                pub fn $name(args: Vec<Expr>) -> Expr {
                    super::$name().call(args)
                }
            )*
        }

        /// Return a list of all functions in this package
        pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
            vec![
                $(
                    $name(),
                )*
            ]
        }
    };
}

/// Create a singleton instance of the function named $GNAME and return it from the
/// function named $NAME
macro_rules! make_function {
    ($UDF:ty, $GNAME:ident, $NAME:ident) => {
        /// Singleton instance of the function
        static $GNAME: OnceLock<Arc<ScalarUDF>> = OnceLock::new();

        /// Return the function implementation
        fn $NAME() -> Arc<ScalarUDF> {
            $GNAME
                .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(<$UDF>::default())))
                .clone()
        }
    };
}

// Macro creates the named module if the feature is enabled
// otherwise creates a stub
macro_rules! make_package {
    ($name:ident, $feature:literal) => {
        #[cfg(feature = $feature)]
        pub mod $name;

        #[cfg(not(feature = $feature))]
        /// Stub module when feature is not enabled
        mod $name {
            use datafusion_expr::ScalarUDF;
            use log::debug;
            use std::sync::Arc;

            pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
                debug!("{} functions disabled", stringify!($name));
                vec![]
            }
        }
    };
}
