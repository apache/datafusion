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

//! Function packages for DataFusion.
//!
//! This crate contains a collection of various function packages for DataFusion,
//! implemented using the extension API. Users may wish to control which functions
//! are available to control the binary size of their application as well as
//! use dialect specific implementations of functions (e.g. Spark vs Postgres)
//!
//! Each package is a implemented as a separate
//! module, activated by a feature flag.
//!
//! # Available Packages
//! See the list of [modules](#modules) in this crate for available packages.
//!
//! # Using A Package
//! You can register all functions in all packages using the [`register_all`] function.
//!
//! To access and use only the functions in a certain package, use the
//! `functions()` method in each module.
//!
//! ```
//! # fn main() -> datafusion_common::Result<()> {
//! # let mut registry = datafusion_execution::registry::MemoryFunctionRegistry::new();
//! # use datafusion_execution::FunctionRegistry;
//! // get the encoding functions
//! use datafusion_functions::encoding;
//! for udf in encoding::functions() {
//!   registry.register_udf(udf)?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Each package also exports an `expr_fn` submodule to help create [`Expr`]s that invoke
//! functions using a fluent style. For example:
//!
//! ```
//! // create an Expr that will invoke the encode function
//! use datafusion_expr::{col, lit};
//! use datafusion_functions::expr_fn;
//! // Equivalent to "encode(my_data, 'hex')" in SQL:
//! let expr = expr_fn::encode(vec![col("my_data"), lit("hex")]);
//! ```
//!
//![`Expr`]: datafusion_expr::Expr
//!
//! # Implementing A New Package
//!
//! To add a new package to this crate, you should follow the model of existing
//! packages. The high level steps are:
//!
//! 1. Create a new module with the appropriate [`ScalarUDF`] implementations.
//!
//! 2. Use the macros in [`macros`] to create standard entry points.
//!
//! 3. Add a new feature to `Cargo.toml`, with any optional dependencies
//!
//! 4. Use the `make_package!` macro to expose the module when the
//! feature is enabled.
//!
//! [`ScalarUDF`]: datafusion_expr::ScalarUDF
use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use log::debug;

#[macro_use]
pub mod macros;

make_package!(
    encoding,
    "encoding_expressions",
    "Hex and binary `encode` and `decode` functions"
);

/// Fluent-style API for creating `Expr`s to invoke functions
pub mod expr_fn {
    #[cfg(feature = "encoding_expressions")]
    pub use super::encoding::expr_fn::*;
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    encoding::functions().into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    Ok(())
}
