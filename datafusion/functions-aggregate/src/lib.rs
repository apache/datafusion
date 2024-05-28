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

//! Aggregate Function packages for [DataFusion].
//!
//! This crate contains a collection of various aggregate function packages for DataFusion,
//! implemented using the extension API. Users may wish to control which functions
//! are available to control the binary size of their application as well as
//! use dialect specific implementations of functions (e.g. Spark vs Postgres)
//!
//! Each package is implemented as a separate
//! module, activated by a feature flag.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//!
//! # Available Packages
//! See the list of [modules](#modules) in this crate for available packages.
//!
//! # Using A Package
//! You can register all functions in all packages using the [`register_all`] function.
//!
//! Each package also exports an `expr_fn` submodule to help create [`Expr`]s that invoke
//! functions using a fluent style. For example:
//!
//![`Expr`]: datafusion_expr::Expr
//!
//! # Implementing A New Package
//!
//! To add a new package to this crate, you should follow the model of existing
//! packages. The high level steps are:
//!
//! 1. Create a new module with the appropriate [AggregateUDF] implementations.
//!
//! 2. Use the macros in [`macros`] to create standard entry points.
//!
//! 3. Add a new feature to `Cargo.toml`, with any optional dependencies
//!
//! 4. Use the `make_package!` macro to expose the module when the
//! feature is enabled.

#[macro_use]
pub mod macros;

pub mod covariance;
pub mod first_last;
pub mod median;

use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::AggregateUDF;
use log::debug;
use std::sync::Arc;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::covariance::covar_samp;
    pub use super::first_last::first_value;
    pub use super::median::median;
}

/// Returns all default aggregate functions
pub fn all_default_aggregate_functions() -> Vec<Arc<AggregateUDF>> {
    vec![
        first_last::first_value_udaf(),
        first_last::last_value_udaf(),
        covariance::covar_samp_udaf(),
        covariance::covar_pop_udaf(),
        median::median_udaf(),
    ]
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<AggregateUDF>> = all_default_aggregate_functions();

    functions.into_iter().try_for_each(|udf| {
        let existing_udaf = registry.register_udaf(udf)?;
        if let Some(existing_udaf) = existing_udaf {
            debug!("Overwrite existing UDAF: {}", existing_udaf.name());
        }
        Ok(()) as Result<()>
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::all_default_aggregate_functions;
    use datafusion_common::Result;
    use std::collections::HashSet;

    #[test]
    fn test_no_duplicate_name() -> Result<()> {
        let mut names = HashSet::new();
        for func in all_default_aggregate_functions() {
            assert!(
                names.insert(func.name().to_string()),
                "duplicate function name: {}",
                func.name()
            );
            for alias in func.aliases() {
                assert!(
                    names.insert(alias.to_string()),
                    "duplicate function name: {}",
                    alias
                );
            }
        }
        Ok(())
    }
}
