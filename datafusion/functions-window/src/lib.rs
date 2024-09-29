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

//! Window Function packages for [DataFusion].
//!
//! This crate contains a collection of various window function packages for DataFusion,
//! implemented using the extension API.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//!
use std::sync::Arc;

use log::debug;

use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::WindowUDF;

#[macro_use]
pub mod macros;
pub mod row_number;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::row_number::row_number;
}

/// Returns all default window functions
pub fn all_default_window_functions() -> Vec<Arc<WindowUDF>> {
    vec![row_number::row_number_udwf()]
}
/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(
    registry: &mut dyn FunctionRegistry,
) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<WindowUDF>> = all_default_window_functions();

    functions.into_iter().try_for_each(|fun| {
        let existing_udwf = registry.register_udwf(fun)?;
        if let Some(existing_udwf) = existing_udwf {
            debug!("Overwrite existing UDWF: {}", existing_udwf.name());
        }
        Ok(()) as datafusion_common::Result<()>
    })?;

    Ok(())
}
