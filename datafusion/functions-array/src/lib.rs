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

//! Array Functions for [DataFusion].
//!
//! This crate contains a collection of array functions implemented using the
//! extension API.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//!
//! You can register the functions in this crate using the [`register_all`] function.
//!

#[macro_use]
pub mod macros;

mod kernels;
mod udf;

use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;
use std::sync::Arc;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::udf::array_dims;
    pub use super::udf::array_ndims;
    pub use super::udf::array_to_string;
    pub use super::udf::cardinality;
    pub use super::udf::gen_series;
    pub use super::udf::range;
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        udf::array_to_string_udf(),
        udf::range_udf(),
        udf::gen_series_udf(),
        udf::array_dims_udf(),
        udf::cardinality_udf(),
        udf::array_ndims_udf(),
    ];
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    Ok(())
}
