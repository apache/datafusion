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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make cheap clones clear: https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! Spark Expression packages for [DataFusion].
//!
//! This crate contains a collection of various Spark function packages for DataFusion,
//! implemented using the extension API.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//!
//!
//! # Available Function Packages
//! See the list of [modules](#modules) in this crate for available packages.
//!
//! # Example: using all function packages
//!
//! You can register all the functions in all packages using the [`register_all`]
//! function as shown below. Any existing functions will be overwritten, with these
//! Spark functions taking priority.
//!
//! ```
//! # use datafusion_execution::FunctionRegistry;
//! # use datafusion_expr::{ScalarUDF, AggregateUDF, WindowUDF};
//! # use datafusion_expr::planner::ExprPlanner;
//! # use datafusion_common::Result;
//! # use std::collections::HashSet;
//! # use std::sync::Arc;
//! # // Note: We can't use a real SessionContext here because the
//! # // `datafusion_spark` crate has no dependence on the DataFusion crate
//! # // thus use a dummy SessionContext that has enough of the implementation
//! # struct SessionContext {}
//! # impl FunctionRegistry for SessionContext {
//! #    fn register_udf(&mut self, _udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> { Ok (None) }
//! #    fn udfs(&self) -> HashSet<String> { unimplemented!() }
//! #    fn udafs(&self) -> HashSet<String> { unimplemented!() }
//! #    fn udwfs(&self) -> HashSet<String> { unimplemented!() }
//! #    fn udf(&self, _name: &str) -> Result<Arc<ScalarUDF>> { unimplemented!() }
//! #    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {unimplemented!() }
//! #    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> { unimplemented!() }
//! #    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> { unimplemented!() }
//! # }
//! # impl SessionContext {
//! #   fn new() -> Self { SessionContext {} }
//! #   async fn sql(&mut self, _query: &str) -> Result<()> { Ok(()) }
//! #  }
//! #
//! # async fn stub() -> Result<()> {
//! // Create a new session context
//! let mut ctx = SessionContext::new();
//! // Register all Spark functions with the context
//! datafusion_spark::register_all(&mut ctx)?;
//! // Run a query using the `sha2` function which is now available and has Spark semantics
//! let df = ctx.sql("SELECT sha2('The input String', 256)").await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Example: calling a specific function in Rust
//!
//! Each package also exports an `expr_fn` submodule that create [`Expr`]s for
//! invoking functions via rust using a fluent style. For example, to invoke the
//! `sha2` function, you can use the following code:
//!
//! ```rust
//! # use datafusion_expr::{col, lit};
//! use datafusion_spark::expr_fn::sha2;
//! // Create the expression `sha2(my_data, 256)`
//! let expr = sha2(col("my_data"), lit(256));
//! ```
//!
//![`Expr`]: datafusion_expr::Expr

pub mod function;

use datafusion_catalog::TableFunction;
use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use log::debug;
use std::sync::Arc;

/// Fluent-style API for creating `Expr`s
#[allow(unused)]
pub mod expr_fn {
    pub use super::function::aggregate::expr_fn::*;
    pub use super::function::array::expr_fn::*;
    pub use super::function::bitmap::expr_fn::*;
    pub use super::function::bitwise::expr_fn::*;
    pub use super::function::collection::expr_fn::*;
    pub use super::function::conditional::expr_fn::*;
    pub use super::function::conversion::expr_fn::*;
    pub use super::function::csv::expr_fn::*;
    pub use super::function::datetime::expr_fn::*;
    pub use super::function::generator::expr_fn::*;
    pub use super::function::hash::expr_fn::*;
    pub use super::function::json::expr_fn::*;
    pub use super::function::lambda::expr_fn::*;
    pub use super::function::map::expr_fn::*;
    pub use super::function::math::expr_fn::*;
    pub use super::function::misc::expr_fn::*;
    pub use super::function::predicate::expr_fn::*;
    pub use super::function::r#struct::expr_fn::*;
    pub use super::function::string::expr_fn::*;
    pub use super::function::table::expr_fn::*;
    pub use super::function::url::expr_fn::*;
    pub use super::function::window::expr_fn::*;
    pub use super::function::xml::expr_fn::*;
}

/// Returns all default scalar functions
pub fn all_default_scalar_functions() -> Vec<Arc<ScalarUDF>> {
    function::array::functions()
        .into_iter()
        .chain(function::bitmap::functions())
        .chain(function::bitwise::functions())
        .chain(function::collection::functions())
        .chain(function::conditional::functions())
        .chain(function::conversion::functions())
        .chain(function::csv::functions())
        .chain(function::datetime::functions())
        .chain(function::generator::functions())
        .chain(function::hash::functions())
        .chain(function::json::functions())
        .chain(function::lambda::functions())
        .chain(function::map::functions())
        .chain(function::math::functions())
        .chain(function::misc::functions())
        .chain(function::predicate::functions())
        .chain(function::string::functions())
        .chain(function::r#struct::functions())
        .chain(function::url::functions())
        .chain(function::xml::functions())
        .collect::<Vec<_>>()
}

/// Returns all default aggregate functions
pub fn all_default_aggregate_functions() -> Vec<Arc<AggregateUDF>> {
    function::aggregate::functions()
}

/// Returns all default window functions
pub fn all_default_window_functions() -> Vec<Arc<WindowUDF>> {
    function::window::functions()
}

/// Returns all default table functions
pub fn all_default_table_functions() -> Vec<Arc<TableFunction>> {
    function::table::functions()
}

/// Registers all enabled packages with a [`FunctionRegistry`], overriding any existing
/// functions if there is a name clash.
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let scalar_functions: Vec<Arc<ScalarUDF>> = all_default_scalar_functions();
    scalar_functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;

    let aggregate_functions: Vec<Arc<AggregateUDF>> = all_default_aggregate_functions();
    aggregate_functions.into_iter().try_for_each(|udf| {
        let existing_udaf = registry.register_udaf(udf)?;
        if let Some(existing_udaf) = existing_udaf {
            debug!("Overwrite existing UDAF: {}", existing_udaf.name());
        }
        Ok(()) as Result<()>
    })?;

    let window_functions: Vec<Arc<WindowUDF>> = all_default_window_functions();
    window_functions.into_iter().try_for_each(|udf| {
        let existing_udwf = registry.register_udwf(udf)?;
        if let Some(existing_udwf) = existing_udwf {
            debug!("Overwrite existing UDWF: {}", existing_udwf.name());
        }
        Ok(()) as Result<()>
    })?;

    Ok(())
}
