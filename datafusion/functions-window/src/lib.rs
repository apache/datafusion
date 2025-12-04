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

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]
// https://github.com/apache/datafusion/issues/18881
#![deny(clippy::allow_attributes)]

//! Window Function packages for [DataFusion].
//!
//! This crate contains a collection of various window function packages for DataFusion,
//! implemented using the extension API.
//!
//! [DataFusion]: https://crates.io/crates/datafusion

use std::sync::Arc;

use log::debug;

use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::WindowUDF;

#[macro_use]
pub mod macros;

pub mod cume_dist;
pub mod lead_lag;
pub mod nth_value;
pub mod ntile;
pub mod rank;
pub mod row_number;

pub mod planner;

mod utils;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::cume_dist::cume_dist;
    pub use super::lead_lag::lag;
    pub use super::lead_lag::lead;
    pub use super::nth_value::{first_value, last_value, nth_value};
    pub use super::ntile::ntile;
    pub use super::rank::{dense_rank, percent_rank, rank};
    pub use super::row_number::row_number;
}

/// Returns all default window functions
pub fn all_default_window_functions() -> Vec<Arc<WindowUDF>> {
    vec![
        cume_dist::cume_dist_udwf(),
        row_number::row_number_udwf(),
        lead_lag::lead_udwf(),
        lead_lag::lag_udwf(),
        rank::rank_udwf(),
        rank::dense_rank_udwf(),
        rank::percent_rank_udwf(),
        ntile::ntile_udwf(),
        nth_value::first_value_udwf(),
        nth_value::last_value_udwf(),
        nth_value::nth_value_udwf(),
    ]
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
