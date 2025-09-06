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
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

//! MatchRecognize Function packages for [DataFusion].
//!
//! This crate contains a collection of various match_recognize function packages for DataFusion,
//! implemented using the extension API.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//!

use std::sync::Arc;

use log::debug;

use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};

pub mod aggregate;
pub mod scalar;
pub mod windows;

pub mod planner;
pub mod rules;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::aggregate::{mr_first_agg, mr_last_agg};
    pub use super::scalar::{classifier, match_number, match_sequence_number};
    pub use super::windows::{mr_first, mr_last, mr_next, mr_prev};
}

/// Returns all default scalar functions for MATCH_RECOGNIZE
pub fn all_default_scalar_functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        scalar::mr_symbol(),
        scalar::match_number(),
        scalar::match_sequence_number(),
        scalar::classifier(),
    ]
}

/// Returns all default aggregate functions for MATCH_RECOGNIZE
pub fn all_default_aggregate_functions() -> Vec<Arc<AggregateUDF>> {
    vec![aggregate::mr_first_udaf(), aggregate::mr_last_udaf()]
}

/// Returns all default window functions for MATCH_RECOGNIZE
pub fn all_default_window_functions() -> Vec<Arc<WindowUDF>> {
    vec![
        windows::mr_first_udwf(),
        windows::mr_last_udwf(),
        windows::mr_prev_udwf(),
        windows::mr_next_udwf(),
    ]
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(
    registry: &mut dyn FunctionRegistry,
) -> datafusion_common::Result<()> {
    let scalar_functions: Vec<Arc<ScalarUDF>> = all_default_scalar_functions();
    let aggregate_functions: Vec<Arc<AggregateUDF>> = all_default_aggregate_functions();
    let window_functions: Vec<Arc<WindowUDF>> = all_default_window_functions();

    scalar_functions.into_iter().try_for_each(|fun| {
        let existing_udf = registry.register_udf(fun)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as datafusion_common::Result<()>
    })?;

    aggregate_functions.into_iter().try_for_each(|fun| {
        let existing_udaf = registry.register_udaf(fun)?;
        if let Some(existing_udaf) = existing_udaf {
            debug!("Overwrite existing UDAF: {}", existing_udaf.name());
        }
        Ok(()) as datafusion_common::Result<()>
    })?;

    window_functions.into_iter().try_for_each(|fun| {
        let existing_udwf = registry.register_udwf(fun)?;
        if let Some(existing_udwf) = existing_udwf {
            debug!("Overwrite existing UDWF: {}", existing_udwf.name());
        }
        Ok(()) as datafusion_common::Result<()>
    })?;
    Ok(())
}
