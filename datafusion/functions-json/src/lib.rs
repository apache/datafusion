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
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! JSON Functions for [DataFusion].
//!
//! This crate contains JSON manipulation functions implemented using the
//! extension API. These functions operate on JSON-encoded strings and provide
//! path-based extraction of values.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//!
//! # Available Functions
//!
//! | Function | Description |
//! |----------|-------------|
//! | [`json_get_str`] | Extract a string value from a JSON string at the given path |
//!
//! # Usage with a [`FunctionRegistry`]
//!
//! You can register all functions using the [`register_all`] function.
//!
//! ```
//! # fn main() -> datafusion_common::Result<()> {
//! # let mut registry = datafusion_execution::registry::MemoryFunctionRegistry::new();
//! # use datafusion_execution::FunctionRegistry;
//! use datafusion_functions_json;
//! datafusion_functions_json::register_all(&mut registry)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Usage with a `SessionStateBuilder` (requires `core` feature)
//!
//! When the optional `core` feature is enabled, this crate also exposes the
//! [`SessionStateBuilderJson`] extension trait for ergonomic registration on a
//! [`datafusion::execution::SessionStateBuilder`].
//!
//! ```ignore
//! use datafusion::execution::SessionStateBuilder;
//! use datafusion_functions_json::SessionStateBuilderJson;
//!
//! let state = SessionStateBuilder::new()
//!     .with_default_features()
//!     .with_json_features()
//!     .build();
//! ```

pub mod json_get_str;

#[cfg(feature = "core")]
mod session_state;

#[cfg(feature = "core")]
pub use session_state::SessionStateBuilderJson;

use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;
use std::sync::Arc;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::json_get_str::json_get_str;
}

/// Return all default JSON functions
pub fn all_default_json_functions() -> Vec<Arc<ScalarUDF>> {
    vec![json_get_str::json_get_str_udf()]
}

/// Registers all JSON functions with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions = all_default_json_functions();
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::all_default_json_functions;
    use datafusion_common::Result;
    use std::collections::HashSet;

    #[test]
    fn test_no_duplicate_name() -> Result<()> {
        let mut names = HashSet::new();
        for func in all_default_json_functions() {
            assert!(
                names.insert(func.name().to_string().to_lowercase()),
                "duplicate function name: {}",
                func.name()
            );
            for alias in func.aliases() {
                assert!(
                    names.insert(alias.to_string().to_lowercase()),
                    "duplicate function name: {alias}"
                );
            }
        }
        Ok(())
    }
}
