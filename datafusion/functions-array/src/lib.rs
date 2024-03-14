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

mod array_has;
mod concat;
mod kernels;
mod make_array;
mod rewrite;
mod udf;
mod utils;

use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;
use std::sync::Arc;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::array_has::array_has;
    pub use super::array_has::array_has_all;
    pub use super::array_has::array_has_any;
    pub use super::concat::array_append;
    pub use super::concat::array_concat;
    pub use super::concat::array_prepend;
    pub use super::make_array::make_array;
    pub use super::udf::array_dims;
    pub use super::udf::array_distinct;
    pub use super::udf::array_empty;
    pub use super::udf::array_length;
    pub use super::udf::array_ndims;
    pub use super::udf::array_repeat;
    pub use super::udf::array_resize;
    pub use super::udf::array_sort;
    pub use super::udf::array_to_string;
    pub use super::udf::cardinality;
    pub use super::udf::flatten;
    pub use super::udf::gen_series;
    pub use super::udf::range;
    pub use super::udf::string_to_array;
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        udf::array_to_string_udf(),
        udf::string_to_array_udf(),
        udf::range_udf(),
        udf::gen_series_udf(),
        udf::array_dims_udf(),
        udf::cardinality_udf(),
        udf::array_ndims_udf(),
        concat::array_append_udf(),
        concat::array_prepend_udf(),
        concat::array_concat_udf(),
        make_array::make_array_udf(),
        array_has::array_has_udf(),
        array_has::array_has_all_udf(),
        array_has::array_has_any_udf(),
        udf::array_empty_udf(),
        udf::array_length_udf(),
        udf::flatten_udf(),
        udf::array_sort_udf(),
        udf::array_distinct_udf(),
        udf::array_repeat_udf(),
        udf::array_resize_udf(),
    ];
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    registry.register_function_rewrite(Arc::new(rewrite::ArrayFunctionRewriter {}))?;

    Ok(())
}
