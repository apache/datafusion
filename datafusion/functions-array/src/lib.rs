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

pub mod array_has;
pub mod cardinality;
pub mod concat;
pub mod dimension;
pub mod empty;
pub mod except;
pub mod extract;
pub mod flatten;
pub mod length;
pub mod make_array;
pub mod position;
pub mod range;
pub mod remove;
pub mod repeat;
pub mod replace;
pub mod resize;
pub mod reverse;
pub mod rewrite;
pub mod set_ops;
pub mod sort;
pub mod string;
pub mod utils;

use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::type_coercion::functions;
use datafusion_expr::ScalarUDF;
use log::debug;
use string::array_to_string;
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::OnceLock;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::array_has::array_has;
    pub use super::array_has::array_has_all;
    pub use super::array_has::array_has_any;
    pub use super::cardinality::cardinality;
    pub use super::concat::array_append;
    pub use super::concat::array_concat;
    pub use super::concat::array_prepend;
    pub use super::dimension::array_dims;
    pub use super::dimension::array_ndims;
    pub use super::empty::array_empty;
    pub use super::except::array_except;
    pub use super::extract::array_element;
    pub use super::extract::array_pop_back;
    pub use super::extract::array_pop_front;
    pub use super::extract::array_slice;
    pub use super::flatten::flatten;
    pub use super::length::array_length;
    pub use super::make_array::make_array;
    pub use super::position::array_position;
    pub use super::position::array_positions;
    pub use super::range::gen_series;
    pub use super::range::range;
    pub use super::remove::array_remove;
    pub use super::remove::array_remove_all;
    pub use super::remove::array_remove_n;
    pub use super::repeat::array_repeat;
    pub use super::replace::array_replace;
    pub use super::replace::array_replace_all;
    pub use super::replace::array_replace_n;
    pub use super::resize::array_resize;
    pub use super::reverse::array_reverse;
    pub use super::set_ops::array_distinct;
    pub use super::set_ops::array_intersect;
    pub use super::set_ops::array_union;
    pub use super::sort::array_sort;
    pub use super::string::array_to_string;
    pub use super::string::string_to_array;
}



pub type ScalarFactory = Box<dyn Fn() -> Arc<ScalarUDF> + Send + Sync>;

/// HashMap Singleton for UDFs
///
/// Replace register_all with our built-in functions
/// Replace  scalar_functions: HashMap<String, Arc<ScalarUDF>> in SessionState
pub fn array_functions() -> &'static Mutex<HashMap<String, ScalarFactory>> {
    static FUNCTIONS: OnceLock<Mutex<HashMap<String, ScalarFactory>>> = OnceLock::new();
    FUNCTIONS.get_or_init(|| {
        let mut functions = HashMap::new();
        functions.insert(
            String::from("array_to_string"),
            Box::new(string::array_to_string_udf) as _,
        );
        functions.insert(
            String::from("string_to_array"),
            Box::new(string::string_to_array_udf) as _,
        );
        functions.insert(String::from("range"), Box::new(range::range_udf) as _);
        functions.insert(
            String::from("gen_series"),
            Box::new(range::gen_series_udf) as _,
        );
        functions.insert(
            String::from("array_dims"),
            Box::new(dimension::array_dims_udf) as _,
        );
        functions.insert(
            String::from("cardinality"),
            Box::new(cardinality::cardinality_udf) as _,
        );
        functions.insert(
            String::from("array_ndims"),
            Box::new(dimension::array_ndims_udf) as _,
        );
        functions.insert(
            String::from("array_append"),
            Box::new(concat::array_append_udf) as _,
        );
        functions.insert(
            String::from("array_prepend"),
            Box::new(concat::array_prepend_udf) as _,
        );
        functions.insert(
            String::from("array_concat"),
            Box::new(concat::array_concat_udf) as _,
        );
        functions.insert(
            String::from("array_except"),
            Box::new(except::array_except_udf) as _,
        );
        functions.insert(
            String::from("array_element"),
            Box::new(extract::array_element_udf) as _,
        );
        functions.insert(
            String::from("array_pop_back"),
            Box::new(extract::array_pop_back_udf) as _,
        );
        functions.insert(
            String::from("array_pop_front"),
            Box::new(extract::array_pop_front_udf) as _,
        );
        functions.insert(
            String::from("array_slice"),
            Box::new(extract::array_slice_udf) as _,
        );
        functions.insert(
            String::from("make_array"),
            Box::new(make_array::make_array_udf) as _,
        );
        functions.insert(
            String::from("array_has"),
            Box::new(array_has::array_has_udf) as _,
        );
        functions.insert(
            String::from("array_has_all"),
            Box::new(array_has::array_has_all_udf) as _,
        );
        functions.insert(
            String::from("array_has_any"),
            Box::new(array_has::array_has_any_udf) as _,
        );
        functions.insert(
            String::from("array_empty"),
            Box::new(empty::array_empty_udf) as _,
        );
        functions.insert(
            String::from("array_length"),
            Box::new(length::array_length_udf) as _,
        );
        functions.insert(
            String::from("flatten"),
            Box::new(flatten::flatten_udf) as _,
        );
        functions.insert(
            String::from("array_sort"),
            Box::new(sort::array_sort_udf) as _,
        );
        functions.insert(
            String::from("array_repeat"),
            Box::new(repeat::array_repeat_udf) as _,
        );
        functions.insert(
            String::from("array_resize"),
            Box::new(resize::array_resize_udf) as _,
        );
        functions.insert(
            String::from("array_reverse"),
            Box::new(reverse::array_reverse_udf) as _,
        );
        functions.insert(
            String::from("array_distinct"),
            Box::new(set_ops::array_distinct_udf) as _,
        );
        functions.insert(
            String::from("array_intersect"),
            Box::new(set_ops::array_intersect_udf) as _,
        );
        functions.insert(
            String::from("array_union"),
            Box::new(set_ops::array_union_udf) as _,
        );
        functions.insert(
            String::from("array_position"),
            Box::new(position::array_position_udf) as _,
        );
        functions.insert(
            String::from("array_positions"),
            Box::new(position::array_positions_udf) as _,
        );
        functions.insert(
            String::from("array_remove"),
            Box::new(remove::array_remove_udf) as _,
        );
        functions.insert(
            String::from("array_remove_all"),
            Box::new(remove::array_remove_all_udf) as _,
        );
        functions.insert(
            String::from("array_remove_n"),
            Box::new(remove::array_remove_n_udf) as _,
        );
        functions.insert(
            String::from("array_replace"),
            Box::new(replace::array_replace_udf) as _,
        );
        functions.insert(
            String::from("array_replace_all"),
            Box::new(replace::array_replace_all_udf) as _,
        );
        functions.insert(
            String::from("array_replace_n"),
            Box::new(replace::array_replace_n_udf) as _,
        );



        // TODO: Add more builtin functions here
        Mutex::new(functions)
    })
}

// Get an UDF by name
//
// Replace with `get_udf`
// fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
//     self.state.scalar_functions().get(name).cloned()
// }
pub fn get_array_udf(name: &str) -> Option<Arc<ScalarUDF>> {
    array_functions().lock().unwrap().get(name).map(|f| f())
}

/// Register a single new UDF, so the user can register their own functions
/// 
/// Repalce old regsiter_udf
pub fn register_array_udf(name: &str, udf: ScalarFactory) -> Option<ScalarFactory> {
    // TODO: Check overwrite?
    array_functions().lock().unwrap().insert(name.to_string(), udf)
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    // let functions: Vec<Arc<ScalarUDF>> = vec![
    //     string::array_to_string_udf(),
    //     string::string_to_array_udf(),
    //     range::range_udf(),
    //     range::gen_series_udf(),
    //     dimension::array_dims_udf(),
    //     cardinality::cardinality_udf(),
    //     dimension::array_ndims_udf(),
    //     concat::array_append_udf(),
    //     concat::array_prepend_udf(),
    //     concat::array_concat_udf(),
    //     except::array_except_udf(),
    //     extract::array_element_udf(),
    //     extract::array_pop_back_udf(),
    //     extract::array_pop_front_udf(),
    //     extract::array_slice_udf(),
    //     make_array::make_array_udf(),
    //     array_has::array_has_udf(),
    //     array_has::array_has_all_udf(),
    //     array_has::array_has_any_udf(),
    //     empty::array_empty_udf(),
    //     length::array_length_udf(),
    //     flatten::flatten_udf(),
    //     sort::array_sort_udf(),
    //     repeat::array_repeat_udf(),
    //     resize::array_resize_udf(),
    //     reverse::array_reverse_udf(),
    //     set_ops::array_distinct_udf(),
    //     set_ops::array_intersect_udf(),
    //     set_ops::array_union_udf(),
    //     position::array_position_udf(),
    //     position::array_positions_udf(),
    //     remove::array_remove_udf(),
    //     remove::array_remove_all_udf(),
    //     remove::array_remove_n_udf(),
    //     replace::array_replace_n_udf(),
    //     replace::array_replace_all_udf(),
    //     replace::array_replace_udf(),
    // ];
    // let functions = vec![
    //     (
    //         String::from("array_remove_all"),
    //         Box::new(remove::array_remove_all_udf),
    //     )
    // ];

    // functions.into_iter().try_for_each(|(name, udf)| {
    //     let existing_udf = registry.register_udf_impl(vec![name.clone()], udf as _)?;
    //     if let Some(existing_udf) = existing_udf {
    //         debug!("Overwrite existing UDF: {}", name);
    //     }
    //     Ok(()) as Result<()>
    // })?;

    registry.register_function_rewrite(Arc::new(rewrite::ArrayFunctionRewriter {}))?;

    Ok(())
}
