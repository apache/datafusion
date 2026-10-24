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
//! # use datafusion_expr::registry::FunctionRegistry;
//! # use datafusion_expr::{ScalarUDF, AggregateUDF, WindowUDF, HigherOrderUDF};
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
//! #    fn higher_order_function_names(&self) -> HashSet<String> { unimplemented!() }
//! #    fn udafs(&self) -> HashSet<String> { unimplemented!() }
//! #    fn udwfs(&self) -> HashSet<String> { unimplemented!() }
//! #    fn udf(&self, _name: &str) -> Result<Arc<ScalarUDF>> { unimplemented!() }
//! #    fn higher_order_function(&self, name: &str) -> Result<Arc<HigherOrderUDF>> { unimplemented!() }
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
//! # Example: using the Spark expression planner
//!
//! The [`planner::SparkFunctionPlanner`] provides Spark-compatible expression
//! planning, such as mapping SQL `EXTRACT` expressions to Spark's `date_part`
//! function. To use it, register it with your session context:
//!
//! ```ignore
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//! use datafusion_spark::planner::SparkFunctionPlanner;
//!
//! let mut ctx = SessionContext::new();
//! // Register the Spark expression planner
//! ctx.register_expr_planner(Arc::new(SparkFunctionPlanner))?;
//! // Now EXTRACT expressions will use Spark semantics
//! let df = ctx.sql("SELECT EXTRACT(YEAR FROM timestamp_col) FROM my_table").await?;
//! ```
//!
//![`Expr`]: datafusion_expr::Expr
//!
//! # Example: enabling Apache Spark features with SessionStateBuilder
//!
//! The recommended way to enable Apache Spark compatibility is to use the
//! `SessionStateBuilderSpark` extension trait. This registers all
//! Apache Spark functions (scalar, aggregate, window, and table) as well as the Apache Spark
//! expression planner.
//!
//! Enable the `core` feature in your `Cargo.toml`:
//! ```toml
//! datafusion-spark = { version = "X", features = ["core"] }
//! ```
//!
//! Then use the extension trait - see [`SessionStateBuilderSpark::with_spark_features`]
//! for an example.

pub mod function;
pub mod planner;

#[cfg(feature = "core")]
mod session_state;

#[cfg(feature = "core")]
pub use session_state::SessionStateBuilderSpark;

use datafusion_common::Result;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_session::TableFunction;
use log::debug;
use std::sync::Arc;

/// Fluent-style API for creating `Expr`s
#[expect(unused_imports)]
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
    pub use super::function::string::expr_fn::*;
    pub use super::function::r#struct::expr_fn::*;
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

#[cfg(test)]
mod strict_tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;
    use datafusion_expr::type_coercion::functions::fields_with_udf;
    use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};

    fn udf(name: &str) -> Arc<ScalarUDF> {
        all_default_scalar_functions()
            .into_iter()
            .find(|func| func.name() == name)
            .unwrap_or_else(|| panic!("no Spark function named {name}"))
    }

    fn list(values: &[ScalarValue]) -> ScalarValue {
        ScalarValue::List(ScalarValue::new_list_nullable(
            values,
            &values[0].data_type(),
        ))
    }

    #[test]
    fn strict_spark_functions_propagate_nulls() {
        let date = ScalarValue::Date32(Some(19000));
        let url = "http://spark.apache.org/path?query=1";
        let cases: Vec<(&str, Vec<ScalarValue>)> = vec![
            // array
            (
                "slice",
                vec![
                    list(&[
                        ScalarValue::from(1_i64),
                        ScalarValue::from(2_i64),
                        ScalarValue::from(3_i64),
                    ]),
                    ScalarValue::from(2_i64),
                    ScalarValue::from(2_i64),
                ],
            ),
            // bitmap
            ("bitmap_bit_position", vec![ScalarValue::from(1_i64)]),
            ("bitmap_bucket_number", vec![ScalarValue::from(1_i64)]),
            ("bitmap_count", vec![ScalarValue::Binary(Some(vec![1, 2]))]),
            // bitwise
            ("bit_count", vec![ScalarValue::from(7_i64)]),
            (
                "bit_get",
                vec![ScalarValue::from(11_i64), ScalarValue::from(0_i32)],
            ),
            ("bitwise_not", vec![ScalarValue::from(1_i64)]),
            // datetime
            ("date_add", vec![date.clone(), ScalarValue::from(1_i32)]),
            ("date_sub", vec![date.clone(), ScalarValue::from(1_i32)]),
            ("last_day", vec![date.clone()]),
            ("make_dt_interval", vec![ScalarValue::from(1_i32)]),
            (
                "make_dt_interval",
                vec![ScalarValue::from(1_i32), ScalarValue::from(2_i32)],
            ),
            (
                "make_dt_interval",
                vec![
                    ScalarValue::from(1_i32),
                    ScalarValue::from(2_i32),
                    ScalarValue::from(3_i32),
                ],
            ),
            (
                "make_dt_interval",
                vec![
                    ScalarValue::from(1_i32),
                    ScalarValue::from(2_i32),
                    ScalarValue::from(3_i32),
                    ScalarValue::from(4.5_f64),
                ],
            ),
            ("monthname", vec![date.clone()]),
            ("weekday", vec![date]),
            // hash
            ("crc32", vec![ScalarValue::from("Spark")]),
            ("sha1", vec![ScalarValue::from("Spark")]),
            (
                "sha2",
                vec![ScalarValue::from("Spark"), ScalarValue::from(256_i32)],
            ),
            // map
            (
                "map_from_arrays",
                vec![
                    list(&[ScalarValue::from("a"), ScalarValue::from("b")]),
                    list(&[ScalarValue::from(1_i64), ScalarValue::from(2_i64)]),
                ],
            ),
            // math
            ("abs", vec![ScalarValue::from(-1_i64)]),
            (
                "atan2",
                vec![ScalarValue::from(0.5_f64), ScalarValue::from(1.0_f64)],
            ),
            ("bin", vec![ScalarValue::from(13_i64)]),
            ("ceil", vec![ScalarValue::from(1.5_f64)]),
            ("expm1", vec![ScalarValue::from(1.0_f64)]),
            ("factorial", vec![ScalarValue::from(5_i32)]),
            ("floor", vec![ScalarValue::from(1.5_f64)]),
            ("hex", vec![ScalarValue::from(255_i64)]),
            (
                "hypot",
                vec![ScalarValue::from(3.0_f64), ScalarValue::from(4.0_f64)],
            ),
            (
                "mod",
                vec![ScalarValue::from(10_i64), ScalarValue::from(3_i64)],
            ),
            ("negative", vec![ScalarValue::from(1_i64)]),
            (
                "pmod",
                vec![ScalarValue::from(10_i64), ScalarValue::from(3_i64)],
            ),
            (
                "pow",
                vec![ScalarValue::from(2.0_f64), ScalarValue::from(3.0_f64)],
            ),
            ("rint", vec![ScalarValue::from(1.5_f64)]),
            ("unhex", vec![ScalarValue::from("537061726B")]),
            (
                "width_bucket",
                vec![
                    ScalarValue::from(5.3_f64),
                    ScalarValue::from(0.2_f64),
                    ScalarValue::from(10.6_f64),
                    ScalarValue::from(5_i32),
                ],
            ),
            // string
            ("ascii", vec![ScalarValue::from("a")]),
            ("char", vec![ScalarValue::from(65_i64)]),
            (
                "ilike",
                vec![ScalarValue::from("Spark"), ScalarValue::from("_PARK")],
            ),
            ("is_valid_utf8", vec![ScalarValue::from("abc")]),
            ("length", vec![ScalarValue::from("abc")]),
            (
                "like",
                vec![ScalarValue::from("Spark"), ScalarValue::from("_park")],
            ),
            ("luhn_check", vec![ScalarValue::from("79927398713")]),
            ("make_valid_utf8", vec![ScalarValue::from("abc")]),
            ("quote", vec![ScalarValue::from("abc")]),
            ("soundex", vec![ScalarValue::from("Miller")]),
            ("space", vec![ScalarValue::from(3_i32)]),
            (
                "substring",
                vec![ScalarValue::from("Spark SQL"), ScalarValue::from(5_i32)],
            ),
            (
                "substring",
                vec![
                    ScalarValue::from("Spark SQL"),
                    ScalarValue::from(5_i32),
                    ScalarValue::from(1_i32),
                ],
            ),
            // url
            (
                "parse_url",
                vec![ScalarValue::from(url), ScalarValue::from("HOST")],
            ),
            (
                "parse_url",
                vec![
                    ScalarValue::from(url),
                    ScalarValue::from("QUERY"),
                    ScalarValue::from("query"),
                ],
            ),
            ("try_url_decode", vec![ScalarValue::from("a%20b")]),
            ("url_decode", vec![ScalarValue::from("a%20b")]),
            ("url_encode", vec![ScalarValue::from("a b")]),
        ];

        for (name, valid_args) in cases {
            let func = udf(name);
            assert!(func.is_strict(), "{name} should be marked strict");
            let valid_args = coerce(&func, valid_args);

            for null_mask in 0..(1 << valid_args.len()) {
                let mut args = valid_args.clone();
                for (arg_idx, arg) in args.iter_mut().enumerate() {
                    if null_mask & (1 << arg_idx) != 0 {
                        *arg = ScalarValue::try_new_null(&arg.data_type()).unwrap();
                    }
                }

                let result =
                    invoke_with_scalar_args(&func, args).unwrap_or_else(|error| {
                        panic!("{name} failed for NULL mask {null_mask:b}: {error}")
                    });
                let expected_null = null_mask != 0;
                let result = result.into_array(1).unwrap();
                assert_eq!(
                    result.null_count() == result.len(),
                    expected_null,
                    "{name} returned {result:?} for NULL mask {null_mask:0width$b}",
                    width = valid_args.len(),
                );
            }
        }
    }

    /// Casts `args` to the types the function's signature coerces them to
    fn coerce(func: &ScalarUDF, args: Vec<ScalarValue>) -> Vec<ScalarValue> {
        let fields = args
            .iter()
            .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
            .collect::<Vec<_>>();
        let coerced_fields = fields_with_udf(&fields, func).unwrap();
        args.iter()
            .zip(&coerced_fields)
            .map(|(arg, field)| arg.cast_to(field.data_type()).unwrap())
            .collect()
    }

    fn invoke_with_scalar_args(
        func: &ScalarUDF,
        args: Vec<ScalarValue>,
    ) -> Result<ColumnarValue> {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Arc::new(Field::new(
                    format!("arg_{idx}"),
                    arg.data_type(),
                    arg.is_null(),
                ))
            })
            .collect::<Vec<_>>();
        let scalar_arguments = args.iter().map(Some).collect::<Vec<_>>();
        let return_field = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        func.invoke_with_args(ScalarFunctionArgs {
            args: args.into_iter().map(ColumnarValue::Scalar).collect(),
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        })
    }
}
