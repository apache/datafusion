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

//! [DataFusion](https://github.com/apache/datafusion)
//! is an extensible query execution framework that uses
//! [Apache Arrow](https://arrow.apache.org) as its in-memory format.
//!
//! This crate is a submodule of DataFusion that provides types representing
//! logical query plans ([LogicalPlan]) and logical expressions ([Expr]) as well as utilities for
//! working with these types.
//!
//! The [expr_fn] module contains functions for creating expressions.

extern crate core;

mod literal;
mod operation;
mod partition_evaluator;
mod table_source;
mod udaf;
mod udf;
mod udwf;

pub mod arguments;
pub mod conditional_expressions;
pub mod execution_props;
pub mod expr;
pub mod expr_fn;
pub mod expr_rewriter;
pub mod expr_schema;
pub mod function;
pub mod select_expr;
pub mod groups_accumulator {
    pub use datafusion_expr_common::groups_accumulator::*;
}
pub mod interval_arithmetic {
    pub use datafusion_expr_common::interval_arithmetic::*;
}
pub mod logical_plan;
pub mod planner;
pub mod registry;
pub mod simplify;
pub mod sort_properties {
    pub use datafusion_expr_common::sort_properties::*;
}
pub mod async_udf;
pub mod statistics {
    pub use datafusion_expr_common::statistics::*;
}
mod predicate_bounds;
pub mod ptr_eq;
pub mod test;
pub mod tree_node;
pub mod type_coercion;
pub mod udf_eq;
pub mod utils;
pub mod var_provider;
pub mod window_frame;
pub mod window_state;

pub use datafusion_doc::{
    aggregate_doc_sections, scalar_doc_sections, window_doc_sections, DocSection,
    Documentation, DocumentationBuilder,
};
pub use datafusion_expr_common::accumulator::Accumulator;
pub use datafusion_expr_common::columnar_value::ColumnarValue;
pub use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};
pub use datafusion_expr_common::operator::Operator;
pub use datafusion_expr_common::signature::{
    ArrayFunctionArgument, ArrayFunctionSignature, Coercion, Signature, TypeSignature,
    TypeSignatureClass, Volatility, TIMEZONE_WILDCARD,
};
pub use datafusion_expr_common::type_coercion::binary;
pub use expr::{
    Between, BinaryExpr, Case, Cast, Expr, GetFieldAccess, GroupingSet, Like,
    Sort as SortExpr, TryCast, WindowFunctionDefinition,
};
pub use expr_fn::*;
pub use expr_schema::ExprSchemable;
pub use function::{
    AccumulatorFactoryFunction, PartitionEvaluatorFactory, ReturnTypeFunction,
    ScalarFunctionImplementation, StateTypeFunction,
};
pub use literal::{
    lit, lit_timestamp_nano, lit_with_metadata, Literal, TimestampLiteral,
};
pub use logical_plan::*;
pub use partition_evaluator::PartitionEvaluator;
#[cfg(feature = "sql")]
pub use sqlparser;
pub use table_source::{TableProviderFilterPushDown, TableSource, TableType};
pub use udaf::{
    udaf_default_display_name, udaf_default_human_display, udaf_default_return_field,
    udaf_default_schema_name, udaf_default_window_function_display_name,
    udaf_default_window_function_schema_name, AggregateUDF, AggregateUDFImpl,
    ReversedUDAF, SetMonotonicity, StatisticsArgs,
};
pub use udf::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl};
pub use udwf::{LimitEffect, ReversedUDWF, WindowUDF, WindowUDFImpl};
pub use window_frame::{WindowFrame, WindowFrameBound, WindowFrameUnits};

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}
