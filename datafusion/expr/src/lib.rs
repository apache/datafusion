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

//! [DataFusion](https://github.com/apache/arrow-datafusion)
//! is an extensible query execution framework that uses
//! [Apache Arrow](https://arrow.apache.org) as its in-memory format.
//!
//! This crate is a submodule of DataFusion that provides types representing
//! logical query plans ([LogicalPlan]) and logical expressions ([Expr]) as well as utilities for
//! working with these types.
//!
//! The [expr_fn] module contains functions for creating expressions.

mod accumulator;
pub mod aggregate_function;
pub mod array_expressions;
mod built_in_function;
mod columnar_value;
pub mod conditional_expressions;
pub mod expr;
pub mod expr_fn;
pub mod expr_rewriter;
pub mod expr_schema;
pub mod expr_visitor;
pub mod field_util;
pub mod function;
mod literal;
pub mod logical_plan;
mod nullif;
mod operator;
mod signature;
pub mod struct_expressions;
mod table_source;
pub mod type_coercion;
mod udaf;
mod udf;
pub mod utils;
pub mod window_frame;
pub mod window_function;

pub use accumulator::{Accumulator, AggregateState};
pub use aggregate_function::AggregateFunction;
pub use built_in_function::BuiltinScalarFunction;
pub use columnar_value::ColumnarValue;
pub use expr::{
    Between, BinaryExpr, Case, Cast, Expr, GetIndexedField, GroupingSet, Like,
};
pub use expr_fn::*;
pub use expr_schema::ExprSchemable;
pub use function::{
    AccumulatorFunctionImplementation, ReturnTypeFunction, ScalarFunctionImplementation,
    StateTypeFunction,
};
pub use literal::{lit, lit_timestamp_nano, Literal, TimestampLiteral};
pub use logical_plan::{
    builder::{
        build_join_schema, union, wrap_projection_for_join_if_necessary, UNNAMED_TABLE,
    },
    Aggregate, CreateCatalog, CreateCatalogSchema, CreateExternalTable,
    CreateMemoryTable, CreateView, CrossJoin, Distinct, DropTable, DropView,
    EmptyRelation, Explain, Extension, Filter, Join, JoinConstraint, JoinType, Limit,
    LogicalPlan, LogicalPlanBuilder, Partitioning, PlanType, PlanVisitor, Projection,
    Repartition, SetVariable, Sort, StringifiedPlan, Subquery, SubqueryAlias, TableScan,
    ToStringifiedPlan, Union, UserDefinedLogicalNode, Values, Window,
};
pub use nullif::SUPPORTED_NULLIF_TYPES;
pub use operator::Operator;
pub use signature::{Signature, TypeSignature, Volatility};
pub use table_source::{TableProviderFilterPushDown, TableSource, TableType};
pub use udaf::AggregateUDF;
pub use udf::ScalarUDF;
pub use window_frame::{WindowFrame, WindowFrameBound, WindowFrameUnits};
pub use window_function::{BuiltInWindowFunction, WindowFunction};
