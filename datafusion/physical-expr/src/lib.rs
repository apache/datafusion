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
// https://github.com/apache/datafusion/issues/18881
#![deny(clippy::allow_attributes)]

// Backward compatibility
pub mod aggregate;
pub mod analysis;
pub mod binary_map {
    pub use datafusion_physical_expr_common::binary_map::{ArrowBytesSet, OutputType};
}
pub mod async_scalar_function;
pub mod equivalence;
pub mod expressions;
pub mod intervals;
mod partitioning;
mod physical_expr;
pub mod planner;
pub mod projection;
mod scalar_function;
pub mod simplifier;
pub mod statistics;
pub mod utils;
pub mod window;

// backwards compatibility
pub mod execution_props {
    pub use datafusion_expr::execution_props::ExecutionProps;
    pub use datafusion_expr::var_provider::{VarProvider, VarType};
}

pub use aggregate::groups_accumulator::{GroupsAccumulatorAdapter, NullState};
pub use analysis::{analyze, AnalysisContext, ExprBoundaries};
pub use equivalence::{
    calculate_union, AcrossPartitions, ConstExpr, EquivalenceProperties,
};
pub use partitioning::{Distribution, Partitioning};
pub use physical_expr::{
    add_offset_to_expr, add_offset_to_physical_sort_exprs, create_lex_ordering,
    create_ordering, create_physical_sort_expr, create_physical_sort_exprs,
    physical_exprs_bag_equal, physical_exprs_contains, physical_exprs_equal,
};

pub use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, PhysicalExprRef};
pub use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, LexRequirement, OrderingRequirements, PhysicalSortExpr,
    PhysicalSortRequirement,
};

pub use planner::{create_physical_expr, create_physical_exprs};
pub use scalar_function::ScalarFunctionExpr;
pub use simplifier::PhysicalExprSimplifier;
pub use utils::{conjunction, conjunction_opt, split_conjunction};

// For backwards compatibility
pub mod tree_node {
    pub use datafusion_physical_expr_common::tree_node::ExprContext;
}
