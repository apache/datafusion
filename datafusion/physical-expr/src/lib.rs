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

pub mod aggregate;
pub mod analysis;
pub mod binary_map;
pub mod conditional_expressions;
pub mod equivalence;
pub mod expressions;
pub mod functions;
pub mod intervals;
pub mod math_expressions;
mod partitioning;
mod physical_expr;
pub mod planner;
mod scalar_function;
pub mod string_expressions;
pub mod udf;
pub mod utils;
pub mod window;

// backwards compatibility
pub mod execution_props {
    pub use datafusion_expr::execution_props::ExecutionProps;
    pub use datafusion_expr::var_provider::{VarProvider, VarType};
}

pub use aggregate::groups_accumulator::{GroupsAccumulatorAdapter, NullState};
pub use analysis::{analyze, AnalysisContext, ExprBoundaries};
pub use datafusion_physical_expr_common::aggregate::AggregateExpr;
pub use equivalence::EquivalenceProperties;
pub use partitioning::{Distribution, Partitioning};
pub use physical_expr::{
    physical_exprs_bag_equal, physical_exprs_contains, physical_exprs_equal,
    PhysicalExprRef,
};

pub use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
pub use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, LexOrderingRef, LexRequirement, LexRequirementRef, PhysicalSortExpr,
    PhysicalSortRequirement,
};

pub use planner::{create_physical_expr, create_physical_exprs};
pub use scalar_function::ScalarFunctionExpr;

pub use datafusion_physical_expr_common::utils::reverse_order_bys;
pub use utils::split_conjunction;

// For backwards compatibility
pub mod sort_properties {
    pub use datafusion_physical_expr_common::sort_properties::{
        ExprOrdering, SortProperties,
    };
}

// For backwards compatibility
pub mod tree_node {
    pub use datafusion_physical_expr_common::tree_node::ExprContext;
}
