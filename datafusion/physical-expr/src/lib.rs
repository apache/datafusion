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

// Make cheap clones clear: https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]

// Backward compatibility
pub mod aggregate {
    pub(crate) mod groups_accumulator {
        #[allow(unused_imports)]
        pub(crate) mod accumulate {
            pub use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::NullState;
        }
        pub use datafusion_functions_aggregate_common::aggregate::groups_accumulator::{
            accumulate::NullState, GroupsAccumulatorAdapter,
        };
    }
    pub(crate) mod stats {
        pub use datafusion_functions_aggregate_common::stats::StatsType;
    }
    pub mod utils {
        pub use datafusion_functions_aggregate_common::utils::{
            adjust_output_array, down_cast_any_ref, get_accum_scalar_values_as_arrays,
            get_sort_options, ordering_fields, DecimalAverager, Hashable,
        };
    }
    pub use datafusion_functions_aggregate_common::aggregate::AggregateExpr;
}
pub mod analysis;
pub mod binary_map {
    pub use datafusion_physical_expr_common::binary_map::{ArrowBytesSet, OutputType};
}
pub mod equivalence;
pub mod expressions;
pub mod functions;
pub mod intervals;
pub mod math_expressions;
mod partitioning;
mod physical_expr;
pub mod planner;
mod scalar_function;
pub mod udf {
    pub use crate::scalar_function::create_physical_expr;
}
pub mod utils;
pub mod window;

// backwards compatibility
pub mod execution_props {
    pub use datafusion_expr::execution_props::ExecutionProps;
    pub use datafusion_expr::var_provider::{VarProvider, VarType};
}

pub use aggregate::groups_accumulator::{GroupsAccumulatorAdapter, NullState};
pub use analysis::{analyze, AnalysisContext, ExprBoundaries};
pub use datafusion_functions_aggregate_common::aggregate::{
    AggregateExpr, AggregatePhysicalExpressions,
};
pub use equivalence::{calculate_union, ConstExpr, EquivalenceProperties};
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
pub mod tree_node {
    pub use datafusion_physical_expr_common::tree_node::ExprContext;
}
