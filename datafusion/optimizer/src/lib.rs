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

//! This module contains a query optimizer that operates against a logical plan and applies
//! some simple rules to a logical plan, such as "Projection Push Down" and "Type Coercion".

#![allow(clippy::module_inception)]
pub mod common_subexpr_eliminate;
pub mod eliminate_filter;
pub mod eliminate_limit;
mod execution_props;
pub mod filter_push_down;
pub mod limit_push_down;
pub mod optimizer;
pub mod projection_push_down;
pub mod single_distinct_to_groupby;
pub mod subquery_filter_to_join;
#[cfg(test)]
pub mod test;
pub mod utils;
mod var_provider;

pub use common_subexpr_eliminate::CommonSubexprEliminate;
pub use eliminate_filter::EliminateFilter;
pub use eliminate_limit::EliminateLimit;
pub use execution_props::ExecutionProps;
pub use filter_push_down::FilterPushDown;
pub use limit_push_down::LimitPushDown;
pub use optimizer::OptimizerRule;
pub use projection_push_down::ProjectionPushDown;
pub use single_distinct_to_groupby::SingleDistinctToGroupBy;
pub use subquery_filter_to_join::SubqueryFilterToJoin;
pub use var_provider::{VarProvider, VarType};
