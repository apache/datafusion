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

pub mod common_subexpr_eliminate;
pub mod decorrelate_where_exists;
pub mod decorrelate_where_in;
pub mod eliminate_filter;
pub mod eliminate_limit;
pub mod expr_simplifier;
pub mod filter_null_join_keys;
pub mod filter_push_down;
pub mod limit_push_down;
pub mod optimizer;
pub mod projection_push_down;
pub mod reduce_cross_join;
pub mod reduce_outer_join;
pub mod scalar_subquery_to_join;
pub mod simplify_expressions;
pub mod single_distinct_to_groupby;
pub mod subquery_filter_to_join;
pub mod type_coercion;
pub mod utils;

pub mod pre_cast_lit_in_comparison;
pub mod rewrite_disjunctive_predicate;
#[cfg(test)]
pub mod test;

pub use optimizer::{OptimizerConfig, OptimizerRule};
