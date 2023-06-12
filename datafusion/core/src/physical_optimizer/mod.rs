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

//! Optimizer that rewrites [`ExecutionPlan`]s.
//!
//! These rules take advantage of physical plan properties , such as
//! "Repartition" or "Sortedness"
//!
//! [`ExecutionPlan`]: crate::physical_plan::ExecutionPlan
pub mod aggregate_statistics;
pub mod coalesce_batches;
pub mod combine_partial_final_agg;
pub mod dist_enforcement;
pub mod global_sort_selection;
pub mod join_selection;
pub mod optimizer;
pub mod pipeline_checker;
pub mod pruning;
pub mod repartition;
pub mod sort_enforcement;
mod sort_pushdown;
mod utils;

pub mod pipeline_fixer;
#[cfg(test)]
pub mod test_utils;

pub use optimizer::PhysicalOptimizerRule;
