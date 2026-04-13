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

//! Reorder data source row groups by grouping key statistics.
//!
//! When an `AggregateExec` sits above a data source that supports row group
//! reordering (e.g., Parquet), this rule pushes the grouping key expressions
//! down so that row groups are read in an order that clusters similar group
//! key values together.
//!
//! Benefits:
//! - Reduces the active cardinality of aggregation hash tables (fewer live
//!   entries at any time)
//! - Improves CPU cache locality for hash table lookups during aggregation

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use std::sync::Arc;

/// Pushes grouping key information down to data sources so they can reorder
/// internal data (e.g., Parquet row groups) for better aggregation locality.
///
/// See module-level documentation for details.
#[derive(Debug, Clone, Default)]
pub struct ReorderByGroupKeys;

impl ReorderByGroupKeys {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for ReorderByGroupKeys {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan: Arc<dyn ExecutionPlan>| {
            let Some(agg) = plan.downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(plan));
            };

            let group_by = agg.group_expr();
            if group_by.is_empty() {
                return Ok(Transformed::no(plan));
            }

            let group_exprs = group_by.input_exprs();
            let input = Arc::clone(agg.input());

            match input.try_pushdown_groupby_order(&group_exprs)? {
                Some(new_input) => {
                    let new_plan = plan.with_new_children(vec![new_input])?;
                    Ok(Transformed::yes(new_plan))
                }
                None => Ok(Transformed::no(plan)),
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "ReorderByGroupKeys"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
