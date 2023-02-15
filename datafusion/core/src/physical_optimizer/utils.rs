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

//! Collection of utility functions that are leveraged by the query optimizer rules

use super::optimizer::PhysicalOptimizerRule;

use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use datafusion_physical_expr::utils::ordering_satisfy;
use datafusion_physical_expr::PhysicalSortExpr;
use std::sync::Arc;

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
pub fn optimize_children(
    optimizer: &impl PhysicalOptimizerRule,
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .iter()
        .map(|child| optimizer.optimize(Arc::clone(child), config))
        .collect::<Result<Vec<_>>>()?;

    if children.is_empty() {
        Ok(Arc::clone(&plan))
    } else {
        with_new_children_if_necessary(plan, children)
    }
}

/// This utility function adds a `SortExec` above an operator according to the
/// given ordering requirements while preserving the original partitioning.
pub fn add_sort_above(
    node: &mut Arc<dyn ExecutionPlan>,
    sort_expr: Vec<PhysicalSortExpr>,
) -> Result<()> {
    // If the ordering requirement is already satisfied, do not add a sort.
    if !ordering_satisfy(node.output_ordering(), Some(&sort_expr), || {
        node.equivalence_properties()
    }) {
        *node = Arc::new(if node.output_partitioning().partition_count() > 1 {
            SortExec::new_with_partitioning(sort_expr, node.clone(), true, None)
        } else {
            SortExec::try_new(sort_expr, node.clone(), None)?
        }) as _
    }
    Ok(())
}
