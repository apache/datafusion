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

use std::sync::Arc;

use datafusion_common::{config::ConfigOptions, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    execution_plan::{ExecutionPlanFilterPushdownResult, FilterPushdownSupport}, with_new_children_if_necessary, ExecutionPlan
};

use crate::PhysicalOptimizerRule;

fn pushdown_filters(
    node: &Arc<dyn ExecutionPlan>,
    parent_filters: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<ExecutionPlanFilterPushdownResult>> {
    let node_filters = node.filters_for_pushdown()?;
    let children = node.children();
    let mut new_children = Vec::with_capacity(children.len());
    let all_filters = parent_filters
        .iter()
        .chain(node_filters.iter())
        .cloned()
        .collect::<Vec<_>>();
    let mut filter_pushdown_result = if children.is_empty() {
        vec![FilterPushdownSupport::Inexact; all_filters.len()]
    } else {
        vec![FilterPushdownSupport::Exact; all_filters.len()]
    };
    for child in children {
        if child.supports_filter_pushdown() {
            if let Some(result) = pushdown_filters(child, &all_filters)? {
                new_children.push(result.inner);
                for (all_filters_idx, support) in result.support.iter().enumerate() {
                    if !matches!(support, FilterPushdownSupport::Exact) {
                        filter_pushdown_result[all_filters_idx] =
                            FilterPushdownSupport::Inexact;
                    }
                }
            } else {
                new_children.push(Arc::clone(child));
                // If the child does not support filter pushdown, mark all filters as inexact
                for support in filter_pushdown_result.iter_mut() {
                    *support = FilterPushdownSupport::Inexact;
                }
            }
        } else {
            // Reset the filters we are pushing down.
            if let Some(result) = pushdown_filters(child, &Vec::new())? {
                new_children.push(result.inner);
            } else {
                new_children.push(Arc::clone(child));
            }
        };
    }

    let mut result_node = with_new_children_if_necessary(Arc::clone(node), new_children)?;

    // Now update the node with the result of the pushdown of it's filters
    let pushdown_result = filter_pushdown_result[parent_filters.len()..].to_vec();
    if let Some(new_node) =
        Arc::clone(node).with_filter_pushdown_result(&pushdown_result)?
    {
        result_node = new_node;
    };

    // And check if it can absorb the remaining filters
    let remaining_filter_indexes = (0..parent_filters.len())
        .filter(|&i| !matches!(filter_pushdown_result[i], FilterPushdownSupport::Exact))
        .collect::<Vec<_>>();
    println!("Remaining filter indexes: {:?}", remaining_filter_indexes);
    if !remaining_filter_indexes.is_empty() {
        let remaining_filters = remaining_filter_indexes
            .iter()
            .map(|&i| &parent_filters[i])
            .collect::<Vec<_>>();
        let remaining_filters_dbg = format!(
            "Remaining filters being pushed down into {:?} {:?}",
            remaining_filters, node
        );
        println!("{}", remaining_filters_dbg);
        if let Some(result) = node.push_down_filters_from_parents(&remaining_filters)? {
            result_node = result.inner;
            for (parent_filter_index, support) in
                remaining_filter_indexes.iter().zip(result.support)
            {
                // If any of the remaining filters are not exact, mark them as inexact
                if !matches!(support, FilterPushdownSupport::Exact) {
                    filter_pushdown_result[*parent_filter_index] =
                        FilterPushdownSupport::Inexact;
                }
            }
        }
    }
    Ok(Some(ExecutionPlanFilterPushdownResult::new(
        result_node,
        filter_pushdown_result[..parent_filters.len()].to_vec(), // only return the support for the original parent filters
    )))
}

#[derive(Debug)]
pub struct FilterPushdown {}

impl Default for FilterPushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterPushdown {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for FilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(result) = pushdown_filters(&plan, &[])? {
            Ok(result.inner)
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "FilterPushdown"
    }

    fn schema_check(&self) -> bool {
        true // Filter pushdown does not change the schema of the plan
    }
}
