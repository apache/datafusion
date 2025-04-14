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

use crate::PhysicalOptimizerRule;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{config::ConfigOptions, DataFusionError, Result};
use datafusion_physical_expr::conjunction;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::filter_pushdown::{
    FilterDescription, FilterPushdownSupport,
};
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::ExecutionPlan;

/// A physical optimizer rule that pushes down filters in the execution plan.
/// See [`ExecutionPlan::try_pushdown_filters`] for a detailed description of the algorithm.
#[derive(Debug)]
pub struct PushdownFilter {}

impl Default for PushdownFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl PushdownFilter {
    pub fn new() -> Self {
        Self {}
    }
}

pub type FilterDescriptionContext = PlanContext<FilterDescription>;

impl PhysicalOptimizerRule for PushdownFilter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let context = FilterDescriptionContext::new_default(plan);

        context
            .transform_down(|mut node| {
                let (mut child_filters, remaining_filters, plan) =
                    match node.plan.try_pushdown_filters(
                        FilterDescription {
                            filters: node.data.take_filters(),
                        },
                        config,
                    )? {
                        FilterPushdownSupport::Supported {
                            child_filters,
                            remaining_filters,
                            op: plan,
                        } => (child_filters, remaining_filters, plan),
                        FilterPushdownSupport::NotSupported(fd) => {
                            (vec![], fd, Arc::clone(&node.plan))
                        }
                    };

                if remaining_filters.filters.is_empty() {
                    node = FilterDescriptionContext::new_default(plan);
                    for (child, filter) in node.children.iter_mut().zip(child_filters) {
                        child.data = filter;
                    }
                } else {
                    let mut new_child_node = FilterDescriptionContext::new_default(plan);
                    new_child_node.data = child_filters.swap_remove(0);
                    node.plan = Arc::new(FilterExec::try_new(
                        conjunction(remaining_filters.filters),
                        Arc::clone(&new_child_node.plan),
                    )?);
                    node.children = vec![new_child_node];
                    node.data = FilterDescription::default();
                }
                Ok(Transformed::yes(node))
            })
            .map(|updated| updated.data.plan)
    }

    fn name(&self) -> &str {
        "PushdownFilter"
    }

    fn schema_check(&self) -> bool {
        true // Filter pushdown does not change the schema of the plan
    }
}
