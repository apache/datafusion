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

//! AddCoalescePartitionsExec adds CoalescePartitionsExec to plans
//! with more than one partition, to coalesce them into one partition
//! when the node needs a single partition
use super::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::with_new_children_if_necessary;
use crate::{
    error::Result,
    physical_plan::{coalesce_partitions::CoalescePartitionsExec, Distribution},
};
use std::sync::Arc;

/// Introduces CoalescePartitionsExec
#[derive(Default)]
pub struct AddCoalescePartitionsExec {}

impl AddCoalescePartitionsExec {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for AddCoalescePartitionsExec {
    fn optimize(
        &self,
        plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
        _config: &crate::execution::context::SessionConfig,
    ) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
        if plan.children().is_empty() {
            // leaf node, children cannot be replaced
            Ok(plan.clone())
        } else {
            let children = plan
                .children()
                .iter()
                .map(|child| self.optimize(child.clone(), _config))
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(children.len(), plan.required_input_distribution().len());
            let new_children = children
                .into_iter()
                .zip(plan.required_input_distribution())
                .map(|(child, dist)| match dist {
                    Distribution::SinglePartition
                        if child.output_partitioning().partition_count() > 1 =>
                    {
                        Arc::new(CoalescePartitionsExec::new(child.clone()))
                    }
                    _ => child,
                })
                .collect::<Vec<_>>();
            with_new_children_if_necessary(plan, new_children)
        }
    }

    fn name(&self) -> &str {
        "add_merge_exec"
    }
}
