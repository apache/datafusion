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

//! Rule to push hash partitioning down and replace round-robin partitioning

use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan, Partitioning};
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use std::sync::Arc;

/// This rule attempts to push hash repartitions (usually introduced to facilitate hash partitioned
/// joins) down to the table scan and replace any round-robin partitioning that exists. It is
/// wasteful to perform round-robin partition first only to repartition later for the join. It is
/// more efficient to just partition by the join key(s) right away.
#[derive(Default)]
pub struct AvoidRepartition {}

impl PhysicalOptimizerRule for AvoidRepartition {
    fn name(&self) -> &str {
        "avoid_repartition"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        remove_redundant_repartitioning(plan, None)
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn remove_redundant_repartitioning(
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(repart) = plan.as_any().downcast_ref::<RepartitionExec>() {
        match repart.partitioning() {
            Partitioning::RoundRobinBatch(_) => {
                if let Some(p) = partitioning {
                    // drop the round-robin repartition and replace with the hash partitioning
                    Ok(Arc::new(RepartitionExec::try_new(
                        plan.children()[0].clone(),
                        p,
                    )?))
                } else {
                    Ok(plan.clone())
                }
            }
            Partitioning::Hash(_, _) => {
                let p = repart.partitioning().clone();
                let new_plan = optimize_children(plan, Some(p.clone()))?;
                if new_plan.children()[0].output_partitioning() == p {
                    // drop this hash partitioning if we pushed it down
                    Ok(new_plan.children()[0].clone())
                } else {
                    Ok(new_plan)
                }
            }
            _ => optimize_children(plan, partitioning),
        }
    } else {
        optimize_children(plan, partitioning)
    }
}

fn optimize_children(
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let inputs = plan.children();
    if inputs.is_empty() {
        return Ok(plan.clone());
    }
    let mut new_inputs = vec![];
    for input in &inputs {
        let new_input =
            remove_redundant_repartitioning(input.clone(), partitioning.clone())?;
        new_inputs.push(new_input);
    }
    with_new_children_if_necessary(plan, new_inputs)
}
