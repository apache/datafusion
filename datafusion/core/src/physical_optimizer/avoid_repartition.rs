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
pub struct AvoidRepartition {}

impl Default for AvoidRepartition {
    fn default() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for AvoidRepartition {
    fn name(&self) -> &str {
        "avoid_repartition"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (new_plan, _) = remove_redundant_repartitioning(plan, None)?;
        Ok(new_plan)
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn remove_redundant_repartitioning(
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    if let Some(x) = plan.as_any().downcast_ref::<RepartitionExec>() {
        match x.partitioning() {
            Partitioning::RoundRobinBatch(_) => {
                if let Some(p) = partitioning {
                    // drop the round-robin repartition and replace with the hash partitioning
                    let x =
                        RepartitionExec::try_new(plan.children()[0].clone(), p.clone())?;
                    Ok((Arc::new(x), true))
                } else {
                    Ok((plan.clone(), false))
                }
            }
            Partitioning::Hash(_, _) => {
                let p = x.partitioning().clone();
                let (new_plan, pushed_down) = optimize_children(plan, Some(p))?;
                if pushed_down {
                    // drop this hash partitioning if we pushed it down
                    Ok((new_plan.children()[0].clone(), false))
                } else {
                    Ok((new_plan, false))
                }
            }
            _ => optimize_children(plan, partitioning),
        }
    } else {
        optimize_children(plan, partitioning)
    }
}

pub fn optimize_children(
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    let inputs = plan.children();
    let mut new_inputs = vec![];
    let mut pushed_downs = vec![];
    for input in &inputs {
        let (new_input, pushed_down) =
            remove_redundant_repartitioning(input.clone(), partitioning.clone())?;
        new_inputs.push(new_input);
        pushed_downs.push(pushed_down);
    }
    let pushed_down = if pushed_downs.len() == 1 {
        pushed_downs[0]
    } else {
        false
    };
    Ok((
        with_new_children_if_necessary(plan, new_inputs)?,
        pushed_down,
    ))
}
