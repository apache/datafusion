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
use datafusion_expr::{LogicalPlan, Partitioning, Repartition};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;

pub struct AvoidRepartition {}

impl Default for AvoidRepartition {
    fn default() -> Self {
        Self {}
    }
}

impl OptimizerRule for AvoidRepartition {
    fn try_optimize(&self, plan: &LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Option<LogicalPlan>> {
        optimize(plan, None)
    }

    fn name(&self) -> &str {
        "avoid_repartition"
    }
}

fn optimize(plan: &LogicalPlan, partitioning: Option<Partitioning>) -> Result<Option<LogicalPlan>> {
    match plan {
        LogicalPlan::Repartition(Repartition { partitioning_scheme, ..}) => {
            match partitioning_scheme {
                Partitioning::RoundRobinBatch(_) => {
                    if let Some(p) = partitioning {
                        // drop the round-robin repartition and replace with the hash partitioning
                        Ok(Some(LogicalPlan::Repartition(Repartition {
                            input: Arc::new(plan.inputs()[0].clone()),
                            partitioning_scheme: p
                        })))
                    } else {
                        Ok(Some(plan.clone()))
                    }
                }
                Partitioning::Hash(_, _) => {
                    let new_plan = optimize_children(plan, Some(partitioning_scheme.clone()))?.unwrap();
                    // TODO drop this hash partitioning if we pushed it down
                    Ok(Some(new_plan))
                },
                Partitioning::DistributeBy(_) => Ok(Some(plan.clone()))
            }
        }
        _ => optimize_children(plan, partitioning)
    }
}

fn optimize_children(plan: &LogicalPlan, partitioning: Option<Partitioning>) -> Result<Option<LogicalPlan>> {
    let inputs = plan.inputs();
    let mut new_inputs = vec![];
    for input in &inputs {
        let input = optimize(input, partitioning.clone())?;
        let new_input = match input {
            Some(plan) => plan,
            None => inputs.get(new_inputs.len()).unwrap().clone().clone()
        };
        new_inputs.push(new_input);
    }
    Ok(Some(plan.with_new_inputs(new_inputs.as_slice())?))
}