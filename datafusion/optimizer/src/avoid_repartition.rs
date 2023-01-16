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

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::{LogicalPlan, Partitioning, Repartition};
use std::sync::Arc;

/// This rule attempts to push hash repartitions (usually introduced to faciliate hash partitioned
/// joins) down to the table scan and replace any round-robin partitioning that exists. It is
/// wasteful to perform round-robin partition first only to repartition later for the join. It is
/// more efficient to just partition by the join key(s) right away.
pub struct AvoidRepartition {}

impl Default for AvoidRepartition {
    fn default() -> Self {
        Self {}
    }
}

impl OptimizerRule for AvoidRepartition {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let x = optimize(plan, None)?;
        Ok(x.0)
    }

    fn name(&self) -> &str {
        "avoid_repartition"
    }
}

fn optimize(
    plan: &LogicalPlan,
    partitioning: Option<Partitioning>,
) -> Result<(Option<LogicalPlan>, bool)> {
    match plan {
        LogicalPlan::Repartition(Repartition {
            partitioning_scheme,
            ..
        }) => {
            match partitioning_scheme {
                Partitioning::RoundRobinBatch(_) => {
                    if let Some(p) = partitioning {
                        // drop the round-robin repartition and replace with the hash partitioning
                        let x = LogicalPlan::Repartition(Repartition {
                            input: Arc::new(plan.inputs()[0].clone()),
                            partitioning_scheme: p,
                        });
                        Ok((Some(x), true))
                    } else {
                        Ok((Some(plan.clone()), false))
                    }
                }
                Partitioning::Hash(_, _) => {
                    let (new_plan, pushed_down) =
                        optimize_children(plan, Some(partitioning_scheme.clone()))?;
                    if pushed_down {
                        // drop this hash partitioning if we pushed it down
                        Ok((Some(new_plan.unwrap().inputs()[0].clone()), false))
                    } else {
                        Ok((new_plan, false))
                    }
                }
                Partitioning::DistributeBy(_) => Ok((Some(plan.clone()), false)),
            }
        }
        _ => optimize_children(plan, partitioning),
    }
}

fn optimize_children(
    plan: &LogicalPlan,
    partitioning: Option<Partitioning>,
) -> Result<(Option<LogicalPlan>, bool)> {
    let inputs = plan.inputs();
    if inputs.len() != 1 {
        return Ok((None, false));
    }
    let (input, pushed_down) = optimize(&inputs[0], partitioning.clone())?;
    let new_input = match input {
        Some(plan) => plan,
        None => inputs[0].clone(),
    };
    let new_inputs = vec![new_input];
    Ok((
        Some(plan.with_new_inputs(new_inputs.as_slice())?),
        pushed_down,
    ))
}
