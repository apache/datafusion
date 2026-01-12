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

use crate::PhysicalOptimizerRule;
use datafusion_common::JoinSide;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::buffer::BufferExec;
use datafusion_physical_plan::joins::HashJoinExec;
use std::sync::Arc;

/// Looks for all the [HashJoinExec]s in the plan and places a [BufferExec] node with the
/// configured capacity in the probe side:
///
/// ```text
///            ┌───────────────────┐
///            │   HashJoinExec    │
///            └─────▲────────▲────┘
///          ┌───────┘        └─────────┐
///          │                          │
/// ┌────────────────┐         ┌─────────────────┐
/// │   Build side   │       + │   BufferExec    │
/// └────────────────┘         └────────▲────────┘
///                                     │
///                            ┌────────┴────────┐
///                            │   Probe side    │
///                            └─────────────────┘
/// ```
///
/// Which allows eagerly pulling it even before the build side has completely finished.
#[derive(Debug, Default)]
pub struct HashJoinBuffering {}

impl HashJoinBuffering {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for HashJoinBuffering {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let capacity = config.execution.hash_join_buffering_capacity;
        if capacity == 0 {
            return Ok(plan);
        }

        plan.transform_down(|plan| {
            let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };
            let plan = Arc::clone(&plan);
            Ok(Transformed::yes(
                if HashJoinExec::probe_side() == JoinSide::Left {
                    // Do not stack BufferExec nodes together.
                    if node.left.as_any().downcast_ref::<BufferExec>().is_some() {
                        return Ok(Transformed::no(plan));
                    }
                    plan.with_new_children(vec![
                        Arc::new(BufferExec::new(Arc::clone(&node.left), capacity)),
                        Arc::clone(&node.right),
                    ])?
                } else {
                    // Do not stack BufferExec nodes together.
                    if node.right.as_any().downcast_ref::<BufferExec>().is_some() {
                        return Ok(Transformed::no(plan));
                    }
                    plan.with_new_children(vec![
                        Arc::clone(&node.left),
                        Arc::new(BufferExec::new(Arc::clone(&node.right), capacity)),
                    ])?
                },
            ))
        })
        .data()
    }

    fn name(&self) -> &str {
        "HashJoinBuffering"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
