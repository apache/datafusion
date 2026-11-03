// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::PhysicalOptimizerRule;
use datafusion_common::JoinSide;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::buffer::BufferExec;
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
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
            let Some(node) = plan.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };
            let plan = Arc::clone(&plan);
            Ok(Transformed::yes(
                if HashJoinExec::probe_side() == JoinSide::Left {
                    // Do not stack BufferExec nodes together.
                    if node.left.is::<BufferExec>() {
                        return Ok(Transformed::no(plan));
                    }
                    replace_children_if_necessary(
                        plan,
                        vec![
                            Arc::new(BufferExec::new(Arc::clone(&node.left), capacity)),
                            Arc::clone(&node.right),
                        ],
                    )?
                } else {
                    // Do not stack BufferExec nodes together.
                    if node.right.is::<BufferExec>() {
                        return Ok(Transformed::no(plan));
                    }
                    replace_children_if_necessary(
                        plan,
                        vec![
                            Arc::clone(&node.left),
                            Arc::new(BufferExec::new(Arc::clone(&node.right), capacity)),
                        ],
                    )?
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
