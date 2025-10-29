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
use datafusion_common::config::ConfigOptions;
use datafusion_common::internal_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::async_func::AsyncFuncExec;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Optimizer rule that introduces CoalesceAsyncExec to reduce the number of async executions.
#[derive(Default, Debug)]
pub struct CoalesceAsyncExecInput {}

impl CoalesceAsyncExecInput {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for CoalesceAsyncExecInput {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let target_batch_size = config.execution.batch_size;
        plan.transform(|plan| {
            if let Some(async_exec) = plan.as_any().downcast_ref::<AsyncFuncExec>() {
                if async_exec.children().len() != 1 {
                    return internal_err!(
                        "Expected AsyncFuncExec to have exactly one child"
                    );
                }
                let child = Arc::clone(async_exec.children()[0]);
                let coalesce_exec =
                    Arc::new(CoalesceBatchesExec::new(child, target_batch_size));
                let coalesce_async_exec = plan.with_new_children(vec![coalesce_exec])?;
                Ok(Transformed::yes(coalesce_async_exec))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "coalesce_async_exec_input"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
