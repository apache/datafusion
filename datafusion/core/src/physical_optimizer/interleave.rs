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

//! Rule to replace [`UnionExec`] with [`InterleaveExec`].

use std::sync::Arc;

use datafusion_common::{
    config::ConfigOptions,
    tree_node::{Transformed, TreeNode},
    Result,
};

use crate::physical_plan::{
    union::{can_interleave, InterleaveExec, UnionExec},
    ExecutionPlan,
};

use super::PhysicalOptimizerRule;

/// Rule to replace [`UnionExec`] with [`InterleaveExec`].
#[derive(Default)]
pub struct Interleave {}

impl Interleave {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for Interleave {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() {
                if can_interleave(union_exec.inputs()) {
                    let plan = InterleaveExec::try_new(union_exec.inputs().clone())?;
                    return Ok(Transformed::Yes(Arc::new(plan)));
                }
            }
            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "interleave"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
