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

//! Collection of utility functions that are leveraged by the query optimizer rules

use super::optimizer::PhysicalOptimizerRule;
use crate::execution::context::SessionConfig;

use crate::error::Result;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use std::sync::Arc;

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
pub fn optimize_children(
    optimizer: &impl PhysicalOptimizerRule,
    plan: Arc<dyn ExecutionPlan>,
    session_config: &SessionConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .iter()
        .map(|child| optimizer.optimize(Arc::clone(child), session_config))
        .collect::<Result<Vec<_>>>()?;

    if children.is_empty() {
        Ok(Arc::clone(&plan))
    } else {
        with_new_children_if_necessary(plan, children)
    }
}
