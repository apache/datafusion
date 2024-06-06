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

use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::{Optimizer, OptimizerContext, OptimizerRule};

mod common_subexpr_eliminate;
mod push_down_filter;
mod user_defined;

fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

pub(super) fn assert_optimized_plan_eq(
    rule: Arc<dyn OptimizerRule + Send + Sync>,
    plan: LogicalPlan,
    expected: &str,
) -> Result<()> {
    // Apply the rule once
    let opt_context = OptimizerContext::new().with_max_passes(1);

    let optimizer = Optimizer::with_rules(vec![rule.clone()]);
    let optimized_plan = optimizer.optimize(plan, &opt_context, observe)?;
    let formatted_plan = format!("{optimized_plan:?}");
    assert_eq!(formatted_plan, expected);

    Ok(())
}
