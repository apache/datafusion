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

//! [`EliminateAggregationSelfJoin`] eliminates aggregation expressions
//! over self joins that can be translated to window expressions.

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::{tree_node::Transformed, Result};
use datafusion_expr::LogicalPlan;

#[derive(Default, Debug)]
pub struct EliminateAggregationSelfJoin;

impl EliminateAggregationSelfJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
impl OptimizerRule for EliminateAggregationSelfJoin {
    fn name(&self) -> &str {
        "eliminate_aggregation_self_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(plan))
    }
}
