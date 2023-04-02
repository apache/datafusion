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

use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
use crate::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};

#[derive(Default)]
pub struct CombinePartialFinalAggregate {}

impl CombinePartialFinalAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for CombinePartialFinalAggregate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let transformed = if let Some(AggregateExec {
                mode: final_mode,
                input: final_input,
                group_by: final_group_by,
                aggr_expr: final_aggr_expr,
                ..
            }) = plan.as_any().downcast_ref::<AggregateExec>()
            {
                if matches!(
                    final_mode,
                    AggregateMode::Final | AggregateMode::FinalPartitioned
                ) {
                    if let Some(AggregateExec {
                        mode,
                        group_by,
                        aggr_expr,
                        input,
                        input_schema,
                        ..
                    }) = final_input.as_any().downcast_ref::<AggregateExec>()
                    {
                        if matches!(mode, AggregateMode::Partial)
                            && final_group_by.eq(group_by)
                            && final_aggr_expr.len() == aggr_expr.len()
                        {
                            Some(Arc::new(AggregateExec::try_new(
                                AggregateMode::Single,
                                group_by.clone(),
                                aggr_expr.to_vec(),
                                input.clone(),
                                input_schema.clone(),
                            )?))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            Ok(if let Some(transformed) = transformed {
                Transformed::Yes(transformed)
            } else {
                Transformed::No(plan)
            })
        })
    }

    fn name(&self) -> &str {
        "CombinePartialFinalAggregate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
