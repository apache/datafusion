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

//! CombinePartialFinalAggregate optimizer rule checks the adjacent Partial and Final AggregateExecs
//! and try to combine them if necessary
use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
use crate::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};

/// CombinePartialFinalAggregate optimizer rule combines the adjacent Partial and Final AggregateExecs
/// into a Single AggregateExec if their grouping exprs and aggregate exprs equal.
///
/// This rule should be applied after the EnforceDistribution and EnforceSorting rules
///
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
            let transformed = plan.as_any().downcast_ref::<AggregateExec>().and_then(
                |AggregateExec {
                     mode: final_mode,
                     input: final_input,
                     group_by: final_group_by,
                     aggr_expr: final_aggr_expr,
                     ..
                 }| {
                    if matches!(
                        final_mode,
                        AggregateMode::Final | AggregateMode::FinalPartitioned
                    ) {
                        final_input
                            .as_any()
                            .downcast_ref::<AggregateExec>()
                            .and_then(
                                |AggregateExec {
                                     mode: input_mode,
                                     input: partial_input,
                                     group_by: input_group_by,
                                     aggr_expr: input_aggr_expr,
                                     input_schema,
                                     ..
                                 }| {
                                    if matches!(input_mode, AggregateMode::Partial)
                                        && final_group_by.eq(input_group_by)
                                        && final_aggr_expr.len() == input_aggr_expr.len()
                                        && final_aggr_expr
                                            .iter()
                                            .zip(input_aggr_expr.iter())
                                            .all(|(final_expr, partial_expr)| {
                                                final_expr.eq(partial_expr)
                                            })
                                    {
                                        AggregateExec::try_new(
                                            AggregateMode::Single,
                                            input_group_by.clone(),
                                            input_aggr_expr.to_vec(),
                                            partial_input.clone(),
                                            input_schema.clone(),
                                        )
                                        .ok()
                                        .map(Arc::new)
                                    } else {
                                        None
                                    }
                                },
                            )
                    } else {
                        None
                    }
                },
            );

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
