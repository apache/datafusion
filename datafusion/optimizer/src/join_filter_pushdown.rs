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

//! [`JoinFilterPushdown`] pushdown join filter to scan dynamically

use std::sync::Arc;

use datafusion_common::{tree_node::Transformed, DFSchema, DataFusionError};
use datafusion_expr::{
    expr::ScalarFunction,
    utils::{DynamicJoinFilterPushdownColumn, DynamicJoinFilterPushdownInfo},
    BinaryExpr, Expr, ExprSchemable, JoinType, LogicalPlan,
};
use datafusion_functions_aggregate::min_max;

use crate::{
    optimizer::ApplyOrder, push_down_filter::on_lr_is_preserved, OptimizerConfig,
    OptimizerRule,
};

#[derive(Default, Debug)]
pub struct JoinFilterPushdown {}

impl OptimizerRule for JoinFilterPushdown {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if !config.options().optimizer.dynamic_join_pushdown {
            return Ok(Transformed::no(plan));
        }

        match plan {
            LogicalPlan::Join(join) => {
                if unsupported_join_type(&(join.join_type)) {
                    return Ok(Transformed::no(plan));
                }
                let mut join_filter_pushdown_info = DynamicJoinFilterPushdownInfo::new();
                // iterate the on clause and generate the filter info
                for (i, (left, right)) in join.on.iter().enumerate() {
                    // only support left to be a column
                    if let (Expr::Column(l), Expr::Column(_)) = (left, right) {
                        join_filter_pushdown_info.push_filter(
                            DynamicJoinFilterPushdownColumn::new(i, Arc::new(l.clone())),
                        );
                    }
                }
                let mut probe = join.right.as_ref();
                // on the probe sides, we want to make sure that we can push the filter to the probe side
                loop {
                    if matches!(probe, LogicalPlan::TableScan(_)) {
                        break;
                    }
                    match probe {
                        LogicalPlan::Limit(_)
                        | LogicalPlan::Filter(_)
                        | LogicalPlan::Sort(_)
                        | LogicalPlan::Distinct(_) => {
                            probe = &probe.inputs()[0];
                        }
                        LogicalPlan::Projection(project) => {
                            for filter in join_filter_pushdown_info.filters.iter() {
                                if !project.schema.has_column(&filter.column) {
                                    return Ok(Transformed::no(plan));
                                }
                            }
                            probe = &probe.inputs()[0];
                        }
                        _ => return Ok(Transformed::no(plan)),
                    }
                }
                // create all the aggregate function which could be applied on
                let aggregates: Vec<_> = join_filter_pushdown_info
                    .filters
                    .iter()
                    .flat_map(|filter| {
                        let max_agg =
                            min_max::max(join.on[filter.condition_idx].0.clone());
                        let min_agg =
                            min_max::min(join.on[filter.condition_idx].0.clone());
                        vec![max_agg, min_agg]
                    })
                    .collect();
                join_filter_pushdown_info.push_aggregates(aggregates);
                // assign the value
                join.with_filter_pushdown_info(Arc::new(join_filter_pushdown_info));
                Ok(Transformed::yes(plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
    fn name(&self) -> &str {
        "join_filter_pushdown"
    }
}

fn unsupported_join_type(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::RightSemi | JoinType::RightAnti
    )
}
