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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{DFSchema, DataFusionError, Result};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => {
                self.select_to_plan(*s, planner_context, outer_query_schema)
            }
            SetExpr::Values(v) => {
                self.sql_values_to_plan(v, &planner_context.prepare_param_data_types)
            }
            SetExpr::SetOperation {
                op,
                left,
                right,
                set_quantifier,
            } => {
                let all = match set_quantifier {
                    SetQuantifier::All => true,
                    SetQuantifier::Distinct | SetQuantifier::None => false,
                };

                let left_plan =
                    self.set_expr_to_plan(*left, planner_context, outer_query_schema)?;
                let right_plan =
                    self.set_expr_to_plan(*right, planner_context, outer_query_schema)?;
                match (op, all) {
                    (SetOperator::Union, true) => LogicalPlanBuilder::from(left_plan)
                        .union(right_plan)?
                        .build(),
                    (SetOperator::Union, false) => LogicalPlanBuilder::from(left_plan)
                        .union_distinct(right_plan)?
                        .build(),
                    (SetOperator::Intersect, true) => {
                        LogicalPlanBuilder::intersect(left_plan, right_plan, true)
                    }
                    (SetOperator::Intersect, false) => {
                        LogicalPlanBuilder::intersect(left_plan, right_plan, false)
                    }
                    (SetOperator::Except, true) => {
                        LogicalPlanBuilder::except(left_plan, right_plan, true)
                    }
                    (SetOperator::Except, false) => {
                        LogicalPlanBuilder::except(left_plan, right_plan, false)
                    }
                }
            }
            SetExpr::Query(q) => self.query_to_plan(*q, planner_context),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Query {set_expr} not implemented yet"
            ))),
        }
    }
}
