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
use datafusion_common::{not_impl_err, plan_datafusion_err, plan_err, Column, Result};
use datafusion_expr::{
    ExprSchemable, JoinType, LateralBatchedTableFunction, LogicalPlan, LogicalPlanBuilder,
};
use sqlparser::ast::{
    Join, JoinConstraint, JoinOperator, ObjectName, TableFactor, TableWithJoins,
};
use std::collections::HashSet;
use std::sync::Arc;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn plan_table_with_joins(
        &self,
        t: TableWithJoins,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let mut left = if is_lateral(&t.relation) {
            self.create_relation_subquery(t.relation, planner_context)?
        } else {
            self.create_relation(t.relation, planner_context)?
        };
        let old_outer_from_schema = planner_context.outer_from_schema();
        for join in t.joins {
            planner_context.extend_outer_from_schema(left.schema())?;
            left = self.parse_relation_join(left, join, planner_context)?;
        }
        planner_context.set_outer_from_schema(old_outer_from_schema);
        Ok(left)
    }

    pub(crate) fn parse_relation_join(
        &self,
        left: LogicalPlan,
        join: Join,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let is_lateral = is_lateral_join(&join)?;

        if is_lateral {
            if let Some(lateral_tf_plan) = self.try_create_lateral_table_function(
                &left,
                &join.relation,
                planner_context,
            )? {
                // LateralBatchedTableFunction already combines input + function output
                match &join.join_operator {
                    JoinOperator::CrossJoin(_) | JoinOperator::CrossApply => {
                        return Ok(lateral_tf_plan);
                    }
                    _ => {
                        // For other join types, use lateral function as right side
                        let right = lateral_tf_plan;
                        return self.finish_join(
                            left,
                            right,
                            join.join_operator,
                            planner_context,
                        );
                    }
                }
            }
        }

        let Join {
            relation,
            join_operator,
            ..
        } = join;

        let right = if is_lateral {
            self.create_relation_subquery(relation, planner_context)?
        } else {
            self.create_relation(relation, planner_context)?
        };

        self.finish_join(left, right, join_operator, planner_context)
    }

    fn finish_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_operator: JoinOperator,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match join_operator {
            JoinOperator::LeftOuter(constraint) | JoinOperator::Left(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left, planner_context)
            }
            JoinOperator::RightOuter(constraint) | JoinOperator::Right(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right, planner_context)
            }
            JoinOperator::Inner(constraint) | JoinOperator::Join(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner, planner_context)
            }
            JoinOperator::LeftSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftSemi,
                planner_context,
            ),
            JoinOperator::RightSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightSemi,
                planner_context,
            ),
            JoinOperator::LeftAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftAnti,
                planner_context,
            ),
            JoinOperator::RightAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightAnti,
                planner_context,
            ),
            JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full, planner_context)
            }
            JoinOperator::CrossJoin(JoinConstraint::None) => {
                self.parse_cross_join(left, right)
            }
            other => not_impl_err!("Unsupported JOIN operator {other:?}"),
        }
    }

    fn parse_cross_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: JoinConstraint,
        join_type: JoinType,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let join_schema = left.schema().join(right.schema())?;
                // parse ON expression
                let expr = self.sql_to_expr(sql_expr, &join_schema, planner_context)?;
                LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(expr))?
                    .build()
            }
            JoinConstraint::Using(object_names) => {
                let keys = object_names
                    .into_iter()
                    .map(|object_name| {
                        let ObjectName(mut object_names) = object_name;
                        if object_names.len() != 1 {
                            not_impl_err!(
                                "Invalid identifier in USING clause. Expected single identifier, got {}", ObjectName(object_names)
                            )
                        } else {
                            let id = object_names.swap_remove(0);
                            id.as_ident()
                                .ok_or_else(|| {
                                    plan_datafusion_err!(
                                        "Expected identifier in USING clause"
                                    )
                                })
                                .map(|ident| Column::from_name(self.ident_normalizer.normalize(ident.clone())))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                LogicalPlanBuilder::from(left)
                    .join_using(right, join_type, keys)?
                    .build()
            }
            JoinConstraint::Natural => {
                let left_cols: HashSet<&String> =
                    left.schema().fields().iter().map(|f| f.name()).collect();
                let keys: Vec<Column> = right
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .filter(|f| left_cols.contains(f))
                    .map(Column::from_name)
                    .collect();
                if keys.is_empty() {
                    self.parse_cross_join(left, right)
                } else {
                    LogicalPlanBuilder::from(left)
                        .join_using(right, join_type, keys)?
                        .build()
                }
            }
            JoinConstraint::None => LogicalPlanBuilder::from(left)
                .join_on(right, join_type, [])?
                .build(),
        }
    }

    /// Try to create a LateralBatchedTableFunction node if this is a lateral batched table function
    fn try_create_lateral_table_function(
        &self,
        input_plan: &LogicalPlan,
        relation: &TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        let TableFactor::Function {
            name, args, alias, ..
        } = relation
        else {
            return Ok(None);
        };

        let tbl_func_ref = self.object_name_to_table_reference(name.clone())?;
        let func_name = tbl_func_ref.table();

        if !self.context_provider.is_batched_table_function(func_name) {
            return Ok(None);
        }

        let input_schema = input_plan.schema();

        let func_args: Vec<datafusion_expr::Expr> = args
            .iter()
            .map(|arg| match arg {
                sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(expr),
                )
                | sqlparser::ast::FunctionArg::Named {
                    arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ..
                } => self.sql_expr_to_logical_expr(
                    expr.clone(),
                    input_schema.as_ref(),
                    planner_context,
                ),
                _ => plan_err!("Unsupported function argument: {arg:?}"),
            })
            .collect::<Result<Vec<_>>>()?;

        let arg_types: Vec<arrow::datatypes::DataType> = func_args
            .iter()
            .map(|e| e.get_type(input_schema.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        let source = self
            .context_provider
            .get_batched_table_function_source(func_name, &arg_types)?
            .ok_or_else(|| {
                datafusion_common::plan_datafusion_err!(
                    "Failed to get source for batched table function '{}'",
                    func_name
                )
            })?;

        // Get schema from source
        let func_schema = source.schema();

        // Use alias if provided, otherwise "function_name()"
        let qualifier = alias
            .as_ref()
            .map(|a| self.ident_normalizer.normalize(a.name.clone()))
            .unwrap_or_else(|| format!("{func_name}()"));

        let qualified_func_schema =
            datafusion_common::DFSchema::try_from_qualified_schema(
                &qualifier,
                &func_schema,
            )?;

        let combined_schema = input_schema.as_ref().join(&qualified_func_schema)?;

        let lateral_plan =
            LogicalPlan::LateralBatchedTableFunction(LateralBatchedTableFunction {
                input: Arc::new(input_plan.clone()),
                function_name: func_name.to_string(),
                source,
                args: func_args,
                schema: Arc::new(combined_schema),
                table_function_schema: Arc::new(qualified_func_schema),
                projection: None,
                filters: vec![],
                fetch: None,
            });

        Ok(Some(lateral_plan))
    }
}

/// Return `true` iff the given [`TableFactor`] is lateral.
pub(crate) fn is_lateral(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Derived { lateral, .. } => *lateral,
        TableFactor::Function { lateral, .. } => *lateral,
        TableFactor::UNNEST { .. } => true,
        _ => false,
    }
}

/// Return `true` iff the given [`Join`] is lateral.
pub(crate) fn is_lateral_join(join: &Join) -> Result<bool> {
    let is_lateral_syntax = is_lateral(&join.relation);
    let is_apply_syntax = match join.join_operator {
        JoinOperator::FullOuter(..)
        | JoinOperator::Right(..)
        | JoinOperator::RightOuter(..)
        | JoinOperator::RightAnti(..)
        | JoinOperator::RightSemi(..)
            if is_lateral_syntax =>
        {
            return not_impl_err!(
                "LATERAL syntax is not supported for \
                 FULL OUTER and RIGHT [OUTER | ANTI | SEMI] joins"
            );
        }
        JoinOperator::CrossApply | JoinOperator::OuterApply => true,
        _ => false,
    };
    Ok(is_lateral_syntax || is_apply_syntax)
}
