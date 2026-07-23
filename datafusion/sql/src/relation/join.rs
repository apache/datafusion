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
use datafusion_common::{
    Column, DFSchema, Result, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::utils::split_conjunction_owned;
use datafusion_expr::{
    AsOfMatch, BinaryExpr, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
};
use sqlparser::ast::{
    Join, JoinConstraint, JoinOperator, ObjectName, TableFactor, TableWithJoins,
};
use std::collections::HashSet;

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
        let right = if is_lateral_join(&join)? {
            self.create_relation_subquery(join.relation, planner_context)?
        } else {
            self.create_relation(join.relation, planner_context)?
        };
        match join.join_operator {
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
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => self.parse_asof_join(
                left,
                right,
                match_condition,
                constraint,
                planner_context,
            ),
            other => not_impl_err!("Unsupported JOIN operator {other:?}"),
        }
    }

    fn parse_asof_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        sql_match_condition: sqlparser::ast::Expr,
        constraint: JoinConstraint,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let join_schema = left.schema().join(right.schema())?;
        let match_condition =
            self.sql_to_expr(sql_match_condition, &join_schema, planner_context)?;
        let Expr::BinaryExpr(BinaryExpr {
            left: match_left,
            op,
            right: match_right,
        }) = match_condition
        else {
            return plan_err!("ASOF MATCH_CONDITION must be a single comparison");
        };
        if !matches!(
            op,
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
        ) {
            return plan_err!(
                "ASOF MATCH_CONDITION requires <, <=, >, or >=, found {op}"
            );
        }
        if !expr_owned_by(&match_left, left.schema())
            || !expr_owned_by(&match_right, right.schema())
        {
            return plan_err!(
                "ASOF MATCH_CONDITION left operand must reference only the left input and right operand only the right input"
            );
        }
        let match_condition = AsOfMatch::new(*match_left, op, *match_right);

        match constraint {
            JoinConstraint::On(sql_on) => {
                let on = self.sql_to_expr(sql_on, &join_schema, planner_context)?;
                let on = split_conjunction_owned(on)
                    .into_iter()
                    .map(|predicate| {
                        let Expr::BinaryExpr(BinaryExpr {
                            left: on_left,
                            op: Operator::Eq,
                            right: on_right,
                        }) = predicate
                        else {
                            return plan_err!(
                                "ASOF ON accepts only equality conditions combined with AND"
                            );
                        };
                        if expr_owned_by(&on_left, left.schema())
                            && expr_owned_by(&on_right, right.schema())
                        {
                            Ok((*on_left, *on_right))
                        } else if expr_owned_by(&on_right, left.schema())
                            && expr_owned_by(&on_left, right.schema())
                        {
                            Ok((*on_right, *on_left))
                        } else {
                            plan_err!(
                                "Each ASOF equality condition must compare one left expression with one right expression"
                            )
                        }
                    })
                    .collect::<Result<_>>()?;
                LogicalPlanBuilder::from(left)
                    .asof_join(right, on, match_condition)?
                    .build()
            }
            JoinConstraint::Using(object_names) => {
                let keys = object_names
                    .into_iter()
                    .map(|object_name| {
                        let ObjectName(mut object_names) = object_name;
                        if object_names.len() != 1 {
                            return not_impl_err!(
                                "Invalid identifier in ASOF USING clause. Expected single identifier, got {}",
                                ObjectName(object_names)
                            );
                        }
                        let id = object_names.swap_remove(0);
                        id.as_ident()
                            .ok_or_else(|| {
                                plan_datafusion_err!(
                                    "Expected identifier in ASOF USING clause"
                                )
                            })
                            .map(|ident| {
                                Column::from_name(
                                    self.ident_normalizer.normalize(ident.clone()),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                LogicalPlanBuilder::from(left)
                    .asof_join_using(right, keys, match_condition)?
                    .build()
            }
            JoinConstraint::None => LogicalPlanBuilder::from(left)
                .asof_join(right, vec![], match_condition)?
                .build(),
            JoinConstraint::Natural => {
                not_impl_err!("NATURAL ASOF JOIN is not supported")
            }
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
                self.warn_on_null_equality_predicate(&sql_expr);
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
}

fn expr_owned_by(expr: &Expr, schema: &DFSchema) -> bool {
    let columns = expr.column_refs();
    !columns.is_empty()
        && columns
            .iter()
            .all(|column| schema.is_column_from_schema(column))
}

/// Returns `true` if the given [`TableFactor`] is lateral.
pub(crate) fn is_lateral(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Derived { lateral, .. } => *lateral,
        TableFactor::Function { lateral, .. } => *lateral,
        TableFactor::UNNEST { .. } => true,
        _ => false,
    }
}

/// Returns `true` if the given [`Join`] is lateral.
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
