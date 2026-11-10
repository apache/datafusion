// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::ast::{
    Expr as SQLExpr, Ident, Join, JoinConstraint, JoinKind, TableFactor, TableRef,
    TableWithJoins,
};
use crate::expr::reject_window_functions;
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{Column, HashMap, Result, not_impl_err, plan_err};
use datafusion_expr::{JoinType, LogicalPlan, LogicalPlanBuilder};
use std::collections::HashSet;

use super::is_unnest_factor;

const JOIN_ON_HELP: &str =
    "Compute the window function in a subquery and join on its result";

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn plan_table_with_joins(
        &self,
        t: TableWithJoins<'_>,
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
        join: Join<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let lateral = is_lateral_join(&join)?;
        let Join {
            kind,
            spelled_out: _,
            table,
            constraint,
        } = join;
        let right = if lateral {
            self.create_relation_subquery(table, planner_context)?
        } else {
            self.create_relation(table, planner_context)?
        };
        let join_type = match kind {
            JoinKind::Left => JoinType::Left,
            JoinKind::Right => JoinType::Right,
            JoinKind::Inner => JoinType::Inner,
            JoinKind::LeftSemi => JoinType::LeftSemi,
            JoinKind::RightSemi => JoinType::RightSemi,
            JoinKind::LeftAnti => JoinType::LeftAnti,
            JoinKind::RightAnti => JoinType::RightAnti,
            JoinKind::Full => JoinType::Full,
            JoinKind::Cross => {
                return match constraint {
                    JoinConstraint::None => self.parse_cross_join(left, right),
                    other => not_impl_err!(
                        "Unsupported JOIN operator CROSS JOIN with constraint {other:?}"
                    ),
                };
            }
            JoinKind::AsOf => {
                return match constraint {
                    JoinConstraint::AsOf { condition, on } => self.parse_asof_join(
                        left,
                        right,
                        condition,
                        on.map_or(JoinConstraint::None, JoinConstraint::On),
                        planner_context,
                    ),
                    other => not_impl_err!(
                        "Unsupported JOIN operator ASOF JOIN without MATCH_CONDITION: {other:?}"
                    ),
                };
            }
            other => return not_impl_err!("Unsupported JOIN operator {other:?}"),
        };
        self.parse_join(left, right, constraint, join_type, planner_context)
    }

    fn parse_asof_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        sql_match_condition: SQLExpr<'_>,
        constraint: JoinConstraint<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let join_schema = left.schema().join(right.schema())?;
        let match_condition =
            self.sql_to_expr(sql_match_condition, &join_schema, planner_context)?;

        match constraint {
            JoinConstraint::On(sql_on) => {
                let window_span = self.window_function_span(&sql_on, &HashMap::new());
                let on = self.sql_to_expr(sql_on, &join_schema, planner_context)?;
                reject_window_functions(&on, "JOIN ON", JOIN_ON_HELP, window_span)?;
                LogicalPlanBuilder::from(left)
                    .asof_join_on(right, Some(on), match_condition)?
                    .build()
            }
            JoinConstraint::Using(idents) => {
                let keys = self.using_columns(idents);
                LogicalPlanBuilder::from(left)
                    .asof_join_using(right, keys, match_condition)?
                    .build()
            }
            JoinConstraint::None => LogicalPlanBuilder::from(left)
                .asof_join_on(right, None, match_condition)?
                .build(),
            JoinConstraint::Natural => plan_err!("NATURAL ASOF JOIN is not supported"),
            other => not_impl_err!(
                "ASOF JOIN constraint {other:?} is not yet supported with the new SQL parser"
            ),
        }
    }

    /// The columns of a `USING (a, b)` list.
    fn using_columns(&self, idents: Vec<Ident<'_>>) -> Vec<Column> {
        idents
            .into_iter()
            .map(|ident| Column::from_name(self.ident_normalizer.normalize(ident)))
            .collect()
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
        constraint: JoinConstraint<'_>,
        join_type: JoinType,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let join_schema = left.schema().join(right.schema())?;
                // parse ON expression
                self.warn_on_null_equality_predicate(&sql_expr);
                let window_span = self.window_function_span(&sql_expr, &HashMap::new());
                let expr = self.sql_to_expr(sql_expr, &join_schema, planner_context)?;
                // A join condition is evaluated before window functions are
                // computed, so they may not appear in it
                reject_window_functions(&expr, "JOIN ON", JOIN_ON_HELP, window_span)?;
                LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(expr))?
                    .build()
            }
            JoinConstraint::Using(idents) => {
                let keys = self.using_columns(idents);
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
            other => not_impl_err!(
                "JOIN constraint {other:?} is not yet supported with the new SQL parser"
            ),
        }
    }
}

/// Returns `true` if the given [`TableRef`] is lateral.
pub(crate) fn is_lateral(table: &TableRef<'_>) -> bool {
    matches!(table.factor, TableFactor::Lateral(_)) || is_unnest_factor(&table.factor)
}

/// Returns `true` if the given [`Join`] is lateral.
pub(crate) fn is_lateral_join(join: &Join<'_>) -> Result<bool> {
    let is_lateral_syntax = is_lateral(&join.table);
    match join.kind {
        JoinKind::Full | JoinKind::Right | JoinKind::RightAnti | JoinKind::RightSemi
            if is_lateral_syntax =>
        {
            not_impl_err!(
                "LATERAL syntax is not supported for \
                 FULL OUTER and RIGHT [OUTER | ANTI | SEMI] joins"
            )
        }
        _ => Ok(is_lateral_syntax),
    }
}
