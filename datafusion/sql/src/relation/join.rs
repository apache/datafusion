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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel, UsingMergedKey};
use datafusion_common::{
    Column, Result, internal_datafusion_err, not_impl_err, plan_datafusion_err,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
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

                self.plan_using_join(left, right, join_type, keys, planner_context)
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
                    self.plan_using_join(left, right, join_type, keys, planner_context)
                }
            }
            JoinConstraint::None => LogicalPlanBuilder::from(left)
                .join_on(right, join_type, [])?
                .build(),
        }
    }

    /// Plan a `USING` / `NATURAL` join, registering merged keys when the left side
    /// can be NULL-padded.
    ///
    /// Per SQL:2016 §7.10 a `USING`/`NATURAL` join column is
    /// `COALESCE(left.k, right.k)`. For `RIGHT`/`FULL` outer joins the left key is
    /// NULL-padded for unmatched rows, so resolving an unqualified reference to the
    /// left key alone yields the wrong (NULL) value. We keep both per-side keys in
    /// the join's output schema (so `left.k` / `right.k` remain individually
    /// addressable) and register a [`UsingMergedKey`] so that a later *unqualified*
    /// reference to the key resolves to the coalesced value instead. See
    /// [`SqlToRel::sql_identifier_to_expr`].
    ///
    /// `INNER`/`LEFT` joins need no rewrite: their left key is never NULL-padded, so
    /// the existing resolution to the left column is already correct.
    fn plan_using_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        keys: Vec<Column>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let join = LogicalPlanBuilder::from(left)
            .join_using(right, join_type, keys)?
            .build()?;

        // Only RIGHT/FULL outer joins can NULL-pad the left key.
        if !matches!(join_type, JoinType::Right | JoinType::Full) {
            return Ok(join);
        }

        // `join_using` degrades to a cross join + filter when the keys are not
        // hashable; there is no USING merge to register in that case.
        let LogicalPlan::Join(inner) = &join else {
            return Ok(join);
        };
        if inner.join_constraint != datafusion_expr::JoinConstraint::Using {
            return Ok(join);
        }

        let coalesce = if matches!(join_type, JoinType::Full) {
            Some(
                self.context_provider
                    .get_function_meta("coalesce")
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Unable to find expected 'coalesce' function"
                        )
                    })?,
            )
        } else {
            None
        };

        let mut merged_keys = Vec::with_capacity(inner.on.len());
        for (a, b) in &inner.on {
            // `join_using` may order the equi-join pair as (right, left) depending on
            // which side is hashable, so identify the sides by schema membership.
            let (Expr::Column(ca), Expr::Column(cb)) = (a, b) else {
                continue;
            };
            let (left_col, right_col) = if inner.left.schema().has_column(ca) {
                (ca.clone(), cb.clone())
            } else {
                (cb.clone(), ca.clone())
            };
            let name = left_col.name.clone();
            // The replacement is left *unaliased* so that it stays clean when
            // substituted into arbitrary expression contexts (e.g. `WHERE k = 4`).
            // SELECT-list output naming (`... AS k`) and `SELECT *` expansion are
            // handled where the key name is known (see `sql_select_to_rex`).
            let replacement = match &coalesce {
                // FULL: either side may be NULL-padded -> COALESCE(left, right).
                Some(coalesce) => Expr::ScalarFunction(ScalarFunction::new_udf(
                    Arc::clone(coalesce),
                    vec![
                        Expr::Column(left_col.clone()),
                        Expr::Column(right_col.clone()),
                    ],
                )),
                // RIGHT: the right side is always present.
                None => Expr::Column(right_col.clone()),
            };
            merged_keys.push(UsingMergedKey {
                name,
                left: left_col,
                right: right_col,
                replacement,
            });
        }
        planner_context.register_using_merged_keys(merged_keys);

        Ok(join)
    }
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
