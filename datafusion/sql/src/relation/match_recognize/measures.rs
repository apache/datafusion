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

//! MEASURES planning for MATCH_RECOGNIZE.
//!
//! This module plans `MEASURES` expressions in the context of the
//! `MATCH_RECOGNIZE` schema. If any measure contains a window function, it
//! injects a `Window` plan above the MR node and safely rebases the expressions
//! to refer to the new plan. When `ONE ROW PER MATCH` is required, it wraps
//! non-aggregate measures with `LAST_VALUE(... ORDER BY MATCH_SEQUENCE_NUMBER)`
//! and plans a corresponding `Aggregate` to reduce per-match rows.

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::utils::rebase_expr;
use datafusion_common::{plan_err, DFSchemaRef, Result};
use datafusion_expr::col;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::logical_plan::NamedExpr;
use datafusion_expr::match_recognize::columns::MrMetadataColumn;
use datafusion_expr::utils::{find_aggregate_exprs, find_window_exprs};
use datafusion_expr::{Expr, LogicalPlan};
use sqlparser::ast::{Expr as SQLExpr, OrderByExpr};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn plan_measures_exprs(
        &self,
        measures: Vec<sqlparser::ast::Measure>,
        mr_schema: &DFSchemaRef,
        partition_by: &[SQLExpr],
        order_by: &[OrderByExpr],
        rows_per_match: Option<sqlparser::ast::RowsPerMatch>,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<NamedExpr>> {
        let measures_exprs = measures
            .into_iter()
            .map(|measure| {
                let expr = self.sql_to_expr_with_match_recognize_context(
                    crate::planner::MatchRecognizeClause::Measures,
                    measure.expr,
                    mr_schema,
                    partition_by,
                    order_by,
                    rows_per_match.clone(),
                    planner_context,
                )?;
                Ok(NamedExpr {
                    expr,
                    name: measure.alias.value,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(measures_exprs)
    }

    pub(super) fn maybe_apply_window_and_rebase_measures(
        plan: LogicalPlan,
        measures_exprs: Vec<NamedExpr>,
    ) -> Result<(LogicalPlan, Vec<NamedExpr>)> {
        let window_exprs = find_window_exprs(
            measures_exprs
                .iter()
                .map(|ne| ne.expr.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        );

        if window_exprs.is_empty() {
            Ok((plan, measures_exprs))
        } else {
            let window_plan = datafusion_expr::LogicalPlanBuilder::window_plan(
                plan,
                window_exprs.clone(),
            )?;
            let measures_exprs = measures_exprs
                .iter()
                .map(|ne| {
                    let rebased_expr =
                        rebase_expr(&ne.expr, &window_exprs, &window_plan)?;
                    Ok(NamedExpr {
                        expr: rebased_expr,
                        name: ne.name.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((window_plan, measures_exprs))
        }
    }

    pub(super) fn apply_aggregate_and_rebase_measures(
        &self,
        plan: LogicalPlan,
        measures_exprs: Vec<NamedExpr>,
        partition_by_exprs: &[Expr],
    ) -> Result<(LogicalPlan, Vec<NamedExpr>)> {
        // Ensure LAST_VALUE aggregate is available
        let last_value_udaf = match self.context_provider.get_aggregate_meta("last_value")
        {
            Some(f) => f,
            None => return plan_err!("LAST_VALUE aggregate function is not registered"),
        };

        // ORDER BY __mr_match_sequence_number for reduction across match rows
        let mr_order_by =
            vec![col(MrMetadataColumn::MatchSequenceNumber.as_ref()).sort(true, false)];

        // Wrap non-aggregate expressions with LAST_VALUE(expr ORDER BY __mr_match_sequence_number)
        let wrapped_measures = measures_exprs
            .into_iter()
            .map(|ne| {
                let has_agg =
                    !find_aggregate_exprs(std::slice::from_ref(&ne.expr)).is_empty();
                if has_agg {
                    ne
                } else {
                    let agg_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
                        std::sync::Arc::clone(&last_value_udaf),
                        vec![ne.expr],
                        false,
                        None,
                        mr_order_by.clone(),
                        None,
                    ));
                    NamedExpr {
                        expr: agg_expr,
                        name: ne.name,
                    }
                }
            })
            .collect::<Vec<_>>();

        // Collect all aggregate expressions (including newly wrapped ones)
        let aggregate_exprs = find_aggregate_exprs(
            wrapped_measures
                .iter()
                .map(|ne| ne.expr.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        );

        if aggregate_exprs.is_empty() {
            // No measures provided; nothing to aggregate
            return Ok((plan, wrapped_measures));
        }

        // Build aggregate plan grouped by PARTITION BY columns and match number
        let mut group_expr = partition_by_exprs.to_vec();
        group_expr.push(col(MrMetadataColumn::MatchNumber.as_ref()));
        let aggregate_plan = datafusion_expr::LogicalPlanBuilder::from(plan)
            .aggregate(group_expr, aggregate_exprs.clone())?
            .build()?;

        let rebased_measures = wrapped_measures
            .iter()
            .map(|ne| {
                let rebased_expr =
                    rebase_expr(&ne.expr, &aggregate_exprs, &aggregate_plan)?;
                Ok(NamedExpr {
                    expr: rebased_expr,
                    name: ne.name.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((aggregate_plan, rebased_measures))
    }
}
