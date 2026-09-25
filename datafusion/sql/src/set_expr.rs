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

use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{
    Column, DataFusionError, Diagnostic, Result, Span, not_impl_err, plan_err,
};
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder,
    expr::{WindowFunction, WindowFunctionDefinition},
};
use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier, Spanned};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        crate::stack::maybe_grow(|| {
            let set_expr_span = Span::try_from_sqlparser_span(set_expr.span());
            match set_expr {
                SetExpr::Select(s) => self.select_to_plan(*s, None, planner_context),
                SetExpr::Values(v) => self.sql_values_to_plan(v, planner_context),
                SetExpr::SetOperation {
                    op,
                    left,
                    right,
                    set_quantifier,
                } => {
                    let left_span = Span::try_from_sqlparser_span(left.span());
                    let right_span = Span::try_from_sqlparser_span(right.span());
                    let left_plan = self.set_expr_to_plan(*left, planner_context);
                    // Store the left plan's schema so that the right side can
                    // alias duplicate expressions to match. Skip for BY NAME
                    // operations since those match columns by name, not position.
                    if let Ok(plan) = &left_plan
                        && plan.schema().fields().len() > 1
                        && !matches!(
                            set_quantifier,
                            SetQuantifier::ByName
                                | SetQuantifier::AllByName
                                | SetQuantifier::DistinctByName
                        )
                    {
                        planner_context
                            .set_set_expr_left_schema(Some(Arc::clone(plan.schema())));
                    }
                    let right_plan = self.set_expr_to_plan(*right, planner_context);
                    planner_context.set_set_expr_left_schema(None);
                    let (left_plan, right_plan) = match (left_plan, right_plan) {
                        (Ok(left_plan), Ok(right_plan)) => (left_plan, right_plan),
                        (Err(left_err), Err(right_err)) => {
                            return Err(DataFusionError::Collection(vec![
                                left_err, right_err,
                            ]));
                        }
                        (Err(err), _) | (_, Err(err)) => {
                            return Err(err);
                        }
                    };
                    if !(set_quantifier == SetQuantifier::ByName
                        || set_quantifier == SetQuantifier::AllByName)
                    {
                        self.validate_set_expr_num_of_columns(
                            op,
                            left_span,
                            right_span,
                            &left_plan,
                            &right_plan,
                            set_expr_span,
                        )?;
                    }
                    self.set_operation_to_plan(op, left_plan, right_plan, set_quantifier)
                }
                SetExpr::Query(q) => self.query_to_plan(*q, planner_context),
                _ => not_impl_err!("Query {set_expr} not implemented yet"),
            }
        })
    }

    pub(super) fn is_union_all(set_quantifier: SetQuantifier) -> Result<bool> {
        match set_quantifier {
            SetQuantifier::All | SetQuantifier::AllByName => Ok(true),
            SetQuantifier::Distinct
            | SetQuantifier::ByName
            | SetQuantifier::DistinctByName
            | SetQuantifier::None => Ok(false),
        }
    }

    fn validate_set_expr_num_of_columns(
        &self,
        op: SetOperator,
        left_span: Option<Span>,
        right_span: Option<Span>,
        left_plan: &LogicalPlan,
        right_plan: &LogicalPlan,
        set_expr_span: Option<Span>,
    ) -> Result<()> {
        if left_plan.schema().fields().len() == right_plan.schema().fields().len() {
            return Ok(());
        }
        let diagnostic = Diagnostic::new_error(
            format!("{op} queries have different number of columns"),
            set_expr_span,
        )
        .with_note(
            format!("this side has {} fields", left_plan.schema().fields().len()),
            left_span,
        )
        .with_note(
            format!(
                "this side has {} fields",
                right_plan.schema().fields().len()
            ),
            right_span,
        );
        plan_err!("{} queries have different number of columns", op; diagnostic =diagnostic)
    }

    pub(super) fn set_operation_to_plan(
        &self,
        op: SetOperator,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        set_quantifier: SetQuantifier,
    ) -> Result<LogicalPlan> {
        match (op, set_quantifier) {
            (SetOperator::Union, SetQuantifier::All) => {
                LogicalPlanBuilder::from(left_plan)
                    .union(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::AllByName) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_by_name(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_distinct(right_plan)?
                    .build()
            }
            (
                SetOperator::Union,
                SetQuantifier::ByName | SetQuantifier::DistinctByName,
            ) => LogicalPlanBuilder::from(left_plan)
                .union_by_name_distinct(right_plan)?
                .build(),
            (SetOperator::Intersect, SetQuantifier::All) => {
                self.intersect_or_except_all(left_plan, right_plan, true)
            }
            (SetOperator::Intersect, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::intersect(left_plan, right_plan, false)
            }
            (SetOperator::Except, SetQuantifier::All) => {
                self.intersect_or_except_all(left_plan, right_plan, false)
            }
            (SetOperator::Except, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::except(left_plan, right_plan, false)
            }
            (op, quantifier) => {
                not_impl_err!("{op} {quantifier} not implemented")
            }
        }
    }

    fn intersect_or_except_all(
        &self,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        intersect: bool,
    ) -> Result<LogicalPlan> {
        let Some(row_number) = self.context_provider.get_window_meta("row_number") else {
            return plan_err!("row_number window function is not registered");
        };
        let left_columns = left_plan.schema().columns();
        let right_columns = right_plan.schema().columns();
        let row_number_name = "__datafusion_set_operation_row_number";

        let with_row_number = |plan: LogicalPlan, columns: &[Column]| {
            let mut row_number_expr = WindowFunction::new(
                WindowFunctionDefinition::WindowUDF(Arc::clone(&row_number)),
                vec![],
            );
            row_number_expr.params.partition_by =
                columns.iter().cloned().map(Expr::Column).collect();
            LogicalPlanBuilder::from(plan)
                .window(vec![
                    Expr::WindowFunction(Box::new(row_number_expr))
                        .alias(row_number_name),
                ])?
                .build()
        };

        let left_plan = with_row_number(left_plan, &left_columns)?;
        let right_plan = with_row_number(right_plan, &right_columns)?;
        let left_builder = LogicalPlanBuilder::from(left_plan);
        let right_builder = LogicalPlanBuilder::from(right_plan);
        let (left_builder, right_builder, requalified) =
            datafusion_expr::logical_plan::builder::requalify_sides_if_needed(
                left_builder,
                right_builder,
            )?;
        let left_plan = left_builder.build()?;
        let right_plan = right_builder.build()?;

        let join_keys = left_plan
            .schema()
            .fields()
            .iter()
            .zip(right_plan.schema().fields().iter())
            .map(|(left_field, right_field)| {
                (
                    Column::from_name(left_field.name()),
                    Column::from_name(right_field.name()),
                )
            })
            .collect();
        let joined = LogicalPlanBuilder::from(left_plan.clone()).join_detailed(
            right_plan,
            if intersect {
                datafusion_expr::JoinType::Inner
            } else {
                datafusion_expr::JoinType::LeftAnti
            },
            join_keys,
            None,
            datafusion_common::NullEquality::NullEqualsNull,
        )?;

        let projection = left_columns
            .into_iter()
            .map(|column| {
                if requalified {
                    Expr::Column(Column::new(
                        Some(datafusion_common::TableReference::bare("left")),
                        column.name,
                    ))
                } else {
                    Expr::Column(column)
                }
            })
            .collect::<Vec<_>>();
        joined.project(projection)?.build()
    }
}
