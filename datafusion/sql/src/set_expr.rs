// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::ast::{SetExpr, SetOperator, SetQuantifier};
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{
    DataFusionError, Diagnostic, Result, Span, not_impl_err, plan_err,
};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};

/// The SQL keyword of a set operator.
pub(crate) fn set_operator_name(op: SetOperator) -> &'static str {
    match op {
        SetOperator::Union => "UNION",
        SetOperator::Except => "EXCEPT",
        SetOperator::Intersect => "INTERSECT",
        SetOperator::Minus => "MINUS",
    }
}

/// The SQL keywords of a set quantifier, with `BY NAME` if given.
fn set_quantifier_name(quantifier: SetQuantifier, by_name: bool) -> &'static str {
    match (quantifier, by_name) {
        (SetQuantifier::None, false) => "",
        (SetQuantifier::All, false) => "ALL",
        (SetQuantifier::Distinct, false) => "DISTINCT",
        (SetQuantifier::None, true) => "BY NAME",
        (SetQuantifier::All, true) => "ALL BY NAME",
        (SetQuantifier::Distinct, true) => "DISTINCT BY NAME",
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn set_expr_to_plan(
        &self,
        set_expr: SetExpr<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        crate::stack::maybe_grow(|| {
            // Set expressions have no span in the new SQL parser.
            let set_expr_span: Option<Span> = None;
            match set_expr {
                SetExpr::Select(s) => {
                    self.select_to_plan(*s, vec![], None, planner_context)
                }
                SetExpr::Values(v) | SetExpr::RowValues(v) => {
                    self.sql_values_to_plan(v, planner_context)
                }
                SetExpr::SetOperation {
                    op,
                    left,
                    right,
                    quantifier,
                    by_name,
                } => {
                    let left_span: Option<Span> = None;
                    let right_span: Option<Span> = None;
                    let left_plan = self.set_expr_to_plan(*left, planner_context);
                    // Store the left plan's schema so that the right side can
                    // alias duplicate expressions to match. Skip for BY NAME
                    // operations since those match columns by name, not position.
                    if let Ok(plan) = &left_plan
                        && plan.schema().fields().len() > 1
                        && !by_name
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
                    // `DISTINCT BY NAME` is validated, as before the port.
                    if !(by_name && quantifier != SetQuantifier::Distinct) {
                        self.validate_set_expr_num_of_columns(
                            op,
                            left_span,
                            right_span,
                            &left_plan,
                            &right_plan,
                            set_expr_span,
                        )?;
                    }
                    self.set_operation_to_plan(
                        op, left_plan, right_plan, quantifier, by_name,
                    )
                }
                SetExpr::Query(q) => self.query_to_plan(*q, planner_context),
                other => not_impl_err!("Query {other} not implemented yet"),
            }
        })
    }

    pub(super) fn is_union_all(set_quantifier: SetQuantifier) -> Result<bool> {
        match set_quantifier {
            SetQuantifier::All => Ok(true),
            SetQuantifier::Distinct | SetQuantifier::None => Ok(false),
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
        let op = set_operator_name(op);
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
        by_name: bool,
    ) -> Result<LogicalPlan> {
        match (op, set_quantifier, by_name) {
            (SetOperator::Union, SetQuantifier::All, false) => {
                LogicalPlanBuilder::from(left_plan)
                    .union(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::All, true) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_by_name(right_plan)?
                    .build()
            }
            (
                SetOperator::Union,
                SetQuantifier::Distinct | SetQuantifier::None,
                false,
            ) => LogicalPlanBuilder::from(left_plan)
                .union_distinct(right_plan)?
                .build(),
            (SetOperator::Union, SetQuantifier::Distinct | SetQuantifier::None, true) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_by_name_distinct(right_plan)?
                    .build()
            }
            (SetOperator::Intersect, SetQuantifier::All, false) => {
                LogicalPlanBuilder::intersect(left_plan, right_plan, true)
            }
            (
                SetOperator::Intersect,
                SetQuantifier::Distinct | SetQuantifier::None,
                false,
            ) => LogicalPlanBuilder::intersect(left_plan, right_plan, false),
            (SetOperator::Except, SetQuantifier::All, false) => {
                LogicalPlanBuilder::except(left_plan, right_plan, true)
            }
            (
                SetOperator::Except,
                SetQuantifier::Distinct | SetQuantifier::None,
                false,
            ) => LogicalPlanBuilder::except(left_plan, right_plan, false),
            (op, quantifier, by_name) => {
                not_impl_err!(
                    "{} {} not implemented",
                    set_operator_name(op),
                    set_quantifier_name(quantifier, by_name)
                )
            }
        }
    }
}
