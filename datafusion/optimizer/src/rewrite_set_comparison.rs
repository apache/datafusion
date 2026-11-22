// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Optimizer rule rewriting `SetComparison` subqueries (e.g. `= ANY`,
//! `> ALL`) into boolean expressions built from `EXISTS` subqueries
//! that capture SQL three-valued logic.

use crate::utils::{NamePreserver, merge_into_schema};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, ExprSchema, Result, ScalarValue, plan_err};
use datafusion_expr::expr::{self, Exists, SetComparison, SetQuantifier};
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::{Expr, LogicalPlan, lit};
use std::sync::Arc;

use datafusion_expr::utils::merge_schema;

/// Rewrite `SetComparison` expressions to scalar subqueries that return the
/// correct boolean value (including SQL NULL semantics). After this rule
/// runs, later rules such as `ScalarSubqueryToJoin` can decorrelate and
/// remove the remaining subquery.
#[derive(Debug, Default)]
pub struct RewriteSetComparison;

impl RewriteSetComparison {
    /// Create a new `RewriteSetComparison` optimizer rule.
    pub fn new() -> Self {
        Self
    }

    fn rewrite_plan(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut schema = merge_schema(&plan.inputs());
        if let Some(merge_schema) = merge_into_schema(&plan)? {
            schema = merge_schema;
        }
        // This rule replaces `x > ALL (…)` with a `CASE WHEN EXISTS …`, whose display
        // name is not the name of the field it produces. Rewriting an expression
        // without preserving its name therefore changes the output schema, and the
        // optimizer's "the schema is invariant under optimization" check fails with an
        // internal error rather than returning the answer. `simplify_exprs` and
        // `type_coercion` preserve names around `map_expressions` for the same reason;
        // doing it here also stops this rule leaving mismatched names in `Aggregate`
        // and `Window` nodes, which a projection-only fix could not reach.
        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let original_name = name_preserver.save(&expr);
            expr.transform_up(|expr| rewrite_set_comparison(expr, &schema))
                .map(|t| t.update_data(|expr| original_name.restore(expr)))
        })
    }
}

impl OptimizerRule for RewriteSetComparison {
    fn name(&self) -> &str {
        "rewrite_set_comparison"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|plan| self.rewrite_plan(plan))
    }
}

fn rewrite_set_comparison(
    expr: Expr,
    outer_schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    match expr {
        Expr::SetComparison(set_comparison) => {
            let rewritten = build_set_comparison_subquery(set_comparison, outer_schema)?;
            Ok(Transformed::yes(rewritten))
        }
        _ => Ok(Transformed::no(expr)),
    }
}

fn build_set_comparison_subquery(
    set_comparison: SetComparison,
    outer_schema: &DFSchema,
) -> Result<Expr> {
    let SetComparison {
        expr,
        subquery,
        op,
        quantifier,
    } = set_comparison;

    let left_expr = to_outer_reference(*expr, outer_schema)?;
    let subquery_schema = subquery.subquery.schema();
    if subquery_schema.fields().is_empty() {
        return plan_err!("single expression required.");
    }
    // avoid `head_output_expr` for aggr/window plan, it will gives group-by expr if exists
    let right_expr = Expr::Column(Column::from(subquery_schema.qualified_field(0)));

    let comparison = Expr::BinaryExpr(expr::BinaryExpr::new(
        Box::new(left_expr),
        op,
        Box::new(right_expr),
    ));

    let true_exists =
        exists_subquery(&subquery, Expr::IsTrue(Box::new(comparison.clone())))?;
    let null_exists =
        exists_subquery(&subquery, Expr::IsNull(Box::new(comparison.clone())))?;

    let result_expr = match quantifier {
        SetQuantifier::Any => Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![
                (Box::new(true_exists), Box::new(lit(true))),
                (
                    Box::new(null_exists),
                    Box::new(Expr::Literal(ScalarValue::Boolean(None), None)),
                ),
            ],
            else_expr: Some(Box::new(lit(false))),
        }),
        SetQuantifier::All => {
            let false_exists =
                exists_subquery(&subquery, Expr::IsFalse(Box::new(comparison.clone())))?;
            Expr::Case(expr::Case {
                expr: None,
                when_then_expr: vec![
                    (Box::new(false_exists), Box::new(lit(false))),
                    (
                        Box::new(null_exists),
                        Box::new(Expr::Literal(ScalarValue::Boolean(None), None)),
                    ),
                ],
                else_expr: Some(Box::new(lit(true))),
            })
        }
    };

    Ok(result_expr)
}

fn exists_subquery(subquery: &Subquery, filter: Expr) -> Result<Expr> {
    let plan = LogicalPlanBuilder::from(subquery.subquery.as_ref().clone())
        .filter(filter)?
        .build()?;
    let outer_ref_columns = plan.all_out_ref_exprs();
    Ok(Expr::Exists(Exists {
        subquery: Subquery {
            subquery: Arc::new(plan),
            outer_ref_columns,
            spans: subquery.spans.clone(),
        },
        negated: false,
    }))
}

fn to_outer_reference(expr: Expr, outer_schema: &DFSchema) -> Result<Expr> {
    expr.transform_up(|expr| match expr {
        Expr::Column(col) => {
            let field = outer_schema.field_from_column(&col)?;
            Ok(Transformed::yes(Expr::OuterReferenceColumn(
                Arc::clone(field),
                col,
            )))
        }
        Expr::OuterReferenceColumn(_, _) => Ok(Transformed::no(expr)),
        _ => Ok(Transformed::no(expr)),
    })
    .map(|t| t.data)
}
