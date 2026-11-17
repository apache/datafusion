// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Optimizer rule rewriting `SetComparison` subqueries (e.g. `= ANY`,
//! `> ALL`) into boolean expressions built from `EXISTS` subqueries
//! that capture SQL three-valued logic.

use crate::utils::{NamePreserver, merge_into_schema};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
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
        // Only a node whose own expressions contain a set comparison has anything to
        // rewrite. Return before merging its inputs' schemas, which is quadratic in the
        // number of inputs of a wide union.
        if !has_set_comparison(&plan)? {
            return Ok(Transformed::no(plan));
        }
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

/// Returns true if one of `plan`'s own expressions, not counting its inputs, contains a
/// set comparison.
fn has_set_comparison(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = expr.exists(|e| Ok(matches!(e, Expr::SetComparison(_))))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
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

#[cfg(test)]
mod tests {
    use datafusion_common::Spans;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_expr::logical_plan::Union;
    use datafusion_expr::{Operator, col};

    use super::*;
    use crate::OptimizerContext;
    use crate::test::test_table_scan_with_name;

    fn wide_union() -> Result<LogicalPlan> {
        let inputs = ["t1", "t2", "t3"]
            .into_iter()
            .map(|name| test_table_scan_with_name(name).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        Ok(LogicalPlan::Union(Union::try_new_with_loose_types(inputs)?))
    }

    #[test]
    fn skips_union() -> Result<()> {
        let rewritten = RewriteSetComparison::new()
            .rewrite(wide_union()?, &OptimizerContext::new())?;
        assert!(!rewritten.transformed);
        Ok(())
    }

    #[test]
    fn rewrites_set_comparison_above_union() -> Result<()> {
        let subquery_plan = LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
            .project(vec![col("sq.a")])?
            .build()?;
        let subquery = Subquery {
            subquery: Arc::new(subquery_plan),
            outer_ref_columns: vec![],
            spans: Spans::new(),
        };
        let predicate = Expr::SetComparison(SetComparison::new(
            Box::new(col("a")),
            subquery,
            Operator::Gt,
            SetQuantifier::All,
        ));
        let plan = LogicalPlanBuilder::from(wide_union()?)
            .filter(predicate)?
            .build()?;

        let rewritten =
            RewriteSetComparison::new().rewrite(plan, &OptimizerContext::new())?;
        assert!(rewritten.transformed);

        let mut remaining = false;
        rewritten.data.apply_with_subqueries(|node| {
            remaining |= has_set_comparison(node)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(!remaining);
        Ok(())
    }
}
