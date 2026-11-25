// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Combine scalar reductions over a common filtered scan.
use crate::optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, Result};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Projection, TableScan};
use datafusion_expr::{ExprSchemable, Operator};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct FuseScalarAggregates;

fn reduction(expr: &Expr) -> Option<(&TableScan, AggregateFunction)> {
    let Expr::ScalarSubquery(sq) = expr else {
        return None;
    };
    if !sq.outer_ref_columns.is_empty() {
        return None;
    }
    let mut plan = sq.subquery.as_ref();
    if let LogicalPlan::Projection(p) = plan {
        let [column] = p.expr.as_slice() else {
            return None;
        };
        let column = if let Expr::Alias(a) = column {
            a.expr.as_ref()
        } else {
            column
        };
        if p.input.schema().fields().len() != 1
            || p.input
                .schema()
                .index_of_column(column.try_as_col()?)
                .ok()?
                != 0
        {
            return None;
        }
        plan = &p.input;
    }
    let LogicalPlan::Aggregate(a) = plan else {
        return None;
    };
    let [Expr::AggregateFunction(func)] = a.aggr_expr.as_slice() else {
        return None;
    };
    if !a.group_expr.is_empty()
        || a.aggr_expr[0].is_volatile()
        || func.params.filter.is_some()
        || func.params.null_treatment.is_some()
        || func.params.distinct
        || !func.params.order_by.is_empty()
        || !(func
            .func
            .inner()
            .is::<datafusion_functions_aggregate::average::Avg>()
            || func
                .func
                .inner()
                .is::<datafusion_functions_aggregate::count::Count>())
        || !func
            .params
            .args
            .iter()
            .all(|e| matches!(e, Expr::Column(_) | Expr::Literal(..)))
    {
        return None;
    }
    let LogicalPlan::Filter(f) = a.input.as_ref() else {
        return None;
    };
    let LogicalPlan::TableScan(scan) = f.input.as_ref() else {
        return None;
    };
    if scan.fetch.is_some() || scan.skip.is_some() || !scan.filters.is_empty() {
        return None;
    }
    // Replacing a scalar subquery inside CASE may evaluate its predicate on
    // rows that the original branch did not visit. Admit only total predicates.
    let unsafe_predicate = f
        .predicate
        .exists(|e| {
            let safe = match e {
                Expr::Column(_)
                | Expr::Literal(..)
                | Expr::Between(_)
                | Expr::IsNull(_)
                | Expr::IsNotNull(_) => true,
                Expr::Cast(c) => {
                    let to = c.field.data_type();
                    match c.expr.as_ref() {
                        Expr::Literal(value, _) => value.cast_to(to).is_ok(),
                        Expr::Column(_) => {
                            c.expr.get_type(f.input.schema()).is_ok_and(|from| {
                                from == *to
                                    || (from.is_integer()
                                        && to.is_integer()
                                        && from.primitive_width() < to.primitive_width()
                                        && (!from.is_signed_integer()
                                            || to.is_signed_integer()))
                            })
                        }
                        _ => false,
                    }
                }
                Expr::BinaryExpr(b) => matches!(
                    b.op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::And
                        | Operator::Or
                ),
                _ => false,
            };
            Ok(!safe)
        })
        .ok()?;
    if unsafe_predicate {
        return None;
    }
    let mut result = func.clone();
    result.params.filter = Some(Box::new(f.predicate.clone()));
    Some((scan, result))
}

impl OptimizerRule for FuseScalarAggregates {
    fn name(&self) -> &str {
        "fuse_scalar_aggregates"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(p) = &plan else {
            return Ok(Transformed::no(plan));
        };
        if !p.expr.iter().any(|e| {
            e.exists(|e| Ok(matches!(e, Expr::ScalarSubquery(_))))
                .unwrap_or(false)
        }) {
            return Ok(Transformed::no(plan));
        }
        let mut scan = None::<TableScan>;
        let mut aggregates = Vec::new();
        let mut predicates = Vec::new();
        let mut columns = Vec::new();
        let mut alias = None;
        for expr in &p.expr {
            let expr = expr
                .clone()
                .transform_up(|expr| {
                    let Some((source, func)) = reduction(&expr) else {
                        return Ok(Transformed::no(expr));
                    };
                    if let Some(base) = &scan {
                        if base != source || !Arc::ptr_eq(&base.source, &source.source) {
                            return Ok(Transformed::no(expr));
                        }
                    } else {
                        scan = Some(source.clone());
                    }
                    let relation = alias.get_or_insert_with(|| {
                        loop {
                            let alias =
                                config.alias_generator().next("__scalar_aggregates");
                            if !p
                                .input
                                .schema()
                                .iter()
                                .any(|(q, _)| q.is_some_and(|q| q.table() == alias))
                            {
                                break alias;
                            }
                        }
                    });
                    if let Some(filter) = func.params.filter.as_deref()
                        && !predicates.contains(filter)
                    {
                        predicates.push(filter.clone());
                    }
                    let name = format!("value_{}", aggregates.len());
                    aggregates.push(Expr::AggregateFunction(func).alias(&name));
                    Ok(Transformed::yes(Expr::Column(Column::new(
                        Some(relation.as_str()),
                        name,
                    ))))
                })
                .data()?;
            columns.push(expr);
        }
        if aggregates.len() < 2 {
            return Ok(Transformed::no(plan));
        }
        let (Some(scan), Some(alias), Some(predicate)) =
            (scan, alias, datafusion_expr::utils::disjunction(predicates))
        else {
            return Ok(Transformed::no(plan));
        };
        // Preserve pruning for rows that contribute to none of the reductions.
        let agg = LogicalPlanBuilder::from(LogicalPlan::TableScan(scan))
            .filter(predicate)?
            .aggregate(Vec::<Expr>::new(), aggregates)?
            .alias(alias)?
            .build()?;
        let joined = LogicalPlanBuilder::from(p.input.as_ref().clone())
            .cross_join(agg)?
            .build()?;
        let output = Projection::try_new_with_schema(
            columns,
            Arc::new(joined),
            Arc::clone(&p.schema),
        )?;
        Ok(Transformed::yes(LogicalPlan::Projection(output)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;

    #[tokio::test]
    async fn empty_and_nullable_scalar_reductions() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE t(k INT, x DECIMAL(9,2)) AS VALUES (1,NULL), (1,4), (2,9)")
            .await?
            .collect()
            .await?;
        let sql = "SELECT (SELECT count(*) FROM t WHERE k=3) AS n,
                          (SELECT avg(x) FROM t WHERE k=3) IS NULL AS empty,
                          CAST((SELECT avg(x) FROM t WHERE k=1) AS INT) AS mean";
        let frame = ctx.sql(sql).await?;
        let plan = frame
            .clone()
            .into_optimized_plan()?
            .display_indent()
            .to_string();
        assert!(plan.contains("__scalar_aggregates"), "{plan}");
        assert_eq!(plan.matches("TableScan: t").count(), 1, "{plan}");
        assert_batches_eq!(
            [
                "+---+-------+------+",
                "| n | empty | mean |",
                "+---+-------+------+",
                "| 0 | true  | 4    |",
                "+---+-------+------+",
            ],
            &frame.collect().await?
        );
        Ok(())
    }
}
