// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Reuse the grouped input when a filter compares a measure with its own extremum.
use crate::datasource::{listing::ListingTable, source_as_provider};
use datafusion_common::{
    Column, Result,
    tree_node::{Transformed, TreeNode},
};
use datafusion_expr::{
    Expr, ExprSchemable, Filter, LogicalPlan, LogicalPlanBuilder, Operator, Projection,
    expr::WindowFunction,
};
use datafusion_functions_aggregate::min_max::{Max, Min};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct GroupedExtremum;

impl OptimizerRule for GroupedExtremum {
    fn name(&self) -> &str {
        "grouped_extremum"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Filter(filter) = &plan
            && let Some(rewritten) = grouped_extremum(filter, config)
        {
            return Ok(Transformed::yes(rewritten));
        }
        Ok(Transformed::no(plan))
    }
}

// Logical scan equality alone does not establish that two providers describe
// the same data. Only admit simple repeatable operations on the same file table.
fn same_repeatable_input(mut left: &LogicalPlan, mut right: &LogicalPlan) -> bool {
    let mut seen_aggregate = false;
    loop {
        if left.expressions().iter().any(|expr| {
            expr.exists(|e| {
                Ok(match e {
                    Expr::Alias(_)
                    | Expr::Column(_)
                    | Expr::Literal(..)
                    | Expr::BinaryExpr(_)
                    | Expr::Cast(_) => false,
                    Expr::AggregateFunction(a) => {
                        !(a.func
                            .inner()
                            .is::<datafusion_functions_aggregate::sum::Sum>()
                            || a.func.inner().is::<Min>()
                            || a.func.inner().is::<Max>())
                    }
                    _ => true,
                })
            })
            .unwrap_or(true)
        }) {
            return false;
        }
        match (left, right) {
            (LogicalPlan::TableScan(l), LogicalPlan::TableScan(r))
                if l.skip.is_none() && l.fetch.is_none() =>
            {
                return source_as_provider(&l.source)
                    .ok()
                    .zip(source_as_provider(&r.source).ok())
                    .is_some_and(|(l, r)| l.is::<ListingTable>() && Arc::ptr_eq(&l, &r));
            }
            (LogicalPlan::Aggregate(l), LogicalPlan::Aggregate(r)) if !seen_aggregate => {
                seen_aggregate = true;
                left = &l.input;
                right = &r.input;
            }
            (LogicalPlan::Projection(l), LogicalPlan::Projection(r)) => {
                left = &l.input;
                right = &r.input;
            }
            (LogicalPlan::Filter(l), LogicalPlan::Filter(r)) => {
                left = &l.input;
                right = &r.input;
            }
            _ => return false,
        }
    }
}

fn grouped_extremum(f: &Filter, config: &dyn OptimizerConfig) -> Option<LogicalPlan> {
    let LogicalPlan::Aggregate(groups) = f.input.as_ref() else {
        return None;
    };
    let Expr::BinaryExpr(cmp) = &f.predicate else {
        return None;
    };
    if cmp.op != Operator::Eq {
        return None;
    }
    let ((Expr::Column(operand), Expr::ScalarSubquery(subquery))
    | (Expr::ScalarSubquery(subquery), Expr::Column(operand))) =
        (cmp.left.as_ref(), cmp.right.as_ref())
    else {
        return None;
    };
    let [key @ Expr::Column(_)] = groups.group_expr.as_slice() else {
        return None;
    };
    if !subquery.outer_ref_columns.is_empty()
        || groups.aggr_expr.len() != 1
        || groups.schema.index_of_column(operand).ok()? != 1
    {
        return None;
    }
    fn projected_column(expr: &Expr) -> Option<&Column> {
        match expr {
            Expr::Alias(a) => projected_column(&a.expr),
            Expr::Column(c) => Some(c),
            _ => None,
        }
    }
    fn through_projections(
        mut plan: &LogicalPlan,
        mut index: usize,
    ) -> Option<(&LogicalPlan, usize)> {
        loop {
            match plan {
                LogicalPlan::SubqueryAlias(a) => plan = &a.input,
                LogicalPlan::Projection(p)
                    if p.expr.iter().all(|e| projected_column(e).is_some()) =>
                {
                    index = p
                        .input
                        .schema()
                        .index_of_column(projected_column(p.expr.get(index)?)?)
                        .ok()?;
                    plan = &p.input;
                }
                _ => return Some((plan, index)),
            }
        }
    }
    let (LogicalPlan::Aggregate(summary), 0) =
        through_projections(&subquery.subquery, 0)?
    else {
        return None;
    };
    let [Expr::AggregateFunction(agg)] = summary.aggr_expr.as_slice() else {
        return None;
    };
    let [Expr::Column(measure)] = agg.params.args.as_slice() else {
        return None;
    };
    if !summary.group_expr.is_empty()
        || agg.params.distinct
        || agg.params.filter.is_some()
        || !agg.params.order_by.is_empty()
        || agg.params.null_treatment.is_some()
        || !(agg.func.inner().is::<Max>() || agg.func.inner().is::<Min>())
    {
        return None;
    }
    let index = summary.input.schema().index_of_column(measure).ok()?;
    let (producer, 1) = through_projections(&summary.input, index)? else {
        return None;
    };
    if producer != f.input.as_ref() || !same_repeatable_input(producer, &f.input) {
        return None;
    }
    // A floating-point reduction can depend on the partition merge order.
    // Compare only exact integer and decimal measures.
    let measure_type = groups.schema.field(1).data_type();
    if !(measure_type.is_integer() || measure_type.is_decimal())
        || !key.get_type(groups.input.schema()).ok()?.is_integer()
    {
        return None;
    }
    let name = config.alias_generator().next("__group_extremum");
    if groups
        .schema
        .fields()
        .iter()
        .any(|field| field.name() == &name)
    {
        return None;
    }
    let value = Expr::Column(operand.clone());
    let window = WindowFunction::new(Arc::clone(&agg.func), vec![value.clone()]);
    let selected = LogicalPlanBuilder::from(f.input.as_ref().clone())
        .window([Expr::WindowFunction(Box::new(window)).alias(name.clone())])
        .ok()?
        .filter(value.eq(Expr::Column(Column::new_unqualified(name))))
        .ok()?
        .build()
        .ok()?;
    Some(LogicalPlan::Projection(Projection::new_from_schema(
        Arc::new(selected),
        Arc::clone(&groups.schema),
    )))
}

#[cfg(all(test, feature = "parquet", feature = "sql"))]
mod tests {
    use super::*;
    use crate::prelude::{ParquetReadOptions, SessionContext};
    use datafusion_common::assert_batches_eq;

    #[tokio::test]
    async fn grouped_extremum_keeps_ties_and_ignores_nulls() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("items.parquet");
        let ctx = SessionContext::new();
        ctx.sql(&format!("COPY (SELECT * FROM (VALUES (1,10),(2,7),(2,3),(3,NULL),(4,1)) AS v(k,x)) TO '{}' STORED AS PARQUET", path.display())).await?.collect().await?;
        ctx.register_parquet(
            "items",
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
        ctx.sql("CREATE VIEW totals AS SELECT k, SUM(x) AS n FROM items GROUP BY k")
            .await?;
        let query = ctx
            .sql("SELECT k FROM totals WHERE n=(SELECT MAX(n) FROM totals) ORDER BY k")
            .await?;
        assert!(
            query
                .clone()
                .into_optimized_plan()?
                .display_indent()
                .to_string()
                .contains("WindowAggr")
        );
        assert_batches_eq!(
            ["+---+", "| k |", "+---+", "| 1 |", "| 2 |", "+---+"],
            &query.collect().await?
        );
        let different = ctx
            .sql("SELECT k FROM totals WHERE n=(SELECT MAX(n) FROM totals WHERE k=4)")
            .await?;
        assert!(
            !different
                .clone()
                .into_optimized_plan()?
                .display_indent()
                .to_string()
                .contains("__group_extremum")
        );
        assert_batches_eq!(
            ["+---+", "| k |", "+---+", "| 4 |", "+---+"],
            &different.collect().await?
        );
        Ok(())
    }
}
