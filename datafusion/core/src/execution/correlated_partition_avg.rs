// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Reuse the outer rows for a correlated aggregate over the same immutable inputs.
use crate::common::tree_node::TreeNode;
use crate::datasource::{TableProvider, listing::ListingTable};
use crate::{
    common::{Column, JoinType, Result, TableReference, tree_node::Transformed},
    datasource::source_as_provider,
    functions_aggregate::average::Avg,
    logical_expr::{
        Expr, ExprSchemable, JoinConstraint, LogicalPlan, LogicalPlanBuilder, Operator,
        expr::WindowFunction,
        expr_fn::binary_expr,
        logical_plan::{Filter, Projection, Subquery},
        utils::{conjunction, split_conjunction},
    },
    optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug)]
pub(crate) struct CorrelatedPartitionAvg;

// The rule runs after expression simplification and before scalar decorrelation.
// Require both the table name and the identity of its listing provider to match.
fn sources(
    plan: &LogicalPlan,
    tables: &mut BTreeMap<TableReference, Arc<dyn TableProvider>>,
) -> Option<()> {
    match plan {
        LogicalPlan::Join(j)
            if j.join_type == JoinType::Inner
                && j.join_constraint == JoinConstraint::On
                && j.on.is_empty()
                && j.filter.is_none()
                && !j.null_aware =>
        {
            sources(&j.left, tables)?;
            sources(&j.right, tables)
        }
        LogicalPlan::TableScan(s)
            if s.skip.is_none()
                && s.filters.is_empty()
                && s.fetch.is_none()
                && s.projection.is_none() =>
        {
            let provider = source_as_provider(&s.source).ok()?;
            provider.downcast_ref::<ListingTable>()?;
            tables
                .insert(s.table_name.clone(), provider)
                .is_none()
                .then_some(())
        }
        _ => None,
    }
}

fn condition(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(b) => {
            matches!(
                b.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) && matches!(&*b.left, Expr::Column(_) | Expr::Literal(..))
                && matches!(&*b.right, Expr::Column(_) | Expr::Literal(..))
        }
        Expr::Like(l) => {
            matches!(&*l.expr, Expr::Column(_))
                && l.escape_char.is_none()
                && matches!(&*l.pattern, Expr::Literal(value, _) if value.try_as_str().flatten().is_some_and(|s| s.len() <= 256 && !s.contains('\\')))
        }
        _ => false,
    }
}

// Retain the analyzer's decimal casts on both sides of the comparison.
fn comparison(term: &Expr) -> Option<(&Expr, Operator, &Subquery)> {
    let Expr::BinaryExpr(b) = term else {
        return None;
    };
    if !matches!(
        b.op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return None;
    }
    match (&*b.left, &*b.right) {
        (value, Expr::ScalarSubquery(q)) => Some((value, b.op, q)),
        (Expr::ScalarSubquery(q), value) => Some((value, b.op.swap()?, q)),
        _ => None,
    }
}

// The scalar projection may scale a decimal average by a literal. Rebuild
// only this deterministic arithmetic over the window result.
fn projected_value(expr: &Expr, output: &Column, value: &Expr) -> Option<Expr> {
    match expr {
        Expr::Column(c) if c == output => Some(value.clone()),
        Expr::Alias(a) => projected_value(&a.expr, output, value),
        Expr::Cast(c) => {
            let mut c = c.clone();
            c.expr = Box::new(projected_value(&c.expr, output, value)?);
            Some(Expr::Cast(c))
        }
        Expr::BinaryExpr(b) if b.op == Operator::Multiply => {
            let (l, r) = match (&*b.left, &*b.right) {
                (lit @ Expr::Literal(..), e) => {
                    (lit.clone(), projected_value(e, output, value)?)
                }
                (e, lit @ Expr::Literal(..)) => {
                    (projected_value(e, output, value)?, lit.clone())
                }
                _ => return None,
            };
            Some(binary_expr(l, b.op, r))
        }
        _ => None,
    }
}

fn rewrite(f: &Filter) -> Option<LogicalPlan> {
    if !matches!(f.input.as_ref(), LogicalPlan::Join(_))
        || !f.predicate.contains_scalar_subquery()
    {
        return None;
    }
    let mut outer = split_conjunction(&f.predicate);
    let pos = outer.iter().position(|t| comparison(t).is_some())?;
    let (operand, op, subquery) = comparison(outer.remove(pos))?;
    let input = match operand {
        Expr::Cast(c) => c.expr.as_ref(),
        e => e,
    };
    let Expr::Column(measure) = input else {
        return None;
    };
    let LogicalPlan::Projection(p) = subquery.subquery.as_ref() else {
        return None;
    };
    let LogicalPlan::Aggregate(a) = p.input.as_ref() else {
        return None;
    };
    let [projected] = p.expr.as_slice() else {
        return None;
    };
    let [Expr::AggregateFunction(agg)] = a.aggr_expr.as_slice() else {
        return None;
    };
    let LogicalPlan::Filter(inner) = a.input.as_ref() else {
        return None;
    };
    if !a.group_expr.is_empty()
        || !agg.func.inner().is::<Avg>()
        || agg.params.distinct
        || agg.params.filter.is_some()
        || agg.params.null_treatment.is_some()
        || !agg.params.order_by.is_empty()
        || !matches!(agg.params.args.as_slice(), [Expr::Column(c)] if c==measure)
        || !outer.iter().all(|e| condition(e))
    {
        return None;
    }
    let dtype = agg.params.args[0].get_type(inner.input.schema()).ok()?;
    if !matches!(dtype, arrow::datatypes::DataType::Decimal128(..)) {
        return None;
    }
    let mut shared = BTreeMap::new();
    let mut all = BTreeMap::new();
    sources(&inner.input, &mut shared)?;
    sources(&f.input, &mut all)?;
    if shared.len() >= all.len()
        || !shared.contains_key(measure.relation.as_ref()?)
        || !shared
            .iter()
            .all(|(name, t)| all.get(name).is_some_and(|other| Arc::ptr_eq(t, other)))
    {
        return None;
    }

    let mut unmatched = outer.clone();
    let mut partition = Vec::new();
    for term in split_conjunction(&inner.predicate) {
        let pair = match term {
            Expr::BinaryExpr(b) if b.op == Operator::Eq => match (&*b.left, &*b.right) {
                (Expr::OuterReferenceColumn(_, c), Expr::Column(d))
                | (Expr::Column(d), Expr::OuterReferenceColumn(_, c)) => Some((c, d)),
                _ => None,
            },
            _ => None,
        };
        let index = if let Some((c, d)) = pair {
            if shared.contains_key(c.relation.as_ref()?)
                || !shared.contains_key(d.relation.as_ref()?)
            {
                return None;
            }
            let key = Expr::Column(c.clone());
            if !key.get_type(f.input.schema()).ok()?.is_integer() {
                return None;
            }
            let inside = Expr::Column(d.clone());
            let forward = key.clone().eq(inside.clone());
            let reverse = inside.eq(key.clone());
            if !partition.contains(&key) {
                partition.push(key);
            }
            unmatched
                .iter()
                .position(|&term| term == &forward || term == &reverse)?
        } else {
            if !condition(term) {
                return None;
            }
            unmatched.iter().position(|e| **e == *term)?
        };
        unmatched.remove(index);
    }
    // An extra predicate on any shared relation could discard rows
    // contributing to the average. Remaining filters must refer only
    // to qualifying dimension rows.
    if partition.is_empty()
        || unmatched.is_empty()
        || !unmatched.iter().all(|e| {
            e.column_refs()
                .iter()
                .all(|c| c.relation.as_ref().is_some_and(|r| !shared.contains_key(r)))
        })
    {
        return None;
    }
    let [Expr::Column(key)] = partition.as_slice() else {
        return None;
    };
    let dimension = key.relation.as_ref()?;
    if all.len() != shared.len() + 1 {
        return None;
    }
    // Assign a separate partition to each qualifying dimension row. Duplicated
    // dimension keys must not duplicate the rows used in a decimal AVG state.
    let ordinal = "__correlated_dimension_row";
    let alias = "__correlated_partition_avg";
    if f.input
        .schema()
        .fields()
        .iter()
        .any(|f| f.name() == ordinal)
    {
        return None;
    }
    let retained = conjunction(unmatched.into_iter().cloned())?;
    let numbered = f
        .input
        .as_ref()
        .clone()
        .transform_up(|p| {
            if let LogicalPlan::TableScan(s) = &p
                && &s.table_name == dimension
            {
                let p = LogicalPlanBuilder::from(p)
                    .filter(retained.clone())?
                    .window([
                        crate::functions_window::expr_fn::row_number().alias(ordinal)
                    ])?
                    .build()?;
                return Ok(Transformed::yes(p));
            }
            Ok(Transformed::no(p.recompute_schema()?))
        })
        .ok()?
        .data;
    partition.push(Expr::Column(Column::new_unqualified(ordinal)));
    if f.input
        .schema()
        .fields()
        .iter()
        .any(|field| field.name() == alias)
    {
        return None;
    }
    let threshold = projected_value(
        projected,
        &a.schema.qualified_field(0).into(),
        &Expr::Column(Column::new_unqualified(alias)),
    )?;
    let mut window = WindowFunction::new(Arc::clone(&agg.func), agg.params.args.clone());
    window.params.partition_by = partition;
    let input = LogicalPlanBuilder::from(numbered)
        .filter(conjunction(outer.into_iter().cloned())?)
        .ok()?
        .window([Expr::WindowFunction(Box::new(window)).alias(alias)])
        .ok()?
        .filter(binary_expr(operand.clone(), op, threshold))
        .ok()?
        .build()
        .ok()?;
    Some(LogicalPlan::Projection(Projection::new_from_schema(
        Arc::new(input),
        Arc::clone(f.input.schema()),
    )))
}

impl OptimizerRule for CorrelatedPartitionAvg {
    fn name(&self) -> &str {
        "correlated_partition_avg"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Filter(f) = &plan
            && let Some(result) = rewrite(f)
        {
            return Ok(Transformed::yes(result));
        }
        Ok(Transformed::no(plan))
    }
}

#[cfg(all(test, feature = "sql", feature = "parquet"))]
mod tests {
    use crate::prelude::{ParquetReadOptions, SessionContext};
    use datafusion_common::{Result, assert_batches_eq};
    #[tokio::test]
    async fn correlated_average_preserves_dimension_duplicates() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = SessionContext::new();
        for (table, sql) in [
            (
                "f",
                "select k,cast(x as decimal(12,2)) x from (values (1,1),(1,9),(2,NULL),(2,7)) t(k,x)",
            ),
            (
                "d",
                "select * from (values (1,9),(1,9),(2,9),(3,8)) t(k,tag)",
            ),
        ] {
            let p = dir.path().join(format!("{table}.parquet"));
            ctx.sql(&format!(
                "copy ({sql}) to '{}' stored as parquet",
                p.display()
            ))
            .await?
            .collect()
            .await?;
            ctx.register_parquet(
                table,
                p.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
        let df=ctx.sql("select f.x from f,d where d.tag=9 and d.k=f.k and f.x > (select 1.3*avg(f.x) from f where f.k=d.k) order by f.x").await?;
        assert!(
            df.clone()
                .into_optimized_plan()?
                .display_indent()
                .to_string()
                .contains("__correlated_partition_avg")
        );
        assert_batches_eq!(
            [
                "+------+", "| x    |", "+------+", "| 9.00 |", "| 9.00 |", "+------+"
            ],
            &df.collect().await?
        );
        let df=ctx.sql("select f.x from f,d where d.tag=9 and d.k=f.k and f.x > (select 1.3*avg(f.x) from f where f.k=d.k and f.x<5)").await?;
        assert!(
            !df.into_optimized_plan()?
                .display_indent()
                .to_string()
                .contains("__correlated_partition_avg")
        );
        Ok(())
    }
}
