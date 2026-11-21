// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Replace joins on proven consecutive ranks with adjacent-row windows.
use crate::{
    common::{Column, ExprSchema, ScalarValue},
    functions_aggregate::average::Avg,
    functions_window::{
        expr_fn::{lag, lead},
        rank::Rank,
    },
    logical_expr::{
        Expr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder, Operator,
        WindowFunctionDefinition,
        expr::Sort,
        lit,
        logical_plan::{Projection, SubqueryAlias},
        utils::split_conjunction,
    },
};
use arrow::datatypes::DataType;
use std::{collections::BTreeSet, sync::Arc};

fn bare(expr: &Expr) -> &Expr {
    if let Expr::Alias(a) = expr {
        &a.expr
    } else {
        expr
    }
}

fn body(arm: &SubqueryAlias) -> &LogicalPlan {
    let mut plan = arm.input.as_ref();
    while let LogicalPlan::SubqueryAlias(a) = plan {
        plan = &a.input;
    }
    plan
}

struct Keys {
    rank: usize,
    partition: Vec<usize>,
    order: Vec<(usize, bool, bool)>,
}

// A GROUP BY produces one row per full key, including NULL groups. A rank
// has no peers if its partition and ordering retain that entire key.
fn ranked_keys(p: &Projection) -> Option<Keys> {
    let projection: Vec<_> = p
        .expr
        .iter()
        .map(|e| bare(e).try_as_col())
        .collect::<Option<_>>()?;
    let output = |c: &Column| projection.iter().position(|&k| k == c);
    let mut node = p.input.as_ref();
    let mut keys = None;
    let mut group_key = BTreeSet::new();
    while let LogicalPlan::Window(w) = node {
        let n = w.input.schema().fields().len();
        for (i, e) in w.window_expr.iter().enumerate() {
            let Expr::WindowFunction(f) = e else {
                return None;
            };
            let params = &f.params;
            if params.filter.is_some()
                || params.distinct
                || params.null_treatment.is_some()
            {
                return None;
            }
            match &f.fun {
                WindowFunctionDefinition::WindowUDF(fun)
                    if fun.inner().is::<Rank>()
                        && matches!(fun.name(), "rank" | "dense_rank") =>
                {
                    if keys.is_some()
                        || !params.args.is_empty()
                        || params.order_by.is_empty()
                    {
                        return None;
                    }
                    let mut index = |expr: &Expr| {
                        let c = expr.try_as_col()?;
                        let ty = w.input.schema().field_from_column(c).ok()?.data_type();
                        if !(ty.is_integer()
                            || matches!(
                                ty,
                                DataType::Utf8
                                    | DataType::Utf8View
                                    | DataType::LargeUtf8
                                    | DataType::Date32
                                    | DataType::Date64
                                    | DataType::Decimal128(..)
                            ))
                        {
                            return None;
                        }
                        group_key.insert(c.clone());
                        output(c)
                    };
                    keys = Some(Keys {
                        rank: output(&w.schema.qualified_field(n + i).into())?,
                        partition: params
                            .partition_by
                            .iter()
                            .map(&mut index)
                            .collect::<Option<_>>()?,
                        order: params
                            .order_by
                            .iter()
                            .map(|s| Some((index(&s.expr)?, s.asc, s.nulls_first)))
                            .collect::<Option<_>>()?,
                    });
                }
                WindowFunctionDefinition::AggregateUDF(fun)
                    if fun.inner().is::<Avg>()
                        && params.order_by.is_empty()
                        && params.window_frame.start_bound.is_unbounded()
                        && params.window_frame.end_bound.is_unbounded()
                        && params
                            .partition_by
                            .iter()
                            .all(|e| e.try_as_col().is_some())
                        && matches!(params.args.as_slice(), [Expr::Column(c)] if w.input.schema().field_from_column(c).is_ok_and(|f| matches!(f.data_type(), DataType::Decimal128(..)))) =>
                    {}
                _ => return None,
            }
        }
        node = &w.input;
    }
    let LogicalPlan::Aggregate(a) = node else {
        return None;
    };
    if a.group_expr.len() != group_key.len()
        || !a
            .group_expr
            .iter()
            .all(|e| e.try_as_col().is_some_and(|c| group_key.contains(c)))
    {
        return None;
    }
    keys
}

// Type coercion promotes UInt64 rank arithmetic with a signed literal to
// scale-zero decimals. Only unwrap casts that represent the full rank domain.
fn rank_column(e: &Expr) -> Option<&Column> {
    match e {
        Expr::Cast(c)
            if matches!(c.field.data_type(), DataType::Decimal128(20..=38, 0)) =>
        {
            c.expr.try_as_col()
        }
        _ => e.try_as_col(),
    }
}

fn rewrite(p: &Projection) -> Option<LogicalPlan> {
    let LogicalPlan::Filter(f) = p.input.as_ref() else {
        return None;
    };
    let LogicalPlan::Join(j) = f.input.as_ref() else {
        return None;
    };
    if !matches!(j.left.as_ref(), LogicalPlan::Join(_))
        && !matches!(j.right.as_ref(), LogicalPlan::Join(_))
    {
        return None;
    }
    let mut arms = vec![];
    if !product(&f.input, &mut arms, 0) || arms.len() != 3 {
        return None;
    }
    let common = body(arms[0]);
    let LogicalPlan::Projection(base) = common else {
        return None;
    };
    if !matches!(base.input.as_ref(), LogicalPlan::Window(_)) {
        return None;
    }
    let keys = ranked_keys(base)?;
    if !repeatable(common).ok()? {
        return None;
    }
    let expected_sources = sources(common).ok()?;
    for arm in &arms[1..] {
        if body(arm) != common || sources(body(arm)).ok()? != expected_sources {
            return None;
        }
    }
    let slot = |c: &Column| {
        let arm = arms
            .iter()
            .position(|a| c.relation.as_ref() == Some(&a.alias))?;
        Some((arm, arms[arm].schema.index_of_column(c).ok()?))
    };
    let mut edges = BTreeSet::new();
    let mut adjacent = [None; 2];
    for term in split_conjunction(&f.predicate) {
        let Expr::BinaryExpr(eq) = term else {
            return None;
        };
        if eq.op != Operator::Eq {
            return None;
        }
        if let (Some(l), Some(r)) = (eq.left.try_as_col(), eq.right.try_as_col()) {
            let (a, k) = slot(l)?;
            let (b, h) = slot(r)?;
            if a == b || k != h || !keys.partition.contains(&k) {
                return None;
            }
            edges.insert((k, a.min(b), a.max(b)));
            continue;
        }
        let (rank, offset) = if let Expr::BinaryExpr(b) = eq.right.as_ref() {
            (rank_column(&eq.left)?, b)
        } else if let Expr::BinaryExpr(b) = eq.left.as_ref() {
            (rank_column(&eq.right)?, b)
        } else {
            return None;
        };
        let side = match offset.op {
            Operator::Plus => 0,
            Operator::Minus => 1,
            _ => return None,
        };
        let literal = match offset.right.as_ref() {
            Expr::Cast(c)
                if matches!(c.field.data_type(), DataType::Decimal128(20..=38, 0)) =>
            {
                c.expr.as_ref()
            }
            other => other,
        };
        if !matches!(
            literal,
            Expr::Literal(ScalarValue::Decimal128(Some(1), 20, 0), _)
                | Expr::Literal(ScalarValue::Int64(Some(1)), _)
        ) {
            return None;
        }
        let (a, k) = slot(rank)?;
        let (b, h) = slot(rank_column(&offset.left)?)?;
        if k != keys.rank || h != k || a == b || adjacent[side].replace((a, b)).is_some()
        {
            return None;
        }
    }
    let [(center, prev), (same, next)] = [adjacent[0]?, adjacent[1]?];
    if center != same || prev == next {
        return None;
    }
    for k in &keys.partition {
        // Two distinct ordinary equalities connect all three aliases.
        if edges.iter().filter(|(col, _, _)| col == k).count() < 2 {
            return None;
        }
    }
    let mut layers = vec![];
    let mut aggregate = base.input.as_ref();
    let mut rank_layer = None;
    while let LogicalPlan::Window(w) = aggregate {
        if w.window_expr.iter().any(|e| matches!(e,Expr::WindowFunction(f) if matches!(&f.fun,WindowFunctionDefinition::WindowUDF(fun) if fun.inner().is::<Rank>()))) {
            rank_layer=Some(layers.len());
        }
        layers.push(w);
        aggregate = &w.input;
    }
    let rank_layer = rank_layer?;
    if base
        .input
        .schema()
        .fields()
        .iter()
        .any(|f| f.name().starts_with("__rank_neighbor_"))
    {
        return None;
    }
    let col = |i: usize| bare(&base.expr[i]).clone();
    let partition: Vec<_> = keys.partition.iter().map(|&i| col(i)).collect();
    let order: Vec<Sort> = keys
        .order
        .iter()
        .map(|&(i, asc, nulls)| col(i).sort(asc, nulls))
        .collect();
    let window = |e: Expr| {
        e.partition_by(partition.clone())
            .order_by(order.clone())
            .build()
            .ok()
    };
    let mut windows = vec![];
    let mut outputs = vec![];
    for (index, expr) in p.expr.iter().enumerate() {
        let (arm, i) = slot(bare(expr).try_as_col()?)?;
        let value = if arm == prev || arm == next {
            layers[rank_layer]
                .input
                .schema()
                .index_of_column(col(i).try_as_col()?)
                .ok()?;
            let shifted = if arm == prev {
                lag(col(i), Some(1), None)
            } else {
                lead(col(i), Some(1), None)
            };
            let name = format!("__rank_neighbor_{index}");
            windows.push(window(shifted)?.alias(name.clone()));
            crate::logical_expr::col(name)
        } else {
            col(i)
        };
        let (q, field) = p.schema.qualified_field(index);
        outputs.push(value.alias_qualified(q.cloned(), field.name()));
    }
    // NULL measures are valid neighbors. Check existence independently.
    let marker = "__rank_neighbor_exists";
    windows.push(window(lead(lit(1i64), Some(1), None))?.alias(marker));
    let first = "__rank_neighbor_previous";
    windows.push(window(lag(lit(1i64), Some(1), None))?.alias(first));
    let mut valid = crate::logical_expr::col(first)
        .is_not_null()
        .and(crate::logical_expr::col(marker).is_not_null());
    for k in partition {
        valid = valid.and(k.is_not_null());
    }
    // Attach the shifts to the existing rank ordering. Window columns are
    // resolved by name, so the other layers keep their original inputs.
    let mut input = LogicalPlanBuilder::from(aggregate.clone());
    for (i, layer) in layers.into_iter().enumerate().rev() {
        let mut expressions = layer.window_expr.clone();
        if i == rank_layer {
            expressions.append(&mut windows);
        }
        input = input.window(expressions).ok()?;
    }
    let input = input.filter(valid).ok()?.build().ok()?;
    Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(outputs, Arc::new(input), Arc::clone(&p.schema))
            .ok()?,
    ))
}

use crate::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use crate::common::{JoinType, NullEquality, Result};
use crate::optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};

fn sources(plan: &LogicalPlan) -> Result<Vec<usize>> {
    let mut result = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            result.push(Arc::as_ptr(&scan.source).cast::<()>() as usize);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(result)
}

fn repeatable(plan: &LogicalPlan) -> Result<bool> {
    let unsafe_input = plan.exists(|node| {
        let eligible = match node {
            LogicalPlan::Aggregate(agg) => agg.aggr_expr.iter().all(|expr| {
                let Expr::AggregateFunction(f) = expr else {
                    return false;
                };
                let [Expr::Column(c)] = f.params.args.as_slice() else {
                    return false;
                };
                f.func.inner().is::<crate::functions_aggregate::sum::Sum>()
                    && agg.input.schema().field_from_column(c).is_ok_and(|field| {
                        matches!(field.data_type(), DataType::Decimal128(..))
                    })
            }),
            LogicalPlan::TableScan(scan) => scan.fetch.is_none() && scan.skip.is_none(),
            LogicalPlan::Join(join) => join.join_type == JoinType::Inner,
            LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_) => true,
            _ => false,
        };
        Ok(!eligible
            || node.expressions().iter().any(|expr| {
                expr.is_volatile()
                    || expr
                        .exists(|e| {
                            Ok(matches!(
                                e,
                                Expr::ScalarSubquery(_)
                                    | Expr::Exists(_)
                                    | Expr::InSubquery(_)
                                    | Expr::OuterReferenceColumn(..)
                            ))
                        })
                        .unwrap_or(true)
            }))
    })?;
    Ok(!unsafe_input)
}

#[derive(Debug)]
pub(super) struct RankedNeighbors;

impl OptimizerRule for RankedNeighbors {
    fn name(&self) -> &str {
        "ranked_neighbors"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Projection(ref projection) = plan
            && let Some(rewritten) = rewrite(projection)
        {
            return Ok(Transformed::yes(rewritten));
        }
        Ok(Transformed::no(plan))
    }
}
fn product<'a>(
    p: &'a LogicalPlan,
    arms: &mut Vec<&'a SubqueryAlias>,
    depth: usize,
) -> bool {
    if depth >= 12 || arms.len() >= 12 {
        return false;
    }
    match p {
        LogicalPlan::Join(j)
            if j.join_type == JoinType::Inner
                && j.on.is_empty()
                && j.filter.is_none()
                && !j.null_aware
                && j.null_equality == NullEquality::NullEqualsNothing =>
        {
            product(&j.left, arms, depth + 1) && product(&j.right, arms, depth + 1)
        }
        LogicalPlan::SubqueryAlias(a) => {
            arms.push(a);
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::SessionContext;

    #[tokio::test]
    async fn rewrite_only_when_order_and_partition_identify_a_group() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE e(k INT, t INT, v DECIMAL(9,2)) AS VALUES (1,1,NULL), (1,2,4), (1,2,5), (1,3,NULL), (NULL,1,1), (NULL,2,2), (NULL,3,3)").await?.collect().await?;
        let sql="WITH b AS (SELECT k,t,sum(v) AS total,rank() OVER (PARTITION BY k ORDER BY t) AS pos FROM e GROUP BY k,t)
           SELECT p.total AS previous,n.total AS following FROM b p,b c,b n
           WHERE c.k=p.k AND n.k=c.k AND c.pos=p.pos+1 AND c.pos=n.pos-1";
        for (query, eligible) in [
            (sql.to_owned(), true),
            (sql.replace("GROUP BY k,t", "GROUP BY k,t,v"), false),
        ] {
            let frame = ctx.sql(&query).await?;
            let plan = frame
                .clone()
                .into_optimized_plan()?
                .display_indent()
                .to_string();
            assert_eq!(plan.contains("__rank_neighbor_exists"), eligible, "{plan}");
            if eligible {
                let rows = frame.collect().await?;
                assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
                let row = rows.iter().find(|b| b.num_rows() == 1).unwrap();
                assert!(row.column(0).is_null(0) && row.column(1).is_null(0));
            }
        }
        Ok(())
    }
}
