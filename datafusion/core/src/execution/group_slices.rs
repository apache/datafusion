// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Share repeated aggregations selected at consecutive integer keys.
use crate::{
    common::{
        Column, NullEquality, Result, ScalarValue,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    datasource::source_as_provider,
    functions_aggregate::expr_fn::{count, max},
    logical_expr::{
        Expr, ExprFunctionExt, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder,
        Operator, lit,
        logical_plan::{Distinct, Filter, Projection, SubqueryAlias},
        utils::{conjunction, split_conjunction},
    },
};
use arrow::datatypes::DataType;
use std::{collections::BTreeMap, sync::Arc};

use crate::common::tree_node::Transformed;
use crate::optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};
use datafusion_common::ExprSchema;

fn safe_expr(e: &Expr) -> bool {
    match e {
        Expr::Column(_) | Expr::Literal(..) => true,
        Expr::Alias(a) => safe_expr(&a.expr),
        Expr::BinaryExpr(b) => {
            matches!(b.op, Operator::Eq | Operator::And)
                && safe_expr(&b.left)
                && safe_expr(&b.right)
        }
        _ => false,
    }
}

// Net measures such as quantity - COALESCE(return_quantity, 0) are
// evaluated inside the original producer, with the original types. Only
// deterministic exact arithmetic is repeatable across the selected periods.
fn repeatable_measure(expr: &Expr, schema: &datafusion_common::DFSchema) -> bool {
    match expr {
        Expr::Alias(a) => repeatable_measure(&a.expr, schema),
        Expr::BinaryExpr(b) if b.op == Operator::Minus => {
            matches!(b.left.as_ref(), Expr::Column(_))
                && expr
                    .get_type(schema)
                    .is_ok_and(|t| t.is_integer() || t.is_decimal())
                && repeatable_measure(&b.right, schema)
        }
        Expr::Case(case) if case.expr.is_none() => {
            let [(when, value)] = case.when_then_expr.as_slice() else {
                return false;
            };
            matches!(when.as_ref(), Expr::IsNotNull(test) if test == value)
                && repeatable_measure(value, schema)
                && matches!(case.else_expr.as_deref(), Some(Expr::Literal(_, _)))
        }
        Expr::Cast(cast) => {
            let Ok(DataType::Decimal128(from_precision, from_scale)) =
                cast.expr.get_type(schema)
            else {
                return false;
            };
            let DataType::Decimal128(to_precision, to_scale) = cast.field.data_type()
            else {
                return false;
            };
            matches!(cast.expr.as_ref(), Expr::Column(_))
                && *to_scale >= from_scale
                && i16::from(*to_precision) - i16::from(*to_scale)
                    >= i16::from(from_precision) - i16::from(from_scale)
        }
        other => safe_expr(other),
    }
}

fn signed_integer(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(v) => *v,
        ScalarValue::Int32(v) => v.map(i64::from),
        ScalarValue::Int16(v) => v.map(i64::from),
        ScalarValue::Int8(v) => v.map(i64::from),
        _ => None,
    }
}
fn sources(plan: &LogicalPlan) -> Result<Vec<usize>> {
    let mut values = vec![];
    plan.apply(|plan| {
        if let LogicalPlan::TableScan(s) = plan {
            let p = source_as_provider(&s.source)?;
            values.push(Arc::as_ptr(&p).cast::<()>() as usize);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(values)
}
fn repeatable(plan: &LogicalPlan) -> Result<bool> {
    let mut ok = true;
    plan.apply(|p| {
        ok = match p {
            LogicalPlan::TableScan(s) => {
                s.skip.is_none()
                    && s.fetch.is_none()
                    && s.filters.is_empty()
                    && source_as_provider(&s.source)
                        .is_ok_and(|v| v.is::<crate::datasource::listing::ListingTable>())
            }
            LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Distinct(Distinct::All(_))
            | LogicalPlan::Union(_) => true,
            LogicalPlan::Projection(p) => p
                .expr
                .iter()
                .all(|e| repeatable_measure(e, p.input.schema())),
            LogicalPlan::Aggregate(a) => {
                a.group_expr.iter().all(|e| matches!(e, Expr::Column(_)))
                    && a.aggr_expr.iter().all(|e| {
                        let Expr::AggregateFunction(ag) = e else {
                            return false;
                        };
                        let [Expr::Column(column)] = ag.params.args.as_slice() else {
                            return false;
                        };
                        ag.func.inner().is::<crate::functions_aggregate::sum::Sum>()
                            && a.input.schema().field_from_column(column).is_ok_and(
                                |field| {
                                    field.data_type().is_decimal()
                                        || field.data_type().is_integer()
                                },
                            )
                            && ag.params.filter.is_none()
                            && ag.params.order_by.is_empty()
                            && !ag.params.distinct
                    })
            }
            LogicalPlan::Filter(f) => safe_expr(&f.predicate),
            LogicalPlan::Join(j) => {
                matches!(j.join_type, JoinType::Inner | JoinType::Left)
                    && j.on.iter().all(|(a, b)| {
                        matches!((a, b), (Expr::Column(_), Expr::Column(_)))
                    })
                    && j.filter.as_ref().is_none_or(safe_expr)
            }
            _ => false,
        };
        Ok(if ok {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })?;
    Ok(ok)
}

// Trace every output through pure renames to a group or aggregate. If a
// grouping column is hidden, the remaining columns need not identify a row.
fn group_columns(plan: &LogicalPlan) -> Option<(Vec<usize>, usize)> {
    let mut indices: Vec<_> = (0..plan.schema().fields().len()).collect();
    let mut node = plan;
    loop {
        match node {
            LogicalPlan::SubqueryAlias(a) => node = &a.input,
            LogicalPlan::Projection(p) => {
                for i in &mut indices {
                    let e = p.expr.get(*i)?;
                    let e = if let Expr::Alias(a) = e {
                        a.expr.as_ref()
                    } else {
                        e
                    };
                    *i = p.input.schema().index_of_column(e.try_as_col()?).ok()?;
                }
                node = &p.input;
            }
            LogicalPlan::Aggregate(a)
                if a.group_expr.iter().all(|e| matches!(e, Expr::Column(_))) =>
            {
                let n = a.group_expr.len();
                return ((0..n)
                    .all(|k| indices.iter().filter(|i| **i == k).count() == 1))
                .then_some((indices, n));
            }
            _ => return None,
        }
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

fn binding<'a>(column: &Column, terms: &[&'a Expr]) -> Option<&'a Expr> {
    terms.iter().find_map(|term| {
        let Expr::BinaryExpr(b) = term else {
            return None;
        };
        if b.op != Operator::Eq {
            return None;
        }
        for (key, value) in [(&b.left, &b.right), (&b.right, &b.left)] {
            if key.try_as_col() == Some(column)
                && matches!(value.as_ref(), Expr::Literal(v,None) if signed_integer(v).is_some())
            {
                return Some(value.as_ref());
            }
        }
        None
    })
}

fn joined(columns: &[Column], terms: &[&Expr]) -> bool {
    // Follow ordinary equality even when written as a chain or in reverse.
    let mut ids: Vec<usize> = (0..columns.len()).collect();
    for term in terms {
        let Expr::BinaryExpr(b) = term else { continue };
        if b.op != Operator::Eq {
            continue;
        }
        if let (Some(l), Some(r)) = (b.left.try_as_col(), b.right.try_as_col())
            && let (Some(i), Some(j)) = (
                columns.iter().position(|c| c == l),
                columns.iter().position(|c| c == r),
            )
        {
            let (from, to) = (ids[i], ids[j]);
            for id in &mut ids {
                if *id == from {
                    *id = to
                }
            }
        }
    }
    ids.iter().all(|&i| i == ids[0])
}

fn fold(arms: &[&SubqueryAlias], terms: &[&Expr]) -> Result<Option<LogicalPlan>> {
    let base = &arms[0].input;
    let Some((mapping, n)) = group_columns(base) else {
        return Ok(None);
    };
    let schema = base.schema();
    // Shadowed CTE names may have entirely different columns and sources.
    if arms.iter().skip(1).any(|arm| arm.input != *base) {
        return Ok(None);
    }
    if n < 2
        || mapping.len() > 32
        || schema
            .fields()
            .iter()
            .any(|f| !f.metadata().is_empty() || f.name().starts_with("__group_slice_"))
    {
        return Ok(None);
    }
    let cols: Vec<_> = schema.columns().into_iter().map(Expr::Column).collect();
    let mut keys = Vec::new();
    let mut constants = BTreeMap::new();
    let mut predicates = Vec::new();
    let mut slice = None;
    for (i, &idx) in mapping.iter().enumerate() {
        if idx >= n {
            let ty = schema.field(i).data_type();
            if !ty.is_integer() && !matches!(ty, DataType::Decimal128(..)) {
                return Ok(None);
            }
            continue;
        }
        let aliases: Vec<Column> = arms
            .iter()
            .map(|a| a.schema.qualified_field(i).into())
            .collect();
        let values: Option<Vec<_>> = aliases.iter().map(|c| binding(c, terms)).collect();
        if let Some(values) = values {
            let mut ints = Vec::new();
            for value in &values {
                let Expr::Literal(v, None) = value else {
                    return Ok(None);
                };
                if &v.data_type() != schema.field(i).data_type() {
                    return Ok(None);
                }
                ints.push(signed_integer(v).unwrap());
            }
            let mut ordered = ints.clone();
            ordered.sort_unstable();
            if ordered[0] == *ordered.last().unwrap() {
                constants.insert(i, values[0].clone());
                predicates.push(cols[i].clone().eq(values[0].clone()));
            } else if slice.is_none()
                && ordered
                    .windows(2)
                    .all(|w| w[0].checked_add(1) == Some(w[1]))
            {
                let lo = ints.iter().position(|v| v == &ordered[0]).unwrap();
                let hi = ints.iter().position(|v| Some(v) == ordered.last()).unwrap();
                predicates.push(
                    cols[i]
                        .clone()
                        .between(values[lo].clone(), values[hi].clone()),
                );
                slice = Some((i, values));
            } else {
                return Ok(None);
            }
        } else if (schema.field(i).data_type().is_integer()
            || matches!(
                schema.field(i).data_type(),
                DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
                    | DataType::Decimal128(..)
            ))
            && joined(&aliases, terms)
        {
            keys.push(cols[i].clone());
            predicates.push(cols[i].clone().is_not_null());
        } else {
            return Ok(None);
        }
    }
    let Some((slice, values)) = slice else {
        return Ok(None);
    };

    if keys.is_empty() || !repeatable(base)? {
        return Ok(None);
    }
    let source = sources(base)?;
    for arm in &arms[1..] {
        if sources(&arm.input)? != source {
            return Ok(None);
        }
    }
    let mut aggs = vec![count(lit(1)).alias("__group_slice_count")];
    let mut outputs = Vec::new();
    for (a, (arm, value)) in arms.iter().zip(values).enumerate() {
        for (i, &idx) in mapping.iter().enumerate() {
            let expr = if i == slice {
                value.clone()
            } else if let Some(c) = constants.get(&i) {
                c.clone()
            } else if idx < n {
                cols[i].clone()
            } else {
                let name = format!("__group_slice_{a}_{i}");
                aggs.push(
                    max(cols[i].clone())
                        .filter(cols[slice].clone().eq(value.clone()))
                        .build()?
                        .alias(&name),
                );
                crate::logical_expr::col(name)
            };
            outputs.push(
                expr.alias_qualified(Some(arm.alias.clone()), schema.field(i).name()),
            );
        }
    }
    let grouped = LogicalPlanBuilder::from(base.as_ref().clone())
        .filter(conjunction(predicates).unwrap())?
        .aggregate(keys, aggs)?
        .filter(
            crate::logical_expr::col("__group_slice_count").eq(lit(arms.len() as i64)),
        )?
        .build()?;
    let schema = arms[1..]
        .iter()
        .try_fold(arms[0].schema.as_ref().clone(), |schema, arm| {
            schema.join(&arm.schema)
        })?;
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(outputs, Arc::new(grouped), Arc::new(schema))?,
    )))
}

fn combine_group_slices(f: &Filter) -> Result<Option<LogicalPlan>> {
    let mut arms = Vec::new();
    if !product(&f.input, &mut arms, 0) || arms.len() < 2 {
        return Ok(None);
    }
    // Only group occurrences of the same named subquery. Source identity and
    // the complete logical body are checked before any rewrite is accepted.
    let mut sets: BTreeMap<_, Vec<usize>> = BTreeMap::new();
    if arms.len() == 2 {
        sets.insert(&arms[0].alias, vec![0, 1]);
    } else {
        for (i, arm) in arms.iter().enumerate() {
            if let LogicalPlan::SubqueryAlias(a) = arm.input.as_ref() {
                sets.entry(&a.alias).or_default().push(i);
            }
        }
        sets.retain(|_, indices| indices.len() >= 3);
    }
    if sets.is_empty()
        || f.predicate.is_volatile()
        || f.predicate.contains_outer()
        || f.predicate.exists(|e| {
            Ok(matches!(
                e,
                Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
            ))
        })?
    {
        return Ok(None);
    }
    let terms = split_conjunction(&f.predicate);
    let mut replacements = BTreeMap::new();
    let mut consumed = vec![false; arms.len()];
    for indices in sets.values() {
        let members: Vec<_> = indices.iter().map(|&i| arms[i]).collect();
        if let Some(p) = fold(&members, &terms)? {
            replacements.insert(indices[0], p);
            for &i in &indices[1..] {
                consumed[i] = true;
            }
        }
    }
    if replacements.is_empty() {
        return Ok(None);
    }
    let mut product = None;
    for (i, a) in arms.into_iter().enumerate() {
        if consumed[i] {
            continue;
        }
        let next = replacements
            .remove(&i)
            .unwrap_or_else(|| LogicalPlan::SubqueryAlias(a.clone()));
        product = Some(match product {
            None => next,
            Some(parent) => LogicalPlanBuilder::from(parent).cross_join(next)?.build()?,
        });
    }
    let input = product.unwrap();
    let input = if input.schema() == f.input.schema() {
        input
    } else {
        LogicalPlan::Projection(Projection::try_new_with_schema(
            f.input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect(),
            Arc::new(input),
            Arc::clone(f.input.schema()),
        )?)
    };
    Ok(Some(LogicalPlan::Filter(Filter::try_new(
        f.predicate.clone(),
        Arc::new(input),
    )?)))
}

#[derive(Debug)]
pub(super) struct CombineGroupSlices;
impl OptimizerRule for CombineGroupSlices {
    fn name(&self) -> &str {
        "combine_group_slices"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Filter(f) = &plan
            && let Some(result) = combine_group_slices(f)?
        {
            return Ok(Transformed::yes(result));
        }
        Ok(Transformed::no(plan))
    }
}

#[cfg(all(test, feature = "parquet", feature = "sql"))]
mod tests {
    use crate::prelude::{ParquetReadOptions, SessionContext};
    use datafusion_common::{Result, assert_batches_eq};
    #[tokio::test]
    async fn slices_preserve_missing_groups_and_null_measures() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("periods.parquet");
        let ctx = SessionContext::new();
        ctx.sql(&format!("COPY (SELECT k,p,CAST(v AS DECIMAL(10,0)) AS v FROM (VALUES (1,1,4),(1,1,6),(1,2,NULL),(2,1,9),(NULL,1,3),(NULL,2,4)) t(k,p,v)) TO '{}' STORED AS PARQUET",file.display())).await?.collect().await?;
        ctx.register_parquet("t", file.to_str().unwrap(), ParquetReadOptions::default())
            .await?;
        let df=ctx.sql("WITH g AS (SELECT k,p,SUM(v) AS v FROM t GROUP BY k,p) SELECT a.k,a.v AS v1,b.v AS v2 FROM g a,g b WHERE a.p=1 AND b.p=2 AND a.k=b.k").await?;
        assert!(
            df.clone()
                .into_optimized_plan()?
                .display_indent()
                .to_string()
                .contains("__group_slice_")
        );
        assert_batches_eq!(
            [
                "+---+----+----+",
                "| k | v1 | v2 |",
                "+---+----+----+",
                "| 1 | 10 |    |",
                "+---+----+----+"
            ],
            &df.collect().await?
        );
        Ok(())
    }
    #[tokio::test]
    async fn union_slices_deduplicate_before_summing() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("sales.parquet");
        let ctx = SessionContext::new();
        ctx.sql(&format!(
            "COPY (SELECT * FROM (VALUES (1,2022,10,2),(1,2023,9,NULL),
             (1,2023,7,1),(2,2022,4,0),(3,2022,NULL,0),(3,2023,5,2))
             t(k,yr,qty,returned)) TO '{}' STORED AS PARQUET",
            file.display()
        ))
        .await?
        .collect()
        .await?;
        for name in ["sales", "archive"] {
            ctx.register_parquet(
                name,
                file.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
        let query = "WITH detail AS (
            SELECT k,yr,qty-COALESCE(returned,0) AS net FROM sales
            UNION SELECT k,yr,qty-COALESCE(returned,0) AS net FROM archive
        ), g AS (SELECT k,yr,SUM(net) AS net FROM detail GROUP BY k,yr)
        SELECT a.k,a.net AS prior,b.net AS current FROM g a,g b
        WHERE a.k=b.k AND a.yr=2023-1 AND b.yr=2023 ORDER BY a.k";
        let df = ctx.sql(query).await?;
        assert!(
            df.clone()
                .into_optimized_plan()?
                .display_indent()
                .to_string()
                .contains("__group_slice_")
        );
        assert_batches_eq!(
            [
                "+---+-------+---------+",
                "| k | prior | current |",
                "+---+-------+---------+",
                "| 1 | 8     | 15      |",
                "| 3 |       | 3       |",
                "+---+-------+---------+"
            ],
            &df.collect().await?
        );
        Ok(())
    }
}
