// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Derive probe and build filters from exact bounds on inner equijoin keys.
use std::collections::HashMap;
use std::sync::Arc;

use crate::optimizer::PhysicalOptimizerContext;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{JoinType, NullEquality, Result, ScalarValue, stats::Precision};
use datafusion_expr::Operator;
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{BinaryExpr, Column, Literal},
    utils::conjunction_opt,
};
use datafusion_physical_plan::{
    ExecutionPlan, filter::FilterExec, joins::HashJoinExec, statistics::StatisticsArgs,
};

type Plan = Arc<dyn ExecutionPlan>;
type Bounds = (ScalarValue, ScalarValue);

fn numeric(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
        _ => None,
    }
}

// A one-key difference on a million-key dimension is a poor reason to add
// a row-level filter. Use domain width only for costing, never for correctness.
fn narrows(own: &Bounds, other: &Bounds) -> Option<bool> {
    let (lo, hi) = (numeric(&own.0)?, numeric(&own.1)?);
    let kept = (hi.min(numeric(&other.1)?) - lo.max(numeric(&other.0)?) + 1).max(0);
    Some(4 * kept <= 3 * (hi - lo + 1))
}

fn inner(plan: &Plan) -> Option<&HashJoinExec> {
    let join = plan.downcast_ref::<HashJoinExec>()?;
    (*join.join_type() == JoinType::Inner
        && join.null_equality() == NullEquality::NullEqualsNothing
        && !join.contains_projection())
    .then_some(join)
}

// Descend through an inner join and intersect the known bounds of keys
// explicitly equated at that join. Other operators use their exact statistics.
fn bounds(
    plan: &Plan,
    index: usize,
    context: &dyn PhysicalOptimizerContext,
    depth: usize,
    memo: &mut HashMap<(usize, usize, usize), Option<Bounds>>,
) -> Result<Option<Bounds>> {
    // The input tree remains alive throughout this join's analysis.
    let key = (Arc::as_ptr(plan).cast::<()>() as usize, index, depth);
    if let Some(range) = memo.get(&key) {
        return Ok(range.clone());
    }
    let range = compute_bounds(plan, index, context, depth, memo)?;
    memo.insert(key, range.clone());
    Ok(range)
}

fn compute_bounds(
    plan: &Plan,
    index: usize,
    context: &dyn PhysicalOptimizerContext,
    depth: usize,
    memo: &mut HashMap<(usize, usize, usize), Option<Bounds>>,
) -> Result<Option<Bounds>> {
    if depth > 12 {
        return Ok(None);
    }
    if let Some(join) = inner(plan) {
        let width = join.left().schema().fields().len();
        let (side, other, index, left) = if index < width {
            (join.left(), join.right(), index, true)
        } else {
            (join.right(), join.left(), index - width, false)
        };
        let mut range = bounds(side, index, context, depth + 1, memo)?;
        for (a, b) in join.on() {
            let (here, there) = if left { (a, b) } else { (b, a) };
            if here
                .downcast_ref::<Column>()
                .is_some_and(|c| c.index() == index)
                && let Some(c) = there.downcast_ref::<Column>()
                && let Some((lo, hi)) =
                    bounds(other, c.index(), context, depth + 1, memo)?
            {
                range = Some(match range {
                    Some((l, h)) => {
                        (if lo > l { lo } else { l }, if hi < h { hi } else { h })
                    }
                    None => (lo, hi),
                });
            }
        }
        return Ok(range);
    }
    if !plan.schema().field(index).data_type().is_integer() {
        return Ok(None);
    }
    let stats = context.compute_statistics(plan, &StatisticsArgs::new())?;
    let Some(col) = stats.column_statistics.get(index) else {
        return Ok(None);
    };
    match (&col.min_value, &col.max_value) {
        (Precision::Exact(lo), Precision::Exact(hi))
            if !lo.is_null() && !hi.is_null() =>
        {
            Ok(Some((lo.clone(), hi.clone())))
        }
        _ => Ok(None),
    }
}

pub(crate) fn restrict(
    plan: Plan,
    context: &dyn PhysicalOptimizerContext,
) -> Result<Transformed<Plan>> {
    let Some(join) = inner(&plan) else {
        return Ok(Transformed::no(plan));
    };
    let mut memo = HashMap::new();
    let mut left = Vec::<Arc<dyn PhysicalExpr>>::new();
    let mut right = Vec::<Arc<dyn PhysicalExpr>>::new();
    for (l, r) in join.on() {
        let (Some(lc), Some(rc)) =
            (l.downcast_ref::<Column>(), r.downcast_ref::<Column>())
        else {
            continue;
        };
        if join.left().schema().field(lc.index()).data_type()
            != join.right().schema().field(rc.index()).data_type()
        {
            continue;
        }
        let (Some(a), Some(b)) = (
            bounds(join.left(), lc.index(), context, 0, &mut memo)?,
            bounds(join.right(), rc.index(), context, 0, &mut memo)?,
        ) else {
            continue;
        };
        for (own, other, key, target) in [(&a, &b, l, &mut left), (&b, &a, r, &mut right)]
        {
            if narrows(own, other) != Some(true) {
                continue;
            }
            for (benefit, op, value) in [
                (other.0 > own.0, Operator::GtEq, &other.0),
                (other.1 < own.1, Operator::LtEq, &other.1),
            ] {
                if benefit {
                    target.push(Arc::new(BinaryExpr::new(
                        Arc::clone(key),
                        op,
                        Arc::new(Literal::new(value.clone())),
                    )));
                }
            }
        }
    }
    if left.is_empty() && right.is_empty() {
        return Ok(Transformed::no(plan));
    }
    let mut children = Vec::new();
    for (input, predicates) in [(join.left(), left), (join.right(), right)] {
        children.push(match conjunction_opt(predicates) {
            Some(p) => Arc::new(FilterExec::try_new(p, Arc::clone(input))?) as Plan,
            None => Arc::clone(input),
        });
    }
    Ok(Transformed::yes(plan.replace_children(
        children,
        datafusion_physical_plan::execution_plan::ReplaceChildrenOptions::new(
            datafusion_physical_plan::execution_plan::ChildrenPropertiesMode::Recompute,
        ),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_gate_handles_disjoint_and_wide_domains() {
        let range = |lo: i64, hi: i64| (ScalarValue::from(lo), ScalarValue::from(hi));
        assert_eq!(narrows(&range(0, 100), &range(0, 99)), Some(false));
        assert_eq!(
            narrows(&range(i64::MIN, i64::MAX), &range(0, 100)),
            Some(true)
        );
        assert_eq!(narrows(&range(2, 2), &range(3, 4)), Some(true));
    }
}
