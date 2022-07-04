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

//! Collection of utility functions that are leveraged by the query optimizer rules

use std::collections::HashSet;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::{
    and,
    logical_plan::{Filter, LogicalPlan},
    utils::from_plan,
    Expr, Operator,
};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{Ordering, AtomicUsize};
use itertools::{Either, Itertools};
use lazy_static::lazy_static;
use datafusion_common::{Column};

lazy_static! {
    static ref ID: Mutex<AtomicUsize> = Mutex::new(AtomicUsize::new(1));
}

pub fn get_id() -> usize {
    (*ID.lock().unwrap()).fetch_add(1usize, Ordering::SeqCst)
}

/// Exposed for testing
pub fn reset_id() {
    (*ID.lock().unwrap()).store(1usize, Ordering::SeqCst)
}

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
/// This also handles the case when the `plan` is a [`LogicalPlan::Explain`].
pub fn optimize_children(
    optimizer: &impl OptimizerRule,
    plan: &LogicalPlan,
    optimizer_config: &mut OptimizerConfig,
) -> Result<LogicalPlan> {
    let new_exprs = plan.expressions();
    let new_inputs = plan
        .inputs()
        .into_iter()
        .map(|plan| optimizer.optimize(plan, optimizer_config))
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, &new_exprs, &new_inputs)
}

/// converts "A AND B AND C" => [A, B, C]
pub fn split_conjunction<'a>(predicate: &'a Expr, predicates: &mut Vec<&'a Expr>) {
    match predicate {
        Expr::BinaryExpr {
            right,
            op: Operator::And,
            left,
        } => {
            split_conjunction(left, predicates);
            split_conjunction(right, predicates);
        }
        Expr::Alias(expr, _) => {
            split_conjunction(expr, predicates);
        }
        other => predicates.push(other),
    }
}

/// returns a new [LogicalPlan] that wraps `plan` in a [LogicalPlan::Filter] with
/// its predicate be all `predicates` ANDed.
pub fn add_filter(plan: LogicalPlan, predicates: &[&Expr]) -> LogicalPlan {
    // reduce filters to a single filter with an AND
    let predicate = predicates
        .iter()
        .skip(1)
        .fold(predicates[0].clone(), |acc, predicate| {
            and(acc, (*predicate).to_owned())
        });

    LogicalPlan::Filter(Filter {
        predicate,
        input: Arc::new(plan),
    })
}

pub fn find_join_exprs(
    filters: Vec<&Expr>,
    fields: &HashSet<String>,
) -> ((Vec<Column>, Vec<Column>), Vec<Expr>) {
    let (joins, others): (Vec<_>, Vec<_>) = filters.iter()
        .partition_map(|filter| {
        let (left, op, right) = match filter {
            Expr::BinaryExpr { left, op, right } => (*left.clone(), *op, *right.clone()),
            _ => return Either::Right((*filter).clone()),
        };
        match op {
            Operator::Eq => {}
            _ => return Either::Right((*filter).clone()),
        }
        let left = match left {
            Expr::Column(c) => c,
            _ => return Either::Right((*filter).clone()),
        };
        let right = match right {
            Expr::Column(c) => c,
            _ => return Either::Right((*filter).clone()),
        };
        if fields.contains(&left.flat_name()) && fields.contains(&right.flat_name()) {
            return Either::Right((*filter).clone()); // Need one of each
        }
        if !fields.contains(&left.flat_name()) && !fields.contains(&right.flat_name()) {
            return Either::Right((*filter).clone()); // Need one of each
        }

        let sorted = if fields.contains(&left.name) {
            (right.flat_name(), left.flat_name())
        } else {
            (left.flat_name(), right.flat_name())
        };

        Either::Left(sorted)
    });

    let right_cols: Vec<_> = joins.iter()
        .map(|it| &it.1)
        .map(|it| Column::from(it.as_str()))
        .collect();
    let left_cols: Vec<_> = joins.iter()
        .map(|it| &it.0)
        .map(|it| Column::from(it.as_str()))
        .collect();

    ((left_cols, right_cols), others)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion_common::Column;
    use datafusion_expr::{col, utils::expr_to_columns};
    use std::collections::HashSet;

    #[test]
    fn test_collect_expr() -> Result<()> {
        let mut accum: HashSet<Column> = HashSet::new();
        expr_to_columns(
            &Expr::Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Float64,
            },
            &mut accum,
        )?;
        expr_to_columns(
            &Expr::Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Float64,
            },
            &mut accum,
        )?;
        assert_eq!(1, accum.len());
        assert!(accum.contains(&Column::from_name("a")));
        Ok(())
    }
}
