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

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::{plan_err, Column, DFSchemaRef};
use datafusion_expr::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use datafusion_expr::{
    and, col, combine_filters,
    logical_plan::{Filter, LogicalPlan},
    utils::from_plan,
    Expr, Operator,
};
use std::collections::HashSet;
use std::sync::Arc;

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

/// Recursively scans a slice of expressions for any `Or` operators
///
/// # Arguments
///
/// * `predicates` - the expressions to scan
///
/// # Return value
///
/// A PlanError if a disjunction is found
pub fn verify_not_disjunction(predicates: &[&Expr]) -> Result<()> {
    struct DisjunctionVisitor {}

    impl ExpressionVisitor for DisjunctionVisitor {
        fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
            match expr {
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::Or,
                    right: _,
                } => {
                    plan_err!("Optimizing disjunctions not supported!")
                }
                _ => Ok(Recursion::Continue(self)),
            }
        }
    }

    for predicate in predicates.iter() {
        predicate.accept(DisjunctionVisitor {})?;
    }

    Ok(())
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

/// Looks for correlating expressions: equality expressions with one field from the subquery, and
/// one not in the subquery (closed upon from outer scope)
///
/// # Arguments
///
/// * `exprs` - List of expressions that may or may not be joins
/// * `schema` - HashSet of fully qualified (table.col) fields in subquery schema
///
/// # Return value
///
/// Tuple of (expressions containing joins, remaining non-join expressions)
pub fn find_join_exprs(
    exprs: Vec<&Expr>,
    schema: &DFSchemaRef,
) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let fields: HashSet<_> = schema
        .fields()
        .iter()
        .map(|it| it.qualified_name())
        .collect();

    let mut joins = vec![];
    let mut others = vec![];
    for filter in exprs.iter() {
        let (left, op, right) = match filter {
            Expr::BinaryExpr { left, op, right } => (*left.clone(), *op, *right.clone()),
            _ => {
                others.push((*filter).clone());
                continue;
            }
        };
        let left = match left {
            Expr::Column(c) => c,
            _ => {
                others.push((*filter).clone());
                continue;
            }
        };
        let right = match right {
            Expr::Column(c) => c,
            _ => {
                others.push((*filter).clone());
                continue;
            }
        };
        if fields.contains(&left.flat_name()) && fields.contains(&right.flat_name()) {
            others.push((*filter).clone());
            continue; // both columns present (none closed-upon)
        }
        if !fields.contains(&left.flat_name()) && !fields.contains(&right.flat_name()) {
            others.push((*filter).clone());
            continue; // neither column present (syntax error?)
        }
        match op {
            Operator::Eq => {}
            Operator::NotEq => {}
            _ => {
                plan_err!(format!("can't optimize {} column comparison", op))?;
            }
        }

        joins.push((*filter).clone())
    }

    Ok((joins, others))
}

/// Extracts correlating columns from expressions
///
/// # Arguments
///
/// * `exprs` - List of expressions that correlate a subquery to an outer scope
/// * `schema` - subquery schema
/// * `include_negated` - true if `NotEq` counts as a join operator
///
/// # Return value
///
/// Tuple of (outer-scope cols, subquery cols, non-correlation expressions)
pub fn exprs_to_join_cols(
    exprs: &[Expr],
    schema: &DFSchemaRef,
    include_negated: bool,
) -> Result<(Vec<Column>, Vec<Column>, Option<Expr>)> {
    let fields: HashSet<_> = schema
        .fields()
        .iter()
        .map(|it| it.qualified_name())
        .collect();

    let mut joins: Vec<(String, String)> = vec![];
    let mut others: Vec<Expr> = vec![];
    for filter in exprs.iter() {
        let (left, op, right) = match filter {
            Expr::BinaryExpr { left, op, right } => (*left.clone(), *op, *right.clone()),
            _ => plan_err!("Invalid correlation expression!")?,
        };
        match op {
            Operator::Eq => {}
            Operator::NotEq => {
                if !include_negated {
                    others.push((*filter).clone());
                    continue;
                }
            }
            _ => plan_err!(format!("Correlation operator unsupported: {}", op))?,
        }
        let left = left.try_into_col()?;
        let right = right.try_into_col()?;
        let sorted = if fields.contains(&left.flat_name()) {
            (right.flat_name(), left.flat_name())
        } else {
            (left.flat_name(), right.flat_name())
        };
        joins.push(sorted);
    }

    let (left_cols, right_cols): (Vec<_>, Vec<_>) = joins
        .into_iter()
        .map(|(l, r)| (Column::from(l.as_str()), Column::from(r.as_str())))
        .unzip();
    let pred = combine_filters(&others);

    Ok((left_cols, right_cols, pred))
}

/// Returns the first (and only) element in a slice, or an error
///
/// # Arguments
///
/// * `slice` - The slice to extract from
///
/// # Return value
///
/// The first element, or an error
pub fn only_or_err<T>(slice: &[T]) -> Result<&T> {
    match slice {
        [it] => Ok(it),
        [] => plan_err!("No items found!"),
        _ => plan_err!("More than one item found!"),
    }
}

/// Merge and deduplicate two sets Column slices
///
/// # Arguments
///
/// * `a` - A tuple of slices of Columns
/// * `b` - A tuple of slices of Columns
///
/// # Return value
///
/// The deduplicated union of the two slices
pub fn merge_cols(
    a: (&[Column], &[Column]),
    b: (&[Column], &[Column]),
) -> (Vec<Column>, Vec<Column>) {
    let e =
        a.0.iter()
            .map(|it| it.flat_name())
            .chain(a.1.iter().map(|it| it.flat_name()))
            .map(|it| Column::from(it.as_str()));
    let f =
        b.0.iter()
            .map(|it| it.flat_name())
            .chain(b.1.iter().map(|it| it.flat_name()))
            .map(|it| Column::from(it.as_str()));
    let mut g = e.zip(f).collect::<Vec<_>>();
    g.dedup();
    g.into_iter().unzip()
}

/// Change the relation on a slice of Columns
///
/// # Arguments
///
/// * `new_table` - The table/relation for the new columns
/// * `cols` - A slice of Columns
///
/// # Return value
///
/// A new slice of columns, now belonging to the new table
pub fn swap_table(new_table: &str, cols: &[Column]) -> Vec<Column> {
    cols.iter()
        .map(|it| Column {
            relation: Some(new_table.to_string()),
            name: it.name.clone(),
        })
        .collect()
}

pub fn alias_cols(cols: &[Column]) -> Vec<Expr> {
    cols.iter()
        .map(|it| col(it.flat_name().as_str()).alias(it.name.as_str()))
        .collect()
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
