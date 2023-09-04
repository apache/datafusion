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

//! Logic to inject guarantees with expressions.
//!
use datafusion_common::{tree_node::TreeNodeRewriter, Result, ScalarValue};
use datafusion_expr::{expr::InList, lit, Between, BinaryExpr, Expr, Operator};
use std::collections::HashMap;

/// A bound on the value of an expression.
#[derive(Debug, Clone, PartialEq)]
pub struct GuaranteeBound {
    /// The value of the bound.
    pub bound: ScalarValue,
    /// If true, the bound is exclusive. If false, the bound is inclusive.
    /// In terms of inequalities, this means the bound is `<` or `>` rather than
    /// `<=` or `>=`.
    pub open: bool,
}

impl GuaranteeBound {
    /// Create a new bound.
    pub fn new(bound: ScalarValue, open: bool) -> Self {
        Self { bound, open }
    }
}

impl Default for GuaranteeBound {
    fn default() -> Self {
        Self {
            bound: ScalarValue::Null,
            open: false,
        }
    }
}

/// The null status of an expression.
///
/// This might be populated by null count statistics, for example. A null count
/// of zero would mean `NeverNull`, while a null count equal to row count would
/// mean `AlwaysNull`.
#[derive(Debug, Clone, PartialEq)]
pub enum NullStatus {
    /// The expression is guaranteed to be non-null.
    NeverNull,
    /// The expression is guaranteed to be null.
    AlwaysNull,
    /// The expression isn't guaranteed to never be null or always be null.
    MaybeNull,
}

/// A set of constraints on the value of an expression.
///
/// This is similar to [datafusion_physical_expr::intervals::Interval], except
/// that this is designed for working with logical expressions and also handles
/// nulls.
#[derive(Debug, Clone, PartialEq)]
pub struct Guarantee {
    /// The min values that the expression can take on. If `min.bound` is
    pub min: GuaranteeBound,
    /// The max values that the expression can take on.
    pub max: GuaranteeBound,
    /// Whether the expression is expected to be either always null or never null.
    pub null_status: NullStatus,
}

impl Guarantee {
    /// Create a new guarantee.
    pub fn new(
        min: Option<GuaranteeBound>,
        max: Option<GuaranteeBound>,
        null_status: NullStatus,
    ) -> Self {
        Self {
            min: min.unwrap_or_default(),
            max: max.unwrap_or_default(),
            null_status,
        }
    }

    /// Whether values are guaranteed to be greater than the given value.
    fn greater_than(&self, value: &ScalarValue) -> bool {
        self.min.bound > *value || (self.min.bound == *value && self.min.open)
    }

    fn greater_than_or_eq(&self, value: &ScalarValue) -> bool {
        self.min.bound >= *value
    }

    fn less_than(&self, value: &ScalarValue) -> bool {
        self.max.bound < *value || (self.max.bound == *value && self.max.open)
    }

    fn less_than_or_eq(&self, value: &ScalarValue) -> bool {
        self.max.bound <= *value
    }

    /// Whether the guarantee could contain the given value.
    fn contains(&self, value: &ScalarValue) -> bool {
        !self.less_than(value) && !self.greater_than(value)
    }
}

impl From<&ScalarValue> for Guarantee {
    fn from(value: &ScalarValue) -> Self {
        Self {
            min: GuaranteeBound {
                bound: value.clone(),
                open: false,
            },
            max: GuaranteeBound {
                bound: value.clone(),
                open: false,
            },
            null_status: if value.is_null() {
                NullStatus::AlwaysNull
            } else {
                NullStatus::NeverNull
            },
        }
    }
}

/// Rewrite expressions to incorporate guarantees.
///
///
pub(crate) struct GuaranteeRewriter<'a> {
    guarantees: HashMap<&'a Expr, &'a Guarantee>,
}

impl<'a> GuaranteeRewriter<'a> {
    pub fn new(guarantees: impl IntoIterator<Item = &'a (Expr, Guarantee)>) -> Self {
        Self {
            guarantees: guarantees.into_iter().map(|(k, v)| (k, v)).collect(),
        }
    }
}

impl<'a> TreeNodeRewriter for GuaranteeRewriter<'a> {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match &expr {
            // IS NUll / NOT NUll
            Expr::IsNull(inner) => {
                if let Some(guarantee) = self.guarantees.get(inner.as_ref()) {
                    match guarantee.null_status {
                        NullStatus::AlwaysNull => Ok(lit(true)),
                        NullStatus::NeverNull => Ok(lit(false)),
                        NullStatus::MaybeNull => Ok(expr),
                    }
                } else {
                    Ok(expr)
                }
            }
            Expr::IsNotNull(inner) => {
                if let Some(guarantee) = self.guarantees.get(inner.as_ref()) {
                    match guarantee.null_status {
                        NullStatus::AlwaysNull => Ok(lit(false)),
                        NullStatus::NeverNull => Ok(lit(true)),
                        NullStatus::MaybeNull => Ok(expr),
                    }
                } else {
                    Ok(expr)
                }
            }
            // Inequality expressions
            Expr::Between(Between {
                expr: inner,
                negated,
                low,
                high,
            }) => {
                if let Some(guarantee) = self.guarantees.get(inner.as_ref()) {
                    match (low.as_ref(), high.as_ref()) {
                        (Expr::Literal(low), Expr::Literal(high)) => {
                            if guarantee.greater_than_or_eq(low)
                                && guarantee.less_than_or_eq(high)
                            {
                                // All values are between the bounds
                                Ok(lit(!negated))
                            } else if guarantee.greater_than(high)
                                || guarantee.less_than(low)
                            {
                                // All values are outside the bounds
                                Ok(lit(*negated))
                            } else {
                                Ok(expr)
                            }
                        }
                        (Expr::Literal(low), _)
                            if !guarantee.less_than(low) && !negated =>
                        {
                            // All values are below the lower bound
                            Ok(lit(false))
                        }
                        (_, Expr::Literal(high))
                            if !guarantee.greater_than(high) && !negated =>
                        {
                            // All values are above the upper bound
                            Ok(lit(false))
                        }
                        _ => Ok(expr),
                    }
                } else {
                    Ok(expr)
                }
            }

            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                // Check if this is a comparison
                match op {
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => {}
                    _ => return Ok(expr),
                };

                // Check if this is a comparison between a column and literal
                let (col, op, value) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(_), Expr::Literal(value)) => (left, *op, value),
                    (Expr::Literal(value), Expr::Column(_)) => {
                        (right, op.swap().unwrap(), value)
                    }
                    _ => return Ok(expr),
                };

                // TODO: can this be simplified?
                if let Some(guarantee) = self.guarantees.get(col.as_ref()) {
                    match op {
                        Operator::Eq => {
                            if guarantee.greater_than(value) || guarantee.less_than(value)
                            {
                                // All values are outside the bounds
                                Ok(lit(false))
                            } else if guarantee.greater_than_or_eq(value)
                                && guarantee.less_than_or_eq(value)
                            {
                                // All values are equal to the bound
                                Ok(lit(true))
                            } else {
                                Ok(expr)
                            }
                        }
                        Operator::NotEq => {
                            if guarantee.greater_than(value) || guarantee.less_than(value)
                            {
                                // All values are outside the bounds
                                Ok(lit(true))
                            } else if guarantee.greater_than_or_eq(value)
                                && guarantee.less_than_or_eq(value)
                            {
                                // All values are equal to the bound
                                Ok(lit(false))
                            } else {
                                Ok(expr)
                            }
                        }
                        Operator::Gt => {
                            if guarantee.less_than_or_eq(value) {
                                // All values are less than or equal to the bound
                                Ok(lit(false))
                            } else if guarantee.greater_than(value) {
                                // All values are greater than the bound
                                Ok(lit(true))
                            } else {
                                Ok(expr)
                            }
                        }
                        Operator::GtEq => {
                            if guarantee.less_than(value) {
                                // All values are less than the bound
                                Ok(lit(false))
                            } else if guarantee.greater_than_or_eq(value) {
                                // All values are greater than or equal to the bound
                                Ok(lit(true))
                            } else {
                                Ok(expr)
                            }
                        }
                        Operator::Lt => {
                            if guarantee.greater_than_or_eq(value) {
                                // All values are greater than or equal to the bound
                                Ok(lit(false))
                            } else if guarantee.less_than(value) {
                                // All values are less than the bound
                                Ok(lit(true))
                            } else {
                                Ok(expr)
                            }
                        }
                        Operator::LtEq => {
                            if guarantee.greater_than(value) {
                                // All values are greater than the bound
                                Ok(lit(false))
                            } else if guarantee.less_than_or_eq(value) {
                                // All values are less than or equal to the bound
                                Ok(lit(true))
                            } else {
                                Ok(expr)
                            }
                        }
                        _ => Ok(expr),
                    }
                } else {
                    Ok(expr)
                }
            }

            // Columns (if bounds are equal and closed and column is not nullable)
            Expr::Column(_) => {
                if let Some(guarantee) = self.guarantees.get(&expr) {
                    if guarantee.min == guarantee.max
                        // Case where column has a single valid value
                        && ((!guarantee.min.open
                            && !guarantee.min.bound.is_null()
                            && guarantee.null_status == NullStatus::NeverNull)
                            // Case where column is always null
                            || (guarantee.min.bound.is_null()
                                && guarantee.null_status == NullStatus::AlwaysNull))
                    {
                        Ok(lit(guarantee.min.bound.clone()))
                    } else {
                        Ok(expr)
                    }
                } else {
                    Ok(expr)
                }
            }

            Expr::InList(InList {
                expr: inner,
                list,
                negated,
            }) => {
                if let Some(guarantee) = self.guarantees.get(inner.as_ref()) {
                    // Can remove items from the list that don't match the guarantee
                    let new_list: Vec<Expr> = list
                        .iter()
                        .filter(|item| {
                            if let Expr::Literal(item) = item {
                                guarantee.contains(item)
                            } else {
                                true
                            }
                        })
                        .cloned()
                        .collect();

                    Ok(Expr::InList(InList {
                        expr: inner.clone(),
                        list: new_list,
                        negated: *negated,
                    }))
                } else {
                    Ok(expr)
                }
            }

            _ => Ok(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion_common::tree_node::TreeNode;
    use datafusion_expr::{col, lit};

    #[test]
    fn test_null_handling() {
        // IsNull / IsNotNull can be rewritten to true / false
        let guarantees = vec![
            // Note: AlwaysNull case handled by test_column_single_value test,
            // since it's a special case of a column with a single value.
            (col("x"), Guarantee::new(None, None, NullStatus::NeverNull)),
        ];
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

        // x IS NULL => guaranteed false
        let expr = col("x").is_null();
        let output = expr.clone().rewrite(&mut rewriter).unwrap();
        assert_eq!(output, lit(false));

        // x IS NOT NULL => guaranteed true
        let expr = col("x").is_not_null();
        let output = expr.clone().rewrite(&mut rewriter).unwrap();
        assert_eq!(output, lit(true));
    }

    #[test]
    fn test_inequalities() {
        let guarantees = vec![
            // 1 < x <= 3
            (
                col("x"),
                Guarantee::new(
                    Some(GuaranteeBound::new(ScalarValue::Int32(Some(1)), true)),
                    Some(GuaranteeBound::new(ScalarValue::Int32(Some(3)), false)),
                    NullStatus::NeverNull,
                ),
            ),
            // 2021-01-01 <= y
            (
                col("y"),
                Guarantee::new(
                    Some(GuaranteeBound::new(ScalarValue::Date32(Some(18628)), false)),
                    None,
                    NullStatus::NeverNull,
                ),
            ),
            // "abc" < z <= "def"
            (
                col("z"),
                Guarantee::new(
                    Some(GuaranteeBound::new(
                        ScalarValue::Utf8(Some("abc".to_string())),
                        true,
                    )),
                    Some(GuaranteeBound::new(
                        ScalarValue::Utf8(Some("def".to_string())),
                        false,
                    )),
                    NullStatus::MaybeNull,
                ),
            ),
        ];
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

        // These cases should be simplified
        let cases = &[
            (col("x").lt_eq(lit(1)), false),
            (col("x").lt_eq(lit(3)), true),
            (col("x").gt(lit(3)), false),
            (col("y").gt_eq(lit(ScalarValue::Date32(Some(18628)))), true),
            (col("y").gt_eq(lit(ScalarValue::Date32(Some(17000)))), true),
            (col("y").lt_eq(lit(ScalarValue::Date32(Some(17000)))), false),
        ];

        for (expr, expected_value) in cases {
            let output = expr.clone().rewrite(&mut rewriter).unwrap();
            assert_eq!(
                output,
                Expr::Literal(ScalarValue::Boolean(Some(*expected_value)))
            );
        }

        // These cases should be left as-is
        let cases = &[
            col("x").gt(lit(2)),
            col("x").lt_eq(lit(2)),
            col("x").between(lit(2), lit(5)),
            col("x").not_between(lit(3), lit(10)),
            col("y").gt(lit(ScalarValue::Date32(Some(19000)))),
        ];

        for expr in cases {
            let output = expr.clone().rewrite(&mut rewriter).unwrap();
            assert_eq!(&output, expr);
        }
    }

    #[test]
    fn test_column_single_value() {
        let scalars = [
            ScalarValue::Null,
            ScalarValue::Int32(Some(1)),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(None),
            ScalarValue::Utf8(Some("abc".to_string())),
            ScalarValue::LargeUtf8(Some("def".to_string())),
            ScalarValue::Date32(Some(18628)),
            ScalarValue::Date32(None),
            ScalarValue::Decimal128(Some(1000), 19, 2),
        ];

        for scalar in &scalars {
            let guarantees = vec![(col("x"), Guarantee::from(scalar))];
            let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

            let output = col("x").rewrite(&mut rewriter).unwrap();
            assert_eq!(output, Expr::Literal(scalar.clone()));
        }
    }

    #[test]
    fn test_in_list() {
        let guarantees = vec![
            // 1 <= x < 10
            (
                col("x"),
                Guarantee::new(
                    Some(GuaranteeBound::new(ScalarValue::Int32(Some(1)), false)),
                    Some(GuaranteeBound::new(ScalarValue::Int32(Some(10)), true)),
                    NullStatus::NeverNull,
                ),
            ),
        ];
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

        // These cases should be simplified so the list doesn't contain any
        // values the guarantee says are outside the range.
        // (column_name, starting_list, negated, expected_list)
        let cases = &[
            // x IN (9, 11) => x IN (9)
            ("x", vec![9, 11], false, vec![9]),
            // x IN (10, 2) => x IN (2)
            ("x", vec![10, 2], false, vec![2]),
            // x NOT IN (9, 11) => x NOT IN (9)
            ("x", vec![9, 11], true, vec![9]),
            // x NOT IN (0, 22) => x NOT IN ()
            ("x", vec![0, 22], true, vec![]),
        ];

        for (column_name, starting_list, negated, expected_list) in cases {
            let expr = col(*column_name).in_list(
                starting_list
                    .iter()
                    .map(|v| lit(ScalarValue::Int32(Some(*v))))
                    .collect(),
                *negated,
            );
            let output = expr.clone().rewrite(&mut rewriter).unwrap();
            let expected_list = expected_list
                .iter()
                .map(|v| lit(ScalarValue::Int32(Some(*v))))
                .collect();
            assert_eq!(
                output,
                Expr::InList(InList {
                    expr: Box::new(col(*column_name)),
                    list: expected_list,
                    negated: *negated,
                })
            );
        }
    }
}
