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
use datafusion_expr::Expr;
use std::collections::HashMap;

/// A bound on the value of an expression.
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
            null_status: match value {
                ScalarValue::Null => NullStatus::AlwaysNull,
                _ => NullStatus::NeverNull,
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
        // IS NUll / NOT NUll

        // Inequality expressions

        // Columns (if bounds are equal and closed and column is not nullable)

        // In list

        Ok(expr)
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
            (col("x"), Guarantee::new(None, None, NullStatus::AlwaysNull)),
            (col("y"), Guarantee::new(None, None, NullStatus::NeverNull)),
        ];
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

        let cases = &[
            (col("x").is_null(), true),
            (col("x").is_not_null(), false),
            (col("y").is_null(), false),
            (col("y").is_not_null(), true),
        ];

        for (expr, expected_value) in cases {
            let output = expr.clone().rewrite(&mut rewriter).unwrap();
            assert_eq!(
                output,
                Expr::Literal(ScalarValue::Boolean(Some(*expected_value)))
            );
        }
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
            (col("x").gt(lit(3)), false),
            (col("y").gt_eq(lit(18628)), true),
            (col("y").gt(lit(19000)), true),
            (col("y").lt_eq(lit(17000)), false),
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
            col("x").lt_eq(lit(3)),
            col("y").gt_eq(lit(17000)),
        ];

        for expr in cases {
            let output = expr.clone().rewrite(&mut rewriter).unwrap();
            assert_eq!(&output, expr);
        }
    }

    #[test]
    fn test_column_single_value() {
        let guarantees = vec![
            // x = 2
            (col("x"), Guarantee::from(&ScalarValue::Int32(Some(2)))),
            // y is Null
            (col("y"), Guarantee::from(&ScalarValue::Null)),
        ];
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

        // These cases should be simplified
        let cases = &[
            (col("x").lt_eq(lit(1)), false),
            (col("x").gt(lit(3)), false),
            (col("x").eq(lit(1)), false),
            (col("x").eq(lit(2)), true),
            (col("x").gt(lit(1)), true),
            (col("x").lt_eq(lit(2)), true),
            (col("x").is_not_null(), true),
            (col("x").is_null(), false),
            (col("y").is_null(), true),
            (col("y").is_not_null(), false),
            (col("y").lt_eq(lit(17000)), false),
        ];

        for (expr, expected_value) in cases {
            let output = expr.clone().rewrite(&mut rewriter).unwrap();
            assert_eq!(
                output,
                Expr::Literal(ScalarValue::Boolean(Some(*expected_value)))
            );
        }
    }

    #[test]
    fn test_in_list() {
        let guarantees = vec![
            // x = 2
            (col("x"), Guarantee::from(&ScalarValue::Int32(Some(2)))),
            // 1 <= y < 10
            (
                col("y"),
                Guarantee::new(
                    Some(GuaranteeBound::new(ScalarValue::Int32(Some(1)), false)),
                    Some(GuaranteeBound::new(ScalarValue::Int32(Some(10)), true)),
                    NullStatus::NeverNull,
                ),
            ),
            // z is null
            (col("z"), Guarantee::from(&ScalarValue::Null)),
        ];
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

        // These cases should be simplified
        let cases = &[
            // x IN ()
            (col("x").in_list(vec![], false), false),
            // x IN (10, 11)
            (col("x").in_list(vec![lit(10), lit(11)], false), false),
            // x IN (10, 2)
            (col("x").in_list(vec![lit(10), lit(2)], false), true),
            // x NOT IN (10, 2)
            (col("x").in_list(vec![lit(10), lit(2)], true), false),
            // y IN (10, 11)
            (col("y").in_list(vec![lit(10), lit(11)], false), false),
            // y NOT IN (0, 22)
            (col("y").in_list(vec![lit(0), lit(22)], true), true),
            // z IN (10, 11)
            (col("z").in_list(vec![lit(10), lit(11)], false), false),
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
            // y IN (10, 2)
            col("y").in_list(vec![lit(10), lit(2)], false),
            // y NOT IN (10, 2)
            col("y").in_list(vec![lit(10), lit(2)], true),
        ];

        for expr in cases {
            let output = expr.clone().rewrite(&mut rewriter).unwrap();
            assert_eq!(&output, expr);
        }
    }
}
