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

//! Simplify expressions optimizer rule

use crate::execution::context::ExecutionProps;
use crate::logical_plan::LogicalPlan;
use crate::logical_plan::{lit, Expr};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::scalar::ScalarValue;
use crate::{error::Result, logical_plan::Operator};

/// Simplify expressions optimizer.
/// # Introduction
/// It uses boolean algebra laws to simplify or reduce the number of terms in expressions.
///
/// Filter: b > 2 AND b > 2
/// is optimized to
/// Filter: b > 2
pub struct SimplifyExpressions {}

fn expr_contains(expr: &Expr, needle: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr {
            left,
            op: Operator::And,
            right,
        } => expr_contains(left, needle) || expr_contains(right, needle),
        Expr::BinaryExpr {
            left,
            op: Operator::Or,
            right,
        } => expr_contains(left, needle) || expr_contains(right, needle),
        _ => expr == needle,
    }
}

fn as_binary_expr(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::BinaryExpr { .. } => Some(expr),
        _ => None,
    }
}

fn operator_is_boolean(op: Operator) -> bool {
    op == Operator::And || op == Operator::Or
}

fn is_one(s: &Expr) -> bool {
    match s {
        Expr::Literal(ScalarValue::Int8(Some(1)))
        | Expr::Literal(ScalarValue::Int16(Some(1)))
        | Expr::Literal(ScalarValue::Int32(Some(1)))
        | Expr::Literal(ScalarValue::Int64(Some(1)))
        | Expr::Literal(ScalarValue::UInt8(Some(1)))
        | Expr::Literal(ScalarValue::UInt16(Some(1)))
        | Expr::Literal(ScalarValue::UInt32(Some(1)))
        | Expr::Literal(ScalarValue::UInt64(Some(1))) => true,
        Expr::Literal(ScalarValue::Float32(Some(v))) if *v == 1. => true,
        Expr::Literal(ScalarValue::Float64(Some(v))) if *v == 1. => true,
        _ => false,
    }
}

fn is_true(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v))) => *v,
        _ => false,
    }
}

fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(v) => v.is_null(),
        _ => false,
    }
}

fn is_false(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v))) => !(*v),
        _ => false,
    }
}

fn simplify(expr: &Expr) -> Expr {
    match expr {
        Expr::BinaryExpr {
            left,
            op: Operator::Or,
            right,
        } if is_true(left) || is_true(right) => lit(true),
        Expr::BinaryExpr {
            left,
            op: Operator::Or,
            right,
        } if is_false(left) => simplify(right),
        Expr::BinaryExpr {
            left,
            op: Operator::Or,
            right,
        } if is_false(right) => simplify(left),
        Expr::BinaryExpr {
            left,
            op: Operator::Or,
            right,
        } if left == right => simplify(left),
        Expr::BinaryExpr {
            left,
            op: Operator::And,
            right,
        } if is_false(left) || is_false(right) => lit(false),
        Expr::BinaryExpr {
            left,
            op: Operator::And,
            right,
        } if is_true(right) => simplify(left),
        Expr::BinaryExpr {
            left,
            op: Operator::And,
            right,
        } if is_true(left) => simplify(right),
        Expr::BinaryExpr {
            left,
            op: Operator::And,
            right,
        } if left == right => simplify(right),
        Expr::BinaryExpr {
            left,
            op: Operator::Multiply,
            right,
        } if is_one(left) => simplify(right),
        Expr::BinaryExpr {
            left,
            op: Operator::Multiply,
            right,
        } if is_one(right) => simplify(left),
        Expr::BinaryExpr {
            left,
            op: Operator::Divide,
            right,
        } if is_one(right) => simplify(left),
        Expr::BinaryExpr {
            left,
            op: Operator::Divide,
            right,
        } if left == right && is_null(left) => *left.clone(),
        Expr::BinaryExpr {
            left,
            op: Operator::Divide,
            right,
        } if left == right => lit(1),
        Expr::BinaryExpr { left, op, right }
            if left == right && operator_is_boolean(*op) =>
        {
            simplify(left)
        }
        Expr::BinaryExpr {
            left,
            op: Operator::Or,
            right,
        } if expr_contains(left, right) => as_binary_expr(left)
            .map(|x| match x {
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::Or,
                    right: _,
                } => simplify(&x.clone()),
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::And,
                    right: _,
                } => simplify(&*right.clone()),
                _ => expr.clone(),
            })
            .unwrap_or_else(|| expr.clone()),
        Expr::BinaryExpr {
            left,
            op: Operator::Or,
            right,
        } if expr_contains(right, left) => as_binary_expr(right)
            .map(|x| match x {
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::Or,
                    right: _,
                } => simplify(&*right.clone()),
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::And,
                    right: _,
                } => simplify(&*left.clone()),
                _ => expr.clone(),
            })
            .unwrap_or_else(|| expr.clone()),
        Expr::BinaryExpr {
            left,
            op: Operator::And,
            right,
        } if expr_contains(left, right) => as_binary_expr(left)
            .map(|x| match x {
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::Or,
                    right: _,
                } => simplify(&*right.clone()),
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::And,
                    right: _,
                } => simplify(&x.clone()),
                _ => expr.clone(),
            })
            .unwrap_or_else(|| expr.clone()),
        Expr::BinaryExpr {
            left,
            op: Operator::And,
            right,
        } if expr_contains(right, left) => as_binary_expr(right)
            .map(|x| match x {
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::Or,
                    right: _,
                } => simplify(&*left.clone()),
                Expr::BinaryExpr {
                    left: _,
                    op: Operator::And,
                    right: _,
                } => simplify(&x.clone()),
                _ => expr.clone(),
            })
            .unwrap_or_else(|| expr.clone()),
        Expr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
            left: Box::new(simplify(left)),
            op: *op,
            right: Box::new(simplify(right)),
        },
        _ => expr.clone(),
    }
}

fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|input| optimize(input))
        .collect::<Result<Vec<_>>>()?;
    let expr = plan
        .expressions()
        .into_iter()
        .map(|x| simplify(&x))
        .collect::<Vec<_>>();
    utils::from_plan(plan, &expr, &new_inputs)
}

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        "simplify_expressions"
    }

    fn optimize(
        &self,
        plan: &LogicalPlan,
        _execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        optimize(plan)
    }
}

impl SimplifyExpressions {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{and, binary_expr, col, lit, Expr, LogicalPlanBuilder};
    use crate::test::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = SimplifyExpressions::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn test_simplify_or_true() -> Result<()> {
        let expr_a = col("c").or(lit(true));
        let expr_b = lit(true).or(col("c"));
        let expected = lit(true);

        assert_eq!(simplify(&expr_a), expected);
        assert_eq!(simplify(&expr_b), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_or_false() -> Result<()> {
        let expr_a = lit(false).or(col("c"));
        let expr_b = col("c").or(lit(false));
        let expected = col("c");

        assert_eq!(simplify(&expr_a), expected);
        assert_eq!(simplify(&expr_b), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_or_same() -> Result<()> {
        let expr = col("c").or(col("c"));
        let expected = col("c");

        assert_eq!(simplify(&expr), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_and_false() -> Result<()> {
        let expr_a = lit(false).and(col("c"));
        let expr_b = col("c").and(lit(false));
        let expected = lit(false);

        assert_eq!(simplify(&expr_a), expected);
        assert_eq!(simplify(&expr_b), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_and_same() -> Result<()> {
        let expr = col("c").and(col("c"));
        let expected = col("c");

        assert_eq!(simplify(&expr), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_and_true() -> Result<()> {
        let expr_a = lit(true).and(col("c"));
        let expr_b = col("c").and(lit(true));
        let expected = col("c");

        assert_eq!(simplify(&expr_a), expected);
        assert_eq!(simplify(&expr_b), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_multiply_by_one() -> Result<()> {
        let expr_a = binary_expr(col("c"), Operator::Multiply, lit(1));
        let expr_b = binary_expr(lit(1), Operator::Multiply, col("c"));
        let expected = col("c");

        assert_eq!(simplify(&expr_a), expected);
        assert_eq!(simplify(&expr_b), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_divide_by_one() -> Result<()> {
        let expr = binary_expr(col("c"), Operator::Divide, lit(1));
        let expected = col("c");

        assert_eq!(simplify(&expr), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_divide_by_same() -> Result<()> {
        let expr = binary_expr(col("c"), Operator::Divide, col("c"));
        let expected = lit(1);

        assert_eq!(simplify(&expr), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_simple_and() -> Result<()> {
        // (c > 5) AND (c > 5)
        let expr = (col("c").gt(lit(5))).and(col("c").gt(lit(5)));
        let expected = col("c").gt(lit(5));

        assert_eq!(simplify(&expr), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_composed_and() -> Result<()> {
        // ((c > 5) AND (d < 6)) AND (c > 5)
        let expr = binary_expr(
            binary_expr(col("c").gt(lit(5)), Operator::And, col("d").lt(lit(6))),
            Operator::And,
            col("c").gt(lit(5)),
        );
        let expected =
            binary_expr(col("c").gt(lit(5)), Operator::And, col("d").lt(lit(6)));

        assert_eq!(simplify(&expr), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_negated_and() -> Result<()> {
        // (c > 5) AND !(c > 5) -- can't remove
        let expr = binary_expr(
            col("c").gt(lit(5)),
            Operator::And,
            Expr::not(col("c").gt(lit(5))),
        );
        let expected = expr.clone();

        assert_eq!(simplify(&expr), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_or_and() -> Result<()> {
        // (c > 5) OR ((d < 6) AND (c > 5) -- can remove
        let expr = binary_expr(
            col("c").gt(lit(5)),
            Operator::Or,
            binary_expr(col("d").lt(lit(6)), Operator::And, col("c").gt(lit(5))),
        );
        let expected = col("c").gt(lit(5));

        assert_eq!(simplify(&expr), expected);
        Ok(())
    }

    #[test]
    fn test_simplify_and_and_false() -> Result<()> {
        let expr =
            binary_expr(lit(ScalarValue::Boolean(None)), Operator::And, lit(false));
        let expr_eq = lit(false);

        assert_eq!(simplify(&expr), expr_eq);
        Ok(())
    }

    #[test]
    fn test_simplify_divide_null_by_null() -> Result<()> {
        let null = Expr::Literal(ScalarValue::Int32(None));
        let expr_plus = binary_expr(null.clone(), Operator::Divide, null.clone());
        let expr_eq = null;

        assert_eq!(simplify(&expr_plus), expr_eq);
        Ok(())
    }

    #[test]
    fn test_simplify_do_not_simplify_arithmetic_expr() -> Result<()> {
        let expr_plus = binary_expr(lit(1), Operator::Plus, lit(1));
        let expr_eq = binary_expr(lit(1), Operator::Eq, lit(1));

        assert_eq!(simplify(&expr_plus), expr_plus);
        assert_eq!(simplify(&expr_eq), expr_eq);

        Ok(())
    }

    #[test]
    fn test_simplify_optimized_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(and(col("b").gt(lit(1)), col("b").gt(lit(1))))?
            .build()?;

        assert_optimized_plan_eq(
            &plan,
            "\
	        Filter: #test.b Gt Int32(1)\
            \n  Projection: #test.a\
            \n    TableScan: test projection=None",
        );
        Ok(())
    }

    // ((c > 5) AND (d < 6)) AND (c > 5) --> (c > 5) AND (d < 6)
    #[test]
    fn test_simplify_optimized_plan_with_composed_and() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(and(
                and(col("a").gt(lit(5)), col("b").lt(lit(6))),
                col("a").gt(lit(5)),
            ))?
            .build()?;

        assert_optimized_plan_eq(
            &plan,
            "\
            Filter: #test.a Gt Int32(5) And #test.b Lt Int32(6)\
            \n  Projection: #test.a\
	        \n    TableScan: test projection=None",
        );
        Ok(())
    }
}
