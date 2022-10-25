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

//! Simplify expressions optimizer rule and implementation

use super::{ExprSimplifier, SimplifyContext, SimplifyInfo};
use super::utils::*;
use crate::{OptimizerConfig, OptimizerRule};
use arrow::error::ArrowError;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::{
    expr_fn::{and, or},
    expr_rewriter::{ExprRewritable, ExprRewriter},
    lit,
    logical_plan::LogicalPlan,
    utils::from_plan,
    BuiltinScalarFunction, Expr, Operator,
};
use datafusion_physical_expr::execution_props::ExecutionProps;



/// Optimizer Pass that simplifies [`LogicalPlan`]s by rewriting
/// [`Expr`]`s evaluating constants and applying algebraic
/// simplifications
///
/// # Introduction
/// It uses boolean algebra laws to simplify or reduce the number of terms in expressions.
///
/// # Example:
/// `Filter: b > 2 AND b > 2`
/// is optimized to
/// `Filter: b > 2`
///
#[derive(Default)]
pub struct SimplifyExpressions {}

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        "simplify_expressions"
    }

    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut execution_props = ExecutionProps::new();
        execution_props.query_execution_start_time =
            optimizer_config.query_execution_start_time();
        self.optimize_internal(plan, &execution_props)
    }
}

impl SimplifyExpressions {
    fn optimize_internal(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        // We need to pass down the all schemas within the plan tree to `optimize_expr` in order to
        // to evaluate expression types. For example, a projection plan's schema will only include
        // projected columns. With just the projected schema, it's not possible to infer types for
        // expressions that references non-projected columns within the same project plan or its
        // children plans.
        let info = plan
            .all_schemas()
            .into_iter()
            .fold(SimplifyContext::new(execution_props), |context, schema| {
                context.with_schema(schema.clone())
            });

        let simplifier = ExprSimplifier::new(info);

        let new_inputs = plan
            .inputs()
            .iter()
            .map(|input| self.optimize_internal(input, execution_props))
            .collect::<Result<Vec<_>>>()?;

        let expr = plan
            .expressions()
            .into_iter()
            .map(|e| {
                // We need to keep original expression name, if any.
                // Constant folding should not change expression name.
                let name = &e.display_name();

                // Apply the actual simplification logic
                let new_e = simplifier.simplify(e)?;

                let new_name = &new_e.display_name();

                if let (Ok(expr_name), Ok(new_expr_name)) = (name, new_name) {
                    if expr_name != new_expr_name {
                        Ok(new_e.alias(expr_name))
                    } else {
                        Ok(new_e)
                    }
                } else {
                    Ok(new_e)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        from_plan(plan, &expr, &new_inputs)
    }
}

impl SimplifyExpressions {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Simplifies [`Expr`]s by applying algebraic transformation rules
///
/// Example transformations that are applied:
/// * `expr = true` and `expr != false` to `expr` when `expr` is of boolean type
/// * `expr = false` and `expr != true` to `!expr` when `expr` is of boolean type
/// * `true = true` and `false = false` to `true`
/// * `false = true` and `true = false` to `false`
/// * `!!expr` to `expr`
/// * `expr = null` and `expr != null` to `null`
pub(crate) struct Simplifier<'a, S> {
    info: &'a S,
}

impl<'a, S> Simplifier<'a, S> {
    pub fn new(info: &'a S) -> Self {
        Self { info }
    }
}

impl<'a, S: SimplifyInfo> ExprRewriter for Simplifier<'a, S> {
    /// rewrite the expression simplifying any constant expressions
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        use Operator::{And, Divide, Eq, Modulo, Multiply, NotEq, Or};

        let info = self.info;
        let new_expr = match expr {
            //
            // Rules for Eq
            //

            // true = A  --> A
            // false = A --> !A
            // null = A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Eq,
                right,
            }) if is_bool_lit(&left) && info.is_boolean_type(&right)? => {
                match as_bool_lit(*left)? {
                    Some(true) => *right,
                    Some(false) => Expr::Not(right),
                    None => lit_bool_null(),
                }
            }
            // A = true  --> A
            // A = false --> !A
            // A = null --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Eq,
                right,
            }) if is_bool_lit(&right) && info.is_boolean_type(&left)? => {
                match as_bool_lit(*right)? {
                    Some(true) => *left,
                    Some(false) => Expr::Not(left),
                    None => lit_bool_null(),
                }
            }

            //
            // Rules for NotEq
            //

            // true != A  --> !A
            // false != A --> A
            // null != A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: NotEq,
                right,
            }) if is_bool_lit(&left) && info.is_boolean_type(&right)? => {
                match as_bool_lit(*left)? {
                    Some(true) => Expr::Not(right),
                    Some(false) => *right,
                    None => lit_bool_null(),
                }
            }
            // A != true  --> !A
            // A != false --> A
            // A != null --> null,
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: NotEq,
                right,
            }) if is_bool_lit(&right) && info.is_boolean_type(&left)? => {
                match as_bool_lit(*right)? {
                    Some(true) => Expr::Not(left),
                    Some(false) => *left,
                    None => lit_bool_null(),
                }
            }

            //
            // Rules for OR
            //

            // true OR A --> true (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right: _,
            }) if is_true(&left) => *left,
            // false OR A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if is_false(&left) => *right,
            // A OR true --> true (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Or,
                right,
            }) if is_true(&right) => *right,
            // A OR false --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if is_false(&right) => *left,
            // (..A..) OR A --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if expr_contains(&left, &right, Or) => *left,
            // A OR (..A..) --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if expr_contains(&right, &left, Or) => *right,
            // A OR (A AND B) --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if !info.nullable(&right)? && is_op_with(And, &right, &left) => *left,
            // (A AND B) OR A --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if !info.nullable(&left)? && is_op_with(And, &left, &right) => *right,

            //
            // Rules for AND
            //

            // true AND A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if is_true(&left) => *right,
            // false AND A --> false (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right: _,
            }) if is_false(&left) => *left,
            // A AND true --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if is_true(&right) => *left,
            // A AND false --> false (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: And,
                right,
            }) if is_false(&right) => *right,
            // (..A..) AND A --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if expr_contains(&left, &right, And) => *left,
            // A AND (..A..) --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if expr_contains(&right, &left, And) => *right,
            // A AND (A OR B) --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if !info.nullable(&right)? && is_op_with(Or, &right, &left) => *left,
            // (A OR B) AND A --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if !info.nullable(&left)? && is_op_with(Or, &left, &right) => *right,

            //
            // Rules for Multiply
            //

            // A * 1 --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if is_one(&right) => *left,
            // 1 * A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if is_one(&left) => *right,
            // A * null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Multiply,
                right,
            }) if is_null(&right) => *right,
            // null * A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right: _,
            }) if is_null(&left) => *left,

            // A * 0 --> 0 (if A is not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if !info.nullable(&left)? && is_zero(&right) => *right,
            // 0 * A --> 0 (if A is not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if !info.nullable(&right)? && is_zero(&left) => *left,

            //
            // Rules for Divide
            //

            // A / 1 --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right,
            }) if is_one(&right) => *left,
            // null / A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right: _,
            }) if is_null(&left) => *left,
            // A / null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Divide,
                right,
            }) if is_null(&right) => *right,
            // 0 / 0 -> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right,
            }) if is_zero(&left) && is_zero(&right) => {
                Expr::Literal(ScalarValue::Int32(None))
            }
            // A / 0 -> DivideByZero Error
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right,
            }) if !info.nullable(&left)? && is_zero(&right) => {
                return Err(DataFusionError::ArrowError(ArrowError::DivideByZero))
            }

            //
            // Rules for Modulo
            //

            // A % null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Modulo,
                right,
            }) if is_null(&right) => *right,
            // null % A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Modulo,
                right: _,
            }) if is_null(&left) => *left,
            // A % 1 --> 0
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Modulo,
                right,
            }) if !info.nullable(&left)? && is_one(&right) => lit(0),
            // A % 0 --> DivideByZero Error
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Modulo,
                right,
            }) if !info.nullable(&left)? && is_zero(&right) => {
                return Err(DataFusionError::ArrowError(ArrowError::DivideByZero))
            }

            //
            // Rules for Not
            //
            Expr::Not(inner) => negate_clause(*inner),

            //
            // Rules for Case
            //

            // CASE
            //   WHEN X THEN A
            //   WHEN Y THEN B
            //   ...
            //   ELSE Q
            // END
            //
            // ---> (X AND A) OR (Y AND B AND NOT X) OR ... (NOT (X OR Y) AND Q)
            //
            // Note: the rationale for this rewrite is that the expr can then be further
            // simplified using the existing rules for AND/OR
            Expr::Case(case)
                if !case.when_then_expr.is_empty()
                && case.when_then_expr.len() < 3 // The rewrite is O(n!) so limit to small number
                && info.is_boolean_type(&case.when_then_expr[0].1)? =>
            {
                // The disjunction of all the when predicates encountered so far
                let mut filter_expr = lit(false);
                // The disjunction of all the cases
                let mut out_expr = lit(false);

                for (when, then) in case.when_then_expr {
                    let case_expr = when
                        .as_ref()
                        .clone()
                        .and(filter_expr.clone().not())
                        .and(*then);

                    out_expr = out_expr.or(case_expr);
                    filter_expr = filter_expr.or(*when);
                }

                if let Some(else_expr) = case.else_expr {
                    let case_expr = filter_expr.not().and(*else_expr);
                    out_expr = out_expr.or(case_expr);
                }

                // Do a first pass at simplification
                out_expr.rewrite(self)?
            }

            // concat
            Expr::ScalarFunction {
                fun: BuiltinScalarFunction::Concat,
                args,
            } => simpl_concat(args)?,

            // concat_ws
            Expr::ScalarFunction {
                fun: BuiltinScalarFunction::ConcatWithSeparator,
                args,
            } => match &args[..] {
                [delimiter, vals @ ..] => simpl_concat_ws(delimiter, vals)?,
                _ => Expr::ScalarFunction {
                    fun: BuiltinScalarFunction::ConcatWithSeparator,
                    args,
                },
            },

            //
            // Rules for Between
            //

            // a between 3 and 5  -->  a >= 3 AND a <=5
            // a not between 3 and 5  -->  a < 3 OR a > 5
            Expr::Between(between) => {
                if between.negated {
                    let l = *between.expr.clone();
                    let r = *between.expr;
                    or(l.lt(*between.low), r.gt(*between.high))
                } else {
                    and(
                        between.expr.clone().gt_eq(*between.low),
                        between.expr.lt_eq(*between.high),
                    )
                }
            }
            expr => {
                // no additional rewrites possible
                expr
            }
        };
        Ok(new_expr)
    }
}

#[cfg(test)]
mod tests {
    use crate::simplify_expressions::utils::for_test::{to_timestamp_expr, now_expr, cast_to_int64_expr};

    use super::*;
    use arrow::datatypes::{DataType, Schema, Field};
    use chrono::{DateTime, TimeZone, Utc};
    use datafusion_common::{DFField, DFSchemaRef, DFSchema};
    use datafusion_expr::Between;
    use datafusion_expr::expr::Case;
    use datafusion_expr::expr_fn::{concat, concat_ws};
    use datafusion_expr::logical_plan::table_scan;
    use datafusion_expr::{
        and, binary_expr, col, lit,
        logical_plan::builder::LogicalPlanBuilder, Expr,
        ExprSchemable,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    /// A macro to assert that one string is contained within another with
    /// a nice error message if they are not.
    ///
    /// Usage: `assert_contains!(actual, expected)`
    ///
    /// Is a macro so test error
    /// messages are on the same line as the failure;
    ///
    /// Both arguments must be convertable into Strings (Into<String>)
    macro_rules! assert_contains {
        ($ACTUAL: expr, $EXPECTED: expr) => {
            let actual_value: String = $ACTUAL.into();
            let expected_value: String = $EXPECTED.into();
            assert!(
                actual_value.contains(&expected_value),
                "Can not find expected in actual.\n\nExpected:\n{}\n\nActual:\n{}",
                expected_value,
                actual_value
            );
        };
    }

    #[test]
    fn test_simplify_or_true() {
        let expr_a = col("c2").or(lit(true));
        let expr_b = lit(true).or(col("c2"));
        let expected = lit(true);

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_or_false() {
        let expr_a = lit(false).or(col("c2"));
        let expr_b = col("c2").or(lit(false));
        let expected = col("c2");

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_or_same() {
        let expr = col("c2").or(col("c2"));
        let expected = col("c2");

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_false() {
        let expr_a = lit(false).and(col("c2"));
        let expr_b = col("c2").and(lit(false));
        let expected = lit(false);

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_and_same() {
        let expr = col("c2").and(col("c2"));
        let expected = col("c2");

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_true() {
        let expr_a = lit(true).and(col("c2"));
        let expr_b = col("c2").and(lit(true));
        let expected = col("c2");

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_multiply_by_one() {
        let expr_a = binary_expr(col("c2"), Operator::Multiply, lit(1));
        let expr_b = binary_expr(lit(1), Operator::Multiply, col("c2"));
        let expected = col("c2");

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);

        let expr = binary_expr(
            col("c2"),
            Operator::Multiply,
            Expr::Literal(ScalarValue::Decimal128(Some(10000000000), 38, 10)),
        );
        assert_eq!(simplify(expr), expected);
        let expr = binary_expr(
            Expr::Literal(ScalarValue::Decimal128(Some(10000000000), 31, 10)),
            Operator::Multiply,
            col("c2"),
        );
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_multiply_by_null() {
        let null = Expr::Literal(ScalarValue::Null);
        // A * null --> null
        {
            let expr = binary_expr(col("c2"), Operator::Multiply, null.clone());
            assert_eq!(simplify(expr), null);
        }
        // null * A --> null
        {
            let expr = binary_expr(null.clone(), Operator::Multiply, col("c2"));
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_multiply_by_zero() {
        // cannot optimize A * null (null * A) if A is nullable
        {
            let expr_a = binary_expr(col("c2"), Operator::Multiply, lit(0));
            let expr_b = binary_expr(lit(0), Operator::Multiply, col("c2"));

            assert_eq!(simplify(expr_a.clone()), expr_a);
            assert_eq!(simplify(expr_b.clone()), expr_b);
        }
        // 0 * A --> 0 if A is not nullable
        {
            let expr = binary_expr(lit(0), Operator::Multiply, col("c2_non_null"));
            assert_eq!(simplify(expr), lit(0));
        }
        // A * 0 --> 0 if A is not nullable
        {
            let expr = binary_expr(col("c2_non_null"), Operator::Multiply, lit(0));
            assert_eq!(simplify(expr), lit(0));
        }
        // A * Decimal128(0) --> 0 if A is not nullable
        {
            let expr = binary_expr(
                col("c2_non_null"),
                Operator::Multiply,
                Expr::Literal(ScalarValue::Decimal128(Some(0), 31, 10)),
            );
            assert_eq!(
                simplify(expr),
                Expr::Literal(ScalarValue::Decimal128(Some(0), 31, 10))
            );
            let expr = binary_expr(
                Expr::Literal(ScalarValue::Decimal128(Some(0), 31, 10)),
                Operator::Multiply,
                col("c2_non_null"),
            );
            assert_eq!(
                simplify(expr),
                Expr::Literal(ScalarValue::Decimal128(Some(0), 31, 10))
            );
        }
    }

    #[test]
    fn test_simplify_divide_by_one() {
        let expr = binary_expr(col("c2"), Operator::Divide, lit(1));
        let expected = col("c2");
        assert_eq!(simplify(expr), expected);
        let expr = binary_expr(
            col("c2"),
            Operator::Divide,
            Expr::Literal(ScalarValue::Decimal128(Some(10000000000), 31, 10)),
        );
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_divide_null() {
        // A / null --> null
        let null = Expr::Literal(ScalarValue::Null);
        {
            let expr = binary_expr(col("c"), Operator::Divide, null.clone());
            assert_eq!(simplify(expr), null);
        }
        // null / A --> null
        {
            let expr = binary_expr(null.clone(), Operator::Divide, col("c"));
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_divide_by_same() {
        let expr = binary_expr(col("c2"), Operator::Divide, col("c2"));
        // if c2 is null, c2 / c2 = null, so can't simplify
        let expected = expr.clone();

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_divide_zero_by_zero() {
        // 0 / 0 -> null
        let expr = binary_expr(lit(0), Operator::Divide, lit(0));
        let expected = Expr::Literal(ScalarValue::Int32(None));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: ArrowError(DivideByZero)"
    )]
    fn test_simplify_divide_by_zero() {
        // A / 0 -> DivideByZeroError
        let expr = binary_expr(col("c2_non_null"), Operator::Divide, lit(0));

        simplify(expr);
    }

    #[test]
    fn test_simplify_modulo_by_null() {
        let null = Expr::Literal(ScalarValue::Null);
        // A % null --> null
        {
            let expr = binary_expr(col("c2"), Operator::Modulo, null.clone());
            assert_eq!(simplify(expr), null);
        }
        // null % A --> null
        {
            let expr = binary_expr(null.clone(), Operator::Modulo, col("c2"));
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_modulo_by_one() {
        let expr = binary_expr(col("c2"), Operator::Modulo, lit(1));
        // if c2 is null, c2 % 1 = null, so can't simplify
        let expected = expr.clone();

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_modulo_by_one_non_null() {
        let expr = binary_expr(col("c2_non_null"), Operator::Modulo, lit(1));
        let expected = lit(0);
        assert_eq!(simplify(expr), expected);
        let expr = binary_expr(
            col("c2_non_null"),
            Operator::Modulo,
            Expr::Literal(ScalarValue::Decimal128(Some(10000000000), 31, 10)),
        );
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: ArrowError(DivideByZero)"
    )]
    fn test_simplify_modulo_by_zero_non_null() {
        let expr = binary_expr(col("c2_non_null"), Operator::Modulo, lit(0));
        simplify(expr);
    }

    #[test]
    fn test_simplify_simple_and() {
        // (c > 5) AND (c > 5)
        let expr = (col("c2").gt(lit(5))).and(col("c2").gt(lit(5)));
        let expected = col("c2").gt(lit(5));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_composed_and() {
        // ((c > 5) AND (c1 < 6)) AND (c > 5)
        let expr = binary_expr(
            binary_expr(col("c2").gt(lit(5)), Operator::And, col("c1").lt(lit(6))),
            Operator::And,
            col("c2").gt(lit(5)),
        );
        let expected =
            binary_expr(col("c2").gt(lit(5)), Operator::And, col("c1").lt(lit(6)));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_negated_and() {
        // (c > 5) AND !(c > 5) -- > (c > 5) AND (c <= 5)
        let expr = binary_expr(
            col("c2").gt(lit(5)),
            Operator::And,
            Expr::not(col("c2").gt(lit(5))),
        );
        let expected = col("c2").gt(lit(5)).and(col("c2").lt_eq(lit(5)));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_or_and() {
        let l = col("c2").gt(lit(5));
        let r = binary_expr(col("c1").lt(lit(6)), Operator::And, col("c2").gt(lit(5)));

        // (c2 > 5) OR ((c1 < 6) AND (c2 > 5))
        let expr = binary_expr(l.clone(), Operator::Or, r.clone());

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) AND (c2 > 5)) OR (c2 > 5)
        let expr = binary_expr(l, Operator::Or, r);

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_or_and_non_null() {
        let l = col("c2_non_null").gt(lit(5));
        let r = binary_expr(
            col("c1_non_null").lt(lit(6)),
            Operator::And,
            col("c2_non_null").gt(lit(5)),
        );

        // (c2 > 5) OR ((c1 < 6) AND (c2 > 5)) --> c2 > 5
        let expr = binary_expr(l.clone(), Operator::Or, r.clone());

        // This is only true if `c1 < 6` is not nullable / can not be null.
        let expected = col("c2_non_null").gt(lit(5));

        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) AND (c2 > 5)) OR (c2 > 5) --> c2 > 5
        let expr = binary_expr(l, Operator::Or, r);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_or() {
        let l = col("c2").gt(lit(5));
        let r = binary_expr(col("c1").lt(lit(6)), Operator::Or, col("c2").gt(lit(5)));

        // (c2 > 5) AND ((c1 < 6) OR (c2 > 5)) --> c2 > 5
        let expr = binary_expr(l.clone(), Operator::And, r.clone());

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) OR (c2 > 5)) AND (c2 > 5) --> c2 > 5
        let expr = binary_expr(l, Operator::And, r);
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_or_non_null() {
        let l = col("c2_non_null").gt(lit(5));
        let r = binary_expr(
            col("c1_non_null").lt(lit(6)),
            Operator::Or,
            col("c2_non_null").gt(lit(5)),
        );

        // (c2 > 5) AND ((c1 < 6) OR (c2 > 5)) --> c2 > 5
        let expr = binary_expr(l.clone(), Operator::And, r.clone());

        // This is only true if `c1 < 6` is not nullable / can not be null.
        let expected = col("c2_non_null").gt(lit(5));

        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) OR (c2 > 5)) AND (c2 > 5) --> c2 > 5
        let expr = binary_expr(l, Operator::And, r);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_null_and_false() {
        let expr = binary_expr(lit_bool_null(), Operator::And, lit(false));
        let expr_eq = lit(false);

        assert_eq!(simplify(expr), expr_eq);
    }

    #[test]
    fn test_simplify_divide_null_by_null() {
        let null = Expr::Literal(ScalarValue::Int32(None));
        let expr_plus = binary_expr(null.clone(), Operator::Divide, null.clone());
        let expr_eq = null;

        assert_eq!(simplify(expr_plus), expr_eq);
    }

    #[test]
    fn test_simplify_simplify_arithmetic_expr() {
        let expr_plus = binary_expr(lit(1), Operator::Plus, lit(1));
        let expr_eq = binary_expr(lit(1), Operator::Eq, lit(1));

        assert_eq!(simplify(expr_plus), lit(2));
        assert_eq!(simplify(expr_eq), lit(true));
    }

    #[test]
    fn test_simplify_concat_ws() {
        let null = Expr::Literal(ScalarValue::Utf8(None));
        // the delimiter is not a literal
        {
            let expr = concat_ws(col("c"), vec![lit("a"), null.clone(), lit("b")]);
            let expected = concat_ws(col("c"), vec![lit("a"), lit("b")]);
            assert_eq!(simplify(expr), expected);
        }

        // the delimiter is an empty string
        {
            let expr = concat_ws(lit(""), vec![col("a"), lit("c"), lit("b")]);
            let expected = concat(&[col("a"), lit("cb")]);
            assert_eq!(simplify(expr), expected);
        }

        // the delimiter is a not-empty string
        {
            let expr = concat_ws(
                lit("-"),
                vec![
                    null.clone(),
                    col("c0"),
                    lit("hello"),
                    null.clone(),
                    lit("rust"),
                    col("c1"),
                    lit(""),
                    lit(""),
                    null,
                ],
            );
            let expected = concat_ws(
                lit("-"),
                vec![col("c0"), lit("hello-rust"), col("c1"), lit("-")],
            );
            assert_eq!(simplify(expr), expected)
        }
    }

    #[test]
    fn test_simplify_concat_ws_with_null() {
        let null = Expr::Literal(ScalarValue::Utf8(None));
        // null delimiter -> null
        {
            let expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
            assert_eq!(simplify(expr), null);
        }

        // filter out null args
        {
            let expr = concat_ws(lit("|"), vec![col("c1"), null.clone(), col("c2")]);
            let expected = concat_ws(lit("|"), vec![col("c1"), col("c2")]);
            assert_eq!(simplify(expr), expected);
        }

        // nested test
        {
            let sub_expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
            let expr = concat_ws(lit("|"), vec![sub_expr, col("c3")]);
            assert_eq!(simplify(expr), concat_ws(lit("|"), vec![col("c3")]));
        }

        // null delimiter (nested)
        {
            let sub_expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
            let expr = concat_ws(sub_expr, vec![col("c3"), col("c4")]);
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_concat() {
        let null = Expr::Literal(ScalarValue::Utf8(None));
        let expr = concat(&[
            null.clone(),
            col("c0"),
            lit("hello "),
            null.clone(),
            lit("rust"),
            col("c1"),
            lit(""),
            null,
        ]);
        let expected = concat(&[col("c0"), lit("hello rust"), col("c1")]);
        assert_eq!(simplify(expr), expected)
    }


    // ------------------------------
    // ----- Simplifier tests -------
    // ------------------------------

    fn simplify(expr: Expr) -> Expr {
        let schema = expr_test_schema();
        let execution_props = ExecutionProps::new();
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&execution_props).with_schema(schema),
        );
        simplifier.simplify(expr).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "c1", DataType::Utf8, true),
                    DFField::new(None, "c2", DataType::Boolean, true),
                    DFField::new(None, "c1_non_null", DataType::Utf8, false),
                    DFField::new(None, "c2_non_null", DataType::Boolean, false),
                ],
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn simplify_expr_not_not() {
        assert_eq!(simplify(col("c2").not().not().not()), col("c2").not(),);
    }

    #[test]
    fn simplify_expr_null_comparison() {
        // x = null is always null
        assert_eq!(
            simplify(lit(true).eq(lit(ScalarValue::Boolean(None)))),
            lit(ScalarValue::Boolean(None)),
        );

        // null != null is always null
        assert_eq!(
            simplify(
                lit(ScalarValue::Boolean(None)).not_eq(lit(ScalarValue::Boolean(None)))
            ),
            lit(ScalarValue::Boolean(None)),
        );

        // x != null is always null
        assert_eq!(
            simplify(col("c2").not_eq(lit(ScalarValue::Boolean(None)))),
            lit(ScalarValue::Boolean(None)),
        );

        // null = x is always null
        assert_eq!(
            simplify(lit(ScalarValue::Boolean(None)).eq(col("c2"))),
            lit(ScalarValue::Boolean(None)),
        );
    }

    #[test]
    fn simplify_expr_eq() {
        let schema = expr_test_schema();
        assert_eq!(col("c2").get_type(&schema).unwrap(), DataType::Boolean);

        // true = ture -> true
        assert_eq!(simplify(lit(true).eq(lit(true))), lit(true));

        // true = false -> false
        assert_eq!(simplify(lit(true).eq(lit(false))), lit(false),);

        // c2 = true -> c2
        assert_eq!(simplify(col("c2").eq(lit(true))), col("c2"));

        // c2 = false => !c2
        assert_eq!(simplify(col("c2").eq(lit(false))), col("c2").not(),);
    }

    #[test]
    fn simplify_expr_eq_skip_nonboolean_type() {
        let schema = expr_test_schema();

        // When one of the operand is not of boolean type, folding the
        // other boolean constant will change return type of
        // expression to non-boolean.
        //
        // Make sure c1 column to be used in tests is not boolean type
        assert_eq!(col("c1").get_type(&schema).unwrap(), DataType::Utf8);

        // don't fold c1 = foo
        assert_eq!(simplify(col("c1").eq(lit("foo"))), col("c1").eq(lit("foo")),);
    }

    #[test]
    fn simplify_expr_not_eq() {
        let schema = expr_test_schema();

        assert_eq!(col("c2").get_type(&schema).unwrap(), DataType::Boolean);

        // c2 != true -> !c2
        assert_eq!(simplify(col("c2").not_eq(lit(true))), col("c2").not(),);

        // c2 != false -> c2
        assert_eq!(simplify(col("c2").not_eq(lit(false))), col("c2"),);

        // test constant
        assert_eq!(simplify(lit(true).not_eq(lit(true))), lit(false),);

        assert_eq!(simplify(lit(true).not_eq(lit(false))), lit(true),);
    }

    #[test]
    fn simplify_expr_not_eq_skip_nonboolean_type() {
        let schema = expr_test_schema();

        // when one of the operand is not of boolean type, folding the
        // other boolean constant will change return type of
        // expression to non-boolean.
        assert_eq!(col("c1").get_type(&schema).unwrap(), DataType::Utf8);

        assert_eq!(
            simplify(col("c1").not_eq(lit("foo"))),
            col("c1").not_eq(lit("foo")),
        );
    }

    #[test]
    fn simplify_expr_case_when_then_else() {
        // CASE WHERE c2 != false THEN "ok" == "not_ok" ELSE c2 == true
        // -->
        // CASE WHERE c2 THEN false ELSE c2
        // -->
        // false
        assert_eq!(
            simplify(Expr::Case(Case::new(
                None,
                vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit("not_ok"))),
                )],
                Some(Box::new(col("c2").eq(lit(true)))),
            ))),
            col("c2").not().and(col("c2")) // #1716
        );

        // CASE WHERE c2 != false THEN "ok" == "ok" ELSE c2
        // -->
        // CASE WHERE c2 THEN true ELSE c2
        // -->
        // c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case(Case::new(
                None,
                vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit("ok"))),
                )],
                Some(Box::new(col("c2").eq(lit(true)))),
            )))),
            col("c2").or(col("c2").not().and(col("c2"))) // #1716
        );

        // CASE WHERE ISNULL(c2) THEN true ELSE c2
        // -->
        // ISNULL(c2) OR c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case(Case::new(
                None,
                vec![(Box::new(col("c2").is_null()), Box::new(lit(true)),)],
                Some(Box::new(col("c2"))),
            )))),
            col("c2")
                .is_null()
                .or(col("c2").is_not_null().and(col("c2")))
        );

        // CASE WHERE c1 then true WHERE c2 then false ELSE true
        // --> c1 OR (NOT(c1) AND c2 AND FALSE) OR (NOT(c1 OR c2) AND TRUE)
        // --> c1 OR (NOT(c1) AND NOT(c2))
        // --> c1 OR NOT(c2)
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case(Case::new(
                None,
                vec![
                    (Box::new(col("c1")), Box::new(lit(true)),),
                    (Box::new(col("c2")), Box::new(lit(false)),),
                ],
                Some(Box::new(lit(true))),
            )))),
            col("c1").or(col("c1").not().and(col("c2").not()))
        );

        // CASE WHERE c1 then true WHERE c2 then true ELSE false
        // --> c1 OR (NOT(c1) AND c2 AND TRUE) OR (NOT(c1 OR c2) AND FALSE)
        // --> c1 OR (NOT(c1) AND c2)
        // --> c1 OR c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case(Case::new(
                None,
                vec![
                    (Box::new(col("c1")), Box::new(lit(true)),),
                    (Box::new(col("c2")), Box::new(lit(false)),),
                ],
                Some(Box::new(lit(true))),
            )))),
            col("c1").or(col("c1").not().and(col("c2").not()))
        );
    }

    #[test]
    fn simplify_expr_bool_or() {
        // col || true is always true
        assert_eq!(simplify(col("c2").or(lit(true))), lit(true),);

        // col || false is always col
        assert_eq!(simplify(col("c2").or(lit(false))), col("c2"),);

        // true || null is always true
        assert_eq!(simplify(lit(true).or(lit_bool_null())), lit(true),);

        // null || true is always true
        assert_eq!(simplify(lit_bool_null().or(lit(true))), lit(true),);

        // false || null is always null
        assert_eq!(simplify(lit(false).or(lit_bool_null())), lit_bool_null(),);

        // null || false is always null
        assert_eq!(simplify(lit_bool_null().or(lit(false))), lit_bool_null(),);

        // ( c1 BETWEEN Int32(0) AND Int32(10) ) OR Boolean(NULL)
        // it can be either NULL or  TRUE depending on the value of `c1 BETWEEN Int32(0) AND Int32(10)`
        // and should not be rewritten
        let expr = Expr::Between(Between::new(
            Box::new(col("c1")),
            false,
            Box::new(lit(0)),
            Box::new(lit(10)),
        ));
        let expr = expr.or(lit_bool_null());
        let result = simplify(expr);

        let expected_expr = or(
            and(col("c1").gt_eq(lit(0)), col("c1").lt_eq(lit(10))),
            lit_bool_null(),
        );
        assert_eq!(expected_expr, result);
    }

    #[test]
    fn simplify_expr_bool_and() {
        // col & true is always col
        assert_eq!(simplify(col("c2").and(lit(true))), col("c2"),);
        // col & false is always false
        assert_eq!(simplify(col("c2").and(lit(false))), lit(false),);

        // true && null is always null
        assert_eq!(simplify(lit(true).and(lit_bool_null())), lit_bool_null(),);

        // null && true is always null
        assert_eq!(simplify(lit_bool_null().and(lit(true))), lit_bool_null(),);

        // false && null is always false
        assert_eq!(simplify(lit(false).and(lit_bool_null())), lit(false),);

        // null && false is always false
        assert_eq!(simplify(lit_bool_null().and(lit(false))), lit(false),);

        // c1 BETWEEN Int32(0) AND Int32(10) AND Boolean(NULL)
        // it can be either NULL or FALSE depending on the value of `c1 BETWEEN Int32(0) AND Int32(10)`
        // and the Boolean(NULL) should remain
        let expr = Expr::Between(Between::new(
            Box::new(col("c1")),
            false,
            Box::new(lit(0)),
            Box::new(lit(10)),
        ));
        let expr = expr.and(lit_bool_null());
        let result = simplify(expr);

        let expected_expr = and(
            and(col("c1").gt_eq(lit(0)), col("c1").lt_eq(lit(10))),
            lit_bool_null(),
        );
        assert_eq!(expected_expr, result);
    }

    #[test]
    fn simplify_expr_between() {
        // c2 between 3 and 4 is c2 >= 3 and c2 <= 4
        let expr = Expr::Between(Between::new(
            Box::new(col("c2")),
            false,
            Box::new(lit(3)),
            Box::new(lit(4)),
        ));
        assert_eq!(
            simplify(expr),
            and(col("c2").gt_eq(lit(3)), col("c2").lt_eq(lit(4)))
        );

        // c2 not between 3 and 4 is c2 < 3 or c2 > 4
        let expr = Expr::Between(Between::new(
            Box::new(col("c2")),
            true,
            Box::new(lit(3)),
            Box::new(lit(4)),
        ));
        assert_eq!(
            simplify(expr),
            or(col("c2").lt(lit(3)), col("c2").gt(lit(4)))
        );
    }

    // ------------------------------
    // -- SimplifyExpressions tests -
    // (test plans are simplified correctly)
    // ------------------------------

    fn test_table_scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::UInt32, false),
        ]);
        table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = SimplifyExpressions::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn test_simplify_optimized_plan() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])
            .unwrap()
            .filter(and(col("b").gt(lit(1)), col("b").gt(lit(1))))
            .unwrap()
            .build()
            .unwrap();

        assert_optimized_plan_eq(
            &plan,
            "\
	        Filter: test.b > Int32(1)\
            \n  Projection: test.a\
            \n    TableScan: test",
        );
    }

    #[test]
    fn test_simplify_optimized_plan_with_or() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])
            .unwrap()
            .filter(or(col("b").gt(lit(1)), col("b").gt(lit(1))))
            .unwrap()
            .build()
            .unwrap();

        assert_optimized_plan_eq(
            &plan,
            "\
            Filter: test.b > Int32(1)\
            \n  Projection: test.a\
            \n    TableScan: test",
        );
    }

    #[test]
    fn test_simplify_optimized_plan_with_composed_and() {
        let table_scan = test_table_scan();
        // ((c > 5) AND (d < 6)) AND (c > 5) --> (c > 5) AND (d < 6)
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])
            .unwrap()
            .filter(and(
                and(col("a").gt(lit(5)), col("b").lt(lit(6))),
                col("a").gt(lit(5)),
            ))
            .unwrap()
            .build()
            .unwrap();

        assert_optimized_plan_eq(
            &plan,
            "\
            Filter: test.a > Int32(5) AND test.b < Int32(6)\
            \n  Projection: test.a, test.b\
	        \n    TableScan: test",
        );
    }

    #[test]
    fn test_simplity_optimized_plan_eq_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(true)))
            .unwrap()
            .filter(col("c").eq(lit(false)))
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: test.a\
        \n  Filter: NOT test.c\
        \n    Filter: test.b\
        \n      TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_not_eq_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)))
            .unwrap()
            .filter(col("c").not_eq(lit(false)))
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: test.a\
        \n  Limit: skip=0, fetch=1\
        \n    Filter: test.c\
        \n      Filter: NOT test.b\
        \n        TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_and_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).and(col("c").eq(lit(true))))
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: test.a\
        \n  Filter: NOT test.b AND test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_or_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).or(col("c").eq(lit(false))))
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: test.a\
        \n  Filter: NOT test.b OR NOT test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_not_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(false)).not())
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: test.a\
        \n  Filter: test.b\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_support_projection() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("d"), col("b").eq(lit(false))])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: test.a, test.d, NOT test.b AS test.b = Boolean(false)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_support_aggregate() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("c"), col("b")])
            .unwrap()
            .aggregate(
                vec![col("a"), col("c")],
                vec![
                    datafusion_expr::max(col("b").eq(lit(true))),
                    datafusion_expr::min(col("b")),
                ],
            )
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Aggregate: groupBy=[[test.a, test.c]], aggr=[[MAX(test.b) AS MAX(test.b = Boolean(true)), MIN(test.b)]]\
        \n  Projection: test.a, test.c, test.b\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_support_values() {
        let expr1 = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(1)),
            Operator::Plus,
            Box::new(lit(2)),
        ));
        let expr2 = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(2)),
            Operator::Minus,
            Box::new(lit(1)),
        ));
        let values = vec![vec![expr1, expr2]];
        let plan = LogicalPlanBuilder::values(values).unwrap().build().unwrap();

        let expected = "\
        Values: (Int32(3) AS Int32(1) + Int32(2), Int32(1) AS Int32(2) - Int32(1))";

        assert_optimized_plan_eq(&plan, expected);
    }

    // expect optimizing will result in an error, returning the error string
    fn get_optimized_plan_err(plan: &LogicalPlan, date_time: &DateTime<Utc>) -> String {
        let mut config =
            OptimizerConfig::new().with_query_execution_start_time(*date_time);
        let rule = SimplifyExpressions::new();

        let err = rule
            .optimize(plan, &mut config)
            .expect_err("expected optimization to fail");

        err.to_string()
    }

    fn get_optimized_plan_formatted(
        plan: &LogicalPlan,
        date_time: &DateTime<Utc>,
    ) -> String {
        let mut config =
            OptimizerConfig::new().with_query_execution_start_time(*date_time);
        let rule = SimplifyExpressions::new();

        let optimized_plan = rule
            .optimize(plan, &mut config)
            .expect("failed to optimize plan");
        format!("{:?}", optimized_plan)
    }

    #[test]
    fn to_timestamp_expr_folded() {
        let table_scan = test_table_scan();
        let proj = vec![to_timestamp_expr("2020-09-08T12:00:00+00:00")];

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: TimestampNanosecond(1599566400000000000, None) AS totimestamp(Utf8(\"2020-09-08T12:00:00+00:00\"))\
            \n  TableScan: test"
            .to_string();
        let actual = get_optimized_plan_formatted(&plan, &Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_timestamp_expr_wrong_arg() {
        let table_scan = test_table_scan();
        let proj = vec![to_timestamp_expr("I'M NOT A TIMESTAMP")];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Error parsing 'I'M NOT A TIMESTAMP' as timestamp";
        let actual = get_optimized_plan_err(&plan, &Utc::now());
        assert_contains!(actual, expected);
    }

    #[test]
    fn cast_expr() {
        let table_scan = test_table_scan();
        let proj = vec![Expr::Cast {
            expr: Box::new(lit("0")),
            data_type: DataType::Int32,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: Int32(0) AS Utf8(\"0\")\
            \n  TableScan: test";
        let actual = get_optimized_plan_formatted(&plan, &Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn cast_expr_wrong_arg() {
        let table_scan = test_table_scan();
        let proj = vec![Expr::Cast {
            expr: Box::new(lit("")),
            data_type: DataType::Int32,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Cannot cast string '' to value of Int32 type";
        let actual = get_optimized_plan_err(&plan, &Utc::now());
        assert_contains!(actual, expected);
    }

    #[test]
    fn multiple_now_expr() {
        let table_scan = test_table_scan();
        let time = Utc::now();
        let proj = vec![
            now_expr(),
            Expr::Alias(Box::new(now_expr()), "t2".to_string()),
        ];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        // expect the same timestamp appears in both exprs
        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected = format!(
            "Projection: TimestampNanosecond({}, Some(\"UTC\")) AS now(), TimestampNanosecond({}, Some(\"UTC\")) AS t2\
            \n  TableScan: test",
            time.timestamp_nanos(),
            time.timestamp_nanos()
        );

        assert_eq!(expected, actual);
    }

    #[test]
    fn simplify_and_eval() {
        // demonstrate a case where the evaluation needs to run prior
        // to the simplifier for it to work
        let table_scan = test_table_scan();
        let time = Utc::now();
        // (true or false) != col --> !col
        let proj = vec![lit(true).or(lit(false)).not_eq(col("a"))];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected =
            "Projection: NOT test.a AS Boolean(true) OR Boolean(false) != test.a\
                        \n  TableScan: test";

        assert_eq!(expected, actual);
    }

    #[test]
    fn now_less_than_timestamp() {
        let table_scan = test_table_scan();

        let ts_string = "2020-09-08T12:05:00+00:00";
        let time = chrono::Utc.timestamp_nanos(1599566400000000000i64);

        //  cast(now() as int) < cast(to_timestamp(...) as int) + 50000_i64
        let plan =
            LogicalPlanBuilder::from(table_scan)
                .filter(
                    cast_to_int64_expr(now_expr())
                        .lt(cast_to_int64_expr(to_timestamp_expr(ts_string))
                            + lit(50000_i64)),
                )
                .unwrap()
                .build()
                .unwrap();

        // Note that constant folder runs and folds the entire
        // expression down to a single constant (true)
        let expected = "Filter: Boolean(true)\
                        \n  TableScan: test";
        let actual = get_optimized_plan_formatted(&plan, &time);

        assert_eq!(expected, actual);
    }

    #[test]
    fn select_date_plus_interval() {
        let table_scan = test_table_scan();

        let ts_string = "2020-09-08T12:05:00+00:00";
        let time = chrono::Utc.timestamp_nanos(1599566400000000000i64);

        //  now() < cast(to_timestamp(...) as int) + 5000000000
        let schema = table_scan.schema();

        let date_plus_interval_expr = to_timestamp_expr(ts_string)
            .cast_to(&DataType::Date32, schema)
            .unwrap()
            + Expr::Literal(ScalarValue::IntervalDayTime(Some(123i64 << 32)));

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .project(vec![date_plus_interval_expr])
            .unwrap()
            .build()
            .unwrap();

        println!("{:?}", plan);

        // Note that constant folder runs and folds the entire
        // expression down to a single constant (true)
        let expected = r#"Projection: Date32("18636") AS totimestamp(Utf8("2020-09-08T12:05:00+00:00")) + IntervalDayTime("528280977408")
  TableScan: test"#;
        let actual = get_optimized_plan_formatted(&plan, &time);

        assert_eq!(expected, actual);
    }

    #[test]
    fn simplify_not_binary() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d <= Int32(10)\
            \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_bool_and() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).and(col("d").lt(lit(100))).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d <= Int32(10) OR test.d >= Int32(100)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_bool_or() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).or(col("d").lt(lit(100))).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d <= Int32(10) AND test.d >= Int32(100)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).not().not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d > Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_null() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").is_null().not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d IS NOT NULL\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_null() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").is_not_null().not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d IS NULL\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_in() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").in_list(vec![lit(1), lit(2), lit(3)], false).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d NOT IN ([Int32(1), Int32(2), Int32(3)])\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_in() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").in_list(vec![lit(1), lit(2), lit(3)], true).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d IN ([Int32(1), Int32(2), Int32(3)])\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_between() {
        let table_scan = test_table_scan();
        let qual = Expr::Between(Between::new(
            Box::new(col("d")),
            false,
            Box::new(lit(1)),
            Box::new(lit(10)),
        ));

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(qual.not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d < Int32(1) OR test.d > Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_between() {
        let table_scan = test_table_scan();
        let qual = Expr::Between(Between::new(
            Box::new(col("d")),
            true,
            Box::new(lit(1)),
            Box::new(lit(10)),
        ));

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(qual.not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d >= Int32(1) AND test.d <= Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_like() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan");

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").like(col("b")).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.a NOT LIKE test.b\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_like() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan");

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").not_like(col("b")).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.a LIKE test.b\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_distinct_from() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(binary_expr(col("d"), Operator::IsDistinctFrom, lit(10)).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d IS NOT DISTINCT FROM Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_distinct_from() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(binary_expr(col("d"), Operator::IsNotDistinctFrom, lit(10)).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: test.d IS DISTINCT FROM Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }
}
