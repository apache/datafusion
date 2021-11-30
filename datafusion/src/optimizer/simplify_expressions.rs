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

use arrow::array::new_null_array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::DataFusionError;
use crate::execution::context::{ExecutionContextState, ExecutionProps};
use crate::logical_plan::{lit, DFSchemaRef, Expr};
use crate::logical_plan::{DFSchema, ExprRewriter, LogicalPlan, RewriteRecursion};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::physical_plan::functions::Volatility;
use crate::physical_plan::planner::DefaultPhysicalPlanner;
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

/// Partially evaluate `Expr`s so constant subtrees are evaluated at plan time.
///
/// Note it does not handle algebriac rewrites such as `(a and false)` --> `a`
///
/// ```
/// # use datafusion::prelude::*;
/// # use datafusion::optimizer::simplify_expressions::ConstEvaluator;
/// # use datafusion::execution::context::ExecutionProps;
///
/// let execution_props = ExecutionProps::new();
/// let mut const_evaluator = ConstEvaluator::new(&execution_props);
///
/// // (1 + 2) + a
/// let expr = (lit(1) + lit(2)) + col("a");
///
/// // is rewritten to (3 + a);
/// let rewritten = expr.rewrite(&mut const_evaluator).unwrap();
/// assert_eq!(rewritten, lit(3) + col("a"));
/// ```
pub struct ConstEvaluator {
    /// can_evaluate is used during the depth-first-search of the
    /// Expr tree to track if any siblings (or their descendants) were
    /// non evaluatable (e.g. had a column reference or volatile
    /// function)
    ///
    /// Specifically, can_evaluate[N] represents the state of
    /// traversal when we are N levels deep in the tree, one entry for
    /// this Expr and each of its parents.
    ///
    /// After visiting all siblings if can_evauate.top() is true, that
    /// means there were no non evaluatable siblings (or their
    /// descendants) so this Expr can be evaluated
    can_evaluate: Vec<bool>,

    ctx_state: ExecutionContextState,
    planner: DefaultPhysicalPlanner,
    input_schema: DFSchema,
    input_batch: RecordBatch,
}

impl ExprRewriter for ConstEvaluator {
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        // Default to being able to evaluate this node
        self.can_evaluate.push(true);

        // if this expr is not ok to evaluate, mark entire parent
        // stack as not ok (as all parents have at least one child or
        // descendant that is non evaluateable

        if !Self::can_evaluate(expr) {
            // walk back up stack, marking first parent that is not mutable
            let parent_iter = self.can_evaluate.iter_mut().rev();
            for p in parent_iter {
                if !*p {
                    // optimization: if we find an element on the
                    // stack already marked, know all elements above are also marked
                    break;
                }
                *p = false;
            }
        }

        // NB: do not short circuit recursion even if we find a non
        // evaluatable node (so we can fold other children, args to
        // functions, etc)
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if self.can_evaluate.pop().unwrap() {
            let scalar = self.evaluate_to_scalar(expr)?;
            Ok(Expr::Literal(scalar))
        } else {
            Ok(expr)
        }
    }
}

impl ConstEvaluator {
    /// Create a new `ConstantEvaluator`. Session constants (such as
    /// the time for `now()` are taken from the passed
    /// `execution_props`.
    pub fn new(execution_props: &ExecutionProps) -> Self {
        let planner = DefaultPhysicalPlanner::default();
        let ctx_state = ExecutionContextState {
            execution_props: execution_props.clone(),
            ..ExecutionContextState::new()
        };
        let input_schema = DFSchema::empty();

        // The dummy column name is unused and doesn't matter as only
        // expressions without column references can be evaluated
        static DUMMY_COL_NAME: &str = ".";
        let schema = Schema::new(vec![Field::new(DUMMY_COL_NAME, DataType::Null, true)]);

        // Need a single "input" row to produce a single output row
        let col = new_null_array(&DataType::Null, 1);
        let input_batch =
            RecordBatch::try_new(std::sync::Arc::new(schema), vec![col]).unwrap();

        Self {
            can_evaluate: vec![],
            ctx_state,
            planner,
            input_schema,
            input_batch,
        }
    }

    /// Can a function of the specified volatility be evaluated?
    fn volatility_ok(volatility: Volatility) -> bool {
        match volatility {
            Volatility::Immutable => true,
            // Values for functions such as now() are taken from ExecutionProps
            Volatility::Stable => true,
            Volatility::Volatile => false,
        }
    }

    /// Can the expression be evaluated at plan time, (assuming all of
    /// its children can also be evaluated)?
    fn can_evaluate(expr: &Expr) -> bool {
        // check for reasons we can't evaluate this node
        //
        // NOTE all expr types are listed here so when new ones are
        // added they can be checked for their ability to be evaluated
        // at plan time
        match expr {
            // Has no runtime cost, but needed during planning
            Expr::Alias(..) => false,
            Expr::AggregateFunction { .. } => false,
            Expr::AggregateUDF { .. } => false,
            Expr::ScalarVariable(_) => false,
            Expr::Column(_) => false,
            Expr::ScalarFunction { fun, .. } => Self::volatility_ok(fun.volatility()),
            Expr::ScalarUDF { fun, .. } => Self::volatility_ok(fun.signature.volatility),
            Expr::WindowFunction { .. } => false,
            Expr::Sort { .. } => false,
            Expr::Wildcard => false,

            Expr::Literal(_) => true,
            Expr::BinaryExpr { .. } => true,
            Expr::Not(_) => true,
            Expr::IsNotNull(_) => true,
            Expr::IsNull(_) => true,
            Expr::Negative(_) => true,
            Expr::Between { .. } => true,
            Expr::Case { .. } => true,
            Expr::Cast { .. } => true,
            Expr::TryCast { .. } => true,
            Expr::InList { .. } => true,
            Expr::GetIndexedField { .. } => true,
        }
    }

    /// Internal helper to evaluates an Expr
    fn evaluate_to_scalar(&self, expr: Expr) -> Result<ScalarValue> {
        if let Expr::Literal(s) = expr {
            return Ok(s);
        }

        let phys_expr = self.planner.create_physical_expr(
            &expr,
            &self.input_schema,
            &self.input_batch.schema(),
            &self.ctx_state,
        )?;
        let col_val = phys_expr.evaluate(&self.input_batch)?;
        match col_val {
            crate::physical_plan::ColumnarValue::Array(a) => {
                if a.len() != 1 {
                    Err(DataFusionError::Execution(format!(
                        "Could not evaluate the expressison, found a result of length {}",
                        a.len()
                    )))
                } else {
                    Ok(ScalarValue::try_from_array(&a, 0)?)
                }
            }
            crate::physical_plan::ColumnarValue::Scalar(s) => Ok(s),
        }
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
pub(crate) struct Simplifier<'a> {
    /// input schemas
    schemas: Vec<&'a DFSchemaRef>,
}

impl<'a> Simplifier<'a> {
    pub fn new(schemas: Vec<&'a DFSchemaRef>) -> Self {
        Self { schemas }
    }

    fn is_boolean_type(&self, expr: &Expr) -> bool {
        for schema in &self.schemas {
            if let Ok(DataType::Boolean) = expr.get_type(schema) {
                return true;
            }
        }

        false
    }

    fn boolean_folding_for_or(
        const_bool: &Option<bool>,
        bool_expr: Box<Expr>,
        left_right_order: bool,
    ) -> Expr {
        // See if we can fold 'const_bool OR bool_expr' to a constant boolean
        match const_bool {
            // TRUE or expr (including NULL) = TRUE
            Some(true) => Expr::Literal(ScalarValue::Boolean(Some(true))),
            // FALSE or expr (including NULL) = expr
            Some(false) => *bool_expr,
            None => match *bool_expr {
                // NULL or TRUE = TRUE
                Expr::Literal(ScalarValue::Boolean(Some(true))) => {
                    Expr::Literal(ScalarValue::Boolean(Some(true)))
                }
                // NULL or FALSE = NULL
                Expr::Literal(ScalarValue::Boolean(Some(false))) => {
                    Expr::Literal(ScalarValue::Boolean(None))
                }
                // NULL or NULL = NULL
                Expr::Literal(ScalarValue::Boolean(None)) => {
                    Expr::Literal(ScalarValue::Boolean(None))
                }
                // NULL or expr can be either NULL or TRUE
                // So let us not rewrite it
                _ => {
                    let mut left =
                        Box::new(Expr::Literal(ScalarValue::Boolean(*const_bool)));
                    let mut right = bool_expr;
                    if !left_right_order {
                        std::mem::swap(&mut left, &mut right);
                    }

                    Expr::BinaryExpr {
                        left,
                        op: Operator::Or,
                        right,
                    }
                }
            },
        }
    }

    fn boolean_folding_for_and(
        const_bool: &Option<bool>,
        bool_expr: Box<Expr>,
        left_right_order: bool,
    ) -> Expr {
        // See if we can fold 'const_bool AND bool_expr' to a constant boolean
        match const_bool {
            // TRUE and expr (including NULL) = expr
            Some(true) => *bool_expr,
            // FALSE and expr (including NULL) = FALSE
            Some(false) => Expr::Literal(ScalarValue::Boolean(Some(false))),
            None => match *bool_expr {
                // NULL and TRUE = NULL
                Expr::Literal(ScalarValue::Boolean(Some(true))) => {
                    Expr::Literal(ScalarValue::Boolean(None))
                }
                // NULL and FALSE = FALSE
                Expr::Literal(ScalarValue::Boolean(Some(false))) => {
                    Expr::Literal(ScalarValue::Boolean(Some(false)))
                }
                // NULL and NULL = NULL
                Expr::Literal(ScalarValue::Boolean(None)) => {
                    Expr::Literal(ScalarValue::Boolean(None))
                }
                // NULL and expr can either be NULL or FALSE
                // So let us not rewrite it
                _ => {
                    let mut left =
                        Box::new(Expr::Literal(ScalarValue::Boolean(*const_bool)));
                    let mut right = bool_expr;
                    if !left_right_order {
                        std::mem::swap(&mut left, &mut right);
                    }

                    Expr::BinaryExpr {
                        left,
                        op: Operator::And,
                        right,
                    }
                }
            },
        }
    }
}

impl<'a> ExprRewriter for Simplifier<'a> {
    /// rewrite the expression simplifying any constant expressions
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let new_expr = match expr {
            Expr::BinaryExpr { left, op, right } => match op {
                Operator::Eq => match (left.as_ref(), right.as_ref()) {
                    (
                        Expr::Literal(ScalarValue::Boolean(l)),
                        Expr::Literal(ScalarValue::Boolean(r)),
                    ) => match (l, r) {
                        (Some(l), Some(r)) => {
                            Expr::Literal(ScalarValue::Boolean(Some(l == r)))
                        }
                        _ => Expr::Literal(ScalarValue::Boolean(None)),
                    },
                    (Expr::Literal(ScalarValue::Boolean(b)), _)
                        if self.is_boolean_type(&right) =>
                    {
                        match b {
                            Some(true) => *right,
                            Some(false) => Expr::Not(right),
                            None => Expr::Literal(ScalarValue::Boolean(None)),
                        }
                    }
                    (_, Expr::Literal(ScalarValue::Boolean(b)))
                        if self.is_boolean_type(&left) =>
                    {
                        match b {
                            Some(true) => *left,
                            Some(false) => Expr::Not(left),
                            None => Expr::Literal(ScalarValue::Boolean(None)),
                        }
                    }
                    _ => Expr::BinaryExpr {
                        left,
                        op: Operator::Eq,
                        right,
                    },
                },
                Operator::NotEq => match (left.as_ref(), right.as_ref()) {
                    (
                        Expr::Literal(ScalarValue::Boolean(l)),
                        Expr::Literal(ScalarValue::Boolean(r)),
                    ) => match (l, r) {
                        (Some(l), Some(r)) => {
                            Expr::Literal(ScalarValue::Boolean(Some(l != r)))
                        }
                        _ => Expr::Literal(ScalarValue::Boolean(None)),
                    },
                    (Expr::Literal(ScalarValue::Boolean(b)), _)
                        if self.is_boolean_type(&right) =>
                    {
                        match b {
                            Some(true) => Expr::Not(right),
                            Some(false) => *right,
                            None => Expr::Literal(ScalarValue::Boolean(None)),
                        }
                    }
                    (_, Expr::Literal(ScalarValue::Boolean(b)))
                        if self.is_boolean_type(&left) =>
                    {
                        match b {
                            Some(true) => Expr::Not(left),
                            Some(false) => *left,
                            None => Expr::Literal(ScalarValue::Boolean(None)),
                        }
                    }
                    _ => Expr::BinaryExpr {
                        left,
                        op: Operator::NotEq,
                        right,
                    },
                },
                Operator::Or => match (left.as_ref(), right.as_ref()) {
                    (Expr::Literal(ScalarValue::Boolean(b)), _)
                        if self.is_boolean_type(&right) =>
                    {
                        Self::boolean_folding_for_or(b, right, true)
                    }
                    (_, Expr::Literal(ScalarValue::Boolean(b)))
                        if self.is_boolean_type(&left) =>
                    {
                        Self::boolean_folding_for_or(b, left, false)
                    }
                    _ => Expr::BinaryExpr {
                        left,
                        op: Operator::Or,
                        right,
                    },
                },
                Operator::And => match (left.as_ref(), right.as_ref()) {
                    (Expr::Literal(ScalarValue::Boolean(b)), _)
                        if self.is_boolean_type(&right) =>
                    {
                        Self::boolean_folding_for_and(b, right, true)
                    }
                    (_, Expr::Literal(ScalarValue::Boolean(b)))
                        if self.is_boolean_type(&left) =>
                    {
                        Self::boolean_folding_for_and(b, left, false)
                    }
                    _ => Expr::BinaryExpr {
                        left,
                        op: Operator::And,
                        right,
                    },
                },
                _ => Expr::BinaryExpr { left, op, right },
            },
            // Not(Not(expr)) --> expr
            Expr::Not(inner) => {
                if let Expr::Not(negated_inner) = *inner {
                    *negated_inner
                } else {
                    Expr::Not(inner)
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
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use chrono::{DateTime, TimeZone, Utc};

    use super::*;
    use crate::logical_plan::{
        and, binary_expr, col, create_udf, lit, lit_timestamp_nano, DFField, Expr,
        LogicalPlanBuilder,
    };
    use crate::physical_plan::functions::{make_scalar_function, BuiltinScalarFunction};
    use crate::physical_plan::udf::ScalarUDF;
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
	        Filter: #test.b > Int32(1)\
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
            Filter: #test.a > Int32(5) AND #test.b < Int32(6)\
            \n  Projection: #test.a\
	        \n    TableScan: test projection=None",
        );
        Ok(())
    }

    #[test]
    fn test_const_evaluator() {
        // true --> true
        test_evaluate(lit(true), lit(true));
        // true or true --> true
        test_evaluate(lit(true).or(lit(true)), lit(true));
        // true or false --> true
        test_evaluate(lit(true).or(lit(false)), lit(true));

        // "foo" == "foo" --> true
        test_evaluate(lit("foo").eq(lit("foo")), lit(true));
        // "foo" != "foo" --> false
        test_evaluate(lit("foo").not_eq(lit("foo")), lit(false));

        // c = 1 --> c = 1
        test_evaluate(col("c").eq(lit(1)), col("c").eq(lit(1)));
        // c = 1 + 2 --> c + 3
        test_evaluate(col("c").eq(lit(1) + lit(2)), col("c").eq(lit(3)));
        // (foo != foo) OR (c = 1) --> false OR (c = 1)
        test_evaluate(
            (lit("foo").not_eq(lit("foo"))).or(col("c").eq(lit(1))),
            lit(false).or(col("c").eq(lit(1))),
        );
    }

    #[test]
    fn test_const_evaluator_scalar_functions() {
        // concat("foo", "bar") --> "foobar"
        let expr = Expr::ScalarFunction {
            args: vec![lit("foo"), lit("bar")],
            fun: BuiltinScalarFunction::Concat,
        };
        test_evaluate(expr, lit("foobar"));

        // ensure arguments are also constant folded
        // concat("foo", concat("bar", "baz")) --> "foobarbaz"
        let concat1 = Expr::ScalarFunction {
            args: vec![lit("bar"), lit("baz")],
            fun: BuiltinScalarFunction::Concat,
        };
        let expr = Expr::ScalarFunction {
            args: vec![lit("foo"), concat1],
            fun: BuiltinScalarFunction::Concat,
        };
        test_evaluate(expr, lit("foobarbaz"));

        // Check non string arguments
        // to_timestamp("2020-09-08T12:00:00+00:00") --> timestamp(1599566400000000000i64)
        let expr = Expr::ScalarFunction {
            args: vec![lit("2020-09-08T12:00:00+00:00")],
            fun: BuiltinScalarFunction::ToTimestamp,
        };
        test_evaluate(expr, lit_timestamp_nano(1599566400000000000i64));

        // check that non foldable arguments are folded
        // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
        let expr = Expr::ScalarFunction {
            args: vec![col("a")],
            fun: BuiltinScalarFunction::ToTimestamp,
        };
        test_evaluate(expr.clone(), expr);

        // check that non foldable arguments are folded
        // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
        let expr = Expr::ScalarFunction {
            args: vec![col("a")],
            fun: BuiltinScalarFunction::ToTimestamp,
        };
        test_evaluate(expr.clone(), expr);

        // volatile / stable functions should not be evaluated
        // rand() + (1 + 2) --> rand() + 3
        let fun = BuiltinScalarFunction::Random;
        assert_eq!(fun.volatility(), Volatility::Volatile);
        let rand = Expr::ScalarFunction { args: vec![], fun };
        let expr = rand.clone() + (lit(1) + lit(2));
        let expected = rand + lit(3);
        test_evaluate(expr, expected);

        // parenthesization matters: can't rewrite
        // (rand() + 1) + 2 --> (rand() + 1) + 2)
        let fun = BuiltinScalarFunction::Random;
        assert_eq!(fun.volatility(), Volatility::Volatile);
        let rand = Expr::ScalarFunction { args: vec![], fun };
        let expr = (rand + lit(1)) + lit(2);
        test_evaluate(expr.clone(), expr);
    }

    #[test]
    fn test_const_evaluator_now() {
        let ts_nanos = 1599566400000000000i64;
        let time = chrono::Utc.timestamp_nanos(ts_nanos);
        let ts_string = "2020-09-08T12:05:00+00:00";

        // now() --> ts
        test_evaluate_with_start_time(now_expr(), lit_timestamp_nano(ts_nanos), &time);

        // CAST(now() as int64) + 100 --> ts + 100
        let expr = cast_to_int64_expr(now_expr()) + lit(100);
        test_evaluate_with_start_time(expr, lit(ts_nanos + 100), &time);

        //  now() < cast(to_timestamp(...) as int) + 50000 ---> true
        let expr = cast_to_int64_expr(now_expr())
            .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000));
        test_evaluate_with_start_time(expr, lit(true), &time);
    }

    fn now_expr() -> Expr {
        Expr::ScalarFunction {
            args: vec![],
            fun: BuiltinScalarFunction::Now,
        }
    }

    fn cast_to_int64_expr(expr: Expr) -> Expr {
        Expr::Cast {
            expr: expr.into(),
            data_type: DataType::Int64,
        }
    }

    fn to_timestamp_expr(arg: impl Into<String>) -> Expr {
        Expr::ScalarFunction {
            args: vec![lit(arg.into())],
            fun: BuiltinScalarFunction::ToTimestamp,
        }
    }

    #[test]
    fn test_evaluator_udfs() {
        let args = vec![lit(1) + lit(2), lit(30) + lit(40)];
        let folded_args = vec![lit(3), lit(70)];

        // immutable UDF should get folded
        // udf_add(1+2, 30+40) --> 73
        let expr = Expr::ScalarUDF {
            args: args.clone(),
            fun: make_udf_add(Volatility::Immutable),
        };
        test_evaluate(expr, lit(73));

        // stable UDF should be entirely folded
        // udf_add(1+2, 30+40) --> 73
        let fun = make_udf_add(Volatility::Stable);
        let expr = Expr::ScalarUDF {
            args: args.clone(),
            fun: Arc::clone(&fun),
        };
        test_evaluate(expr, lit(73));

        // volatile UDF should have args folded
        // udf_add(1+2, 30+40) --> udf_add(3, 70)
        let fun = make_udf_add(Volatility::Volatile);
        let expr = Expr::ScalarUDF {
            args,
            fun: Arc::clone(&fun),
        };
        let expected_expr = Expr::ScalarUDF {
            args: folded_args,
            fun: Arc::clone(&fun),
        };
        test_evaluate(expr, expected_expr);
    }

    // Make a UDF that adds its two values together, with the specified volatility
    fn make_udf_add(volatility: Volatility) -> Arc<ScalarUDF> {
        let input_types = vec![DataType::Int32, DataType::Int32];
        let return_type = Arc::new(DataType::Int32);

        let fun = |args: &[ArrayRef]| {
            let arg0 = &args[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("cast failed");
            let arg1 = &args[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("cast failed");

            // 2. perform the computation
            let array = arg0
                .iter()
                .zip(arg1.iter())
                .map(|args| {
                    if let (Some(arg0), Some(arg1)) = args {
                        Some(arg0 + arg1)
                    } else {
                        // one or both args were Null
                        None
                    }
                })
                .collect::<Int32Array>();

            Ok(Arc::new(array) as ArrayRef)
        };

        let fun = make_scalar_function(fun);
        Arc::new(create_udf(
            "udf_add",
            input_types,
            return_type,
            volatility,
            fun,
        ))
    }

    fn test_evaluate_with_start_time(
        input_expr: Expr,
        expected_expr: Expr,
        date_time: &DateTime<Utc>,
    ) {
        let execution_props = ExecutionProps {
            query_execution_start_time: *date_time,
        };

        let mut const_evaluator = ConstEvaluator::new(&execution_props);
        let evaluated_expr = input_expr
            .clone()
            .rewrite(&mut const_evaluator)
            .expect("successfully evaluated");

        assert_eq!(
            evaluated_expr, expected_expr,
            "Mismatch evaluating {}\n  Expected:{}\n  Got:{}",
            input_expr, expected_expr, evaluated_expr
        );
    }

    fn test_evaluate(input_expr: Expr, expected_expr: Expr) {
        test_evaluate_with_start_time(input_expr, expected_expr, &Utc::now())
    }

    #[test]
    fn simplify_expr_not_not() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        assert_eq!(
            (col("c2").not().not().not()).rewrite(&mut rewriter)?,
            col("c2").not(),
        );

        Ok(())
    }

    #[test]
    fn simplify_expr_null_comparison() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        // x = null is always null
        assert_eq!(
            (lit(true).eq(lit(ScalarValue::Boolean(None)))).rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // null != null is always null
        assert_eq!(
            (lit(ScalarValue::Boolean(None)).not_eq(lit(ScalarValue::Boolean(None))))
                .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // x != null is always null
        assert_eq!(
            (col("c2").not_eq(lit(ScalarValue::Boolean(None)))).rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // null = x is always null
        assert_eq!(
            (lit(ScalarValue::Boolean(None)).eq(col("c2"))).rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        Ok(())
    }

    #[test]
    fn simplify_expr_eq() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        assert_eq!(col("c2").get_type(&schema)?, DataType::Boolean);

        // true = ture -> true
        assert_eq!((lit(true).eq(lit(true))).rewrite(&mut rewriter)?, lit(true),);

        // true = false -> false
        assert_eq!(
            (lit(true).eq(lit(false))).rewrite(&mut rewriter)?,
            lit(false),
        );

        // c2 = true -> c2
        assert_eq!((col("c2").eq(lit(true))).rewrite(&mut rewriter)?, col("c2"),);

        // c2 = false => !c2
        assert_eq!(
            (col("c2").eq(lit(false))).rewrite(&mut rewriter)?,
            col("c2").not(),
        );

        Ok(())
    }

    #[test]
    fn simplify_expr_eq_skip_nonboolean_type() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        // When one of the operand is not of boolean type, folding the other boolean constant will
        // change return type of expression to non-boolean.
        //
        // Make sure c1 column to be used in tests is not boolean type
        assert_eq!(col("c1").get_type(&schema)?, DataType::Utf8);

        // don't fold c1 = true
        assert_eq!(
            (col("c1").eq(lit(true))).rewrite(&mut rewriter)?,
            col("c1").eq(lit(true)),
        );

        // don't fold c1 = false
        assert_eq!(
            (col("c1").eq(lit(false))).rewrite(&mut rewriter)?,
            col("c1").eq(lit(false)),
        );

        // test constant operands
        assert_eq!(
            (lit(1).eq(lit(true))).rewrite(&mut rewriter)?,
            lit(1).eq(lit(true)),
        );

        assert_eq!(
            (lit("a").eq(lit(false))).rewrite(&mut rewriter)?,
            lit("a").eq(lit(false)),
        );

        Ok(())
    }

    #[test]
    fn simplify_expr_not_eq() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        assert_eq!(col("c2").get_type(&schema)?, DataType::Boolean);

        // c2 != true -> !c2
        assert_eq!(
            (col("c2").not_eq(lit(true))).rewrite(&mut rewriter)?,
            col("c2").not(),
        );

        // c2 != false -> c2
        assert_eq!(
            (col("c2").not_eq(lit(false))).rewrite(&mut rewriter)?,
            col("c2"),
        );

        // test constant
        assert_eq!(
            (lit(true).not_eq(lit(true))).rewrite(&mut rewriter)?,
            lit(false),
        );

        assert_eq!(
            (lit(true).not_eq(lit(false))).rewrite(&mut rewriter)?,
            lit(true),
        );

        Ok(())
    }

    #[test]
    fn simplify_expr_not_eq_skip_nonboolean_type() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        // when one of the operand is not of boolean type, folding the other boolean constant will
        // change return type of expression to non-boolean.
        assert_eq!(col("c1").get_type(&schema)?, DataType::Utf8);

        assert_eq!(
            (col("c1").not_eq(lit(true))).rewrite(&mut rewriter)?,
            col("c1").not_eq(lit(true)),
        );

        assert_eq!(
            (col("c1").not_eq(lit(false))).rewrite(&mut rewriter)?,
            col("c1").not_eq(lit(false)),
        );

        // test constants
        assert_eq!(
            (lit(1).not_eq(lit(true))).rewrite(&mut rewriter)?,
            lit(1).not_eq(lit(true)),
        );

        assert_eq!(
            (lit("a").not_eq(lit(false))).rewrite(&mut rewriter)?,
            lit("a").not_eq(lit(false)),
        );

        Ok(())
    }

    #[test]
    fn simplify_expr_case_when_then_else() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        assert_eq!(
            (Box::new(Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit(true))),
                )],
                else_expr: Some(Box::new(col("c2").eq(lit(true)))),
            }))
            .rewrite(&mut rewriter)?,
            Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(col("c2")),
                    Box::new(lit("ok").eq(lit(true)))
                )],
                else_expr: Some(Box::new(col("c2"))),
            }
        );

        Ok(())
    }

    #[test]
    fn simplify_expr_bool_or() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        // col || true is always true
        assert_eq!(
            (col("c2").or(Expr::Literal(ScalarValue::Boolean(Some(true)))))
                .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(Some(true))),
        );

        // col || false is always col
        assert_eq!(
            (col("c2").or(Expr::Literal(ScalarValue::Boolean(Some(false)))))
                .rewrite(&mut rewriter)?,
            col("c2"),
        );

        // true || null is always true
        assert_eq!(
            (Expr::Literal(ScalarValue::Boolean(Some(true)))
                .or(Expr::Literal(ScalarValue::Boolean(None))))
            .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(Some(true))),
        );

        // null || true is always true
        assert_eq!(
            (Expr::Literal(ScalarValue::Boolean(None))
                .or(Expr::Literal(ScalarValue::Boolean(Some(true)))))
            .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(Some(true))),
        );

        // false || null is always null
        assert_eq!(
            (Expr::Literal(ScalarValue::Boolean(Some(false)))
                .or(Expr::Literal(ScalarValue::Boolean(None))))
            .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // null || false is always null
        assert_eq!(
            (Expr::Literal(ScalarValue::Boolean(None))
                .or(Expr::Literal(ScalarValue::Boolean(Some(false)))))
            .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // ( c1 BETWEEN Int32(0) AND Int32(10) ) OR Boolean(NULL)
        // it can be either NULL or  TRUE depending on the value of `c1 BETWEEN Int32(0) AND Int32(10)`
        // and should not be rewritten
        let expr = Expr::Between {
            expr: Box::new(col("c1")),
            negated: false,
            low: Box::new(lit(0)),
            high: Box::new(lit(10)),
        };
        let expr = expr.or(Expr::Literal(ScalarValue::Boolean(None)));
        let result = expr.clone().rewrite(&mut rewriter)?;
        assert_eq!(expr, result);

        Ok(())
    }

    #[test]
    fn simplify_expr_bool_and() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = Simplifier {
            schemas: vec![&schema],
        };

        // col & true is always col
        assert_eq!(
            (col("c2").and(Expr::Literal(ScalarValue::Boolean(Some(true)))))
                .rewrite(&mut rewriter)?,
            col("c2"),
        );
        // col & false is always false
        assert_eq!(
            (col("c2").and(Expr::Literal(ScalarValue::Boolean(Some(false)))))
                .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(Some(false))),
        );

        // true && null is always null
        assert_eq!(
            (Expr::Literal(ScalarValue::Boolean(Some(true)))
                .and(Expr::Literal(ScalarValue::Boolean(None))))
            .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // null && true is always null
        assert_eq!(
            (Expr::Literal(ScalarValue::Boolean(None))
                .and(Expr::Literal(ScalarValue::Boolean(Some(true)))))
            .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // false && null is always false
        assert_eq!(
            (Expr::Literal(ScalarValue::Boolean(Some(false)))
                .and(Expr::Literal(ScalarValue::Boolean(None))))
            .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(Some(false))),
        );

        // null && false is always false
        assert_eq!(
            (Expr::Literal(ScalarValue::Boolean(None))
                .and(Expr::Literal(ScalarValue::Boolean(Some(false)))))
            .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(Some(false))),
        );

        // c1 BETWEEN Int32(0) AND Int32(10) AND Boolean(NULL)
        // it can be either NULL or FALSE depending on the value of `c1 BETWEEN Int32(0) AND Int32(10`
        // and should not be rewritten
        let expr = Expr::Between {
            expr: Box::new(col("c1")),
            negated: false,
            low: Box::new(lit(0)),
            high: Box::new(lit(10)),
        };
        let expr = expr.and(Expr::Literal(ScalarValue::Boolean(None)));
        let result = expr.clone().rewrite(&mut rewriter)?;
        assert_eq!(expr, result);

        Ok(())
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new(vec![
                DFField::new(None, "c1", DataType::Utf8, true),
                DFField::new(None, "c2", DataType::Boolean, true),
            ])
            .unwrap(),
        )
    }
}
