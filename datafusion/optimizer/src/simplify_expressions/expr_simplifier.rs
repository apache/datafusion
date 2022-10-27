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

//! Expression simplification API

use super::utils::*;
use crate::type_coercion::TypeCoercionRewriter;
use arrow::{
    array::new_null_array,
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    and,
    expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion},
    lit, or, BinaryExpr, BuiltinScalarFunction, ColumnarValue, Expr, Volatility,
};
use datafusion_physical_expr::{create_physical_expr, execution_props::ExecutionProps};

use super::SimplifyInfo;

/// This structure handles API for expression simplification
pub struct ExprSimplifier<S> {
    info: S,
}

impl<S: SimplifyInfo> ExprSimplifier<S> {
    /// Create a new `ExprSimplifier` with the given `info` such as an
    /// instance of [`SimplifyContext`]. See
    /// [`simplify`](Self::simplify) for an example.
    pub fn new(info: S) -> Self {
        Self { info }
    }

    /// Simplifies this [`Expr`]`s as much as possible, evaluating
    /// constants and applying algebraic simplifications.
    ///
    /// The types of the expression must match what operators expect,
    /// or else an error may occur trying to evaluate. See
    /// [`coerce`](Self::coerce) for a function to help.
    ///
    /// # Example:
    ///
    /// `b > 2 AND b > 2`
    ///
    /// can be written to
    ///
    /// `b > 2`
    ///
    /// ```
    /// use datafusion_expr::{col, lit, Expr};
    /// use datafusion_common::Result;
    /// use datafusion_physical_expr::execution_props::ExecutionProps;
    /// use datafusion_optimizer::simplify_expressions::{ExprSimplifier, SimplifyInfo};
    ///
    /// /// Simple implementation that provides `Simplifier` the information it needs
    /// /// See SimplifyContext for a structure that does this.
    /// #[derive(Default)]
    /// struct Info {
    ///   execution_props: ExecutionProps,
    /// };
    ///
    /// impl SimplifyInfo for Info {
    ///   fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
    ///     Ok(false)
    ///   }
    ///   fn nullable(&self, expr: &Expr) -> Result<bool> {
    ///     Ok(true)
    ///   }
    ///   fn execution_props(&self) -> &ExecutionProps {
    ///     &self.execution_props
    ///   }
    /// }
    ///
    /// // Create the simplifier
    /// let simplifier = ExprSimplifier::new(Info::default());
    ///
    /// // b < 2
    /// let b_lt_2 = col("b").gt(lit(2));
    ///
    /// // (b < 2) OR (b < 2)
    /// let expr = b_lt_2.clone().or(b_lt_2.clone());
    ///
    /// // (b < 2) OR (b < 2) --> (b < 2)
    /// let expr = simplifier.simplify(expr).unwrap();
    /// assert_eq!(expr, b_lt_2);
    /// ```
    pub fn simplify(&self, expr: Expr) -> Result<Expr> {
        let mut simplifier = Simplifier::new(&self.info);
        let mut const_evaluator = ConstEvaluator::try_new(self.info.execution_props())?;

        // TODO iterate until no changes are made during rewrite
        // (evaluating constants can enable new simplifications and
        // simplifications can enable new constant evaluation)
        // https://github.com/apache/arrow-datafusion/issues/1160
        expr.rewrite(&mut const_evaluator)?
            .rewrite(&mut simplifier)?
            // run both passes twice to try an minimize simplifications that we missed
            .rewrite(&mut const_evaluator)?
            .rewrite(&mut simplifier)
    }

    /// Apply type coercion to an [`Expr`] so that it can be
    /// evaluated as a [`PhysicalExpr`](datafusion_physical_expr::PhysicalExpr).
    ///
    /// See the [type coercion module](datafusion_expr::type_coercion)
    /// documentation for more details on type coercion
    ///
    // Would be nice if this API could use the SimplifyInfo
    // rather than creating an DFSchemaRef coerces rather than doing
    // it manually.
    // https://github.com/apache/arrow-datafusion/issues/3793
    pub fn coerce(&self, expr: Expr, schema: DFSchemaRef) -> Result<Expr> {
        let mut expr_rewrite = TypeCoercionRewriter { schema };

        expr.rewrite(&mut expr_rewrite)
    }
}

#[allow(rustdoc::private_intra_doc_links)]
/// Partially evaluate `Expr`s so constant subtrees are evaluated at plan time.
///
/// Note it does not handle algebraic rewrites such as `(a or false)`
/// --> `a`, which is handled by [`Simplifier`]
struct ConstEvaluator<'a> {
    /// can_evaluate is used during the depth-first-search of the
    /// Expr tree to track if any siblings (or their descendants) were
    /// non evaluatable (e.g. had a column reference or volatile
    /// function)
    ///
    /// Specifically, `can_evaluate[N]` represents the state of
    /// traversal when we are N levels deep in the tree, one entry for
    /// this Expr and each of its parents.
    ///
    /// After visiting all siblings if can_evauate.top() is true, that
    /// means there were no non evaluatable siblings (or their
    /// descendants) so this Expr can be evaluated
    can_evaluate: Vec<bool>,

    execution_props: &'a ExecutionProps,
    input_schema: DFSchema,
    input_batch: RecordBatch,
}

impl<'a> ExprRewriter for ConstEvaluator<'a> {
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        // Default to being able to evaluate this node
        self.can_evaluate.push(true);

        // if this expr is not ok to evaluate, mark entire parent
        // stack as not ok (as all parents have at least one child or
        // descendant that can not be evaluated

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
        match self.can_evaluate.pop() {
            Some(true) => Ok(Expr::Literal(self.evaluate_to_scalar(expr)?)),
            Some(false) => Ok(expr),
            _ => Err(DataFusionError::Internal(
                "Failed to pop can_evaluate".to_string(),
            )),
        }
    }
}

impl<'a> ConstEvaluator<'a> {
    /// Create a new `ConstantEvaluator`. Session constants (such as
    /// the time for `now()` are taken from the passed
    /// `execution_props`.
    pub fn try_new(execution_props: &'a ExecutionProps) -> Result<Self> {
        // The dummy column name is unused and doesn't matter as only
        // expressions without column references can be evaluated
        static DUMMY_COL_NAME: &str = ".";
        let schema = Schema::new(vec![Field::new(DUMMY_COL_NAME, DataType::Null, true)]);
        let input_schema = DFSchema::try_from(schema.clone())?;
        // Need a single "input" row to produce a single output row
        let col = new_null_array(&DataType::Null, 1);
        let input_batch = RecordBatch::try_new(std::sync::Arc::new(schema), vec![col])?;

        Ok(Self {
            can_evaluate: vec![],
            execution_props,
            input_schema,
            input_batch,
        })
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
            Expr::Alias(..)
            | Expr::AggregateFunction { .. }
            | Expr::AggregateUDF { .. }
            | Expr::ScalarVariable(_, _)
            | Expr::Column(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::WindowFunction { .. }
            | Expr::Sort { .. }
            | Expr::GroupingSet(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. } => false,
            Expr::ScalarFunction { fun, .. } => Self::volatility_ok(fun.volatility()),
            Expr::ScalarUDF { fun, .. } => Self::volatility_ok(fun.signature.volatility),
            Expr::Literal(_)
            | Expr::BinaryExpr { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::Case(_)
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::InList { .. }
            | Expr::GetIndexedField { .. } => true,
        }
    }

    /// Internal helper to evaluates an Expr
    pub(crate) fn evaluate_to_scalar(&mut self, expr: Expr) -> Result<ScalarValue> {
        if let Expr::Literal(s) = expr {
            return Ok(s);
        }

        let phys_expr = create_physical_expr(
            &expr,
            &self.input_schema,
            &self.input_batch.schema(),
            self.execution_props,
        )?;
        let col_val = phys_expr.evaluate(&self.input_batch)?;
        match col_val {
            ColumnarValue::Array(a) => {
                if a.len() != 1 {
                    Err(DataFusionError::Execution(format!(
                        "Could not evaluate the expression, found a result of length {}",
                        a.len()
                    )))
                } else {
                    Ok(ScalarValue::try_from_array(&a, 0)?)
                }
            }
            ColumnarValue::Scalar(s) => Ok(s),
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
struct Simplifier<'a, S> {
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
        use datafusion_expr::Operator::{And, Divide, Eq, Modulo, Multiply, NotEq, Or};

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
                return Err(DataFusionError::ArrowError(ArrowError::DivideByZero));
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
                return Err(DataFusionError::ArrowError(ArrowError::DivideByZero));
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
    use std::{collections::HashMap, sync::Arc};

    use crate::simplify_expressions::{
        utils::for_test::{cast_to_int64_expr, now_expr, to_timestamp_expr},
        SimplifyContext,
    };

    use super::*;
    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema},
    };
    use chrono::{DateTime, TimeZone, Utc};
    use datafusion_common::{DFField, ToDFSchema};
    use datafusion_expr::*;
    use datafusion_physical_expr::{
        execution_props::ExecutionProps, functions::make_scalar_function,
    };

    // ------------------------------
    // --- ExprSimplifier tests -----
    // ------------------------------
    #[test]
    fn api_basic() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        let expr = lit(1) + lit(2);
        let expected = lit(3);
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    #[test]
    fn basic_coercion() {
        let schema = test_schema();
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(schema.clone()));

        // Note expr type is int32 (not int64)
        // (1i64 + 2i32) < i
        let expr = (lit(1i64) + lit(2i32)).lt(col("i"));
        // should fully simplify to 3 < i (though i has been coerced to i64)
        let expected = lit(3i64).lt(col("i"));

        // Would be nice if this API could use the SimplifyInfo
        // rather than creating an DFSchemaRef coerces rather than doing
        // it manually.
        // https://github.com/apache/arrow-datafusion/issues/3793
        let expr = simplifier.coerce(expr, schema).unwrap();

        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    fn test_schema() -> DFSchemaRef {
        Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("b", DataType::Boolean, true),
        ])
        .to_dfschema_ref()
        .unwrap()
    }

    #[test]
    fn simplify_and_constant_prop() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        // should be able to simplify to false
        // (i * (1 - 2)) > 0
        let expr = (col("i") * (lit(1) - lit(1))).gt(lit(0));
        let expected = lit(false);
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    #[test]
    fn simplify_and_constant_prop_with_case() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        //   CASE
        //     WHEN i>5 AND false THEN i > 5
        //     WHEN i<5 AND true THEN i < 5
        //     ELSE false
        //   END
        //
        // Can be simplified to `i < 5`
        let expr = when(col("i").gt(lit(5)).and(lit(false)), col("i").gt(lit(5)))
            .when(col("i").lt(lit(5)).and(lit(true)), col("i").lt(lit(5)))
            .otherwise(lit(false))
            .unwrap();
        let expected = col("i").lt(lit(5));
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    // ------------------------------
    // --- ConstEvaluator tests -----
    // ------------------------------
    fn test_evaluate_with_start_time(
        input_expr: Expr,
        expected_expr: Expr,
        date_time: &DateTime<Utc>,
    ) {
        let execution_props = ExecutionProps {
            query_execution_start_time: *date_time,
            var_providers: None,
        };

        let mut const_evaluator = ConstEvaluator::try_new(&execution_props).unwrap();
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
        let expr = call_fn("concat", vec![lit("foo"), lit("bar")]).unwrap();
        test_evaluate(expr, lit("foobar"));

        // ensure arguments are also constant folded
        // concat("foo", concat("bar", "baz")) --> "foobarbaz"
        let concat1 = call_fn("concat", vec![lit("bar"), lit("baz")]).unwrap();
        let expr = call_fn("concat", vec![lit("foo"), concat1]).unwrap();
        test_evaluate(expr, lit("foobarbaz"));

        // Check non string arguments
        // to_timestamp("2020-09-08T12:00:00+00:00") --> timestamp(1599566400000000000i64)
        let expr =
            call_fn("to_timestamp", vec![lit("2020-09-08T12:00:00+00:00")]).unwrap();
        test_evaluate(expr, lit_timestamp_nano(1599566400000000000i64));

        // check that non foldable arguments are folded
        // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
        let expr = call_fn("to_timestamp", vec![col("a")]).unwrap();
        test_evaluate(expr.clone(), expr);

        // check that non foldable arguments are folded
        // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
        let expr = call_fn("to_timestamp", vec![col("a")]).unwrap();
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

        // CAST(now() as int64) + 100_i64 --> ts + 100_i64
        let expr = cast_to_int64_expr(now_expr()) + lit(100_i64);
        test_evaluate_with_start_time(expr, lit(ts_nanos + 100), &time);

        //  CAST(now() as int64) < cast(to_timestamp(...) as int64) + 50000_i64 ---> true
        let expr = cast_to_int64_expr(now_expr())
            .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000i64));
        test_evaluate_with_start_time(expr, lit(true), &time);
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

    // ------------------------------
    // --- Simplifier tests -----
    // ------------------------------

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
}
