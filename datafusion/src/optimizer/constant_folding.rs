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

//! This module contains an optimizer which performs boolean simplification and constant folding

use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::{DFSchemaRef, Expr, LogicalPlan, Operator};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::physical_plan::expressions::helpers::evaluate;
use crate::physical_plan::functions::{BuiltinScalarFunction, Volatility};
use crate::scalar::ScalarValue;

struct ConstantRewriter<'a> {
    execution_props: &'a ExecutionProps,
    schemas: Vec<&'a DFSchemaRef>,
}

/// Optimizer that evaluates scalar expressions and simplifies comparison expressions involving boolean literals.
///
/// Recursively go through all expressions and simplify the following cases:
/// * `expr = true` and `expr != false` to `expr` when `expr` is of boolean type
/// * `expr = false` and `expr != true` to `!expr` when `expr` is of boolean type
/// * `true = true` and `false = false` to `true`
/// * `false = true` and `true = false` to `false`
/// * `!!expr` to `expr`
/// * `expr = null` and `expr != null` to `null`
pub struct ConstantFolding {}

impl ConstantFolding {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
impl OptimizerRule for ConstantFolding {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        let mut rewriter = ConstantRewriter {
            execution_props,
            schemas: plan.all_schemas(),
        };

        match plan {
            LogicalPlan::Filter { predicate, input } => Ok(LogicalPlan::Filter {
                predicate: rewriter.rewrite(predicate.clone()),
                input: match self.optimize(input, execution_props) {
                    Ok(plan) => Arc::new(plan),
                    _ => input.clone(),
                },
            }),
            // Rest: recurse into plan, apply optimization where possible
            LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin { .. } => {
                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| match self.optimize(plan, execution_props) {
                        Ok(opt_plan) => opt_plan,
                        _ => (*plan).clone(),
                    })
                    .collect::<Vec<_>>();

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| rewriter.rewrite(e))
                    .collect::<Vec<_>>();

                utils::from_plan(plan, &expr, &new_inputs)
            }
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation { .. } => {
                Ok(plan.clone())
            }
        }
    }

    fn name(&self) -> &str {
        "const_folder"
    }
}

impl<'a> ConstantRewriter<'a> {
    fn is_boolean_type(&self, expr: &Expr) -> bool {
        for schema in &self.schemas {
            if let Ok(DataType::Boolean) = expr.get_type(schema) {
                return true;
            }
        }

        false
    }

    pub fn rewrite(&mut self, mut expr: Expr) -> Expr {
        let name = match &expr {
            Expr::Alias(_, name) => Some(name.clone()),
            _ => None,
        };

        let rewrite_root = self.rewrite_const_expr(&mut expr);
        if rewrite_root {
            match evaluate(&expr) {
                Ok(s) => expr = Expr::Literal(s),
                Err(_) => return expr,
            }
        }
        match name {
            Some(name) => {
                let existing_alias = match &expr {
                    Expr::Alias(_, new_alias) => Some(new_alias.as_str()),
                    _ => None,
                };
                let apply_new_alias = match existing_alias {
                    Some(new) => *new != name,
                    None => true,
                };
                if apply_new_alias {
                    expr = Expr::Alias(Box::new(expr), name);
                }
                expr
            }
            None => expr,
        }
    }

    ///Evaluates all literal expressions in the list.
    fn const_fold_list_eager(&mut self, args: &mut Vec<Expr>) {
        for arg in args.iter_mut() {
            if self.rewrite_const_expr(arg) {
                if let Ok(s) = evaluate(arg) {
                    *arg = Expr::Literal(s);
                }
            }
        }
    }
    ///Tests to see if the list passed in is all literal expressions, if they are then it returns true.
    ///If some expressions are not literal then the literal expressions are evaluated and it returns false.
    fn const_fold_list(&mut self, args: &mut Vec<Expr>) -> bool {
        let can_rewrite = args
            .iter_mut()
            .map(|e| self.rewrite_const_expr(e))
            .collect::<Vec<bool>>();
        if can_rewrite.iter().all(|f| *f) {
            return true;
        } else {
            for (rewrite_expr, expr) in can_rewrite.iter().zip(args) {
                if *rewrite_expr {
                    if let Ok(s) = evaluate(expr) {
                        *expr = Expr::Literal(s);
                    }
                }
            }
        }
        false
    }
    ///This attempts to simplify expressions of the form col(Boolean) = Boolean and col(Boolean) != Boolean
    /// e.g. col(Boolean) = Some(true) -> col(Boolean). It also handles == and != between two boolean literals as
    /// the binary operator physical expression currently doesn't handle them.

    fn binary_column_const_fold(
        &mut self,
        left: &mut Box<Expr>,
        op: &Operator,
        right: &mut Box<Expr>,
    ) -> Option<Expr> {
        let expr = match (left.as_ref(), op, right.as_ref()) {
            (
                Expr::Literal(ScalarValue::Boolean(l)),
                Operator::Eq,
                Expr::Literal(ScalarValue::Boolean(r)),
            ) => {
                let literal_bool = Expr::Literal(ScalarValue::Boolean(match (l, r) {
                    (Some(l), Some(r)) => Some(*l == *r),
                    _ => None,
                }));
                Some(literal_bool)
            }
            (
                Expr::Literal(ScalarValue::Boolean(l)),
                Operator::NotEq,
                Expr::Literal(ScalarValue::Boolean(r)),
            ) => {
                let literal_bool = match (l, r) {
                    (Some(l), Some(r)) => {
                        Expr::Literal(ScalarValue::Boolean(Some(l != r)))
                    }
                    _ => Expr::Literal(ScalarValue::Boolean(None)),
                };
                Some(literal_bool)
            }
            (Expr::Literal(ScalarValue::Boolean(b)), Operator::Eq, col)
            | (col, Operator::Eq, Expr::Literal(ScalarValue::Boolean(b)))
                if self.is_boolean_type(col) =>
            {
                Some(match b {
                    Some(true) => col.clone(),
                    Some(false) => Expr::Not(Box::new(col.clone())),
                    None => Expr::Literal(ScalarValue::Boolean(None)),
                })
            }
            (Expr::Literal(ScalarValue::Boolean(b)), Operator::NotEq, col)
            | (col, Operator::NotEq, Expr::Literal(ScalarValue::Boolean(b)))
                if self.is_boolean_type(col) =>
            {
                Some(match b {
                    Some(true) => Expr::Not(Box::new(col.clone())),
                    Some(false) => col.clone(),
                    None => Expr::Literal(ScalarValue::Boolean(None)),
                })
            }
            _ => None,
        };
        expr
    }

    fn rewrite_const_expr(&mut self, expr: &mut Expr) -> bool {
        let can_rewrite = match expr {
            Expr::Alias(e, _) => self.rewrite_const_expr(e),
            Expr::Column(_) => false,
            Expr::ScalarVariable(_) => false,
            Expr::Literal(_) => true,
            Expr::BinaryExpr { left, op, right } => {
                //Check if left and right are const, much like the Not Not optimization this is done first to make sure any
                //Non-scalar execution optimizations, such as col<boolean>("test") = NULL->false are performed first
                let left_const = self.rewrite_const_expr(left);
                let right_const = self.rewrite_const_expr(right);
                let mut can_rewrite = match (left_const, right_const) {
                    (true, true) => true,
                    (false, false) => false,
                    (true, false) => {
                        if let Ok(s) = evaluate(left) {
                            *left.as_mut() = Expr::Literal(s);
                        }
                        false
                    }
                    (false, true) => {
                        if let Ok(s) = evaluate(right) {
                            *right.as_mut() = Expr::Literal(s);
                        }
                        false
                    }
                };

                can_rewrite = match self.binary_column_const_fold(left, op, right) {
                    Some(e) => {
                        let expr: &mut Expr = expr;
                        *expr = e;
                        self.rewrite_const_expr(expr)
                    }
                    None => can_rewrite,
                };

                can_rewrite
            }

            Expr::Not(e) => {
                //Check if the expression can be rewritten. This may trigger simplifications such as col("b") = false -> NOT col("b")
                //Then check if inner expression is Not and if so replace expr with the inner
                let can_rewrite = self.rewrite_const_expr(e);
                match e.as_mut() {
                    Expr::Not(inner) => {
                        let inner = std::mem::replace(inner.as_mut(), Expr::Wildcard);
                        *expr = inner;
                        self.rewrite_const_expr(expr)
                    }
                    _ => can_rewrite,
                }
            }
            Expr::IsNotNull(e) => self.rewrite_const_expr(e),
            Expr::IsNull(e) => self.rewrite_const_expr(e),
            Expr::Negative(e) => self.rewrite_const_expr(e),
            Expr::Between {
                expr, low, high, ..
            } => match (
                self.rewrite_const_expr(expr),
                self.rewrite_const_expr(low),
                self.rewrite_const_expr(high),
            ) {
                (true, true, true) => true,
                (expr_const, low_const, high_const) => {
                    if expr_const {
                        if let Ok(s) = evaluate(expr) {
                            let expr: &mut Expr = expr;
                            *expr = Expr::Literal(s);
                        }
                    }
                    if low_const {
                        if let Ok(s) = evaluate(expr) {
                            let expr: &mut Expr = expr;
                            *expr = Expr::Literal(s);
                        }
                    }
                    if high_const {
                        if let Ok(s) = evaluate(expr) {
                            let expr: &mut Expr = expr;
                            *expr = Expr::Literal(s);
                        }
                    }
                    false
                }
            },
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                if expr
                    .as_mut()
                    .map(|e| self.rewrite_const_expr(e))
                    .unwrap_or(false)
                {
                    let expr_inner = expr.as_mut().unwrap();
                    if let Ok(s) = evaluate(expr_inner) {
                        *expr_inner.as_mut() = Expr::Literal(s);
                    }
                }

                if else_expr
                    .as_mut()
                    .map(|e| self.rewrite_const_expr(e))
                    .unwrap_or(false)
                {
                    let expr_inner = else_expr.as_mut().unwrap();
                    if let Ok(s) = evaluate(expr_inner) {
                        *expr_inner.as_mut() = Expr::Literal(s);
                    }
                }

                for (when, then) in when_then_expr {
                    let when: &mut Expr = when;
                    let then: &mut Expr = then;
                    if self.rewrite_const_expr(when) {
                        if let Ok(s) = evaluate(when) {
                            *when = Expr::Literal(s);
                        }
                    }
                    if self.rewrite_const_expr(then) {
                        if let Ok(s) = evaluate(then) {
                            *then = Expr::Literal(s);
                        }
                    }
                }
                false
            }
            Expr::Cast { expr, .. } => self.rewrite_const_expr(expr),
            Expr::TryCast { expr, .. } => self.rewrite_const_expr(expr),
            Expr::Sort { expr, .. } => {
                if self.rewrite_const_expr(expr) {
                    if let Ok(s) = evaluate(expr) {
                        let expr: &mut Expr = expr;
                        *expr = Expr::Literal(s);
                    }
                }
                false
            }
            Expr::ScalarFunction {
                fun: BuiltinScalarFunction::Now,
                ..
            } => {
                *expr = Expr::Literal(ScalarValue::TimestampNanosecond(Some(
                    self.execution_props
                        .query_execution_start_time
                        .timestamp_nanos(),
                )));
                true
            }
            Expr::ScalarFunction { fun, args } => {
                if args.is_empty() {
                    false
                } else {
                    let volatility = fun.volatility();
                    match volatility {
                        Volatility::Immutable => self.const_fold_list(args),
                        _ => {
                            self.const_fold_list_eager(args);
                            false
                        }
                    }
                }
            }
            Expr::ScalarUDF { fun, args } => {
                if args.is_empty() {
                    false
                } else {
                    let volatility = fun.volatility();
                    match volatility {
                        Volatility::Immutable => self.const_fold_list(args),
                        _ => {
                            self.const_fold_list_eager(args);
                            false
                        }
                    }
                }
            }
            Expr::AggregateFunction { args, .. } => {
                self.const_fold_list_eager(args);
                false
            }
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                self.const_fold_list_eager(args);
                self.const_fold_list_eager(partition_by);
                self.const_fold_list_eager(order_by);
                false
            }
            Expr::AggregateUDF { args, .. } => {
                self.const_fold_list_eager(args);
                false
            }
            Expr::InList { expr, list, .. } => {
                let expr_const = self.rewrite_const_expr(expr);
                let list_literals = self.const_fold_list(list);
                match (expr_const, list_literals) {
                    (true, true) => true,

                    (false, false) => false,
                    (true, false) => {
                        if let Ok(s) = evaluate(expr) {
                            let expr: &mut Expr = expr;
                            *expr = Expr::Literal(s);
                        }
                        false
                    }
                    (false, true) => {
                        self.const_fold_list_eager(list);
                        false
                    }
                }
            }
            Expr::Wildcard => false,
        };
        can_rewrite
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        logical_plan::{
            abs, col, create_udf, lit, max, min, DFField, DFSchema, LogicalPlanBuilder,
        },
        physical_plan::{functions::make_scalar_function, udf::ScalarUDF},
    };
    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::{Field, Schema};

    use chrono::{DateTime, Utc};

    fn test_table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::UInt32, false),
            Field::new("e", DataType::Float64, false),
        ]);
        LogicalPlanBuilder::scan_empty(Some("test"), &schema, None)?.build()
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

    #[test]
    fn optimize_expr_not_not() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
        };

        assert_eq!(
            rewriter.rewrite(col("c2").not().not().not()),
            col("c2").not(),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_null_comparison() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
        };

        // x = null is always null
        assert_eq!(
            rewriter.rewrite(lit(true).eq(lit(ScalarValue::Boolean(None)))),
            lit(ScalarValue::Boolean(None)),
        );

        // null != null is always null
        assert_eq!(
            rewriter.rewrite(
                lit(ScalarValue::Boolean(None)).not_eq(lit(ScalarValue::Boolean(None)))
            ),
            lit(ScalarValue::Boolean(None)),
        );

        // x != null is always null
        assert_eq!(
            rewriter.rewrite(col("c2").not_eq(lit(ScalarValue::Boolean(None)))),
            lit(ScalarValue::Boolean(None)),
        );

        // null = x is always null
        assert_eq!(
            rewriter.rewrite(lit(ScalarValue::Boolean(None)).eq(col("c2"))),
            lit(ScalarValue::Boolean(None)),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_eq() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
        };

        assert_eq!(col("c2").get_type(&schema)?, DataType::Boolean);

        // true = ture -> true
        assert_eq!(rewriter.rewrite(lit(true).eq(lit(true))), lit(true),);

        // true = false -> false
        assert_eq!(rewriter.rewrite(lit(true).eq(lit(false))), lit(false),);

        // c2 = true -> c2
        assert_eq!(rewriter.rewrite(col("c2").eq(lit(true))), col("c2"),);

        // c2 = false => !c2
        assert_eq!(rewriter.rewrite(col("c2").eq(lit(false))), col("c2").not(),);

        Ok(())
    }

    #[test]
    fn optimize_expr_eq_skip_nonboolean_type() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
        };

        // When one of the operand is not of boolean type, folding the other boolean constant will
        // change return type of expression to non-boolean.
        //
        // Make sure c1 column to be used in tests is not boolean type
        assert_eq!(col("c1").get_type(&schema)?, DataType::Utf8);

        // don't fold c1 = true
        assert_eq!(
            rewriter.rewrite(col("c1").eq(lit(true))),
            col("c1").eq(lit(true)),
        );

        // don't fold c1 = false
        assert_eq!(
            rewriter.rewrite(col("c1").eq(lit(false))),
            col("c1").eq(lit(false)),
        );

        // test constant operands
        assert_eq!(rewriter.rewrite(lit(1).eq(lit(true))), lit(1).eq(lit(true)),);

        assert_eq!(
            rewriter.rewrite(lit("a").eq(lit(false))),
            lit("a").eq(lit(false)),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_not_eq() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
        };

        assert_eq!(col("c2").get_type(&schema)?, DataType::Boolean);

        // c2 != true -> !c2
        assert_eq!(
            rewriter.rewrite(col("c2").not_eq(lit(true))),
            col("c2").not(),
        );

        // c2 != false -> c2
        assert_eq!(rewriter.rewrite(col("c2").not_eq(lit(false))), col("c2"),);

        // test constant
        assert_eq!(rewriter.rewrite(lit(true).not_eq(lit(true))), lit(false),);

        assert_eq!(rewriter.rewrite(lit(true).not_eq(lit(false))), lit(true),);

        Ok(())
    }

    #[test]
    fn optimize_expr_not_eq_skip_nonboolean_type() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
        };

        // when one of the operand is not of boolean type, folding the other boolean constant will
        // change return type of expression to non-boolean.
        assert_eq!(col("c1").get_type(&schema)?, DataType::Utf8);

        assert_eq!(
            rewriter.rewrite(col("c1").not_eq(lit(true))),
            col("c1").not_eq(lit(true)),
        );

        assert_eq!(
            rewriter.rewrite(col("c1").not_eq(lit(false))),
            col("c1").not_eq(lit(false)),
        );

        // test constants
        assert_eq!(
            rewriter.rewrite(lit(1).not_eq(lit(true))),
            lit(1).not_eq(lit(true)),
        );

        assert_eq!(
            rewriter.rewrite(lit("a").not_eq(lit(false))),
            lit("a").not_eq(lit(false)),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_case_when_then_else() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
        };

        assert_eq!(
            rewriter.rewrite(Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit(true))),
                )],
                else_expr: Some(Box::new(col("c2").eq(lit(true)))),
            }),
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

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = ConstantFolding::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn optimize_plan_eq_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(true)))?
            .filter(col("c").eq(lit(false)))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #test.a\
        \n  Filter: NOT #test.c\
        \n    Filter: #test.b\
        \n      TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_not_eq_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)))?
            .filter(col("c").not_eq(lit(false)))?
            .limit(1)?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #test.a\
        \n  Limit: 1\
        \n    Filter: #test.c\
        \n      Filter: NOT #test.b\
        \n        TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_and_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).and(col("c").eq(lit(true))))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #test.a\
        \n  Filter: NOT #test.b AND #test.c\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_or_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).or(col("c").eq(lit(false))))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #test.a\
        \n  Filter: NOT #test.b OR NOT #test.c\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_not_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(false)).not())?
            .project(vec![col("a")])?
            .build()?;
        let expected = "\
        Projection: #test.a\
        \n  Filter: #test.b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_support_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("d"), col("b").eq(lit(false))])?
            .build()?;

        let expected = "\
        Projection: #test.a, #test.d, NOT #test.b\
        \n  TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_support_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("c"), col("b")])?
            .aggregate(
                vec![col("a"), col("c")],
                vec![max(col("b").eq(lit(true))), min(col("b"))],
            )?
            .build()?;

        let expected = "\
        Aggregate: groupBy=[[#test.a, #test.c]], aggr=[[MAX(#test.b), MIN(#test.b)]]\
        \n  Projection: #test.a, #test.c, #test.b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    fn get_optimized_plan_formatted(
        plan: &LogicalPlan,
        date_time: &DateTime<Utc>,
    ) -> String {
        let rule = ConstantFolding::new();
        let execution_props = ExecutionProps {
            query_execution_start_time: *date_time,
        };

        let optimized_plan = rule
            .optimize(plan, &execution_props)
            .expect("failed to optimize plan");
        return format!("{:?}", optimized_plan);
    }

    #[test]
    fn to_timestamp_expr() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::ScalarFunction {
            args: vec![Expr::Literal(ScalarValue::Utf8(Some(
                "2020-09-08T12:00:00+00:00".to_string(),
            )))],
            fun: BuiltinScalarFunction::ToTimestamp,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: TimestampNanosecond(1599566400000000000)\
            \n  TableScan: test projection=None"
            .to_string();
        let actual = get_optimized_plan_formatted(&plan, &chrono::Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_timestamp_expr_wrong_arg() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::ScalarFunction {
            args: vec![Expr::Literal(ScalarValue::Utf8(Some(
                "I'M NOT A TIMESTAMP".to_string(),
            )))],
            fun: BuiltinScalarFunction::ToTimestamp,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: totimestamp(Utf8(\"I\'M NOT A TIMESTAMP\"))\
            \n  TableScan: test projection=None";
        let actual = get_optimized_plan_formatted(&plan, &chrono::Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_timestamp_expr_no_arg() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::ScalarFunction {
            args: vec![],
            fun: BuiltinScalarFunction::ToTimestamp,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: totimestamp()\
            \n  TableScan: test projection=None";
        let actual = get_optimized_plan_formatted(&plan, &chrono::Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn cast_expr() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Utf8(Some("0".to_string())))),
            data_type: DataType::Int32,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: Int32(0)\
            \n  TableScan: test projection=None";
        let actual = get_optimized_plan_formatted(&plan, &chrono::Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn cast_expr_wrong_arg() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::TryCast {
            expr: Box::new(Expr::Literal(ScalarValue::Utf8(Some("".to_string())))),
            data_type: DataType::Int32,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: Int32(NULL)\
            \n  TableScan: test projection=None";
        let actual = get_optimized_plan_formatted(&plan, &chrono::Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn single_now_expr() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::ScalarFunction {
            args: vec![],
            fun: BuiltinScalarFunction::Now,
        }];
        let time = chrono::Utc::now();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = format!(
            "Projection: TimestampNanosecond({})\
            \n  TableScan: test projection=None",
            time.timestamp_nanos()
        );
        let actual = get_optimized_plan_formatted(&plan, &time);

        assert_eq!(expected, actual);
    }

    #[test]
    fn multiple_now_expr() {
        let table_scan = test_table_scan().unwrap();
        let time = chrono::Utc::now();
        let proj = vec![
            Expr::ScalarFunction {
                args: vec![],
                fun: BuiltinScalarFunction::Now,
            },
            Expr::Alias(
                Box::new(Expr::ScalarFunction {
                    args: vec![],
                    fun: BuiltinScalarFunction::Now,
                }),
                "t2".to_string(),
            ),
        ];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected = format!(
            "Projection: TimestampNanosecond({}), TimestampNanosecond({}) AS t2\
            \n  TableScan: test projection=None",
            time.timestamp_nanos(),
            time.timestamp_nanos()
        );

        assert_eq!(actual, expected);
    }

    fn create_pow_with_volatilty(volatilty: Volatility) -> ScalarUDF {
        let pow = |args: &[ArrayRef]| {
            assert_eq!(args.len(), 2);

            let base = &args[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("cast failed");
            let exponent = &args[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("cast failed");

            assert_eq!(exponent.len(), base.len());

            let array = base
                .iter()
                .zip(exponent.iter())
                .map(|(base, exponent)| match (base, exponent) {
                    (Some(base), Some(exponent)) => Some(base.powf(exponent)),
                    _ => None,
                })
                .collect::<Float64Array>();
            Ok(Arc::new(array) as ArrayRef)
        };
        let pow = make_scalar_function(pow);
        let name = match volatilty {
            Volatility::Immutable => "pow",
            Volatility::Stable => "pow_stable",
            Volatility::Volatile => "pow_vol",
        };
        create_udf(
            name,
            vec![DataType::Float64, DataType::Float64],
            Arc::new(DataType::Float64),
            volatilty,
            pow,
        )
    }

    #[test]
    fn test_constant_evaluate_binop() -> Result<()> {
        let scan = test_table_scan()?;

        //Trying to get non literal expression that has the value Boolean(NULL) so that the symbolic constant_folding can be tested
        let proj = vec![Expr::TryCast {
            expr: Box::new(lit("")),
            data_type: DataType::Int32,
        }
        .eq(lit(0))
        .eq(col("a"))];
        let time = chrono::Utc::now();
        let plan = LogicalPlanBuilder::from(scan.clone())
            .project(proj)
            .unwrap()
            .build()
            .unwrap();
        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected = "Projection: Boolean(NULL)\
        \n  TableScan: test projection=None";
        assert_eq!(actual, expected);

        //Another test for boolean expression constant folding true = #test.a -> true
        let proj = vec![Expr::TryCast {
            expr: Box::new(lit("0")),
            data_type: DataType::Int32,
        }
        .eq(lit(0))
        .eq(col("a"))];
        let time = chrono::Utc::now();
        let plan = LogicalPlanBuilder::from(scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();
        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected = "Projection: #test.a\
        \n  TableScan: test projection=None";
        assert_eq!(actual, expected);

        Ok(())
    }

    //Testing that immutable scalar UDFs are inlined, stable or volatile UDFs are not inlined, and the arguments to a stable or volatile UDF are still folded
    #[test]
    fn test_udf_inlining() -> Result<()> {
        let scan = test_table_scan()?;
        let pow_immut = create_pow_with_volatilty(Volatility::Immutable);
        let pow_stab = create_pow_with_volatilty(Volatility::Stable);
        let pow_vol = create_pow_with_volatilty(Volatility::Volatile);
        let pow_res_2 = vec![abs(lit(1.0) - lit(3)), lit(2)];
        let proj = vec![
            pow_immut.call(pow_res_2.clone()) * col("e").alias("constant"),
            pow_stab.call(pow_res_2.clone()) * col("e").alias("stable"),
            pow_vol.call(pow_res_2) * col("e"),
        ];
        let time = chrono::Utc::now();
        let plan = LogicalPlanBuilder::from(scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();
        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected = "Projection: Float64(4) * #test.e AS constant, pow_stable(Float64(2), Int32(2)) * #test.e AS stable, pow_vol(Float64(2), Int32(2)) * #test.e\
            \n  TableScan: test projection=None".to_string();

        assert_eq!(actual, expected);
        Ok(())
    }
}
