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

//! Boolean comparison rule rewrites redundant comparison expression involving boolean literal into
//! unary expression.

use std::sync::Arc;

use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use arrow::datatypes::DataType;

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::{DFSchemaRef, Expr, ExprRewriter, LogicalPlan, Operator};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::physical_plan::functions::BuiltinScalarFunction;
use crate::scalar::ScalarValue;
use arrow::compute::{kernels, DEFAULT_CAST_OPTIONS};

/// Optimizer that simplifies comparison expressions involving boolean literals.
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
        // We need to pass down the all schemas within the plan tree to `optimize_expr` in order to
        // to evaluate expression types. For example, a projection plan's schema will only include
        // projected columns. With just the projected schema, it's not possible to infer types for
        // expressions that references non-projected columns within the same project plan or its
        // children plans.
        let mut rewriter = ConstantRewriter {
            schemas: plan.all_schemas(),
            execution_props,
        };

        match plan {
            LogicalPlan::Filter { predicate, input } => Ok(LogicalPlan::Filter {
                predicate: predicate.clone().rewrite(&mut rewriter)?,
                input: Arc::new(self.optimize(input, execution_props)?),
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
                    .map(|plan| self.optimize(plan, execution_props))
                    .collect::<Result<Vec<_>>>()?;

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &expr, &new_inputs)
            }
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation { .. } => {
                Ok(plan.clone())
            }
        }
    }

    fn name(&self) -> &str {
        "constant_folding"
    }
}

struct ConstantRewriter<'a> {
    /// input schemas
    schemas: Vec<&'a DFSchemaRef>,
    execution_props: &'a ExecutionProps,
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
}

impl<'a> ExprRewriter for ConstantRewriter<'a> {
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
                _ => Expr::BinaryExpr { left, op, right },
            },
            Expr::Not(inner) => {
                // Not(Not(expr)) --> expr
                if let Expr::Not(negated_inner) = *inner {
                    *negated_inner
                } else {
                    Expr::Not(inner)
                }
            }
            Expr::ScalarFunction {
                fun: BuiltinScalarFunction::Now,
                ..
            } => Expr::Literal(ScalarValue::TimestampNanosecond(Some(
                self.execution_props
                    .query_execution_start_time
                    .timestamp_nanos(),
            ))),
            Expr::ScalarFunction {
                fun: BuiltinScalarFunction::ToTimestamp,
                args,
            } => {
                if !args.is_empty() {
                    match &args[0] {
                        Expr::Literal(ScalarValue::Utf8(Some(val))) => {
                            match string_to_timestamp_nanos(val) {
                                Ok(timestamp) => Expr::Literal(
                                    ScalarValue::TimestampNanosecond(Some(timestamp)),
                                ),
                                _ => Expr::ScalarFunction {
                                    fun: BuiltinScalarFunction::ToTimestamp,
                                    args,
                                },
                            }
                        }
                        _ => Expr::ScalarFunction {
                            fun: BuiltinScalarFunction::ToTimestamp,
                            args,
                        },
                    }
                } else {
                    Expr::ScalarFunction {
                        fun: BuiltinScalarFunction::ToTimestamp,
                        args,
                    }
                }
            }
            Expr::Cast {
                expr: inner,
                data_type,
            } => match inner.as_ref() {
                Expr::Literal(val) => {
                    let scalar_array = val.to_array();
                    let cast_array = kernels::cast::cast_with_options(
                        &scalar_array,
                        &data_type,
                        &DEFAULT_CAST_OPTIONS,
                    )?;
                    let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
                    Expr::Literal(cast_scalar)
                }
                _ => Expr::Cast {
                    expr: inner,
                    data_type,
                },
            },
            expr => {
                // no rewrite possible
                expr
            }
        };
        Ok(new_expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{
        col, lit, max, min, DFField, DFSchema, LogicalPlanBuilder,
    };

    use arrow::datatypes::*;
    use chrono::{DateTime, Utc};

    fn test_table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::UInt32, false),
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
            (col("c2").not().not().not()).rewrite(&mut rewriter)?,
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
    fn optimize_expr_eq() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
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
    fn optimize_expr_not_eq() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
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
    fn optimize_expr_case_when_then_else() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
            execution_props: &ExecutionProps::new(),
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
        \n  Filter: NOT #test.b And #test.c\
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
        \n  Filter: NOT #test.b Or NOT #test.c\
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
        let proj = vec![Expr::Cast {
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
}
