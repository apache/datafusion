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

//! Constant folding and algebraic simplification

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;

/// Simplifies plans by rewriting [`Expr`]`s evaluating constants
/// and applying algebraic simplifications
///
/// For example
/// `true && col` --> `col` where `col` is a boolean types
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
        let mut simplifier =
            super::simplify_expressions::Simplifier::new(plan.all_schemas());

        let mut const_evaluator =
            super::simplify_expressions::ConstEvaluator::new(execution_props);

        match plan {
            // Recurse into plan, apply optimization where possible
            LogicalPlan::Filter { .. }
            | LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition(_)
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::Limit(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin(_) => {
                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan, execution_props))
                    .collect::<Result<Vec<_>>>()?;

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| {
                        // We need to keep original expression name, if any.
                        // Constant folding should not change expression name.
                        let name = &e.name(plan.schema());

                        // TODO iterate until no changes are made
                        // during rewrite (evaluating constants can
                        // enable new simplifications and
                        // simplifications can enable new constant
                        // evaluation)
                        let new_e = e
                            // fold constants and then simplify
                            .rewrite(&mut const_evaluator)?
                            .rewrite(&mut simplifier)?;

                        let new_name = &new_e.name(plan.schema());

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

                utils::from_plan(plan, &expr, &new_inputs)
            }
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation(_) => {
                Ok(plan.clone())
            }
        }
    }

    fn name(&self) -> &str {
        "constant_folding"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        assert_contains,
        logical_plan::{col, lit, max, min, Expr, LogicalPlanBuilder, Operator},
        physical_plan::functions::BuiltinScalarFunction,
    };

    use arrow::datatypes::*;
    use chrono::{DateTime, TimeZone, Utc};

    fn test_table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::UInt32, false),
        ]);
        LogicalPlanBuilder::scan_empty(Some("test"), &schema, None)?.build()
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
        \n  Filter: NOT #test.c AS test.c = Boolean(false)\
        \n    Filter: #test.b AS test.b = Boolean(true)\
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
        \n    Filter: #test.c AS test.c != Boolean(false)\
        \n      Filter: NOT #test.b AS test.b != Boolean(true)\
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
        \n  Filter: NOT #test.b AND #test.c AS test.b != Boolean(true) AND test.c = Boolean(true)\
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
        \n  Filter: NOT #test.b OR NOT #test.c AS test.b != Boolean(true) OR test.c = Boolean(false)\
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
        \n  Filter: #test.b AS NOT test.b = Boolean(false)\
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
        Projection: #test.a, #test.d, NOT #test.b AS test.b = Boolean(false)\
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
        Aggregate: groupBy=[[#test.a, #test.c]], aggr=[[MAX(#test.b) AS MAX(test.b = Boolean(true)), MIN(#test.b)]]\
        \n  Projection: #test.a, #test.c, #test.b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_support_values() -> Result<()> {
        let expr1 = Expr::BinaryExpr {
            left: Box::new(lit(1)),
            op: Operator::Plus,
            right: Box::new(lit(2)),
        };
        let expr2 = Expr::BinaryExpr {
            left: Box::new(lit(2)),
            op: Operator::Minus,
            right: Box::new(lit(1)),
        };
        let values = vec![vec![expr1, expr2]];
        let plan = LogicalPlanBuilder::values(values)?.build()?;

        let expected = "\
        Values: (Int32(3) AS Int32(1) + Int32(2), Int32(1) AS Int32(2) - Int32(1))";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    // expect optimizing will result in an error, returning the error string
    fn get_optimized_plan_err(plan: &LogicalPlan, date_time: &DateTime<Utc>) -> String {
        let rule = ConstantFolding::new();
        let execution_props = ExecutionProps {
            query_execution_start_time: *date_time,
        };

        let err = rule
            .optimize(plan, &execution_props)
            .expect_err("expected optimization to fail");

        err.to_string()
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

    /// Create a to_timestamp expr
    fn to_timestamp_expr(arg: impl Into<String>) -> Expr {
        Expr::ScalarFunction {
            args: vec![lit(arg.into())],
            fun: BuiltinScalarFunction::ToTimestamp,
        }
    }

    #[test]
    fn to_timestamp_expr_folded() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![to_timestamp_expr("2020-09-08T12:00:00+00:00")];

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: TimestampNanosecond(1599566400000000000) AS totimestamp(Utf8(\"2020-09-08T12:00:00+00:00\"))\
            \n  TableScan: test projection=None"
            .to_string();
        let actual = get_optimized_plan_formatted(&plan, &Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_timestamp_expr_wrong_arg() {
        let table_scan = test_table_scan().unwrap();
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
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::Cast {
            expr: Box::new(lit("0")),
            data_type: DataType::Int32,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: Int32(0) AS CAST(Utf8(\"0\") AS Int32)\
            \n  TableScan: test projection=None";
        let actual = get_optimized_plan_formatted(&plan, &Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn cast_expr_wrong_arg() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::Cast {
            expr: Box::new(lit("")),
            data_type: DataType::Int32,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected =
            "Cannot cast string '' to value of arrow::datatypes::types::Int32Type type";
        let actual = get_optimized_plan_err(&plan, &Utc::now());
        assert_contains!(actual, expected);
    }

    fn now_expr() -> Expr {
        Expr::ScalarFunction {
            args: vec![],
            fun: BuiltinScalarFunction::Now,
        }
    }

    #[test]
    fn multiple_now_expr() {
        let table_scan = test_table_scan().unwrap();
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
            "Projection: TimestampNanosecond({}) AS now(), TimestampNanosecond({}) AS t2\
            \n  TableScan: test projection=None",
            time.timestamp_nanos(),
            time.timestamp_nanos()
        );

        assert_eq!(actual, expected);
    }

    #[test]
    fn simplify_and_eval() {
        // demonstrate a case where the evaluation needs to run prior
        // to the simplifier for it to work
        let table_scan = test_table_scan().unwrap();
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
            "Projection: NOT #test.a AS Boolean(true) OR Boolean(false) != test.a\
                        \n  TableScan: test projection=None";

        assert_eq!(actual, expected);
    }

    fn cast_to_int64_expr(expr: Expr) -> Expr {
        Expr::Cast {
            expr: expr.into(),
            data_type: DataType::Int64,
        }
    }

    #[test]
    fn now_less_than_timestamp() {
        let table_scan = test_table_scan().unwrap();

        let ts_string = "2020-09-08T12:05:00+00:00";
        let time = chrono::Utc.timestamp_nanos(1599566400000000000i64);

        //  now() < cast(to_timestamp(...) as int) + 5000000000
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                cast_to_int64_expr(now_expr())
                    .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000)),
            )
            .unwrap()
            .build()
            .unwrap();

        // Note that constant folder runs and folds the entire
        // expression down to a single constant (true)
        let expected = "Filter: Boolean(true) AS CAST(now() AS Int64) < CAST(totimestamp(Utf8(\"2020-09-08T12:05:00+00:00\")) AS Int64) + Int32(50000)\
                        \n  TableScan: test projection=None";
        let actual = get_optimized_plan_formatted(&plan, &time);

        assert_eq!(expected, actual);
    }
}
