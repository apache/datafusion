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

//! Optimizer rule to push down aggregations in the query plan.
//!
//! It can split an aggregate into final and partial part in order to
//! distribute the aggregation among union inputs.

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DFSchema, DataFusionError, Result};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{
    col, Aggregate, AggregateFunction as AggrEnum, Expr, LogicalPlanBuilder, Union,
};
use std::collections::HashMap;

/// Optimization rule that tries to push down aggregations.
#[derive(Default)]
pub struct PushDownAggregate {}

impl PushDownAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownAggregate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(None);
        };

        match *aggregate.input {
            LogicalPlan::Union(ref u)
                if u.inputs.iter().all(|i| !contains_aggregate(i))
                    && !aggregate.aggr_expr.is_empty() =>
            {
                aggregate_with_union(aggregate, u)
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "push_down_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

fn aggregate_with_union(
    aggregate: &Aggregate,
    union: &Union,
) -> Result<Option<LogicalPlan>> {
    let Some(b) = PartialAggregateBuilder::try_from(aggregate)? else {
        return Ok(None);
    };

    Ok(Some(b.build_with_union(union)?))
}

// Plan is an aggregate or is a projected aggregate.
fn contains_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Projection(p) => contains_aggregate(&p.input),
        _ => false,
    }
}

#[derive(Default)]
struct AggregateBuilder {
    aggr_expr: Vec<Expr>,
    group_expr: Vec<Expr>,
}

impl AggregateBuilder {
    fn build(&self, b: LogicalPlanBuilder) -> Result<LogicalPlanBuilder> {
        b.aggregate(self.group_expr.clone(), self.aggr_expr.clone())
    }
}

#[derive(Default)]
struct ProjectionBuilder {
    expr: Vec<Expr>,
}

impl ProjectionBuilder {
    fn build(&self, b: LogicalPlanBuilder) -> Result<LogicalPlanBuilder> {
        b.project(self.expr.clone())
    }

    /// Converts expressions from `src_schema` to `dst_schema`.
    fn build_realigned(
        &self,
        b: LogicalPlanBuilder,
        src_schema: &DFSchema,
    ) -> Result<LogicalPlanBuilder> {
        let schema = b.schema().clone();
        b.project(rename_columns(&self.expr, src_schema, &schema)?)
    }
}

fn rename_columns(
    expr: &[Expr],
    src_schema: &DFSchema,
    dst_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    let orig_cols = src_schema
        .fields()
        .iter()
        .map(|f| f.qualified_column())
        .collect::<Vec<_>>();
    let dest_cols = dst_schema
        .fields()
        .iter()
        .map(|f| f.qualified_column())
        .collect::<Vec<_>>();
    let replace_map: HashMap<&Column, &Column> =
        orig_cols.iter().zip(dest_cols.iter()).collect();

    expr.iter()
        .map(|e| replace_col(e.clone(), &replace_map))
        .collect()
}

/// Splits aggregate into final and partial parts.
/// For instance, it spreads aggregate among union inputs.
///
/// From:
///
/// ```ignore
/// Aggregate
///   Union
///     PlanA
/// ```
///
/// Into:
///
/// ```ignore
/// Projection: from helper aliases to original names
///   Aggregate: final aggregate
///     Union
///       Aggregate: partial aggregates
///         Projection: from original names to helper aliases
///           PlanA
/// ```
///
/// Helper aliases allow to avoid name collisions, when input columns
/// contain name of some aggregation
/// (see `sum_of_union_with_evil_column` test).
#[derive(Default)]
struct PartialAggregateBuilder {
    exit_projection: ProjectionBuilder,
    final_aggregate: AggregateBuilder,

    partial_aggregate: AggregateBuilder,
    entry_projection: ProjectionBuilder,

    aggr_aliases: HashMap<Expr, String>,
}

impl PartialAggregateBuilder {
    /// Split aggregate into final and partial parts.
    /// Returns [`None`] if some aggregation is not supported.
    fn try_from(aggr: &Aggregate) -> Result<Option<Self>> {
        let mut s: PartialAggregateBuilder = Default::default();

        for group_expr in &aggr.group_expr {
            s.add_group_expr(group_expr)?;
        }

        for aggr_expr in &aggr.aggr_expr {
            if !s.add_aggr_expr(aggr_expr)? {
                return Ok(None);
            }
        }

        Ok(Some(s))
    }

    fn add_group_expr(&mut self, group_expr: &Expr) -> Result<()> {
        let alias_str = format!("group_by_{}", self.entry_projection.expr.len() + 1);
        let column = col(alias_str.clone());

        self.entry_projection
            .expr
            .push(group_expr.clone().alias(alias_str));
        self.exit_projection
            .expr
            .push(column.clone().alias(group_expr.display_name()?));

        self.partial_aggregate.group_expr.push(column.clone());
        self.final_aggregate.group_expr.push(column);

        Ok(())
    }

    fn add_aggr_expr(&mut self, aggr_expr: &Expr) -> Result<bool> {
        let orig_aggr = match aggr_expr {
            Expr::AggregateFunction(a) => a,
            Expr::AggregateUDF { .. } => return Ok(false),
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Expected an aggregate function, got {}",
                    aggr_expr.variant_name()
                )))
            }
        };

        if orig_aggr.distinct || orig_aggr.filter.is_some() {
            return Ok(false);
        }

        let arg = if orig_aggr.args.len() == 1 {
            &orig_aggr.args[0]
        } else {
            return Ok(false);
        };

        // transforms SUM(my_col + 1) => SUM(aggr_1)
        let input = self.get_or_create_aggr_alias(arg);

        let Some(final_projection) = split_aggregate(
            orig_aggr.fun.clone(),
            input,
            &mut self.partial_aggregate.aggr_expr,
            &mut self.final_aggregate.aggr_expr,
        )? else { return Ok(false) };

        self.exit_projection
            .expr
            .push(final_projection.alias(aggr_expr.display_name()?));

        Ok(true)
    }

    fn get_or_create_aggr_alias(&mut self, arg: &Expr) -> Expr {
        let alias_str = if let Some(s) = self.aggr_aliases.get(arg) {
            s.clone()
        } else {
            let alias_str = format!("aggr_{}", self.aggr_aliases.len() + 1);
            self.aggr_aliases.insert(arg.clone(), alias_str.clone());

            self.entry_projection
                .expr
                .push(arg.clone().alias(alias_str.clone()));

            alias_str
        };
        col(alias_str)
    }

    fn build_partial_part(
        &self,
        b: LogicalPlanBuilder,
        src_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlanBuilder> {
        let b = if let Some(src_schema) = src_schema {
            self.entry_projection.build_realigned(b, src_schema)?
        } else {
            self.entry_projection.build(b)?
        };
        self.partial_aggregate.build(b)
    }

    fn build_final_part(&self, b: LogicalPlanBuilder) -> Result<LogicalPlanBuilder> {
        let b = self.final_aggregate.build(b)?;
        self.exit_projection.build(b)
    }

    fn build_with_union(self, union: &Union) -> Result<LogicalPlan> {
        // Names of the field in the union are from the first input.
        let n = union.inputs.len();
        let first_input = LogicalPlanBuilder::from(union.inputs[0].as_ref().clone());
        let first_input_schema = first_input.schema().clone();

        let mut union_builder = self.build_partial_part(first_input, None)?;

        for i in 1..n {
            let p = self.build_partial_part(
                LogicalPlanBuilder::from(union.inputs[i].as_ref().clone()),
                Some(first_input_schema.as_ref()),
            )?;
            union_builder = union_builder.union(p.build()?)?;
        }
        self.build_final_part(union_builder)?.build()
    }
}

fn split_aggregate(
    fun: AggrEnum,
    arg: Expr,
    partial_aggregates: &mut Vec<Expr>,
    final_aggregates: &mut Vec<Expr>,
) -> Result<Option<Expr>> {
    match fun {
        AggrEnum::Sum | AggrEnum::Min | AggrEnum::Max => {
            let partial_aggr = Expr::AggregateFunction(AggregateFunction::new(
                fun.clone(),
                vec![arg],
                false,
                None,
            ));
            let final_aggr = Expr::AggregateFunction(AggregateFunction::new(
                fun,
                vec![column_from_expr(&partial_aggr)?],
                false,
                None,
            ));

            partial_aggregates.push(partial_aggr);

            let final_proj = column_from_expr(&final_aggr)?;
            final_aggregates.push(final_aggr);
            Ok(Some(final_proj))
        }
        AggrEnum::Count => {
            let partial_aggr = Expr::AggregateFunction(AggregateFunction::new(
                AggrEnum::Count,
                vec![arg],
                false,
                None,
            ));
            let final_aggr = Expr::AggregateFunction(AggregateFunction::new(
                AggrEnum::Sum,
                vec![column_from_expr(&partial_aggr)?],
                false,
                None,
            ));

            partial_aggregates.push(partial_aggr);

            let final_proj = column_from_expr(&final_aggr)?;
            final_aggregates.push(final_aggr);
            Ok(Some(final_proj))
        }
        _ => Ok(None),
    }
}

fn column_from_expr(expr: &Expr) -> Result<Expr> {
    Ok(col(expr.display_name()?))
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::test;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{
        col, lit, logical_plan::builder, logical_plan::builder::LogicalPlanBuilder,
        utils::COUNT_STAR_EXPANSION, Operator,
    };
    use std::sync::Arc;

    use datafusion_expr as expr;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        test::assert_optimized_plan_eq(Arc::new(PushDownAggregate::new()), plan, expected)
    }

    fn assert_optimization_skipped(plan: &LogicalPlan) -> Result<()> {
        test::assert_optimization_skipped(Arc::new(PushDownAggregate::new()), plan)
    }

    fn test_table_scan_fields() -> Vec<Field> {
        vec![
            Field::new("my_group_a", DataType::UInt32, false),
            Field::new("my_group_b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
            Field::new("d", DataType::UInt32, false),
        ]
    }

    fn test_table_scan_with_name(table_name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(test_table_scan_fields());
        builder::table_scan(Some(table_name), &schema, None)?.build()
    }

    fn test_union(table_count: u8) -> Result<LogicalPlanBuilder> {
        assert!(1 <= table_count);

        let mut b = LogicalPlanBuilder::from(test_table_scan_with_name("test1")?);

        for i in 2..=table_count {
            b = b.union(test_table_scan_with_name(&format!("test{i}"))?)?;
        }

        Ok(b)
    }

    #[test]
    fn union_distinct_is_not_changed() -> Result<()> {
        // This optimization is only correct for `UNION ALL` variant.
        let table_scan_1 = test_table_scan_with_name("test1")?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .union_distinct(table_scan_2)?
            .aggregate([] as [Expr; 0], [datafusion_expr::sum(col("c"))])?
            .build()?;

        assert_optimization_skipped(&plan)
    }

    #[test]
    fn count_distinct_of_union_is_not_changed() -> Result<()> {
        // This optimization is only correct for non-distinct aggregations.
        let plan = test_union(2)?
            .aggregate([] as [Expr; 0], [datafusion_expr::count_distinct(col("c"))])?
            .build()?;

        assert_optimization_skipped(&plan)
    }

    #[test]
    fn median_of_union_is_not_changed() -> Result<()> {
        // `MEDIAN` cannot be split into partial aggregations.

        let plan = test_union(2)?
            .aggregate(
                [] as [Expr; 0],
                [
                    datafusion_expr::count(col("c")),
                    datafusion_expr::median(col("c")),
                ],
            )?
            .build()?;

        assert_optimization_skipped(&plan)
    }

    #[test]
    fn group_by_without_aggr_union_is_not_changed() -> Result<()> {
        let plan = test_union(2)?
            .aggregate(
                [
                    col("my_group_a"),
                    expr::binary_expr(col("my_group_b"), Operator::Plus, lit(5)),
                ],
                [] as [Expr; 0],
            )?
            .build()?;

        assert_optimization_skipped(&plan)
    }

    #[test]
    fn grouped_single_aggregate_of_unions() -> Result<()> {
        // test each supported aggregation
        let cases = vec![
            (
                expr::count(lit(COUNT_STAR_EXPANSION)),
                "Projection: group_by_1 AS test1.my_group_a, group_by_2 AS test1.my_group_b + Int32(5), SUM(COUNT(aggr_1)) AS COUNT(UInt8(1))\
                \n  Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(COUNT(aggr_1))]]\
                \n    Union\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[COUNT(aggr_1)]]\
                \n        Projection: test1.my_group_a AS group_by_1, test1.my_group_b + Int32(5) AS group_by_2, UInt8(1) AS aggr_1\
                \n          TableScan: test1\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[COUNT(aggr_1)]]\
                \n        Projection: test2.my_group_a AS group_by_1, test2.my_group_b + Int32(5) AS group_by_2, UInt8(1) AS aggr_1\
                \n          TableScan: test2",
            ),
            (
                expr::count(col("c")),
                "Projection: group_by_1 AS test1.my_group_a, group_by_2 AS test1.my_group_b + Int32(5), SUM(COUNT(aggr_1)) AS COUNT(test1.c)\
                \n  Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(COUNT(aggr_1))]]\
                \n    Union\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[COUNT(aggr_1)]]\
                \n        Projection: test1.my_group_a AS group_by_1, test1.my_group_b + Int32(5) AS group_by_2, test1.c AS aggr_1\
                \n          TableScan: test1\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[COUNT(aggr_1)]]\
                \n        Projection: test2.my_group_a AS group_by_1, test2.my_group_b + Int32(5) AS group_by_2, test2.c AS aggr_1\
                \n          TableScan: test2",
            ),
            (
                expr::sum(col("c")),
                "Projection: group_by_1 AS test1.my_group_a, group_by_2 AS test1.my_group_b + Int32(5), SUM(SUM(aggr_1)) AS SUM(test1.c)\
                \n  Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(SUM(aggr_1))]]\
                \n    Union\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(aggr_1)]]\
                \n        Projection: test1.my_group_a AS group_by_1, test1.my_group_b + Int32(5) AS group_by_2, test1.c AS aggr_1\
                \n          TableScan: test1\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(aggr_1)]]\
                \n        Projection: test2.my_group_a AS group_by_1, test2.my_group_b + Int32(5) AS group_by_2, test2.c AS aggr_1\
                \n          TableScan: test2",
            ),
            (
                expr::max(col("c")),
                "Projection: group_by_1 AS test1.my_group_a, group_by_2 AS test1.my_group_b + Int32(5), MAX(MAX(aggr_1)) AS MAX(test1.c)\
                \n  Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[MAX(MAX(aggr_1))]]\
                \n    Union\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[MAX(aggr_1)]]\
                \n        Projection: test1.my_group_a AS group_by_1, test1.my_group_b + Int32(5) AS group_by_2, test1.c AS aggr_1\
                \n          TableScan: test1\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[MAX(aggr_1)]]\
                \n        Projection: test2.my_group_a AS group_by_1, test2.my_group_b + Int32(5) AS group_by_2, test2.c AS aggr_1\
                \n          TableScan: test2",
            ),
            (
                expr::min(col("d")),
                "Projection: group_by_1 AS test1.my_group_a, group_by_2 AS test1.my_group_b + Int32(5), MIN(MIN(aggr_1)) AS MIN(test1.d)\
                \n  Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[MIN(MIN(aggr_1))]]\
                \n    Union\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[MIN(aggr_1)]]\
                \n        Projection: test1.my_group_a AS group_by_1, test1.my_group_b + Int32(5) AS group_by_2, test1.d AS aggr_1\
                \n          TableScan: test1\
                \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[MIN(aggr_1)]]\
                \n        Projection: test2.my_group_a AS group_by_1, test2.my_group_b + Int32(5) AS group_by_2, test2.d AS aggr_1\
                \n          TableScan: test2",
            ),
        ];

        for (aggr, expected) in cases {
            let plan = test_union(2)?
                .aggregate(
                    [
                        col("my_group_a"),
                        expr::binary_expr(col("my_group_b"), Operator::Plus, lit(5)),
                    ],
                    [aggr],
                )?
                .build()?;

            assert_optimized_plan_eq(&plan, expected)?;
        }

        Ok(())
    }

    #[test]
    fn non_grouped_count_of_union() -> Result<()> {
        let plan = test_union(2)?
            .aggregate([] as [Expr; 0], [expr::sum(col("c"))])?
            .build()?;

        let expected = "Projection: SUM(SUM(aggr_1)) AS SUM(test1.c)\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(SUM(aggr_1))]]\
        \n    Union\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(aggr_1)]]\
        \n        Projection: test1.c AS aggr_1\
        \n          TableScan: test1\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(aggr_1)]]\
        \n        Projection: test2.c AS aggr_1\
        \n          TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn sum_of_union_with_evil_column() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("SUM(SUM(alpha))", DataType::UInt32, false),
            Field::new("alpha", DataType::UInt32, false),
        ]);
        let plan = builder::table_scan(Some("test1"), &schema, None)?
            .union(builder::table_scan(Some("test2"), &schema, None)?.build()?)?
            .aggregate([col("SUM(SUM(alpha))")], [expr::sum(col("alpha"))])?
            .build()?;

        let expected = "Projection: group_by_1 AS test1.SUM(SUM(alpha)), SUM(SUM(aggr_1)) AS SUM(test1.alpha)\
        \n  Aggregate: groupBy=[[group_by_1]], aggr=[[SUM(SUM(aggr_1))]]\
        \n    Union\
        \n      Aggregate: groupBy=[[group_by_1]], aggr=[[SUM(aggr_1)]]\
        \n        Projection: test1.SUM(SUM(alpha)) AS group_by_1, test1.alpha AS aggr_1\
        \n          TableScan: test1\
        \n      Aggregate: groupBy=[[group_by_1]], aggr=[[SUM(aggr_1)]]\
        \n        Projection: test2.SUM(SUM(alpha)) AS group_by_1, test2.alpha AS aggr_1\
        \n          TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn non_grouped_many_aggregates_of_union() -> Result<()> {
        let plan = test_union(2)?
            .aggregate(
                [] as [Expr; 0],
                [
                    expr::sum(col("c")),
                    expr::count(col("c")),
                    expr::max(col("c")),
                    expr::min(col("d")),
                    expr::max(col("d")),
                    expr::count(lit(COUNT_STAR_EXPANSION)),
                ],
            )?
            .build()?;

        let expected = "Projection: SUM(SUM(aggr_1)) AS SUM(test1.c), SUM(COUNT(aggr_1)) AS COUNT(test1.c), MAX(MAX(aggr_1)) AS MAX(test1.c), MIN(MIN(aggr_2)) AS MIN(test1.d), MAX(MAX(aggr_2)) AS MAX(test1.d), SUM(COUNT(aggr_3)) AS COUNT(UInt8(1))\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(SUM(aggr_1)), SUM(COUNT(aggr_1)), MAX(MAX(aggr_1)), MIN(MIN(aggr_2)), MAX(MAX(aggr_2)), SUM(COUNT(aggr_3))]]\
        \n    Union\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test1.c AS aggr_1, test1.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test1\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test2.c AS aggr_1, test2.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn non_grouped_many_aggregates_of_many_unions() -> Result<()> {
        let plan = test_union(3)?
            .aggregate(
                [] as [Expr; 0],
                [
                    expr::sum(col("c")),
                    expr::count(col("c")),
                    expr::max(col("c")),
                    expr::min(col("d")),
                    expr::max(col("d")),
                    expr::count(lit(COUNT_STAR_EXPANSION)),
                ],
            )?
            .build()?;

        let expected = "Projection: SUM(SUM(aggr_1)) AS SUM(test1.c), SUM(COUNT(aggr_1)) AS COUNT(test1.c), MAX(MAX(aggr_1)) AS MAX(test1.c), MIN(MIN(aggr_2)) AS MIN(test1.d), MAX(MAX(aggr_2)) AS MAX(test1.d), SUM(COUNT(aggr_3)) AS COUNT(UInt8(1))\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(SUM(aggr_1)), SUM(COUNT(aggr_1)), MAX(MAX(aggr_1)), MIN(MIN(aggr_2)), MAX(MAX(aggr_2)), SUM(COUNT(aggr_3))]]\
        \n    Union\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test1.c AS aggr_1, test1.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test1\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test2.c AS aggr_1, test2.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test2\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test3.c AS aggr_1, test3.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test3";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn grouped_many_aggregates_of_union() -> Result<()> {
        let plan = test_union(2)?
            .aggregate(
                [
                    col("my_group_a"),
                    expr::binary_expr(col("my_group_b"), Operator::Plus, lit(5)),
                ],
                [
                    expr::sum(col("c")),
                    expr::count(col("c")),
                    expr::max(col("c")),
                    expr::min(col("d")),
                    expr::max(col("d")),
                    expr::count(lit(COUNT_STAR_EXPANSION)),
                ],
            )?
            .build()?;

        let expected = "Projection: group_by_1 AS test1.my_group_a, group_by_2 AS test1.my_group_b + Int32(5), SUM(SUM(aggr_1)) AS SUM(test1.c), SUM(COUNT(aggr_1)) AS COUNT(test1.c), MAX(MAX(aggr_1)) AS MAX(test1.c), MIN(MIN(aggr_2)) AS MIN(test1.d), MAX(MAX(aggr_2)) AS MAX(test1.d), SUM(COUNT(aggr_3)) AS COUNT(UInt8(1))\
        \n  Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(SUM(aggr_1)), SUM(COUNT(aggr_1)), MAX(MAX(aggr_1)), MIN(MIN(aggr_2)), MAX(MAX(aggr_2)), SUM(COUNT(aggr_3))]]\
        \n    Union\
        \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test1.my_group_a AS group_by_1, test1.my_group_b + Int32(5) AS group_by_2, test1.c AS aggr_1, test1.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test1\
        \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test2.my_group_a AS group_by_1, test2.my_group_b + Int32(5) AS group_by_2, test2.c AS aggr_1, test2.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn grouped_many_aggregates_many_unions() -> Result<()> {
        let plan = test_union(3)?
            .aggregate(
                [
                    col("my_group_a"),
                    expr::binary_expr(col("my_group_b"), Operator::Plus, lit(5)),
                ],
                [
                    expr::sum(col("c")),
                    expr::count(col("c")),
                    expr::max(col("c")),
                    expr::min(col("d")),
                    expr::max(col("d")),
                    expr::count(lit(COUNT_STAR_EXPANSION)),
                ],
            )?
            .build()?;

        let expected = "Projection: group_by_1 AS test1.my_group_a, group_by_2 AS test1.my_group_b + Int32(5), SUM(SUM(aggr_1)) AS SUM(test1.c), SUM(COUNT(aggr_1)) AS COUNT(test1.c), MAX(MAX(aggr_1)) AS MAX(test1.c), MIN(MIN(aggr_2)) AS MIN(test1.d), MAX(MAX(aggr_2)) AS MAX(test1.d), SUM(COUNT(aggr_3)) AS COUNT(UInt8(1))\
        \n  Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(SUM(aggr_1)), SUM(COUNT(aggr_1)), MAX(MAX(aggr_1)), MIN(MIN(aggr_2)), MAX(MAX(aggr_2)), SUM(COUNT(aggr_3))]]\
        \n    Union\
        \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test1.my_group_a AS group_by_1, test1.my_group_b + Int32(5) AS group_by_2, test1.c AS aggr_1, test1.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test1\
        \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test2.my_group_a AS group_by_1, test2.my_group_b + Int32(5) AS group_by_2, test2.c AS aggr_1, test2.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test2\
        \n      Aggregate: groupBy=[[group_by_1, group_by_2]], aggr=[[SUM(aggr_1), COUNT(aggr_1), MAX(aggr_1), MIN(aggr_2), MAX(aggr_2), COUNT(aggr_3)]]\
        \n        Projection: test3.my_group_a AS group_by_1, test3.my_group_b + Int32(5) AS group_by_2, test3.c AS aggr_1, test3.d AS aggr_2, UInt8(1) AS aggr_3\
        \n          TableScan: test3";

        assert_optimized_plan_eq(&plan, expected)
    }
}
