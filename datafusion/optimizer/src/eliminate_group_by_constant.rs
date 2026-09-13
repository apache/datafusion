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

//! [`EliminateGroupByConstant`] simplifies a `GROUP BY` clause, and removes the
//! aggregate altogether when the grouping makes it redundant.
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, Dependency, Result, ScalarValue};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{
    Aggregate, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Volatility, cast,
    lit, when,
};

/// Optimizer rule that simplifies aggregation based on its `GROUP BY` clause.
///
/// Constant expressions are removed from the `GROUP BY`, with a projection on
/// top to preserve the original schema.
///
/// When the `GROUP BY` covers a unique key of the input, every group holds
/// exactly one row, so the aggregate produces one output row per input row like
/// a projection, and each aggregate function returns that row's value:
///
/// ```text
/// -- lineitem is keyed by (l_orderkey, l_linenumber)
/// SELECT l_orderkey, l_linenumber, sum(l_quantity)
/// FROM lineitem GROUP BY l_orderkey, l_linenumber
///
/// -- becomes
/// SELECT l_orderkey, l_linenumber, CAST(l_quantity AS Decimal128(38, 2))
/// FROM lineitem
/// ```
///
/// That also removes a `SELECT DISTINCT` over a unique key, which the planner
/// turns into an aggregate with no aggregate expressions.
#[derive(Default, Debug)]
pub struct EliminateGroupByConstant {}

impl EliminateGroupByConstant {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateGroupByConstant {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Aggregate(aggregate) => {
                if let Some(projection) = eliminate_aggregate(&aggregate)? {
                    return Ok(Transformed::yes(projection));
                }

                // Collect bare column references in GROUP BY
                let group_by_columns: HashSet<&datafusion_common::Column> = aggregate
                    .group_expr
                    .iter()
                    .filter_map(|expr| match expr {
                        Expr::Column(c) => Some(c),
                        _ => None,
                    })
                    .collect();

                let (redundant, required): (Vec<_>, Vec<_>) = aggregate
                    .group_expr
                    .iter()
                    .partition(|expr| is_redundant_group_expr(expr, &group_by_columns));
                // Return now if no simplification can be done. We also bail out
                // if applying the optimization would eliminate all of the
                // grouping expressions (e.g., GROUP BY on only constant
                // expressions): this would turn a grouped aggregate into an
                // ungrouped aggregate, which changes query semantics (grouped
                // aggregates produce an empty result set on an empty input,
                // whereas ungrouped aggregates return a single row).
                if redundant.is_empty() || required.is_empty() {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }

                let simplified_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
                    aggregate.input,
                    required.into_iter().cloned().collect(),
                    aggregate.aggr_expr.clone(),
                )?);

                let projection_expr =
                    aggregate.group_expr.into_iter().chain(aggregate.aggr_expr);

                let projection = LogicalPlanBuilder::from(simplified_aggregate)
                    .project(projection_expr)?
                    .build()?;

                Ok(Transformed::yes(projection))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "eliminate_group_by_constant"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

/// Replaces an aggregate whose `GROUP BY` covers a unique key of its input with
/// a projection, or returns `None` when that cannot be done.
fn eliminate_aggregate(aggregate: &Aggregate) -> Result<Option<LogicalPlan>> {
    // An empty GROUP BY produces one row even for an empty input, which a
    // projection would not. Grouping sets do not group by every expression at
    // once, so a key among them proves nothing.
    if aggregate.group_expr.is_empty()
        || aggregate
            .group_expr
            .iter()
            .any(|expr| matches!(expr, Expr::GroupingSet(_)))
        || !group_by_covers_unique_key(&aggregate.input, &aggregate.group_expr)
    {
        return Ok(None);
    }

    // The output schema has to survive untouched, so each aggregate is replaced
    // by an expression of the same type aliased to the same name.
    let output_fields = aggregate.schema.fields();
    let mut projection = aggregate.group_expr.clone();
    for (index, aggr_expr) in aggregate.aggr_expr.iter().enumerate() {
        let field = &output_fields[aggregate.group_expr.len() + index];
        let Some(value) =
            single_row_value(aggr_expr, field.data_type(), aggregate.input.schema())?
        else {
            return Ok(None);
        };
        projection.push(value.alias(field.name()));
    }

    let input = Arc::clone(&aggregate.input);
    Ok(Some(
        LogicalPlanBuilder::from(Arc::unwrap_or_clone(input))
            .project(projection)?
            .build()?,
    ))
}

/// Returns true when the GROUP BY expressions include a unique key of the
/// input, so that every group holds exactly one row.
///
/// Extra grouping expressions beyond the key are harmless: they can only split
/// groups further, and the groups are already singletons.
fn group_by_covers_unique_key(input: &LogicalPlan, group_expr: &[Expr]) -> bool {
    let schema = input.schema();
    let grouped: HashSet<usize> = group_expr
        .iter()
        .filter_map(|expr| match expr {
            Expr::Alias(alias) => alias.expr.as_ref().try_as_col(),
            _ => expr.try_as_col(),
        })
        .filter_map(|column| schema.maybe_index_of_column(column))
        .collect();
    if grouped.is_empty() {
        return false;
    }

    schema.functional_dependencies().iter().any(|dependency| {
        // A nullable key does not identify rows: two rows can both be NULL and
        // are then not distinguished by the grouping either.
        let nullable = dependency.nullable
            && dependency
                .source_indices
                .iter()
                .any(|&index| schema.field(index).is_nullable());
        !nullable
            && dependency.mode == Dependency::Single
            // The dependency has to determine the whole row, not just part of it.
            && dependency.target_indices.len() == schema.fields().len()
            && dependency
                .source_indices
                .iter()
                .all(|index| grouped.contains(index))
    })
}

/// The value an aggregate takes over a group of exactly one row, as an
/// expression over that row, or `None` when it cannot be expressed.
fn single_row_value(
    aggr_expr: &Expr,
    output_type: &DataType,
    input_schema: &DFSchema,
) -> Result<Option<Expr>> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = aggr_expr else {
        return Ok(None);
    };
    // A FILTER can exclude the single row, leaving the aggregate with no input
    // at all, which is a different value for every function.
    if params.filter.is_some() {
        return Ok(None);
    }
    // DISTINCT, ORDER BY and IGNORE NULLS all make no difference to a group of
    // one row: there is nothing to deduplicate or order, and a single NULL is
    // skipped by the functions below anyway.
    let [arg] = params.args.as_slice() else {
        return Ok(None);
    };

    Ok(match func.name() {
        // Over one row these all return that row's value, in the type the
        // aggregate would have returned.
        "min" | "max" | "sum" | "avg" | "first_value" | "last_value" => {
            Some(if &arg.get_type(input_schema)? == output_type {
                arg.clone()
            } else {
                cast(arg.clone(), output_type.clone())
            })
        }
        // COUNT ignores NULLs, so it is 1 unless the single value is NULL.
        "count" => Some(if arg.nullable(input_schema)? {
            when(arg.clone().is_null(), lit(ScalarValue::Int64(Some(0))))
                .otherwise(lit(ScalarValue::Int64(Some(1))))?
        } else {
            lit(ScalarValue::Int64(Some(1)))
        }),
        _ => None,
    })
}

/// Checks if a GROUP BY expression is redundant (can be removed without
/// changing grouping semantics). An expression is redundant if it is a
/// deterministic function of constants and columns already present as bare
/// column references in the GROUP BY.
fn is_redundant_group_expr(
    expr: &Expr,
    group_by_columns: &HashSet<&datafusion_common::Column>,
) -> bool {
    // Bare column references are never redundant - they define the grouping
    if matches!(expr, Expr::Column(_)) {
        return false;
    }
    is_deterministic_of(expr, group_by_columns)
}

/// Returns true if `expr` is a deterministic expression whose only column
/// references are contained in `known_columns`.
fn is_deterministic_of(
    expr: &Expr,
    known_columns: &HashSet<&datafusion_common::Column>,
) -> bool {
    match expr {
        Expr::Alias(e) => is_deterministic_of(&e.expr, known_columns),
        Expr::Column(c) => known_columns.contains(c),
        Expr::Literal(_, _) => true,
        Expr::BinaryExpr(e) => {
            is_deterministic_of(&e.left, known_columns)
                && is_deterministic_of(&e.right, known_columns)
        }
        Expr::ScalarFunction(e) => {
            matches!(
                e.func.signature().volatility,
                Volatility::Immutable | Volatility::Stable
            ) && e
                .args
                .iter()
                .all(|arg| is_deterministic_of(arg, known_columns))
        }
        Expr::Cast(e) => is_deterministic_of(&e.expr, known_columns),
        Expr::TryCast(e) => is_deterministic_of(&e.expr, known_columns),
        Expr::Negative(e) => is_deterministic_of(e, known_columns),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::*;

    use arrow::datatypes::DataType;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{
        ColumnarValue, LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
        Signature, TypeSignature, col, lit,
    };

    use datafusion_functions_aggregate::expr_fn::count;

    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateGroupByConstant::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct ScalarUDFMock {
        signature: Signature,
    }

    impl ScalarUDFMock {
        fn new_with_volatility(volatility: Volatility) -> Self {
            Self {
                signature: Signature::new(TypeSignature::Any(1), volatility),
            }
        }
    }

    impl ScalarUDFImpl for ScalarUDFMock {
        fn name(&self) -> &str {
            "scalar_fn_mock"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }

    #[test]
    fn test_eliminate_gby_literal() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), lit(1u32)], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, UInt32(1), count(test.c)
          Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]
            TableScan: test
        ")
    }

    #[test]
    fn test_no_op_only_constant_with_aggregate() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![lit("test"), lit(123u32)], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[Utf8("test"), UInt32(123)]], aggr=[[count(test.c)]]
          TableScan: test
        "#)
    }

    #[test]
    fn test_no_op_no_constants() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), col("b")], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a, test.b]], aggr=[[count(test.c)]]
          TableScan: test
        ")
    }

    #[test]
    fn test_no_op_only_constant() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![lit(123u32)], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[UInt32(123)]], aggr=[[]]
          TableScan: test
        ")
    }

    #[test]
    fn test_eliminate_constant_with_alias() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(
                vec![lit(123u32).alias("const"), col("a")],
                vec![count(col("c"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: UInt32(123) AS const, test.a, count(test.c)
          Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]
            TableScan: test
        ")
    }

    #[test]
    fn test_eliminate_scalar_fn_with_constant_arg() -> Result<()> {
        let udf = ScalarUDF::new_from_impl(ScalarUDFMock::new_with_volatility(
            Volatility::Immutable,
        ));
        let udf_expr =
            Expr::ScalarFunction(ScalarFunction::new_udf(udf.into(), vec![lit(123u32)]));
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![udf_expr, col("a")], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: scalar_fn_mock(UInt32(123)), test.a, count(test.c)
          Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]
            TableScan: test
        ")
    }

    #[test]
    fn test_eliminate_deterministic_expr_of_group_by_column() -> Result<()> {
        let scan = test_table_scan()?;
        // GROUP BY a, a - 1, a - 2, a - 3  ->  GROUP BY a
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(
                vec![
                    col("a"),
                    col("a") - lit(1u32),
                    col("a") - lit(2u32),
                    col("a") - lit(3u32),
                ],
                vec![count(col("c"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, test.a - UInt32(1), test.a - UInt32(2), test.a - UInt32(3), count(test.c)
          Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]
            TableScan: test
        ")
    }

    #[test]
    fn test_no_eliminate_independent_columns() -> Result<()> {
        // GROUP BY a, b - 1 should NOT eliminate b - 1 (b is not a group by column)
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), col("b") - lit(1u32)], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a, test.b - UInt32(1)]], aggr=[[count(test.c)]]
          TableScan: test
        ")
    }

    #[test]
    fn test_no_op_volatile_scalar_fn_with_constant_arg() -> Result<()> {
        let udf = ScalarUDF::new_from_impl(ScalarUDFMock::new_with_volatility(
            Volatility::Volatile,
        ));
        let udf_expr =
            Expr::ScalarFunction(ScalarFunction::new_udf(udf.into(), vec![lit(123u32)]));
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![udf_expr, col("a")], vec![count(col("c"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[scalar_fn_mock(UInt32(123)), test.a]], aggr=[[count(test.c)]]
          TableScan: test
        ")
    }
}
