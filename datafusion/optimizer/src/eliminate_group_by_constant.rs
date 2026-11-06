// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`EliminateGroupByConstant`] removes constant and functionally redundant
//! expressions from `GROUP BY` clause
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, ColumnId, Result};
use datafusion_expr::{Aggregate, Expr, LogicalPlan, NamedExpr, Projection, Volatility};

/// Optimizer rule that removes constant expressions from `GROUP BY` clause
/// and places additional projection on top of aggregation, to preserve
/// original schema
#[derive(Default, Debug)]
pub struct EliminateGroupByConstant {}

impl EliminateGroupByConstant {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateGroupByConstant {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            // grouping sets are left alone: a constant in a set is NULL in
            // the rows of the sets that omit it
            LogicalPlan::Aggregate(aggregate) if aggregate.grouping_sets.is_none() => {
                // Collect bare column references in GROUP BY
                let group_by_columns: HashSet<&Column> = aggregate
                    .group_expr
                    .iter()
                    .filter_map(|expr| match &expr.expr {
                        Expr::Column(c) => Some(c),
                        _ => None,
                    })
                    .collect();

                let is_redundant: Vec<bool> = aggregate
                    .group_expr
                    .iter()
                    .map(|expr| is_redundant_group_expr(expr, &group_by_columns))
                    .collect();
                let redundant_count = is_redundant.iter().filter(|r| **r).count();
                // Return now if no simplification can be done. We also bail out
                // if applying the optimization would eliminate all of the
                // grouping expressions (e.g., GROUP BY on only constant
                // expressions): this would turn a grouped aggregate into an
                // ungrouped aggregate, which changes query semantics (grouped
                // aggregates produce an empty result set on an empty input,
                // whereas ungrouped aggregates return a single row).
                if redundant_count == 0 || redundant_count == aggregate.group_expr.len() {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }

                // the output id of each bare GROUP BY column, which a
                // redundant expression reads once it is computed over the
                // simplified aggregate (the output can define a new id)
                let group_by_outputs: Vec<(Column, ColumnId)> = aggregate
                    .group_expr
                    .iter()
                    .filter_map(|entry| match &entry.expr {
                        Expr::Column(c) => Some((c.clone(), entry.id)),
                        _ => None,
                    })
                    .collect();
                let original_schema = Arc::clone(&aggregate.schema);
                let required = aggregate
                    .group_expr
                    .iter()
                    .zip(&is_redundant)
                    .filter(|(_, redundant)| !**redundant)
                    .map(|(expr, _)| expr.clone())
                    .collect();
                let simplified_aggregate = Aggregate::try_new(
                    aggregate.input,
                    required,
                    None,
                    aggregate.aggr_expr,
                )?;
                let simplified_schema = Arc::clone(&simplified_aggregate.schema);

                // Refer to the output columns of the simplified aggregate by
                // position, keeping the ids of the outputs of the original
                // aggregate: a redundant expression defines its output, the
                // others pass the outputs of the simplified aggregate (which
                // keeps their entries) through. Do not re-plan the original group expressions on top
                // of it: an expression can refer to an input column through a
                // name that the aggregate output does not carry, so a rebuilt
                // projection does not always resolve. A redundant expression
                // refers only to bare GROUP BY columns, which the simplified
                // aggregate outputs unchanged.
                let mut projection_expr =
                    Vec::with_capacity(original_schema.fields().len());
                let mut next_output = 0;
                for (i, (expr, redundant)) in aggregate
                    .group_expr
                    .into_iter()
                    .zip(is_redundant)
                    .enumerate()
                {
                    let (qualifier, field) = original_schema.qualified_field(i);
                    let id = expr.id;
                    let (expr, passes_through) = if redundant {
                        let expr = expr
                            .expr
                            .transform(|expr| match expr {
                                Expr::Column(c) => {
                                    // by id, else as the rule found it
                                    // redundant (by name)
                                    let output = group_by_outputs
                                        .iter()
                                        .find(|(col, _)| col.same_column(&c))
                                        .or_else(|| {
                                            group_by_outputs
                                                .iter()
                                                .find(|(col, _)| *col == c)
                                        });
                                    match output {
                                        Some((_, id)) => Ok(Transformed::yes(
                                            Expr::Column(c.with_id(Some(*id))),
                                        )),
                                        None => Ok(Transformed::no(Expr::Column(c))),
                                    }
                                }
                                _ => Ok(Transformed::no(expr)),
                            })?
                            .data;
                        (expr, false)
                    } else {
                        let column =
                            Column::from(simplified_schema.qualified_field(next_output))
                                .with_id(simplified_schema.field_id(next_output));
                        next_output += 1;
                        (Expr::Column(column), true)
                    };
                    let entry = if expr.qualified_name()
                        == (qualifier.cloned(), field.name().clone())
                    {
                        NamedExpr::new(expr, id)
                    } else if passes_through {
                        // the column is renamed: a new column
                        NamedExpr::new(
                            expr.alias_qualified(qualifier.cloned(), field.name()),
                            config.column_id_generator().next(),
                        )
                    } else {
                        NamedExpr::new(
                            expr.alias_qualified(qualifier.cloned(), field.name()),
                            id,
                        )
                    };
                    projection_expr.push(entry);
                }
                for i in next_output..simplified_schema.fields().len() {
                    let column = Column::from(simplified_schema.qualified_field(i))
                        .with_id(simplified_schema.field_id(i));
                    projection_expr.push(NamedExpr::allocate(
                        Expr::Column(column),
                        &simplified_schema,
                        config.column_id_generator(),
                    ));
                }

                let input = LogicalPlan::Aggregate(simplified_aggregate);
                let projection = LogicalPlan::Projection(Projection::try_new(
                    projection_expr,
                    Arc::new(input),
                )?);

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

/// Checks if a GROUP BY expression is redundant (can be removed without
/// changing grouping semantics). An expression is redundant if it is a
/// deterministic function of constants and columns already present as bare
/// column references in the GROUP BY.
fn is_redundant_group_expr(expr: &Expr, group_by_columns: &HashSet<&Column>) -> bool {
    // Bare column references are never redundant - they define the grouping
    if matches!(expr, Expr::Column(_)) {
        return false;
    }
    is_deterministic_of(expr, group_by_columns)
}

/// Returns true if `expr` is a deterministic expression whose only column
/// references are contained in `known_columns`.
fn is_deterministic_of(expr: &Expr, known_columns: &HashSet<&Column>) -> bool {
    match expr {
        Expr::Alias(e) => is_deterministic_of(&e.expr, known_columns),
        // by id (a reference's label can differ from the grouping column's)
        Expr::Column(c) => known_columns.iter().any(|known| known.same_column(c)),
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

    /// A required GROUP BY expression refers to an input column without the
    /// qualifier that the input schema gives it. `ExtractLeafExpressions`
    /// creates such references when it moves an extraction projection below a
    /// `SubqueryAlias`. The rule must keep the plan valid.
    #[test]
    fn test_eliminate_with_unqualified_reference_to_qualified_input() -> Result<()> {
        let input = LogicalPlanBuilder::from(test_table_scan()?)
            .alias("m")?
            .build()?;
        let plan = LogicalPlan::Aggregate(new_aggregate(
            Arc::new(input),
            vec![
                Expr::Column(Column::new_unqualified("b")).alias("k"),
                col("m.a"),
                col("m.a") + lit(1u32),
            ],
            vec![count(col("m.c"))],
        )?);

        assert_optimized_plan_equal!(plan, @r"
        Projection: k, m.a, m.a + UInt32(1), count(m.c)
          Aggregate: groupBy=[[b AS k, m.a]], aggr=[[count(m.c)]]
            SubqueryAlias: m
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
