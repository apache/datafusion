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

//! [`OptimizeUnions`]: removes `Union` nodes in the logical plan.
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::expr_rewriter::coerce_plan_expr_for_schema;
use datafusion_expr::{Distinct, LogicalPlan, Projection, Union};
use itertools::Itertools;
use std::sync::Arc;

#[derive(Default, Debug)]
/// An optimization rule that
/// 1. replaces nested unions with a single union.
/// 2. removes unions with a single input.
pub struct OptimizeUnions;

impl OptimizeUnions {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for OptimizeUnions {
    fn name(&self) -> &str {
        "optimize_unions"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Union(Union { mut inputs, .. }) if inputs.len() == 1 => Ok(
                Transformed::yes(Arc::unwrap_or_clone(inputs.pop().unwrap())),
            ),
            LogicalPlan::Union(Union { inputs, schema }) => {
                let inputs = inputs
                    .into_iter()
                    .flat_map(extract_plans_from_union)
                    .map(|plan| coerce_plan_expr_for_schema(plan, &schema))
                    .collect::<Result<Vec<_>>>()?;

                Ok(Transformed::yes(LogicalPlan::Union(Union {
                    inputs: inputs.into_iter().map(Arc::new).collect_vec(),
                    schema,
                })))
            }
            LogicalPlan::Distinct(Distinct::All(nested_plan)) => {
                match Arc::unwrap_or_clone(nested_plan) {
                    LogicalPlan::Union(Union { inputs, schema }) => {
                        let inputs = inputs
                            .into_iter()
                            .map(extract_plan_from_distinct)
                            .flat_map(extract_plans_from_union)
                            .map(|plan| coerce_plan_expr_for_schema(plan, &schema))
                            .collect::<Result<Vec<_>>>()?;

                        Ok(Transformed::yes(LogicalPlan::Distinct(Distinct::All(
                            Arc::new(LogicalPlan::Union(Union {
                                inputs: inputs.into_iter().map(Arc::new).collect_vec(),
                                schema: Arc::clone(&schema),
                            })),
                        ))))
                    }
                    nested_plan => Ok(Transformed::no(LogicalPlan::Distinct(
                        Distinct::All(Arc::new(nested_plan)),
                    ))),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn extract_plans_from_union(plan: Arc<LogicalPlan>) -> Vec<LogicalPlan> {
    match Arc::unwrap_or_clone(plan) {
        LogicalPlan::Union(Union { inputs, .. }) => inputs
            .into_iter()
            .map(Arc::unwrap_or_clone)
            .collect::<Vec<_>>(),
        // While unnesting, unwrap a Projection whose input is a nested Union,
        // flatten the inner Union, and push the same Projection down onto
        // each of the nested Union’s children.
        //
        // Example:
        //   Union { Projection { Union { plan1, plan2 } }, plan3 }
        //     => Union { Projection { plan1 }, Projection { plan2 }, plan3 }
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            ..
        }) => match Arc::unwrap_or_clone(input) {
            LogicalPlan::Union(Union { inputs, .. }) => inputs
                .into_iter()
                .map(Arc::unwrap_or_clone)
                .map(|plan| {
                    LogicalPlan::Projection(
                        Projection::try_new_with_schema(
                            expr.clone(),
                            Arc::new(plan),
                            Arc::clone(&schema),
                        )
                        .unwrap(),
                    )
                })
                .collect::<Vec<_>>(),

            plan => vec![LogicalPlan::Projection(
                Projection::try_new_with_schema(expr, Arc::new(plan), schema).unwrap(),
            )],
        },
        plan => vec![plan],
    }
}

fn extract_plan_from_distinct(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    match Arc::unwrap_or_clone(plan) {
        LogicalPlan::Distinct(Distinct::All(plan)) => plan,
        plan => Arc::new(plan),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::analyzer::Analyzer;
    use crate::analyzer::type_coercion::TypeCoercion;
    use crate::assert_optimized_plan_eq_snapshot;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{col, logical_plan::table_scan};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])
    }

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let options = ConfigOptions::default();
            let analyzed_plan = Analyzer::with_rules(vec![Arc::new(TypeCoercion::new())])
                .execute_and_check($plan, &options, |_, _| {})?;
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(OptimizeUnions::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                analyzed_plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn eliminate_nothing() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder.clone().union(plan_builder.build()?)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          TableScan: table
          TableScan: table
        ")
    }

    #[test]
    fn eliminate_distinct_nothing() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union_distinct(plan_builder.build()?)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            TableScan: table
            TableScan: table
        ")
    }

    #[test]
    fn eliminate_nested_union() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .union(plan_builder.build()?)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          TableScan: table
          TableScan: table
          TableScan: table
          TableScan: table
        ")
    }

    #[test]
    fn eliminate_nested_union_with_distinct_union() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union_distinct(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .union(plan_builder.build()?)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          Distinct:
            Union
              TableScan: table
              TableScan: table
          TableScan: table
          TableScan: table
        ")
    }

    #[test]
    fn eliminate_nested_distinct_union() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .union_distinct(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .union_distinct(plan_builder.build()?)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            TableScan: table
            TableScan: table
            TableScan: table
            TableScan: table
            TableScan: table
        ")
    }

    #[test]
    fn eliminate_nested_distinct_union_with_distinct_table() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union_distinct(plan_builder.clone().distinct()?.build()?)?
            .union(plan_builder.clone().distinct()?.build()?)?
            .union_distinct(plan_builder.build()?)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            TableScan: table
            TableScan: table
            TableScan: table
            TableScan: table
        ")
    }

    // We don't need to use project_with_column_index in logical optimizer,
    // after LogicalPlanBuilder::union, we already have all equal expression aliases
    #[test]
    fn eliminate_nested_union_with_projection() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(
                plan_builder
                    .clone()
                    .project(vec![col("id").alias("table_id"), col("key"), col("value")])?
                    .build()?,
            )?
            .union(
                plan_builder
                    .project(vec![col("id").alias("_id"), col("key"), col("value")])?
                    .build()?,
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          TableScan: table
          Projection: table.id AS id, table.key, table.value
            TableScan: table
          Projection: table.id AS id, table.key, table.value
            TableScan: table
        ")
    }

    #[test]
    fn eliminate_nested_distinct_union_with_projection() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union_distinct(
                plan_builder
                    .clone()
                    .project(vec![col("id").alias("table_id"), col("key"), col("value")])?
                    .build()?,
            )?
            .union_distinct(
                plan_builder
                    .project(vec![col("id").alias("_id"), col("key"), col("value")])?
                    .build()?,
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            TableScan: table
            Projection: table.id AS id, table.key, table.value
              TableScan: table
            Projection: table.id AS id, table.key, table.value
              TableScan: table
        ")
    }

    #[test]
    fn eliminate_nested_union_in_projection() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .project(vec![col("id").alias("table_id"), col("key"), col("value")])?
            .union(plan_builder.build()?)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          Projection: id AS table_id, key, value
            TableScan: table
          Projection: id AS table_id, key, value
            TableScan: table
          TableScan: table
        ")
    }

    #[test]
    fn eliminate_nested_union_with_type_cast_projection() -> Result<()> {
        let table_1 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]),
            None,
        )?;

        let table_2 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float32, false),
            ]),
            None,
        )?;

        let table_3 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int16, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float32, false),
            ]),
            None,
        )?;

        let plan = table_1
            .union(table_2.build()?)?
            .union(table_3.build()?)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          TableScan: table_1
          Projection: CAST(table_1.id AS Int64) AS id, table_1.key, CAST(table_1.value AS Float64) AS value
            TableScan: table_1
          Projection: CAST(table_1.id AS Int64) AS id, table_1.key, CAST(table_1.value AS Float64) AS value
            TableScan: table_1
        ")
    }

    #[test]
    fn eliminate_nested_distinct_union_with_type_cast_projection() -> Result<()> {
        let table_1 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]),
            None,
        )?;

        let table_2 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float32, false),
            ]),
            None,
        )?;

        let table_3 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int16, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float32, false),
            ]),
            None,
        )?;

        let plan = table_1
            .union_distinct(table_2.build()?)?
            .union_distinct(table_3.build()?)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            TableScan: table_1
            Projection: CAST(table_1.id AS Int64) AS id, table_1.key, CAST(table_1.value AS Float64) AS value
              TableScan: table_1
            Projection: CAST(table_1.id AS Int64) AS id, table_1.key, CAST(table_1.value AS Float64) AS value
              TableScan: table_1
        ")
    }

    #[test]
    fn eliminate_one_union() -> Result<()> {
        let plan = table_scan(Some("table"), &schema(), None)?.build()?;
        let schema = Arc::clone(plan.schema());
        // note it is not possible to create a single input union via
        // LogicalPlanBuilder so create it manually here
        let plan = LogicalPlan::Union(Union {
            inputs: vec![Arc::new(plan)],
            schema,
        });

        // Note we can't use the same assert_optimized_plan_equal as creating a
        // single input union is not possible via LogicalPlanBuilder and other passes
        // throw errors / don't handle the schema correctly.
        assert_optimized_plan_eq_snapshot!(
            OptimizerContext::new().with_max_passes(1),
            vec![Arc::new(OptimizeUnions::new())],
            plan,
            @r"
        TableScan: table
        "
        )
    }
}
