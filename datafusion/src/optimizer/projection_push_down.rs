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

//! Projection Push Down optimizer rule ensures that only referenced columns are
//! loaded into memory

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::{DFField, DFSchema, DFSchemaRef, LogicalPlan, ToDFSchema};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use arrow::datatypes::Schema;
use arrow::error::Result as ArrowResult;
use std::{collections::HashSet, sync::Arc};
use utils::optimize_explain;

/// Optimizer that removes unused projections and aggregations from plans
/// This reduces both scans and
pub struct ProjectionPushDown {}

impl OptimizerRule for ProjectionPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        // set of all columns refered by the plan (and thus considered required by the root)
        let required_columns = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<HashSet<String>>();
        optimize_plan(self, plan, &required_columns, false, execution_props)
    }

    fn name(&self) -> &str {
        "projection_push_down"
    }
}

impl ProjectionPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn get_projected_schema(
    schema: &Schema,
    required_columns: &HashSet<String>,
    has_projection: bool,
) -> Result<(Vec<usize>, DFSchemaRef)> {
    // once we reach the table scan, we can use the accumulated set of column
    // names to construct the set of column indexes in the scan
    //
    // we discard non-existing columns because some column names are not part of the schema,
    // e.g. when the column derives from an aggregation
    let mut projection: Vec<usize> = required_columns
        .iter()
        .map(|name| schema.index_of(name))
        .filter_map(ArrowResult::ok)
        .collect();

    if projection.is_empty() {
        if has_projection {
            // Ensure that we are reading at least one column from the table in case the query
            // does not reference any columns directly such as "SELECT COUNT(1) FROM table"
            projection.push(0);
        } else {
            // for table scan without projection, we default to return all columns
            projection = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<Vec<usize>>();
        }
    }

    // sort the projection otherwise we get non-deterministic behavior
    projection.sort_unstable();

    // create the projected schema
    let mut projected_fields: Vec<DFField> = Vec::with_capacity(projection.len());
    for i in &projection {
        projected_fields.push(DFField::from(schema.fields()[*i].clone()));
    }

    Ok((projection, projected_fields.to_dfschema_ref()?))
}

/// Recursively transverses the logical plan removing expressions and that are not needed.
fn optimize_plan(
    optimizer: &ProjectionPushDown,
    plan: &LogicalPlan,
    required_columns: &HashSet<String>, // set of columns required up to this step
    has_projection: bool,
    execution_props: &ExecutionProps,
) -> Result<LogicalPlan> {
    let mut new_required_columns = required_columns.clone();
    match plan {
        LogicalPlan::Projection {
            input,
            expr,
            schema,
        } => {
            // projection:
            // * remove any expression that is not required
            // * construct the new set of required columns

            let mut new_expr = Vec::new();
            let mut new_fields = Vec::new();

            // Gather all columns needed for expressions in this Projection
            schema
                .fields()
                .iter()
                .enumerate()
                .try_for_each(|(i, field)| {
                    if required_columns.contains(field.name()) {
                        new_expr.push(expr[i].clone());
                        new_fields.push(field.clone());

                        // gather the new set of required columns
                        utils::expr_to_column_names(&expr[i], &mut new_required_columns)
                    } else {
                        Ok(())
                    }
                })?;

            let new_input = optimize_plan(
                optimizer,
                &input,
                &new_required_columns,
                true,
                execution_props,
            )?;
            if new_fields.is_empty() {
                // no need for an expression at all
                Ok(new_input)
            } else {
                Ok(LogicalPlan::Projection {
                    expr: new_expr,
                    input: Arc::new(new_input),
                    schema: DFSchemaRef::new(DFSchema::new(new_fields)?),
                })
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            schema,
        } => {
            for (l, r) in on {
                new_required_columns.insert(l.to_owned());
                new_required_columns.insert(r.to_owned());
            }
            Ok(LogicalPlan::Join {
                left: Arc::new(optimize_plan(
                    optimizer,
                    &left,
                    &new_required_columns,
                    true,
                    execution_props,
                )?),
                right: Arc::new(optimize_plan(
                    optimizer,
                    &right,
                    &new_required_columns,
                    true,
                    execution_props,
                )?),

                join_type: *join_type,
                on: on.clone(),
                schema: schema.clone(),
            })
        }
        LogicalPlan::Window {
            schema,
            window_expr,
            input,
            // FIXME implement next
            // filter_by_expr,
            // FIXME implement next
            // partition_by_expr,
            // FIXME implement next
            // order_by_expr,
            // FIXME implement next
            // window_frame,
            ..
        } => {
            // Gather all columns needed for expressions in this Window
            let mut new_window_expr = Vec::new();
            window_expr.iter().try_for_each(|expr| {
                let name = &expr.name(&schema)?;
                if required_columns.contains(name) {
                    new_window_expr.push(expr.clone());
                    new_required_columns.insert(name.clone());
                    // add to the new set of required columns
                    utils::expr_to_column_names(expr, &mut new_required_columns)
                } else {
                    Ok(())
                }
            })?;

            let new_schema = DFSchema::new(
                schema
                    .fields()
                    .iter()
                    .filter(|x| new_required_columns.contains(x.name()))
                    .cloned()
                    .collect(),
            )?;

            Ok(LogicalPlan::Window {
                window_expr: new_window_expr,
                // FIXME implement next
                // partition_by_expr: partition_by_expr.clone(),
                // FIXME implement next
                // order_by_expr: order_by_expr.clone(),
                // FIXME implement next
                // window_frame: window_frame.clone(),
                input: Arc::new(optimize_plan(
                    optimizer,
                    &input,
                    &new_required_columns,
                    true,
                    execution_props,
                )?),
                schema: DFSchemaRef::new(new_schema),
            })
        }
        LogicalPlan::Aggregate {
            schema,
            input,
            group_expr,
            aggr_expr,
            ..
        } => {
            // aggregate:
            // * remove any aggregate expression that is not required
            // * construct the new set of required columns

            utils::exprlist_to_column_names(group_expr, &mut new_required_columns)?;

            // Gather all columns needed for expressions in this Aggregate
            let mut new_aggr_expr = Vec::new();
            aggr_expr.iter().try_for_each(|expr| {
                let name = &expr.name(&schema)?;

                if required_columns.contains(name) {
                    new_aggr_expr.push(expr.clone());
                    new_required_columns.insert(name.clone());

                    // add to the new set of required columns
                    utils::expr_to_column_names(expr, &mut new_required_columns)
                } else {
                    Ok(())
                }
            })?;

            let new_schema = DFSchema::new(
                schema
                    .fields()
                    .iter()
                    .filter(|x| new_required_columns.contains(x.name()))
                    .cloned()
                    .collect(),
            )?;

            Ok(LogicalPlan::Aggregate {
                group_expr: group_expr.clone(),
                aggr_expr: new_aggr_expr,
                input: Arc::new(optimize_plan(
                    optimizer,
                    &input,
                    &new_required_columns,
                    true,
                    execution_props,
                )?),
                schema: DFSchemaRef::new(new_schema),
            })
        }
        // scans:
        // * remove un-used columns from the scan projection
        LogicalPlan::TableScan {
            table_name,
            source,
            filters,
            limit,
            ..
        } => {
            let (projection, projected_schema) =
                get_projected_schema(&source.schema(), required_columns, has_projection)?;

            // return the table scan with projection
            Ok(LogicalPlan::TableScan {
                table_name: table_name.to_string(),
                source: source.clone(),
                projection: Some(projection),
                projected_schema,
                filters: filters.clone(),
                limit: *limit,
            })
        }
        LogicalPlan::Explain {
            verbose,
            plan,
            stringified_plans,
            schema,
        } => {
            let schema = schema.as_ref().to_owned().into();
            optimize_explain(
                optimizer,
                *verbose,
                &*plan,
                stringified_plans,
                &schema,
                execution_props,
            )
        }
        // all other nodes: Add any additional columns used by
        // expressions in this node to the list of required columns
        LogicalPlan::Limit { .. }
        | LogicalPlan::Filter { .. }
        | LogicalPlan::Repartition { .. }
        | LogicalPlan::EmptyRelation { .. }
        | LogicalPlan::Sort { .. }
        | LogicalPlan::CreateExternalTable { .. }
        | LogicalPlan::Union { .. }
        | LogicalPlan::CrossJoin { .. }
        | LogicalPlan::Extension { .. } => {
            let expr = plan.expressions();
            // collect all required columns by this plan
            utils::exprlist_to_column_names(&expr, &mut new_required_columns)?;

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| {
                    optimize_plan(
                        optimizer,
                        plan,
                        &new_required_columns,
                        has_projection,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            utils::from_plan(plan, &expr, &new_inputs)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::logical_plan::{col, lit};
    use crate::logical_plan::{max, min, Expr, LogicalPlanBuilder};
    use crate::test::*;
    use arrow::datatypes::DataType;

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#b)]]\
        \n  TableScan: test projection=Some([1])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[#c]], aggr=[[MAX(#b)]]\
        \n  TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_no_group_by_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("c"))?
            .aggregate(vec![], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#b)]]\
        \n  Filter: #c\
        \n    TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn cast() -> Result<()> {
        let table_scan = test_table_scan()?;

        let projection = LogicalPlanBuilder::from(&table_scan)
            .project(vec![Expr::Cast {
                expr: Box::new(col("c")),
                data_type: DataType::Float64,
            }])?
            .build()?;

        let expected = "Projection: CAST(#c AS Float64)\
        \n  TableScan: test projection=Some([2])";

        assert_optimized_plan_eq(&projection, expected);

        Ok(())
    }

    #[test]
    fn table_scan_projected_schema() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "Projection: #a, #b\
        \n  TableScan: test projection=Some([0, 1])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("c"), col("a")])?
            .limit(5)?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a"]);

        let expected = "Limit: 5\
        \n  Projection: #c, #a\
        \n    TableScan: test projection=Some([0, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_scan_without_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan).build()?;
        // should expand projection to all columns without projection
        let expected = "TableScan: test projection=Some([0, 1, 2])";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn table_scan_with_literal_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![lit(1_i64), lit(2_i64)])?
            .build()?;
        let expected = "Projection: Int64(1), Int64(2)\
                      \n  TableScan: test projection=Some([0])";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// tests that it removes unused columns in projections
    #[test]
    fn table_unused_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // we never use "b" in the first projection => remove it
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("c").gt(lit(1)))?
            .aggregate(vec![col("c")], vec![max(col("a"))])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "MAX(a)"]);

        let expected = "\
        Aggregate: groupBy=[[#c]], aggr=[[MAX(#a)]]\
        \n  Filter: #c Gt Int32(1)\
        \n    Projection: #c, #a\
        \n      TableScan: test projection=Some([0, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    /// tests that it removes un-needed projections
    #[test]
    fn table_unused_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // there is no need for the first projection
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a"]);

        let expected = "\
        Projection: Int32(1) AS a\
        \n  TableScan: test projection=Some([0])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    /// tests that optimizing twice yields same plan
    #[test]
    fn test_double_optimization() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        let optimized_plan1 = optimize(&plan).expect("failed to optimize plan");
        let optimized_plan2 =
            optimize(&optimized_plan1).expect("failed to optimize plan");

        let formatted_plan1 = format!("{:?}", optimized_plan1);
        let formatted_plan2 = format!("{:?}", optimized_plan2);
        assert_eq!(formatted_plan1, formatted_plan2);
        Ok(())
    }

    /// tests that it removes an aggregate is never used downstream
    #[test]
    fn table_unused_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // we never use "min(b)" => remove it
        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![col("a"), col("c")], vec![max(col("b")), min(col("b"))])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("MAX(b)")])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a", "MAX(b)"]);

        let expected = "\
        Projection: #c, #a, #MAX(b)\
        \n  Filter: #c Gt Int32(1)\
        \n    Aggregate: groupBy=[[#a, #c]], aggr=[[MAX(#b)]]\
        \n      TableScan: test projection=Some([0, 1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = ProjectionPushDown::new();
        rule.optimize(plan, &ExecutionProps::new())
    }
}
