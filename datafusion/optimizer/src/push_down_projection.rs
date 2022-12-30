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

use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::Field;
use arrow::error::Result as ArrowResult;
use datafusion_common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, Result, ToDFSchema,
};
use datafusion_expr::utils::grouping_set_to_exprlist;
use datafusion_expr::{
    logical_plan::{
        builder::{build_join_schema, LogicalPlanBuilder},
        Aggregate, Analyze, Join, LogicalPlan, Projection, SubqueryAlias, TableScan,
        Union, Window,
    },
    utils::{expr_to_columns, exprlist_to_columns, find_sort_exprs, from_plan},
    Expr,
};
use std::collections::HashMap;
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

/// Optimizer that removes unused projections and aggregations from plans
/// This reduces both scans and
#[derive(Default)]
pub struct PushDownProjection {}

impl OptimizerRule for PushDownProjection {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // set of all columns referred by the plan (and thus considered required by the root)
        let required_columns = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.qualified_column())
            .collect::<HashSet<Column>>();
        Ok(Some(optimize_plan(
            self,
            plan,
            &required_columns,
            false,
            config,
        )?))
    }

    fn name(&self) -> &str {
        "push_down_projection"
    }
}

impl PushDownProjection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Recursively transverses the logical plan removing expressions and that are not needed.
fn optimize_plan(
    _optimizer: &PushDownProjection,
    plan: &LogicalPlan,
    required_columns: &HashSet<Column>, // set of columns required up to this step
    has_projection: bool,
    _config: &dyn OptimizerConfig,
) -> Result<LogicalPlan> {
    let mut new_required_columns = required_columns.clone();
    let new_plan = match plan {
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            ..
        }) => {
            // projection:
            // * remove any expression that is not required
            // * construct the new set of required columns

            let mut new_expr = Vec::new();
            let mut new_fields = Vec::new();
            // When meet projection, its expr must contain all columns that its child need.
            // So we need create a empty required_columns instead use original new_required_columns.
            // Otherwise it cause redundant columns.
            let mut new_required_columns = HashSet::new();

            // Gather all columns needed for expressions in this Projection
            schema.fields().iter().enumerate().for_each(|(i, field)| {
                if required_columns.contains(&field.qualified_column()) {
                    new_expr.push(expr[i].clone());
                    new_fields.push(field.clone());
                }
            });

            for e in new_expr.iter() {
                expr_to_columns(e, &mut new_required_columns)?
            }

            let new_input =
                optimize_plan(_optimizer, input, &new_required_columns, true, _config)?;

            let new_required_columns_optimized = new_input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<Column>>();

            let all_column_exprs = new_expr.iter().all(|e| matches!(e, Expr::Column(_)));

            if new_fields.is_empty()
                || (has_projection
                    && all_column_exprs
                    && &new_required_columns_optimized == required_columns)
            {
                // no need for an expression at all
                Ok(new_input)
            } else {
                let metadata = new_input.schema().metadata().clone();
                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    new_expr,
                    Arc::new(new_input),
                    DFSchemaRef::new(DFSchema::new_with_metadata(new_fields, metadata)?),
                )?))
            }
        }
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            null_equals_null,
            ..
        }) => {
            for (l, r) in on {
                new_required_columns.extend(l.to_columns()?);
                new_required_columns.extend(r.to_columns()?);
            }

            if let Some(expr) = filter {
                expr_to_columns(expr, &mut new_required_columns)?;
            }

            let optimized_left = Arc::new(optimize_plan(
                _optimizer,
                left,
                &new_required_columns,
                true,
                _config,
            )?);

            let optimized_right = Arc::new(optimize_plan(
                _optimizer,
                right,
                &new_required_columns,
                true,
                _config,
            )?);

            let schema = build_join_schema(
                optimized_left.schema(),
                optimized_right.schema(),
                join_type,
            )?;

            Ok(LogicalPlan::Join(Join {
                left: optimized_left,
                right: optimized_right,
                join_type: *join_type,
                join_constraint: *join_constraint,
                on: on.clone(),
                filter: filter.clone(),
                schema: DFSchemaRef::new(schema),
                null_equals_null: *null_equals_null,
            }))
        }
        LogicalPlan::Window(Window {
            window_expr, input, ..
        }) => {
            // Gather all columns needed for expressions in this Window
            let mut new_window_expr = Vec::new();
            {
                window_expr.iter().try_for_each(|expr| {
                    let name = &expr.display_name()?;
                    let column = Column::from_name(name);
                    if required_columns.contains(&column) {
                        new_window_expr.push(expr.clone());
                        new_required_columns.insert(column);
                        // add to the new set of required columns
                        expr_to_columns(expr, &mut new_required_columns)
                    } else {
                        Ok(())
                    }
                })?;
            }

            // none columns in window expr are needed, remove the window expr
            if new_window_expr.is_empty() {
                return LogicalPlanBuilder::from(optimize_plan(
                    _optimizer,
                    input,
                    required_columns,
                    true,
                    _config,
                )?)
                .build();
            };

            // for all the retained window expr, find their sort expressions if any, and retain these
            exprlist_to_columns(
                &find_sort_exprs(&new_window_expr),
                &mut new_required_columns,
            )?;

            LogicalPlanBuilder::from(optimize_plan(
                _optimizer,
                input,
                &new_required_columns,
                true,
                _config,
            )?)
            .window(new_window_expr)?
            .build()
        }
        LogicalPlan::Aggregate(Aggregate {
            group_expr,
            aggr_expr,
            input,
            ..
        }) => {
            // aggregate:
            // * remove any aggregate expression that is not required
            // * construct the new set of required columns

            // Find distinct group by exprs in the case where we have a grouping set
            let all_group_expr: Vec<Expr> = grouping_set_to_exprlist(group_expr)?;
            exprlist_to_columns(&all_group_expr, &mut new_required_columns)?;

            // Gather all columns needed for expressions in this Aggregate
            let mut new_aggr_expr = Vec::new();
            aggr_expr.iter().try_for_each(|expr| {
                let name = &expr.display_name()?;
                let column = Column::from_name(name);
                if required_columns.contains(&column) {
                    new_aggr_expr.push(expr.clone());
                    new_required_columns.insert(column);

                    // add to the new set of required columns
                    expr_to_columns(expr, &mut new_required_columns)
                } else {
                    Ok(())
                }
            })?;

            Ok(LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(optimize_plan(
                    _optimizer,
                    input,
                    &new_required_columns,
                    true,
                    _config,
                )?),
                group_expr.clone(),
                new_aggr_expr,
            )?))
        }
        // scans:
        // * remove un-used columns from the scan projection
        LogicalPlan::TableScan(scan) => {
            push_down_scan(scan, &new_required_columns, has_projection)
        }
        LogicalPlan::Explain { .. } => Err(DataFusionError::Internal(
            "Unsupported logical plan: Explain must be root of the plan".to_string(),
        )),
        LogicalPlan::Analyze(a) => {
            // make sure we keep all the columns from the input plan
            let required_columns = a
                .input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<Column>>();

            Ok(LogicalPlan::Analyze(Analyze {
                input: Arc::new(optimize_plan(
                    _optimizer,
                    &a.input,
                    &required_columns,
                    false,
                    _config,
                )?),
                verbose: a.verbose,
                schema: a.schema.clone(),
            }))
        }
        LogicalPlan::Union(Union { inputs, schema }) => {
            // UNION inputs will reference the same column with different identifiers, so we need
            // to populate new_required_columns by unqualified column name based on required fields
            // from the resulting UNION output
            let union_required_fields = schema
                .fields()
                .iter()
                .filter(|f| new_required_columns.contains(&f.qualified_column()))
                .map(|f| f.field())
                .collect::<HashSet<&Field>>();
            let new_inputs = inputs
                .iter()
                .map(|input_plan| {
                    input_plan
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| union_required_fields.contains(f.field()))
                        .for_each(|f| {
                            new_required_columns.insert(f.qualified_column());
                        });
                    optimize_plan(
                        _optimizer,
                        input_plan,
                        &new_required_columns,
                        has_projection,
                        _config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let new_schema = DFSchema::new_with_metadata(
                schema
                    .fields()
                    .iter()
                    .filter(|f| union_required_fields.contains(f.field()))
                    .cloned()
                    .collect(),
                schema.metadata().clone(),
            )?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs.iter().cloned().map(Arc::new).collect(),
                schema: Arc::new(new_schema),
            }))
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
            let new_required_columns =
                replace_alias(required_columns, alias, input.schema());
            let child = optimize_plan(
                _optimizer,
                input,
                &new_required_columns,
                has_projection,
                _config,
            )?;
            from_plan(plan, &plan.expressions(), &[child])
        }
        // all other nodes: Add any additional columns used by
        // expressions in this node to the list of required columns
        LogicalPlan::Limit(_)
        | LogicalPlan::Filter { .. }
        | LogicalPlan::Repartition(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Sort { .. }
        | LogicalPlan::CreateExternalTable(_)
        | LogicalPlan::CreateMemoryTable(_)
        | LogicalPlan::CreateView(_)
        | LogicalPlan::CreateCatalogSchema(_)
        | LogicalPlan::CreateCatalog(_)
        | LogicalPlan::DropTable(_)
        | LogicalPlan::DropView(_)
        | LogicalPlan::SetVariable(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Extension { .. }
        | LogicalPlan::Prepare(_) => {
            let expr = plan.expressions();
            // collect all required columns by this plan
            exprlist_to_columns(&expr, &mut new_required_columns)?;

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|input_plan| {
                    optimize_plan(
                        _optimizer,
                        input_plan,
                        &new_required_columns,
                        has_projection,
                        _config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            from_plan(plan, &expr, &new_inputs)
        }
    };

    // when this rule is applied multiple times it will insert duplicate nested projections,
    // so we catch this here
    let with_dupe_projection_removed = match new_plan? {
        LogicalPlan::Projection(p) => match p.input.as_ref() {
            LogicalPlan::Projection(p2) if projection_equal(&p, p2) => {
                LogicalPlan::Projection(p2.clone())
            }
            _ => LogicalPlan::Projection(p),
        },
        other => other,
    };

    Ok(with_dupe_projection_removed)
}

fn projection_equal(p: &Projection, p2: &Projection) -> bool {
    p.expr.len() == p2.expr.len() && p.expr.iter().zip(&p2.expr).all(|(l, r)| l == r)
}

fn replace_alias(
    required_columns: &HashSet<Column>,
    alias: &str,
    input_schema: &DFSchemaRef,
) -> HashSet<Column> {
    let mut map = HashMap::new();
    for field in input_schema.fields() {
        let col = field.qualified_column();
        let alias_col = Column {
            relation: Some(alias.to_owned()),
            name: col.name.clone(),
        };
        map.insert(alias_col, col);
    }
    required_columns
        .iter()
        .map(|col| map.get(col).unwrap_or(col).clone())
        .collect::<HashSet<_>>()
}

fn push_down_scan(
    scan: &TableScan,
    required_columns: &HashSet<Column>,
    has_projection: bool,
) -> Result<LogicalPlan> {
    // once we reach the table scan, we can use the accumulated set of column
    // names to construct the set of column indexes in the scan
    //
    // we discard non-existing columns because some column names are not part of the schema,
    // e.g. when the column derives from an aggregation
    //
    // Use BTreeSet to remove potential duplicates (e.g. union) as
    // well as to sort the projection to ensure deterministic behavior
    let schema = scan.source.schema();
    let mut projection: BTreeSet<usize> = required_columns
        .iter()
        .filter(|c| {
            c.relation.is_none() || c.relation.as_ref().unwrap() == &scan.table_name
        })
        .map(|c| schema.index_of(&c.name))
        .filter_map(ArrowResult::ok)
        .collect();

    if projection.is_empty() {
        if has_projection && !schema.fields().is_empty() {
            // Ensure that we are reading at least one column from the table in case the query
            // does not reference any columns directly such as "SELECT COUNT(1) FROM table",
            // except when the table is empty (no column)
            projection.insert(0);
        } else {
            // for table scan without projection, we default to return all columns
            projection = scan
                .source
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<BTreeSet<usize>>();
        }
    }

    // create the projected schema
    let projected_fields: Vec<DFField> = projection
        .iter()
        .map(|i| DFField::from_qualified(&scan.table_name, schema.fields()[*i].clone()))
        .collect();

    let projection = projection.into_iter().collect::<Vec<_>>();
    let projected_schema = projected_fields.to_dfschema_ref()?;

    // return the table scan with projection
    Ok(LogicalPlan::TableScan(TableScan {
        table_name: scan.table_name.clone(),
        source: scan.source.clone(),
        projection: Some(projection),
        projected_schema,
        filters: scan.filters.clone(),
        fetch: scan.fetch,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use crate::OptimizerContext;
    use arrow::datatypes::{DataType, Schema};
    use datafusion_expr::expr;
    use datafusion_expr::expr::Cast;
    use datafusion_expr::{
        col, count, lit,
        logical_plan::{builder::LogicalPlanBuilder, JoinType},
        max, min, AggregateFunction, Expr,
    };
    use std::collections::HashMap;

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(test.b)]]\
        \n  TableScan: test projection=[b]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[MAX(test.b)]]\
        \n  TableScan: test projection=[b, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_group_by_with_table_alias() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("a")?
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[a.c]], aggr=[[MAX(a.b)]]\
        \n  SubqueryAlias: a\
        \n    TableScan: test projection=[b, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_no_group_by_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("c").gt(lit(1)))?
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(test.b)]]\
        \n  Filter: test.c > Int32(1)\
        \n    TableScan: test projection=[b, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn redundant_project() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        let expected = "Projection: test.a, test.c, test.b\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reorder_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .build()?;
        let expected = "Projection: test.c, test.b, test.a\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn noncontinuous_redundant_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("b").gt(lit(1)))?
            .filter(col("a").gt(lit(1)))?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        let expected = "Projection: test.a, test.c, test.b\
        \n  Filter: test.a > Int32(1)\
        \n    Filter: test.b > Int32(1)\
        \n      Filter: test.c > Int32(1)\
        \n        TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn join_schema_trim_full_join_column_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]), None)?
            .project(vec![col("a"), col("b"), col("c1")])?
            .build()?;

        // make sure projections are pushed down to both table scans
        let expected = "Projection: test.a, test.b, test2.c1\
        \n  Left Join: test.a = test2.c1\
        \n    TableScan: test projection=[a, b]\
        \n    TableScan: test2 projection=[c1]";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "c1", DataType::UInt32, false),
                ],
                HashMap::new(),
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_partial_join_column_projection() -> Result<()> {
        // test join column push down without explicit column projections

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]), None)?
            // projecting joined column `a` should push the right side column `c1` projection as
            // well into test2 table even though `c1` is not referenced in projection.
            .project(vec![col("a"), col("b")])?
            .build()?;

        // make sure projections are pushed down to both table scans
        let expected = "Projection: test.a, test.b\
        \n  Left Join: test.a = test2.c1\
        \n    TableScan: test projection=[a, b]\
        \n    TableScan: test2 projection=[c1]";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "c1", DataType::UInt32, false),
                ],
                HashMap::new(),
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_using_join() -> Result<()> {
        // shared join columns from using join should be pushed to both sides

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join_using(table2_scan, JoinType::Left, vec!["a"])?
            .project(vec![col("a"), col("b")])?
            .build()?;

        // make sure projections are pushed down to table scan
        let expected = "Projection: test.a, test.b\
        \n  Left Join: Using test.a = test2.a\
        \n    TableScan: test projection=[a, b]\
        \n    TableScan: test2 projection=[a]";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "a", DataType::UInt32, false),
                ],
                HashMap::new(),
            )?,
        );

        Ok(())
    }

    #[test]
    fn cast() -> Result<()> {
        let table_scan = test_table_scan()?;

        let projection = LogicalPlanBuilder::from(table_scan)
            .project(vec![Expr::Cast(Cast::new(
                Box::new(col("c")),
                DataType::Float64,
            ))])?
            .build()?;

        let expected = "Projection: CAST(test.c AS Float64)\
        \n  TableScan: test projection=[c]";

        assert_optimized_plan_eq(&projection, expected);

        Ok(())
    }

    #[test]
    fn table_scan_projected_schema() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "Projection: test.a, test.b\
        \n  TableScan: test projection=[a, b]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_scan_projected_schema_non_qualified_relation() -> Result<()> {
        let table_scan = test_table_scan()?;
        let input_schema = table_scan.schema();
        assert_eq!(3, input_schema.fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // Build the LogicalPlan directly (don't use PlanBuilder), so
        // that the Column references are unqualified (e.g. their
        // relation is `None`). PlanBuilder resolves the expressions
        let expr = vec![col("a"), col("b")];
        let plan =
            LogicalPlan::Projection(Projection::try_new(expr, Arc::new(table_scan))?);

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "Projection: a, b\
        \n  TableScan: test projection=[a, b]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a")])?
            .limit(0, Some(5))?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a"]);

        let expected = "Limit: skip=0, fetch=5\
        \n  Projection: test.c, test.a\
        \n    TableScan: test projection=[a, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_scan_without_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan).build()?;
        // should expand projection to all columns without projection
        let expected = "TableScan: test projection=[a, b, c]";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn table_scan_with_literal_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1_i64), lit(2_i64)])?
            .build()?;
        let expected = "Projection: Int64(1), Int64(2)\
                      \n  TableScan: test projection=[a]";
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
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("c").gt(lit(1)))?
            .aggregate(vec![col("c")], vec![max(col("a"))])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "MAX(test.a)"]);

        let expected = "\
        Aggregate: groupBy=[[test.c]], aggr=[[MAX(test.a)]]\
        \n  Filter: test.c > Int32(1)\
        \n    Projection: test.c, test.a\
        \n      TableScan: test projection=[a, c]";

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
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a"]);

        let expected = "\
        Projection: Int32(1) AS a\
        \n  TableScan: test projection=[a]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    /// tests that optimizing twice yields same plan
    #[test]
    fn test_double_optimization() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        let optimized_plan1 = optimize(&plan).expect("failed to optimize plan");
        let optimized_plan2 =
            optimize(&optimized_plan1).expect("failed to optimize plan");

        let formatted_plan1 = format!("{optimized_plan1:?}");
        let formatted_plan2 = format!("{optimized_plan2:?}");
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
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("c")], vec![max(col("b")), min(col("b"))])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("MAX(test.b)")])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a", "MAX(test.b)"]);

        let expected = "Projection: test.c, test.a, MAX(test.b)\
        \n  Filter: test.c > Int32(1)\
        \n    Aggregate: groupBy=[[test.a, test.c]], aggr=[[MAX(test.b)]]\
        \n      TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_filter_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;

        let aggr_with_filter = Expr::AggregateFunction(expr::AggregateFunction::new(
            AggregateFunction::Count,
            vec![col("b")],
            false,
            Some(Box::new(col("c").gt(lit(42)))),
        ));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count(col("b")), aggr_with_filter.alias("count2")],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b), COUNT(test.b) FILTER (WHERE c > Int32(42)) AS count2]]\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);
    }

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = PushDownProjection::new();
        Ok(rule.try_optimize(plan, &OptimizerContext::new())?.unwrap())
    }
}
