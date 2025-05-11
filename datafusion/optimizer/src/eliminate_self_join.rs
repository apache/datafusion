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

//! [`EliminateSelfJoin`] eliminates self joins on unique constraint columns

use std::sync::Arc;

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use arrow::datatypes::Field;
use datafusion_common::{tree_node::Transformed, HashSet, Result};
use datafusion_expr::{
    builder::subquery_alias, Expr, Filter, Join, JoinConstraint, JoinType, LogicalPlan,
    SubqueryAlias, TableScan,
};

#[derive(Default, Debug)]
pub struct EliminateSelfJoin;

impl EliminateSelfJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn is_unique_constraint(field: &Field) -> bool {
    // FIXME: This is a placeholder. In a real implementation, this function should check
    // if the field is part of a unique constraint in the schema.
    // For now, we will just check if the field name is "id".
    field.name() == "id"
}

fn optimize(left: &LogicalPlan, right: &LogicalPlan) -> Option<LogicalPlan> {
    match (left, right) {
        (LogicalPlan::TableScan(left_scan), LogicalPlan::TableScan(right_scan)) => {
            let filters = left_scan
                .filters
                .iter()
                .chain(right_scan.filters.iter())
                .cloned()
                .collect();
            // FIXME: double iteration over the filters
            let projection = match (&left_scan.projection, &right_scan.projection) {
                (Some(left_projection), Some(right_projection)) => Some(
                    left_projection
                        .iter()
                        .chain(right_projection.iter())
                        .cloned()
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>(),
                ),
                (Some(left_projection), None) => Some(left_projection.clone()),
                (None, Some(right_projection)) => Some(right_projection.clone()),
                (None, None) => None,
            };
            let fetch = match (left_scan.fetch, right_scan.fetch) {
                (Some(left_fetch), Some(right_fetch)) => {
                    Some(left_fetch.max(right_fetch))
                }
                (Some(rows), None) | (None, Some(rows)) => Some(rows),
                (None, None) => None,
            };
            let table_scan = TableScan::try_new(
                left_scan.table_name.clone(),
                Arc::clone(&left_scan.source),
                projection,
                filters,
                fetch,
            )
            .unwrap();
            Some(LogicalPlan::TableScan(table_scan))
        }
        (
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input: left_input,
                alias: left_alias,
                ..
            }),
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input: right_input, ..
            }),
        ) => {
            // TODO: rename aliases used in the join
            let plan = optimize(left_input, right_input)?;
            subquery_alias(plan, left_alias.clone()).ok()
        }
        _ => None,
    }
}

impl OptimizerRule for EliminateSelfJoin {
    fn name(&self) -> &str {
        "self_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(Join {
                ref left,
                ref right,
                ref on,
                join_type,
                join_constraint,
                ref filter,
                ..
            }) if join_type == JoinType::Inner
                && join_constraint == JoinConstraint::Using
                // TODO: equality of `inner` `apachearrow::datatypes::SchemaRef` doesn't
                // mean equality of the tables
                && left.schema().inner() == right.schema().inner() =>
            {
                // TODO: Check if left and right are the same table
                // If `on` includes a column with a unique constraint, we can eliminate the self join
                let mut unique_constraint = None;
                for on_expr in on {
                    match on_expr {
                        (Expr::Column(left_col), Expr::Column(right_col)) => {
                            let left_field = left.schema().field_with_name(
                                left_col.relation.as_ref(),
                                left_col.name(),
                            )?;
                            let right_field = right.schema().field_with_name(
                                right_col.relation.as_ref(),
                                right_col.name(),
                            )?;
                            if left_field == right_field
                                && is_unique_constraint(left_field)
                                && is_unique_constraint(right_field)
                            {
                                unique_constraint =
                                    Some((left_col.clone(), right_col.clone()));
                            }
                        }
                        _ => {
                            unreachable!("Join condition is not a column equality");
                        }
                    }
                }
                if unique_constraint.is_none() {
                    // If we don't have a unique constraint, we cannot eliminate the join
                    return Ok(Transformed::no(plan));
                }
                // If we reach here, it means we can eliminate the self join
                if let Some(plan) = optimize(left.as_ref(), right.as_ref()) {
                    let plan = if let Some(filter) = filter {
                        LogicalPlan::Filter(Filter::try_new(
                            filter.clone(),
                            Arc::new(plan),
                        )?)
                    } else {
                        plan
                    };
                    Ok(Transformed::yes(plan))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            // This is called `EliminateSelfJoin` after all
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::OptimizerContext;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Column, Result};
    use datafusion_expr::builder::subquery_alias;
    use datafusion_expr::table_scan;
    use datafusion_expr::JoinConstraint;
    use datafusion_expr::{
        col, lit, logical_plan::builder::LogicalPlanBuilder, JoinType, LogicalPlan,
    };
    use datafusion_sql::TableReference;
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(super::EliminateSelfJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    fn test_table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("department", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let left = table_scan(Some("employees"), &schema, None)?.build()?;
        let left = subquery_alias(left, TableReference::from("a"))?;
        let right = table_scan(Some("employees"), &schema, None)?.build()?;
        let right = subquery_alias(right, TableReference::from("b"))?;

        let plan = LogicalPlanBuilder::new(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("id")], vec![Column::from_name("id")]),
                None,
            )?
            .build()?;
        // TODO: double check if this monkey patch is needed. If only `join_keys` is specified, the `join_constraint` should be `Using`
        let join = match plan {
            LogicalPlan::Join(mut join) => {
                join.join_constraint = JoinConstraint::Using;
                join
            }
            _ => panic!("Expected a Join"),
        };
        LogicalPlanBuilder::new(LogicalPlan::Join(join))
            .filter(col("b.department").eq(lit("HR")))?
            .project(vec![col("a.id")])?
            .build()
    }

    #[test]
    fn join_on_unique_key_with_filter() -> Result<()> {
        let plan = test_table_scan()?;

        assert_optimized_plan_equal!(plan, @r#"
        Projection: a.id
          Filter: a.department = Utf8("HR")
            SubqueryAlias: a
              TableScan: employees
        "#)
    }
}
