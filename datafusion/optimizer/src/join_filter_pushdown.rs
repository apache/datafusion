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

//! [`JoinFilterPushdown`] pushdown join filter to scan dynamically

use datafusion_common::{tree_node::Transformed, DataFusionError};
use datafusion_expr::{utils::DynamicFilterColumn, Expr, JoinType, LogicalPlan};

use crate::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule};

#[derive(Default, Debug)]
pub struct JoinFilterPushdown {}

impl JoinFilterPushdown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for JoinFilterPushdown {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if !config.options().optimizer.dynamic_join_pushdown {
            return Ok(Transformed::no(plan));
        }

        match plan {
            LogicalPlan::Join(mut join) => {
                if unsupported_join_type(&(join.join_type)) {
                    return Ok(Transformed::no(LogicalPlan::Join(join)));
                }

                let mut columns = Vec::new();
                let mut build_side_names = Vec::new();
                // Iterate the on clause and generate the filter info
                for (left, right) in join.on.iter() {
                    // Only support left to be a column
                    if let (Expr::Column(l), Expr::Column(r)) = (left, right) {
                        columns.push(r.clone());
                        build_side_names.push(l.name().to_owned());
                    }
                }

                let mut probe = join.right.as_ref();
                // On the probe sides, we want to make sure that we can push the filter to the probe side
                loop {
                    if matches!(probe, LogicalPlan::TableScan(_)) {
                        break;
                    }
                    match probe {
                        LogicalPlan::Limit(_)
                        | LogicalPlan::Filter(_)
                        | LogicalPlan::Sort(_)
                        | LogicalPlan::Distinct(_) => {
                            probe = probe.inputs()[0];
                        }
                        LogicalPlan::Projection(project) => {
                            for column in &columns {
                                if !project.schema.has_column(column) {
                                    return Ok(Transformed::no(LogicalPlan::Join(join)));
                                }
                            }
                            probe = probe.inputs()[0];
                        }
                        _ => return Ok(Transformed::no(LogicalPlan::Join(join))),
                    }
                }
                let dynamic_columns = columns
                    .into_iter()
                    .zip(build_side_names)
                    .map(|(column, name)| DynamicFilterColumn::new(name, column))
                    .collect::<Vec<_>>();
                // Assign the value
                join = join.with_dynamic_pushdown_columns(dynamic_columns);
                Ok(Transformed::yes(LogicalPlan::Join(join)))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
    fn name(&self) -> &str {
        "join_filter_pushdown"
    }
}

fn unsupported_join_type(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::RightSemi | JoinType::RightAnti
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Column;
    use datafusion_expr::{
        col, logical_plan::table_scan, JoinType, LogicalPlan, LogicalPlanBuilder,
    };
    use std::sync::Arc;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ])
    }

    fn get_optimized_plan(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        Ok(generate_optimized_plan_with_rules(
            vec![Arc::new(JoinFilterPushdown::new())],
            plan,
        ))
    }

    #[test]
    fn test_inner_join_with_pushdown() -> Result<(), DataFusionError> {
        let t1 = table_scan(Some("t1"), &schema(), None)?.build()?;
        let t2 = table_scan(Some("t2"), &schema(), None)?.build()?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .build()?;
        let optimized_plan = get_optimized_plan(plan)?;
        if let LogicalPlan::Join(join) = optimized_plan {
            assert!(join.dynamic_pushdown_columns.is_some());
            assert_eq!(join.dynamic_pushdown_columns.as_ref().unwrap().len(), 1);
            assert_eq!(
                join.dynamic_pushdown_columns.as_ref().unwrap()[0]
                    .column
                    .name,
                "a"
            );
        } else {
            panic!("Expected Join operation");
        }
        Ok(())
    }

    #[test]
    fn test_left_join_no_pushdown() -> Result<(), DataFusionError> {
        let t1 = table_scan(Some("t1"), &schema(), None)?.build()?;
        let t2 = table_scan(Some("t2"), &schema(), None)?.build()?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .build()?;

        let optimized_plan = get_optimized_plan(plan)?;
        if let LogicalPlan::Join(join) = optimized_plan {
            assert!(join.dynamic_pushdown_columns.is_none());
        } else {
            panic!("Expected Join operation");
        }
        Ok(())
    }

    #[test]
    fn test_join_with_projection() -> Result<(), DataFusionError> {
        let t1 = table_scan(Some("t1"), &schema(), None)?.build()?;
        let t2 = table_scan(Some("t2"), &schema(), None)?.build()?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .project(vec![col("t1.a"), col("t2.b")])?
            .build()?;

        let optimized_plan = get_optimized_plan(plan)?;
        if let LogicalPlan::Projection(projection) = optimized_plan {
            if let LogicalPlan::Join(join) = projection.input.as_ref() {
                assert!(join.dynamic_pushdown_columns.is_some());
                assert_eq!(join.dynamic_pushdown_columns.as_ref().unwrap().len(), 1);
                assert_eq!(
                    join.dynamic_pushdown_columns.as_ref().unwrap()[0]
                        .column
                        .name,
                    "a"
                );
            } else {
                panic!("Expected Join operation under Projection");
            }
        } else {
            panic!("Expected Projection operation");
        }
        Ok(())
    }

    #[test]
    fn test_join_with_multiple_keys() -> Result<(), DataFusionError> {
        let t1 = table_scan(Some("t1"), &schema(), None)?.build()?;
        let t2 = table_scan(Some("t2"), &schema(), None)?.build()?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Inner,
                (
                    vec![Column::from_name("a"), Column::from_name("b")],
                    vec![Column::from_name("a"), Column::from_name("b")],
                ),
                None,
            )?
            .build()?;

        let optimized_plan = get_optimized_plan(plan)?;
        if let LogicalPlan::Join(join) = optimized_plan {
            assert!(join.dynamic_pushdown_columns.is_some());
            assert_eq!(join.dynamic_pushdown_columns.as_ref().unwrap().len(), 2);
            assert_eq!(
                join.dynamic_pushdown_columns.as_ref().unwrap()[0]
                    .column
                    .name(),
                "a"
            );
            assert_eq!(
                join.dynamic_pushdown_columns.as_ref().unwrap()[1]
                    .column
                    .name(),
                "b"
            );
        } else {
            panic!("Expected Join operation");
        }
        Ok(())
    }

    #[test]
    fn test_join_with_filter() -> Result<(), DataFusionError> {
        let t1 = table_scan(Some("t1"), &schema(), None)?.build()?;
        let t2 = table_scan(Some("t2"), &schema(), None)?.build()?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.b").gt(col("t1.c")))?
            .build()?;

        let optimized_plan = get_optimized_plan(plan)?;
        if let LogicalPlan::Filter(filter) = optimized_plan {
            if let LogicalPlan::Join(join) = filter.input.as_ref() {
                assert!(join.dynamic_pushdown_columns.is_some());
                assert_eq!(join.dynamic_pushdown_columns.as_ref().unwrap().len(), 1);
                assert_eq!(
                    join.dynamic_pushdown_columns.as_ref().unwrap()[0]
                        .column
                        .name(),
                    "a"
                );
            } else {
                panic!("Expected Join operation under Filter");
            }
        } else {
            panic!("Expected Filter operation");
        }
        Ok(())
    }
}
