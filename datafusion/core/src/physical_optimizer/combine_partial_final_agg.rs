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

//! CombinePartialFinalAggregate optimizer rule checks the adjacent Partial and Final AggregateExecs
//! and try to combine them if necessary
use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
use crate::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};

/// CombinePartialFinalAggregate optimizer rule combines the adjacent Partial and Final AggregateExecs
/// into a Single AggregateExec if their grouping exprs and aggregate exprs equal.
///
/// This rule should be applied after the EnforceDistribution and EnforceSorting rules
///
#[derive(Default)]
pub struct CombinePartialFinalAggregate {}

impl CombinePartialFinalAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for CombinePartialFinalAggregate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let transformed = plan.as_any().downcast_ref::<AggregateExec>().and_then(
                |AggregateExec {
                     mode: final_mode,
                     input: final_input,
                     group_by: final_group_by,
                     aggr_expr: final_aggr_expr,
                     ..
                 }| {
                    if matches!(
                        final_mode,
                        AggregateMode::Final | AggregateMode::FinalPartitioned
                    ) {
                        final_input
                            .as_any()
                            .downcast_ref::<AggregateExec>()
                            .and_then(
                                |AggregateExec {
                                     mode: input_mode,
                                     input: partial_input,
                                     group_by: input_group_by,
                                     aggr_expr: input_aggr_expr,
                                     input_schema,
                                     ..
                                 }| {
                                    if matches!(input_mode, AggregateMode::Partial)
                                        && final_group_by.eq(input_group_by)
                                        && final_aggr_expr.len() == input_aggr_expr.len()
                                        && final_aggr_expr
                                            .iter()
                                            .zip(input_aggr_expr.iter())
                                            .all(|(final_expr, partial_expr)| {
                                                final_expr.eq(partial_expr)
                                            })
                                    {
                                        AggregateExec::try_new(
                                            AggregateMode::Single,
                                            input_group_by.clone(),
                                            input_aggr_expr.to_vec(),
                                            partial_input.clone(),
                                            input_schema.clone(),
                                        )
                                        .ok()
                                        .map(Arc::new)
                                    } else {
                                        None
                                    }
                                },
                            )
                    } else {
                        None
                    }
                },
            );

            Ok(if let Some(transformed) = transformed {
                Transformed::Yes(transformed)
            } else {
                Transformed::No(plan)
            })
        })
    }

    fn name(&self) -> &str {
        "CombinePartialFinalAggregate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_physical_expr::expressions::Count;
    use datafusion_physical_expr::AggregateExpr;

    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::expressions::lit;
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::{displayable, Partitioning, Statistics};

    /// Runs the CombinePartialFinalAggregate optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            // run optimizer
            let optimizer = CombinePartialFinalAggregate {};
            let config = ConfigOptions::new();
            let optimized = optimizer.optimize($PLAN, &config)?;
            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent().to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]))
    }

    fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    fn partial_aggregate_exec(
        input: Arc<dyn ExecutionPlan>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::default(),
                aggr_expr,
                input,
                schema,
            )
            .unwrap(),
        )
    }

    fn final_aggregate_exec(
        input: Arc<dyn ExecutionPlan>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::default(),
                aggr_expr,
                input,
                schema,
            )
            .unwrap(),
        )
    }

    fn repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10)).unwrap(),
        )
    }

    #[test]
    fn aggregations_not_combined() -> Result<()> {
        let schema = schema();

        let aggr_expr = vec![Arc::new(Count::new(
            lit(1i8),
            "COUNT(1)".to_string(),
            DataType::Int64,
        )) as _];
        let plan = final_aggregate_exec(
            repartition_exec(partial_aggregate_exec(
                parquet_exec(&schema),
                aggr_expr.clone(),
            )),
            aggr_expr,
        );
        // should not combine the Partial/Final AggregateExecs
        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[COUNT(1)]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "AggregateExec: mode=Partial, gby=[], aggr=[COUNT(1)]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[a, b]",
        ];
        assert_optimized!(expected, plan);

        let aggr_expr1 = vec![Arc::new(Count::new(
            lit(1i8),
            "COUNT(1)".to_string(),
            DataType::Int64,
        )) as _];
        let aggr_expr2 = vec![Arc::new(Count::new(
            lit(1i8),
            "COUNT(2)".to_string(),
            DataType::Int64,
        )) as _];

        let plan = final_aggregate_exec(
            partial_aggregate_exec(parquet_exec(&schema), aggr_expr1),
            aggr_expr2,
        );
        // should not combine the Partial/Final AggregateExecs
        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[COUNT(2)]",
            "AggregateExec: mode=Partial, gby=[], aggr=[COUNT(1)]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[a, b]",
        ];

        assert_optimized!(expected, plan);

        Ok(())
    }

    #[test]
    fn aggregations_combined() -> Result<()> {
        let schema = schema();
        let aggr_expr = vec![Arc::new(Count::new(
            lit(1i8),
            "COUNT(1)".to_string(),
            DataType::Int64,
        )) as _];

        let plan = final_aggregate_exec(
            partial_aggregate_exec(parquet_exec(&schema), aggr_expr.clone()),
            aggr_expr,
        );
        // should combine the Partial/Final AggregateExecs to tne Single AggregateExec
        let expected = &[
            "AggregateExec: mode=Single, gby=[], aggr=[COUNT(1)]",
            "ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[a, b]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }
}
