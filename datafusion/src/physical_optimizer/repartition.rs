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

//! Repartition optimizer that introduces repartition nodes to increase the level of parallism available
use std::sync::Arc;

use super::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::Partitioning::*;
use crate::physical_plan::{
    empty::EmptyExec, repartition::RepartitionExec, ExecutionPlan,
};
use crate::{error::Result, execution::context::ExecutionConfig};

/// Optimizer that introduces repartition to introduce more parallelism in the plan
#[derive(Default)]
pub struct Repartition {}

impl Repartition {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn optimize_partitions(
    target_partitions: usize,
    plan: Arc<dyn ExecutionPlan>,
    should_repartition: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Recurse into children bottom-up (added nodes should be as deep as possible)

    let new_plan = if plan.children().is_empty() {
        // leaf node - don't replace children
        plan.clone()
    } else {
        let should_repartition_children = plan.should_repartition_children();
        let children = plan
            .children()
            .iter()
            .map(|child| {
                optimize_partitions(
                    target_partitions,
                    child.clone(),
                    should_repartition_children,
                )
            })
            .collect::<Result<_>>()?;
        plan.with_new_children(children)?
    };

    let perform_repartition = match new_plan.output_partitioning() {
        // Apply when underlying node has less than `self.target_partitions` amount of concurrency
        RoundRobinBatch(x) => x < target_partitions,
        UnknownPartitioning(x) => x < target_partitions,
        // we don't want to introduce partitioning after hash partitioning
        // as the plan will likely depend on this
        Hash(_, _) => false,
    };

    // TODO: EmptyExec causes failures with RepartitionExec
    // But also not very useful to inlude
    let is_empty_exec = plan.as_any().downcast_ref::<EmptyExec>().is_some();

    if perform_repartition && should_repartition && !is_empty_exec {
        Ok(Arc::new(RepartitionExec::try_new(
            new_plan,
            RoundRobinBatch(target_partitions),
        )?))
    } else {
        Ok(new_plan)
    }
}

impl PhysicalOptimizerRule for Repartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ExecutionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Don't run optimizer if target_partitions == 1
        if config.target_partitions == 1 {
            Ok(plan)
        } else {
            optimize_partitions(config.target_partitions, plan, false)
        }
    }

    fn name(&self) -> &str {
        "repartition"
    }
}
#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::datasource::PartitionedFile;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
    use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use crate::physical_plan::union::UnionExec;
    use crate::physical_plan::{displayable, Statistics};
    use crate::test::object_store::TestObjectStore;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c1", DataType::Boolean, true)]))
    }

    fn parquet_exec() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store: TestObjectStore::new_arc(&[("x", 100)]),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            None,
        ))
    }

    fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(col("c1", &schema()).unwrap(), input).unwrap())
    }

    fn hash_aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        Arc::new(
            HashAggregateExec::try_new(
                AggregateMode::Final,
                vec![],
                vec![],
                Arc::new(
                    HashAggregateExec::try_new(
                        AggregateMode::Partial,
                        vec![],
                        vec![],
                        input,
                        schema.clone(),
                    )
                    .unwrap(),
                ),
                schema,
            )
            .unwrap(),
        )
    }

    fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            100,
        ))
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    #[test]
    fn added_repartition_to_single_partition() -> Result<()> {
        let optimizer = Repartition {};

        let optimized = optimizer.optimize(
            hash_aggregate(parquet_exec()),
            &ExecutionConfig::new().with_target_partitions(10),
        )?;

        let plan = displayable(optimized.as_ref()).indent().to_string();

        let expected = &[
            "HashAggregateExec: mode=Final, gby=[], aggr=[]",
            "HashAggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "ParquetExec: limit=None, partitions=[x]",
        ];

        assert_eq!(&trim_plan_display(&plan), &expected);
        Ok(())
    }

    #[test]
    fn repartition_deepest_node() -> Result<()> {
        let optimizer = Repartition {};

        let optimized = optimizer.optimize(
            hash_aggregate(filter_exec(parquet_exec())),
            &ExecutionConfig::new().with_target_partitions(10),
        )?;

        let plan = displayable(optimized.as_ref()).indent().to_string();

        let expected = &[
            "HashAggregateExec: mode=Final, gby=[], aggr=[]",
            "HashAggregateExec: mode=Partial, gby=[], aggr=[]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "ParquetExec: limit=None, partitions=[x]",
        ];

        assert_eq!(&trim_plan_display(&plan), &expected);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit() -> Result<()> {
        let optimizer = Repartition {};

        let optimized = optimizer.optimize(
            hash_aggregate(limit_exec(filter_exec(limit_exec(parquet_exec())))),
            &ExecutionConfig::new().with_target_partitions(10),
        )?;

        let plan = displayable(optimized.as_ref()).indent().to_string();

        let expected = &[
            "HashAggregateExec: mode=Final, gby=[], aggr=[]",
            "HashAggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "GlobalLimitExec: limit=100",
            "LocalLimitExec: limit=100",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "GlobalLimitExec: limit=100",
            "LocalLimitExec: limit=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: limit=None, partitions=[x]",
        ];

        assert_eq!(&trim_plan_display(&plan), &expected);
        Ok(())
    }

    #[test]
    fn repartition_ignores_union() -> Result<()> {
        let optimizer = Repartition {};

        let optimized = optimizer.optimize(
            Arc::new(UnionExec::new(vec![parquet_exec(); 5])),
            &ExecutionConfig::new().with_target_partitions(5),
        )?;

        let plan = displayable(optimized.as_ref()).indent().to_string();

        let expected = &[
            "UnionExec",
            // Expect no repartition of ParquetExec
            "ParquetExec: limit=None, partitions=[x]",
            "ParquetExec: limit=None, partitions=[x]",
            "ParquetExec: limit=None, partitions=[x]",
            "ParquetExec: limit=None, partitions=[x]",
            "ParquetExec: limit=None, partitions=[x]",
        ];

        assert_eq!(&trim_plan_display(&plan), &expected);
        Ok(())
    }
}
