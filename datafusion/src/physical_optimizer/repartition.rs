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
use crate::physical_plan::{
    empty::EmptyExec, repartition::RepartitionExec, ExecutionPlan,
};
use crate::physical_plan::{Distribution, Partitioning::*};
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
    requires_single_partition: bool,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Recurse into children bottom-up (added nodes should be as deep as possible)

    let new_plan = if plan.children().is_empty() {
        // leaf node - don't replace children
        plan.clone()
    } else {
        let children = plan
            .children()
            .iter()
            .map(|child| {
                optimize_partitions(
                    target_partitions,
                    matches!(
                        plan.required_child_distribution(),
                        Distribution::SinglePartition
                    ),
                    child.clone(),
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

    if perform_repartition && !requires_single_partition && !is_empty_exec {
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
            optimize_partitions(config.target_partitions, true, plan)
        }
    }

    fn name(&self) -> &str {
        "repartition"
    }
}
#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;

    use super::*;
    use crate::datasource::PartitionedFile;
    use crate::physical_plan::file_format::{ParquetExec, PhysicalPlanConfig};
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::Statistics;
    use crate::test::object_store::TestObjectStore;

    #[test]
    fn added_repartition_to_single_partition() -> Result<()> {
        let file_schema = Arc::new(Schema::empty());
        let parquet_project = ProjectionExec::try_new(
            vec![],
            Arc::new(ParquetExec::new(
                PhysicalPlanConfig {
                    object_store: TestObjectStore::new_arc(&[("x", 100)]),
                    file_schema,
                    file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                    statistics: Statistics::default(),
                    projection: None,
                    batch_size: 2048,
                    limit: None,
                    table_partition_cols: vec![],
                },
                None,
            )),
        )?;

        let optimizer = Repartition {};

        let optimized = optimizer.optimize(
            Arc::new(parquet_project),
            &ExecutionConfig::new().with_target_partitions(10),
        )?;

        assert_eq!(
            optimized.children()[0]
                .output_partitioning()
                .partition_count(),
            10
        );

        Ok(())
    }

    #[test]
    fn repartition_deepest_node() -> Result<()> {
        let file_schema = Arc::new(Schema::empty());
        let parquet_project = ProjectionExec::try_new(
            vec![],
            Arc::new(ProjectionExec::try_new(
                vec![],
                Arc::new(ParquetExec::new(
                    PhysicalPlanConfig {
                        object_store: TestObjectStore::new_arc(&[("x", 100)]),
                        file_schema,
                        file_groups: vec![vec![PartitionedFile::new(
                            "x".to_string(),
                            100,
                        )]],
                        statistics: Statistics::default(),
                        projection: None,
                        batch_size: 2048,
                        limit: None,
                        table_partition_cols: vec![],
                    },
                    None,
                )),
            )?),
        )?;

        let optimizer = Repartition {};

        let optimized = optimizer.optimize(
            Arc::new(parquet_project),
            &ExecutionConfig::new().with_target_partitions(10),
        )?;

        // RepartitionExec is added to deepest node
        assert!(optimized.children()[0]
            .as_any()
            .downcast_ref::<RepartitionExec>()
            .is_none());
        assert!(optimized.children()[0].children()[0]
            .as_any()
            .downcast_ref::<RepartitionExec>()
            .is_some());

        Ok(())
    }
}
