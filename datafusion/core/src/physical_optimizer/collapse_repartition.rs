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

//! Collapse repartition optimizer that eliminates adjacent repartition nodes
use std::sync::Arc;

use super::optimizer::PhysicalOptimizerRule;
use super::utils::optimize_children;
use crate::physical_plan::Partitioning;
use crate::physical_plan::{repartition::RepartitionExec, ExecutionPlan};
use crate::{error::Result, execution::context::SessionConfig};

/// Optimizer that eliminates adjacent repartitions, preferring to keep the top repartition,
/// to avoid unnecessary repartition operations
#[derive(Default)]
pub struct CollapseRepartition {}

impl CollapseRepartition {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Compare top and bottom, if both are `RepartitionExec` and fulfill any of the following conditions:
///
/// 1. If both have `RoundRobinBatch` partitioning, then keep the larger count
///
/// 2. Else pick the top one
///
/// Returns which `RepartitionExec` to keep, else return None if top or bottom is not a `RepartitionExec`
///
fn repartitions_compatible(
    top: &Arc<dyn ExecutionPlan>,
    bottom: &Arc<dyn ExecutionPlan>,
) -> Option<(bool, bool)> {
    match top
        .as_any()
        .downcast_ref::<RepartitionExec>()
        .zip(bottom.as_any().downcast_ref::<RepartitionExec>())
        .map(|(l, r)| (l.partitioning(), r.partitioning()))
    {
        Some((
            Partitioning::RoundRobinBatch(left_partition_count),
            Partitioning::RoundRobinBatch(right_partition_count),
        )) => {
            if left_partition_count >= right_partition_count {
                Some((true, false))
            } else {
                Some((false, true))
            }
        }
        Some(_) => Some((true, false)),
        _ => None,
    }
}

impl PhysicalOptimizerRule for CollapseRepartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(child) = plan.children().first() {
            // Have a child to compare against
            match repartitions_compatible(&plan, child) {
                Some((true, _)) => {
                    // Keep parent repartition, moving children of child to parent
                    self.optimize(plan.with_new_children(child.children())?, _config)
                }
                Some((_, true)) => {
                    // Keep child repartition
                    self.optimize(child.clone(), _config)
                }
                _ => {
                    // Can't eliminate either, recurse into child
                    optimize_children(self, plan, _config)
                }
            }
        } else {
            // Leaf node, terminate recursion
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "collapse_repartition"
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::PhysicalExpr;

    use super::*;
    use crate::config::ConfigOptions;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::{displayable, Partitioning, Statistics};

    fn parquet_exec() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: Arc::new(Schema::new(vec![Field::new(
                    "c1",
                    DataType::Boolean,
                    true,
                )])),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                config_options: ConfigOptions::new().into_shareable(),
            },
            None,
            None,
        ))
    }

    fn round_robin_repartition(
        input: Arc<dyn ExecutionPlan>,
        num: usize,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(num)).unwrap(),
        )
    }

    fn hash_repartition(
        input: Arc<dyn ExecutionPlan>,
        cols: Vec<Arc<dyn PhysicalExpr>>,
        num: usize,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(cols, num)).unwrap())
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            // run optimizer
            let optimizer = CollapseRepartition {};
            let optimized = optimizer.optimize($PLAN, &SessionConfig::new())?;

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

    #[test]
    fn collapse_adjacent_repartitions_into_single() -> Result<()> {
        let plan = hash_repartition(
            hash_repartition(
                round_robin_repartition(
                    round_robin_repartition(
                        round_robin_repartition(parquet_exec(), 10),
                        30,
                    ),
                    20,
                ),
                vec![Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 1))],
                10,
            ),
            vec![Arc::new(Column::new("a", 0))],
            20,
        );

        let expected = [
            "RepartitionExec: partitioning=Hash([Column { name: \"a\", index: 0 }], 20)",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }
}
