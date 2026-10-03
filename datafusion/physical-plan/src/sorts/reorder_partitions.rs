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

//! Defines the reorder-partitions plan.

use std::sync::Arc;

use crate::{
    ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    StatisticsArgs,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, Statistics, internal_err};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Pass-through operator that reorders partitions based on a provided permutation.
#[derive(Debug)]
pub struct ReorderPartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    /// For each output partition, the corresponding input partition
    permutation: Vec<usize>,
    properties: Arc<PlanProperties>,
}

impl ReorderPartitionsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, permutation: Vec<usize>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            permutation,
            properties,
        }
    }

    fn map_partition(&self, partition: usize) -> Result<usize> {
        self.permutation.get(partition).copied().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "ReorderPartitionsExec invalid partition {partition} for permutation of length {}",
                self.permutation.len()
            )
        })
    }
}

impl DisplayAs for ReorderPartitionsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ReorderPartitionsExec: order={:?}", self.permutation)
            }
            DisplayFormatType::TreeRender => writeln!(f, "ReorderPartitionsExec"),
        }
    }
}

impl ExecutionPlan for ReorderPartitionsExec {
    fn name(&self) -> &'static str {
        "ReorderPartitionsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "ReorderPartitionsExec expected 1 child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(
            Arc::<dyn ExecutionPlan>::clone(&children[0]),
            self.permutation.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(self.map_partition(partition)?, context)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        if input_stats.is_empty() {
            return internal_err!(
                "Could not get required input stats for ReorderPartitionsExec"
            );
        }
        Ok(Arc::clone(&input_stats[0]))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        match partition {
            None => vec![ChildStats::At(None)],
            Some(partition) => match self.map_partition(partition) {
                Ok(input_partition) => vec![ChildStats::At(Some(input_partition))],
                Err(_) => vec![],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::collect;
    use crate::statistics::StatisticsContext;
    use crate::test::TestMemoryExec;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::assert_batches_eq;
    use datafusion_common::stats::Precision;

    /// Returns an input plan with `num_partitions` partitions, where
    /// partition `i` contains a single batch with the value `i`
    fn partitioned_input(num_partitions: usize) -> Arc<dyn ExecutionPlan> {
        let partitions: Vec<Vec<RecordBatch>> = (0..num_partitions)
            .map(|i| {
                let arr: ArrayRef = Arc::new(Int32Array::from(vec![i as i32]));
                vec![RecordBatch::try_from_iter(vec![("i", arr)]).unwrap()]
            })
            .collect();
        let schema = partitions[0][0].schema();
        TestMemoryExec::try_new_exec(&partitions, schema, None).unwrap()
    }

    #[tokio::test]
    async fn test_reorders_partitions() {
        let task_ctx = Arc::new(TaskContext::default());
        let exec = ReorderPartitionsExec::new(partitioned_input(3), vec![2, 0, 1]);

        for (output_partition, input_partition) in [(0, 2), (1, 0), (2, 1)] {
            let stream = exec
                .execute(output_partition, Arc::clone(&task_ctx))
                .unwrap();
            let batches = collect(stream).await.unwrap();
            let expected_row = format!("| {input_partition} |");
            assert_batches_eq!(
                ["+---+", "| i |", "+---+", &expected_row, "+---+"],
                &batches
            );
        }
    }

    #[tokio::test]
    async fn test_execute_invalid_partition() {
        let task_ctx = Arc::new(TaskContext::default());
        let exec = ReorderPartitionsExec::new(partitioned_input(3), vec![2, 0, 1]);

        let err = match exec.execute(3, task_ctx) {
            Ok(_) => panic!("Expected an Err result"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("invalid partition 3 for permutation of length 3"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_child_stats_requests_map_partitions() {
        let exec = ReorderPartitionsExec::new(partitioned_input(3), vec![2, 0, 1]);

        // Overall stats come from overall stats of the input
        assert_eq!(exec.child_stats_requests(None), vec![ChildStats::At(None)]);

        // Partition-specific stats require stats from remapped partition index
        for (output_partition, input_partition) in [(0, 2), (1, 0), (2, 1)] {
            assert_eq!(
                exec.child_stats_requests(Some(output_partition)),
                vec![ChildStats::At(Some(input_partition))]
            );
        }

        // An out-of-range partition shouldn't panic
        assert_eq!(exec.child_stats_requests(Some(3)), vec![]);
    }

    #[test]
    fn test_statistics_passed_through() {
        // Input where partition i contains i + 1 rows, so partition statistics can be
        // matched to their index.
        let partitions: Vec<Vec<RecordBatch>> = (0..3)
            .map(|i| {
                let arr: ArrayRef = Arc::new(Int32Array::from(vec![i as i32; i + 1]));
                vec![RecordBatch::try_from_iter(vec![("i", arr)]).unwrap()]
            })
            .collect();
        let schema = partitions[0][0].schema();
        let input = TestMemoryExec::try_new_exec(&partitions, schema, None).unwrap();
        let exec = ReorderPartitionsExec::new(input, vec![2, 0, 1]);

        let ctx = StatisticsContext::new();

        // Overall stats come from overall stats of the input
        let stats = ctx.compute(&exec, &StatisticsArgs::new()).unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(6));

        // Partition-specific stats map to stats from the remapped partition index
        for (output_partition, input_partition) in [(0, 2), (1, 0), (2, 1)] {
            let stats = ctx
                .compute(
                    &exec,
                    &StatisticsArgs::new().with_partition(Some(output_partition)),
                )
                .unwrap();
            assert_eq!(stats.num_rows, Precision::Exact(input_partition + 1));
        }
    }
}
