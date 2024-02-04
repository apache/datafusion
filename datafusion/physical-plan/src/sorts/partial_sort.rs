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

//! Partial Sort that deals with an arbitrary size of input.
//! It will do in-memory sorting

use arrow::compute;
use arrow::compute::{lexsort_to_indices, take};
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::{ready, Stream, StreamExt};
use log::trace;

use datafusion_common::utils::evaluate_partition_ranges;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, TaskContext};
use datafusion_physical_expr::EquivalenceProperties;

use crate::expressions::PhysicalSortExpr;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};

/// Partial Sort execution plan.
#[derive(Debug, Clone)]
pub struct PartialSortExec {
    /// Input schema
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    common_prefix_length: usize,
    /// Containing all metrics set created during sort
    metrics_set: ExecutionPlanMetricsSet,
    /// Preserve partitions of input plan. If false, the input partitions
    /// will be sorted and merged into a single output partition.
    preserve_partitioning: bool,
    /// Fetch highest/lowest n results
    fetch: Option<usize>,
}

impl PartialSortExec {
    /// Create a new partial sort execution plan that produces partitions leveraging already sorted chunks
    pub fn new(expr: Vec<PhysicalSortExpr>, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            expr,
            common_prefix_length: 0,
            metrics_set: ExecutionPlanMetricsSet::new(),
            preserve_partitioning: false,
            fetch: None,
        }
    }

    pub fn with_common_prefix_length(mut self, common_prefix_length: usize) -> Self {
        self.common_prefix_length = common_prefix_length;
        self
    }

    /// Whether this `PartialSortExec` preserves partitioning of the children
    pub fn preserve_partitioning(&self) -> bool {
        self.preserve_partitioning
    }

    /// Specify the partitioning behavior of this sort exec
    ///
    /// If `preserve_partitioning` is true, sorts each partition
    /// individually, producing one sorted stream for each input partition.
    ///
    /// If `preserve_partitioning` is false, sorts and merges all
    /// input partitions producing a single, sorted partition.
    pub fn with_preserve_partitioning(mut self, preserve_partitioning: bool) -> Self {
        self.preserve_partitioning = preserve_partitioning;
        self
    }

    /// Modify how many rows to include in the result
    ///
    /// If None, then all rows will be returned, in sorted order.
    /// If Some, then only the top `fetch` rows will be returned.
    /// This can reduce the memory pressure required by the sort
    /// operation since rows that are not going to be included
    /// can be dropped.
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &[PhysicalSortExpr] {
        &self.expr
    }

    /// If `Some(fetch)`, limits output to only the first "fetch" items
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl DisplayAs for PartialSortExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let expr = PhysicalSortExpr::format_list(&self.expr);
                let common_prefix_length = self.common_prefix_length;
                match self.fetch {
                    Some(fetch) => {
                        write!(f, "PartialSortExec: TopK(fetch={fetch}), expr=[{expr}], common_prefix_length=[{common_prefix_length}]", )
                    }
                    None => write!(f, "PartialSortExec: expr=[{expr}], common_prefix_length=[{common_prefix_length}]"),
                }
            }
        }
    }
}

impl ExecutionPlan for PartialSortExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        if self.preserve_partitioning {
            self.input.output_partitioning()
        } else {
            Partitioning::UnknownPartitioning(1)
        }
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.expr)
    }

    // TODO: check usages
    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.preserve_partitioning {
            vec![Distribution::UnspecifiedDistribution]
        } else {
            // global sort
            // TODO support RangePartition and OrderedDistribution
            vec![Distribution::SinglePartition]
        }
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        // Reset the ordering equivalence class with the new ordering:
        self.input
            .equivalence_properties()
            .with_reorder(self.expr.to_vec())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_sort = PartialSortExec::new(self.expr.clone(), children[0].clone())
            .with_fetch(self.fetch)
            .with_preserve_partitioning(self.preserve_partitioning);

        Ok(Arc::new(new_sort))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start PartialSortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        let input = self.input.execute(partition, context.clone())?;

        trace!(
            "End PartialSortExec's input.execute for partition: {}",
            partition
        );

        // Make sure common prefix length is larger than 0
        // Otherwise, we should use SortExec.
        assert!(self.common_prefix_length > 0);

        Ok(Box::pin(PartialSortStream {
            input,
            expr: self.expr.clone(),
            schema: self.schema(),
            common_prefix_length: self.common_prefix_length,
            input_batch: RecordBatch::new_empty(self.schema().clone()),
            fetch: self.fetch,
            is_closed: false,
            baseline_metrics: BaselineMetrics::new(&self.metrics_set, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

// todo: documentation
struct PartialSortStream {
    /// The input plan
    input: SendableRecordBatchStream,
    expr: Vec<PhysicalSortExpr>,
    /// The input schema
    schema: SchemaRef,
    common_prefix_length: usize,
    input_batch: RecordBatch,
    /// Fetch top N results
    fetch: Option<usize>,
    /// Whether the stream has finished returning all of its data or not
    is_closed: bool,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
}

impl Stream for PartialSortStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // we can't predict the size of incoming batches so re-use the size hint from the input
        self.input.size_hint()
    }
}

impl RecordBatchStream for PartialSortStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl PartialSortStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.is_closed && self.input_batch.num_rows() == 0 {
            return Poll::Ready(None);
        }
        let result = match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                self.input_batch = compute::concat_batches(
                    &self.schema.clone(),
                    &[self.input_batch.clone(), batch],
                )?;
                let slice_point =
                    self.get_slice_point(self.common_prefix_length, &self.input_batch)?;
                if slice_point == self.input_batch.num_rows() {
                    Ok(RecordBatch::new_empty(self.schema.clone()))
                } else {
                    self.emit(slice_point)
                }
            }
            Some(Err(e)) => Err(e),
            None => {
                self.is_closed = true;
                // once there is no more input sort the rest of the inserted batches in input_batch
                self.emit(self.input_batch.num_rows())
            }
        };

        Poll::Ready(Some(result))
    }

    fn emit(self: &mut Pin<&mut Self>, slice_point: usize) -> Result<RecordBatch> {
        let original_num_rows = self.input_batch.num_rows();
        let sliced = self.input_batch.slice(0, slice_point);
        self.input_batch = self
            .input_batch
            .slice(slice_point, self.input_batch.num_rows() - slice_point);
        let sort_columns = self
            .expr
            .clone()
            .iter()
            .map(|expr| expr.clone().evaluate_to_sort_column(&sliced))
            .collect::<Result<Vec<_>>>()?;
        let sorted_indices = lexsort_to_indices(&sort_columns, None)?;
        let sorted_columns = sliced
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &sorted_indices, None))
            .collect::<Result<_, _>>()?;
        let result = RecordBatch::try_new(self.schema.clone(), sorted_columns)?;
        assert_eq!(
            original_num_rows,
            result.num_rows() + self.input_batch.num_rows()
        );
        self.stream_limit(result)
    }

    fn get_slice_point(
        &self,
        common_prefix_len: usize,
        batch: &RecordBatch,
    ) -> Result<usize> {
        let common_prefix_sort_keys = (0..common_prefix_len)
            .map(|idx| self.expr[idx].evaluate_to_sort_column(batch))
            .collect::<Result<Vec<_>>>()?;
        let partition_points =
            evaluate_partition_ranges(batch.num_rows(), &common_prefix_sort_keys)?;
        // If partition points are [0..100], [100..200], [200..300]
        // we should return 200, which is the safest and furthest partition boundary
        // Please note that we shouldn't return 300 (which is number of rows in the batch),
        // because this boundary may change with new data.
        let mut slice_point = partition_points[0].end;
        if partition_points.len() > 2 {
            slice_point = partition_points[partition_points.len() - 2].end;
        }
        Ok(slice_point)
    }

    fn stream_limit(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        if let Some(remaining_fetch) = self.fetch {
            if remaining_fetch <= batch.num_rows() {
                self.fetch = Some(0);
                self.is_closed = true;
                self.input_batch = RecordBatch::new_empty(self.schema.clone());
                return Ok(batch.slice(0, remaining_fetch));
            }
            self.fetch = Some(remaining_fetch - batch.num_rows());
        }
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::*;
    use datafusion_common::assert_batches_eq;

    use crate::expressions::col;
    use crate::memory::MemoryExec;
    use crate::sorts::sort::SortExec;
    use crate::test;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::{collect, displayable};

    use super::*;

    use futures::FutureExt;
    use itertools::Itertools;

    fn print_plan(plan: &Arc<dyn ExecutionPlan>) {
        let formatted = displayable(plan.as_ref()).indent(true).to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        println!("{:#?}", actual);
    }

    #[tokio::test]
    async fn test_partial_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source = test::build_table_scan_i32(
            ("a", &vec![0, 0, 0, 1, 1, 1]),
            ("b", &vec![1, 1, 2, 2, 3, 3]),
            ("c", &vec![1, 0, 5, 4, 3, 2]),
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let partial_sort_exec = Arc::new(
            PartialSortExec::new(
                vec![
                    PhysicalSortExpr {
                        expr: col("a", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("c", &schema)?,
                        options: option_asc,
                    },
                ],
                source.clone(),
            )
            .with_common_prefix_length(2),
        ) as Arc<dyn ExecutionPlan>;
        print_plan(&partial_sort_exec);

        let result = collect(partial_sort_exec, task_ctx.clone()).await?;

        let expected_after_sort = [
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 0 | 1 | 0 |",
            "| 0 | 1 | 1 |",
            "| 0 | 2 | 5 |",
            "| 1 | 2 | 4 |",
            "| 1 | 3 | 2 |",
            "| 1 | 3 | 3 |",
            "+---+---+---+",
        ];
        assert_eq!(2, result.len());
        assert_batches_eq!(expected_after_sort, &result);
        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_sort_with_fetch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source = test::build_table_scan_i32(
            ("a", &vec![0, 0, 1, 1, 1]),
            ("b", &vec![1, 2, 2, 3, 3]),
            ("c", &vec![0, 1, 2, 3, 4]),
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let partial_sort_exec = Arc::new(
            PartialSortExec::new(
                vec![
                    PhysicalSortExpr {
                        expr: col("a", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("c", &schema)?,
                        options: option_asc,
                    },
                ],
                source.clone(),
            )
            .with_common_prefix_length(2)
            .with_fetch(Some(4)),
        ) as Arc<dyn ExecutionPlan>;
        print_plan(&partial_sort_exec);

        let result = collect(partial_sort_exec, task_ctx.clone()).await?;

        let expected_after_sort = [
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 0 | 1 | 0 |",
            "| 0 | 2 | 1 |",
            "| 1 | 2 | 2 |",
            "| 1 | 3 | 3 |",
            "+---+---+---+",
        ];
        assert_eq!(2, result.len());
        assert_batches_eq!(expected_after_sort, &result);
        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_sort2() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source = test::build_table_scan_i32(
            ("a", &vec![0, 0, 0, 0, 1, 1, 1, 1]),
            ("b", &vec![1, 1, 3, 3, 4, 4, 2, 2]),
            ("c", &vec![0, 1, 2, 3, 4, 5, 6, 7]),
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let partial_sort_exec = Arc::new(
            PartialSortExec::new(
                vec![
                    PhysicalSortExpr {
                        expr: col("a", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("c", &schema)?,
                        options: option_asc,
                    },
                ],
                source,
            )
            .with_common_prefix_length(1),
        );

        let result = collect(partial_sort_exec, task_ctx.clone()).await?;
        assert_eq!(2, result.len());
        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );
        let expected = [
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 0 | 1 | 0 |",
            "| 0 | 1 | 1 |",
            "| 0 | 3 | 2 |",
            "| 0 | 3 | 3 |",
            "| 1 | 2 | 6 |",
            "| 1 | 2 | 7 |",
            "| 1 | 4 | 4 |",
            "| 1 | 4 | 5 |",
            "+---+---+---+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    fn prepare_partitioned_input() -> Arc<dyn ExecutionPlan> {
        let batch1 = test::build_table_i32(
            ("a", &vec![1; 100]),
            ("b", &(0..100).rev().collect()),
            ("c", &(0..100).rev().collect()),
        );
        let batch2 = test::build_table_i32(
            ("a", &[&vec![1; 25][..], &vec![2; 75][..]].concat()),
            ("b", &(100..200).rev().collect()),
            ("c", &(0..100).collect()),
        );
        let batch3 = test::build_table_i32(
            ("a", &[&vec![3; 50][..], &vec![4; 50][..]].concat()),
            ("b", &(150..250).rev().collect()),
            ("c", &(0..100).rev().collect()),
        );
        let batch4 = test::build_table_i32(
            ("a", &vec![4; 100]),
            ("b", &(50..150).rev().collect()),
            ("c", &(0..100).rev().collect()),
        );
        let schema = batch1.schema();
        Arc::new(
            MemoryExec::try_new(
                &[vec![batch1, batch2, batch3, batch4]],
                schema.clone(),
                None,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>
    }

    #[tokio::test]
    async fn test_partitioned_input_partial_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let mem_exec = prepare_partitioned_input();
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let schema = mem_exec.schema();
        let partial_sort_executor = PartialSortExec::new(
            vec![
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: option_desc,
                },
                PhysicalSortExpr {
                    expr: col("c", &schema)?,
                    options: option_asc,
                },
            ],
            mem_exec.clone(),
        )
        .with_common_prefix_length(1);
        let partial_sort_exec =
            Arc::new(partial_sort_executor.clone()) as Arc<dyn ExecutionPlan>;
        let sort_exec = Arc::new(SortExec::new(
            partial_sort_executor.expr,
            partial_sort_executor.input,
        )) as Arc<dyn ExecutionPlan>;
        let result = collect(partial_sort_exec, task_ctx.clone()).await?;
        assert_eq!(
            result.iter().map(|r| r.num_rows()).collect_vec(),
            [0, 125, 125, 0, 150]
        );

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );
        let partial_sort_result = compute::concat_batches(&schema, &result).unwrap();
        let sort_result = collect(sort_exec, task_ctx.clone()).await?;
        assert_eq!(sort_result[0], partial_sort_result);

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_input_partial_sort_with_fetch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let mem_exec = prepare_partitioned_input();
        let schema = mem_exec.schema();
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        for (fetch_size, expected_batch_num_rows) in [
            (Some(50), vec![0, 50]),
            (Some(120), vec![0, 120]),
            (Some(150), vec![0, 125, 25]),
            (Some(250), vec![0, 125, 125]),
        ] {
            let partial_sort_executor = PartialSortExec::new(
                vec![
                    PhysicalSortExpr {
                        expr: col("a", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema)?,
                        options: option_desc,
                    },
                    PhysicalSortExpr {
                        expr: col("c", &schema)?,
                        options: option_asc,
                    },
                ],
                mem_exec.clone(),
            )
            .with_fetch(fetch_size)
            .with_common_prefix_length(1);

            let partial_sort_exec =
                Arc::new(partial_sort_executor.clone()) as Arc<dyn ExecutionPlan>;
            let sort_exec = Arc::new(
                SortExec::new(partial_sort_executor.expr, partial_sort_executor.input)
                    .with_fetch(fetch_size),
            ) as Arc<dyn ExecutionPlan>;
            let result = collect(partial_sort_exec, task_ctx.clone()).await?;
            assert_eq!(
                result.iter().map(|r| r.num_rows()).collect_vec(),
                expected_batch_num_rows
            );

            assert_eq!(
                task_ctx.runtime_env().memory_pool.reserved(),
                0,
                "The sort should have returned all memory used back to the memory manager"
            );
            let partial_sort_result = compute::concat_batches(&schema, &result)?;
            let sort_result = collect(sort_exec, task_ctx.clone()).await?;
            assert_eq!(sort_result[0], partial_sort_result);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_metadata() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let field_metadata: HashMap<String, String> =
            vec![("foo".to_string(), "bar".to_string())]
                .into_iter()
                .collect();
        let schema_metadata: HashMap<String, String> =
            vec![("baz".to_string(), "barf".to_string())]
                .into_iter()
                .collect();

        let mut field = Field::new("field_name", DataType::UInt64, true);
        field.set_metadata(field_metadata.clone());
        let schema = Schema::new_with_metadata(vec![field], schema_metadata.clone());
        let schema = Arc::new(schema);

        let data: ArrayRef =
            Arc::new(vec![1, 1, 2].into_iter().map(Some).collect::<UInt64Array>());

        let batch = RecordBatch::try_new(schema.clone(), vec![data])?;
        let input = Arc::new(MemoryExec::try_new(&[vec![batch]], schema.clone(), None)?);

        let partial_sort_exec = Arc::new(
            PartialSortExec::new(
                vec![PhysicalSortExpr {
                    expr: col("field_name", &schema)?,
                    options: SortOptions::default(),
                }],
                input,
            )
            .with_common_prefix_length(1),
        );

        let result: Vec<RecordBatch> = collect(partial_sort_exec, task_ctx).await?;
        let expected_batch = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    vec![1, 1].into_iter().map(Some).collect::<UInt64Array>(),
                )],
            )?,
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    vec![2].into_iter().map(Some).collect::<UInt64Array>(),
                )],
            )?,
        ];

        // Data is correct
        assert_eq!(&expected_batch, &result);

        // explicitly ensure the metadata is present
        assert_eq!(result[0].schema().fields()[0].metadata(), &field_metadata);
        assert_eq!(result[0].schema().metadata(), &schema_metadata);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Float64, true),
        ]));
        let option_asc = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(2.0_f32),
                    Some(2.0_f32),
                    Some(3.0_f32),
                    Some(3.0_f32),
                    Some(3.0_f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(20.0_f64),
                    Some(20.0_f64),
                    Some(40.0_f64),
                    Some(40.0_f64),
                    Some(f64::NAN),
                    None,
                    None,
                    Some(f64::NAN),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0_f64),
                    Some(20.0_f64),
                    Some(10.0_f64),
                    Some(100.0_f64),
                    Some(f64::NAN),
                    Some(100.0_f64),
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )?;

        let partial_sort_exec = Arc::new(
            PartialSortExec::new(
                vec![
                    PhysicalSortExpr {
                        expr: col("a", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("c", &schema)?,
                        options: option_desc,
                    },
                ],
                Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?),
            )
            .with_common_prefix_length(2),
        );

        let expected = [
            "+-----+------+-------+",
            "| a   | b    | c     |",
            "+-----+------+-------+",
            "| 1.0 | 20.0 | 20.0  |",
            "| 1.0 | 20.0 | 10.0  |",
            "| 1.0 | 40.0 | 10.0  |",
            "| 2.0 | 40.0 | 100.0 |",
            "| 2.0 | NaN  | NaN   |",
            "| 3.0 |      |       |",
            "| 3.0 |      | 100.0 |",
            "| 3.0 | NaN  | NaN   |",
            "+-----+------+-------+",
        ];

        assert_eq!(
            DataType::Float32,
            *partial_sort_exec.schema().field(0).data_type()
        );
        assert_eq!(
            DataType::Float64,
            *partial_sort_exec.schema().field(1).data_type()
        );
        assert_eq!(
            DataType::Float64,
            *partial_sort_exec.schema().field(2).data_type()
        );

        let result: Vec<RecordBatch> =
            collect(partial_sort_exec.clone(), task_ctx).await?;
        assert_batches_eq!(expected, &result);
        assert_eq!(result.len(), 2);
        let metrics = partial_sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 8);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());
        assert_eq!(DataType::Float64, *columns[2].data_type());

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let sort_exec = Arc::new(
            PartialSortExec::new(
                vec![PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: SortOptions::default(),
                }],
                blocking_exec,
            )
            .with_common_prefix_length(1),
        );

        let fut = collect(sort_exec, task_ctx.clone());
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }
}
