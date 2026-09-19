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

//! Merge that deals with an arbitrary size of streaming inputs.
//! This is an order-preserving merge.

use crate::metrics::BaselineMetrics;
use crate::sorts::multi_level_merge::MultiLevelMergeBuilder;
use crate::sorts::{
    merge::SortPreservingMergeStream,
    stream::{FieldCursorStream, RowCursorStream},
};
use crate::{EmptyRecordBatchStream, SendableRecordBatchStream, SpillManager};
use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::human_readable_size;
use datafusion_common::{Result, assert_or_internal_err, internal_err};
use datafusion_execution::SpillFile;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryPool, MemoryReservation, MergeMemoryPool, UnboundedMemoryPool,
};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use std::sync::Arc;

/// Allowance for simultaneously retained merge inputs and materialized output.
/// This preserves the source/output portion of the caller's existing heuristic
/// merge estimate, rather than enforcing a hard bound on all live allocations.
#[derive(Debug)]
pub(super) struct MergeBatchMemoryBudget {
    pub memory_limit: usize,
    pub input_batch_sizes: Vec<usize>,
}

macro_rules! primitive_merge_helper {
    ($t:ty, $($v:ident),+) => {
        merge_helper!(PrimitiveArray<$t>, $($v),+)
    };
}

macro_rules! merge_helper {
    ($t:ty, $sort:ident, $streams:ident, $schema:ident, $tracking_metrics:ident, $batch_size:ident, $fetch:ident, $reservation:ident, $enable_round_robin_tie_breaker:ident, $batch_memory_budget:ident) => {{
        let streams =
            FieldCursorStream::<$t>::new($sort, $streams, $reservation.new_empty());
        return Ok(SortPreservingMergeStream::new(
            Box::new(streams),
            $schema,
            $tracking_metrics,
            $batch_size,
            $fetch,
            $reservation,
            $enable_round_robin_tie_breaker,
        )
        .with_batch_memory_budget($batch_memory_budget)
        .into_stream());
    }};
}

pub struct SortedSpillFile {
    pub file: Arc<dyn SpillFile>,

    /// how much memory the largest memory batch is taking
    pub max_record_batch_memory: usize,
}

impl std::fmt::Debug for SortedSpillFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.file.path() {
            Some(path) => write!(
                f,
                "SortedSpillFile({:?}) takes {}",
                path,
                human_readable_size(self.max_record_batch_memory)
            ),
            None => write!(
                f,
                "SortedSpillFile(<custom_backend>) takes {}",
                human_readable_size(self.max_record_batch_memory)
            ),
        }
    }
}

#[derive(Default)]
pub struct StreamingMergeBuilder<'a> {
    streams: Vec<SendableRecordBatchStream>,
    sorted_spill_files: Vec<SortedSpillFile>,
    spill_manager: Option<SpillManager>,
    schema: Option<SchemaRef>,
    expressions: Option<&'a LexOrdering>,
    metrics: Option<BaselineMetrics>,
    batch_size: Option<usize>,
    fetch: Option<usize>,
    reservation: Option<MemoryReservation>,
    merge_pool: Option<Arc<MergeMemoryPool>>,
    /// Leave memory for the aggregate consuming the merged spill rows.
    reserve_replay_headroom: bool,
    batch_memory_budget: Option<MergeBatchMemoryBudget>,
    enable_round_robin_tie_breaker: bool,
}

impl<'a> StreamingMergeBuilder<'a> {
    pub fn new() -> Self {
        Self {
            enable_round_robin_tie_breaker: true,
            ..Default::default()
        }
    }

    pub fn with_streams(mut self, streams: Vec<SendableRecordBatchStream>) -> Self {
        self.streams = streams;
        self
    }

    pub fn with_sorted_spill_files(
        mut self,
        sorted_spill_files: Vec<SortedSpillFile>,
    ) -> Self {
        self.sorted_spill_files = sorted_spill_files;
        self
    }

    pub fn with_spill_manager(mut self, spill_manager: SpillManager) -> Self {
        self.spill_manager = Some(spill_manager);
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_expressions(mut self, expressions: &'a LexOrdering) -> Self {
        self.expressions = Some(expressions);
        self
    }

    pub fn with_metrics(mut self, metrics: BaselineMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    pub fn with_reservation(mut self, reservation: MemoryReservation) -> Self {
        self.reservation = Some(reservation);
        self
    }

    /// Keep spill workspace until the final merge pass selects its buffer budget.
    pub(super) fn with_merge_pool(mut self, pool: Arc<MergeMemoryPool>) -> Self {
        self.merge_pool = Some(pool);
        self
    }

    /// Leave room for aggregate replay by checking that the pool can admit
    /// merge buffers plus equal headroom. Release the headroom before replay.
    pub(crate) fn with_replay_headroom(mut self) -> Self {
        self.reserve_replay_headroom = true;
        self
    }

    /// Bound intermediate source retention and output materialization together.
    pub(super) fn with_batch_memory_budget(
        mut self,
        budget: Option<MergeBatchMemoryBudget>,
    ) -> Self {
        self.batch_memory_budget = budget;
        self
    }

    /// See [SortPreservingMergeExec::with_round_robin_repartition] for more
    /// information.
    ///
    /// [SortPreservingMergeExec::with_round_robin_repartition]: crate::sorts::sort_preserving_merge::SortPreservingMergeExec::with_round_robin_repartition
    pub fn with_round_robin_tie_breaker(
        mut self,
        enable_round_robin_tie_breaker: bool,
    ) -> Self {
        self.enable_round_robin_tie_breaker = enable_round_robin_tie_breaker;
        self
    }

    /// Bypass the mempool and avoid using the memory reservation.
    ///
    /// This is not marked as `pub` because it is not recommended to use this method
    pub(super) fn with_bypass_mempool(self) -> Self {
        let mem_pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());

        self.with_reservation(
            MemoryConsumer::new("merge stream mock memory").register(&mem_pool),
        )
    }

    pub fn build(self) -> Result<SendableRecordBatchStream> {
        let Self {
            streams,
            sorted_spill_files,
            spill_manager,
            schema,
            metrics,
            batch_size,
            reservation,
            merge_pool,
            reserve_replay_headroom,
            batch_memory_budget,
            fetch,
            expressions,
            enable_round_robin_tie_breaker,
        } = self;

        // Early return if expressions are empty:
        let Some(expressions) = expressions else {
            return internal_err!("Sort expressions cannot be empty for streaming merge");
        };
        let schema = schema.expect("Schema cannot be empty for streaming merge");

        if fetch.is_some_and(|fetch| fetch == 0) {
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema)));
        }

        let batch_size =
            batch_size.expect("Batch size cannot be empty for streaming merge");

        if batch_size == 0 {
            return internal_err!("Batch size cannot be zero for streaming merge");
        }

        if !sorted_spill_files.is_empty() {
            // Unwrapping mandatory fields
            let metrics = metrics.expect("Metrics cannot be empty for streaming merge");
            let reservation =
                reservation.expect("Reservation cannot be empty for streaming merge");

            return Ok(MultiLevelMergeBuilder::new(
                spill_manager.expect("spill_manager should exist"),
                schema,
                sorted_spill_files,
                streams,
                expressions.clone(),
                metrics,
                batch_size,
                reservation,
                fetch,
                enable_round_robin_tie_breaker,
            )
            .with_merge_pool(merge_pool)
            .with_replay_headroom(reserve_replay_headroom)
            .create_spillable_merge_stream());
        }

        // Early return if streams are empty:
        assert_or_internal_err!(
            !streams.is_empty(),
            "Streams/sorted spill files cannot be empty for streaming merge"
        );

        // Unwrapping mandatory fields
        let metrics = metrics.expect("Metrics cannot be empty for streaming merge");
        let reservation =
            reservation.expect("Reservation cannot be empty for streaming merge");
        if let Some(budget) = &batch_memory_budget {
            assert_or_internal_err!(
                budget.input_batch_sizes.len() == streams.len(),
                "merge batch budget must provide one maximum size per input"
            );
        }

        // Special case single column comparisons with optimized cursor implementations
        if expressions.len() == 1 {
            let sort = expressions[0].clone();
            let data_type = sort.expr.data_type(schema.as_ref())?;
            downcast_primitive! {
                data_type => (primitive_merge_helper, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, batch_memory_budget),
                DataType::Utf8 => merge_helper!(StringArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, batch_memory_budget)
                DataType::Utf8View => merge_helper!(StringViewArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, batch_memory_budget)
                DataType::LargeUtf8 => merge_helper!(LargeStringArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, batch_memory_budget)
                DataType::Binary => merge_helper!(BinaryArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, batch_memory_budget)
                DataType::LargeBinary => merge_helper!(LargeBinaryArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, batch_memory_budget)
                _ => {}
            }
        }

        let streams = RowCursorStream::try_new(
            schema.as_ref(),
            expressions,
            streams,
            reservation.new_empty(),
        )?;
        Ok(SortPreservingMergeStream::new(
            Box::new(streams),
            schema,
            metrics,
            batch_size,
            fetch,
            reservation,
            enable_round_robin_tie_breaker,
        )
        .with_batch_memory_budget(batch_memory_budget)
        .into_stream())
    }
}

#[cfg(test)]
mod tests {
    use crate::spill::{get_record_batch_memory_size, spill_manager::GetSlicedSize};
    use crate::{common::collect, stream::RecordBatchStreamAdapter};
    use std::sync::Arc;

    use super::*;

    use arrow::array::{ArrayRef, RecordBatch};
    use arrow::compute::{cast, concat_batches};
    use arrow::datatypes::{Field, Int32Type, Int64Type, Schema};
    use arrow_schema::SortOptions;
    use datafusion_common::Result;
    use datafusion_execution::TaskContext;
    use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };
    use futures::StreamExt;

    #[rstest::rstest]
    #[case::primitive(DataType::Int32)]
    #[case::strings(DataType::Utf8)]
    #[case::views(DataType::Utf8View)]
    #[tokio::test]
    async fn intermediate_merge_bounds_short_batches(
        #[case] data_type: DataType,
        #[values(false, true)] row_cursor: bool,
        #[values(None, Some(7))] fetch: Option<usize>,
    ) -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("x", data_type.clone(), false)]));
        let sort = PhysicalSortExpr::new_default(col("x", &schema)?);
        // Two keys exercise RowCursorStream as well as the specialized cursors.
        let ordering = if row_cursor {
            [sort.clone(), sort].into()
        } else {
            [sort].into()
        };

        for budgeted in [false, true] {
            let mut input_batch_sizes = Vec::new();
            let streams = (0..2)
                .map(|run| {
                    let mut max_batch_bytes = 0;
                    let batches = (0..3)
                        .map(|batch| {
                            let values = [4 * batch + run, 4 * batch + run + 2];
                            let array: ArrayRef = match data_type {
                                DataType::Int32 => {
                                    Arc::new(Int32Array::from_iter_values(values))
                                }
                                DataType::Utf8 => {
                                    Arc::new(StringArray::from_iter_values(
                                        values.map(|value| format!("{value:024}")),
                                    ))
                                }
                                DataType::Utf8View => {
                                    Arc::new(StringViewArray::from_iter_values(
                                        values.map(|value| format!("{value:024}")),
                                    ))
                                }
                                _ => unreachable!(),
                            };
                            let batch =
                                RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
                            max_batch_bytes =
                                max_batch_bytes.max(get_record_batch_memory_size(&batch));
                            Ok(batch)
                        })
                        .collect::<Vec<Result<RecordBatch>>>();
                    input_batch_sizes.push(max_batch_bytes);
                    Box::pin(RecordBatchStreamAdapter::new(
                        Arc::clone(&schema),
                        futures::stream::iter(batches),
                    )) as SendableRecordBatchStream
                })
                .collect();
            let max_output_bytes = input_batch_sizes.iter().sum::<usize>();
            let stream = StreamingMergeBuilder::new()
                .with_schema(Arc::clone(&schema))
                .with_expressions(&ordering)
                .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
                .with_streams(streams)
                .with_batch_size(8192)
                .with_fetch(fetch)
                .with_batch_memory_budget(budgeted.then_some(MergeBatchMemoryBudget {
                    memory_limit: 2 * max_output_bytes,
                    input_batch_sizes,
                }))
                .with_bypass_mempool()
                .build()?;
            let batches = collect(stream).await?;
            let merged = concat_batches(&schema, &batches)?;
            let actual = cast(merged.column(0), &DataType::Int32)?;
            let expected = Int32Array::from_iter_values(0..fetch.unwrap_or(12) as i32);
            assert_eq!(actual.as_primitive::<Int32Type>(), &expected);

            if budgeted {
                for batch in &batches {
                    assert!(get_record_batch_memory_size(batch) <= max_output_bytes);
                }
            } else {
                assert_eq!(batches.len(), 1, "ordinary merges retain their batching");
            }
        }
        Ok(())
    }

    #[rstest::rstest]
    #[case::short_inputs(1000)]
    #[case::full_inputs(8192)]
    #[tokio::test]
    async fn intermediate_merge_keeps_full_output_batches(
        #[case] input_rows: usize,
        #[values(false, true)] row_cursor: bool,
    ) -> Result<()> {
        const RUNS: usize = 8;
        const BATCHES: usize = 10;
        const OUTPUT_ROWS: usize = 8192;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let sort = PhysicalSortExpr::new_default(col("x", &schema)?);
        let ordering = if row_cursor {
            [sort.clone(), sort].into()
        } else {
            [sort].into()
        };
        let mut input_batch_sizes = Vec::new();
        let mut streams = Vec::new();
        for run in 0..RUNS {
            let mut batches = Vec::new();
            let mut max_batch_bytes = 0;
            for batch_index in 0..BATCHES {
                let values =
                    Int64Array::from_iter_values((0..input_rows).map(|row| {
                        ((batch_index * input_rows + row) * RUNS + run) as i64
                    }));
                let batch =
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])?;
                max_batch_bytes =
                    max_batch_bytes.max(get_record_batch_memory_size(&batch));
                batches.push(Ok(batch));
            }
            input_batch_sizes.push(max_batch_bytes);
            streams.push(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(batches),
            )) as SendableRecordBatchStream);
        }
        let memory_limit = 2 * input_batch_sizes.iter().sum::<usize>();
        let stream = StreamingMergeBuilder::new()
            .with_schema(Arc::clone(&schema))
            .with_expressions(&ordering)
            .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
            .with_streams(streams)
            .with_batch_size(OUTPUT_ROWS)
            .with_batch_memory_budget(Some(MergeBatchMemoryBudget {
                memory_limit,
                input_batch_sizes,
            }))
            .with_bypass_mempool()
            .build()?;
        let batches = collect(stream).await?;
        if input_rows == OUTPUT_ROWS {
            assert_eq!(batches.len(), RUNS * BATCHES);
            assert!(batches.iter().all(|batch| batch.num_rows() == OUTPUT_ROWS));
        } else {
            // Unconditional boundary flushing produces 80 batches here, mostly
            // one-row fragments. Future rows from every live input still need
            // a conservative allowance, so short inputs can require early
            // output, but should at least halve the number of batches.
            assert!(
                batches.len() <= RUNS * BATCHES / 2,
                "{} output batches",
                batches.len()
            );
        }
        let merged = concat_batches(&schema, &batches)?;
        assert_eq!(
            merged.column(0).as_primitive::<Int64Type>(),
            &Int64Array::from_iter_values(0..(RUNS * BATCHES * input_rows) as i64)
        );
        Ok(())
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn intermediate_merge_preserves_ties_across_pending_input_boundaries(
        #[values(false, true)] row_cursor: bool,
        #[values(false, true)] round_robin: bool,
    ) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("source", DataType::Int32, false),
        ]));
        let sort = PhysicalSortExpr::new_default(col("key", &schema)?);
        let ordering = if row_cursor {
            [sort.clone(), sort].into()
        } else {
            [sort].into()
        };
        let mut expected = None;
        for budgeted in [false, true] {
            let mut streams = Vec::new();
            let mut input_batch_sizes = Vec::new();
            for source in 0..2 {
                let mut batches = Vec::new();
                let mut max_batch_bytes = 0;
                for batch_index in 0..3 {
                    // Equal keys span two input batches, and source tags make
                    // changes to the tie-breaking order observable.
                    let batch = RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int32Array::from(vec![batch_index / 2; 2])),
                            Arc::new(Int32Array::from(vec![source; 2])),
                        ],
                    )?;
                    max_batch_bytes =
                        max_batch_bytes.max(get_record_batch_memory_size(&batch));
                    batches.push(Ok(batch));
                }
                input_batch_sizes.push(max_batch_bytes);
                let input = futures::stream::iter(batches).then(|batch| async {
                    tokio::task::yield_now().await;
                    batch
                });
                streams.push(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&schema),
                    input,
                )) as SendableRecordBatchStream);
            }
            let memory_limit = 2 * input_batch_sizes.iter().sum::<usize>();
            let stream = StreamingMergeBuilder::new()
                .with_schema(Arc::clone(&schema))
                .with_expressions(&ordering)
                .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
                .with_streams(streams)
                .with_batch_size(8192)
                .with_round_robin_tie_breaker(round_robin)
                .with_batch_memory_budget(budgeted.then_some(MergeBatchMemoryBudget {
                    memory_limit,
                    input_batch_sizes,
                }))
                .with_bypass_mempool()
                .build()?;
            let batches = collect(stream).await?;
            let merged = concat_batches(&schema, &batches)?;
            assert_eq!(
                merged.column(0).as_primitive::<Int32Type>(),
                &Int32Array::from(vec![0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1])
            );
            if let Some(expected) = &expected {
                assert_eq!(&merged, expected);
            } else {
                expected = Some(merged);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn intermediate_merge_releases_consumed_dictionary_batches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new(
                "payload",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8View),
                ),
                false,
            ),
        ]));
        let mut max_input_bytes = 0;
        let mut max_input_memory = 0;
        let mut streams = Vec::new();
        for run in 0..2 {
            let mut batches = Vec::new();
            for batch_index in 0..3 {
                let keys = Int32Array::from(vec![
                    4 * batch_index + run,
                    4 * batch_index + run + 2,
                ]);
                let values = StringViewArray::from(vec![format!(
                    "{run}:{batch_index}:{}",
                    "x".repeat(1024),
                )]);
                let payload = DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 0]),
                    Arc::new(values),
                )?;
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(keys), Arc::new(payload)],
                )?;
                max_input_bytes = max_input_bytes.max(batch.get_sliced_size()?);
                max_input_memory =
                    max_input_memory.max(get_record_batch_memory_size(&batch));
                batches.push(Ok(batch));
            }
            streams.push(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(batches),
            )) as SendableRecordBatchStream);
        }
        let ordering = [PhysicalSortExpr::new_default(col("x", &schema)?)].into();
        let stream = StreamingMergeBuilder::new()
            .with_schema(schema)
            .with_expressions(&ordering)
            .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
            .with_streams(streams)
            .with_batch_size(8192)
            .with_batch_memory_budget(Some(MergeBatchMemoryBudget {
                memory_limit: 4 * max_input_memory,
                input_batch_sizes: vec![max_input_memory; 2],
            }))
            .with_bypass_mempool()
            .build()?;
        let batches = collect(stream).await?;
        let mut expected_key = 0;
        for batch in batches {
            // Arrow's dictionary interleave can concatenate values from all
            // buffered batches, including ones with no selected rows. Fully
            // consumed inputs must be removed before accepting replacements.
            assert!(batch.get_sliced_size()? <= 2 * max_input_bytes);
            assert!(batch.column(1).as_dictionary::<Int32Type>().values().len() <= 2);
            let payload = cast(batch.column(1), &DataType::Utf8View)?;
            for (key, value) in batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .iter()
                .zip(payload.as_string_view().iter())
            {
                assert_eq!(*key, expected_key);
                assert_eq!(
                    value,
                    Some(
                        format!("{}:{}:{}", key % 2, key / 4, "x".repeat(1024)).as_str()
                    )
                );
                expected_key += 1;
            }
        }
        assert_eq!(expected_key, 12);
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_merge_fetch_zero_with_only_1_stream() {
        test_fetch_0_should_output_0_rows(1, 0).await.unwrap();
    }
    #[tokio::test]
    async fn test_sort_merge_fetch_zero_with_2_streams() {
        test_fetch_0_should_output_0_rows(2, 0).await.unwrap();
    }
    #[tokio::test]
    async fn test_sort_merge_fetch_zero_with_only_1_spill_file() {
        test_fetch_0_should_output_0_rows(0, 1).await.unwrap();
    }
    #[tokio::test]
    async fn test_sort_merge_fetch_zero_with_2_spill_files() {
        test_fetch_0_should_output_0_rows(0, 2).await.unwrap();
    }
    #[tokio::test]
    async fn test_sort_merge_fetch_zero_with_1_stream_and_1_spill_file() {
        test_fetch_0_should_output_0_rows(1, 1).await.unwrap();
    }

    async fn test_fetch_0_should_output_0_rows(
        number_of_streams: usize,
        number_of_spilled_files: usize,
    ) -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
        let schema = batch.schema();

        let sort: LexOrdering = [PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }]
        .into();

        let streams = (0..number_of_streams)
            .map(|_| {
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&schema),
                    futures::stream::iter(vec![Ok(batch.clone())]),
                )) as SendableRecordBatchStream
            })
            .collect::<Vec<SendableRecordBatchStream>>();

        let spill_manager = SpillManager::new(
            task_ctx.runtime_env(),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(&schema),
        );

        let mut sorted_spill_files: Vec<SortedSpillFile> = vec![];

        for _ in 0..number_of_spilled_files {
            let file = spill_manager
                .spill_record_batch_and_finish(std::slice::from_ref(&batch), "spill")
                .unwrap()
                .unwrap();
            sorted_spill_files.push(SortedSpillFile {
                file,
                max_record_batch_memory: batch.get_array_memory_size(),
            });
        }

        let sorted_output_stream = StreamingMergeBuilder::new()
            .with_batch_size(100)
            .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
            // Just to avoid having to provide memory pool
            .with_bypass_mempool()
            .with_schema(schema)
            .with_streams(streams)
            .with_sorted_spill_files(sorted_spill_files)
            .with_spill_manager(spill_manager)
            .with_expressions(&sort)
            // The whole point of the test - fetch is 0
            .with_fetch(Some(0))
            .build()
            .unwrap();

        let collected = collect(sorted_output_stream).await.unwrap();
        let total: usize = collected.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "fetch=Some(0) must emit zero rows, got {total}");

        Ok(())
    }
}
