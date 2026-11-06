// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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

macro_rules! primitive_merge_helper {
    ($t:ty, $($v:ident),+) => {
        merge_helper!(PrimitiveArray<$t>, $($v),+)
    };
}

macro_rules! merge_helper {
    ($t:ty, $sort:ident, $streams:ident, $schema:ident, $tracking_metrics:ident, $batch_size:ident, $target_batch_bytes:ident, $output_construction_reservation:ident, $retain_output_construction_reservation:ident, $shared_input_buffers_reserved:ident, $fetch:ident, $reservation:ident, $enable_round_robin_tie_breaker:ident) => {{
        let streams =
            FieldCursorStream::<$t>::new($sort, $streams, $reservation.new_empty());
        return Ok(SortPreservingMergeStream::new(
            Box::new(streams),
            $schema,
            $tracking_metrics,
            $batch_size,
            $target_batch_bytes,
            $output_construction_reservation,
            $retain_output_construction_reservation,
            $fetch,
            $reservation,
            $enable_round_robin_tie_breaker,
        )
        .with_reserved_shared_input_buffers($shared_input_buffers_reserved)
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

/// Writer-generated statistics stay attached to their file through selection,
/// splitting and intermediate writes. External public files have no progress
/// guarantee and retain the conservative admission path.
pub(crate) struct SpillFileWithMemory {
    pub file: Arc<dyn SpillFile>,
    pub max_record_batch_memory: usize,
    pub progress: Option<super::ProgressStats>,
}

impl From<SortedSpillFile> for SpillFileWithMemory {
    fn from(file: SortedSpillFile) -> Self {
        Self {
            file: file.file,
            max_record_batch_memory: file.max_record_batch_memory,
            progress: None,
        }
    }
}

#[derive(Default)]
pub struct StreamingMergeBuilder<'a> {
    streams: Vec<SendableRecordBatchStream>,
    sorted_spill_files: Vec<SpillFileWithMemory>,
    spill_manager: Option<SpillManager>,
    schema: Option<SchemaRef>,
    expressions: Option<&'a LexOrdering>,
    metrics: Option<BaselineMetrics>,
    batch_size: Option<usize>,
    target_batch_bytes: Option<usize>,
    output_construction_reservation: Option<MemoryReservation>,
    retain_output_construction_reservation: bool,
    shared_input_buffers_reserved: bool,
    fetch: Option<usize>,
    reservation: Option<MemoryReservation>,
    merge_pool: Option<Arc<MergeMemoryPool>>,
    /// Leave memory for the aggregate consuming the merged spill rows.
    reserve_replay_headroom: bool,
    min_spill_batch_rows: Option<usize>,
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
        self.sorted_spill_files =
            sorted_spill_files.into_iter().map(Into::into).collect();
        self
    }

    pub(crate) fn with_spill_inputs(mut self, files: Vec<SpillFileWithMemory>) -> Self {
        self.sorted_spill_files = files;
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

    pub(super) fn with_target_batch_bytes(
        mut self,
        target_batch_bytes: Option<usize>,
    ) -> Self {
        self.target_batch_bytes = target_batch_bytes;
        self
    }

    pub(super) fn with_output_construction_reservation(
        mut self,
        reservation: Option<MemoryReservation>,
    ) -> Self {
        self.output_construction_reservation = reservation;
        self
    }

    /// Retain pre-admitted output construction workspace across stream polls.
    pub(super) fn with_retained_output_construction_reservation(
        mut self,
        retain: bool,
    ) -> Self {
        self.retain_output_construction_reservation = retain;
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
    pub(crate) fn with_merge_pool(mut self, pool: Arc<MergeMemoryPool>) -> Self {
        self.merge_pool = Some(pool);
        self
    }

    /// Leave room for aggregate replay by checking that the pool can admit
    /// merge buffers plus equal headroom. Release the headroom before replay.
    pub(crate) fn with_replay_headroom(mut self) -> Self {
        self.reserve_replay_headroom = true;
        self
    }

    /// Limit intermediate rewrites when the retained buffer reservation can
    /// also admit the final replay pass with its headroom.
    /// `min_spill_batch_rows` is the minimum largest-batch row count across input
    /// runs. A run without a full batch disables sizing for the whole replay,
    /// since merging short batches can increase the intermediate run's budget.
    pub(crate) fn with_intermediate_merge_sizing(
        mut self,
        min_spill_batch_rows: Option<usize>,
    ) -> Self {
        self.min_spill_batch_rows = min_spill_batch_rows;
        self
    }

    /// Credit shared View data only while the caller's input grant is held.
    pub(super) fn with_reserved_shared_input_buffers(mut self, covered: bool) -> Self {
        self.shared_input_buffers_reserved = covered;
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
            target_batch_bytes,
            output_construction_reservation,
            retain_output_construction_reservation,
            shared_input_buffers_reserved,
            reservation,
            merge_pool,
            reserve_replay_headroom,
            min_spill_batch_rows,
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

            return Ok(MultiLevelMergeBuilder::new_with_memory(
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
            .with_intermediate_merge_sizing(min_spill_batch_rows)
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

        // Special case single column comparisons with optimized cursor implementations
        if expressions.len() == 1 {
            let sort = expressions[0].clone();
            let data_type = sort.expr.data_type(schema.as_ref())?;
            downcast_primitive! {
                data_type => (primitive_merge_helper, sort, streams, schema, metrics, batch_size, target_batch_bytes, output_construction_reservation, retain_output_construction_reservation, shared_input_buffers_reserved, fetch, reservation, enable_round_robin_tie_breaker),
                DataType::Utf8 => merge_helper!(StringArray, sort, streams, schema, metrics, batch_size, target_batch_bytes, output_construction_reservation, retain_output_construction_reservation, shared_input_buffers_reserved, fetch, reservation, enable_round_robin_tie_breaker)
                DataType::Utf8View => merge_helper!(StringViewArray, sort, streams, schema, metrics, batch_size, target_batch_bytes, output_construction_reservation, retain_output_construction_reservation, shared_input_buffers_reserved, fetch, reservation, enable_round_robin_tie_breaker)
                DataType::LargeUtf8 => merge_helper!(LargeStringArray, sort, streams, schema, metrics, batch_size, target_batch_bytes, output_construction_reservation, retain_output_construction_reservation, shared_input_buffers_reserved, fetch, reservation, enable_round_robin_tie_breaker)
                DataType::Binary => merge_helper!(BinaryArray, sort, streams, schema, metrics, batch_size, target_batch_bytes, output_construction_reservation, retain_output_construction_reservation, shared_input_buffers_reserved, fetch, reservation, enable_round_robin_tie_breaker)
                DataType::LargeBinary => merge_helper!(LargeBinaryArray, sort, streams, schema, metrics, batch_size, target_batch_bytes, output_construction_reservation, retain_output_construction_reservation, shared_input_buffers_reserved, fetch, reservation, enable_round_robin_tie_breaker)
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
            target_batch_bytes,
            output_construction_reservation,
            retain_output_construction_reservation,
            fetch,
            reservation,
            enable_round_robin_tie_breaker,
        )
        .with_reserved_shared_input_buffers(shared_input_buffers_reserved)
        .into_stream())
    }
}

#[cfg(test)]
mod tests {
    use crate::{common::collect, stream::RecordBatchStreamAdapter};
    use std::sync::Arc;

    use super::*;

    use arrow::array::{ArrayRef, RecordBatch};
    use arrow_schema::SortOptions;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_execution::TaskContext;
    use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };

    /// Lazy one-row inputs expose retention that pre-built input vectors hide.
    /// Without draining before input replacement, the builder retains all twenty
    /// source arrays even though only two input cursors are live.
    #[tokio::test]
    async fn test_variable_merge_releases_exhausted_inputs() {
        use arrow::array::Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};
        use futures::StreamExt;
        use std::sync::{Mutex, Weak};

        for (variable, nested_fixed, fetch, fail_at, stop_early) in [
            (true, false, None, None, false),
            (true, false, Some(7), None, false),
            (true, false, Some(20), None, false),
            (true, false, None, Some(4), false),
            (true, false, None, None, true),
            (false, false, None, None, false),
            (false, true, None, None, false),
            (false, true, Some(7), None, false),
            (false, true, None, Some(4), false),
            (false, true, None, None, true),
        ] {
            let schema = Arc::new(Schema::new(if variable {
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("payload", DataType::Utf8, false),
                ]
            } else if nested_fixed {
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new(
                        "payload",
                        DataType::Struct(
                            vec![Arc::new(Field::new("child", DataType::Int32, false))]
                                .into(),
                        ),
                        false,
                    ),
                ]
            } else {
                vec![Field::new("key", DataType::Int32, false)]
            }));
            let seen = Arc::new(Mutex::new(Vec::<Weak<dyn Array>>::new()));
            let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let streams = (0..2)
                .map(|partition| {
                    let schema = Arc::clone(&schema);
                    let seen = Arc::clone(&seen);
                    let peak = Arc::clone(&peak);
                    let input_schema = Arc::clone(&schema);
                    let input = futures::stream::iter(0..10).map(move |row| {
                        if fail_at == Some(row) && partition == 0 {
                            return Err(DataFusionError::Execution(
                                "input failed".into(),
                            ));
                        }
                        let key: ArrayRef =
                            Arc::new(Int32Array::from(vec![row * 2 + partition]));
                        let mut columns = vec![key];
                        if variable {
                            columns.push(Arc::new(StringArray::from(vec![
                                "x".repeat(4096),
                            ])));
                        }
                        if nested_fixed {
                            let child: ArrayRef = Arc::new(Int32Array::from(vec![row]));
                            columns.push(Arc::new(StructArray::new(
                                vec![Arc::new(Field::new(
                                    "child",
                                    DataType::Int32,
                                    false,
                                ))]
                                .into(),
                                vec![child],
                                None,
                            )));
                        }
                        let tracked = columns.last().unwrap();
                        let mut seen = seen.lock().unwrap();
                        seen.push(Arc::downgrade(tracked));
                        let live = seen.iter().filter(|a| a.strong_count() != 0).count();
                        peak.fetch_max(live, std::sync::atomic::Ordering::Relaxed);
                        RecordBatch::try_new(Arc::clone(&input_schema), columns)
                            .map_err(Into::into)
                    });
                    Box::pin(RecordBatchStreamAdapter::new(schema, input))
                        as SendableRecordBatchStream
                })
                .collect();
            let sort: LexOrdering =
                [PhysicalSortExpr::new_default(col("key", &schema).unwrap())].into();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
            let output = MemoryConsumer::new("output").register(&pool);
            let mut stream = StreamingMergeBuilder::new()
                .with_batch_size(20)
                .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
                .with_bypass_mempool()
                .with_schema(schema)
                .with_streams(streams)
                .with_expressions(&sort)
                .with_target_batch_bytes(Some(64 * 1024))
                .with_output_construction_reservation(Some(output))
                .with_fetch(fetch)
                .build()
                .unwrap();
            let mut keys = Vec::new();
            let mut failed = false;
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => {
                        if !variable && !nested_fixed {
                            assert_eq!(
                                batch.num_rows(),
                                20,
                                "fixed-width row batching must not change"
                            );
                        }
                        keys.extend_from_slice(
                            batch
                                .column(0)
                                .as_primitive::<arrow::datatypes::Int32Type>()
                                .values(),
                        );
                    }
                    Err(error) => {
                        assert!(error.to_string().contains("input failed"));
                        failed = true;
                        break;
                    }
                }
                if stop_early {
                    break;
                }
            }
            drop(stream);
            assert_eq!(pool.reserved(), 0);
            assert!(seen.lock().unwrap().iter().all(|a| a.strong_count() == 0));
            assert_eq!(failed, fail_at.is_some());
            if !failed && !stop_early {
                assert_eq!(keys, (0..fetch.unwrap_or(20) as i32).collect::<Vec<_>>());
            }
            if variable || nested_fixed {
                assert!(peak.load(std::sync::atomic::Ordering::Relaxed) <= 2);
            }
        }
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
