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

macro_rules! primitive_merge_helper {
    ($t:ty, $($v:ident),+) => {
        merge_helper!(PrimitiveArray<$t>, $($v),+)
    };
}

macro_rules! merge_helper {
    ($t:ty, $sort:ident, $streams:ident, $schema:ident, $tracking_metrics:ident, $batch_size:ident, $fetch:ident, $reservation:ident, $enable_round_robin_tie_breaker:ident) => {{
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
                data_type => (primitive_merge_helper, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker),
                DataType::Utf8 => merge_helper!(StringArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker)
                DataType::Utf8View => merge_helper!(StringViewArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker)
                DataType::LargeUtf8 => merge_helper!(LargeStringArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker)
                DataType::Binary => merge_helper!(BinaryArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker)
                DataType::LargeBinary => merge_helper!(LargeBinaryArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker)
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
    use datafusion_common::Result;
    use datafusion_execution::TaskContext;
    use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };

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
