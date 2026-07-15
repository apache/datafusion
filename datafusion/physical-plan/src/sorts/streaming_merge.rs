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
use crate::{SendableRecordBatchStream, SpillManager};
use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::human_readable_size;
use datafusion_common::{Result, assert_or_internal_err, internal_err};
use datafusion_execution::SpillFile;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
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
            fetch,
            expressions,
            enable_round_robin_tie_breaker,
        } = self;

        // Early return if expressions are empty:
        let Some(expressions) = expressions else {
            return internal_err!("Sort expressions cannot be empty for streaming merge");
        };

        if !sorted_spill_files.is_empty() {
            // Unwrapping mandatory fields
            let schema = schema.expect("Schema cannot be empty for streaming merge");
            let metrics = metrics.expect("Metrics cannot be empty for streaming merge");
            let batch_size =
                batch_size.expect("Batch size cannot be empty for streaming merge");
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
            .create_spillable_merge_stream());
        }

        // Early return if streams are empty:
        assert_or_internal_err!(
            !streams.is_empty(),
            "Streams/sorted spill files cannot be empty for streaming merge"
        );

        // Unwrapping mandatory fields
        let schema = schema.expect("Schema cannot be empty for streaming merge");
        let metrics = metrics.expect("Metrics cannot be empty for streaming merge");
        let batch_size =
            batch_size.expect("Batch size cannot be empty for streaming merge");
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
    use std::{ops::Deref, sync::Arc};

    use arrow::{
        array::{ArrayRef, AsArray, Int32Array, RecordBatch},
        datatypes::Int32Type,
    };
    use arrow_schema::{Field, Fields, Schema, SchemaRef, SortOptions};
    use datafusion_common::Result;
    use datafusion_execution::{
        memory_pool::{
            GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
        },
        runtime_env::RuntimeEnv,
    };
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
    use datafusion_physical_expr_common::metrics::{
        BaselineMetrics, ExecutionPlanMetricsSet, SpillMetrics,
    };
    use futures::TryStreamExt;
    use itertools::Itertools;

    use crate::{
        SpillManager,
        sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder},
    };

    fn create_batches(
        arrays: Vec<(&str, ArrayRef)>,
        batch_size: usize,
    ) -> Vec<RecordBatch> {
        let fields = arrays
            .iter()
            .map(|(name, arr)| {
                Arc::new(Field::new(
                    *name,
                    arr.data_type().clone(),
                    arr.is_nullable(),
                ))
            })
            .collect::<Fields>();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema,
            arrays.into_iter().map(|(_name, arr)| arr).collect(),
        )
        .unwrap();

        (0..batch.num_rows().div_ceil(batch_size))
            .map(|index| {
                let offset = index * batch_size;
                let left = batch.num_rows() - offset;
                batch.slice(offset, left.min(batch_size))
            })
            .collect::<Vec<_>>()
    }

    fn create_builder<'a>(
        schema: &SchemaRef,
        merge_fan_in: Option<usize>,
    ) -> StreamingMergeBuilder<'a> {
        let runtime = Arc::new(RuntimeEnv::default());

        if let Some(merge_fan_in) = merge_fan_in {
            runtime
                .disk_manager
                .set_max_spill_merge_fan_in(merge_fan_in);
        }

        let mem_pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let spill_manager = SpillManager::new(
            runtime,
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(schema),
        );

        StreamingMergeBuilder::new()
            .with_spill_manager(spill_manager)
            .with_reservation(
                MemoryConsumer::new("merge stream mock memory").register(&mem_pool),
            )
            .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::default(), 0))
            .with_schema(Arc::clone(schema))
    }

    fn spill_batches(
        spill_manager: &SpillManager,
        batches_for_files: &[&[RecordBatch]],
    ) -> Vec<SortedSpillFile> {
        batches_for_files
            .iter()
            .map(|batches| {
                let (file, max_record_batch_memory) = spill_manager
                    .spill_record_batch_iter_and_return_max_batch_memory(
                        batches.iter().map(Ok),
                        "spill",
                    )
                    .unwrap()
                    .unwrap();

                SortedSpillFile {
                    file,
                    max_record_batch_memory,
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn multi_level_merge_sort_should_respect_limit_for_single_spill_file() {
        let batch_size = 10;
        let batches = create_batches(
            vec![(
                "a",
                Arc::new((0..(batch_size * 2) as i32).collect::<Int32Array>()),
            )],
            batch_size,
        );

        let schema = batches[0].schema();

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("a", &schema).unwrap(),
            // Match the existing sort order of the data
            SortOptions::default().asc(),
        )])
        .unwrap();

        let fetch = (batch_size as f64 * 1.5) as usize;
        assert_ne!(fetch % batch_size, 0);

        let merge_builder = create_builder(&schema, None)
            .with_batch_size(batch_size)
            .with_fetch(Some(fetch))
            .with_expressions(&ordering);

        let spilled_files =
            spill_batches(merge_builder.spill_manager.as_ref().unwrap(), &[&batches]);

        let stream = merge_builder
            .with_sorted_spill_files(spilled_files)
            .build()
            .unwrap();

        let sorted: Result<Vec<RecordBatch>> = stream.try_collect().await;
        let sorted = sorted.unwrap();

        let output = arrow::compute::concat_batches(&schema, sorted.iter()).unwrap();

        assert_eq!(output.num_rows(), fetch);

        let rows = output.column(0).as_primitive::<Int32Type>();
        assert_eq!(
            rows.values().deref(),
            (0..(fetch as i32)).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn multi_level_merge_sort_should_be_stable_when_round_robin_tie_breaker_is_disabled()
     {
        let input_batch_size = 10;
        let num_rows = input_batch_size * 6;
        let batches = create_batches(
            vec![
                // All the same value so it should match
                (
                    "a",
                    Arc::new((0..num_rows).map(|_| 0).collect::<Int32Array>()),
                ),
                (
                    "other",
                    Arc::new((0..num_rows as i32).collect::<Int32Array>()),
                ),
            ],
            input_batch_size,
        );

        let schema = batches[0].schema();

        let files_batches = batches.chunks(2).collect::<Vec<_>>();
        assert_eq!(files_batches.len(), 3);

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("a", &schema).unwrap(),
        )])
        .unwrap();
        let output_batch_size = (input_batch_size as f64 * 1.5) as usize;

        let merge_builder = create_builder(&schema, Some(2))
            .with_batch_size(output_batch_size)
            .with_fetch(None)
            .with_expressions(&ordering)
            .with_round_robin_tie_breaker(false);

        let spilled_files = spill_batches(
            merge_builder.spill_manager.as_ref().unwrap(),
            &files_batches,
        );

        let stream = merge_builder
            .with_sorted_spill_files(spilled_files)
            .build()
            .unwrap();

        let sorted: Result<Vec<RecordBatch>> = stream.try_collect().await;
        let sorted = sorted.unwrap();

        let output = arrow::compute::concat_batches(&schema, sorted.iter()).unwrap();

        let other_rows = output.column(1).as_primitive::<Int32Type>();
        assert_eq!(
            other_rows.values().deref(),
            (0..num_rows as i32).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn multi_level_merge_sort_should_respect_batch_size_for_single_spill_file() {
        let input_batch_size = 10;
        let num_rows = input_batch_size * 4;
        let batches = create_batches(
            vec![("a", Arc::new((0..num_rows as i32).collect::<Int32Array>()))],
            input_batch_size,
        );

        let schema = batches[0].schema();

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("a", &schema).unwrap(),
            // Match the existing sort order of the data
            SortOptions::default().asc(),
        )])
        .unwrap();
        let output_batch_size = (input_batch_size as f64 * 1.5) as usize;

        let merge_builder = create_builder(&schema, None)
            .with_batch_size(output_batch_size)
            .with_fetch(None)
            .with_expressions(&ordering);

        let spilled_files =
            spill_batches(merge_builder.spill_manager.as_ref().unwrap(), &[&batches]);

        let stream = merge_builder
            .with_sorted_spill_files(spilled_files)
            .build()
            .unwrap();

        let sorted: Result<Vec<RecordBatch>> = stream.try_collect().await;
        let sorted = sorted.unwrap();

        assert_eq!(sorted.iter().map(|a| a.num_rows()).sum::<usize>(), num_rows);

        for sorted_batch in sorted.iter().dropping_back(1) {
            assert_eq!(
                sorted_batch.num_rows(),
                output_batch_size,
                "batch size mismatch"
            );
        }

        let last_batch_num_rows = sorted.last().unwrap().num_rows();
        assert!(
            last_batch_num_rows <= output_batch_size,
            "last batch size mismatch: {last_batch_num_rows} <= {output_batch_size}"
        );
    }

    // TODO - single spill file does not respect batch size
    //
}
