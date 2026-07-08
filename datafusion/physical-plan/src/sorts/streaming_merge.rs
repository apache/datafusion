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
        return Ok(Box::pin(SortPreservingMergeStream::new(
            Box::new(streams),
            $schema,
            $tracking_metrics,
            $batch_size,
            $fetch,
            $reservation,
            $enable_round_robin_tie_breaker,
        )));
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
        Ok(Box::pin(SortPreservingMergeStream::new(
            Box::new(streams),
            schema,
            metrics,
            batch_size,
            fetch,
            reservation,
            enable_round_robin_tie_breaker,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::collect;
    use crate::memory::MemoryStream;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::Int32Type;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_execution::memory_pool::GreedyMemoryPool;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::metrics::ExecutionPlanMetricsSet;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
    use std::sync::Arc;

    /// Merge the given streams (each a list of batches of nullable `sort_key`
    /// values, nulls sorted first) and return the merged values.
    async fn merge_nullable_i32_streams(
        inputs: Vec<Vec<Vec<Option<i32>>>>,
        batch_size: usize,
        fetch: Option<usize>,
    ) -> Vec<Vec<Option<i32>>> {
        merge_nullable_i32_streams_with_pool(
            inputs,
            batch_size,
            fetch,
            Arc::new(UnboundedMemoryPool::default()),
        )
        .await
    }

    async fn merge_nullable_i32_streams_with_pool(
        inputs: Vec<Vec<Vec<Option<i32>>>>,
        batch_size: usize,
        fetch: Option<usize>,
        mem_pool: Arc<dyn MemoryPool>,
    ) -> Vec<Vec<Option<i32>>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sort_key",
            DataType::Int32,
            true,
        )]));

        let streams = inputs
            .into_iter()
            .map(|batches| {
                let batches = batches
                    .into_iter()
                    .map(|values| {
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(Int32Array::from(values))],
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                Box::pin(
                    MemoryStream::try_new(batches, Arc::clone(&schema), None).unwrap(),
                ) as SendableRecordBatchStream
            })
            .collect::<Vec<_>>();

        let merged = StreamingMergeBuilder::new()
            .with_streams(streams)
            .with_batch_size(batch_size)
            .with_schema(Arc::clone(&schema))
            .with_reservation(
                MemoryConsumer::new("merge stream mock memory").register(&mem_pool),
            )
            .with_expressions(
                &([
                    PhysicalSortExpr::new_default(col("sort_key", &schema).unwrap())
                        .asc(),
                ]
                .into()),
            )
            .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
            .with_fetch(fetch)
            .build()
            .unwrap();

        let output = collect(merged).await.unwrap();

        output
            .iter()
            .map(|b| b.column(0).as_primitive::<Int32Type>().iter().collect())
            .collect()
    }

    /// Same as [`merge_nullable_i32_streams`] for non-nullable values.
    async fn merge_i32_streams(
        inputs: Vec<Vec<Vec<i32>>>,
        batch_size: usize,
        fetch: Option<usize>,
    ) -> Vec<Vec<i32>> {
        let inputs = inputs
            .into_iter()
            .map(|stream| {
                stream
                    .into_iter()
                    .map(|batch| batch.into_iter().map(Some).collect())
                    .collect()
            })
            .collect();

        merge_nullable_i32_streams(inputs, batch_size, fetch)
            .await
            .into_iter()
            .map(|batch| batch.into_iter().map(|v| v.unwrap()).collect())
            .collect()
    }

    fn flatten_vec<T>(input: Vec<Vec<T>>) -> Vec<T> {
        input.into_iter().flatten().collect()
    }

    #[tokio::test]
    async fn test_merge_non_overlapping_batches() {
        // streams take turns holding the smallest batch
        let streams = vec![
            vec![
                vec![1, 2, 3, 4, 5],
                vec![16, 17, 18, 19, 20],
                vec![31, 32, 33],
            ],
            vec![
                vec![6, 7, 8, 9, 10],
                vec![21, 22, 23, 24, 25],
                vec![34, 35, 36],
            ],
            vec![
                vec![11, 12, 13, 14, 15],
                vec![26, 27, 28, 29, 30],
                vec![37, 38],
            ],
        ];

        let output = merge_i32_streams(streams, 5, None).await;

        assert_eq!(flatten_vec(output), (1..=38).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_merge_full_batch_release_memory() {
        // The pool only fits a few batches of the ~400KB total input:
        // if skipped batches were not released as the merge progresses, the reservation would exhaust the pool.
        const ROWS: i32 = 1024; // ~4KB per batch
        const BATCHES_PER_STREAM: i32 = 50;

        let batch_of = |start: i32| (start..start + ROWS).map(Some).collect::<Vec<_>>();
        let streams = (0..2)
            .map(|stream| {
                (0..BATCHES_PER_STREAM)
                    .map(|batch| batch_of((batch * 2 + stream) * ROWS))
                    .collect()
            })
            .collect();

        let output = merge_nullable_i32_streams_with_pool(
            streams,
            ROWS as usize,
            None,
            Arc::new(GreedyMemoryPool::new(64 * 1024)),
        )
        .await;

        let expected = (0..2 * BATCHES_PER_STREAM * ROWS)
            .map(Some)
            .collect::<Vec<_>>();
        assert_eq!(flatten_vec(output), expected);
    }

    #[tokio::test]
    async fn test_merge_interleaved_rows() {
        // every batch overlaps batches of the other streams
        let streams = vec![
            vec![vec![1, 4, 7], vec![10, 13, 16]],
            vec![vec![2, 5, 8], vec![11, 14, 17]],
            vec![vec![3, 6, 9], vec![12, 15, 18]],
        ];

        let output = merge_i32_streams(streams, 4, None).await;

        assert_eq!(flatten_vec(output), (1..=18).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_merge_some_rows_from_other_streams_then_a_whole_batch_from_one() {
        // one row from stream 0, two from stream 1 (batch_size 4 is still
        // not filled), then stream 2's entire batch [4, 5, 6, 7] sorts before
        // everything the other streams have left (8 and 11)
        let streams = vec![
            vec![vec![1, 8, 9, 10], vec![17, 18, 19, 20]],
            vec![vec![2, 3, 11, 12], vec![21, 22, 23, 24]],
            vec![vec![4, 5, 6, 7], vec![13, 14, 15, 16]],
        ];

        let output = merge_i32_streams(streams, 4, None).await;

        assert_eq!(flatten_vec(output), (1..=24).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_merge_equal_values_across_batch_and_stream_boundaries() {
        // the value 3 ends stream 0's first batch, starts its second
        // one, and appears in every other stream as well
        let streams = vec![
            vec![vec![1, 2, 3], vec![3, 3, 4]],
            vec![vec![3, 4, 5], vec![5, 6, 7]],
            vec![vec![3, 3, 3], vec![7, 7, 7]],
        ];

        let output = merge_i32_streams(streams, 3, None).await;

        assert_eq!(
            flatten_vec(output),
            vec![1, 2, 3, 3, 3, 3, 3, 3, 3, 4, 4, 5, 5, 6, 7, 7, 7, 7]
        );
    }

    #[tokio::test]
    async fn test_merge_with_nulls() {
        // nulls sort first; stream 0's first batch is all nulls, so its last
        // row (null) ties with the null heads of the other streams, and nulls
        // also compare against non-null values on every batch boundary
        let streams = vec![
            vec![vec![None, None], vec![Some(1), Some(2)]],
            vec![vec![None, Some(3)], vec![Some(4), Some(5)]],
            vec![vec![None, Some(6)], vec![Some(7), Some(8)]],
        ];

        let output = merge_nullable_i32_streams(streams, 4, None).await;

        assert_eq!(
            flatten_vec(output),
            vec![
                None,
                None,
                None,
                None,
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                Some(8)
            ]
        );
    }

    #[tokio::test]
    async fn test_merge_streams_of_different_lengths() {
        // stream 1 ends first, then stream 2, stream 0 finishes alone
        let streams = vec![
            vec![
                vec![1, 2],
                vec![3, 4],
                vec![5, 6],
                vec![7, 8],
                vec![20, 21, 22],
            ],
            vec![vec![0, 9]],
            vec![vec![4, 5], vec![10, 11, 12]],
        ];

        let output = merge_i32_streams(streams, 4, None).await;

        assert_eq!(
            flatten_vec(output),
            vec![0, 1, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 11, 12, 20, 21, 22]
        );
    }

    #[tokio::test]
    async fn test_merge_batch_size_smaller_than_input_batches() {
        let streams = vec![
            vec![vec![1, 4, 7, 10], vec![13, 14, 15, 16]],
            vec![vec![2, 5, 8, 11]],
            vec![vec![3, 6, 9, 12]],
        ];

        let output = merge_i32_streams(streams, 2, None).await;

        assert_eq!(flatten_vec(output), (1..=16).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_merge_batch_size_larger_than_entire_input() {
        let streams = vec![
            vec![vec![1, 4, 7, 10], vec![13, 14, 15, 16]],
            vec![vec![2, 5, 8, 11]],
            vec![vec![3, 6, 9, 12]],
        ];

        let output = merge_i32_streams(streams, 8192, None).await;

        assert_eq!(flatten_vec(output), (1..=16).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_merge_fetch_smaller_than_input() {
        let streams = vec![
            vec![vec![1, 4, 7], vec![10, 11, 12]],
            vec![vec![2, 5, 8]],
            vec![vec![3, 6, 9]],
        ];

        let output = merge_i32_streams(streams, 4, Some(5)).await;

        assert_eq!(flatten_vec(output), vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_merge_fetch_larger_than_input() {
        let streams = vec![
            vec![vec![1, 4, 7], vec![10, 11, 12]],
            vec![vec![2, 5, 8]],
            vec![vec![3, 6, 9]],
        ];

        let output = merge_i32_streams(streams, 4, Some(100)).await;

        assert_eq!(flatten_vec(output), (1..=12).collect::<Vec<_>>());
    }
}
