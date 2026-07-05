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
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::metrics::ExecutionPlanMetricsSet;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sort_preserving_merge_stream_with_one_stream_larger_than_other() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sort_key",
            DataType::Int32,
            false,
        )]));

        let data_left = vec![
            Int32Array::from(vec![1, 3, 5, 7, 9]),
            Int32Array::from(vec![11, 13, 15, 17, 19]),
            Int32Array::from(vec![21, 23, 25, 27, 29]),
            Int32Array::from(vec![31, 33, 35, 37, 39]),
            Int32Array::from(vec![41, 43, 45, 47, 49]),
        ];

        let data_right = vec![
            Int32Array::from(vec![0, 2, 4, 6, 8]),
            Int32Array::from(vec![9, 10, 11, 12, 13]),
        ];

        for fetch in [
            None,
            Some(2),
            Some(10),
            Some(13),
            Some(17),
            Some(20),
            Some(25),
            Some(30),
        ]
        .iter()
        {
            let data_left_stream = MemoryStream::try_new(
                data_left
                    .iter()
                    .map(|a| {
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(a.clone())],
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>(),
                Arc::clone(&schema),
                None,
            )
            .unwrap();

            let data_right_stream = MemoryStream::try_new(
                data_right
                    .iter()
                    .map(|a| {
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(a.clone())],
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>(),
                Arc::clone(&schema),
                None,
            )
            .unwrap();

            let merged_sort = StreamingMergeBuilder::new()
                .with_streams(vec![
                    Box::pin(data_left_stream),
                    Box::pin(data_right_stream),
                ])
                .with_batch_size(5)
                .with_schema(Arc::clone(&schema))
                .with_reservation({
                    let mem_pool: Arc<dyn MemoryPool> =
                        Arc::new(UnboundedMemoryPool::default());

                    MemoryConsumer::new("merge stream mock memory").register(&mem_pool)
                })
                .with_expressions(
                    &([
                        PhysicalSortExpr::new_default(col("sort_key", &schema).unwrap())
                            .asc(),
                    ]
                    .into()),
                )
                .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
                .with_fetch(*fetch)
                .build()
                .unwrap();

            let output = collect(merged_sort).await.unwrap();
            let sorted_output = output
                .into_iter()
                .flat_map(|b| b.column(0).as_primitive::<Int32Type>().values().to_vec())
                .collect::<Vec<_>>();

            let expected = vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 10, 11, 11, 12, 13, 13, 15, 17, 19, 21,
                23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49,
            ];

            assert_eq!(sorted_output, &expected[0..fetch.unwrap_or(expected.len())]);
        }
    }
}
