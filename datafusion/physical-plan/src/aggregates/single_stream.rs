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

//! Single-stage hash aggregation stream implementation.
//!
//! This stream is part of the incremental migration from
//! [`crate::aggregates::grouped_hash_stream::GroupedHashAggregateStream`].
//!
//! See issue for details: <https://github.com/apache/datafusion/issues/22710>

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, async_try_stream};
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{AggregateHashTable, SingleMarker};
use crate::metrics::BaselineMetrics;
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream};

/// Hash aggregation can run the full logical aggregation in one operator. This
/// stream implements the single stage for grouped hash aggregation.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=single)
///
/// ## Single Stage Behavior
/// Input: raw rows
/// Output: final aggregate values for all groups (for example, `AVG(x)`)
///
/// This stream implements the complete aggregation without a partial/final
/// split. It consumes raw input rows and emits final aggregate values.
pub(crate) struct SingleHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// Hash table containing group keys and accumulators.
    hash_table: AggregateHashTable<SingleMarker>,
}

impl SingleHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            super::AggregateMode::Single | super::AggregateMode::SinglePartitioned
        ));
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<SingleMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("SingleHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            hash_table,
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let baseline_metrics = self.baseline_metrics.clone();
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, self.create_stream()));

        Box::pin(ObservedStream::new(stream, baseline_metrics, None))
    }

    /// State transitions are implemented using the generator pattern; see the
    /// comments in [`async_try_stream`].
    ///
    /// Conceptually: ReadingInput -> ProducingOutput -> Done.
    fn create_stream(self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|mut emitter| async move {
            let Self {
                mut input,
                baseline_metrics,
                reservation,
                mut hash_table,
                ..
            } = self;
            let elapsed_compute = baseline_metrics.elapsed_compute().clone();

            debug_assert!(hash_table.is_building());
            while let Some(batch) = input.next().await.transpose()? {
                {
                    let _timer = elapsed_compute.timer();
                    hash_table.aggregate_batch(&batch)?;
                }
                reservation.try_resize(hash_table.memory_size())?;
            }

            {
                let _timer = elapsed_compute.timer();

                // Input is exhausted. Release the upstream pipeline before draining output.
                drop(input);
                hash_table.start_output()?;
            }

            debug_assert!(!hash_table.is_building());
            loop {
                let next_batch = {
                    let _timer = elapsed_compute.timer();
                    hash_table.next_output_batch()?
                };
                let Some(batch) = next_batch else {
                    return Ok(());
                };

                reservation.try_resize(hash_table.memory_size())?;
                debug_assert!(batch.num_rows() > 0);

                if hash_table.is_done() {
                    drop(hash_table);
                    reservation.free();
                    emitter.emit(batch).await;
                    return Ok(());
                }

                emitter.emit(batch).await;
            }
        })
    }
}
