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

//! Streaming in-memory exchange: an `Arc`-shared registry of bounded
//! single-producer / single-consumer channels that carry Arrow-IPC-encoded
//! batch frames between producer and consumer stages, with no disk and no
//! barrier. This is the in-process analogue of a network shuffle transport.
//!
//! A shuffle edge from a producer stage with `T` tasks to a consumer stage
//! that reads `P` output partitions is a grid of `T × P` channels, each keyed
//! `(producer_stage_id, producer_task, consumer_partition)`. The producer
//! task `t` owns the `Sender` of channel `(s, t, d)` for every output bucket
//! `d`; the consumer partition `d` owns the `Receiver` of channel `(s, t, d)`
//! for every producer task `t`.

pub mod codec;
pub mod sink;
pub mod source;

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use datafusion::error::{DataFusionError, Result};
use tokio::sync::mpsc::{Receiver, Sender, channel};

pub use codec::ExchangeCodec;
pub use sink::ExchangeSinkExec;
pub use source::ExchangeSourceExec;

/// Key identifying a single channel: `(producer_stage_id, producer_task,
/// consumer_partition)`.
type ChannelKey = (usize, usize, usize);

/// One IPC frame per message (`Vec<u8>` = one IPC-encoded [`RecordBatch`]).
///
/// [`RecordBatch`]: datafusion::arrow::record_batch::RecordBatch
pub type Frame = Vec<u8>;

/// A single SPSC channel's endpoints, either of which can be moved out exactly
/// once via [`InMemoryExchange::take_sender`] / [`InMemoryExchange::take_receiver`].
struct ChannelSlot {
    sender: Option<Sender<Frame>>,
    receiver: Option<Receiver<Frame>>,
}

struct Inner {
    channels: HashMap<ChannelKey, ChannelSlot>,
    registered_stages: HashSet<usize>,
}

/// A registry of bounded SPSC channels — "the wire". Shared as an
/// `Arc<InMemoryExchange>` by every task of every stage; it is the one piece
/// of state tasks share (like a Flight endpoint), and it is never serialized
/// into a plan.
pub struct InMemoryExchange {
    inner: Mutex<Inner>,
}

impl InMemoryExchange {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner: Mutex::new(Inner {
                channels: HashMap::new(),
                registered_stages: HashSet::new(),
            }),
        })
    }

    /// Pre-create the `num_tasks × num_partitions` channels for one shuffle
    /// edge, each bounded to `capacity` frames. Idempotent per `stage_id`: a
    /// re-registration of an already-registered stage is a no-op (the existing
    /// channels are left untouched).
    pub fn register_stage(
        &self,
        stage_id: usize,
        num_tasks: usize,
        num_partitions: usize,
        capacity: usize,
    ) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.registered_stages.insert(stage_id) {
            // Already registered — no-op.
            return;
        }
        for task in 0..num_tasks {
            for partition in 0..num_partitions {
                let (tx, rx) = channel(capacity);
                inner.channels.insert(
                    (stage_id, task, partition),
                    ChannelSlot {
                        sender: Some(tx),
                        receiver: Some(rx),
                    },
                );
            }
        }
    }

    /// Producer side: move out the `Sender` for `(stage_id, task, partition)`.
    /// Errors if the channel was never registered or its sender was already
    /// taken.
    pub fn take_sender(
        &self,
        stage_id: usize,
        task: usize,
        partition: usize,
    ) -> Result<Sender<Frame>> {
        let mut inner = self.inner.lock().unwrap();
        let slot = inner.channels.get_mut(&(stage_id, task, partition)).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "exchange: no channel registered for sender (stage={stage_id}, task={task}, partition={partition})"
            ))
        })?;
        slot.sender.take().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "exchange: sender already taken for (stage={stage_id}, task={task}, partition={partition})"
            ))
        })
    }

    /// Consumer side: move out the `Receiver` for `(stage_id, task,
    /// partition)`. Errors if the channel was never registered or its receiver
    /// was already taken.
    pub fn take_receiver(
        &self,
        stage_id: usize,
        task: usize,
        partition: usize,
    ) -> Result<Receiver<Frame>> {
        let mut inner = self.inner.lock().unwrap();
        let slot = inner.channels.get_mut(&(stage_id, task, partition)).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "exchange: no channel registered for receiver (stage={stage_id}, task={task}, partition={partition})"
            ))
        })?;
        slot.receiver.take().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "exchange: receiver already taken for (stage={stage_id}, task={task}, partition={partition})"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::runtime::SpawnedTask;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::test::TestMemoryExec;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    use super::InMemoryExchange;
    use super::sink::ExchangeSinkExec;
    use super::source::ExchangeSourceExec;

    fn int32_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    /// Drives a sink stream to completion, returning the number of data
    /// batches it yielded (must be 0).
    async fn drain_sink(
        mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> usize {
        let mut n = 0;
        while let Some(res) = stream.next().await {
            res.unwrap();
            n += 1;
        }
        n
    }

    /// Drives a source stream to completion, returning all Int32 values read.
    async fn collect_source(
        mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> Vec<i32> {
        let mut values = Vec::new();
        while let Some(res) = stream.next().await {
            let batch = res.unwrap();
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            values.extend(col.iter().flatten());
        }
        values
    }

    /// Real streaming round-trip: 2 producer tasks hash-partition into 3
    /// buckets; 3 consumer partitions merge across both producers. Sink and
    /// source tasks run CONCURRENTLY — the source blocks on its channels until
    /// the sink feeds them, so both must be spawned together.
    #[tokio::test]
    async fn streaming_round_trip_across_concurrent_sink_and_source() {
        let exchange = InMemoryExchange::new();
        let stage_id = 9;
        let num_tasks = 2;
        let num_partitions = 3;
        exchange.register_stage(stage_id, num_tasks, num_partitions, 4);

        let schema = int32_schema();
        let batch0 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0, 1, 2]))],
        )
        .unwrap();
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![3, 4, 5]))],
        )
        .unwrap();
        let input = TestMemoryExec::try_new_exec(
            &[vec![batch0], vec![batch1]],
            schema.clone(),
            None,
        )
        .unwrap();

        let partitioning =
            Partitioning::Hash(vec![col("a", &schema).unwrap()], num_partitions);
        let sink = Arc::new(
            ExchangeSinkExec::try_new(stage_id, input, partitioning, exchange.clone())
                .unwrap(),
        );
        let source = Arc::new(
            ExchangeSourceExec::try_new(
                stage_id,
                schema.clone(),
                num_tasks,
                num_partitions,
                exchange.clone(),
            )
            .unwrap(),
        );

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        // Spawn ALL tasks (sinks + sources) before joining any: a sink blocks
        // on `send().await` until a source drains, so joining sinks first
        // would deadlock.
        let mut sink_handles = Vec::new();
        for t in 0..num_tasks {
            let stream = sink.execute(t, task_ctx.clone()).unwrap();
            sink_handles.push(SpawnedTask::spawn(drain_sink(stream)));
        }
        let mut source_handles = Vec::new();
        for d in 0..num_partitions {
            let stream = source.execute(d, task_ctx.clone()).unwrap();
            source_handles.push(SpawnedTask::spawn(collect_source(stream)));
        }

        // Sinks must yield zero data batches.
        for h in sink_handles {
            let n = h.join().await.unwrap();
            assert_eq!(n, 0, "ExchangeSinkExec must yield zero batches");
        }

        // Collect per-partition outputs and check hash-partition consistency:
        // every value must land in the bucket its hash selects.
        let mut partitioner =
            datafusion::physical_plan::repartition::BatchPartitioner::try_new(
                Partitioning::Hash(vec![col("a", &schema).unwrap()], num_partitions),
                datafusion::physical_plan::metrics::MetricBuilder::new(
                    &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new(),
                )
                .subset_time("t", 0),
                0,
                1,
            )
            .unwrap();

        let mut all_values: Vec<i32> = Vec::new();
        for (d, h) in source_handles.into_iter().enumerate() {
            let values = h.join().await.unwrap();
            for v in &values {
                let one = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![*v]))],
                )
                .unwrap();
                let mut bucket = None;
                for res in partitioner.partition_iter(one).unwrap() {
                    let (b, batch) = res.unwrap();
                    if batch.num_rows() > 0 {
                        bucket = Some(b);
                    }
                }
                assert_eq!(
                    bucket,
                    Some(d),
                    "value {v} arrived at partition {d} but hashes to bucket {bucket:?}"
                );
            }
            all_values.extend(values);
        }

        assert_eq!(all_values.len(), 6, "expected all 6 input rows");
        let got: HashSet<i32> = all_values.into_iter().collect();
        let expected: HashSet<i32> = [0, 1, 2, 3, 4, 5].into_iter().collect();
        assert_eq!(got, expected);
    }

    /// Tight-backpressure test: capacity=1 with many rows, so producers block
    /// on full channels while consumers drain. Must complete (not deadlock);
    /// a `timeout` turns a hang into a failure instead of hanging CI.
    #[tokio::test]
    async fn tight_backpressure_does_not_deadlock() {
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            let exchange = InMemoryExchange::new();
            let stage_id = 3;
            let num_tasks = 2;
            let num_partitions = 4;
            // Capacity 1: the tightest bound that still makes progress.
            exchange.register_stage(stage_id, num_tasks, num_partitions, 1);

            let schema = int32_schema();
            // 200 rows per task, spread across many small batches so the sink
            // blocks on `send().await` repeatedly.
            let mut task0 = Vec::new();
            let mut task1 = Vec::new();
            for i in 0..100i32 {
                task0.push(
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int32Array::from(vec![i, i + 1000]))],
                    )
                    .unwrap(),
                );
                task1.push(
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int32Array::from(vec![i + 100_000, i + 200_000]))],
                    )
                    .unwrap(),
                );
            }
            let input =
                TestMemoryExec::try_new_exec(&[task0, task1], schema.clone(), None)
                    .unwrap();

            let partitioning =
                Partitioning::Hash(vec![col("a", &schema).unwrap()], num_partitions);
            let sink = Arc::new(
                ExchangeSinkExec::try_new(
                    stage_id,
                    input,
                    partitioning,
                    exchange.clone(),
                )
                .unwrap(),
            );
            let source = Arc::new(
                ExchangeSourceExec::try_new(
                    stage_id,
                    schema.clone(),
                    num_tasks,
                    num_partitions,
                    exchange.clone(),
                )
                .unwrap(),
            );

            let ctx = SessionContext::new();
            let task_ctx = ctx.task_ctx();

            let mut sink_handles = Vec::new();
            for t in 0..num_tasks {
                let stream = sink.execute(t, task_ctx.clone()).unwrap();
                sink_handles.push(SpawnedTask::spawn(drain_sink(stream)));
            }
            let mut source_handles = Vec::new();
            for d in 0..num_partitions {
                let stream = source.execute(d, task_ctx.clone()).unwrap();
                source_handles.push(SpawnedTask::spawn(collect_source(stream)));
            }

            for h in sink_handles {
                assert_eq!(h.join().await.unwrap(), 0);
            }
            let mut total = 0usize;
            for h in source_handles {
                total += h.join().await.unwrap().len();
            }
            total
        })
        .await;

        let total = result.expect("backpressure round-trip deadlocked / timed out");
        // 2 tasks × 200 rows each.
        assert_eq!(total, 400, "expected all 400 rows to round-trip");
    }
}
