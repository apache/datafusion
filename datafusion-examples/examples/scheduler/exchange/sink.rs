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

//! `ExchangeSinkExec`: the producer side of a streaming in-memory exchange.
//! Runs one input partition (task), hash-partitions its rows into `P` output
//! buckets, IPC-encodes each bucket batch into a frame, and pushes it into the
//! corresponding channel of the shared [`InMemoryExchange`]. Yields zero data
//! batches — it is a pure side-effecting sink, the streaming replacement for
//! `ShuffleWriterExec`.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use tokio::sync::mpsc::Sender;

use super::{Frame, InMemoryExchange};

/// Producer operator for a shuffle edge. For task `t` it takes the `P` senders
/// `(stage_id, t, d)` for `d in 0..P`, runs input partition `t`, routes each
/// batch through a [`BatchPartitioner`] into `P` buckets, IPC-encodes each
/// bucket batch to a frame, and awaits `send` on the bucket's channel (the
/// await is the backpressure point). When the input ends it drops all senders,
/// signalling end-of-stream to the consumers.
///
/// This operator's own output partitioning mirrors its *input's* partitioning
/// (task count follows the input); `output_partitioning` only controls how many
/// buckets each task fans out into.
pub struct ExchangeSinkExec {
    stage_id: usize,
    input: Arc<dyn ExecutionPlan>,
    output_partitioning: Partitioning,
    exchange: Arc<InMemoryExchange>,
    properties: Arc<PlanProperties>,
}

impl ExchangeSinkExec {
    pub fn try_new(
        stage_id: usize,
        input: Arc<dyn ExecutionPlan>,
        output_partitioning: Partitioning,
        exchange: Arc<InMemoryExchange>,
    ) -> Result<Self> {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            stage_id,
            input,
            output_partitioning,
            exchange,
            properties,
        })
    }

    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    #[cfg(test)]
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn output_partitioning_spec(&self) -> &Partitioning {
        &self.output_partitioning
    }
}

impl fmt::Debug for ExchangeSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ExchangeSinkExec")
            .field("stage_id", &self.stage_id)
            .field("output_partitioning", &self.output_partitioning)
            .finish()
    }
}

impl DisplayAs for ExchangeSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ExchangeSinkExec: stage={}, output_partitioning={:?}",
                    self.stage_id, self.output_partitioning
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "stage={}", self.stage_id)
            }
        }
    }
}

impl ExecutionPlan for ExchangeSinkExec {
    fn name(&self) -> &'static str {
        "ExchangeSinkExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ExchangeSinkExec::try_new(
            self.stage_id,
            children.swap_remove(0),
            self.output_partitioning.clone(),
            Arc::clone(&self.exchange),
        )?))
    }

    /// Streams input partition `partition` into the exchange, then completes
    /// with zero data batches.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let num_output_partitions = self.output_partitioning.partition_count();

        // Take this task's `P` senders up front; a missing/taken sender is a
        // wiring bug and fails here, before any input is polled.
        let mut senders = Vec::with_capacity(num_output_partitions);
        for d in 0..num_output_partitions {
            senders.push(self.exchange.take_sender(self.stage_id, partition, d)?);
        }

        let input_stream = self.input.execute(partition, context)?;
        let schema = self.input.schema();
        let num_input_partitions = self.input.output_partitioning().partition_count();

        let fut = run_sink(
            input_stream,
            schema.clone(),
            self.output_partitioning.clone(),
            senders,
            partition,
            num_input_partitions,
        );

        let stream = futures::stream::once(fut).filter_map(|res| async move {
            match res {
                Ok(()) => None,
                Err(e) => Some(Err(e)),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Drains `input`, hash-partitioning each batch into buckets, IPC-encoding
/// each bucket batch to a frame and awaiting its send on the bucket's channel.
/// Returns after the input ends; dropping `senders` on return closes the
/// channels, signalling end-of-stream to the consumers.
async fn run_sink(
    mut input: SendableRecordBatchStream,
    schema: SchemaRef,
    partitioning: Partitioning,
    senders: Vec<Sender<Frame>>,
    task_id: usize,
    num_input_partitions: usize,
) -> Result<()> {
    let metrics_set = ExecutionPlanMetricsSet::new();
    let timer = MetricBuilder::new(&metrics_set).subset_time("repartition_time", task_id);
    let mut partitioner =
        BatchPartitioner::try_new(partitioning, timer, task_id, num_input_partitions)?;

    while let Some(batch) = input.next().await {
        // A genuine error from this sink's own input stream still propagates
        // as a query error.
        let batch = batch?;
        for res in partitioner.partition_iter(batch)? {
            let (bucket, part_batch) = res?;
            let frame = encode_frame(&part_batch, &schema)?;
            // `Sender::send` errs ONLY when the receiver is gone: the consumer
            // hung up (e.g. a LIMIT upstream stopped pulling, or the query is
            // tearing down). That is not a query error — it is a graceful stop
            // of this sink. Stop sending and end cleanly; dropping `senders`
            // closes the remaining channels for the other consumers.
            if senders[bucket].send(frame).await.is_err() {
                return Ok(());
            }
        }
    }

    // Explicit for clarity: closing every sender signals end-of-stream.
    drop(senders);
    Ok(())
}

/// IPC-encodes a single [`RecordBatch`] into one self-contained frame using an
/// Arrow `StreamWriter` (schema + one batch + end marker) written into a
/// `Vec<u8>`.
fn encode_frame(batch: &RecordBatch, schema: &SchemaRef) -> Result<Frame> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}
