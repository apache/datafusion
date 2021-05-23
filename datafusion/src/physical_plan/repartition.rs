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

//! The repartition operator maps N input partitions to M output partitions based on a
//! partitioning scheme.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use std::{any::Any, vec};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, SQLMetric};
use arrow::record_batch::RecordBatch;
use arrow::{array::Array, error::Result as ArrowResult};
use arrow::{compute::take, datatypes::SchemaRef};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{hash_join::create_hashes, RecordBatchStream, SendableRecordBatchStream};
use async_trait::async_trait;

use futures::stream::Stream;
use futures::StreamExt;
use hashbrown::HashMap;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    Mutex,
};
use tokio::task::JoinHandle;

type MaybeBatch = Option<ArrowResult<RecordBatch>>;

/// The repartition operator maps N input partitions to M output partitions based on a
/// partitioning scheme. No guarantees are made about the order of the resulting partitions.
#[derive(Debug)]
pub struct RepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Partitioning scheme to use
    partitioning: Partitioning,
    /// Channels for sending batches from input partitions to output partitions.
    /// Key is the partition number
    channels: Arc<
        Mutex<
            HashMap<usize, (UnboundedSender<MaybeBatch>, UnboundedReceiver<MaybeBatch>)>,
        >,
    >,
    /// Time in nanos to execute child operator and fetch batches
    fetch_time_nanos: Arc<SQLMetric>,
    /// Time in nanos to perform repartitioning
    repart_time_nanos: Arc<SQLMetric>,
    /// Time in nanos for sending resulting batches to channels
    send_time_nanos: Arc<SQLMetric>,
}

impl RepartitionExec {
    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Partitioning scheme to use
    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }
}

#[async_trait]
impl ExecutionPlan for RepartitionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(RepartitionExec::try_new(
                children[0].clone(),
                self.partitioning.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "RepartitionExec wrong number of children".to_string(),
            )),
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // lock mutexes
        let mut channels = self.channels.lock().await;

        let num_input_partitions = self.input.output_partitioning().partition_count();
        let num_output_partitions = self.partitioning.partition_count();

        // if this is the first partition to be invoked then we need to set up initial state
        if channels.is_empty() {
            // create one channel per *output* partition
            for partition in 0..num_output_partitions {
                // Note that this operator uses unbounded channels to avoid deadlocks because
                // the output partitions can be read in any order and this could cause input
                // partitions to be blocked when sending data to output UnboundedReceivers that are not
                // being read yet. This may cause high memory usage if the next operator is
                // reading output partitions in order rather than concurrently. One workaround
                // for this would be to add spill-to-disk capabilities.
                let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<
                    Option<ArrowResult<RecordBatch>>,
                >();
                channels.insert(partition, (sender, receiver));
            }
            // Use fixed random state
            let random = ahash::RandomState::with_seeds(0, 0, 0, 0);

            // launch one async task per *input* partition
            for i in 0..num_input_partitions {
                let random_state = random.clone();
                let input = self.input.clone();
                let fetch_time = self.fetch_time_nanos.clone();
                let repart_time = self.repart_time_nanos.clone();
                let send_time = self.send_time_nanos.clone();
                let mut txs: HashMap<_, _> = channels
                    .iter()
                    .map(|(partition, (tx, _rx))| (*partition, tx.clone()))
                    .collect();
                let partitioning = self.partitioning.clone();
                let _: JoinHandle<Result<()>> = tokio::spawn(async move {
                    // execute the child operator
                    let now = Instant::now();
                    let mut stream = input.execute(i).await?;
                    fetch_time.add(now.elapsed().as_nanos() as usize);

                    let mut counter = 0;
                    let hashes_buf = &mut vec![];

                    loop {
                        // fetch the next batch
                        let now = Instant::now();
                        let result = stream.next().await;
                        fetch_time.add(now.elapsed().as_nanos() as usize);

                        if result.is_none() {
                            break;
                        }
                        let result = result.unwrap();

                        match &partitioning {
                            Partitioning::RoundRobinBatch(_) => {
                                let now = Instant::now();
                                let output_partition = counter % num_output_partitions;
                                let tx = txs.get_mut(&output_partition).unwrap();
                                tx.send(Some(result)).map_err(|e| {
                                    DataFusionError::Execution(e.to_string())
                                })?;
                                send_time.add(now.elapsed().as_nanos() as usize);
                            }
                            Partitioning::Hash(exprs, _) => {
                                let now = Instant::now();
                                let input_batch = result?;
                                let arrays = exprs
                                    .iter()
                                    .map(|expr| {
                                        Ok(expr
                                            .evaluate(&input_batch)?
                                            .into_array(input_batch.num_rows()))
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                hashes_buf.clear();
                                hashes_buf.resize(arrays[0].len(), 0);
                                // Hash arrays and compute buckets based on number of partitions
                                let hashes =
                                    create_hashes(&arrays, &random_state, hashes_buf)?;
                                let mut indices = vec![vec![]; num_output_partitions];
                                for (index, hash) in hashes.iter().enumerate() {
                                    indices
                                        [(*hash % num_output_partitions as u64) as usize]
                                        .push(index as u64)
                                }
                                repart_time.add(now.elapsed().as_nanos() as usize);
                                for (num_output_partition, partition_indices) in
                                    indices.into_iter().enumerate()
                                {
                                    let now = Instant::now();
                                    let indices = partition_indices.into();
                                    // Produce batches based on indices
                                    let columns = input_batch
                                        .columns()
                                        .iter()
                                        .map(|c| {
                                            take(c.as_ref(), &indices, None).map_err(
                                                |e| {
                                                    DataFusionError::Execution(
                                                        e.to_string(),
                                                    )
                                                },
                                            )
                                        })
                                        .collect::<Result<Vec<Arc<dyn Array>>>>()?;
                                    let output_batch = RecordBatch::try_new(
                                        input_batch.schema(),
                                        columns,
                                    );
                                    repart_time.add(now.elapsed().as_nanos() as usize);
                                    let now = Instant::now();
                                    let tx = txs.get_mut(&num_output_partition).unwrap();
                                    tx.send(Some(output_batch)).map_err(|e| {
                                        DataFusionError::Execution(e.to_string())
                                    })?;
                                    send_time.add(now.elapsed().as_nanos() as usize);
                                }
                            }
                            other => {
                                // this should be unreachable as long as the validation logic
                                // in the constructor is kept up-to-date
                                return Err(DataFusionError::NotImplemented(format!(
                                    "Unsupported repartitioning scheme {:?}",
                                    other
                                )));
                            }
                        }
                        counter += 1;
                    }

                    // notify each output partition that this input partition has no more data
                    for (_, tx) in txs {
                        tx.send(None)
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    }
                    Ok(())
                });
            }
        }

        // now return stream for the specified *output* partition which will
        // read from the channel
        Ok(Box::pin(RepartitionStream {
            num_input_partitions,
            num_input_partitions_processed: 0,
            schema: self.input.schema(),
            input: UnboundedReceiverStream::new(channels.remove(&partition).unwrap().1),
        }))
    }

    fn metrics(&self) -> HashMap<String, SQLMetric> {
        let mut metrics = HashMap::new();
        metrics.insert("fetchTime".to_owned(), (*self.fetch_time_nanos).clone());
        metrics.insert(
            "repartitionTime".to_owned(),
            (*self.repart_time_nanos).clone(),
        );
        metrics.insert("sendTime".to_owned(), (*self.send_time_nanos).clone());
        metrics
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "RepartitionExec: partitioning={:?}", self.partitioning)
            }
        }
    }
}

impl RepartitionExec {
    /// Create a new RepartitionExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Self> {
        Ok(RepartitionExec {
            input,
            partitioning,
            channels: Arc::new(Mutex::new(HashMap::new())),
            fetch_time_nanos: SQLMetric::time_nanos(),
            repart_time_nanos: SQLMetric::time_nanos(),
            send_time_nanos: SQLMetric::time_nanos(),
        })
    }
}

struct RepartitionStream {
    /// Number of input partitions that will be sending batches to this output channel
    num_input_partitions: usize,
    /// Number of input partitions that have finished sending batches to this output channel
    num_input_partitions_processed: usize,
    /// Schema
    schema: SchemaRef,
    /// channel containing the repartitioned batches
    input: UnboundedReceiverStream<Option<ArrowResult<RecordBatch>>>,
}

impl Stream for RepartitionStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Some(v))) => Poll::Ready(Some(v)),
            Poll::Ready(Some(None)) => {
                self.num_input_partitions_processed += 1;
                if self.num_input_partitions == self.num_input_partitions_processed {
                    // all input partitions have finished sending batches
                    Poll::Ready(None)
                } else {
                    // other partitions still have data to send
                    self.poll_next(cx)
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for RepartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::memory::MemoryExec;
    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn one_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition];

        // repartition from 1 input to 4 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(4)).await?;

        assert_eq!(4, output_partitions.len());
        assert_eq!(13, output_partitions[0].len());
        assert_eq!(13, output_partitions[1].len());
        assert_eq!(12, output_partitions[2].len());
        assert_eq!(12, output_partitions[3].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_one_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 1 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(1)).await?;

        assert_eq!(1, output_partitions.len());
        assert_eq!(150, output_partitions[0].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 5 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(5)).await?;

        assert_eq!(5, output_partitions.len());
        assert_eq!(30, output_partitions[0].len());
        assert_eq!(30, output_partitions[1].len());
        assert_eq!(30, output_partitions[2].len());
        assert_eq!(30, output_partitions[3].len());
        assert_eq!(30, output_partitions[4].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_hash_partition() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        let output_partitions = repartition(
            &schema,
            partitions,
            Partitioning::Hash(
                vec![Arc::new(crate::physical_plan::expressions::Column::new(
                    &"c0",
                ))],
                8,
            ),
        )
        .await?;

        let total_rows: usize = output_partitions.iter().map(|x| x.len()).sum();

        assert_eq!(8, output_partitions.len());
        assert_eq!(total_rows, 8 * 50 * 3);

        Ok(())
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    fn create_vec_batches(schema: &Arc<Schema>, n: usize) -> Vec<RecordBatch> {
        let batch = create_batch(schema);
        let mut vec = Vec::with_capacity(n);
        for _ in 0..n {
            vec.push(batch.clone());
        }
        vec
    }

    fn create_batch(schema: &Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap()
    }

    async fn repartition(
        schema: &SchemaRef,
        input_partitions: Vec<Vec<RecordBatch>>,
        partitioning: Partitioning,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        // create physical plan
        let exec = MemoryExec::try_new(&input_partitions, schema.clone(), None)?;
        let exec = RepartitionExec::try_new(Arc::new(exec), partitioning)?;

        // execute and collect results
        let mut output_partitions = vec![];
        for i in 0..exec.partitioning.partition_count() {
            // execute this *output* partition and collect all batches
            let mut stream = exec.execute(i).await?;
            let mut batches = vec![];
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }
            output_partitions.push(batches);
        }
        Ok(output_partitions)
    }

    #[tokio::test]
    async fn many_to_many_round_robin_within_tokio_task() -> Result<()> {
        let join_handle: JoinHandle<Result<Vec<Vec<RecordBatch>>>> =
            tokio::spawn(async move {
                // define input partitions
                let schema = test_schema();
                let partition = create_vec_batches(&schema, 50);
                let partitions =
                    vec![partition.clone(), partition.clone(), partition.clone()];

                // repartition from 3 input to 5 output
                repartition(&schema, partitions, Partitioning::RoundRobinBatch(5)).await
            });

        let output_partitions = join_handle
            .await
            .map_err(|e| DataFusionError::Internal(e.to_string()))??;

        assert_eq!(5, output_partitions.len());
        assert_eq!(30, output_partitions[0].len());
        assert_eq!(30, output_partitions[1].len());
        assert_eq!(30, output_partitions[2].len());
        assert_eq!(30, output_partitions[3].len());
        assert_eq!(30, output_partitions[4].len());

        Ok(())
    }
}
