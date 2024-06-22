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

use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use datafusion_common::stats::Precision;
use std::any::Any;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::pin::Pin;
use std::result;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::PartitionLocation;
use crate::AbortOnDropMany;
use crate::PartitionStats;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::PlanProperties;
use datafusion_physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use log::{error, info};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

/// ShuffleReaderExec reads partitions that have already been materialized by a ShuffleWriterExec
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// The query stage id to read from
    pub stage_id: usize,
    pub(crate) schema: SchemaRef,
    /// Each partition of a shuffle can read data from multiple locations
    pub partition: Vec<Vec<PartitionLocation>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl ShuffleReaderExec {
    /// Create a new ShuffleReaderExec
    pub fn try_new(
        stage_id: usize,
        partition: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            stage_id,
            schema,
            partition,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for ShuffleReaderExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ShuffleReaderExec: partitions={}", self.partition.len())
            }
        }
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        todo!()
    }

    // fn output_partitioning(&self) -> Partitioning {
    //     // TODO partitioning may be known and could be populated here
    //     // see https://github.com/apache/arrow-datafusion/issues/758
    //     Partitioning::UnknownPartitioning(self.partition.len())
    // }
    //
    // fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
    //     None
    // }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ShuffleReaderExec::try_new(
            self.stage_id,
            self.partition.clone(),
            self.schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_id = context.task_id().unwrap_or_else(|| partition.to_string());
        info!("ShuffleReaderExec::execute({})", task_id);

        // // TODO make the maximum size configurable, or make it depends on global memory control
        // let max_request_num = 50usize;
        // let mut partition_locations = HashMap::new();
        // for p in &self.partition[partition] {
        //     partition_locations
        //         .entry(p.executor_meta.id.clone())
        //         .or_insert_with(Vec::new)
        //         .push(p.clone());
        // }
        // // Sort partitions for evenly send fetching partition requests to avoid hot executors within one task
        // let mut partition_locations: Vec<PartitionLocation> = partition_locations
        //     .into_values()
        //     .flat_map(|ps| ps.into_iter().enumerate())
        //     .sorted_by(|(p1_idx, _), (p2_idx, _)| Ord::cmp(p1_idx, p2_idx))
        //     .map(|(_, p)| p)
        //     .collect();
        // // Shuffle partitions for evenly send fetching partition requests to avoid hot executors within multiple tasks
        // partition_locations.shuffle(&mut thread_rng());
        //
        // let response_receiver =
        //     send_fetch_partitions(partition_locations, max_request_num);
        //
        // let result = RecordBatchStreamAdapter::new(
        //     Arc::new(self.schema.as_ref().clone()),
        //     response_receiver.try_flatten(),
        // );
        // Ok(Box::pin(result))

        todo!()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(stats_for_partitions(
            self.schema.fields().len(),
            self.partition
                .iter()
                .flatten()
                .map(|loc| loc.partition_stats),
        ))
    }
}

fn stats_for_partitions(
    num_fields: usize,
    partition_stats: impl Iterator<Item = PartitionStats>,
) -> Statistics {
    // TODO stats: add column statistics to PartitionStats
    let (num_rows, total_byte_size) =
        partition_stats.fold((Some(0), Some(0)), |(num_rows, total_byte_size), part| {
            // if any statistic is unkown it makes the entire statistic unkown
            let num_rows = num_rows
                .zip(Some(part.num_rows))
                .map(|(a, b)| a + b as usize);
            let total_byte_size = total_byte_size
                .zip(Some(part.num_bytes))
                .map(|(a, b)| a + b as usize);
            (num_rows, total_byte_size)
        });

    Statistics {
        num_rows: num_rows.map(Precision::Exact).unwrap_or(Precision::Absent),
        total_byte_size: total_byte_size
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        column_statistics: vec![ColumnStatistics::new_unknown(); num_fields],
    }
}

struct LocalShuffleStream {
    reader: StreamReader<BufReader<File>>,
}

impl LocalShuffleStream {
    pub fn new(reader: StreamReader<BufReader<File>>) -> Self {
        LocalShuffleStream { reader }
    }
}

impl Stream for LocalShuffleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.reader.next() {
            return Poll::Ready(Some(batch.map_err(|e| e.into())));
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for LocalShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// Adapter for a tokio ReceiverStream that implements the SendableRecordBatchStream interface
struct AbortableReceiverStream {
    inner: ReceiverStream<result::Result<SendableRecordBatchStream, DataFusionError>>,

    #[allow(dead_code)]
    drop_helper: AbortOnDropMany<()>,
}

impl AbortableReceiverStream {
    /// Construct a new SendableRecordBatchReceiverStream which will send batches of the specified schema from inner
    pub fn create(
        rx: tokio::sync::mpsc::Receiver<
            result::Result<SendableRecordBatchStream, DataFusionError>,
        >,
        join_handles: Vec<JoinHandle<()>>,
    ) -> AbortableReceiverStream {
        let inner = ReceiverStream::new(rx);
        Self {
            inner,
            drop_helper: AbortOnDropMany(join_handles),
        }
    }
}

impl Stream for AbortableReceiverStream {
    type Item = result::Result<SendableRecordBatchStream, ArrowError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner
            .poll_next_unpin(cx)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
    }
}

fn send_fetch_partitions(
    partition_locations: Vec<PartitionLocation>,
    max_request_num: usize,
) -> AbortableReceiverStream {
    let (response_sender, response_receiver) = mpsc::channel(max_request_num);
    let semaphore = Arc::new(Semaphore::new(max_request_num));
    let mut join_handles = vec![];
    let (local_locations, remote_locations): (Vec<_>, Vec<_>) = partition_locations
        .into_iter()
        .partition(check_is_local_location);

    info!(
        "local shuffle file counts:{}, remote shuffle file count:{}.",
        local_locations.len(),
        remote_locations.len()
    );

    // keep local shuffle files reading in serial order for memory control.
    let response_sender_c = response_sender.clone();
    let join_handle = tokio::spawn(async move {
        for p in local_locations {
            let r = PartitionReaderEnum::Local.fetch_partition(&p).await;
            if let Err(e) = response_sender_c.send(r).await {
                error!("Fail to send response event to the channel due to {}", e);
            }
        }
    });
    join_handles.push(join_handle);

    for p in remote_locations.into_iter() {
        let semaphore = semaphore.clone();
        let response_sender = response_sender.clone();
        let join_handle = tokio::spawn(async move {
            // Block if exceeds max request number
            let permit = semaphore.acquire_owned().await.unwrap();
            let r = PartitionReaderEnum::FlightRemote.fetch_partition(&p).await;
            // Block if the channel buffer is ful
            if let Err(e) = response_sender.send(r).await {
                error!("Fail to send response event to the channel due to {}", e);
            }
            // Increase semaphore by dropping existing permits.
            drop(permit);
        });
        join_handles.push(join_handle);
    }

    AbortableReceiverStream::create(response_receiver, join_handles)
}

fn check_is_local_location(location: &PartitionLocation) -> bool {
    std::path::Path::new(location.path.as_str()).exists()
}

/// Partition reader Trait, different partition reader can have
#[async_trait]
trait PartitionReader: Send + Sync + Clone {
    // Read partition data from PartitionLocation
    async fn fetch_partition(
        &self,
        location: &PartitionLocation,
    ) -> result::Result<SendableRecordBatchStream, DataFusionError>;
}

#[derive(Clone)]
enum PartitionReaderEnum {
    Local,
    FlightRemote,
    #[allow(dead_code)]
    ObjectStoreRemote,
}

#[async_trait]
impl PartitionReader for PartitionReaderEnum {
    // Notice return `DataFusionError::FetchFailed` will let scheduler re-schedule the task.
    async fn fetch_partition(
        &self,
        location: &PartitionLocation,
    ) -> result::Result<SendableRecordBatchStream, DataFusionError> {
        match self {
            PartitionReaderEnum::FlightRemote => todo!(), //fetch_partition_remote(location).await,
            PartitionReaderEnum::Local => fetch_partition_local(location).await,
            PartitionReaderEnum::ObjectStoreRemote => {
                fetch_partition_object_store(location).await
            }
        }
    }
}

async fn fetch_partition_local(
    location: &PartitionLocation,
) -> result::Result<SendableRecordBatchStream, DataFusionError> {
    let path = &location.path;
    // let metadata = &location.executor_meta;
    let partition_id = &location.partition_id;

    let reader = fetch_partition_local_inner(path).unwrap();
    //.map_err(|e| {
    // return DataFusionError::FetchFailed may let scheduler retry this task.
    // DataFusionError::FetchFailed(
    //     metadata.id.clone(),
    //     partition_id.stage_id,
    //     partition_id.partition_id,
    //     e.to_string(),
    // )
    // panic!() //TODO
    // })?;
    Ok(Box::pin(LocalShuffleStream::new(reader)))
}

fn fetch_partition_local_inner(
    path: &str,
) -> result::Result<StreamReader<BufReader<File>>, DataFusionError> {
    let file = File::open(path).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to open partition file at {path}: {e:?}"
        ))
    })?;
    let reader = StreamReader::try_new(file, None).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to new arrow FileReader at {path}: {e:?}"
        ))
    })?;
    Ok(reader)
}

async fn fetch_partition_object_store(
    _location: &PartitionLocation,
) -> result::Result<SendableRecordBatchStream, DataFusionError> {
    Err(DataFusionError::NotImplemented(
        "Should not use ObjectStorePartitionReader".to_string(),
    ))
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use arrow::array::{Int32Array, StringArray, UInt32Array};
//     use arrow::datatypes::{DataType, Field, Schema};
//     use arrow::ipc::writer::StreamWriter;
//     use arrow::record_batch::RecordBatch;
//     use datafusion_common::DataFusionError;
//     use datafusion_physical_expr::expressions::Column;
//     use datafusion_physical_plan::common;
//     use datafusion_physical_plan::memory::MemoryExec;
//     use datafusion_prelude::SessionContext;
//     use tempfile::{tempdir, TempDir};
//
//     #[tokio::test]
//     async fn test_stats_for_partitions_empty() {
//         let result = stats_for_partitions(0, std::iter::empty());
//
//         let exptected = Statistics {
//             num_rows: Precision::Exact(0),
//             total_byte_size: Precision::Exact(0),
//             column_statistics: vec![],
//         };
//
//         assert_eq!(result, exptected);
//     }
//
//     #[tokio::test]
//     async fn test_stats_for_partitions_full() {
//         let part_stats = vec![
//             PartitionStats {
//                 num_rows: Some(10),
//                 num_bytes: Some(84),
//                 num_batches: Some(1),
//             },
//             PartitionStats {
//                 num_rows: Some(4),
//                 num_bytes: Some(65),
//                 num_batches: None,
//             },
//         ];
//
//         let result = stats_for_partitions(0, part_stats.into_iter());
//
//         let exptected = Statistics {
//             num_rows: Precision::Exact(14),
//             total_byte_size: Precision::Exact(149),
//             column_statistics: vec![],
//         };
//
//         assert_eq!(result, exptected);
//     }
//
//     #[tokio::test]
//     async fn test_stats_for_partitions_missing() {
//         let part_stats = vec![
//             PartitionStats {
//                 num_rows: Some(10),
//                 num_bytes: Some(84),
//                 num_batches: Some(1),
//             },
//             PartitionStats {
//                 num_rows: None,
//                 num_bytes: None,
//                 num_batches: None,
//             },
//         ];
//
//         let result = stats_for_partitions(0, part_stats.into_iter());
//
//         let exptected = Statistics {
//             num_rows: Precision::Absent,
//             total_byte_size: Precision::Absent,
//             column_statistics: vec![],
//         };
//
//         assert_eq!(result, exptected);
//     }
//
//     #[tokio::test]
//     async fn test_fetch_partitions_error_mapping() -> Result<()> {
//         let session_ctx = SessionContext::new();
//         let task_ctx = session_ctx.task_ctx();
//         let schema = Schema::new(vec![
//             Field::new("a", DataType::Int32, false),
//             Field::new("b", DataType::Int32, false),
//             Field::new("c", DataType::Int32, false),
//         ]);
//
//         let job_id = "test_job_1";
//         let input_stage_id = 2;
//         let mut partitions: Vec<PartitionLocation> = vec![];
//         for partition_id in 0..4 {
//             partitions.push(PartitionLocation {
//                 map_partition_id: 0,
//                 partition_id: PartitionId {
//                     job_id: job_id.to_string(),
//                     stage_id: input_stage_id,
//                     partition_id,
//                 },
//                 executor_meta: ExecutorMetadata {
//                     id: "executor_1".to_string(),
//                     host: "executor_1".to_string(),
//                     port: 7070,
//                     grpc_port: 8080,
//                     specification: ExecutorSpecification { task_slots: 1 },
//                 },
//                 partition_stats: Default::default(),
//                 path: "test_path".to_string(),
//             })
//         }
//
//         let shuffle_reader_exec = ShuffleReaderExec::try_new(
//             input_stage_id,
//             vec![partitions],
//             Arc::new(schema),
//         )?;
//         let mut stream = shuffle_reader_exec.execute(0, task_ctx)?;
//         let batches = utils::collect_stream(&mut stream).await;
//
//         assert!(batches.is_err());
//
//         // DataFusionError::FetchFailed -> ArrowError::ExternalError -> DataFusionError::FetchFailed
//         let ballista_error = batches.unwrap_err();
//         assert!(matches!(
//             ballista_error,
//             DataFusionError::FetchFailed(_, _, _, _)
//         ));
//
//         Ok(())
//     }
//
//     #[tokio::test]
//     async fn test_send_fetch_partitions_1() {
//         test_send_fetch_partitions(1, 10).await;
//     }
//
//     #[tokio::test]
//     async fn test_send_fetch_partitions_n() {
//         test_send_fetch_partitions(4, 10).await;
//     }
//
//     #[tokio::test]
//     async fn test_read_local_shuffle() {
//         let session_ctx = SessionContext::new();
//         let task_ctx = session_ctx.task_ctx();
//         let work_dir = TempDir::new().unwrap();
//         let input = ShuffleWriterExec::try_new(
//             "local_file".to_owned(),
//             1,
//             create_test_data_plan().unwrap(),
//             work_dir.into_path().to_str().unwrap().to_owned(),
//             Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 1)),
//         )
//             .unwrap();
//
//         let mut stream = input.execute(0, task_ctx).unwrap();
//
//         let batches = utils::collect_stream(&mut stream)
//             .await
//             .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
//             .unwrap();
//
//         let path = batches[0].columns()[1]
//             .as_any()
//             .downcast_ref::<StringArray>()
//             .unwrap();
//
//         // from to input partitions test the first one with two batches
//         let file_path = path.value(0);
//         let reader = fetch_partition_local_inner(file_path).unwrap();
//
//         let mut stream: Pin<Box<dyn RecordBatchStream + Send>> =
//             async { Box::pin(LocalShuffleStream::new(reader)) }.await;
//
//         let result = utils::collect_stream(&mut stream)
//             .await
//             .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
//             .unwrap();
//
//         assert_eq!(result.len(), 2);
//         for b in result {
//             assert_eq!(b, create_test_batch())
//         }
//     }
//
//     async fn test_send_fetch_partitions(max_request_num: usize, partition_num: usize) {
//         let schema = get_test_partition_schema();
//         let data_array = Int32Array::from(vec![1]);
//         let batch =
//             RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(data_array)])
//                 .unwrap();
//         let tmp_dir = tempdir().unwrap();
//         let file_path = tmp_dir.path().join("shuffle_data");
//         let file = File::create(&file_path).unwrap();
//         let mut writer = StreamWriter::try_new(file, &schema).unwrap();
//         writer.write(&batch).unwrap();
//         writer.finish().unwrap();
//
//         let partition_locations = get_test_partition_locations(
//             partition_num,
//             file_path.to_str().unwrap().to_string(),
//         );
//
//         let response_receiver =
//             send_fetch_partitions(partition_locations, max_request_num);
//
//         let stream = RecordBatchStreamAdapter::new(
//             Arc::new(schema),
//             response_receiver.try_flatten(),
//         );
//
//         let result = common::collect(Box::pin(stream)).await.unwrap();
//         assert_eq!(partition_num, result.len());
//     }
//
//     fn get_test_partition_locations(n: usize, path: String) -> Vec<PartitionLocation> {
//         (0..n)
//             .map(|partition_id| PartitionLocation {
//                 map_partition_id: 0,
//                 partition_id: PartitionId {
//                     job_id: "job".to_string(),
//                     stage_id: 1,
//                     partition_id,
//                 },
//                 executor_meta: ExecutorMetadata {
//                     id: format!("exec{partition_id}"),
//                     host: "localhost".to_string(),
//                     port: 50051,
//                     grpc_port: 50052,
//                     specification: ExecutorSpecification { task_slots: 12 },
//                 },
//                 partition_stats: Default::default(),
//                 path: path.clone(),
//             })
//             .collect()
//     }
//
//     fn get_test_partition_schema() -> Schema {
//         Schema::new(vec![Field::new("id", DataType::Int32, false)])
//     }
//
//     // create two partitions each has two same batches
//     fn create_test_data_plan() -> Result<Arc<dyn ExecutionPlan>> {
//         let batch = create_test_batch();
//         let partition = vec![batch.clone(), batch];
//         let partitions = vec![partition.clone(), partition];
//         Ok(Arc::new(MemoryExec::try_new(
//             &partitions,
//             create_test_schema(),
//             None,
//         )?))
//     }
//
//     fn create_test_batch() -> RecordBatch {
//         RecordBatch::try_new(
//             create_test_schema(),
//             vec![
//                 Arc::new(UInt32Array::from(vec![Some(1), Some(2), Some(3)])),
//                 Arc::new(StringArray::from(vec![
//                     Some("rust"),
//                     Some("datafusion"),
//                     Some("ballista"),
//                 ])),
//             ],
//         )
//             .unwrap()
//     }
//
//     fn create_test_schema() -> SchemaRef {
//         Arc::new(Schema::new(vec![
//             Field::new("number", DataType::UInt32, true),
//             Field::new("str", DataType::Utf8, true),
//         ]))
//     }
// }
