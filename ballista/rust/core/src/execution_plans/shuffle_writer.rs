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

//! ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
//! can be executed as one unit with each partition being executed in parallel. The output of each
//! partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
//! will use the ShuffleReaderExec to read these results.

use std::fs::File;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{any::Any, pin::Pin};

use crate::error::BallistaError;
use crate::memory_stream::MemoryStream;
use crate::utils;

use crate::serde::scheduler::PartitionStats;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, StringBuilder, StructBuilder, UInt32Builder,
    UInt64Builder,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::hash_join::create_hashes;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SQLMetric,
};
use futures::StreamExt;
use hashbrown::HashMap;
use log::info;
use uuid::Uuid;

/// ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of each
/// partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
#[derive(Debug, Clone)]
pub struct ShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: String,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    work_dir: String,
    /// Optional shuffle output partitioning
    shuffle_output_partitioning: Option<Partitioning>,
    /// Shuffle write metrics
    metrics: ShuffleWriteMetrics,
}

#[derive(Debug, Clone)]
struct ShuffleWriteMetrics {
    /// Time spend writing batches to shuffle files
    write_time: Arc<SQLMetric>,
}

impl ShuffleWriteMetrics {
    fn new() -> Self {
        Self {
            write_time: SQLMetric::time_nanos(),
        }
    }
}

impl ShuffleWriterExec {
    /// Create a new shuffle writer
    pub fn try_new(
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
            shuffle_output_partitioning,
            metrics: ShuffleWriteMetrics::new(),
        })
    }

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        match &self.shuffle_output_partitioning {
            Some(p) => p.clone(),
            _ => Partitioning::UnknownPartitioning(1),
        }
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.len() == 1);
        Ok(Arc::new(ShuffleWriterExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            children[0].clone(),
            self.work_dir.clone(),
            self.shuffle_output_partitioning.clone(),
        )?))
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        let now = Instant::now();

        let mut stream = self.plan.execute(partition).await?;

        let mut path = PathBuf::from(&self.work_dir);
        path.push(&self.job_id);
        path.push(&format!("{}", self.stage_id));

        match &self.shuffle_output_partitioning {
            None => {
                path.push(&format!("{}", partition));
                std::fs::create_dir_all(&path)?;
                path.push("data.arrow");
                let path = path.to_str().unwrap();
                info!("Writing results to {}", path);

                // stream results to disk
                let stats = utils::write_stream_to_disk(
                    &mut stream,
                    path,
                    self.metrics.write_time.clone(),
                )
                .await
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

                info!(
                    "Executed partition {} in {} seconds. Statistics: {}",
                    partition,
                    now.elapsed().as_secs(),
                    stats
                );

                let schema = result_schema();

                // build result set with summary of the partition execution status
                let mut part_builder = UInt32Builder::new(1);
                part_builder.append_value(partition as u32)?;
                let part: ArrayRef = Arc::new(part_builder.finish());

                let mut path_builder = StringBuilder::new(1);
                path_builder.append_value(&path)?;
                let path: ArrayRef = Arc::new(path_builder.finish());

                let stats: ArrayRef = stats
                    .to_arrow_arrayref()
                    .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
                let batch = RecordBatch::try_new(schema.clone(), vec![part, path, stats])
                    .map_err(DataFusionError::ArrowError)?;

                Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
            }

            Some(Partitioning::Hash(exprs, n)) => {
                let num_output_partitions = *n;

                // we won't necessary produce output for every possible partition, so we
                // create writers on demand
                let mut writers: Vec<Option<ShuffleWriter>> = vec![];
                for _ in 0..num_output_partitions {
                    writers.push(None);
                }

                let hashes_buf = &mut vec![];
                let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
                while let Some(result) = stream.next().await {
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
                    let hashes = create_hashes(&arrays, &random_state, hashes_buf)?;
                    let mut indices = vec![vec![]; num_output_partitions];
                    for (index, hash) in hashes.iter().enumerate() {
                        indices[(*hash % num_output_partitions as u64) as usize]
                            .push(index as u64)
                    }
                    for (output_partition, partition_indices) in
                        indices.into_iter().enumerate()
                    {
                        let indices = partition_indices.into();
                        // Produce batches based on indices
                        let columns = input_batch
                            .columns()
                            .iter()
                            .map(|c| {
                                take(c.as_ref(), &indices, None).map_err(|e| {
                                    DataFusionError::Execution(e.to_string())
                                })
                            })
                            .collect::<Result<Vec<Arc<dyn Array>>>>()?;

                        let output_batch =
                            RecordBatch::try_new(input_batch.schema(), columns)?;

                        // write batch out
                        let start = Instant::now();
                        match &mut writers[output_partition] {
                            Some(w) => {
                                w.write(&output_batch)?;
                            }
                            None => {
                                let mut path = path.clone();
                                path.push(&format!("{}", output_partition));
                                std::fs::create_dir_all(&path)?;

                                path.push("data.arrow");
                                let path = path.to_str().unwrap();
                                info!("Writing results to {}", path);

                                let mut writer =
                                    ShuffleWriter::new(path, stream.schema().as_ref())?;

                                writer.write(&output_batch)?;
                                writers[output_partition] = Some(writer);
                            }
                        }
                        self.metrics.write_time.add_elapsed(start);
                    }
                }

                // build metadata result batch
                let num_writers = writers.iter().filter(|w| w.is_some()).count();
                let mut partition_builder = UInt32Builder::new(num_writers);
                let mut path_builder = StringBuilder::new(num_writers);
                let mut num_rows_builder = UInt64Builder::new(num_writers);
                let mut num_batches_builder = UInt64Builder::new(num_writers);
                let mut num_bytes_builder = UInt64Builder::new(num_writers);

                for (i, w) in writers.iter_mut().enumerate() {
                    match w {
                        Some(w) => {
                            w.finish()?;
                            path_builder.append_value(w.path())?;
                            partition_builder.append_value(i as u32)?;
                            num_rows_builder.append_value(w.num_rows)?;
                            num_batches_builder.append_value(w.num_batches)?;
                            num_bytes_builder.append_value(w.num_bytes)?;
                        }
                        None => {}
                    }
                }

                // build arrays
                let partition_num: ArrayRef = Arc::new(partition_builder.finish());
                let path: ArrayRef = Arc::new(path_builder.finish());
                let field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
                    Box::new(num_rows_builder),
                    Box::new(num_batches_builder),
                    Box::new(num_bytes_builder),
                ];
                let mut stats_builder = StructBuilder::new(
                    PartitionStats::default().arrow_struct_fields(),
                    field_builders,
                );
                for _ in 0..num_writers {
                    stats_builder.append(true)?;
                }
                let stats = Arc::new(stats_builder.finish());

                // build result batch containing metadata
                let schema = result_schema();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![partition_num, path, stats],
                )
                .map_err(DataFusionError::ArrowError)?;

                Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
            }

            _ => Err(DataFusionError::Execution(
                "Invalid shuffle partitioning scheme".to_owned(),
            )),
        }
    }

    fn metrics(&self) -> HashMap<String, SQLMetric> {
        let mut metrics = HashMap::new();
        metrics.insert("writeTime".to_owned(), (*self.metrics.write_time).clone());
        metrics
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "ShuffleWriterExec: {:?}",
                    self.shuffle_output_partitioning
                )
            }
        }
    }
}

fn result_schema() -> SchemaRef {
    let stats = PartitionStats::default();
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt32, false),
        Field::new("path", DataType::Utf8, false),
        stats.arrow_struct_repr(),
    ]))
}

struct ShuffleWriter {
    path: String,
    writer: FileWriter<File>,
    num_batches: u64,
    num_rows: u64,
    num_bytes: u64,
}

impl ShuffleWriter {
    fn new(path: &str, schema: &Schema) -> Result<Self> {
        let file = File::create(path)
            .map_err(|e| {
                BallistaError::General(format!(
                    "Failed to create partition file at {}: {:?}",
                    path, e
                ))
            })
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            path: path.to_owned(),
            writer: FileWriter::try_new(file, schema)?,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        self.num_batches += 1;
        self.num_rows += batch.num_rows() as u64;
        let num_bytes: usize = batch
            .columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum();
        self.num_bytes += num_bytes as u64;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(DataFusionError::ArrowError)
    }

    fn path(&self) -> &str {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, StructArray, UInt32Array, UInt64Array};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::memory::MemoryExec;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test() -> Result<()> {
        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0).await?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let path = batch.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let file0 = path.value(0);
        assert!(
            file0.ends_with("/jobOne/1/0/data.arrow")
                || file0.ends_with("\\jobOne\\1\\0\\data.arrow")
        );
        let file1 = path.value(1);
        assert!(
            file1.ends_with("/jobOne/1/1/data.arrow")
                || file1.ends_with("\\jobOne\\1\\1\\data.arrow")
        );

        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let num_rows = stats
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(4, num_rows.value(0));
        assert_eq!(4, num_rows.value(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned() -> Result<()> {
        let input_plan = create_input_plan()?;
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0).await?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let num_rows = stats
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(2, num_rows.value(0));
        assert_eq!(2, num_rows.value(1));

        Ok(())
    }

    fn create_input_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];
        Ok(Arc::new(MemoryExec::try_new(&partitions, schema, None)?))
    }
}
