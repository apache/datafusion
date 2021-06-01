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

//! QueryStageExec represents a section of a query plan that has consistent partitioning and
//! can be executed as one unit with each partition being executed in parallel. The output of
//! a query stage either forms the input of another query stage or can be the final result of
//! a query.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use std::{any::Any, pin::Pin};

use crate::error::BallistaError;
use crate::memory_stream::MemoryStream;
use crate::utils;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, RecordBatchStream};
use log::info;
use uuid::Uuid;

/// QueryStageExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of
/// a query stage either forms the input of another query stage or can be the final result of
/// a query.
#[derive(Debug, Clone)]
pub struct QueryStageExec {
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
}

impl QueryStageExec {
    /// Create a new query stage
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
impl ExecutionPlan for QueryStageExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.plan.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.len() == 1);
        Ok(Arc::new(QueryStageExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            children[0].clone(),
            self.work_dir.clone(),
            None,
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
                let stats = utils::write_stream_to_disk(&mut stream, &path)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

                info!(
                    "Executed partition {} in {} seconds. Statistics: {:?}",
                    partition,
                    now.elapsed().as_secs(),
                    stats
                );

                let schema = Arc::new(Schema::new(vec![
                    Field::new("path", DataType::Utf8, false),
                    stats.arrow_struct_repr(),
                ]));

                // build result set with summary of the partition execution status
                let mut c0 = StringBuilder::new(1);
                c0.append_value(&path).unwrap();
                let path: ArrayRef = Arc::new(c0.finish());

                let stats: ArrayRef = stats
                    .to_arrow_arrayref()
                    .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
                let batch = RecordBatch::try_new(schema.clone(), vec![path, stats])
                    .map_err(DataFusionError::ArrowError)?;

                Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
            }

            Some(Partitioning::Hash(_, _)) => {
                //TODO re-use code from RepartitionExec to split each batch into
                // partitions and write to one IPC file per partition
                // See https://github.com/apache/arrow-datafusion/issues/456
                Err(DataFusionError::NotImplemented(
                    "Shuffle partitioning not implemented yet".to_owned(),
                ))
            }

            _ => Err(DataFusionError::Execution(
                "Invalid shuffle partitioning scheme".to_owned(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, StructArray, UInt32Array, UInt64Array};
    use datafusion::physical_plan::memory::MemoryExec;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test() -> Result<()> {
        let input_plan = create_input_plan()?;
        let work_dir = TempDir::new()?;
        let query_stage = QueryStageExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            None,
        )?;
        let mut stream = query_stage.execute(0).await?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert!(batches.len() == 1);
        let batch = &batches[0];
        assert_eq!(2, batch.num_columns());
        assert_eq!(1, batch.num_rows());
        let path = batch.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let file = path.value(0);
        assert!(file.ends_with("data.arrow"));
        let stats = batch.columns()[1]
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
