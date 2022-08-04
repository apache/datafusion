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

//! Execution plan for reading line-delimited JSON files
use arrow::json::reader::DecoderOptions;

use crate::datasource::listing::FileRange;
use crate::error::{DataFusionError, Result};
use crate::execution::context::SessionState;
use crate::execution::context::TaskContext;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::file_format::delimited_stream::newline_delimited_stream;
use crate::physical_plan::file_format::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::{datatypes::SchemaRef, json};
use bytes::Buf;
use futures::{StreamExt, TryStreamExt};
use object_store::{GetResult, ObjectMeta, ObjectStore};
use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};

use super::FileScanConfig;

/// Execution plan for scanning NdJson data source
#[derive(Debug, Clone)]
pub struct NdJsonExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl NdJsonExec {
    /// Create a new JSON reader execution plan provided base configurations
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for NdJsonExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let proj = self.base_config.projected_file_column_names();

        let batch_size = context.session_config().batch_size();
        let file_schema = Arc::clone(&self.base_config.file_schema);

        let options = DecoderOptions::new().with_batch_size(batch_size);
        let options = if let Some(proj) = proj {
            options.with_projection(proj)
        } else {
            options
        };

        let opener = JsonOpener {
            file_schema,
            options,
        };

        let stream = FileStream::new(
            &self.base_config,
            partition,
            context,
            opener,
            BaselineMetrics::new(&self.metrics, partition),
        )?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
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
                    "JsonExec: limit={:?}, files={}",
                    self.base_config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

struct JsonOpener {
    options: DecoderOptions,
    file_schema: SchemaRef,
}

impl FileOpener for JsonOpener {
    fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: ObjectMeta,
        _range: Option<FileRange>,
    ) -> FileOpenFuture {
        let options = self.options.clone();
        let schema = self.file_schema.clone();
        Box::pin(async move {
            match store.get(&file.location).await? {
                GetResult::File(file, _) => {
                    let reader = json::Reader::new(file, schema.clone(), options);
                    Ok(futures::stream::iter(reader).boxed())
                }
                GetResult::Stream(s) => {
                    Ok(newline_delimited_stream(s.map_err(Into::into))
                        .map_ok(move |bytes| {
                            let reader = json::Reader::new(
                                bytes.reader(),
                                schema.clone(),
                                options.clone(),
                            );
                            futures::stream::iter(reader)
                        })
                        .try_flatten()
                        .boxed())
                }
            }
        })
    }
}

pub async fn plan_to_json(
    state: &SessionState,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    // create directory to contain the CSV files (one per partition)
    let fs_path = Path::new(path);
    match fs::create_dir(fs_path) {
        Ok(()) => {
            let mut tasks = vec![];
            for i in 0..plan.output_partitioning().partition_count() {
                let plan = plan.clone();
                let filename = format!("part-{}.json", i);
                let path = fs_path.join(&filename);
                let file = fs::File::create(path)?;
                let mut writer = json::LineDelimitedWriter::new(file);
                let task_ctx = Arc::new(TaskContext::from(state));
                let stream = plan.execute(i, task_ctx)?;
                let handle: JoinHandle<Result<()>> = task::spawn(async move {
                    stream
                        .map(|batch| writer.write(batch?))
                        .try_collect()
                        .await
                        .map_err(DataFusionError::from)
                });
                tasks.push(handle);
            }
            futures::future::join_all(tasks).await;
            Ok(())
        }
        Err(e) => Err(DataFusionError::Execution(format!(
            "Could not create directory {}: {:?}",
            path, e
        ))),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;

    use crate::assert_batches_eq;
    use crate::datasource::file_format::{json::JsonFormat, FileFormat};
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_plan::file_format::chunked_store::ChunkedStore;
    use crate::prelude::NdJsonReadOptions;
    use crate::prelude::*;
    use crate::test::object_store::local_unpartitioned_file;
    use tempfile::TempDir;

    use super::*;

    const TEST_DATA_BASE: &str = "tests/jsons";

    async fn prepare_store(
        ctx: &SessionContext,
    ) -> (ObjectStoreUrl, Vec<Vec<PartitionedFile>>, SchemaRef) {
        let store_url = ObjectStoreUrl::local_filesystem();
        let store = ctx.runtime_env().object_store(&store_url).unwrap();

        let path = format!("{}/1.json", TEST_DATA_BASE);
        let meta = local_unpartitioned_file(path);
        let schema = JsonFormat::default()
            .infer_schema(&store, &[meta.clone()])
            .await
            .unwrap();

        (store_url, vec![vec![meta.into()]], schema)
    }

    #[tokio::test]
    async fn nd_json_exec_file_without_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        use arrow::datatypes::DataType;

        let (object_store_url, file_groups, file_schema) =
            prepare_store(&session_ctx).await;

        let exec = NdJsonExec::new(FileScanConfig {
            object_store_url,
            file_groups,
            file_schema,
            statistics: Statistics::default(),
            projection: None,
            limit: Some(3),
            table_partition_cols: vec![],
        });

        // TODO: this is not where schema inference should be tested

        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 4);

        // a,b,c,d should be inferred
        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap();

        assert_eq!(
            inferred_schema.field_with_name("a").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(matches!(
            inferred_schema.field_with_name("b").unwrap().data_type(),
            DataType::List(_)
        ));
        assert_eq!(
            inferred_schema.field_with_name("d").unwrap().data_type(),
            &DataType::Utf8
        );

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 3);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);

        Ok(())
    }

    #[tokio::test]
    async fn nd_json_exec_file_with_missing_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        use arrow::datatypes::DataType;
        let (object_store_url, file_groups, actual_schema) =
            prepare_store(&session_ctx).await;

        let mut fields = actual_schema.fields().clone();
        fields.push(Field::new("missing_col", DataType::Int32, true));
        let missing_field_idx = fields.len() - 1;

        let file_schema = Arc::new(Schema::new(fields));

        let exec = NdJsonExec::new(FileScanConfig {
            object_store_url,
            file_groups,
            file_schema,
            statistics: Statistics::default(),
            projection: None,
            limit: Some(3),
            table_partition_cols: vec![],
        });

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 3);
        let values = batch
            .column(missing_field_idx)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.is_null(0));
        assert!(values.is_null(1));
        assert!(values.is_null(2));

        Ok(())
    }

    #[tokio::test]
    async fn nd_json_exec_file_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&session_ctx).await;

        let exec = NdJsonExec::new(FileScanConfig {
            object_store_url,
            file_groups,
            file_schema,
            statistics: Statistics::default(),
            projection: Some(vec![0, 2]),
            limit: None,
            table_partition_cols: vec![],
        });
        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 2);

        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap_err();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap_err();

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 4);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_json_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        let ctx =
            SessionContext::with_config(SessionConfig::new().with_target_partitions(8));

        let path = format!("{}/1.json", TEST_DATA_BASE);

        // register json file with the execution context
        ctx.register_json("test", path.as_str(), NdJsonReadOptions::default())
            .await?;

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let df = ctx.sql("SELECT a, b FROM test").await?;
        df.write_json(&out_dir).await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new();

        // register each partition as well as the top level dir
        let json_read_option = NdJsonReadOptions::default();
        ctx.register_json(
            "part0",
            &format!("{}/part-0.json", out_dir),
            json_read_option.clone(),
        )
        .await?;
        ctx.register_json("allparts", &out_dir, json_read_option)
            .await?;

        let part0 = ctx.sql("SELECT a, b FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT a, b FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_chunked() {
        let mut ctx = SessionContext::new();

        for chunk_size in [10, 20, 30, 40] {
            ctx.runtime_env().register_object_store(
                "file",
                "",
                Arc::new(ChunkedStore::new(
                    Arc::new(LocalFileSystem::new()),
                    chunk_size,
                )),
            );

            let path = format!("{}/1.json", TEST_DATA_BASE);
            let frame = ctx.read_json(path, Default::default()).await.unwrap();
            let results = frame.collect().await.unwrap();

            assert_batches_eq!(
                &[
                    "+-----+----------------+---------------+------+",
                    "| a   | b              | c             | d    |",
                    "+-----+----------------+---------------+------+",
                    "| 1   | [2, 1.3, -6.1] | [false, true] | 4    |",
                    "| -10 | [2, 1.3, -6.1] | [true, true]  | 4    |",
                    "| 2   | [2, , -6.1]    | [false, ]     | text |",
                    "|     |                |               |      |",
                    "+-----+----------------+---------------+------+",
                ],
                &results
            );
        }
    }
}
