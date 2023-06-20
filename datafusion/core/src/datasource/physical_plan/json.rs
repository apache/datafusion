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
use crate::datasource::file_format::file_type::FileCompressionType;
use crate::datasource::physical_plan::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::datasource::physical_plan::FileMeta;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::common::AbortOnDropSingle;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::physical_plan::{
    ordering_equivalence_properties_helper, DisplayAs, DisplayFormatType, ExecutionPlan,
    Partitioning, SendableRecordBatchStream, Statistics,
};
use datafusion_execution::TaskContext;

use arrow::json::ReaderBuilder;
use arrow::{datatypes::SchemaRef, json};
use datafusion_physical_expr::{LexOrdering, OrderingEquivalenceProperties};

use bytes::{Buf, Bytes};
use futures::{ready, stream, StreamExt, TryStreamExt};
use object_store::{GetResult, ObjectStore};
use std::any::Any;
use std::fs;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::task::Poll;
use tokio::task::{self, JoinHandle};

use super::FileScanConfig;

/// Execution plan for scanning NdJson data source
#[derive(Debug, Clone)]
pub struct NdJsonExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    file_compression_type: FileCompressionType,
}

impl NdJsonExec {
    /// Create a new JSON reader execution plan provided base configurations
    pub fn new(
        base_config: FileScanConfig,
        file_compression_type: FileCompressionType,
    ) -> Self {
        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
            file_compression_type,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
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

    fn unbounded_output(&self, _: &[bool]) -> Result<bool> {
        Ok(self.base_config.infinite_source)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.projected_output_ordering
            .first()
            .map(|ordering| ordering.as_slice())
    }

    fn ordering_equivalence_properties(&self) -> OrderingEquivalenceProperties {
        ordering_equivalence_properties_helper(
            self.schema(),
            &self.projected_output_ordering,
        )
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
        let batch_size = context.session_config().batch_size();
        let (projected_schema, ..) = self.base_config.project();

        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;
        let opener = JsonOpener {
            batch_size,
            projected_schema,
            file_compression_type: self.file_compression_type.to_owned(),
            object_store,
        };

        let stream =
            FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "JsonExec: ")?;
        self.base_config.fmt_as(t, f)
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// A [`FileOpener`] that opens a JSON file and yields a [`FileOpenFuture`]
pub struct JsonOpener {
    batch_size: usize,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl JsonOpener {
    /// Returns a  [`JsonOpener`]
    pub fn new(
        batch_size: usize,
        projected_schema: SchemaRef,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            file_compression_type,
            object_store,
        }
    }
}

impl FileOpener for JsonOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let store = self.object_store.clone();
        let schema = self.projected_schema.clone();
        let batch_size = self.batch_size;

        let file_compression_type = self.file_compression_type.to_owned();
        Ok(Box::pin(async move {
            match store.get(file_meta.location()).await? {
                GetResult::File(file, _) => {
                    let bytes = file_compression_type.convert_read(file)?;
                    let reader = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build(BufReader::new(bytes))?;
                    Ok(futures::stream::iter(reader).boxed())
                }
                GetResult::Stream(s) => {
                    let mut decoder = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build_decoder()?;

                    let s = s.map_err(DataFusionError::from);
                    let mut input =
                        file_compression_type.convert_stream(s.boxed())?.fuse();
                    let mut buffered = Bytes::new();

                    let s = stream::poll_fn(move |cx| {
                        loop {
                            if buffered.is_empty() {
                                buffered = match ready!(input.poll_next_unpin(cx)) {
                                    Some(Ok(b)) => b,
                                    Some(Err(e)) => {
                                        return Poll::Ready(Some(Err(e.into())))
                                    }
                                    None => break,
                                };
                            }
                            let read = buffered.len();

                            let decoded = match decoder.decode(buffered.as_ref()) {
                                Ok(decoded) => decoded,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };

                            buffered.advance(decoded);
                            if decoded != read {
                                break;
                            }
                        }

                        Poll::Ready(decoder.flush().transpose())
                    });
                    Ok(s.boxed())
                }
            }
        }))
    }
}

pub async fn plan_to_json(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    // create directory to contain the CSV files (one per partition)
    let fs_path = Path::new(path);
    if let Err(e) = fs::create_dir(fs_path) {
        return Err(DataFusionError::Execution(format!(
            "Could not create directory {path}: {e:?}"
        )));
    }

    let mut tasks = vec![];
    for i in 0..plan.output_partitioning().partition_count() {
        let plan = plan.clone();
        let filename = format!("part-{i}.json");
        let path = fs_path.join(filename);
        let file = fs::File::create(path)?;
        let mut writer = json::LineDelimitedWriter::new(file);
        let stream = plan.execute(i, task_ctx.clone())?;
        let handle: JoinHandle<Result<()>> = task::spawn(async move {
            stream
                .map(|batch| writer.write(&batch?))
                .try_collect()
                .await
                .map_err(DataFusionError::from)
        });
        tasks.push(AbortOnDropSingle::new(handle));
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .try_for_each(|result| {
            result.map_err(|e| DataFusionError::Execution(format!("{e}")))?
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::datatypes::{Field, SchemaBuilder};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;

    use crate::assert_batches_eq;
    use crate::datasource::file_format::file_type::FileType;
    use crate::datasource::file_format::{json::JsonFormat, FileFormat};
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::datasource::physical_plan::chunked_store::ChunkedStore;
    use crate::execution::context::SessionState;
    use crate::prelude::NdJsonReadOptions;
    use crate::prelude::*;
    use crate::test::partitioned_file_groups;
    use datafusion_common::cast::{as_int32_array, as_int64_array, as_string_array};
    use rstest::*;
    use tempfile::TempDir;
    use url::Url;

    use super::*;

    const TEST_DATA_BASE: &str = "tests/data";

    async fn prepare_store(
        state: &SessionState,
        file_compression_type: FileCompressionType,
    ) -> (ObjectStoreUrl, Vec<Vec<PartitionedFile>>, SchemaRef) {
        let store_url = ObjectStoreUrl::local_filesystem();
        let store = state.runtime_env().object_store(&store_url).unwrap();

        let filename = "1.json";
        let file_groups = partitioned_file_groups(
            TEST_DATA_BASE,
            filename,
            1,
            FileType::JSON,
            file_compression_type.to_owned(),
        )
        .unwrap();
        let meta = file_groups
            .get(0)
            .unwrap()
            .get(0)
            .unwrap()
            .clone()
            .object_meta;
        let schema = JsonFormat::default()
            .with_file_compression_type(file_compression_type.to_owned())
            .infer_schema(state, &store, &[meta.clone()])
            .await
            .unwrap();

        (store_url, file_groups, schema)
    }

    async fn test_additional_stores(
        file_compression_type: FileCompressionType,
        store: Arc<dyn ObjectStore>,
    ) {
        let ctx = SessionContext::new();
        let url = Url::parse("file://").unwrap();
        ctx.runtime_env().register_object_store(&url, store.clone());
        let filename = "1.json";
        let file_groups = partitioned_file_groups(
            TEST_DATA_BASE,
            filename,
            1,
            FileType::JSON,
            file_compression_type.to_owned(),
        )
        .unwrap();
        let path = file_groups
            .get(0)
            .unwrap()
            .get(0)
            .unwrap()
            .object_meta
            .location
            .as_ref();

        let store_url = ObjectStoreUrl::local_filesystem();
        let url: &Url = store_url.as_ref();
        let path_buf = Path::new(url.path()).join(path);
        let path = path_buf.to_str().unwrap();

        let ext = FileType::JSON
            .get_ext_with_compression(file_compression_type.to_owned())
            .unwrap();

        let read_options = NdJsonReadOptions::default()
            .file_extension(ext.as_str())
            .file_compression_type(file_compression_type.to_owned());
        let frame = ctx.read_json(path, read_options).await.unwrap();
        let results = frame.collect().await.unwrap();

        assert_batches_eq!(
            &[
                "+-----+------------------+---------------+------+",
                "| a   | b                | c             | d    |",
                "+-----+------------------+---------------+------+",
                "| 1   | [2.0, 1.3, -6.1] | [false, true] | 4    |",
                "| -10 | [2.0, 1.3, -6.1] | [true, true]  | 4    |",
                "| 2   | [2.0, , -6.1]    | [false, ]     | text |",
                "|     |                  |               |      |",
                "+-----+------------------+---------------+------+",
            ],
            &results
        );
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn nd_json_exec_file_without_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        use arrow::datatypes::DataType;

        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned()).await;

        let exec = NdJsonExec::new(
            FileScanConfig {
                object_store_url,
                file_groups,
                file_schema,
                statistics: Statistics::default(),
                projection: None,
                limit: Some(3),
                table_partition_cols: vec![],
                output_ordering: vec![],
                infinite_source: false,
            },
            file_compression_type.to_owned(),
        );

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
        let values = as_int64_array(batch.column(0))?;
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);

        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn nd_json_exec_file_with_missing_column(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        use arrow::datatypes::DataType;
        let (object_store_url, file_groups, actual_schema) =
            prepare_store(&state, file_compression_type.to_owned()).await;

        let mut builder = SchemaBuilder::from(actual_schema.fields());
        builder.push(Field::new("missing_col", DataType::Int32, true));

        let file_schema = Arc::new(builder.finish());
        let missing_field_idx = file_schema.fields.len() - 1;

        let exec = NdJsonExec::new(
            FileScanConfig {
                object_store_url,
                file_groups,
                file_schema,
                statistics: Statistics::default(),
                projection: None,
                limit: Some(3),
                table_partition_cols: vec![],
                output_ordering: vec![],
                infinite_source: false,
            },
            file_compression_type.to_owned(),
        );

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 3);
        let values = as_int32_array(batch.column(missing_field_idx))?;
        assert_eq!(values.len(), 3);
        assert!(values.is_null(0));
        assert!(values.is_null(1));
        assert!(values.is_null(2));

        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn nd_json_exec_file_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned()).await;

        let exec = NdJsonExec::new(
            FileScanConfig {
                object_store_url,
                file_groups,
                file_schema,
                statistics: Statistics::default(),
                projection: Some(vec![0, 2]),
                limit: None,
                table_partition_cols: vec![],
                output_ordering: vec![],
                infinite_source: false,
            },
            file_compression_type.to_owned(),
        );
        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 2);

        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap_err();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap_err();

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 4);
        let values = as_int64_array(batch.column(0))?;
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn nd_json_exec_file_mixed_order_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned()).await;

        let exec = NdJsonExec::new(
            FileScanConfig {
                object_store_url,
                file_groups,
                file_schema,
                statistics: Statistics::default(),
                projection: Some(vec![3, 0, 2]),
                limit: None,
                table_partition_cols: vec![],
                output_ordering: vec![],
                infinite_source: false,
            },
            file_compression_type.to_owned(),
        );
        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 3);

        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap_err();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap();

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 4);

        let values = as_string_array(batch.column(0))?;
        assert_eq!(values.value(0), "4");
        assert_eq!(values.value(1), "4");
        assert_eq!(values.value(2), "text");

        let values = as_int64_array(batch.column(1))?;
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

        let path = format!("{TEST_DATA_BASE}/1.json");

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
            &format!("{out_dir}/part-0.json"),
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

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn test_chunked_json(
        file_compression_type: FileCompressionType,
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) {
        test_additional_stores(
            file_compression_type,
            Arc::new(ChunkedStore::new(
                Arc::new(LocalFileSystem::new()),
                chunk_size,
            )),
        )
        .await;
    }

    #[tokio::test]
    async fn write_json_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/data/corrupt.csv", options).await?;
        let tmp_dir = TempDir::new()?;
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let e = df
            .write_json(&out_dir)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!("Arrow error: Parser error: Error while parsing value d for column 0 at line 4", format!("{e}"));
        Ok(())
    }
}
