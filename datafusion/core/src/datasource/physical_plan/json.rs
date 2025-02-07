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

use std::any::Any;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use super::{calculate_range, FileScanConfig, RangeCalculation};
use crate::datasource::data_source::FileSource;
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::file_format::{deserialize_stream, DecoderDeserializer};
use crate::datasource::listing::{ListingTableUrl, PartitionedFile};
use crate::datasource::physical_plan::file_stream::{FileOpenFuture, FileOpener};
use crate::datasource::physical_plan::FileMeta;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use arrow::json::ReaderBuilder;
use arrow::{datatypes::SchemaRef, json};
use datafusion_common::{Constraints, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::source::DataSourceExec;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};

use futures::{StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

/// Execution plan for scanning NdJson data source
#[derive(Debug, Clone)]
#[deprecated(since = "46.0.0", note = "use DataSourceExec instead")]
pub struct NdJsonExec {
    inner: DataSourceExec,
    base_config: FileScanConfig,
    file_compression_type: FileCompressionType,
}

#[allow(unused, deprecated)]
impl NdJsonExec {
    /// Create a new JSON reader execution plan provided base configurations
    pub fn new(
        base_config: FileScanConfig,
        file_compression_type: FileCompressionType,
    ) -> Self {
        let (
            projected_schema,
            projected_constraints,
            projected_statistics,
            projected_output_ordering,
        ) = base_config.project();
        let cache = Self::compute_properties(
            projected_schema,
            &projected_output_ordering,
            projected_constraints,
            &base_config,
        );

        let json = JsonSource::default();
        let base_config = base_config
            .with_file_compression_type(file_compression_type)
            .with_source(Arc::new(json));

        Self {
            inner: DataSourceExec::new(Arc::new(base_config.clone())),
            file_compression_type: base_config.file_compression_type,
            base_config,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// Ref to file compression type
    pub fn file_compression_type(&self) -> &FileCompressionType {
        &self.file_compression_type
    }

    fn file_scan_config(&self) -> FileScanConfig {
        let source = self.inner.source();
        source
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .unwrap()
            .clone()
    }

    fn json_source(&self) -> JsonSource {
        let source = self.file_scan_config();
        source
            .file_source()
            .as_any()
            .downcast_ref::<JsonSource>()
            .unwrap()
            .clone()
    }

    fn output_partitioning_helper(file_scan_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_scan_config.file_groups.len())
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        constraints: Constraints,
        file_scan_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings)
            .with_constraints(constraints);

        PlanProperties::new(
            eq_properties,
            Self::output_partitioning_helper(file_scan_config), // Output Partitioning
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.base_config.file_groups = file_groups.clone();
        let mut file_source = self.file_scan_config();
        file_source = file_source.with_file_groups(file_groups);
        self.inner = self.inner.with_source(Arc::new(file_source));
        self
    }
}

#[allow(unused, deprecated)]
impl DisplayAs for NdJsonExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[allow(unused, deprecated)]
impl ExecutionPlan for NdJsonExec {
    fn name(&self) -> &'static str {
        "NdJsonExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &datafusion_common::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.inner.repartitioned(target_partitions, config)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit)
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

/// JsonSource holds the extra configuration that is necessary for [`JsonOpener`]
#[derive(Clone, Default)]
pub struct JsonSource {
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl JsonSource {
    /// Initialize a JsonSource with default values
    pub fn new() -> Self {
        Self::default()
    }
}

impl FileSource for JsonSource {
    fn create_file_opener(
        &self,
        object_store: Result<Arc<dyn ObjectStore>>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(JsonOpener {
            batch_size: self
                .batch_size
                .expect("Batch size must set before creating opener"),
            projected_schema: base_config.projected_file_schema(),
            file_compression_type: base_config.file_compression_type,
            object_store: object_store?,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set to call"))
    }

    fn file_type(&self) -> &str {
        "json"
    }

    fn supports_repartition(&self, config: &FileScanConfig) -> bool {
        !(config.file_compression_type.is_compressed() || config.new_lines_in_values)
    }
}

impl FileOpener for JsonOpener {
    /// Open a partitioned NDJSON file.
    ///
    /// If `file_meta.range` is `None`, the entire file is opened.
    /// Else `file_meta.range` is `Some(FileRange{start, end})`, which corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    ///
    /// See [`CsvOpener`](super::CsvOpener) for an example.
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.projected_schema);
        let batch_size = self.batch_size;
        let file_compression_type = self.file_compression_type.to_owned();

        Ok(Box::pin(async move {
            let calculated_range = calculate_range(&file_meta, &store, None).await?;

            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(
                        futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed()
                    )
                }
            };

            let options = GetOptions {
                range,
                ..Default::default()
            };

            let result = store.get_opts(file_meta.location(), options).await?;

            match result.payload {
                GetResultPayload::File(mut file, _) => {
                    let bytes = match file_meta.range {
                        None => file_compression_type.convert_read(file)?,
                        Some(_) => {
                            file.seek(SeekFrom::Start(result.range.start as _))?;
                            let limit = result.range.end - result.range.start;
                            file_compression_type.convert_read(file.take(limit as u64))?
                        }
                    };

                    let reader = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build(BufReader::new(bytes))?;

                    Ok(futures::stream::iter(reader).boxed())
                }
                GetResultPayload::Stream(s) => {
                    let s = s.map_err(DataFusionError::from);

                    let decoder = ReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build_decoder()?;
                    let input = file_compression_type.convert_stream(s.boxed())?.fuse();

                    Ok(deserialize_stream(
                        input,
                        DecoderDeserializer::from(decoder),
                    ))
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
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let storeref = Arc::clone(&store);
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.json", parsed.prefix());
        let file = object_store::path::Path::parse(filename)?;

        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut buf_writer = BufWriter::new(storeref, file.clone());

            let mut buffer = Vec::with_capacity(1024);
            while let Some(batch) = stream.next().await.transpose()? {
                let mut writer = json::LineDelimitedWriter::new(buffer);
                writer.write(&batch)?;
                buffer = writer.into_inner();
                buf_writer.write_all(&buffer).await?;
                buffer.clear();
            }

            buf_writer.shutdown().await.map_err(DataFusionError::from)
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?, // propagate DataFusion error
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;
    use crate::dataframe::DataFrameWriteOptions;
    use crate::datasource::file_format::{json::JsonFormat, FileFormat};
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::execution::context::SessionState;
    use crate::prelude::{
        CsvReadOptions, NdJsonReadOptions, SessionConfig, SessionContext,
    };
    use crate::test::partitioned_file_groups;
    use crate::{assert_batches_eq, assert_batches_sorted_eq};

    use arrow::array::Array;
    use arrow::datatypes::{Field, SchemaBuilder};
    use datafusion_common::cast::{as_int32_array, as_int64_array, as_string_array};

    use object_store::chunked::ChunkedStore;
    use object_store::local::LocalFileSystem;
    use rstest::*;
    use tempfile::TempDir;
    use url::Url;

    const TEST_DATA_BASE: &str = "tests/data";

    async fn prepare_store(
        state: &SessionState,
        file_compression_type: FileCompressionType,
        work_dir: &Path,
    ) -> (ObjectStoreUrl, Vec<Vec<PartitionedFile>>, SchemaRef) {
        let store_url = ObjectStoreUrl::local_filesystem();
        let store = state.runtime_env().object_store(&store_url).unwrap();

        let filename = "1.json";
        let file_groups = partitioned_file_groups(
            TEST_DATA_BASE,
            filename,
            1,
            Arc::new(JsonFormat::default()),
            file_compression_type.to_owned(),
            work_dir,
        )
        .unwrap();
        let meta = file_groups
            .first()
            .unwrap()
            .first()
            .unwrap()
            .clone()
            .object_meta;
        let schema = JsonFormat::default()
            .with_file_compression_type(file_compression_type.to_owned())
            .infer_schema(state, &store, std::slice::from_ref(&meta))
            .await
            .unwrap();

        (store_url, file_groups, schema)
    }

    async fn test_additional_stores(
        file_compression_type: FileCompressionType,
        store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let url = Url::parse("file://").unwrap();
        ctx.register_object_store(&url, store.clone());
        let filename = "1.json";
        let tmp_dir = TempDir::new()?;
        let file_groups = partitioned_file_groups(
            TEST_DATA_BASE,
            filename,
            1,
            Arc::new(JsonFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )
        .unwrap();
        let path = file_groups
            .first()
            .unwrap()
            .first()
            .unwrap()
            .object_meta
            .location
            .as_ref();

        let store_url = ObjectStoreUrl::local_filesystem();
        let url: &Url = store_url.as_ref();
        let path_buf = Path::new(url.path()).join(path);
        let path = path_buf.to_str().unwrap();

        let ext = JsonFormat::default()
            .get_ext_with_compression(&file_compression_type)
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
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn nd_json_exec_file_without_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        use arrow::datatypes::DataType;

        let tmp_dir = TempDir::new()?;
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned(), tmp_dir.path()).await;

        let source = Arc::new(JsonSource::new());
        let conf = FileScanConfig::new(object_store_url, file_schema, source)
            .with_file_groups(file_groups)
            .with_limit(Some(3))
            .with_file_compression_type(file_compression_type.to_owned());
        let exec = conf.new_exec();

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
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn nd_json_exec_file_with_missing_column(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        use arrow::datatypes::DataType;

        let tmp_dir = TempDir::new()?;
        let (object_store_url, file_groups, actual_schema) =
            prepare_store(&state, file_compression_type.to_owned(), tmp_dir.path()).await;

        let mut builder = SchemaBuilder::from(actual_schema.fields());
        builder.push(Field::new("missing_col", DataType::Int32, true));

        let file_schema = Arc::new(builder.finish());
        let missing_field_idx = file_schema.fields.len() - 1;

        let source = Arc::new(JsonSource::new());
        let conf = FileScanConfig::new(object_store_url, file_schema, source)
            .with_file_groups(file_groups)
            .with_limit(Some(3))
            .with_file_compression_type(file_compression_type.to_owned());
        let exec = conf.new_exec();

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
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn nd_json_exec_file_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let tmp_dir = TempDir::new()?;
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned(), tmp_dir.path()).await;

        let source = Arc::new(JsonSource::new());
        let conf = FileScanConfig::new(object_store_url, file_schema, source)
            .with_file_groups(file_groups)
            .with_projection(Some(vec![0, 2]))
            .with_file_compression_type(file_compression_type.to_owned());
        let exec = conf.new_exec();
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
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn nd_json_exec_file_mixed_order_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let tmp_dir = TempDir::new()?;
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned(), tmp_dir.path()).await;

        let source = Arc::new(JsonSource::new());
        let conf = FileScanConfig::new(object_store_url, file_schema, source)
            .with_file_groups(file_groups)
            .with_projection(Some(vec![3, 0, 2]))
            .with_file_compression_type(file_compression_type.to_owned());
        let exec = conf.new_exec();
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
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(8),
        );

        let path = format!("{TEST_DATA_BASE}/1.json");

        // register json file with the execution context
        ctx.register_json("test", path.as_str(), NdJsonReadOptions::default())
            .await?;

        // register a local file system object store for /tmp directory
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
        let out_dir_url = "file://local/out/";
        let df = ctx.sql("SELECT a, b FROM test").await?;
        df.write_json(out_dir_url, DataFrameWriteOptions::new(), None)
            .await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new();

        // get name of first part
        let paths = fs::read_dir(&out_dir).unwrap();
        let mut part_0_name: String = "".to_owned();
        for path in paths {
            let name = path
                .unwrap()
                .path()
                .file_name()
                .expect("Should be a file name")
                .to_str()
                .expect("Should be a str")
                .to_owned();
            if name.ends_with("_0.json") {
                part_0_name = name;
                break;
            }
        }

        if part_0_name.is_empty() {
            panic!("Did not find part_0 in json output files!")
        }

        // register each partition as well as the top level dir
        let json_read_option = NdJsonReadOptions::default();
        ctx.register_json(
            "part0",
            &format!("{out_dir}/{part_0_name}"),
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
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn test_chunked_json(
        file_compression_type: FileCompressionType,
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) -> Result<()> {
        test_additional_stores(
            file_compression_type,
            Arc::new(ChunkedStore::new(
                Arc::new(LocalFileSystem::new()),
                chunk_size,
            )),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn write_json_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();
        // register a local file system object store for /tmp directory
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/data/corrupt.csv", options).await?;
        let out_dir_url = "file://local/out";
        let e = df
            .write_json(out_dir_url, DataFrameWriteOptions::new(), None)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!(e.strip_backtrace(), "Arrow error: Parser error: Error while parsing value d for column 0 at line 4");
        Ok(())
    }

    #[tokio::test]
    async fn ndjson_schema_infer_max_records() -> Result<()> {
        async fn read_test_data(schema_infer_max_records: usize) -> Result<SchemaRef> {
            let ctx = SessionContext::new();

            let options = NdJsonReadOptions {
                schema_infer_max_records,
                ..Default::default()
            };

            let batches = ctx
                .read_json("tests/data/4.json", options)
                .await?
                .collect()
                .await?;

            Ok(batches[0].schema())
        }

        // Use only the first 2 rows to infer the schema, those have 2 fields.
        let schema = read_test_data(2).await?;
        assert_eq!(schema.fields().len(), 2);

        // Use all rows to infer the schema, those have 5 fields.
        let schema = read_test_data(10).await?;
        assert_eq!(schema.fields().len(), 5);

        Ok(())
    }

    #[rstest(
        file_compression_type,
        case::uncompressed(FileCompressionType::UNCOMPRESSED),
        case::gzip(FileCompressionType::GZIP),
        case::bzip2(FileCompressionType::BZIP2),
        case::xz(FileCompressionType::XZ),
        case::zstd(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn test_json_with_repartitioning(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);

        let tmp_dir = TempDir::new()?;
        let (store_url, file_groups, _) =
            prepare_store(&ctx.state(), file_compression_type, tmp_dir.path()).await;

        // It's important to have less than `target_partitions` amount of file groups, to
        // trigger repartitioning.
        assert_eq!(
            file_groups.len(),
            1,
            "Expected prepared store with single file group"
        );

        let path = file_groups
            .first()
            .unwrap()
            .first()
            .unwrap()
            .object_meta
            .location
            .as_ref();

        let url: &Url = store_url.as_ref();
        let path_buf = Path::new(url.path()).join(path);
        let path = path_buf.to_str().unwrap();
        let ext = JsonFormat::default()
            .get_ext_with_compression(&file_compression_type)
            .unwrap();

        let read_option = NdJsonReadOptions::default()
            .file_compression_type(file_compression_type)
            .file_extension(ext.as_str());

        let df = ctx.read_json(path, read_option).await?;
        let res = df.collect().await;

        // Output sort order is nondeterministic due to multiple
        // target partitions. To handle it, assert compares sorted
        // result.
        assert_batches_sorted_eq!(
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
            &res?
        );
        Ok(())
    }
}
