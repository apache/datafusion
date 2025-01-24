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

//! Execution plan for reading line-delimited Avro files

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use super::{FileOpener, FileScanConfig};
#[cfg(feature = "avro")]
use crate::datasource::avro_to_arrow::Reader as AvroReader;
use crate::datasource::data_source::{FileSource, FileType};
use crate::error::Result;

use arrow::datatypes::SchemaRef;
use datafusion_common::{Constraints, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

use object_store::ObjectStore;

/// Execution plan for scanning Avro data source
#[derive(Debug, Clone)]
#[deprecated(since = "46.0.0", note = "use DataSourceExec instead")]
pub struct AvroExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

#[allow(unused, deprecated)]
impl AvroExec {
    /// Create a new Avro reader execution plan provided base configurations
    pub fn new(base_config: FileScanConfig) -> Self {
        let (
            projected_schema,
            projected_constraints,
            projected_statistics,
            projected_output_ordering,
        ) = base_config.project();
        let cache = Self::compute_properties(
            Arc::clone(&projected_schema),
            &projected_output_ordering,
            projected_constraints,
            &base_config,
        );
        Self {
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
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
        let n_partitions = file_scan_config.file_groups.len();

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(n_partitions), // Output Partitioning
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

#[allow(unused, deprecated)]
impl DisplayAs for AvroExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "AvroExec: ")?;
        self.base_config.fmt_as(t, f)
    }
}

#[allow(unused, deprecated)]
impl ExecutionPlan for AvroExec {
    fn name(&self) -> &'static str {
        "AvroExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
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
    #[cfg(not(feature = "avro"))]
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(crate::error::DataFusionError::NotImplemented(
            "Cannot execute avro plan without avro feature enabled".to_string(),
        ))
    }
    #[cfg(feature = "avro")]
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        use super::file_stream::FileStream;
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let config = Arc::new(private::DeprecatedAvroConfig {
            schema: Arc::clone(&self.base_config.file_schema),
            batch_size: context.session_config().batch_size(),
            projection: self.base_config.projected_file_column_names(),
            object_store,
        });
        let opener = private::DeprecatedAvroOpener { config };

        let stream = FileStream::new(
            &self.base_config,
            partition,
            Arc::new(opener),
            &self.metrics,
        )?;
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fetch(&self) -> Option<usize> {
        self.base_config.limit
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let new_config = self.base_config.clone().with_limit(limit);

        Some(Arc::new(Self {
            base_config: new_config,
            projected_statistics: self.projected_statistics.clone(),
            projected_schema: Arc::clone(&self.projected_schema),
            projected_output_ordering: self.projected_output_ordering.clone(),
            metrics: self.metrics.clone(),
            cache: self.cache.clone(),
        }))
    }
}

/// AvroSource holds the extra configuration that is necessary for opening avro files
#[derive(Clone, Default)]
pub struct AvroSource {
    schema: Option<SchemaRef>,
    batch_size: Option<usize>,
    projection: Option<Vec<String>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl AvroSource {
    /// Initialize an AvroSource with default values
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "avro")]
    fn open<R: std::io::Read>(&self, reader: R) -> Result<AvroReader<'static, R>> {
        AvroReader::try_new(
            reader,
            Arc::clone(self.schema.as_ref().expect("Schema must set before open")),
            self.batch_size.expect("Batch size must set before open"),
            self.projection.clone(),
        )
    }
}

impl FileSource for AvroSource {
    #[cfg(feature = "avro")]
    fn create_file_opener(
        &self,
        object_store: Result<Arc<dyn ObjectStore>>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(private::AvroOpener {
            config: Arc::new(self.clone()),
            object_store: object_store?,
        }))
    }

    #[cfg(not(feature = "avro"))]
    fn create_file_opener(
        &self,
        _object_store: Result<Arc<dyn ObjectStore>>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        panic!("Avro feature is not enabled in this build")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.schema = Some(schema);
        Arc::new(conf)
    }
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projection = config.projected_file_column_names();
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }

    fn file_type(&self) -> FileType {
        FileType::Avro
    }
}

#[cfg(feature = "avro")]
mod private {
    use super::*;
    use crate::datasource::physical_plan::file_stream::{FileOpenFuture, FileOpener};
    use crate::datasource::physical_plan::FileMeta;

    use bytes::Buf;
    use futures::StreamExt;
    use object_store::{GetResultPayload, ObjectStore};

    pub struct DeprecatedAvroConfig {
        pub schema: SchemaRef,
        pub batch_size: usize,
        pub projection: Option<Vec<String>>,
        pub object_store: Arc<dyn ObjectStore>,
    }

    impl DeprecatedAvroConfig {
        fn open<R: std::io::Read>(&self, reader: R) -> Result<AvroReader<'static, R>> {
            AvroReader::try_new(
                reader,
                Arc::clone(&self.schema),
                self.batch_size,
                self.projection.clone(),
            )
        }
    }

    pub struct DeprecatedAvroOpener {
        pub config: Arc<DeprecatedAvroConfig>,
    }
    impl FileOpener for DeprecatedAvroOpener {
        fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
            let config = Arc::clone(&self.config);
            Ok(Box::pin(async move {
                let r = config.object_store.get(file_meta.location()).await?;
                match r.payload {
                    GetResultPayload::File(file, _) => {
                        let reader = config.open(file)?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let reader = config.open(bytes.reader())?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                }
            }))
        }
    }

    pub struct AvroOpener {
        pub config: Arc<AvroSource>,
        pub object_store: Arc<dyn ObjectStore>,
    }

    impl FileOpener for AvroOpener {
        fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
            let config = Arc::clone(&self.config);
            let object_store = Arc::clone(&self.object_store);
            Ok(Box::pin(async move {
                let r = object_store.get(file_meta.location()).await?;
                match r.payload {
                    GetResultPayload::File(file, _) => {
                        let reader = config.open(file)?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let reader = config.open(bytes.reader())?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                }
            }))
        }
    }
}

#[cfg(test)]
#[cfg(feature = "avro")]
mod tests {
    use super::*;
    use crate::arrow::datatypes::{DataType, Field, SchemaBuilder};
    use crate::datasource::data_source::FileSourceConfig;
    use crate::datasource::file_format::{avro::AvroFormat, FileFormat};
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::prelude::SessionContext;
    use crate::scalar::ScalarValue;
    use crate::test::object_store::local_unpartitioned_file;

    use datafusion_physical_plan::ExecutionPlan;

    use futures::StreamExt;
    use object_store::chunked::ChunkedStore;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use rstest::*;
    use url::Url;

    #[tokio::test]
    async fn avro_exec_without_partition() -> Result<()> {
        test_with_stores(Arc::new(LocalFileSystem::new())).await
    }

    #[rstest]
    #[tokio::test]
    async fn test_chunked_avro(
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) -> Result<()> {
        test_with_stores(Arc::new(ChunkedStore::new(
            Arc::new(LocalFileSystem::new()),
            chunk_size,
        )))
        .await
    }

    async fn test_with_stores(store: Arc<dyn ObjectStore>) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let url = Url::parse("file://").unwrap();
        session_ctx.register_object_store(&url, store.clone());

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let meta = local_unpartitioned_file(filename);

        let file_schema = AvroFormat {}
            .infer_schema(&state, &store, std::slice::from_ref(&meta))
            .await?;

        let conf = FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema)
            .with_file(meta.into())
            .with_projection(Some(vec![0, 1, 2]));

        let source_config = Arc::new(AvroSource::new());
        let source_exec = FileSourceConfig::new_exec(conf, source_config);
        assert_eq!(
            source_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        let mut results = source_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = [
            "+----+----------+-------------+",
            "| id | bool_col | tinyint_col |",
            "+----+----------+-------------+",
            "| 4  | true     | 0           |",
            "| 5  | false    | 1           |",
            "| 6  | true     | 0           |",
            "| 7  | false    | 1           |",
            "| 2  | true     | 0           |",
            "| 3  | false    | 1           |",
            "| 0  | true     | 0           |",
            "| 1  | false    | 1           |",
            "+----+----------+-------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn avro_exec_missing_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let object_store = Arc::new(LocalFileSystem::new()) as _;
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let meta = local_unpartitioned_file(filename);
        let actual_schema = AvroFormat {}
            .infer_schema(&state, &object_store, std::slice::from_ref(&meta))
            .await?;

        let mut builder = SchemaBuilder::from(actual_schema.fields());
        builder.push(Field::new("missing_col", DataType::Int32, true));

        let file_schema = Arc::new(builder.finish());
        // Include the missing column in the projection
        let projection = Some(vec![0, 1, 2, actual_schema.fields().len()]);

        let conf = FileScanConfig::new(object_store_url, file_schema)
            .with_file(meta.into())
            .with_projection(projection);

        let source_config = Arc::new(AvroSource::new());
        let source_exec = FileSourceConfig::new_exec(conf, source_config);
        assert_eq!(
            source_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );

        let mut results = source_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = [
            "+----+----------+-------------+-------------+",
            "| id | bool_col | tinyint_col | missing_col |",
            "+----+----------+-------------+-------------+",
            "| 4  | true     | 0           |             |",
            "| 5  | false    | 1           |             |",
            "| 6  | true     | 0           |             |",
            "| 7  | false    | 1           |             |",
            "| 2  | true     | 0           |             |",
            "| 3  | false    | 1           |             |",
            "| 0  | true     | 0           |             |",
            "| 1  | false    | 1           |             |",
            "+----+----------+-------------+-------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn avro_exec_with_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let object_store = Arc::new(LocalFileSystem::new()) as _;
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let meta = local_unpartitioned_file(filename);
        let file_schema = AvroFormat {}
            .infer_schema(&state, &object_store, std::slice::from_ref(&meta))
            .await?;

        let mut partitioned_file = PartitionedFile::from(meta);
        partitioned_file.partition_values = vec![ScalarValue::from("2021-10-26")];

        let projection = Some(vec![0, 1, file_schema.fields().len(), 2]);
        let conf = FileScanConfig::new(object_store_url, file_schema)
            // select specific columns of the files as well as the partitioning
            // column which is supposed to be the last column in the table schema.
            .with_projection(projection)
            .with_file(partitioned_file)
            .with_table_partition_cols(vec![Field::new("date", DataType::Utf8, false)]);

        let source_config = Arc::new(AvroSource::new());
        let source_exec = FileSourceConfig::new_exec(conf, source_config);

        assert_eq!(
            source_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );

        let mut results = source_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = [
            "+----+----------+------------+-------------+",
            "| id | bool_col | date       | tinyint_col |",
            "+----+----------+------------+-------------+",
            "| 4  | true     | 2021-10-26 | 0           |",
            "| 5  | false    | 2021-10-26 | 1           |",
            "| 6  | true     | 2021-10-26 | 0           |",
            "| 7  | false    | 2021-10-26 | 1           |",
            "| 2  | true     | 2021-10-26 | 0           |",
            "| 3  | false    | 2021-10-26 | 1           |",
            "| 0  | true     | 2021-10-26 | 0           |",
            "| 1  | false    | 2021-10-26 | 1           |",
            "+----+----------+------------+-------------+",
        ];
        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }
}
