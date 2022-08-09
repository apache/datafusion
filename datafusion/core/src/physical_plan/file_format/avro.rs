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
use crate::error::Result;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::datatypes::SchemaRef;

use crate::execution::context::TaskContext;
use crate::physical_plan::metrics::ExecutionPlanMetricsSet;
use std::any::Any;
use std::sync::Arc;

use super::FileScanConfig;

/// Execution plan for scanning Avro data source
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AvroExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl AvroExec {
    /// Create a new Avro reader execution plan provided base configurations
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
}

impl ExecutionPlan for AvroExec {
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
        let config = Arc::new(private::AvroConfig {
            schema: Arc::clone(&self.base_config.file_schema),
            batch_size: context.session_config().batch_size(),
            projection: self.base_config.projected_file_column_names(),
        });
        let opener = private::AvroOpener { config };

        let stream = FileStream::new(
            &self.base_config,
            partition,
            context,
            opener,
            self.metrics.clone(),
        )?;
        Ok(Box::pin(stream))
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
                    "AvroExec: files={}, limit={:?}",
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                    self.base_config.limit,
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

#[cfg(feature = "avro")]
mod private {
    use super::*;
    use crate::physical_plan::file_format::file_stream::{FileOpenFuture, FileOpener};
    use crate::physical_plan::file_format::FileMeta;
    use bytes::Buf;
    use futures::StreamExt;
    use object_store::{GetResult, ObjectStore};

    pub struct AvroConfig {
        pub schema: SchemaRef,
        pub batch_size: usize,
        pub projection: Option<Vec<String>>,
    }

    impl AvroConfig {
        fn open<R: std::io::Read>(
            &self,
            reader: R,
        ) -> Result<crate::avro_to_arrow::Reader<'static, R>> {
            crate::avro_to_arrow::Reader::try_new(
                reader,
                self.schema.clone(),
                self.batch_size,
                self.projection.clone(),
            )
        }
    }

    pub struct AvroOpener {
        pub config: Arc<AvroConfig>,
    }

    impl FileOpener for AvroOpener {
        fn open(
            &self,
            store: Arc<dyn ObjectStore>,
            file_meta: FileMeta,
        ) -> Result<FileOpenFuture> {
            let config = self.config.clone();
            Ok(Box::pin(async move {
                match store.get(file_meta.location()).await? {
                    GetResult::File(file, _) => {
                        let reader = config.open(file)?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                    r @ GetResult::Stream(_) => {
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
    use crate::datasource::file_format::{avro::AvroFormat, FileFormat};
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::prelude::SessionContext;
    use crate::scalar::ScalarValue;
    use crate::test::object_store::local_unpartitioned_file;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;

    use super::*;

    #[tokio::test]
    async fn avro_exec_without_partition() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let store = Arc::new(LocalFileSystem::new()) as _;
        let meta = local_unpartitioned_file(filename);

        let file_schema = AvroFormat {}.infer_schema(&store, &[meta.clone()]).await?;

        let avro_exec = AvroExec::new(FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_groups: vec![vec![meta.into()]],
            file_schema,
            statistics: Statistics::default(),
            projection: Some(vec![0, 1, 2]),
            limit: None,
            table_partition_cols: vec![],
        });
        assert_eq!(avro_exec.output_partitioning().partition_count(), 1);

        let ctx = SessionContext::new();
        let mut results = avro_exec
            .execute(0, ctx.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = vec![
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
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let object_store = Arc::new(LocalFileSystem::new()) as _;
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let meta = local_unpartitioned_file(filename);
        let actual_schema = AvroFormat {}
            .infer_schema(&object_store, &[meta.clone()])
            .await?;

        let mut fields = actual_schema.fields().clone();
        fields.push(Field::new("missing_col", DataType::Int32, true));

        let file_schema = Arc::new(Schema::new(fields));
        // Include the missing column in the projection
        let projection = Some(vec![0, 1, 2, actual_schema.fields().len()]);

        let avro_exec = AvroExec::new(FileScanConfig {
            object_store_url,
            file_groups: vec![vec![meta.into()]],
            file_schema,
            statistics: Statistics::default(),
            projection,
            limit: None,
            table_partition_cols: vec![],
        });
        assert_eq!(avro_exec.output_partitioning().partition_count(), 1);

        let ctx = SessionContext::new();
        let mut results = avro_exec
            .execute(0, ctx.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = vec![
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
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let object_store = Arc::new(LocalFileSystem::new()) as _;
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let meta = local_unpartitioned_file(filename);
        let file_schema = AvroFormat {}
            .infer_schema(&object_store, &[meta.clone()])
            .await?;

        let mut partitioned_file = PartitionedFile::from(meta);
        partitioned_file.partition_values =
            vec![ScalarValue::Utf8(Some("2021-10-26".to_owned()))];

        let avro_exec = AvroExec::new(FileScanConfig {
            // select specific columns of the files as well as the partitioning
            // column which is supposed to be the last column in the table schema.
            projection: Some(vec![0, 1, file_schema.fields().len(), 2]),
            object_store_url,
            file_groups: vec![vec![partitioned_file]],
            file_schema,
            statistics: Statistics::default(),
            limit: None,
            table_partition_cols: vec!["date".to_owned()],
        });
        assert_eq!(avro_exec.output_partitioning().partition_count(), 1);

        let ctx = SessionContext::new();
        let mut results = avro_exec
            .execute(0, ctx.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = vec![
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
