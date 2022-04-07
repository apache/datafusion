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
#[cfg(feature = "avro")]
use crate::avro_to_arrow;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::datatypes::SchemaRef;
#[cfg(feature = "avro")]
use arrow::error::ArrowError;

use crate::execution::context::TaskContext;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

#[cfg(feature = "avro")]
use super::file_stream::{BatchIter, FileStream};
use super::FileScanConfig;

/// Execution plan for scanning Avro data source
#[derive(Debug, Clone)]
pub struct AvroExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
}

impl AvroExec {
    /// Create a new Avro reader execution plan provided base configurations
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
        }
    }
    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
}

#[async_trait]
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
    async fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::NotImplemented(
            "Cannot execute avro plan without avro feature enabled".to_string(),
        ))
    }

    #[cfg(feature = "avro")]
    async fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let proj = self.base_config.projected_file_column_names();

        let batch_size = context.session_config().batch_size;
        let file_schema = Arc::clone(&self.base_config.file_schema);

        // The avro reader cannot limit the number of records, so `remaining` is ignored.
        let fun = move |file, _remaining: &Option<usize>| {
            let reader_res = avro_to_arrow::Reader::try_new(
                file,
                Arc::clone(&file_schema),
                batch_size,
                proj.clone(),
            );
            match reader_res {
                Ok(r) => Box::new(r) as BatchIter,
                Err(e) => Box::new(
                    vec![Err(ArrowError::ExternalError(Box::new(e)))].into_iter(),
                ),
            }
        };

        Ok(Box::pin(FileStream::new(
            Arc::clone(&self.base_config.object_store),
            self.base_config.file_groups[partition].clone(),
            fun,
            Arc::clone(&self.projected_schema),
            self.base_config.limit,
            self.base_config.table_partition_cols.clone(),
        )))
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

#[cfg(test)]
#[cfg(feature = "avro")]
mod tests {
    use crate::datasource::object_store::local::{
        local_object_reader_stream, LocalFileSystem,
    };
    use crate::datasource::{
        file_format::{avro::AvroFormat, FileFormat},
        listing::local_unpartitioned_file,
    };
    use crate::scalar::ScalarValue;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use sqlparser::ast::ObjectType::Schema;

    use super::*;

    #[tokio::test]
    async fn avro_exec_without_partition() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let avro_exec = AvroExec::new(FileScanConfig {
            object_store: Arc::new(LocalFileSystem {}),
            file_groups: vec![vec![local_unpartitioned_file(filename.clone())]],
            file_schema: AvroFormat {}
                .infer_schema(local_object_reader_stream(vec![filename]))
                .await?,
            statistics: Statistics::default(),
            projection: Some(vec![0, 1, 2]),
            limit: None,
            table_partition_cols: vec![],
        });
        assert_eq!(avro_exec.output_partitioning().partition_count(), 1);

        let mut results = avro_exec.execute(0).await.expect("plan execution failed");
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
        let actual_schema = AvroFormat {}
            .infer_schema(local_object_reader_stream(vec![filename]))
            .await?;

        let mut fields = actual_schema.fields().clone();
        fields.push(Field::new("missing_col", DataType::Int32, true));

        let file_schema = Arc::new(Schema::new(fields));

        let avro_exec = AvroExec::new(FileScanConfig {
            object_store: Arc::new(LocalFileSystem {}),
            file_groups: vec![vec![local_unpartitioned_file(filename.clone())]],
            file_schema,
            statistics: Statistics::default(),
            // Include the missing column in the projection
            projection: Some(vec![0, 1, 2, file_schema.fields().len()]),
            limit: None,
            table_partition_cols: vec![],
        });
        assert_eq!(avro_exec.output_partitioning().partition_count(), 1);

        let mut results = avro_exec.execute(0).await.expect("plan execution failed");
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
        let mut partitioned_file = local_unpartitioned_file(filename.clone());
        partitioned_file.partition_values =
            vec![ScalarValue::Utf8(Some("2021-10-26".to_owned()))];
        let file_schema = AvroFormat {}
            .infer_schema(local_object_reader_stream(vec![filename]))
            .await?;

        let avro_exec = AvroExec::new(FileScanConfig {
            // select specific columns of the files as well as the partitioning
            // column which is supposed to be the last column in the table schema.
            projection: Some(vec![0, 1, file_schema.fields().len(), 2]),
            object_store: Arc::new(LocalFileSystem {}),
            file_groups: vec![vec![partitioned_file]],
            file_schema: file_schema,
            statistics: Statistics::default(),
            limit: None,
            table_partition_cols: vec!["date".to_owned()],
        });
        assert_eq!(avro_exec.output_partitioning().partition_count(), 1);

        let mut results = avro_exec.execute(0).await.expect("plan execution failed");
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
