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
use crate::datasource::object_store::ObjectStore;
use crate::datasource::PartitionedFile;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::datatypes::{Schema, SchemaRef};
#[cfg(feature = "avro")]
use arrow::error::ArrowError;

use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

#[cfg(feature = "avro")]
use super::file_stream::{BatchIter, FileStream};

/// Execution plan for scanning Avro data source
#[derive(Debug, Clone)]
pub struct AvroExec {
    object_store: Arc<dyn ObjectStore>,
    file_groups: Vec<Vec<PartitionedFile>>,
    statistics: Statistics,
    file_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    batch_size: usize,
    limit: Option<usize>,
}

impl AvroExec {
    /// Create a new Avro reader execution plan provided file list and schema
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        file_groups: Vec<Vec<PartitionedFile>>,
        statistics: Statistics,
        file_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = match &projection {
            None => Arc::clone(&file_schema),
            Some(p) => Arc::new(Schema::new(
                p.iter().map(|i| file_schema.field(*i).clone()).collect(),
            )),
        };

        Self {
            object_store,
            file_groups,
            statistics,
            file_schema,
            projection,
            projected_schema,
            batch_size,
            limit,
        }
    }
    /// List of data files
    pub fn file_groups(&self) -> &[Vec<PartitionedFile>] {
        &self.file_groups
    }
    /// The schema before projection
    pub fn file_schema(&self) -> &SchemaRef {
        &self.file_schema
    }
    /// Optional projection for which columns to load
    pub fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }
    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
    /// Limit in nr. of rows
    pub fn limit(&self) -> Option<usize> {
        self.limit
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
        Partitioning::UnknownPartitioning(self.file_groups.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    #[cfg(not(feature = "avro"))]
    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::NotImplemented(
            "Cannot execute avro plan without avro feature enabled".to_string(),
        ))
    }

    #[cfg(feature = "avro")]
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let proj = self.projection.as_ref().map(|p| {
            p.iter()
                .map(|col_idx| self.file_schema.field(*col_idx).name())
                .cloned()
                .collect()
        });

        let batch_size = self.batch_size;
        let file_schema = Arc::clone(&self.file_schema);

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
            Arc::clone(&self.object_store),
            self.file_groups[partition].clone(),
            fun,
            Arc::clone(&self.projected_schema),
            self.limit,
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
                    "AvroExec: files={}, batch_size={}, limit={:?}",
                    super::FileGroupsDisplay(&self.file_groups),
                    self.batch_size,
                    self.limit,
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}

#[cfg(test)]
#[cfg(feature = "avro")]
mod tests {

    use crate::datasource::object_store::local::{
        local_file_meta, local_object_reader_stream, LocalFileSystem,
    };

    use super::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        use futures::StreamExt;

        use crate::datasource::file_format::{avro::AvroFormat, FileFormat};

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let avro_exec = AvroExec::new(
            Arc::new(LocalFileSystem {}),
            vec![vec![PartitionedFile {
                file_meta: local_file_meta(filename.clone()),
            }]],
            Statistics::default(),
            AvroFormat {}
                .infer_schema(local_object_reader_stream(vec![filename]))
                .await?,
            Some(vec![0, 1, 2]),
            1024,
            None,
        );
        assert_eq!(avro_exec.output_partitioning().partition_count(), 1);

        let mut results = avro_exec.execute(0).await?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }
}
