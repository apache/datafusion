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
use async_trait::async_trait;

use crate::datasource::object_store::ObjectStore;
use crate::datasource::PartitionedFile;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::{
    datatypes::{Schema, SchemaRef},
    json,
};
use std::any::Any;
use std::sync::Arc;

use super::file_stream::{BatchIter, FileStream};

/// Execution plan for scanning NdJson data source
#[derive(Debug, Clone)]
pub struct NdJsonExec {
    object_store: Arc<dyn ObjectStore>,
    file_groups: Vec<Vec<PartitionedFile>>,
    statistics: Statistics,
    file_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    batch_size: usize,
    limit: Option<usize>,
}

impl NdJsonExec {
    /// Create a new JSON reader execution plan provided file list and schema
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
}

#[async_trait]
impl ExecutionPlan for NdJsonExec {
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
            Ok(Arc::new(self.clone()) as Arc<dyn ExecutionPlan>)
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let proj = self.projection.as_ref().map(|p| {
            p.iter()
                .map(|col_idx| self.file_schema.field(*col_idx).name())
                .cloned()
                .collect()
        });

        let batch_size = self.batch_size;
        let file_schema = Arc::clone(&self.file_schema);

        // The json reader cannot limit the number of records, so `remaining` is ignored.
        let fun = move |file, _remaining: &Option<usize>| {
            Box::new(json::Reader::new(
                file,
                Arc::clone(&file_schema),
                batch_size,
                proj.clone(),
            )) as BatchIter
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
                    "JsonExec: batch_size={}, limit={:?}, files={}",
                    self.batch_size,
                    self.limit,
                    super::FileGroupsDisplay(&self.file_groups),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::datasource::{
        file_format::{json::JsonFormat, FileFormat},
        object_store::local::{
            local_file_meta, local_object_reader_stream, LocalFileSystem,
        },
    };

    use super::*;

    const TEST_DATA_BASE: &str = "tests/jsons";

    async fn infer_schema(path: String) -> Result<SchemaRef> {
        JsonFormat::default()
            .infer_schema(local_object_reader_stream(vec![path]))
            .await
    }

    #[tokio::test]
    async fn nd_json_exec_file_without_projection() -> Result<()> {
        use arrow::datatypes::DataType;
        let path = format!("{}/1.json", TEST_DATA_BASE);
        let exec = NdJsonExec::new(
            Arc::new(LocalFileSystem {}),
            vec![vec![PartitionedFile {
                file_meta: local_file_meta(path.clone()),
            }]],
            Default::default(),
            infer_schema(path).await?,
            None,
            1024,
            Some(3),
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

        let mut it = exec.execute(0).await?;
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
    async fn nd_json_exec_file_projection() -> Result<()> {
        let path = format!("{}/1.json", TEST_DATA_BASE);
        let exec = NdJsonExec::new(
            Arc::new(LocalFileSystem {}),
            vec![vec![PartitionedFile {
                file_meta: local_file_meta(path.clone()),
            }]],
            Default::default(),
            infer_schema(path).await?,
            Some(vec![0, 2]),
            1024,
            None,
        );
        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 2);

        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap_err();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap_err();

        let mut it = exec.execute(0).await?;
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
}
