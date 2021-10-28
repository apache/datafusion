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

//! Execution plan for reading CSV files

use crate::datasource::file_format::PhysicalPlanConfig;
use crate::datasource::object_store::ObjectStore;
use crate::datasource::PartitionedFile;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

use arrow::csv;
use arrow::datatypes::SchemaRef;
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use super::file_stream::{BatchIter, FileStream};

/// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    object_store: Arc<dyn ObjectStore>,
    file_groups: Vec<Vec<PartitionedFile>>,
    file_schema: SchemaRef,
    projected_schema: SchemaRef,
    statistics: Statistics,
    has_header: bool,
    delimiter: u8,
    projection: Option<Vec<usize>>,
    batch_size: usize,
    limit: Option<usize>,
    table_partition_cols: Vec<String>,
}

impl CsvExec {
    /// Create a new CSV reader execution plan provided file list and schema
    pub fn new(base_config: PhysicalPlanConfig, has_header: bool, delimiter: u8) -> Self {
        let (projected_schema, projected_statistics) = super::project(
            &base_config.projection,
            Arc::clone(&base_config.file_schema),
            base_config.statistics,
        );

        Self {
            object_store: base_config.object_store,
            file_groups: base_config.file_groups,
            file_schema: base_config.file_schema,
            statistics: projected_statistics,
            has_header,
            delimiter,
            projection: base_config.projection,
            projected_schema,
            batch_size: base_config.batch_size,
            limit: base_config.limit,
            table_partition_cols: base_config.table_partition_cols,
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
    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.has_header
    }
    /// A column delimiter
    pub fn delimiter(&self) -> u8 {
        self.delimiter
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
    /// Partitioning column names
    pub fn table_partition_cols(&self) -> &[String] {
        &self.table_partition_cols
    }
}

#[async_trait]
impl ExecutionPlan for CsvExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.file_groups.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
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

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let batch_size = self.batch_size;
        let file_schema = Arc::clone(&self.file_schema);
        let projection = self.projection.clone();
        let has_header = self.has_header;
        let delimiter = self.delimiter;
        let start_line = if has_header { 1 } else { 0 };

        let fun = move |file, remaining: &Option<usize>| {
            let bounds = remaining.map(|x| (0, x + start_line));
            Box::new(csv::Reader::new(
                file,
                Arc::clone(&file_schema),
                has_header,
                Some(delimiter),
                batch_size,
                bounds,
                projection.clone(),
            )) as BatchIter
        };

        Ok(Box::pin(FileStream::new(
            Arc::clone(&self.object_store),
            self.file_groups[partition].clone(),
            fun,
            Arc::clone(&self.projected_schema),
            self.limit,
            self.table_partition_cols.clone(),
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
                    "CsvExec: files={}, has_header={}, batch_size={}, limit={:?}",
                    super::FileGroupsDisplay(&self.file_groups),
                    self.has_header,
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
mod tests {
    use super::*;
    use crate::{
        datasource::object_store::local::{local_unpartitioned_file, LocalFileSystem},
        test::aggr_test_schema,
    };
    use futures::StreamExt;

    #[tokio::test]
    async fn csv_exec_with_projection() -> Result<()> {
        let file_schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::new(
            PhysicalPlanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema,
                file_groups: vec![vec![local_unpartitioned_file(path)]],
                statistics: Statistics::default(),
                projection: Some(vec![0, 2, 4]),
                batch_size: 1024,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );
        assert_eq!(13, csv.file_schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());
        let mut stream = csv.execute(0).await?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(3, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c3", batch_schema.field(1).name());
        assert_eq!("c5", batch_schema.field(2).name());
        Ok(())
    }

    #[tokio::test]
    async fn csv_exec_without_projection() -> Result<()> {
        let file_schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::new(
            PhysicalPlanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema,
                file_groups: vec![vec![local_unpartitioned_file(path)]],
                statistics: Statistics::default(),
                projection: None,
                batch_size: 1024,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );
        assert_eq!(13, csv.file_schema.fields().len());
        assert_eq!(13, csv.projected_schema.fields().len());
        assert_eq!(13, csv.schema().fields().len());
        let mut it = csv.execute(0).await?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(13, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c2", batch_schema.field(1).name());
        assert_eq!("c3", batch_schema.field(2).name());
        Ok(())
    }
}
