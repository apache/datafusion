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

//! [`ParquetMorselizer`] — row-group-level morselizer for parquet files.

use std::fmt;
use std::sync::Arc;

use crate::{ParquetAccessPlan, ParquetFileReaderFactory};

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::merge_partitions::{MorselResult, Morselizer, WorkItem};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use futures::StreamExt;
use futures::future::BoxFuture;
use parquet::arrow::async_reader::AsyncFileReader;

/// Work item types for the parquet morselizer.
enum ParquetWorkItem {
    /// A file that hasn't been morselized yet. Metadata will be read lazily
    /// when this item is executed, and the file will be split into row groups.
    File(PartitionedFile),
    /// A single row group within a file. The `PartitionedFile` carries a
    /// `ParquetAccessPlan` extension that selects exactly one row group.
    RowGroup(PartitionedFile),
}

/// A [`Morselizer`] that expands parquet files into row-group-level work items.
///
/// When a file work item is executed:
/// 1. Metadata is read lazily (first access populates the metadata cache)
/// 2. The file is split into one work item per row group
/// 3. The first row group is executed immediately
/// 4. Remaining row groups are pushed to the shared queue for other streams
///
/// When a row-group work item is executed, it is opened directly via the
/// opener (metadata comes from the cache).
pub(crate) struct ParquetMorselizer {
    /// Files to scan
    files: Vec<PartitionedFile>,
    /// Opener for creating streams for individual files/row groups
    opener: Arc<dyn FileOpener>,
    /// Factory for creating readers (used for metadata reads)
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Metrics for the reader factory
    metrics: ExecutionPlanMetricsSet,
    /// Metadata size hint
    metadata_size_hint: Option<usize>,
    /// Output schema
    schema: SchemaRef,
}

impl fmt::Debug for ParquetMorselizer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetMorselizer")
            .field("num_files", &self.files.len())
            .finish()
    }
}

impl ParquetMorselizer {
    pub(crate) fn new(
        files: Vec<PartitionedFile>,
        opener: Arc<dyn FileOpener>,
        reader_factory: Arc<dyn ParquetFileReaderFactory>,
        metrics: ExecutionPlanMetricsSet,
        metadata_size_hint: Option<usize>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            files,
            opener,
            reader_factory,
            metrics,
            metadata_size_hint,
            schema,
        }
    }
}

impl Morselizer for ParquetMorselizer {
    fn initial_items(&self) -> Vec<WorkItem> {
        self.files
            .iter()
            .map(|f| WorkItem::new(ParquetWorkItem::File(f.clone())))
            .collect()
    }

    fn execute_item(
        &self,
        item: WorkItem,
        _context: Arc<TaskContext>,
    ) -> Result<BoxFuture<'static, Result<MorselResult>>> {
        let work_item: ParquetWorkItem = item
            .downcast()
            .expect("ParquetMorselizer work item should be ParquetWorkItem");

        match work_item {
            ParquetWorkItem::File(file) => self.execute_file_item(file),
            ParquetWorkItem::RowGroup(file) => self.execute_row_group_item(file),
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl ParquetMorselizer {
    /// Execute a file-level work item: read metadata, split into row groups.
    fn execute_file_item(
        &self,
        file: PartitionedFile,
    ) -> Result<BoxFuture<'static, Result<MorselResult>>> {
        // Create a reader to fetch metadata
        let mut reader: Box<dyn AsyncFileReader + Send> =
            self.reader_factory.create_reader(
                0, // partition index doesn't matter for metadata reads
                file.clone(),
                self.metadata_size_hint,
                &self.metrics,
            )?;
        let opener = Arc::clone(&self.opener);

        Ok(Box::pin(async move {
            // Read metadata (this populates the cache for subsequent reads)
            let metadata = reader
                .get_metadata(None)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            let num_row_groups = metadata.num_row_groups();

            if num_row_groups == 0 {
                // No row groups — return an empty stream
                let empty: Vec<Result<arrow::record_batch::RecordBatch>> = vec![];
                return Ok(MorselResult {
                    stream: futures::stream::iter(empty).boxed(),
                    additional_items: vec![],
                });
            }

            // Create a work item for each row group except the first
            let mut additional_items =
                Vec::with_capacity(num_row_groups.saturating_sub(1));
            for rg_idx in 1..num_row_groups {
                let mut access_plan = ParquetAccessPlan::new_none(num_row_groups);
                access_plan.scan(rg_idx);
                let mut rg_file = file.clone();
                rg_file.extensions = Some(Arc::new(access_plan));
                additional_items.push(WorkItem::new(ParquetWorkItem::RowGroup(rg_file)));
            }

            // Execute the first row group
            let mut first_file = file;
            let mut access_plan = ParquetAccessPlan::new_none(num_row_groups);
            access_plan.scan(0);
            first_file.extensions = Some(Arc::new(access_plan));

            let future = opener.open(first_file)?;
            let stream = future.await?;

            Ok(MorselResult {
                stream,
                additional_items,
            })
        }))
    }

    /// Execute a row-group-level work item: open the specific row group.
    fn execute_row_group_item(
        &self,
        file: PartitionedFile,
    ) -> Result<BoxFuture<'static, Result<MorselResult>>> {
        let future = self.opener.open(file)?;

        Ok(Box::pin(async move {
            let stream = future.await?;
            Ok(MorselResult {
                stream,
                additional_items: vec![],
            })
        }))
    }
}
