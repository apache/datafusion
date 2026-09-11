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

//! Parquet access through a bound storage backend.
//!
//! Metadata inference and scans use the same Storage and metadata decoder.
//! The table owns its binding; replacing a runtime registration affects only
//! subsequently constructed tables. No ObjectStore client is used by this path.
use crate::ParquetFileReaderFactory;
use crate::source::ParquetSource;
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_common::{DFSchema, DataFusionError, Result};
use datafusion_datasource::{
    PartitionedFile, file_scan_config::FileScanConfigBuilder, source::DataSourceExec,
};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::{ExecutionPlan, metrics::ExecutionPlanMetricsSet};
use datafusion_session::{Session, TableProvider};
use datafusion_storage::{FileAccessContext, FileInfo, StorageUrl};
use parquet::arrow::parquet_to_arrow_schema;
use std::sync::Arc;

/// A fixed set of Parquet files and the backend that discovered them.
///
/// Manifest callers can supply file metadata directly and omit listing. Files
/// discovered from a URL are snapshotted at construction; construct a new table
/// to discover additional files. Schema inference reads through the same binding
/// used by every subsequent scan.
#[derive(Debug)]
pub struct StorageParquetTable {
    storage: Arc<datafusion_storage::StorageBinding>,
    url: StorageUrl,
    files: Vec<FileInfo>,
    schema: SchemaRef,
    reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
}
impl StorageParquetTable {
    /// Build a table from explicit file metadata. An explicit schema avoids
    /// metadata I/O during construction and permits an empty file list.
    pub async fn try_new(
        storage: Arc<datafusion_storage::StorageBinding>,
        files: Vec<FileInfo>,
        schema: Option<SchemaRef>,
    ) -> Result<Self> {
        let url = storage.url().clone();
        let schema = match schema {
            Some(schema) => schema,
            None => {
                if files.is_empty() {
                    return Err(DataFusionError::Plan(
                        "cannot infer schema from an empty file list".into(),
                    ));
                }
                let context = FileAccessContext::new("parquet-schema-inference");
                let _cancel_on_drop = context.cancellation.clone().drop_guard();
                let mut schemas = Vec::with_capacity(files.len());
                for file in &files {
                    let factory =
                        crate::DefaultParquetFileReaderFactory::new(Arc::clone(&storage))
                            .with_context(context.clone());
                    let mut reader = factory.create_reader(
                        0,
                        PartitionedFile::new_from_meta(file.clone()),
                        None,
                        &ExecutionPlanMetricsSet::new(),
                    )?;
                    let metadata = reader.get_metadata(None).await?;
                    schemas.push(parquet_to_arrow_schema(
                        metadata.file_metadata().schema_descr(),
                        metadata.file_metadata().key_value_metadata(),
                    )?);
                }
                Arc::new(Schema::try_merge(schemas)?)
            }
        };
        Ok(Self {
            storage,
            url,
            files,
            schema,
            reader_factory: None,
        })
    }

    /// Override the format-level reader. Inference can be avoided by supplying
    /// a schema to `try_new`; the scan never replaces this custom factory.
    pub fn with_parquet_file_reader_factory(
        mut self,
        factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.reader_factory = Some(factory);
        self
    }
}
#[async_trait]
impl TableProvider for StorageParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut source = ParquetSource::new(Arc::clone(&self.schema))
            .with_storage(Arc::clone(&self.storage))
            .with_pushdown_filters(true);
        if let Some(factory) = &self.reader_factory {
            source = source.with_parquet_file_reader_factory(Arc::clone(factory));
        }
        if let Some(predicate) =
            datafusion_expr::utils::conjunction(filters.iter().cloned())
        {
            source = source.with_predicate(session.create_physical_expr(
                predicate,
                &DFSchema::try_from(Arc::clone(&self.schema))?,
            )?);
        }
        let mut builder = FileScanConfigBuilder::new(self.url.clone(), Arc::new(source));
        for file in &self.files {
            let partition = PartitionedFile::new_from_meta(file.clone());
            builder = builder.with_file(partition);
        }
        let config = builder
            .with_projection_indices(projection.map(<[usize]>::to_vec))?
            .with_limit(if filters.is_empty() { limit } else { None })
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}
