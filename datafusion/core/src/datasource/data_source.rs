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

//! DataSource and FileSource trait implementations

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::{
    FileGroupPartitioner, FileOpener, FileScanConfig, FileStream,
};

use arrow_schema::SchemaRef;
use datafusion_common::Statistics;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::source::{DataSource, DataSourceExec};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType};

use object_store::ObjectStore;

/// Common behaviors that every `FileSourceConfig` needs to implement.
pub trait FileSource: Send + Sync {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: datafusion_common::Result<Arc<dyn ObjectStore>>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion_common::Result<Arc<dyn FileOpener>>;
    /// Any
    fn as_any(&self) -> &dyn Any;
    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource>;
    /// Initialize new instance with a new schema
    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource>;
    /// Initialize new instance with projection information
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource>;
    /// Initialize new instance with projected statistics
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource>;
    /// Return execution plan metrics
    fn metrics(&self) -> &ExecutionPlanMetricsSet;
    /// Return projected statistics
    fn statistics(&self) -> datafusion_common::Result<Statistics>;
    /// Returns the file type such as Arrow, Avro, Parquet, ...
    fn file_type(&self) -> FileType;
    /// Format FileType specific information
    fn fmt_extra(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }
}

/// Determines file types
pub enum FileType {
    /// Arrow File
    Arrow,
    /// Avro File
    Avro,
    /// CSV File
    Csv,
    /// JSON File
    Json,
    /// Parquet File
    Parquet,
}

impl FileType {
    fn to_str(&self) -> &str {
        match self {
            FileType::Arrow => "arrow",
            FileType::Avro => "avro",
            FileType::Csv => "csv",
            FileType::Json => "json",
            FileType::Parquet => "parquet",
        }
    }

    /// Is the file type avro?
    pub fn is_avro(&self) -> bool {
        matches!(self, FileType::Avro)
    }

    /// Is the file type csv?
    pub fn is_csv(&self) -> bool {
        matches!(self, FileType::Csv)
    }

    /// Is the file type parquet?
    pub fn is_parquet(&self) -> bool {
        matches!(self, FileType::Parquet)
    }
}

/// Holds generic file configuration, and common behaviors for file sources.
/// Can be initialized with a `FileScanConfig`
/// and a `dyn FileSource` type such as `CsvSource`, `ParquetSource`, `AvroSource`, etc.
#[derive(Clone)]
pub struct FileSourceConfig {
    source: Arc<dyn FileSource>,
    base_config: FileScanConfig,
}

impl FileSourceConfig {
    // TODO: This function should be moved into DataSourceExec once FileScanConfig and FileSourceConfig moved out of datafusion/core
    /// Returns a new [`DataSourceExec`] from file configurations
    pub fn new_exec(
        base_config: FileScanConfig,
        file_source: Arc<dyn FileSource>,
    ) -> Arc<DataSourceExec> {
        let source = Arc::new(Self::new(base_config, file_source));
        Arc::new(DataSourceExec::new(source))
    }

    /// Initialize a new `FileSourceConfig` instance.
    pub fn new(base_config: FileScanConfig, file_source: Arc<dyn FileSource>) -> Self {
        let (
            _projected_schema,
            _constraints,
            projected_statistics,
            _projected_output_ordering,
        ) = base_config.project();
        let file_source = file_source.with_statistics(projected_statistics);

        Self {
            source: file_source,
            base_config,
        }
    }

    /// Write the data_type based on file_source
    fn fmt_file_source(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, ", file_type={}", self.source.file_type().to_str())?;
        self.source.fmt_extra(t, f)
    }

    /// Returns the base_config
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// Returns the file_source
    pub fn file_source(&self) -> &Arc<dyn FileSource> {
        &self.source
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.base_config.file_groups = file_groups;
        self
    }

    fn supports_repartition(&self) -> bool {
        !(self.base_config.file_compression_type.is_compressed()
            || self.base_config.new_lines_in_values
            || self.source.file_type().is_avro())
    }
}

impl DataSource for FileSourceConfig {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url);

        let source = self
            .source
            .with_batch_size(context.session_config().batch_size())
            .with_schema(Arc::clone(&self.base_config.file_schema))
            .with_projection(&self.base_config);

        let opener =
            source.create_file_opener(object_store, &self.base_config, partition)?;

        let stream =
            FileStream::new(&self.base_config, partition, opener, source.metrics())?;
        Ok(Box::pin(stream))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.base_config.fmt_as(t, f)?;
        self.fmt_file_source(t, f)
    }

    /// Redistribute files across partitions according to their size
    /// See comments on [`FileGroupPartitioner`] for more detail.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
    ) -> datafusion_common::Result<Option<Arc<dyn DataSource>>> {
        if !self.supports_repartition() {
            return Ok(None);
        }

        let repartitioned_file_groups_option = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(output_ordering.is_some())
            .repartition_file_groups(&self.base_config.file_groups);

        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            let source = self.clone().with_file_groups(repartitioned_file_groups);
            return Ok(Some(Arc::new(source)));
        }
        Ok(None)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        let (schema, constraints, _, orderings) = self.base_config.project();
        EquivalenceProperties::new_with_orderings(schema, orderings.as_slice())
            .with_constraints(constraints)
    }

    fn statistics(&self) -> datafusion_common::Result<Statistics> {
        self.source.statistics()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let config = self.base_config.clone().with_limit(limit);
        Some(Arc::new(Self {
            source: Arc::clone(&self.source),
            base_config: config,
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.base_config.limit
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.source.metrics().clone()
    }
}
