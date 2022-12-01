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

//! Execution plan for reading Parquet files

use arrow::datatypes::{DataType, SchemaRef};
use fmt::Debug;
use std::any::Any;
use std::fmt;
use std::fs;
use std::ops::Range;
use std::sync::Arc;

use crate::config::OPT_PARQUET_ENABLE_PAGE_INDEX;
use crate::config::OPT_PARQUET_PUSHDOWN_FILTERS;
use crate::config::OPT_PARQUET_REORDER_FILTERS;
use crate::datasource::file_format::parquet::fetch_parquet_metadata;
use crate::physical_plan::file_format::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::physical_plan::file_format::FileMeta;
use crate::{
    error::{DataFusionError, Result},
    execution::context::{SessionState, TaskContext},
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        expressions::PhysicalSortExpr,
        file_format::{FileScanConfig, SchemaAdapter},
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use arrow::error::ArrowError;
use bytes::Bytes;
use datafusion_expr::Expr;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use log::debug;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{ConvertedType, LogicalType};
use parquet::errors::ParquetError;
use parquet::file::{metadata::ParquetMetaData, properties::WriterProperties};
use parquet::schema::types::ColumnDescriptor;

mod metrics;
mod page_filter;
mod row_filter;
mod row_groups;

pub use metrics::ParquetFileMetrics;

use super::get_output_ordering;

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Override for `Self::with_pushdown_filters`. If None, uses
    /// values from base_config
    pushdown_filters: Option<bool>,
    /// Override for `Self::with_reorder_filters`. If None, uses
    /// values from base_config
    reorder_filters: Option<bool>,
    /// Override for `Self::with_enable_page_index`. If None, uses
    /// values from base_config
    enable_page_index: Option<bool>,
    /// Base configuraton for this scan
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for row filtering during parquet scan
    predicate: Option<Arc<Expr>>,
    /// Optional predicate for pruning row groups
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Optional hint for the size of the parquet metadata
    metadata_size_hint: Option<usize>,
    /// Optional user defined parquet file reader factory
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        base_config: FileScanConfig,
        predicate: Option<Expr>,
        metadata_size_hint: Option<usize>,
    ) -> Self {
        debug!("Creating ParquetExec, files: {:?}, projection {:?}, predicate: {:?}, limit: {:?}",
        base_config.file_groups, base_config.projection, predicate, base_config.limit);

        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        let pruning_predicate = predicate
            .clone()
            .and_then(|predicate_expr| {
                match PruningPredicate::try_new(
                    predicate_expr,
                    base_config.file_schema.clone(),
                ) {
                    Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                    Err(e) => {
                        debug!("Could not create pruning predicate for: {}", e);
                        predicate_creation_errors.add(1);
                        None
                    }
                }
            })
            .and_then(|pruning_predicate| {
                // If the pruning predicate can't prune anything, don't try
                if pruning_predicate.allways_true() {
                    None
                } else {
                    Some(pruning_predicate)
                }
            });

        // Save original predicate
        let predicate = predicate.map(Arc::new);

        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            pushdown_filters: None,
            reorder_filters: None,
            enable_page_index: None,
            base_config,
            projected_schema,
            projected_statistics,
            metrics,
            predicate,
            pruning_predicate,
            metadata_size_hint,
            parquet_file_reader_factory: None,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// Optional reference to this parquet scan's pruning predicate
    pub fn pruning_predicate(&self) -> Option<&Arc<PruningPredicate>> {
        self.pruning_predicate.as_ref()
    }

    /// Optional user defined parquet file reader factory.
    ///
    /// `ParquetFileReaderFactory` complements `TableProvider`, It enables users to provide custom
    /// implementation for data access operations.
    ///
    /// If custom `ParquetFileReaderFactory` is provided, then data access operations will be routed
    /// to this factory instead of `ObjectStore`.
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
    }

    /// If true, any filter [`Expr`]s on the scan will converted to a
    /// [`RowFilter`](parquet::arrow::arrow_reader::RowFilter) in the
    /// `ParquetRecordBatchStream`. These filters are applied by the
    /// parquet decoder to skip unecessairly decoding other columns
    /// which would not pass the predicate. Defaults to false
    pub fn with_pushdown_filters(mut self, pushdown_filters: bool) -> Self {
        self.pushdown_filters = Some(pushdown_filters);
        self
    }

    /// Return the value described in [`Self::with_pushdown_filters`]
    pub fn pushdown_filters(&self) -> bool {
        self.pushdown_filters
            .or_else(|| {
                self.base_config
                    .config_options
                    .read()
                    .get_bool(OPT_PARQUET_PUSHDOWN_FILTERS)
            })
            // default to false
            .unwrap_or_default()
    }

    /// If true, the `RowFilter` made by `pushdown_filters` may try to
    /// minimize the cost of filter evaluation by reordering the
    /// predicate [`Expr`]s. If false, the predicates are applied in
    /// the same order as specified in the query. Defaults to false.
    pub fn with_reorder_filters(mut self, reorder_filters: bool) -> Self {
        self.reorder_filters = Some(reorder_filters);
        self
    }

    /// Return the value described in [`Self::with_reorder_filters`]
    pub fn reorder_filters(&self) -> bool {
        self.reorder_filters
            .or_else(|| {
                self.base_config
                    .config_options
                    .read()
                    .get_bool(OPT_PARQUET_REORDER_FILTERS)
            })
            // default to false
            .unwrap_or_default()
    }

    /// If enabled, the reader will read the page index
    /// This is used to optimise filter pushdown
    /// via `RowSelector` and `RowFilter` by
    /// eliminating unnecessary IO and decoding
    pub fn with_enable_page_index(mut self, enable_page_index: bool) -> Self {
        self.enable_page_index = Some(enable_page_index);
        self
    }

    /// Return the value described in [`Self::with_enable_page_index`]
    pub fn enable_page_index(&self) -> bool {
        self.enable_page_index
            .or_else(|| {
                self.base_config
                    .config_options
                    .read()
                    .get_bool(OPT_PARQUET_ENABLE_PAGE_INDEX)
            })
            // default to false
            .unwrap_or_default()
    }
}

impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        get_output_ordering(&self.base_config)
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition_index: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let parquet_file_reader_factory = self
            .parquet_file_reader_factory
            .as_ref()
            .map(|f| Ok(Arc::clone(f)))
            .unwrap_or_else(|| {
                ctx.runtime_env()
                    .object_store(&self.base_config.object_store_url)
                    .map(|store| {
                        Arc::new(DefaultParquetFileReaderFactory::new(store))
                            as Arc<dyn ParquetFileReaderFactory>
                    })
            })?;

        let opener = ParquetOpener {
            partition_index,
            projection: Arc::from(projection),
            batch_size: ctx.session_config().batch_size(),
            predicate: self.predicate.clone(),
            pruning_predicate: self.pruning_predicate.clone(),
            table_schema: self.base_config.file_schema.clone(),
            metadata_size_hint: self.metadata_size_hint,
            metrics: self.metrics.clone(),
            parquet_file_reader_factory,
            pushdown_filters: self.pushdown_filters(),
            reorder_filters: self.reorder_filters(),
            enable_page_index: self.enable_page_index(),
        };

        let stream = FileStream::new(
            &self.base_config,
            partition_index,
            ctx,
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
                let predicate_string = self
                    .predicate
                    .as_ref()
                    .map(|p| format!(", predicate={}", p))
                    .unwrap_or_default();

                let pruning_predicate_string = self
                    .pruning_predicate
                    .as_ref()
                    .map(|pre| format!(", pruning_predicate={}", pre.predicate_expr()))
                    .unwrap_or_default();

                let output_ordering_string = self
                    .output_ordering()
                    .map(make_output_ordering_string)
                    .unwrap_or_default();

                write!(
                    f,
                    "ParquetExec: limit={:?}, partitions={}{}{}{}, projection={}",
                    self.base_config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                    predicate_string,
                    pruning_predicate_string,
                    output_ordering_string,
                    super::ProjectSchemaDisplay(&self.projected_schema),
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

fn make_output_ordering_string(ordering: &[PhysicalSortExpr]) -> String {
    use std::fmt::Write;
    let mut w: String = ", output_ordering=[".into();

    for (i, e) in ordering.iter().enumerate() {
        if i > 0 {
            write!(&mut w, ", ").unwrap()
        }
        write!(&mut w, "{}", e).unwrap()
    }
    write!(&mut w, "]").unwrap();
    w
}

/// Implements [`FormatReader`] for a parquet file
struct ParquetOpener {
    partition_index: usize,
    projection: Arc<[usize]>,
    batch_size: usize,
    predicate: Option<Arc<Expr>>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    table_schema: SchemaRef,
    metadata_size_hint: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    pushdown_filters: bool,
    reorder_filters: bool,
    enable_page_index: bool,
}

impl FileOpener for ParquetOpener {
    fn open(
        &self,
        _: Arc<dyn ObjectStore>,
        file_meta: FileMeta,
    ) -> Result<FileOpenFuture> {
        let file_range = file_meta.range.clone();

        let file_metrics = ParquetFileMetrics::new(
            self.partition_index,
            file_meta.location().as_ref(),
            &self.metrics,
        );

        let reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                file_meta,
                self.metadata_size_hint,
                &self.metrics,
            )?;

        let schema_adapter = SchemaAdapter::new(self.table_schema.clone());
        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let predicate = self.predicate.clone();
        let pruning_predicate = self.pruning_predicate.clone();
        let table_schema = self.table_schema.clone();
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let enable_page_index = self.enable_page_index;

        Ok(Box::pin(async move {
            let options = ArrowReaderOptions::new().with_page_index(enable_page_index);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                    .await?;
            let adapted_projections =
                schema_adapter.map_projections(builder.schema(), &projection)?;

            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                adapted_projections.iter().cloned(),
            );

            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    predicate.as_ref(),
                    builder.schema().as_ref(),
                    table_schema.as_ref(),
                    builder.metadata(),
                    reorder_predicates,
                    &file_metrics,
                );

                match row_filter {
                    Ok(Some(filter)) => {
                        builder = builder.with_row_filter(filter);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!(
                            "Ignoring error building row filter for '{:?}': {}",
                            predicate, e
                        );
                    }
                };
            };

            // Row group pruning: attempt to skip entire row_groups
            // using metadata on the row groups
            let file_metadata = builder.metadata();
            let row_groups = row_groups::prune_row_groups(
                file_metadata.row_groups(),
                file_range,
                pruning_predicate.as_ref().map(|p| p.as_ref()),
                &file_metrics,
            );

            // page index pruning: if all data on individual pages can
            // be ruled using page metadata, rows from other columns
            // with that range can be skipped as well
            if let Some(row_selection) = (enable_page_index && !row_groups.is_empty())
                .then(|| {
                    page_filter::build_page_filter(
                        pruning_predicate.as_ref().map(|p| p.as_ref()),
                        builder.schema().clone(),
                        &row_groups,
                        file_metadata.as_ref(),
                        &file_metrics,
                    )
                })
                .transpose()?
                .flatten()
            {
                builder = builder.with_row_selection(row_selection);
            }

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_groups)
                .build()?;

            let adapted = stream
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .map(move |maybe_batch| {
                    maybe_batch.and_then(|b| {
                        schema_adapter
                            .adapt_batch(b, &projection)
                            .map_err(Into::into)
                    })
                });

            Ok(adapted.boxed())
        }))
    }
}

/// Factory of parquet file readers.
///
/// Provides means to implement custom data access interface.
pub trait ParquetFileReaderFactory: Debug + Send + Sync + 'static {
    /// Provides `AsyncFileReader` over parquet file specified in `FileMeta`
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>>;
}

#[derive(Debug)]
pub struct DefaultParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
}

impl DefaultParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage
struct ParquetFileReader {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    file_metrics: ParquetFileMetrics,
    metadata_size_hint: Option<usize>,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.file_metrics.bytes_scanned.add(range.end - range.start);

        self.store
            .get_range(&self.meta.location, range)
            .map_err(|e| {
                ParquetError::General(format!("AsyncChunkReader::get_bytes error: {}", e))
            })
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let total = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total);

        async move {
            self.store
                .get_ranges(&self.meta.location, &ranges)
                .await
                .map_err(|e| {
                    ParquetError::General(format!(
                        "AsyncChunkReader::get_byte_ranges error: {}",
                        e
                    ))
                })
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = fetch_parquet_metadata(
                self.store.as_ref(),
                &self.meta,
                self.metadata_size_hint,
            )
            .await
            .map_err(|e| {
                ParquetError::General(format!(
                    "AsyncChunkReader::get_metadata error: {}",
                    e
                ))
            })?;
            Ok(Arc::new(metadata))
        })
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );

        Ok(Box::new(ParquetFileReader {
            meta: file_meta.object_meta,
            store: Arc::clone(&self.store),
            metadata_size_hint,
            file_metrics,
        }))
    }
}

/// Executes a query and writes the results to a partitioned Parquet file.
pub async fn plan_to_parquet(
    state: &SessionState,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
    writer_properties: Option<WriterProperties>,
) -> Result<()> {
    let path = path.as_ref();
    // create directory to contain the Parquet files (one per partition)
    let fs_path = std::path::Path::new(path);
    match fs::create_dir(fs_path) {
        Ok(()) => {
            let mut tasks = vec![];
            for i in 0..plan.output_partitioning().partition_count() {
                let plan = plan.clone();
                let filename = format!("part-{}.parquet", i);
                let path = fs_path.join(filename);
                let file = fs::File::create(path)?;
                let mut writer =
                    ArrowWriter::try_new(file, plan.schema(), writer_properties.clone())?;
                let task_ctx = Arc::new(TaskContext::from(state));
                let stream = plan.execute(i, task_ctx)?;
                let handle: tokio::task::JoinHandle<Result<()>> =
                    tokio::task::spawn(async move {
                        stream
                            .map(|batch| writer.write(&batch?))
                            .try_collect()
                            .await
                            .map_err(DataFusionError::from)?;
                        writer.close().map_err(DataFusionError::from).map(|_| ())
                    });
                tasks.push(handle);
            }
            futures::future::join_all(tasks)
                .await
                .into_iter()
                .try_for_each(|result| {
                    result.map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                })?;
            Ok(())
        }
        Err(e) => Err(DataFusionError::Execution(format!(
            "Could not create directory {}: {:?}",
            path, e
        ))),
    }
}

// TODO: consolidate code with arrow-rs
// Convert the bytes array to i128.
// The endian of the input bytes array must be big-endian.
// Copy from the arrow-rs
pub(crate) fn from_bytes_to_i128(b: &[u8]) -> i128 {
    assert!(b.len() <= 16, "Decimal128Array supports only up to size 16");
    let first_bit = b[0] & 128u8 == 128u8;
    let mut result = if first_bit { [255u8; 16] } else { [0u8; 16] };
    for (i, v) in b.iter().enumerate() {
        result[i + (16 - b.len())] = *v;
    }
    // The bytes array are from parquet file and must be the big-endian.
    // The endian is defined by parquet format, and the reference document
    // https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/src/main/thrift/parquet.thrift#L66
    i128::from_be_bytes(result)
}

// Convert parquet column schema to arrow data type, and just consider the
// decimal data type.
pub(crate) fn parquet_to_arrow_decimal_type(
    parquet_column: &ColumnDescriptor,
) -> Option<DataType> {
    let type_ptr = parquet_column.self_type_ptr();
    match type_ptr.get_basic_info().logical_type() {
        Some(LogicalType::Decimal { scale, precision }) => {
            Some(DataType::Decimal128(precision as u8, scale as i8))
        }
        _ => match type_ptr.get_basic_info().converted_type() {
            ConvertedType::DECIMAL => Some(DataType::Decimal128(
                type_ptr.get_precision() as u8,
                type_ptr.get_scale() as i8,
            )),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    // See also `parquet_exec` integration test

    use super::*;
    use crate::config::ConfigOptions;
    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::datasource::file_format::test_util::scan_format;
    use crate::datasource::listing::{FileRange, PartitionedFile};
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::execution::options::CsvReadOptions;
    use crate::physical_plan::displayable;
    use crate::physical_plan::file_format::partition_type_wrap;
    use crate::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;
    use crate::{
        assert_batches_sorted_eq,
        datasource::file_format::{parquet::ParquetFormat, FileFormat},
        physical_plan::collect,
    };
    use arrow::array::{ArrayRef, Float32Array, Int32Array};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::{
        array::{Int64Array, Int8Array, StringArray},
        datatypes::{DataType, Field},
    };
    use chrono::{TimeZone, Utc};
    use datafusion_common::assert_contains;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit, when};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::ObjectMeta;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    struct RoundTripResult {
        /// Data that was read back from ParquetFiles
        batches: Result<Vec<RecordBatch>>,
        /// The physical plan that was created (that has statistics, etc)
        parquet_exec: Arc<ParquetExec>,
    }

    /// writes each RecordBatch as an individual parquet file and re-reads
    /// the data back. Returns the data as [RecordBatch]es
    async fn round_trip_to_parquet(
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        schema: Option<SchemaRef>,
        predicate: Option<Expr>,
        pushdown_predicate: bool,
    ) -> Result<Vec<RecordBatch>> {
        round_trip(
            batches,
            projection,
            schema,
            predicate,
            pushdown_predicate,
            false,
        )
        .await
        .batches
    }

    /// Writes each RecordBatch as an individual parquet file and then
    /// reads them back. Returns the parquet exec as well as the data
    /// as [RecordBatch]es
    async fn round_trip(
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        schema: Option<SchemaRef>,
        predicate: Option<Expr>,
        pushdown_predicate: bool,
        page_index_predicate: bool,
    ) -> RoundTripResult {
        let file_schema = match schema {
            Some(schema) => schema,
            None => Arc::new(
                Schema::try_merge(batches.iter().map(|b| b.schema().as_ref().clone()))
                    .unwrap(),
            ),
        };

        let (meta, _files) = store_parquet(batches, page_index_predicate).await.unwrap();
        let file_groups = meta.into_iter().map(Into::into).collect();

        // prepare the scan
        let mut parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: vec![file_groups],
                file_schema,
                statistics: Statistics::default(),
                projection,
                limit: None,
                table_partition_cols: vec![],
                config_options: ConfigOptions::new().into_shareable(),
                output_ordering: None,
            },
            predicate,
            None,
        );

        if pushdown_predicate {
            parquet_exec = parquet_exec
                .with_pushdown_filters(true)
                .with_reorder_filters(true);
        }

        if page_index_predicate {
            parquet_exec = parquet_exec.with_enable_page_index(true);
        }

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let parquet_exec = Arc::new(parquet_exec);
        RoundTripResult {
            batches: collect(parquet_exec.clone(), task_ctx).await,
            parquet_exec,
        }
    }

    // Add a new column with the specified field name to the RecordBatch
    fn add_to_batch(
        batch: &RecordBatch,
        field_name: &str,
        array: ArrayRef,
    ) -> RecordBatch {
        let mut fields = batch.schema().fields().clone();
        fields.push(Field::new(field_name, array.data_type().clone(), true));
        let schema = Arc::new(Schema::new(fields));

        let mut columns = batch.columns().to_vec();
        columns.push(array);
        RecordBatch::try_new(schema, columns).expect("error; creating record batch")
    }

    fn create_batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        columns.into_iter().fold(
            RecordBatch::new_empty(Arc::new(Schema::new(vec![]))),
            |batch, (field_name, arr)| add_to_batch(&batch, field_name, arr.clone()),
        )
    }

    #[tokio::test]
    async fn write_parquet_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/csv/corrupt.csv", options).await?;
        let tmp_dir = TempDir::new()?;
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let e = df
            .write_parquet(&out_dir, None)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!("Parquet error: Arrow: underlying Arrow error: Parser error: Error while parsing value d for column 0 at line 4", format!("{}", e));
        Ok(())
    }

    #[tokio::test]
    async fn evolved_schema() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));
        // batch1: c1(string)
        let batch1 = add_to_batch(
            &RecordBatch::new_empty(Arc::new(Schema::new(vec![]))),
            "c1",
            c1,
        );

        // batch2: c1(string) and c2(int64)
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let batch2 = add_to_batch(&batch1, "c2", c2);

        // batch3: c1(string) and c3(int8)
        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));
        let batch3 = add_to_batch(&batch1, "c3", c3);

        // read/write them files:
        let read =
            round_trip_to_parquet(vec![batch1, batch2, batch3], None, None, None, false)
                .await
                .unwrap();
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c2 | c3 |",
            "+-----+----+----+",
            "|     |    |    |",
            "|     |    | 20 |",
            "|     | 2  |    |",
            "| Foo |    |    |",
            "| Foo |    | 10 |",
            "| Foo | 1  |    |",
            "| bar |    |    |",
            "| bar |    |    |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_inconsistent_order() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1)]);

        // read/write them files:
        let read = round_trip_to_parquet(vec![batch1, batch2], None, None, None, false)
            .await
            .unwrap();
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c2 | c3 |",
            "+-----+----+----+",
            "| Foo | 1  | 10 |",
            "|     | 2  | 20 |",
            "| bar |    |    |",
            "| Foo | 1  | 10 |",
            "|     | 2  | 20 |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_intersection() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        // read/write them files:
        let read = round_trip_to_parquet(vec![batch1, batch2], None, None, None, false)
            .await
            .unwrap();
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c3 | c2 |",
            "+-----+----+----+",
            "| Foo | 10 |    |",
            "|     | 20 |    |",
            "| bar |    |    |",
            "|     | 10 | 1  |",
            "|     | 20 | 2  |",
            "|     |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_intersection_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        let filter = col("c2").eq(lit(2_i64));

        // read/write them files:
        let read =
            round_trip_to_parquet(vec![batch1, batch2], None, None, Some(filter), false)
                .await
                .unwrap();
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c3 | c2 |",
            "+-----+----+----+",
            "|     |    |    |",
            "|     | 10 | 1  |",
            "|     | 20 |    |",
            "|     | 20 | 2  |",
            "| Foo | 10 |    |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_intersection_filter_with_filter_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        let filter = col("c2").eq(lit(2_i64)).or(col("c2").eq(lit(1_i64)));

        // read/write them files:
        let rt =
            round_trip(vec![batch1, batch2], None, None, Some(filter), true, false).await;
        let expected = vec![
            "+----+----+----+",
            "| c1 | c3 | c2 |",
            "+----+----+----+",
            "|    | 10 | 1  |",
            "|    | 20 | 2  |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());
        let metrics = rt.parquet_exec.metrics().unwrap();
        // Note there are were 6 rows in total (across three batches)
        assert_eq!(get_value(&metrics, "pushdown_rows_filtered"), 4);
    }

    #[tokio::test]
    async fn evolved_schema_projection() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        let c4: ArrayRef =
            Arc::new(StringArray::from(vec![Some("baz"), Some("boo"), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string), c4(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1), ("c4", c4)]);

        // read/write them files:
        let read = round_trip_to_parquet(
            vec![batch1, batch2],
            Some(vec![0, 3]),
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let expected = vec![
            "+-----+-----+",
            "| c1  | c4  |",
            "+-----+-----+",
            "| Foo | baz |",
            "|     | boo |",
            "| bar |     |",
            "| Foo |     |",
            "|     |     |",
            "| bar |     |",
            "+-----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1)]);

        let filter = col("c3").eq(lit(0_i8));

        // read/write them files:
        let read =
            round_trip_to_parquet(vec![batch1, batch2], None, None, Some(filter), false)
                .await
                .unwrap();

        // Predicate should prune all row groups
        assert_eq!(read.len(), 0);
    }

    #[tokio::test]
    async fn evolved_schema_disjoint_schema_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // batch2: c2(int64)
        let batch2 = create_batch(vec![("c2", c2)]);

        let filter = col("c2").eq(lit(1_i64));

        // read/write them files:
        let read =
            round_trip_to_parquet(vec![batch1, batch2], None, None, Some(filter), false)
                .await
                .unwrap();

        // This does not look correct since the "c2" values in the result do not in fact match the predicate `c2 == 0`
        // but parquet pruning is not exact. If the min/max values are not defined (which they are not in this case since the it is
        // a null array, then the pruning predicate (currently) can not be applied.
        // In a real query where this predicate was pushed down from a filter stage instead of created directly in the `ParquetExec`,
        // the filter stage would be preserved as a separate execution plan stage so the actual query results would be as expected.
        let expected = vec![
            "+-----+----+",
            "| c1  | c2 |",
            "+-----+----+",
            "|     |    |",
            "|     |    |",
            "|     | 1  |",
            "|     | 2  |",
            "| Foo |    |",
            "| bar |    |",
            "+-----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_disjoint_schema_with_filter_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // batch2: c2(int64)
        let batch2 = create_batch(vec![("c2", c2)]);

        let filter = col("c2").eq(lit(1_i64));

        // read/write them files:
        let rt =
            round_trip(vec![batch1, batch2], None, None, Some(filter), true, false).await;

        let expected = vec![
            "+----+----+",
            "| c1 | c2 |",
            "+----+----+",
            "|    | 1  |",
            "+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());
        let metrics = rt.parquet_exec.metrics().unwrap();
        // Note there are were 6 rows in total (across three batches)
        assert_eq!(get_value(&metrics, "pushdown_rows_filtered"), 5);
    }

    #[tokio::test]
    async fn multi_column_predicate_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = create_batch(vec![("c1", c1.clone()), ("c2", c2.clone())]);

        // Columns in different order to schema
        let filter = col("c2").eq(lit(1_i64)).or(col("c1").eq(lit("bar")));

        // read/write them files:
        let read = round_trip_to_parquet(vec![batch1], None, None, Some(filter), true)
            .await
            .unwrap();

        let expected = vec![
            "+-----+----+",
            "| c1  | c2 |",
            "+-----+----+",
            "| Foo | 1  |",
            "| bar |    |",
            "+-----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_incompatible_types() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        let c4: ArrayRef =
            Arc::new(Float32Array::from(vec![Some(1.0_f32), Some(2.0_f32), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string), c4(string)
        let batch2 = create_batch(vec![("c3", c4), ("c2", c2), ("c1", c1)]);

        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int8, true),
        ]);

        // read/write them files:
        let read = round_trip_to_parquet(
            vec![batch1, batch2],
            None,
            Some(Arc::new(schema)),
            None,
            false,
        )
        .await;
        assert_contains!(read.unwrap_err().to_string(),
                         "Execution error: Failed to map column projection for field c3. Incompatible data types Float32 and Int8");
    }

    #[tokio::test]
    async fn parquet_exec_with_projection() -> Result<()> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = "alltypes_plain.parquet";
        let ctx = SessionContext::new();
        let format = ParquetFormat::new(ctx.config_options());
        let parquet_exec =
            scan_format(&format, &testdata, filename, Some(vec![0, 1, 2]), None)
                .await
                .unwrap();
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let task_ctx = SessionContext::new().task_ctx();
        let mut results = parquet_exec.execute(0, task_ctx)?;
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

    #[tokio::test]
    async fn parquet_exec_with_range() -> Result<()> {
        fn file_range(meta: &ObjectMeta, start: i64, end: i64) -> PartitionedFile {
            PartitionedFile {
                object_meta: meta.clone(),
                partition_values: vec![],
                range: Some(FileRange { start, end }),
                extensions: None,
            }
        }

        async fn assert_parquet_read(
            file_groups: Vec<Vec<PartitionedFile>>,
            expected_row_num: Option<usize>,
            task_ctx: Arc<TaskContext>,
            file_schema: SchemaRef,
        ) -> Result<()> {
            let parquet_exec = ParquetExec::new(
                FileScanConfig {
                    object_store_url: ObjectStoreUrl::local_filesystem(),
                    file_groups,
                    file_schema,
                    statistics: Statistics::default(),
                    projection: None,
                    limit: None,
                    table_partition_cols: vec![],
                    config_options: ConfigOptions::new().into_shareable(),
                    output_ordering: None,
                },
                None,
                None,
            );
            assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);
            let results = parquet_exec.execute(0, task_ctx)?.next().await;

            if let Some(expected_row_num) = expected_row_num {
                let batch = results.unwrap()?;
                assert_eq!(expected_row_num, batch.num_rows());
            } else {
                assert!(results.is_none());
            }

            Ok(())
        }

        let session_ctx = SessionContext::new();
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_plain.parquet", testdata);

        let meta = local_unpartitioned_file(filename);

        let store = Arc::new(LocalFileSystem::new()) as _;
        let file_schema = ParquetFormat::new(session_ctx.config_options())
            .infer_schema(&store, &[meta.clone()])
            .await?;

        let group_empty = vec![vec![file_range(&meta, 0, 5)]];
        let group_contain = vec![vec![file_range(&meta, 5, i64::MAX)]];
        let group_all = vec![vec![
            file_range(&meta, 0, 5),
            file_range(&meta, 5, i64::MAX),
        ]];

        assert_parquet_read(
            group_empty,
            None,
            session_ctx.task_ctx(),
            file_schema.clone(),
        )
        .await?;
        assert_parquet_read(
            group_contain,
            Some(8),
            session_ctx.task_ctx(),
            file_schema.clone(),
        )
        .await?;
        assert_parquet_read(group_all, Some(8), session_ctx.task_ctx(), file_schema)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let store = session_ctx
            .runtime_env()
            .object_store(&object_store_url)
            .unwrap();

        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_plain.parquet", testdata);

        let meta = local_unpartitioned_file(filename);

        let schema = ParquetFormat::new(session_ctx.config_options())
            .infer_schema(&store, &[meta.clone()])
            .await
            .unwrap();

        let partitioned_file = PartitionedFile {
            object_meta: meta,
            partition_values: vec![
                ScalarValue::Utf8(Some("2021".to_owned())),
                ScalarValue::Utf8(Some("10".to_owned())),
                ScalarValue::Utf8(Some("26".to_owned())),
            ],
            range: None,
            extensions: None,
        };

        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url,
                file_groups: vec![vec![partitioned_file]],
                file_schema: schema,
                statistics: Statistics::default(),
                // file has 10 cols so index 12 should be month
                projection: Some(vec![0, 1, 2, 12]),
                limit: None,
                table_partition_cols: vec![
                    ("year".to_owned(), partition_type_wrap(DataType::Utf8)),
                    ("month".to_owned(), partition_type_wrap(DataType::Utf8)),
                    ("day".to_owned(), partition_type_wrap(DataType::Utf8)),
                ],
                config_options: ConfigOptions::new().into_shareable(),
                output_ordering: None,
            },
            None,
            None,
        );
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0, task_ctx)?;
        let batch = results.next().await.unwrap()?;
        let expected = vec![
            "+----+----------+-------------+-------+",
            "| id | bool_col | tinyint_col | month |",
            "+----+----------+-------------+-------+",
            "| 4  | true     | 0           | 10    |",
            "| 5  | false    | 1           | 10    |",
            "| 6  | true     | 0           | 10    |",
            "| 7  | false    | 1           | 10    |",
            "| 2  | true     | 0           | 10    |",
            "| 3  | false    | 1           | 10    |",
            "| 0  | true     | 0           | 10    |",
            "| 1  | false    | 1           | 10    |",
            "+----+----------+-------------+-------+",
        ];
        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_error() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let location = Path::from_filesystem_path(".")
            .unwrap()
            .child("invalid.parquet");

        let partitioned_file = PartitionedFile {
            object_meta: ObjectMeta {
                location,
                last_modified: Utc.timestamp_nanos(0),
                size: 1337,
            },
            partition_values: vec![],
            range: None,
            extensions: None,
        };

        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: vec![vec![partitioned_file]],
                file_schema: Arc::new(Schema::empty()),
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                config_options: ConfigOptions::new().into_shareable(),
                output_ordering: None,
            },
            None,
            None,
        );

        let mut results = parquet_exec.execute(0, task_ctx)?;
        let batch = results.next().await.unwrap();
        // invalid file should produce an error to that effect
        assert_contains!(batch.unwrap_err().to_string(), "invalid.parquet not found");
        assert!(results.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_page_index_exec_metrics() {
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(2)]));
        let c2: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), Some(4), Some(5)]));
        let batch1 = create_batch(vec![("int", c1.clone())]);
        let batch2 = create_batch(vec![("int", c2.clone())]);

        let filter = col("int").eq(lit(4_i32));

        let rt =
            round_trip(vec![batch1, batch2], None, None, Some(filter), false, true).await;

        let metrics = rt.parquet_exec.metrics().unwrap();

        // assert the batches and some metrics
        let expected = vec![
            "+-----+", "| int |", "+-----+", "| 3   |", "| 4   |", "| 5   |", "+-----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());
        assert_eq!(get_value(&metrics, "page_index_rows_filtered"), 3);
        assert!(
            get_value(&metrics, "page_index_eval_time") > 0,
            "no eval time in metrics: {:#?}",
            metrics
        );
    }

    #[tokio::test]
    async fn parquet_exec_metrics() {
        let c1: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Foo"),
            None,
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("zzz"),
        ]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // on
        let filter = col("c1").not_eq(lit("bar"));

        // read/write them files:
        let rt = round_trip(vec![batch1], None, None, Some(filter), true, false).await;

        let metrics = rt.parquet_exec.metrics().unwrap();

        // assert the batches and some metrics
        let expected = vec![
            "+-----+", "| c1  |", "+-----+", "| Foo |", "| zzz |", "+-----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());

        // pushdown predicates have eliminated all 4 bar rows and the
        // null row for 5 rows total
        assert_eq!(get_value(&metrics, "pushdown_rows_filtered"), 5);
        assert!(
            get_value(&metrics, "pushdown_eval_time") > 0,
            "no eval time in metrics: {:#?}",
            metrics
        );
    }

    #[tokio::test]
    async fn parquet_exec_display() {
        let c1: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Foo"),
            None,
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("zzz"),
        ]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // on
        let filter = col("c1").not_eq(lit("bar"));

        let rt = round_trip(vec![batch1], None, None, Some(filter), true, false).await;

        // should have a pruning predicate
        let pruning_predicate = &rt.parquet_exec.pruning_predicate;
        assert!(pruning_predicate.is_some());

        // convert to explain plan form
        let display = displayable(rt.parquet_exec.as_ref()).indent().to_string();

        assert_contains!(
            &display,
            "pruning_predicate=c1_min@0 != bar OR bar != c1_max@1"
        );

        assert_contains!(&display, r#"predicate=c1 != Utf8("bar")"#);

        assert_contains!(&display, "projection=[c1]");
    }

    #[tokio::test]
    async fn parquet_exec_skip_empty_pruning() {
        let c1: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Foo"),
            None,
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("zzz"),
        ]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // filter is too complicated for pruning
        let filter = when(col("c1").not_eq(lit("bar")), lit(true))
            .otherwise(lit(false))
            .unwrap();

        let rt =
            round_trip(vec![batch1], None, None, Some(filter.clone()), true, false).await;

        // Should not contain a pruning predicate
        let pruning_predicate = &rt.parquet_exec.pruning_predicate;
        assert!(
            pruning_predicate.is_none(),
            "Still had pruning predicate: {:?}",
            pruning_predicate
        );

        // but does still has a pushdown down predicate
        let predicate = rt.parquet_exec.predicate.as_ref();
        assert_eq!(predicate.unwrap().as_ref(), &filter);
    }

    /// returns the sum of all the metrics with the specified name
    /// the returned set.
    ///
    /// Count: returns value
    /// Time: returns elapsed nanoseconds
    ///
    /// Panics if no such metric.
    fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
        match metrics.sum_by_name(metric_name) {
            Some(v) => v.as_usize(),
            _ => {
                panic!(
                    "Expected metric not found. Looking for '{}' in\n\n{:#?}",
                    metric_name, metrics
                );
            }
        }
    }

    fn populate_csv_partitions(
        tmp_dir: &TempDir,
        partition_count: usize,
        file_extension: &str,
    ) -> Result<SchemaRef> {
        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
            Field::new("c3", DataType::Boolean, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{}.{}", partition, file_extension);
            let file_path = tmp_dir.path().join(filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
                file.write_all(data.as_bytes())?;
            }
        }

        Ok(schema)
    }

    #[tokio::test]
    async fn write_parquet_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        // let mut ctx = create_ctx(&tmp_dir, 4).await?;
        let ctx =
            SessionContext::with_config(SessionConfig::new().with_target_partitions(8));
        let schema = populate_csv_partitions(&tmp_dir, 4, ".csv")?;
        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // execute a simple query and write the results to parquet
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        df.write_parquet(&out_dir, None).await?;
        // write_parquet(&mut ctx, "SELECT c1, c2 FROM test", &out_dir, None).await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new();

        // register each partition as well as the top level dir
        ctx.register_parquet(
            "part0",
            &format!("{}/part-0.parquet", out_dir),
            ParquetReadOptions::default(),
        )
        .await?;
        ctx.register_parquet(
            "part1",
            &format!("{}/part-1.parquet", out_dir),
            ParquetReadOptions::default(),
        )
        .await?;
        ctx.register_parquet(
            "part2",
            &format!("{}/part-2.parquet", out_dir),
            ParquetReadOptions::default(),
        )
        .await?;
        ctx.register_parquet(
            "part3",
            &format!("{}/part-3.parquet", out_dir),
            ParquetReadOptions::default(),
        )
        .await?;
        ctx.register_parquet("allparts", &out_dir, ParquetReadOptions::default())
            .await?;

        let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT c1, c2 FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 40);

        Ok(())
    }
}
