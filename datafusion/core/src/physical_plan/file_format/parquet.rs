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

use fmt::Debug;
use std::fmt;
use std::fs;
use std::ops::Range;
use std::sync::Arc;
use std::{any::Any, convert::TryInto};

use crate::config::OPT_PARQUET_ENABLE_PAGE_INDEX;
use crate::config::OPT_PARQUET_PUSHDOWN_FILTERS;
use crate::config::OPT_PARQUET_REORDER_FILTERS;
use crate::datasource::file_format::parquet::fetch_parquet_metadata;
use crate::datasource::listing::FileRange;
use crate::physical_plan::file_format::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::physical_plan::file_format::row_filter::build_row_filter;
use crate::physical_plan::file_format::FileMeta;
use crate::{
    error::{DataFusionError, Result},
    execution::context::{SessionState, TaskContext},
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
    physical_plan::{
        expressions::PhysicalSortExpr,
        file_format::{FileScanConfig, SchemaAdapter},
        metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
    scalar::ScalarValue,
};
use arrow::array::{BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use bytes::Bytes;
use datafusion_common::Column;
use datafusion_expr::Expr;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use log::debug;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{ConvertedType, LogicalType};
use parquet::errors::ParquetError;
use parquet::file::page_index::index::Index;
use parquet::file::{
    metadata::{ParquetMetaData, RowGroupMetaData},
    properties::WriterProperties,
    statistics::Statistics as ParquetStatistics,
};
use parquet::format::PageLocation;
use parquet::schema::types::ColumnDescriptor;

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for pruning row groups
    pruning_predicate: Option<PruningPredicate>,
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

        let pruning_predicate = predicate.and_then(|predicate_expr| {
            match PruningPredicate::try_new(
                predicate_expr,
                base_config.file_schema.clone(),
            ) {
                Ok(pruning_predicate) => Some(pruning_predicate),
                Err(e) => {
                    debug!("Could not create pruning predicate for: {}", e);
                    predicate_creation_errors.add(1);
                    None
                }
            }
        });

        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            metrics,
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
    pub fn pruning_predicate(&self) -> Option<&PruningPredicate> {
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
    pub fn with_pushdown_filters(self, pushdown_filters: bool) -> Self {
        self.base_config
            .config_options
            .write()
            .set_bool(OPT_PARQUET_PUSHDOWN_FILTERS, pushdown_filters);
        self
    }

    /// Return the value described in [`Self::with_pushdown_filters`]
    pub fn pushdown_filters(&self) -> bool {
        self.base_config
            .config_options
            .read()
            .get_bool(OPT_PARQUET_PUSHDOWN_FILTERS)
            // default to false
            .unwrap_or_default()
    }

    /// If true, the `RowFilter` made by `pushdown_filters` may try to
    /// minimize the cost of filter evaluation by reordering the
    /// predicate [`Expr`]s. If false, the predicates are applied in
    /// the same order as specified in the query. Defaults to false.
    pub fn with_reorder_filters(self, reorder_filters: bool) -> Self {
        self.base_config
            .config_options
            .write()
            .set_bool(OPT_PARQUET_REORDER_FILTERS, reorder_filters);
        self
    }

    /// Return the value described in [`Self::with_reorder_filters`]
    pub fn reorder_filters(&self) -> bool {
        self.base_config
            .config_options
            .read()
            .get_bool(OPT_PARQUET_REORDER_FILTERS)
            // default to false
            .unwrap_or_default()
    }

    /// If enabled, the reader will read the page index
    /// This is used to optimise filter pushdown
    /// via `RowSelector` and `RowFilter` by
    /// eliminating unnecessary IO and decoding
    pub fn with_enable_page_index(self, enable_page_index: bool) -> Self {
        self.base_config
            .config_options
            .write()
            .set_bool(OPT_PARQUET_ENABLE_PAGE_INDEX, enable_page_index);
        self
    }

    /// Return the value described in [`Self::with_enable_page_index`]
    pub fn enable_page_index(&self) -> bool {
        self.base_config
            .config_options
            .read()
            .get_bool(OPT_PARQUET_ENABLE_PAGE_INDEX)
            // default to false
            .unwrap_or_default()
    }
}

/// Stores metrics about the parquet execution for a particular parquet file.
///
/// This component is a subject to **change** in near future and is exposed for low level integrations
/// through [ParquetFileReaderFactory].
#[derive(Debug, Clone)]
pub struct ParquetFileMetrics {
    /// Number of times the predicate could not be evaluated
    pub predicate_evaluation_errors: metrics::Count,
    /// Number of row groups pruned using
    pub row_groups_pruned: metrics::Count,
    /// Total number of bytes scanned
    pub bytes_scanned: metrics::Count,
}

impl ParquetFileMetrics {
    /// Create new metrics
    pub fn new(
        partition: usize,
        filename: &str,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let predicate_evaluation_errors = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("predicate_evaluation_errors", partition);

        let row_groups_pruned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("row_groups_pruned", partition);

        let bytes_scanned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("bytes_scanned", partition);

        Self {
            predicate_evaluation_errors,
            row_groups_pruned,
            bytes_scanned,
        }
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
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
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
                if let Some(pre) = &self.pruning_predicate {
                    write!(
                        f,
                        "ParquetExec: limit={:?}, partitions={}, predicate={}, projection={}",
                        self.base_config.limit,
                        super::FileGroupsDisplay(&self.base_config.file_groups),
                        pre.predicate_expr(),
                        super::ProjectSchemaDisplay(&self.projected_schema),
                    )
                } else {
                    write!(
                        f,
                        "ParquetExec: limit={:?}, partitions={}, projection={}",
                        self.base_config.limit,
                        super::FileGroupsDisplay(&self.base_config.file_groups),
                        super::ProjectSchemaDisplay(&self.projected_schema),
                    )
                }
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

/// Implements [`FormatReader`] for a parquet file
struct ParquetOpener {
    partition_index: usize,
    projection: Arc<[usize]>,
    batch_size: usize,
    pruning_predicate: Option<PruningPredicate>,
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

        let metrics = ParquetFileMetrics::new(
            self.partition_index,
            file_meta.location().as_ref(),
            &self.metrics,
        );

        let reader =
            BoxedAsyncFileReader(self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                file_meta,
                self.metadata_size_hint,
                &self.metrics,
            )?);

        let schema_adapter = SchemaAdapter::new(self.table_schema.clone());
        let batch_size = self.batch_size;
        let projection = self.projection.clone();
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

            if let Some(predicate) = pushdown_filters
                .then(|| pruning_predicate.as_ref().map(|p| p.logical_expr()))
                .flatten()
            {
                if let Ok(Some(filter)) = build_row_filter(
                    predicate.clone(),
                    builder.schema().as_ref(),
                    table_schema.as_ref(),
                    builder.metadata(),
                    reorder_predicates,
                ) {
                    builder = builder.with_row_filter(filter);
                }
            };

            let file_metadata = builder.metadata();
            let groups = file_metadata.row_groups();
            let row_groups =
                prune_row_groups(groups, file_range, pruning_predicate.clone(), &metrics);

            if enable_page_index && check_page_index_push_down_valid(&pruning_predicate) {
                let file_offset_indexes = file_metadata.offset_indexes();
                let file_page_indexes = file_metadata.page_indexes();
                if let (Some(file_offset_indexes), Some(file_page_indexes)) =
                    (file_offset_indexes, file_page_indexes)
                {
                    let mut selectors = Vec::with_capacity(row_groups.len());
                    for r in &row_groups {
                        selectors.extend(
                            prune_pages_in_one_row_group(
                                &groups[*r],
                                pruning_predicate.clone(),
                                file_offset_indexes.get(*r),
                                file_page_indexes.get(*r),
                                &metrics,
                            )
                            .map_err(|e| {
                                ArrowError::ParquetError(format!(
                                    "Fail in prune_pages_in_one_row_group: {}",
                                    e
                                ))
                            }),
                        );
                    }
                    debug!(
                        "Use filter and page index create RowSelection {:?} ",
                        &selectors
                    );
                    builder = builder.with_row_selection(RowSelection::from(
                        selectors.into_iter().flatten().collect::<Vec<_>>(),
                    ));
                }
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

// Check PruningPredicates just work on one column.
fn check_page_index_push_down_valid(predicate: &Option<PruningPredicate>) -> bool {
    if let Some(predicate) = predicate {
        // for now we only support pushDown on one col, because each col may have different page numbers, its hard to get
        // `num_containers` from `PruningStatistics`.
        let cols = predicate.need_input_columns_ids();
        //Todo more specific rules
        if cols.len() == 1 {
            return true;
        }
    }
    false
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
    metrics: ParquetFileMetrics,
    metadata_size_hint: Option<usize>,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.metrics.bytes_scanned.add(range.end - range.start);

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
        self.metrics.bytes_scanned.add(total);

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
        let parquet_file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );

        Ok(Box::new(ParquetFileReader {
            meta: file_meta.object_meta,
            store: Arc::clone(&self.store),
            metadata_size_hint,
            metrics: parquet_file_metrics,
        }))
    }
}

///
/// BoxedAsyncFileReader has been created to satisfy type requirements of
/// parquet stream builder constructor.
///
/// Temporary pending https://github.com/apache/arrow-rs/pull/2368
struct BoxedAsyncFileReader(Box<dyn AsyncFileReader + Send>);

impl AsyncFileReader for BoxedAsyncFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, ::parquet::errors::Result<Bytes>> {
        self.0.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    // TODO: This where bound forces us to enable #![allow(where_clauses_object_safety)] (#3081)
    // Upstream issue https://github.com/apache/arrow-rs/issues/2372
    where
        Self: Send,
    {
        self.0.get_byte_ranges(ranges)
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, ::parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.0.get_metadata()
    }
}

/// Wraps parquet statistics in a way
/// that implements [`PruningStatistics`]
struct RowGroupPruningStatistics<'a> {
    row_group_metadata: &'a RowGroupMetaData,
    parquet_schema: &'a Schema,
}

/// Wraps page_index statistics in a way
/// that implements [`PruningStatistics`]
struct PagesPruningStatistics<'a> {
    //row_group_metadata: &'a RowGroupMetaData,
    page_indexes: &'a Vec<Index>,
    offset_indexes: &'a Vec<Vec<PageLocation>>,
    parquet_schema: &'a Schema,
    col_id: usize,
}

// TODO: consolidate code with arrow-rs
// Convert the bytes array to i128.
// The endian of the input bytes array must be big-endian.
// Copy from the arrow-rs
fn from_bytes_to_i128(b: &[u8]) -> i128 {
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

/// Extract the min/max statistics from a `ParquetStatistics` object
macro_rules! get_statistic {
    ($column_statistics:expr, $func:ident, $bytes_func:ident, $target_arrow_type:expr) => {{
        if !$column_statistics.has_min_max_set() {
            return None;
        }
        match $column_statistics {
            ParquetStatistics::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.$func()))),
            ParquetStatistics::Int32(s) => {
                match $target_arrow_type {
                    // int32 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(*s.$func() as i128),
                            precision,
                            scale,
                        ))
                    }
                    _ => Some(ScalarValue::Int32(Some(*s.$func()))),
                }
            }
            ParquetStatistics::Int64(s) => {
                match $target_arrow_type {
                    // int64 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(*s.$func() as i128),
                            precision,
                            scale,
                        ))
                    }
                    _ => Some(ScalarValue::Int64(Some(*s.$func()))),
                }
            }
            // 96 bit ints not supported
            ParquetStatistics::Int96(_) => None,
            ParquetStatistics::Float(s) => Some(ScalarValue::Float32(Some(*s.$func()))),
            ParquetStatistics::Double(s) => Some(ScalarValue::Float64(Some(*s.$func()))),
            ParquetStatistics::ByteArray(s) => {
                // TODO support decimal type for byte array type
                let s = std::str::from_utf8(s.$bytes_func())
                    .map(|s| s.to_string())
                    .ok();
                Some(ScalarValue::Utf8(s))
            }
            // type not supported yet
            ParquetStatistics::FixedLenByteArray(s) => {
                match $target_arrow_type {
                    // just support the decimal data type
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(from_bytes_to_i128(s.$bytes_func())),
                            precision,
                            scale,
                        ))
                    }
                    _ => None,
                }
            }
        }
    }};
}

// Extract the min or max value calling `func` or `bytes_func` on the ParquetStatistics as appropriate
macro_rules! get_min_max_values {
    ($self:expr, $column:expr, $func:ident, $bytes_func:ident) => {{
        let (_column_index, field) =
            if let Some((v, f)) = $self.parquet_schema.column_with_name(&$column.name) {
                (v, f)
            } else {
                // Named column was not present
                return None;
            };

        let data_type = field.data_type();
        // The result may be None, because DataFusion doesn't have support for ScalarValues of the column type
        let null_scalar: ScalarValue = data_type.try_into().ok()?;

        $self.row_group_metadata
            .columns()
            .iter()
            .find(|c| c.column_descr().name() == &$column.name)
            .and_then(|c| if c.statistics().is_some() {Some((c.statistics().unwrap(), c.column_descr()))} else {None})
            .map(|(stats, column_descr)|
                {
                    let target_data_type = parquet_to_arrow_decimal_type(column_descr);
                    get_statistic!(stats, $func, $bytes_func, target_data_type)
                })
            .flatten()
            // column either didn't have statistics at all or didn't have min/max values
            .or_else(|| Some(null_scalar.clone()))
            .map(|s| s.to_array())
    }}
}

// Extract the null count value on the ParquetStatistics
macro_rules! get_null_count_values {
    ($self:expr, $column:expr) => {{
        let value = ScalarValue::UInt64(
            if let Some(col) = $self
                .row_group_metadata
                .columns()
                .iter()
                .find(|c| c.column_descr().name() == &$column.name)
            {
                col.statistics().map(|s| s.null_count())
            } else {
                Some($self.row_group_metadata.num_rows() as u64)
            },
        );

        Some(value.to_array())
    }};
}

// Extract the min or max value calling `func` from page idex
macro_rules! get_min_max_values_for_page_index {
    ($self:expr, $column:expr, $func:ident) => {{
        if let Some((col_id_index, _field)) =
            $self.parquet_schema.column_with_name(&$column.name)
        {
            if let Some(page_index) = $self.page_indexes.get(col_id_index) {
                match page_index {
                    Index::NONE => None,
                    Index::INT32(index) => {
                        let vec = &index.indexes;
                        Some(Arc::new(Int32Array::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                    Index::INT64(index) => {
                        let vec = &index.indexes;
                        Some(Arc::new(Int64Array::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                    Index::FLOAT(index) => {
                        let vec = &index.indexes;
                        Some(Arc::new(Float32Array::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                    Index::DOUBLE(index) => {
                        let vec = &index.indexes;
                        Some(Arc::new(Float64Array::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                    Index::BOOLEAN(index) => {
                        let vec = &index.indexes;
                        Some(Arc::new(BooleanArray::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                    Index::INT96(_)
                    | Index::BYTE_ARRAY(_)
                    | Index::FIXED_LEN_BYTE_ARRAY(_) => {
                        //Todo support these type
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    }};
}

// Convert parquet column schema to arrow data type, and just consider the
// decimal data type.
fn parquet_to_arrow_decimal_type(parquet_column: &ColumnDescriptor) -> Option<DataType> {
    let type_ptr = parquet_column.self_type_ptr();
    match type_ptr.get_basic_info().logical_type() {
        Some(LogicalType::Decimal { scale, precision }) => {
            Some(DataType::Decimal128(precision as u8, scale as u8))
        }
        _ => match type_ptr.get_basic_info().converted_type() {
            ConvertedType::DECIMAL => Some(DataType::Decimal128(
                type_ptr.get_precision() as u8,
                type_ptr.get_scale() as u8,
            )),
            _ => None,
        },
    }
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, min, min_bytes)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, max, max_bytes)
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        get_null_count_values!(self, column)
    }
}

impl<'a> PruningStatistics for PagesPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values_for_page_index!(self, column, min)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values_for_page_index!(self, column, max)
    }

    fn num_containers(&self) -> usize {
        self.offset_indexes.get(self.col_id).unwrap().len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        if let Some((col_id_index, _field)) =
            self.parquet_schema.column_with_name(&column.name)
        {
            if let Some(page_index) = self.page_indexes.get(col_id_index) {
                match page_index {
                    Index::NONE => None,
                    Index::BOOLEAN(index) => Some(Arc::new(Int64Array::from_iter(
                        index.indexes.iter().map(|x| x.null_count),
                    ))),
                    Index::INT32(index) => Some(Arc::new(Int64Array::from_iter(
                        index.indexes.iter().map(|x| x.null_count),
                    ))),
                    Index::INT64(index) => Some(Arc::new(Int64Array::from_iter(
                        index.indexes.iter().map(|x| x.null_count),
                    ))),
                    Index::FLOAT(index) => Some(Arc::new(Int64Array::from_iter(
                        index.indexes.iter().map(|x| x.null_count),
                    ))),
                    Index::DOUBLE(index) => Some(Arc::new(Int64Array::from_iter(
                        index.indexes.iter().map(|x| x.null_count),
                    ))),
                    Index::INT96(_)
                    | Index::BYTE_ARRAY(_)
                    | Index::FIXED_LEN_BYTE_ARRAY(_) => {
                        // Todo support these types
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

fn prune_row_groups(
    groups: &[RowGroupMetaData],
    range: Option<FileRange>,
    predicate: Option<PruningPredicate>,
    metrics: &ParquetFileMetrics,
) -> Vec<usize> {
    // TODO: Columnar pruning
    let mut filtered = Vec::with_capacity(groups.len());
    for (idx, metadata) in groups.iter().enumerate() {
        if let Some(range) = &range {
            let offset = metadata.column(0).file_offset();
            if offset < range.start || offset >= range.end {
                continue;
            }
        }

        if let Some(predicate) = &predicate {
            let pruning_stats = RowGroupPruningStatistics {
                row_group_metadata: metadata,
                parquet_schema: predicate.schema().as_ref(),
            };
            match predicate.prune(&pruning_stats) {
                Ok(values) => {
                    // NB: false means don't scan row group
                    if !values[0] {
                        metrics.row_groups_pruned.add(1);
                        continue;
                    }
                }
                // stats filter array could not be built
                // return a closure which will not filter out any row groups
                Err(e) => {
                    debug!("Error evaluating row group predicate values {}", e);
                    metrics.predicate_evaluation_errors.add(1);
                }
            }
        }

        filtered.push(idx)
    }
    filtered
}

fn prune_pages_in_one_row_group(
    group: &RowGroupMetaData,
    predicate: Option<PruningPredicate>,
    offset_indexes: Option<&Vec<Vec<PageLocation>>>,
    page_indexes: Option<&Vec<Index>>,
    metrics: &ParquetFileMetrics,
) -> Result<Vec<RowSelector>> {
    let num_rows = group.num_rows() as usize;
    if let (Some(predicate), Some(offset_indexes), Some(page_indexes)) =
        (&predicate, offset_indexes, page_indexes)
    {
        let pruning_stats = PagesPruningStatistics {
            page_indexes,
            offset_indexes,
            parquet_schema: predicate.schema().as_ref(),
            // now we assume only support one col.
            col_id: *predicate
                .need_input_columns_ids()
                .iter()
                .take(1)
                .next()
                .unwrap(),
        };

        match predicate.prune(&pruning_stats) {
            Ok(values) => {
                let mut vec = Vec::with_capacity(values.len());
                if let Some(cols_offset_indexes) =
                    offset_indexes.get(pruning_stats.col_id)
                {
                    let row_vec =
                        create_row_count_in_each_page(cols_offset_indexes, num_rows);
                    assert_eq!(row_vec.len(), values.len());
                    let mut sum_row = *row_vec.first().unwrap();
                    let mut selected = *values.first().unwrap();

                    for (i, &f) in values.iter().skip(1).enumerate() {
                        if f == selected {
                            sum_row += *row_vec.get(i).unwrap();
                        } else {
                            let selector = if selected {
                                RowSelector::select(sum_row)
                            } else {
                                RowSelector::skip(sum_row)
                            };
                            vec.push(selector);
                            sum_row = *row_vec.get(i).unwrap();
                            selected = f;
                        }
                    }

                    let selector = if selected {
                        RowSelector::select(sum_row)
                    } else {
                        RowSelector::skip(sum_row)
                    };
                    vec.push(selector);
                    return Ok(vec);
                } else {
                    debug!("Error evaluating page index predicate values missing page index col_id is{}", pruning_stats.col_id);
                    metrics.predicate_evaluation_errors.add(1);
                }
            }
            // stats filter array could not be built
            // return a closure which will not filter out any row groups
            Err(e) => {
                debug!("Error evaluating page index predicate values {}", e);
                metrics.predicate_evaluation_errors.add(1);
            }
        }
    }
    Err(DataFusionError::ParquetError(ParquetError::General(
        "Got some error in prune_pages_in_one_row_group, plz try open the debuglog mode"
            .to_string(),
    )))
}

fn create_row_count_in_each_page(
    location: &Vec<PageLocation>,
    num_rows: usize,
) -> Vec<usize> {
    let mut vec = Vec::with_capacity(location.len());
    location.windows(2).for_each(|x| {
        let start = x[0].first_row_index as usize;
        let end = x[1].first_row_index as usize;
        vec.push(end - start);
    });
    vec.push(num_rows - location.last().unwrap().first_row_index as usize);
    vec
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
                let path = fs_path.join(&filename);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigOptions;
    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::datasource::file_format::test_util::scan_format;
    use crate::datasource::listing::{FileRange, PartitionedFile};
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::execution::options::CsvReadOptions;
    use crate::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;
    use crate::{
        assert_batches_sorted_eq,
        datasource::file_format::{parquet::ParquetFormat, FileFormat},
        physical_plan::collect,
    };
    use arrow::array::Float32Array;
    use arrow::datatypes::DataType::Decimal128;
    use arrow::record_batch::RecordBatch;
    use arrow::{
        array::{Int64Array, Int8Array, StringArray},
        datatypes::{DataType, Field},
    };
    use chrono::{TimeZone, Utc};
    use datafusion_common::assert_contains;
    use datafusion_expr::{cast, col, lit};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::ObjectMeta;
    use parquet::basic::LogicalType;
    use parquet::data_type::{ByteArray, FixedLenByteArray};
    use parquet::{
        basic::Type as PhysicalType,
        file::{metadata::RowGroupMetaData, statistics::Statistics as ParquetStatistics},
        schema::types::SchemaDescPtr,
    };
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    /// writes each RecordBatch as an individual parquet file and then
    /// reads it back in to the named location.
    async fn round_trip_to_parquet(
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        schema: Option<SchemaRef>,
        predicate: Option<Expr>,
        pushdown_predicate: bool,
    ) -> Result<Vec<RecordBatch>> {
        let file_schema = match schema {
            Some(schema) => schema,
            None => Arc::new(Schema::try_merge(
                batches.iter().map(|b| b.schema().as_ref().clone()),
            )?),
        };

        let (meta, _files) = store_parquet(batches).await?;
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
            },
            predicate,
            None,
        );

        if pushdown_predicate {
            parquet_exec = parquet_exec
                .with_pushdown_filters(true)
                .with_reorder_filters(true);
        }

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        collect(Arc::new(parquet_exec), task_ctx).await
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

        let filter = col("c2").eq(lit(2_i64));

        // read/write them files:
        let read =
            round_trip_to_parquet(vec![batch1, batch2], None, None, Some(filter), true)
                .await
                .unwrap();
        let expected = vec![
            "+----+----+----+",
            "| c1 | c3 | c2 |",
            "+----+----+----+",
            "|    | 20 | 2  |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
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
    async fn evolved_schema_disjoint_schema_filter_with_pushdown() {
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
            round_trip_to_parquet(vec![batch1, batch2], None, None, Some(filter), true)
                .await
                .unwrap();

        let expected = vec![
            "+----+----+",
            "| c1 | c2 |",
            "+----+----+",
            "|    | 1  |",
            "+----+----+",
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
        let format = ParquetFormat::default();
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
        let file_schema = ParquetFormat::default()
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

        let schema = ParquetFormat::default()
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
                    "year".to_owned(),
                    "month".to_owned(),
                    "day".to_owned(),
                ],
                config_options: ConfigOptions::new().into_shareable(),
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

    fn parquet_file_metrics() -> ParquetFileMetrics {
        let metrics = Arc::new(ExecutionPlanMetricsSet::new());
        ParquetFileMetrics::new(0, "file.parquet", &metrics)
    }

    #[test]
    fn row_group_pruning_predicate_simple_expr() {
        use datafusion_expr::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        let schema_descr = get_test_schema_descr(vec![(
            "c1",
            PhysicalType::INT32,
            None,
            None,
            None,
            None,
        )]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(1), Some(10), None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );

        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(&[rgm1, rgm2], None, Some(pruning_predicate), &metrics),
            vec![1]
        );
    }

    #[test]
    fn row_group_pruning_predicate_missing_stats() {
        use datafusion_expr::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();

        let schema_descr = get_test_schema_descr(vec![(
            "c1",
            PhysicalType::INT32,
            None,
            None,
            None,
            None,
        )]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(None, None, None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        // missing statistics for first row group mean that the result from the predicate expression
        // is null / undefined so the first row group can't be filtered out
        assert_eq!(
            prune_row_groups(&[rgm1, rgm2], None, Some(pruning_predicate), &metrics),
            vec![0, 1]
        );
    }

    #[test]
    fn row_group_pruning_predicate_partial_expr() {
        use datafusion_expr::{col, lit};
        // test row group predicate with partially supported expression
        // int > 1 and int % 2 => c1_max > 1 and true
        let expr = col("c1").gt(lit(15)).and(col("c2").modulus(lit(2)));
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]));
        let pruning_predicate = PruningPredicate::try_new(expr, schema.clone()).unwrap();

        let schema_descr = get_test_schema_descr(vec![
            ("c1", PhysicalType::INT32, None, None, None, None),
            ("c2", PhysicalType::INT32, None, None, None, None),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
            ],
        );

        let metrics = parquet_file_metrics();
        let groups = &[rgm1, rgm2];
        // the first row group is still filtered out because the predicate expression can be partially evaluated
        // when conditions are joined using AND
        assert_eq!(
            prune_row_groups(groups, None, Some(pruning_predicate), &metrics),
            vec![1]
        );

        // if conditions in predicate are joined with OR and an unsupported expression is used
        // this bypasses the entire predicate expression and no row groups are filtered out
        let expr = col("c1").gt(lit(15)).or(col("c2").modulus(lit(2)));
        let pruning_predicate = PruningPredicate::try_new(expr, schema).unwrap();

        // if conditions in predicate are joined with OR and an unsupported expression is used
        // this bypasses the entire predicate expression and no row groups are filtered out
        assert_eq!(
            prune_row_groups(groups, None, Some(pruning_predicate), &metrics),
            vec![0, 1]
        );
    }

    fn gen_row_group_meta_data_for_pruning_predicate() -> Vec<RowGroupMetaData> {
        let schema_descr = get_test_schema_descr(vec![
            ("c1", PhysicalType::INT32, None, None, None, None),
            ("c2", PhysicalType::BOOLEAN, None, None, None, None),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 1, false),
            ],
        );
        vec![rgm1, rgm2]
    }

    #[test]
    fn row_group_pruning_predicate_null_expr() {
        use datafusion_expr::{col, lit};
        // int > 1 and IsNull(bool) => c1_max > 1 and bool_null_count > 0
        let expr = col("c1").gt(lit(15)).and(col("c2").is_null());
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Boolean, false),
        ]));
        let pruning_predicate = PruningPredicate::try_new(expr, schema).unwrap();
        let groups = gen_row_group_meta_data_for_pruning_predicate();

        let metrics = parquet_file_metrics();
        // First row group was filtered out because it contains no null value on "c2".
        assert_eq!(
            prune_row_groups(&groups, None, Some(pruning_predicate), &metrics),
            vec![1]
        );
    }

    #[test]
    fn row_group_pruning_predicate_eq_null_expr() {
        use datafusion_expr::{col, lit};
        // test row group predicate with an unknown (Null) expr
        //
        // int > 1 and bool = NULL => c1_max > 1 and null
        let expr = col("c1")
            .gt(lit(15))
            .and(col("c2").eq(lit(ScalarValue::Boolean(None))));
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Boolean, false),
        ]));
        let pruning_predicate = PruningPredicate::try_new(expr, schema).unwrap();
        let groups = gen_row_group_meta_data_for_pruning_predicate();

        let metrics = parquet_file_metrics();
        // bool = NULL always evaluates to NULL (and thus will not
        // pass predicates. Ideally these should both be false
        assert_eq!(
            prune_row_groups(&groups, None, Some(pruning_predicate), &metrics),
            vec![1]
        );
    }

    #[test]
    fn row_group_pruning_predicate_decimal_type() {
        // For the decimal data type, parquet can use `INT32`, `INT64`, `BYTE_ARRAY`, `FIXED_LENGTH_BYTE_ARRAY` to
        // store the data.
        // In this case, construct four types of statistics to filtered with the decimal predication.

        // INT32: c1 > 5, the c1 is decimal(9,2)
        // The type of scalar value if decimal(9,2), don't need to do cast
        let expr = col("c1").gt(lit(ScalarValue::Decimal128(Some(500), 9, 2)));
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(9, 2), false)]);
        let schema_descr = get_test_schema_descr(vec![(
            "c1",
            PhysicalType::INT32,
            Some(LogicalType::Decimal {
                scale: 2,
                precision: 9,
            }),
            Some(9),
            Some(2),
            None,
        )]);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            // [1.00, 6.00]
            // c1 > 5, this row group will be included in the results.
            vec![ParquetStatistics::int32(
                Some(100),
                Some(600),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            // [0.1, 0.2]
            // c1 > 5, this row group will not be included in the results.
            vec![ParquetStatistics::int32(Some(10), Some(20), None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(&[rgm1, rgm2], None, Some(pruning_predicate), &metrics),
            vec![0]
        );

        // INT32: c1 > 5, but parquet decimal type has different precision or scale to arrow decimal
        // The c1 type is decimal(9,0) in the parquet file, and the type of scalar is decimal(5,2).
        // We should convert all type to the coercion type, which is decimal(11,2)
        // The decimal of arrow is decimal(5,2), the decimal of parquet is decimal(9,0)
        let expr = cast(col("c1"), DataType::Decimal128(11, 2)).gt(cast(
            lit(ScalarValue::Decimal128(Some(500), 5, 2)),
            Decimal128(11, 2),
        ));
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(9, 0), false)]);
        let schema_descr = get_test_schema_descr(vec![(
            "c1",
            PhysicalType::INT32,
            Some(LogicalType::Decimal {
                scale: 0,
                precision: 9,
            }),
            Some(9),
            Some(0),
            None,
        )]);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            // [100, 600]
            // c1 > 5, this row group will be included in the results.
            vec![ParquetStatistics::int32(
                Some(100),
                Some(600),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            // [10, 20]
            // c1 > 5, this row group will be included in the results.
            vec![ParquetStatistics::int32(Some(10), Some(20), None, 0, false)],
        );
        let rgm3 = get_row_group_meta_data(
            &schema_descr,
            // [0, 2]
            // c1 > 5, this row group will not be included in the results.
            vec![ParquetStatistics::int32(Some(0), Some(2), None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(
                &[rgm1, rgm2, rgm3],
                None,
                Some(pruning_predicate),
                &metrics
            ),
            vec![0, 1]
        );

        // INT64: c1 < 5, the c1 is decimal(18,2)
        let expr = col("c1").lt(lit(ScalarValue::Decimal128(Some(500), 18, 2)));
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(18, 2), false)]);
        let schema_descr = get_test_schema_descr(vec![(
            "c1",
            PhysicalType::INT64,
            Some(LogicalType::Decimal {
                scale: 2,
                precision: 18,
            }),
            Some(18),
            Some(2),
            None,
        )]);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            // [6.00, 8.00]
            vec![ParquetStatistics::int32(
                Some(600),
                Some(800),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            // [0.1, 0.2]
            vec![ParquetStatistics::int64(Some(10), Some(20), None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(&[rgm1, rgm2], None, Some(pruning_predicate), &metrics),
            vec![1]
        );

        // FIXED_LENGTH_BYTE_ARRAY: c1 = decimal128(100000, 28, 3), the c1 is decimal(18,2)
        // the type of parquet is decimal(18,2)
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(18, 2), false)]);
        // cast the type of c1 to decimal(28,3)
        let left = cast(col("c1"), DataType::Decimal128(28, 3));
        let expr = left.eq(lit(ScalarValue::Decimal128(Some(100000), 28, 3)));
        let schema_descr = get_test_schema_descr(vec![(
            "c1",
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            Some(LogicalType::Decimal {
                scale: 2,
                precision: 18,
            }),
            Some(18),
            Some(2),
            Some(16),
        )]);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        // we must use the big-endian when encode the i128 to bytes or vec[u8].
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::fixed_len_byte_array(
                // 5.00
                Some(FixedLenByteArray::from(ByteArray::from(
                    500i128.to_be_bytes().to_vec(),
                ))),
                // 80.00
                Some(FixedLenByteArray::from(ByteArray::from(
                    8000i128.to_be_bytes().to_vec(),
                ))),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::fixed_len_byte_array(
                // 5.00
                Some(FixedLenByteArray::from(ByteArray::from(
                    500i128.to_be_bytes().to_vec(),
                ))),
                // 200.00
                Some(FixedLenByteArray::from(ByteArray::from(
                    20000i128.to_be_bytes().to_vec(),
                ))),
                None,
                0,
                false,
            )],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(&[rgm1, rgm2], None, Some(pruning_predicate), &metrics),
            vec![1]
        );

        // TODO: BYTE_ARRAY support read decimal from parquet, after the 20.0.0 arrow-rs release
    }

    fn get_row_group_meta_data(
        schema_descr: &SchemaDescPtr,
        column_statistics: Vec<ParquetStatistics>,
    ) -> RowGroupMetaData {
        use parquet::file::metadata::ColumnChunkMetaData;
        let mut columns = vec![];
        for (i, s) in column_statistics.iter().enumerate() {
            let column = ColumnChunkMetaData::builder(schema_descr.column(i))
                .set_statistics(s.clone())
                .build()
                .unwrap();
            columns.push(column);
        }
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .build()
            .unwrap()
    }

    #[allow(clippy::type_complexity)]
    fn get_test_schema_descr(
        fields: Vec<(
            &str,
            PhysicalType,
            Option<LogicalType>,
            Option<i32>, // precision
            Option<i32>, // scale
            Option<i32>, // length of bytes
        )>,
    ) -> SchemaDescPtr {
        use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};
        let mut schema_fields = fields
            .iter()
            .map(|(n, t, logical, precision, scale, length)| {
                let mut builder = SchemaType::primitive_type_builder(n, *t);
                // add logical type for the parquet field
                match logical {
                    None => {}
                    Some(logical_type) => {
                        builder = builder.with_logical_type(Some(logical_type.clone()));
                    }
                };
                match precision {
                    None => {}
                    Some(v) => {
                        builder = builder.with_precision(*v);
                    }
                };
                match scale {
                    None => {}
                    Some(v) => {
                        builder = builder.with_scale(*v);
                    }
                }
                match length {
                    None => {}
                    Some(v) => {
                        builder = builder.with_length(*v);
                    }
                }
                Arc::new(builder.build().unwrap())
            })
            .collect::<Vec<_>>();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(&mut schema_fields)
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
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
            let file_path = tmp_dir.path().join(&filename);
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

    #[tokio::test]
    async fn parquet_exec_with_page_index_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let store = session_ctx
            .runtime_env()
            .object_store(&object_store_url)
            .unwrap();

        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_tiny_pages.parquet", testdata);

        let meta = local_unpartitioned_file(filename);

        let schema = ParquetFormat::default()
            .infer_schema(&store, &[meta.clone()])
            .await
            .unwrap();

        let partitioned_file = PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            extensions: None,
        };

        // create filter month == 1;
        let filter = col("month").eq(lit(1_i32));
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url,
                file_groups: vec![vec![partitioned_file]],
                file_schema: schema,
                statistics: Statistics::default(),
                // file has 10 cols so index 12 should be month
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                config_options: ConfigOptions::new().into_shareable(),
            },
            Some(filter),
            None,
        );

        let parquet_exec_page_index = parquet_exec.clone().with_enable_page_index(true);

        let mut results = parquet_exec_page_index.execute(0, task_ctx)?;

        let batch = results.next().await.unwrap()?;

        //  from the page index should create below RowSelection
        //  vec.push(RowSelector::select(312));
        //  vec.push(RowSelector::skip(3330));
        //  vec.push(RowSelector::select(333));
        //  vec.push(RowSelector::skip(3330));
        // total 645 row

        assert_eq!(batch.num_rows(), 645);
        Ok(())
    }
}
