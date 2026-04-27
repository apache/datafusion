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

//! [`ParquetMorselizer`] state machines for opening Parquet files

use crate::page_filter::PagePruningAccessPlanFilter;
use crate::row_filter::build_projection_read_plan;
use crate::row_group_filter::{BloomFilterStatistics, RowGroupAccessPlanFilter};
use crate::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
    apply_file_schema_type_coercions, coerce_int96_to_resolution, row_filter,
};
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::DataType;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef, TimeUnit};
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_common::stats::Precision;
use datafusion_common::{
    ColumnStatistics, DataFusionError, Result, ScalarValue, Statistics, exec_err,
};
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::physical_expr::{
    PhysicalExpr, is_dynamic_physical_expr,
};
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder,
    MetricCategory, PruningMetrics,
};
use datafusion_pruning::{FilePruner, PruningPredicate, build_pruning_predicate};

#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;
use futures::{
    FutureExt, Stream, StreamExt, future::BoxFuture, ready, stream::BoxStream,
};
use log::debug;
use parquet::DecodeResult;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::parquet_column;
use parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use parquet::basic::Type;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

/// Stateless Parquet morselizer implementation.
///
/// Reading a Parquet file is a multi-stage process, with multiple CPU-intensive
/// steps interspersed with I/O steps. The code in this module implements the steps
/// as an explicit state machine -- see [`ParquetOpenState`] for details.
#[derive(Clone)]
pub(super) struct ParquetMorselizer {
    /// Execution partition index
    pub(crate) partition_index: usize,
    /// Projection to apply on top of the table schema (i.e. can reference partition columns).
    pub projection: ProjectionExprs,
    /// Target number of rows in each output RecordBatch
    pub batch_size: usize,
    /// Optional limit on the number of rows to read
    pub(crate) limit: Option<usize>,
    /// If should keep the output rows in order
    pub preserve_order: bool,
    /// Optional predicate to apply during the scan
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Table schema, including partition columns.
    pub table_schema: TableSchema,
    /// Optional hint for how large the initial request to read parquet metadata
    /// should be
    pub metadata_size_hint: Option<usize>,
    /// Metrics for reporting
    pub metrics: ExecutionPlanMetricsSet,
    /// Factory for instantiating parquet reader
    pub parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Should the filters be evaluated during the parquet scan using
    /// [`DataFusionArrowPredicate`](row_filter::DatafusionArrowPredicate)?
    pub pushdown_filters: bool,
    /// Should the filters be reordered to optimize the scan?
    pub reorder_filters: bool,
    /// Should we force the reader to use RowSelections for filtering
    pub force_filter_selections: bool,
    /// Should the page index be read from parquet files, if present, to skip
    /// data pages
    pub enable_page_index: bool,
    /// Should the bloom filter be read from parquet, if present, to skip row
    /// groups
    pub enable_bloom_filter: bool,
    /// Should row group pruning be applied
    pub enable_row_group_stats_pruning: bool,
    /// Coerce INT96 timestamps to specific TimeUnit
    pub coerce_int96: Option<TimeUnit>,
    /// Optional parquet FileDecryptionProperties
    #[cfg(feature = "parquet_encryption")]
    pub file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    /// Rewrite expressions in the context of the file schema
    pub(crate) expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    /// Optional factory to create file decryption properties dynamically
    #[cfg(feature = "parquet_encryption")]
    pub encryption_factory:
        Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
    /// Maximum size of the predicate cache, in bytes. If none, uses
    /// the arrow-rs default.
    pub max_predicate_cache_size: Option<usize>,
    /// Whether to read row groups in reverse order
    pub reverse_row_groups: bool,
}

impl fmt::Debug for ParquetMorselizer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetMorselizer")
            .field("partition_index", &self.partition_index)
            .field("preserve_order", &self.preserve_order)
            .field("enable_page_index", &self.enable_page_index)
            .field("enable_bloom_filter", &self.enable_bloom_filter)
            .finish()
    }
}

impl Morselizer for ParquetMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        Ok(Box::new(ParquetMorselPlanner::try_new(self, file)?))
    }
}

/// States for [`ParquetMorselPlanner`]
///
/// These states correspond to the steps required to read and apply various
/// filter operations.
///
/// States whose names beginning with `Load` represent waiting on IO to resolve
///
/// ```text
///      Start
///        |
///        v
/// [LoadEncryption]?
///        |
///        v
///    PruneFile
///        |
///        v
///   LoadMetadata
///        |
///        v
///  PrepareFilters
///        |
///        v
///   LoadPageIndex
///        |
///        v
/// PruneWithStatistics
///        |
///        v
///  LoadBloomFilters
///        |
///        v
/// PruneWithBloomFilters
///        |
///        v
///   BuildStream
///        |
///        v
///       Done
/// ```
///
/// Note: `LoadEncryption` is only present when the `parquet_encryption` feature is
/// enabled. All other states are always visited in the order shown above,
/// though any async state may return `Poll::Pending` and then resume later.
enum ParquetOpenState {
    Start {
        prepared: Box<PreparedParquetOpen>,
        #[cfg(feature = "parquet_encryption")]
        encryption_context: Arc<EncryptionContext>,
    },
    /// Loading encryption footers
    #[cfg(feature = "parquet_encryption")]
    LoadEncryption(BoxFuture<'static, Result<Box<PreparedParquetOpen>>>),
    /// Try to prune file using only file-level statistics and partition
    /// values before loading any parquet metadata
    PruneFile(Box<PreparedParquetOpen>),
    /// Loading Parquet metadata (in footer)
    LoadMetadata(BoxFuture<'static, Result<MetadataLoadedParquetOpen>>),
    /// Specialize any filters for the actual file schema (only known after
    /// metadata is loaded)
    PrepareFilters(Box<MetadataLoadedParquetOpen>),
    /// Loading [Parquet Page Index](https://parquet.apache.org/docs/file-format/pageindex/)
    LoadPageIndex(BoxFuture<'static, Result<FiltersPreparedParquetOpen>>),
    /// Pruning Row Groups
    PruneWithStatistics(Box<FiltersPreparedParquetOpen>),
    /// Loading bloom filters required for row-group pruning
    LoadBloomFilters(BoxFuture<'static, Result<BloomFiltersLoadedParquetOpen>>),
    /// Pruning with preloaded Bloom Filters
    PruneWithBloomFilters(Box<BloomFiltersLoadedParquetOpen>),
    /// Builds the final reader stream
    ///
    /// TODO: split state as this currently does both I/O and CPU work.
    BuildStream(Box<RowGroupsPrunedParquetOpen>),
    /// Terminal state: the final opened stream is ready to return.
    Ready(BoxStream<'static, Result<RecordBatch>>),
    /// Terminal state: reading complete
    Done,
}

impl fmt::Debug for ParquetOpenState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self {
            ParquetOpenState::Start { .. } => "Start",
            #[cfg(feature = "parquet_encryption")]
            ParquetOpenState::LoadEncryption(_) => "LoadEncryption",
            ParquetOpenState::PruneFile(_) => "PruneFile",
            ParquetOpenState::LoadMetadata(_) => "LoadMetadata",
            ParquetOpenState::PrepareFilters(_) => "PrepareFilters",
            ParquetOpenState::LoadPageIndex(_) => "LoadPageIndex",
            ParquetOpenState::PruneWithStatistics(_) => "PruneWithStatistics",
            ParquetOpenState::LoadBloomFilters(_) => "LoadBloomFilters",
            ParquetOpenState::PruneWithBloomFilters(_) => "PruneWithBloomFilters",
            ParquetOpenState::BuildStream(_) => "BuildStream",
            ParquetOpenState::Ready(_) => "Ready",
            ParquetOpenState::Done => "Done",
        };
        f.write_str(state)
    }
}

struct PreparedParquetOpen {
    partition_index: usize,
    partitioned_file: PartitionedFile,
    file_range: Option<datafusion_datasource::FileRange>,
    extensions: Option<Arc<dyn std::any::Any + Send + Sync>>,
    file_name: String,
    file_metrics: ParquetFileMetrics,
    baseline_metrics: BaselineMetrics,
    file_pruner: Option<FilePruner>,
    metadata_size_hint: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    async_file_reader: Box<dyn AsyncFileReader>,
    batch_size: usize,
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    output_schema: SchemaRef,
    projection: ProjectionExprs,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    reorder_predicates: bool,
    pushdown_filters: bool,
    force_filter_selections: bool,
    enable_page_index: bool,
    enable_bloom_filter: bool,
    enable_row_group_stats_pruning: bool,
    limit: Option<usize>,
    coerce_int96: Option<TimeUnit>,
    expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    predicate_creation_errors: Count,
    max_predicate_cache_size: Option<usize>,
    reverse_row_groups: bool,
    preserve_order: bool,
    #[cfg(feature = "parquet_encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
}

/// State of [`ParquetOpenState`]
///
/// Result of loading parquet metadata after file-level pruning is complete.
struct MetadataLoadedParquetOpen {
    prepared: PreparedParquetOpen,
    reader_metadata: ArrowReaderMetadata,
    options: ArrowReaderOptions,
}

/// State of [`ParquetOpenState`]
///
/// Pruning Predicate and DataPage pruning information
/// specialized for the files specific schema.
struct FiltersPreparedParquetOpen {
    loaded: MetadataLoadedParquetOpen,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
}

/// State of [`ParquetOpenState`]
///
/// Result of CPU-only row-group pruning before optional bloom-filter I/O.
struct RowGroupsPrunedParquetOpen {
    prepared: FiltersPreparedParquetOpen,
    row_groups: RowGroupAccessPlanFilter,
}

/// State of [`ParquetOpenState`]
///
/// Result of loading bloom filters needed for row-group pruning.
struct BloomFiltersLoadedParquetOpen {
    prepared: RowGroupsPrunedParquetOpen,
    /// Bloom filters loaded for each row group that remains under consideration.
    ///
    /// indexed by parquet row-group index
    row_group_bloom_filters: Vec<BloomFilterStatistics>,
}

impl ParquetOpenState {
    /// Applies one CPU-only state transition.
    ///
    /// `Load*` states do not transition here and are returned unchanged so the
    /// driver loop can poll their inner futures separately.
    ///
    /// Implements state machine described in [`ParquetOpenState`]
    fn transition(self) -> Result<ParquetOpenState> {
        match self {
            ParquetOpenState::Start {
                prepared,
                #[cfg(feature = "parquet_encryption")]
                encryption_context,
            } => {
                #[cfg(feature = "parquet_encryption")]
                {
                    let mut prepared = *prepared;
                    let future = async move {
                        let file_location =
                            &prepared.partitioned_file.object_meta.location;
                        prepared.file_decryption_properties = encryption_context
                            .get_file_decryption_properties(file_location)
                            .await?;
                        Ok(Box::new(prepared))
                    }
                    .boxed();
                    Ok(ParquetOpenState::LoadEncryption(future))
                }
                #[cfg(not(feature = "parquet_encryption"))]
                {
                    Ok(ParquetOpenState::PruneFile(prepared))
                }
            }
            #[cfg(feature = "parquet_encryption")]
            ParquetOpenState::LoadEncryption(future) => {
                Ok(ParquetOpenState::LoadEncryption(future))
            }
            ParquetOpenState::PruneFile(prepared) => {
                let Some(prepared) = (*prepared).prune_file()? else {
                    return Ok(ParquetOpenState::Done);
                };
                Ok(ParquetOpenState::LoadMetadata(prepared.load().boxed()))
            }
            ParquetOpenState::LoadMetadata(future) => {
                Ok(ParquetOpenState::LoadMetadata(future))
            }
            ParquetOpenState::PrepareFilters(loaded) => {
                let prepared_filters = loaded.prepare_filters()?;
                Ok(ParquetOpenState::LoadPageIndex(
                    prepared_filters.load_page_index().boxed(),
                ))
            }
            ParquetOpenState::LoadPageIndex(future) => {
                Ok(ParquetOpenState::LoadPageIndex(future))
            }
            ParquetOpenState::PruneWithStatistics(prepared) => {
                let prepared_row_groups = prepared.prune_row_groups()?;
                Ok(ParquetOpenState::LoadBloomFilters(
                    prepared_row_groups.load_bloom_filters().boxed(),
                ))
            }
            ParquetOpenState::LoadBloomFilters(future) => {
                Ok(ParquetOpenState::LoadBloomFilters(future))
            }
            ParquetOpenState::PruneWithBloomFilters(loaded) => Ok(
                ParquetOpenState::BuildStream(Box::new(loaded.prune_bloom_filters())),
            ),
            ParquetOpenState::BuildStream(prepared) => {
                Ok(ParquetOpenState::Ready(prepared.build_stream()?))
            }
            ParquetOpenState::Ready(stream) => Ok(ParquetOpenState::Ready(stream)),
            ParquetOpenState::Done => {
                panic!("ParquetOpenFuture polled after completion");
            }
        }
    }
}

/// Implements the Morsel API
struct ParquetStreamMorsel {
    stream: BoxStream<'static, Result<RecordBatch>>,
}

impl ParquetStreamMorsel {
    fn new(stream: BoxStream<'static, Result<RecordBatch>>) -> Self {
        Self { stream }
    }
}

impl fmt::Debug for ParquetStreamMorsel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetStreamMorsel")
            .finish_non_exhaustive()
    }
}

impl Morsel for ParquetStreamMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        self.stream
    }
}

/// Per-file planner that owns the current [`ParquetOpenState`].
struct ParquetMorselPlanner {
    /// Ready to perform CPU-only planning work.
    state: ParquetOpenState,
}

impl fmt::Debug for ParquetMorselPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ParquetMorselPlanner::Ready")
            .field(&self.state)
            .finish()
    }
}

impl ParquetMorselPlanner {
    fn try_new(morselizer: &ParquetMorselizer, file: PartitionedFile) -> Result<Self> {
        let prepared = morselizer.prepare_open_file(file)?;
        #[cfg(feature = "parquet_encryption")]
        let state = ParquetOpenState::Start {
            prepared: Box::new(prepared),
            encryption_context: Arc::new(morselizer.get_encryption_context()),
        };
        #[cfg(not(feature = "parquet_encryption"))]
        let state = ParquetOpenState::Start {
            prepared: Box::new(prepared),
        };
        Ok(Self { state })
    }

    /// Schedule an I/O future that resolves to the next planner to run.
    ///
    /// This helper
    ///
    /// 1. drives one I/O phase to completion
    /// 2. wraps the resulting state in a new [`ParquetMorselPlanner`]
    /// 3. returns a [`MorselPlan`] containing the boxed future for the caller
    ///    to poll
    ///
    fn schedule_io<F>(future: F) -> MorselPlan
    where
        F: Future<Output = Result<ParquetOpenState>> + Send + 'static,
    {
        let io_future = async move {
            let next_state = future.await?;
            Ok(Box::new(ParquetMorselPlanner { state: next_state }) as _)
        };
        MorselPlan::new().with_pending_planner(io_future)
    }
}

impl MorselPlanner for ParquetMorselPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        if let ParquetOpenState::Done = self.state {
            return Ok(None);
        }

        let state = self.state.transition()?;

        match state {
            #[cfg(feature = "parquet_encryption")]
            ParquetOpenState::LoadEncryption(future) => {
                Ok(Some(Self::schedule_io(async move {
                    Ok(ParquetOpenState::PruneFile(future.await?))
                })))
            }
            ParquetOpenState::LoadMetadata(future) => {
                Ok(Some(Self::schedule_io(async move {
                    Ok(ParquetOpenState::PrepareFilters(Box::new(future.await?)))
                })))
            }
            ParquetOpenState::LoadPageIndex(future) => {
                Ok(Some(Self::schedule_io(async move {
                    Ok(ParquetOpenState::PruneWithStatistics(Box::new(
                        future.await?,
                    )))
                })))
            }
            ParquetOpenState::LoadBloomFilters(future) => {
                Ok(Some(Self::schedule_io(async move {
                    Ok(ParquetOpenState::PruneWithBloomFilters(Box::new(
                        future.await?,
                    )))
                })))
            }
            ParquetOpenState::Ready(stream) => {
                let morsels: Vec<Box<dyn Morsel>> =
                    vec![Box::new(ParquetStreamMorsel::new(stream))];
                Ok(Some(MorselPlan::new().with_morsels(morsels)))
            }
            ParquetOpenState::Done => Ok(None),
            cpu_state => Ok(Some(
                MorselPlan::new()
                    .with_planners(vec![Box::new(Self { state: cpu_state })]),
            )),
        }
    }
}

impl ParquetMorselizer {
    /// Perform the CPU-only setup for opening a parquet file.
    fn prepare_open_file(
        &self,
        partitioned_file: PartitionedFile,
    ) -> Result<PreparedParquetOpen> {
        let file_range = partitioned_file.range.clone();
        let extensions = partitioned_file.extensions.clone();
        let file_name = partitioned_file.object_meta.location.to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);
        let baseline_metrics = BaselineMetrics::new(&self.metrics, self.partition_index);

        let metadata_size_hint = partitioned_file
            .metadata_size_hint
            .or(self.metadata_size_hint);

        let async_file_reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                partitioned_file.clone(),
                metadata_size_hint,
                &self.metrics,
            )?;

        // Calculate the output schema from the original projection (before literal replacement)
        // so we get correct field names from column references
        let logical_file_schema = Arc::clone(self.table_schema.file_schema());
        let output_schema = Arc::new(
            self.projection
                .project_schema(self.table_schema.table_schema())?,
        );

        // Build a combined map for replacing column references with literal values.
        // This includes:
        // 1. Partition column values from the file path (e.g., region=us-west-2)
        // 2. Constant columns detected from file statistics (where min == max)
        //
        // Although partition columns *are* constant columns, we don't want to rely on
        // statistics for them being populated if we can use the partition values
        // (which are guaranteed to be present).
        //
        // For example, given a partition column `region` and predicate
        // `region IN ('us-east-1', 'eu-central-1')` with file path
        // `/data/region=us-west-2/...`, the predicate is rewritten to
        // `'us-west-2' IN ('us-east-1', 'eu-central-1')` which simplifies to FALSE.
        //
        // While partition column optimization is done during logical planning,
        // there are cases where partition columns may appear in more complex
        // predicates that cannot be simplified until we open the file (such as
        // dynamic predicates).
        let mut literal_columns: HashMap<String, ScalarValue> = self
            .table_schema
            .table_partition_cols()
            .iter()
            .zip(partitioned_file.partition_values.iter())
            .map(|(field, value)| (field.name().clone(), value.clone()))
            .collect();
        // Add constant columns from file statistics.
        // Note that if there are statistics for partition columns there will be overlap,
        // but since we use a HashMap, we'll just overwrite the partition values with the
        // constant values from statistics (which should be the same).
        literal_columns.extend(constant_columns_from_stats(
            partitioned_file.statistics.as_deref(),
            &logical_file_schema,
        ));

        let mut projection = self.projection.clone();
        let mut predicate = self.predicate.clone();
        if !literal_columns.is_empty() {
            projection = projection.try_map_exprs(|expr| {
                replace_columns_with_literals(Arc::clone(&expr), &literal_columns)
            })?;
            predicate = predicate
                .map(|p| replace_columns_with_literals(p, &literal_columns))
                .transpose()?;
        }

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Rows)
            .global_counter("num_predicate_creation_errors");

        // Apply literal replacements to projection and predicate
        let file_pruner = predicate
            .as_ref()
            .filter(|p| is_dynamic_physical_expr(p) || partitioned_file.has_statistics())
            .and_then(|p| {
                FilePruner::try_new(
                    Arc::clone(p),
                    &logical_file_schema,
                    &partitioned_file,
                    predicate_creation_errors.clone(),
                )
            });

        Ok(PreparedParquetOpen {
            partition_index: self.partition_index,
            partitioned_file,
            file_range,
            extensions,
            file_name,
            file_metrics,
            baseline_metrics,
            file_pruner,
            metadata_size_hint,
            metrics: self.metrics.clone(),
            parquet_file_reader_factory: Arc::clone(&self.parquet_file_reader_factory),
            async_file_reader,
            batch_size: self.batch_size,
            logical_file_schema: Arc::clone(&logical_file_schema),
            physical_file_schema: logical_file_schema,
            output_schema,
            projection,
            predicate,
            reorder_predicates: self.reorder_filters,
            pushdown_filters: self.pushdown_filters,
            force_filter_selections: self.force_filter_selections,
            enable_page_index: self.enable_page_index,
            enable_bloom_filter: self.enable_bloom_filter,
            enable_row_group_stats_pruning: self.enable_row_group_stats_pruning,
            limit: self.limit,
            coerce_int96: self.coerce_int96,
            expr_adapter_factory: Arc::clone(&self.expr_adapter_factory),
            predicate_creation_errors,
            max_predicate_cache_size: self.max_predicate_cache_size,
            reverse_row_groups: self.reverse_row_groups,
            preserve_order: self.preserve_order,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties: None,
        })
    }
}

impl PreparedParquetOpen {
    /// Attempt file-level pruning before any metadata is loaded.
    ///
    /// Returns `None` if the file can be skipped completely.
    fn prune_file(mut self) -> Result<Option<Self>> {
        // Prune this file using the file level statistics and partition values.
        // Since dynamic filters may have been updated since planning it is possible that we are able
        // to prune files now that we couldn't prune at planning time.
        // It is assumed that there is no point in doing pruning here if the predicate is not dynamic,
        // as it would have been done at planning time.
        // We'll also check this after every record batch we read,
        // and if at some point we are able to prove we can prune the file using just the file level statistics
        // we can end the stream early.
        //
        // Make a FilePruner only if there is either
        // 1. a dynamic expr in the predicate
        // 2. the file has file-level statistics.
        //
        // File-level statistics may prune the file without loading
        // any row groups or metadata.
        //
        // Dynamic filters may prune the file after initial
        // planning, as the dynamic filter is updated during
        // execution.
        //
        // The case where there is a dynamic filter but no
        // statistics corresponds to a dynamic filter that
        // references partition columns. While rare, this is possible
        // e.g. `select * from table order by partition_col limit
        // 10` could hit this condition.
        if let Some(file_pruner) = &mut self.file_pruner
            && file_pruner.should_prune()?
        {
            self.file_metrics
                .files_ranges_pruned_statistics
                .add_pruned(1);
            return Ok(None);
        }

        self.file_metrics
            .files_ranges_pruned_statistics
            .add_matched(1);
        Ok(Some(self))
    }

    /// Load parquet metadata after file-level pruning is complete.
    async fn load(mut self) -> Result<MetadataLoadedParquetOpen> {
        // Don't load the page index yet. Since it is not stored inline in
        // the footer, loading the page index if it is not needed will do
        // unnecessary I/O. We decide later if it is needed to evaluate the
        // pruning predicates. Thus default to not requesting it from the
        // underlying reader.
        let options =
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
        #[cfg(feature = "parquet_encryption")]
        let mut options = options;
        #[cfg(feature = "parquet_encryption")]
        if let Some(fd_val) = &self.file_decryption_properties {
            options = options.with_file_decryption_properties(Arc::clone(fd_val));
        }

        let mut metadata_timer = self.file_metrics.metadata_load_time.timer();
        // Begin by loading the metadata from the underlying reader (note
        // the returned metadata may actually include page indexes as some
        // readers may return page indexes even when not requested -- for
        // example when they are cached)
        let reader_metadata =
            ArrowReaderMetadata::load_async(&mut self.async_file_reader, options.clone())
                .await?;
        metadata_timer.stop();
        drop(metadata_timer);

        Ok(MetadataLoadedParquetOpen {
            prepared: self,
            reader_metadata,
            options,
        })
    }
}

impl MetadataLoadedParquetOpen {
    /// Prepare file-schema coercions and pruning predicates once metadata is
    /// loaded.
    fn prepare_filters(self) -> Result<FiltersPreparedParquetOpen> {
        let MetadataLoadedParquetOpen {
            mut prepared,
            mut reader_metadata,
            mut options,
        } = self;

        // Note about schemas: we are actually dealing with **3 different schemas** here:
        // - The table schema as defined by the TableProvider.
        //   This is what the user sees, what they get when they `SELECT * FROM table`, etc.
        // - The logical file schema: this is the table schema minus any hive partition columns and projections.
        //   This is what the physical file schema is coerced to.
        // - The physical file schema: this is the schema that the arrow-rs
        //   parquet reader will actually produce.
        let mut physical_file_schema = Arc::clone(reader_metadata.schema());

        // The schema loaded from the file may not be the same as the
        // desired schema (for example if we want to instruct the parquet
        // reader to read strings using Utf8View instead). Update if necessary
        if let Some(merged) = apply_file_schema_type_coercions(
            &prepared.logical_file_schema,
            &physical_file_schema,
        ) {
            physical_file_schema = Arc::new(merged);
            options = options.with_schema(Arc::clone(&physical_file_schema));
            reader_metadata = ArrowReaderMetadata::try_new(
                Arc::clone(reader_metadata.metadata()),
                options.clone(),
            )?;
        }

        if let Some(ref coerce) = prepared.coerce_int96
            && let Some(merged) = coerce_int96_to_resolution(
                reader_metadata.parquet_schema(),
                &physical_file_schema,
                coerce,
            )
        {
            physical_file_schema = Arc::new(merged);
            options = options.with_schema(Arc::clone(&physical_file_schema));
            reader_metadata = ArrowReaderMetadata::try_new(
                Arc::clone(reader_metadata.metadata()),
                options.clone(),
            )?;
        }

        // Adapt the projection & filter predicate to the physical file schema.
        // This evaluates missing columns and inserts any necessary casts.
        // After rewriting to the file schema, further simplifications may be possible.
        // For example, if `'a' = col_that_is_missing` becomes `'a' = NULL` that can then be simplified to `FALSE`
        // and we can avoid doing any more work on the file (bloom filters, loading the page index, etc.).
        // Additionally, if any casts were inserted we can move casts from the column to the literal side:
        // `CAST(col AS INT) = 5` can become `col = CAST(5 AS <col type>)`, which can be evaluated statically.
        //
        // When the schemas are identical and there is no predicate, the
        // rewriter is a no-op: column indices already match (partition
        // columns are appended after file columns in the table schema),
        // types are the same, and there are no missing columns. Skip the
        // tree walk entirely in that case.
        let needs_rewrite = prepared.predicate.is_some()
            || prepared.logical_file_schema != physical_file_schema;
        if needs_rewrite {
            let rewriter = prepared.expr_adapter_factory.create(
                Arc::clone(&prepared.logical_file_schema),
                Arc::clone(&physical_file_schema),
            )?;
            let simplifier = PhysicalExprSimplifier::new(&physical_file_schema);
            prepared.predicate = prepared
                .predicate
                .map(|p| simplifier.simplify(rewriter.rewrite(p)?))
                .transpose()?;
            prepared.projection = prepared
                .projection
                .try_map_exprs(|p| simplifier.simplify(rewriter.rewrite(p)?))?;
        }
        prepared.physical_file_schema = Arc::clone(&physical_file_schema);

        // Build predicates for this specific file
        let pruning_predicate = build_pruning_predicates(
            prepared.predicate.as_ref(),
            &physical_file_schema,
            &prepared.predicate_creation_errors,
        );

        // Only build page pruning predicate if page index is enabled
        let page_pruning_predicate = if prepared.enable_page_index {
            prepared.predicate.as_ref().and_then(|predicate| {
                let p = build_page_pruning_predicate(predicate, &physical_file_schema);
                (p.filter_number() > 0).then_some(p)
            })
        } else {
            None
        };

        Ok(FiltersPreparedParquetOpen {
            loaded: MetadataLoadedParquetOpen {
                prepared,
                reader_metadata,
                options,
            },
            pruning_predicate,
            page_pruning_predicate,
        })
    }
}

impl FiltersPreparedParquetOpen {
    /// Load the page index if pruning requires it and metadata did not include it.
    async fn load_page_index(mut self) -> Result<Self> {
        // The page index is not stored inline in the parquet footer so the
        // metadata load above may not have read the page index structures yet.
        // If we need them for reading and they aren't yet loaded, we need to
        // load them now.
        if self.page_pruning_predicate.is_some() {
            self.loaded.reader_metadata = load_page_index(
                self.loaded.reader_metadata,
                &mut self.loaded.prepared.async_file_reader,
                self.loaded
                    .options
                    .clone()
                    .with_page_index_policy(PageIndexPolicy::Optional),
            )
            .await?;
        }

        Ok(self)
    }

    /// Prune row groups using file ranges and parquet metadata.
    fn prune_row_groups(self) -> Result<RowGroupsPrunedParquetOpen> {
        let loaded = &self.loaded;
        let prepared = &loaded.prepared;
        let file_metadata = Arc::clone(loaded.reader_metadata.metadata());
        let rg_metadata = file_metadata.row_groups();

        // Determine which row groups to actually read. The idea is to skip
        // as many row groups as possible based on the metadata and query
        let mut row_groups = RowGroupAccessPlanFilter::new(create_initial_plan(
            &prepared.file_name,
            prepared.extensions.clone(),
            rg_metadata.len(),
        )?);

        // If there is a range restricting what parts of the file to read
        if let Some(range) = prepared.file_range.as_ref() {
            row_groups.prune_by_range(rg_metadata, range);
        }

        // If there is a predicate that can be evaluated against the metadata
        if let Some(predicate) = self.pruning_predicate.as_ref().map(|p| p.as_ref()) {
            if prepared.enable_row_group_stats_pruning {
                row_groups.prune_by_statistics(
                    &prepared.physical_file_schema,
                    loaded.reader_metadata.parquet_schema(),
                    rg_metadata,
                    predicate,
                    &prepared.file_metrics,
                );
            } else {
                // Update metrics: statistics unavailable, so all row groups are
                // matched (not pruned)
                prepared
                    .file_metrics
                    .row_groups_pruned_statistics
                    .add_matched(row_groups.remaining_row_group_count());
            }

            if !prepared.enable_bloom_filter || row_groups.is_empty() {
                // Update metrics: bloom filter unavailable, so all row groups are
                // matched (not pruned)
                prepared
                    .file_metrics
                    .row_groups_pruned_bloom_filter
                    .add_matched(row_groups.remaining_row_group_count());
            }
        } else {
            // Update metrics: no predicate, so all row groups are matched (not pruned)
            let remaining = row_groups.remaining_row_group_count();
            prepared
                .file_metrics
                .row_groups_pruned_statistics
                .add_matched(remaining);
            prepared
                .file_metrics
                .row_groups_pruned_bloom_filter
                .add_matched(remaining);
        }

        Ok(RowGroupsPrunedParquetOpen {
            prepared: self,
            row_groups,
        })
    }
}

impl RowGroupsPrunedParquetOpen {
    /// Load bloom filters needed for pruning when enabled and a pruning predicate exists.
    async fn load_bloom_filters(mut self) -> Result<BloomFiltersLoadedParquetOpen> {
        let num_row_groups = self
            .prepared
            .loaded
            .reader_metadata
            .metadata()
            .num_row_groups();
        let mut row_group_bloom_filters =
            vec![BloomFilterStatistics::new(); num_row_groups];

        if let Some(predicate) =
            self.prepared.pruning_predicate.as_ref().map(|p| p.as_ref())
            && self.prepared.loaded.prepared.enable_bloom_filter
            && !self.row_groups.is_empty()
        {
            // Use the existing reader for bloom filter I/O;
            // replace with a fresh reader for decoding below.
            let reader_metadata = self.prepared.loaded.reader_metadata.clone();
            let replacement_reader = {
                let prepared = &self.prepared.loaded.prepared;
                prepared.parquet_file_reader_factory.create_reader(
                    prepared.partition_index,
                    prepared.partitioned_file.clone(),
                    prepared.metadata_size_hint,
                    &prepared.metrics,
                )?
            };

            let prepared = &mut self.prepared.loaded.prepared;
            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                mem::replace(&mut prepared.async_file_reader, replacement_reader),
                reader_metadata,
            );
            let parquet_columns: Vec<(String, usize, Type)> = predicate
                .literal_columns()
                .into_iter()
                .filter_map(|column_name| {
                    let parquet_schema = builder.parquet_schema();
                    let (column_idx, _) = parquet_column(
                        parquet_schema,
                        &prepared.physical_file_schema,
                        &column_name,
                    )?;
                    Some((
                        column_name,
                        column_idx,
                        parquet_schema.column(column_idx).physical_type(),
                    ))
                })
                .collect();

            for idx in self.row_groups.row_group_indexes() {
                let mut row_group_filters =
                    BloomFilterStatistics::with_capacity(parquet_columns.len());
                for (column_name, column_idx, physical_type) in &parquet_columns {
                    let bf: Sbbf = match builder
                        .get_row_group_column_bloom_filter(idx, *column_idx)
                        .await
                    {
                        Ok(Some(bf)) => bf,
                        Ok(None) => continue,
                        Err(e) => {
                            debug!("Ignoring error reading bloom filter: {e}");
                            prepared.file_metrics.predicate_evaluation_errors.add(1);
                            continue;
                        }
                    };
                    row_group_filters.insert(column_name, bf, *physical_type);
                }
                row_group_bloom_filters[idx] = row_group_filters;
            }
        }

        Ok(BloomFiltersLoadedParquetOpen {
            prepared: self,
            row_group_bloom_filters,
        })
    }
}

impl BloomFiltersLoadedParquetOpen {
    /// Apply bloom filter pruning using already loaded bloom filters.
    fn prune_bloom_filters(mut self) -> RowGroupsPrunedParquetOpen {
        let bloom_filter_eval_time = self
            .prepared
            .prepared
            .loaded
            .prepared
            .file_metrics
            .bloom_filter_eval_time
            .clone();
        let _timer_guard = bloom_filter_eval_time.timer();
        if let Some(predicate) = self
            .prepared
            .prepared
            .pruning_predicate
            .as_ref()
            .map(|p| p.as_ref())
            && self.prepared.prepared.loaded.prepared.enable_bloom_filter
            && !self.prepared.row_groups.is_empty()
        {
            self.prepared.row_groups.prune_by_bloom_filters(
                predicate,
                &self.prepared.prepared.loaded.prepared.file_metrics,
                &self.row_group_bloom_filters,
            );
        }

        self.prepared
    }
}

impl RowGroupsPrunedParquetOpen {
    /// Build the final parquet stream once all pruning work is complete.
    fn build_stream(self) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let RowGroupsPrunedParquetOpen {
            prepared,
            mut row_groups,
        } = self;
        let FiltersPreparedParquetOpen {
            loaded,
            pruning_predicate: _,
            page_pruning_predicate,
        } = prepared;
        let MetadataLoadedParquetOpen {
            prepared,
            reader_metadata,
            options: _,
        } = loaded;

        let file_metadata = Arc::clone(reader_metadata.metadata());
        let rg_metadata = file_metadata.row_groups();

        // Filter pushdown: evaluate predicates during scan
        let row_filter = if let Some(predicate) = prepared
            .pushdown_filters
            .then_some(prepared.predicate.clone())
            .flatten()
        {
            let row_filter = row_filter::build_row_filter(
                &predicate,
                &prepared.physical_file_schema,
                file_metadata.as_ref(),
                prepared.reorder_predicates,
                &prepared.file_metrics,
            );

            match row_filter {
                Ok(Some(filter)) => Some(filter),
                Ok(None) => None,
                Err(e) => {
                    debug!("Ignoring error building row filter for '{predicate:?}': {e}");
                    None
                }
            }
        } else {
            None
        };

        // Prune by limit if limit is set and limit order is not sensitive
        if let (Some(limit), false) = (prepared.limit, prepared.preserve_order) {
            row_groups.prune_by_limit(limit, rg_metadata, &prepared.file_metrics);
        }

        // Page index pruning: if all data on individual pages can
        // be ruled using page metadata, rows from other columns
        // with that range can be skipped as well.
        let mut access_plan = row_groups.build();
        if prepared.enable_page_index
            && !access_plan.is_empty()
            && let Some(page_pruning_predicate) = page_pruning_predicate
        {
            access_plan = page_pruning_predicate.prune_plan_with_page_index(
                access_plan,
                &prepared.physical_file_schema,
                reader_metadata.parquet_schema(),
                file_metadata.as_ref(),
                &prepared.file_metrics,
            );
        }

        // Prepare the access plan (extract row groups and row selection)
        let mut prepared_plan = access_plan.prepare(rg_metadata)?;

        // Potentially reverse the access plan for performance.
        // See `ParquetSource::try_pushdown_sort` for the rationale.
        if prepared.reverse_row_groups {
            prepared_plan = prepared_plan.reverse(file_metadata.as_ref())?;
        }

        let arrow_reader_metrics = ArrowReaderMetrics::enabled();
        let read_plan = build_projection_read_plan(
            prepared.projection.expr_iter(),
            &prepared.physical_file_schema,
            reader_metadata.parquet_schema(),
        );

        let mut decoder_builder =
            ParquetPushDecoderBuilder::new_with_metadata(reader_metadata)
                .with_projection(read_plan.projection_mask)
                .with_batch_size(prepared.batch_size)
                .with_metrics(arrow_reader_metrics.clone());

        if let Some(row_filter) = row_filter {
            decoder_builder = decoder_builder.with_row_filter(row_filter);
        }
        if prepared.force_filter_selections {
            decoder_builder =
                decoder_builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
        }
        if let Some(row_selection) = prepared_plan.row_selection {
            decoder_builder = decoder_builder.with_row_selection(row_selection);
        }
        decoder_builder =
            decoder_builder.with_row_groups(prepared_plan.row_group_indexes);
        if let Some(limit) = prepared.limit {
            decoder_builder = decoder_builder.with_limit(limit);
        }
        if let Some(max_predicate_cache_size) = prepared.max_predicate_cache_size {
            decoder_builder =
                decoder_builder.with_max_predicate_cache_size(max_predicate_cache_size);
        }

        let decoder = decoder_builder.build()?;

        let predicate_cache_inner_records =
            prepared.file_metrics.predicate_cache_inner_records.clone();
        let predicate_cache_records =
            prepared.file_metrics.predicate_cache_records.clone();

        // Check if we need to replace the schema to handle things like differing nullability or metadata.
        // See note below about file vs. output schema.
        let stream_schema = read_plan.projected_schema;
        let replace_schema = stream_schema != prepared.output_schema;

        // Rebase column indices to match the narrowed stream schema.
        // The projection expressions have indices based on physical_file_schema,
        // but the stream only contains the columns selected by the ProjectionMask.
        let projection = prepared
            .projection
            .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
        let projector = projection.make_projector(&stream_schema)?;
        let output_schema = Arc::clone(&prepared.output_schema);
        let files_ranges_pruned_statistics =
            prepared.file_metrics.files_ranges_pruned_statistics.clone();
        let stream = futures::stream::unfold(
            PushDecoderStreamState {
                decoder,
                reader: prepared.async_file_reader,
                projector,
                output_schema,
                replace_schema,
                arrow_reader_metrics,
                predicate_cache_inner_records,
                predicate_cache_records,
                baseline_metrics: prepared.baseline_metrics,
            },
            |state| async move { state.transition().await },
        )
        .fuse();

        // Wrap the stream so a dynamic filter can stop the file scan early.
        if let Some(file_pruner) = prepared.file_pruner {
            let stream = stream.boxed();
            Ok(EarlyStoppingStream::new(
                stream,
                file_pruner,
                files_ranges_pruned_statistics,
            )
            .boxed())
        } else {
            Ok(stream.boxed())
        }
    }
}

/// State for a stream that decodes a single Parquet file using a push-based decoder.
///
/// The [`transition`](Self::transition) method drives the decoder in a loop: it requests
/// byte ranges from the [`AsyncFileReader`], pushes the fetched data into the
/// [`ParquetPushDecoder`], and yields projected [`RecordBatch`]es until the file is
/// fully consumed.
struct PushDecoderStreamState {
    decoder: ParquetPushDecoder,
    reader: Box<dyn AsyncFileReader>,
    projector: Projector,
    output_schema: Arc<Schema>,
    replace_schema: bool,
    arrow_reader_metrics: ArrowReaderMetrics,
    predicate_cache_inner_records: Gauge,
    predicate_cache_records: Gauge,
    baseline_metrics: BaselineMetrics,
}

impl PushDecoderStreamState {
    /// Advances the decoder state machine until the next [`RecordBatch`] is
    /// produced, the file is fully consumed, or an error occurs.
    ///
    /// On each iteration the decoder is polled via [`ParquetPushDecoder::try_decode`]:
    /// - [`NeedsData`](DecodeResult::NeedsData) – the requested byte ranges are
    ///   fetched from the [`AsyncFileReader`] and fed back into the decoder.
    /// - [`Data`](DecodeResult::Data) – a decoded batch is projected and returned.
    /// - [`Finished`](DecodeResult::Finished) – signals end-of-stream (`None`).
    ///
    /// Takes `self` by value (rather than `&mut self`) so the generated future
    /// owns the state directly. This avoids a Stacked Borrows violation under
    /// miri where `&mut self` creates a single opaque borrow that conflicts
    /// with `unfold`'s ownership across yield points.
    async fn transition(mut self) -> Option<(Result<RecordBatch>, Self)> {
        loop {
            match self.decoder.try_decode() {
                Ok(DecodeResult::NeedsData(ranges)) => {
                    let data = self
                        .reader
                        .get_byte_ranges(ranges.clone())
                        .await
                        .map_err(DataFusionError::from);
                    match data {
                        Ok(data) => {
                            if let Err(e) = self.decoder.push_ranges(ranges, data) {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                        }
                        Err(e) => return Some((Err(e), self)),
                    }
                }
                Ok(DecodeResult::Data(batch)) => {
                    let mut timer = self.baseline_metrics.elapsed_compute().timer();
                    self.copy_arrow_reader_metrics();
                    let result = self.project_batch(&batch);
                    timer.stop();
                    // Release the borrow on baseline_metrics before moving self
                    drop(timer);
                    return Some((result, self));
                }
                Ok(DecodeResult::Finished) => {
                    return None;
                }
                Err(e) => {
                    return Some((Err(DataFusionError::from(e)), self));
                }
            }
        }
    }

    /// Copies metrics from ArrowReaderMetrics (the metrics collected by the
    /// arrow-rs parquet reader) to the parquet file metrics for DataFusion
    fn copy_arrow_reader_metrics(&self) {
        if let Some(v) = self.arrow_reader_metrics.records_read_from_inner() {
            self.predicate_cache_inner_records.set(v);
        }
        if let Some(v) = self.arrow_reader_metrics.records_read_from_cache() {
            self.predicate_cache_records.set(v);
        }
    }

    fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut batch = self.projector.project_batch(batch)?;
        if self.replace_schema {
            // Ensure the output batch has the expected schema.
            // This handles things like schema level and field level metadata, which may not be present
            // in the physical file schema.
            // It is also possible for nullability to differ; some writers create files with
            // OPTIONAL fields even when there are no nulls in the data.
            // In these cases it may make sense for the logical schema to be `NOT NULL`.
            // RecordBatch::try_new_with_options checks that if the schema is NOT NULL
            // the array cannot contain nulls, amongst other checks.
            let (_stream_schema, arrays, num_rows) = batch.into_parts();
            let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
            batch = RecordBatch::try_new_with_options(
                Arc::clone(&self.output_schema),
                arrays,
                &options,
            )?;
        }
        Ok(batch)
    }
}

type ConstantColumns = HashMap<String, ScalarValue>;

/// Extract constant column values from statistics, keyed by column name in the logical file schema.
fn constant_columns_from_stats(
    statistics: Option<&Statistics>,
    file_schema: &SchemaRef,
) -> ConstantColumns {
    let mut constants = HashMap::new();
    let Some(statistics) = statistics else {
        return constants;
    };

    let num_rows = match statistics.num_rows {
        Precision::Exact(num_rows) => Some(num_rows),
        _ => None,
    };

    for (idx, column_stats) in statistics
        .column_statistics
        .iter()
        .take(file_schema.fields().len())
        .enumerate()
    {
        let field = file_schema.field(idx);
        if let Some(value) =
            constant_value_from_stats(column_stats, num_rows, field.data_type())
        {
            constants.insert(field.name().clone(), value);
        }
    }

    constants
}

fn constant_value_from_stats(
    column_stats: &ColumnStatistics,
    num_rows: Option<usize>,
    data_type: &DataType,
) -> Option<ScalarValue> {
    if let (Precision::Exact(min), Precision::Exact(max)) =
        (&column_stats.min_value, &column_stats.max_value)
        && min == max
        && !min.is_null()
        && matches!(column_stats.null_count, Precision::Exact(0))
    {
        // Cast to the expected data type if needed (e.g., Utf8 -> Dictionary)
        if min.data_type() != *data_type {
            return min.cast_to(data_type).ok();
        }
        return Some(min.clone());
    }

    if let (Some(num_rows), Precision::Exact(nulls)) =
        (num_rows, &column_stats.null_count)
        && *nulls == num_rows
    {
        return ScalarValue::try_new_null(data_type).ok();
    }

    None
}

/// Wraps an inner RecordBatchStream and a [`FilePruner`]
///
/// This can terminate the scan early when some dynamic filters is updated after
/// the scan starts, so we discover after the scan starts that the file can be
/// pruned (can't have matching rows).
struct EarlyStoppingStream<S> {
    /// Has the stream finished processing? All subsequent polls will return
    /// None
    done: bool,
    file_pruner: FilePruner,
    files_ranges_pruned_statistics: PruningMetrics,
    /// The inner stream
    inner: S,
}

impl<S> EarlyStoppingStream<S> {
    pub fn new(
        stream: S,
        file_pruner: FilePruner,
        files_ranges_pruned_statistics: PruningMetrics,
    ) -> Self {
        Self {
            done: false,
            inner: stream,
            file_pruner,
            files_ranges_pruned_statistics,
        }
    }
}

impl<S> EarlyStoppingStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    fn check_prune(&mut self, input: Result<RecordBatch>) -> Result<Option<RecordBatch>> {
        let batch = input?;

        // Since dynamic filters may have been updated, see if we can stop
        // reading this stream entirely.
        if self.file_pruner.should_prune()? {
            self.files_ranges_pruned_statistics.add_pruned(1);
            // Previously this file range has been counted as matched
            self.files_ranges_pruned_statistics.subtract_matched(1);
            self.done = true;
            Ok(None)
        } else {
            // Return the adapted batch
            Ok(Some(batch))
        }
    }
}

impl<S> Stream for EarlyStoppingStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match ready!(self.inner.poll_next_unpin(cx)) {
            None => {
                // input done
                self.done = true;
                Poll::Ready(None)
            }
            Some(input_batch) => {
                let output = self.check_prune(input_batch);
                Poll::Ready(output.transpose())
            }
        }
    }
}

#[derive(Default)]
struct EncryptionContext {
    #[cfg(feature = "parquet_encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    #[cfg(feature = "parquet_encryption")]
    encryption_factory: Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
}

#[cfg(feature = "parquet_encryption")]
impl EncryptionContext {
    fn new(
        file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
        encryption_factory: Option<(
            Arc<dyn EncryptionFactory>,
            EncryptionFactoryOptions,
        )>,
    ) -> Self {
        Self {
            file_decryption_properties,
            encryption_factory,
        }
    }

    async fn get_file_decryption_properties(
        &self,
        file_location: &object_store::path::Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        match &self.file_decryption_properties {
            Some(file_decryption_properties) => {
                Ok(Some(Arc::clone(file_decryption_properties)))
            }
            None => match &self.encryption_factory {
                Some((encryption_factory, encryption_config)) => Ok(encryption_factory
                    .get_file_decryption_properties(encryption_config, file_location)
                    .await?),
                None => Ok(None),
            },
        }
    }
}

#[cfg(not(feature = "parquet_encryption"))]
#[expect(dead_code)]
impl EncryptionContext {
    async fn get_file_decryption_properties(
        &self,
        _file_location: &object_store::path::Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        Ok(None)
    }
}

impl ParquetMorselizer {
    #[cfg(feature = "parquet_encryption")]
    fn get_encryption_context(&self) -> EncryptionContext {
        EncryptionContext::new(
            self.file_decryption_properties.clone(),
            self.encryption_factory.clone(),
        )
    }

    #[cfg(not(feature = "parquet_encryption"))]
    #[expect(dead_code)]
    fn get_encryption_context(&self) -> EncryptionContext {
        EncryptionContext::default()
    }
}

/// Return the initial [`ParquetAccessPlan`]
///
/// If the user has supplied one as an extension, use that
/// otherwise return a plan that scans all row groups
///
/// Returns an error if an invalid `ParquetAccessPlan` is provided
///
/// Note: file_name is only used for error messages
fn create_initial_plan(
    file_name: &str,
    extensions: Option<Arc<dyn std::any::Any + Send + Sync>>,
    row_group_count: usize,
) -> Result<ParquetAccessPlan> {
    if let Some(extensions) = extensions {
        if let Some(access_plan) = extensions.downcast_ref::<ParquetAccessPlan>() {
            let plan_len = access_plan.len();
            if plan_len != row_group_count {
                return exec_err!(
                    "Invalid ParquetAccessPlan for {file_name}. Specified {plan_len} row groups, but file has {row_group_count}"
                );
            }

            // check row group count matches the plan
            return Ok(access_plan.clone());
        } else {
            debug!("DataSourceExec Ignoring unknown extension specified for {file_name}");
        }
    }

    // default to scanning all row groups
    Ok(ParquetAccessPlan::new_all(row_group_count))
}

/// Build a page pruning predicate from an optional predicate expression.
/// If the predicate is None or the predicate cannot be converted to a page pruning
/// predicate, return None.
pub(crate) fn build_page_pruning_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
) -> Arc<PagePruningAccessPlanFilter> {
    Arc::new(PagePruningAccessPlanFilter::new(
        predicate,
        Arc::clone(file_schema),
    ))
}

pub(crate) fn build_pruning_predicates(
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    file_schema: &SchemaRef,
    predicate_creation_errors: &Count,
) -> Option<Arc<PruningPredicate>> {
    let predicate = predicate.as_ref()?;
    build_pruning_predicate(
        Arc::clone(predicate),
        file_schema,
        predicate_creation_errors,
    )
}

/// Returns a `ArrowReaderMetadata` with the page index loaded, loading
/// it from the underlying `AsyncFileReader` if necessary.
async fn load_page_index<T: AsyncFileReader>(
    reader_metadata: ArrowReaderMetadata,
    input: &mut T,
    options: ArrowReaderOptions,
) -> Result<ArrowReaderMetadata> {
    let parquet_metadata = reader_metadata.metadata();
    let missing_column_index = parquet_metadata.column_index().is_none();
    let missing_offset_index = parquet_metadata.offset_index().is_none();
    // You may ask yourself: why are we even checking if the page index is already loaded here?
    // Didn't we explicitly *not* load it above?
    // Well it's possible that a custom implementation of `AsyncFileReader` gives you
    // the page index even if you didn't ask for it (e.g. because it's cached)
    // so it's important to check that here to avoid extra work.
    if missing_column_index || missing_offset_index {
        let m = Arc::try_unwrap(Arc::clone(parquet_metadata))
            .unwrap_or_else(|e| e.as_ref().clone());
        let mut reader = ParquetMetaDataReader::new_with_metadata(m)
            .with_page_index_policy(PageIndexPolicy::Optional);
        reader.load_page_index(input).await?;
        let new_parquet_metadata = reader.finish()?;
        let new_arrow_reader =
            ArrowReaderMetadata::try_new(Arc::new(new_parquet_metadata), options)?;
        Ok(new_arrow_reader)
    } else {
        // No need to load the page index again, just return the existing metadata
        Ok(reader_metadata)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::{ConstantColumns, ParquetMorselizer, constant_columns_from_stats};
    use crate::{DefaultParquetFileReaderFactory, RowGroupAccess};
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use bytes::{BufMut, BytesMut};
    use datafusion_common::{
        ColumnStatistics, ScalarValue, Statistics, internal_err, record_batch,
        stats::Precision,
    };
    use datafusion_datasource::morsel::{Morsel, Morselizer};
    use datafusion_datasource::{PartitionedFile, TableSchema};
    use datafusion_expr::{col, lit};
    use datafusion_physical_expr::{
        PhysicalExpr,
        expressions::{Column, DynamicFilterPhysicalExpr, Literal},
        planner::logical2physical,
        projection::ProjectionExprs,
    };
    use datafusion_physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, replace_columns_with_literals,
    };
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::collections::VecDeque;
    use std::sync::Arc;

    /// Builder for creating [`ParquetMorselizer`] instances with sensible defaults for tests.
    /// This helps reduce code duplication and makes it clear what differs between test cases.
    struct ParquetMorselizerBuilder {
        store: Option<Arc<dyn ObjectStore>>,
        table_schema: Option<TableSchema>,
        partition_index: usize,
        projection_indices: Option<Vec<usize>>,
        projection: Option<ProjectionExprs>,
        batch_size: usize,
        limit: Option<usize>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
        metrics: ExecutionPlanMetricsSet,
        pushdown_filters: bool,
        reorder_filters: bool,
        force_filter_selections: bool,
        enable_page_index: bool,
        enable_bloom_filter: bool,
        enable_row_group_stats_pruning: bool,
        coerce_int96: Option<TimeUnit>,
        max_predicate_cache_size: Option<usize>,
        reverse_row_groups: bool,
        preserve_order: bool,
    }

    impl ParquetMorselizerBuilder {
        /// Create a new builder with sensible defaults for tests.
        fn new() -> Self {
            Self {
                store: None,
                table_schema: None,
                partition_index: 0,
                projection_indices: None,
                projection: None,
                batch_size: 1024,
                limit: None,
                predicate: None,
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                pushdown_filters: false,
                reorder_filters: false,
                force_filter_selections: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                enable_row_group_stats_pruning: false,
                coerce_int96: None,
                max_predicate_cache_size: None,
                reverse_row_groups: false,
                preserve_order: false,
            }
        }

        /// Set the object store (required for building).
        fn with_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
            self.store = Some(store);
            self
        }

        /// Create a simple table schema from a file schema (for files without partition columns).
        fn with_schema(mut self, file_schema: SchemaRef) -> Self {
            self.table_schema = Some(TableSchema::from_file_schema(file_schema));
            self
        }

        /// Set a custom table schema (for files with partition columns).
        fn with_table_schema(mut self, table_schema: TableSchema) -> Self {
            self.table_schema = Some(table_schema);
            self
        }

        /// Set projection by column indices (convenience method for common case).
        fn with_projection_indices(mut self, indices: &[usize]) -> Self {
            self.projection_indices = Some(indices.to_vec());
            self
        }

        /// Set the predicate.
        fn with_predicate(mut self, predicate: Arc<dyn PhysicalExpr>) -> Self {
            self.predicate = Some(predicate);
            self
        }

        /// Enable pushdown filters.
        fn with_pushdown_filters(mut self, enable: bool) -> Self {
            self.pushdown_filters = enable;
            self
        }

        /// Enable filter reordering.
        fn with_reorder_filters(mut self, enable: bool) -> Self {
            self.reorder_filters = enable;
            self
        }

        /// Enable row group stats pruning.
        fn with_row_group_stats_pruning(mut self, enable: bool) -> Self {
            self.enable_row_group_stats_pruning = enable;
            self
        }

        /// Enable page index.
        fn with_enable_page_index(mut self, enable: bool) -> Self {
            self.enable_page_index = enable;
            self
        }

        /// Set reverse row groups flag.
        fn with_reverse_row_groups(mut self, enable: bool) -> Self {
            self.reverse_row_groups = enable;
            self
        }

        /// Build the ParquetMorselizer instance.
        ///
        /// # Panics
        ///
        /// Panics if required fields (store, schema/table_schema) are not set.
        fn build(self) -> ParquetMorselizer {
            let store = self
                .store
                .expect("ParquetMorselizerBuilder: store must be set via with_store()");
            let table_schema = self.table_schema.expect(
                "ParquetMorselizerBuilder: table_schema must be set via with_schema() or with_table_schema()",
            );
            let file_schema = Arc::clone(table_schema.file_schema());

            let projection = if let Some(projection) = self.projection {
                projection
            } else if let Some(indices) = self.projection_indices {
                ProjectionExprs::from_indices(&indices, &file_schema)
            } else {
                // Default: project all columns
                let all_indices: Vec<usize> = (0..file_schema.fields().len()).collect();
                ProjectionExprs::from_indices(&all_indices, &file_schema)
            };

            ParquetMorselizer {
                partition_index: self.partition_index,
                projection,
                batch_size: self.batch_size,
                limit: self.limit,
                preserve_order: self.preserve_order,
                predicate: self.predicate,
                table_schema,
                metadata_size_hint: self.metadata_size_hint,
                metrics: self.metrics,
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(store),
                ),
                pushdown_filters: self.pushdown_filters,
                reorder_filters: self.reorder_filters,
                force_filter_selections: self.force_filter_selections,
                enable_page_index: self.enable_page_index,
                enable_bloom_filter: self.enable_bloom_filter,
                enable_row_group_stats_pruning: self.enable_row_group_stats_pruning,
                coerce_int96: self.coerce_int96,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: self.max_predicate_cache_size,
                reverse_row_groups: self.reverse_row_groups,
            }
        }
    }

    /// Test helper that drives a [`ParquetMorselizer`] to completion and returns
    /// the first stream morsel it produces.
    ///
    /// This mirrors how `FileStream` consumes the morsel APIs: it repeatedly
    /// plans CPU work, awaits any discovered I/O futures, and feeds the planner
    /// back into the ready queue until a stream morsel is ready.
    async fn open_file(
        morselizer: &ParquetMorselizer,
        file: PartitionedFile,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut planners = VecDeque::from([morselizer.plan_file(file)?]);
        let mut morsels: VecDeque<Box<dyn Morsel>> = VecDeque::new();

        loop {
            if let Some(morsel) = morsels.pop_front() {
                return Ok(Box::pin(morsel.into_stream()));
            }

            let Some(planner) = planners.pop_front() else {
                return Ok(Box::pin(futures::stream::empty()));
            };

            if let Some(mut plan) = planner.plan()? {
                morsels.extend(plan.take_morsels());
                planners.extend(plan.take_ready_planners());

                if let Some(pending_planner) = plan.take_pending_planner() {
                    planners.push_front(pending_planner.await?);
                    continue;
                }

                if morsels.is_empty() && planners.is_empty() {
                    return internal_err!("planner returned an empty morsel plan");
                }
            }
        }
    }

    fn constant_int_stats() -> (Statistics, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let statistics = Statistics {
            num_rows: Precision::Exact(3),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::from(5i32)),
                    min_value: Precision::Exact(ScalarValue::from(5i32)),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Absent,
                },
                ColumnStatistics::new_unknown(),
            ],
        };
        (statistics, schema)
    }

    #[test]
    fn extract_constant_columns_non_null() {
        let (statistics, schema) = constant_int_stats();
        let constants = constant_columns_from_stats(Some(&statistics), &schema);
        assert_eq!(constants.len(), 1);
        assert_eq!(constants.get("a"), Some(&ScalarValue::from(5i32)));
        assert!(!constants.contains_key("b"));
    }

    #[test]
    fn extract_constant_columns_all_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let statistics = Statistics {
            num_rows: Precision::Exact(2),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(2),
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };

        let constants = constant_columns_from_stats(Some(&statistics), &schema);
        assert_eq!(
            constants.get("a"),
            Some(&ScalarValue::Utf8(None)),
            "all-null column should be treated as constant null"
        );
    }

    #[test]
    fn rewrite_projection_to_literals() {
        let (statistics, schema) = constant_int_stats();
        let constants = constant_columns_from_stats(Some(&statistics), &schema);
        let projection = ProjectionExprs::from_indices(&[0, 1], &schema);

        let rewritten = projection
            .try_map_exprs(|expr| replace_columns_with_literals(expr, &constants))
            .unwrap();
        let exprs = rewritten.as_ref();
        assert!(exprs[0].expr.downcast_ref::<Literal>().is_some());
        assert!(exprs[1].expr.downcast_ref::<Column>().is_some());

        // Only column `b` should remain in the projection mask
        assert_eq!(rewritten.column_indices(), vec![1]);
    }

    #[test]
    fn rewrite_physical_expr_literal() {
        let mut constants = ConstantColumns::new();
        constants.insert("a".to_string(), ScalarValue::from(7i32));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));

        let rewritten = replace_columns_with_literals(expr, &constants).unwrap();
        assert!(rewritten.downcast_ref::<Literal>().is_some());
    }

    async fn count_batches_and_rows(
        mut stream: BoxStream<'static, Result<RecordBatch>>,
    ) -> (usize, usize) {
        let mut num_batches = 0;
        let mut num_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            num_rows += batch.num_rows();
            num_batches += 1;
        }
        (num_batches, num_rows)
    }

    /// Helper to collect all int32 values from the first column of batches
    async fn collect_int32_values(
        mut stream: BoxStream<'static, Result<RecordBatch>>,
    ) -> Vec<i32> {
        use arrow::array::Array;
        let mut values = vec![];
        while let Some(Ok(batch)) = stream.next().await {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            for i in 0..array.len() {
                if !array.is_null(i) {
                    values.push(array.value(i));
                }
            }
        }
        values
    }

    async fn write_parquet(
        store: Arc<dyn ObjectStore>,
        filename: &str,
        batch: arrow::record_batch::RecordBatch,
    ) -> usize {
        write_parquet_batches(store, filename, vec![batch], None).await
    }

    /// Write multiple batches to a parquet file with optional writer properties
    async fn write_parquet_batches(
        store: Arc<dyn ObjectStore>,
        filename: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
        props: Option<WriterProperties>,
    ) -> usize {
        let mut out = BytesMut::new().writer();
        {
            let schema = batches[0].schema();
            let mut writer = ArrowWriter::try_new(&mut out, schema, props).unwrap();
            for batch in batches {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
        let data = out.into_inner().freeze();
        let data_len = data.len();
        store.put(&Path::from(filename), data.into()).await.unwrap();
        data_len
    }

    fn make_dynamic_expr(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            expr.children().into_iter().map(Arc::clone).collect(),
            expr,
        ))
    }

    #[tokio::test]
    async fn test_prune_on_statistics() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(
            ("a", Int32, vec![Some(1), Some(2), Some(2)]),
            ("b", Float32, vec![Some(1.0), Some(2.0), None])
        )
        .unwrap();

        let data_size =
            write_parquet(Arc::clone(&store), "test.parquet", batch.clone()).await;

        let schema = batch.schema();
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        )
        .with_statistics(Arc::new(
            Statistics::new_unknown(&schema)
                .add_column_statistics(ColumnStatistics::new_unknown())
                .add_column_statistics(
                    ColumnStatistics::new_unknown()
                        .with_min_value(Precision::Exact(ScalarValue::Float32(Some(1.0))))
                        .with_max_value(Precision::Exact(ScalarValue::Float32(Some(2.0))))
                        .with_null_count(Precision::Exact(1)),
                ),
        ));

        let make_opener = |predicate| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_projection_indices(&[0, 1])
                .with_predicate(predicate)
                .with_row_group_stats_pruning(true)
                .build()
        };

        // A filter on "a" should not exclude any rows even if it matches the data
        let expr = col("a").eq(lit(1));
        let predicate = logical2physical(&expr, &schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // A filter on `b = 5.0` should exclude all rows
        let expr = col("b").eq(lit(ScalarValue::Float32(Some(5.0))));
        let predicate = logical2physical(&expr, &schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    #[tokio::test]
    async fn test_prune_on_partition_statistics_with_dynamic_expression() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "part=1/file.parquet", batch.clone()).await;

        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(
            "part=1/file.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        file.partition_values = vec![ScalarValue::Int32(Some(1))];

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));

        let table_schema_for_opener = TableSchema::new(
            file_schema.clone(),
            vec![Arc::new(Field::new("part", DataType::Int32, false))],
        );
        let make_opener = |predicate| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema_for_opener.clone())
                .with_projection_indices(&[0])
                .with_predicate(predicate)
                .with_row_group_stats_pruning(true)
                .build()
        };

        // Filter should match the partition value
        let expr = col("part").eq(lit(1));
        // Mark the expression as dynamic even if it's not to force partition pruning to happen
        // Otherwise we assume it already happened at the planning stage and won't re-do the work here
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value
        let expr = col("part").eq(lit(2));
        // Mark the expression as dynamic even if it's not to force partition pruning to happen
        // Otherwise we assume it already happened at the planning stage and won't re-do the work here
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    #[tokio::test]
    async fn test_prune_on_partition_values_and_file_statistics() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(
            ("a", Int32, vec![Some(1), Some(2), Some(3)]),
            ("b", Float64, vec![Some(1.0), Some(2.0), None])
        )
        .unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "part=1/file.parquet", batch.clone()).await;
        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(
            "part=1/file.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        file.partition_values = vec![ScalarValue::Int32(Some(1))];
        file.statistics = Some(Arc::new(
            Statistics::new_unknown(&file_schema)
                .add_column_statistics(ColumnStatistics::new_unknown())
                .add_column_statistics(
                    ColumnStatistics::new_unknown()
                        .with_min_value(Precision::Exact(ScalarValue::Float64(Some(1.0))))
                        .with_max_value(Precision::Exact(ScalarValue::Float64(Some(2.0))))
                        .with_null_count(Precision::Exact(1)),
                ),
        ));
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, true),
        ]));
        let table_schema_for_opener = TableSchema::new(
            file_schema.clone(),
            vec![Arc::new(Field::new("part", DataType::Int32, false))],
        );
        let make_opener = |predicate| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema_for_opener.clone())
                .with_projection_indices(&[0])
                .with_predicate(predicate)
                .with_row_group_stats_pruning(true)
                .build()
        };

        // Filter should match the partition value and file statistics
        let expr = col("part").eq(lit(1)).and(col("b").eq(lit(1.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Should prune based on partition value but not file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(1.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on file statistics but not partition value
        let expr = col("part").eq(lit(1)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on both partition value and file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    #[tokio::test]
    async fn test_prune_on_partition_value_and_data_value() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Note: number 3 is missing!
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(4)])).unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "part=1/file.parquet", batch.clone()).await;

        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(
            "part=1/file.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        file.partition_values = vec![ScalarValue::Int32(Some(1))];

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));

        let table_schema_for_opener = TableSchema::new(
            file_schema.clone(),
            vec![Arc::new(Field::new("part", DataType::Int32, false))],
        );
        let make_opener = |predicate| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema_for_opener.clone())
                .with_projection_indices(&[0])
                .with_predicate(predicate)
                .with_pushdown_filters(true) // note that this is true!
                .with_reorder_filters(true)
                .build()
        };

        // Filter should match the partition value and data value
        let expr = col("part").eq(lit(1)).or(col("a").eq(lit(1)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should match the partition value but not the data value
        let expr = col("part").eq(lit(1)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value but match the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(1)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 1);

        // Filter should not match the partition value or the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    /// Test that if the filter is not a dynamic filter and we have no stats we don't do extra pruning work at the file level.
    #[tokio::test]
    async fn test_opener_pruning_skipped_on_static_filters() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "part=1/file.parquet", batch.clone()).await;

        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(
            "part=1/file.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        file.partition_values = vec![ScalarValue::Int32(Some(1))];
        file.statistics = Some(Arc::new(
            Statistics::default().add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(1))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(3))))
                    .with_null_count(Precision::Exact(0)),
            ),
        ));

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("part", DataType::Int32, false),
        ]));

        let table_schema_for_opener = TableSchema::new(
            file_schema.clone(),
            vec![Arc::new(Field::new("part", DataType::Int32, false))],
        );
        let make_opener = |predicate| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema_for_opener.clone())
                .with_projection_indices(&[0])
                .with_predicate(predicate)
                .build()
        };

        // This filter could prune based on statistics, but since it's not dynamic it's not applied for pruning
        // (the assumption is this happened already at planning time)
        let expr = col("a").eq(lit(42));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // If we make the filter dynamic, it should prune.
        // This allows dynamic filters to prune partitions/files even if they are populated late into execution.
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // If we have a filter that touches partition columns only and is dynamic, it should prune even if there are no stats.
        file.statistics = Some(Arc::new(Statistics::new_unknown(&file_schema)));
        let expr = col("part").eq(lit(2));
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Similarly a filter that combines partition and data columns should prune even if there are no stats.
        let expr = col("part").eq(lit(2)).and(col("a").eq(lit(42)));
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    #[tokio::test]
    async fn test_reverse_scan_row_groups() {
        use parquet::file::properties::WriterProperties;

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Create multiple batches to ensure multiple row groups
        let batch1 =
            record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let batch2 =
            record_batch!(("a", Int32, vec![Some(4), Some(5), Some(6)])).unwrap();
        let batch3 =
            record_batch!(("a", Int32, vec![Some(7), Some(8), Some(9)])).unwrap();

        // Write parquet file with multiple row groups
        // Force small row groups by setting max_row_group_size
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3)) // Force each batch into its own row group
            .build();

        let data_len = write_parquet_batches(
            Arc::clone(&store),
            "test.parquet",
            vec![batch1.clone(), batch2, batch3],
            Some(props),
        )
        .await;

        let schema = batch1.schema();
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        );

        let make_opener = |reverse_scan: bool| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_projection_indices(&[0])
                .with_reverse_row_groups(reverse_scan)
                .build()
        };

        // Test normal scan (forward)
        let opener = make_opener(false);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let forward_values = collect_int32_values(stream).await;

        // Test reverse scan
        let opener = make_opener(true);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let reverse_values = collect_int32_values(stream).await;

        // The forward scan should return data in the order written
        assert_eq!(forward_values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // With reverse scan, row groups are reversed, so we expect:
        // Row group 3 (7,8,9), then row group 2 (4,5,6), then row group 1 (1,2,3)
        assert_eq!(reverse_values, vec![7, 8, 9, 4, 5, 6, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_reverse_scan_single_row_group() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Create a single batch (single row group)
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "test.parquet", batch.clone()).await;

        let schema = batch.schema();
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );

        let make_opener = |reverse_scan: bool| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_projection_indices(&[0])
                .with_reverse_row_groups(reverse_scan)
                .build()
        };

        // With a single row group, forward and reverse should be the same
        // (only the row group order is reversed, not the rows within)
        let opener_forward = make_opener(false);
        let stream_forward = open_file(&opener_forward, file.clone()).await.unwrap();
        let (batches_forward, _) = count_batches_and_rows(stream_forward).await;

        let opener_reverse = make_opener(true);
        let stream_reverse = open_file(&opener_reverse, file).await.unwrap();
        let (batches_reverse, _) = count_batches_and_rows(stream_reverse).await;

        // Both should have the same number of batches since there's only one row group
        assert_eq!(batches_forward, batches_reverse);
    }

    #[tokio::test]
    async fn test_reverse_scan_with_row_selection() {
        use parquet::file::properties::WriterProperties;

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Create 3 batches with DIFFERENT selection patterns
        let batch1 =
            record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3), Some(4)]))
                .unwrap(); // 4 rows
        let batch2 =
            record_batch!(("a", Int32, vec![Some(5), Some(6), Some(7), Some(8)]))
                .unwrap(); // 4 rows
        let batch3 =
            record_batch!(("a", Int32, vec![Some(9), Some(10), Some(11), Some(12)]))
                .unwrap(); // 4 rows

        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .build();

        let data_len = write_parquet_batches(
            Arc::clone(&store),
            "test.parquet",
            vec![batch1.clone(), batch2, batch3],
            Some(props),
        )
        .await;

        let schema = batch1.schema();

        use crate::ParquetAccessPlan;
        use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

        let mut access_plan = ParquetAccessPlan::new_all(3);
        // Row group 0: skip first 2, select last 2 (should get: 3, 4)
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::skip(2), RowSelector::select(2)]),
        );
        // Row group 1: select all (should get: 5, 6, 7, 8)
        // Row group 2: select first 2, skip last 2 (should get: 9, 10)
        access_plan.scan_selection(
            2,
            RowSelection::from(vec![RowSelector::select(2), RowSelector::skip(2)]),
        );

        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        )
        .with_extensions(Arc::new(access_plan));

        let make_opener = |reverse_scan: bool| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_projection_indices(&[0])
                .with_reverse_row_groups(reverse_scan)
                .build()
        };

        // Forward scan: RG0(3,4), RG1(5,6,7,8), RG2(9,10)
        let opener = make_opener(false);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let forward_values = collect_int32_values(stream).await;

        // Forward scan should produce: RG0(3,4), RG1(5,6,7,8), RG2(9,10)
        assert_eq!(
            forward_values,
            vec![3, 4, 5, 6, 7, 8, 9, 10],
            "Forward scan should select correct rows based on RowSelection"
        );

        // Reverse scan
        // CORRECT behavior: reverse row groups AND their corresponding selections
        // - RG2 is read first, WITH RG2's selection (select 2, skip 2) -> 9, 10
        // - RG1 is read second, WITH RG1's selection (select all) -> 5, 6, 7, 8
        // - RG0 is read third, WITH RG0's selection (skip 2, select 2) -> 3, 4
        let opener = make_opener(true);
        let stream = open_file(&opener, file).await.unwrap();
        let reverse_values = collect_int32_values(stream).await;

        // Correct expected result: row groups reversed but each keeps its own selection
        // RG2 with its selection (9,10), RG1 with its selection (5,6,7,8), RG0 with its selection (3,4)
        assert_eq!(
            reverse_values,
            vec![9, 10, 5, 6, 7, 8, 3, 4],
            "Reverse scan should reverse row group order while maintaining correct RowSelection for each group"
        );
    }

    #[tokio::test]
    async fn test_reverse_scan_with_non_contiguous_row_groups() {
        use parquet::file::properties::WriterProperties;

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Create 4 batches (4 row groups)
        let batch0 = record_batch!(("a", Int32, vec![Some(1), Some(2)])).unwrap();
        let batch1 = record_batch!(("a", Int32, vec![Some(3), Some(4)])).unwrap();
        let batch2 = record_batch!(("a", Int32, vec![Some(5), Some(6)])).unwrap();
        let batch3 = record_batch!(("a", Int32, vec![Some(7), Some(8)])).unwrap();

        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();

        let data_len = write_parquet_batches(
            Arc::clone(&store),
            "test.parquet",
            vec![batch0.clone(), batch1, batch2, batch3],
            Some(props),
        )
        .await;

        let schema = batch0.schema();

        use crate::ParquetAccessPlan;
        use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

        // KEY: Skip RG1 (non-contiguous!)
        // Only scan row groups: [0, 2, 3]
        let mut access_plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Scan, // RG0
            RowGroupAccess::Skip, // RG1 - SKIPPED!
            RowGroupAccess::Scan, // RG2
            RowGroupAccess::Scan, // RG3
        ]);

        // Add RowSelection for each scanned row group
        // RG0: select first row (1), skip second (2)
        access_plan.scan_selection(
            0,
            RowSelection::from(vec![RowSelector::select(1), RowSelector::skip(1)]),
        );
        // RG1: skipped, no selection needed
        // RG2: select first row (5), skip second (6)
        access_plan.scan_selection(
            2,
            RowSelection::from(vec![RowSelector::select(1), RowSelector::skip(1)]),
        );
        // RG3: select first row (7), skip second (8)
        access_plan.scan_selection(
            3,
            RowSelection::from(vec![RowSelector::select(1), RowSelector::skip(1)]),
        );

        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        )
        .with_extensions(Arc::new(access_plan));

        let make_opener = |reverse_scan: bool| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_projection_indices(&[0])
                .with_reverse_row_groups(reverse_scan)
                .build()
        };

        // Forward scan: RG0(1), RG2(5), RG3(7)
        // Note: RG1 is completely skipped
        let opener = make_opener(false);
        let stream = open_file(&opener, file.clone()).await.unwrap();
        let forward_values = collect_int32_values(stream).await;

        assert_eq!(
            forward_values,
            vec![1, 5, 7],
            "Forward scan with non-contiguous row groups"
        );

        // Reverse scan: RG3(7), RG2(5), RG0(1)
        // WITHOUT the bug fix, this would return WRONG values
        // because the RowSelection would be incorrectly mapped
        let opener = make_opener(true);
        let stream = open_file(&opener, file).await.unwrap();
        let reverse_values = collect_int32_values(stream).await;

        assert_eq!(
            reverse_values,
            vec![7, 5, 1],
            "Reverse scan with non-contiguous row groups should correctly map RowSelection"
        );
    }

    /// Test that page pruning predicates are only built and applied when `enable_page_index` is true.
    ///
    /// The file has a single row group with 10 pages (10 rows each, values 1..100).
    /// With page index enabled, pages whose max value <= 90 are pruned, returning only
    /// the last page (rows 91..100). With page index disabled, all 100 rows are returned
    /// since neither pushdown nor row-group pruning is active.
    #[tokio::test]
    async fn test_page_pruning_predicate_respects_enable_page_index() {
        use parquet::file::properties::WriterProperties;

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // 100 rows with values 1..=100, written as a single row group with 10 rows per page
        let values: Vec<i32> = (1..=100).collect();
        let batch = record_batch!((
            "a",
            Int32,
            values.iter().map(|v| Some(*v)).collect::<Vec<_>>()
        ))
        .unwrap();
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(10)
            .set_write_batch_size(10)
            .build();
        let schema = batch.schema();
        let data_size = write_parquet_batches(
            Arc::clone(&store),
            "test.parquet",
            vec![batch],
            Some(props),
        )
        .await;

        let file = PartitionedFile::new("test.parquet".to_string(), data_size as u64);

        // predicate: a > 90 — should allow page index to prune first 9 pages
        let predicate = logical2physical(&col("a").gt(lit(90i32)), &schema);

        let make_morselizer = |enable_page_index| {
            ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_predicate(Arc::clone(&predicate))
                .with_enable_page_index(enable_page_index)
                // disable pushdown and row-group pruning so the only pruning path is page index
                .with_pushdown_filters(false)
                .with_row_group_stats_pruning(false)
                .build()
        };
        let (_, rows_with_page_index) = count_batches_and_rows(
            open_file(&make_morselizer(true), file.clone())
                .await
                .unwrap(),
        )
        .await;
        let (_, rows_without_page_index) = count_batches_and_rows(
            open_file(&make_morselizer(false), file).await.unwrap(),
        )
        .await;

        assert_eq!(
            rows_with_page_index, 10,
            "page index should prune 9 of 10 pages"
        );
        assert_eq!(
            rows_without_page_index, 100,
            "without page index all rows are returned"
        );
    }
}
