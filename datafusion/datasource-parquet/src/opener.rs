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

use crate::access_plan::PreparedAccessPlan;
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
use std::collections::{HashMap, VecDeque};
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
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader, RowFilter,
    RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::parquet_column;
use parquet::arrow::push_decoder::{
    ParquetPushDecoder, ParquetPushDecoderBuilder, StrategySwap,
};
use parquet::basic::Type;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

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
    /// Optional predicate conjuncts for row filtering during the scan.
    /// Each conjunct is tagged with a stable `FilterId` for the adaptive
    /// [`crate::selectivity::SelectivityTracker`] so per-filter stats
    /// accumulate across files.
    pub predicate_conjuncts:
        Option<Vec<(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)>>,
    /// Adaptive selectivity tracker shared across files. Each opener feeds
    /// per-batch stats and asks for the current optimal split between
    /// row-level and post-scan placement at row-group boundaries.
    pub selectivity_tracker: Arc<crate::selectivity::SelectivityTracker>,
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
    extensions: datafusion_datasource::FileExtensions,
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
    /// Predicate conjuncts with stable `FilterId`s for the adaptive
    /// selectivity tracker. Carried forward from
    /// [`ParquetMorselizer::predicate_conjuncts`]. The combined predicate
    /// (used for pruning and `FilePruner`) is recomputed on demand from
    /// these conjuncts.
    predicate_conjuncts:
        Option<Vec<(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)>>,
    /// Shared adaptive selectivity tracker.
    selectivity_tracker: Arc<crate::selectivity::SelectivityTracker>,
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
    /// Per-conjunct row-group pruning rates surfaced as a side-effect of
    /// `prune_by_statistics_with_per_conjunct_stats`.  Threaded into the
    /// adaptive scheduler's `pruning_rates` map alongside the
    /// page-pruning rates so the initial-placement prior gets the
    /// strongest available signal per FilterId.
    row_group_per_conjunct: Vec<datafusion_pruning::PerConjunctPruneStats>,
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
        let mut predicate_conjuncts = self.predicate_conjuncts.clone();
        if !literal_columns.is_empty() {
            projection = projection.try_map_exprs(|expr| {
                replace_columns_with_literals(Arc::clone(&expr), &literal_columns)
            })?;
            // Rewrite each conjunct individually so per-conjunct FilterIds
            // remain stable and continue to refer to the same expression
            // across files (modulo literal substitution).
            if let Some(ref mut conjuncts) = predicate_conjuncts {
                for (_id, expr) in conjuncts.iter_mut() {
                    *expr = replace_columns_with_literals(
                        Arc::clone(expr),
                        &literal_columns,
                    )?;
                }
            }
        }

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Rows)
            .global_counter("num_predicate_creation_errors");

        // Combine conjuncts into a single AND-ed predicate for the file-level
        // pruner and for early statistics-driven elimination. The file
        // pruner does not need per-conjunct identities — only a boolean
        // expression over file-level columns and partition values.
        let combined_predicate: Option<Arc<dyn PhysicalExpr>> =
            predicate_conjuncts.as_ref().map(|conjuncts| {
                datafusion_physical_expr::conjunction(
                    conjuncts.iter().map(|(_, e)| Arc::clone(e)),
                )
            });

        // Apply literal replacements to projection and predicate
        let file_pruner = combined_predicate
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
            predicate_conjuncts,
            selectivity_tracker: Arc::clone(&self.selectivity_tracker),
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
    /// Reconstruct a single AND-ed predicate from the per-conjunct list.
    /// Used for pruning, page-index setup, and `FilePruner` construction —
    /// callers that don't care about the per-conjunct `FilterId` identities.
    fn combined_predicate(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.predicate_conjuncts.as_ref().map(|conjuncts| {
            datafusion_physical_expr::conjunction(
                conjuncts.iter().map(|(_, e)| Arc::clone(e)),
            )
        })
    }

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
        let needs_rewrite = prepared.predicate_conjuncts.is_some()
            || prepared.logical_file_schema != physical_file_schema;
        if needs_rewrite {
            let rewriter = prepared.expr_adapter_factory.create(
                Arc::clone(&prepared.logical_file_schema),
                Arc::clone(&physical_file_schema),
            )?;
            let simplifier = PhysicalExprSimplifier::new(&physical_file_schema);
            // Rewrite each conjunct individually so per-conjunct FilterIds
            // remain stable across files.
            if let Some(ref mut conjuncts) = prepared.predicate_conjuncts {
                for (_, expr) in conjuncts.iter_mut() {
                    *expr = simplifier.simplify(rewriter.rewrite(Arc::clone(expr))?)?;
                }
            }
            prepared.projection = prepared
                .projection
                .try_map_exprs(|p| simplifier.simplify(rewriter.rewrite(p)?))?;
        }
        prepared.physical_file_schema = Arc::clone(&physical_file_schema);

        // Combined AND-ed predicate is only used for pruning / page-index
        // setup; the conjunct-level identities are preserved separately for
        // the adaptive selectivity tracker.
        let combined_predicate = prepared.combined_predicate();

        // Build predicates for this specific file. When we have
        // FilterId-tagged conjuncts available, use the tagged
        // constructor so the row-group pruning pass surfaces
        // per-FilterId pruning rates as a side-effect of the same
        // iteration that produces the row-group prune decision (see
        // `PruningPredicate::prune_per_conjunct`). Otherwise fall back
        // to the existing combined-predicate path.
        let pruning_predicate = if let Some(conjuncts) =
            prepared.predicate_conjuncts.as_ref()
            && !conjuncts.is_empty()
        {
            match PruningPredicate::try_new_tagged_conjuncts(
                conjuncts.as_slice(),
                Arc::clone(&physical_file_schema),
            ) {
                Ok(p) if !p.always_true() => Some(Arc::new(p)),
                _ => None,
            }
        } else {
            build_pruning_predicates(
                combined_predicate.as_ref(),
                &physical_file_schema,
                &prepared.predicate_creation_errors,
            )
        };

        // Only build page pruning predicate if page index is enabled.
        // Prefer the *tagged* constructor when we have FilterId-tagged
        // conjuncts available: this lets `prune_plan_with_per_conjunct_stats`
        // surface per-FilterId pruning rates as a side-effect of the
        // pruning iteration the opener was going to run anyway. The
        // adaptive scheduler then consumes those rates as its initial-
        // placement prior — no extra pruning passes.
        let page_pruning_predicate = if prepared.enable_page_index {
            if let Some(conjuncts) = prepared.predicate_conjuncts.as_ref()
                && !conjuncts.is_empty()
            {
                let p = Arc::new(PagePruningAccessPlanFilter::new_tagged(
                    conjuncts.as_slice(),
                    &physical_file_schema,
                ));
                (p.filter_number() > 0).then_some(p)
            } else {
                combined_predicate.as_ref().and_then(|predicate| {
                    let p =
                        build_page_pruning_predicate(predicate, &physical_file_schema);
                    (p.filter_number() > 0).then_some(p)
                })
            }
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
            &prepared.extensions,
            rg_metadata.len(),
        )?);

        // If there is a range restricting what parts of the file to read
        if let Some(range) = prepared.file_range.as_ref() {
            row_groups.prune_by_range(rg_metadata, range);
        }

        // If there is a predicate that can be evaluated against the metadata
        let mut row_group_per_conjunct: Vec<datafusion_pruning::PerConjunctPruneStats> =
            Vec::new();
        if let Some(predicate) = self.pruning_predicate.as_ref().map(|p| p.as_ref()) {
            if prepared.enable_row_group_stats_pruning {
                row_group_per_conjunct = row_groups
                    .prune_by_statistics_with_per_conjunct_stats(
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
            row_group_per_conjunct,
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
            // Capture per-conjunct bloom-filter rates; merge into the
            // RowGroupsPrunedParquetOpen accumulator alongside the
            // row-group-stats rates.  Bloom filters often catch
            // string-equality / IN predicates that min/max stats miss.
            let bloom_per_conjunct = self
                .prepared
                .row_groups
                .prune_by_bloom_filters_with_per_conjunct_stats(
                    predicate,
                    &self.prepared.prepared.loaded.prepared.file_metrics,
                    &self.row_group_bloom_filters,
                );
            // Merge into the row_group_per_conjunct accumulator. For
            // each FilterId, keep the strongest signal (max
            // pruning_rate) seen across the two sources.
            for bloom in bloom_per_conjunct {
                if let Some(rate) = bloom.pruning_rate() {
                    if let Some(existing) = self
                        .prepared
                        .row_group_per_conjunct
                        .iter_mut()
                        .find(|s| s.tag == bloom.tag)
                    {
                        let existing_rate = existing.pruning_rate().unwrap_or(0.0);
                        if rate > existing_rate {
                            existing.containers_seen = bloom.containers_seen;
                            existing.containers_pruned = bloom.containers_pruned;
                        }
                    } else {
                        self.prepared.row_group_per_conjunct.push(bloom);
                    }
                }
            }
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
            row_group_per_conjunct,
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

        // Adaptive filter placement at file open. Ask the shared
        // `SelectivityTracker` to split predicate conjuncts (already adapted
        // to `physical_file_schema`) into row-level and post-scan buckets
        // based on stats accumulated across earlier files. The same split
        // is re-evaluated mid-stream at row-group boundaries via
        // `AdaptiveParquetStream::maybe_swap_strategy`.
        //
        // The set of leaf-column indices in the user projection — passed
        // to the tracker so its byte-ratio heuristic only counts filter
        // columns *not already in the projection* (a column that's in
        // the projection costs zero extra I/O to push down).
        let projection_columns: std::collections::HashSet<usize> =
            datafusion_physical_expr::utils::collect_columns(
                &datafusion_physical_expr::conjunction(prepared.projection.expr_iter()),
            )
            .iter()
            .map(|c| c.index())
            .collect();
        let projection_compressed_bytes = row_filter::total_compressed_bytes(
            &projection_columns.iter().copied().collect::<Vec<_>>(),
            file_metadata.as_ref(),
        );

        // === Run row-group + page pruning FIRST so we can capture
        //     per-conjunct page-pruning rates and feed them into
        //     `partition_filters` as the initial-placement prior. ===
        // Prune by limit if limit is set and limit order is not sensitive
        if let (Some(limit), false) = (prepared.limit, prepared.preserve_order) {
            row_groups.prune_by_limit(limit, rg_metadata, &prepared.file_metrics);
        }

        // Initial-placement prior map: per-FilterId pruning rates fed
        // to `partition_filters` so it can decide row-level vs
        // post-scan based on real selectivity stats from the prunings
        // the opener already ran.
        //
        // Two sources, both side-effects of work that happens regardless
        // of this experiment:
        //   1. Row-group min/max pruning
        //      (see `prune_by_statistics_with_per_conjunct_stats`).
        //   2. Page-index pruning (see
        //      `prune_plan_with_per_conjunct_stats`). Page-level is
        //      strictly finer-grained; when both produce a rate for
        //      the same FilterId, the page-level rate wins (it's
        //      written second).
        let mut access_plan = row_groups.build();
        let mut page_pruning_rates: HashMap<crate::selectivity::FilterId, f64> =
            HashMap::new();
        // Source 1: row-group rates
        for stats in &row_group_per_conjunct {
            if let Some(tag) = stats.tag
                && let Some(rate) = stats.pruning_rate()
            {
                page_pruning_rates.insert(tag, rate);
            }
        }
        // Source 2: page-index rates (override row-group when both are present)
        if prepared.enable_page_index
            && !access_plan.is_empty()
            && let Some(page_pruning_predicate) = page_pruning_predicate.as_ref()
        {
            let (new_plan, per_conjunct, pages_skipped_by_fully_matched) =
                page_pruning_predicate.prune_plan_with_per_conjunct_stats(
                    access_plan,
                    &prepared.physical_file_schema,
                    reader_metadata.parquet_schema(),
                    file_metadata.as_ref(),
                    &prepared.file_metrics,
                );
            access_plan = new_plan;
            for stats in per_conjunct {
                if let Some(tag) = stats.tag
                    && let Some(rate) = stats.pruning_rate()
                {
                    page_pruning_rates.insert(tag, rate);
                }
            }
            ParquetFileMetrics::add_page_index_pages_skipped_by_fully_matched(
                &prepared.metrics,
                prepared.partition_index,
                &prepared.file_name,
                pages_skipped_by_fully_matched,
            );
        }

        let (row_filter_conjuncts, mut post_scan_conjuncts) = if prepared.pushdown_filters
            && let Some(conjuncts) = prepared.predicate_conjuncts.clone()
            && !conjuncts.is_empty()
        {
            let partitioned = prepared.selectivity_tracker.partition_filters(
                conjuncts,
                &projection_columns,
                projection_compressed_bytes,
                file_metadata.as_ref(),
                &prepared.physical_file_schema,
                reader_metadata.parquet_schema(),
                &page_pruning_rates,
            );
            (partitioned.row_filters, partitioned.post_scan)
        } else {
            (Vec::new(), Vec::new())
        };

        // Build row-level `ArrowPredicate`s for the row_filters bucket. Any
        // conjunct that `build_row_filter` reports as `unbuildable` falls
        // through to the post-scan bucket so we never silently drop a
        // filter — dropping would relax the user's predicate and return
        // wrong results.
        // Capture the row-filter id set before any potential move into
        // `post_scan_conjuncts` on the error fall-through below, so the
        // adaptive stream can detect placement changes against this baseline.
        let initial_row_filter_ids: std::collections::BTreeSet<
            crate::selectivity::FilterId,
        > = row_filter_conjuncts.iter().map(|(id, _)| *id).collect();

        let row_filter = if !row_filter_conjuncts.is_empty() {
            match row_filter::build_row_filter(
                &row_filter_conjuncts,
                &prepared.physical_file_schema,
                file_metadata.as_ref(),
                projection_compressed_bytes,
                &prepared.selectivity_tracker,
                &prepared.file_metrics,
            ) {
                Ok((row_filter, unbuildable)) => {
                    post_scan_conjuncts.extend(unbuildable);
                    row_filter
                }
                Err(e) => {
                    debug!(
                        "Error building row filter for {row_filter_conjuncts:?}: {e}; \
                         falling all row-filter candidates through to post-scan"
                    );
                    post_scan_conjuncts.extend(row_filter_conjuncts.clone());
                    None
                }
            }
        } else {
            None
        };

        // If the build above failed and dropped every row-filter candidate
        // into post-scan, treat the active set as empty so the first
        // mid-stream swap will rebuild from scratch using whatever the
        // tracker decides next.
        let active_row_filter_ids = if row_filter.is_some() {
            initial_row_filter_ids
        } else {
            std::collections::BTreeSet::new()
        };

        // (prune_by_limit + page-index pruning ran above so we could
        // pass the per-conjunct page rates into `partition_filters`.)

        let arrow_reader_metrics = ArrowReaderMetrics::enabled();

        // Build the decoder's projection over (user projection ∪
        // initial post-scan filter columns). Row-level filter columns
        // live in the RowFilter's per-predicate masks, so they don't
        // need to be in the decoder's output stream batch.
        //
        // The mask is NOT fixed for the file's life — `maybe_swap_strategy`
        // can grow or shrink it at row-group boundaries when the optimal
        // mask cols change (e.g. a filter promotes to row-level so its
        // cols leave the mask, or a previously-placeholder dynamic filter
        // wakes up and its cols enter the mask via a post-scan placement).
        // Each mask change triggers a rebuild of `stream_schema`,
        // `projector`, and the post-scan filter rebase; arrow-rs's
        // `StrategySwap::with_projection` then installs the new mask
        // before the next row group is read.
        // Keep the *unrebased* projection (against physical_file_schema) so
        // dynamic-mask changes can re-rebase it against a new stream_schema.
        let original_projection = prepared.projection.clone();

        // Build initial decoder projection state (stream_schema, projection
        // mask, projector, rebased post-scan filters). Same helper is used
        // for mid-stream mask swaps in `maybe_swap_strategy`.
        let proj_state = build_decoder_projection_state(
            &original_projection,
            &post_scan_conjuncts,
            &projection_columns,
            &prepared.physical_file_schema,
            reader_metadata.parquet_schema(),
            &prepared.output_schema,
        )?;

        // Split the access plan into consecutive runs of row groups that share
        // the same filter requirement. Fully-matched runs (where row-group/page
        // statistics already proved every row satisfies the predicate) skip the
        // RowFilter *and* the post-scan filters — both are no-ops there and
        // wasted CPU/IO. Other runs use the adaptive setup computed above.
        //
        // `has_filter_work` reflects whether any conjunct survived adaptive
        // partitioning; if not, `split_runs` returns a single no-filter run.
        let has_filter_work = row_filter.is_some() || !post_scan_conjuncts.is_empty();
        let mut runs = access_plan.split_runs(has_filter_work);
        if prepared.reverse_row_groups {
            runs.reverse();
        }
        let run_count = runs.len();

        // Decoder-level limit can only be pushed in when:
        //   * there's a single run (otherwise later runs would never see the
        //     limit), AND
        //   * that run has no post-scan filters (otherwise the decoder may
        //     short-circuit before post-scan would have rejected enough rows).
        // For multi-run files, we enforce the limit at the stream level via
        // `remaining_limit`.
        let single_no_post_scan =
            run_count == 1 && (!runs[0].needs_filter || post_scan_conjuncts.is_empty());
        let decoder_limit = prepared.limit.filter(|_| single_no_post_scan);
        let remaining_limit = prepared.limit.filter(|_| !single_no_post_scan);

        // Pre-compute a stripped projection state for any !needs_filter runs,
        // since they don't need post-scan-filter columns in the decoder mask.
        // Built lazily on first use to avoid the cost when all runs need
        // the filter.
        let mut no_filter_proj_state: Option<DecoderProjectionState> = None;

        // Build one `RunDecoder` per run. The first run becomes the active
        // state of the stream; the rest queue up as `pending_runs`.
        let mut run_decoders: VecDeque<RunDecoder> = VecDeque::with_capacity(run_count);
        // Hand the eagerly-built first row_filter into the first needs_filter
        // run; subsequent needs_filter runs rebuild it from the same partition
        // (RowFilter is `!Clone`).
        let mut first_row_filter = row_filter;
        let mut first_filter_run_consumed = false;
        for run in runs {
            let prepared_access_plan = {
                let mut plan = run.access_plan.prepare(rg_metadata)?;
                if prepared.reverse_row_groups {
                    plan = plan.reverse(file_metadata.as_ref())?;
                }
                plan
            };

            let run_decoder = if run.needs_filter {
                // Adaptive setup. The first needs_filter run reuses the
                // already-built row_filter; later ones rebuild it.
                //
                // `post_scan_conjuncts` and `active_row_filter_ids` were
                // finalized above when the first `build_row_filter` ran —
                // either with the success path's `unbuildable` folded in, or
                // with the error path's full fall-through. Because
                // `build_row_filter` is deterministic given the same inputs,
                // re-running it for later runs produces an identical
                // success/failure partition, so we reuse those vars verbatim
                // and only need a fresh `RowFilter` (it is `!Clone`).
                let (row_filter, post_scan_for_run, active_ids_for_run) =
                    if !first_filter_run_consumed {
                        first_filter_run_consumed = true;
                        (
                            first_row_filter.take(),
                            post_scan_conjuncts.clone(),
                            active_row_filter_ids.clone(),
                        )
                    } else {
                        let rf = if !row_filter_conjuncts.is_empty() {
                            match row_filter::build_row_filter(
                                &row_filter_conjuncts,
                                &prepared.physical_file_schema,
                                file_metadata.as_ref(),
                                projection_compressed_bytes,
                                &prepared.selectivity_tracker,
                                &prepared.file_metrics,
                            ) {
                                Ok((rf, _unbuildable)) => rf,
                                Err(e) => {
                                    debug!(
                                        "Error rebuilding row filter for next run: {e}; \
                                         falling all row-filter candidates through to post-scan"
                                    );
                                    None
                                }
                            }
                        } else {
                            None
                        };
                        (
                            rf,
                            post_scan_conjuncts.clone(),
                            active_row_filter_ids.clone(),
                        )
                    };

                build_run_decoder(
                    /*needs_filter=*/ true,
                    prepared_access_plan,
                    reader_metadata.clone(),
                    row_filter,
                    &post_scan_for_run,
                    active_ids_for_run,
                    &proj_state,
                    projection_compressed_bytes,
                    file_metadata.as_ref(),
                    arrow_reader_metrics.clone(),
                    prepared.batch_size,
                    prepared.force_filter_selections,
                    prepared.max_predicate_cache_size,
                    decoder_limit,
                )?
            } else {
                // Fully-matched run: no row filter, no post-scan filters,
                // narrower mask (no post-scan-filter cols). Build the
                // stripped projection state once and reuse.
                if no_filter_proj_state.is_none() {
                    no_filter_proj_state = Some(build_decoder_projection_state(
                        &original_projection,
                        &[],
                        &projection_columns,
                        &prepared.physical_file_schema,
                        reader_metadata.parquet_schema(),
                        &prepared.output_schema,
                    )?);
                }
                let no_filter_state = no_filter_proj_state.as_ref().unwrap();

                build_run_decoder(
                    /*needs_filter=*/ false,
                    prepared_access_plan,
                    reader_metadata.clone(),
                    None,
                    &[],
                    std::collections::BTreeSet::new(),
                    no_filter_state,
                    projection_compressed_bytes,
                    file_metadata.as_ref(),
                    arrow_reader_metrics.clone(),
                    prepared.batch_size,
                    prepared.force_filter_selections,
                    prepared.max_predicate_cache_size,
                    decoder_limit,
                )?
            };
            run_decoders.push_back(run_decoder);
        }

        let initial = run_decoders
            .pop_front()
            .expect("split_runs always yields at least one run");
        let pending_runs = run_decoders;

        let predicate_cache_inner_records =
            prepared.file_metrics.predicate_cache_inner_records.clone();
        let predicate_cache_records =
            prepared.file_metrics.predicate_cache_records.clone();
        let filter_apply_time = prepared.file_metrics.filter_apply_time.clone();

        let output_schema = Arc::clone(&prepared.output_schema);
        let files_ranges_pruned_statistics =
            prepared.file_metrics.files_ranges_pruned_statistics.clone();
        let stream = futures::stream::unfold(
            AdaptiveParquetStream {
                decoder: initial.decoder,
                reader: prepared.async_file_reader,
                active_reader: None,
                file_metadata: Arc::clone(&file_metadata),
                parquet_schema: file_metadata.file_metadata().schema_descr_ptr(),
                physical_file_schema: Arc::clone(&prepared.physical_file_schema),
                stream_schema: initial.stream_schema,
                file_metrics: prepared.file_metrics.clone(),
                tracker: Arc::clone(&prepared.selectivity_tracker),
                all_conjuncts: prepared.predicate_conjuncts.unwrap_or_default(),
                projection_columns,
                projection_compressed_bytes,
                active_row_filter_ids: initial.active_row_filter_ids,
                post_scan_filters: initial.post_scan_filters,
                post_scan_other_bytes_per_row: initial.post_scan_other_bytes_per_row,
                filter_apply_time,
                projector: initial.projector,
                original_projection,
                current_mask_cols: initial.current_mask_cols,
                output_schema,
                replace_schema: initial.replace_schema,
                arrow_reader_metrics,
                predicate_cache_inner_records,
                predicate_cache_records,
                baseline_metrics: prepared.baseline_metrics,
                pushdown_filters: prepared.pushdown_filters,
                page_pruning_rates,
                current_run_needs_filter: initial.needs_filter,
                pending_runs,
                remaining_limit,
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

/// Bundles the per-mask decoder projection state. Produced at file open
/// from the initial filter partition and again at any row-group boundary
/// where `maybe_swap_strategy` decides the mask cols changed. Holding
/// everything together keeps the two code paths in sync — the same
/// expressions go through the same `build_projection_read_plan` ->
/// `reassign_expr_columns` -> `make_projector` chain in both cases.
struct DecoderProjectionState {
    /// Schema of batches yielded by the decoder (post-mask, pre-projector).
    stream_schema: SchemaRef,
    /// Mask installed on the decoder / passed to `StrategySwap`.
    projection_mask: parquet::arrow::ProjectionMask,
    /// Projector that maps stream batches to the user-visible output.
    projector: Projector,
    /// Post-scan filter exprs rebased against `stream_schema`.
    rebased_post_scan: Vec<(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)>,
    /// True when `stream_schema != output_schema` and `project_batch`
    /// must replace the schema metadata before yielding.
    replace_schema: bool,
    /// Leaf column indices the mask covers, kept for cheap change
    /// detection on the next swap.
    mask_cols: std::collections::BTreeSet<usize>,
}

/// Build a fresh [`DecoderProjectionState`] for the given partition of
/// post-scan filters. Used at file open with the initial partition, and
/// again on every mask-changing swap.
///
/// The "mask cols" are derived from `(user projection ∪ post_scan_conjuncts
/// columns)`. Row-level filter columns are *not* in this set — arrow-rs's
/// per-predicate masks decode them separately from the output stream
/// batch.
fn build_decoder_projection_state(
    original_projection: &ProjectionExprs,
    post_scan_conjuncts: &[(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)],
    projection_columns: &std::collections::HashSet<usize>,
    physical_file_schema: &SchemaRef,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    output_schema: &SchemaRef,
) -> Result<DecoderProjectionState> {
    let read_plan = build_projection_read_plan(
        original_projection
            .expr_iter()
            .chain(post_scan_conjuncts.iter().map(|(_, expr)| Arc::clone(expr))),
        physical_file_schema,
        parquet_schema,
    );
    let stream_schema = Arc::clone(&read_plan.projected_schema);
    let replace_schema = stream_schema != *output_schema;

    let rebased_projection = original_projection
        .clone()
        .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
    let projector = rebased_projection.make_projector(&stream_schema)?;

    let rebased_post_scan = post_scan_conjuncts
        .iter()
        .map(|(id, expr)| {
            reassign_expr_columns(Arc::clone(expr), &stream_schema).map(|e| (*id, e))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut mask_cols: std::collections::BTreeSet<usize> =
        projection_columns.iter().copied().collect();
    for (_, expr) in post_scan_conjuncts {
        for c in datafusion_physical_expr::utils::collect_columns(expr) {
            mask_cols.insert(c.index());
        }
    }

    Ok(DecoderProjectionState {
        stream_schema,
        projection_mask: read_plan.projection_mask,
        projector,
        rebased_post_scan,
        replace_schema,
        mask_cols,
    })
}

/// Per-run decoder bundle. One [`AdaptiveParquetStream`] holds one of these
/// as its active state and queues the rest in `pending_runs`. Each run gets
/// its own decoder with run-specific filter configuration:
///
/// * "Fully matched" runs (`needs_filter == false`) carry no row filter,
///   no post-scan filters, and use a stripped projection mask containing
///   only the user-projection columns — neither row-level pushdown nor
///   post-scan filtering does any work that the row-group statistics
///   haven't already proved redundant.
/// * "Needs filter" runs carry the adaptive `(row_filter, post_scan)`
///   placement decided at file open. Within such a run,
///   [`AdaptiveParquetStream::maybe_swap_strategy`] can still re-place
///   filters at row-group boundaries; placement is *not* re-evaluated when
///   crossing into a different run (the next run's setup is fixed at file
///   open and the decoder is rebuilt fresh).
struct RunDecoder {
    decoder: ParquetPushDecoder,
    /// True if this run still needs filter evaluation (row-level + post-scan
    /// as adaptively partitioned). False for fully-matched runs that skip
    /// filtering entirely.
    needs_filter: bool,
    /// Schema of batches yielded by this run's decoder.
    stream_schema: SchemaRef,
    /// Projector mapping this run's stream batches to the user-visible
    /// output. Run-specific because the stream_schema can differ across
    /// runs (no-filter runs use a narrower mask).
    projector: Projector,
    /// Post-scan filters for this run, rebased to its stream_schema.
    /// Empty for fully-matched runs.
    post_scan_filters: Vec<(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)>,
    /// Per-post-scan-filter "other-bytes-per-row" cost metric. Empty for
    /// fully-matched runs.
    post_scan_other_bytes_per_row: Vec<f64>,
    /// Leaf-column indices in this run's decoder projection mask.
    current_mask_cols: std::collections::BTreeSet<usize>,
    /// FilterIds currently applied as row-level predicates inside this
    /// run's decoder. Empty for fully-matched runs and for needs_filter
    /// runs whose `build_row_filter` returned `None`.
    active_row_filter_ids: std::collections::BTreeSet<crate::selectivity::FilterId>,
    /// True when `stream_schema != output_schema` for this run.
    replace_schema: bool,
}

/// Build a single [`RunDecoder`] for one row-group run. Used both for the
/// initial decoder built at file open and for queued decoders that the
/// stream switches into when the active decoder finishes.
///
/// `proj_state` carries the projection state appropriate for this run —
/// the full adaptive state for `needs_filter` runs, or the stripped
/// user-projection-only state for fully-matched runs. The caller is
/// responsible for picking the right `proj_state` *and* `post_scan_for_run`
/// (which must match the state used to build `proj_state`).
#[expect(clippy::too_many_arguments)]
fn build_run_decoder(
    needs_filter: bool,
    prepared_access_plan: PreparedAccessPlan,
    reader_metadata: ArrowReaderMetadata,
    row_filter: Option<RowFilter>,
    post_scan_for_run: &[(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)],
    active_row_filter_ids: std::collections::BTreeSet<crate::selectivity::FilterId>,
    proj_state: &DecoderProjectionState,
    projection_compressed_bytes: usize,
    file_metadata: &ParquetMetaData,
    arrow_reader_metrics: ArrowReaderMetrics,
    batch_size: usize,
    force_filter_selections: bool,
    max_predicate_cache_size: Option<usize>,
    decoder_limit: Option<usize>,
) -> Result<RunDecoder> {
    let mut builder = ParquetPushDecoderBuilder::new_with_metadata(reader_metadata)
        .with_projection(proj_state.projection_mask.clone())
        .with_batch_size(batch_size)
        .with_metrics(arrow_reader_metrics);

    if let Some(row_filter) = row_filter {
        builder = builder.with_row_filter(row_filter);
    }
    if force_filter_selections {
        builder = builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
    }
    if let Some(row_selection) = prepared_access_plan.row_selection {
        builder = builder.with_row_selection(row_selection);
    }
    builder = builder.with_row_groups(prepared_access_plan.row_group_indexes);
    if let Some(limit) = decoder_limit {
        builder = builder.with_limit(limit);
    }
    if let Some(max_predicate_cache_size) = max_predicate_cache_size {
        builder = builder.with_max_predicate_cache_size(max_predicate_cache_size);
    }

    let decoder = builder.build()?;

    // Per-post-scan-filter "other-bytes-per-row" cost, used by the tracker
    // to compare promote/demote utility across row-level and post-scan
    // candidates on the same axis.
    let total_rows: i64 = file_metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .sum();
    let post_scan_other_bytes_per_row: Vec<f64> = post_scan_for_run
        .iter()
        .map(|(_, expr)| {
            let filter_cols: Vec<usize> =
                datafusion_physical_expr::utils::collect_columns(expr)
                    .iter()
                    .map(|c| c.index())
                    .collect();
            let filter_compressed =
                row_filter::total_compressed_bytes(&filter_cols, file_metadata);
            if total_rows > 0 {
                projection_compressed_bytes.saturating_sub(filter_compressed) as f64
                    / total_rows as f64
            } else {
                0.0
            }
        })
        .collect();

    Ok(RunDecoder {
        decoder,
        needs_filter,
        stream_schema: Arc::clone(&proj_state.stream_schema),
        projector: proj_state.projector.clone(),
        post_scan_filters: proj_state.rebased_post_scan.clone(),
        post_scan_other_bytes_per_row,
        current_mask_cols: proj_state.mask_cols.clone(),
        active_row_filter_ids,
        replace_schema: proj_state.replace_schema,
    })
}

/// State for a stream that decodes a single Parquet file with adaptive
/// filter scheduling.
///
/// The [`transition`](Self::transition) method drives one row group at a
/// time:
///
/// 1. Pull a [`ParquetRecordBatchReader`] for the next row group via
///    [`ParquetPushDecoder::try_next_reader`], fetching ranges as needed.
/// 2. Iterate the reader synchronously; each batch goes through any
///    post-scan filters (which feed per-filter stats into the shared
///    [`SelectivityTracker`](crate::selectivity::SelectivityTracker)) and
///    then through the projector.
/// 3. When the reader exhausts, ask the tracker to re-partition filters
///    based on accumulated stats. If the placement changed, build a new
///    `RowFilter` and call [`ParquetPushDecoder::swap_strategy`] before
///    requesting the next row group.
///
/// Why one decoder per file (vs the chunk-per-row-group split in PR #9):
/// - Reuses arrow-rs `PushBuffers` across row groups so already-fetched
///   bytes that survive a strategy swap aren't re-requested.
/// - Avoids per-chunk reader minting and per-chunk `RowFilter` rebuild
///   (`RowFilter` is `!Clone`).
/// - One [`EarlyStoppingStream`] wrap covers the whole file — no
///   chunk-0-only special case for the non-`Clone` `FilePruner`.
struct AdaptiveParquetStream {
    decoder: ParquetPushDecoder,
    reader: Box<dyn AsyncFileReader>,
    /// Active row-group reader. `None` between row groups (when a swap
    /// can be applied) and at start.
    active_reader: Option<ParquetRecordBatchReader>,
    /// Parquet metadata for the file. Used by the tracker to size filter
    /// vs projection bytes when re-partitioning.
    file_metadata: Arc<ParquetMetaData>,
    /// Parquet `SchemaDescriptor`, used by the page-pruning prior in the
    /// tracker to construct `RowGroupPruningStatistics`.
    parquet_schema: Arc<parquet::schema::types::SchemaDescriptor>,
    /// Schema used for filter expressions before rebase.
    physical_file_schema: SchemaRef,
    /// Wide schema the decoder yields — including post-scan-filter columns
    /// not in the user projection. Stable across the file even when a
    /// strategy swap moves filters around.
    stream_schema: SchemaRef,
    file_metrics: ParquetFileMetrics,
    tracker: Arc<crate::selectivity::SelectivityTracker>,
    /// Full set of predicate conjuncts for this file (with stable
    /// FilterIds), re-fed to `partition_filters` at every row-group
    /// boundary.
    all_conjuncts: Vec<(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)>,
    /// Leaf-column indices in the user projection — passed to the tracker
    /// so its byte-ratio heuristic can subtract overlap with the
    /// projection (a filter column already in the projection costs no
    /// extra I/O at row-level).
    projection_columns: std::collections::HashSet<usize>,
    /// Total compressed bytes for the user projection. Constant across
    /// the file; reused at every swap decision.
    projection_compressed_bytes: usize,
    /// Set of FilterIds currently applied as row-level predicates inside
    /// the decoder. A change in this set drives the swap.
    active_row_filter_ids: std::collections::BTreeSet<crate::selectivity::FilterId>,
    /// Post-scan filters expressed against `stream_schema`. Updated on
    /// swap.
    post_scan_filters: Vec<(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)>,
    /// Per-post-scan-filter "other-bytes-per-row" cost metric — bytes of
    /// projection columns *not* referenced by this filter, amortised.
    /// Same units as the row-filter path's `other_projected_bytes_per_row`
    /// so promote/demote rankings compare on a single axis.
    post_scan_other_bytes_per_row: Vec<f64>,
    filter_apply_time: datafusion_physical_plan::metrics::Time,
    projector: Projector,
    /// The user projection expressed against `physical_file_schema`,
    /// kept here so the projector can be rebuilt against a new
    /// `stream_schema` whenever the decoder mask changes.
    original_projection: ProjectionExprs,
    /// Leaf column indices currently covered by the decoder's projection
    /// mask. Maintained in lockstep with the decoder; updated on every
    /// successful `swap_strategy(with_projection(...))`. A change in this
    /// set triggers the projector / stream_schema rebuild.
    current_mask_cols: std::collections::BTreeSet<usize>,
    output_schema: Arc<Schema>,
    replace_schema: bool,
    arrow_reader_metrics: ArrowReaderMetrics,
    predicate_cache_inner_records: Gauge,
    predicate_cache_records: Gauge,
    baseline_metrics: BaselineMetrics,
    /// Whether filter pushdown is enabled for this file. When `false`,
    /// `swap_strategy` is never called and `post_scan_filters` is empty.
    pushdown_filters: bool,
    /// Per-FilterId page-pruning rates collected as a side-effect of
    /// the page-index pruning the opener already ran on this file.
    /// Empty when page index wasn't loaded or `predicate_conjuncts` was
    /// not tagged. Threaded into every `partition_filters` call so the
    /// initial-placement prior can use real selectivity stats from the
    /// already-completed pruning instead of re-evaluating.
    page_pruning_rates: HashMap<crate::selectivity::FilterId, f64>,
    /// True when the currently-active decoder is processing a run that
    /// needs filter evaluation. False for fully-matched runs whose row
    /// groups are known to entirely satisfy the predicate from row-group
    /// (and possibly page-index) statistics — those runs skip both the
    /// row-level RowFilter and any post-scan filtering, and `maybe_swap_strategy`
    /// becomes a no-op for them.
    current_run_needs_filter: bool,
    /// Additional decoders queued for subsequent row-group runs. Used when
    /// the access plan was split because consecutive row groups have
    /// differing filter requirements (e.g. a stripe of fully-matched row
    /// groups in the middle of an otherwise filtered scan). Each entry
    /// carries the full per-run state (decoder, projector, filters, mask)
    /// because runs may use different masks / filters.
    pending_runs: VecDeque<RunDecoder>,
    /// Global remaining row limit across all decoder runs.
    ///
    /// Decoder-local limits are only safe for single-run scans without
    /// post-scan filters (see file-open setup). When the scan is split
    /// across multiple decoders, or when the active run has post-scan
    /// filters that may reject more rows, the combined stream limit is
    /// enforced here by slicing batches.
    remaining_limit: Option<usize>,
}

impl AdaptiveParquetStream {
    /// Advances the state machine until the next batch is produced, the
    /// file is fully consumed, or an error occurs. Drives one row group
    /// at a time, swapping filter strategy at row-group boundaries.
    ///
    /// Takes `self` by value so the generated future owns the state
    /// directly — same rationale as the previous `PushDecoderStreamState`:
    /// `&mut self` creates a Stacked Borrows conflict with `unfold`'s
    /// ownership across yield points under miri.
    async fn transition(mut self) -> Option<(Result<RecordBatch>, Self)> {
        loop {
            if self.remaining_limit == Some(0) {
                return None;
            }
            // Step 1: ensure we have a reader for the current row group.
            if self.active_reader.is_none() {
                // Re-evaluate filter placement at every row-group boundary.
                // Skipped for fully-matched runs (no filter to place) and
                // when pushdown is disabled.
                if self.pushdown_filters
                    && self.current_run_needs_filter
                    && let Err(e) = self.maybe_swap_strategy()
                {
                    return Some((Err(e), self));
                }
                // Pull the next reader, fetching data as needed.
                loop {
                    match self.decoder.try_next_reader() {
                        Ok(DecodeResult::NeedsData(ranges)) => {
                            let n_ranges = ranges.len();
                            let started = datafusion_common::instant::Instant::now();
                            match self.reader.get_byte_ranges(ranges.clone()).await {
                                Ok(data) => {
                                    let elapsed = started.elapsed().as_nanos() as u64;
                                    self.tracker.record_fetch(n_ranges, elapsed);
                                    if let Err(e) = self.decoder.push_ranges(ranges, data)
                                    {
                                        return Some((
                                            Err(DataFusionError::from(e)),
                                            self,
                                        ));
                                    }
                                }
                                Err(e) => {
                                    return Some((Err(DataFusionError::from(e)), self));
                                }
                            }
                        }
                        Ok(DecodeResult::Data(reader)) => {
                            self.active_reader = Some(reader);
                            break;
                        }
                        Ok(DecodeResult::Finished) => {
                            // Current run finished. If another run is queued,
                            // swap its state in and continue.
                            if let Some(next) = self.pending_runs.pop_front() {
                                self.decoder = next.decoder;
                                self.current_run_needs_filter = next.needs_filter;
                                self.stream_schema = next.stream_schema;
                                self.projector = next.projector;
                                self.post_scan_filters = next.post_scan_filters;
                                self.post_scan_other_bytes_per_row =
                                    next.post_scan_other_bytes_per_row;
                                self.current_mask_cols = next.current_mask_cols;
                                self.active_row_filter_ids = next.active_row_filter_ids;
                                self.replace_schema = next.replace_schema;
                                continue;
                            }
                            return None;
                        }
                        Err(e) => return Some((Err(DataFusionError::from(e)), self)),
                    }
                }
            }

            // Step 2: pull the next batch out of the active reader. Reader
            // iteration is synchronous because all bytes for the row group
            // were already pushed before the reader was constructed.
            let batch_result = self
                .active_reader
                .as_mut()
                .expect("active_reader set above")
                .next();
            let batch = match batch_result {
                Some(Ok(batch)) => batch,
                Some(Err(e)) => return Some((Err(DataFusionError::from(e)), self)),
                None => {
                    // Row group exhausted — drop the reader so the next
                    // iteration goes back to step 1 and considers a swap.
                    self.active_reader = None;
                    continue;
                }
            };

            // Step 3: post-scan filters + projector + schema replacement.
            //
            // Fully-matched runs (`!current_run_needs_filter`) carry an empty
            // `post_scan_filters` set by construction, so the post-scan
            // branch is effectively skipped without an extra flag check.
            let mut timer = self.baseline_metrics.elapsed_compute().timer();
            self.copy_arrow_reader_metrics();
            let filtered = if self.post_scan_filters.is_empty() {
                Ok(batch)
            } else {
                let start = datafusion_common::instant::Instant::now();
                let r = apply_post_scan_filters_with_stats(
                    batch,
                    &self.post_scan_filters,
                    &self.post_scan_other_bytes_per_row,
                    &self.tracker,
                );
                self.filter_apply_time.add_elapsed(start);
                r
            };
            match filtered {
                // Post-scan may filter every row in a batch. Skip empty
                // outputs so the consumer doesn't see noise batches.
                Ok(b) if b.num_rows() == 0 => {
                    timer.stop();
                    continue;
                }
                Ok(b) => {
                    // Apply the global cross-run row limit when enforced at
                    // the stream level. Decoder-local limits handle the
                    // single-run no-post-scan case at file open.
                    let b = if let Some(remaining) = self.remaining_limit {
                        if b.num_rows() > remaining {
                            self.remaining_limit = Some(0);
                            b.slice(0, remaining)
                        } else {
                            self.remaining_limit = Some(remaining - b.num_rows());
                            b
                        }
                    } else {
                        b
                    };
                    let result = self.project_batch(&b);
                    timer.stop();
                    drop(timer);
                    return Some((result, self));
                }
                Err(e) => {
                    timer.stop();
                    drop(timer);
                    return Some((Err(e), self));
                }
            }
        }
    }

    /// Re-evaluate filter placement at a row-group boundary. The
    /// resulting `StrategySwap` may install:
    ///
    /// - A new `RowFilter` (if the row-level filter set changed), and/or
    /// - A new `ProjectionMask` (if the optimal mask cols changed —
    ///   shrinks when a filter promotes out of post-scan, grows when a
    ///   filter newly enters post-scan, e.g. a dynamic placeholder that
    ///   woke up).
    ///
    /// When the mask changes we rebuild `stream_schema`, `projector`,
    /// and re-rebase post-scan filter exprs against the new schema. The
    /// invariant: `post_scan_filters` are always expressed in terms of
    /// `stream_schema`, and `stream_schema` always matches the decoder's
    /// current projection mask.
    ///
    /// No-op when the decoder isn't at a swap point or there are no
    /// conjuncts.
    fn maybe_swap_strategy(&mut self) -> Result<()> {
        if !self.decoder.can_swap_strategy() || self.all_conjuncts.is_empty() {
            return Ok(());
        }
        let partitioned = self.tracker.partition_filters(
            self.all_conjuncts.clone(),
            &self.projection_columns,
            self.projection_compressed_bytes,
            self.file_metadata.as_ref(),
            &self.physical_file_schema,
            self.parquet_schema.as_ref(),
            &self.page_pruning_rates,
        );

        let new_ids: std::collections::BTreeSet<crate::selectivity::FilterId> =
            partitioned.row_filters.iter().map(|(id, _)| *id).collect();

        // Cheap pre-check: if the row-filter set AND the mask cols would
        // both be identical, skip the rest of the work. The mask check
        // uses just the heuristic post-scan list (no unbuildable yet);
        // unbuildable can only grow the col set, so if we see equality
        // here we're guaranteed equality after merging — safe to bail.
        if new_ids == self.active_row_filter_ids {
            let mut tentative_mask_cols: std::collections::BTreeSet<usize> =
                self.projection_columns.iter().copied().collect();
            for (_, expr) in &partitioned.post_scan {
                for c in datafusion_physical_expr::utils::collect_columns(expr) {
                    tentative_mask_cols.insert(c.index());
                }
            }
            if tentative_mask_cols == self.current_mask_cols {
                // Placement unchanged. Post-scan and dropped filters can
                // change with stats but they don't need a decoder-level
                // swap — `apply_post_scan_filters_with_stats` already
                // consults `tracker.is_filter_skipped` per batch.
                return Ok(());
            }
        }

        // Rebuild the row filter from the new row-level set.
        let (row_filter, unbuildable) = row_filter::build_row_filter(
            &partitioned.row_filters,
            &self.physical_file_schema,
            self.file_metadata.as_ref(),
            self.projection_compressed_bytes,
            &self.tracker,
            &self.file_metrics,
        )?;

        // Combine post-scan + unbuildable into the new post-scan set.
        // Unbuildable filters may reference cols outside the heuristic
        // post-scan list, so the mask check has to wait until after we
        // merge them.
        let mut post_scan = partitioned.post_scan;
        post_scan.extend(unbuildable);

        // Rebuild the per-mask decoder state. The helper computes
        // `mask_cols` from `(projection_columns ∪ post_scan cols)` so we
        // pick up any unbuildable cols that just joined post-scan.
        let new_state = build_decoder_projection_state(
            &self.original_projection,
            &post_scan,
            &self.projection_columns,
            &self.physical_file_schema,
            self.parquet_schema.as_ref(),
            &self.output_schema,
        )?;
        let mask_will_change = new_state.mask_cols != self.current_mask_cols;

        let total_rows: i64 = self
            .file_metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum();

        let post_scan_other_bytes_per_row: Vec<f64> = post_scan
            .iter()
            .map(|(_, expr)| {
                let cols: Vec<usize> =
                    datafusion_physical_expr::utils::collect_columns(expr)
                        .iter()
                        .map(|c| c.index())
                        .collect();
                let filter_compressed = row_filter::total_compressed_bytes(
                    &cols,
                    self.file_metadata.as_ref(),
                );
                if total_rows > 0 {
                    self.projection_compressed_bytes
                        .saturating_sub(filter_compressed) as f64
                        / total_rows as f64
                } else {
                    0.0
                }
            })
            .collect();

        // Build the StrategySwap; only include `with_projection` when
        // the mask actually changed (avoid unnecessary arrow-rs internal
        // rebuilds when only the RowFilter set changed).
        let mut swap = StrategySwap::new().with_filter(row_filter);
        if mask_will_change {
            swap = swap.with_projection(new_state.projection_mask.clone());
        }

        self.decoder
            .swap_strategy(swap)
            .map_err(DataFusionError::from)?;

        // Decoder accepted the swap; commit state changes.
        self.active_row_filter_ids = new_ids;
        self.post_scan_filters = new_state.rebased_post_scan;
        self.post_scan_other_bytes_per_row = post_scan_other_bytes_per_row;
        if mask_will_change {
            self.stream_schema = new_state.stream_schema;
            self.projector = new_state.projector;
            self.replace_schema = new_state.replace_schema;
            self.current_mask_cols = new_state.mask_cols;
        }
        Ok(())
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

/// Apply a list of post-scan filters to a batch in order, AND-ing their
/// boolean masks. Each filter's evaluation reports stats to the shared
/// [`SelectivityTracker`](crate::selectivity::SelectivityTracker) in the
/// same units as the row-filter path so promote/demote decisions can
/// compare row-level and post-scan filter effectiveness on one axis.
///
/// `other_bytes_per_row[i]` is the bytes-per-row of the projection columns
/// *not* referenced by `filters[i]` — i.e. the late-materialization saving
/// per pruned row.
fn apply_post_scan_filters_with_stats(
    batch: RecordBatch,
    filters: &[(crate::selectivity::FilterId, Arc<dyn PhysicalExpr>)],
    other_bytes_per_row: &[f64],
    tracker: &crate::selectivity::SelectivityTracker,
) -> Result<RecordBatch> {
    use arrow::array::BooleanArray;
    use arrow::compute::{and, filter_record_batch};
    use datafusion_common::cast::as_boolean_array;

    if batch.num_rows() == 0 {
        return Ok(batch);
    }

    let input_rows = batch.num_rows() as u64;
    let mut combined_mask: Option<BooleanArray> = None;

    for (i, (id, expr)) in filters.iter().enumerate() {
        // Mid-stream skip: the tracker sets this flag on
        // `OptionalFilterPhysicalExpr` whose CI upper bound has fallen
        // below `min_bytes_per_sec`. Correctness is preserved because the
        // originating join independently enforces the predicate. We do
        // not update the tracker for a skipped batch.
        if tracker.is_filter_skipped(*id) {
            continue;
        }

        // Per-batch tracker bookkeeping. We measure every batch (no
        // sampling): the `Instant + tracker.update` path is hot, but
        // skipping samples delays first-promotion by N× and that
        // dominates the steady-state lock contention on
        // strongly-selective queries (Q22 / Q23 / Q24). The Welford
        // accumulator converges within the first row group either way.
        let start = datafusion_common::instant::Instant::now();
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let bool_arr = as_boolean_array(result.as_ref())?;
        let nanos = start.elapsed().as_nanos() as u64;
        let num_matched = bool_arr.true_count() as u64;

        // Convert the raw "all the non-filter projection bytes for
        // this batch" into a *scatter-aware* skippable count: only
        // the sub-windows of the bool array with zero survivors
        // represent decode work that late-materialization would
        // actually skip. A 50% filter on uniform data scores 0
        // here; a 50% filter on contiguous data scores ~0.5.
        let total_other_bytes = (other_bytes_per_row[i] * input_rows as f64) as u64;
        let skippable_bytes =
            crate::selectivity::count_skippable_bytes(bool_arr, total_other_bytes);
        tracker.update(*id, num_matched, input_rows, nanos, skippable_bytes);

        if num_matched < input_rows {
            combined_mask = Some(match combined_mask {
                Some(prev) => and(&prev, bool_arr)?,
                None => bool_arr.clone(),
            });
        }
    }

    match combined_mask {
        Some(mask) => Ok(filter_record_batch(&batch, &mask)?),
        None => Ok(batch),
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
    extensions: &datafusion_datasource::FileExtensions,
    row_group_count: usize,
) -> Result<ParquetAccessPlan> {
    if let Some(access_plan) = extensions.get::<ParquetAccessPlan>() {
        let plan_len = access_plan.len();
        if plan_len != row_group_count {
            return exec_err!(
                "Invalid ParquetAccessPlan for {file_name}. Specified {plan_len} row groups, but file has {row_group_count}"
            );
        }
        return Ok(access_plan.clone());
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

            // Split the test-supplied AND-of-conjuncts predicate into the
            // tagged-conjunct shape `ParquetMorselizer` now expects. Tests
            // continue to pass a single `Arc<dyn PhysicalExpr>` for
            // ergonomics.
            let predicate_conjuncts = self.predicate.as_ref().map(|p| {
                datafusion_physical_expr::split_conjunction(p)
                    .into_iter()
                    .enumerate()
                    .map(|(id, expr)| (id, Arc::clone(expr)))
                    .collect::<Vec<_>>()
            });

            ParquetMorselizer {
                partition_index: self.partition_index,
                projection,
                batch_size: self.batch_size,
                limit: self.limit,
                preserve_order: self.preserve_order,
                predicate_conjuncts,
                selectivity_tracker: Arc::new(
                    crate::selectivity::SelectivityTracker::default(),
                ),
                table_schema,
                metadata_size_hint: self.metadata_size_hint,
                metrics: self.metrics,
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(store),
                ),
                pushdown_filters: self.pushdown_filters,
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
        .with_extension(access_plan);

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
        .with_extension(access_plan);

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
