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

mod early_stop;
mod encryption;

use self::early_stop::EarlyStoppingStream;
#[cfg(feature = "parquet_encryption")]
use self::encryption::EncryptionContext;
use crate::access_plan::PreparedAccessPlan;
use crate::decoder_projection::DecoderProjection;
use crate::page_filter::PagePruningAccessPlanFilter;
use crate::push_decoder::{
    DecoderBuilderConfig, PushDecoderStreamState, RgPlanEntry, RowGroupPruner,
};
use crate::row_filter::RowFilterGenerator;
use crate::row_group_filter::{BloomFilterStatistics, RowGroupAccessPlanFilter};
use crate::{
    Int96Coercer, ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
    ParquetRowSelection, ParquetVirtualColumn, apply_file_schema_type_coercions,
};
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use datafusion_physical_expr_adapter::schema_rewriter::rewrite_input_file_name_in_projection;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::mem;
use std::sync::Arc;

use arrow::datatypes::{FieldRef, Schema, SchemaRef, TimeUnit};
#[cfg(feature = "parquet_encryption")]
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{
    ColumnStatistics, HashSet, Result, ScalarValue, Statistics, exec_err, internal_err,
};
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_physical_expr::expressions::{Column, DynamicFilterTracking};
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory,
};
use datafusion_pruning::{FilePruner, PruningPredicate, build_pruning_predicate};

#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::BoxStream};
use log::debug;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::parquet_column;
use parquet::basic::Type;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader, RowGroupMetaData};

/// Morselizer-level state for virtual columns, precomputed once per scan
/// partition so each file skips the validator walks, `null_replacements`
/// rebuild, and one of the `append_fields` allocations.
///
/// Only constructed when the scan actually requests virtual columns;
/// [`ParquetMorselizer`] and [`PreparedParquetOpen`] hold
/// `Option<Arc<VirtualColumnsState>>` so the zero-virtual-column path (the
/// common case) pays nothing.
pub(crate) struct VirtualColumnsState {
    /// Shared list of virtual column fields. Cloned as a `Vec` only at the
    /// arrow-rs `with_virtual_columns` call site, which takes it by value.
    virtual_columns: Arc<Vec<FieldRef>>,
    /// Null-literal substitutions keyed by virtual column name, used to strip
    /// virtual-column references from the projection fed into
    /// `build_projection_read_plan` (which only understands file columns).
    null_replacements: HashMap<String, ScalarValue>,
    /// `logical_file_schema` with the virtual columns appended. Fed into the
    /// per-file expression rewriter so virtual-column references
    /// identity-rewrite instead of being replaced with null literals.
    logical_schema_with_virtual: SchemaRef,
}

impl VirtualColumnsState {
    /// Validate each field carries a supported arrow virtual extension type
    /// and precompute the per-scan derived state.
    fn try_new(
        virtual_columns: Vec<FieldRef>,
        logical_file_schema: &SchemaRef,
    ) -> Result<Self> {
        // Gate which extension types we forward to arrow-rs. Adding a new
        // supported virtual column means adding a `ParquetVirtualColumn`
        // variant — not editing a stringly-typed allowlist here.
        for field in &virtual_columns {
            ParquetVirtualColumn::try_from(field)?;
        }
        let null_replacements = virtual_columns
            .iter()
            .map(|f| ScalarValue::try_from(f.data_type()).map(|v| (f.name().clone(), v)))
            .collect::<Result<HashMap<String, ScalarValue>>>()?;
        let logical_schema_with_virtual =
            append_fields(logical_file_schema, &virtual_columns);
        Ok(Self {
            virtual_columns: Arc::new(virtual_columns),
            null_replacements,
            logical_schema_with_virtual,
        })
    }

    /// Validated virtual column fields, in declaration order.
    pub(crate) fn virtual_columns(&self) -> &[FieldRef] {
        &self.virtual_columns
    }

    /// Null-literal substitutions keyed by virtual column name. Used to strip
    /// virtual-column references from a projection before it is fed into the
    /// parquet `ProjectionMask` (which only understands file columns).
    pub(crate) fn null_replacements(&self) -> &HashMap<String, ScalarValue> {
        &self.null_replacements
    }
}

/// Build the per-scan virtual-column state.
///
/// Two checks run here:
/// - Extension-type allowlist via [`VirtualColumnsState::try_new`]: returns
///   `Err` for unsupported virtual extension types.
/// - Predicate-reference check (when pushdown is enabled): returns `Err` if
///   the predicate references a virtual column. The contract is that callers
///   route filters through
///   [`ParquetSource::try_pushdown_filters`](crate::source::ParquetSource),
///   which classifies virtual-col filters as `PushedDown::No`. Erroring here
///   prevents silent wrong results for callers that bypass that path and set
///   the predicate directly on `ParquetSource`.
///
/// Returns `None` when the scan has no virtual columns, so callers avoid
/// allocating the shared state on the common path.
pub(crate) fn build_virtual_columns_state(
    virtual_columns: &[FieldRef],
    logical_file_schema: &SchemaRef,
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    pushdown_filters: bool,
) -> Result<Option<Arc<VirtualColumnsState>>> {
    if virtual_columns.is_empty() {
        return Ok(None);
    }
    if pushdown_filters && let Some(predicate) = predicate {
        validate_predicate_does_not_reference_virtual_columns(
            predicate,
            virtual_columns,
        )?;
    }
    let state =
        VirtualColumnsState::try_new(virtual_columns.to_vec(), logical_file_schema)?;
    Ok(Some(Arc::new(state)))
}

/// Return `base` unchanged when `extra` is empty; otherwise build a new schema
/// with `extra` appended to `base`'s fields.
pub(crate) fn append_fields(base: &SchemaRef, extra: &[FieldRef]) -> SchemaRef {
    if extra.is_empty() {
        return Arc::clone(base);
    }
    let fields = base
        .fields()
        .iter()
        .cloned()
        .chain(extra.iter().cloned())
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// Reject predicates that reference a virtual column.
///
/// arrow-rs's `RowFilter` evaluates predicates against a `ProjectionMask` that
/// addresses parquet leaves only; virtual columns (e.g. `row_number`) are
/// synthesized by the reader *after* filter evaluation and cannot be referenced
/// inside a row filter. Silently dropping such a predicate would produce wrong
/// results.
fn validate_predicate_does_not_reference_virtual_columns(
    predicate: &Arc<dyn PhysicalExpr>,
    virtual_columns: &[FieldRef],
) -> Result<()> {
    if virtual_columns.is_empty() {
        return Ok(());
    }
    let virtual_names: HashSet<&str> =
        virtual_columns.iter().map(|f| f.name().as_str()).collect();
    let mut offender: Option<String> = None;
    predicate.apply(|node: &Arc<dyn PhysicalExpr>| {
        if let Some(column) = node.downcast_ref::<Column>()
            && virtual_names.contains(column.name())
        {
            offender = Some(column.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let Some(name) = offender {
        return internal_err!(
            "Predicate references virtual column '{name}'; route via \
             ParquetSource::try_pushdown_filters."
        );
    }
    Ok(())
}

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
    /// [`DatafusionArrowPredicate`](crate::row_filter::DatafusionArrowPredicate)?
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
    /// Optional timezone applied to INT96-coerced timestamps. When `Some`, the
    /// coerced column type becomes `Timestamp(<coerce_int96>, Some(<tz>))`.
    /// No effect when `coerce_int96` is `None`.
    pub coerce_int96_tz: Option<Arc<str>>,
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
    /// Optional sort order used to reorder row groups by their min/max statistics.
    pub sort_order_for_reorder: Option<LexOrdering>,
    /// Per-scan virtual-column state (validation already performed). `None`
    /// when no virtual columns are requested — the common path.
    pub(crate) virtual_state: Option<Arc<VirtualColumnsState>>,
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
/// PruneWithStatistics
///        |
///        v
///   LoadPageIndex?   (skipped when all surviving row groups are fully matched)
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
    /// Pruning Row Groups
    PruneWithStatistics(Box<FiltersPreparedParquetOpen>),
    /// Loading [Parquet Page Index](https://parquet.apache.org/docs/file-format/pageindex/)
    LoadPageIndex(BoxFuture<'static, Result<RowGroupsPrunedParquetOpen>>),
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
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Per-scan virtual-column state, Arc-cloned from [`ParquetMorselizer`] so
    /// each file shares validated fields, precomputed null replacements, and
    /// the logical-with-virtual schema. `None` when no virtual columns were
    /// requested.
    virtual_state: Option<Arc<VirtualColumnsState>>,
    reorder_predicates: bool,
    pushdown_filters: bool,
    force_filter_selections: bool,
    enable_page_index: bool,
    enable_bloom_filter: bool,
    enable_row_group_stats_pruning: bool,
    limit: Option<usize>,
    coerce_int96: Option<TimeUnit>,
    coerce_int96_tz: Option<Arc<str>>,
    expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    predicate_creation_errors: Count,
    max_predicate_cache_size: Option<usize>,
    reverse_row_groups: bool,
    sort_order_for_reorder: Option<LexOrdering>,
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
                Ok(ParquetOpenState::PruneWithStatistics(Box::new(
                    prepared_filters,
                )))
            }
            ParquetOpenState::PruneWithStatistics(prepared) => {
                let prepared_row_groups = (*prepared).prune_row_groups()?;
                if should_load_page_index(
                    prepared_row_groups.prepared.page_pruning_predicate.as_ref(),
                    &prepared_row_groups.row_groups,
                ) {
                    Ok(ParquetOpenState::LoadPageIndex(
                        prepared_row_groups.load_page_index().boxed(),
                    ))
                } else {
                    if prepared_row_groups
                        .prepared
                        .page_pruning_predicate
                        .is_some()
                        && !prepared_row_groups.row_groups.is_empty()
                    {
                        let prepared = &prepared_row_groups.prepared.loaded.prepared;
                        ParquetFileMetrics::add_page_index_load_skipped(
                            &prepared.metrics,
                            prepared.partition_index,
                            &prepared.file_name,
                            1,
                        );
                    }
                    Ok(ParquetOpenState::LoadBloomFilters(
                        prepared_row_groups.load_bloom_filters().boxed(),
                    ))
                }
            }
            ParquetOpenState::LoadPageIndex(future) => {
                Ok(ParquetOpenState::LoadPageIndex(future))
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
                    Ok(ParquetOpenState::LoadBloomFilters(
                        future.await?.load_bloom_filters().boxed(),
                    ))
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

        // Replace any `input_file_name()` UDFs in the projection with a literal for this file.
        projection = rewrite_input_file_name_in_projection(projection, &file_name)?;

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Rows)
            .global_counter("num_predicate_creation_errors");

        // `FilePruner::try_new` decides whether a pruner is worthwhile (it needs
        // a statistics struct, and either real column statistics or a dynamic
        // filter that can prune via partition-value folding) and returns `None`
        // otherwise. For a static predicate the pruner's tracker reports no
        // changes, so it runs once and adds no ongoing cost.
        let file_pruner = predicate.as_ref().and_then(|p| {
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
            virtual_state: self.virtual_state.as_ref().map(Arc::clone),
            reorder_predicates: self.reorder_filters,
            pushdown_filters: self.pushdown_filters,
            force_filter_selections: self.force_filter_selections,
            enable_page_index: self.enable_page_index,
            enable_bloom_filter: self.enable_bloom_filter,
            enable_row_group_stats_pruning: self.enable_row_group_stats_pruning,
            limit: self.limit,
            coerce_int96: self.coerce_int96,
            coerce_int96_tz: self.coerce_int96_tz.clone(),
            expr_adapter_factory: Arc::clone(&self.expr_adapter_factory),
            predicate_creation_errors,
            max_predicate_cache_size: self.max_predicate_cache_size,
            reverse_row_groups: self.reverse_row_groups,
            sort_order_for_reorder: self.sort_order_for_reorder.clone(),
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
        // Since dynamic filters may have been updated since planning it is
        // possible that we are able to prune files now that we couldn't prune at
        // planning time. The `FilePruner` (built when the predicate is dynamic or
        // the file carries statistics) also watches any still-active dynamic
        // filter, so the
        // `EarlyStoppingStream` wrapping the scan can re-check after each batch
        // and end the stream early once a tightened filter proves the file can
        // be skipped.
        //
        // File-level statistics may prune the file without loading any row
        // groups or metadata. Partition column predicates are already folded to
        // literals (see `replace_columns_with_literals` above), so a dynamic
        // filter that references only partition columns can prune here too even
        // when the file has no column statistics, e.g.
        // `select * from t order by partition_col limit 10`.
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
        let mut options =
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
        if let Some(schema) = self.partitioned_file.arrow_schema.as_ref() {
            options = options.with_schema(Arc::clone(schema));
        }
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
        //   parquet reader will actually produce for the file's columns. Any
        //   virtual columns (see [`crate::TableSchema::virtual_columns`]) are
        //   produced separately by the reader and are not part of this schema.
        let mut physical_file_schema = Arc::clone(reader_metadata.schema());

        // The schema loaded from the file may not be the same as the
        // desired schema (for example if we want to instruct the parquet
        // reader to read strings using Utf8View instead). Update if necessary
        let mut metadata_dirty = false;
        if let Some(merged) = apply_file_schema_type_coercions(
            &prepared.logical_file_schema,
            &physical_file_schema,
        ) {
            physical_file_schema = Arc::new(merged);
            options = options.with_schema(Arc::clone(&physical_file_schema));
            metadata_dirty = true;
        }

        if let Some(ref coerce) = prepared.coerce_int96
            && let Some(merged) = Int96Coercer::new(
                reader_metadata.parquet_schema(),
                &physical_file_schema,
                coerce,
            )
            .with_timezone(prepared.coerce_int96_tz.clone())
            .coerce()
        {
            physical_file_schema = Arc::new(merged);
            options = options.with_schema(Arc::clone(&physical_file_schema));
            metadata_dirty = true;
        }

        // Arrow-rs appends virtual columns to the supplied schema internally,
        // so any `with_schema` coercion above must stay limited to file columns.
        if let Some(state) = prepared.virtual_state.as_ref() {
            options = options.with_virtual_columns((*state.virtual_columns).clone())?;
            metadata_dirty = true;
        }

        if metadata_dirty {
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
            // When virtual columns are requested, augment the logical and
            // physical schemas passed to the rewriter/simplifier with those
            // fields. The rewriter identity-rewrites references found in both
            // schemas, keeping virtual-column references as `Column` rather
            // than replacing them with null literals; the simplifier needs
            // them present so it can resolve their data types while walking
            // expression trees. We keep `physical_file_schema` itself as the
            // pure file schema so downstream predicate pushdown, pruning, and
            // row filter construction stay unaffected.
            let (logical_for_rewrite, physical_for_rewrite) =
                if let Some(state) = prepared.virtual_state.as_ref() {
                    (
                        Arc::clone(&state.logical_schema_with_virtual),
                        append_fields(&physical_file_schema, &state.virtual_columns),
                    )
                } else {
                    (
                        Arc::clone(&prepared.logical_file_schema),
                        Arc::clone(&physical_file_schema),
                    )
                };
            let rewriter = prepared.expr_adapter_factory.create(
                Arc::clone(&logical_for_rewrite),
                Arc::clone(&physical_for_rewrite),
            )?;
            let simplifier = PhysicalExprSimplifier::new(&physical_for_rewrite);
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
            rg_metadata,
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
    /// Load the page index if pruning requires it and metadata did not include it.
    async fn load_page_index(mut self) -> Result<Self> {
        self.prepared.loaded.reader_metadata = load_page_index(
            self.prepared.loaded.reader_metadata.clone(),
            &mut self.prepared.loaded.prepared.async_file_reader,
            self.prepared
                .loaded
                .options
                .clone()
                .with_page_index_policy(PageIndexPolicy::Optional),
        )
        .await?;

        Ok(self)
    }

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
            let parquet_columns: Vec<(String, usize, Type, i32)> = predicate
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
                        parquet_schema.column(column_idx).type_length(),
                    ))
                })
                .collect();

            for idx in self.row_groups.row_group_indexes() {
                let mut row_group_filters =
                    BloomFilterStatistics::with_capacity(parquet_columns.len());
                for (column_name, column_idx, physical_type, type_length) in
                    &parquet_columns
                {
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
                    row_group_filters.insert(
                        column_name,
                        bf,
                        *physical_type,
                        *type_length,
                    );
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

        // Prune by limit if limit is set and limit order is not sensitive
        if let (Some(limit), false) = (prepared.limit, prepared.preserve_order) {
            row_groups.prune_by_limit(limit, rg_metadata, &prepared.file_metrics);
        }

        // Build the access plan. Fully matched row groups have all rows
        // satisfying the predicate, so page pruning and row filter evaluation
        // can be skipped for them.
        let mut access_plan = row_groups.build();

        // Page index pruning: if all data on individual pages can
        // be ruled using page metadata, rows from other columns
        // with that range can be skipped as well.
        if prepared.enable_page_index
            && !access_plan.is_empty()
            && let Some(page_pruning_predicate) = page_pruning_predicate
        {
            let page_pruning_result = page_pruning_predicate
                .prune_plan_with_page_index_and_metrics(
                    access_plan,
                    &prepared.physical_file_schema,
                    reader_metadata.parquet_schema(),
                    file_metadata.as_ref(),
                    &prepared.file_metrics,
                );
            access_plan = page_pruning_result.access_plan;
            ParquetFileMetrics::add_page_index_pages_skipped_by_fully_matched(
                &prepared.metrics,
                prepared.partition_index,
                &prepared.file_name,
                page_pruning_result.pages_skipped_by_fully_matched,
            );
        }

        // Prepare access plans, then apply row-group ordering tweaks per
        // run. Two composable steps:
        //
        // 1. `reorder_by_statistics`: sort row groups by `min(col)` ASC.
        //    Fixes out-of-order row groups (e.g. append-heavy workloads).
        //    Skipped gracefully when statistics aren't available or the
        //    sort expression isn't a plain column.
        //
        // 2. `reverse`: flip the iteration order for DESC requests, applied
        //    AFTER any reorder so the reversed order is correct whether or
        //    not reorder changed anything. Also handles `row_selection`
        //    remapping.
        //
        // For sorted data: reorder is a no-op, reverse gives perfect DESC.
        // For unsorted data: reorder fixes the order, reverse flips for DESC.
        //
        // Both inputs come from the sort-pushdown channel —
        // `ParquetSource::try_pushdown_sort` sets `sort_order_for_reorder`
        // and/or `reverse_row_groups`.
        let prepare_access_plan =
            |plan: ParquetAccessPlan| -> Result<PreparedAccessPlan> {
                let mut prepared_plan = plan.prepare(rg_metadata)?;
                if let Some(sort_order) = prepared.sort_order_for_reorder.as_ref() {
                    prepared_plan = prepared_plan.reorder_by_statistics(
                        sort_order,
                        file_metadata.as_ref(),
                        &prepared.physical_file_schema,
                    )?;
                }
                if prepared.reverse_row_groups {
                    prepared_plan = prepared_plan.reverse(file_metadata.as_ref())?;
                }
                Ok(prepared_plan)
            };

        let arrow_reader_metrics = ArrowReaderMetrics::enabled();

        // Build the decoder projection (mask + per-batch transform) in a
        // single call. Encapsulating it behind `DecoderProjection` keeps the
        // opener's orchestration body focused on filter / decoder / stream
        // wiring.
        let decoder_projection = DecoderProjection::try_new(
            &prepared.projection,
            &prepared.physical_file_schema,
            reader_metadata.parquet_schema(),
            &prepared.output_schema,
            prepared.virtual_state.as_deref(),
        )?;

        let (decoder, rg_plan) = {
            let pushdown_predicate = prepared
                .pushdown_filters
                .then_some(prepared.predicate.as_ref())
                .flatten();
            let mut row_filter_generator = RowFilterGenerator::new(
                pushdown_predicate,
                &prepared.physical_file_schema,
                file_metadata.as_ref(),
                prepared.reorder_predicates,
                &prepared.file_metrics,
            );

            // Build the prepared access plan first — `prepare_access_plan` may
            // call `reorder_by_statistics` (for `sort_order_for_reorder`) and
            // `reverse` (for `reverse_row_groups`), both of which mutate
            // `row_group_indexes` to the physical scan order the decoder will
            // actually read. We MUST build our `rg_plan` from this reordered
            // list, otherwise our per-RG pruner check would consult the
            // metadata of a different RG than the decoder is about to yield.
            let decoder_config = DecoderBuilderConfig {
                projection_mask: decoder_projection.projection_mask(),
                batch_size: prepared.batch_size,
                arrow_reader_metrics: &arrow_reader_metrics,
                force_filter_selections: prepared.force_filter_selections,
                decoder_limit: prepared.limit,
            };

            let prepared_access_plan = prepare_access_plan(access_plan)?;
            let rg_plan: VecDeque<RgPlanEntry> = prepared_access_plan
                .row_group_indexes
                .iter()
                .copied()
                .map(|rg_index| RgPlanEntry { rg_index })
                .collect();

            let mut builder =
                decoder_config.build(prepared_access_plan, reader_metadata.clone());
            if let Some(row_filter) = row_filter_generator.next_filter() {
                builder = builder.with_row_filter(row_filter);
                if let Some(max_predicate_cache_size) = prepared.max_predicate_cache_size
                {
                    builder =
                        builder.with_max_predicate_cache_size(max_predicate_cache_size);
                }
            }

            (builder.build()?, rg_plan)
        };

        let predicate_cache_inner_records =
            prepared.file_metrics.predicate_cache_inner_records.clone();
        let predicate_cache_records =
            prepared.file_metrics.predicate_cache_records.clone();

        let files_ranges_pruned_statistics =
            prepared.file_metrics.files_ranges_pruned_statistics.clone();

        // Build a dynamic row-group pruner only when all three conditions hold:
        //   1) the scan has a predicate (so there is something to evaluate),
        //   2) the predicate has at least one not-yet-complete dynamic filter
        //      (`DynamicFilterTracking::Watching`) — static or already-complete
        //      predicates were fully consumed by `prune_by_statistics` at file
        //      open, so re-evaluating them per RG boundary would be wasted work,
        //   3) there is at least one pending RG that could be skipped.
        // The pruner subscribes once to every still-incomplete dynamic filter
        // via the `DynamicFilterTracker` watch channel (#22460), so detecting
        // a threshold change is a single atomic load — not a tree walk per
        // RG check.
        let row_group_pruner = match (&prepared.predicate, rg_plan.len() > 1) {
            (Some(predicate), true)
                if matches!(
                    DynamicFilterTracking::classify(predicate),
                    DynamicFilterTracking::Watching(_)
                ) =>
            {
                Some(RowGroupPruner::new(
                    Arc::clone(predicate),
                    Arc::clone(&prepared.physical_file_schema),
                    Arc::clone(reader_metadata.metadata()),
                    prepared.predicate_creation_errors.clone(),
                    prepared.file_metrics.predicate_evaluation_errors.clone(),
                ))
            }
            _ => None,
        };
        let row_groups_pruned_dynamic = prepared
            .file_metrics
            .row_groups_pruned_dynamic_filter
            .clone();

        let stream = PushDecoderStreamState {
            decoder: Some(decoder),
            active_reader: None,
            rg_plan,
            reader: prepared.async_file_reader,
            decoder_projection,
            arrow_reader_metrics,
            predicate_cache_inner_records,
            predicate_cache_records,
            baseline_metrics: prepared.baseline_metrics,
            row_group_pruner,
            row_groups_pruned_dynamic,
        }
        .into_stream();

        // Wrap the stream so a dynamic filter can stop the file scan early, but
        // only when the pruner is still watching a filter that can change
        // mid-scan. For a static (or already-complete) predicate the up-front
        // `prune_file` check already captured everything that can be pruned, so
        // per-batch re-checking would only add overhead.
        match prepared.file_pruner {
            Some(file_pruner) if file_pruner.is_watching() => {
                Ok(EarlyStoppingStream::new(
                    stream,
                    file_pruner,
                    files_ranges_pruned_statistics,
                )
                .boxed())
            }
            _ => Ok(stream),
        }
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

/// Return the initial [`ParquetAccessPlan`]
///
/// If the user has supplied a parquet access extension, use that; otherwise
/// return a plan that scans all row groups.
///
/// Returns an error if an invalid parquet access extension is provided.
///
/// Note: file_name is only used for error messages
fn create_initial_plan(
    file_name: &str,
    extensions: &datafusion_datasource::FileExtensions,
    rg_metadata: &[RowGroupMetaData],
) -> Result<ParquetAccessPlan> {
    let row_group_count = rg_metadata.len();
    match (
        extensions.get::<ParquetAccessPlan>(),
        extensions.get::<ParquetRowSelection>(),
    ) {
        (Some(_), Some(_)) => exec_err!(
            "Invalid parquet access extensions for {file_name}. \
            Specify either ParquetAccessPlan or ParquetRowSelection, not both"
        ),
        (Some(access_plan), None) => {
            let plan_len = access_plan.len();
            if plan_len != row_group_count {
                return exec_err!(
                    "Invalid ParquetAccessPlan for {file_name}. Specified {plan_len} row groups, but file has {row_group_count}"
                );
            }
            Ok(access_plan.clone())
        }
        (None, Some(row_selection)) => {
            ParquetAccessPlan::try_new_from_overall_row_selection(
                row_selection.selection().clone(),
                rg_metadata,
            )
        }
        // default to scanning all row groups
        (None, None) => Ok(ParquetAccessPlan::new_all(row_group_count)),
    }
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

/// Returns true if the page index must be loaded for page-level pruning.
///
/// The page index can only prune when at least one surviving row group is not
/// fully matched by row-group statistics alone.
fn should_load_page_index(
    page_pruning_predicate: Option<&Arc<PagePruningAccessPlanFilter>>,
    row_groups: &RowGroupAccessPlanFilter,
) -> bool {
    page_pruning_predicate.is_some_and(|_| {
        let fully_matched = row_groups.is_fully_matched();
        row_groups
            .row_group_indexes()
            .any(|idx| !fully_matched[idx])
    })
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
    use crate::{
        CachedParquetFileReaderFactory, DefaultParquetFileReaderFactory,
        ParquetFileReaderFactory, ParquetRowSelection, RowGroupAccess,
    };
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use bytes::{BufMut, BytesMut};
    use datafusion_common::{
        ColumnStatistics, ScalarValue, Statistics, assert_contains, internal_err,
        record_batch, stats::Precision,
    };
    use datafusion_datasource::morsel::{Morsel, Morselizer};
    use datafusion_datasource::{PartitionedFile, TableSchema, TableSchemaBuilder};
    use datafusion_execution::cache::cache_manager::{
        CachedFileMetadataEntry, FileMetadataCache,
    };
    use datafusion_execution::cache::default_cache::DefaultCache;
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
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::file::properties::WriterProperties;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
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
        parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
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

    #[test]
    fn create_initial_plan_from_parquet_row_selection_extension() {
        use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

        let mut extensions = datafusion_datasource::FileExtensions::new();
        extensions.insert(ParquetRowSelection::new(RowSelection::from(vec![
            RowSelector::select(10),
            RowSelector::skip(20),
            RowSelector::select(30),
        ])));
        let rg_metadata = row_group_metadata(&[10, 20, 30]);

        let access_plan =
            create_initial_plan("test.parquet", &extensions, &rg_metadata).unwrap();

        assert_eq!(
            access_plan,
            ParquetAccessPlan::new(vec![
                RowGroupAccess::Scan,
                RowGroupAccess::Skip,
                RowGroupAccess::Scan,
            ])
        );
    }

    #[test]
    fn create_initial_plan_rejects_multiple_access_extensions() {
        use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

        let mut extensions = datafusion_datasource::FileExtensions::new();
        extensions.insert(ParquetAccessPlan::new_all(3));
        extensions.insert(ParquetRowSelection::new(RowSelection::from(vec![
            RowSelector::select(60),
        ])));
        let rg_metadata = row_group_metadata(&[10, 20, 30]);

        let err = create_initial_plan("test.parquet", &extensions, &rg_metadata)
            .unwrap_err()
            .to_string();

        assert_contains!(
            err,
            "Specify either ParquetAccessPlan or ParquetRowSelection, not both"
        );
    }

    fn row_group_metadata(row_counts: &[i64]) -> Vec<RowGroupMetaData> {
        let schema_descr = test_schema_descr();

        row_counts
            .iter()
            .map(|num_rows| {
                let column = ColumnChunkMetaData::builder(schema_descr.column(0))
                    .set_num_values(*num_rows)
                    .build()
                    .unwrap();

                RowGroupMetaData::builder(Arc::clone(&schema_descr))
                    .set_num_rows(*num_rows)
                    .set_column_metadata(vec![column])
                    .build()
                    .unwrap()
            })
            .collect()
    }

    fn test_schema_descr() -> SchemaDescPtr {
        use parquet::basic::{LogicalType, Type as PhysicalType};
        use parquet::schema::types::Type as SchemaType;

        let field = SchemaType::primitive_type_builder("a", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .unwrap();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
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
                parquet_file_reader_factory: None,
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
            self.table_schema = Some(TableSchema::from(file_schema));
            self
        }

        /// Set a custom table schema (for files with partition columns).
        fn with_table_schema(mut self, table_schema: TableSchema) -> Self {
            self.table_schema = Some(table_schema);
            self
        }

        /// Set projection by column indices.
        ///
        /// The indices are resolved against the **file schema**, not the full
        /// table schema. Callers that need to project partition columns or
        /// virtual columns must use [`Self::with_projection`] and construct a
        /// [`ProjectionExprs`] against [`TableSchema::table_schema`].
        fn with_projection_indices(mut self, indices: &[usize]) -> Self {
            self.projection_indices = Some(indices.to_vec());
            self
        }

        /// Set an explicit projection.
        ///
        /// Prefer this over [`Self::with_projection_indices`] whenever the
        /// projection must reference partition or virtual columns, since
        /// `with_projection_indices` resolves its indices against the file
        /// schema only.
        fn with_projection(mut self, projection: ProjectionExprs) -> Self {
            self.projection = Some(projection);
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

        fn with_metrics(mut self, metrics: ExecutionPlanMetricsSet) -> Self {
            self.metrics = metrics;
            self
        }

        fn with_parquet_file_reader_factory(
            mut self,
            factory: Arc<dyn ParquetFileReaderFactory>,
        ) -> Self {
            self.parquet_file_reader_factory = Some(factory);
            self
        }

        /// Set a row limit.
        fn with_limit(mut self, limit: usize) -> Self {
            self.limit = Some(limit);
            self
        }

        /// Set reverse row groups flag.
        fn with_reverse_row_groups(mut self, enable: bool) -> Self {
            self.reverse_row_groups = enable;
            self
        }

        /// Build the ParquetMorselizer instance, unwrapping validation errors.
        ///
        /// # Panics
        ///
        /// Panics if required fields (store, schema/table_schema) are not set,
        /// or if virtual-column validation fails. Use [`Self::try_build`]
        /// when the test wants to assert on the validation error.
        fn build(self) -> ParquetMorselizer {
            self.try_build().expect("ParquetMorselizerBuilder::build")
        }

        /// Build the ParquetMorselizer instance, returning any morselizer-level
        /// validation error (e.g. unsupported virtual extension type, or a
        /// predicate that references a virtual column with
        /// `pushdown_filters=true`).
        ///
        /// # Panics
        ///
        /// Panics if required fields (store, schema/table_schema) are not set.
        fn try_build(self) -> Result<ParquetMorselizer> {
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

            let virtual_state = build_virtual_columns_state(
                table_schema.virtual_columns(),
                table_schema.file_schema(),
                self.predicate.as_ref(),
                self.pushdown_filters,
            )?;

            Ok(ParquetMorselizer {
                partition_index: self.partition_index,
                projection,
                batch_size: self.batch_size,
                limit: self.limit,
                preserve_order: self.preserve_order,
                predicate: self.predicate,
                table_schema,
                metadata_size_hint: self.metadata_size_hint,
                metrics: self.metrics,
                parquet_file_reader_factory: self
                    .parquet_file_reader_factory
                    .unwrap_or_else(|| {
                        Arc::new(DefaultParquetFileReaderFactory::new(store)) as _
                    }),
                pushdown_filters: self.pushdown_filters,
                reorder_filters: self.reorder_filters,
                force_filter_selections: self.force_filter_selections,
                enable_page_index: self.enable_page_index,
                enable_bloom_filter: self.enable_bloom_filter,
                enable_row_group_stats_pruning: self.enable_row_group_stats_pruning,
                coerce_int96: self.coerce_int96,
                // End-to-end coercion behavior (including timezone) is
                // covered by parquet.slt. No opener-level test currently
                // needs a non-default value here.
                coerce_int96_tz: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: self.max_predicate_cache_size,
                reverse_row_groups: self.reverse_row_groups,
                sort_order_for_reorder: None,
                virtual_state,
            })
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
        batch: RecordBatch,
    ) -> usize {
        write_parquet_batches(store, filename, vec![batch], None).await
    }

    /// Write multiple batches to a parquet file with optional writer properties
    async fn write_parquet_batches(
        store: Arc<dyn ObjectStore>,
        filename: &str,
        batches: Vec<RecordBatch>,
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

    fn counter_metric_value(metrics: &ExecutionPlanMetricsSet, name: &str) -> usize {
        use datafusion_physical_plan::metrics::MetricValue;
        metrics
            .clone_inner()
            .sum_by_name(name)
            .map(|metric| match metric {
                MetricValue::Count { count, .. } => count.value(),
                _ => 0,
            })
            .unwrap_or(0)
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

        let table_schema_for_opener = TableSchemaBuilder::from(&file_schema)
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "part",
                DataType::Int32,
                false,
            ))])
            .build();
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
        let table_schema_for_opener = TableSchemaBuilder::from(&file_schema)
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "part",
                DataType::Int32,
                false,
            ))])
            .build();
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

        let table_schema_for_opener = TableSchemaBuilder::from(&file_schema)
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "part",
                DataType::Int32,
                false,
            ))])
            .build();
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

        let table_schema_for_opener = TableSchemaBuilder::from(&file_schema)
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "part",
                DataType::Int32,
                false,
            ))])
            .build();
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
    async fn test_opener_prioritizes_partitioned_file_schema() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(
            ("a", Int32, vec![Some(1), Some(2), Some(2)]),
            ("b", Float32, vec![Some(1.0), Some(2.0), None])
        )
        .unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "test.parquet", batch.clone()).await;

        let schema = batch.schema();
        let query_file = async |schema: SchemaRef| -> Result<(usize, usize)> {
            let file = PartitionedFile::new(
                "test.parquet".to_string(),
                u64::try_from(data_size).unwrap(),
            )
            .with_arrow_schema(schema.clone());

            let predicate = logical2physical(&col("a").eq(lit(1)), &schema);
            let opener = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_predicate(predicate)
                .build();

            let stream = open_file(&opener, file.clone()).await?;
            Ok(count_batches_and_rows(stream).await)
        };

        let (num_batches, num_rows) =
            query_file(schema.clone()).await.expect("query_file");
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        let mismatching_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float64, true),
        ]);
        assert_eq!(
            query_file(SchemaRef::new(mismatching_schema))
                .await
                .unwrap_err()
                .message(),
            "Arrow: Incompatible supplied Arrow schema: data type mismatch for field b: requested Float64 but found Float32"
        );
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

    #[test]
    fn should_load_page_index_without_predicate() {
        use crate::RowGroupAccessPlanFilter;
        let row_groups = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(2));
        assert!(!should_load_page_index(None, &row_groups));
    }

    #[test]
    fn should_load_page_index_when_surviving_row_groups_not_fully_matched() {
        use crate::RowGroupAccessPlanFilter;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let predicate = logical2physical(&col("a").gt(lit(50i32)), &schema);
        let page_predicate = build_page_pruning_predicate(&predicate, &schema);
        let row_groups = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(2));
        assert!(should_load_page_index(Some(&page_predicate), &row_groups));
    }

    #[test]
    fn should_load_page_index_when_all_surviving_row_groups_fully_matched() {
        use crate::RowGroupAccessPlanFilter;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let predicate = logical2physical(&col("a").is_not_null(), &schema);
        let page_predicate = build_page_pruning_predicate(&predicate, &schema);
        let mut plan = ParquetAccessPlan::new_all(1);
        plan.mark_fully_matched(0);
        let row_groups = RowGroupAccessPlanFilter::new(plan);
        assert!(!should_load_page_index(Some(&page_predicate), &row_groups));
    }

    #[tokio::test]
    async fn test_page_index_skipped_when_row_groups_fully_matched() {
        use parquet::file::properties::WriterProperties;

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
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
        let data_len = write_parquet_batches(
            Arc::clone(&store),
            "test.parquet",
            vec![batch],
            Some(props),
        )
        .await;
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        );
        let predicate = logical2physical(&col("a").gt(lit(0i32)), &schema);
        let metrics = ExecutionPlanMetricsSet::new();

        let morselizer = ParquetMorselizerBuilder::new()
            .with_store(Arc::clone(&store))
            .with_schema(Arc::clone(&schema))
            .with_predicate(Arc::clone(&predicate))
            .with_enable_page_index(true)
            .with_row_group_stats_pruning(true)
            .with_pushdown_filters(false)
            .with_metrics(metrics.clone())
            .build();

        let (_, rows) =
            count_batches_and_rows(open_file(&morselizer, file).await.unwrap()).await;
        assert_eq!(rows, 100);
        assert_eq!(counter_metric_value(&metrics, "page_index_load_skipped"), 1);
    }

    #[tokio::test]
    async fn test_page_index_skipped_with_cached_reader_factory() {
        use parquet::file::properties::WriterProperties;

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let metadata_cache: Arc<FileMetadataCache> =
            Arc::new(DefaultCache::<Path, CachedFileMetadataEntry>::new(
                64 * 1024 * 1024,
            ));
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
        let data_len = write_parquet_batches(
            Arc::clone(&store),
            "test.parquet",
            vec![batch],
            Some(props),
        )
        .await;
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        );
        let predicate = logical2physical(&col("a").gt(lit(0i32)), &schema);
        let metrics = ExecutionPlanMetricsSet::new();

        let morselizer = ParquetMorselizerBuilder::new()
            .with_store(Arc::clone(&store))
            .with_schema(Arc::clone(&schema))
            .with_predicate(Arc::clone(&predicate))
            .with_enable_page_index(true)
            .with_row_group_stats_pruning(true)
            .with_pushdown_filters(false)
            .with_metrics(metrics.clone())
            .with_parquet_file_reader_factory(Arc::new(
                CachedParquetFileReaderFactory::new(
                    Arc::clone(&store),
                    Arc::clone(&metadata_cache),
                ),
            ))
            .build();

        let (_, rows) =
            count_batches_and_rows(open_file(&morselizer, file).await.unwrap()).await;
        assert_eq!(rows, 100);
        assert_eq!(counter_metric_value(&metrics, "page_index_load_skipped"), 1);

        let cached = metadata_cache
            .get(&Path::from("test.parquet"))
            .expect("metadata cache should contain the file");
        let extra_info = cached.file_metadata.extra_info();
        let page_index_cached = extra_info.get("page_index").map(String::as_str);
        assert_eq!(
            page_index_cached,
            Some("false"),
            "cached metadata should not include page index when opener skips it"
        );
    }

    #[tokio::test]
    async fn test_page_index_loaded_when_not_fully_matched() {
        use parquet::file::properties::WriterProperties;

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
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
        let data_len = write_parquet_batches(
            Arc::clone(&store),
            "test.parquet",
            vec![batch],
            Some(props),
        )
        .await;
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        );
        let predicate = logical2physical(&col("a").gt(lit(90i32)), &schema);
        let metrics = ExecutionPlanMetricsSet::new();

        let morselizer = ParquetMorselizerBuilder::new()
            .with_store(Arc::clone(&store))
            .with_schema(Arc::clone(&schema))
            .with_predicate(Arc::clone(&predicate))
            .with_enable_page_index(true)
            .with_pushdown_filters(false)
            .with_row_group_stats_pruning(false)
            .with_metrics(metrics.clone())
            .build();

        let (_, rows) =
            count_batches_and_rows(open_file(&morselizer, file).await.unwrap()).await;
        assert_eq!(rows, 10);
        assert_eq!(counter_metric_value(&metrics, "page_index_load_skipped"), 0);
    }

    async fn fully_matched_split_test_file(
        store: Arc<dyn ObjectStore>,
    ) -> (SchemaRef, PartitionedFile) {
        use parquet::file::properties::WriterProperties;

        let batch0 =
            record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let batch1 =
            record_batch!(("a", Int32, vec![Some(4), Some(5), Some(6)])).unwrap();
        let batch2 =
            record_batch!(("a", Int32, vec![Some(7), Some(1), Some(2)])).unwrap();

        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();

        let data_len = write_parquet_batches(
            Arc::clone(&store),
            "test.parquet",
            vec![batch0.clone(), batch1, batch2],
            Some(props),
        )
        .await;

        let schema = batch0.schema();
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        );
        (schema, file)
    }

    #[tokio::test]
    async fn test_fully_matched_runs_respect_global_limit() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let (schema, file) = fully_matched_split_test_file(Arc::clone(&store)).await;
        let predicate = logical2physical(&col("a").gt_eq(lit(3)), &schema);

        let opener = ParquetMorselizerBuilder::new()
            .with_store(Arc::clone(&store))
            .with_schema(Arc::clone(&schema))
            .with_projection_indices(&[0])
            .with_predicate(predicate)
            .with_pushdown_filters(true)
            .with_row_group_stats_pruning(true)
            .with_limit(4)
            .build();

        let values = collect_int32_values(open_file(&opener, file).await.unwrap()).await;
        assert_eq!(values, vec![3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_fully_matched_runs_preserve_reverse_order() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let (schema, file) = fully_matched_split_test_file(Arc::clone(&store)).await;
        let predicate = logical2physical(&col("a").gt_eq(lit(3)), &schema);

        let opener = ParquetMorselizerBuilder::new()
            .with_store(Arc::clone(&store))
            .with_schema(Arc::clone(&schema))
            .with_projection_indices(&[0])
            .with_predicate(predicate)
            .with_pushdown_filters(true)
            .with_row_group_stats_pruning(true)
            .with_reverse_row_groups(true)
            .build();

        let values = collect_int32_values(open_file(&opener, file).await.unwrap()).await;
        assert_eq!(values, vec![7, 4, 5, 6, 3]);
    }

    /// Helpers for tests that exercise parquet virtual columns
    /// (e.g. `row_number`) plumbed through `TableSchema`/`ParquetOpener`.
    mod virtual_columns {
        use super::*;
        use arrow::array::{Array, Int64Array, StringArray};
        use arrow::datatypes::FieldRef;
        use datafusion_common::config::ConfigOptions;
        use datafusion_expr::ScalarUDF;
        use datafusion_functions::core::input_file_name::InputFileNameFunc;
        use datafusion_physical_expr::{ScalarFunctionExpr, projection::ProjectionExpr};
        use parquet::arrow::RowNumber;

        /// Build a parquet `row_number` virtual column field. Spark's
        /// `_tmp_metadata_row_index` is declared nullable, so the default
        /// matches that contract; tests that need `nullable=false` can
        /// override via `with_nullable`.
        fn row_number_field(name: &str, nullable: bool) -> FieldRef {
            Arc::new(
                Field::new(name, DataType::Int64, nullable)
                    .with_extension_type(RowNumber),
            )
        }

        fn input_file_name_expr() -> Arc<dyn PhysicalExpr> {
            Arc::new(ScalarFunctionExpr::new(
                "input_file_name",
                Arc::new(ScalarUDF::from(InputFileNameFunc::new())),
                vec![],
                Arc::new(Field::new("input_file_name", DataType::Utf8, true)),
                Arc::new(ConfigOptions::default()),
            ))
        }

        /// Collect every `Int64` value from the given column in every batch
        /// of a stream. Used to verify the `row_number` column end to end.
        async fn collect_int64_values(
            mut stream: BoxStream<'static, Result<RecordBatch>>,
            column: usize,
        ) -> Vec<i64> {
            let mut out = vec![];
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                let array = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("expected Int64 column");
                for i in 0..array.len() {
                    assert!(
                        !array.is_null(i),
                        "row_number values produced by the reader must not be null"
                    );
                    out.push(array.value(i));
                }
            }
            out
        }

        /// Write a parquet file containing `num_row_groups` groups of
        /// `rows_per_group` rows with a single `value` Int64 column.
        /// Values are `0..num_row_groups*rows_per_group`.
        async fn write_grouped_file(
            store: &Arc<dyn ObjectStore>,
            path: &str,
            num_row_groups: usize,
            rows_per_group: usize,
        ) -> (SchemaRef, usize) {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let mut batches = Vec::with_capacity(num_row_groups);
            for g in 0..num_row_groups {
                let start = (g * rows_per_group) as i64;
                let values: Vec<i64> = (start..start + rows_per_group as i64).collect();
                batches.push(
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int64Array::from(values))],
                    )
                    .unwrap(),
                );
            }
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(rows_per_group))
                .build();
            let data_size =
                write_parquet_batches(Arc::clone(store), path, batches, Some(props))
                    .await;
            (schema, data_size)
        }

        #[tokio::test]
        async fn test_row_index_basic() {
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let (file_schema, data_size) =
                write_grouped_file(&store, "basic.parquet", 1, 5).await;

            let rn_field = row_number_field("row_number", false);
            let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
                .with_virtual_columns(vec![Arc::clone(&rn_field)])
                .build();
            // Project [value, row_number] — indices in table_schema are
            // [0 file:value, 1 virtual:row_number].
            let projection =
                ProjectionExprs::from_indices(&[0, 1], table_schema.table_schema());

            let morselizer = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema)
                .with_projection(projection)
                .build();

            let file = PartitionedFile::new(
                "basic.parquet".to_string(),
                u64::try_from(data_size).unwrap(),
            );
            let stream = open_file(&morselizer, file).await.unwrap();
            let row_numbers = collect_int64_values(stream, 1).await;
            assert_eq!(row_numbers, vec![0, 1, 2, 3, 4]);
        }

        #[tokio::test]
        async fn test_row_index_projection_only() {
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let (file_schema, data_size) =
                write_grouped_file(&store, "proj_only.parquet", 1, 4).await;

            let rn_field = row_number_field("row_number", false);
            let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
                .with_virtual_columns(vec![Arc::clone(&rn_field)])
                .build();
            // Project only the virtual column (index 1).
            let projection =
                ProjectionExprs::from_indices(&[1], table_schema.table_schema());

            let morselizer = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema)
                .with_projection(projection)
                .build();

            let file = PartitionedFile::new(
                "proj_only.parquet".to_string(),
                u64::try_from(data_size).unwrap(),
            );
            let stream = open_file(&morselizer, file).await.unwrap();
            let row_numbers = collect_int64_values(stream, 0).await;
            assert_eq!(row_numbers, vec![0, 1, 2, 3]);
        }

        #[tokio::test]
        async fn test_input_file_name_projection() {
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let path = "dir/input_file_name.parquet";
            let (file_schema, data_size) = write_grouped_file(&store, path, 1, 3).await;

            let projection = ProjectionExprs::new([
                ProjectionExpr::new(Arc::new(Column::new("value", 0)), "value"),
                ProjectionExpr::new(input_file_name_expr(), "file_name"),
            ]);

            let morselizer = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(file_schema)
                .with_projection(projection)
                .build();

            let file =
                PartitionedFile::new(path.to_string(), u64::try_from(data_size).unwrap());
            let mut stream = open_file(&morselizer, file).await.unwrap();
            let batch = stream.next().await.unwrap().unwrap();
            assert!(stream.next().await.is_none());

            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "value");
            assert_eq!(batch.schema().field(1).name(), "file_name");

            let file_names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("file_name column should be Utf8");
            assert_eq!(file_names.len(), 3);
            for i in 0..file_names.len() {
                assert_eq!(file_names.value(i), path);
            }
        }

        #[tokio::test]
        async fn test_row_index_multi_row_group() {
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let (file_schema, data_size) =
                write_grouped_file(&store, "multi_rg.parquet", 3, 100).await;

            let rn_field = row_number_field("row_number", false);
            let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
                .with_virtual_columns(vec![Arc::clone(&rn_field)])
                .build();
            let projection =
                ProjectionExprs::from_indices(&[0, 1], table_schema.table_schema());

            let morselizer = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema)
                .with_projection(projection)
                .build();

            let file = PartitionedFile::new(
                "multi_rg.parquet".to_string(),
                u64::try_from(data_size).unwrap(),
            );
            let stream = open_file(&morselizer, file).await.unwrap();
            let row_numbers = collect_int64_values(stream, 1).await;
            let expected: Vec<i64> = (0..300).collect();
            assert_eq!(row_numbers, expected);
        }

        #[tokio::test]
        async fn test_row_index_with_row_group_skip() {
            // 3 row groups of 100 rows. A predicate that excludes the middle
            // row group (values 100..200) must leave absolute row numbers
            // 0..100 and 200..300 intact — not 0..200. This guards against
            // the arrow-rs bug fixed in apache/arrow-rs#8863.
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let (file_schema, data_size) =
                write_grouped_file(&store, "rg_skip.parquet", 3, 100).await;

            let rn_field = row_number_field("row_number", false);
            let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
                .with_virtual_columns(vec![Arc::clone(&rn_field)])
                .build();
            let projection =
                ProjectionExprs::from_indices(&[0, 1], table_schema.table_schema());

            // `value < 100 OR value >= 200` prunes the middle row group via
            // min/max statistics.
            let expr = col("value")
                .lt(lit(100i64))
                .or(col("value").gt_eq(lit(200i64)));
            let predicate = logical2physical(&expr, table_schema.table_schema());

            let morselizer = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema)
                .with_projection(projection)
                .with_predicate(predicate)
                .with_row_group_stats_pruning(true)
                .build();

            let file = PartitionedFile::new(
                "rg_skip.parquet".to_string(),
                u64::try_from(data_size).unwrap(),
            );
            let stream = open_file(&morselizer, file).await.unwrap();
            let row_numbers = collect_int64_values(stream, 1).await;
            let expected: Vec<i64> = (0..100).chain(200..300).collect();
            assert_eq!(row_numbers, expected);
        }

        #[tokio::test]
        async fn test_row_index_with_partition_cols() {
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let (file_schema, data_size) =
                write_grouped_file(&store, "part=5/data.parquet", 1, 3).await;

            let rn_field = row_number_field("row_number", false);
            let partition_col = Arc::new(Field::new("part", DataType::Int32, false));
            let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
                .with_table_partition_cols(vec![Arc::clone(&partition_col)])
                .with_virtual_columns(vec![Arc::clone(&rn_field)])
                .build();
            // table_schema layout: [value(0), part(1), row_number(2)].
            let projection =
                ProjectionExprs::from_indices(&[0, 1, 2], table_schema.table_schema());

            let morselizer = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema)
                .with_projection(projection)
                .build();

            let mut file = PartitionedFile::new(
                "part=5/data.parquet".to_string(),
                u64::try_from(data_size).unwrap(),
            );
            file.partition_values = vec![ScalarValue::Int32(Some(5))];

            let stream = open_file(&morselizer, file).await.unwrap();
            let mut stream = stream;
            let batch = stream.next().await.unwrap().unwrap();
            assert!(stream.next().await.is_none());

            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.schema().field(0).name(), "value");
            assert_eq!(batch.schema().field(1).name(), "part");
            assert_eq!(batch.schema().field(2).name(), "row_number");

            let part = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            assert!(part.iter().all(|v| v == Some(5)));

            let rn = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let rn_values: Vec<i64> = (0..rn.len()).map(|i| rn.value(i)).collect();
            assert_eq!(rn_values, vec![0, 1, 2]);
        }

        #[tokio::test]
        async fn test_row_index_nullable_int64() {
            // Spark declares `_tmp_metadata_row_index` nullable. Verify the
            // nullability flag flows through unchanged.
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let (file_schema, data_size) =
                write_grouped_file(&store, "nullable.parquet", 1, 3).await;

            let rn_field = row_number_field("_tmp_metadata_row_index", true);
            let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
                .with_virtual_columns(vec![Arc::clone(&rn_field)])
                .build();
            let projection =
                ProjectionExprs::from_indices(&[0, 1], table_schema.table_schema());

            let morselizer = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema)
                .with_projection(projection)
                .build();

            let file = PartitionedFile::new(
                "nullable.parquet".to_string(),
                u64::try_from(data_size).unwrap(),
            );
            let mut stream = open_file(&morselizer, file).await.unwrap();
            let batch = stream.next().await.unwrap().unwrap();

            let schema_field = batch.schema().field(1).clone();
            assert_eq!(schema_field.name(), "_tmp_metadata_row_index");
            assert_eq!(schema_field.data_type(), &DataType::Int64);
            assert!(
                schema_field.is_nullable(),
                "nullable flag should be preserved for Spark's row index field"
            );
        }

        #[tokio::test]
        async fn test_unsupported_virtual_extension_type_rejected() {
            // Guard: opener must reject virtual columns carrying extension
            // types outside the tested allowlist, rather than silently
            // forwarding them to arrow-rs (where they would produce columns
            // we have not validated against DataFusion's projection and
            // predicate paths).
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let (file_schema, _data_size) =
                write_grouped_file(&store, "unsupported.parquet", 1, 1).await;

            // RowGroupIndex is a real arrow-rs virtual type but is not in
            // SUPPORTED_VIRTUAL_EXTENSION_TYPES until a test is added for it.
            let rg_field = Arc::new(
                Field::new("row_group_index", DataType::Int64, false)
                    .with_extension_type(parquet::arrow::RowGroupIndex),
            );
            let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
                .with_virtual_columns(vec![rg_field])
                .build();
            let projection =
                ProjectionExprs::from_indices(&[0, 1], table_schema.table_schema());

            // Validation now happens at morselizer-build time (once per scan
            // partition), not once per file inside `prepare_open_file`.
            let err = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_table_schema(table_schema)
                .with_projection(projection)
                .try_build()
                .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("parquet.virtual.row_group_index"),
                "error should name the unsupported extension type, got: {msg}"
            );
        }

        /// Build a morselizer + file for a 5-row single-row-group parquet at
        /// `path`, with a single `row_number` virtual column and the given
        /// physical predicate applied to
        /// `table_schema = [value(0), row_number(1)]`.
        async fn build_pushdown_morselizer(
            store: &Arc<dyn ObjectStore>,
            path: &str,
            predicate_expr: datafusion_expr::Expr,
            pushdown_filters: bool,
        ) -> Result<(ParquetMorselizer, PartitionedFile)> {
            let (file_schema, data_size) = write_grouped_file(store, path, 1, 5).await;
            let rn_field = row_number_field("row_number", false);
            let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
                .with_virtual_columns(vec![Arc::clone(&rn_field)])
                .build();
            let projection =
                ProjectionExprs::from_indices(&[0, 1], table_schema.table_schema());
            let predicate =
                logical2physical(&predicate_expr, table_schema.table_schema());

            let morselizer = ParquetMorselizerBuilder::new()
                .with_store(Arc::clone(store))
                .with_table_schema(table_schema)
                .with_projection(projection)
                .with_predicate(predicate)
                .with_pushdown_filters(pushdown_filters)
                .try_build()?;

            let file =
                PartitionedFile::new(path.to_string(), u64::try_from(data_size).unwrap());
            Ok((morselizer, file))
        }

        // The predicate-vs-virtual-column check rejects callers that bypass
        // `ParquetSource::try_pushdown_filters` (which keeps virtual-col
        // filters above the scan as a `FilterExec`) and set the predicate
        // directly on the source with pushdown enabled. Without this guard,
        // arrow-rs's `RowFilter` would silently drop the virtual-col conjunct
        // and produce wrong results.
        #[tokio::test]
        async fn test_row_index_predicate_pushdown_mixed_or_errors() {
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let expr = col("row_number")
                .eq(lit(2i64))
                .or(col("value").eq(lit(4i64)));
            let err =
                build_pushdown_morselizer(&store, "pushdown_mixed.parquet", expr, true)
                    .await
                    .unwrap_err();
            assert!(
                err.to_string().contains("try_pushdown_filters"),
                "error should mention try_pushdown_filters, got: {err}"
            );
        }

        #[tokio::test]
        async fn test_row_index_predicate_pushdown_virtual_only_errors() {
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let expr = col("row_number").eq(lit(2i64));
            let err = build_pushdown_morselizer(
                &store,
                "pushdown_virtual_only.parquet",
                expr,
                true,
            )
            .await
            .unwrap_err();
            assert!(
                err.to_string().contains("try_pushdown_filters"),
                "error should mention try_pushdown_filters, got: {err}"
            );
        }

        #[tokio::test]
        async fn test_row_index_predicate_allowed_when_pushdown_disabled() {
            // Guards the `pushdown_filters=false` path: the predicate is only
            // used for stats pruning (a no-op for row_number) and must not
            // trip the virtual-column check.
            let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
            let expr = col("row_number").eq(lit(2i64));
            let (morselizer, file) =
                build_pushdown_morselizer(&store, "pushdown_off.parquet", expr, false)
                    .await
                    .unwrap();

            let stream = open_file(&morselizer, file).await.unwrap();
            let (_batches, rows) = count_batches_and_rows(stream).await;
            assert_eq!(rows, 5);
        }
    }
}
