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

//! [`ParquetMorselizer`] for morselizing Parquet files

use crate::page_filter::PagePruningAccessPlanFilter;
use crate::row_group_filter::RowGroupAccessPlanFilter;
use crate::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
    apply_file_schema_type_coercions, coerce_int96_to_resolution, row_filter,
};
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Schema};
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use parquet::errors::ParquetError;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::mem;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{SchemaRef, TimeUnit};
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
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, PruningMetrics,
};
use datafusion_pruning::{FilePruner, PruningPredicate, build_pruning_predicate};

#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;
use futures::{FutureExt, Stream, StreamExt, ready, stream::BoxStream};
use log::debug;
use parquet::DecodeResult;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use tokio::sync::oneshot;

/// Implements [`Morselizer`] for a parquet file.
///
/// The current implementation preserves parity with the existing opener path:
///
/// 1. `morselize` creates a single planner for the input file
/// 2. the planner's first `plan` call returns an I/O future
/// 3. that future runs the copied parquet open/setup flow:
///    file pruning, metadata loading, optional page-index / bloom-filter work,
///    row-group pruning, decoder construction, and final stream setup
/// 4. the next `plan` call emits a single ready morsel wrapping that prepared stream
///
/// This keeps the behavioral parity of `opener.rs` while routing execution
/// through the new `Morselizer` / `MorselPlanner` API.
#[derive(Clone)]
pub struct ParquetMorselizer {
    state: Arc<ParquetMorselizerState>,
}

/// State needed to plan Parquet morsels
pub struct ParquetMorselizerState {
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
    /// Should the filters be evaluated during the parquet scan using the
    /// parquet row-filter predicate machinery?
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
    /// Rewrite expressions in the context of the file schema
    pub(crate) expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    /// Encryption configuration used to resolve per-file decryption properties.
    pub(crate) encryption_context: EncryptionContext,
    /// Maximum size of the predicate cache, in bytes. If none, uses
    /// the arrow-rs default.
    pub max_predicate_cache_size: Option<usize>,
    /// Whether to read row groups in reverse order
    pub reverse_row_groups: bool,
}

impl ParquetMorselizer {
    pub(crate) fn new(state: ParquetMorselizerState) -> Self {
        Self {
            state: Arc::new(state),
        }
    }
}

impl Deref for ParquetMorselizer {
    type Target = ParquetMorselizerState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl Debug for ParquetMorselizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetMorselizer")
            .field("partition_index", &self.partition_index)
            .field("batch_size", &self.batch_size)
            .field("limit", &self.limit)
            .field("preserve_order", &self.preserve_order)
            .field("metadata_size_hint", &self.metadata_size_hint)
            .field("pushdown_filters", &self.pushdown_filters)
            .field("reorder_filters", &self.reorder_filters)
            .field("force_filter_selections", &self.force_filter_selections)
            .field("enable_page_index", &self.enable_page_index)
            .field("enable_bloom_filter", &self.enable_bloom_filter)
            .field(
                "enable_row_group_stats_pruning",
                &self.enable_row_group_stats_pruning,
            )
            .field("coerce_int96", &self.coerce_int96)
            .field("max_predicate_cache_size", &self.max_predicate_cache_size)
            .field("reverse_row_groups", &self.reverse_row_groups)
            .finish()
    }
}

/// Result of preparing a PartitionedFile using CPU before any I/O.
///
/// This captures the state computed from `PartitionedFile`, the table schema,
/// and scan configuration so that later planner states only need to perform
/// async work such as metadata loading and stream construction.
struct PreparedParquetOpen {
    state: Arc<ParquetMorselizerState>,
    partitioned_file: PartitionedFile,
    file_range: Option<datafusion_datasource::FileRange>,
    extensions: Option<Arc<dyn std::any::Any + Send + Sync>>,
    file_metrics: ParquetFileMetrics,
    file_pruner: Option<FilePruner>,
    metadata_size_hint: Option<usize>,
    async_file_reader: Box<dyn AsyncFileReader>,
    logical_file_schema: SchemaRef,
    output_schema: Arc<Schema>,
    projection: ProjectionExprs,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    #[cfg(feature = "parquet_encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    baseline_metrics: BaselineMetrics,
}

/// Result of loading parquet metadata after file-level pruning has completed.
struct MetadataLoadedParquetOpen {
    prepared: PreparedParquetOpen,
    reader_metadata: ArrowReaderMetadata,
    options: ArrowReaderOptions,
}

/// Result of CPU-only preparation after metadata has been loaded.
///
/// This captures the file schema coercions and file-specific pruning predicates
/// so the next async step only has to fetch any missing page index data.
struct FiltersPreparedParquetOpen {
    loaded: MetadataLoadedParquetOpen,
    physical_file_schema: SchemaRef,
    projection: ProjectionExprs,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
}

/// Result of CPU-only row-group pruning using parquet metadata.
///
/// This captures the row groups that remain after range, statistics, and
/// limit-based pruning so the next async step can optionally load and apply
/// bloom filters before the final stream is built.
struct RowGroupsPreparedParquetOpen {
    prepared: FiltersPreparedParquetOpen,
    row_groups: RowGroupAccessPlanFilter,
}

impl ParquetMorselizerState {
    /// Perform the CPU-only setup for opening a parquet file.
    fn prepare_open_file(
        self: &Arc<Self>,
        partitioned_file: PartitionedFile,
    ) -> Result<PreparedParquetOpen> {
        // -----------------------------------
        // Step: prepare configurations, etc.
        // -----------------------------------
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

        // Apply literal replacements to projection and predicate
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
            .global_counter("num_predicate_creation_errors");

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
            state: Arc::clone(self),
            partitioned_file,
            file_range,
            extensions,
            file_metrics,
            file_pruner,
            metadata_size_hint,
            async_file_reader,
            logical_file_schema,
            output_schema,
            projection,
            predicate,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties: None,
            baseline_metrics,
        })
    }
}

impl PreparedParquetOpen {
    /// CPU-only file pruning performed before metadata I/O begins.
    ///
    /// Returns `None` if the file was completely pruned.
    fn prune_file(mut self) -> Result<Option<Self>> {
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

    /// Fetch parquet metadata once file-level pruning is complete.
    async fn load(mut self) -> Result<MetadataLoadedParquetOpen> {
        let options =
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
        #[cfg(feature = "parquet_encryption")]
        let mut options = options;
        #[cfg(feature = "parquet_encryption")]
        if let Some(fd_val) = &self.file_decryption_properties {
            options = options.with_file_decryption_properties(Arc::clone(fd_val));
        }
        let reader_metadata = {
            let mut metadata_timer = self.file_metrics.metadata_load_time.timer();
            let reader_metadata = ArrowReaderMetadata::load_async(
                &mut self.async_file_reader,
                options.clone(),
            )
            .await?;
            metadata_timer.stop();
            reader_metadata
        };
        Ok(MetadataLoadedParquetOpen {
            prepared: self,
            reader_metadata,
            options,
        })
    }
}

impl MetadataLoadedParquetOpen {
    /// Prepare file-specific filters and schema coercions after metadata is loaded.
    fn prepare_filters(self) -> Result<FiltersPreparedParquetOpen> {
        let MetadataLoadedParquetOpen {
            mut prepared,
            mut reader_metadata,
            mut options,
        } = self;
        let state = Arc::clone(&prepared.state);
        let coerce_int96 = state.coerce_int96;
        let predicate_creation_errors = MetricBuilder::new(&state.metrics)
            .global_counter("num_predicate_creation_errors");
        let expr_adapter_factory = Arc::clone(&state.expr_adapter_factory);

        // Note about schemas: we are actually dealing with **3 different schemas** here:
        // - The table schema as defined by the TableProvider.
        //   This is what the user sees, what they get when they `SELECT * FROM table`, etc.
        // - The logical file schema: this is the table schema minus any hive partition columns and projections.
        //   This is what the physical file schema is coerced to.
        // - The physical file schema: this is the schema that the arrow-rs
        //   parquet reader will actually produce.
        let logical_file_schema = Arc::clone(&prepared.logical_file_schema);
        let mut physical_file_schema = Arc::clone(reader_metadata.schema());

        // The schema loaded from the file may not be the same as the
        // desired schema (for example if we want to instruct the parquet
        // reader to read strings using Utf8View instead). Update if necessary.
        if let Some(merged) =
            apply_file_schema_type_coercions(&logical_file_schema, &physical_file_schema)
        {
            physical_file_schema = Arc::new(merged);
            options = options.with_schema(Arc::clone(&physical_file_schema));
            reader_metadata = ArrowReaderMetadata::try_new(
                Arc::clone(reader_metadata.metadata()),
                options.clone(),
            )?;
        }

        if let Some(ref coerce) = coerce_int96
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
        let rewriter = expr_adapter_factory.create(
            Arc::clone(&logical_file_schema),
            Arc::clone(&physical_file_schema),
        )?;
        let simplifier = PhysicalExprSimplifier::new(&physical_file_schema);
        prepared.predicate = prepared
            .predicate
            .map(|p| simplifier.simplify(rewriter.rewrite(p)?))
            .transpose()?;
        // Adapt projections to the physical file schema as well
        prepared.projection = prepared
            .projection
            .try_map_exprs(|p| simplifier.simplify(rewriter.rewrite(p)?))?;

        // Build predicates for this specific file
        let (pruning_predicate, page_pruning_predicate) = build_pruning_predicates(
            prepared.predicate.as_ref(),
            &physical_file_schema,
            &predicate_creation_errors,
        );

        let projection = prepared.projection.clone();
        let predicate = prepared.predicate.clone();

        Ok(FiltersPreparedParquetOpen {
            loaded: MetadataLoadedParquetOpen {
                prepared,
                reader_metadata,
                options,
            },
            physical_file_schema,
            projection,
            predicate,
            pruning_predicate,
            page_pruning_predicate,
        })
    }
}

impl FiltersPreparedParquetOpen {
    /// Fetch the page index if it is needed and missing from the loaded metadata.
    async fn load_page_index(mut self) -> Result<Self> {
        let enable_page_index = self.loaded.prepared.state.enable_page_index;
        // The page index is not stored inline in the parquet footer so the
        // metadata load above may not have read the page index structures yet.
        // If we need them for reading and they aren't yet loaded, we need to
        // load them now.
        if should_enable_page_index(enable_page_index, &self.page_pruning_predicate) {
            self.loaded.reader_metadata = load_page_index(
                self.loaded.reader_metadata,
                &mut self.loaded.prepared.async_file_reader,
                // Since we're manually loading the page index the option here
                // should not matter but we pass it in for consistency.
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
    fn prepare_row_groups(self) -> Result<RowGroupsPreparedParquetOpen> {
        let loaded = &self.loaded;
        let inner = &loaded.prepared;
        let state = &inner.state;

        // Determine which row groups to actually read. The idea is to skip
        // as many row groups as possible based on the metadata and query.
        let file_metadata = Arc::clone(loaded.reader_metadata.metadata());
        let rg_metadata = file_metadata.row_groups();
        let file_name = inner.partitioned_file.object_meta.location.to_string();
        let pruning_pred = self.pruning_predicate.as_ref().map(|p| p.as_ref());

        let access_plan =
            create_initial_plan(&file_name, inner.extensions.clone(), rg_metadata.len())?;
        let mut row_groups = RowGroupAccessPlanFilter::new(access_plan);

        // If there is a range restricting what parts of the file to read.
        if let Some(range) = inner.file_range.as_ref() {
            row_groups.prune_by_range(rg_metadata, range);
        }

        // If there is a predicate that can be evaluated against the metadata.
        if let Some(pruning_pred) = pruning_pred {
            if state.enable_row_group_stats_pruning {
                row_groups.prune_by_statistics(
                    &self.physical_file_schema,
                    loaded.reader_metadata.parquet_schema(),
                    rg_metadata,
                    pruning_pred,
                    &inner.file_metrics,
                );
            } else {
                inner
                    .file_metrics
                    .row_groups_pruned_statistics
                    .add_matched(row_groups.remaining_row_group_count());
            }

            if !state.enable_bloom_filter || row_groups.is_empty() {
                inner
                    .file_metrics
                    .row_groups_pruned_bloom_filter
                    .add_matched(row_groups.remaining_row_group_count());
            }
        } else {
            let n_remaining_row_groups = row_groups.remaining_row_group_count();
            inner
                .file_metrics
                .row_groups_pruned_statistics
                .add_matched(n_remaining_row_groups);
            inner
                .file_metrics
                .row_groups_pruned_bloom_filter
                .add_matched(n_remaining_row_groups);
        }

        Ok(RowGroupsPreparedParquetOpen {
            prepared: self,
            row_groups,
        })
    }
}

impl RowGroupsPreparedParquetOpen {
    /// Apply bloom filter pruning when it is enabled and a pruning predicate exists.
    async fn prune_bloom_filters(mut self) -> Result<Self> {
        let loaded = &mut self.prepared.loaded;
        let inner = &mut loaded.prepared;
        let state = &inner.state;
        let pruning_pred = self.prepared.pruning_predicate.as_ref().map(|p| p.as_ref());

        if let Some(pruning_pred) = pruning_pred
            && state.enable_bloom_filter
            && !self.row_groups.is_empty()
        {
            let bf_reader = mem::replace(
                &mut inner.async_file_reader,
                state.parquet_file_reader_factory.create_reader(
                    state.partition_index,
                    inner.partitioned_file.clone(),
                    inner.metadata_size_hint,
                    &state.metrics,
                )?,
            );
            let mut bf_builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                bf_reader,
                loaded.reader_metadata.clone(),
            );
            self.row_groups
                .prune_by_bloom_filters(
                    &self.prepared.physical_file_schema,
                    &mut bf_builder,
                    pruning_pred,
                    &inner.file_metrics,
                )
                .await;
        }

        Ok(self)
    }

    /// Build one or more parquet reading states once all pruning work is complete.
    ///
    /// In the common case, this returns one child reader state per selected row
    /// group. That lets each row group become its own morsel planner, which
    /// exposes more CPU work to `FileStream`.
    ///
    /// The current implementation keeps a conservative fallback to a single
    /// combined reader when the scan has a file-scoped dynamic pruner or a
    /// global `LIMIT`, as both of those semantics are currently tracked at the
    /// file level rather than the row-group level.
    fn build_stream_readers(self) -> Result<Vec<ReadingParquetState>> {
        let RowGroupsPreparedParquetOpen {
            prepared,
            mut row_groups,
        } = self;
        let FiltersPreparedParquetOpen {
            loaded,
            physical_file_schema,
            projection,
            predicate,
            pruning_predicate: _,
            page_pruning_predicate,
        } = prepared;
        let MetadataLoadedParquetOpen {
            prepared,
            reader_metadata,
            options: _,
        } = loaded;
        let PreparedParquetOpen {
            state,
            partitioned_file: _,
            file_range: _,
            extensions: _,
            file_metrics,
            file_pruner,
            metadata_size_hint: _,
            async_file_reader,
            logical_file_schema: _,
            output_schema,
            projection: _,
            predicate: _,
            #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: _,
            baseline_metrics,
        } = prepared;

        let batch_size = state.batch_size;
        let reorder_predicates = state.reorder_filters;
        let pushdown_filters = state.pushdown_filters;
        let force_filter_selections = state.force_filter_selections;
        let limit = state.limit;
        let max_predicate_cache_size = state.max_predicate_cache_size;
        let reverse_row_groups = state.reverse_row_groups;
        let preserve_order = state.preserve_order;
        let file_metadata = Arc::clone(reader_metadata.metadata());
        let rg_metadata = file_metadata.row_groups();

        // Prune by limit if limit is set and limit order is not sensitive.
        if let (Some(limit), false) = (limit, preserve_order) {
            row_groups.prune_by_limit(limit, rg_metadata, &file_metrics);
        }

        // --------------------------------------------------------
        // Step: prune pages from the kept row groups
        //
        // Page index pruning: if all data on individual pages can
        // be ruled out using page metadata, rows from other columns
        // with that range can be skipped as well.
        // --------------------------------------------------------
        let mut access_plan = row_groups.build();
        if !access_plan.is_empty()
            && let Some(ref p) = page_pruning_predicate
        {
            access_plan = p.prune_plan_with_page_index(
                access_plan,
                &physical_file_schema,
                reader_metadata.parquet_schema(),
                file_metadata.as_ref(),
                &file_metrics,
            );
        }

        // Prepare the access plan (extract row groups and row selection).
        let mut prepared_plan = access_plan.prepare(rg_metadata)?;

        // ----------------------------------------------------------
        // Step: potentially reverse the access plan for performance.
        // See `ParquetSource::try_pushdown_sort` for the rationale.
        // ----------------------------------------------------------
        if reverse_row_groups {
            prepared_plan = prepared_plan.reverse(file_metadata.as_ref())?;
        }

        if prepared_plan.row_group_indexes.is_empty() {
            return Ok(vec![]);
        }

        // Keep each file range as a single prepared plan. Splitting into one
        // planner per row group adds scheduler overhead and is not beneficial
        // for the common scan path today.
        //
        // Step: construct builder for the final RecordBatch stream
        let mut builder =
            ParquetPushDecoderBuilder::new_with_metadata(reader_metadata.clone())
                .with_batch_size(batch_size);

        // Step: optionally add row filter to the builder.
        //
        // Row filter is used for late materialization in parquet decoding, see
        // `row_filter` for details.
        if let Some(predicate) = pushdown_filters.then_some(predicate.as_ref()).flatten()
        {
            let row_filter = row_filter::build_row_filter(
                predicate,
                &physical_file_schema,
                file_metadata.as_ref(),
                reorder_predicates,
                &file_metrics,
            );

            match row_filter {
                Ok(Some(filter)) => {
                    builder = builder.with_row_filter(filter);
                }
                Ok(None) => {}
                Err(e) => {
                    debug!("Ignoring error building row filter for '{predicate:?}': {e}");
                }
            };
        };
        if force_filter_selections {
            builder = builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
        }

        if let Some(row_selection) = prepared_plan.row_selection {
            builder = builder.with_row_selection(row_selection);
        }
        builder = builder.with_row_groups(prepared_plan.row_group_indexes);

        if let Some(limit) = limit {
            builder = builder.with_limit(limit)
        }

        if let Some(max_predicate_cache_size) = max_predicate_cache_size {
            builder = builder.with_max_predicate_cache_size(max_predicate_cache_size);
        }

        // Metrics from the arrow reader itself.
        let arrow_reader_metrics = ArrowReaderMetrics::enabled();

        let indices = projection.column_indices();
        let mask =
            ProjectionMask::roots(reader_metadata.parquet_schema(), indices.clone());

        let decoder = builder
            .with_projection(mask)
            .with_metrics(arrow_reader_metrics.clone())
            .build()?;

        let predicate_cache_inner_records =
            file_metrics.predicate_cache_inner_records.clone();
        let predicate_cache_records = file_metrics.predicate_cache_records.clone();

        // Rebase column indices to match the narrowed stream schema.
        // The projection expressions have indices based on
        // `physical_file_schema`, but the stream only contains the columns
        // selected by the `ProjectionMask`.
        let stream_schema = Arc::new(physical_file_schema.project(&indices)?);
        let replace_schema = stream_schema != output_schema;
        let projection = projection
            .clone()
            .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
        let projector = projection.make_projector(&stream_schema)?;
        let push_decoder_state = PushDecoderStreamState {
            decoder,
            reader: async_file_reader,
            projector,
            output_schema: Arc::clone(&output_schema),
            replace_schema,
            arrow_reader_metrics,
            predicate_cache_inner_records,
            predicate_cache_records,
            baseline_metrics: baseline_metrics.clone(),
        };

        // Keep file-scoped early-stop behavior on the single-planner path.
        let reading_state = if let Some(file_pruner) = file_pruner {
            ReadingParquetState::with_early_stop(
                push_decoder_state,
                file_pruner,
                file_metrics.files_ranges_pruned_statistics.clone(),
            )
        } else {
            ReadingParquetState::new(push_decoder_state)
        };

        Ok(vec![reading_state])
    }
}

impl ParquetMorselizerState {
    /// Resolve file-specific decryption properties before metadata I/O.
    #[cfg(feature = "parquet_encryption")]
    async fn load_file_decryption_properties(
        self: &ParquetMorselizerState,
        file_location: object_store::path::Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        let encryption_context = self.get_encryption_context();
        encryption_context
            .get_file_decryption_properties(&file_location)
            .await
    }

    /// Resolve file-specific decryption properties before metadata I/O.
    #[cfg(not(feature = "parquet_encryption"))]
    #[expect(dead_code)]
    async fn load_file_decryption_properties(
        self: &ParquetMorselizerState,
        _file_location: object_store::path::Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        Ok(None)
    }
}

impl Morselizer for ParquetMorselizer {
    fn morselize(&self, file: PartitionedFile) -> Result<Vec<Box<dyn MorselPlanner>>> {
        Ok(vec![Box::new(ParquetMorselPlanner::new(
            Arc::clone(&self.state),
            file,
        ))])
    }
}

/// CPU-only states for [`ParquetMorselPlanner`].
///
/// These are the states when the MorselPlanner has more CPU work to do
enum ReadyState {
    /// Planner has not started any work yet.
    Start(Box<PartitionedFile>),
    /// Planner is ready to resolve any file-specific encryption properties.
    #[cfg(feature = "parquet_encryption")]
    PrepareEncryption(Box<PreparedParquetOpen>),
    /// Planner can do file-level pruning before requesting parquet metadata.
    PruneFiles(Box<PreparedParquetOpen>),
    /// Planner has loaded parquet metadata and can do CPU-only filter preparation.
    PrepareFilters(Box<MetadataLoadedParquetOpen>),
    /// Planner has prepared filters and can request any missing page index data.
    Prepared(Box<FiltersPreparedParquetOpen>),
    /// Planner has prepared row-group pruning and can optionally load bloom filters.
    BuildStream(Box<RowGroupsPreparedParquetOpen>),
    /// Planner has one or more per-row-group reading states ready to turn into
    /// the current planner plus any child planners.
    FanoutRowGroups(Vec<ReadingParquetState>),
    /// Planner has a prepared push decoder and is trying to produce the first
    /// record batch before yielding a morsel.
    ReadingParquet(Box<ReadingParquetState>),
    /// Planner has a fully prepared morsel ready to emit.
    EmitMorsel(BoxStream<'static, Result<RecordBatch>>),
}

impl ReadyState {
    fn start(file: PartitionedFile) -> Self {
        Self::Start(Box::new(file))
    }

    #[cfg(feature = "parquet_encryption")]
    fn prepare_encryption(prepared: PreparedParquetOpen) -> Self {
        Self::PrepareEncryption(Box::new(prepared))
    }

    fn prune_files(prepared: PreparedParquetOpen) -> Self {
        Self::PruneFiles(Box::new(prepared))
    }

    fn prepared(prepared: MetadataLoadedParquetOpen) -> Self {
        Self::PrepareFilters(prepared.into())
    }

    fn filters_prepared(prepared: FiltersPreparedParquetOpen) -> Self {
        Self::Prepared(prepared.into())
    }

    fn build_stream(prepared: RowGroupsPreparedParquetOpen) -> Self {
        Self::BuildStream(prepared.into())
    }

    fn fanout_row_groups(states: Vec<ReadingParquetState>) -> Self {
        Self::FanoutRowGroups(states)
    }

    fn reading_parquet(state: ReadingParquetState) -> Self {
        Self::ReadingParquet(Box::new(state))
    }

    fn emit_morsel(stream: BoxStream<'static, Result<RecordBatch>>) -> Self {
        Self::EmitMorsel(stream)
    }
}

/// Scheduler-visible state for [`ParquetMorselPlanner`].
///
/// This allows tracking outstanding IOs
enum ParquetMorselPlannerState {
    /// Planner can make progress using CPU only.
    Ready(Box<ReadyState>),
    /// Planner has outstanding async I/O and will become ready again when it completes.
    WaitingIo(WaitingIoState),
    /// Planner has emitted its morsel and has no further work.
    Done,
}

impl ParquetMorselPlannerState {
    fn ready(ready_state: ReadyState) -> Self {
        Self::Ready(Box::new(ready_state))
    }

    /// Return a planner state that emits an empty morsel stream.
    ///
    /// This is used when file-level pruning determines the file can be skipped
    /// before any parquet metadata or row-group work is needed, while still
    /// flowing through the normal morsel emission path in `FileStream`.
    fn empty_file() -> Self {
        Self::ready(ReadyState::emit_morsel(futures::stream::empty().boxed()))
    }
}

/// Result of an in-flight planner I/O phase.
struct WaitingIoState {
    /// Waiting for an async step to produce the next CPU-ready planner state.
    receiver: oneshot::Receiver<Result<ReadyState>>,
}

impl ParquetMorselPlannerState {
    fn name(&self) -> &'static str {
        match self {
            Self::Ready(ready_state) => match ready_state.as_ref() {
                ReadyState::Start(_) => "Ready(Start)",
                #[cfg(feature = "parquet_encryption")]
                ReadyState::PrepareEncryption(_) => "Ready(PrepareEncryption)",
                ReadyState::PruneFiles(_) => "Ready(PruneFiles)",
                ReadyState::PrepareFilters(_) => "Ready(PrepareFilters)",
                ReadyState::Prepared(_) => "Ready(Prepared)",
                ReadyState::BuildStream(_) => "Ready(BuildStream)",
                ReadyState::FanoutRowGroups(_) => "Ready(FanoutRowGroups)",
                ReadyState::ReadingParquet(_) => "Ready(ReadingParquet)",
                ReadyState::EmitMorsel(_) => "Ready(EmitMorsel)",
            },
            Self::WaitingIo(_) => "WaitingIo",
            Self::Done => "Done",
        }
    }
}

/// Planner wrapper that exposes the copied opener logic through the generic
/// morsel-planning API.
struct ParquetMorselPlanner {
    morselizer: Arc<ParquetMorselizerState>,
    state: ParquetMorselPlannerState,
}

impl Debug for ParquetMorselPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetMorselPlanner")
            .field("morselizer", &"...")
            .field("state", &self.state.name())
            .finish()
    }
}

impl ParquetMorselPlanner {
    fn new(morselizer: Arc<ParquetMorselizerState>, file: PartitionedFile) -> Self {
        Self {
            morselizer,
            state: ParquetMorselPlannerState::ready(ReadyState::start(file)),
        }
    }

    fn with_ready_state(
        morselizer: Arc<ParquetMorselizerState>,
        ready_state: ReadyState,
    ) -> Self {
        Self {
            morselizer,
            state: ParquetMorselPlannerState::ready(ready_state),
        }
    }

    #[cfg(feature = "parquet_encryption")]
    fn needs_file_decryption_properties(&self) -> bool {
        self.morselizer
            .encryption_context
            .needs_file_decryption_properties()
    }

    /// Schedule an async step and transition the planner into `WaitingIo`.
    ///
    /// Sets `self.state` to ParquetMorselPlannerState::WaitingIo
    fn schedule_io<F>(&mut self, future: F) -> Option<MorselPlan>
    where
        F: Future<Output = Result<ReadyState>> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let io_future = async move {
            let next_state = future.await?;
            // Ignore error as it means the receiver shutdown (likely due to a
            // real error) and we don't want to confuse error reporting by
            // reporting a closed channel.
            let _ = sender.send(Ok(next_state));
            Ok(())
        };
        self.state = ParquetMorselPlannerState::WaitingIo(WaitingIoState { receiver });
        Some(MorselPlan::new().with_io_future(io_future.boxed()))
    }

    /// Drive the initial push-decoder loop until it either needs more I/O or
    /// can yield a stream with at least its first batch ready.
    ///
    /// This relies on the push decoder not doing IO after it has begun to produce
    /// RecordBatches, which is only true when reading a single record batch
    fn prepare_reading_parquet(
        &mut self,
        reading: ReadingParquetState,
    ) -> Result<Option<MorselPlan>> {
        let ReadingParquetState {
            mut state,
            file_pruner,
            files_ranges_pruned_statistics,
        } = reading;

        match state.decoder.try_decode() {
            Ok(DecodeResult::NeedsData(ranges)) => Ok(self.schedule_io(async move {
                let data = state.reader.get_byte_ranges(ranges.clone()).await?;
                state.decoder.push_ranges(ranges, data)?;
                Ok(ReadyState::reading_parquet(ReadingParquetState {
                    state,
                    file_pruner,
                    files_ranges_pruned_statistics,
                }))
            })),
            Ok(DecodeResult::Data(batch)) => {
                state.copy_arrow_reader_metrics();
                let batch = state.project_batch(&batch)?;
                let stream = ReadingParquetState {
                    state,
                    file_pruner,
                    files_ranges_pruned_statistics,
                }
                .into_stream(Some(batch));
                Ok(Some(MorselPlan::new().with_morsels(vec![Box::new(
                    ParquetStreamMorsel::new(stream),
                )])))
            }
            Ok(DecodeResult::Finished) => {
                let stream = ReadingParquetState {
                    state,
                    file_pruner,
                    files_ranges_pruned_statistics,
                }
                .into_stream(None);
                Ok(Some(MorselPlan::new().with_morsels(vec![Box::new(
                    ParquetStreamMorsel::new(stream),
                )])))
            }
            Err(e) => Err(DataFusionError::from(e)),
        }
    }
}

impl MorselPlanner for ParquetMorselPlanner {
    fn plan(&mut self) -> Result<Option<MorselPlan>> {
        // Core state machine transition
        let state = mem::replace(&mut self.state, ParquetMorselPlannerState::Done);
        match state {
            ParquetMorselPlannerState::Ready(ready_state) => match *ready_state {
                ReadyState::Start(file) => {
                    let prepared = self.morselizer.prepare_open_file(*file)?;
                    #[cfg(feature = "parquet_encryption")]
                    {
                        if self.needs_file_decryption_properties() {
                            self.state = ParquetMorselPlannerState::ready(
                                ReadyState::prepare_encryption(prepared),
                            );
                        } else {
                            self.state = ParquetMorselPlannerState::ready(
                                ReadyState::prune_files(prepared),
                            );
                        }
                    }
                    #[cfg(not(feature = "parquet_encryption"))]
                    {
                        self.state = ParquetMorselPlannerState::ready(
                            ReadyState::prune_files(prepared),
                        );
                    }
                    Ok(Some(MorselPlan::new()))
                }
                #[cfg(feature = "parquet_encryption")]
                ReadyState::PrepareEncryption(mut prepared) => {
                    let file_location =
                        prepared.partitioned_file.object_meta.location.clone();
                    let state = Arc::clone(&prepared.state);
                    Ok(self.schedule_io(async move {
                        let properties =
                            state.load_file_decryption_properties(file_location).await?;
                        prepared.file_decryption_properties = properties;
                        Ok(ReadyState::prune_files(*prepared))
                    }))
                }
                ReadyState::PruneFiles(prepared) => {
                    let Some(prepared) = prepared.prune_file()? else {
                        // File was totally pruned
                        self.state = ParquetMorselPlannerState::empty_file();
                        return Ok(Some(MorselPlan::new()));
                    };
                    Ok(self.schedule_io(async move {
                        let loaded = prepared.load().await?;
                        Ok(ReadyState::prepared(loaded))
                    }))
                }
                ReadyState::PrepareFilters(prepared) => {
                    let prepared = prepared.prepare_filters()?;
                    self.state = ParquetMorselPlannerState::ready(
                        ReadyState::filters_prepared(prepared),
                    );
                    Ok(Some(MorselPlan::new()))
                }
                ReadyState::Prepared(prepared) => Ok(self.schedule_io(async move {
                    let prepared = prepared.load_page_index().await?;
                    let prepared = prepared.prepare_row_groups()?;
                    Ok(ReadyState::build_stream(prepared))
                })),
                ReadyState::BuildStream(prepared) => {
                    let should_prune_bloom = prepared
                        .prepared
                        .pruning_predicate
                        .is_some()
                        && prepared.prepared.loaded.prepared.state.enable_bloom_filter
                        && !prepared.row_groups.is_empty();
                    if should_prune_bloom {
                        Ok(self.schedule_io(async move {
                            let prepared = prepared.prune_bloom_filters().await?;
                            let reading_states = prepared.build_stream_readers()?;
                            Ok(ReadyState::fanout_row_groups(reading_states))
                        }))
                    } else {
                        let reading_states = prepared.build_stream_readers()?;
                        self.state = ParquetMorselPlannerState::ready(
                            ReadyState::fanout_row_groups(reading_states),
                        );
                        Ok(Some(MorselPlan::new()))
                    }
                }
                ReadyState::FanoutRowGroups(reading_states) => {
                    let mut reading_states: VecDeque<_> = reading_states.into();
                    let Some(first_state) = reading_states.pop_front() else {
                        self.state = ParquetMorselPlannerState::empty_file();
                        return Ok(Some(MorselPlan::new()));
                    };

                    let child_planners = reading_states
                        .into_iter()
                        .map(|reading_state| {
                            Box::new(ParquetMorselPlanner::with_ready_state(
                                Arc::clone(&self.morselizer),
                                ReadyState::reading_parquet(reading_state),
                            )) as Box<dyn MorselPlanner>
                        })
                        .collect();

                    self.state = ParquetMorselPlannerState::ready(
                        ReadyState::reading_parquet(first_state),
                    );
                    Ok(Some(MorselPlan::new().with_planners(child_planners)))
                }
                ReadyState::ReadingParquet(reading) => {
                    self.prepare_reading_parquet(*reading)
                }
                ReadyState::EmitMorsel(stream) => Ok(Some(
                    MorselPlan::new()
                        .with_morsels(vec![Box::new(ParquetStreamMorsel::new(stream))]),
                )),
            },
            ParquetMorselPlannerState::WaitingIo(WaitingIoState { mut receiver }) => {
                match receiver.try_recv() {
                    Ok(next_state) => {
                        self.state = ParquetMorselPlannerState::ready(next_state?);
                        Ok(Some(MorselPlan::new()))
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {
                        self.state =
                            ParquetMorselPlannerState::WaitingIo(WaitingIoState {
                                receiver,
                            });
                        Ok(None)
                    }
                    Err(oneshot::error::TryRecvError::Closed) => {
                        Err(DataFusionError::Execution(
                            "Parquet morsel planner I/O completion channel closed"
                                .to_string(),
                        ))
                    }
                }
            }
            ParquetMorselPlannerState::Done => Ok(None),
        }
    }
}

struct ReadingParquetState {
    state: PushDecoderStreamState,
    file_pruner: Option<FilePruner>,
    files_ranges_pruned_statistics: Option<PruningMetrics>,
}

impl ReadingParquetState {
    fn new(state: PushDecoderStreamState) -> Self {
        Self {
            state,
            file_pruner: None,
            files_ranges_pruned_statistics: None,
        }
    }

    fn with_early_stop(
        state: PushDecoderStreamState,
        file_pruner: FilePruner,
        files_ranges_pruned_statistics: PruningMetrics,
    ) -> Self {
        Self {
            state,
            file_pruner: Some(file_pruner),
            files_ranges_pruned_statistics: Some(files_ranges_pruned_statistics),
        }
    }

    fn into_stream(
        self,
        first_batch: Option<RecordBatch>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let stream = stream_from_push_decoder_state(self.state, first_batch).boxed();
        wrap_stream_with_early_stop(
            stream,
            self.file_pruner,
            self.files_ranges_pruned_statistics,
        )
    }
}

struct ParquetStreamMorsel {
    stream: BoxStream<'static, Result<RecordBatch>>,
}

impl Debug for ParquetStreamMorsel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetStreamMorsel")
            .field("stream", &"...")
            .finish()
    }
}

impl ParquetStreamMorsel {
    fn new(stream: BoxStream<'static, Result<RecordBatch>>) -> Self {
        Self { stream }
    }
}

impl Morsel for ParquetStreamMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        self.stream
    }

    fn split(&mut self) -> Result<Vec<Box<dyn Morsel>>> {
        Ok(vec![])
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
    async fn transition(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            match self.decoder.try_decode() {
                Ok(DecodeResult::NeedsData(ranges)) => {
                    let fetch = async {
                        let data = self.reader.get_byte_ranges(ranges.clone()).await?;
                        self.decoder.push_ranges(ranges, data)?;
                        Ok::<_, ParquetError>(())
                    };
                    if let Err(e) = fetch.await {
                        return Some(Err(DataFusionError::from(e)));
                    }
                }
                Ok(DecodeResult::Data(batch)) => {
                    let mut timer = self.baseline_metrics.elapsed_compute().timer();
                    self.copy_arrow_reader_metrics();
                    let result = self.project_batch(&batch);
                    timer.stop();
                    return Some(result);
                }
                Ok(DecodeResult::Finished) => {
                    return None;
                }
                Err(e) => {
                    return Some(Err(DataFusionError::from(e)));
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
            let (_schema, arrays, num_rows) = batch.into_parts();
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

fn stream_from_push_decoder_state(
    state: PushDecoderStreamState,
    first_batch: Option<RecordBatch>,
) -> impl Stream<Item = Result<RecordBatch>> + Send + 'static {
    let first = first_batch
        .map(|batch| futures::stream::once(async move { Ok(batch) }).left_stream())
        .unwrap_or_else(|| futures::stream::empty().right_stream());

    first.chain(futures::stream::unfold(state, |mut state| async move {
        let result = state.transition().await;
        result.map(|r| (r, state))
    }))
}

fn wrap_stream_with_early_stop(
    stream: BoxStream<'static, Result<RecordBatch>>,
    file_pruner: Option<FilePruner>,
    files_ranges_pruned_statistics: Option<PruningMetrics>,
) -> BoxStream<'static, Result<RecordBatch>> {
    match (file_pruner, files_ranges_pruned_statistics) {
        (Some(file_pruner), Some(files_ranges_pruned_statistics)) => {
            EarlyStoppingStream::new(stream, file_pruner, files_ranges_pruned_statistics)
                .boxed()
        }
        _ => stream,
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

#[derive(Default, Clone)]
pub(crate) struct EncryptionContext {
    #[cfg(feature = "parquet_encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    #[cfg(feature = "parquet_encryption")]
    encryption_factory: Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
}

#[cfg(feature = "parquet_encryption")]
impl EncryptionContext {
    fn needs_file_decryption_properties(&self) -> bool {
        self.file_decryption_properties.is_some() || self.encryption_factory.is_some()
    }

    pub(crate) fn new(
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
    fn needs_file_decryption_properties(&self) -> bool {
        false
    }

    async fn get_file_decryption_properties(
        &self,
        _file_location: &object_store::path::Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        Ok(None)
    }
}

impl ParquetMorselizerState {
    #[cfg(feature = "parquet_encryption")]
    fn get_encryption_context(&self) -> EncryptionContext {
        self.encryption_context.clone()
    }

    #[cfg(not(feature = "parquet_encryption"))]
    #[expect(dead_code)]
    fn get_encryption_context(&self) -> EncryptionContext {
        self.encryption_context.clone()
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
) -> (
    Option<Arc<PruningPredicate>>,
    Option<Arc<PagePruningAccessPlanFilter>>,
) {
    let Some(predicate) = predicate.as_ref() else {
        return (None, None);
    };
    let pruning_predicate = build_pruning_predicate(
        Arc::clone(predicate),
        file_schema,
        predicate_creation_errors,
    );
    let page_pruning_predicate = build_page_pruning_predicate(predicate, file_schema);
    (pruning_predicate, Some(page_pruning_predicate))
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

fn should_enable_page_index(
    enable_page_index: bool,
    page_pruning_predicate: &Option<Arc<PagePruningAccessPlanFilter>>,
) -> bool {
    enable_page_index
        && page_pruning_predicate.is_some()
        && page_pruning_predicate
            .as_ref()
            .map(|p| p.filter_number() > 0)
            .unwrap_or(false)
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use super::{
        ConstantColumns, EncryptionContext, ParquetMorselizerState,
        constant_columns_from_stats,
    };
    use crate::{DefaultParquetFileReaderFactory, ParquetMorselizer, RowGroupAccess};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use bytes::{BufMut, BytesMut};
    use datafusion_common::{
        ColumnStatistics, DataFusionError, ScalarValue, Statistics, record_batch,
        stats::Precision,
    };
    use datafusion_datasource::{
        PartitionedFile, TableSchema,
        morsel::{MorselPlanner, Morselizer},
    };
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
    use futures::{Stream, StreamExt};
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

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
        coerce_int96: Option<arrow::datatypes::TimeUnit>,
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

            ParquetMorselizer::new(ParquetMorselizerState {
                partition_index: self.partition_index,
                projection,
                batch_size: self.batch_size,
                limit: self.limit,
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
                expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
                encryption_context: EncryptionContext::default(),
                max_predicate_cache_size: self.max_predicate_cache_size,
                reverse_row_groups: self.reverse_row_groups,
                preserve_order: self.preserve_order,
            })
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
        assert!(exprs[0].expr.as_any().downcast_ref::<Literal>().is_some());
        assert!(exprs[1].expr.as_any().downcast_ref::<Column>().is_some());

        // Only column `b` should remain in the projection mask
        assert_eq!(rewritten.column_indices(), vec![1]);
    }

    #[test]
    fn rewrite_physical_expr_literal() {
        let mut constants = ConstantColumns::new();
        constants.insert("a".to_string(), ScalarValue::from(7i32));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));

        let rewritten = replace_columns_with_literals(expr, &constants).unwrap();
        assert!(rewritten.as_any().downcast_ref::<Literal>().is_some());
    }

    async fn count_batches_and_rows(
        mut stream: std::pin::Pin<
            Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>,
        >,
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
        mut stream: std::pin::Pin<
            Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>,
        >,
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

    async fn open_via_morselizer(
        morselizer: &ParquetMorselizer,
        file: PartitionedFile,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>
    {
        let mut planners: VecDeque<Box<dyn MorselPlanner>> =
            morselizer.morselize(file).unwrap().into();
        let mut streams = Vec::new();

        while let Some(mut planner) = planners.pop_front() {
            while let Some(mut plan) = planner.plan().unwrap() {
                if let Some(io_future) = plan.take_io_future() {
                    io_future.await.unwrap();
                    continue;
                }

                streams.extend(
                    plan.take_morsels()
                        .into_iter()
                        .map(|morsel| morsel.into_stream()),
                );
                planners.extend(plan.take_planners());
            }
        }

        futures::stream::iter(streams).flatten().boxed()
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
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // A filter on `b = 5.0` should exclude all rows
        let expr = col("b").eq(lit(ScalarValue::Float32(Some(5.0))));
        let predicate = logical2physical(&expr, &schema);
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file).await;
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
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value
        let expr = col("part").eq(lit(2));
        // Mark the expression as dynamic even if it's not to force partition pruning to happen
        // Otherwise we assume it already happened at the planning stage and won't re-do the work here
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file).await;
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
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Should prune based on partition value but not file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(1.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on file statistics but not partition value
        let expr = col("part").eq(lit(1)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on both partition value and file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file).await;
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
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should match the partition value but not the data value
        let expr = col("part").eq(lit(1)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value but match the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(1)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 1);

        // Filter should not match the partition value or the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file).await;
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
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // If we make the filter dynamic, it should prune.
        // This allows dynamic filters to prune partitions/files even if they are populated late into execution.
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // If we have a filter that touches partition columns only and is dynamic, it should prune even if there are no stats.
        file.statistics = Some(Arc::new(Statistics::new_unknown(&file_schema)));
        let expr = col("part").eq(lit(2));
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Similarly a filter that combines partition and data columns should prune even if there are no stats.
        let expr = col("part").eq(lit(2)).and(col("a").eq(lit(42)));
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = open_via_morselizer(&opener, file.clone()).await;
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
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let forward_values = collect_int32_values(stream).await;

        // Test reverse scan
        let opener = make_opener(true);
        let stream = open_via_morselizer(&opener, file.clone()).await;
        let reverse_values = collect_int32_values(stream).await;

        // The forward scan should return data in the order written
        assert_eq!(forward_values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // With reverse scan, row groups are reversed, so we expect:
        // Row group 3 (7,8,9), then row group 2 (4,5,6), then row group 1 (1,2,3)
        assert_eq!(reverse_values, vec![7, 8, 9, 4, 5, 6, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_morselizer_basic_parity() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch1 =
            record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let batch2 = record_batch!(("a", Int32, vec![Some(4), Some(5)])).unwrap();

        let data_len = write_parquet_batches(
            Arc::clone(&store),
            "morselizer_basic_parity.parquet",
            vec![batch1, batch2],
            None,
        )
        .await;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let opener = ParquetMorselizerBuilder::new()
            .with_store(Arc::clone(&store))
            .with_schema(schema)
            .with_projection_indices(&[0])
            .build();

        let file = PartitionedFile::new(
            "morselizer_basic_parity.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        );

        let opener_values =
            collect_int32_values(open_via_morselizer(&opener, file.clone()).await).await;
        let morsel_values =
            collect_int32_values(open_via_morselizer(&opener, file).await).await;

        assert_eq!(opener_values, morsel_values);
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
        let stream_forward = open_via_morselizer(&opener_forward, file.clone()).await;
        let (batches_forward, _) = count_batches_and_rows(stream_forward).await;

        let opener_reverse = make_opener(true);
        let stream_reverse = open_via_morselizer(&opener_reverse, file).await;
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
        let stream = open_via_morselizer(&opener, file.clone()).await;
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
        let stream = open_via_morselizer(&opener, file).await;
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
        let stream = open_via_morselizer(&opener, file.clone()).await;
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
        let stream = open_via_morselizer(&opener, file).await;
        let reverse_values = collect_int32_values(stream).await;

        assert_eq!(
            reverse_values,
            vec![7, 5, 1],
            "Reverse scan with non-contiguous row groups should correctly map RowSelection"
        );
    }
}
