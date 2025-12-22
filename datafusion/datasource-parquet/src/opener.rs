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

//! [`ParquetOpener`] for opening Parquet files

use crate::page_filter::PagePruningAccessPlanFilter;
use crate::row_group_filter::RowGroupAccessPlanFilter;
use crate::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
    apply_file_schema_type_coercions, coerce_int96_to_resolution, row_filter,
};
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::DataType;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use std::collections::HashMap;
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
    Count, ExecutionPlanMetricsSet, MetricBuilder, PruningMetrics,
};
use datafusion_pruning::{FilePruner, PruningPredicate, build_pruning_predicate};

use crate::sort::reverse_row_selection;
#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;
use futures::{Stream, StreamExt, TryStreamExt, ready};
use log::debug;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader, RowGroupMetaData};

/// Implements [`FileOpener`] for a parquet file
pub(super) struct ParquetOpener {
    /// Execution partition index
    pub partition_index: usize,
    /// Projection to apply on top of the table schema (i.e. can reference partition columns).
    pub projection: ProjectionExprs,
    /// Target number of rows in each output RecordBatch
    pub batch_size: usize,
    /// Optional limit on the number of rows to read
    pub limit: Option<usize>,
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

/// Represents a prepared access plan with optional row selection
struct PreparedAccessPlan {
    /// Row group indexes to read
    row_group_indexes: Vec<usize>,
    /// Optional row selection for filtering within row groups
    row_selection: Option<parquet::arrow::arrow_reader::RowSelection>,
}

impl PreparedAccessPlan {
    /// Create a new prepared access plan from a ParquetAccessPlan
    fn from_access_plan(
        access_plan: ParquetAccessPlan,
        rg_metadata: &[RowGroupMetaData],
    ) -> Result<Self> {
        let row_group_indexes = access_plan.row_group_indexes();
        let row_selection = access_plan.into_overall_row_selection(rg_metadata)?;

        Ok(Self {
            row_group_indexes,
            row_selection,
        })
    }

    /// Reverse the access plan for reverse scanning
    fn reverse(
        mut self,
        file_metadata: &parquet::file::metadata::ParquetMetaData,
    ) -> Result<Self> {
        // Reverse the row group indexes
        self.row_group_indexes = self.row_group_indexes.into_iter().rev().collect();

        // If we have a row selection, reverse it to match the new row group order
        if let Some(row_selection) = self.row_selection {
            self.row_selection =
                Some(reverse_row_selection(&row_selection, file_metadata)?);
        }

        Ok(self)
    }

    /// Apply this access plan to a ParquetRecordBatchStreamBuilder
    fn apply_to_builder(
        self,
        mut builder: ParquetRecordBatchStreamBuilder<Box<dyn AsyncFileReader>>,
    ) -> ParquetRecordBatchStreamBuilder<Box<dyn AsyncFileReader>> {
        if let Some(row_selection) = self.row_selection {
            builder = builder.with_row_selection(row_selection);
        }
        builder.with_row_groups(self.row_group_indexes)
    }
}

impl FileOpener for ParquetOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let file_range = partitioned_file.range.clone();
        let extensions = partitioned_file.extensions.clone();
        let file_location = partitioned_file.object_meta.location.clone();
        let file_name = file_location.to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);

        let metadata_size_hint = partitioned_file
            .metadata_size_hint
            .or(self.metadata_size_hint);

        let mut async_file_reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                partitioned_file.clone(),
                metadata_size_hint,
                &self.metrics,
            )?;

        let batch_size = self.batch_size;

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

        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let force_filter_selections = self.force_filter_selections;
        let coerce_int96 = self.coerce_int96;
        let enable_bloom_filter = self.enable_bloom_filter;
        let enable_row_group_stats_pruning = self.enable_row_group_stats_pruning;
        let limit = self.limit;

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .global_counter("num_predicate_creation_errors");

        let expr_adapter_factory = Arc::clone(&self.expr_adapter_factory);

        let enable_page_index = self.enable_page_index;
        #[cfg(feature = "parquet_encryption")]
        let encryption_context = self.get_encryption_context();
        let max_predicate_cache_size = self.max_predicate_cache_size;

        let reverse_row_groups = self.reverse_row_groups;
        Ok(Box::pin(async move {
            #[cfg(feature = "parquet_encryption")]
            let file_decryption_properties = encryption_context
                .get_file_decryption_properties(&file_location)
                .await?;

            // Prune this file using the file level statistics and partition values.
            // Since dynamic filters may have been updated since planning it is possible that we are able
            // to prune files now that we couldn't prune at planning time.
            // It is assumed that there is no point in doing pruning here if the predicate is not dynamic,
            // as it would have been done at planning time.
            // We'll also check this after every record batch we read,
            // and if at some point we are able to prove we can prune the file using just the file level statistics
            // we can end the stream early.
            let mut file_pruner = predicate
                .as_ref()
                .filter(|p| {
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
                    is_dynamic_physical_expr(p) || partitioned_file.has_statistics()
                })
                .and_then(|p| {
                    FilePruner::try_new(
                        Arc::clone(p),
                        &logical_file_schema,
                        &partitioned_file,
                        predicate_creation_errors.clone(),
                    )
                });

            if let Some(file_pruner) = &mut file_pruner
                && file_pruner.should_prune()?
            {
                // Return an empty stream immediately to skip the work of setting up the actual stream
                file_metrics.files_ranges_pruned_statistics.add_pruned(1);
                return Ok(futures::stream::empty().boxed());
            }

            file_metrics.files_ranges_pruned_statistics.add_matched(1);

            // Don't load the page index yet. Since it is not stored inline in
            // the footer, loading the page index if it is not needed will do
            // unnecessary I/O. We decide later if it is needed to evaluate the
            // pruning predicates. Thus default to not requesting if from the
            // underlying reader.
            let mut options = ArrowReaderOptions::new().with_page_index(false);
            #[cfg(feature = "parquet_encryption")]
            if let Some(fd_val) = file_decryption_properties {
                options = options.with_file_decryption_properties(Arc::clone(&fd_val));
            }
            let mut metadata_timer = file_metrics.metadata_load_time.timer();

            // Begin by loading the metadata from the underlying reader (note
            // the returned metadata may actually include page indexes as some
            // readers may return page indexes even when not requested -- for
            // example when they are cached)
            let mut reader_metadata =
                ArrowReaderMetadata::load_async(&mut async_file_reader, options.clone())
                    .await?;

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
                &logical_file_schema,
                &physical_file_schema,
            ) {
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
            );
            let simplifier = PhysicalExprSimplifier::new(&physical_file_schema);
            predicate = predicate
                .map(|p| simplifier.simplify(rewriter.rewrite(p)?))
                .transpose()?;
            // Adapt projections to the physical file schema as well
            projection = projection
                .try_map_exprs(|p| simplifier.simplify(rewriter.rewrite(p)?))?;

            // Build predicates for this specific file
            let (pruning_predicate, page_pruning_predicate) = build_pruning_predicates(
                predicate.as_ref(),
                &physical_file_schema,
                &predicate_creation_errors,
            );

            // The page index is not stored inline in the parquet footer so the
            // code above may not have read the page index structures yet. If we
            // need them for reading and they aren't yet loaded, we need to load them now.
            if should_enable_page_index(enable_page_index, &page_pruning_predicate) {
                reader_metadata = load_page_index(
                    reader_metadata,
                    &mut async_file_reader,
                    // Since we're manually loading the page index the option here should not matter but we pass it in for consistency
                    options.with_page_index(true),
                )
                .await?;
            }

            metadata_timer.stop();

            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                async_file_reader,
                reader_metadata,
            );

            let indices = projection.column_indices();

            let mask = ProjectionMask::roots(builder.parquet_schema(), indices);

            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    &predicate,
                    &physical_file_schema,
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
                            "Ignoring error building row filter for '{predicate:?}': {e}"
                        );
                    }
                };
            };
            if force_filter_selections {
                builder =
                    builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
            }

            // Determine which row groups to actually read. The idea is to skip
            // as many row groups as possible based on the metadata and query
            let file_metadata = Arc::clone(builder.metadata());
            let predicate = pruning_predicate.as_ref().map(|p| p.as_ref());
            let rg_metadata = file_metadata.row_groups();
            // track which row groups to actually read
            let access_plan =
                create_initial_plan(&file_name, extensions, rg_metadata.len())?;
            let mut row_groups = RowGroupAccessPlanFilter::new(access_plan);
            // if there is a range restricting what parts of the file to read
            if let Some(range) = file_range.as_ref() {
                row_groups.prune_by_range(rg_metadata, range);
            }

            // If there is a predicate that can be evaluated against the metadata
            if let Some(predicate) = predicate.as_ref() {
                if enable_row_group_stats_pruning {
                    row_groups.prune_by_statistics(
                        &physical_file_schema,
                        builder.parquet_schema(),
                        rg_metadata,
                        predicate,
                        &file_metrics,
                    );
                } else {
                    // Update metrics: statistics unavailable, so all row groups are
                    // matched (not pruned)
                    file_metrics
                        .row_groups_pruned_statistics
                        .add_matched(row_groups.remaining_row_group_count());
                }

                if enable_bloom_filter && !row_groups.is_empty() {
                    row_groups
                        .prune_by_bloom_filters(
                            &physical_file_schema,
                            &mut builder,
                            predicate,
                            &file_metrics,
                        )
                        .await;
                } else {
                    // Update metrics: bloom filter unavailable, so all row groups are
                    // matched (not pruned)
                    file_metrics
                        .row_groups_pruned_bloom_filter
                        .add_matched(row_groups.remaining_row_group_count());
                }
            } else {
                // Update metrics: no predicate, so all row groups are matched (not pruned)
                let n_remaining_row_groups = row_groups.remaining_row_group_count();
                file_metrics
                    .row_groups_pruned_statistics
                    .add_matched(n_remaining_row_groups);
                file_metrics
                    .row_groups_pruned_bloom_filter
                    .add_matched(n_remaining_row_groups);
            }

            let mut access_plan = row_groups.build();

            // page index pruning: if all data on individual pages can
            // be ruled using page metadata, rows from other columns
            // with that range can be skipped as well
            if enable_page_index
                && !access_plan.is_empty()
                && let Some(p) = page_pruning_predicate
            {
                access_plan = p.prune_plan_with_page_index(
                    access_plan,
                    &physical_file_schema,
                    builder.parquet_schema(),
                    file_metadata.as_ref(),
                    &file_metrics,
                );
            }

            // Prepare the access plan (extract row groups and row selection)
            let mut prepared_plan =
                PreparedAccessPlan::from_access_plan(access_plan, rg_metadata)?;

            // If reverse scanning is enabled, reverse the prepared plan
            if reverse_row_groups {
                prepared_plan = prepared_plan.reverse(file_metadata.as_ref())?;
            }

            // Apply the prepared plan to the builder
            builder = prepared_plan.apply_to_builder(builder);

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            if let Some(max_predicate_cache_size) = max_predicate_cache_size {
                builder = builder.with_max_predicate_cache_size(max_predicate_cache_size);
            }

            // metrics from the arrow reader itself
            let arrow_reader_metrics = ArrowReaderMetrics::enabled();

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_metrics(arrow_reader_metrics.clone())
                .build()?;

            let files_ranges_pruned_statistics =
                file_metrics.files_ranges_pruned_statistics.clone();
            let predicate_cache_inner_records =
                file_metrics.predicate_cache_inner_records.clone();
            let predicate_cache_records = file_metrics.predicate_cache_records.clone();

            let stream_schema = Arc::clone(stream.schema());
            // Check if we need to replace the schema to handle things like differing nullability or metadata.
            // See note below about file vs. output schema.
            let replace_schema = !stream_schema.eq(&output_schema);

            // Rebase column indices to match the narrowed stream schema.
            // The projection expressions have indices based on physical_file_schema,
            // but the stream only contains the columns selected by the ProjectionMask.
            let projection = projection
                .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;

            let projector = projection.make_projector(&stream_schema)?;

            let stream = stream.map_err(DataFusionError::from).map(move |b| {
                b.and_then(|mut b| {
                    copy_arrow_reader_metrics(
                        &arrow_reader_metrics,
                        &predicate_cache_inner_records,
                        &predicate_cache_records,
                    );
                    b = projector.project_batch(&b)?;
                    if replace_schema {
                        // Ensure the output batch has the expected schema.
                        // This handles things like schema level and field level metadata, which may not be present
                        // in the physical file schema.
                        // It is also possible for nullability to differ; some writers create files with
                        // OPTIONAL fields even when there are no nulls in the data.
                        // In these cases it may make sense for the logical schema to be `NOT NULL`.
                        // RecordBatch::try_new_with_options checks that if the schema is NOT NULL
                        // the array cannot contain nulls, amongst other checks.
                        let (_stream_schema, arrays, num_rows) = b.into_parts();
                        let options =
                            RecordBatchOptions::new().with_row_count(Some(num_rows));
                        RecordBatch::try_new_with_options(
                            Arc::clone(&output_schema),
                            arrays,
                            &options,
                        )
                        .map_err(Into::into)
                    } else {
                        Ok(b)
                    }
                })
            });

            if let Some(file_pruner) = file_pruner {
                Ok(EarlyStoppingStream::new(
                    stream,
                    file_pruner,
                    files_ranges_pruned_statistics,
                )
                .boxed())
            } else {
                Ok(stream.boxed())
            }
        }))
    }
}

/// Copies metrics from ArrowReaderMetrics (the metrics collected by the
/// arrow-rs parquet reader) to the parquet file metrics for DataFusion
fn copy_arrow_reader_metrics(
    arrow_reader_metrics: &ArrowReaderMetrics,
    predicate_cache_inner_records: &Count,
    predicate_cache_records: &Count,
) {
    if let Some(v) = arrow_reader_metrics.records_read_from_inner() {
        predicate_cache_inner_records.add(v);
    }

    if let Some(v) = arrow_reader_metrics.records_read_from_cache() {
        predicate_cache_records.add(v);
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

impl ParquetOpener {
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
    use std::sync::Arc;

    use super::{ConstantColumns, constant_columns_from_stats};
    use crate::{DefaultParquetFileReaderFactory, opener::ParquetOpener};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use bytes::{BufMut, BytesMut};
    use datafusion_common::{
        ColumnStatistics, DataFusionError, ScalarValue, Statistics, record_batch,
        stats::Precision,
    };
    use datafusion_datasource::{PartitionedFile, TableSchema, file_stream::FileOpener};
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
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// Builder for creating [`ParquetOpener`] instances with sensible defaults for tests.
    /// This helps reduce code duplication and makes it clear what differs between test cases.
    struct ParquetOpenerBuilder {
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
    }

    impl ParquetOpenerBuilder {
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

        /// Build the ParquetOpener instance.
        ///
        /// # Panics
        ///
        /// Panics if required fields (store, schema/table_schema) are not set.
        fn build(self) -> ParquetOpener {
            let store = self
                .store
                .expect("ParquetOpenerBuilder: store must be set via with_store()");
            let table_schema = self.table_schema.expect(
                "ParquetOpenerBuilder: table_schema must be set via with_schema() or with_table_schema()",
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

            ParquetOpener {
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
            Box<
                dyn Stream<Item = Result<arrow::array::RecordBatch, DataFusionError>>
                    + Send,
            >,
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
            Box<
                dyn Stream<Item = Result<arrow::array::RecordBatch, DataFusionError>>
                    + Send,
            >,
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
        let children = expr.children().into_iter().map(Arc::clone).collect();
        Arc::new(DynamicFilterPhysicalExpr::new(expr, children))
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
            ParquetOpenerBuilder::new()
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
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // A filter on `b = 5.0` should exclude all rows
        let expr = col("b").eq(lit(ScalarValue::Float32(Some(5.0))));
        let predicate = logical2physical(&expr, &schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file).unwrap().await.unwrap();
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
            ParquetOpenerBuilder::new()
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
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value
        let expr = col("part").eq(lit(2));
        // Mark the expression as dynamic even if it's not to force partition pruning to happen
        // Otherwise we assume it already happened at the planning stage and won't re-do the work here
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(file).unwrap().await.unwrap();
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
            ParquetOpenerBuilder::new()
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
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Should prune based on partition value but not file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(1.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on file statistics but not partition value
        let expr = col("part").eq(lit(1)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on both partition value and file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file).unwrap().await.unwrap();
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
            ParquetOpenerBuilder::new()
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
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should match the partition value but not the data value
        let expr = col("part").eq(lit(1)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value but match the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(1)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 1);

        // Filter should not match the partition value or the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file).unwrap().await.unwrap();
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
            ParquetOpenerBuilder::new()
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
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // If we make the filter dynamic, it should prune.
        // This allows dynamic filters to prune partitions/files even if they are populated late into execution.
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // If we have a filter that touches partition columns only and is dynamic, it should prune even if there are no stats.
        file.statistics = Some(Arc::new(Statistics::new_unknown(&file_schema)));
        let expr = col("part").eq(lit(2));
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Similarly a filter that combines partition and data columns should prune even if there are no stats.
        let expr = col("part").eq(lit(2)).and(col("a").eq(lit(42)));
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
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
            .set_max_row_group_size(3) // Force each batch into its own row group
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
            ParquetOpenerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_projection_indices(&[0])
                .with_reverse_row_groups(reverse_scan)
                .build()
        };

        // Test normal scan (forward)
        let opener = make_opener(false);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let forward_values = collect_int32_values(stream).await;

        // Test reverse scan
        let opener = make_opener(true);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
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
            ParquetOpenerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_projection_indices(&[0])
                .with_reverse_row_groups(reverse_scan)
                .build()
        };

        // With a single row group, forward and reverse should be the same
        // (only the row group order is reversed, not the rows within)
        let opener_forward = make_opener(false);
        let stream_forward = opener_forward.open(file.clone()).unwrap().await.unwrap();
        let (batches_forward, _) = count_batches_and_rows(stream_forward).await;

        let opener_reverse = make_opener(true);
        let stream_reverse = opener_reverse.open(file).unwrap().await.unwrap();
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
            .set_max_row_group_size(4)
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
            ParquetOpenerBuilder::new()
                .with_store(Arc::clone(&store))
                .with_schema(Arc::clone(&schema))
                .with_projection_indices(&[0])
                .with_reverse_row_groups(reverse_scan)
                .build()
        };

        // Forward scan: RG0(3,4), RG1(5,6,7,8), RG2(9,10)
        let opener = make_opener(false);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
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
        let stream = opener.open(file).unwrap().await.unwrap();
        let reverse_values = collect_int32_values(stream).await;

        // Correct expected result: row groups reversed but each keeps its own selection
        // RG2 with its selection (9,10), RG1 with its selection (5,6,7,8), RG0 with its selection (3,4)
        assert_eq!(
            reverse_values,
            vec![9, 10, 5, 6, 7, 8, 3, 4],
            "Reverse scan should reverse row group order while maintaining correct RowSelection for each group"
        );
    }
}
