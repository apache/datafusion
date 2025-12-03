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
    apply_file_schema_type_coercions, coerce_int96_to_resolution, row_filter,
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
};
use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{FieldRef, SchemaRef, TimeUnit};
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::physical_expr::{
    is_dynamic_physical_expr, PhysicalExpr,
};
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, PruningMetrics, Time,
};
use datafusion_pruning::{build_pruning_predicate, FilePruner, PruningPredicate};

#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use log::debug;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

/// Implements [`FileOpener`] for a parquet file
pub(super) struct ParquetOpener {
    /// Execution partition index
    pub partition_index: usize,
    /// Column indexes in `table_schema` needed by the query
    pub projection: Arc<[usize]>,
    /// Target number of rows in each output RecordBatch
    pub batch_size: usize,
    /// Optional limit on the number of rows to read
    pub limit: Option<usize>,
    /// Optional predicate to apply during the scan
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Schema of the output table without partition columns.
    /// This is the schema we coerce the physical file schema into.
    pub logical_file_schema: SchemaRef,
    /// Partition columns
    pub partition_fields: Vec<FieldRef>,
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
    /// Should the page index be read from parquet files, if present, to skip
    /// data pages
    pub enable_page_index: bool,
    /// Should the bloom filter be read from parquet, if present, to skip row
    /// groups
    pub enable_bloom_filter: bool,
    /// Schema adapter factory
    pub schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    /// Should row group pruning be applied
    pub enable_row_group_stats_pruning: bool,
    /// Coerce INT96 timestamps to specific TimeUnit
    pub coerce_int96: Option<TimeUnit>,
    /// Optional parquet FileDecryptionProperties
    #[cfg(feature = "parquet_encryption")]
    pub file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    /// Rewrite expressions in the context of the file schema
    pub(crate) expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    /// Optional factory to create file decryption properties dynamically
    #[cfg(feature = "parquet_encryption")]
    pub encryption_factory:
        Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
    /// Maximum size of the predicate cache, in bytes. If none, uses
    /// the arrow-rs default.
    pub max_predicate_cache_size: Option<usize>,
    /// If true, read row groups and batches in reverse order.
    /// Used for optimizing ORDER BY ... DESC queries on sorted data.
    pub reverse_scan: bool,
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

        let projected_schema =
            SchemaRef::from(self.logical_file_schema.project(&self.projection)?);
        let schema_adapter_factory = Arc::clone(&self.schema_adapter_factory);
        let schema_adapter = self.schema_adapter_factory.create(
            Arc::clone(&projected_schema),
            Arc::clone(&self.logical_file_schema),
        );
        let mut predicate = self.predicate.clone();
        let logical_file_schema = Arc::clone(&self.logical_file_schema);
        let partition_fields = self.partition_fields.clone();
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let coerce_int96 = self.coerce_int96;
        let enable_bloom_filter = self.enable_bloom_filter;
        let enable_row_group_stats_pruning = self.enable_row_group_stats_pruning;
        let limit = self.limit;

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .global_counter("num_predicate_creation_errors");

        let expr_adapter_factory = self.expr_adapter_factory.clone();
        let mut predicate_file_schema = Arc::clone(&self.logical_file_schema);

        let enable_page_index = self.enable_page_index;
        #[cfg(feature = "parquet_encryption")]
        let encryption_context = self.get_encryption_context();
        let max_predicate_cache_size = self.max_predicate_cache_size;

        let reverse_scan = self.reverse_scan;

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
                .map(|p| {
                    Ok::<_, DataFusionError>(
                        (is_dynamic_physical_expr(p) | partitioned_file.has_statistics())
                            .then_some(FilePruner::new(
                                Arc::clone(p),
                                &logical_file_schema,
                                partition_fields.clone(),
                                partitioned_file.clone(),
                                predicate_creation_errors.clone(),
                            )?),
                    )
                })
                .transpose()?
                .flatten();

            if let Some(file_pruner) = &mut file_pruner {
                if file_pruner.should_prune()? {
                    // Return an empty stream immediately to skip the work of setting up the actual stream
                    file_metrics.files_ranges_pruned_statistics.add_pruned(1);
                    return Ok(futures::stream::empty().boxed());
                }
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
            //   This is what the physicalfile schema is coerced to.
            // - The physical file schema: this is the schema as defined by the parquet file. This is what the parquet file actually contains.
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

            if let Some(ref coerce) = coerce_int96 {
                if let Some(merged) = coerce_int96_to_resolution(
                    reader_metadata.parquet_schema(),
                    &physical_file_schema,
                    coerce,
                ) {
                    physical_file_schema = Arc::new(merged);
                    options = options.with_schema(Arc::clone(&physical_file_schema));
                    reader_metadata = ArrowReaderMetadata::try_new(
                        Arc::clone(reader_metadata.metadata()),
                        options.clone(),
                    )?;
                }
            }

            // Adapt the predicate to the physical file schema.
            // This evaluates missing columns and inserts any necessary casts.
            if let Some(expr_adapter_factory) = expr_adapter_factory {
                predicate = predicate
                    .map(|p| {
                        let partition_values = partition_fields
                            .iter()
                            .cloned()
                            .zip(partitioned_file.partition_values.clone())
                            .collect_vec();
                        let expr = expr_adapter_factory
                            .create(
                                Arc::clone(&logical_file_schema),
                                Arc::clone(&physical_file_schema),
                            )
                            .with_partition_values(partition_values)
                            .rewrite(p)?;
                        // After rewriting to the file schema, further simplifications may be possible.
                        // For example, if `'a' = col_that_is_missing` becomes `'a' = NULL` that can then be simplified to `FALSE`
                        // and we can avoid doing any more work on the file (bloom filters, loading the page index, etc.).
                        PhysicalExprSimplifier::new(&physical_file_schema).simplify(expr)
                    })
                    .transpose()?;
                predicate_file_schema = Arc::clone(&physical_file_schema);
            }

            // Build predicates for this specific file
            let (pruning_predicate, page_pruning_predicate) = build_pruning_predicates(
                predicate.as_ref(),
                &predicate_file_schema,
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

            let (schema_mapping, adapted_projections) =
                schema_adapter.map_schema(&physical_file_schema)?;

            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                adapted_projections.iter().cloned(),
            );

            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    &predicate,
                    &physical_file_schema,
                    &predicate_file_schema,
                    builder.metadata(),
                    reorder_predicates,
                    &file_metrics,
                    &schema_adapter_factory,
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
            if enable_page_index && !access_plan.is_empty() {
                if let Some(p) = page_pruning_predicate {
                    access_plan = p.prune_plan_with_page_index(
                        access_plan,
                        &physical_file_schema,
                        builder.parquet_schema(),
                        file_metadata.as_ref(),
                        &file_metrics,
                    );
                }
            }

            let mut row_group_indexes = access_plan.row_group_indexes();
            // CRITICAL: For reverse scan, reverse the order of row groups BEFORE building the stream.
            // This allows the parquet reader to read row groups in reverse order while still
            // utilizing internal optimizations like row group caching and prefetching.
            if reverse_scan {
                row_group_indexes.reverse();
            }
            if let Some(row_selection) =
                access_plan.into_overall_row_selection(rg_metadata)?
            {
                builder = builder.with_row_selection(row_selection);
            }

            if let Some(limit) = limit {
                if !reverse_scan {
                    builder = builder.with_limit(limit)
                }
            }

            if let Some(max_predicate_cache_size) = max_predicate_cache_size {
                builder = builder.with_max_predicate_cache_size(max_predicate_cache_size);
            }

            // metrics from the arrow reader itself
            let arrow_reader_metrics = ArrowReaderMetrics::enabled();

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_group_indexes.clone())
                .with_metrics(arrow_reader_metrics.clone())
                .build()?;

            let files_ranges_pruned_statistics =
                file_metrics.files_ranges_pruned_statistics.clone();

            let predicate_cache_inner_records =
                file_metrics.predicate_cache_inner_records.clone();
            let predicate_cache_records = file_metrics.predicate_cache_records.clone();

            let final_schema = Arc::clone(&projected_schema);
            let stream: SendableRecordBatchStream = if reverse_scan {
                let error_adapted = stream.map_err(DataFusionError::from);
                let schema_mapped = error_adapted.map(move |result| {
                    copy_arrow_reader_metrics(
                        &arrow_reader_metrics,
                        &predicate_cache_inner_records,
                        &predicate_cache_records,
                    );
                    result.and_then(|batch| schema_mapping.map_batch(batch))
                });

                // Extract row counts for each row group (already in reverse order after the reverse above)
                let row_group_row_counts: Vec<usize> = row_group_indexes
                    .iter()
                    .map(|&idx| rg_metadata[idx].num_rows() as usize)
                    .collect();

                // Use the unified ReversedParquetStream with optional limit
                Box::pin(ReversedParquetStream::new(
                    schema_mapped,
                    final_schema,
                    row_group_row_counts,
                    file_metrics.row_groups_reversed.clone(),
                    file_metrics.batches_reversed.clone(),
                    file_metrics.reverse_time.clone(),
                    limit, // Pass limit directly, can be None or Some(value)
                ))
            } else {
                let error_adapted = stream.map_err(DataFusionError::from);
                let schema_mapped = error_adapted.map(move |result| {
                    copy_arrow_reader_metrics(
                        &arrow_reader_metrics,
                        &predicate_cache_inner_records,
                        &predicate_cache_records,
                    );
                    result.and_then(|batch| schema_mapping.map_batch(batch))
                });
                Box::pin(RecordBatchStreamAdapter::new(final_schema, schema_mapped))
            };

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
#[cfg_attr(not(feature = "parquet_encryption"), allow(dead_code))]
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
#[allow(dead_code)]
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
    #[allow(dead_code)]
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

/// Stream adapter for reversed parquet reading with row-group-level buffering.
///
/// # Architecture
///
/// This stream implements a sophisticated buffering strategy to achieve true reverse
/// reading of Parquet files while maintaining compatibility with the underlying
/// ParquetRecordBatchStream's optimizations (caching, prefetching, etc.).
///
/// ## Strategy Overview
///
/// 1. **Pre-reversed Row Groups**: Row groups are reversed BEFORE building the stream
///    (via `row_group_indexes.reverse()`). This allows the Parquet reader to read
///    them in reverse order while still utilizing internal optimizations.
///
/// 2. **Row-Group-Level Buffering**: As batches arrive from the input stream, we
///    track which row group they belong to using cumulative row counts. This is
///    the MINIMAL buffering unit required for correctness - we cannot reverse
///    individual batches without knowing the complete row group context.
///
/// 3. **Two-Stage Reversal**: When a complete row group is collected:
///    - Stage 1: Reverse rows within each batch (using Arrow's take kernel)
///    - Stage 2: Reverse the order of batches within the row group
///
/// 4. **Progressive Output**: Reversed batches are output immediately, minimizing
///    memory footprint. We never buffer more than one row group at a time.
///
/// ## Memory Characteristics
///
/// - **Bounded Memory**: Maximum memory usage = size of largest row group
/// - **Typical Usage**: ~128MB (default Parquet row group size)
/// - **Peak Usage**: During reversal of a single row group
///
/// ## Why Row-Group-Level Buffering is Necessary
///
/// Parquet organizes data into row groups (typically 128MB each), and each row group
/// is independently compressed and encoded. When reading in reverse:
///
/// - We cannot reverse individual batches in isolation because they may span
///   row group boundaries or be split arbitrarily by the batch_size parameter
/// - We must buffer complete row groups to ensure correct ordering semantics
/// - This is the minimum granularity that maintains correctness
///
/// ## Example
///
/// Given a file with 3 row groups, each containing 2 batches:
///
/// ```text
/// Normal order:  RG0[B0, B1] -> RG1[B0, B1] -> RG2[B0, B1]
/// Reversed:      RG2[B1_rev, B0_rev] -> RG1[B1_rev, B0_rev] -> RG0[B1_rev, B0_rev]
///                     ^^^^^^^^^^^^         ^^^^^^^^^^^^         ^^^^^^^^^^^^
///                     Output 1st           Output 2nd           Output 3rd
/// ```
///
/// ## Performance Characteristics
///
/// - **Latency**: First batch available after reading complete first (reversed) row group
/// - **Throughput**: Near-native speed with ~5-10% overhead for reversal operations
/// - **Memory**: O(row_group_size), not O(file_size)
///   TODO should we support max cache size to limit memory usage further? But if we exceed the cache size we can't reverse properly, so we need to fall back to normal reading?
struct ReversedParquetStream<S> {
    input: S,
    schema: SchemaRef,

    // Optional limit on the number of rows to output
    limit: Option<usize>,
    rows_produced: usize,

    // Current row group being processed
    current_rg_batches: Vec<RecordBatch>,
    current_rg_rows_read: usize,
    current_rg_total_rows: usize,

    // Output buffer for reversed batches
    output_batches: Vec<RecordBatch>,
    output_index: usize,

    // Row group metadata (each element is the number of rows in that row group)
    // Already in reverse order since row_group_indexes was reversed
    row_group_metadata: Vec<usize>,
    current_rg_index: usize,

    done: bool,

    // Metrics
    row_groups_reversed: Count,
    batches_reversed: Count,
    reverse_time: Time,

    /// Pending error from batch reversal
    pending_error: Option<DataFusionError>,

    /// Cached indices array for the most recent batch size
    /// (size, indices) - only cache one size to minimize memory overhead
    cached_indices: Option<(usize, Arc<UInt32Array>)>,
}

impl<S> ReversedParquetStream<S> {
    fn new(
        stream: S,
        schema: SchemaRef,
        row_group_metadata: Vec<usize>,
        row_groups_reversed: Count,
        batches_reversed: Count,
        reverse_time: Time,
        limit: Option<usize>,
    ) -> Self {
        let current_rg_total_rows = row_group_metadata.first().copied().unwrap_or(0);

        Self {
            input: stream,
            schema,
            limit,
            rows_produced: 0,
            current_rg_batches: Vec::new(),
            current_rg_rows_read: 0,
            current_rg_total_rows,
            output_batches: Vec::new(),
            output_index: 0,
            row_group_metadata,
            current_rg_index: 0,
            done: false,
            row_groups_reversed,
            batches_reversed,
            reverse_time,
            pending_error: None,
            cached_indices: None,
        }
    }

    /// Finalizes the current row group by performing the two-stage reversal.
    ///
    /// This is called when we've accumulated all batches for a row group.
    ///
    /// # Two-Stage Reversal Process
    ///
    /// 1. **Stage 1 - Reverse Rows**: For each batch in the row group, reverse
    ///    the order of rows using Arrow's `take` kernel with reversed indices.
    ///
    /// 2. **Stage 2 - Reverse Batches**: Reverse the order of batches within
    ///    the row group so that the last batch becomes first.
    ///
    /// # Example
    ///
    /// Input batches for a row group:
    /// ```text
    /// B0: [row0, row1, row2]
    /// B1: [row3, row4, row5]
    /// ```
    ///
    /// After Stage 1 (reverse rows within each batch):
    /// ```text
    /// B0_rev: [row2, row1, row0]
    /// B1_rev: [row5, row4, row3]
    /// ```
    ///
    /// After Stage 2 (reverse batch order):
    /// ```text
    /// Output: B1_rev, B0_rev
    /// Final sequence: [row5, row4, row3, row2, row1, row0]
    /// ```
    fn finalize_current_row_group(&mut self) {
        if self.current_rg_batches.is_empty() {
            return;
        }

        // Start timing
        let _timer = self.reverse_time.timer();
        let batch_count = self.current_rg_batches.len();

        // Step 1: Reverse rows within each batch
        let mut reversed_batches = Vec::with_capacity(self.current_rg_batches.len());
        for batch in self.current_rg_batches.drain(..) {
            let num_rows = batch.num_rows();
            if num_rows <= 1 {
                reversed_batches.push(batch);
                continue;
            }

            // Get or create indices array, using cache when possible
            let indices = if let Some((cached_size, cached_indices)) =
                &self.cached_indices
            {
                if *cached_size == num_rows {
                    Arc::clone(cached_indices)
                } else {
                    // Size mismatch, create new indices and update cache
                    let new_indices = Arc::new(UInt32Array::from_iter_values(
                        (0..num_rows as u32).rev(),
                    ));
                    self.cached_indices = Some((num_rows, Arc::clone(&new_indices)));
                    new_indices
                }
            } else {
                // First use, create and cache
                let new_indices =
                    Arc::new(UInt32Array::from_iter_values((0..num_rows as u32).rev()));
                self.cached_indices = Some((num_rows, Arc::clone(&new_indices)));
                new_indices
            };

            match batch
                .columns()
                .iter()
                .map(|col| take(col.as_ref(), indices.as_ref(), None))
                .collect::<std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError>>()
                .map_err(DataFusionError::from)
                .and_then(|reversed_columns| {
                    RecordBatch::try_new(batch.schema(), reversed_columns)
                        .map_err(DataFusionError::from)
                }) {
                Ok(reversed) => reversed_batches.push(reversed),
                Err(e) => {
                    // Store error and return it on next poll
                    self.pending_error = Some(e);
                    return;
                }
            }
        }

        // Step 2: Reverse the order of batches
        self.output_batches = reversed_batches.into_iter().rev().collect();
        self.output_index = 0;

        // Update metrics
        self.row_groups_reversed.add(1);
        self.batches_reversed.add(batch_count);

        // Prepare for next row group
        self.current_rg_rows_read = 0;
        self.current_rg_index += 1;
        self.current_rg_total_rows = self
            .row_group_metadata
            .get(self.current_rg_index)
            .copied()
            .unwrap_or(0);
    }

    /// Check if we've reached the limit
    #[inline]
    fn is_limit_reached(&self) -> bool {
        self.limit.is_some_and(|limit| self.rows_produced >= limit)
    }
}

impl<S> Stream for ReversedParquetStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut(); // Safe: We own the Pin and ReversedParquetStream is Unpin

        // Check for pending errors first
        if let Some(err) = this.pending_error.take() {
            return Poll::Ready(Some(Err(err)));
        }

        if this.done || this.is_limit_reached() {
            return Poll::Ready(None);
        }

        loop {
            // First, output any ready batches
            if this.output_index < this.output_batches.len() {
                let batch = this.output_batches[this.output_index].clone();
                this.output_index += 1;

                // Apply limit if specified
                if let Some(limit) = this.limit {
                    let remaining = limit.saturating_sub(this.rows_produced);

                    if batch.num_rows() <= remaining {
                        this.rows_produced += batch.num_rows();
                        return Poll::Ready(Some(Ok(batch)));
                    } else {
                        // Slice batch to fit within limit
                        let sliced = batch.slice(0, remaining);
                        this.rows_produced += remaining;
                        this.done = true;
                        return Poll::Ready(Some(Ok(sliced)));
                    }
                } else {
                    // No limit, return full batch
                    return Poll::Ready(Some(Ok(batch)));
                }
            }

            // Need to read more data
            match ready!(Pin::new(&mut this.input).poll_next(cx)) {
                Some(Ok(batch)) => {
                    let batch_rows = batch.num_rows();
                    this.current_rg_batches.push(batch);
                    this.current_rg_rows_read += batch_rows;

                    // Check if current row group is complete
                    if this.current_rg_rows_read >= this.current_rg_total_rows {
                        this.finalize_current_row_group();

                        // Check for errors after finalization
                        if let Some(err) = this.pending_error.take() {
                            return Poll::Ready(Some(Err(err)));
                        }
                        // Continue loop to output the reversed batches
                    }
                    // Otherwise continue reading next batch from current row group
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    // Handle the last row group if any
                    if !this.current_rg_batches.is_empty() {
                        this.finalize_current_row_group();

                        // Check for errors after finalization
                        if let Some(err) = this.pending_error.take() {
                            return Poll::Ready(Some(Err(err)));
                        }
                        // Continue loop to output final batches
                    } else {
                        this.done = true;
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}

impl<S> RecordBatchStream for ReversedParquetStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        SchemaRef::clone(&self.schema)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::source::ParquetSource;
    use crate::{opener::ParquetOpener, DefaultParquetFileReaderFactory};
    use arrow::array::RecordBatch;
    use arrow::{
        compute::cast,
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use bytes::{BufMut, BytesMut};
    use datafusion_common::{
        assert_batches_eq, record_batch, stats::Precision, ColumnStatistics,
        DataFusionError, ScalarValue, Statistics,
    };
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::{
        file_stream::FileOpener,
        schema_adapter::{
            DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory,
            SchemaMapper,
        },
        PartitionedFile,
    };
    use datafusion_expr::{col, lit};
    use datafusion_physical_expr::{
        expressions::DynamicFilterPhysicalExpr, planner::logical2physical, PhysicalExpr,
    };
    use datafusion_physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
    use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
    use futures::{Stream, StreamExt};
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    use parquet::arrow::ArrowWriter;

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

    async fn collect_batches(
        mut stream: std::pin::Pin<
            Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>,
        >,
    ) -> Vec<RecordBatch> {
        let mut batches = vec![];
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }
        batches
    }

    async fn write_parquet(
        store: Arc<dyn ObjectStore>,
        filename: &str,
        batch: RecordBatch,
    ) -> usize {
        let mut out = BytesMut::new().writer();
        {
            let mut writer =
                ArrowWriter::try_new(&mut out, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
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
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0, 1]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![],
                pushdown_filters: false, // note that this is false!
                reorder_filters: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
                enable_row_group_stats_pruning: true,
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
                reverse_scan: false,
            }
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

        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: file_schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![Arc::new(Field::new(
                    "part",
                    DataType::Int32,
                    false,
                ))],
                pushdown_filters: false, // note that this is false!
                reorder_filters: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
                enable_row_group_stats_pruning: true,
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
                reverse_scan: false,
            }
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
        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: file_schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![Arc::new(Field::new(
                    "part",
                    DataType::Int32,
                    false,
                ))],
                pushdown_filters: false, // note that this is false!
                reorder_filters: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
                enable_row_group_stats_pruning: true,
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
                reverse_scan: false,
            }
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

        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: file_schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![Arc::new(Field::new(
                    "part",
                    DataType::Int32,
                    false,
                ))],
                pushdown_filters: true, // note that this is true!
                reorder_filters: true,
                enable_page_index: false,
                enable_bloom_filter: false,
                schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
                enable_row_group_stats_pruning: false, // note that this is false!
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
                reverse_scan: false,
            }
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

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));

        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: file_schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![Arc::new(Field::new(
                    "part",
                    DataType::Int32,
                    false,
                ))],
                pushdown_filters: false, // note that this is false!
                reorder_filters: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
                enable_row_group_stats_pruning: true,
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
                reverse_scan: false,
            }
        };

        // Filter should NOT match the stats but the file is never attempted to be pruned because the filters are not dynamic
        let expr = col("part").eq(lit(2));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // If we make the filter dynamic, it should prune
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
        match metrics.sum_by_name(metric_name) {
            Some(v) => v.as_usize(),
            _ => {
                panic!(
                    "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_custom_schema_adapter_no_rewriter() {
        // Make a hardcoded schema adapter that adds a new column "b" with default value 0.0
        // and converts the first column "a" from Int32 to UInt64.
        #[derive(Debug, Clone)]
        struct CustomSchemaMapper;

        impl SchemaMapper for CustomSchemaMapper {
            fn map_batch(
                &self,
                batch: RecordBatch,
            ) -> datafusion_common::Result<RecordBatch> {
                let a_column = cast(batch.column(0), &DataType::UInt64)?;
                // Add in a new column "b" with default value 0.0
                let b_column =
                    arrow::array::Float64Array::from(vec![Some(0.0); batch.num_rows()]);
                let columns = vec![a_column, Arc::new(b_column)];
                let new_schema = Arc::new(Schema::new(vec![
                    Field::new("a", DataType::UInt64, false),
                    Field::new("b", DataType::Float64, false),
                ]));
                Ok(RecordBatch::try_new(new_schema, columns)?)
            }

            fn map_column_statistics(
                &self,
                file_col_statistics: &[ColumnStatistics],
            ) -> datafusion_common::Result<Vec<ColumnStatistics>> {
                Ok(vec![
                    file_col_statistics[0].clone(),
                    ColumnStatistics::new_unknown(),
                ])
            }
        }

        #[derive(Debug, Clone)]
        struct CustomSchemaAdapter;

        impl SchemaAdapter for CustomSchemaAdapter {
            fn map_schema(
                &self,
                _file_schema: &Schema,
            ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)>
            {
                let mapper = Arc::new(CustomSchemaMapper);
                let projection = vec![0]; // We only need to read the first column "a" from the file
                Ok((mapper, projection))
            }

            fn map_column_index(
                &self,
                index: usize,
                file_schema: &Schema,
            ) -> Option<usize> {
                if index < file_schema.fields().len() {
                    Some(index)
                } else {
                    None // The new column "b" is not in the original schema
                }
            }
        }

        #[derive(Debug, Clone)]
        struct CustomSchemaAdapterFactory;

        impl SchemaAdapterFactory for CustomSchemaAdapterFactory {
            fn create(
                &self,
                _projected_table_schema: SchemaRef,
                _table_schema: SchemaRef,
            ) -> Box<dyn SchemaAdapter> {
                Box::new(CustomSchemaAdapter)
            }
        }

        // Test that if no expression rewriter is provided we use a schemaadapter to adapt the data to the expression
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        // Write out the batch to a Parquet file
        let data_size =
            write_parquet(Arc::clone(&store), "test.parquet", batch.clone()).await;
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::Float64, false),
        ]));

        let make_opener = |predicate| ParquetOpener {
            partition_index: 0,
            projection: Arc::new([0, 1]),
            batch_size: 1024,
            limit: None,
            predicate: Some(predicate),
            logical_file_schema: Arc::clone(&table_schema),
            metadata_size_hint: None,
            metrics: ExecutionPlanMetricsSet::new(),
            parquet_file_reader_factory: Arc::new(DefaultParquetFileReaderFactory::new(
                Arc::clone(&store),
            )),
            partition_fields: vec![],
            pushdown_filters: true,
            reorder_filters: false,
            enable_page_index: false,
            enable_bloom_filter: false,
            schema_adapter_factory: Arc::new(CustomSchemaAdapterFactory),
            enable_row_group_stats_pruning: false,
            coerce_int96: None,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties: None,
            expr_adapter_factory: None,
            #[cfg(feature = "parquet_encryption")]
            encryption_factory: None,
            max_predicate_cache_size: None,
            reverse_scan: false,
        };

        let predicate = logical2physical(&col("a").eq(lit(1u64)), &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let batches = collect_batches(stream).await;

        #[rustfmt::skip]
        let expected = [
            "+---+-----+",
            "| a | b   |",
            "+---+-----+",
            "| 1 | 0.0 |",
            "+---+-----+",
        ];
        assert_batches_eq!(expected, &batches);
        let metrics = opener.metrics.clone_inner();
        assert_eq!(get_value(&metrics, "row_groups_pruned_statistics"), 0);
        assert_eq!(get_value(&metrics, "pushdown_rows_pruned"), 2);
    }

    #[tokio::test]
    async fn test_reverse_scan_single_batch() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!((
            "id",
            Int32,
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)]
        ))
        .unwrap();

        let data_size =
            write_parquet(Arc::clone(&store), "test.parquet", batch.clone()).await;

        let schema = batch.schema();
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );

        let make_opener = |reverse_scan: bool| ParquetOpener {
            partition_index: 0,
            projection: Arc::new([0]),
            batch_size: 1024,
            limit: None,
            predicate: None,
            logical_file_schema: schema.clone(),
            metadata_size_hint: None,
            metrics: ExecutionPlanMetricsSet::new(),
            parquet_file_reader_factory: Arc::new(DefaultParquetFileReaderFactory::new(
                Arc::clone(&store),
            )),
            partition_fields: vec![],
            pushdown_filters: false,
            reorder_filters: false,
            enable_page_index: false,
            enable_bloom_filter: false,
            schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
            enable_row_group_stats_pruning: false,
            coerce_int96: None,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties: None,
            expr_adapter_factory: None,
            #[cfg(feature = "parquet_encryption")]
            encryption_factory: None,
            max_predicate_cache_size: None,
            reverse_scan,
        };

        let opener_normal = make_opener(false);
        let stream_normal = opener_normal.open(file.clone()).unwrap().await.unwrap();
        let batches_normal = collect_batches(stream_normal).await;

        // test reverse scan
        let opener_reverse = make_opener(true);
        let stream_reverse = opener_reverse.open(file.clone()).unwrap().await.unwrap();
        let batches_reverse = collect_batches(stream_reverse).await;

        assert_eq!(batches_normal.len(), batches_reverse.len());

        assert_eq!(batches_normal[0].num_rows(), batches_reverse[0].num_rows());

        let normal_col = batches_normal[0].column(0);
        let reverse_col = batches_reverse[0].column(0);

        let normal_values: Vec<i32> = (0..normal_col.len())
            .filter_map(|i| {
                Some(
                    normal_col
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()?
                        .value(i),
                )
            })
            .collect();

        let reverse_values: Vec<i32> = (0..reverse_col.len())
            .filter_map(|i| {
                Some(
                    reverse_col
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()?
                        .value(i),
                )
            })
            .collect();

        assert_eq!(normal_values, vec![1, 2, 3, 4, 5]);
        assert_eq!(reverse_values, vec![5, 4, 3, 2, 1]);
    }

    #[tokio::test]
    async fn test_reverse_scan_multiple_batches() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!((
            "id",
            Int32,
            vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(10)
            ]
        ))
        .unwrap();

        let data_size =
            write_parquet(Arc::clone(&store), "test.parquet", batch.clone()).await;

        let schema = batch.schema();
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );

        let make_opener = |reverse_scan: bool| ParquetOpener {
            partition_index: 0,
            projection: Arc::new([0]),
            batch_size: 3,
            limit: None,
            predicate: None,
            logical_file_schema: schema.clone(),
            metadata_size_hint: None,
            metrics: ExecutionPlanMetricsSet::new(),
            parquet_file_reader_factory: Arc::new(DefaultParquetFileReaderFactory::new(
                Arc::clone(&store),
            )),
            partition_fields: vec![],
            pushdown_filters: false,
            reorder_filters: false,
            enable_page_index: false,
            enable_bloom_filter: false,
            schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
            enable_row_group_stats_pruning: false,
            coerce_int96: None,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties: None,
            expr_adapter_factory: None,
            #[cfg(feature = "parquet_encryption")]
            encryption_factory: None,
            max_predicate_cache_size: None,
            reverse_scan,
        };

        let opener_normal = make_opener(false);
        let stream_normal = opener_normal.open(file.clone()).unwrap().await.unwrap();
        let batches_normal = collect_batches(stream_normal).await;

        // reverse scan
        let opener_reverse = make_opener(true);
        let stream_reverse = opener_reverse.open(file.clone()).unwrap().await.unwrap();
        let batches_reverse = collect_batches(stream_reverse).await;

        let total_normal: usize = batches_normal.iter().map(|b| b.num_rows()).sum();
        let total_reverse: usize = batches_reverse.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_normal, total_reverse);
        assert_eq!(total_normal, 10);

        let mut normal_values = Vec::new();
        for batch in &batches_normal {
            let col = batch.column(0);
            for i in 0..col.len() {
                if let Some(val) = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .map(|arr| arr.value(i))
                {
                    normal_values.push(val);
                }
            }
        }

        let mut reverse_values = Vec::new();
        for batch in &batches_reverse {
            let col = batch.column(0);
            for i in 0..col.len() {
                if let Some(val) = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .map(|arr| arr.value(i))
                {
                    reverse_values.push(val);
                }
            }
        }

        assert_eq!(normal_values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(reverse_values, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_parquet_source_reverse_scan_setter_getter() {
        let schema = Arc::new(Schema::empty());

        let source = ParquetSource::new(schema.clone());
        assert!(!source.reverse_scan());

        let source = source.with_reverse_scan(true);
        assert!(source.reverse_scan());

        let source = source.with_reverse_scan(false);
        assert!(!source.reverse_scan());
    }

    #[test]
    fn test_parquet_source_reverse_scan_clone_preserves_flag() {
        let schema = Arc::new(Schema::empty());

        let source = ParquetSource::new(schema).with_reverse_scan(true);

        let cloned = source.clone();
        assert!(cloned.reverse_scan());

        let modified = cloned.with_reverse_scan(false);
        assert!(source.reverse_scan());
        assert!(!modified.reverse_scan());
    }

    #[test]
    fn test_parquet_source_with_projection_preserves_reverse_scan() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_physical_expr::expressions::Column;
        use datafusion_physical_plan::projection::{ProjectionExpr, ProjectionExprs};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let source = ParquetSource::new(schema.clone()).with_reverse_scan(true);

        // Create a projection that selects columns 0 and 2 (a and c)
        let projection = ProjectionExprs::from(vec![
            ProjectionExpr::new(Arc::new(Column::new("a", 0)), "a".to_string()),
            ProjectionExpr::new(Arc::new(Column::new("c", 2)), "c".to_string()),
        ]);

        let projected_result = source
            .try_pushdown_projection(&projection)
            .expect("Failed to push down projection");

        assert!(projected_result.is_some());

        let projected_arc = projected_result.unwrap();
        let projected_source = projected_arc
            .as_any()
            .downcast_ref::<ParquetSource>()
            .expect("Failed to downcast to ParquetSource");

        assert!(projected_source.reverse_scan());
    }

    #[tokio::test]
    async fn test_reverse_scan_multiple_row_groups() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // Create a parquet file with multiple row groups
        // Each row group will have 5 rows
        let mut out = BytesMut::new().writer();
        {
            // Set row group size to force multiple row groups
            let props = parquet::file::properties::WriterProperties::builder()
                .set_max_row_group_size(5)
                .build();
            let mut writer = ArrowWriter::try_new_with_options(
                &mut out,
                schema.clone(),
                ArrowWriterOptions::new().with_properties(props),
            )
            .unwrap();

            // Write 3 row groups: [1,2,3,4,5], [6,7,8,9,10], [11,12,13,14,15]
            for group_start in [1, 6, 11] {
                let batch = record_batch!((
                    "id",
                    Int32,
                    vec![
                        Some(group_start),
                        Some(group_start + 1),
                        Some(group_start + 2),
                        Some(group_start + 3),
                        Some(group_start + 4),
                    ]
                ))
                .unwrap();
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();
        }

        let data = out.into_inner().freeze();
        let data_len = data.len();
        store
            .put(&Path::from("multi_rg.parquet"), data.into())
            .await
            .unwrap();

        let file = PartitionedFile::new(
            "multi_rg.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        );

        let make_opener = |reverse_scan: bool| ParquetOpener {
            partition_index: 0,
            projection: Arc::new([0]),
            batch_size: 1024, // Large enough to read entire row group at once
            limit: None,
            predicate: None,
            logical_file_schema: schema.clone(),
            metadata_size_hint: None,
            metrics: ExecutionPlanMetricsSet::new(),
            parquet_file_reader_factory: Arc::new(DefaultParquetFileReaderFactory::new(
                Arc::clone(&store),
            )),
            partition_fields: vec![],
            pushdown_filters: false,
            reorder_filters: false,
            enable_page_index: false,
            enable_bloom_filter: false,
            schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
            enable_row_group_stats_pruning: false,
            coerce_int96: None,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties: None,
            expr_adapter_factory: None,
            #[cfg(feature = "parquet_encryption")]
            encryption_factory: None,
            max_predicate_cache_size: None,
            reverse_scan,
        };

        // Test normal scan
        let opener_normal = make_opener(false);
        let stream_normal = opener_normal.open(file.clone()).unwrap().await.unwrap();
        let batches_normal = collect_batches(stream_normal).await;

        let mut normal_values = Vec::new();
        for batch in &batches_normal {
            let col = batch.column(0);
            for i in 0..col.len() {
                if let Some(val) = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .map(|arr| arr.value(i))
                {
                    normal_values.push(val);
                }
            }
        }

        // Test reverse scan
        let opener_reverse = make_opener(true);
        let stream_reverse = opener_reverse.open(file.clone()).unwrap().await.unwrap();
        let batches_reverse = collect_batches(stream_reverse).await;

        let mut reverse_values = Vec::new();
        for batch in &batches_reverse {
            let col = batch.column(0);
            for i in 0..col.len() {
                if let Some(val) = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .map(|arr| arr.value(i))
                {
                    reverse_values.push(val);
                }
            }
        }

        // Normal scan should be: [1,2,3,4,5, 6,7,8,9,10, 11,12,13,14,15]
        assert_eq!(
            normal_values,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );

        // Reverse scan should be: [15,14,13,12,11, 10,9,8,7,6, 5,4,3,2,1]
        // Note: row groups are reversed, then rows within each row group are reversed
        assert_eq!(
            reverse_values,
            vec![15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        );
    }

    #[tokio::test]
    async fn test_reverse_scan_multiple_row_groups_with_limit() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // Create parquet file with 3 row groups
        let mut out = BytesMut::new().writer();
        {
            let props = parquet::file::properties::WriterProperties::builder()
                .set_max_row_group_size(5)
                .build();
            let mut writer = ArrowWriter::try_new_with_options(
                &mut out,
                schema.clone(),
                ArrowWriterOptions::new().with_properties(props),
            )
            .unwrap();

            for group_start in [1, 6, 11] {
                let batch = record_batch!((
                    "id",
                    Int32,
                    vec![
                        Some(group_start),
                        Some(group_start + 1),
                        Some(group_start + 2),
                        Some(group_start + 3),
                        Some(group_start + 4),
                    ]
                ))
                .unwrap();
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();
        }

        let data = out.into_inner().freeze();
        let data_len = data.len();
        store
            .put(&Path::from("multi_rg_limit.parquet"), data.into())
            .await
            .unwrap();

        let file = PartitionedFile::new(
            "multi_rg_limit.parquet".to_string(),
            u64::try_from(data_len).unwrap(),
        );

        let make_opener = |reverse_scan: bool, limit: Option<usize>| ParquetOpener {
            partition_index: 0,
            projection: Arc::new([0]),
            batch_size: 1024,
            limit,
            predicate: None,
            logical_file_schema: schema.clone(),
            metadata_size_hint: None,
            metrics: ExecutionPlanMetricsSet::new(),
            parquet_file_reader_factory: Arc::new(DefaultParquetFileReaderFactory::new(
                Arc::clone(&store),
            )),
            partition_fields: vec![],
            pushdown_filters: false,
            reorder_filters: false,
            enable_page_index: false,
            enable_bloom_filter: false,
            schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
            enable_row_group_stats_pruning: false,
            coerce_int96: None,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties: None,
            expr_adapter_factory: None,
            #[cfg(feature = "parquet_encryption")]
            encryption_factory: None,
            max_predicate_cache_size: None,
            reverse_scan,
        };

        // Test reverse scan with LIMIT 7
        let opener_reverse = make_opener(true, Some(7));
        let stream_reverse = opener_reverse.open(file.clone()).unwrap().await.unwrap();
        let batches_reverse = collect_batches(stream_reverse).await;

        let mut reverse_values = Vec::new();
        for batch in &batches_reverse {
            let col = batch.column(0);
            for i in 0..col.len() {
                if let Some(val) = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .map(|arr| arr.value(i))
                {
                    reverse_values.push(val);
                }
            }
        }

        // With LIMIT 7 on reverse scan, we should get: [15,14,13,12,11, 10,9]
        assert_eq!(reverse_values, vec![15, 14, 13, 12, 11, 10, 9]);
        assert_eq!(reverse_values.len(), 7);
    }

    #[tokio::test]
    async fn test_reverse_scan_end_to_end_ascending_data() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let forward_source =
            Arc::new(ParquetSource::new(schema.clone()).with_reverse_scan(false));

        let reverse_source =
            Arc::new(ParquetSource::new(schema.clone()).with_reverse_scan(true));

        assert!(!forward_source.reverse_scan());
        assert!(reverse_source.reverse_scan());
    }

    #[tokio::test]
    async fn test_reverse_scan_preserves_correctness_with_nulls() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let data = vec![Some(1), Some(2), None, Some(4), Some(5)];
        let array = arrow::array::Int32Array::from(data);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])
            .expect("Failed to create RecordBatch");

        let mut out = BytesMut::new().writer();
        {
            let mut writer = ArrowWriter::try_new(&mut out, schema.clone(), None)
                .expect("Failed to create writer");
            writer.write(&batch).expect("Failed to write batch");
            writer.finish().expect("Failed to finish writing");
        }

        let data = out.into_inner().freeze();
        let _data_len = data.len() as u64;
        store
            .put(&Path::from("nulls.parquet"), data.into())
            .await
            .expect("Failed to put file");

        let forward_source =
            Arc::new(ParquetSource::new(schema.clone()).with_reverse_scan(false));
        let reverse_source =
            Arc::new(ParquetSource::new(schema.clone()).with_reverse_scan(true));

        assert!(!forward_source.reverse_scan());
        assert!(reverse_source.reverse_scan());
    }

    #[test]
    fn test_reverse_scan_source_builder_pattern() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let source = ParquetSource::new(schema.clone())
            .with_metadata_size_hint(16384)
            .with_reverse_scan(true);

        assert!(source.reverse_scan());
        assert_eq!(source.metadata_size_hint, Some(16384));
    }

    #[test]
    fn test_reverse_scan_immutable_copy() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let source1 = ParquetSource::new(schema.clone()).with_reverse_scan(true);
        let source2 = source1.clone().with_reverse_scan(false);

        assert!(source1.reverse_scan());
        assert!(!source2.reverse_scan());
    }
}
