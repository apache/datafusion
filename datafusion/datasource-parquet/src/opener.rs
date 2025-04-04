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

use std::sync::Arc;

use crate::page_filter::PagePruningAccessPlanFilter;
use crate::row_group_filter::RowGroupAccessPlanFilter;
use crate::{
    apply_file_schema_type_coercions, row_filter, should_enable_page_index,
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion_datasource::file_meta::FileMeta;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use datafusion_common::{exec_err, Result};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_optimizer::pruning::PruningPredicate;
use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

use futures::{StreamExt, TryStreamExt};
use log::debug;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaDataReader;

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
    /// Schema of the output table
    pub table_schema: SchemaRef,
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
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let file_range = file_meta.range.clone();
        let extensions = file_meta.extensions.clone();
        let file_name = file_meta.location().to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);

        let metadata_size_hint = file_meta.metadata_size_hint.or(self.metadata_size_hint);

        let mut reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                file_meta,
                metadata_size_hint,
                &self.metrics,
            )?;

        let batch_size = self.batch_size;

        let projected_schema =
            SchemaRef::from(self.table_schema.project(&self.projection)?);
        let schema_adapter_factory = Arc::clone(&self.schema_adapter_factory);
        let schema_adapter = self
            .schema_adapter_factory
            .create(projected_schema, Arc::clone(&self.table_schema));
        let predicate = self.predicate.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let enable_bloom_filter = self.enable_bloom_filter;
        let enable_row_group_stats_pruning = self.enable_row_group_stats_pruning;
        let limit = self.limit;

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .global_counter("num_predicate_creation_errors");

        let enable_page_index = self.enable_page_index;

        Ok(Box::pin(async move {
            // Don't load the page index yet - we will decide later if we need it
            let options = ArrowReaderOptions::new().with_page_index(false);

            let mut metadata_timer = file_metrics.metadata_load_time.timer();
            let mut metadata =
                ArrowReaderMetadata::load_async(&mut reader, options.clone()).await?;
            // Note about schemas: we are actually dealing with **3 different schemas** here:
            // - The table schema as defined by the TableProvider. This is what the user sees, what they get when they `SELECT * FROM table`, etc.
            // - The "virtual" file schema: this is the table schema minus any hive partition columns and projections. This is what the file schema is coerced to.
            // - The physical file schema: this is the schema as defined by the parquet file. This is what the parquet file actually contains.
            let mut physical_file_schema = Arc::clone(metadata.schema());

            // read with view types
            if let Some(merged) =
                apply_file_schema_type_coercions(&table_schema, &physical_file_schema)
            {
                physical_file_schema = Arc::new(merged);
            }

            // Build predicates for this specific file
            let (pruning_predicate, page_pruning_predicate) = build_pruning_predicates(
                &predicate,
                &physical_file_schema,
                &predicate_creation_errors,
            );

            // Now check if we should load the page index
            if should_enable_page_index(enable_page_index, &page_pruning_predicate) {
                metadata = load_page_index(
                    metadata,
                    &mut reader,
                    // Since we're manually loading the page index the option here should not matter but we pass it in for consistency
                    ArrowReaderOptions::new()
                        .with_page_index(true)
                        .with_schema(physical_file_schema.clone()),
                )
                .await?;
            }

            metadata_timer.stop();

            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata);

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
                    &table_schema,
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
                            "Ignoring error building row filter for '{:?}': {}",
                            predicate, e
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
                }
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

            let row_group_indexes = access_plan.row_group_indexes();
            if let Some(row_selection) =
                access_plan.into_overall_row_selection(rg_metadata)?
            {
                builder = builder.with_row_selection(row_selection);
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_group_indexes)
                .build()?;

            let adapted = stream
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .map(move |maybe_batch| {
                    maybe_batch
                        .and_then(|b| schema_mapping.map_batch(b).map_err(Into::into))
                });

            Ok(adapted.boxed())
        }))
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

/// Build a pruning predicate from an optional predicate expression.
/// If the predicate is None or the predicate cannot be converted to a pruning
/// predicate, return None.
/// If there is an error creating the pruning predicate it is recorded by incrementing
/// the `predicate_creation_errors` counter.
pub(crate) fn build_pruning_predicate(
    predicate: Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
    predicate_creation_errors: &Count,
) -> Option<Arc<PruningPredicate>> {
    match PruningPredicate::try_new(predicate, Arc::clone(file_schema)) {
        Ok(pruning_predicate) => {
            if !pruning_predicate.always_true() {
                return Some(Arc::new(pruning_predicate));
            }
        }
        Err(e) => {
            debug!("Could not create pruning predicate for: {e}");
            predicate_creation_errors.add(1);
        }
    }
    None
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

fn build_pruning_predicates(
    predicate: &Option<Arc<dyn PhysicalExpr>>,
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

async fn load_page_index<T: AsyncFileReader>(
    arrow_reader: ArrowReaderMetadata,
    input: &mut T,
    options: ArrowReaderOptions,
) -> Result<ArrowReaderMetadata> {
    let parquet_metadata = arrow_reader.metadata();
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
        let mut reader =
            ParquetMetaDataReader::new_with_metadata(m).with_page_indexes(true);
        reader.load_page_index(input).await?;
        let new_parquet_metadata = reader.finish()?;
        let new_arrow_reader =
            ArrowReaderMetadata::try_new(Arc::new(new_parquet_metadata), options)?;
        Ok(new_arrow_reader)
    } else {
        // No need to load the page index again, just return the existing metadata
        Ok(arrow_reader)
    }
}
