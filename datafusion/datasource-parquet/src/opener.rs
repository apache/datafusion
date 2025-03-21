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

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::page_filter::PagePruningAccessPlanFilter;
use crate::row_group_filter::RowGroupAccessPlanFilter;
use crate::{
    apply_file_schema_type_coercions, row_filter, should_enable_page_index,
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_datasource::file_expr_rewriter::FileExpressionRewriter;
use datafusion_datasource::file_meta::FileMeta;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion_common::{exec_err, Result, SchemaError};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_optimizer::pruning::PruningPredicate;
use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

use futures::{StreamExt, TryStreamExt};
use log::debug;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

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
    /// Optional pruning predicate applied to row group statistics
    pub pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Optional pruning predicate applied to data page statistics
    pub page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
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
    /// Filter expression rewriter factory
    pub filter_expression_rewriter: Option<Arc<dyn FileExpressionRewriter>>,
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        // Note about schemas: we are actually dealing with _4_ different schemas here:
        // - The table schema as defined by the TableProvider. This is what the user sees, what they get when they `SELECT * FROM table`, etc.
        // - The "virtual" file schema: this is the table schema minus any hive partition columns. This is what the file schema is coerced to.
        // - The physical file schema: this is the schema as defined by the parquet file. This is what the parquet file actually contains.
        // - The filter schema: a hybrid of the virtual file schema and the physical file schema.
        //   If a filter is rewritten to reference columns that are in the physical file schema but not the virtual file schema, we need to add those columns to the filter schema so that the filter can be evaluated.
        //   This schema is generated by taking any columns from the virtual file schema that are referenced by the filter and adding any columns from the physical file schema that are referenced by the filter but not in the virtual file schema.
        //   Columns from the virtual file schema are added in the order they appear in the virtual file schema.
        //   The columns from the physical file schema are always added to the end of the schema, in the order they appear in the physical file schema.
        //
        // I think it might be wise to do some renaming of parameters where possible, e.g. rename `file_schema` to `table_schema_without_partition_columns` and `physical_file_schema` or something like that.
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
        let mut predicate = self.predicate.clone();
        let mut pruning_predicate = self.pruning_predicate.clone();
        let mut page_pruning_predicate = self.page_pruning_predicate.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let mut filter_schema = Arc::clone(&table_schema);
        let filter_expression_rewriter = self.filter_expression_rewriter.clone();
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let enable_page_index = should_enable_page_index(
            self.enable_page_index,
            &self.predicate,
            &self.page_pruning_predicate,
            &self.filter_expression_rewriter,
        );
        let enable_bloom_filter = self.enable_bloom_filter;
        let limit = self.limit;

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .global_counter("num_predicate_creation_errors");

        Ok(Box::pin(async move {
            let options = ArrowReaderOptions::new().with_page_index(enable_page_index);

            let mut metadata_timer = file_metrics.metadata_load_time.timer();
            let metadata =
                ArrowReaderMetadata::load_async(&mut reader, options.clone()).await?;
            let mut schema = Arc::clone(metadata.schema());

            // read with view types
            if let Some(merged) = apply_file_schema_type_coercions(&table_schema, &schema)
            {
                schema = Arc::new(merged);
            }

            let options = ArrowReaderOptions::new()
                .with_page_index(enable_page_index)
                .with_schema(Arc::clone(&schema));
            let metadata =
                ArrowReaderMetadata::try_new(Arc::clone(metadata.metadata()), options)?;

            metadata_timer.stop();

            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata);

            let file_schema = Arc::clone(builder.schema());

            // Try to rewrite the predicate using our filter expression rewriter
            // This must happen before build_row_filter and other subsequent steps
            // so that they all use the rewritten predicate if it was rewritten.
            if let Some(original_predicate) = predicate.clone() {
                if let Some(filter_rewriter) = filter_expression_rewriter {
                    if let Ok(rewritten) = filter_rewriter
                        .rewrite(Arc::clone(&file_schema), original_predicate)
                    {
                        let mut filter_schema_builder =
                            FilterSchemaBuilder::new(&file_schema, &table_schema);
                        rewritten.visit(&mut filter_schema_builder)?;
                        filter_schema = filter_schema_builder.build();
                        // If we rewrote the filter we need to recompute the pruning predicates to match the new filter.
                        page_pruning_predicate = Some(build_page_pruning_predicate(
                            &rewritten,
                            &filter_schema,
                        ));
                        pruning_predicate = build_pruning_predicate(
                            Arc::clone(&rewritten),
                            &filter_schema,
                            &predicate_creation_errors,
                        );
                        // Update the predicate to the rewritten version
                        predicate = Some(rewritten);
                    }
                }
            }

            let schema_adapter = schema_adapter_factory
                .create(Arc::clone(&projected_schema), Arc::clone(&filter_schema));

            let (schema_mapping, adapted_projections) =
                schema_adapter.map_schema(&file_schema)?;

            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                adapted_projections.iter().cloned(),
            );

            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    &predicate,
                    &file_schema,
                    &filter_schema,
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
                row_groups.prune_by_statistics(
                    &file_schema,
                    builder.parquet_schema(),
                    rg_metadata,
                    predicate,
                    &file_metrics,
                );

                if enable_bloom_filter && !row_groups.is_empty() {
                    row_groups
                        .prune_by_bloom_filters(
                            &file_schema,
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
                        &file_schema,
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

/// A vistor for a PhysicalExpr that collects all column references to determine what columns the expression needs to be evaluated.
struct FilterSchemaBuilder<'schema> {
    filter_schema_fields: BTreeSet<Arc<Field>>,
    file_schema: &'schema Schema,
    table_schema: &'schema Schema,
}

impl<'schema> FilterSchemaBuilder<'schema> {
    fn new(file_schema: &'schema Schema, table_schema: &'schema Schema) -> Self {
        Self {
            filter_schema_fields: BTreeSet::new(),
            file_schema,
            table_schema,
        }
    }

    fn sort_fields(
        fields: &mut Vec<Arc<Field>>,
        table_schema: &Schema,
        file_schema: &Schema,
    ) {
        fields.sort_by_key(|f| f.name().to_string());
        fields.dedup_by_key(|f| f.name().to_string());
        fields.sort_by_key(|f| {
            let table_schema_index =
                table_schema.index_of(f.name()).unwrap_or(usize::MAX);
            let file_schema_index = file_schema.index_of(f.name()).unwrap_or(usize::MAX);
            (table_schema_index, file_schema_index)
        });
    }

    fn build(self) -> SchemaRef {
        let mut fields = self.filter_schema_fields.into_iter().collect::<Vec<_>>();
        FilterSchemaBuilder::sort_fields(
            &mut fields,
            self.table_schema,
            self.file_schema,
        );
        Arc::new(Schema::new(fields))
    }
}

impl<'node> TreeNodeVisitor<'node> for FilterSchemaBuilder<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        node: &'node Arc<dyn PhysicalExpr>,
    ) -> Result<TreeNodeRecursion> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Ok(field) = self.table_schema.field_with_name(column.name()) {
                self.filter_schema_fields.insert(Arc::new(field.clone()));
            } else if let Ok(field) = self.file_schema.field_with_name(column.name()) {
                self.filter_schema_fields.insert(Arc::new(field.clone()));
            } else {
                // valid fields are the table schema's fields + the file schema's fields, preferring the table schema's fields when there is a conflict
                let mut valid_fields = self
                    .table_schema
                    .fields()
                    .iter()
                    .chain(self.file_schema.fields().iter())
                    .cloned()
                    .collect::<Vec<_>>();
                FilterSchemaBuilder::sort_fields(
                    &mut valid_fields,
                    self.table_schema,
                    self.file_schema,
                );
                let valid_fields = valid_fields
                    .into_iter()
                    .map(|f| datafusion_common::Column::new_unqualified(f.name()))
                    .collect();
                let field = datafusion_common::Column::new_unqualified(column.name());
                return Err(datafusion_common::DataFusionError::SchemaError(
                    SchemaError::FieldNotFound {
                        field: Box::new(field),
                        valid_fields,
                    },
                    Box::new(None),
                ));
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}
