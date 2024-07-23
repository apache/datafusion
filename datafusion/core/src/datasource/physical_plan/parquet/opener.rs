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

use std::cmp::min;
use std::collections::HashMap;
use crate::datasource::physical_plan::parquet::page_filter::PagePruningAccessPlanFilter;
use crate::datasource::physical_plan::parquet::row_group_filter::RowGroupAccessPlanFilter;
use crate::datasource::physical_plan::parquet::{
    row_filter, should_enable_page_index, ParquetAccessPlan,
};
use crate::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, ParquetFileMetrics, ParquetFileReaderFactory,
};
use crate::datasource::schema_adapter::SchemaAdapterFactory;
use crate::physical_optimizer::pruning::PruningPredicate;
use arrow_schema::{ArrowError, SchemaRef};
use datafusion_common::{exec_err, Result};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::{StreamExt, TryStreamExt};
use log::{debug, info, trace};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use std::sync::Arc;
use parquet::schema::types::SchemaDescriptor;
// use datafusion_common::DataFusionError;
use datafusion_common::deep::{has_deep_projection, rewrite_schema, splat_columns};
use crate::datasource::file_format::transform_schema_to_view;

/// Implements [`FileOpener`] for a parquet file
pub(super) struct ParquetOpener {
    pub partition_index: usize,
    pub projection: Arc<[usize]>,
    pub projection_deep: Arc<HashMap<usize, Vec<String>>>,
    pub batch_size: usize,
    pub limit: Option<usize>,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    pub pruning_predicate: Option<Arc<PruningPredicate>>,
    pub page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
    pub table_schema: SchemaRef,
    pub metadata_size_hint: Option<usize>,
    pub metrics: ExecutionPlanMetricsSet,
    pub parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    pub pushdown_filters: bool,
    pub reorder_filters: bool,
    pub enable_page_index: bool,
    pub enable_bloom_filter: bool,
    pub schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    pub schema_force_string_view: bool,
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion_common::Result<FileOpenFuture> {
        let file_range = file_meta.range.clone();
        let extensions = file_meta.extensions.clone();
        let file_name = file_meta.location().to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);

        let mut reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                file_meta,
                self.metadata_size_hint,
                &self.metrics,
            )?;

        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let projection_vec = projection.as_ref().iter().map(|i| *i).collect::<Vec<usize>>();
        debug!("ParquetOpener::open projection={:?}", projection);
        // FIXME @HStack: ADR: why do we need to do this ? our function needs another param maybe ?
        // In the case when the projections requested are empty, we should return an empty schema
        let projected_schema = if projection_vec.len() == 0 {
            SchemaRef::from(self.table_schema.project(&projection)?)
        } else {
            rewrite_schema(
                self.table_schema.clone(),
                &projection_vec,
                self.projection_deep.as_ref()
            )
        };
        let projection_deep = self.projection_deep.clone();
        let schema_adapter = self.schema_adapter_factory.create(projected_schema);
        let predicate = self.predicate.clone();
        let pruning_predicate = self.pruning_predicate.clone();
        let page_pruning_predicate = self.page_pruning_predicate.clone();
        let table_schema = self.table_schema.clone();
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let enable_page_index = should_enable_page_index(
            self.enable_page_index,
            &self.page_pruning_predicate,
        );
        let enable_bloom_filter = self.enable_bloom_filter;
        let limit = self.limit;
        let schema_force_string_view = self.schema_force_string_view;


        Ok(Box::pin(async move {
            let options = ArrowReaderOptions::new().with_page_index(enable_page_index);

            let metadata =
                ArrowReaderMetadata::load_async(&mut reader, options.clone()).await?;
            let mut schema = metadata.schema().clone();

            if schema_force_string_view {
                schema = Arc::new(transform_schema_to_view(&schema));
            }

            let options = ArrowReaderOptions::new()
                .with_page_index(enable_page_index)
                .with_schema(schema.clone());
            let metadata =
                ArrowReaderMetadata::try_new(metadata.metadata().clone(), options)?;

            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata);

            let file_schema = builder.schema().clone();

            let (schema_mapping, adapted_projections) =
                schema_adapter.map_schema(&file_schema)?;

            // let mask = ProjectionMask::roots(
            //     builder.parquet_schema(),
            //     adapted_projections.iter().cloned(),
            // );
            let mask = if has_deep_projection(Some(projection_deep.clone().as_ref())) {
                let leaves = generate_leaf_paths(
                    table_schema.clone(),
                    builder.parquet_schema(),
                    &projection_vec,
                    projection_deep.clone().as_ref()
                );
                info!("ParquetOpener::open, using deep projection parquet leaves: {:?}", leaves.clone());
                // let tmp = builder.parquet_schema();
                // for (i, col) in tmp.columns().iter().enumerate() {
                //     info!("  {}  {}= {:?}", i, col.path(), col);
                // }
                ProjectionMask::leaves(
                    builder.parquet_schema(),
                    leaves,
                )
            } else {
                ProjectionMask::roots(
                    builder.parquet_schema(),
                    adapted_projections.iter().cloned(),
                )
            };
            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    &predicate,
                    &file_schema,
                    &table_schema,
                    builder.metadata(),
                    reorder_predicates,
                    &file_metrics,
                    Arc::clone(&schema_mapping),
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
            let file_metadata = builder.metadata().clone();
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
            debug!("ParquetExec Ignoring unknown extension specified for {file_name}");
        }
    }

    // default to scanning all row groups
    Ok(ParquetAccessPlan::new_all(row_group_count))
}

// FIXME: @HStack ACTUALLY look at the arrow schema and handle map types correctly
//  Right now, we are matching "map-like" parquet leaves like "key_value.key" etc
//  But, we neeed to walk through both the arrow schema (which KNOWS about the map type)
//  and the parquet leaves to do this correctly.
fn equivalent_projection_paths_from_parquet_schema(
    arrow_schema: SchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> Vec<(usize, (String, String))> {
    let mut output: Vec<(usize, (String, String))> = vec![];
    for (i, col) in parquet_schema.columns().iter().enumerate() {
        let original_path = col.path().string();
        let converted_path = convert_parquet_path_to_deep_projection_path(&original_path.as_str());
        output.push((i, (original_path.clone(), converted_path)));
    }
    output
}

fn convert_parquet_path_to_deep_projection_path(parquet_path: &str) -> String {
    if parquet_path.contains(".key_value.key") ||
        parquet_path.contains(".key_value.value") ||
        parquet_path.contains(".entries.keys") ||
        parquet_path.contains(".entries.values") ||
        parquet_path.contains(".list.element") {
        let tmp = parquet_path
            .replace("key_value.key", "*")
            .replace("key_value.value", "*")
            .replace("entries.keys", "*")
            .replace("entries.values", "*")
            .replace("list.element", "*");
        tmp
    } else {
        parquet_path.to_string()
    }
}

fn generate_leaf_paths(
    arrow_schema: SchemaRef,
    parquet_schema: &SchemaDescriptor,
    projection: &Vec<usize>,
    projection_deep: &HashMap<usize, Vec<String>>,
) -> Vec<usize> {
    let actual_projection = if projection.len() == 0 {
        (0..arrow_schema.fields().len()).collect()
    } else {
        projection.clone()
    };
    let splatted =
        splat_columns(arrow_schema.clone(), &actual_projection, &projection_deep);
    trace!(target: "deep", "generate_leaf_paths: splatted: {:?}", &splatted);

    let mut out: Vec<usize> = vec![];
    for (i, (original, converted)) in equivalent_projection_paths_from_parquet_schema(arrow_schema, parquet_schema) {
        // FIXME: @HStack
        //  for map fields, the actual parquet paths look like x.y.z.key_value.key, x.y.z.key_value.value
        //  since we are ignoring these names in the paths, we need to actually collapse this access to a *
        //  so we can filter for them
        //  also, we need BOTH the key and the value for maps otherwise we run into an arrow-rs error
        //  "partial projection of MapArray is not supported"

        trace!(target: "deep", "  generate_leaf_paths looking at index {} {} =  {}", i, &original, &converted);

        let mut found = false;
        for filter in splatted.iter() {
            // check if this filter matches this leaf path
            let filter_pieces = filter.split(".").collect::<Vec<&str>>();
            // let col_pieces = col_path.parts();
            let col_pieces = converted.split(".").collect::<Vec<_>>();
            // let's check
            let mut filter_found = true;
            for i in 0..min(filter_pieces.len(), col_pieces.len()) {
                if i >= filter_pieces.len() {
                    //  we are at the end of the filter, and we matched until now, so we break, we match !
                    break;
                }
                if i >= col_pieces.len() {
                    // we have a longer filter, we matched until now, we match !
                    break;
                }
                // we can actually check
                if !(col_pieces[i] == filter_pieces[i] || filter_pieces[i] == "*") {
                    filter_found = false;
                    break;
                }
            }
            if filter_found {
                found = true;
                break;
            }
        }
        if found {
            out.push(i);
        }
    }
    out
}
