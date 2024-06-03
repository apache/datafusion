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

use crate::datasource::physical_plan::parquet::page_filter::PagePruningPredicate;
use crate::datasource::physical_plan::parquet::row_groups::RowGroupSet;
use crate::datasource::physical_plan::parquet::{row_filter, should_enable_page_index};
use crate::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, ParquetFileMetrics, ParquetFileReaderFactory,
};
use crate::datasource::schema_adapter::SchemaAdapterFactory;
use crate::physical_optimizer::pruning::PruningPredicate;
use arrow_schema::{ArrowError, SchemaRef};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use std::sync::Arc;

/// Implements [`FileOpener`] for a parquet file
pub(super) struct ParquetOpener {
    pub partition_index: usize,
    pub projection: Arc<[usize]>,
    pub batch_size: usize,
    pub limit: Option<usize>,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    pub pruning_predicate: Option<Arc<PruningPredicate>>,
    pub page_pruning_predicate: Option<Arc<PagePruningPredicate>>,
    pub table_schema: SchemaRef,
    pub metadata_size_hint: Option<usize>,
    pub metrics: ExecutionPlanMetricsSet,
    pub parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    pub pushdown_filters: bool,
    pub reorder_filters: bool,
    pub enable_page_index: bool,
    pub enable_bloom_filter: bool,
    pub schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion_common::Result<FileOpenFuture> {
        let file_range = file_meta.range.clone();
        let file_metrics = ParquetFileMetrics::new(
            self.partition_index,
            file_meta.location().as_ref(),
            &self.metrics,
        );

        let reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                file_meta,
                self.metadata_size_hint,
                &self.metrics,
            )?;

        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let projected_schema = SchemaRef::from(self.table_schema.project(&projection)?);
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

        Ok(Box::pin(async move {
            let options = ArrowReaderOptions::new().with_page_index(enable_page_index);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                    .await?;

            let file_schema = builder.schema().clone();

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
            let mut row_groups = RowGroupSet::new(rg_metadata.len());
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

            // page index pruning: if all data on individual pages can
            // be ruled using page metadata, rows from other columns
            // with that range can be skipped as well
            if enable_page_index && !row_groups.is_empty() {
                if let Some(p) = page_pruning_predicate {
                    let pruned = p.prune(
                        &file_schema,
                        builder.parquet_schema(),
                        &row_groups,
                        file_metadata.as_ref(),
                        &file_metrics,
                    )?;
                    if let Some(row_selection) = pruned {
                        builder = builder.with_row_selection(row_selection);
                    }
                }
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_groups.indexes())
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
