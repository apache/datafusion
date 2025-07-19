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
    apply_file_schema_type_coercions, coerce_int96_to_resolution, row_filter,
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion_datasource::file_meta::FileMeta;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;

use arrow::datatypes::{FieldRef, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use datafusion_common::encryption::FileDecryptionProperties;

use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::schema_rewriter::PhysicalExprAdapterFactory;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr_common::physical_expr::{
    is_dynamic_physical_expr, PhysicalExpr,
};
use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion_pruning::{build_pruning_predicate, FilePruner, PruningPredicate};

use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
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
    pub file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    /// Rewrite expressions in the context of the file schema
    pub(crate) expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta, file: PartitionedFile) -> Result<FileOpenFuture> {
        let file_range = file_meta.range.clone();
        let extensions = file_meta.extensions.clone();
        let file_name = file_meta.location().to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);

        let metadata_size_hint = file_meta.metadata_size_hint.or(self.metadata_size_hint);

        let mut async_file_reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                file_meta.clone(),
                metadata_size_hint,
                &self.metrics,
            )?;

        let batch_size = self.batch_size;

        let projected_schema =
            SchemaRef::from(self.logical_file_schema.project(&self.projection)?);
        let schema_adapter_factory = Arc::clone(&self.schema_adapter_factory);
        let schema_adapter = self
            .schema_adapter_factory
            .create(projected_schema, Arc::clone(&self.logical_file_schema));
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

        let mut enable_page_index = self.enable_page_index;
        let file_decryption_properties = self.file_decryption_properties.clone();

        // For now, page index does not work with encrypted files. See:
        // https://github.com/apache/arrow-rs/issues/7629
        if file_decryption_properties.is_some() {
            enable_page_index = false;
        }

        Ok(Box::pin(async move {
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
                        (is_dynamic_physical_expr(p) | file.has_statistics()).then_some(
                            FilePruner::new(
                                Arc::clone(p),
                                &logical_file_schema,
                                partition_fields.clone(),
                                file.clone(),
                                predicate_creation_errors.clone(),
                            )?,
                        ),
                    )
                })
                .transpose()?
                .flatten();

            if let Some(file_pruner) = &mut file_pruner {
                if file_pruner.should_prune()? {
                    // Return an empty stream immediately to skip the work of setting up the actual stream
                    file_metrics.files_ranges_pruned_statistics.add(1);
                    return Ok(futures::stream::empty().boxed());
                }
            }

            // Don't load the page index yet. Since it is not stored inline in
            // the footer, loading the page index if it is not needed will do
            // unecessary I/O. We decide later if it is needed to evaluate the
            // pruning predicates. Thus default to not requesting if from the
            // underlying reader.
            let mut options = ArrowReaderOptions::new().with_page_index(false);
            #[cfg(feature = "parquet_encryption")]
            if let Some(fd_val) = file_decryption_properties {
                options = options.with_file_decryption_properties((*fd_val).clone());
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

            if coerce_int96.is_some() {
                if let Some(merged) = coerce_int96_to_resolution(
                    reader_metadata.parquet_schema(),
                    &physical_file_schema,
                    &(coerce_int96.unwrap()),
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
                            .zip(file.partition_values)
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
        let mut reader =
            ParquetMetaDataReader::new_with_metadata(m).with_page_indexes(true);
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

    use arrow::{
        compute::cast,
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use bytes::{BufMut, BytesMut};
    use chrono::Utc;
    use datafusion_common::{
        assert_batches_eq, record_batch, stats::Precision, ColumnStatistics, ScalarValue,
        Statistics,
    };
    use datafusion_datasource::{
        file_meta::FileMeta,
        file_stream::FileOpener,
        schema_adapter::{
            DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory,
            SchemaMapper,
        },
        PartitionedFile,
    };
    use datafusion_expr::{col, lit};
    use datafusion_physical_expr::{
        expressions::DynamicFilterPhysicalExpr, planner::logical2physical,
        schema_rewriter::DefaultPhysicalExprAdapterFactory, PhysicalExpr,
    };
    use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
    use futures::{Stream, StreamExt};
    use object_store::{memory::InMemory, path::Path, ObjectMeta, ObjectStore};
    use parquet::arrow::ArrowWriter;

    use crate::{opener::ParquetOpener, DefaultParquetFileReaderFactory};

    async fn count_batches_and_rows(
        mut stream: std::pin::Pin<
            Box<
                dyn Stream<
                        Item = Result<
                            arrow::array::RecordBatch,
                            arrow::error::ArrowError,
                        >,
                    > + Send,
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

    async fn collect_batches(
        mut stream: std::pin::Pin<
            Box<
                dyn Stream<
                        Item = Result<
                            arrow::array::RecordBatch,
                            arrow::error::ArrowError,
                        >,
                    > + Send,
            >,
        >,
    ) -> Vec<arrow::array::RecordBatch> {
        let mut batches = vec![];
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }
        batches
    }

    async fn write_parquet(
        store: Arc<dyn ObjectStore>,
        filename: &str,
        batch: arrow::record_batch::RecordBatch,
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
            "file.parquet".to_string(),
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
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
            }
        };

        let make_meta = || FileMeta {
            object_meta: ObjectMeta {
                location: Path::from("test.parquet"),
                last_modified: Utc::now(),
                size: u64::try_from(data_size).unwrap(),
                e_tag: None,
                version: None,
            },
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        // A filter on "a" should not exclude any rows even if it matches the data
        let expr = col("a").eq(lit(1));
        let predicate = logical2physical(&expr, &schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // A filter on `b = 5.0` should exclude all rows
        let expr = col("b").eq(lit(ScalarValue::Float32(Some(5.0))));
        let predicate = logical2physical(&expr, &schema);
        let opener = make_opener(predicate);
        let stream = opener.open(make_meta(), file).unwrap().await.unwrap();
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
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
            }
        };

        let make_meta = || FileMeta {
            object_meta: ObjectMeta {
                location: Path::from("part=1/file.parquet"),
                last_modified: Utc::now(),
                size: u64::try_from(data_size).unwrap(),
                e_tag: None,
                version: None,
            },
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        // Filter should match the partition value
        let expr = col("part").eq(lit(1));
        // Mark the expression as dynamic even if it's not to force partition pruning to happen
        // Otherwise we assume it already happened at the planning stage and won't re-do the work here
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value
        let expr = col("part").eq(lit(2));
        // Mark the expression as dynamic even if it's not to force partition pruning to happen
        // Otherwise we assume it already happened at the planning stage and won't re-do the work here
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(make_meta(), file).unwrap().await.unwrap();
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
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
            }
        };
        let make_meta = || FileMeta {
            object_meta: ObjectMeta {
                location: Path::from("part=1/file.parquet"),
                last_modified: Utc::now(),
                size: u64::try_from(data_size).unwrap(),
                e_tag: None,
                version: None,
            },
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        // Filter should match the partition value and file statistics
        let expr = col("part").eq(lit(1)).and(col("b").eq(lit(1.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Should prune based on partition value but not file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(1.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on file statistics but not partition value
        let expr = col("part").eq(lit(1)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on both partition value and file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(make_meta(), file).unwrap().await.unwrap();
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
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
            }
        };

        let make_meta = || FileMeta {
            object_meta: ObjectMeta {
                location: Path::from("part=1/file.parquet"),
                last_modified: Utc::now(),
                size: u64::try_from(data_size).unwrap(),
                e_tag: None,
                version: None,
            },
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        // Filter should match the partition value and data value
        let expr = col("part").eq(lit(1)).or(col("a").eq(lit(1)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should match the partition value but not the data value
        let expr = col("part").eq(lit(1)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value but match the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(1)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 1);

        // Filter should not match the partition value or the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(make_meta(), file).unwrap().await.unwrap();
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
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
            }
        };

        let make_meta = || FileMeta {
            object_meta: ObjectMeta {
                location: Path::from("part=1/file.parquet"),
                last_modified: Utc::now(),
                size: u64::try_from(data_size).unwrap(),
                e_tag: None,
                version: None,
            },
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        // Filter should NOT match the stats but the file is never attempted to be pruned because the filters are not dynamic
        let expr = col("part").eq(lit(2));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // If we make the filter dynamic, it should prune
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener
            .open(make_meta(), file.clone())
            .unwrap()
            .await
            .unwrap();
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
                batch: arrow::array::RecordBatch,
            ) -> datafusion_common::Result<arrow::array::RecordBatch> {
                let a_column = cast(batch.column(0), &DataType::UInt64)?;
                // Add in a new column "b" with default value 0.0
                let b_column =
                    arrow::array::Float64Array::from(vec![Some(0.0); batch.num_rows()]);
                let columns = vec![a_column, Arc::new(b_column)];
                let new_schema = Arc::new(Schema::new(vec![
                    Field::new("a", DataType::UInt64, false),
                    Field::new("b", DataType::Float64, false),
                ]));
                Ok(arrow::record_batch::RecordBatch::try_new(
                    new_schema, columns,
                )?)
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

        // Test that if no expression rewriter is provided we use a schemaadapter to adapt the data to the expresssion
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

        let file_meta = FileMeta {
            object_meta: ObjectMeta {
                location: Path::from("test.parquet"),
                last_modified: Utc::now(),
                size: u64::try_from(data_size).unwrap(),
                e_tag: None,
                version: None,
            },
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

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
            file_decryption_properties: None,
            expr_adapter_factory: None,
        };

        let predicate = logical2physical(&col("a").eq(lit(1u64)), &table_schema);
        let opener = make_opener(predicate);
        let stream = opener
            .open(file_meta.clone(), file.clone())
            .unwrap()
            .await
            .unwrap();
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
}
