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

//! [`DFParquetMetadata`] for fetching Parquet file metadata, statistics
//! and schema information.

use crate::{
    ObjectStoreFetch, apply_file_schema_type_coercions, coerce_int96_to_resolution,
};
use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::SortOptions;
use arrow::compute::and;
use arrow::compute::kernels::cmp::eq;
use arrow::compute::sum;
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_common::stats::Precision;
use datafusion_common::{
    ColumnStatistics, DataFusionError, Result, ScalarValue, Statistics,
};
use datafusion_execution::cache::cache_manager::{FileMetadata, FileMetadataCache};
use datafusion_functions_aggregate_common::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::Accumulator;
use log::debug;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::{parquet_column, parquet_to_arrow_schema};
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData,
    SortingColumn,
};
use parquet::schema::types::SchemaDescriptor;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Handles fetching Parquet file schema, metadata and statistics
/// from object store.
///
/// This component is exposed for low level integrations through
/// [`ParquetFileReaderFactory`].
///
/// [`ParquetFileReaderFactory`]: crate::ParquetFileReaderFactory
#[derive(Debug)]
pub struct DFParquetMetadata<'a> {
    store: &'a dyn ObjectStore,
    object_meta: &'a ObjectMeta,
    metadata_size_hint: Option<usize>,
    decryption_properties: Option<Arc<FileDecryptionProperties>>,
    file_metadata_cache: Option<Arc<dyn FileMetadataCache>>,
    /// timeunit to coerce INT96 timestamps to
    pub coerce_int96: Option<TimeUnit>,
}

impl<'a> DFParquetMetadata<'a> {
    pub fn new(store: &'a dyn ObjectStore, object_meta: &'a ObjectMeta) -> Self {
        Self {
            store,
            object_meta,
            metadata_size_hint: None,
            decryption_properties: None,
            file_metadata_cache: None,
            coerce_int96: None,
        }
    }

    /// set metadata size hint
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: Option<usize>) -> Self {
        self.metadata_size_hint = metadata_size_hint;
        self
    }

    /// set decryption properties
    pub fn with_decryption_properties(
        mut self,
        decryption_properties: Option<Arc<FileDecryptionProperties>>,
    ) -> Self {
        self.decryption_properties = decryption_properties;
        self
    }

    /// set file metadata cache
    pub fn with_file_metadata_cache(
        mut self,
        file_metadata_cache: Option<Arc<dyn FileMetadataCache>>,
    ) -> Self {
        self.file_metadata_cache = file_metadata_cache;
        self
    }

    /// Set timeunit to coerce INT96 timestamps to
    pub fn with_coerce_int96(mut self, time_unit: Option<TimeUnit>) -> Self {
        self.coerce_int96 = time_unit;
        self
    }

    /// Fetch parquet metadata from the remote object store
    pub async fn fetch_metadata(&self) -> Result<Arc<ParquetMetaData>> {
        let Self {
            store,
            object_meta,
            metadata_size_hint,
            decryption_properties,
            file_metadata_cache,
            coerce_int96: _,
        } = self;

        let fetch = ObjectStoreFetch::new(*store, object_meta);

        // implementation to fetch parquet metadata
        let cache_metadata =
            !cfg!(feature = "parquet_encryption") || decryption_properties.is_none();

        if cache_metadata
            && let Some(parquet_metadata) = file_metadata_cache
                .as_ref()
                .and_then(|file_metadata_cache| file_metadata_cache.get(object_meta))
                .and_then(|file_metadata| {
                    file_metadata
                        .as_any()
                        .downcast_ref::<CachedParquetMetaData>()
                        .map(|cached_parquet_metadata| {
                            Arc::clone(cached_parquet_metadata.parquet_metadata())
                        })
                })
        {
            return Ok(parquet_metadata);
        }

        let mut reader =
            ParquetMetaDataReader::new().with_prefetch_hint(*metadata_size_hint);

        #[cfg(feature = "parquet_encryption")]
        if let Some(decryption_properties) = decryption_properties {
            reader = reader
                .with_decryption_properties(Some(Arc::clone(decryption_properties)));
        }

        if cache_metadata && file_metadata_cache.is_some() {
            // Need to retrieve the entire metadata for the caching to be effective.
            reader = reader.with_page_index_policy(PageIndexPolicy::Optional);
        }

        let metadata = Arc::new(
            reader
                .load_and_finish(fetch, object_meta.size)
                .await
                .map_err(DataFusionError::from)?,
        );

        if cache_metadata && let Some(file_metadata_cache) = file_metadata_cache {
            file_metadata_cache.put(
                object_meta,
                Arc::new(CachedParquetMetaData::new(Arc::clone(&metadata))),
            );
        }

        Ok(metadata)
    }

    /// Read and parse the schema of the Parquet file
    pub async fn fetch_schema(&self) -> Result<Schema> {
        let metadata = self.fetch_metadata().await?;

        let file_metadata = metadata.file_metadata();
        let schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )?;
        let schema = self
            .coerce_int96
            .as_ref()
            .and_then(|time_unit| {
                coerce_int96_to_resolution(
                    file_metadata.schema_descr(),
                    &schema,
                    time_unit,
                )
            })
            .unwrap_or(schema);
        Ok(schema)
    }

    /// Return (path, schema) tuple by fetching the schema from Parquet file
    pub(crate) async fn fetch_schema_with_location(&self) -> Result<(Path, Schema)> {
        let loc_path = self.object_meta.location.clone();
        let schema = self.fetch_schema().await?;
        Ok((loc_path, schema))
    }

    /// Fetch the metadata from the Parquet file via [`Self::fetch_metadata`] and convert
    /// the statistics in the metadata using [`Self::statistics_from_parquet_metadata`]
    pub async fn fetch_statistics(&self, table_schema: &SchemaRef) -> Result<Statistics> {
        let metadata = self.fetch_metadata().await?;
        Self::statistics_from_parquet_metadata(&metadata, table_schema)
    }

    /// Fetch both statistics and ordering from Parquet metadata in a single metadata fetch.
    ///
    /// This is more efficient than calling [`Self::fetch_statistics`] and then separately
    /// extracting ordering, as it only fetches the metadata once.
    ///
    /// # Returns
    /// A tuple of (Statistics, Option<LexOrdering>) where the ordering is `None` if:
    /// - No row groups have sorting_columns
    /// - Row groups have inconsistent sorting_columns
    /// - Sorting columns cannot be mapped to the Arrow schema
    pub async fn fetch_statistics_and_ordering(
        &self,
        table_schema: &SchemaRef,
    ) -> Result<(Statistics, Option<LexOrdering>)> {
        let metadata = self.fetch_metadata().await?;
        let statistics = Self::statistics_from_parquet_metadata(&metadata, table_schema)?;
        let ordering = Self::ordering_from_parquet_metadata(&metadata, table_schema)?;
        Ok((statistics, ordering))
    }

    /// Convert statistics in [`ParquetMetaData`] into [`Statistics`] using [`StatisticsConverter`]
    ///
    /// The statistics are calculated for each column in the table schema
    /// using the row group statistics in the parquet metadata.
    ///
    /// # Key behaviors:
    ///
    /// 1. Extracts row counts and byte sizes from all row groups
    /// 2. Applies schema type coercions to align file schema with table schema
    /// 3. Collects and aggregates statistics across row groups when available
    ///
    /// # When there are no statistics:
    ///
    /// If the Parquet file doesn't contain any statistics (has_statistics is false), the function returns a Statistics object with:
    /// - Exact row count
    /// - Exact byte size
    /// - All column statistics marked as unknown via Statistics::unknown_column(&table_schema)
    /// - Column byte sizes are still calculated and recorded
    ///
    /// # When only some columns have statistics:
    ///
    /// For columns with statistics:
    /// - Min/max values are properly extracted and represented as Precision::Exact
    /// - Null counts are calculated by summing across row groups
    /// - Byte sizes are calculated and recorded
    ///
    /// For columns without statistics,
    /// - For min/max, there are two situations:
    ///     1. The column isn't in arrow schema, then min/max values are set to Precision::Absent
    ///     2. The column is in arrow schema, but not in parquet schema due to schema revolution, min/max values are set to Precision::Exact(null)
    /// - Null counts are set to Precision::Exact(num_rows) (conservatively assuming all values could be null)
    ///
    /// # Byte Size Calculation:
    ///
    /// - For primitive types with known fixed size, exact byte size is calculated as (byte width * number of rows)
    /// - For other types, uncompressed Parquet size is used as an estimate for in-memory size
    /// - If neither method is applicable, byte size is marked as Precision::Absent
    pub fn statistics_from_parquet_metadata(
        metadata: &ParquetMetaData,
        logical_file_schema: &SchemaRef,
    ) -> Result<Statistics> {
        let row_groups_metadata = metadata.row_groups();

        // Use Statistics::default() as opposed to Statistics::new_unknown()
        // because we are going to replace the column statistics below
        // and we don't want to initialize them twice.
        let mut statistics = Statistics::default();
        let mut has_statistics = false;
        let mut num_rows = 0_usize;
        for row_group_meta in row_groups_metadata {
            num_rows += row_group_meta.num_rows() as usize;

            if !has_statistics {
                has_statistics = row_group_meta
                    .columns()
                    .iter()
                    .any(|column| column.statistics().is_some());
            }
        }
        statistics.num_rows = Precision::Exact(num_rows);

        let file_metadata = metadata.file_metadata();
        let mut physical_file_schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )?;

        if let Some(merged) =
            apply_file_schema_type_coercions(logical_file_schema, &physical_file_schema)
        {
            physical_file_schema = merged;
        }

        statistics.column_statistics =
            if has_statistics {
                let (mut max_accs, mut min_accs) =
                    create_max_min_accs(logical_file_schema);
                let mut null_counts_array =
                    vec![Precision::Absent; logical_file_schema.fields().len()];
                let mut column_byte_sizes =
                    vec![Precision::Absent; logical_file_schema.fields().len()];
                let mut is_max_value_exact =
                    vec![Some(true); logical_file_schema.fields().len()];
                let mut is_min_value_exact =
                    vec![Some(true); logical_file_schema.fields().len()];
                logical_file_schema.fields().iter().enumerate().for_each(
                    |(idx, field)| match StatisticsConverter::try_new(
                        field.name(),
                        &physical_file_schema,
                        file_metadata.schema_descr(),
                    ) {
                        Ok(stats_converter) => {
                            let mut accumulators = StatisticsAccumulators {
                                min_accs: &mut min_accs,
                                max_accs: &mut max_accs,
                                null_counts_array: &mut null_counts_array,
                                is_min_value_exact: &mut is_min_value_exact,
                                is_max_value_exact: &mut is_max_value_exact,
                                column_byte_sizes: &mut column_byte_sizes,
                            };
                            summarize_min_max_null_counts(
                                file_metadata.schema_descr(),
                                logical_file_schema,
                                &physical_file_schema,
                                &mut accumulators,
                                idx,
                                &stats_converter,
                                row_groups_metadata,
                            )
                            .ok();
                        }
                        Err(e) => {
                            debug!("Failed to create statistics converter: {e}");
                            null_counts_array[idx] = Precision::Exact(num_rows);
                        }
                    },
                );

                get_col_stats(
                    logical_file_schema,
                    &null_counts_array,
                    &mut max_accs,
                    &mut min_accs,
                    &mut is_max_value_exact,
                    &mut is_min_value_exact,
                    &column_byte_sizes,
                )
            } else {
                // Record column sizes
                logical_file_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(logical_file_schema_index, field)| {
                        let arrow_field =
                            logical_file_schema.field(logical_file_schema_index);
                        let parquet_idx = parquet_column(
                            file_metadata.schema_descr(),
                            &physical_file_schema,
                            arrow_field.name(),
                        )
                        .map(|(idx, _)| idx);
                        let byte_size = compute_arrow_column_size(
                            field.data_type(),
                            row_groups_metadata,
                            parquet_idx,
                            num_rows,
                        );
                        ColumnStatistics::new_unknown().with_byte_size(byte_size)
                    })
                    .collect()
            };

        #[cfg(debug_assertions)]
        {
            // Check that the column statistics length matches the table schema fields length
            assert_eq!(
                statistics.column_statistics.len(),
                logical_file_schema.fields().len(),
                "Column statistics length does not match table schema fields length"
            );
        }

        Ok(statistics)
    }

    /// Extract LexOrdering from Parquet sorting_columns metadata.
    ///
    /// Returns `Ok(None)` if:
    /// - No row groups exist
    /// - No row group has sorting_columns
    /// - Row groups have inconsistent sorting_columns
    /// - A sorting column cannot be mapped to the Arrow schema
    ///
    /// # Arguments
    /// * `metadata` - The Parquet file metadata
    /// * `arrow_schema` - The Arrow schema to map column indices to
    pub(crate) fn ordering_from_parquet_metadata(
        metadata: &ParquetMetaData,
        arrow_schema: &SchemaRef,
    ) -> Result<Option<LexOrdering>> {
        let row_groups = metadata.row_groups();
        if row_groups.is_empty() {
            return Ok(None);
        }

        // Get sorting_columns from first row group
        let first_sorting = match row_groups[0].sorting_columns() {
            Some(cols) if !cols.is_empty() => cols,
            _ => return Ok(None),
        };

        // Verify all row groups have identical sorting_columns
        for rg in &row_groups[1..] {
            match rg.sorting_columns() {
                Some(cols) if cols == first_sorting => {}
                _ => {
                    debug!(
                        "Row groups have inconsistent sorting_columns, treating as unordered"
                    );
                    return Ok(None);
                }
            }
        }

        // Get the Parquet schema descriptor for column name lookup
        let file_metadata = metadata.file_metadata();
        let parquet_schema = file_metadata.schema_descr();

        // Convert Parquet schema to Arrow schema for column mapping
        let parquet_arrow_schema =
            parquet_to_arrow_schema(parquet_schema, file_metadata.key_value_metadata())?;

        // Convert each SortingColumn to PhysicalSortExpr
        let sort_exprs: Vec<PhysicalSortExpr> = first_sorting
            .iter()
            .filter_map(|sorting_col| {
                sorting_column_to_sort_expr(
                    sorting_col,
                    parquet_schema,
                    &parquet_arrow_schema,
                    arrow_schema,
                )
                .ok()
                .flatten()
            })
            .collect();

        // If we couldn't map any columns, return None
        if sort_exprs.is_empty() {
            return Ok(None);
        }

        // If we couldn't map all columns, the ordering is incomplete
        // Only return the ordering if we mapped all sorting columns
        if sort_exprs.len() != first_sorting.len() {
            debug!(
                "Could only map {}/{} sorting columns to Arrow schema",
                sort_exprs.len(),
                first_sorting.len()
            );
            return Ok(None);
        }

        Ok(LexOrdering::new(sort_exprs))
    }
}

/// Convert a Parquet SortingColumn to a PhysicalSortExpr.
///
/// Returns `Ok(None)` if the column cannot be mapped to the Arrow schema.
fn sorting_column_to_sort_expr(
    sorting_col: &SortingColumn,
    parquet_schema: &SchemaDescriptor,
    parquet_arrow_schema: &Schema,
    arrow_schema: &SchemaRef,
) -> Result<Option<PhysicalSortExpr>> {
    let column_idx = sorting_col.column_idx as usize;

    // Get the column path from the Parquet schema
    // The column_idx in SortingColumn refers to leaf columns
    if column_idx >= parquet_schema.num_columns() {
        debug!(
            "SortingColumn column_idx {} out of bounds (schema has {} columns)",
            column_idx,
            parquet_schema.num_columns()
        );
        return Ok(None);
    }

    let parquet_column_desc = parquet_schema.column(column_idx);
    let column_path = parquet_column_desc.path().string();

    // Find the corresponding field in the Parquet-derived Arrow schema
    let parquet_arrow_field_idx = parquet_arrow_schema
        .fields()
        .iter()
        .position(|f| f.name() == &column_path);

    let arrow_field_name = match parquet_arrow_field_idx {
        Some(idx) => parquet_arrow_schema.field(idx).name().clone(),
        None => {
            // For nested columns, the path might be different
            // Try to find by the last component of the path
            let last_component =
                column_path.split('.').next_back().unwrap_or(&column_path);
            if let Ok(field) = parquet_arrow_schema.field_with_name(last_component) {
                field.name().clone()
            } else {
                debug!(
                    "Could not find Arrow field for Parquet column path: {column_path}"
                );
                return Ok(None);
            }
        }
    };

    // Find the field index in the target Arrow schema
    let arrow_field_idx = match arrow_schema.index_of(&arrow_field_name) {
        Ok(idx) => idx,
        Err(_) => {
            debug!(
                "Column '{arrow_field_name}' from Parquet sorting_columns not found in Arrow schema"
            );
            return Ok(None);
        }
    };

    // Create the Column expression
    let column_expr = Arc::new(Column::new(&arrow_field_name, arrow_field_idx));

    // Create the sort options
    let sort_options = SortOptions {
        descending: sorting_col.descending,
        nulls_first: sorting_col.nulls_first,
    };

    Ok(Some(PhysicalSortExpr::new(column_expr, sort_options)))
}

/// Convert a PhysicalSortExpr to a Parquet SortingColumn.
///
/// Returns `Err` if the expression is not a simple column reference,
/// since Parquet's SortingColumn only supports column indices.
pub(crate) fn sort_expr_to_sorting_column(
    sort_expr: &PhysicalSortExpr,
) -> Result<SortingColumn> {
    let column = sort_expr
        .expr
        .as_any()
        .downcast_ref::<Column>()
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Parquet sorting_columns only supports simple column references, \
                 but got expression: {}",
                sort_expr.expr
            ))
        })?;

    Ok(SortingColumn {
        column_idx: column.index() as i32,
        descending: sort_expr.options.descending,
        nulls_first: sort_expr.options.nulls_first,
    })
}

/// Convert a LexOrdering to Vec<SortingColumn> for Parquet.
///
/// Returns `Err` if any expression is not a simple column reference.
pub(crate) fn lex_ordering_to_sorting_columns(
    ordering: &LexOrdering,
) -> Result<Vec<SortingColumn>> {
    ordering.iter().map(sort_expr_to_sorting_column).collect()
}

/// Min/max aggregation can take Dictionary encode input but always produces unpacked
/// (aka non Dictionary) output. We need to adjust the output data type to reflect this.
/// The reason min/max aggregate produces unpacked output because there is only one
/// min/max value per group; there is no needs to keep them Dictionary encoded
fn min_max_aggregate_data_type(input_type: &DataType) -> &DataType {
    if let DataType::Dictionary(_, value_type) = input_type {
        value_type.as_ref()
    } else {
        input_type
    }
}

fn create_max_min_accs(
    schema: &Schema,
) -> (Vec<Option<MaxAccumulator>>, Vec<Option<MinAccumulator>>) {
    let max_values: Vec<Option<MaxAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| {
            MaxAccumulator::try_new(min_max_aggregate_data_type(field.data_type())).ok()
        })
        .collect();
    let min_values: Vec<Option<MinAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| {
            MinAccumulator::try_new(min_max_aggregate_data_type(field.data_type())).ok()
        })
        .collect();
    (max_values, min_values)
}

fn get_col_stats(
    schema: &Schema,
    null_counts: &[Precision<usize>],
    max_values: &mut [Option<MaxAccumulator>],
    min_values: &mut [Option<MinAccumulator>],
    is_max_value_exact: &mut [Option<bool>],
    is_min_value_exact: &mut [Option<bool>],
    column_byte_sizes: &[Precision<usize>],
) -> Vec<ColumnStatistics> {
    (0..schema.fields().len())
        .map(|i| {
            let max_value = match (
                max_values.get_mut(i).unwrap(),
                is_max_value_exact.get(i).unwrap(),
            ) {
                (Some(max_value), Some(true)) => {
                    max_value.evaluate().ok().map(Precision::Exact)
                }
                (Some(max_value), Some(false)) | (Some(max_value), None) => {
                    max_value.evaluate().ok().map(Precision::Inexact)
                }
                (None, _) => None,
            };
            let min_value = match (
                min_values.get_mut(i).unwrap(),
                is_min_value_exact.get(i).unwrap(),
            ) {
                (Some(min_value), Some(true)) => {
                    min_value.evaluate().ok().map(Precision::Exact)
                }
                (Some(min_value), Some(false)) | (Some(min_value), None) => {
                    min_value.evaluate().ok().map(Precision::Inexact)
                }
                (None, _) => None,
            };
            ColumnStatistics {
                null_count: null_counts[i],
                max_value: max_value.unwrap_or(Precision::Absent),
                min_value: min_value.unwrap_or(Precision::Absent),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: column_byte_sizes[i],
            }
        })
        .collect()
}

/// Holds the accumulator state for collecting statistics from row groups
struct StatisticsAccumulators<'a> {
    min_accs: &'a mut [Option<MinAccumulator>],
    max_accs: &'a mut [Option<MaxAccumulator>],
    null_counts_array: &'a mut [Precision<usize>],
    is_min_value_exact: &'a mut [Option<bool>],
    is_max_value_exact: &'a mut [Option<bool>],
    column_byte_sizes: &'a mut [Precision<usize>],
}

fn summarize_min_max_null_counts(
    parquet_schema: &SchemaDescriptor,
    logical_file_schema: &Schema,
    physical_file_schema: &Schema,
    accumulators: &mut StatisticsAccumulators,
    logical_schema_index: usize,
    stats_converter: &StatisticsConverter,
    row_groups_metadata: &[RowGroupMetaData],
) -> Result<()> {
    let max_values = stats_converter.row_group_maxes(row_groups_metadata)?;
    let min_values = stats_converter.row_group_mins(row_groups_metadata)?;
    let null_counts = stats_converter.row_group_null_counts(row_groups_metadata)?;
    let is_max_value_exact_stat =
        stats_converter.row_group_is_max_value_exact(row_groups_metadata)?;
    let is_min_value_exact_stat =
        stats_converter.row_group_is_min_value_exact(row_groups_metadata)?;

    if let Some(max_acc) = &mut accumulators.max_accs[logical_schema_index] {
        max_acc.update_batch(&[Arc::clone(&max_values)])?;
        let mut cur_max_acc = max_acc.clone();
        accumulators.is_max_value_exact[logical_schema_index] = has_any_exact_match(
            &cur_max_acc.evaluate()?,
            &max_values,
            &is_max_value_exact_stat,
        );
    }

    if let Some(min_acc) = &mut accumulators.min_accs[logical_schema_index] {
        min_acc.update_batch(&[Arc::clone(&min_values)])?;
        let mut cur_min_acc = min_acc.clone();
        accumulators.is_min_value_exact[logical_schema_index] = has_any_exact_match(
            &cur_min_acc.evaluate()?,
            &min_values,
            &is_min_value_exact_stat,
        );
    }

    accumulators.null_counts_array[logical_schema_index] = match sum(&null_counts) {
        Some(null_count) => Precision::Exact(null_count as usize),
        None => match null_counts.len() {
            // If sum() returned None we either have no rows or all values are null
            0 => Precision::Exact(0),
            _ => Precision::Absent,
        },
    };

    // This is the same logic as parquet_column but we start from arrow schema index
    // instead of looking up by name.
    let parquet_index = parquet_column(
        parquet_schema,
        physical_file_schema,
        logical_file_schema.field(logical_schema_index).name(),
    )
    .map(|(idx, _)| idx);

    let arrow_field = logical_file_schema.field(logical_schema_index);
    accumulators.column_byte_sizes[logical_schema_index] = compute_arrow_column_size(
        arrow_field.data_type(),
        row_groups_metadata,
        parquet_index,
        row_groups_metadata
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .sum(),
    );

    Ok(())
}

/// Compute the Arrow in-memory size for a single column
fn compute_arrow_column_size(
    data_type: &DataType,
    row_groups_metadata: &[RowGroupMetaData],
    parquet_idx: Option<usize>,
    num_rows: usize,
) -> Precision<usize> {
    // For primitive types with known fixed size, compute exact size
    if let Some(byte_width) = data_type.primitive_width() {
        return Precision::Exact(byte_width * num_rows);
    }

    // Use the uncompressed Parquet size as an estimate for other types
    if let Some(parquet_idx) = parquet_idx {
        let uncompressed_bytes: i64 = row_groups_metadata
            .iter()
            .filter_map(|rg| rg.columns().get(parquet_idx))
            .map(|col| col.uncompressed_size())
            .sum();
        return Precision::Inexact(uncompressed_bytes as usize);
    }

    // Otherwise, we cannot determine the size
    Precision::Absent
}

/// Checks if any occurrence of `value` in `array` corresponds to a `true`
/// entry in the `exactness` array.
///
/// This is used to determine if a calculated statistic (e.g., min or max)
/// is exact, by checking if at least one of its source values was exact.
///
/// # Example
/// - `value`: `0`
/// - `array`: `[0, 1, 0, 3, 0, 5]`
/// - `exactness`: `[true, false, false, false, false, false]`
///
/// The value `0` appears at indices `[0, 2, 4]`. The corresponding exactness
/// values are `[true, false, false]`. Since at least one is `true`, the
/// function returns `Some(true)`.
fn has_any_exact_match(
    value: &ScalarValue,
    array: &ArrayRef,
    exactness: &BooleanArray,
) -> Option<bool> {
    let scalar_array = value.to_scalar().ok()?;
    let eq_mask = eq(&scalar_array, &array).ok()?;
    let combined_mask = and(&eq_mask, exactness).ok()?;
    Some(combined_mask.true_count() > 0)
}

/// Wrapper to implement [`FileMetadata`] for [`ParquetMetaData`].
pub struct CachedParquetMetaData(Arc<ParquetMetaData>);

impl CachedParquetMetaData {
    pub fn new(metadata: Arc<ParquetMetaData>) -> Self {
        Self(metadata)
    }

    pub fn parquet_metadata(&self) -> &Arc<ParquetMetaData> {
        &self.0
    }
}

impl FileMetadata for CachedParquetMetaData {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn memory_size(&self) -> usize {
        self.0.memory_size()
    }

    fn extra_info(&self) -> HashMap<String, String> {
        let page_index =
            self.0.column_index().is_some() && self.0.offset_index().is_some();
        HashMap::from([("page_index".to_owned(), page_index.to_string())])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, Int32Array};
    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;
    use parquet::basic::Type;
    use parquet::file::metadata::{
        ColumnChunkMetaData, FileMetaData, ParquetMetaDataBuilder, RowGroupMetaData,
    };
    use parquet::schema::types::SchemaDescriptor;
    use parquet::schema::types::Type as SchemaType;
    use std::sync::Arc;

    /// Create a test Parquet schema descriptor with given column names (all INT32)
    fn create_test_schema_descr(column_names: &[&str]) -> Arc<SchemaDescriptor> {
        let fields: Vec<Arc<SchemaType>> = column_names
            .iter()
            .map(|name| {
                Arc::new(
                    SchemaType::primitive_type_builder(name, Type::INT32)
                        .build()
                        .unwrap(),
                )
            })
            .collect();

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    /// Create a test Arrow schema with given column names (all Int32)
    fn create_test_arrow_schema(column_names: &[&str]) -> SchemaRef {
        let fields: Vec<Field> = column_names
            .iter()
            .map(|name| Field::new(*name, DataType::Int32, true))
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Create a RowGroupMetaData with the given sorting columns
    fn create_row_group_with_sorting(
        schema_descr: &Arc<SchemaDescriptor>,
        sorting_columns: Option<Vec<SortingColumn>>,
        num_rows: i64,
    ) -> RowGroupMetaData {
        let columns: Vec<ColumnChunkMetaData> = schema_descr
            .columns()
            .iter()
            .map(|col| ColumnChunkMetaData::builder(col.clone()).build().unwrap())
            .collect();

        let mut builder = RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(num_rows)
            .set_column_metadata(columns);

        if let Some(sorting) = sorting_columns {
            builder = builder.set_sorting_columns(Some(sorting));
        }

        builder.build().unwrap()
    }

    /// Create a ParquetMetaData with given row groups
    fn create_parquet_metadata(
        schema_descr: Arc<SchemaDescriptor>,
        row_groups: Vec<RowGroupMetaData>,
    ) -> ParquetMetaData {
        let file_metadata = FileMetaData::new(
            1,                                                      // version
            row_groups.iter().map(|rg| rg.num_rows()).sum::<i64>(), // num_rows
            None,                                                   // created_by
            None,                                                   // key_value_metadata
            schema_descr,                                           // schema_descr
            None,                                                   // column_orders
        );

        ParquetMetaDataBuilder::new(file_metadata)
            .set_row_groups(row_groups)
            .build()
    }

    #[test]
    fn test_ordering_from_parquet_metadata_single_row_group_with_sorting() {
        // Single row group with sorting_columns [a ASC NULLS FIRST, b DESC NULLS LAST]
        let schema_descr = create_test_schema_descr(&["a", "b", "c"]);
        let arrow_schema = create_test_arrow_schema(&["a", "b", "c"]);

        let sorting_columns = vec![
            SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: true,
            }, // a ASC NULLS FIRST
            SortingColumn {
                column_idx: 1,
                descending: true,
                nulls_first: false,
            }, // b DESC NULLS LAST
        ];

        let row_group =
            create_row_group_with_sorting(&schema_descr, Some(sorting_columns), 1000);
        let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        assert!(result.is_some());
        let ordering = result.unwrap();
        assert_eq!(ordering.len(), 2);

        // Check first sort expr (a ASC NULLS FIRST)
        let expr0 = &ordering[0];
        assert_eq!(expr0.expr.to_string(), "a@0");
        assert!(!expr0.options.descending);
        assert!(expr0.options.nulls_first);

        // Check second sort expr (b DESC NULLS LAST)
        let expr1 = &ordering[1];
        assert_eq!(expr1.expr.to_string(), "b@1");
        assert!(expr1.options.descending);
        assert!(!expr1.options.nulls_first);
    }

    #[test]
    fn test_ordering_from_parquet_metadata_multiple_row_groups_identical_sorting() {
        // Multiple row groups with identical sorting_columns
        let schema_descr = create_test_schema_descr(&["a", "b"]);
        let arrow_schema = create_test_arrow_schema(&["a", "b"]);

        let sorting_columns = vec![SortingColumn {
            column_idx: 0,
            descending: false,
            nulls_first: true,
        }]; // a ASC NULLS FIRST

        let row_group1 = create_row_group_with_sorting(
            &schema_descr,
            Some(sorting_columns.clone()),
            500,
        );
        let row_group2 = create_row_group_with_sorting(
            &schema_descr,
            Some(sorting_columns.clone()),
            500,
        );
        let row_group3 =
            create_row_group_with_sorting(&schema_descr, Some(sorting_columns), 500);

        let metadata = create_parquet_metadata(
            schema_descr,
            vec![row_group1, row_group2, row_group3],
        );

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        assert!(result.is_some());
        let ordering = result.unwrap();
        assert_eq!(ordering.len(), 1);

        let expr0 = &ordering[0];
        assert_eq!(expr0.expr.to_string(), "a@0");
        assert!(!expr0.options.descending);
        assert!(expr0.options.nulls_first);
    }

    #[test]
    fn test_ordering_from_parquet_metadata_multiple_row_groups_different_sorting() {
        // Multiple row groups with DIFFERENT sorting_columns → should return None
        let schema_descr = create_test_schema_descr(&["a", "b"]);
        let arrow_schema = create_test_arrow_schema(&["a", "b"]);

        let sorting1 = vec![SortingColumn {
            column_idx: 0,
            descending: false,
            nulls_first: true,
        }]; // a ASC
        let sorting2 = vec![SortingColumn {
            column_idx: 1,
            descending: false,
            nulls_first: true,
        }]; // b ASC (different column)

        let row_group1 =
            create_row_group_with_sorting(&schema_descr, Some(sorting1), 500);
        let row_group2 =
            create_row_group_with_sorting(&schema_descr, Some(sorting2), 500);

        let metadata =
            create_parquet_metadata(schema_descr, vec![row_group1, row_group2]);

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_ordering_from_parquet_metadata_no_row_groups() {
        // No row groups → should return None
        let schema_descr = create_test_schema_descr(&["a", "b"]);
        let arrow_schema = create_test_arrow_schema(&["a", "b"]);

        let metadata = create_parquet_metadata(schema_descr, vec![]);

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_ordering_from_parquet_metadata_no_sorting_columns() {
        // Row groups with no sorting_columns → should return None
        let schema_descr = create_test_schema_descr(&["a", "b"]);
        let arrow_schema = create_test_arrow_schema(&["a", "b"]);

        let row_group = create_row_group_with_sorting(&schema_descr, None, 1000);
        let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_ordering_from_parquet_metadata_empty_sorting_columns() {
        // Row groups with empty sorting_columns vector → should return None
        let schema_descr = create_test_schema_descr(&["a", "b"]);
        let arrow_schema = create_test_arrow_schema(&["a", "b"]);

        let row_group = create_row_group_with_sorting(&schema_descr, Some(vec![]), 1000);
        let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_ordering_from_parquet_metadata_column_index_out_of_bounds() {
        // Sorting column index references a non-existent column → should return None
        let schema_descr = create_test_schema_descr(&["a", "b"]);
        let arrow_schema = create_test_arrow_schema(&["a", "b"]);

        // Column index 5 doesn't exist (only 0 and 1 are valid)
        let sorting_columns = vec![SortingColumn {
            column_idx: 5,
            descending: false,
            nulls_first: true,
        }];

        let row_group =
            create_row_group_with_sorting(&schema_descr, Some(sorting_columns), 1000);
        let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_ordering_from_parquet_metadata_column_not_in_arrow_schema() {
        // Parquet has more columns than Arrow schema
        // If sorting references a column not in Arrow schema, should return None
        let schema_descr = create_test_schema_descr(&["a", "b", "c"]);
        // Arrow schema only has "a" and "b", not "c"
        let arrow_schema = create_test_arrow_schema(&["a", "b"]);

        // Sort by column "c" (index 2 in Parquet)
        let sorting_columns = vec![SortingColumn {
            column_idx: 2,
            descending: false,
            nulls_first: true,
        }];

        let row_group =
            create_row_group_with_sorting(&schema_descr, Some(sorting_columns), 1000);
        let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        // Column "c" is not in Arrow schema, so ordering should be None
        assert!(result.is_none());
    }

    #[test]
    fn test_ordering_from_parquet_metadata_first_has_sorting_second_has_none() {
        // First row group has sorting, second has None → should return None
        let schema_descr = create_test_schema_descr(&["a", "b"]);
        let arrow_schema = create_test_arrow_schema(&["a", "b"]);

        let sorting = vec![SortingColumn {
            column_idx: 0,
            descending: false,
            nulls_first: true,
        }];

        let row_group1 = create_row_group_with_sorting(&schema_descr, Some(sorting), 500);
        let row_group2 = create_row_group_with_sorting(&schema_descr, None, 500);

        let metadata =
            create_parquet_metadata(schema_descr, vec![row_group1, row_group2]);

        let result =
            DFParquetMetadata::ordering_from_parquet_metadata(&metadata, &arrow_schema)
                .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_has_any_exact_match() {
        // Case 1: Mixed exact and inexact matches
        {
            let computed_min = ScalarValue::Int32(Some(0));
            let row_group_mins =
                Arc::new(Int32Array::from(vec![0, 1, 0, 3, 0, 5])) as ArrayRef;
            let exactness =
                BooleanArray::from(vec![true, false, false, false, false, false]);

            let result = has_any_exact_match(&computed_min, &row_group_mins, &exactness);
            assert_eq!(result, Some(true));
        }
        // Case 2: All inexact matches
        {
            let computed_min = ScalarValue::Int32(Some(0));
            let row_group_mins =
                Arc::new(Int32Array::from(vec![0, 1, 0, 3, 0, 5])) as ArrayRef;
            let exactness =
                BooleanArray::from(vec![false, false, false, false, false, false]);

            let result = has_any_exact_match(&computed_min, &row_group_mins, &exactness);
            assert_eq!(result, Some(false));
        }
        // Case 3: All exact matches
        {
            let computed_max = ScalarValue::Int32(Some(5));
            let row_group_maxes =
                Arc::new(Int32Array::from(vec![1, 5, 3, 5, 2, 5])) as ArrayRef;
            let exactness =
                BooleanArray::from(vec![false, true, true, true, false, true]);

            let result = has_any_exact_match(&computed_max, &row_group_maxes, &exactness);
            assert_eq!(result, Some(true));
        }
        // Case 4: All maxes are null values
        {
            let computed_max = ScalarValue::Int32(None);
            let row_group_maxes =
                Arc::new(Int32Array::from(vec![None, None, None, None])) as ArrayRef;
            let exactness = BooleanArray::from(vec![None, Some(true), None, Some(false)]);

            let result = has_any_exact_match(&computed_max, &row_group_maxes, &exactness);
            assert_eq!(result, Some(false));
        }
    }

    #[test]
    fn test_sort_expr_to_sorting_column_asc_nulls_first() {
        use super::sort_expr_to_sorting_column;

        let column = Arc::new(Column::new("a", 0));
        let sort_expr = PhysicalSortExpr::new(
            column,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        );

        let result = sort_expr_to_sorting_column(&sort_expr).unwrap();

        assert_eq!(result.column_idx, 0);
        assert!(!result.descending);
        assert!(result.nulls_first);
    }

    #[test]
    fn test_sort_expr_to_sorting_column_desc_nulls_last() {
        use super::sort_expr_to_sorting_column;

        let column = Arc::new(Column::new("b", 5));
        let sort_expr = PhysicalSortExpr::new(
            column,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );

        let result = sort_expr_to_sorting_column(&sort_expr).unwrap();

        assert_eq!(result.column_idx, 5);
        assert!(result.descending);
        assert!(!result.nulls_first);
    }

    #[test]
    fn test_sort_expr_to_sorting_column_non_column_expr() {
        use super::sort_expr_to_sorting_column;
        use datafusion_common::ScalarValue;
        use datafusion_physical_expr::expressions::Literal;

        // Create a non-column expression (a literal)
        let literal = Arc::new(Literal::new(ScalarValue::Int32(Some(42))));
        let sort_expr = PhysicalSortExpr::new(literal, SortOptions::default());

        let result = sort_expr_to_sorting_column(&sort_expr);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("only supports simple column references"),
            "Expected error about column references, got: {err}"
        );
    }

    #[test]
    fn test_lex_ordering_to_sorting_columns() {
        use super::lex_ordering_to_sorting_columns;

        let col_a = Arc::new(Column::new("a", 0));
        let col_b = Arc::new(Column::new("b", 1));

        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new(
                col_a,
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            ),
            PhysicalSortExpr::new(
                col_b,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ])
        .unwrap();

        let result = lex_ordering_to_sorting_columns(&ordering).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].column_idx, 0);
        assert!(!result[0].descending);
        assert!(result[0].nulls_first);
        assert_eq!(result[1].column_idx, 1);
        assert!(result[1].descending);
        assert!(!result[1].nulls_first);
    }
}
