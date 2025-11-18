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
    apply_file_schema_type_coercions, coerce_int96_to_resolution, ObjectStoreFetch,
};
use arrow::array::{ArrayRef, BooleanArray};
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
use datafusion_physical_plan::Accumulator;
use log::debug;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData,
};
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

        if cache_metadata {
            if let Some(parquet_metadata) = file_metadata_cache
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

        if cache_metadata {
            if let Some(file_metadata_cache) = file_metadata_cache {
                file_metadata_cache.put(
                    object_meta,
                    Arc::new(CachedParquetMetaData::new(Arc::clone(&metadata))),
                );
            }
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
    /// # When only some columns have statistics:
    ///
    /// For columns with statistics:
    /// - Min/max values are properly extracted and represented as Precision::Exact
    /// - Null counts are calculated by summing across row groups
    ///
    /// For columns without statistics,
    /// - For min/max, there are two situations:
    ///     1. The column isn't in arrow schema, then min/max values are set to Precision::Absent
    ///     2. The column is in arrow schema, but not in parquet schema due to schema revolution, min/max values are set to Precision::Exact(null)
    /// - Null counts are set to Precision::Exact(num_rows) (conservatively assuming all values could be null)
    pub fn statistics_from_parquet_metadata(
        metadata: &ParquetMetaData,
        table_schema: &SchemaRef,
    ) -> Result<Statistics> {
        let row_groups_metadata = metadata.row_groups();

        let mut statistics = Statistics::new_unknown(table_schema);
        let mut has_statistics = false;
        let mut num_rows = 0_usize;
        let mut total_byte_size = 0_usize;
        for row_group_meta in row_groups_metadata {
            num_rows += row_group_meta.num_rows() as usize;
            total_byte_size += row_group_meta.total_byte_size() as usize;

            if !has_statistics {
                has_statistics = row_group_meta
                    .columns()
                    .iter()
                    .any(|column| column.statistics().is_some());
            }
        }
        statistics.num_rows = Precision::Exact(num_rows);
        statistics.total_byte_size = Precision::Exact(total_byte_size);

        let file_metadata = metadata.file_metadata();
        let mut file_schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )?;

        if let Some(merged) = apply_file_schema_type_coercions(table_schema, &file_schema)
        {
            file_schema = merged;
        }

        statistics.column_statistics = if has_statistics {
            let (mut max_accs, mut min_accs) = create_max_min_accs(table_schema);
            let mut null_counts_array =
                vec![Precision::Exact(0); table_schema.fields().len()];
            let mut is_max_value_exact = vec![Some(true); table_schema.fields().len()];
            let mut is_min_value_exact = vec![Some(true); table_schema.fields().len()];
            table_schema
                .fields()
                .iter()
                .enumerate()
                .for_each(|(idx, field)| {
                    match StatisticsConverter::try_new(
                        field.name(),
                        &file_schema,
                        file_metadata.schema_descr(),
                    ) {
                        Ok(stats_converter) => {
                            let mut accumulators = StatisticsAccumulators {
                                min_accs: &mut min_accs,
                                max_accs: &mut max_accs,
                                null_counts_array: &mut null_counts_array,
                                is_min_value_exact: &mut is_min_value_exact,
                                is_max_value_exact: &mut is_max_value_exact,
                            };
                            summarize_min_max_null_counts(
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
                    }
                });

            get_col_stats(
                table_schema,
                &null_counts_array,
                &mut max_accs,
                &mut min_accs,
                &mut is_max_value_exact,
                &mut is_min_value_exact,
            )
        } else {
            Statistics::unknown_column(table_schema)
        };

        Ok(statistics)
    }
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
}

fn summarize_min_max_null_counts(
    accumulators: &mut StatisticsAccumulators,
    arrow_schema_index: usize,
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

    if let Some(max_acc) = &mut accumulators.max_accs[arrow_schema_index] {
        max_acc.update_batch(&[Arc::clone(&max_values)])?;
        let mut cur_max_acc = max_acc.clone();
        accumulators.is_max_value_exact[arrow_schema_index] = has_any_exact_match(
            &cur_max_acc.evaluate()?,
            &max_values,
            &is_max_value_exact_stat,
        );
    }

    if let Some(min_acc) = &mut accumulators.min_accs[arrow_schema_index] {
        min_acc.update_batch(&[Arc::clone(&min_values)])?;
        let mut cur_min_acc = min_acc.clone();
        accumulators.is_min_value_exact[arrow_schema_index] = has_any_exact_match(
            &cur_min_acc.evaluate()?,
            &min_values,
            &is_min_value_exact_stat,
        );
    }

    accumulators.null_counts_array[arrow_schema_index] = match sum(&null_counts) {
        Some(null_count) => Precision::Exact(null_count as usize),
        None => match null_counts.len() {
            // If sum() returned None we either have no rows or all values are null
            0 => Precision::Exact(0),
            _ => Precision::Absent,
        },
    };

    Ok(())
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
    use datafusion_common::ScalarValue;
    use std::sync::Arc;

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
}
