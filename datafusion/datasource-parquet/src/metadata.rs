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

use crate::file_format::ObjectStoreFetch;
use crate::{Int96Coercer, apply_file_schema_type_coercions};
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::kernels::cmp::eq;
use arrow::compute::{and, sum};
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_common::stats::Precision;
use datafusion_common::{
    ColumnStatistics, DataFusionError, HashMap, Result, ScalarValue, Statistics,
};
use datafusion_execution::cache::cache_manager::{
    CachedFileMetadataEntry, FileMetadata, FileMetadataCache,
};
use datafusion_functions_aggregate_common::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::Accumulator;
use log::debug;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use parquet::DecodeResult;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::{parquet_column, parquet_to_arrow_schema};
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataPushDecoder, ParquetMetaDataReader,
    RowGroupMetaData, SortingColumn,
};
use parquet::file::statistics::Statistics as ParquetStatistics;
use parquet::schema::types::SchemaDescriptor;
use std::any::Any;
use std::sync::Arc;

/// Minimum fraction of row groups that must report NDV statistics for the
/// merged result to be `Inexact` rather than `Absent`, as the estimate
/// would be too unreliable otherwise.
const PARTIAL_NDV_THRESHOLD: f64 = 0.75;

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
    file_metadata_cache: Option<Arc<FileMetadataCache>>,
    page_index_policy: Option<PageIndexPolicy>,
    /// timeunit to coerce INT96 timestamps to
    pub coerce_int96: Option<TimeUnit>,
    /// Optional timezone applied to INT96-coerced timestamps.
    pub coerce_int96_tz: Option<Arc<str>>,
}

impl<'a> DFParquetMetadata<'a> {
    pub fn new(store: &'a dyn ObjectStore, object_meta: &'a ObjectMeta) -> Self {
        Self {
            store,
            object_meta,
            metadata_size_hint: None,
            decryption_properties: None,
            file_metadata_cache: None,
            page_index_policy: None,
            coerce_int96: None,
            coerce_int96_tz: None,
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
        file_metadata_cache: Option<Arc<FileMetadataCache>>,
    ) -> Self {
        self.file_metadata_cache = file_metadata_cache;
        self
    }

    /// Sets the policy for loading parquet page index structures (column and offset indexes).
    pub fn with_page_index_policy(
        mut self,
        page_index_policy: Option<PageIndexPolicy>,
    ) -> Self {
        self.page_index_policy = page_index_policy;
        self
    }

    /// Set timeunit to coerce INT96 timestamps to
    pub fn with_coerce_int96(mut self, time_unit: Option<TimeUnit>) -> Self {
        self.coerce_int96 = time_unit;
        self
    }

    /// Set the optional timezone applied to INT96-coerced timestamps.
    pub fn with_coerce_int96_tz(mut self, timezone: Option<Arc<str>>) -> Self {
        self.coerce_int96_tz = timezone;
        self
    }

    /// Fetch parquet metadata from the remote object store
    pub async fn fetch_metadata(&self) -> Result<Arc<ParquetMetaData>> {
        // fetch_metadata
        // │
        // ├─ cache_metadata = encryption check
        // ├─ page_index_policy = caller override OR default
        // │
        // ├─ CACHE HIT?
        // │   │
        // │   ├─ has index OR policy=Skip?  → return cache
        // │   │
        // │   └─ else (footer only, wants index)
        // │         → load_page_index (index bytes only)
        // │         → cache_metadata() if allowed
        // │         → return
        // │
        // └─ CACHE MISS
        //       → fetch_metadata_from_store(policy)
        //       → cache_metadata() if allowed
        //       → return
        let cache_metadata =
            !cfg!(feature = "parquet_encryption") || self.decryption_properties.is_none();
        let page_index_policy = self.effective_page_index_policy(cache_metadata);

        if cache_metadata
            && let Some(file_metadata_cache) = self.file_metadata_cache.as_ref()
            && let Some(cached) = file_metadata_cache.get(&self.object_meta.location)
            && cached.is_valid_for(self.object_meta)
            && let Some(cached_parquet) = cached
                .file_metadata
                .as_any()
                .downcast_ref::<CachedParquetMetaData>()
        {
            let cached_metadata = Arc::clone(cached_parquet.parquet_metadata());
            // Reuse the cache when it already has page index, or when the caller
            // asked to skip page index I/O (footer-only metadata is sufficient).
            if Self::metadata_has_page_index(cached_metadata.as_ref())
                || page_index_policy == PageIndexPolicy::Skip
            {
                return Ok(cached_metadata);
            }
            let metadata =
                Self::load_page_index(self.store, self.object_meta, cached_metadata)
                    .await?;
            if cache_metadata {
                self.cache_metadata(Arc::clone(&metadata)).await?;
            }
            return Ok(metadata);
        }

        let metadata = self.fetch_metadata_from_store(page_index_policy).await?;
        if cache_metadata {
            self.cache_metadata(Arc::clone(&metadata)).await?;
        }
        Ok(metadata)
    }

    fn effective_page_index_policy(&self, cache_metadata: bool) -> PageIndexPolicy {
        self.page_index_policy.unwrap_or_else(|| {
            if cache_metadata && self.file_metadata_cache.is_some() {
                PageIndexPolicy::Optional
            } else {
                PageIndexPolicy::Skip
            }
        })
    }

    fn metadata_has_page_index(metadata: &ParquetMetaData) -> bool {
        metadata.column_index().is_some() && metadata.offset_index().is_some()
    }

    async fn cache_metadata(&self, metadata: Arc<ParquetMetaData>) -> Result<()> {
        if let Some(file_metadata_cache) = &self.file_metadata_cache {
            file_metadata_cache.put(
                &self.object_meta.location,
                CachedFileMetadataEntry::new(
                    self.object_meta.clone(),
                    Arc::new(CachedParquetMetaData::new(metadata)),
                ),
            );
        }
        Ok(())
    }

    async fn fetch_metadata_from_store(
        &self,
        page_index_policy: PageIndexPolicy,
    ) -> Result<Arc<ParquetMetaData>> {
        let file_size = self.object_meta.size;
        let mut decoder = ParquetMetaDataPushDecoder::try_new(file_size)
            .map_err(DataFusionError::from)?;

        #[cfg(feature = "parquet_encryption")]
        if let Some(decryption_properties) = &self.decryption_properties {
            decoder = decoder
                .with_file_decryption_properties(Some(Arc::clone(decryption_properties)));
        }

        decoder = decoder.with_page_index_policy(page_index_policy);

        if let Some(hint) = self.metadata_size_hint {
            let prefetch_start = file_size.saturating_sub(hint as u64);
            let prefetch_range = prefetch_start..file_size;
            let data = self
                .store
                .get_ranges(
                    &self.object_meta.location,
                    std::slice::from_ref(&prefetch_range),
                )
                .await
                .map_err(DataFusionError::from)?;
            decoder
                .push_ranges(vec![prefetch_range], data)
                .map_err(DataFusionError::from)?;
        }

        let metadata = loop {
            match decoder.try_decode().map_err(DataFusionError::from)? {
                DecodeResult::Data(metadata) => break metadata,
                DecodeResult::NeedsData(ranges) => {
                    let buffers = self
                        .store
                        .get_ranges(&self.object_meta.location, &ranges)
                        .await
                        .map_err(DataFusionError::from)?;
                    decoder
                        .push_ranges(ranges, buffers)
                        .map_err(DataFusionError::from)?;
                }
                DecodeResult::Finished => {
                    return Err(DataFusionError::Internal(
                        "ParquetMetaDataPushDecoder finished without producing metadata"
                            .to_string(),
                    ));
                }
            }
        };

        Ok(Arc::new(metadata))
    }

    async fn load_page_index(
        store: &dyn ObjectStore,
        object_meta: &ObjectMeta,
        metadata: Arc<ParquetMetaData>,
    ) -> Result<Arc<ParquetMetaData>> {
        if metadata.column_index().is_some() && metadata.offset_index().is_some() {
            return Ok(metadata);
        }
        let metadata =
            Arc::try_unwrap(metadata).unwrap_or_else(|shared| (*shared).clone());
        let mut reader = ParquetMetaDataReader::new_with_metadata(metadata)
            .with_page_index_policy(PageIndexPolicy::Optional);
        let fetch = ObjectStoreFetch::new(store, object_meta);
        reader
            .load_page_index(fetch)
            .await
            .map_err(DataFusionError::from)?;
        Ok(Arc::new(reader.finish().map_err(DataFusionError::from)?))
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
                Int96Coercer::new(file_metadata.schema_descr(), &schema, time_unit)
                    .with_timezone(self.coerce_int96_tz.clone())
                    .coerce()
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
                let mut distinct_counts_array =
                    vec![Precision::Absent; logical_file_schema.fields().len()];
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
                                distinct_counts_array: &mut distinct_counts_array,
                            };
                            summarize_column_statistics(
                                logical_file_schema,
                                &mut accumulators,
                                idx,
                                &stats_converter,
                                row_groups_metadata,
                                num_rows,
                            )
                            .ok();
                        }
                        Err(e) => {
                            debug!("Failed to create statistics converter: {e}");
                            null_counts_array[idx] = Precision::Exact(num_rows);
                        }
                    },
                );

                let mut accumulators = StatisticsAccumulators {
                    min_accs: &mut min_accs,
                    max_accs: &mut max_accs,
                    null_counts_array: &mut null_counts_array,
                    is_min_value_exact: &mut is_min_value_exact,
                    is_max_value_exact: &mut is_max_value_exact,
                    column_byte_sizes: &mut column_byte_sizes,
                    distinct_counts_array: &mut distinct_counts_array,
                };
                accumulators.build_column_statistics(logical_file_schema)
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

/// Holds the accumulator state for collecting statistics from row groups
struct StatisticsAccumulators<'a> {
    min_accs: &'a mut [Option<MinAccumulator>],
    max_accs: &'a mut [Option<MaxAccumulator>],
    null_counts_array: &'a mut [Precision<usize>],
    is_min_value_exact: &'a mut [Option<bool>],
    is_max_value_exact: &'a mut [Option<bool>],
    column_byte_sizes: &'a mut [Precision<usize>],
    distinct_counts_array: &'a mut [Precision<usize>],
}

impl StatisticsAccumulators<'_> {
    /// Converts the accumulated statistics into a vector of `ColumnStatistics`
    fn build_column_statistics(&mut self, schema: &Schema) -> Vec<ColumnStatistics> {
        (0..schema.fields().len())
            .map(|i| {
                let max_value = match (
                    self.max_accs.get_mut(i).unwrap(),
                    self.is_max_value_exact.get(i).unwrap(),
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
                    self.min_accs.get_mut(i).unwrap(),
                    self.is_min_value_exact.get(i).unwrap(),
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
                    null_count: self.null_counts_array[i],
                    max_value: max_value.unwrap_or(Precision::Absent),
                    min_value: min_value.unwrap_or(Precision::Absent),
                    sum_value: Precision::Absent,
                    distinct_count: self.distinct_counts_array[i],
                    byte_size: self.column_byte_sizes[i],
                }
            })
            .collect()
    }
}

fn summarize_column_statistics(
    logical_file_schema: &Schema,
    accumulators: &mut StatisticsAccumulators,
    logical_schema_index: usize,
    stats_converter: &StatisticsConverter,
    row_groups_metadata: &[RowGroupMetaData],
    num_rows: usize,
) -> Result<()> {
    let parquet_index = stats_converter.parquet_column_index();

    if let Some(max_acc) = &mut accumulators.max_accs[logical_schema_index] {
        accumulators.is_max_value_exact[logical_schema_index] = summarize_bound(
            max_acc,
            &stats_converter.row_group_maxes(row_groups_metadata)?,
            parquet_index,
            row_groups_metadata,
            ParquetStatistics::max_is_exact,
            || Ok(stats_converter.row_group_is_max_value_exact(row_groups_metadata)?),
        )?;
    }

    if let Some(min_acc) = &mut accumulators.min_accs[logical_schema_index] {
        accumulators.is_min_value_exact[logical_schema_index] = summarize_bound(
            min_acc,
            &stats_converter.row_group_mins(row_groups_metadata)?,
            parquet_index,
            row_groups_metadata,
            ParquetStatistics::min_is_exact,
            || Ok(stats_converter.row_group_is_min_value_exact(row_groups_metadata)?),
        )?;
    }

    accumulators.null_counts_array[logical_schema_index] =
        summarize_null_counts(stats_converter, row_groups_metadata)?;

    accumulators.distinct_counts_array[logical_schema_index] =
        summarize_distinct_counts(parquet_index, row_groups_metadata);

    let arrow_field = logical_file_schema.field(logical_schema_index);
    accumulators.column_byte_sizes[logical_schema_index] = compute_arrow_column_size(
        arrow_field.data_type(),
        row_groups_metadata,
        parquet_index,
        num_rows,
    );

    Ok(())
}

/// Feed a column's per-row-group min or max `values` into `acc` and decide
/// whether the resulting bound is exact across all row groups.
///
/// `is_exact` reads the per-row-group exactness flag straight from the raw
/// parquet statistics. `row_group_exactness` rebuilds the exactness as a Boolean
/// array and is only called for the rare case where row groups disagree.
fn summarize_bound<A: Accumulator>(
    acc: &mut A,
    values: &ArrayRef,
    parquet_index: Option<usize>,
    row_groups_metadata: &[RowGroupMetaData],
    is_exact: impl Fn(&ParquetStatistics) -> bool,
    row_group_exactness: impl FnOnce() -> Result<BooleanArray>,
) -> Result<Option<bool>> {
    acc.update_batch(&[Arc::clone(values)])?;

    Ok(
        match summarize_row_group_exactness(parquet_index, row_groups_metadata, is_exact)
        {
            ExactnessSummary::AllExact => Some(true),
            ExactnessSummary::NoneExact => Some(false),
            ExactnessSummary::Mixed => {
                let exactness = row_group_exactness()?;
                has_any_exact_match(&acc.evaluate()?, values, &exactness)
            }
        },
    )
}

fn summarize_null_counts(
    stats_converter: &StatisticsConverter,
    row_groups_metadata: &[RowGroupMetaData],
) -> Result<Precision<usize>> {
    if row_groups_metadata.is_empty() {
        return Ok(Precision::Exact(0));
    }

    let null_counts = stats_converter.row_group_null_counts(row_groups_metadata)?;

    match sum(&null_counts) {
        Some(count) => {
            // If any row group has an unknown null_count, either because column
            // statistics are absent or because the null_count field is omitted,
            // report the aggregate as inexact.
            if null_counts.null_count() > 0 {
                Ok(Precision::Inexact(count as usize))
            } else {
                Ok(Precision::Exact(count as usize))
            }
        }
        None => match null_counts.len() {
            // If sum() returned None we either have no rows or all values are null
            0 => Ok(Precision::Exact(0)),
            _ => Ok(Precision::Absent),
        },
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ExactnessSummary {
    AllExact,
    NoneExact,
    Mixed,
}

fn summarize_row_group_exactness(
    parquet_idx: Option<usize>,
    row_groups_metadata: &[RowGroupMetaData],
    exactness: impl Fn(&ParquetStatistics) -> bool,
) -> ExactnessSummary {
    let Some(parquet_idx) = parquet_idx else {
        return ExactnessSummary::NoneExact;
    };

    summarize_exactness(row_groups_metadata.iter().map(|row_group| {
        row_group
            .columns()
            .get(parquet_idx)
            .and_then(|column| column.statistics())
            .map(&exactness)
    }))
}

fn summarize_exactness<I>(exactness: I) -> ExactnessSummary
where
    I: IntoIterator<Item = Option<bool>>,
{
    let mut has_true = false;
    let mut has_false_or_null = false;

    for exactness in exactness {
        match exactness {
            Some(true) => has_true = true,
            Some(false) | None => has_false_or_null = true,
        }

        if has_true && has_false_or_null {
            return ExactnessSummary::Mixed;
        }
    }

    if has_true {
        ExactnessSummary::AllExact
    } else {
        ExactnessSummary::NoneExact
    }
}

/// Extract distinct counts from row group column statistics.
fn summarize_distinct_counts(
    parquet_idx: Option<usize>,
    row_groups_metadata: &[RowGroupMetaData],
) -> Precision<usize> {
    let Some(parquet_idx) = parquet_idx else {
        return Precision::Absent;
    };

    let num_row_groups = row_groups_metadata.len();
    if num_row_groups == 0 {
        return Precision::Absent;
    }

    let required_count = (num_row_groups as f64 * PARTIAL_NDV_THRESHOLD).ceil() as usize;
    let mut ndv_count = 0;
    let mut max_distinct_count: Option<u64> = None;

    for (row_group_idx, row_group) in row_groups_metadata.iter().enumerate() {
        if let Some(distinct_count) = row_group
            .columns()
            .get(parquet_idx)
            .and_then(|col| col.statistics())
            .and_then(|stats| stats.distinct_count_opt())
        {
            ndv_count += 1;
            max_distinct_count = Some(match max_distinct_count {
                Some(max) => max.max(distinct_count),
                None => distinct_count,
            });
        }

        // Return early if there's no chance to reach the required coverage.
        let remaining = num_row_groups - row_group_idx - 1;
        if ndv_count + remaining < required_count {
            return Precision::Absent;
        }
    }

    match max_distinct_count {
        Some(distinct_count) if num_row_groups == 1 => {
            Precision::Exact(distinct_count as usize)
        }
        Some(distinct_count) => Precision::Inexact(distinct_count as usize),
        None => Precision::Absent,
    }
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
    if value.is_null() {
        return Some(false);
    }

    // Shortcut for single row group
    if array.len() == 1 {
        return Some(exactness.is_valid(0) && exactness.value(0));
    }

    let scalar_array = value.to_scalar().ok()?;
    let eq_mask = eq(&scalar_array, &array).ok()?;
    let combined_mask = and(&eq_mask, exactness).ok()?;
    Some(combined_mask.has_true())
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

/// Convert a [`PhysicalSortExpr`] to a Parquet [`SortingColumn`].
///
/// Returns `Err` if the expression is not a simple column reference.
pub(crate) fn sort_expr_to_sorting_column(
    sort_expr: &PhysicalSortExpr,
) -> Result<SortingColumn> {
    let column = sort_expr.expr.downcast_ref::<Column>().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Parquet sorting_columns only supports simple column references, \
                 but got expression: {}",
            sort_expr.expr
        ))
    })?;

    let column_idx: i32 = column.index().try_into().map_err(|_| {
        DataFusionError::Plan(format!(
            "Column index {} is too large to be represented as i32",
            column.index()
        ))
    })?;

    Ok(SortingColumn {
        column_idx,
        descending: sort_expr.options.descending,
        nulls_first: sort_expr.options.nulls_first,
    })
}

/// Convert a [`LexOrdering`] to `Vec<SortingColumn>` for Parquet.
///
/// Returns `Err` if any expression is not a simple column reference.
pub(crate) fn lex_ordering_to_sorting_columns(
    ordering: &LexOrdering,
) -> Result<Vec<SortingColumn>> {
    ordering.iter().map(sort_expr_to_sorting_column).collect()
}

/// Extracts ordering information from Parquet metadata.
///
/// This function reads the sorting_columns from the first row group's metadata
/// and converts them into a [`LexOrdering`] that can be used by the query engine.
///
/// # Arguments
/// * `metadata` - The Parquet metadata containing sorting_columns information
/// * `schema` - The Arrow schema to use for column lookup
///
/// # Returns
/// * `Ok(Some(ordering))` if valid ordering information was found
/// * `Ok(None)` if no sorting columns were specified or they couldn't be resolved
pub fn ordering_from_parquet_metadata(
    metadata: &ParquetMetaData,
    schema: &SchemaRef,
) -> Result<Option<LexOrdering>> {
    // Get the sorting columns from the first row group metadata.
    // If no row groups exist or no sorting columns are specified, return None.
    let sorting_columns = metadata
        .row_groups()
        .first()
        .and_then(|rg| rg.sorting_columns())
        .filter(|cols| !cols.is_empty());

    let Some(sorting_columns) = sorting_columns else {
        return Ok(None);
    };

    let parquet_schema = metadata.file_metadata().schema_descr();

    let sort_exprs =
        sorting_columns_to_physical_exprs(sorting_columns, parquet_schema, schema);

    if sort_exprs.is_empty() {
        return Ok(None);
    }

    Ok(LexOrdering::new(sort_exprs))
}

/// Converts Parquet sorting columns to physical sort expressions.
fn sorting_columns_to_physical_exprs(
    sorting_columns: &[SortingColumn],
    parquet_schema: &SchemaDescriptor,
    arrow_schema: &SchemaRef,
) -> Vec<PhysicalSortExpr> {
    use arrow::compute::SortOptions;

    sorting_columns
        .iter()
        .filter_map(|sc| {
            let parquet_column = parquet_schema.column(sc.column_idx as usize);
            let name = parquet_column.name();

            // Find the column in the arrow schema
            let (index, _) = arrow_schema.column_with_name(name)?;

            let expr = Arc::new(Column::new(name, index));
            let options = SortOptions {
                descending: sc.descending,
                nulls_first: sc.nulls_first,
            };
            Some(PhysicalSortExpr::new(expr, options))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

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
    fn test_summarize_exactness() {
        assert_eq!(
            summarize_exactness([Some(true), Some(true)]),
            ExactnessSummary::AllExact
        );
        assert_eq!(
            summarize_exactness([Some(false), None]),
            ExactnessSummary::NoneExact
        );
        assert_eq!(
            summarize_exactness([Some(true), Some(false)]),
            ExactnessSummary::Mixed
        );
        assert_eq!(
            summarize_exactness([Some(true), None]),
            ExactnessSummary::Mixed
        );
        assert_eq!(
            summarize_exactness(std::iter::empty()),
            ExactnessSummary::NoneExact
        );
    }

    mod ndv_tests {
        use super::*;
        use arrow::datatypes::Field;
        use parquet::basic::Type as PhysicalType;
        use parquet::file::metadata::ColumnChunkMetaData;
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::file::statistics::Statistics as ParquetStatistics;
        use parquet::schema::types::Type as SchemaType;
        use std::fs::File;
        use std::path::PathBuf;

        fn create_schema_descr(num_columns: usize) -> Arc<SchemaDescriptor> {
            let fields: Vec<Arc<SchemaType>> = (0..num_columns)
                .map(|i| {
                    Arc::new(
                        SchemaType::primitive_type_builder(
                            &format!("col_{i}"),
                            PhysicalType::INT32,
                        )
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

        fn create_arrow_schema(num_columns: usize) -> SchemaRef {
            let fields: Vec<Field> = (0..num_columns)
                .map(|i| Field::new(format!("col_{i}"), DataType::Int32, true))
                .collect();
            Arc::new(Schema::new(fields))
        }

        fn create_row_group_with_stats(
            schema_descr: &Arc<SchemaDescriptor>,
            column_stats: Vec<Option<ParquetStatistics>>,
            num_rows: i64,
        ) -> RowGroupMetaData {
            let columns: Vec<ColumnChunkMetaData> = column_stats
                .into_iter()
                .enumerate()
                .map(|(i, stats)| {
                    let mut builder =
                        ColumnChunkMetaData::builder(schema_descr.column(i));
                    if let Some(s) = stats {
                        builder = builder.set_statistics(s);
                    }
                    builder.set_num_values(num_rows).build().unwrap()
                })
                .collect();

            RowGroupMetaData::builder(schema_descr.clone())
                .set_num_rows(num_rows)
                .set_total_byte_size(1000)
                .set_column_metadata(columns)
                .build()
                .unwrap()
        }

        fn create_parquet_metadata(
            schema_descr: Arc<SchemaDescriptor>,
            row_groups: Vec<RowGroupMetaData>,
        ) -> ParquetMetaData {
            use parquet::file::metadata::FileMetaData;

            let num_rows: i64 = row_groups.iter().map(|rg| rg.num_rows()).sum();
            let file_meta = FileMetaData::new(
                1,            // version
                num_rows,     // num_rows
                None,         // created_by
                None,         // key_value_metadata
                schema_descr, // schema_descr
                None,         // column_orders
            );

            ParquetMetaData::new(file_meta, row_groups)
        }

        #[test]
        fn test_summarize_null_counts() {
            let schema_descr = create_schema_descr(1);
            let arrow_schema = create_arrow_schema(2);
            let stats_with_count =
                ParquetStatistics::int32(Some(1), Some(10), None, Some(2), false);
            let stats_without_count =
                ParquetStatistics::int32(Some(1), Some(10), None, None, false);

            let row_groups = vec![
                create_row_group_with_stats(
                    &schema_descr,
                    vec![Some(stats_with_count)],
                    10,
                ),
                create_row_group_with_stats(
                    &schema_descr,
                    vec![Some(stats_without_count.clone())],
                    10,
                ),
                create_row_group_with_stats(&schema_descr, vec![None], 10),
            ];
            let stats_converter =
                StatisticsConverter::try_new("col_0", &arrow_schema, &schema_descr)
                    .unwrap();
            let missing_column_converter =
                StatisticsConverter::try_new("col_1", &arrow_schema, &schema_descr)
                    .unwrap();

            assert_eq!(
                summarize_null_counts(&stats_converter, &row_groups).unwrap(),
                Precision::Inexact(2)
            );
            assert_eq!(
                summarize_null_counts(&missing_column_converter, &row_groups).unwrap(),
                Precision::Absent
            );
            assert_eq!(
                summarize_null_counts(&stats_converter, &[]).unwrap(),
                Precision::Exact(0)
            );
            assert_eq!(
                summarize_null_counts(&missing_column_converter, &[]).unwrap(),
                Precision::Exact(0)
            );

            let missing_counts_unknown_converter =
                StatisticsConverter::try_new("col_0", &arrow_schema, &schema_descr)
                    .unwrap()
                    .with_missing_null_counts_as_zero(false);
            assert_eq!(
                summarize_null_counts(&missing_counts_unknown_converter, &row_groups)
                    .unwrap(),
                Precision::Inexact(2)
            );

            let row_groups_without_count = vec![
                create_row_group_with_stats(
                    &schema_descr,
                    vec![Some(stats_without_count.clone())],
                    10,
                ),
                create_row_group_with_stats(
                    &schema_descr,
                    vec![Some(stats_without_count)],
                    10,
                ),
            ];
            assert_eq!(
                summarize_null_counts(&stats_converter, &row_groups_without_count)
                    .unwrap(),
                Precision::Exact(0)
            );

            let missing_counts_unknown_converter =
                stats_converter.with_missing_null_counts_as_zero(false);
            assert_eq!(
                summarize_null_counts(
                    &missing_counts_unknown_converter,
                    &row_groups_without_count,
                )
                .unwrap(),
                Precision::Absent
            );
        }

        #[test]
        fn test_distinct_count_single_row_group_with_ndv() {
            // Single row group with distinct count should return Exact
            let schema_descr = create_schema_descr(1);
            let arrow_schema = create_arrow_schema(1);

            // Create statistics with distinct_count = 42
            let stats = ParquetStatistics::int32(
                Some(1),   // min
                Some(100), // max
                Some(42),  // distinct_count
                Some(0),   // null_count
                false,     // is_deprecated
            );

            let row_group =
                create_row_group_with_stats(&schema_descr, vec![Some(stats)], 1000);
            let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

            let result = DFParquetMetadata::statistics_from_parquet_metadata(
                &metadata,
                &arrow_schema,
            )
            .unwrap();

            assert_eq!(
                result.column_statistics[0].distinct_count,
                Precision::Exact(42)
            );
        }

        #[test]
        fn test_distinct_count_multiple_row_groups_with_ndv() {
            // Multiple row groups with distinct counts should return Inexact (sum)
            let schema_descr = create_schema_descr(1);
            let arrow_schema = create_arrow_schema(1);

            // Row group 1: distinct_count = 10
            let stats1 = ParquetStatistics::int32(
                Some(1),
                Some(50),
                Some(10), // distinct_count
                Some(0),
                false,
            );

            // Row group 2: distinct_count = 20
            let stats2 = ParquetStatistics::int32(
                Some(51),
                Some(100),
                Some(20), // distinct_count
                Some(0),
                false,
            );

            let row_group1 =
                create_row_group_with_stats(&schema_descr, vec![Some(stats1)], 500);
            let row_group2 =
                create_row_group_with_stats(&schema_descr, vec![Some(stats2)], 500);
            let metadata =
                create_parquet_metadata(schema_descr, vec![row_group1, row_group2]);

            let result = DFParquetMetadata::statistics_from_parquet_metadata(
                &metadata,
                &arrow_schema,
            )
            .unwrap();

            // Max of distinct counts (lower bound since we can't accurately merge NDV)
            assert_eq!(
                result.column_statistics[0].distinct_count,
                Precision::Inexact(20)
            );
        }

        #[test]
        fn test_distinct_count_no_ndv_available() {
            // No distinct count in statistics should return Absent
            let schema_descr = create_schema_descr(1);
            let arrow_schema = create_arrow_schema(1);

            // Create statistics without distinct_count (None)
            let stats = ParquetStatistics::int32(
                Some(1),
                Some(100),
                None, // no distinct_count
                Some(0),
                false,
            );

            let row_group =
                create_row_group_with_stats(&schema_descr, vec![Some(stats)], 1000);
            let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

            let result = DFParquetMetadata::statistics_from_parquet_metadata(
                &metadata,
                &arrow_schema,
            )
            .unwrap();

            assert_eq!(
                result.column_statistics[0].distinct_count,
                Precision::Absent
            );
        }

        #[test]
        fn test_distinct_count_partial_ndv_below_threshold() {
            // 1 of 2 row groups has NDV (50% < 75% threshold) -> Absent
            let schema_descr = create_schema_descr(1);
            let arrow_schema = create_arrow_schema(1);

            let stats1 =
                ParquetStatistics::int32(Some(1), Some(50), Some(15), Some(0), false);
            let stats2 =
                ParquetStatistics::int32(Some(51), Some(100), None, Some(0), false);

            let row_group1 =
                create_row_group_with_stats(&schema_descr, vec![Some(stats1)], 500);
            let row_group2 =
                create_row_group_with_stats(&schema_descr, vec![Some(stats2)], 500);
            let metadata =
                create_parquet_metadata(schema_descr, vec![row_group1, row_group2]);

            let result = DFParquetMetadata::statistics_from_parquet_metadata(
                &metadata,
                &arrow_schema,
            )
            .unwrap();

            assert_eq!(
                result.column_statistics[0].distinct_count,
                Precision::Absent
            );
        }

        #[test]
        fn test_distinct_count_partial_ndv_above_threshold() {
            // 3 of 4 row groups have NDV (75% >= 75% threshold) -> Inexact
            let schema_descr = create_schema_descr(1);
            let arrow_schema = create_arrow_schema(1);

            let stats_with = |ndv| {
                ParquetStatistics::int32(Some(1), Some(100), Some(ndv), Some(0), false)
            };
            let stats_without =
                ParquetStatistics::int32(Some(1), Some(100), None, Some(0), false);

            let rg1 = create_row_group_with_stats(
                &schema_descr,
                vec![Some(stats_with(10))],
                250,
            );
            let rg2 = create_row_group_with_stats(
                &schema_descr,
                vec![Some(stats_with(20))],
                250,
            );
            let rg3 = create_row_group_with_stats(
                &schema_descr,
                vec![Some(stats_with(15))],
                250,
            );
            let rg4 = create_row_group_with_stats(
                &schema_descr,
                vec![Some(stats_without)],
                250,
            );
            let metadata =
                create_parquet_metadata(schema_descr, vec![rg1, rg2, rg3, rg4]);

            let result = DFParquetMetadata::statistics_from_parquet_metadata(
                &metadata,
                &arrow_schema,
            )
            .unwrap();

            assert_eq!(
                result.column_statistics[0].distinct_count,
                Precision::Inexact(20)
            );
        }

        #[test]
        fn test_distinct_count_multiple_columns() {
            // Test with multiple columns, each with different NDV
            let schema_descr = create_schema_descr(3);
            let arrow_schema = create_arrow_schema(3);

            // col_0: distinct_count = 5
            let stats0 =
                ParquetStatistics::int32(Some(1), Some(10), Some(5), Some(0), false);
            // col_1: no distinct_count
            let stats1 =
                ParquetStatistics::int32(Some(1), Some(100), None, Some(0), false);
            // col_2: distinct_count = 100
            let stats2 =
                ParquetStatistics::int32(Some(1), Some(1000), Some(100), Some(0), false);

            let row_group = create_row_group_with_stats(
                &schema_descr,
                vec![Some(stats0), Some(stats1), Some(stats2)],
                1000,
            );
            let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

            let result = DFParquetMetadata::statistics_from_parquet_metadata(
                &metadata,
                &arrow_schema,
            )
            .unwrap();

            assert_eq!(
                result.column_statistics[0].distinct_count,
                Precision::Exact(5)
            );
            assert_eq!(
                result.column_statistics[1].distinct_count,
                Precision::Absent
            );
            assert_eq!(
                result.column_statistics[2].distinct_count,
                Precision::Exact(100)
            );
        }

        #[test]
        fn test_distinct_count_no_statistics_at_all() {
            // No statistics in row group should return Absent for all stats
            let schema_descr = create_schema_descr(1);
            let arrow_schema = create_arrow_schema(1);

            // Create row group without any statistics
            let row_group = create_row_group_with_stats(&schema_descr, vec![None], 1000);
            let metadata = create_parquet_metadata(schema_descr, vec![row_group]);

            let result = DFParquetMetadata::statistics_from_parquet_metadata(
                &metadata,
                &arrow_schema,
            )
            .unwrap();

            assert_eq!(
                result.column_statistics[0].distinct_count,
                Precision::Absent
            );
        }

        /// Integration test that reads a real Parquet file with distinct_count statistics.
        /// The test file was created with DuckDB and has known NDV values:
        /// - id: NULL (high cardinality, not tracked)
        /// - category: 10 distinct values
        /// - name: 5 distinct values
        #[test]
        fn test_distinct_count_from_real_parquet_file() {
            // Path to test file created by DuckDB with distinct_count statistics
            let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("src/test_data/ndv_test.parquet");

            let file = File::open(&path).expect("Failed to open test parquet file");
            let reader =
                SerializedFileReader::new(file).expect("Failed to create reader");
            let parquet_metadata = reader.metadata();

            // Derive Arrow schema from parquet file metadata
            let arrow_schema = Arc::new(
                parquet_to_arrow_schema(
                    parquet_metadata.file_metadata().schema_descr(),
                    None,
                )
                .expect("Failed to convert schema"),
            );

            let result = DFParquetMetadata::statistics_from_parquet_metadata(
                parquet_metadata,
                &arrow_schema,
            )
            .expect("Failed to extract statistics");

            // id: no distinct_count (high cardinality)
            assert_eq!(
                result.column_statistics[0].distinct_count,
                Precision::Absent,
                "id column should have Absent distinct_count"
            );

            // category: 10 distinct values
            assert_eq!(
                result.column_statistics[1].distinct_count,
                Precision::Exact(10),
                "category column should have Exact(10) distinct_count"
            );

            // name: 5 distinct values
            assert_eq!(
                result.column_statistics[2].distinct_count,
                Precision::Exact(5),
                "name column should have Exact(5) distinct_count"
            );
        }
    }
}
