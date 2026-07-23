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

//! Loaded Parquet dictionary-page data, with a [`PruningStatistics`] adapter
//! so the predicate-pruning machinery in [`datafusion_pruning`] can consume
//! it.
//!
//! Some writers (e.g. Grafana Tempo) use a low-cardinality string column's
//! Parquet dictionary as a makeshift row-group index: the dictionary page is
//! the exact, complete set of the row group's distinct non-null values. If a
//! required value is not in the dictionary, the whole row group can be
//! skipped without reading any data pages. This is a strictly stronger
//! (exact, non-probabilistic) signal than a bloom filter, letting us prune
//! both `IN`/`=` (value absent) and `NOT IN`/`!=` (value is the only one
//! present) directions -- see [`DictionaryStatistics::contained`].

use std::collections::{HashMap, HashSet};

use arrow::array::{ArrayRef, BinaryArray, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, DataFusionError, Result, ScalarValue, internal_err};
use parquet::basic::{Encoding, Type};
use parquet::file::metadata::ColumnChunkMetaData;

/// In-memory decoded Parquet dictionary-page values, keyed by column name.
///
/// This structure implements [`PruningStatistics`] and is used to prune
/// Parquet row groups based on the query predicate. Unlike bloom filters,
/// which are probabilistic, the values stored here are assumed to be the
/// *exact* set of distinct values present in the column for the row group --
/// callers must only [`insert`](Self::insert) dictionaries for column chunks
/// that pass [`is_fully_dictionary_encoded`], or pruning will be unsound.
#[derive(Debug, Clone, Default)]
pub struct DictionaryStatistics {
    /// Per-column exact value sets, keyed by predicate column name. Values
    /// are stored as raw bytes so `Utf8`- and `Binary`-typed dictionaries
    /// compare equally to whichever `ScalarValue` variant the predicate
    /// literal happens to use (see [`normalize_literal`]).
    column_values: HashMap<String, HashSet<Vec<u8>>>,
}

impl DictionaryStatistics {
    /// Create an empty [`DictionaryStatistics`]
    pub fn new() -> Self {
        Default::default()
    }

    /// Create an empty [`DictionaryStatistics`] with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            column_values: HashMap::with_capacity(capacity),
        }
    }

    /// Record the exact dictionary values for `column`, decoded as a
    /// `Utf8` or `Binary` array (e.g. from
    /// `ParquetRecordBatchStreamBuilder::get_row_group_column_dictionary`).
    ///
    /// # Panics / Errors
    ///
    /// This does not itself verify that the column chunk is fully
    /// dictionary-encoded -- see [`is_fully_dictionary_encoded`]. Passing a
    /// dictionary that is only a subset of the chunk's actual values (e.g.
    /// because part of the chunk fell back to `PLAIN`) will make pruning
    /// unsound.
    pub fn insert(
        &mut self,
        column: impl Into<String>,
        dictionary: &ArrayRef,
    ) -> Result<()> {
        let values = match dictionary.data_type() {
            DataType::Utf8 => dictionary
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected a StringArray".to_string())
                })?
                .iter()
                .flatten()
                .map(|v| v.as_bytes().to_vec())
                .collect(),
            DataType::Binary => dictionary
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected a BinaryArray".to_string())
                })?
                .iter()
                .flatten()
                .map(|v| v.to_vec())
                .collect(),
            other => {
                return internal_err!(
                    "DictionaryStatistics only supports Utf8/Binary dictionaries, got {other}"
                );
            }
        };
        self.column_values.insert(column.into(), values);
        Ok(())
    }
}

/// Returns `value` normalized to its raw byte representation, if `value` is
/// a string- or binary-like scalar. Mirrors the literal normalization used
/// by [`crate::bloom_filter::BloomFilterStatistics`] so a predicate literal
/// (which may be `Utf8`, `Utf8View`, `LargeUtf8`, etc. depending on how the
/// query was planned) compares equal to a dictionary entry regardless of
/// which of those variants was used.
fn normalize_literal(value: &ScalarValue) -> Option<&[u8]> {
    match value {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => Some(v.as_bytes()),
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::LargeBinary(Some(v)) => Some(v.as_slice()),
        ScalarValue::Dictionary(_, inner) => normalize_literal(inner),
        _ => None,
    }
}

impl PruningStatistics for DictionaryStatistics {
    fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        None
    }

    /// Use the exact dictionary values to determine whether the column is
    /// definitely disjoint from (`Some(false)`), or definitely a subset of
    /// (`Some(true)`), `values`.
    ///
    /// Because the dictionary is the row group's *complete* set of distinct
    /// values (guaranteed by the caller checking
    /// [`is_fully_dictionary_encoded`] before inserting it), this can prove
    /// both directions exactly, unlike a bloom filter's probabilistic
    /// "definitely absent" only.
    fn contained(
        &self,
        column: &Column,
        values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        let dictionary_values = self.column_values.get(column.name.as_str())?;

        let mut literal_bytes: HashSet<&[u8]> = HashSet::with_capacity(values.len());
        for value in values {
            // A literal we can't normalize (e.g. a non-string/binary type
            // that somehow ended up compared against this column) makes the
            // guarantee inconclusive rather than wrong.
            literal_bytes.insert(normalize_literal(value)?);
        }

        let none_present = literal_bytes
            .iter()
            .all(|literal| !dictionary_values.contains(*literal));
        if none_present {
            return Some(BooleanArray::from(vec![Some(false)]));
        }

        let all_present = dictionary_values
            .iter()
            .all(|value| literal_bytes.contains(value.as_slice()));
        if all_present {
            return Some(BooleanArray::from(vec![Some(true)]));
        }

        Some(BooleanArray::from(vec![None]))
    }
}

/// Returns `true` if `col_meta`'s column chunk is entirely `BYTE_ARRAY` and
/// dictionary-encoded, i.e. its dictionary page is the exact, complete set of
/// the row group's distinct non-null values for that column.
///
/// Dictionary encoding is best-effort: writers fall back to `PLAIN` once a
/// column's dictionary grows past a size limit, and once that happens the
/// data pages after the fallback contain values that never entered the
/// dictionary. `dictionary_page_offset().is_some()` alone does not rule this
/// out. Page-encoding statistics
/// (<https://github.com/apache/parquet-format/pull/16>) record every
/// encoding used across a column chunk's data pages, so requiring the mask
/// to contain *only* a dictionary encoding proves every data page decoded
/// from the dictionary.
///
/// First cut: only `BYTE_ARRAY` (`Utf8`/`Binary`) columns are supported.
pub fn is_fully_dictionary_encoded(col_meta: &ColumnChunkMetaData) -> bool {
    col_meta.column_descr().physical_type() == Type::BYTE_ARRAY
        && col_meta.dictionary_page_offset().is_some()
        && col_meta.page_encoding_stats_mask().is_some_and(|mask| {
            mask.is_only(Encoding::PLAIN_DICTIONARY)
                || mask.is_only(Encoding::RLE_DICTIONARY)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::reader::ParquetFileReader;
    use crate::test_util::ExpectedPruning;
    use crate::{ParquetAccessPlan, ParquetFileMetrics, RowGroupAccessPlanFilter};

    use arrow::datatypes::{Field, Schema};
    use bytes::{BufMut, BytesMut};
    use datafusion_expr::{Expr, col, lit};
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion_pruning::PruningPredicate;
    use object_store::ObjectStoreExt;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;
    use parquet::arrow::parquet_column;
    use parquet::basic::EncodingMask;
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::file::properties::WriterProperties;
    use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};

    #[test]
    fn is_fully_dictionary_encoded_requires_only_dictionary_encoding() {
        let schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(
            SchemaType::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    SchemaType::primitive_type_builder("s", Type::BYTE_ARRAY)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        )));

        let fully_dictionary_encoded =
            ColumnChunkMetaData::builder(schema_descr.column(0))
                .set_dictionary_page_offset(Some(0))
                .set_data_page_offset(100)
                .set_page_encoding_stats_mask(EncodingMask::new_from_encodings(
                    [Encoding::RLE_DICTIONARY].iter(),
                ))
                .build()
                .unwrap();
        assert!(is_fully_dictionary_encoded(&fully_dictionary_encoded));

        // Plain fallback mid-chunk: a dictionary page exists, but at least
        // one data page used PLAIN, so the dictionary is not exhaustive.
        let plain_fallback = ColumnChunkMetaData::builder(schema_descr.column(0))
            .set_dictionary_page_offset(Some(0))
            .set_data_page_offset(100)
            .set_page_encoding_stats_mask(EncodingMask::new_from_encodings(
                [Encoding::RLE_DICTIONARY, Encoding::PLAIN].iter(),
            ))
            .build()
            .unwrap();
        assert!(!is_fully_dictionary_encoded(&plain_fallback));

        // No dictionary page at all.
        let no_dictionary = ColumnChunkMetaData::builder(schema_descr.column(0))
            .set_data_page_offset(100)
            .set_page_encoding_stats_mask(EncodingMask::new_from_encodings(
                [Encoding::PLAIN].iter(),
            ))
            .build()
            .unwrap();
        assert!(!is_fully_dictionary_encoded(&no_dictionary));

        // No page encoding stats recorded at all: treat as not prunable.
        let no_stats = ColumnChunkMetaData::builder(schema_descr.column(0))
            .set_dictionary_page_offset(Some(0))
            .set_data_page_offset(100)
            .build()
            .unwrap();
        assert!(!is_fully_dictionary_encoded(&no_stats));
    }

    struct DictionaryFilterTest {
        schema: Schema,
        post_pruning_row_groups: ExpectedPruning,
    }

    impl DictionaryFilterTest {
        /// A small dictionary-encoded string column with three distinct
        /// values, one row group per value so pruning is easy to verify.
        fn new_single_value_row_groups() -> (Self, bytes::Bytes) {
            let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
            let arrow_schema = Arc::new(schema.clone());
            let values = ["alpha", "beta", "gamma"];
            let array: ArrayRef = Arc::new(StringArray::from_iter_values(values));
            let batch =
                arrow::array::RecordBatch::try_new(arrow_schema.clone(), vec![array])
                    .unwrap();

            let props = WriterProperties::builder()
                .set_dictionary_enabled(true)
                .set_max_row_group_row_count(Some(1))
                .build();
            let mut out = BytesMut::new().writer();
            {
                let mut writer =
                    ArrowWriter::try_new(&mut out, arrow_schema, Some(props)).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            (
                Self {
                    schema,
                    post_pruning_row_groups: ExpectedPruning::None,
                },
                out.into_inner().freeze(),
            )
        }

        fn with_expect_all_pruned(mut self) -> Self {
            self.post_pruning_row_groups = ExpectedPruning::All;
            self
        }

        fn with_expect_some_pruned(mut self, remaining: Vec<usize>) -> Self {
            self.post_pruning_row_groups = ExpectedPruning::Some(remaining);
            self
        }

        async fn run(self, data: bytes::Bytes, expr: Expr) {
            let Self {
                schema,
                post_pruning_row_groups,
            } = self;

            let expr = logical2physical(&expr, &Arc::new(schema));
            let pruning_predicate = PruningPredicate::try_new(
                expr,
                Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
            )
            .unwrap();

            let pruned_row_groups =
                test_row_group_dictionary_pruning_predicate(data, &pruning_predicate)
                    .await
                    .unwrap();

            post_pruning_row_groups.assert(&pruned_row_groups);
        }
    }

    /// Evaluates the pruning predicate on the specified row groups and
    /// returns the row groups that are left, loading dictionaries exactly
    /// like [`crate::opener`]'s dictionary-loading stage does.
    async fn test_row_group_dictionary_pruning_predicate(
        data: bytes::Bytes,
        pruning_predicate: &PruningPredicate,
    ) -> Result<RowGroupAccessPlanFilter> {
        use datafusion_datasource::PartitionedFile;
        use object_store::ObjectMeta;

        let object_meta = ObjectMeta {
            location: object_store::path::Path::parse("test.parquet")
                .expect("creating path"),
            last_modified: chrono::DateTime::from(std::time::SystemTime::now()),
            size: data.len() as u64,
            e_tag: None,
            version: None,
        };
        let in_memory = object_store::memory::InMemory::new();
        in_memory
            .put(&object_meta.location, data.into())
            .await
            .expect("put parquet file into in memory object store");

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics =
            ParquetFileMetrics::new(0, object_meta.location.as_ref(), &metrics);
        let inner =
            ParquetObjectReader::new(Arc::new(in_memory), object_meta.location.clone())
                .with_file_size(object_meta.size);

        let partitioned_file = PartitionedFile::new_from_meta(object_meta);

        let reader = ParquetFileReader {
            inner,
            file_metrics: file_metrics.clone(),
            partitioned_file,
        };
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();

        let access_plan = ParquetAccessPlan::new_all(builder.metadata().num_row_groups());
        let mut pruned_row_groups = RowGroupAccessPlanFilter::new(access_plan);
        let literal_columns = pruning_predicate.literal_columns();
        let parquet_columns: Vec<_> = literal_columns
            .into_iter()
            .filter_map(|column_name| {
                let (column_idx, _) = parquet_column(
                    builder.parquet_schema(),
                    pruning_predicate.schema(),
                    &column_name,
                )?;
                Some((column_name.to_string(), column_idx))
            })
            .collect::<Vec<_>>();

        let num_row_groups = builder.metadata().num_row_groups();
        let mut row_group_dictionaries = Vec::with_capacity(num_row_groups);
        row_group_dictionaries.resize_with(num_row_groups, DictionaryStatistics::new);

        for idx in pruned_row_groups.row_group_indexes() {
            let mut dict_stats =
                DictionaryStatistics::with_capacity(parquet_columns.len());
            for (column_name, column_idx) in &parquet_columns {
                let col_meta = builder.metadata().row_group(idx).column(*column_idx);
                if !is_fully_dictionary_encoded(col_meta) {
                    continue;
                }
                let dict = match builder
                    .get_row_group_column_dictionary(idx, *column_idx)
                    .await
                {
                    Ok(Some(dict)) => dict,
                    Ok(None) => continue,
                    Err(e) => {
                        log::debug!("Ignoring error reading dictionary: {e}");
                        file_metrics.predicate_evaluation_errors.add(1);
                        continue;
                    }
                };
                dict_stats.insert(column_name, &dict).unwrap();
            }
            row_group_dictionaries[idx] = dict_stats;
        }
        pruned_row_groups.prune_by_dictionary(
            pruning_predicate,
            &file_metrics,
            &row_group_dictionaries,
        );

        Ok(pruned_row_groups)
    }

    #[tokio::test]
    async fn test_row_group_dictionary_pruning_predicate_absent_value() {
        let (test, data) = DictionaryFilterTest::new_single_value_row_groups();
        test.with_expect_all_pruned()
            .run(data, col("s").eq(lit("delta")))
            .await
    }

    #[tokio::test]
    async fn test_row_group_dictionary_pruning_predicate_in_list_absent() {
        let (test, data) = DictionaryFilterTest::new_single_value_row_groups();
        test.with_expect_all_pruned()
            .run(
                data,
                col("s").in_list(vec![lit("delta"), lit("epsilon")], false),
            )
            .await
    }

    #[tokio::test]
    async fn test_row_group_dictionary_pruning_predicate_present_value() {
        // Each row group has exactly one distinct value, so `s = 'beta'` only
        // survives in the row group whose sole value is "beta"; the other
        // two row groups are provably disjoint from {"beta"}.
        let (test, data) = DictionaryFilterTest::new_single_value_row_groups();
        test.with_expect_some_pruned(vec![1])
            .run(data, col("s").eq(lit("beta")))
            .await
    }

    #[tokio::test]
    async fn test_row_group_dictionary_pruning_predicate_not_eq_sole_value() {
        // Each row group has exactly one distinct value, so `s != <that
        // value>` can never be true within that row group -- this exercises
        // the `NOT IN`/`!=` direction that a bloom filter alone can't prove.
        let (test, data) = DictionaryFilterTest::new_single_value_row_groups();
        test.with_expect_some_pruned(vec![1, 2])
            .run(data, col("s").not_eq(lit("alpha")))
            .await
    }

    #[tokio::test]
    async fn test_row_group_dictionary_pruning_predicate_not_in_list() {
        let (test, data) = DictionaryFilterTest::new_single_value_row_groups();
        test.with_expect_some_pruned(vec![2])
            .run(
                data,
                col("s")
                    .not_eq(lit("alpha"))
                    .and(col("s").not_eq(lit("beta"))),
            )
            .await
    }

    #[tokio::test]
    async fn test_row_group_dictionary_pruning_predicate_plain_fallback_not_pruned() {
        // A large, high-cardinality dictionary forces PLAIN fallback, so the
        // encoding-stats gate should treat the row group as not prunable by
        // dictionary even though a query would otherwise be able to prune it
        // via exact statistics.
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
        let arrow_schema = Arc::new(schema.clone());
        let values: Vec<String> = (0..5000).map(|i| format!("value_{i}")).collect();
        let array: ArrayRef = Arc::new(StringArray::from_iter_values(
            values.iter().map(|v| v.as_str()),
        ));
        let batch = arrow::array::RecordBatch::try_new(arrow_schema.clone(), vec![array])
            .unwrap();

        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(64)
            .build();
        let mut out = BytesMut::new().writer();
        {
            let mut writer =
                ArrowWriter::try_new(&mut out, arrow_schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let data = out.into_inner().freeze();

        let test = DictionaryFilterTest {
            schema,
            post_pruning_row_groups: ExpectedPruning::None,
        };
        // Query a value written late in the column: fallback happens almost
        // immediately given the tiny size limit above, so this value is
        // almost certainly PLAIN-encoded. A broken gate that used only the
        // (small, abandoned) dictionary would incorrectly consider it absent
        // and prune the row group, even though the value is actually present.
        test.run(data, col("s").eq(lit("value_4999"))).await
    }
}
