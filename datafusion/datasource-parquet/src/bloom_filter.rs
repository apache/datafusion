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

//! Loaded Parquet Split Block Bloom Filter (SBBF) data, with a
//! [`PruningStatistics`] adapter so the predicate-pruning machinery in
//! [`datafusion_pruning`] can consume it.

use std::collections::{HashMap, HashSet};

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use parquet::basic::Type;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::Decimal;

/// In memory Parquet Split Block Bloom Filters (SBBF).
///
/// This structure implements [`PruningStatistics`] and is used to prune
/// Parquet row groups and data pages based on the query predicate.
#[derive(Debug, Clone, Default)]
pub(crate) struct BloomFilterStatistics {
    /// Per-column Bloom filters keyed by predicate column name.
    column_sbbf: HashMap<String, ColumnBloomFilter>,
}

#[derive(Debug, Clone)]
struct ColumnBloomFilter {
    /// [`Sbbf`] (Bloom filter).
    sbbf: Sbbf,
    /// Parquet physical [`Type`] needed to evaluate literals against the filter.
    physical_type: Type,
    /// Type length from the Parquet column descriptor.
    type_length: i32,
}

impl BloomFilterStatistics {
    /// Create an empty [`BloomFilterStatistics`]
    pub(crate) fn new() -> Self {
        Default::default()
    }

    /// Create an empty [`BloomFilterStatistics`] with the specified capacity
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            column_sbbf: HashMap::with_capacity(capacity),
        }
    }

    /// Add a Bloom filter and type for the specified column
    pub(crate) fn insert(
        &mut self,
        column: impl Into<String>,
        sbbf: Sbbf,
        ty: Type,
        type_length: i32,
    ) {
        self.column_sbbf.insert(
            column.into(),
            ColumnBloomFilter {
                sbbf,
                physical_type: ty,
                type_length,
            },
        );
    }

    /// Helper function for checking if [`Sbbf`] filter contains [`ScalarValue`].
    ///
    /// In case the type of scalar is not supported, returns `true`, assuming that the
    /// value may be present.
    fn check_scalar(
        sbbf: &Sbbf,
        value: &ScalarValue,
        parquet_type: &Type,
        type_length: i32,
    ) -> bool {
        match value {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::Utf8View(Some(v))
            | ScalarValue::LargeUtf8(Some(v)) => sbbf.check(&v.as_str()),
            ScalarValue::Binary(Some(v))
            | ScalarValue::BinaryView(Some(v))
            | ScalarValue::LargeBinary(Some(v)) => sbbf.check(v),
            ScalarValue::FixedSizeBinary(_size, Some(v)) => sbbf.check(v),
            ScalarValue::Boolean(Some(v)) => sbbf.check(v),
            ScalarValue::Float64(Some(v)) => sbbf.check(v),
            ScalarValue::Float32(Some(v)) => sbbf.check(v),
            ScalarValue::Int64(Some(v)) => sbbf.check(v),
            ScalarValue::Int32(Some(v)) => sbbf.check(v),
            ScalarValue::UInt64(Some(v)) => sbbf.check(v),
            ScalarValue::UInt32(Some(v)) => sbbf.check(v),
            ScalarValue::Decimal128(Some(v), p, s) => match parquet_type {
                Type::INT32 => {
                    //https://github.com/apache/parquet-format/blob/eb4b31c1d64a01088d02a2f9aefc6c17c54cc6fc/Encodings.md?plain=1#L35-L42
                    // All physical type  are little-endian
                    if *p > 9 {
                        //DECIMAL can be used to annotate the following types:
                        //
                        // int32: for 1 <= precision <= 9
                        // int64: for 1 <= precision <= 18
                        return true;
                    }
                    let b = (*v as i32).to_le_bytes();
                    // Use Decimal constructor after https://github.com/apache/arrow-rs/issues/5325
                    let decimal = Decimal::Int32 {
                        value: b,
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                Type::INT64 => {
                    if *p > 18 {
                        return true;
                    }
                    let b = (*v as i64).to_le_bytes();
                    let decimal = Decimal::Int64 {
                        value: b,
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                Type::FIXED_LEN_BYTE_ARRAY => {
                    let Ok(type_length) = usize::try_from(type_length) else {
                        return true;
                    };
                    if type_length == 0 || type_length > 16 {
                        return true;
                    }
                    let b = v.to_be_bytes();
                    let b = b[(b.len() - type_length)..].to_vec();
                    // Use Decimal constructor after https://github.com/apache/arrow-rs/issues/5325
                    let decimal = Decimal::Bytes {
                        value: b.into(),
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                _ => true,
            },
            ScalarValue::Dictionary(_, inner) => BloomFilterStatistics::check_scalar(
                sbbf,
                inner,
                parquet_type,
                type_length,
            ),
            _ => true,
        }
    }
}

impl PruningStatistics for BloomFilterStatistics {
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

    /// Use bloom filters to determine if we are sure this column can not
    /// possibly contain `values`
    ///
    /// The `contained` API returns false if the bloom filters knows that *ALL*
    /// of the values in a column are not present.
    fn contained(
        &self,
        column: &Column,
        values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        let column_bloom_filter = self.column_sbbf.get(column.name.as_str())?;

        // Bloom filters are probabilistic data structures that can return false
        // positives (i.e. it might return true even if the value is not
        // present) however, the bloom filter will return `false` if the value is
        // definitely not present.

        let known_not_present = values
            .iter()
            .map(|value| {
                BloomFilterStatistics::check_scalar(
                    &column_bloom_filter.sbbf,
                    value,
                    &column_bloom_filter.physical_type,
                    column_bloom_filter.type_length,
                )
            })
            // The row group doesn't contain any of the values if
            // all the checks are false
            .all(|v| !v);

        let contains = if known_not_present {
            Some(false)
        } else {
            // Given the bloom filter is probabilistic, we can't be sure that
            // the row group actually contains the values. Return `None` to
            // indicate this uncertainty
            None
        };

        Some(BooleanArray::from(vec![contains]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::reader::ParquetFileReader;
    use crate::test_util::ExpectedPruning;
    use crate::{ParquetAccessPlan, ParquetFileMetrics, RowGroupAccessPlanFilter};

    use arrow::array::Decimal128Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::{BufMut, BytesMut};
    use datafusion_common::Result;
    use datafusion_expr::{Expr, col, lit};
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion_pruning::PruningPredicate;
    use object_store::ObjectStoreExt;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_simple_expr() {
        BloomFilterTest::new_data_index_bloom_encoding_stats()
            .with_expect_all_pruned()
            // generate pruning predicate `(String = "Hello_Not_exists")`
            .run(col(r#""String""#).eq(lit("Hello_Not_Exists")))
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_multiple_expr() {
        BloomFilterTest::new_data_index_bloom_encoding_stats()
            .with_expect_all_pruned()
            // generate pruning predicate `(String = "Hello_Not_exists" OR String = "Hello_Not_exists2")`
            .run(
                lit("1").eq(lit("1")).and(
                    col(r#""String""#)
                        .eq(lit("Hello_Not_Exists"))
                        .or(col(r#""String""#).eq(lit("Hello_Not_Exists2"))),
                ),
            )
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_multiple_expr_view() {
        BloomFilterTest::new_data_index_bloom_encoding_stats()
            .with_expect_all_pruned()
            // generate pruning predicate `(String = "Hello_Not_exists" OR String = "Hello_Not_exists2")`
            .run(
                lit("1").eq(lit("1")).and(
                    col(r#""String""#)
                        .eq(Expr::Literal(
                            ScalarValue::Utf8View(Some(String::from("Hello_Not_Exists"))),
                            None,
                        ))
                        .or(col(r#""String""#).eq(Expr::Literal(
                            ScalarValue::Utf8View(Some(String::from(
                                "Hello_Not_Exists2",
                            ))),
                            None,
                        ))),
                ),
            )
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_sql_in() {
        // load parquet file
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file_name = "data_index_bloom_encoding_stats.parquet";
        let path = format!("{testdata}/{file_name}");
        let data = bytes::Bytes::from(std::fs::read(path).unwrap());

        // generate pruning predicate
        let schema = Schema::new(vec![Field::new("String", DataType::Utf8, false)]);

        let expr = col(r#""String""#).in_list(
            (1..25)
                .map(|i| lit(format!("Hello_Not_Exists{i}")))
                .collect::<Vec<_>>(),
            false,
        );
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();

        let pruned_row_groups = test_row_group_bloom_filter_pruning_predicate(
            file_name,
            data,
            &pruning_predicate,
        )
        .await
        .unwrap();
        assert!(
            pruned_row_groups
                .access_plan()
                .row_group_indexes()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_with_exists_value() {
        BloomFilterTest::new_data_index_bloom_encoding_stats()
            .with_expect_none_pruned()
            // generate pruning predicate `(String = "Hello")`
            .run(col(r#""String""#).eq(lit("Hello")))
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_with_exists_2_values() {
        BloomFilterTest::new_data_index_bloom_encoding_stats()
            .with_expect_none_pruned()
            // generate pruning predicate `(String = "Hello") OR (String = "the quick")`
            .run(
                col(r#""String""#)
                    .eq(lit("Hello"))
                    .or(col(r#""String""#).eq(lit("the quick"))),
            )
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_with_exists_3_values() {
        BloomFilterTest::new_data_index_bloom_encoding_stats()
            .with_expect_none_pruned()
            // generate pruning predicate `(String = "Hello") OR (String = "the quick") OR (String = "are you")`
            .run(
                col(r#""String""#)
                    .eq(lit("Hello"))
                    .or(col(r#""String""#).eq(lit("the quick")))
                    .or(col(r#""String""#).eq(lit("are you"))),
            )
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_with_exists_3_values_view() {
        BloomFilterTest::new_data_index_bloom_encoding_stats()
            .with_expect_none_pruned()
            // generate pruning predicate `(String = "Hello") OR (String = "the quick") OR (String = "are you")`
            .run(
                col(r#""String""#)
                    .eq(Expr::Literal(
                        ScalarValue::Utf8View(Some(String::from("Hello"))),
                        None,
                    ))
                    .or(col(r#""String""#).eq(Expr::Literal(
                        ScalarValue::Utf8View(Some(String::from("the quick"))),
                        None,
                    )))
                    .or(col(r#""String""#).eq(Expr::Literal(
                        ScalarValue::Utf8View(Some(String::from("are you"))),
                        None,
                    ))),
            )
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_with_or_not_eq() {
        BloomFilterTest::new_data_index_bloom_encoding_stats()
            .with_expect_none_pruned()
            // generate pruning predicate `(String = "foo") OR (String != "bar")`
            .run(
                col(r#""String""#)
                    .not_eq(lit("foo"))
                    .or(col(r#""String""#).not_eq(lit("bar"))),
            )
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_without_bloom_filter() {
        // generate pruning predicate on a column without a bloom filter
        BloomFilterTest::new_all_types()
            .with_expect_none_pruned()
            .run(col(r#""string_col""#).eq(lit("0")))
            .await
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_decimal128() {
        for precision in [19, 20, 21, 28, 38] {
            let scale = 2;
            let data = parquet_decimal128_with_bloom_filter(
                precision,
                scale,
                vec![100, 200, 300, 400, 500, 600],
            );
            let schema = Schema::new(vec![Field::new(
                "decimal_col",
                DataType::Decimal128(precision, scale),
                true,
            )]);
            let expr = col("decimal_col").eq(Expr::Literal(
                ScalarValue::Decimal128(Some(500), precision, scale),
                None,
            ));
            let expr = logical2physical(&expr, &schema);
            let pruning_predicate =
                PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();

            let pruned_row_groups = test_row_group_bloom_filter_pruning_predicate(
                &format!("decimal128-{precision}.parquet"),
                data,
                &pruning_predicate,
            )
            .await
            .unwrap();

            assert_eq!(
                pruned_row_groups.access_plan().row_group_indexes(),
                vec![2],
                "precision {precision}"
            );
        }
    }

    #[tokio::test]
    async fn test_row_group_bloom_filter_pruning_predicate_negative_decimal128() {
        for precision in [19, 20, 21, 28, 38] {
            let scale = 2;
            let data = parquet_decimal128_with_bloom_filter(
                precision,
                scale,
                vec![-100, -200, -300, -400, -500, -600],
            );
            let schema = Schema::new(vec![Field::new(
                "decimal_col",
                DataType::Decimal128(precision, scale),
                true,
            )]);
            let expr = col("decimal_col").eq(Expr::Literal(
                ScalarValue::Decimal128(Some(-500), precision, scale),
                None,
            ));
            let expr = logical2physical(&expr, &schema);
            let pruning_predicate =
                PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();

            let pruned_row_groups = test_row_group_bloom_filter_pruning_predicate(
                &format!("negative-decimal128-{precision}.parquet"),
                data,
                &pruning_predicate,
            )
            .await
            .unwrap();

            assert_eq!(
                pruned_row_groups.access_plan().row_group_indexes(),
                vec![2],
                "precision {precision}"
            );
        }
    }

    struct BloomFilterTest {
        file_name: String,
        schema: Schema,
        // which row groups are expected to be left after pruning
        post_pruning_row_groups: ExpectedPruning,
    }

    impl BloomFilterTest {
        /// Return a test for data_index_bloom_encoding_stats.parquet
        /// Note the values in the `String` column are:
        /// ```sql
        /// > select * from './parquet-testing/data/data_index_bloom_encoding_stats.parquet';
        /// +-----------+
        /// | String    |
        /// +-----------+
        /// | Hello     |
        /// | This is   |
        /// | a         |
        /// | test      |
        /// | How       |
        /// | are you   |
        /// | doing     |
        /// | today     |
        /// | the quick |
        /// | brown fox |
        /// | jumps     |
        /// | over      |
        /// | the lazy  |
        /// | dog       |
        /// +-----------+
        /// ```
        fn new_data_index_bloom_encoding_stats() -> Self {
            Self {
                file_name: String::from("data_index_bloom_encoding_stats.parquet"),
                schema: Schema::new(vec![Field::new("String", DataType::Utf8, false)]),
                post_pruning_row_groups: ExpectedPruning::None,
            }
        }

        // Return a test for alltypes_plain.parquet
        fn new_all_types() -> Self {
            Self {
                file_name: String::from("alltypes_plain.parquet"),
                schema: Schema::new(vec![Field::new(
                    "string_col",
                    DataType::Utf8,
                    false,
                )]),
                post_pruning_row_groups: ExpectedPruning::None,
            }
        }

        /// Expect all row groups to be pruned
        pub fn with_expect_all_pruned(mut self) -> Self {
            self.post_pruning_row_groups = ExpectedPruning::All;
            self
        }

        /// Expect all row groups not to be pruned
        pub fn with_expect_none_pruned(mut self) -> Self {
            self.post_pruning_row_groups = ExpectedPruning::None;
            self
        }

        /// Prune this file using the specified expression and check that the expected row groups are left
        async fn run(self, expr: Expr) {
            let Self {
                file_name,
                schema,
                post_pruning_row_groups,
            } = self;

            let testdata = datafusion_common::test_util::parquet_test_data();
            let path = format!("{testdata}/{file_name}");
            let data = bytes::Bytes::from(std::fs::read(path).unwrap());

            let expr = logical2physical(&expr, &schema);
            let pruning_predicate =
                PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();

            let pruned_row_groups = test_row_group_bloom_filter_pruning_predicate(
                &file_name,
                data,
                &pruning_predicate,
            )
            .await
            .unwrap();

            post_pruning_row_groups.assert(&pruned_row_groups);
        }
    }

    fn parquet_decimal128_with_bloom_filter(
        precision: u8,
        scale: i8,
        values: Vec<i128>,
    ) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "decimal_col",
            DataType::Decimal128(precision, scale),
            true,
        )]));
        let array = Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ) as ArrayRef;
        let batch =
            arrow::array::RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .set_bloom_filter_enabled(true)
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let mut out = BytesMut::new().writer();
        {
            let mut writer = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        out.into_inner().freeze()
    }

    /// Evaluates the pruning predicate on the specified row groups and returns the row groups that are left
    async fn test_row_group_bloom_filter_pruning_predicate(
        file_name: &str,
        data: bytes::Bytes,
        pruning_predicate: &PruningPredicate,
    ) -> Result<RowGroupAccessPlanFilter> {
        use datafusion_datasource::PartitionedFile;
        use object_store::ObjectMeta;

        let object_meta = ObjectMeta {
            location: object_store::path::Path::parse(file_name).expect("creating path"),
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
                let (column_idx, _) = parquet::arrow::parquet_column(
                    builder.parquet_schema(),
                    pruning_predicate.schema(),
                    &column_name,
                )?;
                Some((
                    column_name.to_string(),
                    column_idx,
                    builder.parquet_schema().column(column_idx).physical_type(),
                    builder.parquet_schema().column(column_idx).type_length(),
                ))
            })
            .collect::<Vec<_>>();
        let mut row_group_bloom_filters =
            Vec::with_capacity(builder.metadata().num_row_groups());
        row_group_bloom_filters.resize_with(
            builder.metadata().num_row_groups(),
            BloomFilterStatistics::new,
        );
        for idx in pruned_row_groups.row_group_indexes() {
            let mut bloom_filters =
                BloomFilterStatistics::with_capacity(parquet_columns.len());
            for (column_name, column_idx, physical_type, type_length) in &parquet_columns
            {
                let bf = match builder
                    .get_row_group_column_bloom_filter(idx, *column_idx)
                    .await
                {
                    Ok(Some(bf)) => bf,
                    Ok(None) => continue,
                    Err(e) => {
                        log::debug!("Ignoring error reading bloom filter: {e}");
                        file_metrics.predicate_evaluation_errors.add(1);
                        continue;
                    }
                };
                bloom_filters.insert(
                    column_name.clone(),
                    bf,
                    *physical_type,
                    *type_length,
                );
            }
            row_group_bloom_filters[idx] = bloom_filters;
        }
        pruned_row_groups.prune_by_bloom_filters(
            pruning_predicate,
            &file_metrics,
            &row_group_bloom_filters,
        );

        Ok(pruned_row_groups)
    }
}
