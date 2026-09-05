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

//! Regression tests for Parquet bounds that do not follow Arrow's comparison order.

use std::io::Write;
use std::ops::Not;
use std::sync::Arc;

use arrow::array::{BooleanArray, Int32Array, RecordBatch, StringArray, record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use datafusion_common::pruning::{PrunableStatistics, PruningStatistics};
use datafusion_common::stats::Precision;
use datafusion_common::{Column, ScalarValue, Statistics};
use datafusion_expr::{Expr, col, lit};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::planner::logical2physical;
use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet};
use datafusion_pruning::{MAX_IN_LIST_SIZE, PruningPredicate, PruningPredicateBuilder};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{ColumnOrder, LogicalType, SortOrder, Type as PhysicalType};
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::file::metadata::{
    ColumnChunkMetaData, ColumnIndexBuilder, FileMetaData, OffsetIndexBuilder,
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter,
    RowGroupMetaData,
};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::statistics::Statistics as ParquetStatistics;
use parquet::file::writer::TrackedWrite;
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

use crate::RowGroupAccessPlanFilter;
use crate::metadata::{DFParquetMetadata, has_untrusted_min_max_order};
use crate::push_decoder::RowGroupPruner;
use crate::row_group_filter::RowGroupPruningStatistics;
use crate::{PagePruningAccessPlanFilter, ParquetAccessPlan, ParquetFileMetrics};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatisticsOrder {
    Modern,
    Deprecated,
    Missing,
    Unknown,
}

struct TestFile {
    bytes: Bytes,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
}

impl TestFile {
    fn floating(data_type: &DataType) -> Self {
        // The payloads remain distinct even after conversion to Float16.
        let nan = f64::from_bits(0x7ff8_2000_0000_0000);
        let other_nan = f64::from_bits(0x7ff8_4000_0000_0000);
        let values = [
            Some(1.0),
            Some(nan),
            Some(1.0),
            Some(other_nan),
            Some(1.0),
            Some(-nan),
            Some(1.0),
            Some(-other_nan),
            Some(1.0),
            Some(nan),
            None,
            Some(-nan),
            None,
            None,
            None,
            None,
        ]
        .into_iter()
        .map(|value| ScalarValue::Float64(value).cast_to(data_type).unwrap())
        .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", data_type.clone(), true),
            Field::new("n", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                ScalarValue::iter_to_array(values.clone()).unwrap(),
                Arc::new(Int32Array::from_iter_values(0..16)),
                Arc::new(StringArray::from_iter_values(
                    ["a", "b", "c", "d"].into_iter().flat_map(|s| [s; 4]),
                )),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), Some(properties))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = Bytes::from(bytes);
        let metadata = Arc::new(read_metadata(&bytes));
        assert_eq!(metadata.num_row_groups(), 4);
        let finite_bound = match data_type {
            DataType::Float16 => vec![0x00, 0x3c],
            DataType::Float32 => 1.0_f32.to_le_bytes().to_vec(),
            DataType::Float64 => 1.0_f64.to_le_bytes().to_vec(),
            _ => unreachable!(),
        };
        for group in metadata.row_groups().iter().take(3) {
            let stats = group.column(0).statistics().unwrap();
            assert_eq!(stats.min_bytes_opt().unwrap(), finite_bound);
            assert_eq!(stats.max_bytes_opt().unwrap(), finite_bound);
        }
        for group in metadata.offset_index().unwrap() {
            assert_eq!(group[0].page_locations.len(), 2);
        }

        // Verify the actual data pages preserve the signed NaN payloads and
        // nulls. ScalarValue equality compares floating-point bit patterns.
        let decoded = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .unwrap()
            .build()
            .unwrap()
            .flat_map(|batch| {
                let batch = batch.unwrap();
                (0..batch.num_rows())
                    .map(|row| {
                        ScalarValue::try_from_array(batch.column(0).as_ref(), row)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, values);
        Self {
            bytes,
            schema,
            metadata,
        }
    }

    fn new(order: StatisticsOrder) -> Self {
        let batch = record_batch!(
            (
                "s",
                Utf8,
                vec![
                    Some("aé"),
                    Some("az"),
                    Some("b"),
                    None,
                    None,
                    None,
                    Some("d"),
                    Some("e"),
                    Some("f"),
                ]
            ),
            ("n", Int32, [1, 2, 3, 10, 11, 12, 20, 21, 22])
        )
        .unwrap();
        let schema = batch.schema();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_data_page_row_count_limit(3)
            .set_write_batch_size(3)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut original = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut original, Arc::clone(&schema), Some(properties))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let original = Bytes::from(original);
        let metadata = read_metadata(&original);
        assert_eq!(metadata.num_row_groups(), 3);

        if order == StatisticsOrder::Modern {
            return Self {
                bytes: original,
                schema,
                metadata: Arc::new(metadata),
            };
        }

        // Signed-byte comparison gives ["aé", "b"] for the first row
        // group's ["aé", "az", "b"]. The endpoints are not inverted in
        // unsigned order, but the interval wrongly excludes "az".
        let mut row_groups = metadata.row_groups().to_vec();
        let mut columns = row_groups[0].columns().to_vec();
        columns[0] = columns[0]
            .clone()
            .into_builder()
            .set_statistics(ParquetStatistics::byte_array(
                Some(ByteArray::from("aé")),
                Some(ByteArray::from("b")),
                None,
                Some(0),
                order == StatisticsOrder::Deprecated,
            ))
            .build()
            .unwrap();
        row_groups[0] = row_groups[0]
            .clone()
            .into_builder()
            .set_column_metadata(columns)
            .build()
            .unwrap();

        let mut column_index = metadata.column_index().unwrap().clone();
        if matches!(order, StatisticsOrder::Missing | StatisticsOrder::Unknown) {
            let mut index = ColumnIndexBuilder::new(PhysicalType::BYTE_ARRAY);
            index.append(false, "aé".as_bytes().to_vec(), b"b".to_vec(), 0);
            column_index[0][0] = index.build().unwrap();
        }
        let metadata = metadata
            .into_builder()
            .set_row_groups(row_groups)
            .set_column_index(Some(column_index))
            .build();

        // Keep the real data pages, and serialize the replacement statistics
        // and page indexes at their actual file offsets.
        let mut bytes = Vec::new();
        let mut tracked = TrackedWrite::new(&mut bytes);
        tracked
            .write_all(&original[..footer_start(&original)])
            .unwrap();
        ParquetMetaDataWriter::new_with_tracked(tracked, &metadata)
            .finish()
            .unwrap();

        // parquet-rs always writes TYPEORDER and cannot write an unknown
        // ColumnOrder. The final Thrift field is column_orders (field 7): a
        // two-element list of unions, followed by the FileMetaData STOP.
        // Alter just that field to model old and future writers, then verify
        // the decoded footer below. No data or page-index offsets change.
        if matches!(order, StatisticsOrder::Missing | StatisticsOrder::Unknown) {
            let end = bytes.len() - 8;
            let encoded_orders = [0x19, 0x2c, 0x1c, 0, 0, 0x1c, 0, 0, 0];
            let start = end - encoded_orders.len();
            assert_eq!(&bytes[start..end], &encoded_orders);
            let metadata_start = footer_start(&bytes);
            if order == StatisticsOrder::Missing {
                bytes.drain(start..end - 1);
                let new_end = bytes.len() - 8;
                let metadata_len = (new_end - metadata_start) as u32;
                bytes[new_end..new_end + 4].copy_from_slice(&metadata_len.to_le_bytes());
            } else {
                // Change the first union member from field 1 (TYPEORDER) to
                // an unrecognized field 2. The numeric column stays known.
                bytes[start + 2] = 0x2c;
            }
        }

        let bytes = Bytes::from(bytes);
        let metadata = read_metadata(&bytes);
        let expected_order = match order {
            StatisticsOrder::Missing => ColumnOrder::UNDEFINED,
            StatisticsOrder::Unknown => ColumnOrder::UNKNOWN,
            _ => ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED),
        };
        assert_eq!(metadata.file_metadata().column_order(0), expected_order);
        assert_eq!(
            metadata
                .row_group(0)
                .column(0)
                .statistics()
                .unwrap()
                .is_min_max_deprecated(),
            order == StatisticsOrder::Deprecated,
        );
        Self {
            bytes,
            schema,
            metadata: Arc::new(metadata),
        }
    }

    fn predicate(&self, expr: &Expr) -> (Arc<dyn PhysicalExpr>, PruningPredicate) {
        let physical = logical2physical(expr, &self.schema);
        let pruning = PruningPredicateBuilder::new()
            .with_file_schema(Arc::clone(&self.schema))
            .try_build(Arc::clone(&physical))
            .unwrap();
        (physical, pruning)
    }

    fn statistics(&self) -> Statistics {
        DFParquetMetadata::statistics_from_parquet_metadata(&self.metadata, &self.schema)
            .unwrap()
    }

    fn file_matches(&self, predicate: &PruningPredicate) -> bool {
        let stats = PrunableStatistics::new(
            vec![Arc::new(self.statistics())],
            Arc::clone(&self.schema),
        );
        predicate.prune(&stats).unwrap()[0]
    }

    fn row_group_plan(&self, predicate: &PruningPredicate) -> ParquetAccessPlan {
        let mut filter = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(
            self.metadata.num_row_groups(),
        ));
        filter.prune_by_statistics_with_metadata(
            &self.schema,
            &self.metadata,
            predicate,
            &metrics(),
        );
        filter.build()
    }

    fn page_plan(
        &self,
        physical: &Arc<dyn PhysicalExpr>,
        plan: ParquetAccessPlan,
    ) -> ParquetAccessPlan {
        PagePruningAccessPlanFilter::new(physical, Arc::clone(&self.schema))
            .prune_plan_with_page_index(
                plan,
                &self.schema,
                self.metadata.file_metadata().schema_descr(),
                &self.metadata,
                &metrics(),
            )
    }

    fn matching_rows(
        &self,
        physical: &Arc<dyn PhysicalExpr>,
        plan: ParquetAccessPlan,
    ) -> usize {
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(self.bytes.clone())
            .unwrap()
            .with_row_groups(plan.row_group_indexes());
        if let Some(selection) = plan
            .into_overall_row_selection(self.metadata.row_groups())
            .unwrap()
        {
            builder = builder.with_row_selection(selection);
        }
        builder
            .build()
            .unwrap()
            .map(|batch| {
                let batch = batch.unwrap();
                let matches = physical
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap();
                matches
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .true_count()
            })
            .sum()
    }
}

fn footer_start(bytes: &[u8]) -> usize {
    let end = bytes.len() - 8;
    let metadata_len = u32::from_le_bytes(bytes[end..end + 4].try_into().unwrap());
    end - metadata_len as usize
}

fn read_metadata(bytes: &Bytes) -> ParquetMetaData {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(bytes)
        .unwrap()
}

fn metrics() -> ParquetFileMetrics {
    ParquetFileMetrics::new(
        0,
        "statistics-order.parquet",
        &ExecutionPlanMetricsSet::new(),
    )
}

fn assert_float_pruning(
    file: &TestFile,
    expr: &Expr,
    max_in_list_size: usize,
    expected: usize,
) {
    let physical = logical2physical(expr, &file.schema);
    let predicate = PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(&file.schema))
        .with_max_in_list_size(max_in_list_size)
        .try_build(Arc::clone(&physical))
        .unwrap();
    let context = format!(
        "type={}, cap={max_in_list_size}, expr={expr}",
        file.schema.field(0).data_type()
    );
    let all = ParquetAccessPlan::new_all(file.metadata.num_row_groups());
    // Always evaluate the physical residual against the original, unpruned
    // bytes. Pruning layers must not establish each other's expected result.
    assert_eq!(
        file.matching_rows(&physical, all.clone()),
        expected,
        "{context}"
    );
    if expected > 0 {
        assert!(file.file_matches(&predicate), "file: {context}");
    }
    let row_groups = file.row_group_plan(&predicate);
    assert_eq!(
        file.matching_rows(&physical, row_groups.clone()),
        expected,
        "row groups: {context}"
    );
    for index in row_groups.row_group_indexes() {
        if row_groups.is_fully_matched(index) {
            let mut group = ParquetAccessPlan::new_none(file.metadata.num_row_groups());
            group.scan(index);
            assert_eq!(
                file.matching_rows(&physical, group),
                file.metadata.row_group(index).num_rows() as usize,
                "fully matched row group {index}: {context}",
            );
        }
    }
    // Use the opener's configured cap and start the page-only path with all
    // row groups, so file/row-group guards cannot mask unsafe page bounds.
    let pages = crate::opener::build_page_pruning_predicate(
        &physical,
        &file.schema,
        max_in_list_size,
    );
    for (label, plan) in [("pages", all), ("row groups and pages", row_groups)] {
        let plan = pages.prune_plan_with_page_index(
            plan,
            &file.schema,
            file.metadata.file_metadata().schema_descr(),
            &file.metadata,
            &metrics(),
        );
        assert_eq!(
            file.matching_rows(&physical, plan),
            expected,
            "{label}: {context}"
        );
    }
}

#[test]
fn floating_nan_in_lists_preserve_rows_at_every_pruning_level() {
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        let file = TestFile::floating(&data_type);
        let value = |value| lit(ScalarValue::Float64(value).cast_to(&data_type).unwrap());
        let nan = f64::from_bits(0x7ff8_2000_0000_0000);
        for cap in [0, MAX_IN_LIST_SIZE, MAX_IN_LIST_SIZE + 1, 1024] {
            for size in [2, MAX_IN_LIST_SIZE + 1] {
                for nan in [nan, -nan] {
                    let mut values = (2..=size)
                        .map(|v| value(Some(v as f64)))
                        .collect::<Vec<_>>();
                    values.push(value(Some(nan)));
                    assert_float_pruning(
                        &file,
                        &col("f").in_list(values.clone(), false),
                        cap,
                        2,
                    );
                    values[0] = value(None);
                    assert_float_pruning(&file, &col("f").in_list(values, false), cap, 2);
                }
                // The footer/page bounds are [1, 1], but NaNs still satisfy
                // NOT IN (1, ...). Adding NULL makes all nonmatches unknown.
                let mut values = (1..=size)
                    .map(|v| value(Some(v as f64)))
                    .collect::<Vec<_>>();
                assert_float_pruning(
                    &file,
                    &col("f").in_list(values.clone(), true),
                    cap,
                    6,
                );
                values[1] = value(None);
                assert_float_pruning(&file, &col("f").in_list(values, true), cap, 0);
            }
        }
    }
}

#[test]
fn floating_nan_comparisons_do_not_prune_or_fully_match_finite_bounds() {
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        let file = TestFile::floating(&data_type);
        let value = |v| lit(ScalarValue::Float64(Some(v)).cast_to(&data_type).unwrap());
        let nan = f64::from_bits(0x7ff8_2000_0000_0000);
        for (expr, expected) in [
            (col("f").eq(value(1.0)), 5),
            (col("f").not_eq(value(1.0)), 6),
            (col("f").lt(value(1.0)), 3),
            (col("f").gt(value(1.0)), 3),
            (col("f").lt_eq(value(1.0)), 8),
            (col("f").gt_eq(value(1.0)), 8),
            (col("f").eq(value(nan)), 2),
            (col("f").eq(value(-nan)), 2),
            (col("f").eq(value(1.0)).not(), 6),
            (col("f").lt_eq(value(1.0)).not(), 3),
            (col("f").eq(value(nan)).or(col("f").eq(value(-nan))), 4),
        ] {
            assert_float_pruning(&file, &expr, MAX_IN_LIST_SIZE, expected);
        }
    }
}

#[test]
fn floating_bounds_keep_null_counts_and_other_column_statistics() {
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        let file = TestFile::floating(&data_type);
        let statistics = file.statistics();
        let float = &statistics.column_statistics[0];
        assert_eq!(float.min_value, Precision::Absent);
        assert_eq!(float.max_value, Precision::Absent);
        assert_eq!(float.null_count, Precision::Exact(5));
        assert_eq!(
            statistics.column_statistics[1].min_value,
            Precision::Exact(ScalarValue::Int32(Some(0)))
        );
        assert_eq!(
            statistics.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int32(Some(15)))
        );
        assert_eq!(
            statistics.column_statistics[2].min_value,
            Precision::Exact(ScalarValue::Utf8(Some("a".into())))
        );
        assert_eq!(
            statistics.column_statistics[2].max_value,
            Precision::Exact(ScalarValue::Utf8(Some("d".into())))
        );

        for (expr, groups, expected) in [
            (col("f").is_null(), vec![2, 3], 5),
            (col("f").is_not_null(), vec![0, 1, 2], 11),
            (
                col("f").eq(lit(ScalarValue::Float64(Some(1.0))
                    .cast_to(&data_type)
                    .unwrap())),
                vec![0, 1, 2],
                5,
            ),
        ] {
            let (physical, predicate) = file.predicate(&expr);
            assert_eq!(file.row_group_plan(&predicate).row_group_indexes(), groups);
            let pages = file.page_plan(&physical, ParquetAccessPlan::new_all(4));
            assert_eq!(pages.row_group_indexes(), groups);
            assert_eq!(file.matching_rows(&physical, pages), expected);
        }
        for expr in [col("n").eq(lit(99)), col("s").eq(lit("z"))] {
            let (physical, predicate) = file.predicate(&expr);
            assert!(!file.file_matches(&predicate));
            assert!(
                file.row_group_plan(&predicate)
                    .row_group_indexes()
                    .is_empty()
            );
            assert!(
                file.page_plan(&physical, ParquetAccessPlan::new_all(4))
                    .row_group_indexes()
                    .is_empty()
            );
        }
    }
}

#[test]
fn byte_array_order_preserves_matching_rows_at_every_pruning_level() {
    for order in [
        StatisticsOrder::Modern,
        StatisticsOrder::Deprecated,
        StatisticsOrder::Missing,
        StatisticsOrder::Unknown,
    ] {
        let file = TestFile::new(order);
        let (physical, predicate) = file.predicate(&col("s").eq(lit("az")));
        let all = ParquetAccessPlan::new_all(file.metadata.num_row_groups());
        assert_eq!(file.matching_rows(&physical, all.clone()), 1);
        assert!(file.file_matches(&predicate), "order={order:?}");
        let row_groups = file.row_group_plan(&predicate);
        assert!(row_groups.should_scan(0), "order={order:?}");
        assert!(!row_groups.is_fully_matched(0), "order={order:?}");
        assert_eq!(
            row_groups.row_group_indexes(),
            if matches!(order, StatisticsOrder::Modern | StatisticsOrder::Deprecated) {
                vec![0]
            } else {
                vec![0, 2]
            },
        );
        assert_eq!(file.matching_rows(&physical, row_groups.clone()), 1);
        assert_eq!(
            file.matching_rows(&physical, file.page_plan(&physical, all)),
            1
        );
        assert_eq!(
            file.matching_rows(&physical, file.page_plan(&physical, row_groups)),
            1,
            "order={order:?}",
        );

        let mut runtime_pruner = RowGroupPruner::new(
            physical,
            Arc::clone(&file.schema),
            Arc::clone(&file.metadata),
            Count::new(),
            Count::new(),
            MAX_IN_LIST_SIZE,
        );
        assert!(!runtime_pruner.should_prune(&[0]), "order={order:?}");
        assert!(runtime_pruner.should_prune(&[1]), "all-null row group");
    }
}

#[test]
fn large_string_in_list_preserves_rows_with_untrusted_page_order() {
    let max_in_list_size = MAX_IN_LIST_SIZE + 2;
    for order in [
        StatisticsOrder::Modern,
        StatisticsOrder::Missing,
        StatisticsOrder::Unknown,
    ] {
        let file = TestFile::new(order);
        // Only "az" occurs in the file. All other list members are above
        // even the unsafe ["aé", "b"] interval in the first page's index.
        let mut values = (0..=MAX_IN_LIST_SIZE)
            .map(|index| lit(format!("z{index:03}")))
            .collect::<Vec<_>>();
        values.push(lit("az"));
        let physical = logical2physical(&col("s").in_list(values, false), &file.schema);
        let predicate = PruningPredicateBuilder::new()
            .with_file_schema(Arc::clone(&file.schema))
            .with_max_in_list_size(max_in_list_size)
            .try_build(Arc::clone(&physical))
            .unwrap();
        assert!(
            predicate
                .predicate_expr()
                .to_string()
                .contains("IN_SET_INTERSECTS")
        );

        // Exercise the opener's configured-cap path, not the page filter's
        // compatibility constructor that retains the default limit of 20.
        let page_filter = crate::opener::build_page_pruning_predicate(
            &physical,
            &file.schema,
            max_in_list_size,
        );
        assert_eq!(page_filter.filter_number(), 1);
        let all = ParquetAccessPlan::new_all(file.metadata.num_row_groups());
        assert_eq!(file.matching_rows(&physical, all.clone()), 1);
        let file_metrics = metrics();
        let pages = page_filter.prune_plan_with_page_index(
            all,
            &file.schema,
            file.metadata.file_metadata().schema_descr(),
            &file.metadata,
            &file_metrics,
        );
        assert_eq!(
            pages.row_group_indexes(),
            if order == StatisticsOrder::Modern {
                vec![0]
            } else {
                vec![0, 2]
            },
            "order={order:?}",
        );
        assert_eq!(file.matching_rows(&physical, pages), 1, "order={order:?}");
        assert_eq!(
            file_metrics.page_index_rows_pruned.pruned(),
            if order == StatisticsOrder::Modern {
                6
            } else {
                3
            },
            "order={order:?}",
        );
    }
}

#[test]
fn byte_array_order_keeps_null_counts_and_unrelated_column_bounds() {
    for order in [
        StatisticsOrder::Deprecated,
        StatisticsOrder::Missing,
        StatisticsOrder::Unknown,
    ] {
        let file = TestFile::new(order);
        let statistics = file.statistics();
        let string = &statistics.column_statistics[0];
        assert_eq!(string.min_value, Precision::Absent, "order={order:?}");
        assert_eq!(string.max_value, Precision::Absent, "order={order:?}");
        assert_eq!(string.null_count, Precision::Exact(3));
        assert_eq!(
            statistics.column_statistics[1].min_value,
            Precision::Exact(ScalarValue::Int32(Some(1))),
        );
        assert_eq!(
            statistics.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int32(Some(22))),
        );

        let (physical, predicate) = file.predicate(&col("s").is_null());
        assert_eq!(file.row_group_plan(&predicate).row_group_indexes(), vec![1]);
        let page_plan = file.page_plan(&physical, ParquetAccessPlan::new_all(3));
        assert_eq!(page_plan.row_group_indexes(), vec![1]);
        assert_eq!(file.matching_rows(&physical, page_plan), 3);

        let expr = col("s").eq(lit("az")).and(col("n").eq(lit(99)));
        let (physical, predicate) = file.predicate(&expr);
        assert!(!file.file_matches(&predicate));
        assert!(
            file.row_group_plan(&predicate)
                .row_group_indexes()
                .is_empty()
        );
        assert!(
            file.page_plan(&physical, ParquetAccessPlan::new_all(3))
                .row_group_indexes()
                .is_empty(),
        );
    }
}

#[test]
fn byte_array_order_modern_bounds_and_null_only_groups_remain_usable() {
    let file = TestFile::new(StatisticsOrder::Modern);
    let statistics = file.statistics();
    assert_eq!(
        statistics.column_statistics[0].min_value,
        Precision::Exact(ScalarValue::Utf8(Some("az".to_owned()))),
    );
    let (physical, predicate) = file.predicate(&col("s").eq(lit("az")));
    assert_eq!(file.row_group_plan(&predicate).row_group_indexes(), vec![0]);
    assert_eq!(
        file.page_plan(&physical, ParquetAccessPlan::new_all(3))
            .row_group_indexes(),
        vec![0],
    );

    // The compatibility API has no footer. It still uses null counts, but
    // cannot assume that even modern-looking byte-array bounds are unsigned.
    let mut filter = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(3));
    filter.prune_by_statistics(
        &file.schema,
        file.metadata.file_metadata().schema_descr(),
        file.metadata.row_groups(),
        &predicate,
        &metrics(),
    );
    assert_eq!(filter.build().row_group_indexes(), vec![0, 2]);
}

#[test]
fn byte_array_order_guard_follows_parquet_type_not_arrow_representation() {
    let file = TestFile::new(StatisticsOrder::Deprecated);
    let metadata = file.metadata.file_metadata();
    let column = Column::from_name("s");
    for data_type in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
    ] {
        let schema = Schema::new(vec![Field::new("s", data_type, true)]);
        let stats = RowGroupPruningStatistics {
            parquet_schema: metadata.schema_descr(),
            column_orders: metadata.column_orders().map(Vec::as_slice),
            row_group_metadatas: file.metadata.row_groups().iter().collect(),
            arrow_schema: &schema,
            missing_null_counts_as_zero: true,
        };
        for values in [stats.min_values(&column), stats.max_values(&column)] {
            let values = values.unwrap();
            assert!(values.is_null(0));
            assert!(values.is_null(1));
            assert!(!values.is_null(2));
        }
        assert!(stats.null_counts(&column).is_some());
    }
}

fn single_column_metadata(
    parquet_type: ParquetType,
    statistics: ParquetStatistics,
    order: Option<ColumnOrder>,
) -> ParquetMetaData {
    let physical_type = parquet_type.get_physical_type();
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(
        ParquetType::group_type_builder("schema")
            .with_fields(vec![Arc::new(parquet_type)])
            .build()
            .unwrap(),
    )));
    let mut column_index = ColumnIndexBuilder::new(physical_type);
    column_index.append(
        false,
        statistics.min_bytes_opt().unwrap().to_vec(),
        statistics.max_bytes_opt().unwrap().to_vec(),
        0,
    );
    let mut offset_index = OffsetIndexBuilder::new();
    offset_index.append_row_count(3);
    offset_index.append_offset_and_size(0, 1);
    let column = ColumnChunkMetaData::builder(schema.column(0))
        .set_num_values(3)
        .set_statistics(statistics)
        .build()
        .unwrap();
    let group = RowGroupMetaData::builder(Arc::clone(&schema))
        .set_num_rows(3)
        .set_column_metadata(vec![column])
        .build()
        .unwrap();
    ParquetMetaData::new(
        FileMetaData::new(1, 3, None, None, schema, order.map(|order| vec![order])),
        vec![group],
    )
    .into_builder()
    .set_column_index(Some(vec![vec![column_index.build().unwrap()]]))
    .set_offset_index(Some(vec![vec![offset_index.build()]]))
    .build()
}

#[test]
fn fixed_byte_array_and_uuid_orders_guard_bounds_but_not_null_counts() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::FixedSizeBinary(16),
        true,
    )]));
    let padded = |prefix: &[u8]| {
        let mut value = vec![0; 16];
        value[..prefix.len()].copy_from_slice(prefix);
        value
    };
    let min = padded("aé".as_bytes());
    let max = padded(b"b");
    let value = ScalarValue::FixedSizeBinary(16, Some(padded(b"az")));
    let physical = logical2physical(&col("s").eq(lit(value)), &schema);
    let predicate = PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(&schema))
        .try_build(Arc::clone(&physical))
        .unwrap();

    for logical_type in [None, Some(LogicalType::Uuid)] {
        for deprecated in [false, true] {
            for order in [
                None,
                Some(ColumnOrder::UNKNOWN),
                Some(ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)),
                Some(ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED)),
            ] {
                let parquet_type = ParquetType::primitive_type_builder(
                    "s",
                    PhysicalType::FIXED_LEN_BYTE_ARRAY,
                )
                .with_length(16)
                .with_logical_type(logical_type.clone())
                .build()
                .unwrap();
                let statistics = ParquetStatistics::fixed_len_byte_array(
                    Some(FixedLenByteArray::from(min.clone())),
                    Some(FixedLenByteArray::from(max.clone())),
                    None,
                    Some(0),
                    deprecated,
                );
                let metadata = single_column_metadata(parquet_type, statistics, order);
                let trusted =
                    order == Some(ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED));
                let statistics = DFParquetMetadata::statistics_from_parquet_metadata(
                    &metadata, &schema,
                )
                .unwrap();
                assert_eq!(
                    statistics.column_statistics[0].min_value,
                    if trusted && !deprecated {
                        Precision::Exact(ScalarValue::FixedSizeBinary(
                            16,
                            Some(min.clone()),
                        ))
                    } else {
                        Precision::Absent
                    },
                );
                assert_eq!(
                    statistics.column_statistics[0].null_count,
                    Precision::Exact(0),
                );

                let mut row_groups =
                    RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(1));
                row_groups.prune_by_statistics_with_metadata(
                    &schema,
                    &metadata,
                    &predicate,
                    &metrics(),
                );
                assert_eq!(row_groups.build().should_scan(0), !trusted || deprecated);
                let pages =
                    PagePruningAccessPlanFilter::new(&physical, Arc::clone(&schema))
                        .prune_plan_with_page_index(
                            ParquetAccessPlan::new_all(1),
                            &schema,
                            metadata.file_metadata().schema_descr(),
                            &metadata,
                            &metrics(),
                        );
                // Modern page indexes are independent of legacy row-group
                // bounds, but still need a recognized footer order.
                assert_eq!(pages.should_scan(0), !trusted);
            }
        }
    }
}

#[test]
fn signed_decimal_byte_array_statistics_remain_usable() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Decimal128(10, 0),
        true,
    )]));
    for physical_type in [PhysicalType::BYTE_ARRAY, PhysicalType::FIXED_LEN_BYTE_ARRAY] {
        let parquet_type = ParquetType::primitive_type_builder("s", physical_type)
            .with_length(16)
            .with_logical_type(Some(LogicalType::decimal(0, 10)))
            .with_precision(10)
            .with_scale(0)
            .build()
            .unwrap();
        let min = (-2_i128).to_be_bytes().to_vec();
        let max = 3_i128.to_be_bytes().to_vec();
        let statistics = match physical_type {
            PhysicalType::BYTE_ARRAY => ParquetStatistics::byte_array(
                Some(ByteArray::from(min)),
                Some(ByteArray::from(max)),
                None,
                Some(0),
                true,
            ),
            _ => ParquetStatistics::fixed_len_byte_array(
                Some(FixedLenByteArray::from(min)),
                Some(FixedLenByteArray::from(max)),
                None,
                Some(0),
                true,
            ),
        };
        let metadata = single_column_metadata(parquet_type, statistics, None);
        let statistics =
            DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &schema)
                .unwrap();
        assert_eq!(
            statistics.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Decimal128(Some(-2), 10, 0)),
        );
        let physical = logical2physical(
            &col("s").eq(lit(ScalarValue::Decimal128(Some(4), 10, 0))),
            &schema,
        );
        let predicate = PruningPredicateBuilder::new()
            .with_file_schema(Arc::clone(&schema))
            .try_build(physical)
            .unwrap();
        let mut row_groups = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(1));
        row_groups.prune_by_statistics_with_metadata(
            &schema,
            &metadata,
            &predicate,
            &metrics(),
        );
        assert!(!row_groups.build().should_scan(0));
    }
}

#[test]
fn undefined_int96_order_is_never_trusted() {
    let parquet_type = ParquetType::primitive_type_builder("s", PhysicalType::INT96)
        .build()
        .unwrap();
    let schema = SchemaDescriptor::new(Arc::new(
        ParquetType::group_type_builder("schema")
            .with_fields(vec![Arc::new(parquet_type)])
            .build()
            .unwrap(),
    ));
    assert_eq!(schema.column(0).sort_order(), SortOrder::UNDEFINED);

    for order in [
        None,
        Some(ColumnOrder::UNDEFINED),
        Some(ColumnOrder::UNKNOWN),
        Some(ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED)),
        Some(ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)),
        Some(ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED)),
    ] {
        assert!(
            has_untrusted_min_max_order(
                &schema,
                order.as_ref().map(std::slice::from_ref),
                0
            ),
            "order={order:?}",
        );
    }
}

#[test]
fn undefined_logical_byte_array_order_is_not_a_bound() {
    let parquet_type = ParquetType::primitive_type_builder("s", PhysicalType::BYTE_ARRAY)
        .with_logical_type(Some(LogicalType::_Unknown { field_id: 100 }))
        .build()
        .unwrap();
    let statistics = ParquetStatistics::byte_array(
        Some(ByteArray::from("aé")),
        Some(ByteArray::from("b")),
        None,
        Some(0),
        false,
    );
    let metadata = single_column_metadata(
        parquet_type,
        statistics,
        Some(ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED)),
    );
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let file_statistics =
        DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &schema).unwrap();
    assert_eq!(
        file_statistics.column_statistics[0].min_value,
        Precision::Absent
    );
    assert_eq!(
        file_statistics.column_statistics[0].max_value,
        Precision::Absent
    );
    assert_eq!(
        file_statistics.column_statistics[0].null_count,
        Precision::Exact(0)
    );
    let physical = logical2physical(&col("s").eq(lit("az")), &schema);
    let predicate = PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(&schema))
        .try_build(Arc::clone(&physical))
        .unwrap();
    let mut row_groups = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(1));
    row_groups.prune_by_statistics_with_metadata(
        &schema,
        &metadata,
        &predicate,
        &metrics(),
    );
    assert!(row_groups.build().should_scan(0));
    let pages = PagePruningAccessPlanFilter::new(&physical, Arc::clone(&schema))
        .prune_plan_with_page_index(
            ParquetAccessPlan::new_all(1),
            &schema,
            metadata.file_metadata().schema_descr(),
            &metadata,
            &metrics(),
        );
    assert!(pages.should_scan(0));
}
