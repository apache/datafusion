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

//! This file contains an end to end test of extracting statitics from parquet files.
//! It writes data into a parquet file, reads statistics and verifies they are correct

use std::default::Default;
use std::fs::File;
use std::sync::Arc;

use crate::parquet::{struct_array, Scenario};
use arrow::compute::kernels::cast_utils::Parser;
use arrow::datatypes::{
    i256, Date32Type, Date64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{
    make_array, Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::physical_plan::parquet::StatisticsConverter;
use half::f16;
use parquet::arrow::arrow_reader::{
    ArrowReaderBuilder, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use super::make_test_file_rg;

#[derive(Debug, Default, Clone)]
struct Int64Case {
    /// Number of nulls in the column
    null_values: usize,
    /// Non null values in the range `[no_null_values_start,
    /// no_null_values_end]`, one value for each row
    no_null_values_start: i64,
    no_null_values_end: i64,
    /// Number of rows per row group
    row_per_group: usize,
    /// if specified, overrides default statistics settings
    enable_stats: Option<EnabledStatistics>,
    /// If specified, the number of values in each page
    data_page_row_count_limit: Option<usize>,
}

impl Int64Case {
    /// Return a record batch with i64 with Null values
    /// The first no_null_values_end - no_null_values_start values
    /// are non-null with the specified range, the rest are null
    fn make_int64_batches_with_null(&self) -> RecordBatch {
        let schema =
            Arc::new(Schema::new(vec![Field::new("i64", DataType::Int64, true)]));

        let v64: Vec<i64> =
            (self.no_null_values_start as _..self.no_null_values_end as _).collect();

        RecordBatch::try_new(
            schema,
            vec![make_array(
                Int64Array::from_iter(
                    v64.into_iter()
                        .map(Some)
                        .chain(std::iter::repeat(None).take(self.null_values)),
                )
                .to_data(),
            )],
        )
        .unwrap()
    }

    // Create a parquet file with the specified settings
    pub fn build(&self) -> ParquetRecordBatchReaderBuilder<File> {
        let mut output_file = tempfile::Builder::new()
            .prefix("parquert_statistics_test")
            .suffix(".parquet")
            .tempfile()
            .expect("tempfile creation");

        let mut builder =
            WriterProperties::builder().set_max_row_group_size(self.row_per_group);
        if let Some(enable_stats) = self.enable_stats {
            builder = builder.set_statistics_enabled(enable_stats);
        }
        if let Some(data_page_row_count_limit) = self.data_page_row_count_limit {
            builder = builder.set_data_page_row_count_limit(data_page_row_count_limit);
        }
        let props = builder.build();

        let batches = vec![self.make_int64_batches_with_null()];

        let schema = batches[0].schema();

        let mut writer =
            ArrowWriter::try_new(&mut output_file, schema, Some(props)).unwrap();

        // if we have a datapage limit send the batches in one at a time to give
        // the writer a chance to be split into multiple pages
        if self.data_page_row_count_limit.is_some() {
            for batch in batches {
                for i in 0..batch.num_rows() {
                    writer.write(&batch.slice(i, 1)).expect("writing batch");
                }
            }
        } else {
            for batch in batches {
                writer.write(&batch).expect("writing batch");
            }
        }

        // close file
        let _file_meta = writer.close().unwrap();

        // open the file & get the reader
        let file = output_file.reopen().unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        ArrowReaderBuilder::try_new_with_options(file, options).unwrap()
    }
}

/// Defines what data to create in a parquet file
#[derive(Debug, Clone, Copy)]
struct TestReader {
    /// What data to create in the parquet file
    scenario: Scenario,
    /// Number of rows per row group
    row_per_group: usize,
}

impl TestReader {
    /// Create a parquet file with the specified data, and return a
    /// ParquetRecordBatchReaderBuilder opened to that file.
    async fn build(self) -> ParquetRecordBatchReaderBuilder<File> {
        let TestReader {
            scenario,
            row_per_group,
        } = self;
        let file = make_test_file_rg(scenario, row_per_group).await;

        // open the file & get the reader
        let file = file.reopen().unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        ArrowReaderBuilder::try_new_with_options(file, options).unwrap()
    }
}

/// Defines a test case for statistics extraction
struct Test<'a> {
    /// The parquet file reader
    reader: &'a ParquetRecordBatchReaderBuilder<File>,
    expected_min: ArrayRef,
    expected_max: ArrayRef,
    expected_null_counts: UInt64Array,
    expected_row_counts: UInt64Array,
    /// Which column to extract statistics from
    column_name: &'static str,
    /// If true, extracts and compares data page statistics rather than row
    /// group statistics
    test_data_page_statistics: bool,
}

impl<'a> Test<'a> {
    fn run(self) {
        let Self {
            reader,
            expected_min,
            expected_max,
            expected_null_counts,
            expected_row_counts,
            column_name,
            test_data_page_statistics,
        } = self;

        let converter = StatisticsConverter::try_new(
            column_name,
            reader.schema(),
            reader.parquet_schema(),
        )
        .unwrap();

        let row_groups = reader.metadata().row_groups();

        if test_data_page_statistics {
            let column_page_index = reader
                .metadata()
                .column_index()
                .expect("File should have column page indices");

            let column_offset_index = reader
                .metadata()
                .offset_index()
                .expect("File should have column offset indices");

            let row_group_indices = row_groups
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<Vec<_>>();

            let min = converter
                .data_page_mins(
                    column_page_index,
                    column_offset_index,
                    &row_group_indices,
                )
                .unwrap();
            assert_eq!(
                &min, &expected_min,
                "{column_name}: Mismatch with expected data page minimums"
            );

            let max = converter
                .data_page_maxes(
                    column_page_index,
                    column_offset_index,
                    &row_group_indices,
                )
                .unwrap();
            assert_eq!(
                &max, &expected_max,
                "{column_name}: Mismatch with expected data page maximum"
            );

            let null_counts = converter
                .data_page_null_counts(
                    column_page_index,
                    column_offset_index,
                    &row_group_indices,
                )
                .unwrap();

            let expected_null_counts = Arc::new(expected_null_counts) as ArrayRef;
            assert_eq!(
                &null_counts, &expected_null_counts,
                "{column_name}: Mismatch with expected data page null counts. \
                Actual: {null_counts:?}. Expected: {expected_null_counts:?}"
            );

            let row_counts = converter
                .data_page_row_counts(column_offset_index, row_groups, &row_group_indices)
                .unwrap();
            let expected_row_counts = Arc::new(expected_row_counts) as ArrayRef;
            assert_eq!(
                &row_counts, &expected_row_counts,
                "{column_name}: Mismatch with expected row counts. \
                Actual: {row_counts:?}. Expected: {expected_row_counts:?}"
            );
        } else {
            let min = converter.row_group_mins(row_groups).unwrap();
            assert_eq!(
                &min, &expected_min,
                "{column_name}: Mismatch with expected minimums"
            );

            let max = converter.row_group_maxes(row_groups).unwrap();
            assert_eq!(
                &max, &expected_max,
                "{column_name}: Mismatch with expected maximum"
            );

            let null_counts = converter.row_group_null_counts(row_groups).unwrap();
            let expected_null_counts = Arc::new(expected_null_counts) as ArrayRef;
            assert_eq!(
                &null_counts, &expected_null_counts,
                "{column_name}: Mismatch with expected null counts. \
                Actual: {null_counts:?}. Expected: {expected_null_counts:?}"
            );

            let row_counts = StatisticsConverter::row_group_row_counts(
                reader.metadata().row_groups().iter(),
            )
            .unwrap();
            assert_eq!(
                row_counts, expected_row_counts,
                "{column_name}: Mismatch with expected row counts. \
                Actual: {row_counts:?}. Expected: {expected_row_counts:?}"
            );
        }
    }

    /// Run the test and expect a column not found error
    fn run_col_not_found(self) {
        let Self {
            reader,
            expected_min: _,
            expected_max: _,
            expected_null_counts: _,
            expected_row_counts: _,
            column_name,
            ..
        } = self;

        let converter = StatisticsConverter::try_new(
            column_name,
            reader.schema(),
            reader.parquet_schema(),
        );

        assert!(converter.is_err());
    }
}

// TESTS
//
// Remaining cases
//   f64::NAN
// - Using truncated statistics  ("exact min value" and "exact max value" https://docs.rs/parquet/latest/parquet/file/statistics/enum.Statistics.html#method.max_is_exact)

#[tokio::test]
async fn test_one_row_group_without_null() {
    let reader = Int64Case {
        null_values: 0,
        no_null_values_start: 4,
        no_null_values_end: 7,
        row_per_group: 20,
        ..Default::default()
    }
    .build();

    Test {
        reader: &reader,
        // min is 4
        expected_min: Arc::new(Int64Array::from(vec![4])),
        // max is 6
        expected_max: Arc::new(Int64Array::from(vec![6])),
        // no nulls
        expected_null_counts: UInt64Array::from(vec![0]),
        // 3 rows
        expected_row_counts: UInt64Array::from(vec![3]),
        column_name: "i64",
        test_data_page_statistics: false,
    }
    .run()
}

#[tokio::test]
async fn test_one_row_group_with_null_and_negative() {
    let reader = Int64Case {
        null_values: 2,
        no_null_values_start: -1,
        no_null_values_end: 5,
        row_per_group: 20,
        ..Default::default()
    }
    .build();

    Test {
        reader: &reader,
        // min is -1
        expected_min: Arc::new(Int64Array::from(vec![-1])),
        // max is 4
        expected_max: Arc::new(Int64Array::from(vec![4])),
        // 2 nulls
        expected_null_counts: UInt64Array::from(vec![2]),
        // 8 rows
        expected_row_counts: UInt64Array::from(vec![8]),
        column_name: "i64",
        test_data_page_statistics: false,
    }
    .run()
}

#[tokio::test]
async fn test_two_row_group_with_null() {
    let reader = Int64Case {
        null_values: 2,
        no_null_values_start: 4,
        no_null_values_end: 17,
        row_per_group: 10,
        ..Default::default()
    }
    .build();

    Test {
        reader: &reader,
        // mins are [4, 14]
        expected_min: Arc::new(Int64Array::from(vec![4, 14])),
        // maxes are [13, 16]
        expected_max: Arc::new(Int64Array::from(vec![13, 16])),
        // nulls are [0, 2]
        expected_null_counts: UInt64Array::from(vec![0, 2]),
        // row counts are [10, 5]
        expected_row_counts: UInt64Array::from(vec![10, 5]),
        column_name: "i64",
        test_data_page_statistics: false,
    }
    .run()
}

#[tokio::test]
async fn test_two_row_groups_with_all_nulls_in_one() {
    let reader = Int64Case {
        null_values: 4,
        no_null_values_start: -2,
        no_null_values_end: 2,
        row_per_group: 5,
        ..Default::default()
    }
    .build();

    Test {
        reader: &reader,
        // mins are [-2, null]
        expected_min: Arc::new(Int64Array::from(vec![Some(-2), None])),
        // maxes are [1, null]
        expected_max: Arc::new(Int64Array::from(vec![Some(1), None])),
        // nulls are [1, 3]
        expected_null_counts: UInt64Array::from(vec![1, 3]),
        // row counts are [5, 3]
        expected_row_counts: UInt64Array::from(vec![5, 3]),
        column_name: "i64",
        test_data_page_statistics: false,
    }
    .run()
}

#[tokio::test]
async fn test_multiple_data_pages_nulls_and_negatives() {
    let reader = Int64Case {
        null_values: 3,
        no_null_values_start: -1,
        no_null_values_end: 10,
        row_per_group: 20,
        // limit page row count to 4
        data_page_row_count_limit: Some(4),
        enable_stats: Some(EnabledStatistics::Page),
    }
    .build();

    // Data layout looks like this:
    //
    // page 0: [-1, 0, 1, 2]
    // page 1: [3, 4, 5, 6]
    // page 2: [7, 8, 9, null]
    // page 3: [null, null]
    Test {
        reader: &reader,
        expected_min: Arc::new(Int64Array::from(vec![Some(-1), Some(3), Some(7), None])),
        expected_max: Arc::new(Int64Array::from(vec![Some(2), Some(6), Some(9), None])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 1, 2]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 2]),
        column_name: "i64",
        test_data_page_statistics: true,
    }
    .run()
}

/////////////// MORE GENERAL TESTS //////////////////////
// . Many columns in a file
// . Differnet data types
// . Different row group sizes

// Four different integer types
#[tokio::test]
async fn test_int_64() {
    // This creates a parquet files of 4 columns named "i8", "i16", "i32", "i64"
    let reader = TestReader {
        scenario: Scenario::Int,
        row_per_group: 5,
    }
    .build()
    .await;

    // since each row has only one data page, the statistics are the same
    for test_data_page_statistics in [true, false] {
        Test {
            reader: &reader,
            // mins are [-5, -4, 0, 5]
            expected_min: Arc::new(Int64Array::from(vec![-5, -4, 0, 5])),
            // maxes are [-1, 0, 4, 9]
            expected_max: Arc::new(Int64Array::from(vec![-1, 0, 4, 9])),
            // nulls are [0, 0, 0, 0]
            expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
            // row counts are [5, 5, 5, 5]
            expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
            column_name: "i64",
            test_data_page_statistics,
        }
        .run();
    }
}

#[tokio::test]
async fn test_int_32() {
    // This creates a parquet files of 4 columns named "i8", "i16", "i32", "i64"
    let reader = TestReader {
        scenario: Scenario::Int,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        // mins are [-5, -4, 0, 5]
        expected_min: Arc::new(Int32Array::from(vec![-5, -4, 0, 5])),
        // maxes are [-1, 0, 4, 9]
        expected_max: Arc::new(Int32Array::from(vec![-1, 0, 4, 9])),
        // nulls are [0, 0, 0, 0]
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "i32",
        test_data_page_statistics: false,
    }
    .run();
}

// BUG: ignore this test for now
// https://github.com/apache/datafusion/issues/10585
// Note that the file has 4 columns named "i8", "i16", "i32", "i64".
//   - The tests on column i32 and i64 passed.
//   - The tests on column i8 and i16 failed.
#[tokio::test]
async fn test_int_16() {
    // This creates a parquet files of 4 columns named "i8", "i16", "i32", "i64"
    let reader = TestReader {
        scenario: Scenario::Int,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        // mins are [-5, -4, 0, 5]
        // BUG: not sure why this returns same data but in Int32Array type even though I debugged and the columns name is "i16" an its data is Int16
        // My debugging tells me the bug is either at:
        //   1. The new code to get "iter". See the code in this PR with
        // // Get an iterator over the column statistics
        // let iter = row_groups
        // .iter()
        // .map(|x| x.column(parquet_idx).statistics());
        //    OR
        //   2. in the function (and/or its marco) `pub(crate) fn min_statistics<'a, I: Iterator<Item = Option<&'a ParquetStatistics>>>` here
        //      https://github.com/apache/datafusion/blob/ea023e2d4878240eece870cf4b346c7a0667aeed/datafusion/core/src/datasource/physical_plan/parquet/statistics.rs#L179
        expected_min: Arc::new(Int16Array::from(vec![-5, -4, 0, 5])), // panic here because the actual data is Int32Array
        // maxes are [-1, 0, 4, 9]
        expected_max: Arc::new(Int16Array::from(vec![-1, 0, 4, 9])),
        // nulls are [0, 0, 0, 0]
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "i16",
        test_data_page_statistics: false,
    }
    .run();
}

// BUG (same as above): ignore this test for now
// https://github.com/apache/datafusion/issues/10585
#[tokio::test]
async fn test_int_8() {
    // This creates a parquet files of 4 columns named "i8", "i16", "i32", "i64"
    let reader = TestReader {
        scenario: Scenario::Int,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        // mins are [-5, -4, 0, 5]
        // BUG: not sure why this returns same data but in Int32Array even though I debugged and the columns name is "i8" an its data is Int8
        expected_min: Arc::new(Int8Array::from(vec![-5, -4, 0, 5])), // panic here because the actual data is Int32Array
        // maxes are [-1, 0, 4, 9]
        expected_max: Arc::new(Int8Array::from(vec![-1, 0, 4, 9])),
        // nulls are [0, 0, 0, 0]
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "i8",
        test_data_page_statistics: false,
    }
    .run();
}

// timestamp
#[tokio::test]
async fn test_timestamp() {
    // This creates a parquet files of 9 columns named "nanos", "nanos_timezoned", "micros", "micros_timezoned", "millis", "millis_timezoned", "seconds", "seconds_timezoned", "names"
    // "nanos" --> TimestampNanosecondArray
    // "nanos_timezoned" --> TimestampNanosecondArray
    // "micros" --> TimestampMicrosecondArray
    // "micros_timezoned" --> TimestampMicrosecondArray
    // "millis" --> TimestampMillisecondArray
    // "millis_timezoned" --> TimestampMillisecondArray
    // "seconds" --> TimestampSecondArray
    // "seconds_timezoned" --> TimestampSecondArray
    // "names" --> StringArray
    //
    // The file is created by 4 record batches, each has 5 rowws.
    // Since the row group isze is set to 5, those 4 batches will go into 4 row groups
    // This creates a parquet files of 4 columns named "nanos", "nanos_timezoned", "micros", "micros_timezoned", "millis", "millis_timezoned", "seconds", "seconds_timezoned"
    let reader = TestReader {
        scenario: Scenario::Timestamps,
        row_per_group: 5,
    }
    .build()
    .await;

    let tz = "Pacific/Efate";

    Test {
        reader: &reader,
        expected_min: Arc::new(TimestampNanosecondArray::from(vec![
            TimestampNanosecondType::parse("2020-01-01T01:01:01"),
            TimestampNanosecondType::parse("2020-01-01T01:01:11"),
            TimestampNanosecondType::parse("2020-01-01T01:11:01"),
            TimestampNanosecondType::parse("2020-01-11T01:01:01"),
        ])),
        expected_max: Arc::new(TimestampNanosecondArray::from(vec![
            TimestampNanosecondType::parse("2020-01-02T01:01:01"),
            TimestampNanosecondType::parse("2020-01-02T01:01:11"),
            TimestampNanosecondType::parse("2020-01-02T01:11:01"),
            TimestampNanosecondType::parse("2020-01-12T01:01:01"),
        ])),
        // nulls are [1, 1, 1, 1]
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "nanos",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(
            TimestampNanosecondArray::from(vec![
                TimestampNanosecondType::parse("2020-01-01T01:01:01"),
                TimestampNanosecondType::parse("2020-01-01T01:01:11"),
                TimestampNanosecondType::parse("2020-01-01T01:11:01"),
                TimestampNanosecondType::parse("2020-01-11T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        expected_max: Arc::new(
            TimestampNanosecondArray::from(vec![
                TimestampNanosecondType::parse("2020-01-02T01:01:01"),
                TimestampNanosecondType::parse("2020-01-02T01:01:11"),
                TimestampNanosecondType::parse("2020-01-02T01:11:01"),
                TimestampNanosecondType::parse("2020-01-12T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        // nulls are [1, 1, 1, 1]
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "nanos_timezoned",
        test_data_page_statistics: false,
    }
    .run();

    // micros
    Test {
        reader: &reader,
        expected_min: Arc::new(TimestampMicrosecondArray::from(vec![
            TimestampMicrosecondType::parse("2020-01-01T01:01:01"),
            TimestampMicrosecondType::parse("2020-01-01T01:01:11"),
            TimestampMicrosecondType::parse("2020-01-01T01:11:01"),
            TimestampMicrosecondType::parse("2020-01-11T01:01:01"),
        ])),
        expected_max: Arc::new(TimestampMicrosecondArray::from(vec![
            TimestampMicrosecondType::parse("2020-01-02T01:01:01"),
            TimestampMicrosecondType::parse("2020-01-02T01:01:11"),
            TimestampMicrosecondType::parse("2020-01-02T01:11:01"),
            TimestampMicrosecondType::parse("2020-01-12T01:01:01"),
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "micros",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(
            TimestampMicrosecondArray::from(vec![
                TimestampMicrosecondType::parse("2020-01-01T01:01:01"),
                TimestampMicrosecondType::parse("2020-01-01T01:01:11"),
                TimestampMicrosecondType::parse("2020-01-01T01:11:01"),
                TimestampMicrosecondType::parse("2020-01-11T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        expected_max: Arc::new(
            TimestampMicrosecondArray::from(vec![
                TimestampMicrosecondType::parse("2020-01-02T01:01:01"),
                TimestampMicrosecondType::parse("2020-01-02T01:01:11"),
                TimestampMicrosecondType::parse("2020-01-02T01:11:01"),
                TimestampMicrosecondType::parse("2020-01-12T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        // nulls are [1, 1, 1, 1]
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "micros_timezoned",
        test_data_page_statistics: false,
    }
    .run();

    // millis
    Test {
        reader: &reader,
        expected_min: Arc::new(TimestampMillisecondArray::from(vec![
            TimestampMillisecondType::parse("2020-01-01T01:01:01"),
            TimestampMillisecondType::parse("2020-01-01T01:01:11"),
            TimestampMillisecondType::parse("2020-01-01T01:11:01"),
            TimestampMillisecondType::parse("2020-01-11T01:01:01"),
        ])),
        expected_max: Arc::new(TimestampMillisecondArray::from(vec![
            TimestampMillisecondType::parse("2020-01-02T01:01:01"),
            TimestampMillisecondType::parse("2020-01-02T01:01:11"),
            TimestampMillisecondType::parse("2020-01-02T01:11:01"),
            TimestampMillisecondType::parse("2020-01-12T01:01:01"),
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "millis",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(
            TimestampMillisecondArray::from(vec![
                TimestampMillisecondType::parse("2020-01-01T01:01:01"),
                TimestampMillisecondType::parse("2020-01-01T01:01:11"),
                TimestampMillisecondType::parse("2020-01-01T01:11:01"),
                TimestampMillisecondType::parse("2020-01-11T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        expected_max: Arc::new(
            TimestampMillisecondArray::from(vec![
                TimestampMillisecondType::parse("2020-01-02T01:01:01"),
                TimestampMillisecondType::parse("2020-01-02T01:01:11"),
                TimestampMillisecondType::parse("2020-01-02T01:11:01"),
                TimestampMillisecondType::parse("2020-01-12T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        // nulls are [1, 1, 1, 1]
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "millis_timezoned",
        test_data_page_statistics: false,
    }
    .run();

    // seconds
    Test {
        reader: &reader,
        expected_min: Arc::new(TimestampSecondArray::from(vec![
            TimestampSecondType::parse("2020-01-01T01:01:01"),
            TimestampSecondType::parse("2020-01-01T01:01:11"),
            TimestampSecondType::parse("2020-01-01T01:11:01"),
            TimestampSecondType::parse("2020-01-11T01:01:01"),
        ])),
        expected_max: Arc::new(TimestampSecondArray::from(vec![
            TimestampSecondType::parse("2020-01-02T01:01:01"),
            TimestampSecondType::parse("2020-01-02T01:01:11"),
            TimestampSecondType::parse("2020-01-02T01:11:01"),
            TimestampSecondType::parse("2020-01-12T01:01:01"),
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "seconds",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(
            TimestampSecondArray::from(vec![
                TimestampSecondType::parse("2020-01-01T01:01:01"),
                TimestampSecondType::parse("2020-01-01T01:01:11"),
                TimestampSecondType::parse("2020-01-01T01:11:01"),
                TimestampSecondType::parse("2020-01-11T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        expected_max: Arc::new(
            TimestampSecondArray::from(vec![
                TimestampSecondType::parse("2020-01-02T01:01:01"),
                TimestampSecondType::parse("2020-01-02T01:01:11"),
                TimestampSecondType::parse("2020-01-02T01:11:01"),
                TimestampSecondType::parse("2020-01-12T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        // nulls are [1, 1, 1, 1]
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "seconds_timezoned",
        test_data_page_statistics: false,
    }
    .run();
}

// timestamp with different row group sizes
#[tokio::test]
async fn test_timestamp_diff_rg_sizes() {
    // This creates a parquet files of 9 columns named "nanos", "nanos_timezoned", "micros", "micros_timezoned", "millis", "millis_timezoned", "seconds", "seconds_timezoned", "names"
    // "nanos" --> TimestampNanosecondArray
    // "nanos_timezoned" --> TimestampNanosecondArray
    // "micros" --> TimestampMicrosecondArray
    // "micros_timezoned" --> TimestampMicrosecondArray
    // "millis" --> TimestampMillisecondArray
    // "millis_timezoned" --> TimestampMillisecondArray
    // "seconds" --> TimestampSecondArray
    // "seconds_timezoned" --> TimestampSecondArray
    // "names" --> StringArray
    //
    // The file is created by 4 record batches (each has a null row), each has 5 rows but then will be split into 3 row groups with size 8, 8, 4
    let reader = TestReader {
        scenario: Scenario::Timestamps,
        row_per_group: 8, // note that the row group size is 8
    }
    .build()
    .await;

    let tz = "Pacific/Efate";

    Test {
        reader: &reader,
        expected_min: Arc::new(TimestampNanosecondArray::from(vec![
            TimestampNanosecondType::parse("2020-01-01T01:01:01"),
            TimestampNanosecondType::parse("2020-01-01T01:11:01"),
            TimestampNanosecondType::parse("2020-01-11T01:02:01"),
        ])),
        expected_max: Arc::new(TimestampNanosecondArray::from(vec![
            TimestampNanosecondType::parse("2020-01-02T01:01:01"),
            TimestampNanosecondType::parse("2020-01-11T01:01:01"),
            TimestampNanosecondType::parse("2020-01-12T01:01:01"),
        ])),
        // nulls are [1, 2, 1]
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        // row counts are [8, 8, 4]
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "nanos",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(
            TimestampNanosecondArray::from(vec![
                TimestampNanosecondType::parse("2020-01-01T01:01:01"),
                TimestampNanosecondType::parse("2020-01-01T01:11:01"),
                TimestampNanosecondType::parse("2020-01-11T01:02:01"),
            ])
            .with_timezone(tz),
        ),
        expected_max: Arc::new(
            TimestampNanosecondArray::from(vec![
                TimestampNanosecondType::parse("2020-01-02T01:01:01"),
                TimestampNanosecondType::parse("2020-01-11T01:01:01"),
                TimestampNanosecondType::parse("2020-01-12T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        // nulls are [1, 2, 1]
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        // row counts are [8, 8, 4]
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "nanos_timezoned",
        test_data_page_statistics: false,
    }
    .run();

    // micros
    Test {
        reader: &reader,
        expected_min: Arc::new(TimestampMicrosecondArray::from(vec![
            TimestampMicrosecondType::parse("2020-01-01T01:01:01"),
            TimestampMicrosecondType::parse("2020-01-01T01:11:01"),
            TimestampMicrosecondType::parse("2020-01-11T01:02:01"),
        ])),
        expected_max: Arc::new(TimestampMicrosecondArray::from(vec![
            TimestampMicrosecondType::parse("2020-01-02T01:01:01"),
            TimestampMicrosecondType::parse("2020-01-11T01:01:01"),
            TimestampMicrosecondType::parse("2020-01-12T01:01:01"),
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "micros",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(
            TimestampMicrosecondArray::from(vec![
                TimestampMicrosecondType::parse("2020-01-01T01:01:01"),
                TimestampMicrosecondType::parse("2020-01-01T01:11:01"),
                TimestampMicrosecondType::parse("2020-01-11T01:02:01"),
            ])
            .with_timezone(tz),
        ),
        expected_max: Arc::new(
            TimestampMicrosecondArray::from(vec![
                TimestampMicrosecondType::parse("2020-01-02T01:01:01"),
                TimestampMicrosecondType::parse("2020-01-11T01:01:01"),
                TimestampMicrosecondType::parse("2020-01-12T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        // nulls are [1, 2, 1]
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        // row counts are [8, 8, 4]
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "micros_timezoned",
        test_data_page_statistics: false,
    }
    .run();

    // millis
    Test {
        reader: &reader,
        expected_min: Arc::new(TimestampMillisecondArray::from(vec![
            TimestampMillisecondType::parse("2020-01-01T01:01:01"),
            TimestampMillisecondType::parse("2020-01-01T01:11:01"),
            TimestampMillisecondType::parse("2020-01-11T01:02:01"),
        ])),
        expected_max: Arc::new(TimestampMillisecondArray::from(vec![
            TimestampMillisecondType::parse("2020-01-02T01:01:01"),
            TimestampMillisecondType::parse("2020-01-11T01:01:01"),
            TimestampMillisecondType::parse("2020-01-12T01:01:01"),
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "millis",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(
            TimestampMillisecondArray::from(vec![
                TimestampMillisecondType::parse("2020-01-01T01:01:01"),
                TimestampMillisecondType::parse("2020-01-01T01:11:01"),
                TimestampMillisecondType::parse("2020-01-11T01:02:01"),
            ])
            .with_timezone(tz),
        ),
        expected_max: Arc::new(
            TimestampMillisecondArray::from(vec![
                TimestampMillisecondType::parse("2020-01-02T01:01:01"),
                TimestampMillisecondType::parse("2020-01-11T01:01:01"),
                TimestampMillisecondType::parse("2020-01-12T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        // nulls are [1, 2, 1]
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        // row counts are [8, 8, 4]
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "millis_timezoned",
        test_data_page_statistics: false,
    }
    .run();

    // seconds
    Test {
        reader: &reader,
        expected_min: Arc::new(TimestampSecondArray::from(vec![
            TimestampSecondType::parse("2020-01-01T01:01:01"),
            TimestampSecondType::parse("2020-01-01T01:11:01"),
            TimestampSecondType::parse("2020-01-11T01:02:01"),
        ])),
        expected_max: Arc::new(TimestampSecondArray::from(vec![
            TimestampSecondType::parse("2020-01-02T01:01:01"),
            TimestampSecondType::parse("2020-01-11T01:01:01"),
            TimestampSecondType::parse("2020-01-12T01:01:01"),
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "seconds",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(
            TimestampSecondArray::from(vec![
                TimestampSecondType::parse("2020-01-01T01:01:01"),
                TimestampSecondType::parse("2020-01-01T01:11:01"),
                TimestampSecondType::parse("2020-01-11T01:02:01"),
            ])
            .with_timezone(tz),
        ),
        expected_max: Arc::new(
            TimestampSecondArray::from(vec![
                TimestampSecondType::parse("2020-01-02T01:01:01"),
                TimestampSecondType::parse("2020-01-11T01:01:01"),
                TimestampSecondType::parse("2020-01-12T01:01:01"),
            ])
            .with_timezone(tz),
        ),
        // nulls are [1, 2, 1]
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        // row counts are [8, 8, 4]
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "seconds_timezoned",
        test_data_page_statistics: false,
    }
    .run();
}

// date with different row group sizes
#[tokio::test]
async fn test_dates_32_diff_rg_sizes() {
    // This creates a parquet files of 3 columns named "date32", "date64", "names"
    // "date32" --> Date32Array
    // "date64" --> Date64Array
    // "names" --> StringArray
    //
    // The file is created by 4 record batches (each has a null row), each has 5 rows but then will be split into 2 row groups with size 13, 7
    let reader = TestReader {
        scenario: Scenario::Dates,
        row_per_group: 13,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        // mins are [2020-01-01, 2020-10-30]
        expected_min: Arc::new(Date32Array::from(vec![
            Date32Type::parse("2020-01-01"),
            Date32Type::parse("2020-10-30"),
        ])),
        // maxes are [2020-10-29, 2029-11-12]
        expected_max: Arc::new(Date32Array::from(vec![
            Date32Type::parse("2020-10-29"),
            Date32Type::parse("2029-11-12"),
        ])),
        // nulls are [2, 2]
        expected_null_counts: UInt64Array::from(vec![2, 2]),
        // row counts are [13, 7]
        expected_row_counts: UInt64Array::from(vec![13, 7]),
        column_name: "date32",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_time32_second_diff_rg_sizes() {
    let reader = TestReader {
        scenario: Scenario::Time32Second,
        row_per_group: 4,
    }
    .build()
    .await;

    // Test for Time32Second column
    Test {
        reader: &reader,
        // Assuming specific minimum and maximum values for demonstration
        expected_min: Arc::new(Time32SecondArray::from(vec![18506, 18510, 18514, 18518])),
        expected_max: Arc::new(Time32SecondArray::from(vec![18509, 18513, 18517, 18521])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]), // Assuming 1 null per row group for simplicity
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4]),
        column_name: "second",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_time32_millisecond_diff_rg_sizes() {
    let reader = TestReader {
        scenario: Scenario::Time32Millisecond,
        row_per_group: 4,
    }
    .build()
    .await;

    // Test for Time32Millisecond column
    Test {
        reader: &reader,
        // Assuming specific minimum and maximum values for demonstration
        expected_min: Arc::new(Time32MillisecondArray::from(vec![
            3600000, 3600004, 3600008, 3600012,
        ])),
        expected_max: Arc::new(Time32MillisecondArray::from(vec![
            3600003, 3600007, 3600011, 3600015,
        ])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]), // Assuming 1 null per row group for simplicity
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4]),
        column_name: "millisecond",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_time64_microsecond_diff_rg_sizes() {
    let reader = TestReader {
        scenario: Scenario::Time64Microsecond,
        row_per_group: 4,
    }
    .build()
    .await;

    // Test for Time64MicroSecond column
    Test {
        reader: &reader,
        // Assuming specific minimum and maximum values for demonstration
        expected_min: Arc::new(Time64MicrosecondArray::from(vec![
            1234567890123,
            1234567890127,
            1234567890131,
            1234567890135,
        ])),
        expected_max: Arc::new(Time64MicrosecondArray::from(vec![
            1234567890126,
            1234567890130,
            1234567890134,
            1234567890138,
        ])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]), // Assuming 1 null per row group for simplicity
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4]),
        column_name: "microsecond",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_time64_nanosecond_diff_rg_sizes() {
    let reader = TestReader {
        scenario: Scenario::Time64Nanosecond,
        row_per_group: 4,
    }
    .build()
    .await;

    // Test for Time32Second column
    Test {
        reader: &reader,
        // Assuming specific minimum and maximum values for demonstration
        expected_min: Arc::new(Time64NanosecondArray::from(vec![
            987654321012345,
            987654321012349,
            987654321012353,
            987654321012357,
        ])),
        expected_max: Arc::new(Time64NanosecondArray::from(vec![
            987654321012348,
            987654321012352,
            987654321012356,
            987654321012360,
        ])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]), // Assuming 1 null per row group for simplicity
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4]),
        column_name: "nanosecond",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_dates_64_diff_rg_sizes() {
    // The file is created by 4 record batches (each has a null row), each has 5 rows but then will be split into 2 row groups with size 13, 7
    let reader = TestReader {
        scenario: Scenario::Dates,
        row_per_group: 13,
    }
    .build()
    .await;
    Test {
        reader: &reader,
        expected_min: Arc::new(Date64Array::from(vec![
            Date64Type::parse("2020-01-01"),
            Date64Type::parse("2020-10-30"),
        ])),
        expected_max: Arc::new(Date64Array::from(vec![
            Date64Type::parse("2020-10-29"),
            Date64Type::parse("2029-11-12"),
        ])),
        expected_null_counts: UInt64Array::from(vec![2, 2]),
        expected_row_counts: UInt64Array::from(vec![13, 7]),
        column_name: "date64",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_uint() {
    // This creates a parquet files of 4 columns named "u8", "u16", "u32", "u64"
    // "u8" --> UInt8Array
    // "u16" --> UInt16Array
    // "u32" --> UInt32Array
    // "u64" --> UInt64Array

    // The file is created by 4 record batches (each has a null row), each has 5 rows but then will be split into 5 row groups with size 4
    let reader = TestReader {
        scenario: Scenario::UInt,
        row_per_group: 4,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt8Array::from(vec![0, 1, 4, 7, 251])),
        expected_max: Arc::new(UInt8Array::from(vec![3, 4, 6, 250, 254])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4, 4]),
        column_name: "u8",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt16Array::from(vec![0, 1, 4, 7, 251])),
        expected_max: Arc::new(UInt16Array::from(vec![3, 4, 6, 250, 254])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4, 4]),
        column_name: "u16",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt32Array::from(vec![0, 1, 4, 7, 251])),
        expected_max: Arc::new(UInt32Array::from(vec![3, 4, 6, 250, 254])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4, 4]),
        column_name: "u32",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt64Array::from(vec![0, 1, 4, 7, 251])),
        expected_max: Arc::new(UInt64Array::from(vec![3, 4, 6, 250, 254])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4, 4]),
        column_name: "u64",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_int32_range() {
    // This creates a parquet file of 1 column "i"
    // file has 2 record batches, each has 2 rows. They will be saved into one row group
    let reader = TestReader {
        scenario: Scenario::Int32Range,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(Int32Array::from(vec![0])),
        expected_max: Arc::new(Int32Array::from(vec![300000])),
        expected_null_counts: UInt64Array::from(vec![0]),
        expected_row_counts: UInt64Array::from(vec![4]),
        column_name: "i",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_uint32_range() {
    // This creates a parquet file of 1 column "u"
    // file has 2 record batches, each has 2 rows. They will be saved into one row group
    let reader = TestReader {
        scenario: Scenario::UInt32Range,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt32Array::from(vec![0])),
        expected_max: Arc::new(UInt32Array::from(vec![300000])),
        expected_null_counts: UInt64Array::from(vec![0]),
        expected_row_counts: UInt64Array::from(vec![4]),
        column_name: "u",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_numeric_limits_unsigned() {
    // file has 7 rows, 2 row groups: one with 5 rows, one with 2 rows.
    let reader = TestReader {
        scenario: Scenario::NumericLimits,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt8Array::from(vec![u8::MIN, 100])),
        expected_max: Arc::new(UInt8Array::from(vec![100, u8::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "u8",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt16Array::from(vec![u16::MIN, 100])),
        expected_max: Arc::new(UInt16Array::from(vec![100, u16::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "u16",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt32Array::from(vec![u32::MIN, 100])),
        expected_max: Arc::new(UInt32Array::from(vec![100, u32::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "u32",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(UInt64Array::from(vec![u64::MIN, 100])),
        expected_max: Arc::new(UInt64Array::from(vec![100, u64::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "u64",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_numeric_limits_signed() {
    // file has 7 rows, 2 row groups: one with 5 rows, one with 2 rows.
    let reader = TestReader {
        scenario: Scenario::NumericLimits,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(Int8Array::from(vec![i8::MIN, -100])),
        expected_max: Arc::new(Int8Array::from(vec![100, i8::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "i8",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(Int16Array::from(vec![i16::MIN, -100])),
        expected_max: Arc::new(Int16Array::from(vec![100, i16::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "i16",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(Int32Array::from(vec![i32::MIN, -100])),
        expected_max: Arc::new(Int32Array::from(vec![100, i32::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "i32",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(Int64Array::from(vec![i64::MIN, -100])),
        expected_max: Arc::new(Int64Array::from(vec![100, i64::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "i64",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_numeric_limits_float() {
    // file has 7 rows, 2 row groups: one with 5 rows, one with 2 rows.
    let reader = TestReader {
        scenario: Scenario::NumericLimits,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(Float32Array::from(vec![f32::MIN, -100.0])),
        expected_max: Arc::new(Float32Array::from(vec![100.0, f32::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "f32",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(Float64Array::from(vec![f64::MIN, -100.0])),
        expected_max: Arc::new(Float64Array::from(vec![100.0, f64::MAX])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "f64",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(Float32Array::from(vec![-1.0, -100.0])),
        expected_max: Arc::new(Float32Array::from(vec![100.0, -100.0])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "f32_nan",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(Float64Array::from(vec![-1.0, -100.0])),
        expected_max: Arc::new(Float64Array::from(vec![100.0, -100.0])),
        expected_null_counts: UInt64Array::from(vec![0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "f64_nan",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_float64() {
    // This creates a parquet file of 1 column "f"
    // file has 4 record batches, each has 5 rows. They will be saved into 4 row groups
    let reader = TestReader {
        scenario: Scenario::Float64,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(Float64Array::from(vec![-5.0, -4.0, -0.0, 5.0])),
        expected_max: Arc::new(Float64Array::from(vec![-1.0, 0.0, 4.0, 9.0])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "f",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_float16() {
    // This creates a parquet file of 1 column "f"
    // file has 4 record batches, each has 5 rows. They will be saved into 4 row groups
    let reader = TestReader {
        scenario: Scenario::Float16,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(Float16Array::from(
            vec![-5.0, -4.0, -0.0, 5.0]
                .into_iter()
                .map(f16::from_f32)
                .collect::<Vec<_>>(),
        )),
        expected_max: Arc::new(Float16Array::from(
            vec![-1.0, 0.0, 4.0, 9.0]
                .into_iter()
                .map(f16::from_f32)
                .collect::<Vec<_>>(),
        )),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "f",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_decimal() {
    // This creates a parquet file of 1 column "decimal_col" with decimal data type and precicion 9, scale 2
    // file has 3 record batches, each has 5 rows. They will be saved into 3 row groups
    let reader = TestReader {
        scenario: Scenario::Decimal,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(
            Decimal128Array::from(vec![100, -500, 2000])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        ),
        expected_max: Arc::new(
            Decimal128Array::from(vec![600, 600, 6000])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        ),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "decimal_col",
        test_data_page_statistics: false,
    }
    .run();
}
#[tokio::test]
async fn test_decimal_256() {
    // This creates a parquet file of 1 column "decimal256_col" with decimal data type and precicion 9, scale 2
    // file has 3 record batches, each has 5 rows. They will be saved into 3 row groups
    let reader = TestReader {
        scenario: Scenario::Decimal256,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(
            Decimal256Array::from(vec![
                i256::from(100),
                i256::from(-500),
                i256::from(2000),
            ])
            .with_precision_and_scale(9, 2)
            .unwrap(),
        ),
        expected_max: Arc::new(
            Decimal256Array::from(vec![
                i256::from(600),
                i256::from(600),
                i256::from(6000),
            ])
            .with_precision_and_scale(9, 2)
            .unwrap(),
        ),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "decimal256_col",
        test_data_page_statistics: false,
    }
    .run();
}
#[tokio::test]
async fn test_dictionary() {
    let reader = TestReader {
        scenario: Scenario::Dictionary,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(StringArray::from(vec!["abc", "aaa"])),
        expected_max: Arc::new(StringArray::from(vec!["def", "fffff"])),
        expected_null_counts: UInt64Array::from(vec![1, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "string_dict_i8",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(StringArray::from(vec!["abc", "aaa"])),
        expected_max: Arc::new(StringArray::from(vec!["def", "fffff"])),
        expected_null_counts: UInt64Array::from(vec![1, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "string_dict_i32",
        test_data_page_statistics: false,
    }
    .run();

    Test {
        reader: &reader,
        expected_min: Arc::new(Int64Array::from(vec![-100, 0])),
        expected_max: Arc::new(Int64Array::from(vec![0, 100])),
        expected_null_counts: UInt64Array::from(vec![1, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 2]),
        column_name: "int_dict_i8",
        test_data_page_statistics: false,
    }
    .run();
}

#[tokio::test]
async fn test_byte() {
    // This creates a parquet file of 5 columns
    // "name"
    // "service_string"
    // "service_binary"
    // "service_fixedsize"
    // "service_large_binary"

    // file has 3 record batches, each has 5 rows. They will be saved into 3 row groups
    let reader = TestReader {
        scenario: Scenario::ByteArray,
        row_per_group: 5,
    }
    .build()
    .await;

    // column "name"
    Test {
        reader: &reader,
        expected_min: Arc::new(StringArray::from(vec![
            "all frontends",
            "mixed",
            "all backends",
        ])),
        expected_max: Arc::new(StringArray::from(vec![
            "all frontends",
            "mixed",
            "all backends",
        ])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "name",
        test_data_page_statistics: false,
    }
    .run();

    // column "service_string"
    Test {
        reader: &reader,
        expected_min: Arc::new(StringArray::from(vec![
            "frontend five",
            "backend one",
            "backend eight",
        ])),
        expected_max: Arc::new(StringArray::from(vec![
            "frontend two",
            "frontend six",
            "backend six",
        ])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "service_string",
        test_data_page_statistics: false,
    }
    .run();

    // column "service_binary"

    let expected_service_binary_min_values: Vec<&[u8]> =
        vec![b"frontend five", b"backend one", b"backend eight"];

    let expected_service_binary_max_values: Vec<&[u8]> =
        vec![b"frontend two", b"frontend six", b"backend six"];

    Test {
        reader: &reader,
        expected_min: Arc::new(BinaryArray::from(expected_service_binary_min_values)),
        expected_max: Arc::new(BinaryArray::from(expected_service_binary_max_values)),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "service_binary",
        test_data_page_statistics: false,
    }
    .run();

    // column "service_fixedsize"
    // b"fe1", b"be1", b"be4"
    let min_input = vec![vec![102, 101, 49], vec![98, 101, 49], vec![98, 101, 52]];
    // b"fe5", b"fe6", b"be8"
    let max_input = vec![vec![102, 101, 55], vec![102, 101, 54], vec![98, 101, 56]];

    Test {
        reader: &reader,
        expected_min: Arc::new(
            FixedSizeBinaryArray::try_from_iter(min_input.into_iter()).unwrap(),
        ),
        expected_max: Arc::new(
            FixedSizeBinaryArray::try_from_iter(max_input.into_iter()).unwrap(),
        ),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "service_fixedsize",
        test_data_page_statistics: false,
    }
    .run();

    let expected_service_large_binary_min_values: Vec<&[u8]> =
        vec![b"frontend five", b"backend one", b"backend eight"];

    let expected_service_large_binary_max_values: Vec<&[u8]> =
        vec![b"frontend two", b"frontend six", b"backend six"];

    Test {
        reader: &reader,
        expected_min: Arc::new(LargeBinaryArray::from(
            expected_service_large_binary_min_values,
        )),
        expected_max: Arc::new(LargeBinaryArray::from(
            expected_service_large_binary_max_values,
        )),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "service_large_binary",
        test_data_page_statistics: false,
    }
    .run();
}

// PeriodsInColumnNames
#[tokio::test]
async fn test_period_in_column_names() {
    // This creates a parquet file of 2 columns "name" and "service.name"
    // file has 3 record batches, each has 5 rows. They will be saved into 3 row groups
    let reader = TestReader {
        scenario: Scenario::PeriodsInColumnNames,
        row_per_group: 5,
    }
    .build()
    .await;

    // column "name"
    Test {
        reader: &reader,
        expected_min: Arc::new(StringArray::from(vec![
            "HTTP GET / DISPATCH",
            "HTTP PUT / DISPATCH",
            "HTTP GET / DISPATCH",
        ])),
        expected_max: Arc::new(StringArray::from(vec![
            "HTTP GET / DISPATCH",
            "HTTP PUT / DISPATCH",
            "HTTP GET / DISPATCH",
        ])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "name",
        test_data_page_statistics: false,
    }
    .run();

    // column "service.name"
    Test {
        reader: &reader,
        expected_min: Arc::new(StringArray::from(vec!["frontend", "backend", "backend"])),
        expected_max: Arc::new(StringArray::from(vec![
            "frontend", "frontend", "backend",
        ])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "service.name",
        test_data_page_statistics: false,
    }
    .run();
}

// Boolean
#[tokio::test]
async fn test_boolean() {
    // This creates a parquet files of 1 column named "bool"
    // The file is created by 2 record batches each has 5 rows --> 2 row groups
    let reader = TestReader {
        scenario: Scenario::Boolean,
        row_per_group: 5,
    }
    .build()
    .await;

    Test {
        reader: &reader,
        expected_min: Arc::new(BooleanArray::from(vec![false, false])),
        expected_max: Arc::new(BooleanArray::from(vec![true, false])),
        expected_null_counts: UInt64Array::from(vec![1, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5]),
        column_name: "bool",
        test_data_page_statistics: false,
    }
    .run();
}

// struct array
// BUG
// https://github.com/apache/datafusion/issues/10609
// Note that: since I have not worked on struct array before, there may be a bug in the test code rather than the real bug in the code
#[ignore]
#[tokio::test]
async fn test_struct() {
    // This creates a parquet files of 1 column named "struct"
    // The file is created by 1 record batch with 3 rows in the struct array
    let reader = TestReader {
        scenario: Scenario::StructArray,
        row_per_group: 5,
    }
    .build()
    .await;
    Test {
        reader: &reader,
        expected_min: Arc::new(struct_array(vec![(Some(1), Some(6.0), Some(12.0))])),
        expected_max: Arc::new(struct_array(vec![(Some(2), Some(8.5), Some(14.0))])),
        expected_null_counts: UInt64Array::from(vec![0]),
        expected_row_counts: UInt64Array::from(vec![3]),
        column_name: "struct",
        test_data_page_statistics: false,
    }
    .run();
}

// UTF8
#[tokio::test]
async fn test_utf8() {
    let reader = TestReader {
        scenario: Scenario::UTF8,
        row_per_group: 5,
    }
    .build()
    .await;

    // test for utf8
    Test {
        reader: &reader,
        expected_min: Arc::new(StringArray::from(vec!["a", "e"])),
        expected_max: Arc::new(StringArray::from(vec!["d", "i"])),
        expected_null_counts: UInt64Array::from(vec![1, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5]),
        column_name: "utf8",
        test_data_page_statistics: false,
    }
    .run();

    // test for large_utf8
    Test {
        reader: &reader,
        expected_min: Arc::new(LargeStringArray::from(vec!["a", "e"])),
        expected_max: Arc::new(LargeStringArray::from(vec!["d", "i"])),
        expected_null_counts: UInt64Array::from(vec![1, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5]),
        column_name: "large_utf8",
        test_data_page_statistics: false,
    }
    .run();
}

////// Files with missing statistics ///////

#[tokio::test]
async fn test_missing_statistics() {
    let reader = Int64Case {
        null_values: 0,
        no_null_values_start: 4,
        no_null_values_end: 7,
        row_per_group: 5,
        enable_stats: Some(EnabledStatistics::None),
        ..Default::default()
    }
    .build();

    Test {
        reader: &reader,
        expected_min: Arc::new(Int64Array::from(vec![None])),
        expected_max: Arc::new(Int64Array::from(vec![None])),
        expected_null_counts: UInt64Array::from(vec![None]),
        expected_row_counts: UInt64Array::from(vec![3]), // stil has row count statistics
        column_name: "i64",
        test_data_page_statistics: false,
    }
    .run();
}

/////// NEGATIVE TESTS ///////
// column not found
#[tokio::test]
async fn test_column_not_found() {
    let reader = TestReader {
        scenario: Scenario::Dates,
        row_per_group: 5,
    }
    .build()
    .await;
    Test {
        reader: &reader,
        expected_min: Arc::new(Int64Array::from(vec![18262, 18565])),
        expected_max: Arc::new(Int64Array::from(vec![18564, 21865])),
        expected_null_counts: UInt64Array::from(vec![2, 2]),
        expected_row_counts: UInt64Array::from(vec![13, 7]),
        column_name: "not_a_column",
        test_data_page_statistics: false,
    }
    .run_col_not_found();
}
