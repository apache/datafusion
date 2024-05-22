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

use std::fs::File;
use std::sync::Arc;

use arrow_array::{
    make_array, Array, ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch,
    StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::physical_plan::parquet::{
    RequestedStatistics, StatisticsConverter,
};
use parquet::arrow::arrow_reader::{ArrowReaderBuilder, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::parquet::{struct_array, Scenario};

use super::make_test_file_rg;

// TEST HELPERS

/// Return a record batch with i64 with Null values
fn make_int64_batches_with_null(
    null_values: usize,
    no_null_values_start: i64,
    no_null_values_end: i64,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("i64", DataType::Int64, true)]));

    let v64: Vec<i64> = (no_null_values_start as _..no_null_values_end as _).collect();

    RecordBatch::try_new(
        schema,
        vec![make_array(
            Int64Array::from_iter(
                v64.into_iter()
                    .map(Some)
                    .chain(std::iter::repeat(None).take(null_values)),
            )
            .to_data(),
        )],
    )
    .unwrap()
}

// Create a parquet file with one column for data type i64
// Data of the file include
//   . Number of null rows is the given num_null
//   . There are non-null values in the range [no_null_values_start, no_null_values_end], one value each row
//   . The file is divided into row groups of size row_per_group
pub fn parquet_file_one_column(
    num_null: usize,
    no_null_values_start: i64,
    no_null_values_end: i64,
    row_per_group: usize,
) -> ParquetRecordBatchReaderBuilder<File> {
    parquet_file_one_column_stats(
        num_null,
        no_null_values_start,
        no_null_values_end,
        row_per_group,
        EnabledStatistics::Chunk,
    )
}

// Create a parquet file with one column for data type i64
// Data of the file include
//   . Number of null rows is the given num_null
//   . There are non-null values in the range [no_null_values_start, no_null_values_end], one value each row
//   . The file is divided into row groups of size row_per_group
//  . Statistics are enabled/disabled based on the given enable_stats
pub fn parquet_file_one_column_stats(
    num_null: usize,
    no_null_values_start: i64,
    no_null_values_end: i64,
    row_per_group: usize,
    enable_stats: EnabledStatistics,
) -> ParquetRecordBatchReaderBuilder<File> {
    let mut output_file = tempfile::Builder::new()
        .prefix("parquert_statistics_test")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let props = WriterProperties::builder()
        .set_max_row_group_size(row_per_group)
        .set_statistics_enabled(enable_stats)
        .build();

    let batches = vec![make_int64_batches_with_null(
        num_null,
        no_null_values_start,
        no_null_values_end,
    )];

    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(&mut output_file, schema, Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }

    // close file
    let _file_meta = writer.close().unwrap();

    // open the file & get the reader
    let file = output_file.reopen().unwrap();
    ArrowReaderBuilder::try_new(file).unwrap()
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
        ArrowReaderBuilder::try_new(file).unwrap()
    }
}

/// Defines a test case for statistics extraction
struct Test {
    /// The parquet file reader
    reader: ParquetRecordBatchReaderBuilder<File>,
    expected_min: ArrayRef,
    expected_max: ArrayRef,
    expected_null_counts: UInt64Array,
    expected_row_counts: UInt64Array,
    /// Which column to extract statistics from
    column_name: &'static str,
}

impl Test {
    fn run(self) {
        let Self {
            reader,
            expected_min,
            expected_max,
            expected_null_counts,
            expected_row_counts,
            column_name,
        } = self;

        let min = StatisticsConverter::try_new(
            column_name,
            RequestedStatistics::Min,
            reader.schema(),
        )
        .unwrap()
        .extract(reader.metadata())
        .unwrap();

        assert_eq!(&min, &expected_min, "Mismatch with expected minimums");

        let max = StatisticsConverter::try_new(
            column_name,
            RequestedStatistics::Max,
            reader.schema(),
        )
        .unwrap()
        .extract(reader.metadata())
        .unwrap();
        assert_eq!(&max, &expected_max, "Mismatch with expected maximum");

        let null_counts = StatisticsConverter::try_new(
            column_name,
            RequestedStatistics::NullCount,
            reader.schema(),
        )
        .unwrap()
        .extract(reader.metadata())
        .unwrap();
        let expected_null_counts = Arc::new(expected_null_counts) as ArrayRef;
        assert_eq!(
            &null_counts, &expected_null_counts,
            "Mismatch with expected null counts"
        );

        let row_counts = StatisticsConverter::row_counts(reader.metadata()).unwrap();
        assert_eq!(
            row_counts, expected_row_counts,
            "Mismatch with expected row counts"
        );
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
        } = self;

        let min = StatisticsConverter::try_new(
            column_name,
            RequestedStatistics::Min,
            reader.schema(),
        );

        assert!(min.is_err());
    }
}

// TESTS
//
// Remaining cases
//   f64::NAN
// - Using truncated statistics  ("exact min value" and "exact max value" https://docs.rs/parquet/latest/parquet/file/statistics/enum.Statistics.html#method.max_is_exact)

#[tokio::test]
async fn test_one_row_group_without_null() {
    let row_per_group = 20;
    let reader = parquet_file_one_column(0, 4, 7, row_per_group);
    Test {
        reader,
        // min is 4
        expected_min: Arc::new(Int64Array::from(vec![4])),
        // max is 6
        expected_max: Arc::new(Int64Array::from(vec![6])),
        // no nulls
        expected_null_counts: UInt64Array::from(vec![0]),
        // 3 rows
        expected_row_counts: UInt64Array::from(vec![3]),
        column_name: "i64",
    }
    .run()
}

#[tokio::test]
async fn test_one_row_group_with_null_and_negative() {
    let row_per_group = 20;
    let reader = parquet_file_one_column(2, -1, 5, row_per_group);

    Test {
        reader,
        // min is -1
        expected_min: Arc::new(Int64Array::from(vec![-1])),
        // max is 4
        expected_max: Arc::new(Int64Array::from(vec![4])),
        // 2 nulls
        expected_null_counts: UInt64Array::from(vec![2]),
        // 8 rows
        expected_row_counts: UInt64Array::from(vec![8]),
        column_name: "i64",
    }
    .run()
}

#[tokio::test]
async fn test_two_row_group_with_null() {
    let row_per_group = 10;
    let reader = parquet_file_one_column(2, 4, 17, row_per_group);

    Test {
        reader,
        // mins are [4, 14]
        expected_min: Arc::new(Int64Array::from(vec![4, 14])),
        // maxes are [13, 16]
        expected_max: Arc::new(Int64Array::from(vec![13, 16])),
        // nulls are [0, 2]
        expected_null_counts: UInt64Array::from(vec![0, 2]),
        // row counts are [10, 5]
        expected_row_counts: UInt64Array::from(vec![10, 5]),
        column_name: "i64",
    }
    .run()
}

#[tokio::test]
async fn test_two_row_groups_with_all_nulls_in_one() {
    let row_per_group = 5;
    let reader = parquet_file_one_column(4, -2, 2, row_per_group);

    Test {
        reader,
        // mins are [-2, null]
        expected_min: Arc::new(Int64Array::from(vec![Some(-2), None])),
        // maxes are [1, null]
        expected_max: Arc::new(Int64Array::from(vec![Some(1), None])),
        // nulls are [1, 3]
        expected_null_counts: UInt64Array::from(vec![1, 3]),
        // row counts are [5, 3]
        expected_row_counts: UInt64Array::from(vec![5, 3]),
        column_name: "i64",
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
    };

    Test {
        reader: reader.build().await,
        // mins are [-5, -4, 0, 5]
        expected_min: Arc::new(Int64Array::from(vec![-5, -4, 0, 5])),
        // maxes are [-1, 0, 4, 9]
        expected_max: Arc::new(Int64Array::from(vec![-1, 0, 4, 9])),
        // nulls are [0, 0, 0, 0]
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "i64",
    }
    .run();
}

#[tokio::test]
async fn test_int_32() {
    // This creates a parquet files of 4 columns named "i8", "i16", "i32", "i64"
    let reader = TestReader {
        scenario: Scenario::Int,
        row_per_group: 5,
    };

    Test {
        reader: reader.build().await,
        // mins are [-5, -4, 0, 5]
        expected_min: Arc::new(Int32Array::from(vec![-5, -4, 0, 5])),
        // maxes are [-1, 0, 4, 9]
        expected_max: Arc::new(Int32Array::from(vec![-1, 0, 4, 9])),
        // nulls are [0, 0, 0, 0]
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "i32",
    }
    .run();
}

// BUG: ignore this test for now
// https://github.com/apache/datafusion/issues/10585
// Note that the file has 4 columns named "i8", "i16", "i32", "i64".
//   - The tests on column i32 and i64 passed.
//   - The tests on column i8 and i16 failed.
#[ignore]
#[tokio::test]
async fn test_int_16() {
    // This creates a parquet files of 4 columns named "i8", "i16", "i32", "i64"
    let reader = TestReader {
        scenario: Scenario::Int,
        row_per_group: 5,
    };

    Test {
        reader: reader.build().await,
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
    }
    .run();
}

// BUG (same as above): ignore this test for now
// https://github.com/apache/datafusion/issues/10585
#[ignore]
#[tokio::test]
async fn test_int_8() {
    // This creates a parquet files of 4 columns named "i8", "i16", "i32", "i64"
    let reader = TestReader {
        scenario: Scenario::Int,
        row_per_group: 5,
    };

    Test {
        reader: reader.build().await,
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
    }
    .run();
}

// timestamp
#[tokio::test]
async fn test_timestamp() {
    // This creates a parquet files of 5 columns named "nanos", "micros", "millis", "seconds", "names"
    // "nanos" --> TimestampNanosecondArray
    // "micros" --> TimestampMicrosecondArray
    // "millis" --> TimestampMillisecondArray
    // "seconds" --> TimestampSecondArray
    // "names" --> StringArray
    //
    // The file is created by 4 record batches, each has 5 rowws.
    // Since the row group isze is set to 5, those 4 batches will go into 4 row groups
    // This creates a parquet files of 4 columns named "i8", "i16", "i32", "i64"
    let reader = TestReader {
        scenario: Scenario::Timestamps,
        row_per_group: 5,
    };

    Test {
        reader: reader.build().await,
        // mins are [1577840461000000000, 1577840471000000000, 1577841061000000000, 1578704461000000000,]
        expected_min: Arc::new(Int64Array::from(vec![
            1577840461000000000,
            1577840471000000000,
            1577841061000000000,
            1578704461000000000,
        ])),
        // maxes are [1577926861000000000, 1577926871000000000, 1577927461000000000, 1578790861000000000,]
        expected_max: Arc::new(Int64Array::from(vec![
            1577926861000000000,
            1577926871000000000,
            1577927461000000000,
            1578790861000000000,
        ])),
        // nulls are [1, 1, 1, 1]
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        // row counts are [5, 5, 5, 5]
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "nanos",
    }
    .run();

    // micros

    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![
            1577840461000000,
            1577840471000000,
            1577841061000000,
            1578704461000000,
        ])),
        expected_max: Arc::new(Int64Array::from(vec![
            1577926861000000,
            1577926871000000,
            1577927461000000,
            1578790861000000,
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "micros",
    }
    .run();

    // millis
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![
            1577840461000,
            1577840471000,
            1577841061000,
            1578704461000,
        ])),
        expected_max: Arc::new(Int64Array::from(vec![
            1577926861000,
            1577926871000,
            1577927461000,
            1578790861000,
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "millis",
    }
    .run();

    // seconds
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![
            1577840461, 1577840471, 1577841061, 1578704461,
        ])),
        expected_max: Arc::new(Int64Array::from(vec![
            1577926861, 1577926871, 1577927461, 1578790861,
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 1, 1, 1]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "seconds",
    }
    .run();
}

// timestamp with different row group sizes
#[tokio::test]
async fn test_timestamp_diff_rg_sizes() {
    // This creates a parquet files of 5 columns named "nanos", "micros", "millis", "seconds", "names"
    // "nanos" --> TimestampNanosecondArray
    // "micros" --> TimestampMicrosecondArray
    // "millis" --> TimestampMillisecondArray
    // "seconds" --> TimestampSecondArray
    // "names" --> StringArray
    //
    // The file is created by 4 record batches (each has a null row), each has 5 rows but then will be split into 3 row groups with size 8, 8, 4
    let reader = TestReader {
        scenario: Scenario::Timestamps,
        row_per_group: 8, // note that the row group size is 8
    };

    Test {
        reader: reader.build().await,
        // mins are [1577840461000000000, 1577841061000000000, 1578704521000000000]
        expected_min: Arc::new(Int64Array::from(vec![
            1577840461000000000,
            1577841061000000000,
            1578704521000000000,
        ])),
        // maxes are [1577926861000000000, 1578704461000000000, 157879086100000000]
        expected_max: Arc::new(Int64Array::from(vec![
            1577926861000000000,
            1578704461000000000,
            1578790861000000000,
        ])),
        // nulls are [1, 2, 1]
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        // row counts are [8, 8, 4]
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "nanos",
    }
    .run();

    // micros
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![
            1577840461000000,
            1577841061000000,
            1578704521000000,
        ])),
        expected_max: Arc::new(Int64Array::from(vec![
            1577926861000000,
            1578704461000000,
            1578790861000000,
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "micros",
    }
    .run();

    // millis
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![
            1577840461000,
            1577841061000,
            1578704521000,
        ])),
        expected_max: Arc::new(Int64Array::from(vec![
            1577926861000,
            1578704461000,
            1578790861000,
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "millis",
    }
    .run();

    // seconds
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![
            1577840461, 1577841061, 1578704521,
        ])),
        expected_max: Arc::new(Int64Array::from(vec![
            1577926861, 1578704461, 1578790861,
        ])),
        expected_null_counts: UInt64Array::from(vec![1, 2, 1]),
        expected_row_counts: UInt64Array::from(vec![8, 8, 4]),
        column_name: "seconds",
    }
    .run();
}

// date with different row group sizes
// Bug expect `Date32Array` but returns Int32Array
//  https://github.com/apache/datafusion/issues/10587
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
    };
    Test {
        reader: reader.build().await,
        // mins are [18262, 18565,]
        expected_min: Arc::new(Int32Array::from(vec![18262, 18565])),
        // maxes are [18564, 21865,]
        expected_max: Arc::new(Int32Array::from(vec![18564, 21865])),
        // nulls are [2, 2]
        expected_null_counts: UInt64Array::from(vec![2, 2]),
        // row counts are [13, 7]
        expected_row_counts: UInt64Array::from(vec![13, 7]),
        column_name: "date32",
    }
    .run();
}

// BUG: same as above. Expect to return Date64Array but returns Int32Array
// test date with different row group sizes
// https://github.com/apache/datafusion/issues/10587
#[ignore]
#[tokio::test]
async fn test_dates_64_diff_rg_sizes() {
    // The file is created by 4 record batches (each has a null row), each has 5 rows but then will be split into 2 row groups with size 13, 7
    let reader = TestReader {
        scenario: Scenario::Dates,
        row_per_group: 13,
    };
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![18262, 18565])), // panic here because the actual data is Int32Array
        expected_max: Arc::new(Int64Array::from(vec![18564, 21865])),
        expected_null_counts: UInt64Array::from(vec![2, 2]),
        expected_row_counts: UInt64Array::from(vec![13, 7]),
        column_name: "date64",
    }
    .run();
}

// BUG:
// https://github.com/apache/datafusion/issues/10604
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
    };

    // u8
    // BUG: expect UInt8Array but returns Int32Array
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int32Array::from(vec![0, 1, 4, 7, 251])), // shoudld be UInt8Array
        expected_max: Arc::new(Int32Array::from(vec![3, 4, 6, 250, 254])), // shoudld be UInt8Array
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4, 4]),
        column_name: "u8",
    }
    .run();

    // u16
    // BUG: expect UInt16Array but returns Int32Array
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int32Array::from(vec![0, 1, 4, 7, 251])), // shoudld be UInt16Array
        expected_max: Arc::new(Int32Array::from(vec![3, 4, 6, 250, 254])), // shoudld be UInt16Array
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4, 4]),
        column_name: "u16",
    }
    .run();

    // u32
    // BUG: expect UInt32Array but returns Int32Array
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int32Array::from(vec![0, 1, 4, 7, 251])), // shoudld be UInt32Array
        expected_max: Arc::new(Int32Array::from(vec![3, 4, 6, 250, 254])), // shoudld be UInt32Array
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4, 4]),
        column_name: "u32",
    }
    .run();

    // u64
    // BUG: expect UInt64rray but returns Int64Array
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![0, 1, 4, 7, 251])), // shoudld be UInt64Array
        expected_max: Arc::new(Int64Array::from(vec![3, 4, 6, 250, 254])), // shoudld be UInt64Array
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![4, 4, 4, 4, 4]),
        column_name: "u64",
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
    };

    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int32Array::from(vec![0])),
        expected_max: Arc::new(Int32Array::from(vec![300000])),
        expected_null_counts: UInt64Array::from(vec![0]),
        expected_row_counts: UInt64Array::from(vec![4]),
        column_name: "i",
    }
    .run();
}

// BUG: not convert UInt32Array to Int32Array
// https://github.com/apache/datafusion/issues/10604
#[tokio::test]
async fn test_uint32_range() {
    // This creates a parquet file of 1 column "u"
    // file has 2 record batches, each has 2 rows. They will be saved into one row group
    let reader = TestReader {
        scenario: Scenario::UInt32Range,
        row_per_group: 5,
    };

    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int32Array::from(vec![0])), // should be UInt32Array
        expected_max: Arc::new(Int32Array::from(vec![300000])), // should be UInt32Array
        expected_null_counts: UInt64Array::from(vec![0]),
        expected_row_counts: UInt64Array::from(vec![4]),
        column_name: "u",
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
    };

    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Float64Array::from(vec![-5.0, -4.0, -0.0, 5.0])),
        expected_max: Arc::new(Float64Array::from(vec![-1.0, 0.0, 4.0, 9.0])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5, 5]),
        column_name: "f",
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
    };

    Test {
        reader: reader.build().await,
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
    }
    .run();
}

// BUG: not convert BinaryArray to StringArray
// https://github.com/apache/datafusion/issues/10605
#[tokio::test]
async fn test_byte() {
    // This creates a parquet file of 4 columns
    // "name"
    // "service_string"
    // "service_binary"
    // "service_fixedsize"

    // file has 3 record batches, each has 5 rows. They will be saved into 3 row groups
    let reader = TestReader {
        scenario: Scenario::ByteArray,
        row_per_group: 5,
    };

    // column "name"
    Test {
        reader: reader.build().await,
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
    }
    .run();

    // column "service_string"
    Test {
        reader: reader.build().await,
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
    }
    .run();

    // column "service_binary"
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(StringArray::from(vec![
            "frontend five",
            "backend one",
            "backend eight",
        ])), // Shuld be BinaryArray
        expected_max: Arc::new(StringArray::from(vec![
            "frontend two",
            "frontend six",
            "backend six",
        ])), // Shuld be BinaryArray
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "service_binary",
    }
    .run();

    // column "service_fixedsize"
    // b"fe1", b"be1", b"be4"
    let min_input = vec![vec![102, 101, 49], vec![98, 101, 49], vec![98, 101, 52]];
    // b"fe5", b"fe6", b"be8"
    let max_input = vec![vec![102, 101, 55], vec![102, 101, 54], vec![98, 101, 56]];

    Test {
        reader: reader.build().await,
        expected_min: Arc::new(
            FixedSizeBinaryArray::try_from_iter(min_input.into_iter()).unwrap(),
        ),
        expected_max: Arc::new(
            FixedSizeBinaryArray::try_from_iter(max_input.into_iter()).unwrap(),
        ),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "service_fixedsize",
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
    };

    // column "name"
    Test {
        reader: reader.build().await,
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
    }
    .run();

    // column "service.name"
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(StringArray::from(vec!["frontend", "backend", "backend"])),
        expected_max: Arc::new(StringArray::from(vec![
            "frontend", "frontend", "backend",
        ])),
        expected_null_counts: UInt64Array::from(vec![0, 0, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5, 5]),
        column_name: "service.name",
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
    };

    Test {
        reader: reader.build().await,
        expected_min: Arc::new(BooleanArray::from(vec![false, false])),
        expected_max: Arc::new(BooleanArray::from(vec![true, false])),
        expected_null_counts: UInt64Array::from(vec![1, 0]),
        expected_row_counts: UInt64Array::from(vec![5, 5]),
        column_name: "bool",
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
    };
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(struct_array(vec![(Some(1), Some(6.0), Some(12.0))])),
        expected_max: Arc::new(struct_array(vec![(Some(2), Some(8.5), Some(14.0))])),
        expected_null_counts: UInt64Array::from(vec![0]),
        expected_row_counts: UInt64Array::from(vec![3]),
        column_name: "struct",
    }
    .run();
}
////// Files with missing statistics ///////

#[tokio::test]
async fn test_missing_statistics() {
    let row_per_group = 5;
    let reader =
        parquet_file_one_column_stats(0, 4, 7, row_per_group, EnabledStatistics::None);

    Test {
        reader,
        expected_min: Arc::new(Int64Array::from(vec![None])),
        expected_max: Arc::new(Int64Array::from(vec![None])),
        expected_null_counts: UInt64Array::from(vec![None]),
        expected_row_counts: UInt64Array::from(vec![3]), // stil has row count statistics
        column_name: "i64",
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
    };
    Test {
        reader: reader.build().await,
        expected_min: Arc::new(Int64Array::from(vec![18262, 18565])),
        expected_max: Arc::new(Int64Array::from(vec![18564, 21865])),
        expected_null_counts: UInt64Array::from(vec![2, 2]),
        expected_row_counts: UInt64Array::from(vec![13, 7]),
        column_name: "not_a_column",
    }
    .run_col_not_found();
}
