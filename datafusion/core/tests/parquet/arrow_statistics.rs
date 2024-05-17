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

use arrow_array::{make_array, Array, ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::physical_plan::parquet::{
    RequestedStatistics, StatisticsConverter,
};
use parquet::arrow::arrow_reader::{ArrowReaderBuilder, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

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
pub fn parquet_file(
    num_null: usize,
    no_null_values_start: i64,
    no_null_values_end: i64,
    row_per_group: usize,
) -> ParquetRecordBatchReaderBuilder<File> {
    let mut output_file = tempfile::Builder::new()
        .prefix("parquert_statistics_test")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let props = WriterProperties::builder()
        .set_max_row_group_size(row_per_group)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let batches = vec![
        make_int64_batches_with_null(num_null, no_null_values_start, no_null_values_end), // TODO: likely make this more general using
                                                                                          // create_data_batch(scenario); where scenario is the respective enum data type
    ];

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

struct Test {
    reader: ParquetRecordBatchReaderBuilder<File>,
    expected_min: ArrayRef,
    expected_max: ArrayRef,
    expected_null_counts: UInt64Array,
    expected_row_counts: UInt64Array,
}

impl Test {
    fn run(self) {
        let Self {
            reader,
            expected_min,
            expected_max,
            expected_null_counts,
            expected_row_counts,
        } = self;

        let min = StatisticsConverter::try_new(
            "i64",
            RequestedStatistics::Min,
            reader.schema(),
        )
        .unwrap()
        .extract(reader.metadata())
        .unwrap();
        assert_eq!(&min, &expected_min, "Mismatch with expected minimums");

        let max = StatisticsConverter::try_new(
            "i64",
            RequestedStatistics::Max,
            reader.schema(),
        )
        .unwrap()
        .extract(reader.metadata())
        .unwrap();
        assert_eq!(&max, &expected_max, "Mismatch with expected maximum");

        let null_counts = StatisticsConverter::try_new(
            "i64",
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
}

// TESTS
//
// Remaining cases
// - Create parquet files / metadata with missing statistic values
// - Create parquet files / metadata with different data types
// - Create parquet files / metadata with different row group sizes
// - Using truncated statistics  ("exact min value" and "exact max value" https://docs.rs/parquet/latest/parquet/file/statistics/enum.Statistics.html#method.max_is_exact)

#[tokio::test]
async fn test_one_row_group_without_null() {
    let row_per_group = 20;
    let reader = parquet_file(0, 4, 7, row_per_group);
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
    }
    .run()
}

#[tokio::test]
async fn test_one_row_group_with_null_and_negative() {
    let row_per_group = 20;
    let reader = parquet_file(2, -1, 5, row_per_group);

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
    }
    .run()
}

#[tokio::test]
async fn test_two_row_group_with_null() {
    let row_per_group = 10;
    let reader = parquet_file(2, 4, 17, row_per_group);

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
    }
    .run()
}

#[tokio::test]
async fn test_two_row_groups_with_all_nulls_in_one() {
    let row_per_group = 5;
    let reader = parquet_file(4, -2, 2, row_per_group);

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
    }
    .run()
}
