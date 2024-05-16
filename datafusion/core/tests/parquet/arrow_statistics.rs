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
use datafusion::datasource::physical_plan::parquet::arrow_statistics::{
    self, parquet_stats_to_arrow,
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

// TESTS

#[tokio::test]
async fn test_one_row_group_without_null() {
    let row_per_group = 20;
    let reader = parquet_file(0, 4, 7, row_per_group);

    let statistics =
        arrow_statistics::ParquetColumnStatistics::from_parquet_statistics(&reader)
            .expect("getting statistics");

    // only 1 column
    assert_eq!(statistics.len(), 1, "expected 1 column");

    let arrow_stats = parquet_stats_to_arrow(&DataType::Int64, &statistics[0]).unwrap();
    println!("Arrow statistics:\n {arrow_stats:#?}");

    // only one row group
    let min = arrow_stats.min();
    assert_eq!(arrow_stats.min().len(), 1, "expected 1 row group");

    // min is 4
    let expect = Int64Array::from(vec![4]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(min, &expect);

    // max is 6
    let expect = Int64Array::from(vec![6]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(arrow_stats.max(), &expect);

    // no nulls
    let expect = UInt64Array::from(vec![0]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(arrow_stats.null_count(), &expect);

    // 3 rows
    let expect = UInt64Array::from(vec![3]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(arrow_stats.row_count(), &expect);
}

#[tokio::test]
async fn test_one_row_group_with_null_and_negative() {
    let row_per_group = 20;
    let reader = parquet_file(2, -1, 5, row_per_group);

    let statistics =
        arrow_statistics::ParquetColumnStatistics::from_parquet_statistics(&reader)
            .expect("getting statistics");

    // only 1 column
    assert_eq!(statistics.len(), 1, "expected 1 column");

    let arrow_stats = parquet_stats_to_arrow(&DataType::Int64, &statistics[0]).unwrap();
    println!("Arrow statistics:\n {arrow_stats:#?}");

    // only one row group
    let min = arrow_stats.min();
    assert_eq!(arrow_stats.min().len(), 1, "expected 1 row group");

    // min is -1
    let expect = Int64Array::from(vec![-1]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(min, &expect);

    // max is 4
    let expect = Int64Array::from(vec![4]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(arrow_stats.max(), &expect);

    // 2 nulls
    let expect = UInt64Array::from(vec![2]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(arrow_stats.null_count(), &expect);

    // 8 rows
    let expect = UInt64Array::from(vec![8]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(arrow_stats.row_count(), &expect);
}

#[tokio::test]
async fn test_two_row_group_with_null() {
    let row_per_group = 10;
    let reader = parquet_file(2, 4, 17, row_per_group);

    let statistics =
        arrow_statistics::ParquetColumnStatistics::from_parquet_statistics(&reader)
            .expect("getting statistics");

    // only 1 column
    assert_eq!(statistics.len(), 1, "expected 1 column");

    let arrow_stats = parquet_stats_to_arrow(&DataType::Int64, &statistics[0]).unwrap();
    println!("Arrow statistics:\n {arrow_stats:#?}");

    // 2 row groups
    let mins = arrow_stats.min();
    assert_eq!(mins.len(), 2, "expected 2 row groups");

    // mins are [4, 14]
    let expect = Int64Array::from(vec![4, 14]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(mins, &expect);

    // maxs are [13, 16]
    let maxs = arrow_stats.max();
    let expect = Int64Array::from(vec![13, 16]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(maxs, &expect);

    // nuls are [0, 2]
    let nulls = arrow_stats.null_count();
    let expect = UInt64Array::from(vec![0, 2]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(nulls, &expect);

    // row counts are [10, 5]
    let row_counts = arrow_stats.row_count();
    let expect = UInt64Array::from(vec![10, 5]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(row_counts, &expect);
}

#[tokio::test]
async fn test_two_row_groups_with_all_nulls_in_one() {
    let row_per_group = 5;
    let reader = parquet_file(4, -2, 2, row_per_group);

    let statistics =
        arrow_statistics::ParquetColumnStatistics::from_parquet_statistics(&reader)
            .expect("getting statistics");

    // only 1 column
    assert_eq!(statistics.len(), 1, "expected 1 column");

    let arrow_stats = parquet_stats_to_arrow(&DataType::Int64, &statistics[0]).unwrap();
    println!("Arrow statistics:\n {arrow_stats:#?}");

    // 2 row groups
    let mins = arrow_stats.min();
    assert_eq!(mins.len(), 2, "expected 2 row groups");

    // mins are [-2, null]
    assert!(mins.is_null(1));
    // check the first value -2
    // let expect = Int64Array::from(vec![-2]);
    // let expect = Arc::new(expect) as ArrayRef;
    // assert_eq!(mins, &expect);

    // maxs are [1, null
    assert!(arrow_stats.max().is_null(1));
    // let expect = Int64Array::from(vec![1]);
    // let expect = Arc::new(expect) as ArrayRef;
    // assert_eq!(arrow_stats.max(), &expect);

    // nuls are [1, 3]
    let nulls = arrow_stats.null_count();
    let expect = UInt64Array::from(vec![1, 3]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(nulls, &expect);

    // row counts are [5, 3]
    let row_counts = arrow_stats.row_count();
    let expect = UInt64Array::from(vec![5, 3]);
    let expect = Arc::new(expect) as ArrayRef;
    assert_eq!(row_counts, &expect);
}
