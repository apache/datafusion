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

use std::sync::Arc;

// use arrow::json::reader;
use arrow_array::{make_array, Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::statistics::Statistics as ParquetFileStatistics;
use parquet::format::Statistics as ParquetFormatStatistics;

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

// TODO: Eventually, we will need only read_statistics and read_row_count,
// but for my understading, we will keep both write and read statistics.
// Their values should be the same
pub struct RowGroupsStatistics {
    // Statistics from the file writer
    pub write_statistics: Vec<WriteRowGroupStatistics>,

    // Statistics from the file reader
    pub read_statistics: Vec<ReadRowGroupStatistics>,
}

pub struct WriteRowGroupStatistics {
    pub statistics: ParquetFormatStatistics,
    pub row_count: i64,
}

pub struct ReadRowGroupStatistics {
    pub statistics: ParquetFileStatistics,
    pub row_count: i64,
}

// Create a parquet file with one column for data type i64
// Data of the file include
//   . Number of null rows is the given num_null
//   . There are non-null values in the range [no_null_values_start, no_null_values_end], one value each row
//   . The file is divided into row groups of size row_per_group
pub fn parquet_row_group_statistics(
    num_null: usize,
    no_null_values_start: i64,
    no_null_values_end: i64,
    row_per_group: usize,
) -> RowGroupsStatistics {
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

    //////////////// WRITE STATISTICS ///////////////////////
    let file_meta = writer.close().unwrap();

    // meta data of the row groups
    let rg_metas = file_meta.row_groups;

    // Statstics of each grow group
    let mut write_rg_statistics = Vec::new();
    for rg_meta in rg_metas {
        let column = &rg_meta.columns;
        assert_eq!(column.len(), 1, "expected 1 column");
        let col_metadata = &column[0].meta_data;
        let col_metadata = col_metadata.as_ref().unwrap();
        let stats = &col_metadata.statistics;
        let stats = stats.as_ref().unwrap();

        let rg_stats = WriteRowGroupStatistics {
            row_count: rg_meta.num_rows,
            statistics: stats.clone(),
        };
        write_rg_statistics.push(rg_stats);
    }

    //////////////// READ STATISTICS ///////////////////////
    let file = output_file.reopen().unwrap();

    let reader = ArrowReaderBuilder::try_new(file).unwrap();
    let parquet_meta = reader.metadata().clone();

    // meta data of the row groups
    let rg_metas = parquet_meta.row_groups();

    // Statstics of each grow group
    let mut read_rg_statistics = Vec::new();
    for rg_meta in rg_metas {
        let column = &rg_meta.columns();
        assert_eq!(column.len(), 1, "expected 1 column");
        let stats = &column[0].statistics().unwrap();

        let rg_stats = ReadRowGroupStatistics {
            row_count: rg_meta.num_rows() as i64,
            statistics: (*stats).clone(),
        };
        read_rg_statistics.push(rg_stats);
    }

    RowGroupsStatistics {
        write_statistics: write_rg_statistics,
        read_statistics: read_rg_statistics,
    }
}

// TESTS

#[tokio::test]
async fn test_one_row_group_without_null() {
    let row_per_group = 20;
    let rg_statistics = parquet_row_group_statistics(0, 4, 7, row_per_group);

    //////////// Verify write row group statistics ////////////
    let write_rg_stats = &rg_statistics.write_statistics;

    // only 1 row group
    assert_eq!(write_rg_stats.len(), 1, "expected 1 row group");
    let rg = &write_rg_stats[0];

    // 3 rows
    assert_eq!(rg.row_count, 3, "expected 3 rows");

    // no nulls
    assert!(rg.statistics.null_count.is_none(), "expected no nulls");

    let statistics = &rg.statistics;

    // min is [4, 0, 0, 0, 0, 0, 0, 0]
    assert_eq!(
        *statistics.min_value.as_ref().unwrap(),
        [4, 0, 0, 0, 0, 0, 0, 0]
    );
    // max is [6, 0, 0, 0, 0, 0, 0, 0]
    assert_eq!(
        *statistics.max_value.as_ref().unwrap(),
        [6, 0, 0, 0, 0, 0, 0, 0]
    );

    println!("RG statistics:\n {statistics:#?}");
    // This will display:
    // RG statistics:
    // Statistics {
    //     max: Some(
    //         [
    //             6,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //         ],
    //     ),
    //     min: Some(
    //         [
    //             4,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //         ],
    //     ),
    //     null_count: None,
    //     distinct_count: None,
    //     max_value: Some(
    //         [
    //             6,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //         ],
    //     ),
    //     min_value: Some(
    //         [
    //             4,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //             0,
    //         ],
    //     ),
    //     is_max_value_exact: Some(
    //         true,
    //     ),
    //     is_min_value_exact: Some(
    //         true,
    //     ),
    // }

    //////////// Verify read row group statistics ////////////
    let read_rg_stats = &rg_statistics.read_statistics;

    // only 1 row group
    assert_eq!(read_rg_stats.len(), 1, "expected 1 row group");
    let rg = &read_rg_stats[0];

    // 3 rows
    assert_eq!(rg.row_count, 3, "expected 3 rows");

    // rg.statistics is Int64
    let stats = &rg.statistics;
    match stats {
        ParquetFileStatistics::Int64(int64_stats) => {
            // no nulls
            assert_eq!(int64_stats.null_count(), 0, "expected no nulls");
            // min is 4
            assert_eq!(*int64_stats.min(), 4);
            // max is 6
            assert_eq!(*int64_stats.max(), 6);
        }
        _ => panic!("expected Int64"),
    }

    // TODO:
    // 1. run the function (to be implemented) to convert rg_statistics to ArrowStatistics
    // 2. compare the ArrowStatistics with the expected
}

#[tokio::test]
async fn test_one_row_group_with_null_and_negative() {
    let row_per_group = 20;
    let rg_statistics = parquet_row_group_statistics(2, -1, 5, row_per_group);

    //////////// Verify write row group statistics ////////////
    let write_rg_stats = &rg_statistics.write_statistics;

    // only 1 row group
    assert_eq!(write_rg_stats.len(), 1, "expected 1 row group");
    let rg = &write_rg_stats[0];

    // 8 rows
    assert_eq!(rg.row_count, 8, "expected 8 rows");

    // 2 nulls
    assert_eq!(rg.statistics.null_count, Some(2), "expected 2 nulls");

    let statistics = &rg.statistics;

    // // min of -1 is [255, 255, 255, 255, 255, 255, 255, 255]
    assert_eq!(
        *statistics.min_value.as_ref().unwrap(),
        [255, 255, 255, 255, 255, 255, 255, 255]
    );
    // max is [4, 0, 0, 0, 0, 0, 0, 0]
    assert_eq!(
        *statistics.max_value.as_ref().unwrap(),
        [4, 0, 0, 0, 0, 0, 0, 0]
    );

    println!("RG statistics:\n {statistics:#?}");

    //////////// Verify read row group statistics ////////////
    let read_rg_stats = &rg_statistics.read_statistics;

    // only 1 row group
    assert_eq!(read_rg_stats.len(), 1, "expected 1 row group");
    let rg = &read_rg_stats[0];

    // 8 rows
    assert_eq!(rg.row_count, 8, "expected 8 rows");

    // rg.statistics is Int64
    let stats = &rg.statistics;
    match stats {
        ParquetFileStatistics::Int64(int64_stats) => {
            // 2 nulls
            assert_eq!(int64_stats.null_count(), 2, "expected 2 nulls");
            // min of -1
            assert_eq!(*int64_stats.min(), -1);
            // max is 4
            assert_eq!(*int64_stats.max(), 4);
        }
        _ => panic!("expected Int64"),
    }

    // TODO:
    // 1. run the function (to be implemented) to convert rg_statistics to ArrowStatistics
    // 2. compare the ArrowStatistics with the expected
}

#[tokio::test]
async fn test_two_row_group_with_null() {
    let row_per_group = 10;
    let rg_statistics = parquet_row_group_statistics(2, 4, 17, row_per_group);

    //////////// Verify write row group statistics ////////////
    let write_rg_stats = &rg_statistics.write_statistics;

    // 2 row group
    assert_eq!(write_rg_stats.len(), 2, "expected 2 row group");

    // 1st row group with 10 rows (max number of rows)
    let rg1 = &write_rg_stats[0];
    assert_eq!(rg1.row_count, 10, "expected 10 rows");
    let stats = &rg1.statistics;
    // no nulls
    assert_eq!(stats.null_count, None, "expected no nulls");
    // min is [4, 0, 0, 0, 0, 0, 0, 0]
    assert_eq!(*stats.min_value.as_ref().unwrap(), [4, 0, 0, 0, 0, 0, 0, 0]);
    // max is [13, 0, 0, 0, 0, 0, 0, 0]
    assert_eq!(
        *stats.max_value.as_ref().unwrap(),
        [13, 0, 0, 0, 0, 0, 0, 0]
    );
    println!("RG1 statistics:\n {stats:#?}");

    // 2nd row group with 5 rows
    let rg2 = &write_rg_stats[1];
    assert_eq!(rg2.row_count, 5, "expected 5 rows");
    // 2 nulls
    assert_eq!(rg2.statistics.null_count, Some(2), "expected no nulls");
    // min is [14, 0, 0, 0, 0, 0, 0, 0]
    assert_eq!(
        *rg2.statistics.min_value.as_ref().unwrap(),
        [14, 0, 0, 0, 0, 0, 0, 0]
    );
    // max is [16, 0, 0, 0, 0, 0, 0, 0]
    assert_eq!(
        *rg2.statistics.max_value.as_ref().unwrap(),
        [16, 0, 0, 0, 0, 0, 0, 0]
    );

    let stats = &rg2.statistics;
    println!("RG2 statistics:\n {stats:#?}");

    //////////// Verify read row group statistics ////////////
    let read_rg_stats = &rg_statistics.read_statistics;

    // 2 row groups
    assert_eq!(read_rg_stats.len(), 2, "expected 2 row groups");

    // 1st row group with 10 rows (max number of rows)
    let rg1 = &read_rg_stats[0];
    assert_eq!(rg1.row_count, 10, "expected 10 rows");

    // rg1.statistics is Int64
    let stats = &rg1.statistics;
    match stats {
        ParquetFileStatistics::Int64(int64_stats) => {
            // no nulls
            assert_eq!(int64_stats.null_count(), 0, "expected no nulls");
            // min is 4
            assert_eq!(*int64_stats.min(), 4);
            // max is 13
            assert_eq!(*int64_stats.max(), 13);
        }
        _ => panic!("expected Int64"),
    }

    // 2nd row group with 5 rows
    let rg2 = &read_rg_stats[1];
    assert_eq!(rg2.row_count, 5, "expected 5 rows");

    // rg2.statistics is Int64
    let stats = &rg2.statistics;
    match stats {
        ParquetFileStatistics::Int64(int64_stats) => {
            // 2 nulls
            assert_eq!(int64_stats.null_count(), 2, "expected 2 nulls");
            // min is 14
            assert_eq!(*int64_stats.min(), 14);
            // max is 16
            assert_eq!(*int64_stats.max(), 16);
        }
        _ => panic!("expected Int64"),
    }

    // TODO:
    // 1. run the function (to be implemented) to convert rg_statistics to ArrowStatistics
    // 2. compare the ArrowStatistics with the expected
}

#[tokio::test]
async fn test_two_row_groups_with_all_nulls_in_one() {
    let row_per_group = 5;
    let rg_statistics = parquet_row_group_statistics(4, -2, 2, row_per_group);

    //////////// Verify write row group statistics ////////////
    let write_rg_stats = &rg_statistics.write_statistics;

    // 2 row group
    assert_eq!(write_rg_stats.len(), 2, "expected 2 row group");

    // 1st row group with 5 rows
    let rg1 = &write_rg_stats[0];
    assert_eq!(rg1.row_count, 5, "expected 5 rows");
    // 1 null
    assert_eq!(rg1.statistics.null_count, Some(1), "expected 1 null");
    // min of -2 is [254, 255, 255, 255, 255, 255, 255, 255]
    assert_eq!(
        *rg1.statistics.min_value.as_ref().unwrap(),
        [254, 255, 255, 255, 255, 255, 255, 255]
    );
    // max is [1, 0, 0, 0, 0, 0, 0, 0]
    assert_eq!(
        *rg1.statistics.max_value.as_ref().unwrap(),
        [1, 0, 0, 0, 0, 0, 0, 0]
    );
    let stats = &rg1.statistics;
    println!("RG1 statistics:\n {stats:#?}");

    // 2nd row group with 3 rows
    let rg2 = &write_rg_stats[1];
    assert_eq!(rg2.row_count, 3, "expected 3 rows");
    // 3 nulls
    assert_eq!(rg2.statistics.null_count, Some(3), "expected 3 nulls");
    // min is None
    assert_eq!(rg2.statistics.min_value, None);
    // max is None
    assert_eq!(rg2.statistics.max_value, None);
    let stats = &rg2.statistics;
    println!("RG2 statistics:\n {stats:#?}");

    //////////// Verify read row group statistics ////////////
    let read_rg_stats = &rg_statistics.read_statistics;

    // 2 row groups
    assert_eq!(read_rg_stats.len(), 2, "expected 2 row groups");

    // 1st row group with 5 rows
    let rg1 = &read_rg_stats[0];
    assert_eq!(rg1.row_count, 5, "expected 5 rows");

    // rg1.statistics is Int64
    let stats = &rg1.statistics;
    match stats {
        ParquetFileStatistics::Int64(int64_stats) => {
            // 1 null
            assert_eq!(int64_stats.null_count(), 1, "expected 1 null");
            // min of -2
            assert_eq!(*int64_stats.min(), -2);
            // max is 1
            assert_eq!(*int64_stats.max(), 1);
        }
        _ => panic!("expected Int64"),
    }

    // 2nd row group with 3 rows
    let rg2 = &read_rg_stats[1];
    assert_eq!(rg2.row_count, 3, "expected 3 rows");

    // rg2.statistics is Int64
    let stats = &rg2.statistics;
    match stats {
        ParquetFileStatistics::Int64(int64_stats) => {
            // 3 nulls
            assert_eq!(int64_stats.null_count(), 3, "expected 3 nulls");
            // min is None
            assert!(!int64_stats.has_min_max_set());
        }
        _ => panic!("expected Int64"),
    }

    // TODO:
    // 1. run the function (to be implemented) to convert rg_statistics to ArrowStatistics
    // 2. compare the ArrowStatistics with the expected
}
