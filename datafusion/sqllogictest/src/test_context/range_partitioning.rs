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

use std::fs::{File, create_dir_all, remove_dir_all};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::{ScalarValue, SplitPoint};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::{Partitioning, RangePartitioning, SortExpr, col};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::{
    Partitioning as PhysicalPartitioning, PhysicalSortExpr,
    RangePartitioning as PhysicalRangePartitioning, expressions::col as physical_col,
};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::test::TestPartitionStream;
use datafusion::prelude::SessionContext;

// ==============================================================================
// Range Partitioned Table (sqllogictest-only)
// ==============================================================================

/// Registers a simple range-partitioned listing table for testing before
/// declaring such tables is supported via SQL.
pub(super) fn register_range_partitioned_table(ctx: &SessionContext) {
    const RANGE_PARTITIONS: [&[(i32, i32, i32)]; 4] = [
        &[(1, 1, 10), (5, 2, 50)],
        &[(10, 1, 100), (15, 2, 150)],
        &[(20, 1, 200), (25, 2, 250)],
        &[(30, 1, 300), (35, 2, 350)],
    ];
    const SHIFTED_RANGE_PARTITIONS: [&[(i32, i32, i32)]; 4] = [
        &[(1, 1, 10), (5, 2, 50), (10, 1, 100)],
        &[(15, 2, 150)],
        &[(20, 1, 200), (25, 2, 250)],
        &[(30, 1, 300), (35, 2, 350)],
    ];
    const NARROW_RANGE_PARTITIONS: [&[(i32, i32, i32)]; 3] = [
        &[(1, 1, 10), (5, 2, 50)],
        &[(10, 1, 100), (15, 2, 150)],
        &[(20, 1, 200), (25, 2, 250), (30, 1, 300), (35, 2, 350)],
    ];
    const SPARSE_RANGE_PARTITIONS: [&[(i32, i32, i32)]; 4] = [
        &[(5, 2, 50), (8, 3, 80)],
        &[(10, 1, 100)],
        &[(20, 1, 200)],
        &[(30, 1, 300), (40, 4, 400)],
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("range_key", DataType::Int32, false),
        Field::new("non_range_key", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let output_partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            vec![col("range_key").sort(true, true)],
            vec![
                SplitPoint::new(vec![ScalarValue::Int32(Some(10))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(20))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(30))]),
            ],
        )
        .expect("range partitioning should be valid"),
    );

    let range_table_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test_files/scratch_range_partitioning/range_partitioned");

    register_parquet_listing_table(
        ctx,
        "range_partitioned",
        &range_table_dir,
        Arc::clone(&schema),
        range_batches(&schema, RANGE_PARTITIONS),
        output_partitioning,
        None,
    );

    register_unbounded_range_stream_table(
        ctx,
        "unbounded_range_like",
        Arc::clone(&schema),
        [10, 20, 30],
        RANGE_PARTITIONS.map(|rows| rows.to_vec()),
    );
    register_unbounded_range_stream_table(
        ctx,
        "unbounded_range_like_shifted",
        Arc::clone(&schema),
        [15, 20, 30],
        SHIFTED_RANGE_PARTITIONS.map(|rows| rows.to_vec()),
    );

    let shifted_output_partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            vec![col("range_key").sort(true, true)],
            vec![
                SplitPoint::new(vec![ScalarValue::Int32(Some(15))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(20))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(30))]),
            ],
        )
        .expect("range partitioning should be valid"),
    );

    register_parquet_listing_table(
        ctx,
        "range_partitioned_shifted",
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_files/scratch_range_partitioning/range_partitioned_shifted"),
        Arc::clone(&schema),
        range_batches(&schema, SHIFTED_RANGE_PARTITIONS),
        shifted_output_partitioning,
        None,
    );

    // Same rows as `range_partitioned` but split into only three range
    // partitions on `range_key`. Used to exercise the co-partition check when
    // two Range inputs disagree on partition count.
    let narrow_output_partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            vec![col("range_key").sort(true, true)],
            vec![
                SplitPoint::new(vec![ScalarValue::Int32(Some(10))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(20))]),
            ],
        )
        .expect("range partitioning should be valid"),
    );

    register_parquet_listing_table(
        ctx,
        "range_partitioned_narrow",
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_files/scratch_range_partitioning/range_partitioned_narrow"),
        Arc::clone(&schema),
        range_batches(&schema, NARROW_RANGE_PARTITIONS),
        narrow_output_partitioning,
        None,
    );

    let sparse_output_partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            vec![col("range_key").sort(true, true)],
            vec![
                SplitPoint::new(vec![ScalarValue::Int32(Some(10))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(20))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(30))]),
            ],
        )
        .expect("range partitioning should be valid"),
    );

    register_parquet_listing_table(
        ctx,
        "range_partitioned_sparse",
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_files/scratch_range_partitioning/range_partitioned_sparse"),
        Arc::clone(&schema),
        range_batches(&schema, SPARSE_RANGE_PARTITIONS),
        sparse_output_partitioning,
        None,
    );
}

fn register_parquet_listing_table(
    ctx: &SessionContext,
    name: &str,
    table_dir: impl AsRef<Path>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    output_partitioning: Partitioning,
    file_sort_order: Option<Vec<Vec<SortExpr>>>,
) {
    let table_dir = table_dir.as_ref();
    if table_dir.exists() {
        remove_dir_all(table_dir).expect("test table dir should be removable");
    }
    create_dir_all(table_dir).expect("test table dir should be created");
    for (idx, batch) in batches.into_iter().enumerate() {
        let file = File::create(table_dir.join(format!("part-{idx}.parquet")))
            .expect("test table parquet partition should be created");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
            .expect("test table parquet writer should be created");
        writer
            .write(&batch)
            .expect("test table parquet partition should be written");
        writer
            .close()
            .expect("test table parquet writer should close");
    }

    let table_path = format!(
        "{}/",
        table_dir
            .to_str()
            .expect("test table path should be valid utf8")
    );
    let table_url =
        ListingTableUrl::parse(&table_path).expect("test table url should parse");
    let mut options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_output_partitioning(Some(output_partitioning));
    if let Some(file_sort_order) = file_sort_order {
        options = options.with_file_sort_order(file_sort_order);
    }
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(options)
        .with_schema(schema);
    let table =
        ListingTable::try_new(config).expect("test listing table should be valid");

    ctx.register_table(name, Arc::new(table))
        .expect("test listing table registration should succeed");
}

fn register_unbounded_range_stream_table(
    ctx: &SessionContext,
    name: &str,
    schema: Arc<Schema>,
    split_points: [i32; 3],
    partition_rows: [Vec<(i32, i32, i32)>; 4],
) {
    let output_partitioning = PhysicalPartitioning::Range(
        PhysicalRangePartitioning::try_new(
            [PhysicalSortExpr {
                expr: physical_col("range_key", &schema)
                    .expect("range key should exist in stream schema"),
                options: SortOptions::default(),
            }]
            .into(),
            split_points
                .into_iter()
                .map(|value| SplitPoint::new(vec![ScalarValue::Int32(Some(value))]))
                .collect(),
        )
        .expect("range partitioning should be valid"),
    );
    let partitions = partition_rows
        .into_iter()
        .map(|rows| range_stream_partition(Arc::clone(&schema), &rows))
        .collect();

    ctx.register_table(
        name,
        Arc::new(
            StreamingTable::try_new(schema, partitions)
                .expect("range stream table should be valid")
                .with_infinite_table(true)
                .with_output_partitioning(output_partitioning),
        ),
    )
    .expect("test stream table registration should succeed");
}

fn range_stream_partition(
    schema: SchemaRef,
    rows: &[(i32, i32, i32)],
) -> Arc<dyn PartitionStream> {
    Arc::new(TestPartitionStream::new_with_batches(vec![range_batch(
        schema, rows,
    )]))
}

fn range_batches(
    schema: &SchemaRef,
    partitions: impl IntoIterator<Item = &'static [(i32, i32, i32)]>,
) -> Vec<RecordBatch> {
    partitions
        .into_iter()
        .map(|rows| range_batch(Arc::clone(schema), rows))
        .collect()
}

fn range_batch(schema: SchemaRef, rows: &[(i32, i32, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0)))
                as ArrayRef,
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.1)))
                as ArrayRef,
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.2)))
                as ArrayRef,
        ],
    )
    .expect("range batch should be valid")
}

// ==============================================================================
// Time-bin table: range-partitioned on timestamp, sorted on (key, timestamp)
// ==============================================================================

/// Unix nanoseconds for `2024-01-01 00:00:00 UTC`.
const TIME_BIN_EPOCH_NS: i64 = 1_704_067_200_000_000_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;

/// Timestamp helper: minutes and seconds after `2024-01-01 00:00:00 UTC`.
fn time_bin_ts(minutes: i64, seconds: i64) -> i64 {
    TIME_BIN_EPOCH_NS + minutes * NANOS_PER_MINUTE + seconds * NANOS_PER_SECOND
}

/// Row: (key, col1, col2, col3, col4, timestamp_ns, value)
type TimeBinRow = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    i64,
    i64,
);

/// Registers `range_sorted_time_bin` for time-bin aggregation plan tests.
///
/// Two file groups, each covering a 60-minute timestamp range:
/// - partition 0: `[2024-01-01 00:00, 01:00)`
/// - partition 1: `[2024-01-01 01:00, 02:00)`
///
/// Files are range-partitioned on `timestamp` and sorted on `(key, timestamp)`.
/// Because `date_bin(60 seconds, timestamp)` and `date_trunc('hour', timestamp)`
/// do not straddle the hour split, grouping by `(key, time_bin)` is
/// partition-disjoint and aggregation can run in one streaming step. Bins that
/// do straddle the split (for example `date_trunc('day', timestamp)`) still
/// require a hash shuffle.
pub(super) fn register_range_sorted_time_bin_table(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("col1", DataType::Utf8, false),
        Field::new("col2", DataType::Utf8, false),
        Field::new("col3", DataType::Utf8, false),
        Field::new("col4", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));

    // Each partition covers 60 minutes. The split is aligned to the 60-second
    // `date_bin` used by the test query, so time bins do not straddle files.
    let hour_split = time_bin_ts(60, 0);
    let output_partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            vec![col("timestamp").sort(true, true)],
            vec![SplitPoint::new(vec![ScalarValue::TimestampNanosecond(
                Some(hour_split),
                None,
            )])],
        )
        .expect("time-bin range partitioning should be valid"),
    );

    // Within each 60-minute file, rows are sorted by (key, timestamp).
    let partitions = [
        vec![
            ("k1", "x", "y", "z", "a", time_bin_ts(0, 10), 1),
            ("k1", "x", "y", "z", "a", time_bin_ts(0, 40), 2),
            ("k1", "x", "y", "z", "b", time_bin_ts(1, 10), 99),
            ("k2", "x", "y", "z", "a", time_bin_ts(30, 0), 3),
            ("k2", "x", "y", "z", "a", time_bin_ts(30, 30), 4),
        ],
        vec![
            ("k1", "x", "y", "z", "a", time_bin_ts(60, 10), 10),
            ("k1", "x", "y", "z", "a", time_bin_ts(60, 40), 20),
            ("k2", "x", "y", "z", "a", time_bin_ts(90, 0), 30),
            ("k2", "x", "y", "z", "a", time_bin_ts(105, 0), 5),
        ],
    ];

    let table_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test_files/scratch_range_partitioning/range_sorted_time_bin");
    let batches = partitions
        .iter()
        .map(|rows| time_bin_batch(Arc::clone(&schema), rows))
        .collect();
    register_parquet_listing_table(
        ctx,
        "range_sorted_time_bin",
        &table_dir,
        Arc::clone(&schema),
        batches,
        output_partitioning,
        Some(vec![vec![
            col("key").sort(true, true),
            col("timestamp").sort(true, true),
        ]]),
    );
}

fn time_bin_batch(schema: SchemaRef, rows: &[TimeBinRow]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0)))
                as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1)))
                as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2)))
                as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3)))
                as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4)))
                as ArrayRef,
            Arc::new(TimestampNanosecondArray::from_iter_values(
                rows.iter().map(|row| row.5),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.6)))
                as ArrayRef,
        ],
    )
    .expect("time-bin batch should be valid")
}
