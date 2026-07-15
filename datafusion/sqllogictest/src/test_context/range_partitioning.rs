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

use std::fs::{create_dir_all, remove_dir_all, write};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::{ScalarValue, SplitPoint};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::{Partitioning, RangePartitioning, col};
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

    register_csv_listing_table(
        ctx,
        "range_partitioned",
        &range_table_dir,
        Arc::clone(&schema),
        [
            "1,1,10\n5,2,50\n",
            "10,1,100\n15,2,150\n",
            "20,1,200\n25,2,250\n",
            "30,1,300\n35,2,350\n",
        ],
        Some(output_partitioning),
    );

    register_unbounded_range_stream_table(
        ctx,
        "unbounded_range_like",
        Arc::clone(&schema),
        [10, 20, 30],
        [
            vec![(1, 1, 10), (5, 2, 50)],
            vec![(10, 1, 100), (15, 2, 150)],
            vec![(20, 1, 200), (25, 2, 250)],
            vec![(30, 1, 300), (35, 2, 350)],
        ],
    );
    register_unbounded_range_stream_table(
        ctx,
        "unbounded_range_like_shifted",
        Arc::clone(&schema),
        [15, 20, 30],
        [
            vec![(1, 1, 10), (5, 2, 50), (10, 1, 100)],
            vec![(15, 2, 150)],
            vec![(20, 1, 200), (25, 2, 250)],
            vec![(30, 1, 300), (35, 2, 350)],
        ],
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

    register_csv_listing_table(
        ctx,
        "range_partitioned_shifted",
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_files/scratch_range_partitioning/range_partitioned_shifted"),
        Arc::clone(&schema),
        [
            "1,1,10\n5,2,50\n10,1,100\n",
            "15,2,150\n",
            "20,1,200\n25,2,250\n",
            "30,1,300\n35,2,350\n",
        ],
        Some(shifted_output_partitioning),
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

    register_csv_listing_table(
        ctx,
        "range_partitioned_narrow",
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_files/scratch_range_partitioning/range_partitioned_narrow"),
        schema,
        [
            "1,1,10\n5,2,50\n",
            "10,1,100\n15,2,150\n",
            "20,1,200\n25,2,250\n30,1,300\n35,2,350\n",
        ],
        Some(narrow_output_partitioning),
    );
}

fn register_csv_listing_table(
    ctx: &SessionContext,
    name: &str,
    table_dir: impl AsRef<Path>,
    schema: Arc<Schema>,
    partitions: impl IntoIterator<Item = &'static str>,
    output_partitioning: Option<Partitioning>,
) {
    let table_dir = table_dir.as_ref();
    if table_dir.exists() {
        remove_dir_all(table_dir).expect("test table dir should be removable");
    }
    create_dir_all(table_dir).expect("test table dir should be created");
    for (idx, rows) in partitions.into_iter().enumerate() {
        write(table_dir.join(format!("part-{idx}.csv")), rows)
            .expect("test table csv partition should be written");
    }

    let table_path = format!(
        "{}/",
        table_dir
            .to_str()
            .expect("test table path should be valid utf8")
    );
    let table_url =
        ListingTableUrl::parse(&table_path).expect("test table url should parse");
    let options =
        ListingOptions::new(Arc::new(CsvFormat::default().with_has_header(false)))
            .with_output_partitioning(output_partitioning);
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
    let range_key: Vec<i32> = rows.iter().map(|(range_key, _, _)| *range_key).collect();
    let non_range_key: Vec<i32> = rows
        .iter()
        .map(|(_, non_range_key, _)| *non_range_key)
        .collect();
    let value: Vec<i32> = rows.iter().map(|(_, _, value)| *value).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(range_key)) as ArrayRef,
            Arc::new(Int32Array::from(non_range_key)) as ArrayRef,
            Arc::new(Int32Array::from(value)) as ArrayRef,
        ],
    )
    .expect("range stream batch should be valid");
    Arc::new(TestPartitionStream::new_with_batches(vec![batch]))
}
