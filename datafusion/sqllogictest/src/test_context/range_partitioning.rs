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

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::{ScalarValue, SplitPoint};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::{Partitioning, RangePartitioning, col};
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

    register_csv_listing_table(
        ctx,
        "range_partitioned",
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_files/scratch_range_partitioning/range_partitioned"),
        Arc::clone(&schema),
        [
            "1,1,10\n5,2,50\n",
            "10,1,100\n15,2,150\n",
            "20,1,200\n25,2,250\n",
            "30,1,300\n35,2,350\n",
        ],
        Some(output_partitioning),
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
        schema,
        [
            "1,1,10\n5,2,50\n10,1,100\n",
            "15,2,150\n",
            "20,1,200\n25,2,250\n",
            "30,1,300\n35,2,350\n",
        ],
        Some(shifted_output_partitioning),
    );

    let time_schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Int32, false),
    ]));
    let time_output_partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            vec![col("ts").sort(true, true)],
            vec![
                SplitPoint::new(vec![ScalarValue::TimestampNanosecond(
                    Some(1_704_069_000_000_000_000),
                    None,
                )]),
                SplitPoint::new(vec![ScalarValue::TimestampNanosecond(
                    Some(1_704_072_600_000_000_000),
                    None,
                )]),
                SplitPoint::new(vec![ScalarValue::TimestampNanosecond(
                    Some(1_704_076_200_000_000_000),
                    None,
                )]),
            ],
        )
        .expect("range partitioning should be valid"),
    );

    register_csv_listing_table(
        ctx,
        "range_partitioned_time",
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_files/scratch_range_partitioning/range_partitioned_time"),
        time_schema,
        [
            "2024-01-01T00:15:00,1\n",
            "2024-01-01T00:45:00,2\n2024-01-01T01:15:00,4\n",
            "2024-01-01T01:45:00,8\n2024-01-01T02:15:00,16\n",
            "2024-01-01T02:45:00,32\n",
        ],
        Some(time_output_partitioning),
    );

    let time_a_schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("a", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let time_a_split_point = |ts| {
        SplitPoint::new(vec![
            ScalarValue::TimestampNanosecond(Some(ts), None),
            ScalarValue::Int32(Some(i32::MIN)),
        ])
    };
    let time_a_ordering = || vec![col("ts").sort(true, true), col("a").sort(true, true)];

    let time_a_crossing_output_partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            time_a_ordering(),
            vec![
                time_a_split_point(1_704_069_000_000_000_000),
                time_a_split_point(1_704_072_600_000_000_000),
                time_a_split_point(1_704_076_200_000_000_000),
            ],
        )
        .expect("range partitioning should be valid"),
    );

    register_csv_listing_table(
        ctx,
        "range_partitioned_time_a_crossing",
        Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "test_files/scratch_range_partitioning/range_partitioned_time_a_crossing",
        ),
        Arc::clone(&time_a_schema),
        [
            "2024-01-01T00:15:00,1,1\n2024-01-01T00:15:00,2,10\n",
            "2024-01-01T00:45:00,1,2\n2024-01-01T01:15:00,1,4\n2024-01-01T01:15:00,2,20\n",
            "2024-01-01T01:45:00,1,8\n2024-01-01T02:15:00,1,16\n",
            "2024-01-01T02:45:00,1,32\n2024-01-01T03:15:00,1,64\n",
        ],
        Some(time_a_crossing_output_partitioning),
    );

    let time_a_aligned_output_partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            time_a_ordering(),
            vec![
                time_a_split_point(1_704_070_800_000_000_000),
                time_a_split_point(1_704_074_400_000_000_000),
                time_a_split_point(1_704_078_000_000_000_000),
            ],
        )
        .expect("range partitioning should be valid"),
    );

    register_csv_listing_table(
        ctx,
        "range_partitioned_time_a_aligned",
        Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "test_files/scratch_range_partitioning/range_partitioned_time_a_aligned",
        ),
        time_a_schema,
        [
            "2024-01-01T00:15:00,1,1\n2024-01-01T00:45:00,1,2\n2024-01-01T00:15:00,2,10\n",
            "2024-01-01T01:15:00,1,4\n2024-01-01T01:45:00,1,8\n2024-01-01T01:15:00,2,20\n",
            "2024-01-01T02:15:00,1,16\n2024-01-01T02:45:00,1,32\n",
            "2024-01-01T03:15:00,1,64\n",
        ],
        Some(time_a_aligned_output_partitioning),
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
