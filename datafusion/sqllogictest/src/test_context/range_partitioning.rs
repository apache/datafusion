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

use arrow::datatypes::{DataType, Field, Schema};
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
        schema,
        [
            "1,1,10\n5,2,50\n",
            "10,1,100\n15,2,150\n",
            "20,1,200\n25,2,250\n",
            "30,1,300\n35,2,350\n",
        ],
        Some(output_partitioning),
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
