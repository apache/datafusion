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

//! Tests for object store access patterns with [`ListingTable`]\
//!
//! These tests setup a `ListingTable` backed by an in-memory object store
//! that counts the number of requests made against it and then do
//! various operations (table creation, queries with and without predicates)
//! to verify the expected object store access patterns.
//!
//! [`ListingTable`]: datafusion::datasource::listing::ListingTable

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::prelude::{
    CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionContext,
};
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::assert_batches_eq;
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_csv::CsvFormat;
use datafusion_datasource_json::JsonFormat;
use datafusion_execution::cache::TableScopedPath;
use futures::stream::BoxStream;
use insta::assert_snapshot;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use parking_lot::Mutex;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn create_single_csv_file() {
    let test = Test::new().with_single_file_csv().await;
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 2
    - GET  (opts) path=csv_table.csv head=true
    - GET  (opts) path=csv_table.csv
    "
    );
}

#[tokio::test]
async fn query_single_csv_file() {
    let test = Test::new().with_single_file_csv().await;
    assert_snapshot!(
        test.query("select * from csv_table").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.00001 | 5e-12 | true  |
    | 0.00002 | 4e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 2
    - GET  (opts) path=csv_table.csv head=true
    - GET  (opts) path=csv_table.csv
    "
    );
}

#[tokio::test]
async fn create_multi_file_csv_file() {
    let test = Test::new().with_multi_file_csv().await;
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 4
    - LIST prefix=data
    - GET  (opts) path=data/file_0.csv
    - GET  (opts) path=data/file_1.csv
    - GET  (opts) path=data/file_2.csv
    "
    );
}

#[tokio::test]
async fn multi_query_multi_file_csv_file() {
    let test = Test::new().with_multi_file_csv().await;
    assert_snapshot!(
        test.query("select * from csv_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=data/file_0.csv
    - GET  (opts) path=data/file_1.csv
    - GET  (opts) path=data/file_2.csv
    "
    );

    // Force a cache eviction by removing the data limit for the cache
    assert_snapshot!(
        test.query("set datafusion.runtime.list_files_cache_limit=\"0K\"").await,
        @r"
    ------- Query Output (0 rows) -------
    ++
    ++
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 0
    "
    );

    // Then re-enable the cache
    assert_snapshot!(
        test.query("set datafusion.runtime.list_files_cache_limit=\"1M\"").await,
        @r"
    ------- Query Output (0 rows) -------
    ++
    ++
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 0
    "
    );

    // this query should list the table since the cache entries were evicted
    assert_snapshot!(
        test.query("select * from csv_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 4
    - LIST prefix=data
    - GET  (opts) path=data/file_0.csv
    - GET  (opts) path=data/file_1.csv
    - GET  (opts) path=data/file_2.csv
    "
    );

    // this query should not list the table since the entries were added in the previous query
    assert_snapshot!(
        test.query("select * from csv_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=data/file_0.csv
    - GET  (opts) path=data/file_1.csv
    - GET  (opts) path=data/file_2.csv
    "
    );
}

#[tokio::test]
async fn insert_invalidates_overlapping_unscoped_listings() {
    let store = Arc::new(InMemory::new());
    for (path, data) in [
        ("table/region=US/q1/data.csv", "1\n"),
        ("table/region=EU/data.csv", "2\n"),
    ] {
        store.put(&Path::from(path), data.into()).await.unwrap();
    }
    let ctx = SessionContext::new();
    ctx.runtime_env()
        .register_object_store(&Url::parse("mem://").unwrap(), store);
    ctx.sql("SET datafusion.execution.listing_table_ignore_subdirectory = false")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let cache = ctx
        .runtime_env()
        .cache_manager
        .get_list_files_cache()
        .unwrap();
    let tables = [
        ("root_table", "table"),
        ("child_table", "table/region=US"),
        ("descendant_table", "table/region=US/q1"),
        ("sibling_table", "table/region=EU"),
    ];

    for (name, path) in tables {
        // Keep the URLs unscoped so overlapping tables share path-based cache entries.
        let url = ListingTableUrl::parse(format!("mem:///{path}/")).unwrap();
        let options =
            ListingOptions::new(Arc::new(CsvFormat::default().with_has_header(false)))
                .with_file_extension(".csv");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(Arc::clone(&schema));
        ctx.register_table(name, Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
        ctx.sql(&format!("SELECT * FROM {name}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let key = TableScopedPath {
            table: None,
            path: Path::from(path),
        };
        assert!(cache.get(&key).is_some());
    }

    ctx.sql("INSERT INTO child_table VALUES (3)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    for (name, path) in tables {
        let key = TableScopedPath {
            table: None,
            path: Path::from(path),
        };
        let cached = cache.get(&key);
        assert_eq!(cached.is_some(), name == "sibling_table", "{name}");
    }

    let batches = ctx
        .sql("SELECT * FROM root_table ORDER BY value")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+-------+",
            "| value |",
            "+-------+",
            "| 1     |",
            "| 2     |",
            "| 3     |",
            "+-------+"
        ],
        &batches
    );
}

#[tokio::test]
async fn query_multi_csv_file() {
    let test = Test::new().with_multi_file_csv().await;
    assert_snapshot!(
        test.query("select * from csv_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=data/file_0.csv
    - GET  (opts) path=data/file_1.csv
    - GET  (opts) path=data/file_2.csv
    "
    );
}

/// Test that a CSV file split into byte ranges via repartitioning produces
/// exactly one GET request per byte range — no extra requests for boundary seeking.
///
/// With a single file and `target_partitions=3`, the repartitioner produces
/// exactly 3 ranges.  Each range is served by a single [`AlignedBoundaryStream`]
/// which issues exactly one bounded `get_opts` call, so there are 3 data GETs
/// plus 1 HEAD (to determine file size) = **4 total**.
///
/// This test documents the current request pattern to catch regressions.
#[tokio::test]
async fn query_csv_file_with_byte_range_partitions() {
    let test = Test::new().with_single_file_csv_for_range_test().await;
    // Lower the repartition_file_min_size threshold so the small test file gets
    // split, and set target_partitions=3 to produce exactly 3 ranges.
    test.query("SET datafusion.optimizer.repartition_file_min_size = 0")
        .await;
    test.query("SET datafusion.execution.target_partitions = 3")
        .await;
    assert_snapshot!(
        test.query("select * from csv_range_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.00001 | 1e-12 | false |
    | 0.00002 | 2e-12 | false |
    | 0.00003 | 3e-12 | false |
    | 0.00004 | 4e-12 | false |
    | 0.00005 | 5e-12 | false |
    | 0.00006 | 6e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 4
    - GET  (opts) path=csv_range_table.csv head=true
    - GET  (opts) path=csv_range_table.csv range=0-129
    - GET  (opts) path=csv_range_table.csv range=42-129
    - GET  (opts) path=csv_range_table.csv range=85-129
    "
    );
}

#[tokio::test]
async fn query_partitioned_csv_file() {
    let test = Test::new().with_partitioned_csv().await;
    assert_snapshot!(
        test.query("select * from csv_table_partitioned").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00001 | 1e-12 | true  | 1 | 10 | 100 |
    | 0.00003 | 5e-12 | false | 1 | 10 | 100 |
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    | 0.00003 | 3e-12 | true  | 3 | 30 | 300 |
    | 0.00003 | 5e-12 | false | 3 | 30 | 300 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=data/a=1/b=10/c=100/file_1.csv
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.csv
    - GET  (opts) path=data/a=3/b=30/c=300/file_3.csv
    "
    );

    assert_snapshot!(
        test.query("select * from csv_table_partitioned WHERE a=2").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.csv
    "
    );

    assert_snapshot!(
        test.query("select * from csv_table_partitioned WHERE b=20").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.csv
    "
    );

    assert_snapshot!(
        test.query("select * from csv_table_partitioned WHERE c=200").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.csv
    "
    );

    assert_snapshot!(
        test.query("select * from csv_table_partitioned WHERE a=2 AND b=20").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.csv
    "
    );

    assert_snapshot!(
        test.query("select * from csv_table_partitioned WHERE a<2 AND b=10 AND c=100").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00001 | 1e-12 | true  | 1 | 10 | 100 |
    | 0.00003 | 5e-12 | false | 1 | 10 | 100 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=1/b=10/c=100/file_1.csv
    "
    );
}

// =====================================================================
// JSON (NDJSON) tests — mirrors the CSV tests above
// =====================================================================

#[tokio::test]
async fn create_single_json_file() {
    let test = Test::new().with_single_file_json().await;
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 2
    - GET  (opts) path=json_table.json head=true
    - GET  (opts) path=json_table.json
    "
    );
}

#[tokio::test]
async fn query_single_json_file() {
    let test = Test::new().with_single_file_json().await;
    assert_snapshot!(
        test.query("select * from json_table").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.00001 | 5e-12 | true  |
    | 0.00002 | 4e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 2
    - GET  (opts) path=json_table.json head=true
    - GET  (opts) path=json_table.json
    "
    );
}

#[tokio::test]
async fn create_multi_file_json() {
    let test = Test::new().with_multi_file_json().await;
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 4
    - LIST prefix=data
    - GET  (opts) path=data/file_0.json
    - GET  (opts) path=data/file_1.json
    - GET  (opts) path=data/file_2.json
    "
    );
}

#[tokio::test]
async fn multi_query_multi_file_json() {
    let test = Test::new().with_multi_file_json().await;
    assert_snapshot!(
        test.query("select * from json_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=data/file_0.json
    - GET  (opts) path=data/file_1.json
    - GET  (opts) path=data/file_2.json
    "
    );

    // Force a cache eviction by removing the data limit for the cache
    assert_snapshot!(
        test.query("set datafusion.runtime.list_files_cache_limit=\"0K\"").await,
        @r"
    ------- Query Output (0 rows) -------
    ++
    ++
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 0
    "
    );

    // Then re-enable the cache
    assert_snapshot!(
        test.query("set datafusion.runtime.list_files_cache_limit=\"1M\"").await,
        @r"
    ------- Query Output (0 rows) -------
    ++
    ++
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 0
    "
    );

    // this query should list the table since the cache entries were evicted
    assert_snapshot!(
        test.query("select * from json_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 4
    - LIST prefix=data
    - GET  (opts) path=data/file_0.json
    - GET  (opts) path=data/file_1.json
    - GET  (opts) path=data/file_2.json
    "
    );

    // this query should not list the table since the entries were added in the previous query
    assert_snapshot!(
        test.query("select * from json_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=data/file_0.json
    - GET  (opts) path=data/file_1.json
    - GET  (opts) path=data/file_2.json
    "
    );
}

#[tokio::test]
async fn query_multi_json_file() {
    let test = Test::new().with_multi_file_json().await;
    assert_snapshot!(
        test.query("select * from json_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=data/file_0.json
    - GET  (opts) path=data/file_1.json
    - GET  (opts) path=data/file_2.json
    "
    );
}

#[tokio::test]
async fn query_partitioned_json_file() {
    let test = Test::new().with_partitioned_json().await;
    assert_snapshot!(
        test.query("select * from json_table_partitioned").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00001 | 1e-12 | true  | 1 | 10 | 100 |
    | 0.00003 | 5e-12 | false | 1 | 10 | 100 |
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    | 0.00003 | 3e-12 | true  | 3 | 30 | 300 |
    | 0.00003 | 5e-12 | false | 3 | 30 | 300 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=data/a=1/b=10/c=100/file_1.json
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.json
    - GET  (opts) path=data/a=3/b=30/c=300/file_3.json
    "
    );

    assert_snapshot!(
        test.query("select * from json_table_partitioned WHERE a=2").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.json
    "
    );

    assert_snapshot!(
        test.query("select * from json_table_partitioned WHERE b=20").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.json
    "
    );

    assert_snapshot!(
        test.query("select * from json_table_partitioned WHERE c=200").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.json
    "
    );

    assert_snapshot!(
        test.query("select * from json_table_partitioned WHERE a=2 AND b=20").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00002 | 2e-12 | true  | 2 | 20 | 200 |
    | 0.00003 | 5e-12 | false | 2 | 20 | 200 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=2/b=20/c=200/file_2.json
    "
    );

    assert_snapshot!(
        test.query("select * from json_table_partitioned WHERE a<2 AND b=10 AND c=100").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+---+----+-----+
    | d1      | d2    | d3    | a | b  | c   |
    +---------+-------+-------+---+----+-----+
    | 0.00001 | 1e-12 | true  | 1 | 10 | 100 |
    | 0.00003 | 5e-12 | false | 1 | 10 | 100 |
    +---------+-------+-------+---+----+-----+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 1
    - GET  (opts) path=data/a=1/b=10/c=100/file_1.json
    "
    );
}

/// Test that a JSON file split into byte ranges via repartitioning produces
/// exactly one GET request per byte range — no extra requests for boundary seeking.
///
/// With a single file and `target_partitions=3`, the repartitioner produces
/// exactly 3 ranges.  Each range is served by a single [`AlignedBoundaryStream`]
/// which issues exactly one bounded `get_opts` call, so there are 3 data GETs
/// plus 1 HEAD (to determine file size) = **4 total**.
///
/// This differs from the CSV reader, which needs multiple GETs per range.
///
/// This test documents the current request pattern to catch regressions.
#[tokio::test]
async fn query_json_file_with_byte_range_partitions() {
    let test = Test::new().with_single_file_json_for_range_test().await;
    // Lower the repartition_file_min_size threshold so the small test file gets
    // split, and set target_partitions=3 to produce exactly 3 ranges.
    test.query("SET datafusion.optimizer.repartition_file_min_size = 0")
        .await;
    test.query("SET datafusion.execution.target_partitions = 3")
        .await;
    assert_snapshot!(
        test.query("select * from json_range_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+------+
    | c1      | c2    | c3   |
    +---------+-------+------+
    | 0.00001 | 1e-12 | true |
    | 0.00002 | 2e-12 | true |
    | 0.00003 | 3e-12 | true |
    | 0.00004 | 4e-12 | true |
    | 0.00005 | 5e-12 | true |
    | 0.00006 | 6e-12 | true |
    +---------+-------+------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 4
    - GET  (opts) path=json_range_table.json head=true
    - GET  (opts) path=json_range_table.json range=0-216
    - GET  (opts) path=json_range_table.json range=71-216
    - GET  (opts) path=json_range_table.json range=143-216
    "
    );
}

#[tokio::test]
async fn create_single_parquet_file_default() {
    // The default metadata size hint is 512KB
    // which is enough to fetch the entire footer metadata and PageIndex
    // in a single GET request.
    let test = Test::new().with_single_file_parquet().await;
    // expect 1 get request which reads the footer metadata and page index
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 2
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=0-2994
    "
    );
}

#[tokio::test]
async fn create_single_parquet_file_prefetch() {
    // Explicitly specify a prefetch hint that is adequate for the footer and page index
    let test = Test::new()
        .with_parquet_metadata_size_hint(Some(1000))
        .with_single_file_parquet()
        .await;
    // expect 1 1000 byte request which reads the footer metadata and page index
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 2
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=1994-2994
    "
    );
}

#[tokio::test]
async fn create_single_parquet_file_too_small_prefetch() {
    // configure a prefetch size that is too small to fetch the footer
    // metadata
    //
    // Using the ranges from  the test below (with no_prefetch),
    // pick a number less than 730:
    // --------
    // 2286-2294: (8 bytes) footer + length
    // 2264-2986: (722 bytes) footer metadata
    let test = Test::new()
        .with_parquet_metadata_size_hint(Some(500))
        .with_single_file_parquet()
        .await;
    // expect three get requests:
    // 1. read the footer (500 bytes per hint, not enough for the footer metadata)
    // 2. Read the footer metadata
    // 3. reads the PageIndex
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 4
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=2494-2994
    - GET  (ranges) path=parquet_table.parquet ranges=2264-2986
    - GET  (ranges) path=parquet_table.parquet ranges=2124-2264
    "
    );
}

#[tokio::test]
async fn create_single_parquet_file_small_prefetch() {
    // configure a prefetch size that is large enough for the footer
    // metadata but **not** the PageIndex
    //
    // Using the ranges from the test below (with no_prefetch),
    // the 730 is determined as follows;
    // --------
    // 2286-2294: (8 bytes) footer + length
    // 2264-2986: (722 bytes) footer metadata
    let test = Test::new()
        // 740 is enough to get both the footer + length (8 bytes)
        // but not the entire PageIndex
        .with_parquet_metadata_size_hint(Some(740))
        .with_single_file_parquet()
        .await;
    // expect two get requests:
    // 1. read the footer metadata
    // 2. reads the PageIndex
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=2254-2994
    - GET  (ranges) path=parquet_table.parquet ranges=2124-2264
    "
    );
}

#[tokio::test]
async fn create_single_parquet_file_no_prefetch() {
    let test = Test::new()
        // force no prefetch by setting size hint to None
        .with_parquet_metadata_size_hint(None)
        .with_single_file_parquet()
        .await;
    // Without a metadata size hint, the parquet reader
    // does *three* range requests to read the footer metadata:
    // 1. The footer length (last 8 bytes)
    // 2. The footer metadata
    // 3. The PageIndex metadata
    assert_snapshot!(
        test.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 2
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=0-2994
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file() {
    let test = Test::new().with_single_file_parquet().await;
    assert_snapshot!(
        test.query("select count(distinct a), count(b) from parquet_table").await,
        @r"
    ------- Query Output (1 rows) -------
    +---------------------------------+------------------------+
    | count(DISTINCT parquet_table.a) | count(parquet_table.b) |
    +---------------------------------+------------------------+
    | 200                             | 200                    |
    +---------------------------------+------------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=4-534,534-1064
    - GET  (ranges) path=parquet_table.parquet ranges=1064-1594,1594-2124
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_with_single_predicate() {
    let test = Test::new();
    // This test documents the I/O pattern without filter pushdown (the
    // `*_pushdown_filters_*` tests below cover the default); the predicate
    // only prunes row groups here. The option is captured when the table is
    // registered, so it is set first.
    test.set("datafusion.execution.parquet.pushdown_filters", "false")
        .await;
    let test = test.with_single_file_parquet().await;
    assert_snapshot!(
        test.query("select min(a), max(b) from parquet_table WHERE a > 150").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+----------------------+
    | min(parquet_table.a) | max(parquet_table.b) |
    +----------------------+----------------------+
    | 151                  | 1199                 |
    +----------------------+----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 2
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=1064-1594,1594-2124
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_multi_row_groups_multiple_predicates() {
    let test = Test::new();
    // This test documents the I/O pattern without filter pushdown (the
    // `*_pushdown_filters_*` tests below cover the default); the predicates
    // only prune row groups here. The option is captured when the table is
    // registered, so it is set first.
    test.set("datafusion.execution.parquet.pushdown_filters", "false")
        .await;
    let test = test.with_single_file_parquet().await;
    assert_snapshot!(
        test.query("select min(a), max(b) from parquet_table WHERE a > 50 AND b < 1150").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+----------------------+
    | min(parquet_table.a) | max(parquet_table.b) |
    +----------------------+----------------------+
    | 51                   | 1149                 |
    +----------------------+----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=4-534,534-1064
    - GET  (ranges) path=parquet_table.parquet ranges=1064-1594,1594-2124
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_pushdown_filters_one_shot_io() {
    let test = Test::new();
    // the options are captured when the table is registered, so set them first
    test.set("datafusion.execution.parquet.pushdown_filters", "true")
        .await;
    test.set("datafusion.execution.parquet.progressive_io", "false")
        .await;
    let test = test.with_single_file_parquet().await;

    // With filter pushdown enabled and `progressive_io` disabled (the
    // default), all data needed for each row group (the columns needed by
    // the filter and by the projection) is fetched with a single request per
    // row group, the same I/O pattern as when pushdown_filters is disabled
    // (compare `query_single_parquet_file_multi_row_groups_multiple_predicates`):
    //
    // * Row group 1 (`a` in 0..100): `a` (4-534) and `b` (534-1064) are
    //   fetched together, even though `a` is only needed to evaluate the
    //   filter.
    // * Row group 2 (`a` in 100..200): the statistics prove every row
    //   matches, so the filter is skipped and only `b` (1594-2124) is read.
    assert_snapshot!(
        test.query("select min(b) from parquet_table WHERE a > 50").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+
    | min(parquet_table.b) |
    +----------------------+
    | 1051                 |
    +----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=4-534,534-1064
    - GET  (ranges) path=parquet_table.parquet ranges=1594-2124
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_pushdown_filters_progressive_io() {
    let test = Test::new().with_single_file_parquet().await;
    test.set("datafusion.execution.parquet.pushdown_filters", "true")
        .await;
    test.set("datafusion.execution.parquet.progressive_io", "true")
        .await;

    // With `progressive_io` enabled (and an offset index present in the
    // file), a row group that needs the filter is fetched progressively:
    // first the column needed to evaluate the filter (`a`, 4-534), then the
    // remaining projected column (`b`, 534-1064) for the rows that passed,
    // resulting in multiple requests for row group 1. Row group 2 is fully
    // matched by its statistics, so the filter is skipped and only `b` is
    // read, as in the one-shot case.
    assert_snapshot!(
        test.query("select min(b) from parquet_table WHERE a > 50").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+
    | min(parquet_table.b) |
    +----------------------+
    | 1051                 |
    +----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 4
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=4-534
    - GET  (ranges) path=parquet_table.parquet ranges=534-951,951-1064
    - GET  (ranges) path=parquet_table.parquet ranges=1594-2124
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_multi_page_no_pushdown() {
    let test = Test::new();
    // the option is captured when the table is registered, so it is set first
    test.set("datafusion.execution.parquet.pushdown_filters", "false")
        .await;
    let test = test.with_single_file_parquet_multi_page().await;

    // Baseline for the `*_page_index` tests below: without filter pushdown
    // the page index still prunes the first two pages of `a` in row group 1
    // (values 0..40 cannot match `a > 50`) and the matching rows of `b`, so
    // only the dictionary page and the selected data pages of each column
    // chunk are fetched, with a single request per row group.
    assert_snapshot!(
        test.query("select min(b) from parquet_table WHERE a > 50").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+
    | min(parquet_table.b) |
    +----------------------+
    | 1051                 |
    +----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=4-421,492-529,529-569,569-609,609-1026,1097-1134,1134-1174,1174-1214
    - GET  (ranges) path=parquet_table.parquet ranges=1214-1819,1819-2424
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_pushdown_filters_one_shot_io_page_index() {
    let test = Test::new();
    // the options are captured when the table is registered, so set them first
    test.set("datafusion.execution.parquet.pushdown_filters", "true")
        .await;
    test.set("datafusion.execution.parquet.progressive_io", "false")
        .await;
    let test = test.with_single_file_parquet_multi_page().await;

    // Each row group has five 20-row data pages per column. The page index
    // proves the first two pages of `a` in row group 1 (values 0..40) cannot
    // match `a > 50`, so the resulting row selection skips them. With
    // one-shot I/O the data is still fetched with a single request per row
    // group, but only the pages the decoder will read:
    //
    // * `a` (needed by the filter) becomes two ranges: its dictionary page
    //   (4-421) and the run of selected data pages (492-609); the two pruned
    //   pages (421-492) are not fetched.
    // * `b` (the output column) is fetched as one range (609-1214). Note the
    //   decoder reads every page of `b` once a row filter is installed, even
    //   though the first two pages hold no selected rows (compare
    //   `query_single_parquet_file_multi_page_no_pushdown`, which skips
    //   1026-1097). The plan reproduces the decoder's own fetch, so one-shot
    //   I/O reads the same bytes as progressive I/O does.
    // * Row group 2 is fully matched by its statistics, so the filter is
    //   skipped and only `b` (1819-2424) is read.
    assert_snapshot!(
        test.query("select min(b) from parquet_table WHERE a > 50").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+
    | min(parquet_table.b) |
    +----------------------+
    | 1051                 |
    +----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=4-421,492-609,609-1214
    - GET  (ranges) path=parquet_table.parquet ranges=1819-2424
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_pushdown_filters_progressive_io_page_index() {
    let test = Test::new().with_single_file_parquet_multi_page().await;
    test.set("datafusion.execution.parquet.pushdown_filters", "true")
        .await;
    test.set("datafusion.execution.parquet.progressive_io", "true")
        .await;

    // Same file and query as
    // `query_single_parquet_file_pushdown_filters_one_shot_io_page_index`,
    // but fetched progressively: the dictionary and selected pages of `a`
    // first, then (after the filter is evaluated) the dictionary and data
    // pages of `b`. The bytes read are the same as with one-shot I/O; only
    // the number of requests differs.
    assert_snapshot!(
        test.query("select min(b) from parquet_table WHERE a > 50").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+
    | min(parquet_table.b) |
    +----------------------+
    | 1051                 |
    +----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 4
    - GET  (opts) path=parquet_table.parquet head=true
    - GET  (ranges) path=parquet_table.parquet ranges=4-421,492-529,529-569,569-609
    - GET  (ranges) path=parquet_table.parquet ranges=609-1026,1026-1060,1060-1097,1097-1134,1134-1174,1174-1214
    - GET  (ranges) path=parquet_table.parquet ranges=1819-2424
    "
    );
}

/// Runs tests with a request counting object store
struct Test {
    object_store: Arc<RequestCountingObjectStore>,
    session_context: SessionContext,
    /// metadata size hint to use when registering parquet files
    ///
    /// * `None`: uses the default (does not set a size_hint)
    /// * `Some(None)`L: set prefetch hint to None (prefetching)
    /// * `Some(Some(size))`: set prefetch hint to size
    #[expect(
        clippy::option_option,
        reason = "The nesting is meaningful, see the doc comment above"
    )]
    parquet_metadata_size_hint: Option<Option<usize>>,
}

impl Test {
    fn new() -> Self {
        let object_store = Arc::new(RequestCountingObjectStore::new());
        let session_context = SessionContext::new();
        session_context
            .runtime_env()
            .register_object_store(&Url::parse("mem://").unwrap(), object_store.clone());
        Self {
            object_store,
            session_context,
            parquet_metadata_size_hint: None,
        }
    }

    /// Specify the metadata size hint to use when registering parquet files
    fn with_parquet_metadata_size_hint(mut self, size_hint: Option<usize>) -> Self {
        self.parquet_metadata_size_hint = Some(size_hint);
        self
    }

    /// Returns a string representation of all recorded requests thus far
    fn requests(&self) -> String {
        format!("{}", self.object_store)
    }

    /// Set a session configuration value via SQL `SET`
    async fn set(&self, key: &str, value: &str) {
        self.session_context
            .sql(&format!("set {key} = {value}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    /// Store the specified bytes at the given path
    async fn with_bytes(self, path: &str, bytes: impl Into<Bytes>) -> Self {
        let path = Path::from(path);
        self.object_store
            .inner
            .put(&path, PutPayload::from(bytes.into()))
            .await
            .unwrap();
        self
    }

    /// Register a CSV file at the given path
    async fn register_csv(self, table_name: &str, path: &str) -> Self {
        let mut options = CsvReadOptions::new();
        options.has_header = true;
        let url = format!("mem://{path}");
        self.session_context
            .register_csv(table_name, url, options)
            .await
            .unwrap();
        self
    }

    /// Register a partitioned CSV table at the given path
    async fn register_partitioned_csv(self, table_name: &str, path: &str) -> Self {
        let file_format = Arc::new(CsvFormat::default().with_has_header(true));
        let options = ListingOptions::new(file_format);

        let url = format!("mem://{path}").parse().unwrap();
        let table_url = ListingTableUrl::try_new(url, None).unwrap();

        let session_state = self.session_context.state();
        let mut config = ListingTableConfig::new(table_url).with_listing_options(options);
        config = config
            .infer_partitions_from_path(&session_state)
            .await
            .unwrap();
        config = config.infer_schema(&session_state).await.unwrap();

        let table = Arc::new(ListingTable::try_new(config).unwrap());
        self.session_context
            .register_table(table_name, table)
            .unwrap();
        self
    }

    /// Register a Parquet file at the given path
    async fn register_parquet(self, table_name: &str, path: &str) -> Self {
        let path = format!("mem://{path}");
        let mut options: ParquetReadOptions<'_> = ParquetReadOptions::new();

        // If a metadata size hint was specified, apply it
        if let Some(parquet_metadata_size_hint) = self.parquet_metadata_size_hint {
            options = options.metadata_size_hint(parquet_metadata_size_hint);
        }

        self.session_context
            .register_parquet(table_name, path, options)
            .await
            .unwrap();
        self
    }

    /// Register a single CSV file with three columns and two row named
    /// `csv_table`
    async fn with_single_file_csv(self) -> Test {
        // upload CSV data to object store
        let csv_data = r#"c1,c2,c3
0.00001,5e-12,true
0.00002,4e-12,false
"#;
        self.with_bytes("/csv_table.csv", csv_data)
            .await
            .register_csv("csv_table", "/csv_table.csv")
            .await
    }

    /// Register three CSV files in a directory, called `csv_table`
    async fn with_multi_file_csv(mut self) -> Test {
        // upload CSV data to object store
        for i in 0..3 {
            let csv_data1 = format!(
                r#"c1,c2,c3
0.0000{i},{i}e-12,true
0.00003,5e-12,false
"#
            );
            self = self
                .with_bytes(&format!("/data/file_{i}.csv"), csv_data1)
                .await;
        }
        // register table
        self.register_csv("csv_table", "/data/").await
    }

    /// Register three CSV files in a partitioned directory structure, called
    /// `csv_table_partitioned`
    async fn with_partitioned_csv(mut self) -> Test {
        for i in 1..4 {
            // upload CSV data to object store
            let csv_data1 = format!(
                r#"d1,d2,d3
0.0000{i},{i}e-12,true
0.00003,5e-12,false
"#
            );
            self = self
                .with_bytes(
                    &format!("/data/a={i}/b={}/c={}/file_{i}.csv", i * 10, i * 100),
                    csv_data1,
                )
                .await;
        }
        // register table
        self.register_partitioned_csv("csv_table_partitioned", "/data/")
            .await
    }

    /// Register a single CSV file with six equal-length rows for byte-range
    /// repartitioning tests. With a single file and `target_partitions=3`, the
    /// repartitioner creates exactly 3 ranges.
    async fn with_single_file_csv_for_range_test(self) -> Test {
        let csv_data = "c1,c2,c3\n\
                        0.00001,1e-12,false\n\
                        0.00002,2e-12,false\n\
                        0.00003,3e-12,false\n\
                        0.00004,4e-12,false\n\
                        0.00005,5e-12,false\n\
                        0.00006,6e-12,false\n";
        self.with_bytes("/csv_range_table.csv", csv_data)
            .await
            .register_csv("csv_range_table", "/csv_range_table.csv")
            .await
    }

    /// Register a JSON (NDJSON) file at the given path
    async fn register_json(self, table_name: &str, path: &str) -> Self {
        let url = format!("mem://{path}");
        self.session_context
            .register_json(table_name, url, JsonReadOptions::default())
            .await
            .unwrap();
        self
    }

    /// Register a partitioned JSON table at the given path
    async fn register_partitioned_json(self, table_name: &str, path: &str) -> Self {
        let file_format = Arc::new(JsonFormat::default());
        let options = ListingOptions::new(file_format);

        let url = format!("mem://{path}").parse().unwrap();
        let table_url = ListingTableUrl::try_new(url, None).unwrap();

        let session_state = self.session_context.state();
        let mut config = ListingTableConfig::new(table_url).with_listing_options(options);
        config = config
            .infer_partitions_from_path(&session_state)
            .await
            .unwrap();
        config = config.infer_schema(&session_state).await.unwrap();

        let table = Arc::new(ListingTable::try_new(config).unwrap());
        self.session_context
            .register_table(table_name, table)
            .unwrap();
        self
    }

    /// Register a single NDJSON file with three columns and two rows named `json_table`
    async fn with_single_file_json(self) -> Test {
        let json_data = "{\"c1\":0.00001,\"c2\":5e-12,\"c3\":true}\n\
                         {\"c1\":0.00002,\"c2\":4e-12,\"c3\":false}\n";
        self.with_bytes("/json_table.json", json_data)
            .await
            .register_json("json_table", "/json_table.json")
            .await
    }

    /// Register a single NDJSON file with six equal-length rows for byte-range
    /// repartitioning tests. With a single file and `target_partitions=3`, the
    /// repartitioner creates exactly 3 ranges.
    async fn with_single_file_json_for_range_test(self) -> Test {
        let json_data = r#"{"c1":0.00001,"c2":1e-12,"c3":true}
{"c1":0.00002,"c2":2e-12,"c3":true}
{"c1":0.00003,"c2":3e-12,"c3":true}
{"c1":0.00004,"c2":4e-12,"c3":true}
{"c1":0.00005,"c2":5e-12,"c3":true}
{"c1":0.00006,"c2":6e-12,"c3":true}
"#;
        self.with_bytes("/json_range_table.json", json_data)
            .await
            .register_json("json_range_table", "/json_range_table.json")
            .await
    }

    /// Register three NDJSON files in a directory, called `json_table`
    async fn with_multi_file_json(mut self) -> Test {
        for i in 0..3 {
            let json_data = format!(
                "{{\"c1\":0.0000{i},\"c2\":{i}e-12,\"c3\":true}}\n\
                 {{\"c1\":0.00003,\"c2\":5e-12,\"c3\":false}}\n"
            );
            self = self
                .with_bytes(&format!("/data/file_{i}.json"), json_data)
                .await;
        }
        self.register_json("json_table", "/data/").await
    }

    /// Register three NDJSON files in a partitioned directory structure, called
    /// `json_table_partitioned`
    async fn with_partitioned_json(mut self) -> Test {
        for i in 1..4 {
            let json_data = format!(
                "{{\"d1\":0.0000{i},\"d2\":{i}e-12,\"d3\":true}}\n\
                 {{\"d1\":0.00003,\"d2\":5e-12,\"d3\":false}}\n"
            );
            self = self
                .with_bytes(
                    &format!("/data/a={i}/b={}/c={}/file_{i}.json", i * 10, i * 100),
                    json_data,
                )
                .await;
        }
        self.register_partitioned_json("json_table_partitioned", "/data/")
            .await
    }

    /// Add a single parquet file that has two columns and two row groups named `parquet_table`
    ///
    /// Column "a": Int32 with values 0-100] in row group 1
    /// and [101-200] in row group 2
    ///
    /// Column "b": Int32 with values 1000-1100] in row group 1
    /// and [1101-1200] in row group 2
    async fn with_single_file_parquet(self) -> Test {
        // Create parquet bytes
        let a: ArrayRef = Arc::new(Int32Array::from_iter_values(0..200));
        let b: ArrayRef = Arc::new(Int32Array::from_iter_values(1000..1200));
        let batch = RecordBatch::try_from_iter([("a", a), ("b", b)]).unwrap();

        let mut buffer = vec![];
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            &mut buffer,
            batch.schema(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        self.with_bytes("/parquet_table.parquet", buffer)
            .await
            .register_parquet("parquet_table", "/parquet_table.parquet")
            .await
    }

    /// Like [`Self::with_single_file_parquet`], but each column chunk is
    /// written as five 20-row data pages (plus a dictionary page) so that
    /// the page index can prune individual pages within a row group.
    async fn with_single_file_parquet_multi_page(self) -> Test {
        let a: ArrayRef = Arc::new(Int32Array::from_iter_values(0..200));
        let b: ArrayRef = Arc::new(Int32Array::from_iter_values(1000..1200));
        let batch = RecordBatch::try_from_iter([("a", a), ("b", b)]).unwrap();

        let mut buffer = vec![];
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .set_data_page_row_count_limit(20)
            .set_write_batch_size(20)
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            &mut buffer,
            batch.schema(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        self.with_bytes("/parquet_table.parquet", buffer)
            .await
            .register_parquet("parquet_table", "/parquet_table.parquet")
            .await
    }

    /// Runs the specified query and returns a string representation of the results
    /// suitable for comparison with insta snapshots
    ///
    /// Clears all recorded requests before running the query
    async fn query(&self, sql: &str) -> String {
        self.object_store.clear_requests();
        let results = self
            .session_context
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let num_rows = results.iter().map(|batch| batch.num_rows()).sum::<usize>();
        let formatted_result =
            arrow::util::pretty::pretty_format_batches(&results).unwrap();

        let object_store = &self.object_store;

        format!(
            r#"------- Query Output ({num_rows} rows) -------
{formatted_result}
------- Object Store Request Summary -------
{object_store}
"#
        )
    }
}

/// Details of individual requests made through the [`RequestCountingObjectStore`]
#[derive(Clone, Debug)]
enum RequestDetails {
    GetOpts { path: Path, get_options: GetOptions },
    GetRanges { path: Path, ranges: Vec<Range<u64>> },
    List { prefix: Option<Path> },
    ListWithDelimiter { prefix: Option<Path> },
    ListWithOffset { prefix: Option<Path>, offset: Path },
}

fn display_range(range: &Range<u64>) -> impl Display + '_ {
    struct Wrapper<'a>(&'a Range<u64>);
    impl Display for Wrapper<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "{}-{}", self.0.start, self.0.end)
        }
    }
    Wrapper(range)
}
impl Display for RequestDetails {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RequestDetails::GetOpts { path, get_options } => {
                write!(f, "GET  (opts) path={path}")?;
                if let Some(range) = &get_options.range {
                    match range {
                        GetRange::Bounded(range) => {
                            let range = display_range(range);
                            write!(f, " range={range}")?;
                        }
                        GetRange::Offset(offset) => {
                            write!(f, " range=offset:{offset}")?;
                        }
                        GetRange::Suffix(suffix) => {
                            write!(f, " range=suffix:{suffix}")?;
                        }
                    }
                }
                if let Some(version) = &get_options.version {
                    write!(f, " version={version}")?;
                }
                if get_options.head {
                    write!(f, " head=true")?;
                }
            }
            RequestDetails::GetRanges { path, ranges } => {
                write!(f, "GET  (ranges) path={path}")?;
                if !ranges.is_empty() {
                    write!(f, " ranges=")?;
                    for (i, range) in ranges.iter().enumerate() {
                        if i > 0 {
                            write!(f, ",")?;
                        }
                        write!(f, "{}", display_range(range))?;
                    }
                }
            }
            RequestDetails::List { prefix } => {
                write!(f, "LIST")?;
                if let Some(prefix) = prefix {
                    write!(f, " prefix={prefix}")?;
                }
            }
            RequestDetails::ListWithDelimiter { prefix } => {
                write!(f, "LIST (with delimiter)")?;
                if let Some(prefix) = prefix {
                    write!(f, " prefix={prefix}")?;
                }
            }
            RequestDetails::ListWithOffset { prefix, offset } => {
                write!(f, "LIST (with offset) offset={offset}")?;
                if let Some(prefix) = prefix {
                    write!(f, " prefix={prefix}")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct RequestCountingObjectStore {
    /// Inner (memory) store
    inner: Arc<dyn ObjectStore>,
    requests: Mutex<Vec<RequestDetails>>,
}

impl Display for RequestCountingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RequestCountingObjectStore()")?;
        let requests = self.recorded_requests();
        write!(f, "\nTotal Requests: {}", requests.len())?;
        for request in requests {
            write!(f, "\n- {request}")?;
        }
        Ok(())
    }
}

impl RequestCountingObjectStore {
    pub fn new() -> Self {
        let inner = Arc::new(InMemory::new());
        Self {
            inner,
            requests: Mutex::new(vec![]),
        }
    }

    pub fn clear_requests(&self) {
        self.requests.lock().clear();
    }

    /// Return a copy of the recorded requests normalized
    /// by removing the path prefix
    pub fn recorded_requests(&self) -> Vec<RequestDetails> {
        self.requests.lock().to_vec()
    }
}

#[async_trait]
impl ObjectStore for RequestCountingObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options.clone()).await?;
        self.requests.lock().push(RequestDetails::GetOpts {
            path: location.to_owned(),
            get_options: options,
        });
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await?;
        self.requests.lock().push(RequestDetails::GetRanges {
            path: location.to_owned(),
            ranges: ranges.to_vec(),
        });
        Ok(result)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.requests.lock().push(RequestDetails::List {
            prefix: prefix.map(|p| p.to_owned()),
        });

        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.requests.lock().push(RequestDetails::ListWithOffset {
            prefix: prefix.map(|p| p.to_owned()),
            offset: offset.to_owned(),
        });
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.requests
            .lock()
            .push(RequestDetails::ListWithDelimiter {
                prefix: prefix.map(|p| p.to_owned()),
            });
        self.inner.list_with_delimiter(prefix).await
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        unimplemented!()
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        unimplemented!()
    }
}
