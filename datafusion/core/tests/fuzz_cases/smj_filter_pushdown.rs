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

//! Integration tests for dynamic filter pushdown through SortMergeJoinExec.
//!
//! These tests verify that when TopK dynamic filters are pushed through
//! SortMergeJoinExec for Inner joins, the query results remain correct.
//! Each test runs the same query with and without dynamic filter pushdown
//! and compares the results.
//!
//! Data is written to in-memory parquet files (via an InMemory object store)
//! so that the DataSourceExec supports filter pushdown — in-memory tables
//! do not.

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_parquet::ParquetFormat;
use datafusion_execution::object_store::ObjectStoreUrl;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;

fn left_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]))
}

fn left_batch() -> RecordBatch {
    RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi",
                "ivan", "judy",
            ])),
            Arc::new(Float64Array::from(vec![
                90.0, 85.0, 72.0, 95.0, 60.0, 88.0, 77.0, 91.0, 68.0, 83.0,
            ])),
        ],
    )
    .unwrap()
}

fn right_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("dept", DataType::Utf8, false),
        Field::new("rating", DataType::Utf8, false),
    ]))
}

fn right_batch() -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 3, 5, 7, 9, 11, 13])),
            Arc::new(StringArray::from(vec![
                "eng", "sales", "eng", "hr", "sales", "eng", "hr",
            ])),
            Arc::new(StringArray::from(vec![
                "A", "B", "C", "A", "B", "A", "C",
            ])),
        ],
    )
    .unwrap()
}

/// Write a RecordBatch to an in-memory object store as a parquet file.
async fn write_parquet(store: &Arc<dyn ObjectStore>, path: &str, batch: &RecordBatch) {
    let mut buf = vec![];
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    store
        .put(&Path::from(path), PutPayload::from(buf))
        .await
        .unwrap();
}

/// Register a parquet-backed listing table from an in-memory object store.
fn register_listing_table(
    ctx: &SessionContext,
    table_name: &str,
    schema: Arc<Schema>,
    path: &str,
) {
    let format = Arc::new(
        ParquetFormat::default()
            .with_options(ctx.state().table_options().parquet.clone()),
    );
    let options = ListingOptions::new(format);
    let table_path = ListingTableUrl::parse(format!("memory:///{path}")).unwrap();
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(schema);
    let table = Arc::new(ListingTable::try_new(config).unwrap());
    ctx.register_table(table_name, table).unwrap();
}

/// Build a SessionContext backed by in-memory parquet, with SMJ forced.
///
/// Uses 2 target partitions so that the optimizer inserts hash-repartitioning
/// and sort nodes — exercising the filter-passthrough through these operators —
/// while still producing a SortMergeJoinExec (not CollectLeft HashJoin, which
/// is preferred at target_partitions=1).
async fn build_ctx(enable_dynamic_filters: bool) -> SessionContext {
    let cfg = SessionConfig::new()
        .with_target_partitions(2)
        .set_bool("datafusion.optimizer.prefer_hash_join", false)
        .set_bool(
            "datafusion.optimizer.enable_dynamic_filter_pushdown",
            enable_dynamic_filters,
        );
    let ctx = SessionContext::new_with_config(cfg);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let url = ObjectStoreUrl::parse("memory://").unwrap();
    ctx.register_object_store(url.as_ref(), Arc::clone(&store));

    write_parquet(&store, "left.parquet", &left_batch()).await;
    write_parquet(&store, "right.parquet", &right_batch()).await;

    register_listing_table(&ctx, "left_t", left_schema(), "left.parquet");
    register_listing_table(&ctx, "right_t", right_schema(), "right.parquet");

    ctx
}

/// Run a query with and without dynamic filter pushdown, assert same results,
/// and verify the plan uses SortMergeJoinExec.
async fn run_and_compare(query: &str) {
    // Run without dynamic filters (baseline)
    let ctx_off = build_ctx(false).await;
    let expected = ctx_off
        .sql(query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Run with dynamic filters
    let ctx_on = build_ctx(true).await;
    let actual = ctx_on.sql(query).await.unwrap().collect().await.unwrap();

    // Verify results match
    let expected_str = pretty_format_batches(&expected).unwrap().to_string();
    let actual_str = pretty_format_batches(&actual).unwrap().to_string();
    assert_eq!(
        expected_str, actual_str,
        "Results differ for query: {query}\n\nExpected:\n{expected_str}\n\nActual:\n{actual_str}"
    );

    // Verify plan uses SortMergeJoinExec
    let explain = ctx_on
        .sql(&format!("EXPLAIN {query}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan_str = pretty_format_batches(&explain).unwrap().to_string();
    assert!(
        plan_str.contains("SortMergeJoinExec"),
        "Expected SortMergeJoinExec in plan for query: {query}\n\nPlan:\n{plan_str}"
    );
}

// ---- Test cases ----

#[tokio::test]
async fn test_smj_topk_on_left_column() {
    run_and_compare(
        "SELECT l.id, l.name, r.dept \
         FROM left_t l INNER JOIN right_t r ON l.id = r.id \
         ORDER BY l.name LIMIT 3",
    )
    .await;
}

#[tokio::test]
async fn test_smj_topk_on_right_column() {
    run_and_compare(
        "SELECT l.id, l.name, r.dept, r.rating \
         FROM left_t l INNER JOIN right_t r ON l.id = r.id \
         ORDER BY r.rating LIMIT 2",
    )
    .await;
}

#[tokio::test]
async fn test_smj_topk_on_join_key() {
    run_and_compare(
        "SELECT l.id, l.name, r.dept \
         FROM left_t l INNER JOIN right_t r ON l.id = r.id \
         ORDER BY l.id LIMIT 3",
    )
    .await;
}

#[tokio::test]
async fn test_smj_topk_desc_order() {
    run_and_compare(
        "SELECT l.id, l.name, r.dept \
         FROM left_t l INNER JOIN right_t r ON l.id = r.id \
         ORDER BY l.score DESC LIMIT 2",
    )
    .await;
}

#[tokio::test]
async fn test_smj_topk_multi_column_order() {
    run_and_compare(
        "SELECT l.id, l.name, r.dept, r.rating \
         FROM left_t l INNER JOIN right_t r ON l.id = r.id \
         ORDER BY r.dept, l.name LIMIT 3",
    )
    .await;
}

#[tokio::test]
async fn test_smj_topk_with_where_clause() {
    run_and_compare(
        "SELECT l.id, l.name, r.dept \
         FROM left_t l INNER JOIN right_t r ON l.id = r.id \
         WHERE l.score > 70.0 \
         ORDER BY l.name LIMIT 2",
    )
    .await;
}

#[tokio::test]
async fn test_smj_topk_limit_one() {
    run_and_compare(
        "SELECT l.id, l.name, r.dept \
         FROM left_t l INNER JOIN right_t r ON l.id = r.id \
         ORDER BY l.score LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn test_smj_topk_limit_exceeds_rows() {
    run_and_compare(
        "SELECT l.id, l.name, r.dept \
         FROM left_t l INNER JOIN right_t r ON l.id = r.id \
         ORDER BY l.id LIMIT 100",
    )
    .await;
}

#[tokio::test]
async fn test_smj_left_join_correctness() {
    // Left join should produce correct results with or without dynamic filters.
    // No DynamicFilter is pushed through SMJ for non-Inner joins (conservative).
    run_and_compare(
        "SELECT l.id, l.name, r.dept \
         FROM left_t l LEFT JOIN right_t r ON l.id = r.id \
         ORDER BY l.id LIMIT 3",
    )
    .await;
}

#[tokio::test]
async fn test_smj_nested_joins_topk() {
    run_and_compare(
        "SELECT l.id, l.name, r1.dept, r2.rating \
         FROM left_t l \
         INNER JOIN right_t r1 ON l.id = r1.id \
         INNER JOIN right_t r2 ON l.id = r2.id \
         ORDER BY l.name LIMIT 3",
    )
    .await;
}

/// Verify that with dynamic filters enabled, the physical plan for an Inner
/// join + TopK query contains a DynamicFilter pushed down to a DataSourceExec.
/// This confirms the feature is effective end-to-end with parquet.
#[tokio::test]
async fn test_smj_dynamic_filter_present_in_plan() {
    let query = "SELECT l.id, l.name, r.dept \
                 FROM left_t l INNER JOIN right_t r ON l.id = r.id \
                 ORDER BY l.name LIMIT 3";

    let ctx = build_ctx(true).await;
    let explain = ctx
        .sql(&format!("EXPLAIN {query}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan_str = pretty_format_batches(&explain).unwrap().to_string();

    assert!(
        plan_str.contains("SortMergeJoinExec"),
        "Expected SortMergeJoinExec in plan\n\nPlan:\n{plan_str}"
    );

    // With parquet + SMJ + TopK, a DynamicFilter should be pushed through
    // the SortMergeJoinExec to one of the DataSourceExec nodes.
    let has_dynamic_filter = plan_str
        .lines()
        .any(|l| l.contains("DataSourceExec") && l.contains("DynamicFilter"));
    assert!(
        has_dynamic_filter,
        "Expected DynamicFilter pushed to DataSourceExec through SortMergeJoinExec\n\nPlan:\n{plan_str}"
    );
}
