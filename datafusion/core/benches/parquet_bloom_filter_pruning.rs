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

//! Benchmarks for Parquet row-group pruning with Bloom filters on several
//! predicate columns.
//!
//! The file has [`ROW_GROUPS`] row groups and [`MAX_COLUMNS`] `Int32` columns,
//! all with Bloom filters and without min/max statistics, so only Bloom
//! filters can prune. Row 0 of every row group holds `0` in every column, and
//! `-1` appears nowhere. Each query is a conjunction of equality predicates on
//! the first `K` columns:
//!
//! - `reject_first`: `c0 = -1 AND c1 = 0 AND ...`; the first column's filter
//!   can reject every row group.
//! - `reject_last`: `c0 = 0 AND ... AND c{K-1} = -1`; only the last column's
//!   filter can reject, so every filter is loaded before pruning.
//! - `survive_all`: `c0 = 0 AND ... AND c{K-1} = 0`; no filter can reject, so
//!   every filter is loaded and every row group is scanned.
//!
//! Each query runs against an in-memory store and against a store that adds
//! [`STORE_LATENCY`] to every request, which approximates remote storage.
//!
//! Run with `cargo bench -p datafusion --features parquet --bench parquet_bloom_filter_pruning`.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::runtime_env::RuntimeEnv;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tokio::runtime::Runtime;
use url::Url;

const ROW_GROUPS: usize = 64;
const ROW_GROUP_ROWS: usize = 1024;
const MAX_COLUMNS: usize = 32;
const COLUMN_COUNTS: [usize; 3] = [2, 8, 32];
const STORE_LATENCY: Duration = Duration::from_micros(200);
const PRESENT: i32 = 0;
const ABSENT: i32 = -1;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(
        (0..MAX_COLUMNS)
            .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
            .collect::<Vec<_>>(),
    ))
}

fn create_parquet_file() -> Vec<u8> {
    let schema = schema();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_max_ndv(ROW_GROUP_ROWS as u64)
        .set_bloom_filter_fpp(0.01)
        .build();

    let mut data = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut data, Arc::clone(&schema), Some(props)).unwrap();
    for rg in 0..ROW_GROUPS {
        // Row 0 is PRESENT in every row group; other values are unique and positive.
        let values: Int32Array = (0..ROW_GROUP_ROWS)
            .map(|row| {
                if row == 0 {
                    PRESENT
                } else {
                    (rg * ROW_GROUP_ROWS + row) as i32
                }
            })
            .collect();
        let column: ArrayRef = Arc::new(values);
        let columns = vec![column; MAX_COLUMNS];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    data
}

fn make_store_slow(object_store: InMemory) -> Arc<dyn ObjectStore> {
    let config = ThrottleConfig {
        wait_get_per_call: STORE_LATENCY,
        ..Default::default()
    };
    Arc::new(ThrottledStore::new(object_store, config))
}

async fn setup_context() -> SessionContext {
    let data = create_parquet_file();
    let stores: [(&str, Arc<dyn ObjectStore>); 2] = [
        ("memory", Arc::new(InMemory::new())),
        ("slow", make_store_slow(InMemory::new())),
    ];

    // One partition keeps the Bloom filter reads for the file in one opener.
    let config = SessionConfig::new().with_target_partitions(1);
    let rt = Arc::new(RuntimeEnv::default());
    for (name, store) in &stores {
        store
            .put(&Path::from("t/data.parquet"), data.clone().into())
            .await
            .unwrap();
        rt.register_object_store(
            &Url::parse(&format!("{name}://bucket")).unwrap(),
            Arc::clone(store),
        );
    }
    let ctx = SessionContext::new_with_config_rt(config, rt);

    for (name, _) in &stores {
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        ctx.register_listing_table(
            *name,
            format!("{name}://bucket/t/"),
            options,
            Some(schema()),
            None,
        )
        .await
        .unwrap();
    }
    ctx
}

/// Builds the query and its expected row count.
fn query(table: &str, case: &str, columns: usize) -> (String, usize) {
    let predicate = (0..columns)
        .map(|i| {
            let rejecting = match case {
                "reject_first" => i == 0,
                "reject_last" => i == columns - 1,
                "survive_all" => false,
                _ => unreachable!(),
            };
            let value = if rejecting { ABSENT } else { PRESENT };
            format!("c{i} = {value}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let expected_rows = if case == "survive_all" { ROW_GROUPS } else { 0 };
    (
        format!("SELECT c0 FROM {table} WHERE {predicate}"),
        expected_rows,
    )
}

async fn run(ctx: &SessionContext, sql: &str, expected_rows: usize) {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, expected_rows);
}

/// Prints the Bloom filter pruning metric once so a run shows whether Bloom
/// filters actually pruned.
async fn print_bloom_metrics(ctx: &SessionContext, sql: &str) {
    let batches = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    let metric = plan
        .split([',', '['])
        .find(|s| s.contains("row_groups_pruned_bloom_filter"))
        .unwrap_or("row_groups_pruned_bloom_filter not reported");
    println!("{sql}\n  {}", metric.trim());
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    for case in ["reject_first", "reject_last", "survive_all"] {
        for table in ["memory", "slow"] {
            let mut group =
                c.benchmark_group(format!("parquet_bloom_filter_pruning/{table}/{case}"));
            group.sampling_mode(SamplingMode::Flat);
            group.sample_size(10);
            for columns in COLUMN_COUNTS {
                let (sql, expected_rows) = query(table, case, columns);
                rt.block_on(print_bloom_metrics(&ctx, &sql));
                group.bench_with_input(
                    BenchmarkId::new("columns", columns),
                    &sql,
                    |b, sql| {
                        b.to_async(&rt).iter(|| run(&ctx, sql, expected_rows));
                    },
                );
            }
            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
