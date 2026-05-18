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

//! Benchmarks for multi-column GROUP BY performance.
//!
//! Tests the performance of grouping across different cardinality
//! scenarios and column counts. Uses Parquet files so that column
//! statistics (min/max) are available to the optimizer for heuristic
//! decisions about GroupValues implementation selection.
//!
//! The benchmark pre-plans the query and only measures execution time
//! (excludes planning and I/O setup overhead).

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use parking_lot::Mutex;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

const NUM_ROWS: usize = 1_000_000;
const BATCH_SIZE: usize = 8192;

fn build_group_by_sql(num_cols: usize) -> String {
    let cols: Vec<String> = (0..num_cols).map(|i| format!("col_{i}")).collect();
    let col_list = cols.join(", ");
    format!("SELECT {col_list} FROM t GROUP BY {col_list}")
}

fn generate_parquet_file(num_cols: usize, cardinality: usize) -> NamedTempFile {
    let mut rng = StdRng::seed_from_u64(42);
    let fields: Vec<Field> = (0..num_cols)
        .map(|i| Field::new(format!("col_{i}"), DataType::Int32, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let mut temp_file = tempfile::Builder::new()
        .prefix("multi_group_by")
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(NUM_ROWS))
        .build();

    let mut writer =
        ArrowWriter::try_new(&mut temp_file, Arc::clone(&schema), Some(props)).unwrap();

    let num_batches = NUM_ROWS / BATCH_SIZE;
    for _ in 0..num_batches {
        let columns: Vec<ArrayRef> = (0..num_cols)
            .map(|_| {
                let values: Vec<i32> = (0..BATCH_SIZE)
                    .map(|_| rng.random_range(0..cardinality as i32))
                    .collect();
                Arc::new(Int32Array::from(values)) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    temp_file
}

struct BenchContext {
    ctx: Arc<Mutex<SessionContext>>,
    _temp_file: NamedTempFile,
}

#[expect(clippy::needless_pass_by_value)]
fn query(ctx: Arc<Mutex<SessionContext>>, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn prepare_context(rt: &Runtime, num_cols: usize, cardinality: usize) -> BenchContext {
    let temp_file = generate_parquet_file(num_cols, cardinality);
    let path = temp_file.path().to_str().unwrap().to_string();

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    rt.block_on(async {
        ctx.register_parquet("t", &path, Default::default())
            .await
            .unwrap();
        // Warm the OS page cache
        let df = ctx.sql(&build_group_by_sql(num_cols)).await.unwrap();
        let _ = df.collect().await.unwrap();
    });

    BenchContext {
        ctx: Arc::new(Mutex::new(ctx)),
        _temp_file: temp_file,
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // === Experiment 1: Fixed ~100-1000 groups, vary column count ===
    let b_ctx = prepare_context(&rt, 2, 10); // 10^2 = 100 groups
    c.bench_function("fixed_groups_cols_2_grp_100", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(2)))
    });

    let b_ctx = prepare_context(&rt, 3, 5); // 5^3 = 125 groups
    c.bench_function("fixed_groups_cols_3_grp_125", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(3)))
    });

    let b_ctx = prepare_context(&rt, 4, 3); // 3^4 = 81 groups
    c.bench_function("fixed_groups_cols_4_grp_81", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(4)))
    });

    let b_ctx = prepare_context(&rt, 6, 3); // 3^6 = 729 groups
    c.bench_function("fixed_groups_cols_6_grp_729", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(6)))
    });

    let b_ctx = prepare_context(&rt, 8, 2); // 2^8 = 256 groups
    c.bench_function("fixed_groups_cols_8_grp_256", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(8)))
    });

    let b_ctx = prepare_context(&rt, 10, 2); // 2^10 = 1024 groups
    c.bench_function("fixed_groups_cols_10_grp_1024", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(10)))
    });

    // === Experiment 1b: High groups (~1M), vary column count ===
    let b_ctx = prepare_context(&rt, 2, 1000); // 1000^2 = 1M groups
    c.bench_function("high_groups_cols_2_grp_1M", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(2)))
    });

    let b_ctx = prepare_context(&rt, 3, 100); // 100^3 = 1M groups
    c.bench_function("high_groups_cols_3_grp_1M", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(3)))
    });

    let b_ctx = prepare_context(&rt, 4, 32); // 32^4 = ~1M groups
    c.bench_function("high_groups_cols_4_grp_1M", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(4)))
    });

    let b_ctx = prepare_context(&rt, 6, 10); // 10^6 = 1M groups
    c.bench_function("high_groups_cols_6_grp_1M", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(6)))
    });

    let b_ctx = prepare_context(&rt, 8, 6); // 6^8 = ~1.7M groups
    c.bench_function("high_groups_cols_8_grp_1M", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(8)))
    });

    let b_ctx = prepare_context(&rt, 10, 4); // 4^10 = ~1M groups
    c.bench_function("high_groups_cols_10_grp_1M", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(10)))
    });

    // === Experiment 2: Fixed 4 columns, vary group count ===
    let b_ctx = prepare_context(&rt, 4, 2); // 2^4 = 16 groups
    c.bench_function("fixed_4cols_grp_16", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(4)))
    });

    let b_ctx = prepare_context(&rt, 4, 5); // 5^4 = 625 groups
    c.bench_function("fixed_4cols_grp_625", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(4)))
    });

    let b_ctx = prepare_context(&rt, 4, 10); // 10^4 = 10K groups
    c.bench_function("fixed_4cols_grp_10000", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(4)))
    });

    let b_ctx = prepare_context(&rt, 4, 30); // 30^4 = 810K groups
    c.bench_function("fixed_4cols_grp_810000", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(4)))
    });

    let b_ctx = prepare_context(&rt, 4, 100); // 100^4 = 100M groups
    c.bench_function("fixed_4cols_grp_100M", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(4)))
    });

    let b_ctx = prepare_context(&rt, 4, 500); // 500^4 = 62.5B groups
    c.bench_function("fixed_4cols_grp_62B", |b| {
        b.iter(|| query(b_ctx.ctx.clone(), &rt, &build_group_by_sql(4)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
