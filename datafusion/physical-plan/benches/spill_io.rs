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

use arrow::array::{
    Date32Builder, Decimal128Builder, Int32Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use datafusion_common::config::SpillCompression;
use datafusion_common::human_readable_size;
use datafusion_common::instant::Instant;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_physical_plan::SpillManager;
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use tokio::runtime::Runtime;

pub fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
    ]));

    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Date32Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(11, 2)
        .unwrap();

    for i in 0..num_rows {
        a.append_value(i as i32);
        c.append_value(i as i32);
        d.append_value((i * 1000000) as i128);
        if allow_nulls && i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("this is string number {i}"));
        }
    }

    let a = a.finish();
    let b = b.finish();
    let c = c.finish();
    let d = d.finish();

    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
    )
    .unwrap()
}

// BENCHMARK: REVALIDATION OVERHEAD COMPARISON
// ---------------------------------------------------------
// To compare performance with/without Arrow IPC validation:
//
// 1. Locate the function `read_spill`
// 2. Modify the `skip_validation` flag:
//    - Set to `false` to enable validation
// 3. Rerun `cargo bench --bench spill_io`
fn bench_spill_io(c: &mut Criterion) {
    let env = Arc::new(RuntimeEnv::default());
    let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
    ]));
    let spill_manager = SpillManager::new(env, metrics, schema);

    let mut group = c.benchmark_group("spill_io");
    let rt = Runtime::new().unwrap();

    group.bench_with_input(
        BenchmarkId::new("StreamReader/read_100", ""),
        &spill_manager,
        |b, spill_manager| {
            b.iter_batched(
                // Setup phase: Create fresh state for each benchmark iteration.
                // - generate an ipc file.
                // This ensures each iteration starts with clean resources.
                || {
                    let batch = create_batch(8192, true);
                    spill_manager
                        .spill_record_batch_and_finish(&vec![batch; 100], "Test")
                        .unwrap()
                        .unwrap()
                },
                // Benchmark phase:
                // - Execute the read operation via SpillManager
                // - Wait for the consumer to finish processing
                |spill_file| {
                    rt.block_on(async {
                        let stream = spill_manager
                            .read_spill_as_stream(spill_file, None)
                            .unwrap();
                        let _ = collect(stream).await.unwrap();
                    })
                },
                BatchSize::LargeInput,
            )
        },
    );
    group.finish();
}

// Generate `num_batches` RecordBatches mimicking TPC-H Q2's partial aggregate result:
// GROUP BY ps_partkey -> MIN(ps_supplycost)
fn create_q2_like_batches(
    num_batches: usize,
    num_rows: usize,
) -> (Arc<Schema>, Vec<RecordBatch>) {
    // use fixed seed
    let seed = 2;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut batches = Vec::with_capacity(num_batches);

    let mut current_key = 400000_i64;

    let schema = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("min_ps_supplycost", DataType::Decimal128(15, 2), true),
    ]));

    for _ in 0..num_batches {
        let mut partkey_builder = Int64Builder::new();
        let mut cost_builder = Decimal128Builder::new()
            .with_precision_and_scale(15, 2)
            .unwrap();

        for _ in 0..num_rows {
            // Occasionally skip a few partkey values to simulate sparsity
            let jump = if rng.random_bool(0.05) {
                rng.random_range(2..10)
            } else {
                1
            };
            current_key += jump;

            let supply_cost = rng.random_range(10_00..100_000) as i128;

            partkey_builder.append_value(current_key);
            cost_builder.append_value(supply_cost);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(partkey_builder.finish()),
                Arc::new(cost_builder.finish()),
            ],
        )
        .unwrap();

        batches.push(batch);
    }

    (schema, batches)
}

/// Generate `num_batches` RecordBatches mimicking TPC-H Q16's partial aggregate result:
/// GROUP BY (p_brand, p_type, p_size) -> COUNT(DISTINCT ps_suppkey)
pub fn create_q16_like_batches(
    num_batches: usize,
    num_rows: usize,
) -> (Arc<Schema>, Vec<RecordBatch>) {
    let seed = 16;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut batches = Vec::with_capacity(num_batches);

    let schema = Arc::new(Schema::new(vec![
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("alias1", DataType::Int64, false), // COUNT(DISTINCT ps_suppkey)
    ]));

    // Representative string pools
    let brands = ["Brand#32", "Brand#33", "Brand#41", "Brand#42", "Brand#55"];
    let types = [
        "PROMO ANODIZED NICKEL",
        "STANDARD BRUSHED NICKEL",
        "PROMO POLISHED COPPER",
        "ECONOMY ANODIZED BRASS",
        "LARGE BURNISHED COPPER",
        "STANDARD POLISHED TIN",
        "SMALL PLATED STEEL",
        "MEDIUM POLISHED COPPER",
    ];
    let sizes = [3, 9, 14, 19, 23, 36, 45, 49];

    for _ in 0..num_batches {
        let mut brand_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        let mut size_builder = Int32Builder::new();
        let mut count_builder = Int64Builder::new();

        for _ in 0..num_rows {
            let brand = brands[rng.random_range(0..brands.len())];
            let ptype = types[rng.random_range(0..types.len())];
            let size = sizes[rng.random_range(0..sizes.len())];
            let count = rng.random_range(1000..100_000);

            brand_builder.append_value(brand);
            type_builder.append_value(ptype);
            size_builder.append_value(size);
            count_builder.append_value(count);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(brand_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(size_builder.finish()),
                Arc::new(count_builder.finish()),
            ],
        )
        .unwrap();

        batches.push(batch);
    }

    (schema, batches)
}

// Generate `num_batches` RecordBatches mimicking TPC-H Q20's partial aggregate result:
// GROUP BY (l_partkey, l_suppkey) -> SUM(l_quantity)
fn create_q20_like_batches(
    num_batches: usize,
    num_rows: usize,
) -> (Arc<Schema>, Vec<RecordBatch>) {
    let seed = 20;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut batches = Vec::with_capacity(num_batches);

    let mut current_partkey = 400000_i64;

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("sum_l_quantity", DataType::Decimal128(25, 2), true),
    ]));

    for _ in 0..num_batches {
        let mut partkey_builder = Int64Builder::new();
        let mut suppkey_builder = Int64Builder::new();
        let mut quantity_builder = Decimal128Builder::new()
            .with_precision_and_scale(25, 2)
            .unwrap();

        for _ in 0..num_rows {
            // Occasionally skip a few partkey values to simulate sparsity
            let partkey_jump = if rng.random_bool(0.03) {
                rng.random_range(2..6)
            } else {
                1
            };
            current_partkey += partkey_jump;

            let suppkey = rng.random_range(10_000..99_999);
            let quantity = rng.random_range(500..20_000) as i128;

            partkey_builder.append_value(current_partkey);
            suppkey_builder.append_value(suppkey);
            quantity_builder.append_value(quantity);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(partkey_builder.finish()),
                Arc::new(suppkey_builder.finish()),
                Arc::new(quantity_builder.finish()),
            ],
        )
        .unwrap();

        batches.push(batch);
    }

    (schema, batches)
}

/// Generate `num_batches` wide RecordBatches resembling sort-tpch Q10 for benchmarking.
/// This includes multiple numeric, date, and Utf8View columns (15 total).
pub fn create_wide_batches(
    num_batches: usize,
    num_rows: usize,
) -> (Arc<Schema>, Vec<RecordBatch>) {
    let seed = 10;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut batches = Vec::with_capacity(num_batches);

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Decimal128(15, 2), false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
        Field::new("l_discount", DataType::Decimal128(15, 2), false),
        Field::new("l_tax", DataType::Decimal128(15, 2), false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
    ]));

    for _ in 0..num_batches {
        let mut linenum = Int32Builder::new();
        let mut suppkey = Int64Builder::new();
        let mut orderkey = Int64Builder::new();
        let mut partkey = Int64Builder::new();
        let mut quantity = Decimal128Builder::new()
            .with_precision_and_scale(15, 2)
            .unwrap();
        let mut extprice = Decimal128Builder::new()
            .with_precision_and_scale(15, 2)
            .unwrap();
        let mut discount = Decimal128Builder::new()
            .with_precision_and_scale(15, 2)
            .unwrap();
        let mut tax = Decimal128Builder::new()
            .with_precision_and_scale(15, 2)
            .unwrap();
        let mut retflag = StringBuilder::new();
        let mut linestatus = StringBuilder::new();
        let mut shipdate = Date32Builder::new();
        let mut commitdate = Date32Builder::new();
        let mut receiptdate = Date32Builder::new();
        let mut shipinstruct = StringBuilder::new();
        let mut shipmode = StringBuilder::new();

        let return_flags = ["A", "N", "R"];
        let statuses = ["F", "O"];
        let instructs = ["DELIVER IN PERSON", "COLLECT COD", "NONE"];
        let modes = ["TRUCK", "MAIL", "SHIP", "RAIL", "AIR"];

        for i in 0..num_rows {
            linenum.append_value((i % 7) as i32);
            suppkey.append_value(rng.random_range(0..100_000));
            orderkey.append_value(1_000_000 + i as i64);
            partkey.append_value(rng.random_range(0..200_000));

            quantity.append_value(rng.random_range(100..10000) as i128);
            extprice.append_value(rng.random_range(1_000..1_000_000) as i128);
            discount.append_value(rng.random_range(0..10000) as i128);
            tax.append_value(rng.random_range(0..5000) as i128);

            retflag.append_value(return_flags[rng.random_range(0..return_flags.len())]);
            linestatus.append_value(statuses[rng.random_range(0..statuses.len())]);

            let base_date = 10_000;
            shipdate.append_value(base_date + (i % 1000) as i32);
            commitdate.append_value(base_date + (i % 1000) as i32 + 1);
            receiptdate.append_value(base_date + (i % 1000) as i32 + 2);

            shipinstruct.append_value(instructs[rng.random_range(0..instructs.len())]);
            shipmode.append_value(modes[rng.random_range(0..modes.len())]);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(linenum.finish()),
                Arc::new(suppkey.finish()),
                Arc::new(orderkey.finish()),
                Arc::new(partkey.finish()),
                Arc::new(quantity.finish()),
                Arc::new(extprice.finish()),
                Arc::new(discount.finish()),
                Arc::new(tax.finish()),
                Arc::new(retflag.finish()),
                Arc::new(linestatus.finish()),
                Arc::new(shipdate.finish()),
                Arc::new(commitdate.finish()),
                Arc::new(receiptdate.finish()),
                Arc::new(shipinstruct.finish()),
                Arc::new(shipmode.finish()),
            ],
        )
        .unwrap();
        batches.push(batch);
    }
    (schema, batches)
}

// Benchmarks spill write + read performance across multiple compression codecs
// using realistic input data inspired by TPC-H aggregate spill scenarios.
//
// This function prepares synthetic RecordBatches that mimic the schema and distribution
// of intermediate aggregate results from representative TPC-H queries (Q2, Q16, Q20) and sort-tpch Q10.
// The schemas of these batches are:
//      Q2 [Int64, Decimal128]
//      Q16 [Utf8, Utf8, Int32, Int64]
//      Q20 [Int64, Int64, Decimal128]
//      sort-tpch Q10 (wide batch) [Int32, Int64 * 3, Decimal128 * 4, Date * 3, Utf8 * 4]
// For each dataset:
// - It evaluates spill performance under different compression codecs (e.g., Uncompressed, Zstd, LZ4).
// - It measures end-to-end spill write + read performance using Criterion.
// - It prints the observed memory-to-disk compression ratio for each codec.
//
// This helps evaluate the tradeoffs between compression ratio and runtime overhead for various codecs.
fn bench_spill_compression(c: &mut Criterion) {
    let env = Arc::new(RuntimeEnv::default());
    let mut group = c.benchmark_group("spill_compression");
    let rt = Runtime::new().unwrap();
    let compressions = vec![
        SpillCompression::Uncompressed,
        SpillCompression::Zstd,
        SpillCompression::Lz4Frame,
    ];

    // Modify these values to change data volume. Note that each batch contains `num_rows` rows.
    let num_batches = 50;
    let num_rows = 8192;

    // Q2 [Int64, Decimal128]
    let (schema, batches) = create_q2_like_batches(num_batches, num_rows);
    benchmark_spill_batches_for_all_codec(
        &mut group,
        "q2",
        batches,
        &compressions,
        &rt,
        env.clone(),
        schema,
    );
    // Q16 [Utf8, Utf8, Int32, Int64]
    let (schema, batches) = create_q16_like_batches(num_batches, num_rows);
    benchmark_spill_batches_for_all_codec(
        &mut group,
        "q16",
        batches,
        &compressions,
        &rt,
        env.clone(),
        schema,
    );
    // Q20 [Int64, Int64, Decimal128]
    let (schema, batches) = create_q20_like_batches(num_batches, num_rows);
    benchmark_spill_batches_for_all_codec(
        &mut group,
        "q20",
        batches,
        &compressions,
        &rt,
        env.clone(),
        schema,
    );
    // sort-tpch Q10 (wide batch) [Int32, Int64 * 3, Decimal128 * 4, Date * 3, Utf8 * 4]
    let (schema, batches) = create_wide_batches(num_batches, num_rows);
    benchmark_spill_batches_for_all_codec(
        &mut group,
        "wide",
        batches,
        &compressions,
        &rt,
        env,
        schema,
    );
    group.finish();
}

#[expect(clippy::needless_pass_by_value)]
fn benchmark_spill_batches_for_all_codec(
    group: &mut BenchmarkGroup<'_, WallTime>,
    batch_label: &str,
    batches: Vec<RecordBatch>,
    compressions: &[SpillCompression],
    rt: &Runtime,
    env: Arc<RuntimeEnv>,
    schema: Arc<Schema>,
) {
    let mem_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

    for &compression in compressions {
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager =
            SpillManager::new(Arc::clone(&env), metrics.clone(), Arc::clone(&schema))
                .with_compression_type(compression);

        let bench_id = BenchmarkId::new(batch_label, compression.to_string());
        group.bench_with_input(bench_id, &spill_manager, |b, spill_manager| {
            b.iter_batched(
                || batches.clone(),
                |batches| {
                    rt.block_on(async {
                        let spill_file = spill_manager
                            .spill_record_batch_and_finish(
                                &batches,
                                &format!("{batch_label}_{compression}"),
                            )
                            .unwrap()
                            .unwrap();
                        let stream = spill_manager
                            .read_spill_as_stream(spill_file, None)
                            .unwrap();
                        let _ = collect(stream).await.unwrap();
                    })
                },
                BatchSize::LargeInput,
            )
        });

        // Run Spilling Read & Write once more to read file size & calculate bandwidth
        let start = Instant::now();

        let spill_file = spill_manager
            .spill_record_batch_and_finish(
                &batches,
                &format!("{batch_label}_{compression}"),
            )
            .unwrap()
            .unwrap();

        // calculate write_throughput (includes both compression and I/O time) based on in memory batch size
        let write_time = start.elapsed();
        let write_throughput = (mem_bytes as u128 / write_time.as_millis().max(1)) * 1000;

        // calculate compression ratio
        let disk_bytes = std::fs::metadata(spill_file.path())
            .expect("metadata read fail")
            .len() as usize;
        let ratio = mem_bytes as f64 / disk_bytes.max(1) as f64;

        // calculate read_throughput (includes both compression and I/O time) based on in memory batch size
        let rt = Runtime::new().unwrap();
        let start = Instant::now();
        rt.block_on(async {
            let stream = spill_manager
                .read_spill_as_stream(spill_file, None)
                .unwrap();
            let _ = collect(stream).await.unwrap();
        });
        let read_time = start.elapsed();
        let read_throughput = (mem_bytes as u128 / read_time.as_millis().max(1)) * 1000;

        println!(
            "[{} | {:?}] mem: {}| disk: {}| compression ratio: {:.3}x| throughput: (w) {}/s (r) {}/s",
            batch_label,
            compression,
            human_readable_size(mem_bytes),
            human_readable_size(disk_bytes),
            ratio,
            human_readable_size(write_throughput as usize),
            human_readable_size(read_throughput as usize),
        );
    }
}

criterion_group!(benches, bench_spill_io, bench_spill_compression);
criterion_main!(benches);
