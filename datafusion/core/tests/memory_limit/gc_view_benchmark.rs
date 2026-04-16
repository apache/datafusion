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

//! Benchmark for measuring the impact of gc_view_arrays on spill performance.
//! This test creates a GROUP BY workload with StringView columns and a tight
//! memory limit to force spilling, then measures spill file sizes, peak RSS,
//! and query latency.

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use datafusion_execution::memory_pool::FairSpillPool;
use std::sync::Arc;
use std::time::Instant;

/// Create deterministic test data with StringView columns.
/// Uses deterministic strings (no randomness) for reproducibility.
fn create_stringview_batches(
    num_batches: usize,
    rows_per_batch: usize,
    num_groups: usize,
) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group_key", DataType::Utf8View, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mut batches = Vec::with_capacity(num_batches);

    for batch_idx in 0..num_batches {
        // 40+ byte strings ensure they are NOT inlined in StringView
        let strings: Vec<String> = (0..rows_per_batch)
            .map(|row_idx| {
                let group = (batch_idx * rows_per_batch + row_idx) % num_groups;
                format!(
                    "group_{:010}_payload_data_for_testing_{:08}",
                    group, batch_idx
                )
            })
            .collect();

        let string_array =
            StringViewArray::from(strings.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        let values: Vec<i64> = (0..rows_per_batch)
            .map(|i| (batch_idx * rows_per_batch + i) as i64)
            .collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(string_array) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
            ],
        )
        .unwrap();
        batches.push(batch);
    }

    batches
}

/// Run the GROUP BY query with EXPLAIN ANALYZE and extract spill metrics from the output.
async fn run_stringview_aggregate_spill_benchmark(
    pool_size_mb: usize,
    num_batches: usize,
    rows_per_batch: usize,
    num_groups: usize,
) -> (f64, String) {
    let pool_size = pool_size_mb * 1024 * 1024;

    let batches = create_stringview_batches(num_batches, rows_per_batch, num_groups);

    let schema = batches[0].schema();
    let table = MemTable::try_new(schema, vec![batches]).unwrap();

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(pool_size)))
        .build_arc()
        .unwrap();

    let config = SessionConfig::new()
        .with_target_partitions(1) // Single partition for deterministic spill behavior
        .with_batch_size(8192);

    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_table("t", Arc::new(table)).unwrap();

    let start = Instant::now();

    // Use EXPLAIN ANALYZE to get spill metrics in the execution plan output
    let df = ctx
        .sql("EXPLAIN ANALYZE SELECT group_key, COUNT(*) as cnt, SUM(value) as total FROM t GROUP BY group_key")
        .await
        .unwrap();

    let results = df.collect().await.expect("Query should succeed with spilling");
    let query_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    // Extract the EXPLAIN ANALYZE text
    let explain_text = results
        .iter()
        .flat_map(|batch| {
            let plan_col = batch
                .column_by_name("plan")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| plan_col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n");

    (query_time_ms, explain_text)
}

/// Parse a human-readable size like "20.9 MB" or "512.0 K" to bytes.
fn parse_human_size(s: &str) -> Option<usize> {
    let s = s.trim();
    // Try to find a number (possibly with decimal) followed by optional unit
    let num_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let num_str = &s[..num_end].trim();
    let unit = s[num_end..].trim();

    let num: f64 = num_str.parse().ok()?;
    let multiplier = match unit {
        "B" | "" => 1.0,
        "K" => 1024.0,
        "M" | "MB" => 1024.0 * 1024.0,
        "G" | "GB" => 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((num * multiplier) as usize)
}

/// Extract spill_count and spilled_bytes from EXPLAIN ANALYZE output.
/// Metrics are formatted like: spill_count=5, spilled_bytes=20.9 MB
fn extract_spill_metrics(explain_text: &str) -> (usize, usize) {
    let mut spill_count = 0;
    let mut spill_bytes = 0;

    for line in explain_text.lines() {
        if let Some(pos) = line.find("spill_count=") {
            let val_str = &line[pos + "spill_count=".len()..];
            // Take until comma or bracket
            let end = val_str
                .find(|c: char| c == ',' || c == ']')
                .unwrap_or(val_str.len());
            if let Some(v) = parse_human_size(&val_str[..end]) {
                spill_count += v;
            }
        }
        if let Some(pos) = line.find("spilled_bytes=") {
            let val_str = &line[pos + "spilled_bytes=".len()..];
            let end = val_str
                .find(|c: char| c == ',' || c == ']')
                .unwrap_or(val_str.len());
            if let Some(v) = parse_human_size(&val_str[..end]) {
                spill_bytes += v;
            }
        }
    }

    (spill_count, spill_bytes)
}

/// Benchmark: high-cardinality GROUP BY with StringView columns and forced spilling.
///
/// This exercises the hash aggregation spill path where IncrementalSortIterator
/// produces chunks via take_record_batch. Without gc_view_arrays, each chunk
/// retains references to all StringView data buffers from the parent batch,
/// causing N× write amplification in the IPC spill writer.
///
/// Run with: cargo test -p datafusion --test core_integration gc_view_benchmark -- --nocapture
#[tokio::test]
async fn bench_stringview_aggregate_spill() {
    let num_batches = 50;
    let rows_per_batch = 2000;
    let num_groups = 50_000; // High cardinality — many groups force spilling
    let pool_size_mb = 20; // Must be large enough for baseline (no gc) to succeed
    let n_runs = 3;

    eprintln!("\n=== StringView Aggregate Spill Benchmark ===");
    eprintln!(
        "Config: {} batches × {} rows = {} total rows, {} groups, {} MB pool",
        num_batches,
        rows_per_batch,
        num_batches * rows_per_batch,
        num_groups,
        pool_size_mb
    );

    let mut times = Vec::new();
    let mut spill_counts = Vec::new();
    let mut spill_bytes_vec = Vec::new();

    for run in 0..n_runs {
        eprintln!("\nRun {}/{}:", run + 1, n_runs);
        let (time_ms, explain_text) = run_stringview_aggregate_spill_benchmark(
            pool_size_mb,
            num_batches,
            rows_per_batch,
            num_groups,
        )
        .await;

        let (spill_count, spill_bytes) = extract_spill_metrics(&explain_text);

        eprintln!("  Query time: {:.1} ms", time_ms);
        eprintln!("  Spill count: {}", spill_count);
        eprintln!(
            "  Spill bytes: {} ({:.2} MB)",
            spill_bytes,
            spill_bytes as f64 / 1024.0 / 1024.0
        );

        // Print aggregate-related lines from explain for verification
        for line in explain_text.lines() {
            if line.contains("Aggregate") || line.contains("spill") {
                eprintln!("  EXPLAIN: {}", line.trim());
            }
        }

        times.push(time_ms);
        spill_counts.push(spill_count);
        spill_bytes_vec.push(spill_bytes);
    }

    // Compute statistics
    let mean_time: f64 = times.iter().sum::<f64>() / n_runs as f64;
    let mean_spill: f64 =
        spill_bytes_vec.iter().map(|&x| x as f64).sum::<f64>() / n_runs as f64;
    let mean_spill_count: f64 =
        spill_counts.iter().map(|&x| x as f64).sum::<f64>() / n_runs as f64;

    let stddev_time = if n_runs > 1 {
        (times
            .iter()
            .map(|x| (x - mean_time).powi(2))
            .sum::<f64>()
            / (n_runs - 1) as f64)
            .sqrt()
    } else {
        0.0
    };
    let stddev_spill = if n_runs > 1 {
        (spill_bytes_vec
            .iter()
            .map(|&x| (x as f64 - mean_spill).powi(2))
            .sum::<f64>()
            / (n_runs - 1) as f64)
            .sqrt()
    } else {
        0.0
    };

    eprintln!("\n=== RESULTS ({} runs) ===", n_runs);
    eprintln!(
        "Query time:   {:.1} ± {:.1} ms  (range: {:.1} - {:.1})",
        mean_time,
        stddev_time,
        times.iter().cloned().reduce(f64::min).unwrap(),
        times.iter().cloned().reduce(f64::max).unwrap()
    );
    eprintln!("Spill count:  {:.1}", mean_spill_count);
    eprintln!(
        "Spill bytes:  {:.0} ± {:.0}  ({:.2} ± {:.3} MB)",
        mean_spill,
        stddev_spill,
        mean_spill / 1024.0 / 1024.0,
        stddev_spill / 1024.0 / 1024.0,
    );
    eprintln!("Individual spill bytes: {:?}", spill_bytes_vec);
}
