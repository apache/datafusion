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

//! Scaling sweep for the `ParallelWindow` optimizer rule.
//!
//! For each value of `target_partitions`, runs a bounded RANGE-frame
//! window query (no `PARTITION BY` — the shape #23197 calls out as
//! single-core-bottlenecked) twice: once with the `ParallelWindow`
//! rule filtered out of the physical optimizer chain (the baseline),
//! and once with the default chain (the rule fires). Emits a CSV row
//! per iteration to stdout. Pipe to
//! `scripts/plot_parallel_window_scaling.py` for a throughput-vs-cores
//! chart.
//!
//! Run:
//!     cargo bench --bench parallel_window_scaling \
//!         > parallel_window_scaling.csv

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::prelude::{SessionConfig, SessionContext};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, Uniform};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

/// Weak-scaling design: rows scale linearly with cores so total work is
/// proportional to the parallelism budget. The PoC line in the chart
/// should stay roughly flat (constant rows per core ⇒ constant
/// wall-clock if scaling is linear), while the baseline line grows
/// linearly with cores (no parallelization ⇒ wall-clock tracks total
/// rows).
const ROWS_PER_CORE: usize = 500_000;
const BATCH_SIZE: usize = 8 * 1024;
const HALO_RANGE: i64 = 100;
const ITERATIONS: usize = 3;
/// The "cores" axis: each value sets the table partition count, the
/// `target_partitions` budget, and (via `ROWS_PER_CORE`) total rows.
const CORE_SETTINGS: &[usize] = &[1, 2, 4, 8, 16, 32];

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Float64, false),
    ]))
}

/// Build `num_partitions` partitions of `(ts, v)` rows, with `ts`
/// monotonically increasing within each partition AND between partitions
/// (partition `i` covers `ts ∈ [i * per, (i + 1) * per)`). Declaring this
/// ordering on the `MemTable` lets DataFusion elide the `SortExec` from
/// both the baseline and the PoC plans, so the bench measures BWAG +
/// repartitioning cost rather than sort cost.
fn make_partitions(num_partitions: usize) -> Vec<Vec<RecordBatch>> {
    let mut rng = StdRng::seed_from_u64(0xC0FFEE_C0FFEE);
    let v_dist = Uniform::new(0.0f64, 1.0).unwrap();
    let schema = schema();
    (0..num_partitions)
        .map(|part| {
            let mut batches = Vec::new();
            let part_start = (part * ROWS_PER_CORE) as i64;
            let mut next_ts = part_start;
            let mut remaining = ROWS_PER_CORE;
            while remaining > 0 {
                let len = remaining.min(BATCH_SIZE);
                let ts: Vec<i64> = (0..len as i64).map(|i| next_ts + i).collect();
                next_ts += len as i64;
                let v: Vec<f64> = (0..len).map(|_| v_dist.sample(&mut rng)).collect();
                batches.push(
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(ts)),
                            Arc::new(Float64Array::from(v)),
                        ],
                    )
                    .unwrap(),
                );
                remaining -= len;
            }
            batches
        })
        .collect()
}

fn make_ctx(
    data: &[Vec<RecordBatch>],
    target_partitions: usize,
    with_parallel_window: bool,
) -> SessionContext {
    // Data is monotonic per partition (see `make_partitions`), so SortExec
    // runs cheaply, but we deliberately don't declare `with_sort_order` here:
    // the PoC's `RangeRepartitionExec` sources its global extremes from the
    // SortExec it sits above, and elision would break that. Sort cost is
    // already amortized across `target_partitions` input partitions.
    let table = MemTable::try_new(schema(), data.to_vec()).unwrap();
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_batch_size(BATCH_SIZE);

    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config);
    if !with_parallel_window {
        let rules: Vec<_> = PhysicalOptimizer::new()
            .rules
            .into_iter()
            .filter(|r| r.name() != "ParallelWindow")
            .collect();
        builder = builder.with_physical_optimizer_rules(rules);
    }
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("t", Arc::new(table)).unwrap();
    ctx
}

fn run_once(ctx: &SessionContext, rt: &Runtime, sql: &str) -> usize {
    let df = rt.block_on(ctx.sql(sql)).unwrap();
    let batches = rt.block_on(df.collect()).unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    black_box(batches);
    rows
}

fn main() {
    let rt = Runtime::new().unwrap();
    // Five aggregates over the same OVER clause: BWAG fuses them into
    // one operator with five accumulators, so each sliding output row
    // pays ~5x the per-row work of `SUM` alone. Shifts the cost balance
    // toward BWAG and away from the (pre-sorted) input scan.
    let sql = format!(
        "SELECT \
            SUM(v) OVER w, \
            AVG(v) OVER w, \
            MIN(v) OVER w, \
            MAX(v) OVER w, \
            COUNT(*) OVER w \
         FROM t \
         WINDOW w AS \
             (ORDER BY ts RANGE BETWEEN {HALO_RANGE} PRECEDING AND CURRENT ROW)"
    );

    println!("cores,with_poc,iter,seconds,rows");
    for &cores in CORE_SETTINGS {
        let data = make_partitions(cores);
        for &with_poc in &[false, true] {
            let ctx = make_ctx(&data, cores, with_poc);
            // Warmup: compile the plan, prime allocators.
            let warmup_rows = run_once(&ctx, &rt, &sql);
            for iter in 0..ITERATIONS {
                let t = Instant::now();
                let rows = run_once(&ctx, &rt, &sql);
                let secs = t.elapsed().as_secs_f64();
                assert_eq!(
                    rows, warmup_rows,
                    "row count drifted: warmup={warmup_rows} run={rows}"
                );
                println!("{cores},{with_poc},{iter},{secs:.6},{rows}");
                eprintln!(
                    "cores={cores:>2} poc={with_poc:<5} iter={iter} \
                     secs={secs:>6.3} rows={rows}"
                );
            }
        }
    }
}
