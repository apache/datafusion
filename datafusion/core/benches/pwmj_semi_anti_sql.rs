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

//! Criterion benchmark for existence subqueries (`EXISTS` / `NOT EXISTS`) whose
//! correlation is a range predicate (`lhs.key < rhs.key`). The query is planned end to
//! end from SQL, with `datafusion.optimizer.enable_piecewise_merge_join` toggled to pick
//! the operator under test:
//!
//! - **on**: the subquery is decorrelated to a `LeftSemi` / `LeftAnti` join and planned as
//!   `PiecewiseMergeJoin`, together with the `SortExec` the planner inserts on the
//!   buffered side. The sort is included because it is a real cost of that plan.
//! - **off**: the same join falls back to `NestedLoopJoinExec`, which is O(n*m).
//!
//! Both arms compute the same result, so the comparison measures the win from routing an
//! inequality-correlated `EXISTS` / `NOT EXISTS` to PWMJ instead of the nested-loop join.
//!
//! Each arm asserts up front that the operator it means to measure is actually in the
//! physical plan. Without that check a planning change (or running this against a build
//! where PWMJ does not accept existence joins) would silently compare
//! `NestedLoopJoinExec` against itself and report a meaningless ~1.0x.
//!
//! ## Axes
//! - **join type**: `EXISTS` (LeftSemi) and `NOT EXISTS` (LeftAnti).
//! - **match regime**: the fraction of left rows that have at least one matching right
//!   row, set by shifting the right-side key range relative to the left one:
//!   `all_match` (100%), `no_match` (0%) and `half_match` (~50%, where the buffered side
//!   ends up only partially marked). Semi output size grows with that fraction; Anti
//!   output size shrinks.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use tokio::runtime::Runtime;

const LEFT_ROWS: usize = 20_000;
const RIGHT_ROWS: usize = 20_000;
const KEY_SPAN: i32 = 10_000;

/// Two-column schema: (`key`, `payload`).
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", DataType::Int32, false),
    ]))
}

/// Build the batches for a single-partition table of `num_rows` rows. Keys are drawn from
/// `[key_offset, key_offset + KEY_SPAN)` in a fixed, reproducible pattern (no RNG so the
/// benchmark is deterministic).
fn build_batches(
    num_rows: usize,
    key_offset: i32,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    let keys: Vec<i32> = (0..num_rows)
        .map(|i| {
            key_offset
                + (i as i32)
                    .wrapping_mul(2_654_435_761u32 as i32)
                    .rem_euclid(KEY_SPAN)
        })
        .collect();
    let payload: Vec<i32> = (0..num_rows as i32).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(payload)),
        ],
    )
    .unwrap();

    // Slice into 8192-row batches to mirror a realistic scan.
    let batch_size = 8192;
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = (batch.num_rows() - offset).min(batch_size);
        batches.push(batch.slice(offset, len));
        offset += len;
    }
    batches
}

/// Register `lhs` and `rhs` in a context with PWMJ planning on or off.
fn create_context(right_offset: i32, pwmj: bool, schema: &SchemaRef) -> SessionContext {
    let config = SessionConfig::new()
        // Pinned so results are comparable across machines, and so the comparison
        // isolates the join operator rather than how much repartitioning surrounds it.
        .with_target_partitions(1)
        .set_bool("datafusion.optimizer.enable_piecewise_merge_join", pwmj);
    let ctx = SessionContext::new_with_config(config);

    for (name, key_offset, num_rows) in
        [("lhs", 0, LEFT_ROWS), ("rhs", right_offset, RIGHT_ROWS)]
    {
        let table = MemTable::try_new(
            Arc::clone(schema),
            vec![build_batches(num_rows, key_offset, schema)],
        )
        .unwrap();
        ctx.register_table(name, Arc::new(table)).unwrap();
    }
    ctx
}

/// `EXISTS` / `NOT EXISTS` over the range correlation `lhs.key < rhs.key`.
fn query(exists: bool) -> String {
    let negation = if exists { "" } else { "NOT " };
    format!(
        "SELECT lhs.key, lhs.payload FROM lhs \
         WHERE {negation}EXISTS (SELECT 1 FROM rhs WHERE lhs.key < rhs.key)"
    )
}

fn physical_plan(
    ctx: &SessionContext,
    rt: &Runtime,
    sql: &str,
) -> Arc<dyn ExecutionPlan> {
    rt.block_on(async {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    })
}

/// Fail loudly if the plan does not contain every expected fragment, so an arm can never
/// silently measure an operator other than the one it is named after.
fn assert_plan_contains(plan: &Arc<dyn ExecutionPlan>, expected: &[&str], label: &str) {
    let displayed = displayable(plan.as_ref()).indent(false).to_string();
    for fragment in expected {
        assert!(
            displayed.contains(fragment),
            "{label}: expected `{fragment}` in the physical plan, got:\n{displayed}"
        );
    }
}

fn run(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext, rt: &Runtime) -> usize {
    rt.block_on(async {
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    })
}

fn bench_pwmj_semi_anti_sql(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();

    // An existence join needs only *one* match per left row, so all that matters is where
    // `max(rhs.key)` falls inside the left range: a left row survives `EXISTS` iff
    // `lhs.key < max(rhs.key)`. Shifting the right range up therefore saturates at
    // all-match; only shifting it *down* leaves part of the left side unmatched.
    let regimes: [(&str, i32); 3] = [
        // Right keys entirely above the left range: every left row matches.
        ("all_match", KEY_SPAN),
        // Right keys entirely below the left range: no left row matches, so the scan
        // walks the whole buffered side without marking anything.
        ("no_match", -KEY_SPAN),
        // Ranges half-overlap: ~50% of left rows match, so only a suffix of the buffered
        // side gets marked and the scan depth varies per streamed row.
        ("half_match", -KEY_SPAN / 2),
    ];

    let mut group = c.benchmark_group("pwmj_vs_nlj_semi_anti_sql");
    // Nested-loop is O(n*m); keep sample counts modest so the suite finishes.
    group.sample_size(10);

    for (regime, right_offset) in regimes {
        // Only the Semi/Anti half of the join type is pinned, not the side: with PWMJ
        // disabled the planner swaps the nested-loop inputs, so the same `EXISTS` plans as
        // `RightSemi` there and `LeftSemi` under PWMJ.
        for (exists, join_type) in [(true, "Semi"), (false, "Anti")] {
            let sql = query(exists);
            let label = if exists { "semi" } else { "anti" };

            for (arm, pwmj, operator) in [
                ("pwmj", true, "PiecewiseMergeJoin"),
                ("nlj", false, "NestedLoopJoinExec"),
            ] {
                let ctx = create_context(right_offset, pwmj, &s);
                let name = format!("{arm}_{label}_{regime}");
                assert_plan_contains(
                    &physical_plan(&ctx, &rt, &sql),
                    &[operator, join_type],
                    &name,
                );

                // Plan afresh in the untimed setup rather than reusing one plan: the
                // buffered side of `PiecewiseMergeJoinExec` (its visited-indices bitmap
                // and final-pass partition counter) is cached in a `OnceAsync` on the
                // exec, so a second `collect` over the same instance would not repeat
                // the work.
                group.bench_function(BenchmarkId::new(name, RIGHT_ROWS), |b| {
                    b.iter_batched(
                        || physical_plan(&ctx, &rt, &sql),
                        |plan| run(plan, &ctx, &rt),
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_pwmj_semi_anti_sql);
criterion_main!(benches);
