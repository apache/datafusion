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
//! - **`pwmj_enabled`**: on a build that routes existence joins to PWMJ, the subquery is
//!   decorrelated to a `LeftSemi` / `LeftAnti` join and planned as `PiecewiseMergeJoin`,
//!   together with the `SortExec` the planner inserts on the buffered side. The sort is
//!   included because it is a real cost of that plan.
//! - **`nlj`**: the same join is planned as `NestedLoopJoinExec`, which is O(n*m).
//!
//! Both arms compute the same result, so the comparison measures the win from routing an
//! inequality-correlated `EXISTS` / `NOT EXISTS` to PWMJ instead of the nested-loop join.
//!
//! The arms are named after the config they set, not the operator they get, because the
//! enabled arm's operator is build-dependent: while the planner still excludes semi/anti
//! join types from PWMJ, setting the flag changes nothing and both arms plan
//! `NestedLoopJoinExec`. So the enabled arm accepts either operator instead of aborting the
//! run. Benchmark ids stay the same either way, which is what makes such a run useful as a
//! baseline: a later build that does route these joins to PWMJ compares straight against it.
//!
//! What every arm does pin down is *which* operator it planned, printed before the
//! timings, and it says so outright when both arms landed on the same one. Without that a
//! planning change would quietly compare `NestedLoopJoinExec` against itself and the
//! resulting ~1.0x would read as "PWMJ is no faster".
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

/// Operators the `pwmj_enabled` arm is allowed to plan. Two of them, because the planner
/// only hands existence joins to PWMJ once PWMJ accepts semi/anti join types; before that
/// the flag is a no-op and the nested-loop join stays.
const PWMJ_OR_NLJ: &[&str] = &["PiecewiseMergeJoin", "NestedLoopJoinExec"];

/// With the flag off, nothing but the nested-loop join can plan this query.
const NLJ_ONLY: &[&str] = &["NestedLoopJoinExec"];

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

/// Check that the plan contains every fragment in `required` and exactly one of the
/// operators in `one_of`, returning the one it found.
///
/// An arm named after a config flag says nothing about what ran, so each one reports the
/// operator it actually planned. `one_of` has two entries for the enabled arm because
/// whether existence joins reach PWMJ depends on the build; a single-entry list pins the
/// arm down completely.
fn assert_plan_operator<'a>(
    plan: &Arc<dyn ExecutionPlan>,
    required: &[&str],
    one_of: &[&'a str],
    label: &str,
) -> &'a str {
    let displayed = displayable(plan.as_ref()).indent(false).to_string();
    for fragment in required {
        assert!(
            displayed.contains(fragment),
            "{label}: expected `{fragment}` in the physical plan, got:\n{displayed}"
        );
    }

    let found: Vec<&str> = one_of
        .iter()
        .copied()
        .filter(|operator| displayed.contains(operator))
        .collect();
    assert_eq!(
        found.len(),
        1,
        "{label}: expected exactly one of {one_of:?} in the physical plan, found \
         {found:?} in:\n{displayed}"
    );
    found[0]
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

            // Plan and check both arms before timing either, so the operators they picked
            // are on screen ahead of the numbers those operators qualify.
            let arms: Vec<(String, SessionContext, &str)> = [
                ("pwmj_enabled", true, PWMJ_OR_NLJ),
                ("nlj", false, NLJ_ONLY),
            ]
            .into_iter()
            .map(|(arm, pwmj, operators)| {
                let ctx = create_context(right_offset, pwmj, &s);
                let name = format!("{arm}_{label}_{regime}");
                let planned = assert_plan_operator(
                    &physical_plan(&ctx, &rt, &sql),
                    &[join_type],
                    operators,
                    &name,
                );
                println!("{name}: planned {planned}");
                (name, ctx, planned)
            })
            .collect();

            if arms.iter().all(|(_, _, planned)| *planned == arms[0].2) {
                println!(
                    "note: {label}_{regime}: both arms planned {}, so the ratio between \
                     them measures nothing about PWMJ",
                    arms[0].2
                );
            }

            for (name, ctx, _) in &arms {
                // Plan afresh in the untimed setup rather than reusing one plan: the
                // buffered side of `PiecewiseMergeJoinExec` (its visited-indices bitmap
                // and final-pass partition counter) is cached in a `OnceAsync` on the
                // exec, so a second `collect` over the same instance would not repeat
                // the work.
                group.bench_function(BenchmarkId::new(name.as_str(), RIGHT_ROWS), |b| {
                    b.iter_batched(
                        || physical_plan(ctx, &rt, &sql),
                        |plan| run(plan, ctx, &rt),
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
