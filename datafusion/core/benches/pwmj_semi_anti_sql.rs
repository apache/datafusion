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
//! - **join type**: `EXISTS` (LeftSemi) and `NOT EXISTS` (LeftAnti), plus `RIGHT SEMI` and
//!   `RIGHT ANTI` written out as joins, and `EXISTS` inside an always-false disjunction
//!   (LeftMark).
//! - **match regime**: the fraction of rows on the marked side that have at least one match
//!   on the other, set by shifting the right-side key range relative to the left one:
//!   `all_match` (100%), `no_match` (0%) and `half_match` (~50%, where the buffered side
//!   ends up only partially marked). Semi output size grows with that fraction; Anti
//!   output size shrinks.
//!
//! ## Right existence joins
//! `RightSemi` / `RightAnti` mark the *streamed* side, and go through the same case runner,
//! data, regimes and guards as the left ones, so the two halves of existence-join support can
//! be read side by side from one run. Two things differ, both forced:
//!
//! - The join type is **written out** (`RIGHT SEMI JOIN`) rather than reached through
//!   `EXISTS`: a decorrelated `EXISTS` gives `LeftSemi`, and which side a nested-loop join
//!   marks is its own choice, so a subquery cannot pin these join types.
//! - There is **no `SortExec`** in the enabled arm's plan. `∃l. l.key < r.key` is
//!   `min(lhs.key) < r.key` -- one comparison against one buffered key -- so that side is
//!   folded, not ordered.
//!
//! The **same three key offsets** serve both halves, each read against the other side's
//! extreme: a left row survives `EXISTS` iff `lhs.key < max(rhs.key)`, a right row survives
//! `RIGHT SEMI` iff `min(lhs.key) < rhs.key`.
//!
//! ## Mark joins
//! `LeftMark` reuses the exact same watermark-marking path as `LeftSemi` -- the only
//! difference is the final pass, which appends a `mark` column instead of slicing the batch
//! -- so it is folded into the same SQL sweep above via an `EXISTS` wrapped in an
//! always-false `OR`, the one shape that decorrelates to it (see `Kind::LeftMark`'s doc). It
//! needs no build-dependent handling beyond what `Kind::LeftMark` already gets from
//! `PWMJ_OR_NLJ`: on a build that does not yet route `LeftMark` to PWMJ, the planner itself
//! falls back to `NestedLoopJoinExec`, same as every other case here before its dependency
//! landed.
//!
//! `RightMark` has no *direct* SQL surface: no keyword parses to it, and no logical rule
//! constructs it (`decorrelate_predicate_subquery.rs` only ever builds `LeftMark`). The
//! physical optimizer can still land on one indirectly -- `JoinSelection` may call
//! `NestedLoopJoinExec::swap_inputs`, which swaps `LeftMark` to `RightMark` via
//! `JoinType::swap` -- but there is no query text or table layout in this file that drives
//! that swap for a `LeftMark` plan, so `ctx.sql(...)` still never plans a `RightMark` here in
//! practice. `bench_pwmj_right_mark_hand_built`, in a separate benchmark group below, builds
//! `PiecewiseMergeJoinExec` and `NestedLoopJoinExec` directly instead -- a deliberate
//! direct-operator microbenchmark, not a stand-in for a SQL plan, and the one place in this
//! file that deviates from planning through SQL. That also means it has no planner fallback
//! to lean on if the build does not support `RightMark` yet, so it probes `try_new` up front
//! and skips the whole group with a note instead of panicking, the hand-built equivalent of
//! what the planner does automatically for every SQL-planned case in this file.

use std::sync::Arc;

use arrow::array::{Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use datafusion::datasource::MemTable;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{NestedLoopJoinExec, PiecewiseMergeJoinExec};
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{DataFusionError, JoinSide};
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

/// The join type a case exercises, one variant per `JoinType` this file covers. Which side is
/// marked differs, and so does how the join has to be expressed in SQL.
#[derive(Clone, Copy)]
enum Kind {
    /// `EXISTS`, decorrelated to `LeftSemi`; marks `lhs`, the buffered side.
    LeftSemi,
    /// `NOT EXISTS`, decorrelated to `LeftAnti`.
    LeftAnti,
    /// `RIGHT SEMI`, written out; marks `rhs`, the streamed side.
    RightSemi,
    /// `RIGHT ANTI`, written out.
    RightAnti,
    /// `EXISTS` inside an always-false disjunction, decorrelated to `LeftMark`: the outer
    /// filter then needs the subquery's match result as a value rather than as a row filter
    /// (see `decorrelate_predicate_subquery.rs`). No SQL syntax reaches `LeftMark` directly.
    LeftMark,
}

impl Kind {
    /// Whether this join keeps the marked rows that matched, rather than the ones that did not.
    ///
    /// The two are complements, which is why one expected row count covers both. `LeftMark`'s
    /// disjunct is always false, so its final row set is the same as `LeftSemi`'s.
    fn keeps_matched(self) -> bool {
        matches!(self, Kind::LeftSemi | Kind::RightSemi | Kind::LeftMark)
    }

    /// The query, over the range relation `lhs.key < rhs.key`, projecting whichever side this
    /// join type emits.
    fn sql(self) -> &'static str {
        match self {
            Kind::LeftSemi => {
                "SELECT lhs.key, lhs.payload FROM lhs \
                 WHERE EXISTS (SELECT 1 FROM rhs WHERE lhs.key < rhs.key)"
            }
            Kind::LeftAnti => {
                "SELECT lhs.key, lhs.payload FROM lhs \
                 WHERE NOT EXISTS (SELECT 1 FROM rhs WHERE lhs.key < rhs.key)"
            }
            Kind::RightSemi => {
                "SELECT rhs.key, rhs.payload FROM lhs \
                 RIGHT SEMI JOIN rhs ON lhs.key < rhs.key"
            }
            Kind::RightAnti => {
                "SELECT rhs.key, rhs.payload FROM lhs \
                 RIGHT ANTI JOIN rhs ON lhs.key < rhs.key"
            }
            // `lhs.payload` is always >= 0 (see `build_batches`), so `< 0` is always false and
            // the `OR` degenerates to the `EXISTS` alone -- but only after decorrelating to a
            // `LeftMark` join whose `mark` the outer `Filter` reads.
            Kind::LeftMark => {
                "SELECT lhs.key, lhs.payload FROM lhs \
                 WHERE lhs.payload < 0 OR EXISTS (SELECT 1 FROM rhs WHERE lhs.key < rhs.key)"
            }
        }
    }

    /// Benchmark-id prefix. The left ones are unprefixed, which is what they were called before
    /// this file covered both halves.
    fn label(self) -> &'static str {
        match self {
            Kind::LeftSemi => "semi",
            Kind::LeftAnti => "anti",
            Kind::RightSemi => "right_semi",
            Kind::RightAnti => "right_anti",
            Kind::LeftMark => "left_mark",
        }
    }

    /// Plan fragment every arm must contain. Only the Semi/Anti/Mark half of the join type is
    /// pinned, not the side: the planner is free to swap the nested-loop inputs, so the arms
    /// can disagree on the side while computing the same thing.
    fn plan_fragment(self) -> &'static str {
        match self {
            Kind::LeftSemi | Kind::RightSemi => "Semi",
            Kind::LeftAnti | Kind::RightAnti => "Anti",
            Kind::LeftMark => "Mark",
        }
    }

    /// Rows on the marked side: this join emits either those that matched or the rest. Both
    /// tables are the same size, so this is spelled out only because which side is marked is the
    /// whole difference between the two halves.
    fn marked_rows(self) -> usize {
        match self {
            Kind::LeftSemi | Kind::LeftAnti | Kind::LeftMark => LEFT_ROWS,
            Kind::RightSemi | Kind::RightAnti => RIGHT_ROWS,
        }
    }
}

/// Every join type covered, in the order cases run.
const KINDS: [Kind; 5] = [
    Kind::LeftSemi,
    Kind::LeftAnti,
    Kind::RightSemi,
    Kind::RightAnti,
    Kind::LeftMark,
];

/// How much of the marked side has a match, set by where the `rhs` key range sits relative to
/// the `lhs` one.
#[derive(Clone, Copy)]
enum Fraction {
    /// Saturated: every marked row matches.
    All,
    /// Saturated the other way: none does.
    None,
    /// A genuine mix, ~50%.
    Half,
}

impl Fraction {
    /// Marked rows that have a match, or `None` when the answer is only pinned down as a mix.
    fn expected_matched_rows(self, marked_rows: usize) -> Option<usize> {
        match self {
            Fraction::All => Some(marked_rows),
            Fraction::None => Some(0),
            Fraction::Half => None,
        }
    }
}

/// The three regimes, shared by both halves. The offsets are the same for each, because each is
/// read against the *other* side's extreme: shifting the `rhs` range up saturates `EXISTS` via
/// `max(rhs.key)` and `RIGHT SEMI` via `min(lhs.key)` alike, and shifting it down empties both.
const REGIMES: [(&str, i32, Fraction); 3] = [
    // Right keys entirely above the left range.
    ("all_match", KEY_SPAN, Fraction::All),
    // Right keys entirely below it, so a left-marking scan walks the whole buffered side
    // without marking anything.
    ("no_match", -KEY_SPAN, Fraction::None),
    // Ranges half-overlap, so a left-marking scan marks only a suffix of the buffered side and
    // its depth varies per streamed row.
    ("half_match", -KEY_SPAN / 2, Fraction::Half),
];

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

/// Counts of the `mark` column's values across every output batch: `(true, false, null)`.
///
/// A row count alone cannot tell a correct mark join from a broken one: `RightMark` keeps
/// every streamed row regardless of whether it matched, so a buggy `PiecewiseMergeJoinExec`
/// that always marks `true` (or always `false`, or emits `lhs`'s row count under a different
/// column) would still pass a row-count-only check, since `LEFT_ROWS == RIGHT_ROWS` and every
/// regime here returns exactly `RIGHT_ROWS` rows either way. Reading the actual booleans back
/// is what catches that.
fn mark_counts(
    plan: Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
    rt: &Runtime,
) -> (usize, usize, usize) {
    rt.block_on(async {
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let mut true_count = 0;
        let mut false_count = 0;
        let mut null_count = 0;
        for batch in &batches {
            let mark_idx = batch
                .schema()
                .index_of("mark")
                .expect("mark join output is expected to carry a `mark` column");
            let mark = batch
                .column(mark_idx)
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .expect("`mark` column is expected to be boolean");
            for i in 0..mark.len() {
                if mark.is_null(i) {
                    null_count += 1;
                } else if mark.value(i) {
                    true_count += 1;
                } else {
                    false_count += 1;
                }
            }
        }
        (true_count, false_count, null_count)
    })
}

/// One point in a sweep: which join, over what data, and what it must emit.
struct Case<'a> {
    kind: Kind,
    /// Regime name, used in the benchmark id.
    regime: &'a str,
    /// Shifts the `rhs` key range relative to the `lhs` one, setting the fraction of rows on the
    /// marked side that have a match.
    right_offset: i32,
    /// Marked rows that have a match, or `None` when the answer is only pinned down as a mix.
    /// A semi join emits exactly those and an anti join the complement, so one number covers
    /// both.
    expected_matched_rows: Option<usize>,
}

/// Plan both arms of one case, report and cross-check them, then time them.
fn bench_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
    schema: &SchemaRef,
    case: &Case<'_>,
) {
    let &Case {
        kind,
        regime,
        right_offset,
        expected_matched_rows,
    } = case;
    let sql = kind.sql();
    let label = kind.label();

    // Plan and check both arms before timing either, so the operators they picked are on screen
    // ahead of the numbers those operators qualify.
    let arms: Vec<(String, SessionContext, &str)> = [
        ("pwmj_enabled", true, PWMJ_OR_NLJ),
        ("nlj", false, NLJ_ONLY),
    ]
    .into_iter()
    .map(|(arm, pwmj, operators)| {
        let ctx = create_context(right_offset, pwmj, schema);
        let name = format!("{arm}_{label}_{regime}");
        let planned = assert_plan_operator(
            &physical_plan(&ctx, rt, sql),
            &[kind.plan_fragment()],
            operators,
            &name,
        );
        println!("{name}: planned {planned}");
        (name, ctx, planned)
    })
    .collect();

    if arms.iter().all(|(_, _, planned)| *planned == arms[0].2) {
        println!(
            "note: {label}_{regime}: both arms planned {}, so the ratio between them \
             measures nothing about PWMJ",
            arms[0].2
        );
    }

    // Both arms have to agree on the answer, or the comparison is between a fast operator and a
    // wrong one.
    let rows: Vec<usize> = arms
        .iter()
        .map(|(_, ctx, _)| run(physical_plan(ctx, rt, sql), ctx, rt))
        .collect();
    assert_eq!(
        rows[0], rows[1],
        "{label}_{regime}: pwmj_enabled and nlj disagree ({} vs {} rows)",
        rows[0], rows[1]
    );

    // ...and the regime has to be the shape its name claims, or the sweep measures nothing.
    let marked_rows = kind.marked_rows();
    match expected_matched_rows {
        Some(matched) => {
            let expected = if kind.keeps_matched() {
                matched
            } else {
                marked_rows - matched
            };
            assert_eq!(
                rows[0], expected,
                "{label}_{regime}: expected {expected} rows, got {}",
                rows[0]
            );
        }
        None => assert!(
            rows[0] > 0 && rows[0] < marked_rows,
            "{label}_{regime}: {} rows is all-or-nothing, adjust the data shape",
            rows[0]
        ),
    }

    for (name, ctx, _) in &arms {
        // Plan afresh in the untimed setup rather than reusing one plan: the buffered side of
        // `PiecewiseMergeJoinExec` is cached in a `OnceAsync` on the exec -- the visited-indices
        // bitmap and final-pass counter on the left path, the folded min/max on the right one --
        // so a second `collect` over the same instance would not repeat the work.
        group.bench_function(BenchmarkId::new(name.as_str(), RIGHT_ROWS), |b| {
            b.iter_batched(
                || physical_plan(ctx, rt, sql),
                |plan| run(plan, ctx, rt),
                BatchSize::SmallInput,
            )
        });
    }
}

fn bench_pwmj_semi_anti_sql(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();

    let mut group = c.benchmark_group("pwmj_vs_nlj_semi_anti_sql");
    // Nested-loop is O(n*m); keep sample counts modest so the suite finishes.
    group.sample_size(10);

    // Match fraction, at one size, for both halves.
    for (regime, right_offset, fraction) in REGIMES {
        for kind in KINDS {
            bench_case(
                &mut group,
                &rt,
                &s,
                &Case {
                    kind,
                    regime,
                    right_offset,
                    expected_matched_rows: fraction
                        .expected_matched_rows(kind.marked_rows()),
                },
            );
        }
    }

    group.finish();
}

/// A `JoinFilter` for `lhs.key < rhs.key`, the same range relation every case in this file
/// uses, for the `NestedLoopJoinExec` arm below -- which needs one built by hand since it is
/// constructed directly rather than planned from SQL.
fn key_lt_key_filter(s: &SchemaRef) -> JoinFilter {
    let expr = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("key", 0)),
        Operator::Lt,
        Arc::new(Column::new("key", 1)),
    )) as _;
    let column_indices = vec![
        ColumnIndex {
            index: 0,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: 0,
            side: JoinSide::Right,
        },
    ];
    let key_field = s.field_with_name("key").unwrap().clone();
    let intermediate_schema = Schema::new(vec![key_field.clone(), key_field]);
    JoinFilter::new(expr, column_indices, Arc::new(intermediate_schema))
}

/// `RightMark` has no direct SQL surface (see the module doc's "Mark joins" section), so both
/// arms are hand-built here instead of planned from SQL text: `PiecewiseMergeJoinExec` and
/// `NestedLoopJoinExec`, over the same data, the same `lhs.key < rhs.key` relation, and the
/// same join type -- with no risk of comparing an operator against itself, since which
/// operator each arm uses is fixed by construction rather than read back off a plan.
///
/// Neither arm needs a `SortExec`: `RightMark`, like `RightSemi`/`RightAnti`, folds the
/// buffered side to one key regardless of its order, and `NestedLoopJoinExec` never needs
/// either side ordered.
fn bench_pwmj_right_mark_hand_built(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();
    let ctx = SessionContext::new();

    // Every other case in this file falls back to `NestedLoopJoinExec` through the planner
    // itself when a build does not yet support the join type -- there is no such fallback
    // here, since `RightMark` has no SQL surface to plan through in the first place. Probe
    // `try_new` directly and skip the whole group rather than panic, so this benchmark stays
    // runnable (as a no-op) against a build that has not merged `RightMark` support yet, and
    // starts measuring on its own once that support lands.
    //
    // Only the specific `NotImplemented` this build's `try_new` returns for an unsupported
    // join type is treated as "not landed yet"; any other error (a schema mistake here, or a
    // real regression in `try_new`) is a bug in this benchmark or in PWMJ and must panic
    // rather than be swallowed as a skip.
    match PiecewiseMergeJoinExec::try_new(
        Arc::new(EmptyExec::new(Arc::clone(&s))),
        Arc::new(EmptyExec::new(Arc::clone(&s))),
        (
            Arc::new(Column::new("key", 0)) as _,
            Arc::new(Column::new("key", 0)) as _,
        ),
        Operator::Lt,
        JoinType::RightMark,
        1,
    ) {
        Ok(_) => {}
        Err(err @ DataFusionError::NotImplemented(_)) => {
            println!(
                "note: pwmj_vs_nlj_right_mark_hand_built skipped -- this build's \
                 PiecewiseMergeJoinExec does not support RightMark yet: {err}"
            );
            return;
        }
        Err(err) => panic!(
            "pwmj_vs_nlj_right_mark_hand_built: unexpected error probing RightMark \
             support, not the NotImplemented this benchmark skips on: {err}"
        ),
    }

    let mut group = c.benchmark_group("pwmj_vs_nlj_right_mark_hand_built");
    group.sample_size(10);

    for (regime, right_offset, fraction) in REGIMES {
        let lhs_batches = build_batches(LEFT_ROWS, 0, &s);
        let rhs_batches = build_batches(RIGHT_ROWS, right_offset, &s);

        let pwmj_plan = {
            let (lhs_batches, rhs_batches, s) =
                (lhs_batches.clone(), rhs_batches.clone(), Arc::clone(&s));
            move || -> Arc<dyn ExecutionPlan> {
                let lhs = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&lhs_batches),
                    Arc::clone(&s),
                    None,
                )
                .unwrap();
                let rhs = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&rhs_batches),
                    Arc::clone(&s),
                    None,
                )
                .unwrap();
                Arc::new(
                    PiecewiseMergeJoinExec::try_new(
                        lhs,
                        rhs,
                        (
                            Arc::new(Column::new("key", 0)) as _,
                            Arc::new(Column::new("key", 0)) as _,
                        ),
                        Operator::Lt,
                        JoinType::RightMark,
                        1,
                    )
                    .unwrap(),
                )
            }
        };
        let nlj_plan = {
            let (lhs_batches, rhs_batches, s) =
                (lhs_batches.clone(), rhs_batches.clone(), Arc::clone(&s));
            move || -> Arc<dyn ExecutionPlan> {
                let lhs = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&lhs_batches),
                    Arc::clone(&s),
                    None,
                )
                .unwrap();
                let rhs = MemorySourceConfig::try_new_exec(
                    std::slice::from_ref(&rhs_batches),
                    Arc::clone(&s),
                    None,
                )
                .unwrap();
                Arc::new(
                    NestedLoopJoinExec::try_new(
                        lhs,
                        rhs,
                        Some(key_lt_key_filter(&s)),
                        &JoinType::RightMark,
                        None,
                    )
                    .unwrap(),
                )
            }
        };

        // `RightMark` keeps every streamed row, matched or not, so a row count alone cannot
        // tell a correct implementation from a broken one -- both arms return exactly
        // `RIGHT_ROWS` regardless of the regime, unlike `RightSemi`/`RightAnti`, where the
        // regime changes the row count. What the regime actually changes here is which rows
        // get marked `true` vs `false`, so that is what gets checked: the `mark` column's own
        // value distribution, from both arms, against what the regime claims.
        let pwmj_rows = run(pwmj_plan(), &ctx, &rt);
        let nlj_rows = run(nlj_plan(), &ctx, &rt);
        assert_eq!(
            pwmj_rows, nlj_rows,
            "right_mark_{regime}: pwmj and nlj disagree ({pwmj_rows} vs {nlj_rows} rows)"
        );
        assert_eq!(
            pwmj_rows, RIGHT_ROWS,
            "right_mark_{regime}: RightMark must keep every streamed row"
        );

        let (pwmj_true, pwmj_false, pwmj_null) = mark_counts(pwmj_plan(), &ctx, &rt);
        let (nlj_true, nlj_false, nlj_null) = mark_counts(nlj_plan(), &ctx, &rt);
        assert_eq!(
            (pwmj_true, pwmj_false, pwmj_null),
            (nlj_true, nlj_false, nlj_null),
            "right_mark_{regime}: pwmj and nlj disagree on mark values \
             (true/false/null): pwmj={pwmj_true}/{pwmj_false}/{pwmj_null}, \
             nlj={nlj_true}/{nlj_false}/{nlj_null}"
        );
        assert_eq!(
            pwmj_null, 0,
            "right_mark_{regime}: RightMark's mark column must never be null"
        );
        match fraction.expected_matched_rows(RIGHT_ROWS) {
            Some(expected_true) => assert_eq!(
                pwmj_true, expected_true,
                "right_mark_{regime}: expected {expected_true} marked true, got {pwmj_true} \
                 (a uniformly true/false mark column would be caught here)"
            ),
            None => assert!(
                pwmj_true > 0 && pwmj_true < RIGHT_ROWS,
                "right_mark_{regime}: {pwmj_true} rows marked true is all-or-nothing, \
                 adjust the data shape (a uniformly true/false mark column would be caught \
                 here)"
            ),
        }

        group.bench_function(
            BenchmarkId::new("pwmj", format!("{regime}_{RIGHT_ROWS}")),
            |b| {
                b.iter_batched(
                    pwmj_plan.clone(),
                    |plan| run(plan, &ctx, &rt),
                    BatchSize::SmallInput,
                )
            },
        );
        group.bench_function(
            BenchmarkId::new("nlj", format!("{regime}_{RIGHT_ROWS}")),
            |b| {
                b.iter_batched(
                    nlj_plan.clone(),
                    |plan| run(plan, &ctx, &rt),
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_pwmj_semi_anti_sql,
    bench_pwmj_right_mark_hand_built
);
criterion_main!(benches);
