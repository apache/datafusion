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

//! End-to-end test for **runtime row-group pruning** driven by a TopK
//! `SortExec`'s `DynamicFilterPhysicalExpr`.
//!
//! A 5-row-group parquet file is constructed with disjoint statistics on
//! the sort column (`v`): row group `i` contains values
//! `[i*100, (i+1)*100)`. The query `ORDER BY v DESC LIMIT 5` fills the
//! TopK heap from the row group with the largest values; the threshold
//! then proves the remaining row groups cannot contribute. The runtime
//! `RowGroupPruner` in the parquet scan must observe the tightened
//! threshold and increment `row_groups_pruned_dynamic_filter`.
//!
//! We assert a property (`pruned >= 1`) rather than an exact count
//! because batch-arrival timing affects how soon the TopK heap fills,
//! and we don't want this test to become flaky.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use datafusion::prelude::SessionConfig;

use crate::parquet::Unit::RowGroup;
use crate::parquet::{ContextWithParquet, Scenario};

/// Build five `RecordBatch`es whose `v` column ranges are disjoint:
/// batch `i` carries `v` values `[i*100, (i+1)*100)`. When written with
/// `max_row_group_row_count = 100` each batch lands in its own row group.
fn build_five_disjoint_batches(schema: &Arc<Schema>) -> Vec<RecordBatch> {
    (0..5i64)
        .map(|rg| {
            let base = rg * 100;
            let values: Vec<i64> = (base..base + 100).collect();
            let col: ArrayRef = Arc::new(Int64Array::from(values));
            RecordBatch::try_new(Arc::clone(schema), vec![col]).unwrap()
        })
        .collect()
}

/// Build five `RecordBatch`es in *descending* value order: batch 0 holds
/// `v ∈ [400, 500)`, batch 4 holds `v ∈ [0, 100)`. The physical row-group
/// order on disk therefore does **not** match the order a `ORDER BY v ASC`
/// query wants — sort-pushdown's `reorder_by_statistics` must rearrange
/// the access plan so the scan reads RG 4 first, then RG 3, etc.
fn build_five_disjoint_batches_desc(schema: &Arc<Schema>) -> Vec<RecordBatch> {
    (0..5i64)
        .map(|rg| {
            let base = (4 - rg) * 100;
            let values: Vec<i64> = (base..base + 100).collect();
            let col: ArrayRef = Arc::new(Int64Array::from(values));
            RecordBatch::try_new(Arc::clone(schema), vec![col]).unwrap()
        })
        .collect()
}

/// `ORDER BY v DESC LIMIT 5` against a 5-RG file with disjoint per-RG
/// stats must trigger runtime RG pruning: the first RG read fills the
/// heap, and the tightened threshold proves every other RG unreachable.
#[tokio::test]
async fn dynamic_rg_pruning_metric_fires_for_topk_descending_limit() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batches = build_five_disjoint_batches(&schema);

    // `with_custom_data` honors the custom schema + batches and ignores
    // `Scenario`. `Unit::RowGroup(100)` enables `pushdown_filters`, which
    // is required for the TopK dynamic filter to reach the parquet scan.
    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        RowGroup(100),
        Arc::clone(&schema),
        batches,
    )
    .await;

    let output = ctx.query("SELECT v FROM t ORDER BY v DESC LIMIT 5").await;

    assert_eq!(output.result_rows, 5, "query must return LIMIT rows");

    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert!(
        pruned >= 1,
        "dynamic RG pruner must skip at least one row group; \
         pruned={pruned}\n{}",
        output.description(),
    );
}

/// Regression for the rg_plan / `reorder_by_statistics` ordering bug.
///
/// When `sort_order_for_reorder` is set on the parquet scan,
/// `prepare_access_plan` calls
/// [`PreparedAccessPlan::reorder_by_statistics`], which rearranges
/// `row_group_indexes` so the decoder reads row groups in stats-optimal
/// order (smallest-min first for ASC, etc.). The stream's per-RG plan
/// (`rg_plan`) — which the runtime pruner walks one entry at a time —
/// **must use this reordered list**, not the access plan's natural
/// (index-ascending) order. Otherwise the pruner would consult the
/// metadata of RG K while the decoder is actually about to yield RG K',
/// silently producing wrong results.
///
/// This test makes the failure visible:
///
/// - File is written with RGs in *descending* `v` order (RG 0 has the
///   largest values, RG 4 has the smallest).
/// - Query is `ORDER BY v ASC LIMIT 5`, so sort-pushdown reorders the
///   access plan to read RG 4 first, then RG 3, etc.
/// - The smallest five values (which form the entire correct LIMIT
///   answer) live in RG 4 alone. After they are emitted, the TopK
///   threshold tightens enough that the per-RG pruner skips every other
///   RG.
///
/// Without the fix, `rg_plan` would be `[0, 1, 2, 3, 4]` while the
/// decoder reads `[4, 3, 2, 1, 0]`. The first yielded reader (for RG 4
/// in the decoder) would be tracked as if it were RG 0, the pruner
/// would check RG 1's stats (id range 300..400) against a threshold
/// already tightened to `v < 5`, prune RG 1 (because nothing in
/// 300..400 can satisfy `v < 5`), and then the rebuild via
/// `into_builder` would scan a row group whose data does not match its
/// expected metadata. The query would return fewer than five rows or
/// the wrong rows.
#[tokio::test]
async fn dynamic_rg_pruning_handles_sort_pushdown_reorder() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batches = build_five_disjoint_batches_desc(&schema);

    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        RowGroup(100),
        Arc::clone(&schema),
        batches,
    )
    .await;

    let output = ctx.query("SELECT v FROM t ORDER BY v ASC LIMIT 5").await;

    // Correctness — the five smallest values in the file are 0..=4.
    // If `rg_plan` is misaligned with the decoder's read order, the
    // pruner consults the wrong RG's stats and the result row count or
    // values would drift.
    assert_eq!(output.result_rows, 5, "query must return LIMIT rows");
    let formatted = output.pretty_results();
    for v in 0..=4i64 {
        assert!(
            formatted.contains(&format!("| {v} ")),
            "output must contain the smallest value {v}; got:\n{formatted}",
        );
    }

    // Behavior — the per-RG pruner must engage. We don't pin the exact
    // count (batch-arrival timing affects how soon the heap fills); we
    // only require that at least one row group is skipped at runtime.
    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert!(
        pruned >= 1,
        "with `sort_order_for_reorder` active and a tight TopK, the \
         runtime pruner must skip at least one row group; pruned={pruned}\n{}",
        output.description(),
    );
}

/// A query without ORDER BY does not produce a TopK and therefore no
/// `DynamicFilterPhysicalExpr` reaches the scan. The runtime pruner must
/// stay quiet — the metric should be 0.
#[tokio::test]
async fn dynamic_rg_pruning_metric_quiet_without_topk() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batches = build_five_disjoint_batches(&schema);

    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        RowGroup(100),
        Arc::clone(&schema),
        batches,
    )
    .await;

    // Plain `SELECT *` — no sort, no limit, no dynamic filter.
    let output = ctx.query("SELECT v FROM t").await;
    assert_eq!(output.result_rows, 500);

    let pruned = output.row_groups_pruned_dynamic_filter().unwrap_or(0);
    assert_eq!(
        pruned,
        0,
        "without TopK there is no dynamic filter, so the runtime pruner \
         must not fire; pruned={pruned}\n{}",
        output.description(),
    );
}

/// Regression for "into_builder called mid-row-group" — surfaced by
/// ClickBench Q24 / Q26 (`SELECT … WHERE x <> '' ORDER BY ts LIMIT 10`).
///
/// The push-decoder state machine re-enters Step 2 on every iteration of
/// the `transition` loop, including iterations where Step 3 returned
/// `NeedsData` and pushed byte ranges but has not yet produced a reader
/// for the upcoming row group. At those moments the decoder is in
/// `ReadingRowGroup` state but `is_at_row_group_boundary()` is `false`,
/// and the runtime row-group pruner's `into_builder()` rebuild path
/// errored out with:
///
/// ```text
/// Parquet error: into_builder called mid-row-group;
/// check is_at_row_group_boundary() first
/// ```
///
/// The fix in `push_decoder.rs::Step 2` gates the prune-and-rebuild on
/// `is_at_row_group_boundary()`. This test reproduces the trigger: a
/// many-RG file (so the pruner has work to do) plus an `ORDER BY` query
/// whose TopK threshold tightens enough to make the pruner want to
/// rebuild more than once during the scan. Before the fix the query
/// returned an `Execution` / `Parquet` error; after the fix it returns
/// the expected ten rows and the pruner fires.
#[tokio::test]
async fn dynamic_rg_pruner_does_not_call_into_builder_mid_row_group() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    // 20 disjoint row groups of 50 values each. With 20 RGs the pruner
    // gets multiple boundaries to attempt rebuilds, so any path that
    // calls `into_builder` outside a boundary is hit reliably.
    let batches: Vec<RecordBatch> = (0..20i64)
        .map(|rg| {
            let base = rg * 50;
            let values: Vec<i64> = (base..base + 50).collect();
            let col: ArrayRef = Arc::new(Int64Array::from(values));
            RecordBatch::try_new(Arc::clone(&schema), vec![col]).unwrap()
        })
        .collect();

    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        RowGroup(50),
        Arc::clone(&schema),
        batches,
    )
    .await;

    let output = ctx.query("SELECT v FROM t ORDER BY v ASC LIMIT 10").await;

    // Correctness: smallest ten values are 0..=9.
    assert_eq!(output.result_rows, 10, "query must return LIMIT rows");
    let formatted = output.pretty_results();
    for v in 0..=9i64 {
        assert!(
            formatted.contains(&format!("| {v} ")),
            "output must contain smallest value {v}; got:\n{formatted}",
        );
    }

    // Behavior: with 20 disjoint RGs and a tight TopK, the dynamic
    // pruner must skip a meaningful share of them. We don't pin the
    // exact count — what matters is that the scan *completed* without
    // the mid-row-group rebuild error.
    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert!(
        pruned >= 1,
        "dynamic RG pruner must skip at least one row group; \
         pruned={pruned}\n{}",
        output.description(),
    );
}

/// Build five sorted `RecordBatch`es with 1000 values each so that, when
/// the writer is configured with `row_per_group=1000` and
/// `data_page_row_count_limit=100`, every row group ends up with **ten
/// data pages** of 100 rows each. RG `i` covers `[i*1000, (i+1)*1000)`,
/// monotonically ascending — page index will then have tight per-page
/// `min`/`max` and can prune at sub-RG granularity.
fn build_five_thousand_row_rgs(schema: &Arc<Schema>) -> Vec<RecordBatch> {
    (0..5i64)
        .map(|rg| {
            let base = rg * 1000;
            let values: Vec<i64> = (base..base + 1000).collect();
            let col: ArrayRef = Arc::new(Int64Array::from(values));
            RecordBatch::try_new(Arc::clone(schema), vec![col]).unwrap()
        })
        .collect()
}

/// Regression test for <https://github.com/apache/datafusion/issues/24355>:
/// when a page-index `RowSelection` is live, the runtime dynamic row-group
/// pruner is intentionally **not built**, so its `into_builder` rebuild can
/// never drop a row group without slicing the carried selection (which would
/// silently return wrong rows). Correctness is bought at the cost of the
/// dynamic-pruning optimization for this scan.
///
/// The behavior asserted below (pruner disabled →
/// `row_groups_pruned_dynamic_filter == 0`) is expected to change once the
/// proper upstream fix lands, which keeps both mechanisms:
/// <https://github.com/apache/arrow-rs/issues/10624> (tracked on the
/// DataFusion side in <https://github.com/apache/datafusion/issues/24358>).
///
/// Layout: 5 RGs × 1000 rows, with `data_page_row_count_limit=100` so
/// each RG has 10 pages of 100 rows.
///
/// Query: `SELECT v FROM t WHERE v >= 500 ORDER BY v DESC LIMIT 5`.
/// - `v >= 500` engages the page index: in RG 0 (values 0..1000) the
///   first 5 pages (values 0..500) are pruned, the last 5 (500..1000)
///   are scanned. RGs 1..4 keep all their pages (every page has
///   `max >= 500`). The decoder receives a `RowSelection` that masks
///   out those first 5 pages of RG 0 — its presence is what suppresses
///   the runtime pruner.
/// - `ORDER BY v DESC LIMIT 5` would let the tightened TopK threshold
///   (≥ 4995) prune RGs 0..3, but because a row selection is present the
///   runtime pruner is never created, so `row_groups_pruned_dynamic_filter`
///   stays 0. Results are still correct and page-index pruning still runs.
#[tokio::test]
async fn dynamic_rg_pruning_disabled_when_page_index_row_selection_present() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batches = build_five_thousand_row_rgs(&schema);

    // `RowGroupAndPage(1000, 100)` enables both `pushdown_filters` and
    // page-index pruning, and writes a parquet file with 1000-row RGs
    // partitioned into 100-row pages.
    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        crate::parquet::Unit::RowGroupAndPage(1000, 100),
        Arc::clone(&schema),
        batches,
    )
    .await;

    let output = ctx
        .query("SELECT v FROM t WHERE v >= 500 ORDER BY v DESC LIMIT 5")
        .await;

    // Correctness — top-5 values descending are 4995..=4999 (all in RG 4).
    assert_eq!(output.result_rows, 5, "query must return LIMIT rows");
    let formatted = output.pretty_results();
    for v in 4995..=4999i64 {
        assert!(
            formatted.contains(&format!("| {v} ")),
            "output must contain top-5 descending value {v}; got:\n{formatted}",
        );
    }

    // Page-index pruning still engages: RG 0's first 5 pages are entirely
    // < 500. #24355 only suppresses the *runtime* row-group pruner, not
    // page-index pruning, so this must remain non-zero.
    let pages_pruned = output.metric_value("page_index_pages_pruned").unwrap_or(0);
    assert!(
        pages_pruned >= 5,
        "page index must prune at least 5 pages (RG 0 pages 0..5 for v < 500); \
         pruned={pages_pruned}\n{}",
        output.description(),
    );

    // The runtime dynamic pruner must be disabled while a page-index row
    // selection is live (#24355): with no pruner there is no rebuild that
    // could misapply the carried selection. Before the fix the pruner ran
    // and this metric was >= 1.
    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert_eq!(
        pruned,
        0,
        "runtime row-group pruning must be skipped when a page-index row \
         selection is present; pruned={pruned}\n{}",
        output.description(),
    );
}

/// Co-existence test: a `WHERE` clause that gets pushed into the parquet
/// `RowFilter` plus a `TopK` that drives the dynamic RG pruner.
///
/// `v % 2 = 0` cannot be statically pruned and is not page-index-amenable
/// either, so it must run per-row inside the parquet decoder as a
/// `RowFilter`. `ORDER BY v DESC LIMIT 3` then fills the TopK heap and
/// tightens the threshold, triggering runtime RG pruning. The decoder
/// rebuild that happens via
/// `into_builder().with_row_groups(remaining).build()` must preserve the
/// installed `RowFilter` (and any `RowSelection` derived from page-index
/// pruning) across the rebuild — if it didn't, either:
///
/// - The post-prune RGs would silently drop their per-row filtering and
///   the result would contain odd values, OR
/// - The rebuilt decoder would re-emit rows the original was about to
///   yield, double-counting against the limit.
///
/// This test catches both regressions: it pins both the exact result rows
/// (top three even values descending: 498, 496, 494) and asserts the
/// dynamic pruner fired at least once.
#[tokio::test]
async fn dynamic_rg_pruning_coexists_with_row_filter() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batches = build_five_disjoint_batches(&schema);

    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        RowGroup(100),
        Arc::clone(&schema),
        batches,
    )
    .await;

    // `v % 2 = 0` survives stats pruning (every RG straddles even / odd),
    // so the predicate is pushed into the decoder as a `RowFilter` and
    // evaluated per row. The TopK on top still tightens the threshold and
    // engages the runtime RG pruner.
    let output = ctx
        .query("SELECT v FROM t WHERE v % 2 = 0 ORDER BY v DESC LIMIT 3")
        .await;

    assert_eq!(output.result_rows, 3, "query must return LIMIT rows");
    let formatted = output.pretty_results();
    for v in [498i64, 496, 494] {
        assert!(
            formatted.contains(&format!("| {v} ")),
            "output must contain top-3 even descending value {v}; got:\n{formatted}",
        );
    }

    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert!(
        pruned >= 1,
        "with WHERE v % 2 = 0 + TopK the runtime pruner must still skip at \
         least one row group; pruned={pruned}\n{}",
        output.description(),
    );
}

/// Build five two-column `RecordBatch`es: `a` is physically clustered
/// (batch `i` carries `a ∈ [i*100, (i+1)*100)`, disjoint per-RG stats)
/// and `b` is a per-batch shuffle (identical `[0, 100)` range in every
/// RG, useless for pruning).
fn build_two_col_leading_clustered(schema: &Arc<Schema>) -> Vec<RecordBatch> {
    (0..5i64)
        .map(|rg| {
            let base = rg * 100;
            let a: Vec<i64> = (base..base + 100).collect();
            // pseudo-shuffled b, same value set in every RG
            let b: Vec<i64> = (0..100).map(|i| (i * 37) % 100).collect();
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int64Array::from(a)) as ArrayRef,
                    Arc::new(Int64Array::from(b)) as ArrayRef,
                ],
            )
            .unwrap()
        })
        .collect()
}

/// Build five two-column `RecordBatch`es where the *leading* sort key
/// ties everywhere (`a = 1` in every row / RG) and the *secondary* key
/// is clustered but stored in DESC disk order: batch 0 carries
/// `b ∈ [400, 500)`, batch 4 carries `b ∈ [0, 100)`.
///
/// An `ORDER BY a, b LIMIT k` query wants the rows in batch 4 first;
/// reading disk order decodes every RG with a monotonically *improving*
/// threshold that never proves a later RG unwinnable.
fn build_two_col_leading_tied_desc(schema: &Arc<Schema>) -> Vec<RecordBatch> {
    (0..5i64)
        .map(|rg| {
            let base = (4 - rg) * 100;
            let a: Vec<i64> = vec![1; 100];
            let b: Vec<i64> = (base..base + 100).collect();
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int64Array::from(a)) as ArrayRef,
                    Arc::new(Int64Array::from(b)) as ArrayRef,
                ],
            )
            .unwrap()
        })
        .collect()
}

fn two_col_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]))
}

/// A multi-column `ORDER BY a, b LIMIT k` must still engage the runtime
/// RG pruner through the *leading* disjunct of the lexicographic dynamic
/// filter (`a < x OR (a = x AND b < y)`): once the heap fills from the
/// first (best) row group, `min(a) > x` alone proves later RGs
/// unwinnable regardless of `b`.
#[tokio::test]
async fn dynamic_rg_pruning_fires_for_multi_column_sort_leading_clustered() {
    let schema = two_col_schema();
    let batches = build_two_col_leading_clustered(&schema);

    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        RowGroup(100),
        Arc::clone(&schema),
        batches,
    )
    .await;

    let output = ctx
        .query("SELECT a, b FROM t ORDER BY a ASC, b ASC LIMIT 5")
        .await;

    assert_eq!(output.result_rows, 5, "query must return LIMIT rows");

    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert!(
        pruned >= 1,
        "multi-column TopK must prune via the leading column's disjunct; \
         pruned={pruned}\n{}",
        output.description(),
    );
}

/// When the leading sort key ties across all row groups, pruning (and
/// reading the right RG first) must fall to the *secondary* key: RG
/// stats give `min(a) = max(a) = 1` everywhere, so the lex dynamic
/// filter reduces to `a = 1 AND b < y` — prunable via `min(b)`.
///
/// The disk order is adversarial (secondary key DESC), so without
/// multi-column stats reorder the scan reads the worst RG first and the
/// threshold never proves later RGs unwinnable. With multi-column
/// reorder the best RG is read first and every other RG is pruned.
#[tokio::test]
async fn dynamic_rg_pruning_fires_for_multi_column_sort_leading_tied() {
    let schema = two_col_schema();
    let batches = build_two_col_leading_tied_desc(&schema);

    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        RowGroup(100),
        Arc::clone(&schema),
        batches,
    )
    .await;

    let output = ctx
        .query("SELECT a, b FROM t ORDER BY a ASC, b ASC LIMIT 5")
        .await;

    assert_eq!(output.result_rows, 5, "query must return LIMIT rows");
    // The leading key `a = 1` is tied everywhere, so correctness rests
    // entirely on the secondary key: the five smallest `b` values must come
    // back, in ascending secondary order. Assert the exact result rows
    // (full two-column text, in order) rather than just probing for each
    // `b` — a bare `| {b} ` match would be satisfied by the leading `a = 1`
    // column even if that `b` were missing or misordered.
    let formatted = output.pretty_results();
    let data_rows: Vec<&str> = formatted
        .lines()
        .filter(|line| line.starts_with("| 1 |"))
        .collect();
    assert_eq!(
        data_rows,
        vec![
            "| 1 | 0 |",
            "| 1 | 1 |",
            "| 1 | 2 |",
            "| 1 | 3 |",
            "| 1 | 4 |",
        ],
        "output must be exactly (a=1, b=0..=4) in ascending secondary order; got:\n{formatted}",
    );

    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert!(
        pruned >= 1,
        "with the leading key tied everywhere, the secondary key must \
         drive RG reorder + pruning; pruned={pruned}\n{}",
        output.description(),
    );
}

/// Build the #24352 fixture: four 2048-row row groups where the filter column
/// (`search_phrase`) differs from the sort column (`event_time`), and one row
/// group (the second) has an empty post-predicate selection invisible to
/// statistics — its only small `event_time` (50) sits on the row whose
/// `search_phrase` is `''`.
///
///   RG 0: event_time = i*1000                       (i in 0..2048)
///   RG 1: i=2048 -> (50, ''), else (20000+i, 'p'||i) (i in 2048..4096)
///   RG 2: event_time = 100 + (i-4096)               (i in 4096..6144)
///   RG 3: event_time = 5000 + (i-6144)              (i in 6144..8192)
fn build_q26_batches(schema: &Arc<Schema>) -> Vec<RecordBatch> {
    (0..4i64)
        .map(|rg| {
            let mut event_time = Vec::with_capacity(2048);
            let mut search_phrase: Vec<String> = Vec::with_capacity(2048);
            for j in 0..2048i64 {
                let i = rg * 2048 + j;
                let (et, sp) = if i < 2048 {
                    (i * 1000, format!("p{i}"))
                } else if i < 4096 {
                    if i == 2048 {
                        (50, String::new())
                    } else {
                        (20000 + i, format!("p{i}"))
                    }
                } else if i < 6144 {
                    (100 + (i - 4096), format!("p{i}"))
                } else {
                    (5000 + (i - 6144), format!("p{i}"))
                };
                event_time.push(et);
                search_phrase.push(sp);
            }
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int64Array::from(event_time)) as ArrayRef,
                    Arc::new(StringArray::from(search_phrase)) as ArrayRef,
                ],
            )
            .unwrap()
        })
        .collect()
}

/// Per-RG `fully_matched` `RowFilter` skip optimization.
///
/// Stats prove that every row of a fully-matched row group satisfies the
/// pushdown predicate, so the parquet decoder can skip the per-row
/// `RowFilter` for that RG entirely. The stream rebuilds the decoder at
/// the boundary with an empty `RowFilter` and toggles back to the real
/// one at the next non-fully-matched RG.
///
/// Layout: 4 RGs of 3 values each. Predicate `v >= 3 AND v <= 10` makes
/// RG 0 a straddler (1, 2 fail the lower bound), RGs 1..=2 fully matched
/// (every value in [3, 10] by stats), and RG 3 a straddler again (11, 12
/// fail the upper bound). This exercises the full toggle lifecycle:
/// filter ON (RG 0) → OFF across the fully-matched run (RGs 1..=2) → back
/// ON (RG 3), covering both the fully-matched → non-fully-matched and the
/// reverse transition.
///
/// Expected behavior:
/// - the static prune marks RGs 1..=2 as fully_matched at file open;
/// - the stream installs the real `RowFilter` initially (RG 0 not fm);
/// - at the RG 0 → RG 1 boundary the toggle rebuilds with an empty filter
///   and bumps `row_filter_skipped_fully_matched`;
/// - at the RG 2 → RG 3 boundary the toggle reinstalls the real filter, so
///   11 and 12 are correctly excluded;
/// - the query result is identical to running with the filter on.
#[tokio::test]
async fn fully_matched_rgs_skip_row_filter() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    // 4 RGs of 3 rows each. Predicate `v >= 3 AND v <= 10`:
    //   RG 0: 1, 2, 3   ← keeps {3}; min=1,max=3 → straddler, filter ON
    //   RG 1: 4, 5, 6   ← all in [3,10] → fully matched, filter OFF
    //   RG 2: 7, 8, 9   ← fully matched, filter OFF
    //   RG 3: 10,11,12  ← keeps {10}; 11,12 fail v<=10 → straddler, filter back ON
    let groups: [[i64; 3]; 4] = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]];
    let batches: Vec<RecordBatch> = groups
        .iter()
        .map(|vals| {
            let col: ArrayRef = Arc::new(Int64Array::from(vals.to_vec()));
            RecordBatch::try_new(Arc::clone(&schema), vec![col]).unwrap()
        })
        .collect();

    let mut ctx = ContextWithParquet::with_custom_data(
        Scenario::Int,
        RowGroup(3),
        Arc::clone(&schema),
        batches,
    )
    .await;

    let output = ctx
        .query("SELECT v FROM t WHERE v >= 3 AND v <= 10 ORDER BY v ASC")
        .await;

    // Correctness: every value in [3, 10], ascending.
    let expected_rows: Vec<i64> = (3..=10).collect();
    assert_eq!(output.result_rows, expected_rows.len());
    let formatted = output.pretty_results();
    for v in expected_rows {
        assert!(
            formatted.contains(&format!("| {v} ")),
            "output must contain {v}; got:\n{formatted}",
        );
    }
    // The RG 2 → RG 3 transition (fully-matched → non-fully-matched) must
    // reinstall the real filter, so 11 and 12 are filtered out. If the
    // toggle failed to restore the filter they would leak through.
    for v in [11i64, 12] {
        assert!(
            !formatted.contains(&format!("| {v} ")),
            "value {v} must be filtered out by the reinstalled RowFilter; \
             got:\n{formatted}",
        );
    }

    // Behavior: the per-RG `RowFilter` toggle must have fired at least
    // once when transitioning from RG 0 (not fm) into the fully-matched
    // run RGs 1..=2.
    let skipped = output
        .metric_value("row_filter_skipped_fully_matched")
        .unwrap_or(0);
    assert!(
        skipped >= 1,
        "row_filter_skipped_fully_matched must fire at least once; \
         skipped={skipped}\n{}",
        output.description(),
    );
}

/// Regression for #24352: with `pushdown_filters` + TopK dynamic filter, a row
/// group whose post-predicate selection is empty is silently finished by
/// arrow-rs without handing back a reader. Before `rg_plan` was synced to the
/// decoder frontier (`peek_next_row_group`), it trailed the decoder by one, so
/// a later runtime prune rebuilt the decoder from a stale plan and re-read an
/// already-delivered row group — the duplicate rows displaced the true top-k.
#[tokio::test]
async fn topk_pushdown_does_not_reread_delivered_row_group() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Int64, false),
        Field::new("search_phrase", DataType::Utf8, false),
    ]));
    let batches = build_q26_batches(&schema);

    // `RowGroup(2048)` writes one row group per 2048-row batch (4 RGs) and
    // enables `pushdown_filters`, required for the dynamic filter to reach the
    // parquet scan. Page-index reading is disabled: this test exercises the
    // #24352 empty-row-group / rg_plan-sync path, which is row-filter-driven and
    // does not need the page index. With the page index on, `search_phrase <> ''`
    // produces an intra-row-group `RowSelection`, and #24355 disables the runtime
    // pruner whenever a row selection is present — which would stop this test
    // from exercising the dynamic pruner at all.
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.enable_page_index = false;
    let mut ctx = ContextWithParquet::with_config(
        Scenario::Int,
        RowGroup(2048),
        config,
        Some(Arc::clone(&schema)),
        Some(batches),
    )
    .await;

    let output = ctx
        .query(
            "SELECT search_phrase FROM t \
             WHERE search_phrase <> '' ORDER BY event_time LIMIT 10",
        )
        .await;

    // `search_phrase` is unique per row, so any repeated value is the same
    // source row emitted twice. The correct answer is the 10 smallest-
    // `event_time` non-empty phrases, matching DuckDB / pushdown-off.
    assert_eq!(output.result_rows, 10, "{}", output.description());

    // The test must actually exercise the runtime prune/rebuild path that
    // caused #24352 (not just a happy-path scan), otherwise a future default or
    // optimizer change could let it pass without the bug's precondition. Assert
    // the dynamic filter pruned at least one row group.
    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert!(
        pruned >= 1,
        "test must exercise dynamic RG pruning (the #24352 path); pruned={pruned}\n{}",
        output.description(),
    );

    let formatted = output.pretty_results();
    for p in [
        "p0", "p4096", "p4097", "p4098", "p4099", "p4100", "p4101", "p4102", "p4103",
        "p4104",
    ] {
        assert!(
            formatted.contains(&format!("| {p} ")),
            "missing {p} from top-k; got:\n{formatted}",
        );
    }
    // The bug emitted p4096 twice (and dropped p4101..=p4104); assert no dup.
    assert_eq!(
        formatted.matches("| p4096 ").count(),
        1,
        "p4096 emitted more than once — rg_plan/decoder desync; got:\n{formatted}",
    );
}
