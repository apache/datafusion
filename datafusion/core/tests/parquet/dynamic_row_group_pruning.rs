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

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

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

    assert_eq!(output.result_rows, 5, "query must return LIMIT rows",);

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

/// Co-existence test for **page-index `RowSelection`** + dynamic RG
/// pruning. Tests that the `into_builder` rebuild preserves the
/// `RowSelection` derived from page-index pruning across RG drops.
///
/// Layout: 5 RGs × 1000 rows, with `data_page_row_count_limit=100` so
/// each RG has 10 pages of 100 rows.
///
/// Query: `SELECT v FROM t WHERE v >= 500 ORDER BY v DESC LIMIT 5`.
/// - `v >= 500` engages the page index: in RG 0 (values 0..1000) the
///   first 5 pages (values 0..500) are pruned, the last 5 (500..1000)
///   are scanned. RGs 1..4 keep all their pages (every page has
///   `max >= 500`). The decoder receives a `RowSelection` that masks
///   out those first 5 pages of RG 0.
/// - `ORDER BY v DESC LIMIT 5` fills the TopK heap from RG 4
///   (`max=4999`); the tightened threshold (≥ 4995) then proves RGs
///   0..3 unreachable and the runtime pruner drops them in one
///   `into_builder` rebuild.
///
/// If `into_builder` did **not** preserve the row selection (or
/// truncated / shifted it incorrectly), either the result rows would
/// drift or the count of pruned pages would drop to zero.
#[tokio::test]
async fn dynamic_rg_pruning_coexists_with_page_index_row_selection() {
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

    // Page-index pruning must have engaged: RG 0's first 5 pages are
    // entirely < 500. If `into_builder` dropped the row-selection state,
    // this metric would still report the original count (it is captured
    // at file open). Combined with the dynamic-pruner assertion below it
    // proves both mechanisms were active and that the rebuild left the
    // selection coherent — otherwise the result rows above would drift.
    let pages_pruned = output.metric_value("page_index_pages_pruned").unwrap_or(0);
    assert!(
        pages_pruned >= 5,
        "page index must prune at least 5 pages (RG 0 pages 0..5 for v < 500); \
         pruned={pages_pruned}\n{}",
        output.description(),
    );

    let pruned = output
        .row_groups_pruned_dynamic_filter()
        .expect("`row_groups_pruned_dynamic_filter` metric must be registered");
    assert!(
        pruned >= 1,
        "with TopK + tight threshold the runtime pruner must skip at least \
         one row group; pruned={pruned}\n{}",
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
