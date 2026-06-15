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
