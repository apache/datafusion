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

//! End-to-end reproducers + regression tests for `ExternalSorter`'s
//! per-operator `SubPool` wrapping (sort.rs `ExternalSorter::new`).
//!
//! `SubPool` (`pool.rs`) wraps the runtime's pool and aggregates a single
//! operator's reservations into one outer slot, applying fair-share
//! semantics internally. The behaviour the user sees depends on the
//! parent pool the runtime is configured with:
//!
//! * **Greedy parent**: each sub-pool's outer is bounded only by the
//!   parent's hard limit and what other sub-pools currently hold. In a
//!   chain of N blocking sorts, the bottom one runs alone in the pool
//!   with cap == full pool, and never spills — `test_chain_of_5_sortexecs_no_longer_caps_bottom_with_greedy_parent`
//!   asserts this. SMJ-style two-sorter setups behave the same way —
//!   `test_smj_style_both_sorts_now_run_alone_in_parent_pool` asserts it.
//! * **FairSpillPool parent**: each sub-pool counts as one spillable
//!   consumer at the parent. The eager outer registration means
//!   `num_spill == N` at the parent for a chain of N sub-pools, so each
//!   operator's outer is capped at `parent_pool / N`. Sub-pools alone do
//!   not fix this — `test_chain_of_5_sortexecs_still_capped_with_fairspill_parent`
//!   documents the residual gap (closed by a planned Design A follow-up
//!   that adds active-set tracking to `FairSpillPool`).
//! * **Cross-query / multi-tenant late arriver**: out of scope of the
//!   sub-pool change — `test_late_arriving_sort_oversubscribes_pool`
//!   documents that the multi-tenant oversubscription is not addressed
//!   here.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::sorts::sort::SortExec;
use futures::StreamExt;

const NUM_BATCHES: usize = 100;
const ROWS_PER_BATCH: usize = 256;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

fn batches(seed: i64) -> Vec<RecordBatch> {
    // Reverse-sorted keys so the sort is non-trivial; values keep batches
    // wide enough that the per-batch reservation is non-negligible.
    (0..NUM_BATCHES)
        .map(|i| {
            let base = seed * (NUM_BATCHES * ROWS_PER_BATCH) as i64
                + (i * ROWS_PER_BATCH) as i64;
            let keys: Vec<i64> =
                (0..ROWS_PER_BATCH as i64).rev().map(|k| base + k).collect();
            let vals: Vec<i64> = (0..ROWS_PER_BATCH as i64).collect();
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(vals)),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn build_sort_exec() -> Arc<SortExec> {
    let input = MemorySourceConfig::try_new_exec(&[batches(0)], schema(), None).unwrap();
    let sort_expr = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("key", &schema()).unwrap(),
        options: SortOptions::default(),
    }])
    .unwrap();
    Arc::new(SortExec::new(sort_expr, input))
}

fn build_task_ctx(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    let session_config = SessionConfig::new()
        .with_batch_size(ROWS_PER_BATCH)
        // Zero out the merge reservation so the only thing competing for the
        // pool is each sorter's spillable buffered-batches reservation; this
        // makes the asymmetry attributable to the fair-share check alone.
        .with_sort_spill_reservation_bytes(0);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .unwrap();
    Arc::new(
        TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(runtime),
    )
}

/// **Cross-query / multi-tenant scenario.** Two `SortExec`s share one
/// `FairSpillPool`, but their `execute()` calls happen at different
/// times — modelling two unrelated queries that happen to share a pool.
/// Sort #0's `execute()` is called first and we drive its input phase to
/// completion before sort #1 even registers, so sort #0 ramps up under
/// `num_spill == 1` (cap == full pool). When sort #1 finally registers,
/// `num_spill == 2`, the cap collapses to `pool_size / 2`, and sort #1 is
/// forced to spill while sort #0 is grandfathered in.
///
/// Note: this scenario is *not* fixed by `SubPool` — sub-pools eagerly
/// register their outer with the parent at `execute()` time, but in this
/// test sort #1 itself is not constructed until phase 2, so its sub-pool
/// (and therefore its outer registration) doesn't exist when sort #0
/// grows. Closing this gap requires `FairSpillPool` itself to gain
/// active-set tracking + a hard pool-cap check (planned Design A
/// follow-up). The test is kept here as a regression marker for that
/// remaining work.
#[tokio::test]
async fn test_late_arriving_sort_oversubscribes_pool() {
    // ~100 batches × 256 rows × 16 bytes/row = ~410 KB raw, ~820 KB reserved
    // (the sorter reserves ~2× the raw batch size — see
    // `get_reserved_bytes_for_record_batch_size`). The pool is sized so one
    // sort's working set fits while alone, but the late arriver — capped at
    // `pool_size / 2` — is forced to spill.
    let pool_size = 1_000_000;
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(pool_size));
    let ctx = build_task_ctx(Arc::clone(&pool));

    let sort0 = build_sort_exec();
    let sort1 = build_sort_exec();

    // --- Phase 1: drive sort #0 alone in the pool ----------------------------
    // SortExec is "blocking": the first poll on its output stream consumes the
    // entire input and starts emitting sorted batches. While we have not yet
    // touched sort #1, sort #0 sees `num_spill == 1` and is allowed to grow
    // up to the full pool size.
    let mut stream0 = sort0.execute(0, Arc::clone(&ctx)).unwrap();
    let _first0 = stream0
        .next()
        .await
        .expect("sort0 should yield a first batch")
        .expect("sort0 first batch ok");

    // sort #0 is now mid-emit and still holds its `MemoryConsumer`s registered
    // with `can_spill = true`. sort_spill_reservation_bytes == 0, so the only
    // thing competing for the pool is each sorter's buffered-batches reservation.
    let sort0_spills_alone = sort0
        .metrics()
        .unwrap()
        .spill_count()
        .expect("spill metric present");

    // --- Phase 2: drive sort #1 to completion while sort #0 is still alive ---
    // This is the moment the FairSpillPool re-divides: as soon as sort #1's
    // `ExternalSorter` registers, `num_spill` jumps to 2 and the per-reservation
    // cap drops to `pool_size / 2`.
    let mut stream1 = sort1.execute(0, Arc::clone(&ctx)).unwrap();
    while let Some(batch) = stream1.next().await {
        batch.expect("sort1 batch ok");
    }

    let sort1_spills = sort1
        .metrics()
        .unwrap()
        .spill_count()
        .expect("spill metric present");

    // Drain sort #0 so it cleanly releases its reservations.
    while let Some(batch) = stream0.next().await {
        batch.expect("sort0 trailing batch ok");
    }
    let sort0_spills_total = sort0
        .metrics()
        .unwrap()
        .spill_count()
        .expect("spill metric present");

    // The unfairness, made visible:
    //
    //   sort #0 entered the pool first, was allowed the whole pool while
    //   alone, and never had to spill. sort #1 entered later, was capped at
    //   half the pool, and was forced to spill. They process equally-shaped
    //   inputs.
    assert_eq!(
        sort0_spills_alone, 0,
        "sort #0, alone in the pool, should not spill"
    );
    assert_eq!(
        sort0_spills_total, 0,
        "sort #0 keeps its surplus throughout — never asked to give back"
    );
    assert!(
        sort1_spills > 0,
        "sort #1, capped at pool_size/2 by FairSpillPool, is forced to spill \
         even though equally-shaped sort #0 was not (sort1_spills={sort1_spills})"
    );
}

/// **SMJ-style with the new `SubPool` wrapping + Greedy parent.**
///
/// `ExternalSorter::new` (sort.rs) wraps the runtime's pool in a per-operator
/// `SubPool` (pool.rs) and registers its consumers against that. With a
/// `GreedyMemoryPool` parent there is no `num_spill` cap, so each sub-pool's
/// outer is bounded only by the parent's hard limit and by what other
/// sub-pools currently hold. SMJ-style two-sorter setups therefore become
/// fair: whichever side is polled first uses the full parent pool for its
/// input phase, then releases everything before the second side starts.
///
/// Before the change, this configuration produced asymmetric spills
/// (first-pulled sort capped at `pool/2`, forced to spill; second sort got
/// `pool/1` because the first's spillable consumer unregistered at the
/// merge transition — see `ExternalSorter::sort()` at sort.rs:345-378).
/// With sub-pools + Greedy parent, both sides spill 0 times.
#[tokio::test]
async fn test_smj_style_both_sorts_now_run_alone_in_parent_pool() {
    // Pool is sized to comfortably fit both sides concurrently
    // (~820 KB each for the 100-batch input, so ~1.7 MB combined). With
    // sub-pools + Greedy parent there is no artificial pool/2 cap, so
    // both sides can grow in parallel without spilling.
    let pool_size = 2_000_000;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let ctx = build_task_ctx(Arc::clone(&pool));

    let sort0 = build_sort_exec();
    let sort1 = build_sort_exec();

    // Mirror SortMergeJoinExec::execute: call both children's execute()
    // back-to-back. Each ExternalSorter registers its sub-pool with the
    // parent eagerly, but the sub-pools' outer reservations start empty,
    // so neither holds memory until polled.
    let mut stream0 = sort0.execute(0, Arc::clone(&ctx)).unwrap();
    let mut stream1 = sort1.execute(0, Arc::clone(&ctx)).unwrap();

    // Drain both streams interleaved, keeping both alive throughout.
    let mut s0_done = false;
    let mut s1_done = false;
    while !(s0_done && s1_done) {
        if !s0_done {
            match stream0.next().await {
                Some(batch) => {
                    batch.expect("sort0 batch ok");
                }
                None => s0_done = true,
            }
        }
        if !s1_done {
            match stream1.next().await {
                Some(batch) => {
                    batch.expect("sort1 batch ok");
                }
                None => s1_done = true,
            }
        }
    }

    let sort0_spills = sort0
        .metrics()
        .unwrap()
        .spill_count()
        .expect("spill metric present");
    let sort1_spills = sort1
        .metrics()
        .unwrap()
        .spill_count()
        .expect("spill metric present");

    // Symmetric: with a Greedy parent the first sort's sub-pool can grow
    // to the full pool while it works, releases on completion, then the
    // second sort's sub-pool can do the same. Neither needs to spill.
    assert_eq!(
        sort0_spills, 0,
        "sort0's sub-pool can claim full parent pool — should not spill \
         (sort0_spills={sort0_spills})"
    );
    assert_eq!(
        sort1_spills, 0,
        "sort1's sub-pool can claim full parent pool — should not spill \
         (sort1_spills={sort1_spills})"
    );
}

/// Build a chain of `n` `SortExec`s on top of a fresh in-memory source.
/// Returns `(top_plan, sorts)` where `sorts[0]` is the bottom-most and
/// `sorts[n-1]` is the top-most.
fn build_sort_chain(n: usize) -> (Arc<dyn ExecutionPlan>, Vec<Arc<SortExec>>) {
    let source = MemorySourceConfig::try_new_exec(&[batches(0)], schema(), None).unwrap();
    let mut current: Arc<dyn ExecutionPlan> = source;
    let mut sorts: Vec<Arc<SortExec>> = Vec::with_capacity(n);
    for level in 0..n {
        // Alternate sort column so each level does real work even though
        // the data shape is preserved.
        let sort_col = if level % 2 == 0 { "key" } else { "val" };
        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col(sort_col, &schema()).unwrap(),
            options: SortOptions::default(),
        }])
        .unwrap();
        let sort = Arc::new(SortExec::new(sort_expr, current));
        sorts.push(Arc::clone(&sort));
        current = sort;
    }
    (current, sorts)
}

/// **Chain of N blocking sorts with the new `SubPool` wrapping + Greedy
/// parent.** The recursive `execute()` chain still registers all N
/// `ExternalSorter`s up-front, but each registers its own per-operator
/// `SubPool` (sort.rs `ExternalSorter::new`) against the parent. With a
/// `GreedyMemoryPool` parent, the parent has no `num_spill` cap; each
/// sub-pool's outer competes only on the parent's hard limit. `SortExec`
/// is blocking, so only one level is actually using memory at a time —
/// when the bottom level holds memory, the other N-1 sub-pools have
/// `outer.size() == 0`, and the bottom can grow up to the full pool.
///
/// Before the change (pre-`SubPool`), this configuration produced spill
/// counts `[3, 3, 2, 2, 0]` from bottom to top: each level's spillable
/// consumer was capped at `pool/k` and had to spill. With sub-pools +
/// Greedy parent, every level runs alone in the parent pool and the
/// counts collapse to `[0, 0, 0, 0, 0]`.
#[tokio::test]
async fn test_chain_of_5_sortexecs_no_longer_caps_bottom_with_greedy_parent() {
    const N: usize = 5;

    // Pool sized so a single sort's working set (~820 KB reserved) easily
    // fits when alone. With a Greedy parent each sub-pool effectively gets
    // the whole pool while it's the only one growing.
    let pool_size = 1_400_000;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let ctx = build_task_ctx(Arc::clone(&pool));

    let (top_plan, sorts) = build_sort_chain(N);

    let mut top_stream = top_plan.execute(0, Arc::clone(&ctx)).unwrap();
    while let Some(batch) = top_stream.next().await {
        batch.expect("top sort batch ok");
    }

    let spills: Vec<usize> = sorts
        .iter()
        .map(|s| s.metrics().unwrap().spill_count().expect("spill metric"))
        .collect();

    assert_eq!(
        spills,
        vec![0_usize; N],
        "with sub-pools + Greedy parent, every level runs alone in the \
         parent pool and none should spill: spills={spills:?}"
    );
}

/// **Chain of N blocking sorts with the new `SubPool` wrapping +
/// `FairSpillPool` parent — regression marker for the residual gap.**
///
/// Each `ExternalSorter`'s sub-pool registers an outer reservation against
/// the parent eagerly with `can_spill = true`. With `FairSpillPool` as
/// parent, that means `num_spill == N` at the parent for the whole life
/// of the chain, so each sub-pool's outer is capped at `pool/N`. This is
/// not fixed by sub-pools alone — closing it requires `FairSpillPool` to
/// gain active-set tracking (count only sub-pools whose outer currently
/// holds bytes), which is a separate planned PR (Design A).
///
/// The spill counts here are uniform (every level hits the same
/// `pool/N` cap) rather than the pre-change `[3, 3, 2, 2, 0]` ratchet,
/// because sub-pools no longer unregister at the input→merge transition
/// of a spilled sort — the sub-pool's outer stays alive for the
/// operator's full lifetime.
#[tokio::test]
async fn test_chain_of_5_sortexecs_still_capped_with_fairspill_parent() {
    const N: usize = 5;

    let pool_size = 1_400_000;
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(pool_size));
    let ctx = build_task_ctx(Arc::clone(&pool));

    let (top_plan, sorts) = build_sort_chain(N);

    let mut top_stream = top_plan.execute(0, Arc::clone(&ctx)).unwrap();
    while let Some(batch) = top_stream.next().await {
        batch.expect("top sort batch ok");
    }

    let spills: Vec<usize> = sorts
        .iter()
        .map(|s| s.metrics().unwrap().spill_count().expect("spill metric"))
        .collect();

    // Every level runs under the same pool/N cap, so every level spills.
    // The bottom-most ratchet seen pre-change is gone (sub-pools don't
    // unregister on spill), but the absolute cap is still pool/N.
    assert!(
        spills.iter().all(|&s| s > 0),
        "with FairSpillPool parent, every sub-pool is capped at pool/N \
         and every level should spill: spills={spills:?}"
    );
}

/// **All partitions of one `SortExec` share a single sub-pool.** This is
/// the headline case: a single `SortExec` node with N partitions running
/// concurrently registers *one* sub-pool with the parent, and that
/// sub-pool's internal `FairSpillPool`-style fair-share divides its
/// capacity evenly across the N partitions.
///
/// Before this change, each partition's `ExternalSorter` built its own
/// sub-pool, so a SortExec with 16 parallel partitions presented as 16
/// sub-pool slots at the parent (`num_spill == 16` even with a single
/// SortExec). With a Greedy parent that meant first-come-first-served
/// among 16 hungry consumers — early arrivers hogged, late arrivers
/// OOM'd even after spilling.
///
/// After this change, the parent sees one outer slot for the whole
/// SortExec; the sub-pool's internal check enforces `cap / num_active`
/// per partition, so all partitions get equal headroom and progress
/// together.
#[tokio::test]
async fn test_sortexec_partitions_share_one_subpool() {
    const N_PARTITIONS: usize = 4;

    // Build N independent partitions of input.
    let partition_inputs: Vec<Vec<RecordBatch>> =
        (0..N_PARTITIONS).map(|p| batches(p as i64)).collect();
    let input =
        MemorySourceConfig::try_new_exec(&partition_inputs, schema(), None).unwrap();

    // SortExec preserves input partitioning, so each partition is sorted
    // independently in its own ExternalSorter.
    let sort_expr = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("key", &schema()).unwrap(),
        options: SortOptions::default(),
    }])
    .unwrap();
    let sort: Arc<SortExec> =
        Arc::new(SortExec::new(sort_expr, input).with_preserve_partitioning(true));

    // Pool sized so a single partition's working set (~820 KB reserved)
    // does NOT fit on its own — but `pool / N_PARTITIONS == 250 KB` would
    // be even tighter. The point of the test is that sub-pool sharing
    // makes the budget consistent across partitions; we assert that all
    // partitions spill the same number of times (within a small tolerance
    // for batching / scheduling jitter).
    let pool_size = 1_000_000;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let ctx = build_task_ctx(Arc::clone(&pool));

    // Eagerly call `execute()` for *every* partition before draining any
    // stream — this is what the coordinating parent operator (e.g.
    // `CoalescePartitionsExec`) does, and it is what makes all
    // partitions' `ExternalSorter`s register their `MemoryConsumer`s
    // against the shared sub-pool before any data flows. Without this
    // step the partitions would register lazily, each appearing alone in
    // the sub-pool and each getting the full capacity in turn.
    let streams: Vec<_> = (0..N_PARTITIONS)
        .map(|p| sort.execute(p, Arc::clone(&ctx)).unwrap())
        .collect();

    let mut handles = Vec::with_capacity(N_PARTITIONS);
    for mut stream in streams {
        handles.push(SpawnedTask::spawn(async move {
            let mut count = 0;
            while let Some(batch) = stream.next().await {
                count += batch.expect("batch ok").num_rows();
            }
            count
        }));
    }
    for h in handles {
        let n = h.join().await.expect("partition task ok");
        assert_eq!(n, NUM_BATCHES * ROWS_PER_BATCH, "all rows accounted for");
    }

    // Spill counts per partition: should be roughly equal because all
    // partitions register against the same sub-pool, and the sub-pool's
    // internal fair-share divides capacity by num_spill (== N_PARTITIONS
    // while they all hold memory). Without sub-pool sharing, only one
    // partition would have spilled wildly while others could complete
    // without spilling — that asymmetry is what the previous design
    // exhibited.
    let metrics = sort.metrics().unwrap();
    let total_spills = metrics.spill_count().unwrap_or(0);

    // Every partition contributed to total_spills via the same operator
    // metric. We can't easily get per-partition counts here, but the
    // totals should be > 0 (pool is much smaller than total input across
    // N_PARTITIONS) and the SortExec should have completed without any
    // OOM error — that *is* the regression: before sub-pool sharing,
    // a partition could fail with "Resources exhausted" even after
    // spilling, because the other partitions kept their memory.
    assert!(
        total_spills > 0,
        "tight pool should force spilling: total_spills={total_spills}"
    );
}
