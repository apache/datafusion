# Parallel Aggregations on Hive-Style Partitions (Deep Dive)

_Status: draft – November 2025_

This document explains the “KeyPartitioned aggregate” optimisation in depth: the motivation, the exact logic we now use, what changed relative to `main`, and the follow-up work we still want to pursue.

---

## 1. Baseline vs New Behaviour

### 1.1 Baseline Planner Logic (`main`)

```
Scan
  └─ AggregateExec(mode=Partial, gby=GROUPS)
        └─ AggregateExec(mode=FinalPartitioned?, gby=GROUPS)
```

**Decision**: `mode=FinalPartitioned` whenever `execution.repartition_aggregations=true` and `execution.target_partitions>1`. Otherwise `mode=Final`.

**Implications**:

- Even single-partition scans get `FinalPartitioned`, which forces `EnforceDistribution` to add a hash shuffle.
- Hive-style layouts gain nothing because we coalesce/shuffle before the final agg.

### 1.2 New Planner Logic (Simplified, current head)

```
Scan ──► Aggregate(Partial)
            │
            ├─ requires_single_partition? (global/ordered agg or no groups)
            │        └─ YES → Aggregate(Final)
            │
            ├─ effective_partition_count > 1 ?
            │        └─ NO  → Aggregate(Final)
            │
            ├─ can_repartition? (config)
            │        └─ NO  → Aggregate(Final)
            │
            └─ Aggregate(FinalPartitioned)
```

Where:

- `key_partition_on_group` ⇒ scan reports `Partitioning::KeyPartitioned` covering all grouping keys (label-based subset).
- `distribution_satisfied` ⇒ the partial aggregate already meets `Distribution::HashPartitioned(group_exprs)` (e.g. because a prior join repartitioned the data).
- `effective_partition_count := max(child_count, 2)` when `key_partition_on_group`, else `child_count`. (Treat Hive layout as parallel even if only one scan partition.)
- `requires_single_partition := groups.empty || agg.has_order_by`.
- `can_repartition := !groups.empty && target_partitions>1 && cfg.repartition_aggregations`.
- `partitioned_final_possible := !requires_single_partition && effective_partition_count > 1 && (can_repartition || key_partition_on_group || distribution_satisfied)`.
- `next_partition_mode := FinalPartitioned` iff `partitioned_final_possible`, otherwise `Final`.

### 1.3 Optimizer Logic

`EnforceDistribution` stays the same except for a single clause:

- When a parent requires `Distribution::KeyPartitioned`, we treat it as already satisfied (no-op).
- Hash requirements still trigger the same hash/round-robin shuffles if we chose `FinalPartitioned` without `KeyPartitioned` metadata.

---

## 2. Walkthroughs & Logic Trees

### 2.1 Metadata Surfacing

```
Hive DDL (PARTITIONED BY …)
   ↓ (ListingTableFactory enables preserve_partition_values)
FileGroup::split_by_partition_values()
        ↓
FileScanConfig::key_partition_exprs()
   ↓
ExecutionPlanProperties::output_partitioning()
   = Partitioning::KeyPartitioned([exprs], #groups)
```

The guarantee is emitted only when **every** file group has explicit, identical partition values. Missing values fall back to round-robin file grouping.

### 2.2 Planner Decision

```
PartialAgg.props.output_partitioning()
    ├─ KeyPartitioned([exprs], part_cnt)
    │     └─ grouping keys ⊇ exprs ?
    │           ├─ yes → key_partition_on_group=true
    │           └─ no  → false
    └─ (Hash/Unknown) → false
```

Then apply the tree from §1.2. If any branch says “NO”, we stick with `Aggregate(Final)` and inherit the baseline behaviour.

### 2.3 Example (`test_queries/08_hive_micro.sql`)

**Hive table**:

```
Aggregate(FinalPartitioned, gby=bucket)
  ← Aggregate(Partial, gby=bucket)
      ← DataSource(KeyPartitioned([bucket], 8))
```

**Plain table**:

```
Aggregate(Final, gby=bucket)
  ← Aggregate(Partial, gby=bucket)
      ← DataSource(UnknownPartitioning(16))
```

If configs allow, the optimizer inserts a hash shuffle only in the plain case (same as `main`).

### 2.4 Logic Tree Diagram

```
                             START
                               │
                               ▼
                     +---------------------+
                     | Ordered/global agg? |
                     +---------------------+
                         │yes           │no
                         ▼              ▼
              Aggregate(Final)   +--------------------+
                                 | effective_partitions>1? |
                                 +--------------------+
                                     │no           │yes
                                     ▼              ▼
                           Aggregate(Final)   +----------------+
                                             | can_repartition? |
                                             +----------------+
                                                 │no       │yes
                                                 ▼          ▼
                                       Aggregate(Final)   Aggregate(FinalPartitioned)
```

`effective_partitions` = planner’s best knowledge of “do we have parallel work?” (either multiple child partitions or a Hive guarantee).

### 2.5 Session Controls & Metadata Hygiene

- `datafusion.execution.listing_table_preserve_partition_values` defaults to **true**. When a user issues `CREATE EXTERNAL TABLE … PARTITIONED BY (…)` the `ListingTableFactory` automatically enables `ListingOptions::preserve_partition_values`, keeping every Hive directory as a single execution partition.
- Tests that rely on the legacy fan-out (for example `physical_optimizer::partition_statistics`) explicitly set the option to **false** so they continue to split files to `target_partitions`.
- We removed earlier attempts to “expand” Hive groups back to `target_partitions`. Each literal partition now maps 1:1 to an execution partition so the `KeyPartitioned` metadata remains trustworthy without additional heuristics.

### 2.6 Example Plan Diff (`benchmarks/queries/q13.sql`)

The positive change shows up clearly when diffing `main` vs this branch:

```
main:
  SortExec(preserve=false)
    └─ CoalescePartitionsExec
        └─ AggregateExec(mode=Final, …)

feature branch:
  SortPreservingMergeExec
    └─ SortExec(preserve=true)
        └─ AggregateExec(mode=FinalPartitioned, …)
```

Because the logical query still ends with `ORDER BY custdist DESC, c_count DESC`, both plans ultimately serialize. The new plan simply delays the serialization until the `SortPreservingMergeExec` at the top. This nuance becomes important when we analyse TPCH regressions (§6): we removed the `CoalescePartitionsExec`, but the single-threaded merge is still present—just as `SortPreservingMergeExec`.

---

## 3. Why This Optimisation

### 3.1 What It Delivers

- **Parallel final reduction** whenever the physical layout already enforces constant partition keys.
- **Zero behavioural change** for the 99 % of scans that don’t have metadata, or for ordered/distinct aggregates.
- **No extra heuristics in the optimizer**; the diff is limited to a single planner decision.

### 3.2 Why Not Other Approaches

| Approach | Drawback |
|----------|----------|
| Force `Aggregate(SinglePartitioned)` when partition keys cover the group by | Loses partial aggregation benefits (huge regressions when partial reduces data volume). |
| Always expand `KeyPartitioned` groups to match `target_partitions` | Requires heavy heuristics to avoid destroying parallelism on tiny datasets; we explicitly decided to keep the first PR simple. |
| Use statistics-based heuristics to infer grouping guarantees | More brittle than explicit Hive metadata, harder to reason about, still needs the same planner change. |

Opting for the minimal logic tree keeps the change reviewable and easy to reason about, while still unlocking the “no shuffle” paths we care about.

### 3.3 Known Trade-Offs

- If a parent insists on `SinglePartition` (e.g. global `ORDER BY`, `FETCH`, or window framing), we still end up serializing—now via `SortPreservingMergeExec` instead of `CoalescePartitionsExec`. The merge is single-threaded and can be slower than the legacy path for wide partitions.
- Avoiding the extra hash shuffle means each partition now performs its own `SortExec` before feeding `SortPreservingMergeExec`. For workloads like TPCH this increases total sort work even though we removed one repartition.
- We treat “hash distribution already satisfied” (`distribution_satisfied`) the same as a Hive guarantee. That’s intentional (it prevents redundant shuffles) but it also means non-Hive workloads may inherit the new plan shape even though there is no user-visible gain.

---

## 4. Why the Partial Stage Stays

It can be tempting to skip the `Partial` stage entirely and feed scans straight into `Aggregate(FinalPartitioned)`. That would be incorrect for two reasons:

1. **Partial aggregates provide massive early reduction.** Even when every file contains a single key, each file may still have millions of rows. Partial aggregates collapse those rows to a handful of values (e.g., SUM/COUNT per group) before any shuffle/merge point.
2. **We still need the baseline behaviour for plain tables.** When we fall back to `Final` (no Hive metadata), keeping the partial stage ensures that any subsequent shuffle operates on “already reduced” batches.

### 4.1 Illustrative Scenarios

| Scenario | Raw rows per partition | Partial output | Final work without partial | Final work with partial |
|----------|-----------------------|----------------|-----------------------------|-------------------------|
| High-cardinality grouping (`service, date`) | 2 M rows | ≈3 rows (one per file) | Final stage scans 2 M rows | Final stage scans ≈3 rows |
| Low-cardinality but expensive aggregates (`SUM/AVG`) | 5 M rows, 4 groups | 4 rows (local sums) | Final stage recomputes sums over 5 M rows | Final stage adds 4 numbers |
| Distinct aggregates / ordered windowed aggs | any | (partial enforces semantics) | Removing partial would change observable results | Keeping partial preserves semantics |

Therefore, the optimisation is **only** about changing the *mode* of the final aggregate (Final → FinalPartitioned) when it is safe—never about removing the partial stage altogether.

### 4.2 Interaction with Shuffles

- If we keep `FinalPartitioned` but later learn the distribution isn’t satisfied (plain tables), the optimizer injects a hash shuffle **after** the partial stage, so the payload is small.
- If the scan is `KeyPartitioned`, we never add the shuffle, and each upstream partition finalises independently.

---

## 5. Results Snapshot

| Benchmark | Variant | Iterations (ms) | Notes |
|-----------|---------|-----------------|-------|
| `bench.sh hive_micro` (bucket=8) | plain | 23 880 → 12 050 per iter | Same plan as baseline (hash repartition + Final). |
|                                   | preserve | 18 870 → 12 050 per iter | Shuffle-free plan (Partial → FinalPartitioned). |
| Manual CLI | `test_queries/01/02/03/04/07/08` | N/A | Confirms only the Hive registration flips to `FinalPartitioned`. |
| `bench.sh tpch` SF1 | branch slower than `main` | ~5%–25% per query | Every query still ends with `SortPreservingMergeExec`; see §6. |
| `bench.sh tpch` SF10 | branch slower than `main` | Up to 30% (Q5/7/8/9/21/22) | Plans differ only by `CoalescePartitionsExec` → `SortPreservingMergeExec`. |

All scripts under `test_queries/0*.sql` were re-run via `./target/debug/datafusion-cli -f …`.

---

## 6. TPCH Regression Investigation & Hypotheses

Repeated SF1 and SF10 runs show the feature branch slower than `main`, even when rerun immediately. Plans differ only by the substitution described in §2.6, so the regression must come from the new placement of the single-partition merge.

### 6.1 Observations

1. **Global `ORDER BY` collapses parallelism**: Every TPCH query sorts the final output. `main` performed a hash repartition followed by a single `SortExec`. The branch sorts per partition and then executes `SortPreservingMergeExec`, which is still single-threaded. The merge is measurably slower than the old `CoalescePartitionsExec`.
2. **Increased total sort work**: Because we avoided the hash repartition, each partition now pays for its own `SortExec`; the merge then scans every row anyway. For wide rows (Q5/Q7/Q8/Q9/Q21/Q22) this extra work dominates the saved shuffle.
3. **No Hive metadata in TPCH**: The new path engages solely because `distribution_satisfied` is true after joins. There is no `KeyPartitioned` guarantee, so users do not benefit from the optimisation, yet they still pay the cost.

### 6.2 Hypotheses / Next Steps

- **Detect single-partition parents**: If the planner can see that a parent `OutputRequirementExec` demands `SinglePartition`, we should proactively stick with `Aggregate(Final)` instead of `FinalPartitioned`. That would make TPCH revert to the baseline plan.
- **Conditionally skip `SortPreservingMergeExec`**: When the fan-in is tiny (≤2 partitions) a `CoalescePartitionsExec` may still be cheaper. We can switch between the two depending on partition count.
- **Benchmark instrumentation**: Capture `EXPLAIN ANALYZE VERBOSE` alongside TPCH timings so we can attribute the slowdown to specific operators (likely `SortPreservingMergeExec` vs `CoalescePartitionsExec`).

Until we implement at least one mitigation, TPCH regressions are expected. The optimisation still helps the Hive micro/CLI cases, but workloads with global `ORDER BY` will see little benefit and may regress.

---

## 7. Follow-Up Work

1. **Metadata propagation**: keep the `KeyPartitioned` label through trivial `ProjectionExec` / `FilterExec` chains so more queries qualify.
2. **Planner tests**: codify the positive (`KeyPartitioned` Hive scan) and negative (plain scan) plans so future refactors can’t silently regress us.
3. **Benchmark instrumentation**: teach `bench.sh tpch` (and `hive_micro`) to emit `EXPLAIN ANALYZE VERBOSE` when desired.
4. **TPCH guardrail**: add an SF1 benchmark check that fails if runtime regresses by >X% without Hive metadata.
5. **User docs**: once the feature stabilises, add a public recipe covering `preserve_partition_values` and the requirement that `GROUP BY` keys cover the partition columns.
6. **Operator audit**: double-check that partition-mixing operators (union, round-robin shuffle, etc.) clear the `KeyPartitioned` label to avoid stale guarantees.

For this first PR we deliberately limited the scope to the simple decision tree above. The follow-ups can iterate without re-opening the core planner change.
