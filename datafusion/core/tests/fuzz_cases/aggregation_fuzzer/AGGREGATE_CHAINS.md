# Aggregate execution chains

Every valid way to wire `AggregateExec` operators into a plan, the stream each
stage resolves to in `execute_typed` (`datafusion/physical-plan/src/aggregates/mod.rs`),
and what the input and config must look like to reach it. All chains compute
the same logical aggregation and must produce identical results once the
output is sorted by the group keys.

TopK (`limit_options`) is covered with a limit larger than any possible group
count, so the TopK stream must return the complete aggregate.

## How an aggregate stage picks its order mode

Each `AggregateExec` computes its own `InputOrderMode` in `AggregateExec::try_new`
by comparing its group-by expressions against the ordering its input reports:

- **Linear**: the input ordering covers none of the group keys. Plain hash
  aggregation.
- **PartiallySorted**: the input ordering covers some of the group keys but not
  all. The stream can flush groups whenever the sorted prefix changes.
- **Sorted**: the input ordering covers all group keys. The stream emits each
  group as soon as the key changes and never holds more than one group.

Because this is per operator, the same source ordering can produce different
modes at different stages: the shuffle between stages either keeps the ordering
(order-preserving `RepartitionExec`, `SortPreservingMergeExec`) or destroys it
(plain `RepartitionExec`, `CoalescePartitionsExec`). A final stage can never be
more ordered than the partial stage feeding it, because a Linear stage reports
no output ordering.

Two kinds of rows flow through a chain:

- **raw rows**: source data. Consumed by `Partial`, `Single`, `SinglePartitioned`.
- **partial state rows**: intermediate accumulator state produced by `Partial`
  or `PartialReduce`. Consumed by `PartialReduce`, `Final`, `FinalPartitioned`.

## Preconditions for the dedicated streams

All rows below assume these hold. If any fails, every grouped stage falls back
to `GroupedHashAggregateStream` (see "Fallback chains").

- `datafusion.execution.enable_migration_aggregate = true` (default)
- exactly one grouping set (no `GROUPING SETS` / `CUBE` / `ROLLUP`)
- `limit_options = None`

## Memory behavior per stream

| Stream | Under memory pressure | Needs disk manager |
|---|---|---|
| `AggregateStream` (no grouping) | never allocates per group, nothing to do | no |
| `PartialHashAggregateStream` | emits current states downstream, resets table | no |
| `OrderedPartialAggregateStream` | emits current states downstream, resets table (on Sorted input too, since one group can outgrow the budget) | no |
| `PartialReduceHashAggregateStream` | emits current states downstream, resets table | no |
| `FinalHashAggregateStream` | sorts by full group key, spills, merges runs at end | yes |
| `SingleHashAggregateStream` | same as final | yes |
| `OrderedFinalAggregateStream`, Sorted input | streams groups out as key changes, cannot spill | no |
| `OrderedFinalAggregateStream`, PartiallySorted input | spills like final hash | yes |
| `OrderedSingleAggregateStream`, Sorted input | like ordered final with Sorted input | no |
| `OrderedSingleAggregateStream`, PartiallySorted input | spills like single hash | yes |
| `GroupedTopKAggregateStream` | bounded heap, never spills; with a limit above the group count it holds every group | no |
| `GroupedHashAggregateStream` (fallback) | `Partial` emits early: completed prefix groups on ordered input, or everything when there are none; `PartialReduce`, final and single modes spill on Linear or PartiallySorted input and report an error on Sorted input; spilling needs extra sort headroom, so it can fail under a tight pool where the dedicated streams succeed | yes except `Partial` |

`PartialHashAggregateStream` and the fallback in `Partial` mode also have the
skip-partial probe (`skip_partial_aggregation_probe_*` options): when
cardinality is high relative to input rows they stop aggregating and convert raw
rows straight into state rows. This changes what the final stage receives, not
the result. The ordered partial stream has no such probe.

---

## Chains: grouped aggregation

Chains are written bottom to top with `→`.

### Single stage

| Case | Chain | Stream | Input requirements |
|---|---|---|---|
| `single, unordered` | `Single`, Linear | `SingleHashAggregateStream` | raw, 1 partition, no ordering on group keys |
| `single, sorted by all keys` | `Single`, Sorted | `OrderedSingleAggregateStream` | raw, 1 partition, ordered by all group keys |
| `single, sorted by first key` | `Single`, PartiallySorted | `OrderedSingleAggregateStream` with spill | raw, 1 partition, ordered by a subset of the group keys |
| `single_partitioned, unordered` | `SinglePartitioned`, Linear | `SingleHashAggregateStream` per partition | raw, several partitions hash-partitioned by group keys, no ordering |
| `single_partitioned_order_preserving, sorted by all keys` | `SinglePartitioned`, Sorted | `OrderedSingleAggregateStream` per partition | raw, several partitions hash-partitioned by group keys, each ordered by all group keys |
| `single_partitioned_order_preserving, sorted by first key` | `SinglePartitioned`, PartiallySorted | `OrderedSingleAggregateStream` with spill per partition | raw, several partitions hash-partitioned by group keys, each ordered by a subset |

The `single_partitioned` cases are what the `CombinePartialFinalAggregate` optimizer rule produces
from adjacent `Partial` + `FinalPartitioned`. To hit them directly, build the
plan by hand with a hash `RepartitionExec` (order-preserving for the sorted variants)
directly under the aggregate.

### Two stage, ordering lost or absent

| Case | Chain | Streams | Input requirements |
|---|---|---|---|
| `partial_repartition_final, unordered` | `Partial` Linear → hash `RepartitionExec` → `FinalPartitioned` Linear | `PartialHashAggregateStream` → `FinalHashAggregateStream` | raw, several partitions, no ordering. **Default planner output.** |
| `partial_coalesce_final, unordered` | `Partial` Linear → `CoalescePartitionsExec` → `Final` Linear | same | raw, several partitions, no ordering. Planner output when `repartition_aggregations = false` |
| `partial_repartition_final, sorted by all keys` | `Partial` Sorted → hash `RepartitionExec` → `FinalPartitioned` Linear | `OrderedPartialAggregateStream` → `FinalHashAggregateStream` | raw ordered by all group keys, non-preserving shuffle |
| `partial_repartition_final, sorted by first key` | `Partial` PartiallySorted → hash `RepartitionExec` → `FinalPartitioned` Linear | same | raw ordered by a subset of group keys, non-preserving shuffle |
| `partial_coalesce_final, sorted by all keys` | `Partial` Sorted → `CoalescePartitionsExec` → `Final` Linear | same | raw ordered by all group keys |
| `partial_coalesce_final, sorted by first key` | `Partial` PartiallySorted → `CoalescePartitionsExec` → `Final` Linear | same | raw ordered by a subset of group keys |

### Two stage, ordering preserved

| Case | Chain | Streams | Input requirements |
|---|---|---|---|
| `partial_order_preserving_repartition_final, sorted by all keys` | `Partial` Sorted → order-preserving hash `RepartitionExec` → `FinalPartitioned` Sorted | `OrderedPartialAggregateStream` → `OrderedFinalAggregateStream`, no spill | raw, several partitions, each ordered by all group keys |
| `partial_order_preserving_repartition_final, sorted by first key` | `Partial` PartiallySorted → order-preserving hash `RepartitionExec` → `FinalPartitioned` PartiallySorted | `OrderedPartialAggregateStream` → `OrderedFinalAggregateStream` with spill | raw, several partitions, each ordered by a subset |
| `partial_sort_preserving_merge_final, sorted by all keys` | `Partial` Sorted → `SortPreservingMergeExec` → `Final` Sorted | `OrderedPartialAggregateStream` → `OrderedFinalAggregateStream`, no spill | raw, several partitions, each ordered by all group keys |
| `partial_sort_preserving_merge_final, sorted by first key` | `Partial` PartiallySorted → `SortPreservingMergeExec` → `Final` PartiallySorted | `OrderedPartialAggregateStream` → `OrderedFinalAggregateStream` with spill | raw, several partitions, each ordered by a subset |
| `partial_final_single_partition, sorted by all keys` | `Partial` Sorted → `Final` Sorted | `OrderedPartialAggregateStream` → `OrderedFinalAggregateStream` | raw, 1 partition ordered by all group keys, no shuffle |
| `partial_final_single_partition, sorted by first key` | `Partial` PartiallySorted → `Final` PartiallySorted | `OrderedPartialAggregateStream` → `OrderedFinalAggregateStream` with spill | raw, 1 partition ordered by a subset |

The `partial_order_preserving_repartition_final` cases are the planner's ordered fast path when `prefer_existing_sort = true`
and the source reports the ordering.

### Three stage (tree reduce)

Not produced by the SQL planner. Build by hand.

| Case | Chain | Streams | Input requirements |
|---|---|---|---|
| `partial_repartition_reduce_repartition_final` | `Partial` → hash `RepartitionExec` → `PartialReduce` → hash `RepartitionExec` → `FinalPartitioned`, all Linear | `PartialHashAggregateStream` → `PartialReduceHashAggregateStream` → `FinalHashAggregateStream` | raw, several partitions, no ordering |
| `partial_repartition_reduce_coalesce_final` | `Partial` → hash `RepartitionExec` → `PartialReduce` → `CoalescePartitionsExec` → `Final`, all Linear | same | raw, several partitions |
| `partial_local_reduce_repartition_final` | `Partial` → `PartialReduce` → hash `RepartitionExec` → `FinalPartitioned`, all Linear | same | raw, several partitions; the reduce stage merges within each partition only |
| `partial_reduce_final_order_preserving` | `Partial` ordered → order-preserving hash `RepartitionExec` → `PartialReduce` ordered → ... | `OrderedPartialAggregateStream` → **fallback `GroupedHashAggregateStream`** → ... | ordered input; `PartialReduce` has no ordered stream, so this is a fallback chain |

`PartialReduce` is the only stage whose input and output are both partial
state. Its input partitioning does not matter for correctness; hash
partitioning just makes it reduce more.

## Chains: TopK

`GROUP BY k1` with a single `max(v)` and a limit larger than any possible group
count. The planner would only produce the shape with the limit on the final
stage; the others are built by hand.

| Case | Chain | Streams | Input requirements |
|---|---|---|---|
| `top_k_query_single` | `Single`, no limit | `SingleHashAggregateStream` or `OrderedSingleAggregateStream` | reference for the TopK chains |
| `top_k_single` | `Single` with limit | `GroupedTopKAggregateStream` | raw, 1 partition, any ordering |
| `top_k_partial_repartition_final` | `Partial` → hash `RepartitionExec` → `FinalPartitioned` with limit | partial stream → `GroupedTopKAggregateStream` | raw, several partitions. **Planner shape** |
| `top_k_partial_coalesce_final` | `Partial` → `CoalescePartitionsExec` → `Final` with limit | partial stream → `GroupedTopKAggregateStream` | raw, several partitions |
| `top_k_both_stages` | `Partial` with limit → hash `RepartitionExec` → `FinalPartitioned` with limit | `GroupedTopKAggregateStream` → `GroupedTopKAggregateStream` | raw, several partitions |

Sorting by `k1` alone already makes this query's input Sorted, so these chains
have no PartiallySorted variant.

## Chains: group key types

Every chain above groups by two `Int64` keys, which `GroupValuesColumn`
handles. These queries run the single-stage shape and the default two-stage
planner shape with the same six aggregates but different key columns, so each
`GroupValues` implementation is exercised across partial, final and single
stages, with and without spilling.

| Query | Keys | Implementation | Orders |
|---|---|---|---|
| `boolean_key_*` | `b Boolean` | `GroupValuesBoolean` | unordered, sorted |
| `bytes_key_*` | `s Utf8` | `GroupValuesBytes` | unordered, sorted |
| `bytes_view_key_*` | `sv Utf8View` | `GroupValuesBytesView` | unordered, sorted |
| `primitive_key_*` | `p Int64` | `GroupValuesPrimitive` | unordered, sorted |
| `mixed_keys_*` | `b, s, sv, p` | `GroupValuesColumn` with mixed types; the order-preserving shape reaches its ordered variant | unordered, sorted by first key, sorted |
| `struct_key_*` | `st Struct<list: List<Int64>, num: Int64>` | row fallback `GroupValuesRows` | unordered only, structs cannot be sorted by the sort kernels |

Every key column is nullable and carries `cardinality` distinct values, except
the boolean one which has two.

## Chains: no grouping

Every stage is `AggregateStream` regardless of mode or ordering. Input ordering
is irrelevant. Nothing spills.

| Case | Chain | Input requirements |
|---|---|---|
| `no_grouping_single` | `Single` | 1 partition |
| `no_grouping_partial_coalesce_final` | `Partial` → `CoalescePartitionsExec` → `Final` | several partitions. **Default planner output.** |
| `no_grouping_partial_reduce_final` | `Partial` → `CoalescePartitionsExec` → `PartialReduce` → `CoalescePartitionsExec` → `Final` | several partitions, by hand |
| `no_grouping_partial_final_single_partition` | `Partial` → `Final` | 1 partition |

## Fallback chains

Any chain above with one of these flipped runs the same shape on
`GroupedHashAggregateStream` for every grouped stage:

| Case | Trigger | Notes |
|---|---|---|
| migration disabled | `enable_migration_aggregate = false` | Run every grouped chain twice, once per flag value. This is the main old-vs-new comparison. |
| grouping sets | grouping sets present | Order mode is forced to Linear; only the unordered chains are reachable |
| ordered partial reduce | `PartialReduce` with ordered input | `partial_reduce_final_order_preserving` above |

---

## Scenario dimensions

### Cardinality (distinct group keys divided by input rows)

| Scenario | Ratio | What it exercises |
|---|---|---|
| very high | about 1.0, nearly all rows unique | skip-partial probe fires in the Linear partial stage; final and single stages spill hardest; Sorted streams emit one row per group |
| high | about 0.5 | skip-partial probe likely fires; large final tables |
| medium | about 0.05 | partial actually reduces; final table moderate |
| low | about 0.001 | few groups, partial reduces almost everything; PartiallySorted streams see many rows per prefix |
| very low | 1 to 10 groups | degenerate hash tables; `PartialReduce` merges to almost nothing |

Skip-partial only fires in a Linear `Partial` stage, so the very high and high
rows are where the unordered partial chains differ most from the ordered chains.

### Memory

| Scenario | Config | Expected behavior |
|---|---|---|
| high | unlimited pool, or pool much larger than the data | no spill, no early emit anywhere |
| medium | `RuntimeEnvBuilder::with_memory_limit(...)` sized so the final or single table cannot fit, disk manager enabled | partial stages emit early; `FinalHashAggregateStream`, `SingleHashAggregateStream` and the PartiallySorted ordered streams spill; Sorted ordered streams are unaffected |

A resources-exhausted error is never accepted, in either scenario. Every stream
is expected to spill, emit early, or stay bounded, so running out of memory
under the medium pool is treated as a bug in that stream's memory handling or in
how the stages share the pool. The run as a whole must also have spilled
somewhere. Known failures at the time of writing are listed under "Findings".

For the medium scenario also run once with the disk manager disabled: spilling
streams must return a resources-exhausted error rather than wrong results, and
Sorted ordered streams must still succeed.

### Skip partial aggregation

| Scenario | Config | Applies to |
|---|---|---|
| enabled | `skip_partial_aggregation_probe_ratio_threshold = 0.8`, rows threshold lowered to 1024 | chains with a grouped `Partial` stage on Linear input, the only place the probe runs |
| disabled | ratio threshold `1.0`, which turns the probe off | same chains |

Chains without such a stage run with the probe enabled only, since it can never
fire there. With it disabled the `skipped_aggregation_rows` metric must stay at
zero everywhere.

### Full matrix

Every chain in the tables above, plus the migration-disabled fallback over all
grouped ones, times 5 cardinalities, times 2 memory settings, times 2 skip
partial settings where the probe applies.

Every cell must produce the same result as `single, unordered` with unlimited
memory for the same dataset and query, compared after sorting by group keys.

## Data generation attributes needed

To reach each row above the generator must be able to control:

| Attribute | Values | Selects |
|---|---|---|
| group key columns | 1 or more | PartiallySorted is only reachable with at least 2 keys |
| sort of the source | none, all group keys, or a subset of group keys | Linear, Sorted, PartiallySorted |
| partition count | 1 or several | single versus partitioned chains |
| partitioning of the source | arbitrary or hash by group keys | the `single_partitioned` cases need hash partitioning, either from the source or from a `RepartitionExec` under the aggregate |
| cardinality | see scenario table | |
| batch size | small values, 1 to 64, matter | forces group boundaries to cross batches in the ordered streams |
| aggregate set | must include multi-field state such as `avg`, `count distinct`, `median`, `array_agg` | otherwise `Partial`, `Single` and `PartialReduce` are indistinguishable |
| nulls in group keys | yes or no | null groups are a separate code path in group values |

## Per-chain checklist

For each chain above, assert before comparing results:

1. `explain` shows the expected `mode=` and `ordering_mode=` on every `AggregateExec`.
2. In the medium memory scenario, `spill_count > 0` on the stages listed as spilling and `== 0` on the rest.
3. In very high cardinality with a Linear `Partial` stage, the `skipped_aggregation_rows` metric is `> 0`.
4. Results equal the reference (`single, unordered`, unlimited memory) after sorting by group keys.

## Findings from running the matrix

Open: none at the time of writing. The occasional hangs seen earlier under the
limited pool stopped once the source batches were copied instead of sliced (see
the accounting item below); they were only ever observed while every batch was
charged the whole partition and `RepartitionExec` spilled constantly. The
per-case timeout stays in place to catch a recurrence.

Fixed or documented:

- `OrderedPartialAggregateStream` refused to emit early on Sorted input and
  reported an error instead. It now emits its partial state like it does for
  PartiallySorted input, and registers as able to handle memory pressure.
- `PartialReduceHashAggregateStream` registered its reservation as not
  spillable although it emits early under pressure, so `FairSpillPool` neither
  capped it nor counted it against the spillers, and several of them could hold
  the whole pool while the final stage below starved. It now registers as
  spillable.
- The legacy `GroupedHashAggregateStream` in `Partial` mode could only emit
  completed prefix groups on ordered input and reported an error when there
  were none (a boolean or low-cardinality first key spans whole partitions). It
  now falls back to emitting everything and restarting its ordering.
- `GroupValuesColumn::emit(EmitTo::All)` replaced the column builders but left
  the hash map populated, so the next `intern` dereferenced stale group indices
  into empty builders (index out of bounds). The map is now cleared too.
- `RepartitionExec` and the order-preserving merge account a batch by
  `get_array_memory_size`, which for a slice is the size of the whole backing
  buffers. Memory-table sources hand out 64-row slices of a partition, so every
  batch was charged the entire partition and the merge could never reserve one.
  The fuzz test now copies each batch, as a real scan would. The accounting
  itself is unchanged.

- The legacy `GroupedHashAggregateStream` with a single nested group key
  (`Struct`, `Map`) produced duplicate groups after spilling. When it switches
  to merging the sorted spill files it relies on `GroupOrderingFull`, which
  requires group ids in first-seen order, and it recreated the group values
  collector to guarantee that only when there was more than one group column.
  A single nested column has no specialized single-column collector and is
  handled by `GroupValuesColumn` through a row-backed column, whose vectorized
  interning assigns ids out of input order, so the ordering emitted a group
  that was still in progress and the next batch reopened it as a new group.
  Fixed by always recreating the collector for the merge phase.

- `PrimitiveDistinctCountGroupsAccumulator` reported its capacity in `size()`
  but only cleared its buffers when emitting everything, so a table looked as
  large after `take_state_batch` as before. Every dedicated stream then failed
  its post-emit or post-spill resize, and the legacy stream could not reserve
  its sort headroom. Fixed by releasing the buffers on `EmitTo::All`.
- `PartialReduceHashAggregateStream` registers its reservation as not
  spillable, so `FairSpillPool` neither caps it nor counts it against the
  spillers, and several of them can hold the pool while the final stage below
  starves.
- When a final or single stream is starved with nothing reserved, its
  post-spill `try_resize` is really a grow from zero, and the error is reported
  as "Decreasing allocation after spilling should succeed".
