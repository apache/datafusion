<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# ExecutionPlan Check Catalog

This document lists every check that the `physical_plan` module of
`datafusion-property-tests` runs, or plans to run, against an `ExecutionPlan`.
See [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for which checks are
implemented.

Each check has a stable **name**, used in reports and with `allow`, and a
**code** that places it in this catalog. An **invariant** violation is a false
claim that can lead to wrong results; a **lint** is a weaker claim than the plan
could make, which misses an optimization.

Each check has a `CheckKind`, which says what the checker gathers before checks
run:

- `Static` (section A): nothing.
- `Execution` (B0 to B8, C1, C4, `memory_released`): the output of executing
  each node on its own, the memory the execution left reserved, and what
  executing the partition one past the last did.
- `Variant` (sections D and E): the outputs of variant runs.
- `Stream` (B10 to B12, `streams_released`, F2): the results of stream
  experiments.

Every execution uses a fresh copy of the node's subtree made with
`reset_plan_states`.

### Stream experiments

An `Experiment` runs on every node with children:

- The node is rebuilt from a `reset_plan_states` copy of its subtree, with the
  `MockSourceExec` leaves below some or all children replaced by copies with
  the same data and another `StreamBehavior`: stall after their first batch,
  fail after their first batch, or never end. Other leaves are unchanged.
- Every child is wrapped in a pass-through probe that reports the child's own
  properties and records what the node does with the child's streams: when
  they are created, polled, finish and are dropped, and which polls happen
  outside a poll of the node's output.
- Experiments use a batch size of 8. Runs on finite inputs use the execution
  timeout (`PlanChecker::with_timeout`, 30 seconds by default). Runs on inputs
  that never end, and waits for streams and memory to be released, use the
  stream timeout (`PlanChecker::with_stream_timeout`, 2 seconds by default). A
  wait for release ends early once no task is alive on the Tokio runtime, so
  cleanup on threads outside it, such as `spawn_blocking` tasks, is not waited
  for.

### Variant runs

A `Variant` is a rewritten copy of the node, or the node run under other
settings. Variants run after every node was executed normally, each on a
`reset_plan_states` copy, to completion within the execution timeout:

- `WithoutFetch`: the plan returned by `with_fetch(None)`. Its output is the
  **unfetched output**; for a node without a fetch it is the normal output.
- `WithFetch(n)`: the plan returned by `with_fetch(Some(n))`.
- `LimitedInputs(n)`: the node with every child limited to its first `n` rows
  per partition.
- `n` is 1, 7, and one more than the rows of the node's output (with and
  without its fetch) and of each child's output. 0 is not tried, since
  `LIMIT 0` never reaches a physical plan.
- `BatchSize(n)`: session batch size 1, 2, 7 and 8192.
- `BatchLayout(layout)`: the rows of each partition of the `MockSourceExec`
  leaves split into one row per batch, random batches of up to 3 rows with
  empty batches, and one batch per partition, with the same `PlanProperties`.
- `Reset` and `Rerun`: execute the copy to completion twice, with and without
  `reset_plan_states` in between. Their output is the second execution's.

Rows are compared exactly with the functions in `oracle`: as multisets, in
order, and as a prefix of a sorted sequence in which rows that tie on the sort
key may appear in any order. Generated floating point values are small
multiples of 0.5, so their sums do not depend on the order of addition.

### Cases

Every check runs once per **case**, one per `Profile` in
`Profile::defaults()`:

- `default`: three partitions (the second empty), exact statistics
- `single partition`
- `inexact statistics`, `absent statistics`
- `empty input`: every partition empty
- `uniform constants`, `constants per partition`: every column is a declared
  constant, the same in every partition or one per partition
- `copied columns`: every column has a declared-equal `__copy` column
- `sorted by row id`: inputs are sorted by `__row_id` and declare it
- `sorted by columns`: inputs without an ordering of their own are sorted by
  all their columns, in schema order, and declare it. Unlike row ids, the
  values repeat, so orderings that only hold without ties are caught
- `hash partitioned`: inputs are hash partitioned on their first column and
  declare it

Every input has a `__row_id` column (excluded from the above), with a
separate id range per input. Rows are split into random batches of up to 16
rows, with empty batches. Inputs are then sorted or repartitioned as the plan
requires. The `Stream` checks only run in the `default` case.

### Terms

- A **single-child** node has exactly one entry in `children()`.
- An **exact** statistic is `Precision::Exact`, an **estimate** is
  `Precision::Inexact`.
- A fetch on a node with several output partitions limits **each
  partition**, as `LimitPushdown` uses `with_fetch`. Only nodes that combine
  partitions, or have one output partition, apply it to their whole output.
  A partition does not have to keep `n` rows: the partitions of a TopK
  `SortExec` share a threshold. Checks require at most `n` rows per partition
  and at least `min(n, rows)` overall.

## A. Static checks

These checks call the methods that describe the plan, and never call `execute`.

### A1 `equal_cardinality_num_rows`

- **What:** a single-child node with `cardinality_effect() == Equal` and no
  fetch must report the same `num_rows` as its input, overall and per
  partition:
  - `Exact(n)` in: `Exact(m)` out with `m != n` is a violation, `Inexact` out
    is a lint.
  - `Inexact` or `Absent` in: `Exact` out is a violation.
  - `Inexact(n)` in: `Inexact(m)` out with `m != n` is a lint.
  - `Absent` out is left to A9.
- **Partitions:** a single output partition is compared with the whole input.
  Otherwise each partition is compared with the input partition it requests
  (`ChildStats::At(Some(q))`). Partitions that request the overall input
  statistics, as in `RepartitionExec`, are skipped.
- **Why:** `Equal` promises one output row per input row.
  `PassthroughStatisticsProvider`, `sort_pushdown` and
  `limit_pushdown_past_window` rely on this.
- **Severity:** Invariant. Lint when only precision is lost or inexact counts differ.
- **Fix:** pass the input `num_rows` through in `statistics_from_inputs`, or
  return a different `cardinality_effect` if the node can drop or add rows.

### A2 `fetch_not_equal_cardinality`

- **What:** a node whose `fetch()` is `Some(_)` must not return
  `CardinalityEffect::Equal`.
- **Why:** a fetch can drop rows. Rules that treat `Equal` as row-preserving,
  such as `sort_pushdown::can_push_fetch_through` and
  `PassthroughStatisticsProvider`, would ignore the fetch.
- **Severity:** Invariant.
- **Fix:** return `LowerEqual` when a fetch is set, as `SortExec` does.

### A3 `fetch_bounds_num_rows`

- **What:** a node with `fetch() == Some(n)` must report at most `n` rows for
  each partition, and at most `n * partition_count` rows overall.
- **Why:** a fetch is a hard upper bound on rows.
- **Severity:** Invariant for exact row counts. Lint for inexact ones.
- **Fix:** apply the fetch to the statistics, e.g. with
  `Statistics::with_fetch`.

### A4 `cardinality_effect_bounds_num_rows`

- **What:** overall and per partition (matched to input partitions as in A1):
  - a single-child node with `cardinality_effect() == LowerEqual` must not
    report more rows than its input;
  - a node with `cardinality_effect() == GreaterEqual` must not report fewer
    rows than any of its inputs.
- **Why:** the cardinality effect and the statistics must agree. Rules such
  as `topk_aggregation` trust the cardinality effect.
- **Severity:** Invariant for exact row counts. Lint for inexact ones.
- **Fix:** correct whichever of `cardinality_effect` and
  `statistics_from_inputs` is wrong.

### A5 `limit_pushdown_missed`

- **What:** a single-child node with `supports_limit_pushdown() == false`
  that passes its child's rows through:
  - `with_fetch(None)` has `cardinality_effect() == Equal`;
  - `maintains_input_order()[0]` is true;
  - each output partition takes rows from at most one child partition: the
    child has one partition, or each output partition requests the
    statistics of one child partition (unlike `RepartitionExec`).
- **Why:** `LimitPushdown` stops at a node that does not support limit
  pushdown, so its child produces more rows than needed.
- **Severity:** Lint.
- **False positives:** nodes whose output rows depend on later input rows,
  such as window functions like `LEAD` (`WindowAggExec`,
  `BoundedWindowAggExec`) or custom look-ahead operators, and nodes with side
  effects on the rows they read. Allow this check for such nodes.
- **Not checked:** nodes with several children, and nodes that report
  `GreaterEqual`, which D11 checks on data.
- **Fix:** return true from `supports_limit_pushdown`.

### A6 `limit_pushdown_merges_partitions`

- **What:** a node with one output partition and
  `supports_limit_pushdown() == true` must not have a child with several
  partitions. One finding per such child. `CoalescePartitionsExec`,
  `SortPreservingMergeExec`, `GlobalLimitExec` and `LocalLimitExec` are not
  checked.
- **Why:** `LimitPushdown` passes the limit through such a node as a
  per-partition `LocalLimitExec` on the child, so a limit of `n` over `k`
  child partitions returns up to `k * n` rows. Only `CoalescePartitionsExec`
  and `SortPreservingMergeExec` are treated as combining partitions.
- **Severity:** Invariant.
- **Not checked:** several children with one partition each, which can
  return up to `n` rows per child. D2 checks them on data.
- **Fix:** return false from `supports_limit_pushdown` when a child has
  several partitions.

### A7 `per_child_lengths` and `check_invariants`

- **What:**
  - `per_child_lengths`: `maintains_input_order`, `required_input_ordering`,
    `benefits_from_input_partitioning`, `input_distribution_requirements` and
    `child_stats_requests` (overall and per partition) each have one entry
    per child. Each `ChildStats::At(Some(p))` names an existing partition of
    that child.
  - `check_invariants`: `check_invariants(Always)` succeeds, and so does
    `check_invariants(Executable)` when the children meet the node's input
    requirements as `SanityCheckPlan` checks them.
- **Why:** optimizer rules index these vectors by child position, so a wrong
  length can panic, skip a child, or apply one child's requirement to
  another. `SanityCheckPlan` rejects a node that fails
  `check_invariants(Executable)`.
- **Severity:** Invariant.
- **Fix:** return one entry per child, in `children()` order. The default
  `check_invariants` checks some lengths too, so one mistake can be reported
  by both checks.

### A8 `statistics_shape` and `partition_statistics_sum`

- **What:**
  - `statistics_shape`: statistics can be computed overall and per
    partition, with one column statistics entry per field in `schema()`. A
    child's error is reported only on the child.
  - `partition_statistics_sum`: if every partition has an exact `num_rows`,
    the overall `num_rows` must be their exact sum. If the overall count is
    exact, no partition may have a larger exact count.
- **Why:** `statistics_from_inputs` should return `Statistics::new_unknown`
  rather than an error, and `column_statistics` is indexed by field
  position. Row counts feed cost-based decisions, and exact ones can answer
  queries such as `COUNT(*)` directly.
- **Severity:** Invariant. Lint when the overall row count could be exact.
- **Fix:** return `Statistics::new_unknown` instead of an error, and keep
  `column_statistics` in sync with the schema, e.g. with
  `Statistics::project`. For a per-partition fetch, sum `min(rows, fetch)`
  over partitions instead of calling `Statistics::with_fetch` on the merged
  statistics.

### A9 `statistics_ignore_inputs`

- **What:** a single-child node with `cardinality_effect() == Equal` and no
  fetch reports `num_rows` as `Absent` although its input's is known.
- **Why:** an `Equal` node can pass its input's row count through unchanged.
  Reporting `Absent` discards a known count, so every node above it loses it
  too, and cost-based decisions fall back to guesses.
- **Severity:** Lint.
- **Fix:** the usual cause is reading `input_stats` in
  `statistics_from_inputs` without overriding `child_stats_requests`, whose
  default supplies unknown statistics (the report says when). Override it to
  return `ChildStats::At(partition)`, and pass the input row count through.

### A10 `schema_consistency` and `expression_column_refs`

- **What:**
  - `schema_consistency`: `schema()` equals the schema of
    `properties().eq_properties`, including nullability and metadata.
  - `expression_column_refs`: every `Column` in the output orderings,
    equivalence classes, constants and output partitioning has an index
    inside `schema()` and the name of the field at that index. For
    single-child nodes, `Column`s in `apply_expressions` are checked the same
    way against the input schema. The exception is a `Final` `AggregateExec`,
    whose aggregate expressions refer to the input of the `Partial` aggregate
    below it, so they are checked against `AggregateExec::input_schema`.
- **Why:** stale column indexes are a common result of projection pushdown
  and child replacement. They make ordering and partitioning checks refer to
  the wrong column, which can remove a needed sort or repartition.
- **Severity:** Invariant. Lint when `schema_consistency` differs only in
  metadata.
- **Fix:** rebuild expressions and equivalence properties against the current
  schema after changing children or projections.

### A11 `dynamic_expressions_visited` and `dynamic_expressions_reset`

- **What:**
  - `dynamic_expressions_visited`: every expression in
    `dynamic_expressions_produced()` has an expression id that appears in
    the expressions visited by `apply_expressions`.
  - `dynamic_expressions_reset`: after `reset_state`, the node produces none
    of the dynamic expressions it produced before, compared by expression
    id. A filter that keeps its id keeps the values set by execution.
- **Why:** rules that find or rewrite dynamic filters rely on
  `apply_expressions` visiting them. A dynamic filter that survives
  `reset_state` makes re-execution, such as in recursive queries, use stale
  filter values.
- **Severity:** Invariant.
- **Fix:** include produced dynamic filters in `apply_expressions`, and
  recreate them in `reset_state`.

## B. Reported properties hold at runtime

These checks run the plan on generated input and compare the output with what
the plan reports.

### B0 `execution_succeeds`

- **What:** executing every output partition finishes without an error, a
  panic, or exceeding the execution timeout. A failure caused by a failing
  child is only reported on the child.
- **Why:** a node that fails on input meeting its requirements fails real
  queries too, and the other execution checks need output.
- **Severity:** Invariant.
- **Fix:** depends on the error. If the input does not meet an undeclared
  requirement, declare it (C2, C3).

### B1 `batch_schema`

- **What:** every batch has the same number of fields as `schema()`, with the
  same names and data types, and no nulls (counted logically) in fields
  `schema()` declares non-nullable. Nullability flags and metadata should
  match too.
- **Why:** parents build their schema from `schema()` and look up columns by
  position. Nulls in a non-nullable field break operators that skip null
  handling.
- **Severity:** Invariant. Lint for nullability flags and metadata.
- **Fix:** make the batches match `schema()`, or correct `schema()`.

### B2 `exact_statistics_hold`

- **What:** every exact statistic is true for the output, overall and per
  partition: `num_rows`, and per column `min_value`, `max_value`,
  `null_count`, `distinct_count` and `sum_value`.
  - Minimum, maximum and sum are not checked for a column without non-null
    values.
  - Sums are compared by value, whatever their type, and only for integer and
    decimal columns, and not when they overflow the type of SQL `SUM`.
  - Byte sizes are not checked.
- **Attribution:** a child with a false exact statistic can make the node's
  false too, and is reported for it. The node is then checked against
  `statistics_from_inputs` computed from the child statistics it requests,
  with every false exact value replaced by the true one, and is reported for
  what is still false.
- **Reporting:** one finding overall and one per partition, listing each false
  statistic with its claimed and actual values.
- **Why:** exact statistics are used to prove things, such as removing a limit
  or answering an aggregate without reading data.
- **Severity:** Invariant.
- **Fix:** report `Inexact` for anything that cannot be proven.

### B3 `orderings_hold`

- **What:** each output partition is sorted by every ordering in the output
  equivalence properties, with their sort options.
- **Why:** the optimizer removes sorts that a reported ordering satisfies.
- **Severity:** Invariant.
- **Fix:** remove the ordering, or make the node preserve it.

### B4 `constants_hold`

- **What:** each constant expression has a single value within each partition
  (null is a value). A constant uniform across partitions has the same value
  in every partition, the given one if any, compared after a cast to the
  expression's type. Constants with wrong column references are left to A10.
- **Reporting:** one finding per partition with several values, giving two,
  and one per partition whose value differs from the given value or, without
  one, from the first partition with rows.
- **Why:** constants let the optimizer drop sort keys and grouping columns.
- **Severity:** Invariant.
- **Fix:** only mark an expression constant when the node guarantees it.

### B5 `equivalence_classes_hold`

- **What:** the expressions of an equivalence class are equal on every output
  row, compared with the first of the class after a cast to its type. Two
  nulls are equal. Literals are left to B4, wrong column references to A10.
- **Reporting:** one finding per expression and partition, giving the first
  row on which it differs and both values.
- **Why:** equivalences let an ordering or partitioning on one column satisfy
  a requirement on another.
- **Severity:** Invariant.
- **Fix:** only add equivalences the node guarantees, such as from an equality
  filter or join key.

### B6 `constraints_hold`

- **What:** primary key and unique constraints hold on the output: no
  duplicate key tuples, and no nulls in primary key columns.
- **Why:** constraints allow removing redundant grouping and distinct
  operations.
- **Severity:** Invariant.
- **Fix:** drop constraints the node cannot guarantee, such as after a join
  that can duplicate rows.

### B7 `hash_partitioning_holds` and `invalid_partition_errors`

- **What:**
  - `hash_partitioning_holds`: with `Partitioning::Hash(exprs, n)`, every row
    of output partition `p` is one `BatchPartitioner` with the same
    expressions and `n` puts in `p`. One finding per partition with rows of
    other partitions, giving how many and one example key.
  - `invalid_partition_errors`: `execute(n)` on a node with `n` partitions
    returns an error, from `execute` or as the first item of its stream,
    polled once within the execution timeout. A panic, a batch, a stream that
    ends cleanly, or one that produces nothing is reported. Partitions past
    `n` are not tried.
- **Not checked:** a node whose child's hash partitioning is false, or that
  executes the same partition of a child that is reported, since it inherits
  the problem; and keys with wrong column references (A10) or not in the
  output (`UnKnownColumn`).
- **Why:** partitioned joins and aggregates assume inputs hash partitioned on
  equal keys are co-partitioned, or they drop matches and split groups. A
  node asked for a partition it does not have must fail the query, not return
  other rows or panic.
- **Severity:** Invariant.
- **Fix:** report `UnknownPartitioning` unless the partitioning matches the
  repartition hash. Check the partition index and return an error such as
  `internal_err!("Invalid partition {partition}")`.

### B8 `cardinality_effect_holds`

- **What:** the output row count agrees with `cardinality_effect()`: equal
  to, at most, or at least the input row count. `Equal` and `LowerEqual` are
  only checked for single-child nodes.
- **Why:** see A1 and A4.
- **Severity:** Invariant.
- **Fix:** correct `cardinality_effect`.

### B9 `metrics_output_rows`

- **What:** an `output_rows` metric equals the rows produced, and the set of
  metric names does not change between calls to `metrics()` after execution.
- **Why:** `EXPLAIN ANALYZE` and adaptive features read metrics.
- **Severity:** Invariant.
- **Fix:** record output rows on every path that emits a batch.

### B10 `boundedness_holds`

- **What:** the node is rebuilt with the leaves below one child unbounded
  (the `UnboundedInput` experiment), for each child in turn.
  - A rebuilt node that reports `Bounded` must end within the stream timeout
    while that child reports `Unbounded` and keeps delivering rows.
  - Lint: a rebuilt node that reports `Unbounded`, has a fetch, otherwise
    passes every row through (`with_fetch(None)` is `Equal`), and ended.
- **Not checked:** a child that reports `Bounded` or delivers no rows. Each
  unbounded partition stalls after 65536 rows; for a node with a fetch, a run
  that reached that limit is inconclusive and skipped.
- **Why:** `SanityCheckPlan`, `EnforceSorting` and `EnforceDistribution`
  decide based on boundedness. A false `Bounded` lets through a plan that
  never finishes; a missed one rejects plans that would, and `LimitPushdown`
  replacing a `GlobalLimitExec` by a fetch can make a bounded plan unbounded.
- **Severity:** Invariant. Lint for a missed `Bounded`.
- **Fix:** derive boundedness from the inputs, e.g. with
  `boundedness_from_children`, and report `Bounded` when a fetch bounds a
  node that does not drop rows.

### B11 `emission_type_holds`

- **What:** with the leaves below one child unbounded (`UnboundedInput`), for
  each child in turn, a rebuilt node that reports `Incremental` produces at
  least one row within the stream timeout.
- **Not checked:** a child that reports `Final` or delivers no rows, a node
  that produced no rows from the normal input, and a rebuilt node that
  reports `Final` or `Both`.
- **False positives:** each unbounded partition delivers up to 65536 rows,
  enough for a node that buffers up to a batch. A node that needs more before
  it emits is reported; allow this check for it.
- **Why:** streaming queries need incremental operators to emit, and
  `SanityCheckPlan` and `JoinSelection` only accept non-`Final` operators on
  unbounded input.
- **Severity:** Invariant.
- **Fix:** report `Final` if the node waits for all input.

### B12 `lazy_evaluation_holds`

- **What:** a node that reports `EvaluationType::Lazy` only polls its input
  streams from within a poll of its own output, on the same thread (the
  `Laziness` experiment, which polls every output partition to the end).
  Polls from `execute` or from spawned tasks are reported.
- **Not checked:** a node that reports `Eager` but is lazy.
- **Why:** `EnsureCooperative` adds `CooperativeExec` below `Eager` nodes. A
  node that spawns tasks but reports `Lazy` can leave them polling
  non-cooperative inputs that never yield.
- **Severity:** Invariant.
- **Fix:** report `Eager` for nodes that spawn tasks or buffer ahead of
  demand.

### B13 `cooperative_scheduling_holds`

- **What:** with `SchedulingType::Cooperative`, the stream returns `Pending`
  when the Tokio task budget runs out, even over an always ready input.
- **Why:** queries must be cancellable, and a stream that never yields blocks
  the worker thread.
- **Severity:** Invariant.
- **Fix:** consume budget with the helpers in the `coop` module, or report
  `NonCooperative`.

## C. Order and input requirements

These checks track output rows to the input rows they came from by their
`__row_id` column.

### C1 `maintains_input_order_holds`

- **What:** if `maintains_input_order()[i]` is true, then within each output
  partition the rows of each partition of child `i` appear in the order of
  that partition, whether or not the child reports an ordering. A row can
  repeat, as long as no later row of the same child partition comes between
  its copies. The child must have one `__row_id` column with unique ids, and
  the output one column with ids of the child. Other output rows are ignored.
- **Not checked:** how rows of several child partitions are interleaved,
  which B3 checks against the reported ordering; and nodes or children whose
  rows cannot be tracked.
- **Reporting:** one finding per output partition and child partition, giving
  the first row out of order.
- **Why:** sort pushdown, `EnforceSorting` and
  `replace_with_order_preserving_variants` trust the claim. Keeping order only
  for sorted input is a condition the optimizer cannot see.
- **Severity:** Invariant.
- **Fix:** return false for that child, or keep the order of every input.

### C2 `required_input_ordering_honest`

- **What:** for a deterministic node (no fetch, no volatile expressions), if
  `required_input_ordering()[i]` is `None`, shuffling the rows of child `i`
  does not change the output multiset.
- **Why:** a node that needs sorted input but does not say so gives wrong
  results when the optimizer does not sort for it.
- **Severity:** Invariant.
- **Fix:** declare the ordering in `required_input_ordering`.

### C3 `input_distribution_honest`

- **What:** if the required distribution of a child is
  `UnspecifiedDistribution`, the combined output does not depend on how the
  child's rows are split among partitions.
- **Why:** a node that needs hash partitioned or single partition input but
  does not say so gives wrong results when the optimizer does not
  repartition for it.
- **Severity:** Invariant.
- **Fix:** declare the distribution in `input_distribution_requirements`.

### C4 `maintains_input_order_missed`

- **What:** `maintains_input_order()[i]` is false, but every output row is a
  row of child `i`, each output partition has rows of one child partition
  only, in its order (copies of a row next to each other). Rows are tracked
  as in C1. Reported when an output partition has at least two rows of the
  child and, for a child with several partitions, the output has rows of at
  least two of them.
- **Not checked:**
  - a node that reports an output ordering, since sorting keeps tied rows in
    input order;
  - a child that reports an ordering or a constant;
  - a node that can move rows between partitions of a child with several
    partitions (it computes an output partition's statistics from the child's
    overall statistics, as `RepartitionExec` does).
- **Why:** a missed `true` costs an unnecessary sort.
- **Severity:** Lint.
- **Fix:** return true for that child, and keep the child's ordering in the
  output equivalence properties.

## D. Rewrite hooks are equivalent

Every hook that returns a new plan must produce the same results as the
original: the same multiset of rows, and the same sequence, apart from ties,
wherever an ordering is reported.

### D1 `with_fetch_equivalent`

- **What:** for a node whose `with_fetch` returns a plan, with the
  `WithFetch(n)` and `WithoutFetch` variants:
  - `with_fetch(Some(n))` reports `fetch() == Some(n)`, the node's schema and
    partition count, and (lint) the node's orderings.
  - Its output has at most `n` rows per partition, at least
    `min(n, rows of the unfetched output)` overall, and only rows of the
    unfetched output.
  - Where the node reports an ordering, the partitions together contain the
    first `min(n, rows)` rows of the unfetched output, apart from ties, and
    each is sorted. A partition does not have to be a prefix of its own
    partition (partitioned TopK).
  - `with_fetch(None)` reports no fetch, the node's schema and partition
    count, and for a node without a fetch the node's rows.
  - A node with a fetch satisfies the same as `with_fetch(Some(fetch))`.
- **Not checked:** which rows are kept when `with_fetch(None)` returns `None`;
  only the row counts are checked.
- **Why:** `LimitPushdown` and `PushdownSort` replace limits with fetches and
  trust the result.
- **Severity:** Invariant. Lint for a missing ordering.
- **Fix:** apply the fetch after all other work, per output partition.

### D2 `limit_pushdown_equivalent`

- **What:** a node with children for which `supports_limit_pushdown()` is
  true is rebuilt with every child limited to its first `n` rows per
  partition (`LimitedInputs`). Then each output partition has at most `n`
  rows, the output at least `min(n, rows of the normal output)`, only rows of
  the normal (or unfetched) output, and the first rows in sort order where
  the node reports an ordering, as in D1.
- **Partitions:** for `CoalescePartitionsExec` and `SortPreservingMergeExec`,
  which keep the limit, only the first `n` rows of each partition are checked.
  `GlobalLimitExec` and `LocalLimitExec` are not checked, since
  `LimitPushdown` merges them before asking.
- **Not checked:** offsets, which `LimitPushdown` also pushes to the children.
- **Why:** `LimitPushdown` removes the limit above such a node and limits
  every child instead, so the node must produce a valid limit of its output
  from limited inputs. `sort_pushdown` and `limit_pushdown_past_window` push
  fetches through `Equal` nodes that support limit pushdown too.
- **Severity:** Invariant.
- **Fix:** return false from `supports_limit_pushdown`.

### D3 `projection_swap_equivalent`

- **What:** a plan returned by `try_swapping_with_projection` gives the same
  results as the projection on top of the original node.
- **Severity:** Invariant.
- **Fix:** remap every expression through the projection.

### D4 `sort_pushdown_equivalent`

- **What:** when `try_pushdown_sort` returns `Exact`, the new plan's output is
  sorted by the requested order and has the same rows; with `Inexact`, the
  same rows.
- **Severity:** Invariant.
- **Fix:** only return `Exact` when the order is guaranteed.

### D5 `filter_pushdown_equivalent`

- **What:**
  - `gather_filters_for_pushdown` returns one result per parent filter, in
    order, and a filter sent to child `i` only refers to its columns.
  - A filter above the node gives the same results as the plan after
    `FilterPushdown`.
  - A filter reported as `PushedDown::Yes` is fully applied by the child, as
    `FilterExec` relies on when it removes itself.
- **Severity:** Invariant.
- **Fix:** only mark a filter pushed down when the child applies it exactly.

### D6 `repartitioned_equivalent`

- **What:** `repartitioned(n)` gives the same rows, at most `n` partitions,
  and a plan that passes the section B checks.
- **Severity:** Invariant.
- **Fix:** keep the reported properties in sync with the new partitioning.

### D7 `preserve_order_holds`

- **What:** the plan returned by `with_preserve_order(true)` produces output
  sorted by its reported orderings.
- **Severity:** Invariant.
- **Fix:** keep the order when asked to, or return `None`.

### D8 `replace_children_consistent`

- **What:**
  - With unchanged children, `ChildrenPropertiesMode::Keep` and `Recompute`
    give identical properties.
  - `P(A).replace(B)` has the properties of `P(C).replace(B)`, and
    `P(A).replace(B).replace(A)` those of `P(A)`.
  - The wrong number of children is an error.
- **Why:** catches properties cached for the first children, such as an
  aggregate's input order mode.
- **Severity:** Invariant.
- **Fix:** recompute every derived field in `replace_children` with
  `Recompute`.

### D9 `reset_state_reexecution`

- **What:**
  - `Reset`: after `reset_plan_states`, executing the plan again gives the
    same results, compared as in section E.
  - `Rerun`: executing every partition again without a reset gives the same
    results or an error.
  - Errors (for `Reset`), panics and timeouts are reported. A node is only
    compared when every child's second execution matched its normal output.
- **Not checked:** executing a single partition twice.
- **Why:** recursive queries re-execute plans after `reset_plan_states`, and a
  cached plan executed again must not silently return wrong results.
- **Severity:** Invariant.
- **Fix:** clear all execution state, such as shared build sides and dynamic
  filters, in `reset_state`, and return an error from a second execution
  that cannot reuse the first's state.

### D10 `proto_roundtrip`

- **What:** if `try_to_proto` returns `Some`, decoding it gives a plan with
  the same display, properties and results.
- **Severity:** Invariant.
- **Fix:** serialize every field that affects properties or execution.

### D11 `limit_pushdown_missed_at_runtime`

- **What:** a node with children for which `supports_limit_pushdown()` is
  false, but that is **prefix-closed**: with every child limited to its first
  `n` rows per partition (`LimitedInputs`), each output partition has exactly
  the first `min(n, rows)` rows of the same partition of the normal output,
  in order apart from ties. Only reported if a limit removed output rows.
- **Not checked:**
  - nodes A5 reports;
  - a node whose cardinality effect without its fetch is not `Equal` or
    `GreaterEqual`;
  - a node over a child that reports an ordering or a constant whose order
    the node does not maintain;
  - a node that can move rows between partitions, as in C4.
- **Why:** as for A5. It finds nodes whose properties do not show it, such as
  one that reports `GreaterEqual`.
- **Severity:** Lint.
- **Fix:** return true from `supports_limit_pushdown`. Allow this check if the
  node is only prefix-closed for the generated inputs.

## E. Results do not depend on configuration or input layout

Each check runs the node under several settings and requires the same
multiset of rows, every reported ordering holding in each partition, and no
failure. For a node with a fetch, each output must instead be a valid result
of the fetch, as in D1. A node is only compared under a setting in which every
child produced the same rows, in the same order and partitions, as normally.

- **False positives:** a node whose output is only defined up to a later
  step, such as an aggregate with a soft `DISTINCT` limit, which relies on the
  `LIMIT` above it. Allow these checks for such a node.
- **Why:** batch boundaries, partition layouts, encodings and spilling are
  where operators such as sort merge join, windows and sort preserving merge
  most often break.
- **Fix:** handle state that spans batches or partitions, and test the spill
  path.

### E1 `batch_size_invariance`

- **What:** the same results with `batch_size` 1, 2, 7 and 8192
  (`BatchSize`).
- **Severity:** Invariant.

### E2 `batch_boundary_invariance`

- **What:** the same results with the `MockSourceExec` leaves split into one
  row per batch, random batches of up to 3 rows with empty batches, and one
  batch per partition (`BatchLayout`).
- **Severity:** Invariant.

### E3 `partitioning_invariance`

- **What:** the same results across `target_partitions` values and splits of
  input rows that still meet the distribution requirements.
- **Severity:** Invariant.

### E4 `encoding_invariance`

- **What:** the same results with dictionary encoded and plain arrays, and
  with `StringView` and `Utf8` strings.
- **Severity:** Invariant.

### E5 `memory_limit_invariance`

- **What:** with a small memory limit, the same results or a
  `ResourcesExhausted` error, never a panic or a reservation above the limit.
- **Severity:** Invariant.

## F. Lifecycle and robustness

### F1 `memory_released` and `streams_released`

- **What:**
  - `memory_released`: after executing the node to completion with a fresh
    `UnboundedMemoryPool` and dropping its streams and plan, the memory
    reserved is released within the stream timeout. A node whose child also
    leaves memory reserved is not reported.
  - `streams_released` (the `Cancellation` experiment): the leaves stall after
    their first batch, and the output is polled until each partition produced
    a batch or ended, or for 20 milliseconds, then dropped. Every input stream
    must then be dropped within the stream timeout.
  - Planned: spill files are removed.
- **Why:** a cancelled query, or one whose consumer stops early, must stop
  reading its inputs and return its memory.
- **Severity:** Invariant. Lint when input streams are only dropped once the
  plan is dropped.
- **Fix:** tie spawned tasks and reservations to the stream, for example with
  `SpawnedTask`, and do not keep handles to them in the plan.

### F2 `errors_propagate`

- **What:** every leaf fails after its first batch (the `InputError`
  experiment). If an error reached the node, at least one output partition
  returns an error; ending without one, not ending within the execution
  timeout, or panicking is reported.
- **Not checked:** no error reached the node, or the node has a fetch and an
  output partition produced `fetch` rows.
- **Why:** an operator that drops an error returns partial results as if they
  were complete.
- **Severity:** Invariant.
- **Fix:** return input errors as they arrive, including from spawned tasks.

### F3 `empty_input`

- **What:** inputs with zero partitions, zero batches, or only empty batches
  work. The `empty input` case and the empty batches and partitions of the
  other cases cover all but zero partitions.
- **Severity:** Invariant. Lint for emitting empty batches.

### F4 `partition_isolation`

- **What:** executing a single partition while the others are never polled
  finishes without deadlock.
- **Severity:** Lint.
