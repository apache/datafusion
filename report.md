# Dynamic Filter Deadlock Investigation

## Reproducer

Original failing query was TPC-H Q18 at SF1, executed through `datafusion-cli` with:

```bash
DATAFUSION_EXECUTION_TARGET_PARTITIONS=24 ./target/release/datafusion-cli -f /tmp/tpch_q18.sql
```

On `main`, this hung consistently.

## Initial theories that turned out incomplete

### Repartition EOF delivery was blocked by backpressure

The first working theory was:

1. some final hash-join build partitions were empty,
2. `RepartitionExec` sent end-of-stream (`None`) through the same globally gated channels as data,
3. the gate could remain closed long enough that EOF to empty partitions never arrived,
4. those build partitions never completed,
5. sibling hash-join partitions blocked forever waiting on the dynamic-filter barrier.

This explained some symptoms but was not the full root cause.

### Receiver-side empty-and-closed race in distributor channels

A smaller hardening change was tried in `RecvFuture::poll`:

- if the channel is empty and `n_senders == 0`, return `None` immediately

This was plausible but did **not** fix the original Q18 reproducer on clean `main`.

## What the tracing showed

The useful traces were on:

- `RepartitionExec::wait_for_task`
- `PerPartitionStream`
- `DistributionReceiver::drop`
- `HashJoinExec::execute`
- `HashJoinStream::collect_build_side`
- `HashJoinStream::drop`
- `OnceFut::get_shared`

The critical observations were:

1. The stuck final join was:
   - `HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_orderkey@2, l_orderkey@0)]`
2. The empty / problematic build partitions were consistently:
   - `2`, `14`, `21`
3. Those final-inner-join partitions were **created**, but they were dropped in `WaitBuildSide` before ever reaching `COLLECT_BUILD_ENTER`.
4. The parent operator above that join was a partitioned `RightSemi` join.
5. For parent partitions `2`, `14`, `21`, the `RightSemi` build side was empty:
   - `rows=0`, `map_empty=true`
6. `RightSemi` is one of the join types where an empty build side produces an empty result immediately.
7. So those parent `RightSemi` partitions legally transitioned to `Completed` and stopped polling their child final-inner-join partitions.
8. Because the child final-inner-join partitions were never polled, their lazy `left_fut` never ran, so they never reported build data to `SharedBuildAccumulator`.
9. Meanwhile, sibling final-inner-join partitions did run, reported build data, and then blocked in `WaitPartitionBoundsReport` forever, because the barrier still expected all 24 partition reports.

## Actual root cause

The deadlock was a conflict between:

- **lazy build-side collection/reporting** in partitioned hash join, and
- **legal early cancellation** by an upstream parent operator.

More concretely:

1. In `PartitionMode::Partitioned`, build-side collection lives behind a per-partition `OnceFut`.
2. Dynamic-filter reporting also happens only after that future is polled and finishes.
3. `SharedBuildAccumulator` expects one report from every partition.
4. But an upstream partitioned `RightSemi` join can legitimately stop polling some child partitions when its own build side is empty.
5. Those canceled child partitions never report "I am empty", yet the barrier still waits for them.

So the bug was **not** fundamentally in `RepartitionExec`.

`RepartitionExec` exposed the problem, but the actual broken invariant was:

> partitioned hash-join dynamic filter coordination assumed every partition stream would be polled far enough to report build data.

That assumption is false.

## Desired behavior

The correct semantics are:

- dynamic filter finalization should happen only after the **entire build side** is known,
- and "entire build side" includes partitions that are empty.

Therefore, an empty build partition must still contribute:

- its partition id,
- empty membership / empty bounds information,
- and its completion signal for the barrier.

## Proposed fix

For `PartitionMode::Partitioned` when dynamic-filter pushdown is enabled:

- start build-side collection eagerly at `HashJoinExec::execute()` time,
- do not wait for the child `HashJoinStream` to be polled,
- once build-side collection completes, report `PartitionBuildData::Partitioned` immediately,
- including the case where the partition is empty,
- then let the shared future resolve to the collected `JoinLeftData`.

With that change:

- empty partitions still report,
- the barrier completes,
- active sibling partitions stop hanging,
- and a parent operator is still free to cancel the child stream later.

## Validation so far

The eager-report fix:

- made the original Q18 reproducer complete successfully,
- passed the broad `hash_join` test slice,
- passed the existing empty-build dynamic-filter tests,
- and survived a large `cargo test` run aside from an unrelated CLI snapshot mismatch.

## Regression test shape

The right regression test is one that captures this exact cancellation pattern:

1. a partitioned child inner hash join with dynamic-filter pushdown enabled,
2. empty build partitions in some output partitions,
3. a partitioned parent `RightSemi` join above it,
4. parent partitions that legally complete early on those empty partitions,
5. assertion that execution completes instead of hanging.

The test should fail on `main` by timing out or hanging, and pass with the eager-report fix.
