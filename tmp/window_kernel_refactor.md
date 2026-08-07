The proposed refactor makes window function execution simpler and more extensible. I think it is a necessary step if we want to invest further in better vectorization or more parallel execution paradigms.

The existing structure is not ideal: if we keep evolving the current shape, new optimization work will likely add more special cases and make the system harder to reason about.

To sanity-check whether this refactor makes sense, we can use the potential optimizations mentioned in:

- https://github.com/apache/datafusion/issues/23197

The examples include better parallelism and vectorization for fixed frames, parallel execution for prefix frames, and segment-tree-based parallelism. These optimizations are natural extensions of the ideal architecture introduced by this issue, but they are hard to add cleanly with the existing structure.

This issue explains, in order:

- How an ideal structure should look
- The issues in the existing implementation
- A possible implementation plan

### Ideal Architecture

The gist is that we should fully separate the logical and physical layers of window execution.

- Logical layer: `WindowCall` purely describes what we want to calculate. It contains the expressions for arguments, partitioning, ordering, and frame bounds.
- Physical layer: `WindowKernel` purely provides the methods needed for execution. It represents the selected execution algorithm for a specific window call.

This design brings below benefits:
- Simplicity: the control flow is one directional, `WindowCall` decides what window kernel to use, and window kernel purely provide methods for execution.
- Extensibility: adding new parallelism scheme/or improve vectorized fast path means adding one window kernel, no deep structural changes needed.

#### Workflow

```text
SQL / logical physical planning
  -> WindowCall              // pure description: function, args, partition/order/frame
  -> WindowKernel selection  // physical execution protocol chosen from shape + capabilities
  -> WindowExec              // execution routing: choose stream based on selected kernel
      -> NaiveAccumulatorStream
      -> SlidingAccumulatorStream
      -> other specialized streams
```

In rough terms:

```rust
/// pure description: function, args, partition/order/frame
struct WindowCall {
    name: String,
    field: FieldRef,
    function: WindowFunctionKind,
    args: Vec<Arc<dyn PhysicalExpr>>,
    filter: Option<Arc<dyn PhysicalExpr>>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    frame: Arc<WindowFrame>,
    options: WindowOptions,
}

/// pure execution: provided methods needed for a specific path
enum WindowKernel {
    /// Derived from existing Accumulator without `retract_batch`
    /// A nested-loop algorithm will be used.
    NaiveAccumulator(Box<dyn NaiveAccumulatorWindowKernel>),
    /// Derived from existing Accumulator with `retract_batch`
    /// A sliding window algorithm will be.
    SlidingAccumulator(Box<dyn SlidingAccumulatorWindowKernel>),
}
```

DataFusion's existing `Accumulator` API already contains the primitives for two useful aggregate window algorithms:

- `update_batch()` plus `evaluate()` can recompute a result for any frame. This supports a naive nested-loop fallback for all accumulators.
- `retract_batch()` plus `supports_retract_batch()` allow incremental sliding-window execution when rows leave the frame.

If the accumulator does not support `retract_batch()`, a naive nested-loop evaluation can be used. If `retract_batch()` is supported and the window frame is a fixed sliding frame, a sliding-window algorithm can be used for optimization.

Then the implication for newly added user-defined window function is, it should only support the naive method to make it work universally (for aggregate function in window cases, it requires only `update_batch()` for the above naive path), but it can optionally support more fast paths (`retract_batch` for sliding window, or even vectorized API in the future), then the optimizer/execution will route that into the fast path if the query expression shape allows.

Here is a simple example to walk through the above workflow.

#### Workload 1: Sliding Aggregate

Example query:

```sql
SELECT
  avg(x) OVER (
    PARTITION BY k
    ORDER BY ts
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS avg_x
FROM t;
```

Planning:

1. `WindowCall` holds the logical description: `avg(x)`, `PARTITION BY k`, `ORDER BY ts`, and `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`.
2. The planner sees that this is an aggregate window over a fixed moving frame.
3. The planner asks the aggregate accumulator whether it supports `retract_batch()`. `avg` does;
4. The planner chooses `SlidingAccumulatorWindowKernel`.
5. `WindowAggExec` routes execution to a dedicated `SlidingAccumulatorStream`, because the selected kernel has the sliding-window execution protocol.

The kernel API can stay small because it only represents one physical protocol:

```rust
trait SlidingAccumulatorWindowKernel {
    fn evaluate_partition(
        &mut self,
        input: &PartitionWindowInput<'_>,
        frame: &FrameIndex,
    ) -> Result<ArrayRef>;
}

struct PartitionWindowInput<'a> {
    batch: &'a RecordBatch,
    args: Vec<ArrayRef>,
    filter: Option<BooleanArray>,
}
```

Very rough sliding-window algorithm sketch:

```python
acc = create_avg_accumulator()
current_frame = range(0, 0)
output = []

for row_idx in partition_rows:
    next_frame = frame_for(row_idx)

    # Rows that were in the previous frame but are not in the next frame.
    leaving = current_frame.start .. next_frame.start
    if leaving is not empty:
        acc.retract_batch(values_for(leaving))

    # Rows that are in the next frame but were not in the previous frame.
    entering = current_frame.end .. next_frame.end
    if entering is not empty:
        acc.update_batch(values_for(entering))

    output.append(acc.evaluate())
    current_frame = next_frame
```

This is the fast path: each input row is added and removed at most once, so the cost is linear in the partition size for row-based fixed frames.

#### Workload 2: Naive Aggregate Fallback

Example query:

```sql
SELECT
  my_udaf(x) OVER (
    PARTITION BY k
    ORDER BY ts
    ROWS BETWEEN t.n_gap PRECEDING AND CURRENT ROW
  ) AS v
FROM t;
```

Assume `my_udaf` is a user-defined aggregate accumulator that supports `update_batch()` and `evaluate()`, but does not support `retract_batch()`. Also the window frame `t.n_gap` preceding can be arbitrary value, it's not supported by the sliding window algorithm.

Planning:

1. `WindowCall` holds the logical description: `my_udaf(x)`, `PARTITION BY k`, `ORDER BY ts`, and `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`.
2. The planner sees that this is an aggregate window (without `retract_batch()` capability), and also over a non-fixed moving frame.
3. The planner chooses `NaiveAccumulatorWindowKernel`.
4. `WindowAggExec` routes execution to a dedicated `NaiveAccumulatorStream`.

The kernel API can again stay small:

```rust
trait NaiveAccumulatorWindowKernel {
    fn evaluate_partition(
        &self,
        input: &PartitionWindowInput<'_>,
        frame: &FrameIndex,
    ) -> Result<ArrayRef>;
}
```

Naive nested-loop algorithm sketch:

```python
output = []

for row_idx in partition_rows:
    frame = frame_for(row_idx)

    # This is slower, but it only needs update_batch() and evaluate().
    acc = create_my_udaf_accumulator()
    acc.update_batch(values_for(frame))

    output.append(acc.evaluate())
```

### Issue with existing implementation
The major issue is that the existing abstraction layers leak into adjacent layers. I think the original design goal was:

- `WindowExpr` is supposed to be the logical layer.
- `PartitionEvaluator` is supposed to be the physical layer.

Over time, however, these responsibilities have become mixed. The decision-making flow has become bidirectional, and the implementation now relies on special cases to work around abstraction leaks.

My guess is that these are mostly hacks accumulated over the years. I cannot find a strong reason to preserve this design.

### Implementation Plan

I plan to do some prototyping to work out a practical refactoring plan. The known goals are:

- Remove all three `WindowExpr` implementations and use `WindowCall` as the pure logical layer.
- Use `WindowKernel` to replace the `PartitionEvaluator`
    - `PartitionEvaluator` is now a large trait that uses 3+ flags to decide behavior. I think it is hard to use and extend; small, focused traits inside `WindowKernel` enum variants should be better.
    - Provide an adapter like `WindowKernel::LegacyPartitionEvaluator` to make the refactor practical.
- Evolve `WindowAggExec` in this direction and avoid changing `BoundedWindowAggExec`
    - See https://github.com/apache/datafusion/issues/23197#issuecomment-4806401319
