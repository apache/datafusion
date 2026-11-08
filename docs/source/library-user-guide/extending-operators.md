<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Extending Operators

DataFusion supports extending operators with custom [`LogicalPlan`] and [`ExecutionPlan`] nodes and [optimizer rules](https://docs.rs/datafusion/latest/datafusion/optimizer/trait.OptimizerRule.html). The examples below show two ways to extend query planning.

[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html

## About DataFusion µWheel

[DataFusion µWheel](https://github.com/uwheel/datafusion-uwheel/tree/main) is a native DataFusion optimizer which improves query performance for time-based analytics through fast temporal aggregation and pruning using custom indices. The integration of µWheel into DataFusion is a joint effort with the DataFusion community.

### Optimizing Logical Plan

The `rewrite` function transforms logical plans by identifying temporal patterns and aggregation functions that match the stored wheel indices. When match is found, it queries the corresponding index to retrieve pre-computed aggregate values, stores these results in a [MemTable](https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html), and returns as a new `LogicalPlan::TableScan`. If no match is found, the original plan proceeds unchanged through DataFusion's standard execution path.

```rust,ignore
fn rewrite(
  &self,
  plan: LogicalPlan,
  _config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    // Attempts to rewrite a logical plan to a uwheel-based plan that either provides
    // plan-time aggregates or skips execution based on min/max pruning.
    if let Some(rewritten) = self.try_rewrite(&plan) {
        Ok(Transformed::yes(rewritten))
    } else {
        Ok(Transformed::no(plan))
    }
}
```

```rust,ignore
// Converts a uwheel aggregate result to a TableScan with a MemTable as source
fn agg_to_table_scan(result: f64, schema: SchemaRef) -> Result<LogicalPlan> {
  let data = Float64Array::from(vec![result]);
  let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)])?;
  let df_schema = Arc::new(DFSchema::try_from(schema.clone())?);
  let mem_table = MemTable::try_new(schema, vec![vec![record_batch]])?;
  mem_table_as_table_scan(mem_table, df_schema)
}
```

To get a deeper dive into the usage of the µWheel project, visit the [blog post](https://uwheel.rs/post/datafusion_uwheel/) by Max Meldrum.

## A custom TopK operator

The [TopK example] shows how to add a custom operator that finds the rows with
the highest values. It uses a query like this one to find the three customers
with the highest revenue:

```sql
SELECT customer_id, revenue FROM sales ORDER BY revenue DESC LIMIT 3;
```

DataFusion already has optimized support for TopK queries. This example is for
learning how to connect a custom operator to DataFusion, not for replacing the
built-in implementation.

### Describe the operation

The example defines `TopKPlanNode`, a custom logical plan node. A logical plan
describes the operations in a query. The node implements
`UserDefinedLogicalNodeCore` so DataFusion can inspect and update it. Its main
fields are:

```rust,ignore
struct TopKPlanNode {
    k: usize,
    input: LogicalPlan,
    expr: SortExpr,
}
```

- `input` describes where the rows come from, such as the `sales` table.
- `expr` describes how to rank the rows, such as by revenue in descending order.
- `k` is the number of rows to keep. In this query, `k` is 3.

### Add the node to the plan

`TopKOptimizerRule` looks for a sort that has a row limit. The limit can be
part of the sort, or it can be a separate limit directly above the sort. The
sort must have exactly one sort expression. The rule replaces that part of the
plan with a `TopKPlanNode`:

```rust,ignore
LogicalPlan::Extension(Extension {
    node: Arc::new(TopKPlanNode {
        k: *fetch,
        input: input.as_ref().clone(),
        expr: expr[0].clone(),
    }),
})
```

The rule takes `k` from the row limit. It copies the input and the sort
expression from the sort. `LogicalPlan::Extension` wraps the custom node so
that it can be part of a logical plan.

For the query above, the new plan includes:

```text
TopK: k=3
  TableScan: sales
```

The rule is registered with the session so DataFusion can apply it while
optimizing the query.

### Execute the operation

`TopKPlanner` converts the logical `TopKPlanNode` into `TopKExec`, a physical
operator. `TopKExec` reads its input and uses `TopKReader` to keep the highest
values. Once it has read the input, the reader returns the retained rows.

The example is specialized for its test data. It expects particular columns
and types, and its map stores only one customer per revenue value. It does not
handle all sort options or other edge cases, so it should not be used as a
general-purpose TopK operator.

[topk example]: https://github.com/apache/datafusion/blob/main/datafusion/core/tests/user_defined/user_defined_plan.rs
