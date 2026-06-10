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

# Extending Operators

DataFusion lets you add operators that are not part of the built-in set. You do this by introducing a custom node into the [`LogicalPlan`], teaching the optimizer to produce it through an [`OptimizerRule`](https://docs.rs/datafusion/latest/datafusion/optimizer/trait.OptimizerRule.html), and providing a matching [`ExecutionPlan`] so the node can run.

This page walks through a complete example -- a `TopK` operator -- and then points to the µWheel project as a larger, real-world case study.

[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html

## A Worked Example: TopK

"Top K" is a common query shape: return the N rows with the largest (or smallest) value of some column. For example, "find the top 3 customers by revenue":

```sql
CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT)
  STORED AS CSV LOCATION 'tests/data/customer.csv';

SELECT customer_id, revenue FROM sales ORDER BY revenue DESC LIMIT 3;
```

Out of the box, DataFusion plans this as a `Sort` feeding a `Limit`:

```text
> EXPLAIN SELECT customer_id, revenue FROM sales ORDER BY revenue DESC LIMIT 3;
+--------------+--------------------------------------+
| plan_type    | plan                                 |
+--------------+--------------------------------------+
| logical_plan | Limit: 3                             |
|              |   Sort: revenue DESC NULLS FIRST     |
|              |     Projection: customer_id, revenue |
|              |       TableScan: sales               |
+--------------+--------------------------------------+
```

That plan is correct, but it fully sorts the input before throwing away everything except the first three rows. The same answer can be produced by scanning the input once and keeping only the largest `k` values seen so far, which bounds the working memory to `k` rows regardless of input size.

The rest of this section builds a `TopK` operator that recognizes the `Limit` + `Sort` pattern and replaces it with a single, specialized node. A full, runnable version of this example lives in [`user_defined_plan.rs`] in the DataFusion test suite.

[`user_defined_plan.rs`]: https://github.com/apache/datafusion/blob/main/datafusion/core/tests/user_defined/user_defined_plan.rs

There are four pieces to implement, plus the wiring that registers them with a `SessionContext`:

1. A **logical node** (`TopKPlanNode`) that represents the operator in a `LogicalPlan`.
2. An **optimizer rule** (`TopKOptimizerRule`) that rewrites the `Limit` + `Sort` pattern into that node.
3. A **physical operator** (`TopKExec`) that actually computes the result at execution time.
4. An **extension planner** (`TopKPlanner`) that turns the logical node into the physical operator.

### The Logical Node

A custom logical operator implements [`UserDefinedLogicalNodeCore`](https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.UserDefinedLogicalNodeCore.html). The node carries the parameters the operator needs -- here, the fetch count `k`, the single input plan, and the sort expression to rank by:

```rust,ignore
#[derive(PartialEq, Eq, PartialOrd, Hash)]
struct TopKPlanNode {
    k: usize,
    input: LogicalPlan,
    /// The sort expression (this example only supports a single sort expr)
    expr: SortExpr,
}

impl Debug for TopKPlanNode {
    /// Use the explain representation as the Debug format.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for TopKPlanNode {
    fn name(&self) -> &str {
        "TopK"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// The output schema of TopK is the same as its input.
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.expr.expr.clone()]
    }

    /// How the node renders in `EXPLAIN` output, e.g. `TopK: k=10`.
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopK: k={}", self.k)
    }

    /// Rebuild the node after the optimizer has rewritten its
    /// expressions or inputs.
    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 1, "expression size inconsistent");
        Ok(Self {
            k: self.k,
            input: inputs.swap_remove(0),
            expr: self.expr.with_expr(exprs.swap_remove(0)),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false // Disallow limit push-down by default
    }
}
```

The framework relies on `inputs`, `expressions`, and `with_exprs_and_inputs` to treat the node uniformly during plan traversal and rewriting: it reads the children and expressions out, optimizes them, and asks the node to rebuild itself with the results.

### The Optimizer Rule

The optimizer rule is what actually introduces the node. It implements [`OptimizerRule`](https://docs.rs/datafusion/latest/datafusion/optimizer/trait.OptimizerRule.html) and looks for a `Limit` whose input is a single-column `Sort`, replacing the pair with a `TopKPlanNode` wrapped in a [`LogicalPlan::Extension`](https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html):

```rust,ignore
#[derive(Default, Debug)]
struct TopKOptimizerRule {}

impl OptimizerRule for TopKOptimizerRule {
    fn name(&self) -> &str {
        "topk"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        // Note: this rule only looks for a Limit directly above a Sort and
        // replaces it with a TopK node. It does not handle many edge cases
        // (multiple sort columns, ASC vs DESC, etc.).
        let LogicalPlan::Limit(ref limit) = plan else {
            return Ok(Transformed::no(plan));
        };
        let FetchType::Literal(Some(fetch)) = limit.get_fetch_type()? else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::Sort(Sort { expr, input, .. }) = limit.input.as_ref()
            && expr.len() == 1
        {
            // found a sort with a single sort expr -- replace it with a TopK
            return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(TopKPlanNode {
                    k: fetch,
                    input: input.as_ref().clone(),
                    expr: expr[0].clone(),
                }),
            })));
        }

        Ok(Transformed::no(plan))
    }
}
```

Returning `Transformed::yes` tells the optimizer the plan changed, so it knows to keep iterating; `Transformed::no` returns the plan untouched. Setting `apply_order` to `ApplyOrder::TopDown` lets the framework walk the plan tree for you and call `rewrite` on each node.

### The Physical Operator

The logical node only describes *what* to compute. The actual work happens in an [`ExecutionPlan`]. `TopKExec` wraps its input and remembers `k`; its `execute` method returns a stream that scans the input once and keeps a running set of the largest `k` rows:

```rust,ignore
/// Physical operator that implements TopK. This implementation is
/// specialized for the example schema and is meant as an illustration only.
struct TopKExec {
    input: Arc<dyn ExecutionPlan>,
    /// The maximum number of output rows
    k: usize,
    cache: Arc<PlanProperties>,
}

impl TopKExec {
    fn new(input: Arc<dyn ExecutionPlan>, k: usize) -> Self {
        let cache = Self::compute_properties(input.schema());
        Self { input, k, cache: Arc::new(cache) }
    }

    /// Build the cached plan properties: schema, equivalence properties,
    /// output partitioning, emission type, and boundedness.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for TopKExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TopKExec: k={}", self.k)
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for TopKExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    /// Require all input on a single partition so the reader sees every row.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TopKExec::new(children[0].clone(), self.k)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq_or_internal_err!(partition, 0, "TopKExec invalid partition {partition}");

        Ok(Box::pin(TopKReader {
            input: self.input.execute(partition, context)?,
            k: self.k,
            done: false,
            state: BTreeMap::new(),
        }))
    }
}
```

`required_input_distribution` returns `Distribution::SinglePartition`, which tells the planner to insert a repartition if needed so a single `TopKReader` observes all input rows.

The stream itself accumulates rows from each incoming batch into a `BTreeMap` keyed by the sort value, discarding the smallest entry whenever the map grows past `k`. When the input is exhausted it emits a single batch with the surviving rows:

```rust,ignore
/// A very specialized TopK reader. The map keeps at most `k` entries.
struct TopKReader {
    input: SendableRecordBatchStream,
    k: usize,
    done: bool,
    /// revenue -> customer_id, ordered by revenue
    state: BTreeMap<i64, String>,
}

impl Stream for TopKReader {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        let k = self.k;
        let schema = self.schema();
        match self.input.poll_next_unpin(cx) {
            // Fold each input batch into the running top-k set, emitting an
            // empty batch to keep the stream making progress.
            Poll::Ready(Some(Ok(batch))) => {
                self.state = accumulate_batch(&batch, self.state.clone(), &k);
                Poll::Ready(Some(Ok(RecordBatch::new_empty(schema))))
            }
            // Input is drained: materialize the surviving rows, highest first.
            Poll::Ready(None) => {
                self.done = true;
                let (revenue, customer): (Vec<i64>, Vec<&String>) =
                    self.state.iter().rev().unzip();
                let customer: Vec<&str> = customer.iter().map(|&s| &**s).collect();
                Poll::Ready(Some(
                    RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(StringViewArray::from(customer)) as ArrayRef,
                            Arc::new(Int64Array::from(revenue)),
                        ],
                    )
                    .map_err(Into::into),
                ))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for TopKReader {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
```

### Connecting Logical and Physical Plans

Two pieces bridge the logical node and the physical operator. An [`ExtensionPlanner`](https://docs.rs/datafusion/latest/datafusion/physical_planner/trait.ExtensionPlanner.html) knows how to build a `TopKExec` from a `TopKPlanNode`:

```rust,ignore
/// Physical planner for TopK nodes
struct TopKPlanner {}

#[async_trait]
impl ExtensionPlanner for TopKPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(topk_node) = node.as_any().downcast_ref::<TopKPlanNode>() {
                assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                Some(Arc::new(TopKExec::new(physical_inputs[0].clone(), topk_node.k)))
            } else {
                // Not our node: let other planners try.
                None
            },
        )
    }
}
```

Returning `None` for an unrecognized node lets DataFusion try other extension planners, so several custom operators can coexist. A [`QueryPlanner`](https://docs.rs/datafusion/latest/datafusion/execution/context/trait.QueryPlanner.html) then registers that extension planner with the default physical planner:

```rust,ignore
#[derive(Debug)]
struct TopKQueryPlanner {}

#[async_trait]
impl QueryPlanner for TopKQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan TopK nodes, and
        // delegate the rest of physical planning to it.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(TopKPlanner {})]);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
```

### Putting It Together

Finally, register the query planner and the optimizer rule on a [`SessionState`](https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html) and run a query. The optimizer rewrites the `Limit` + `Sort` into a `TopK` node, and the query planner turns it into a `TopKExec`:

```rust,ignore
let config = SessionConfig::new().with_target_partitions(48);
let runtime = Arc::new(RuntimeEnv::default());
let state = SessionStateBuilder::new()
    .with_config(config)
    .with_runtime_env(runtime)
    .with_default_features()
    .with_query_planner(Arc::new(TopKQueryPlanner {}))
    .with_optimizer_rule(Arc::new(TopKOptimizerRule {}))
    .build();
let ctx = SessionContext::new_with_state(state);

// ... register the `sales` table, then:
let df = ctx
    .sql("SELECT customer_id, revenue FROM sales ORDER BY revenue DESC LIMIT 3")
    .await?;
df.show().await?;
```

`EXPLAIN` on the same query now shows the custom node in place of the `Sort` + `Limit`:

```text
| logical_plan  | TopK: k=3        |
|               |   TableScan: sales |
| physical_plan | TopKExec: k=3    |
|               |   ...            |
```

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
