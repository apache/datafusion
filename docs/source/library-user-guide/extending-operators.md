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

# Extending DataFusion's operators: custom LogicalPlan and Execution Plans

This section contains an end-to-end demonstration of creating a user-defined operator in DataFusion. Specifically, it shows how to define a `TopKNode` that implements `ExtensionPlanNode`, add an `OptimizerRule` to rewrite a `LogicalPlan` to use that node, create an `ExecutionPlan`, and finally produce results.

## TopK Background:

Note: DataFusion contains a highly optimized version of the `TopK` operator, but we present a simplified version in this section for explanatory purposes. For more information, see the full implementation in the [DataFusion repository].

[DataFusion repository]: https://docs.rs/datafusion/latest/datafusion/physical_plan/struct.TopK.html

"Top K" operator is a common query optimization used for queries such as "find the top 3 customers by revenue". The(simplified) SQL for such a query might be:

```sql
explain SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
```

And a naive plan would be:

```text
+--------------+----------------------------------------+
| plan_type    | plan                                   |
+--------------+----------------------------------------+
| logical_plan | Limit: 3                               |
|              |   Sort: revenue DESC NULLS FIRST       |
|              |     Projection: customer_id, revenue   |
|              |       TableScan: sales                 |
+--------------+----------------------------------------+
```

While this plan produces the correct answer, the careful reader will note it fully sorts the input before discarding everything other than the top 3 elements.
The same answer can be produced more efficiently by simply keeping track of the top N elements, reducing the total amount of memory buffer.

## Process for Defining Extending Operator

The following example illustrates the implementation of a `TopK` node:

```rust
use std::fmt::Debug;
use std::hash::Hash;
use std::task::{Context, Poll};
use std::{any::Any, collections::BTreeMap, fmt, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::{
    common::cast::{as_int64_array, as_string_array},
    common::{arrow_datafusion_err, internal_err, DFSchemaRef},
    error::{DataFusionError, Result},
    execution::{
        context::{QueryPlanner, SessionState, TaskContext},
        runtime_env::RuntimeEnv,
    },
    logical_expr::{
        Expr, Extension, LogicalPlan, Sort, UserDefinedLogicalNode,
        UserDefinedLogicalNodeCore,
    },
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
        PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::ScalarValue;
use datafusion_expr::{FetchType, InvariantLevel, Projection, SortExpr};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::AnalyzerRule;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};

use async_trait::async_trait;
use futures::{Stream, StreamExt};

#[derive(PartialEq, Eq, PartialOrd, Hash)]
struct TopKPlanNode {
    k: usize,
    input: LogicalPlan,
    /// The sort expression (this example only supports a single sort
    /// expr)
    expr: SortExpr,

    /// A testing-only hashable fixture.
    /// For actual use, define the [`Invariant`] in the [`UserDefinedLogicalNodeCore::invariants`].
    invariant_mock: Option<InvariantMock>,
}

impl Debug for TopKPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
struct InvariantMock {
    should_fail_invariant: bool,
    kind: InvariantLevel,
}

impl UserDefinedLogicalNodeCore for TopKPlanNode {
    fn name(&self) -> &str {
        "TopK"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Schema for TopK is the same as the input
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn check_invariants(&self, check: InvariantLevel, _plan: &LogicalPlan) -> Result<()> {
        if let Some(InvariantMock {
            should_fail_invariant,
            kind,
        }) = self.invariant_mock.clone()
        {
            if should_fail_invariant && check == kind {
                return internal_err!("node fails check, such as improper inputs");
            }
        }
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.expr.expr.clone()]
    }

    /// For example: `TopK: k=10`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopK: k={}", self.k)
    }

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
            invariant_mock: self.invariant_mock.clone(),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false // Disallow limit push-down by default
    }
}

/// Physical planner for TopK nodes
struct TopKPlanner {}

#[async_trait]
impl ExtensionPlanner for TopKPlanner {
    /// Create a physical plan for an extension node
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
                // figure out input name
                Some(Arc::new(TopKExec::new(
                    physical_inputs[0].clone(),
                    topk_node.k,
                )))
            } else {
                None
            },
        )
    }
}

/// Physical operator that implements TopK for u64 data types. This
/// code is not general and is meant as an illustration only
struct TopKExec {
    input: Arc<dyn ExecutionPlan>,
    /// The maximum number of values
    k: usize,
    cache: PlanProperties,
}

impl TopKExec {
    fn new(input: Arc<dyn ExecutionPlan>, k: usize) -> Self {
        let cache = Self::compute_properties(input.schema());
        Self { input, k, cache }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl Debug for TopKExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopKExec")
    }
}

impl DisplayAs for TopKExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TopKExec: k={}", self.k)
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for TopKExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

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

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("TopKExec invalid partition {partition}");
        }

        Ok(Box::pin(TopKReader {
            input: self.input.execute(partition, context)?,
            k: self.k,
            done: false,
            state: BTreeMap::new(),
        }))
    }

    fn statistics(&self) -> Result<Statistics> {
        // to improve the optimizability of this plan
        // better statistics inference could be provided
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

// A very specialized TopK implementation
struct TopKReader {
    /// The input to read data from
    input: SendableRecordBatchStream,
    /// Maximum number of output values
    k: usize,
    /// Have we produced the output yet?
    done: bool,
    /// Output
    state: BTreeMap<i64, String>,
}

/// Keeps track of the revenue from customer_id and stores if it
/// is the top values we have seen so far.
fn add_row(
    top_values: &mut BTreeMap<i64, String>,
    customer_id: &str,
    revenue: i64,
    k: &usize,
) {
    top_values.insert(revenue, customer_id.into());
    // only keep top k
    while top_values.len() > *k {
        remove_lowest_value(top_values)
    }
}

fn remove_lowest_value(top_values: &mut BTreeMap<i64, String>) {
    if !top_values.is_empty() {
        let smallest_revenue = {
            let (revenue, _) = top_values.iter().next().unwrap();
            *revenue
        };
        top_values.remove(&smallest_revenue);
    }
}

fn accumulate_batch(
    input_batch: &RecordBatch,
    mut top_values: BTreeMap<i64, String>,
    k: &usize,
) -> BTreeMap<i64, String> {
    let num_rows = input_batch.num_rows();
    // Assuming the input columns are
    // column[0]: customer_id / UTF8
    // column[1]: revenue: Int64
    let customer_id =
        as_string_array(input_batch.column(0)).expect("Column 0 is not customer_id");

    let revenue = as_int64_array(input_batch.column(1)).unwrap();

    for row in 0..num_rows {
        add_row(
            &mut top_values,
            customer_id.value(row),
            revenue.value(row),
            k,
        );
    }
    top_values
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
        // this aggregates and thus returns a single RecordBatch.

        // take this as immutable
        let k = self.k;
        let schema = self.schema();
        let poll = self.input.poll_next_unpin(cx);

        match poll {
            Poll::Ready(Some(Ok(batch))) => {
                self.state = accumulate_batch(&batch, self.state.clone(), &k);
                Poll::Ready(Some(Ok(RecordBatch::new_empty(schema))))
            }
            Poll::Ready(None) => {
                self.done = true;
                let (revenue, customer): (Vec<i64>, Vec<&String>) =
                    self.state.iter().rev().unzip();

                let customer: Vec<&str> = customer.iter().map(|&s| &**s).collect();
                Poll::Ready(Some(
                    RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(StringArray::from(customer)),
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

#[derive(Default, Debug)]
struct MyAnalyzerRule {}

impl AnalyzerRule for MyAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Self::analyze_plan(plan)
    }

    fn name(&self) -> &str {
        "my_analyzer_rule"
    }
}

impl MyAnalyzerRule {
    fn analyze_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.transform(|plan| {
            Ok(match plan {
                LogicalPlan::Projection(projection) => {
                    let expr = Self::analyze_expr(projection.expr.clone())?;
                    Transformed::yes(LogicalPlan::Projection(Projection::try_new(
                        expr,
                        projection.input,
                    )?))
                }
                _ => Transformed::no(plan),
            })
        })
        .data()
    }

    fn analyze_expr(expr: Vec<Expr>) -> Result<Vec<Expr>> {
        expr.into_iter()
            .map(|e| {
                e.transform(|e| {
                    Ok(match e {
                        Expr::Literal(ScalarValue::Int64(i)) => {
                            // transform to UInt64
                            Transformed::yes(Expr::Literal(ScalarValue::UInt64(
                                i.map(|i| i as u64),
                            )))
                        }
                        _ => Transformed::no(e),
                    })
                })
                .data()
            })
            .collect()
    }
}
```

DataFusion supports extension of operators by transforming logical plan and execution plan through customized [optimizer rules](https://docs.rs/datafusion/latest/datafusion/optimizer/trait.OptimizerRule.html). This section will use the µWheel project to illustrate such capabilities.

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
    // Attemps to rewrite a logical plan to a uwheel-based plan that either provides
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
