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

This module contains an end to end demonstration of creatinga user defined operator in DataFusion.12 .Specifically, it shows how to define a `TopKNode` that implements `ExtensionPlanNode` that add an OptimizerRule to rewrite a `LogicalPlan` to use that node as a `LogicalPlan`, create an `ExecutionPlan` and finally produce results.

## TopK Background:

A "Top K" node is a common query optimization which is used for queries such as "find the top 3 customers by revenue". The(simplified) SQL for such a query might be:

```sql
CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT)
  STORED AS CSV location 'tests/data/customer.csv';
SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
```

And a naive plan would be:

```sql
explain SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
```

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

While this plan produces the correct answer, the careful reader will note it fully sorts the input before discarding everythingother than the top 3 elements.
The same answer can be produced by simply keeping track of the top N elements, reducing the total amount of required buffer memory.
## Process for Defining Extending Operator
The below example illustrates the example of topK node :
### LogicalPlan Node Definition
- This section defines the custom logical plan node `TopKPlanNode`, which represents the `TopK` operation.
- It includes trait implementations like `UserDefinedLogicalNodeCore` and Debug.
code:
```rust
#[derive(Debug)]
struct TopKQueryPlanner {}

#[async_trait]
impl QueryPlanner for TopKQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan TopK nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                TopKPlanner {},
            )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
```
### Optimizer Rule
- Implements the `TopKOptimizerRule` to detect a `Limit` followed by a Sort and replace it with the `TopKPlanNode`.
- Includes the logic for transforming the logical plan.
code:
```rust
#[derive(Default, Debug)]
struct TopKOptimizerRule {
    /// A testing-only hashable fixture.
    invariant_mock: Option<InvariantMock>,
}

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

    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        // Note: this code simply looks for the pattern of a Limit followed by a
        // Sort and replaces it by a TopK node. It does not handle many
        // edge cases (e.g multiple sort columns, sort ASC / DESC), etc.
        let LogicalPlan::Limit(ref limit) = plan else {
            return Ok(Transformed::no(plan));
        };
        let FetchType::Literal(Some(fetch)) = limit.get_fetch_type()? else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::Sort(Sort {
            ref expr,
            ref input,
            ..
        }) = limit.input.as_ref()
        {
            if expr.len() == 1 {
                // we found a sort with a single sort expr, replace with a a TopK
                return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(TopKPlanNode {
                        k: fetch,
                        input: input.as_ref().clone(),
                        expr: expr[0].clone(),
                        invariant_mock: self.invariant_mock.clone(),
                    }),
                })));
            }
        }

        Ok(Transformed::no(plan))
    }
}
```
### Physical planner
-The  `TopKPlanner` is implemented to map the custom logical plan node (`TopKPlanNode`) to a physical execution plan (`TopKExec`).
```rust
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
```

### Physical Execution Plan
- Defines the physical execution operator `TopKExec` and its properties.
- Implements the `ExecutionPlan` trait to describe how the operator is executed.
code:
```rust
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
```
### Execution Logic
- Implements the `TopKReade`r, which processes input batches to calculate the top `k` values.
- Contains helper functions like `add_row`, `remove_lowest_value`, and `accumulate_batch` for execution logic.



```rust
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
```


