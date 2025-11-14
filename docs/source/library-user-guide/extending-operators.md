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

DataFusion supports extension of operators by transforming logical plan and execution plan through customized [optimizer rules](https://docs.rs/datafusion/latest/datafusion/optimizer/trait.OptimizerRule.html). This section demonstrates these capabilities through two examples.

## Example 1: DataFusion µWheel

[DataFusion µWheel](https://github.com/uwheel/datafusion-uwheel/tree/main) is a native DataFusion optimizer which improves query performance for time-based analytics through fast temporal aggregation and pruning using custom indices. The integration of µWheel into DataFusion is a joint effort with the DataFusion community.

### Optimizing Logical Plan

The `rewrite` function transforms logical plans by identifying temporal patterns and aggregation functions that match the stored wheel indices. When match is found, it queries the corresponding index to retrieve pre-computed aggregate values, stores these results in a [MemTable](https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html), and returns as a new `LogicalPlan::TableScan`. If no match is found, the original plan proceeds unchanged through DataFusion's standard execution path.
```rust,ignore
fn rewrite(
  &self,
  plan: LogicalPlan,
  _config: &dyn OptimizerConfig,
) -> Result<Transformed> {
    // Attempts to rewrite a logical plan to a uwheel-based plan that either provides
    // plan-time aggregates or skips execution based on min/max pruning.
    if let Some(rewritten) = self.try_rewrite(&plan) {
        Ok(Transformed::yes(rewritten))
    } else {
        Ok(Transformed::no(plan))
    }
}

// Converts a uwheel aggregate result to a TableScan with a MemTable as source
fn agg_to_table_scan(result: f64, schema: SchemaRef) -> Result {
  let data = Float64Array::from(vec![result]);
  let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)])?;
  let df_schema = Arc::new(DFSchema::try_from(schema.clone())?);
  let mem_table = MemTable::try_new(schema, vec![vec![record_batch]])?;
  mem_table_as_table_scan(mem_table, df_schema)
}
```

To get a deeper dive into the usage of the µWheel project, visit the [blog post](https://uwheel.rs/post/datafusion_uwheel/) by Max Meldrum.

## Example 2: TopK Operator

This example demonstrates creating a custom TopK operator that optimizes queries like "find the top 3 customers by revenue". Instead of fully sorting the input and discarding all but the top K elements, the TopK operator maintains only the K largest elements in memory, significantly reducing memory usage.

### Overview

Creating a custom operator in DataFusion requires implementing four key components that work together:

1. **Logical Plan Node** (`TopKPlanNode`) - Represents the TopK operation in the logical plan
2. **Optimizer Rule** (`TopKOptimizerRule`) - Detects `LIMIT(SORT(...))` patterns and replaces them with TopK
3. **Physical Planner** (`TopKPlanner`) - Converts the logical TopK node into a physical execution plan
4. **Physical Execution** (`TopKExec`) - Executes the TopK algorithm on actual data

### Implementation

The optimizer rule identifies queries with a `LIMIT` clause applied to a `SORT` operation. When this pattern is detected, it replaces the combination with a single `TopK` node:
```rust,ignore
impl OptimizerRule for TopKOptimizerRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed> {
        // Match on LIMIT -> SORT pattern
        let LogicalPlan::Limit(ref limit) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let LogicalPlan::Sort(Sort { ref expr, ref input, .. }) = limit.input.as_ref() {
            if expr.len() == 1 {
                // Replace with TopK extension node
                return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(TopKPlanNode {
                        k: fetch,
                        input: input.as_ref().clone(),
                        expr: expr[0].clone(),
                    }),
                })));
            }
        }

        Ok(Transformed::no(plan))
    }
}
```

The logical plan node implements `UserDefinedLogicalNodeCore` to define how TopK fits into DataFusion's plan structure:
```rust,ignore
impl UserDefinedLogicalNodeCore for TopKPlanNode {
    fn name(&self) -> &str {
        "TopK"
    }

    fn inputs(&self) -> Vec {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec {
        vec![self.expr.expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopK: k={}", self.k)
    }
}
```

The physical planner bridges the logical and physical representations by implementing `ExtensionPlanner`:
```rust,ignore
impl ExtensionPlanner for TopKPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc],
        _session_state: &SessionState,
    ) -> Result<Option<Arc>> {
        // Convert TopKPlanNode to TopKExec
        if let Some(topk_node) = node.as_any().downcast_ref::() {
            Ok(Some(Arc::new(TopKExec::new(
                physical_inputs[0].clone(),
                topk_node.k,
            ))))
        } else {
            Ok(None)
        }
    }
}
```

Finally, the physical execution plan implements the actual TopK algorithm, maintaining only the K largest elements as it processes the input stream.

### Usage

To register the TopK operator with DataFusion:
```rust,ignore
let state = SessionStateBuilder::new()
    .with_query_planner(Arc::new(TopKQueryPlanner {}))
    .with_optimizer_rule(Arc::new(TopKOptimizerRule::default()))
    .build();

let ctx = SessionContext::new_with_state(state);
```

For the complete implementation including the physical execution details, see `datafusion/core/tests/user_defined/user_defined_plan.rs`.
```
