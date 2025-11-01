## User-Defined Plan Example: TopK Operator

This example demonstrates creating a custom TopK operator that optimizes "find the top K elements" queries.

### Background

A "Top K" node is a common query optimization used for queries like "find the top 3 customers by revenue". 

**Example SQL:**
```sql
CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT)
  STORED AS CSV location 'tests/data/customer.csv';

SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
```

**Naive Plan:**
The standard approach fully sorts the input before discarding everything except the top 3 elements.

**Optimized TopK Plan:**
Instead of sorting everything, we maintain only the top K elements in memory, significantly reducing buffer requirements.

### Implementation

The TopK implementation consists of several key components:

#### 1. TopKPlanNode - The Logical Plan Node
```rust,ignore
#[derive(PartialEq, Eq, PartialOrd, Hash)]
struct TopKPlanNode {
    k: usize,
    input: LogicalPlan,
    expr: SortExpr,
}

impl UserDefinedLogicalNodeCore for TopKPlanNode {
    fn name(&self) -> &str {
        "TopK"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.expr.expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopK: k={}", self.k)
    }

    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            k: self.k,
            input: inputs.swap_remove(0),
            expr: self.expr.with_expr(exprs.swap_remove(0)),
        })
    }
}
```

#### 2. TopKOptimizerRule - Rewrites Plans
```rust,ignore
impl OptimizerRule for TopKOptimizerRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Look for pattern: Limit -> Sort and replace with TopK
        let LogicalPlan::Limit(ref limit) = plan else {
            return Ok(Transformed::no(plan));
        };
        
        let FetchType::Literal(Some(fetch)) = limit.get_fetch_type()? else {
            return Ok(Transformed::no(plan));
        };
        
        if let LogicalPlan::Sort(Sort { ref expr, ref input, .. }) = limit.input.as_ref() {
            if expr.len() == 1 {
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

#### 3. TopKPlanner - Creates Physical Plan
```rust,ignore
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

#### 4. TopKExec - Physical Execution
```rust,ignore
struct TopKExec {
    input: Arc<dyn ExecutionPlan>,
    k: usize,
    cache: PlanProperties,
}

impl ExecutionPlan for TopKExec {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(TopKReader {
            input: self.input.execute(partition, context)?,
            k: self.k,
            done: false,
            state: BTreeMap::new(),
        }))
    }
}
```

### Usage

To use the TopK operator in your queries:
```rust,ignore
let config = SessionConfig::new().with_target_partitions(48);
let state = SessionStateBuilder::new()
    .with_config(config)
    .with_query_planner(Arc::new(TopKQueryPlanner {}))
    .with_optimizer_rule(Arc::new(TopKOptimizerRule::default()))
    .build();
    
let ctx = SessionContext::new_with_state(state);
```

For the complete implementation, see `datafusion/core/tests/user_defined/user_defined_plan.rs`.