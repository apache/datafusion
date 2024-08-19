// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This module contains an end to end demonstration of creating
//! a user defined operator in DataFusion.
//!
//! Specifically, it shows how to define a `TopKNode` that implements
//! `ExtensionPlanNode`, add an OptimizerRule to rewrite a
//! `LogicalPlan` to use that node a `LogicalPlan`, create an
//! `ExecutionPlan` and finally produce results.
//!
//! # TopK Background:
//!
//! A "Top K" node is a common query optimization which is used for
//! queries such as "find the top 3 customers by revenue". The
//! (simplified) SQL for such a query might be:
//!
//! ```sql
//! CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT)
//!   STORED AS CSV location 'tests/data/customer.csv';
//!
//! SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
//! ```
//!
//! And a naive plan would be:
//!
//! ```
//! > explain SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
//! +--------------+----------------------------------------+
//! | plan_type    | plan                                   |
//! +--------------+----------------------------------------+
//! | logical_plan | Limit: 3                               |
//! |              |   Sort: revenue DESC NULLS FIRST      |
//! |              |     Projection: customer_id, revenue |
//! |              |       TableScan: sales |
//! +--------------+----------------------------------------+
//! ```
//!
//! While this plan produces the correct answer, the careful reader
//! will note it fully sorts the input before discarding everything
//! other than the top 3 elements.
//!
//! The same answer can be produced by simply keeping track of the top
//! N elements, reducing the total amount of required buffer memory.
//!

use std::fmt::Debug;
use std::task::{Context, Poll};
use std::{any::Any, collections::BTreeMap, fmt, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};

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
        Expr, Extension, Limit, LogicalPlan, Sort, UserDefinedLogicalNode,
        UserDefinedLogicalNodeCore,
    },
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionMode, ExecutionPlan,
        Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
        Statistics,
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::ScalarValue;
use datafusion_expr::Projection;
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::AnalyzerRule;

/// Execute the specified sql and return the resulting record batches
/// pretty printed as a String.
async fn exec_sql(ctx: &SessionContext, sql: &str) -> Result<String> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    pretty_format_batches(&batches)
        .map_err(|e| arrow_datafusion_err!(e))
        .map(|d| d.to_string())
}

/// Create a test table.
async fn setup_table(ctx: SessionContext) -> Result<SessionContext> {
    let sql = "
        CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT)
        STORED AS CSV location 'tests/data/customer.csv'
        OPTIONS('format.has_header' 'false')
    ";

    let expected = vec!["++", "++"];

    let s = exec_sql(&ctx, sql).await?;
    let actual = s.lines().collect::<Vec<_>>();

    assert_eq!(expected, actual, "Creating table");
    Ok(ctx)
}

async fn setup_table_without_schemas(ctx: SessionContext) -> Result<SessionContext> {
    let sql = "
        CREATE EXTERNAL TABLE sales
        STORED AS CSV location 'tests/data/customer.csv'
        OPTIONS('format.has_header' 'false')
    ";

    let expected = vec!["++", "++"];

    let s = exec_sql(&ctx, sql).await?;
    let actual = s.lines().collect::<Vec<_>>();

    assert_eq!(expected, actual, "Creating table");
    Ok(ctx)
}

const QUERY: &str =
    "SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3";

const QUERY1: &str = "SELECT * FROM sales limit 3";

const QUERY2: &str = "SELECT 42, arrow_typeof(42)";

// Run the query using the specified execution context and compare it
// to the known result
async fn run_and_compare_query(ctx: SessionContext, description: &str) -> Result<()> {
    let expected = vec![
        "+-------------+---------+",
        "| customer_id | revenue |",
        "+-------------+---------+",
        "| paul        | 300     |",
        "| jorge       | 200     |",
        "| andy        | 150     |",
        "+-------------+---------+",
    ];

    let s = exec_sql(&ctx, QUERY).await?;
    let actual = s.lines().collect::<Vec<_>>();

    assert_eq!(
        expected,
        actual,
        "output mismatch for {}. Expectedn\n{}Actual:\n{}",
        description,
        expected.join("\n"),
        s
    );
    Ok(())
}

// Run the query using the specified execution context and compare it
// to the known result
async fn run_and_compare_query_with_analyzer_rule(
    ctx: SessionContext,
    description: &str,
) -> Result<()> {
    let expected = vec![
        "+------------+--------------------------+",
        "| UInt64(42) | arrow_typeof(UInt64(42)) |",
        "+------------+--------------------------+",
        "| 42         | UInt64                   |",
        "+------------+--------------------------+",
    ];

    let s = exec_sql(&ctx, QUERY2).await?;
    let actual = s.lines().collect::<Vec<_>>();

    assert_eq!(
        expected,
        actual,
        "output mismatch for {}. Expectedn\n{}Actual:\n{}",
        description,
        expected.join("\n"),
        s
    );
    Ok(())
}

// Run the query using the specified execution context and compare it
// to the known result
async fn run_and_compare_query_with_auto_schemas(
    ctx: SessionContext,
    description: &str,
) -> Result<()> {
    let expected = vec![
        "+----------+----------+",
        "| column_1 | column_2 |",
        "+----------+----------+",
        "| andrew   | 100      |",
        "| jorge    | 200      |",
        "| andy     | 150      |",
        "+----------+----------+",
    ];

    let s = exec_sql(&ctx, QUERY1).await?;
    let actual = s.lines().collect::<Vec<_>>();

    assert_eq!(
        expected,
        actual,
        "output mismatch for {}. Expectedn\n{}Actual:\n{}",
        description,
        expected.join("\n"),
        s
    );
    Ok(())
}

#[tokio::test]
// Run the query using default planners and optimizer
async fn normal_query_without_schemas() -> Result<()> {
    let ctx = setup_table_without_schemas(SessionContext::new()).await?;
    run_and_compare_query_with_auto_schemas(ctx, "Default context").await
}

#[tokio::test]
// Run the query using default planners and optimizer
async fn normal_query() -> Result<()> {
    let ctx = setup_table(SessionContext::new()).await?;
    run_and_compare_query(ctx, "Default context").await
}

#[tokio::test]
// Run the query using default planners, optimizer and custom analyzer rule
async fn normal_query_with_analyzer() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.add_analyzer_rule(Arc::new(MyAnalyzerRule {}));
    run_and_compare_query_with_analyzer_rule(ctx, "MyAnalyzerRule").await
}

#[tokio::test]
// Run the query using topk optimization
async fn topk_query() -> Result<()> {
    // Note the only difference is that the top
    let ctx = setup_table(make_topk_context()).await?;
    run_and_compare_query(ctx, "Topk context").await
}

#[tokio::test]
// Run EXPLAIN PLAN and show the plan was in fact rewritten
async fn topk_plan() -> Result<()> {
    let ctx = setup_table(make_topk_context()).await?;

    let mut expected = ["| logical_plan after topk                               | TopK: k=3                                                                     |",
        "|                                                       |   TableScan: sales projection=[customer_id,revenue]                                  |"].join("\n");

    let explain_query = format!("EXPLAIN VERBOSE {QUERY}");
    let actual_output = exec_sql(&ctx, &explain_query).await?;

    // normalize newlines (output on windows uses \r\n)
    let mut actual_output = actual_output.replace("\r\n", "\n");
    actual_output.retain(|x| !x.is_ascii_whitespace());
    expected.retain(|x| !x.is_ascii_whitespace());

    assert!(
        actual_output.contains(&expected),
        "Expected output not present in actual output\
        \nExpected:\
        \n---------\
        \n{expected}\
        \nActual:\
        \n--------\
        \n{actual_output}"
    );
    Ok(())
}

fn make_topk_context() -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(48);
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_query_planner(Arc::new(TopKQueryPlanner {}))
        .with_optimizer_rule(Arc::new(TopKOptimizerRule {}))
        .with_analyzer_rule(Arc::new(MyAnalyzerRule {}))
        .build();
    SessionContext::new_with_state(state)
}

// ------ The implementation of the TopK code follows -----

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

    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        // Note: this code simply looks for the pattern of a Limit followed by a
        // Sort and replaces it by a TopK node. It does not handle many
        // edge cases (e.g multiple sort columns, sort ASC / DESC), etc.
        if let LogicalPlan::Limit(Limit {
            fetch: Some(fetch),
            input,
            ..
        }) = &plan
        {
            if let LogicalPlan::Sort(Sort {
                ref expr,
                ref input,
                ..
            }) = **input
            {
                if expr.len() == 1 {
                    // we found a sort with a single sort expr, replace with a a TopK
                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(TopKPlanNode {
                            k: *fetch,
                            input: input.as_ref().clone(),
                            expr: expr[0].clone(),
                        }),
                    })));
                }
            }
        }

        Ok(Transformed::no(plan))
    }
}

#[derive(PartialEq, Eq, Hash)]
struct TopKPlanNode {
    k: usize,
    input: LogicalPlan,
    /// The sort expression (this example only supports a single sort
    /// expr)
    expr: Expr,
}

impl Debug for TopKPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
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

    /// Schema for TopK is the same as the input
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.expr.clone()]
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
            expr: exprs.swap_remove(0),
        })
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
    /// The maxium number of values
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
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl Debug for TopKExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopKExec")
    }
}

impl DisplayAs for TopKExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TopKExec: k={}", self.k)
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
