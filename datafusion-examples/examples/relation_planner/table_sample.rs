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

//! This example demonstrates using custom relation planners to implement
//! SQL TABLESAMPLE clause support.
//!
//! TABLESAMPLE allows sampling a fraction or number of rows from a table:
//!   - `SELECT * FROM table TABLESAMPLE BERNOULLI(10)` - 10% sample
//!   - `SELECT * FROM table TABLESAMPLE (100 ROWS)` - 100 rows
//!   - `SELECT * FROM table TABLESAMPLE (10 PERCENT) REPEATABLE(42)` - Reproducible

use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
    hash::{Hash, Hasher},
    ops::{Add, Div, Mul, Sub},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{ArrayRef, Int32Array, RecordBatch, StringArray, UInt32Array},
    compute,
};
use arrow_schema::SchemaRef;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, Poisson};
use tonic::async_trait;

use datafusion::{
    execution::{
        context::QueryPlanner, RecordBatchStream, SendableRecordBatchStream,
        SessionState, SessionStateBuilder, TaskContext,
    },
    physical_expr::EquivalenceProperties,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput},
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    prelude::*,
};
use datafusion_common::{
    internal_err, not_impl_err, plan_datafusion_err, plan_err, DFSchemaRef,
    DataFusionError, Result, Statistics,
};
use datafusion_expr::{
    logical_plan::{Extension, LogicalPlan, LogicalPlanBuilder},
    planner::{
        PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    },
    UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_sql::sqlparser::ast::{
    self, TableFactor, TableSampleMethod, TableSampleUnit,
};

/// This example demonstrates using custom relation planners to implement
/// SQL TABLESAMPLE clause support.
pub async fn table_sample() -> Result<()> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(TableSampleQueryPlanner {}))
        .build();

    let ctx = SessionContext::new_with_state(state.clone());

    // Register sample data table
    register_sample_data(&ctx)?;

    // Register custom planner
    ctx.register_relation_planner(Arc::new(TableSamplePlanner))?;

    println!("Custom Relation Planner: TABLESAMPLE Support");
    println!("============================================\n");
    println!("Note: This shows logical planning for TABLESAMPLE.");
    println!("Physical execution requires additional implementation.\n");

    // Example 1: Full table without any sampling (baseline)
    // Shows: Complete dataset with all 10 rows (1-10 with row_1 to row_10)
    // Expected: 10 rows showing the full sample_data table
    // Actual:
    // +---------+---------+
    // | column1 | column2 |
    // +---------+---------+
    // | 1       | row_1   |
    // | 2       | row_2   |
    // | 3       | row_3   |
    // | 4       | row_4   |
    // | 5       | row_5   |
    // | 6       | row_6   |
    // | 7       | row_7   |
    // | 8       | row_8   |
    // | 9       | row_9   |
    // | 10      | row_10  |
    // +---------+---------+
    run_example(
        &ctx,
        "Example 1: Full table (no sampling)",
        "SELECT * FROM sample_data",
    )
    .await?;

    // Example 2: TABLESAMPLE with BERNOULLI sampling at 30% probability
    // Shows: Random sampling where each row has 30% chance of being selected
    // Expected: ~3 rows (varies due to randomness) from the 10-row dataset
    // Actual:
    // +---------+---------+
    // | column1 | column2 |
    // +---------+---------+
    // | 4       | row_4   |
    // | 6       | row_6   |
    // | 9       | row_9   |
    // +---------+---------+
    run_example(
        &ctx,
        "Example 2: TABLESAMPLE with percentage",
        "SELECT * FROM sample_data TABLESAMPLE BERNOULLI(30 PERCENT)",
    )
    .await?;

    // Example 3: TABLESAMPLE with fractional sampling (50% of data)
    // Shows: Random sampling using decimal fraction instead of percentage
    // Expected: ~5 rows (varies due to randomness) from the 10-row dataset
    // Actual:
    // +---------+---------+
    // | column1 | column2 |
    // +---------+---------+
    // | 3       | row_3   |
    // | 4       | row_4   |
    // | 5       | row_5   |
    // +---------+---------+
    run_example(
        &ctx,
        "Example 3: TABLESAMPLE with fraction",
        "SELECT * FROM sample_data TABLESAMPLE (0.5)",
    )
    .await?;

    // Example 4: TABLESAMPLE with REPEATABLE seed for reproducible results
    // Shows: Deterministic sampling using a fixed seed for consistent results
    // Expected: Same rows selected each time due to fixed seed (42)
    // Actual:
    // +---------+---------+
    // | column1 | column2 |
    // +---------+---------+
    // | 5       | row_5   |
    // | 9       | row_9   |
    // | 10      | row_10  |
    // +---------+---------+
    run_example(
        &ctx,
        "Example 4: TABLESAMPLE with REPEATABLE seed",
        "SELECT * FROM sample_data TABLESAMPLE (0.3) REPEATABLE(42)",
    )
    .await?;

    // Example 5: TABLESAMPLE with exact row count limit
    // Shows: Sampling by limiting to a specific number of rows (not probabilistic)
    // Expected: Exactly 3 rows (first 3 rows from the dataset)
    // Actual:
    // +---------+---------+
    // | column1 | column2 |
    // +---------+---------+
    // | 1       | row_1   |
    // | 2       | row_2   |
    // | 3       | row_3   |
    // +---------+---------+
    run_example(
        &ctx,
        "Example 5: TABLESAMPLE with row count",
        "SELECT * FROM sample_data TABLESAMPLE (3 ROWS)",
    )
    .await?;

    // Example 6: TABLESAMPLE combined with WHERE clause filtering
    // Shows: How sampling works with other query operations like filtering
    // Expected: 3 rows where column1 > 2 (from the 5-row sample)
    // Actual:
    // +---------+---------+
    // | column1 | column2 |
    // +---------+---------+
    // | 3       | row_3   |
    // | 4       | row_4   |
    // | 5       | row_5   |
    // +---------+---------+
    run_example(
        &ctx,
        "Example 6: TABLESAMPLE with WHERE clause",
        r#"SELECT * FROM sample_data 
           TABLESAMPLE (5 ROWS) 
           WHERE column1 > 2"#,
    )
    .await?;

    // Example 7: JOIN between two independently sampled tables
    // Shows: How sampling works in complex queries with multiple table references
    // Expected: Rows where both sampled tables have matching column1 values
    // Actual:
    // +---------+---------+---------+---------+
    // | column1 | column1 | column2 | column2 |
    // +---------+---------+---------+---------+
    // | 2       | 2       | row_2   | row_2   |
    // | 8       | 8       | row_8   | row_8   |
    // | 10      | 10      | row_10  | row_10  |
    // +---------+---------+---------+---------+
    run_example(
        &ctx,
        "Example 7: JOIN between two different TABLESAMPLE tables",
        r#"SELECT t1.column1, t2.column1, t1.column2, t2.column2 
           FROM sample_data t1 TABLESAMPLE (0.7) 
           JOIN sample_data t2 TABLESAMPLE (0.7) 
           ON t1.column1 = t2.column1"#,
    )
    .await?;

    Ok(())
}

/// Register sample data table for the examples
fn register_sample_data(ctx: &SessionContext) -> Result<()> {
    // Create sample_data table with 10 rows: column1 (1-10), column2 (row_1 to row_10)
    let column1: ArrayRef = Arc::new(Int32Array::from((1..=10).collect::<Vec<i32>>()));
    let column2: ArrayRef = Arc::new(StringArray::from(
        (1..=10)
            .map(|i| format!("row_{i}"))
            .collect::<Vec<String>>(),
    ));
    let batch =
        RecordBatch::try_from_iter(vec![("column1", column1), ("column2", column2)])?;
    ctx.register_batch("sample_data", batch)?;

    Ok(())
}

async fn run_example(ctx: &SessionContext, title: &str, sql: &str) -> Result<()> {
    println!("{title}:\n{sql}\n");
    let df = ctx.sql(sql).await?;
    println!("Logical Plan:\n{}\n", df.logical_plan().display_indent());
    df.show().await?;
    Ok(())
}

/// Hashable and comparable f64 for sampling bounds
#[derive(Debug, Clone, Copy, PartialOrd)]
struct Bound(f64);

impl PartialEq for Bound {
    fn eq(&self, other: &Self) -> bool {
        (self.0 - other.0).abs() < f64::EPSILON
    }
}

impl Eq for Bound {}

impl Hash for Bound {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the bits of the f64
        self.0.to_bits().hash(state);
    }
}

impl From<f64> for Bound {
    fn from(value: f64) -> Self {
        Self(value)
    }
}
impl From<Bound> for f64 {
    fn from(value: Bound) -> Self {
        value.0
    }
}

impl AsRef<f64> for Bound {
    fn as_ref(&self) -> &f64 {
        &self.0
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, PartialOrd)]
struct TableSamplePlanNode {
    inner_plan: LogicalPlan,

    lower_bound: Bound,
    upper_bound: Bound,
    with_replacement: bool,
    seed: u64,
}

impl TableSamplePlanNode {
    pub fn new(
        input: LogicalPlan,
        fraction: f64,
        with_replacement: Option<bool>,
        seed: Option<u64>,
    ) -> Self {
        TableSamplePlanNode {
            inner_plan: input,
            lower_bound: Bound::from(0.0),
            upper_bound: Bound::from(fraction),
            with_replacement: with_replacement.unwrap_or(false),
            seed: seed.unwrap_or_else(rand::random),
        }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl UserDefinedLogicalNodeCore for TableSamplePlanNode {
    fn name(&self) -> &str {
        "TableSample"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.inner_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.inner_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        f.write_fmt(format_args!(
            "Sample: {:?} {:?} {:?}",
            self.lower_bound, self.upper_bound, self.seed
        ))
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let input = inputs
            .first()
            .ok_or(DataFusionError::Plan("Should have input".into()))?;
        Ok(Self {
            inner_plan: input.clone(),
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            with_replacement: self.with_replacement,
            seed: self.seed,
        })
    }
}

/// Execution planner with `SampleExec` for `TableSamplePlanNode`
struct TableSampleExtensionPlanner {}

impl TableSampleExtensionPlanner {
    fn build_execution_plan(
        &self,
        specific_node: &TableSamplePlanNode,
        physical_input: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SampleExec {
            input: Arc::clone(physical_input),
            lower_bound: 0.0,
            upper_bound: specific_node.upper_bound.into(),
            with_replacement: specific_node.with_replacement,
            seed: specific_node.seed,
            metrics: Default::default(),
            cache: SampleExec::compute_properties(physical_input),
        }))
    }
}

#[async_trait]
impl ExtensionPlanner for TableSampleExtensionPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(specific_node) = node.as_any().downcast_ref::<TableSamplePlanNode>() {
            println!("Extension planner plan_extension: {:?}", &logical_inputs);
            assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");

            let exec_plan =
                self.build_execution_plan(specific_node, &physical_inputs[0])?;
            Ok(Some(exec_plan))
        } else {
            Ok(None)
        }
    }
}

/// Query planner supporting a `TableSampleExtensionPlanner`
#[derive(Debug)]
struct TableSampleQueryPlanner {}

#[async_trait]
impl QueryPlanner for TableSampleQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Additional extension for table sample node
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                TableSampleExtensionPlanner {},
            )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Physical plan implementation
trait Sampler: Send + Sync {
    fn sample(&mut self, batch: &RecordBatch) -> Result<RecordBatch>;
}

struct BernoulliSampler {
    lower_bound: f64,
    upper_bound: f64,
    rng: StdRng,
}

impl BernoulliSampler {
    fn new(lower_bound: f64, upper_bound: f64, seed: u64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Sampler for BernoulliSampler {
    fn sample(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.upper_bound <= self.lower_bound {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let mut indices = Vec::new();

        for i in 0..batch.num_rows() {
            let rnd: f64 = self.rng.random();

            if rnd >= self.lower_bound && rnd < self.upper_bound {
                indices.push(i as u32);
            }
        }

        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }
        let indices = UInt32Array::from(indices);
        compute::take_record_batch(batch, &indices).map_err(|e| e.into())
    }
}

struct PoissonSampler {
    ratio: f64,
    poisson: Poisson<f64>,
    rng: StdRng,
}

impl PoissonSampler {
    fn try_new(ratio: f64, seed: u64) -> Result<Self> {
        let poisson = Poisson::new(ratio).map_err(|e| plan_datafusion_err!("{}", e))?;
        Ok(Self {
            ratio,
            poisson,
            rng: StdRng::seed_from_u64(seed),
        })
    }
}

impl Sampler for PoissonSampler {
    fn sample(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.ratio <= 0.0 {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let mut indices = Vec::new();

        for i in 0..batch.num_rows() {
            let k = self.poisson.sample(&mut self.rng) as i32;
            for _ in 0..k {
                indices.push(i as u32);
            }
        }

        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let indices = UInt32Array::from(indices);
        compute::take_record_batch(batch, &indices).map_err(|e| e.into())
    }
}

/// SampleExec samples rows from its input based on a sampling method.
/// This is used to implement SQL `SAMPLE` clause.
#[derive(Debug, Clone)]
pub struct SampleExec {
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// The lower bound of the sampling ratio
    lower_bound: f64,
    /// The upper bound of the sampling ratio
    upper_bound: f64,
    /// Whether to sample with replacement
    with_replacement: bool,
    /// Random seed for reproducible sampling
    seed: u64,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Properties equivalence properties, partitioning, etc.
    cache: PlanProperties,
}

impl SampleExec {
    /// Create a new SampleExec with a custom sampling method
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        lower_bound: f64,
        upper_bound: f64,
        with_replacement: bool,
        seed: u64,
    ) -> Result<Self> {
        if lower_bound < 0.0 || upper_bound > 1.0 || lower_bound > upper_bound {
            return internal_err!(
                "Sampling bounds must be between 0.0 and 1.0, and lower_bound <= upper_bound, got [{}, {}]",
                lower_bound, upper_bound
            );
        }

        let cache = Self::compute_properties(&input);

        Ok(Self {
            input,
            lower_bound,
            upper_bound,
            with_replacement,
            seed,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn create_sampler(&self, partition: usize) -> Result<Box<dyn Sampler>> {
        if self.with_replacement {
            Ok(Box::new(PoissonSampler::try_new(
                self.upper_bound - self.lower_bound,
                self.seed + partition as u64,
            )?))
        } else {
            Ok(Box::new(BernoulliSampler::new(
                self.lower_bound,
                self.upper_bound,
                self.seed + partition as u64,
            )))
        }
    }

    /// Whether to sample with replacement
    #[allow(dead_code)]
    pub fn with_replacement(&self) -> bool {
        self.with_replacement
    }

    /// The lower bound of the sampling ratio
    #[allow(dead_code)]
    pub fn lower_bound(&self) -> f64 {
        self.lower_bound
    }

    /// The upper bound of the sampling ratio
    #[allow(dead_code)]
    pub fn upper_bound(&self) -> f64 {
        self.upper_bound
    }

    /// The random seed
    #[allow(dead_code)]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The input plan
    #[allow(dead_code)]
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        input
            .properties()
            .clone()
            .with_eq_properties(EquivalenceProperties::new(input.schema()))
    }
}

impl DisplayAs for SampleExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SampleExec: lower_bound={}, upper_bound={}, with_replacement={}, seed={}",
                    self.lower_bound, self.upper_bound, self.with_replacement, self.seed
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "SampleExec: lower_bound={}, upper_bound={}, with_replacement={}, seed={}",
                    self.lower_bound, self.upper_bound, self.with_replacement, self.seed
                )
            }
        }
    }
}

impl ExecutionPlan for SampleExec {
    fn name(&self) -> &'static str {
        "SampleExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false] // Sampling does not maintain input order
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SampleExec::try_new(
            Arc::clone(&children[0]),
            self.lower_bound,
            self.upper_bound,
            self.with_replacement,
            self.seed,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(SampleExecStream {
            input: input_stream,
            sampler: self.create_sampler(partition)?,
            baseline_metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let input_stats = self.input.partition_statistics(partition)?;

        // Apply sampling ratio to statistics
        let mut stats = input_stats;
        let ratio = self.upper_bound - self.lower_bound;

        stats.num_rows = stats
            .num_rows
            .map(|nr| (nr as f64 * ratio) as usize)
            .to_inexact();
        stats.total_byte_size = stats
            .total_byte_size
            .map(|tb| (tb as f64 * ratio) as usize)
            .to_inexact();

        Ok(stats)
    }
}

/// Stream for the SampleExec operator
struct SampleExecStream {
    /// The input stream
    input: SendableRecordBatchStream,
    /// The sampling method
    sampler: Box<dyn Sampler>,
    /// Runtime metrics recording
    baseline_metrics: BaselineMetrics,
}

impl Stream for SampleExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let start = self.baseline_metrics.elapsed_compute().clone();
                let result = self.sampler.sample(&batch);
                let result = result.record_output(&self.baseline_metrics);
                let _timer = start.timer();
                Poll::Ready(Some(result))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for SampleExecStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Helper to evaluate numeric SQL expressions
fn evaluate_number<
    T: FromStr + Add<Output = T> + Sub<Output = T> + Mul<Output = T> + Div<Output = T>,
>(
    expr: &ast::Expr,
) -> Option<T> {
    match expr {
        ast::Expr::BinaryOp { left, op, right } => {
            let left = evaluate_number::<T>(left);
            let right = evaluate_number::<T>(right);
            match (left, right) {
                (Some(left), Some(right)) => match op {
                    ast::BinaryOperator::Plus => Some(left + right),
                    ast::BinaryOperator::Minus => Some(left - right),
                    ast::BinaryOperator::Multiply => Some(left * right),
                    ast::BinaryOperator::Divide => Some(left / right),
                    _ => None,
                },
                _ => None,
            }
        }
        ast::Expr::Value(value) => match &value.value {
            ast::Value::Number(value, _) => {
                let value = value.to_string();
                let Ok(value) = value.parse::<T>() else {
                    return None;
                };
                Some(value)
            }
            _ => None,
        },
        _ => None,
    }
}

/// Custom relation planner that handles TABLESAMPLE clauses
#[derive(Debug)]
struct TableSamplePlanner;

impl RelationPlanner for TableSamplePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Table {
                sample: Some(sample),
                alias,
                name,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                index_hints,
            } => {
                println!("[TableSamplePlanner] Processing TABLESAMPLE clause");

                let sample = match sample {
                    ast::TableSampleKind::BeforeTableAlias(sample) => sample,
                    ast::TableSampleKind::AfterTableAlias(sample) => sample,
                };
                if let Some(name) = &sample.name {
                    if *name != TableSampleMethod::Bernoulli
                        && *name != TableSampleMethod::Row
                    {
                        // Postgres-style sample. Not supported because DataFusion does not have a concept of pages like PostgreSQL.
                        return not_impl_err!("{} is not supported yet", name);
                    }
                }
                if sample.offset.is_some() {
                    // Clickhouse-style sample. Not supported because it requires knowing the total data size.
                    return not_impl_err!("Offset sample is not supported yet");
                }

                let seed = sample
                    .seed
                    .map(|seed| {
                        let Ok(seed) = seed.value.to_string().parse::<u64>() else {
                            return plan_err!("seed must be a number: {}", seed.value);
                        };
                        Ok(seed)
                    })
                    .transpose()?;

                let sampleless_relation = TableFactor::Table {
                    sample: None,
                    alias: alias.clone(),
                    name: name.clone(),
                    args: args.clone(),
                    with_hints: with_hints.clone(),
                    version: version.clone(),
                    with_ordinality,
                    partitions: partitions.clone(),
                    json_path: json_path.clone(),
                    index_hints: index_hints.clone(),
                };
                let input = context.plan(sampleless_relation)?;

                if let Some(bucket) = sample.bucket {
                    if bucket.on.is_some() {
                        // Hive-style sample, only used when the Hive table is defined with CLUSTERED BY
                        return not_impl_err!(
                            "Bucket sample with ON is not supported yet"
                        );
                    }

                    let Ok(bucket_num) = bucket.bucket.to_string().parse::<u64>() else {
                        return plan_err!("bucket must be a number");
                    };

                    let Ok(total_num) = bucket.total.to_string().parse::<u64>() else {
                        return plan_err!("total must be a number");
                    };
                    let value = bucket_num as f64 / total_num as f64;
                    let plan =
                        TableSamplePlanNode::new(input, value, None, seed).into_plan();
                    return Ok(RelationPlanning::Planned(PlannedRelation::new(
                        plan, alias,
                    )));
                }
                if let Some(quantity) = sample.quantity {
                    return match quantity.unit {
                        Some(TableSampleUnit::Rows) => {
                            let value = evaluate_number::<i64>(&quantity.value);
                            if value.is_none() {
                                return plan_err!(
                                    "quantity must be a number: {:?}",
                                    quantity.value
                                );
                            }
                            let value = value.unwrap();
                            if value < 0 {
                                return plan_err!(
                                    "quantity must be a non-negative number: {:?}",
                                    quantity.value
                                );
                            }
                            Ok(RelationPlanning::Planned(PlannedRelation::new(
                                LogicalPlanBuilder::from(input)
                                    .limit(0, Some(value as usize))?
                                    .build()?,
                                alias,
                            )))
                        }
                        Some(TableSampleUnit::Percent) => {
                            let value = evaluate_number::<f64>(&quantity.value);
                            if value.is_none() {
                                return plan_err!(
                                    "quantity must be a number: {:?}",
                                    quantity.value
                                );
                            }
                            let value = value.unwrap() / 100.0;
                            let plan = TableSamplePlanNode::new(input, value, None, seed)
                                .into_plan();
                            Ok(RelationPlanning::Planned(PlannedRelation::new(
                                plan, alias,
                            )))
                        }
                        None => {
                            // Clickhouse-style sample
                            let value = evaluate_number::<f64>(&quantity.value);
                            if value.is_none() {
                                return plan_err!(
                                    "quantity must be a valid number: {:?}",
                                    quantity.value
                                );
                            }
                            let value = value.unwrap();
                            if value < 0.0 {
                                return plan_err!(
                                    "quantity must be a non-negative number: {:?}",
                                    quantity.value
                                );
                            }
                            if value >= 1.0 {
                                // If value is larger than 1, it is a row limit
                                Ok(RelationPlanning::Planned(PlannedRelation::new(
                                    LogicalPlanBuilder::from(input)
                                        .limit(0, Some(value as usize))?
                                        .build()?,
                                    alias,
                                )))
                            } else {
                                // If value is between 0.0 and 1.0, it is a fraction
                                let plan =
                                    TableSamplePlanNode::new(input, value, None, seed)
                                        .into_plan();
                                Ok(RelationPlanning::Planned(PlannedRelation::new(
                                    plan, alias,
                                )))
                            }
                        }
                    };
                }
                plan_err!("Cannot plan sample SQL")
            }
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}
