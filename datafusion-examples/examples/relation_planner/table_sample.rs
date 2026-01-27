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

//! # TABLESAMPLE Example
//!
//! This example demonstrates implementing SQL `TABLESAMPLE` support using
//! DataFusion's extensibility APIs.
//!
//! This is a working `TABLESAMPLE` implementation that can serve as a starting
//! point for your own projects. It also works as a template for adding other
//! custom SQL operators, covering the full pipeline from parsing to execution.
//!
//! It shows how to:
//!
//! 1. **Parse** TABLESAMPLE syntax via a custom [`RelationPlanner`]
//! 2. **Plan** sampling as a custom logical node ([`TableSamplePlanNode`])
//! 3. **Execute** sampling via a custom physical operator ([`SampleExec`])
//!
//! ## Supported Syntax
//!
//! ```sql
//! -- Bernoulli sampling (each row has N% chance of selection)
//! SELECT * FROM table TABLESAMPLE BERNOULLI(10 PERCENT)
//!
//! -- Fractional sampling (0.0 to 1.0)
//! SELECT * FROM table TABLESAMPLE (0.1)
//!
//! -- Row count limit
//! SELECT * FROM table TABLESAMPLE (100 ROWS)
//!
//! -- Reproducible sampling with a seed
//! SELECT * FROM table TABLESAMPLE (10 PERCENT) REPEATABLE(42)
//! ```
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         SQL Query                               │
//! │  SELECT * FROM t TABLESAMPLE BERNOULLI(10 PERCENT) REPEATABLE(1)│
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    TableSamplePlanner                           │
//! │    (RelationPlanner: parses TABLESAMPLE, creates logical node)  │
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   TableSamplePlanNode                           │
//! │         (UserDefinedLogicalNode: stores sampling params)        │
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                TableSampleExtensionPlanner                      │
//! │       (ExtensionPlanner: creates physical execution plan)       │
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        SampleExec                               │
//! │    (ExecutionPlan: performs actual row sampling at runtime)     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::datatypes::{Float64Type, Int64Type};
use arrow::{
    array::{ArrayRef, Int32Array, RecordBatch, StringArray, UInt32Array},
    compute,
};
use arrow_schema::SchemaRef;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use tonic::async_trait;

use datafusion::optimizer::simplify_expressions::simplify_literal::parse_literal;
use datafusion::{
    execution::{
        RecordBatchStream, SendableRecordBatchStream, SessionState, SessionStateBuilder,
        TaskContext, context::QueryPlanner,
    },
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput},
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    prelude::*,
};
use datafusion_common::{
    DFSchemaRef, DataFusionError, Result, Statistics, internal_err, not_impl_err,
    plan_datafusion_err, plan_err,
};
use datafusion_expr::{
    UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
    logical_plan::{Extension, LogicalPlan, LogicalPlanBuilder},
    planner::{
        PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    },
};
use datafusion_sql::sqlparser::ast::{
    self, TableFactor, TableSampleMethod, TableSampleUnit,
};
use insta::assert_snapshot;

// ============================================================================
// Example Entry Point
// ============================================================================

/// Runs the TABLESAMPLE examples demonstrating various sampling techniques.
pub async fn table_sample() -> Result<()> {
    // Build session with custom query planner for physical planning
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(TableSampleQueryPlanner))
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Register custom relation planner for logical planning
    ctx.register_relation_planner(Arc::new(TableSamplePlanner))?;
    register_sample_data(&ctx)?;

    println!("TABLESAMPLE Example");
    println!("===================\n");

    run_examples(&ctx).await
}

async fn run_examples(ctx: &SessionContext) -> Result<()> {
    // Example 1: Baseline - full table scan
    let results = run_example(
        ctx,
        "Example 1: Full table (baseline)",
        "SELECT * FROM sample_data",
    )
    .await?;
    assert_snapshot!(results, @r"
    +---------+---------+
    | column1 | column2 |
    +---------+---------+
    | 1       | row_1   |
    | 2       | row_2   |
    | 3       | row_3   |
    | 4       | row_4   |
    | 5       | row_5   |
    | 6       | row_6   |
    | 7       | row_7   |
    | 8       | row_8   |
    | 9       | row_9   |
    | 10      | row_10  |
    +---------+---------+
    ");

    // Example 2: Percentage-based Bernoulli sampling
    // REPEATABLE(seed) ensures deterministic results for snapshot testing
    let results = run_example(
        ctx,
        "Example 2: BERNOULLI percentage sampling",
        "SELECT * FROM sample_data TABLESAMPLE BERNOULLI(30 PERCENT) REPEATABLE(123)",
    )
    .await?;
    assert_snapshot!(results, @r"
    +---------+---------+
    | column1 | column2 |
    +---------+---------+
    | 1       | row_1   |
    | 2       | row_2   |
    | 7       | row_7   |
    | 8       | row_8   |
    +---------+---------+
    ");

    // Example 3: Fractional sampling (0.0 to 1.0)
    // REPEATABLE(seed) ensures deterministic results for snapshot testing
    let results = run_example(
        ctx,
        "Example 3: Fractional sampling",
        "SELECT * FROM sample_data TABLESAMPLE (0.5) REPEATABLE(456)",
    )
    .await?;
    assert_snapshot!(results, @r"
    +---------+---------+
    | column1 | column2 |
    +---------+---------+
    | 2       | row_2   |
    | 4       | row_4   |
    | 8       | row_8   |
    +---------+---------+
    ");

    // Example 4: Row count limit (deterministic, no seed needed)
    let results = run_example(
        ctx,
        "Example 4: Row count limit",
        "SELECT * FROM sample_data TABLESAMPLE (3 ROWS)",
    )
    .await?;
    assert_snapshot!(results, @r"
    +---------+---------+
    | column1 | column2 |
    +---------+---------+
    | 1       | row_1   |
    | 2       | row_2   |
    | 3       | row_3   |
    +---------+---------+
    ");

    // Example 5: Sampling combined with filtering
    let results = run_example(
        ctx,
        "Example 5: Sampling with WHERE clause",
        "SELECT * FROM sample_data TABLESAMPLE (5 ROWS) WHERE column1 > 2",
    )
    .await?;
    assert_snapshot!(results, @r"
    +---------+---------+
    | column1 | column2 |
    +---------+---------+
    | 3       | row_3   |
    | 4       | row_4   |
    | 5       | row_5   |
    +---------+---------+
    ");

    // Example 6: Sampling in JOIN queries
    // REPEATABLE(seed) ensures deterministic results for snapshot testing
    let results = run_example(
        ctx,
        "Example 6: Sampling in JOINs",
        r#"SELECT t1.column1, t2.column1, t1.column2, t2.column2
           FROM sample_data t1 TABLESAMPLE (0.7) REPEATABLE(789)
           JOIN sample_data t2 TABLESAMPLE (0.7) REPEATABLE(123)
           ON t1.column1 = t2.column1"#,
    )
    .await?;
    assert_snapshot!(results, @r"
    +---------+---------+---------+---------+
    | column1 | column1 | column2 | column2 |
    +---------+---------+---------+---------+
    | 2       | 2       | row_2   | row_2   |
    | 5       | 5       | row_5   | row_5   |
    | 7       | 7       | row_7   | row_7   |
    | 8       | 8       | row_8   | row_8   |
    | 10      | 10      | row_10  | row_10  |
    +---------+---------+---------+---------+
    ");

    Ok(())
}

/// Helper to run a single example query and capture results.
async fn run_example(ctx: &SessionContext, title: &str, sql: &str) -> Result<String> {
    println!("{title}:\n{sql}\n");
    let df = ctx.sql(sql).await?;
    println!("{}\n", df.logical_plan().display_indent());

    let batches = df.collect().await?;
    let results = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    println!("{results}\n");

    Ok(results)
}

/// Register test data: 10 rows with column1=1..10 and column2="row_1".."row_10"
fn register_sample_data(ctx: &SessionContext) -> Result<()> {
    let column1: ArrayRef = Arc::new(Int32Array::from((1..=10).collect::<Vec<i32>>()));
    let column2: ArrayRef = Arc::new(StringArray::from(
        (1..=10).map(|i| format!("row_{i}")).collect::<Vec<_>>(),
    ));
    let batch =
        RecordBatch::try_from_iter(vec![("column1", column1), ("column2", column2)])?;
    ctx.register_batch("sample_data", batch)?;
    Ok(())
}

// ============================================================================
// Logical Planning: TableSamplePlanner + TableSamplePlanNode
// ============================================================================

/// Relation planner that intercepts `TABLESAMPLE` clauses in SQL and creates
/// [`TableSamplePlanNode`] logical nodes.
#[derive(Debug)]
struct TableSamplePlanner;

impl RelationPlanner for TableSamplePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        // Only handle Table relations with TABLESAMPLE clause
        let TableFactor::Table {
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
        } = relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        // Extract sample spec (handles both before/after alias positions)
        let sample = match sample {
            ast::TableSampleKind::BeforeTableAlias(s)
            | ast::TableSampleKind::AfterTableAlias(s) => s,
        };

        // Validate sampling method
        if let Some(method) = &sample.name
            && *method != TableSampleMethod::Bernoulli
            && *method != TableSampleMethod::Row
        {
            return not_impl_err!(
                "Sampling method {} is not supported (only BERNOULLI and ROW)",
                method
            );
        }

        // Offset sampling (ClickHouse-style) not supported
        if sample.offset.is_some() {
            return not_impl_err!(
                "TABLESAMPLE with OFFSET is not supported (requires total row count)"
            );
        }

        // Parse optional REPEATABLE seed
        let seed = sample
            .seed
            .map(|s| {
                s.value.to_string().parse::<u64>().map_err(|_| {
                    plan_datafusion_err!("REPEATABLE seed must be an integer")
                })
            })
            .transpose()?;

        // Plan the underlying table without the sample clause
        let base_relation = TableFactor::Table {
            sample: None,
            alias: alias.clone(),
            name,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            index_hints,
        };
        let input = context.plan(base_relation)?;

        // Handle bucket sampling (Hive-style: TABLESAMPLE(BUCKET x OUT OF y))
        if let Some(bucket) = sample.bucket {
            if bucket.on.is_some() {
                return not_impl_err!(
                    "TABLESAMPLE BUCKET with ON clause requires CLUSTERED BY table"
                );
            }
            let bucket_num: u64 =
                bucket.bucket.to_string().parse().map_err(|_| {
                    plan_datafusion_err!("bucket number must be an integer")
                })?;
            let total: u64 =
                bucket.total.to_string().parse().map_err(|_| {
                    plan_datafusion_err!("bucket total must be an integer")
                })?;

            let fraction = bucket_num as f64 / total as f64;
            let plan = TableSamplePlanNode::new(input, fraction, seed).into_plan();
            return Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                plan, alias,
            ))));
        }

        // Handle quantity-based sampling
        let Some(quantity) = sample.quantity else {
            return plan_err!(
                "TABLESAMPLE requires a quantity (percentage, fraction, or row count)"
            );
        };
        let quantity_value_expr = context.sql_to_expr(quantity.value, input.schema())?;

        match quantity.unit {
            // TABLESAMPLE (N ROWS) - exact row limit
            Some(TableSampleUnit::Rows) => {
                let rows: i64 = parse_literal::<Int64Type>(&quantity_value_expr)?;
                if rows < 0 {
                    return plan_err!("row count must be non-negative, got {}", rows);
                }
                let plan = LogicalPlanBuilder::from(input)
                    .limit(0, Some(rows as usize))?
                    .build()?;
                Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                    plan, alias,
                ))))
            }

            // TABLESAMPLE (N PERCENT) - percentage sampling
            Some(TableSampleUnit::Percent) => {
                let percent: f64 = parse_literal::<Float64Type>(&quantity_value_expr)?;
                let fraction = percent / 100.0;
                let plan = TableSamplePlanNode::new(input, fraction, seed).into_plan();
                Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                    plan, alias,
                ))))
            }

            // TABLESAMPLE (N) - fraction if <1.0, row limit if >=1.0
            None => {
                let value = parse_literal::<Float64Type>(&quantity_value_expr)?;
                if value < 0.0 {
                    return plan_err!("sample value must be non-negative, got {}", value);
                }
                let plan = if value >= 1.0 {
                    // Interpret as row limit
                    LogicalPlanBuilder::from(input)
                        .limit(0, Some(value as usize))?
                        .build()?
                } else {
                    // Interpret as fraction
                    TableSamplePlanNode::new(input, value, seed).into_plan()
                };
                Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                    plan, alias,
                ))))
            }
        }
    }
}

/// Custom logical plan node representing a TABLESAMPLE operation.
///
/// Stores sampling parameters (bounds, seed) and wraps the input plan.
/// Gets converted to [`SampleExec`] during physical planning.
#[derive(Debug, Clone, Hash, Eq, PartialEq, PartialOrd)]
struct TableSamplePlanNode {
    input: LogicalPlan,
    lower_bound: HashableF64,
    upper_bound: HashableF64,
    seed: u64,
}

impl TableSamplePlanNode {
    /// Create a new sampling node with the given fraction (0.0 to 1.0).
    fn new(input: LogicalPlan, fraction: f64, seed: Option<u64>) -> Self {
        Self {
            input,
            lower_bound: HashableF64(0.0),
            upper_bound: HashableF64(fraction),
            seed: seed.unwrap_or_else(rand::random),
        }
    }

    /// Wrap this node in a LogicalPlan::Extension.
    fn into_plan(self) -> LogicalPlan {
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
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Sample: bounds=[{}, {}], seed={}",
            self.lower_bound.0, self.upper_bound.0, self.seed
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input: inputs.swap_remove(0),
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            seed: self.seed,
        })
    }
}

/// Wrapper for f64 that implements Hash and Eq (required for LogicalPlan).
#[derive(Debug, Clone, Copy, PartialOrd)]
struct HashableF64(f64);

impl PartialEq for HashableF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for HashableF64 {}

impl Hash for HashableF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

// ============================================================================
// Physical Planning: TableSampleQueryPlanner + TableSampleExtensionPlanner
// ============================================================================

/// Custom query planner that registers [`TableSampleExtensionPlanner`] to
/// convert [`TableSamplePlanNode`] into [`SampleExec`].
#[derive(Debug)]
struct TableSampleQueryPlanner;

#[async_trait]
impl QueryPlanner for TableSampleQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            TableSampleExtensionPlanner,
        )]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Extension planner that converts [`TableSamplePlanNode`] to [`SampleExec`].
struct TableSampleExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for TableSampleExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(sample_node) = node.as_any().downcast_ref::<TableSamplePlanNode>()
        else {
            return Ok(None);
        };

        let exec = SampleExec::try_new(
            Arc::clone(&physical_inputs[0]),
            sample_node.lower_bound.0,
            sample_node.upper_bound.0,
            sample_node.seed,
        )?;
        Ok(Some(Arc::new(exec)))
    }
}

// ============================================================================
// Physical Execution: SampleExec + BernoulliSampler
// ============================================================================

/// Physical execution plan that samples rows from its input using Bernoulli sampling.
///
/// Each row is independently selected with probability `(upper_bound - lower_bound)`
/// and appears at most once.
#[derive(Debug, Clone)]
pub struct SampleExec {
    input: Arc<dyn ExecutionPlan>,
    lower_bound: f64,
    upper_bound: f64,
    seed: u64,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl SampleExec {
    /// Create a new SampleExec with Bernoulli sampling (without replacement).
    ///
    /// # Arguments
    /// * `input` - The input execution plan
    /// * `lower_bound` - Lower bound of sampling range (typically 0.0)
    /// * `upper_bound` - Upper bound of sampling range (0.0 to 1.0)
    /// * `seed` - Random seed for reproducible sampling
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        lower_bound: f64,
        upper_bound: f64,
        seed: u64,
    ) -> Result<Self> {
        if lower_bound < 0.0 || upper_bound > 1.0 || lower_bound > upper_bound {
            return internal_err!(
                "Sampling bounds must satisfy 0.0 <= lower <= upper <= 1.0, got [{}, {}]",
                lower_bound,
                upper_bound
            );
        }

        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.properties().partitioning.clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        );

        Ok(Self {
            input,
            lower_bound,
            upper_bound,
            seed,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// Create a sampler for the given partition.
    fn create_sampler(&self, partition: usize) -> BernoulliSampler {
        let seed = self.seed.wrapping_add(partition as u64);
        BernoulliSampler::new(self.lower_bound, self.upper_bound, seed)
    }
}

impl DisplayAs for SampleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "SampleExec: bounds=[{}, {}], seed={}",
            self.lower_bound, self.upper_bound, self.seed
        )
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
        // Sampling preserves row order (rows are filtered, not reordered)
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            children.swap_remove(0),
            self.lower_bound,
            self.upper_bound,
            self.seed,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(SampleStream {
            input: self.input.execute(partition, context)?,
            sampler: self.create_sampler(partition),
            metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let mut stats = self.input.partition_statistics(partition)?;
        let ratio = self.upper_bound - self.lower_bound;

        // Scale statistics by sampling ratio (inexact due to randomness)
        stats.num_rows = stats
            .num_rows
            .map(|n| (n as f64 * ratio) as usize)
            .to_inexact();
        stats.total_byte_size = stats
            .total_byte_size
            .map(|n| (n as f64 * ratio) as usize)
            .to_inexact();

        Ok(stats)
    }
}

/// Bernoulli sampler: includes each row with probability `(upper - lower)`.
/// This is sampling **without replacement** - each row appears at most once.
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

    fn sample(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let range = self.upper_bound - self.lower_bound;
        if range <= 0.0 {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        // Select rows where random value falls in [lower, upper)
        let indices: Vec<u32> = (0..batch.num_rows())
            .filter(|_| {
                let r: f64 = self.rng.random();
                r >= self.lower_bound && r < self.upper_bound
            })
            .map(|i| i as u32)
            .collect();

        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        compute::take_record_batch(batch, &UInt32Array::from(indices))
            .map_err(DataFusionError::from)
    }
}

/// Stream adapter that applies sampling to each batch.
struct SampleStream {
    input: SendableRecordBatchStream,
    sampler: BernoulliSampler,
    metrics: BaselineMetrics,
}

impl Stream for SampleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let elapsed = self.metrics.elapsed_compute().clone();
                let _timer = elapsed.timer();
                let result = self.sampler.sample(&batch);
                Poll::Ready(Some(result.record_output(&self.metrics)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for SampleStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
