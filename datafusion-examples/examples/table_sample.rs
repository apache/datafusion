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

#![allow(unused_imports)]

use datafusion::common::{
    arrow_datafusion_err, internal_err, not_impl_err, plan_datafusion_err, plan_err,
    DFSchema, DFSchemaRef, ResolvedTableReference, Statistics, TableReference,
};
use datafusion::error::Result;
use datafusion::logical_expr::sqlparser::ast::{
    Query, SetExpr, Statement, TableFactor, TableSample, TableSampleMethod,
    TableSampleQuantity, TableSampleUnit,
};
use datafusion::logical_expr::{
    AggregateUDF, Extension, Filter, LogicalPlan, LogicalPlanBuilder, Projection,
    ScalarUDF, TableSource, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
    WindowUDF,
};
use std::any::Any;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use arrow::util::pretty::{pretty_format_batches, pretty_format_batches_with_schema};
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRewriter,
};

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::cte_worktable::CteWorkTable;
use datafusion::common::file_options::file_type::FileType;
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::format_as_file_type;
use datafusion::datasource::{provider_as_source, DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::{
    FunctionRegistry, SendableRecordBatchStream, SessionState, SessionStateBuilder,
    TaskContext,
};
use datafusion::logical_expr::planner::{
    ContextProvider, ExprPlanner, PlannerResult, RawBinaryExpr, TypePlanner,
};
use datafusion::logical_expr::sqlparser::dialect::PostgreSqlDialect;
use datafusion::logical_expr::sqlparser::parser::Parser;
use datafusion::logical_expr::var_provider::is_system_variables;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::{
    displayable, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream,
};
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};
use datafusion::prelude::*;
use datafusion::sql::planner::{ParserOptions, PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast::{TableSampleKind, TableSampleModifier};
use datafusion::sql::unparser::ast::{
    DerivedRelationBuilder, QueryBuilder, RelationBuilder, SelectBuilder,
};
use datafusion::sql::unparser::dialect::CustomDialectBuilder;
use datafusion::sql::unparser::expr_to_sql;
use datafusion::sql::unparser::extension_unparser::UserDefinedLogicalNodeUnparser;
use datafusion::sql::unparser::extension_unparser::{
    UnparseToStatementResult, UnparseWithinStatementResult,
};
use datafusion::sql::unparser::{plan_to_sql, Unparser};
use datafusion::variable::VarType;
use log::{debug, info};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::UInt32Array;
use arrow::compute;
use arrow::record_batch::RecordBatch;

use datafusion::execution::context::QueryPlanner;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast;
use futures::stream::{Stream, StreamExt};
use futures::TryStreamExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Poisson};

/// This example demonstrates the table sample support.

#[derive(Debug, Clone)]
struct TableSamplePlanNode {
    inner_plan: LogicalPlan,

    lower_bound: f64,
    upper_bound: f64,
    with_replacement: bool,
    seed: u64,
}

impl Hash for TableSamplePlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner_plan.hash(state);
        self.lower_bound.to_bits().hash(state);
        self.upper_bound.to_bits().hash(state);
        self.with_replacement.hash(state);
        self.seed.hash(state);
    }
}

impl PartialEq for TableSamplePlanNode {
    fn eq(&self, other: &Self) -> bool {
        self.inner_plan == other.inner_plan
            && (self.lower_bound - other.lower_bound).abs() < f64::EPSILON
            && (self.upper_bound - other.upper_bound).abs() < f64::EPSILON
            && self.with_replacement == other.with_replacement
            && self.seed == other.seed
    }
}

impl Eq for TableSamplePlanNode {}

impl PartialOrd for TableSamplePlanNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner_plan
            .partial_cmp(&other.inner_plan)
            .and_then(|ord| {
                if ord != Ordering::Equal {
                    Some(ord)
                } else {
                    self.lower_bound
                        .partial_cmp(&other.lower_bound)
                        .and_then(|ord| {
                            if ord != Ordering::Equal {
                                Some(ord)
                            } else {
                                self.upper_bound.partial_cmp(&other.upper_bound).and_then(
                                    |ord| {
                                        if ord != Ordering::Equal {
                                            Some(ord)
                                        } else {
                                            self.with_replacement
                                                .partial_cmp(&other.with_replacement)
                                                .and_then(|ord| {
                                                    if ord != Ordering::Equal {
                                                        Some(ord)
                                                    } else {
                                                        self.seed.partial_cmp(&other.seed)
                                                    }
                                                })
                                        }
                                    },
                                )
                            }
                        })
                }
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
        physical_input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SampleExec {
            input: physical_input.clone(),
            lower_bound: 0.0,
            upper_bound: specific_node.upper_bound,
            with_replacement: specific_node.with_replacement,
            seed: specific_node.seed,
            metrics: Default::default(),
            cache: SampleExec::compute_properties(&physical_input),
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
            info!("Extension planner plan_extension: {:?}", &logical_inputs);
            assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");

            let exec_plan =
                self.build_execution_plan(specific_node, physical_inputs[0].clone())?;
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
    pub fn with_replacement(&self) -> bool {
        self.with_replacement
    }

    /// The lower bound of the sampling ratio
    pub fn lower_bound(&self) -> f64 {
        self.lower_bound
    }

    /// The upper bound of the sampling ratio
    pub fn upper_bound(&self) -> f64 {
        self.upper_bound
    }

    /// The random seed
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The input plan
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
        let poll = self.input.poll_next_unpin(cx);
        let baseline_metrics = &mut self.baseline_metrics;

        match poll {
            Poll::Ready(Some(Ok(batch))) => {
                let start = baseline_metrics.elapsed_compute().clone();
                let result = self.sampler.sample(&batch);
                let _timer = start.timer();
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for SampleExecStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Query planner to produce a LogicalPlan from AST with table sampling
struct TableSamplePlanner<'a, S: ContextProvider> {
    context_provider: &'a S,
}

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

impl<'a, S: ContextProvider> TableSamplePlanner<'a, S> {
    pub fn new(context_provider: &'a S) -> Self {
        Self { context_provider }
    }

    pub fn new_node(
        input: LogicalPlan,
        fraction: f64,
        with_replacement: Option<bool>,
        seed: Option<u64>,
    ) -> Result<LogicalPlan> {
        let node = TableSamplePlanNode {
            inner_plan: input,
            lower_bound: 0.0,
            upper_bound: fraction,
            with_replacement: with_replacement.unwrap_or(false),
            seed: seed.unwrap_or_else(rand::random),
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    fn sample_to_logical_plan(
        &self,
        input: LogicalPlan,
        sample: TableSampleKind,
    ) -> Result<LogicalPlan> {
        let sample = match sample {
            TableSampleKind::BeforeTableAlias(sample) => sample,
            TableSampleKind::AfterTableAlias(sample) => sample,
        };
        if let Some(name) = &sample.name {
            if *name != TableSampleMethod::Bernoulli && *name != TableSampleMethod::Row {
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

        if let Some(bucket) = sample.bucket {
            if bucket.on.is_some() {
                // Hive-style sample, only used when the Hive table is defined with CLUSTERED BY
                return not_impl_err!("Bucket sample with ON is not supported yet");
            }

            let Ok(bucket_num) = bucket.bucket.to_string().parse::<u64>() else {
                return plan_err!("bucket must be a number");
            };

            let Ok(total_num) = bucket.total.to_string().parse::<u64>() else {
                return plan_err!("total must be a number");
            };
            let value = bucket_num as f64 / total_num as f64;
            return Self::new_node(input, value, None, seed);
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
                    LogicalPlanBuilder::from(input)
                        .limit(0, Some(value as usize))?
                        .build()
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
                    Self::new_node(input, value, None, seed)
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
                        LogicalPlanBuilder::from(input)
                            .limit(0, Some(value as usize))?
                            .build()
                    } else {
                        // If value is between 0.0 and 1.0, it is a fraction
                        Self::new_node(input, value, None, seed)
                    }
                }
            };
        }
        plan_err!("Cannot plan sample SQL")
    }

    fn create_logical_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        let sql_to_rel = SqlToRel::new(self.context_provider);

        let stmt = statement.clone();
        match &stmt {
            Statement::Query(query) => {
                let inner_plan = sql_to_rel.sql_statement_to_plan(statement)?;
                if let SetExpr::Select(select) = &*query.body {
                    if select.from.len() == 1 {
                        let table_with_joins = select.from.first().unwrap();
                        if let TableFactor::Table {
                            sample: Some(table_sample_kind),
                            ..
                        } = &table_with_joins.relation
                        {
                            debug!("Constructing table sample plan from {:?}", &select);
                            return self.sample_to_logical_plan(
                                inner_plan,
                                table_sample_kind.clone(),
                            );
                        }
                    }
                }
                // Pass-through by default
                Ok(inner_plan)
            }
            _ => sql_to_rel.sql_statement_to_plan(statement),
        }
    }
}

// Context provider for tests

struct MockContextProvider<'a> {
    state: &'a SessionState,
    tables: HashMap<TableReference, Arc<dyn TableSource>>,
}

impl ContextProvider for MockContextProvider<'_> {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        self.tables
            .get(&name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table '{name}' not found"))
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        self.state.expr_planners()
    }

    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        None
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        self.state
            .execution_props()
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&VarType::System)?.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::try_init();

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(TableSampleQueryPlanner {}))
        .build();

    let ctx = SessionContext::new_with_state(state.clone());

    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    let table_source = provider_as_source(ctx.table_provider("alltypes_plain").await?);
    let context_provider = MockContextProvider {
        state: &state,
        tables: HashMap::<TableReference, Arc<dyn TableSource>>::from([(
            "alltypes_plain".into(),
            table_source.clone(),
        )]),
    };

    let sql =
        "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 42 PERCENT REPEATABLE(5) WHERE int_col = 1";

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    let statement = statements.first().expect("one statement");

    // Classical way
    // let sql_to_rel = SqlToRel::new(&context_provider);
    // let logical_plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;

    // Use sampling planner to create a logical plan
    let table_sample_planner = TableSamplePlanner::new(&context_provider);
    let logical_plan = table_sample_planner.create_logical_plan(statement.clone())?;

    let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

    // Inspect physical plan
    let displayable_plan = displayable(physical_plan.as_ref())
        .indent(false)
        .to_string();
    info!("Physical plan:\n{displayable_plan}\n");
    let first_line = displayable_plan.lines().next().unwrap();
    assert_eq!(
        first_line,
        "SampleExec: lower_bound=0, upper_bound=0.42, with_replacement=false, seed=5"
    );

    // Execute via standard sql call - doesn't work
    // let df = ctx.sql(sql).await?;
    // let batches = df.collect().await?;

    // Execute directly via physical plan
    let task_context = Arc::new(TaskContext::from(&ctx));
    let stream = physical_plan.execute(0, task_context)?;
    let batches: Vec<_> = stream.try_collect().await?;

    info!("Batches: {:?}", &batches);

    let result_string = pretty_format_batches(&batches)
        // pretty_format_batches_with_schema(table_source.schema(), &batches)
        .map_err(|e| arrow_datafusion_err!(e))
        .map(|d| d.to_string())?;
    let result_strings = result_string.lines().collect::<Vec<_>>();
    info!("Batch result: {:?}", &result_strings);

    assert_eq!(batches.len(), 1);
    assert_eq!(batches.first().unwrap().num_rows(), 2);

    info!("Done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use datafusion::assert_batches_eq;
    use datafusion::physical_plan::test::TestMemoryExec;
    use futures::TryStreamExt;
    use std::sync::Arc;

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        // Enable RUST_LOG logging configuration for test
        let _ = env_logger::try_init();
    }

    async fn parse_to_logical_plan(sql: &str) -> Result<(SessionContext, LogicalPlan)> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(TableSampleQueryPlanner {}))
            .build();

        let ctx = SessionContext::new_with_state(state.clone());

        let testdata = datafusion::test_util::parquet_test_data();
        ctx.register_parquet(
            "alltypes_plain",
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

        let table_source =
            provider_as_source(ctx.table_provider("alltypes_plain").await?);
        let context_provider = MockContextProvider {
            state: &state,
            tables: HashMap::<TableReference, Arc<dyn TableSource>>::from([(
                "alltypes_plain".into(),
                table_source.clone(),
            )]),
        };

        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        let statement = statements.get(0).expect("one statement");

        // Use sampling
        let table_sample_planner = TableSamplePlanner::new(&context_provider);
        let logical_plan = table_sample_planner.create_logical_plan(statement.clone())?;

        Ok((ctx, logical_plan))
    }

    fn physical_plan_first_line(physical_plan: Arc<dyn ExecutionPlan>) -> String {
        let displayable_plan = displayable(physical_plan.as_ref())
            .indent(false)
            .to_string();
        info!("Physical plan:\n{}\n", displayable_plan);
        let first_line = displayable_plan.lines().next().expect("empty plan");
        first_line.into()
    }

    fn as_table_sample_node(logical_plan: LogicalPlan) -> Result<TableSamplePlanNode> {
        match logical_plan {
            LogicalPlan::Extension(Extension { node }) => {
                if let Some(plan) = node.as_any().downcast_ref::<TableSamplePlanNode>() {
                    Ok(plan.clone())
                } else {
                    plan_err!("Wrong extension node")
                }
            }
            _ => plan_err!("Not an extension node"),
        }
    }

    #[tokio::test]
    async fn test_logical_plan_sample() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 0.42 where int_col = 1";
        let (_, logical_plan) = parse_to_logical_plan(sql).await?;
        let node = as_table_sample_node(logical_plan)?;
        assert_eq!(node.lower_bound, 0.0);
        assert_eq!(node.upper_bound, 0.42);
        assert_eq!(node.with_replacement, false);
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_sample_repeatable() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 0.42 REPEATABLE(123) where int_col = 1";
        let (_, logical_plan) = parse_to_logical_plan(sql).await?;
        let node = as_table_sample_node(logical_plan)?;
        assert_eq!(node.lower_bound, 0.0);
        assert_eq!(node.upper_bound, 0.42);
        assert_eq!(node.with_replacement, false);
        assert_eq!(node.seed, 123);
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_sample_percent() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 42 PERCENT where int_col = 1";
        let (_, logical_plan) = parse_to_logical_plan(sql).await?;
        let node = as_table_sample_node(logical_plan)?;
        assert_eq!(node.lower_bound, 0.0);
        assert_eq!(node.upper_bound, 0.42);
        assert_eq!(node.with_replacement, false);
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_sample_rows() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 42 ROWS where int_col = 1";
        let (_, logical_plan) = parse_to_logical_plan(sql).await?;
        if let LogicalPlan::Limit(limit) = logical_plan {
            assert_eq!(limit.fetch, Some(Box::new(lit(42_i64))));
            assert_eq!(limit.skip, None);
        } else {
            assert!(false, "Expected LogicalPlan::Limit");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_method_system_unsupported() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE SYSTEM 42 where int_col = 1";
        let err = parse_to_logical_plan(sql).await.err().expect("should fail");
        assert!(err.to_string().contains("SYSTEM is not supported yet"));
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_method_block_unsupported() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE BLOCK 42 where int_col = 1";
        let err = parse_to_logical_plan(sql).await.err().expect("should fail");
        assert!(err.to_string().contains("BLOCK is not supported yet"));
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_method_offset_unsupported() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain SAMPLE 42 OFFSET 5 where int_col = 1";
        let err = parse_to_logical_plan(sql).await.err().expect("should fail");
        assert!(err
            .to_string()
            .contains("Offset sample is not supported yet"));
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_sample_clickhouse() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain SAMPLE 0.42 where int_col = 1";
        let (_, logical_plan) = parse_to_logical_plan(sql).await?;
        let node = as_table_sample_node(logical_plan)?;
        assert_eq!(node.lower_bound, 0.0);
        assert_eq!(node.upper_bound, 0.42);
        assert_eq!(node.with_replacement, false);
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_limit_clickhouse() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain SAMPLE 42 where int_col = 1";
        let (_, logical_plan) = parse_to_logical_plan(sql).await?;
        if let LogicalPlan::Limit(limit) = logical_plan {
            assert_eq!(limit.fetch, Some(Box::new(lit(42_i64))));
            assert_eq!(limit.skip, None);
        } else {
            assert!(false, "Expected LogicalPlan::Limit");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_bucket() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE (BUCKET 3 OUT OF 16) where int_col = 1";
        let (_, logical_plan) = parse_to_logical_plan(sql).await?;
        let node = as_table_sample_node(logical_plan)?;
        assert_eq!(node.lower_bound, 0.0);
        assert!((node.upper_bound - 3.0 / 16.0).abs() < f64::EPSILON);
        assert_eq!(node.with_replacement, false);
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_bucket_on_unsupported() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE (BUCKET 3 OUT OF 16 ON int_col) where int_col = 1";
        let err = parse_to_logical_plan(sql).await.err().expect("should fail");
        assert!(err
            .to_string()
            .contains("Bucket sample with ON is not supported yet"));
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_negative_value() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 1-5 where int_col = 1";
        let err = parse_to_logical_plan(sql).await.err().expect("should fail");
        assert!(
            err.to_string()
                .contains("quantity must be a non-negative number"),
            "{err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_plan_not_a_number() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 42 + int_col where int_col = 1";
        let err = parse_to_logical_plan(sql).await.err().expect("should fail");
        assert!(
            err.to_string().contains("quantity must be a valid number"),
            "{err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_physical_plan_sample() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 0.42 where int_col = 1";
        let (ctx, logical_plan) = parse_to_logical_plan(sql).await?;
        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;
        let physical_plan_repr = physical_plan_first_line(physical_plan);
        assert!(physical_plan_repr.starts_with(
            "SampleExec: lower_bound=0, upper_bound=0.42, with_replacement=false, seed="
        ), "{physical_plan_repr}");
        Ok(())
    }

    #[tokio::test]
    async fn test_physical_plan_sample_repeateable() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 0.42 REPEATABLE(123) where int_col = 1";
        let (ctx, logical_plan) = parse_to_logical_plan(sql).await?;
        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;
        let physical_plan_repr = physical_plan_first_line(physical_plan);
        assert_eq!(
            physical_plan_repr,
            "SampleExec: lower_bound=0, upper_bound=0.42, with_replacement=false, seed=123",
            "{physical_plan_repr}"
        );
        Ok(())
    }

    //noinspection RsTypeCheck
    #[tokio::test]
    async fn test_execute() -> Result<()> {
        let sql =
            "SELECT int_col, double_col FROM alltypes_plain TABLESAMPLE 0.42 REPEATABLE(5) where int_col = 1";

        let (ctx, logical_plan) = parse_to_logical_plan(sql).await?;

        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

        // Execute via physical plan
        let task_context = Arc::new(TaskContext::from(&ctx));
        let stream = physical_plan.execute(0, task_context)?;
        let batches: Vec<_> = stream.try_collect().await?;

        // Observed with a specific repeatable seed
        assert_batches_eq!(
            #[rustfmt::skip]
            &[
                "+---------+------------+",
                "| int_col | double_col |",
                "+---------+------------+",
                "| 1       | 10.1       |",
                "| 1       | 10.1       |",
                "+---------+------------+"
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_sample_exec_bernoulli() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;

        let input = Arc::new(TestMemoryExec::try_new(
            &[vec![batch]],
            Arc::clone(&schema),
            None,
        )?);

        let sample_exec = SampleExec::try_new(input, 0.6, 1.0, false, 42)?;

        let context = Arc::new(TaskContext::default());
        let stream = sample_exec.execute(0, context)?;

        let batches = stream.try_collect::<Vec<_>>().await?;
        assert_batches_eq!(
            &["+----+", "| id |", "+----+", "| 3  |", "+----+",],
            &batches
        );

        Ok(())
    }

    //noinspection RsTypeCheck
    #[tokio::test]
    async fn test_sample_exec_poisson() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;

        let input = Arc::new(TestMemoryExec::try_new(
            &[vec![batch]],
            Arc::clone(&schema),
            None,
        )?);

        let sample_exec = SampleExec::try_new(input, 0.0, 0.5, true, 42)?;

        let context = Arc::new(TaskContext::default());
        let stream = sample_exec.execute(0, context)?;

        let batches = stream.try_collect::<Vec<_>>().await?;
        assert_batches_eq!(
            #[rustfmt::skip]
            &[
                "+----+",
                "| id |",
                "+----+",
                "| 3  |",
                "+----+",
            ],
            &batches
        );

        Ok(())
    }

    //noinspection RsTypeCheck
    #[test]
    fn test_sampler_trait() {
        let mut bernoulli_sampler = BernoulliSampler::new(0.0, 0.5, 42);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let result = bernoulli_sampler.sample(&batch).unwrap();
        assert_batches_eq!(
            #[rustfmt::skip]
            &[
                "+----+",
                "| id |",
                "+----+",
                "| 4  |",
                "| 5  |",
                "+----+",
            ],
            &[result]
        );

        let mut poisson_sampler = PoissonSampler::try_new(0.5, 42).unwrap();
        let result = poisson_sampler.sample(&batch).unwrap();
        assert_batches_eq!(
            #[rustfmt::skip]
            &[
                "+----+",
                "| id |",
                "+----+",
                "| 3  |",
                "+----+",
            ],
            &[result]
        );
    }
}
