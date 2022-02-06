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

//! Traits for physical query plan, supporting parallel execution for partitioned relations.

pub use self::metrics::Metric;
use self::metrics::MetricsSet;
use self::{
    coalesce_partitions::CoalescePartitionsExec, display::DisplayableExecutionPlan,
};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::{
    error::{DataFusionError, Result},
    execution::runtime_env::RuntimeEnv,
    scalar::ScalarValue,
};
use arrow::compute::kernels::partition::lexicographical_partition_ranges;
use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use async_trait::async_trait;
pub use datafusion_expr::Accumulator;
pub use datafusion_expr::ColumnarValue;
pub use display::DisplayFormatType;
use futures::stream::Stream;
use std::fmt;
use std::fmt::{Debug, Display};
use std::ops::Range;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, pin::Pin};

/// Trait for types that stream [arrow::record_batch::RecordBatch]
pub trait RecordBatchStream: Stream<Item = ArrowResult<RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

/// Trait for a stream of record batches.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send + Sync>>;

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// Physical planner interface
pub use self::planner::PhysicalPlanner;

/// Statistics for a physical plan node
/// Fields are optional and can be inexact because the sources
/// sometimes provide approximate estimates for performance reasons
/// and the transformations output are not always predictable.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Statistics {
    /// The number of table rows
    pub num_rows: Option<usize>,
    /// total byte of the table rows
    pub total_byte_size: Option<usize>,
    /// Statistics on a column level
    pub column_statistics: Option<Vec<ColumnStatistics>>,
    /// If true, any field that is `Some(..)` is the actual value in the data provided by the operator (it is not
    /// an estimate). Any or all other fields might still be None, in which case no information is known.
    /// if false, any field that is `Some(..)` may contain an inexact estimate and may not be the actual value.
    pub is_exact: bool,
}
/// This table statistics are estimates about column
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColumnStatistics {
    /// Number of null values on column
    pub null_count: Option<usize>,
    /// Maximum value of column
    pub max_value: Option<ScalarValue>,
    /// Minimum value of column
    pub min_value: Option<ScalarValue>,
    /// Number of distinct values
    pub distinct_count: Option<usize>,
}

/// `ExecutionPlan` represent nodes in the DataFusion Physical Plan.
///
/// Each `ExecutionPlan` is Partition-aware and is responsible for
/// creating the actual `async` [`SendableRecordBatchStream`]s
/// of [`RecordBatch`] that incrementally compute the operator's
/// output from its input partition.
///
/// [`ExecutionPlan`] can be displayed in an simplified form using the
/// return value from [`displayable`] in addition to the (normally
/// quite verbose) `Debug` output.
#[async_trait]
pub trait ExecutionPlan: Debug + Send + Sync {
    /// Returns the execution plan as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;

    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning;

    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    /// Returns `true` if the direct children of this `ExecutionPlan` should be repartitioned
    /// to introduce greater concurrency to the plan
    ///
    /// The default implementation returns `true` unless `Self::required_child_distribution`
    /// returns `Distribution::SinglePartition`
    ///
    /// Operators that do not benefit from additional partitioning may want to return `false`
    fn should_repartition_children(&self) -> bool {
        !matches!(
            self.required_child_distribution(),
            Distribution::SinglePartition
        )
    }

    /// Get a list of child execution plans that provide the input for this plan. The returned list
    /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
    /// values for binary nodes (such as joins).
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;
    /// Returns a new plan where all children were replaced by new plans.
    /// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// creates an iterator
    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream>;

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`].
    ///
    /// While the values of the metrics in the returned
    /// [`MetricsSet`]s may change as execution progresses, the
    /// specific metrics will not.
    ///
    /// Once `self.execute()` has returned (technically the future is
    /// resolved) for all available partitions, the set of metrics
    /// should be complete. If this function is called prior to
    /// `execute()` new metrics may appear in subsequent calls.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Format this `ExecutionPlan` to `f` in the specified type.
    ///
    /// Should not include a newline
    ///
    /// Note this function prints a placeholder by default to preserve
    /// backwards compatibility.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExecutionPlan(PlaceHolder)")
    }

    /// Returns the global output statistics for this `ExecutionPlan` node.
    fn statistics(&self) -> Statistics;
}

/// Return a [wrapper](DisplayableExecutionPlan) around an
/// [`ExecutionPlan`] which can be displayed in various easier to
/// understand ways.
///
/// ```
/// use datafusion::prelude::*;
/// use datafusion::physical_plan::displayable;
///
/// #[tokio::main]
/// async fn main() {
///   // Hard code target_partitions as it appears in the RepartitionExec output
///   let config = ExecutionConfig::new()
///       .with_target_partitions(3);
///   let mut ctx = ExecutionContext::with_config(config);
///
///   // register the a table
///   ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).await.unwrap();
///
///   // create a plan to run a SQL query
///   let plan = ctx
///      .create_logical_plan("SELECT a FROM example WHERE a < 5")
///      .unwrap();
///   let plan = ctx.optimize(&plan).unwrap();
///   let physical_plan = ctx.create_physical_plan(&plan).await.unwrap();
///
///   // Format using display string
///   let displayable_plan = displayable(physical_plan.as_ref());
///   let plan_string = format!("{}", displayable_plan.indent());
///
///   assert_eq!("ProjectionExec: expr=[a@0 as a]\
///              \n  CoalesceBatchesExec: target_batch_size=4096\
///              \n    FilterExec: a@0 < 5\
///              \n      RepartitionExec: partitioning=RoundRobinBatch(3)\
///              \n        CsvExec: files=[tests/example.csv], has_header=true, limit=None",
///               plan_string.trim());
/// }
/// ```
///
pub fn displayable(plan: &dyn ExecutionPlan) -> DisplayableExecutionPlan<'_> {
    DisplayableExecutionPlan::new(plan)
}

/// Visit all children of this plan, according to the order defined on `ExecutionPlanVisitor`.
// Note that this would be really nice if it were a method on
// ExecutionPlan, but it can not be because it takes a generic
// parameter and `ExecutionPlan` is a trait
pub fn accept<V: ExecutionPlanVisitor>(
    plan: &dyn ExecutionPlan,
    visitor: &mut V,
) -> std::result::Result<(), V::Error> {
    visitor.pre_visit(plan)?;
    for child in plan.children() {
        visit_execution_plan(child.as_ref(), visitor)?;
    }
    visitor.post_visit(plan)?;
    Ok(())
}

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of `ExecutionPlan` nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
////
/// To use, define a struct that implements this trait and then invoke
/// ['accept'].
///
/// For example, for an execution plan that looks like:
///
/// ```text
/// ProjectionExec: #id
///    FilterExec: state = CO
///       CsvExec:
/// ```
///
/// The sequence of visit operations would be:
/// ```text
/// visitor.pre_visit(ProjectionExec)
/// visitor.pre_visit(FilterExec)
/// visitor.pre_visit(CsvExec)
/// visitor.post_visit(CsvExec)
/// visitor.post_visit(FilterExec)
/// visitor.post_visit(ProjectionExec)
/// ```
pub trait ExecutionPlanVisitor {
    /// The type of error returned by this visitor
    type Error;

    /// Invoked on an `ExecutionPlan` plan before any of its child
    /// inputs have been visited. If Ok(true) is returned, the
    /// recursion continues. If Err(..) or Ok(false) are returned, the
    /// recursion stops immediately and the error, if any, is returned
    /// to `accept`
    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error>;

    /// Invoked on an `ExecutionPlan` plan *after* all of its child
    /// inputs have been visited. The return value is handled the same
    /// as the return value of `pre_visit`. The provided default
    /// implementation returns `Ok(true)`.
    fn post_visit(
        &mut self,
        _plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Recursively calls `pre_visit` and `post_visit` for this node and
/// all of its children, as described on [`ExecutionPlanVisitor`]
pub fn visit_execution_plan<V: ExecutionPlanVisitor>(
    plan: &dyn ExecutionPlan,
    visitor: &mut V,
) -> std::result::Result<(), V::Error> {
    visitor.pre_visit(plan)?;
    for child in plan.children() {
        visit_execution_plan(child.as_ref(), visitor)?;
    }
    visitor.post_visit(plan)?;
    Ok(())
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect(
    plan: Arc<dyn ExecutionPlan>,
    runtime: Arc<RuntimeEnv>,
) -> Result<Vec<RecordBatch>> {
    let stream = execute_stream(plan, runtime).await?;
    common::collect(stream).await
}

/// Execute the [ExecutionPlan] and return a single stream of results
pub async fn execute_stream(
    plan: Arc<dyn ExecutionPlan>,
    runtime: Arc<RuntimeEnv>,
) -> Result<SendableRecordBatchStream> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
        1 => plan.execute(0, runtime).await,
        _ => {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(plan.clone());
            // CoalescePartitionsExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0, runtime).await
        }
    }
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    runtime: Arc<RuntimeEnv>,
) -> Result<Vec<Vec<RecordBatch>>> {
    let streams = execute_stream_partitioned(plan, runtime).await?;
    let mut batches = Vec::with_capacity(streams.len());
    for stream in streams {
        batches.push(common::collect(stream).await?);
    }
    Ok(batches)
}

/// Execute the [ExecutionPlan] and return a vec with one stream per output partition
pub async fn execute_stream_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    runtime: Arc<RuntimeEnv>,
) -> Result<Vec<SendableRecordBatchStream>> {
    let num_partitions = plan.output_partitioning().partition_count();
    let mut streams = Vec::with_capacity(num_partitions);
    for i in 0..num_partitions {
        streams.push(plan.execute(i, runtime.clone()).await?);
    }
    Ok(streams)
}

/// Partitioning schemes supported by operators.
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number of
    /// partitions
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Unknown partitioning scheme with a known number of partitions
    UnknownPartitioning(usize),
}

impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            RoundRobinBatch(n) | Hash(_, n) | UnknownPartitioning(n) => *n,
        }
    }
}

/// Distribution schemes
#[derive(Debug, Clone)]
pub enum Distribution {
    /// Unspecified distribution
    UnspecifiedDistribution,
    /// A single partition is required
    SinglePartition,
    /// Requires children to be distributed in such a way that the same
    /// values of the keys end up in the same partition
    HashPartitioned(Vec<Arc<dyn PhysicalExpr>>),
}

/// Expression that can be evaluated against a RecordBatch
/// A Physical expression knows its type, nullability and how to evaluate itself.
pub trait PhysicalExpr: Send + Sync + Display + Debug {
    /// Returns the physical expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;
}

/// An aggregate expression that:
/// * knows its resulting field
/// * knows how to create its accumulator
/// * knows its accumulator's state's field
/// * knows the expressions from whose its accumulator will receive values
pub trait AggregateExpr: Send + Sync + Debug {
    /// Returns the aggregate expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// the accumulator used to accumulate values from the expressions.
    /// the accumulator expects the same number of arguments as `expressions` and must
    /// return states with the same description as `state_fields`
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// the fields that encapsulate the Accumulator's state
    /// the number of fields here equals the number of states that the accumulator contains
    fn state_fields(&self) -> Result<Vec<Field>>;

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Human readable name such as `"MIN(c2)"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "AggregateExpr: default name"
    }
}

/// A window expression that:
/// * knows its resulting field
pub trait WindowExpr: Send + Sync + Debug {
    /// Returns the window expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this window function.
    fn field(&self) -> Result<Field>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "WindowExpr: default name"
    }

    /// expressions that are passed to the WindowAccumulator.
    /// Functions which take a single input argument, such as `sum`, return a single [`Expr`],
    /// others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// evaluate the window function arguments against the batch and return
    /// array ref, normally the resulting vec is a single element one.
    fn evaluate_args(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.expressions()
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect()
    }

    /// evaluate the window function values against the batch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// evaluate the partition points given the sort columns; if the sort columns are
    /// empty then the result will be a single element vec of the whole column rows.
    fn evaluate_partition_points(
        &self,
        num_rows: usize,
        partition_columns: &[SortColumn],
    ) -> Result<Vec<Range<usize>>> {
        if partition_columns.is_empty() {
            Ok(vec![Range {
                start: 0,
                end: num_rows,
            }])
        } else {
            Ok(lexicographical_partition_ranges(partition_columns)
                .map_err(DataFusionError::ArrowError)?
                .collect::<Vec<_>>())
        }
    }

    /// expressions that's from the window function's partition by clause, empty if absent
    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>];

    /// expressions that's from the window function's order by clause, empty if absent
    fn order_by(&self) -> &[PhysicalSortExpr];

    /// get partition columns that can be used for partitioning, empty if absent
    fn partition_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.partition_by()
            .iter()
            .map(|expr| {
                PhysicalSortExpr {
                    expr: expr.clone(),
                    options: SortOptions::default(),
                }
                .evaluate_to_sort_column(batch)
            })
            .collect()
    }

    /// get sort columns that can be used for peer evaluation, empty if absent
    fn sort_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        let mut sort_columns = self.partition_columns(batch)?;
        let order_by_columns = self
            .order_by()
            .iter()
            .map(|e| e.evaluate_to_sort_column(batch))
            .collect::<Result<Vec<SortColumn>>>()?;
        sort_columns.extend(order_by_columns);
        Ok(sort_columns)
    }
}

/// Applies an optional projection to a [`SchemaRef`], returning the
/// projected schema
///
/// Example:
/// ```
/// use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
/// use datafusion::physical_plan::project_schema;
///
/// // Schema with columns 'a', 'b', and 'c'
/// let schema = SchemaRef::new(Schema::new(vec![
///   Field::new("a", DataType::Int32, true),
///   Field::new("b", DataType::Int64, true),
///   Field::new("c", DataType::Utf8, true),
/// ]));
///
/// // Pick columns 'c' and 'b'
/// let projection = Some(vec![2,1]);
/// let projected_schema = project_schema(
///    &schema,
///    projection.as_ref()
///  ).unwrap();
///
/// let expected_schema = SchemaRef::new(Schema::new(vec![
///   Field::new("c", DataType::Utf8, true),
///   Field::new("b", DataType::Int64, true),
/// ]));
///
/// assert_eq!(projected_schema, expected_schema);
/// ```
pub fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<SchemaRef> {
    let schema = match projection {
        Some(columns) => Arc::new(schema.project(columns)?),
        None => Arc::clone(schema),
    };
    Ok(schema)
}

pub mod aggregates;
pub mod analyze;
pub mod array_expressions;
pub mod coalesce_batches;
pub mod coalesce_partitions;
mod coercion_rule;
pub mod common;
pub mod cross_join;
#[cfg(feature = "crypto_expressions")]
pub mod crypto_expressions;
pub mod datetime_expressions;
pub mod display;
pub mod empty;
pub mod explain;
pub mod expressions;
pub mod file_format;
pub mod filter;
pub mod functions;
pub mod hash_aggregate;
pub mod hash_join;
pub mod hash_utils;
pub(crate) mod hyperloglog;
pub mod join_utils;
pub mod limit;
pub mod math_expressions;
pub mod memory;
pub mod metrics;
pub mod planner;
pub mod projection;
#[cfg(feature = "regex_expressions")]
pub mod regex_expressions;
pub mod repartition;
pub mod sorts;
pub mod stream;
pub mod string_expressions;
pub(crate) mod tdigest;
pub mod type_coercion;
pub mod udaf;
pub mod udf;
#[cfg(feature = "unicode_expressions")]
pub mod unicode_expressions;
pub mod union;
pub mod values;
pub mod window_functions;
pub mod windows;
