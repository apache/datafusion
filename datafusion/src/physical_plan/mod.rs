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

use std::fmt::{self, Debug, Display};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{any::Any, pin::Pin};

use crate::execution::context::ExecutionContextState;
use crate::logical_plan::LogicalPlan;
use crate::{error::Result, scalar::ScalarValue};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};

use async_trait::async_trait;
pub use display::DisplayFormatType;
use futures::stream::Stream;

use self::{display::DisplayableExecutionPlan, merge::MergeExec};
use hashbrown::HashMap;

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

/// SQL metric type
#[derive(Debug, Clone)]
pub enum MetricType {
    /// Simple counter
    Counter,
    /// Wall clock time in nanoseconds
    TimeNanos,
}

/// SQL metric such as counter (number of input or output rows) or timing information about
/// a physical operator.
#[derive(Debug)]
pub struct SQLMetric {
    /// Metric value
    value: AtomicUsize,
    /// Metric type
    metric_type: MetricType,
}

impl Clone for SQLMetric {
    fn clone(&self) -> Self {
        Self {
            value: AtomicUsize::new(self.value.load(Ordering::Relaxed)),
            metric_type: self.metric_type.clone(),
        }
    }
}

impl SQLMetric {
    // relaxed ordering for operations on `value` poses no issues
    // we're purely using atomic ops with no associated memory ops

    /// Create a new metric for tracking a counter
    pub fn counter() -> Arc<SQLMetric> {
        Arc::new(SQLMetric::new(MetricType::Counter))
    }

    /// Create a new metric for tracking time in nanoseconds
    pub fn time_nanos() -> Arc<SQLMetric> {
        Arc::new(SQLMetric::new(MetricType::TimeNanos))
    }

    /// Create a new SQLMetric
    pub fn new(metric_type: MetricType) -> Self {
        Self {
            value: AtomicUsize::new(0),
            metric_type,
        }
    }

    /// Add to the value
    pub fn add(&self, n: usize) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the current value
    pub fn value(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
pub trait PhysicalPlanner {
    /// Create a physical plan from a logical plan
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
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
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream>;

    /// Return a snapshot of the metrics collected during execution
    fn metrics(&self) -> HashMap<String, SQLMetric> {
        HashMap::new()
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
}

/// Return a [wrapper](DisplayableExecutionPlan) around an
/// [`ExecutionPlan`] which can be displayed in various easier to
/// understand ways.
///
/// ```
/// use datafusion::prelude::*;
/// use datafusion::physical_plan::displayable;
///
/// // Hard code concurrency as it appears in the RepartitionExec output
/// let config = ExecutionConfig::new()
///     .with_concurrency(3);
/// let mut ctx = ExecutionContext::with_config(config);
///
/// // register the a table
/// ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).unwrap();
///
/// // create a plan to run a SQL query
/// let plan = ctx
///    .create_logical_plan("SELECT a FROM example WHERE a < 5")
///    .unwrap();
/// let plan = ctx.optimize(&plan).unwrap();
/// let physical_plan = ctx.create_physical_plan(&plan).unwrap();
///
/// // Format using display string
/// let displayable_plan = displayable(physical_plan.as_ref());
/// let plan_string = format!("{}", displayable_plan.indent());
///
/// assert_eq!("ProjectionExec: expr=[a]\
///            \n  CoalesceBatchesExec: target_batch_size=4096\
///            \n    FilterExec: a < 5\
///            \n      RepartitionExec: partitioning=RoundRobinBatch(3)\
///            \n        CsvExec: source=Path(tests/example.csv: [tests/example.csv]), has_header=true",
///             plan_string.trim());
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
pub async fn collect(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(vec![]),
        1 => {
            let it = plan.execute(0).await?;
            common::collect(it).await
        }
        _ => {
            // merge into a single partition
            let plan = MergeExec::new(plan.clone());
            // MergeExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            common::collect(plan.execute(0).await?).await
        }
    }
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect_partitioned(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<Vec<RecordBatch>>> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(vec![]),
        1 => {
            let it = plan.execute(0).await?;
            Ok(vec![common::collect(it).await?])
        }
        _ => {
            let mut partitions = vec![];
            for i in 0..plan.output_partitioning().partition_count() {
                partitions.push(common::collect(plan.execute(i).await?).await?)
            }
            Ok(partitions)
        }
    }
}

/// Partitioning schemes supported by operators.
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified
    /// number of partitions
    /// This partitioning scheme is not yet fully supported. See [ARROW-11011](https://issues.apache.org/jira/browse/ARROW-11011)
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Unknown partitioning scheme with a known number of partitions
    UnknownPartitioning(usize),
}

impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            RoundRobinBatch(n) => *n,
            Hash(_, n) => *n,
            UnknownPartitioning(n) => *n,
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

/// Represents the result from an expression
#[derive(Clone)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value
    Scalar(ScalarValue),
}

impl ColumnarValue {
    fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Array(array_value) => array_value.data_type().clone(),
            ColumnarValue::Scalar(scalar_value) => scalar_value.get_datatype(),
        }
    }

    fn into_array(self, num_rows: usize) -> ArrayRef {
        match self {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
        }
    }
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

    /// the accumulator used to accumulate values from the expressions.
    /// the accumulator expects the same number of arguments as `expressions` and must
    /// return states with the same description as `state_fields`
    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>>;

    /// expressions that are passed to the WindowAccumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;
}

/// A window expression that is a built-in window function
pub trait BuiltInWindowFunctionExpr: Send + Sync + Debug {
    /// Returns the aggregate expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "BuiltInWindowFunctionExpr: default name"
    }

    /// the accumulator used to accumulate values from the expressions.
    /// the accumulator expects the same number of arguments as `expressions` and must
    /// return states with the same description as `state_fields`
    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>>;
}

/// An accumulator represents a stateful object that lives throughout the evaluation of multiple rows and
/// generically accumulates values.
///
/// An accumulator knows how to:
/// * update its state from inputs via `update`
/// * convert its internal state to a vector of scalar values
/// * update its state from multiple accumulators' states via `merge`
/// * compute the final value from its internal state via `evaluate`
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    // in the case of an average on which we track `sum` and `n`, this function should return a vector
    // of two values, sum and n.
    fn state(&self) -> Result<Vec<ScalarValue>>;

    /// updates the accumulator's state from a vector of scalars.
    fn update(&mut self, values: &[ScalarValue]) -> Result<()>;

    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        };
        (0..values[0].len()).try_for_each(|index| {
            let v = values
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            self.update(&v)
        })
    }

    /// updates the accumulator's state from a vector of scalars.
    fn merge(&mut self, states: &[ScalarValue]) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };
        (0..states[0].len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            self.merge(&v)
        })
    }

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;
}

/// A window accumulator represents a stateful object that lives throughout the evaluation of multiple
/// rows and generically accumulates values.
///
/// An accumulator knows how to:
/// * update its state from inputs via `update`
/// * convert its internal state to a vector of scalar values
/// * update its state from multiple accumulators' states via `merge`
/// * compute the final value from its internal state via `evaluate`
pub trait WindowAccumulator: Send + Sync + Debug {
    /// scans the accumulator's state from a vector of scalars, similar to Accumulator it also
    /// optionally generates values.
    fn scan(&mut self, values: &[ScalarValue]) -> Result<Option<ScalarValue>>;

    /// scans the accumulator's state from a vector of arrays.
    fn scan_batch(
        &mut self,
        num_rows: usize,
        values: &[ArrayRef],
    ) -> Result<Option<Vec<ScalarValue>>> {
        // note that for row_number and rank this might be different
        if values.is_empty() {
            return Ok(None);
        };
        // transpose columnar to row based so that we can apply window
        let result: Vec<Option<ScalarValue>> = (0..num_rows)
            .map(|index| {
                let v = values
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, index))
                    .collect::<Result<Vec<_>>>()?;
                self.scan(&v)
            })
            .into_iter()
            .collect::<Result<Vec<Option<ScalarValue>>>>()?;
        let result: Option<Vec<ScalarValue>> = result.into_iter().collect();
        Ok(result)
    }

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<Option<ScalarValue>>;
}

pub mod aggregates;
pub mod array_expressions;
pub mod coalesce_batches;
pub mod common;
pub mod cross_join;
#[cfg(feature = "crypto_expressions")]
pub mod crypto_expressions;
pub mod csv;
pub mod datetime_expressions;
pub mod display;
pub mod distinct_expressions;
pub mod empty;
pub mod explain;
pub mod expressions;
pub mod filter;
pub mod functions;
pub mod group_scalar;
pub mod hash_aggregate;
pub mod hash_join;
pub mod hash_utils;
pub mod limit;
pub mod math_expressions;
pub mod memory;
pub mod merge;
pub mod parquet;
pub mod planner;
pub mod projection;
#[cfg(feature = "regex_expressions")]
pub mod regex_expressions;
pub mod repartition;
pub mod sort;
pub mod string_expressions;
pub mod type_coercion;
pub mod udaf;
pub mod udf;
#[cfg(feature = "unicode_expressions")]
pub mod unicode_expressions;
pub mod union;
pub mod window_functions;
pub mod windows;
