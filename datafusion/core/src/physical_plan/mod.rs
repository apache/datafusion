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
use self::{
    coalesce_partitions::CoalescePartitionsExec, display::DisplayableExecutionPlan,
};
pub use crate::common::{ColumnStatistics, Statistics};
use crate::error::Result;
use crate::physical_plan::expressions::PhysicalSortExpr;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

pub use datafusion_expr::Accumulator;
pub use datafusion_expr::ColumnarValue;
pub use datafusion_physical_expr::aggregate::row_accumulator::RowAccumulator;
use datafusion_physical_expr::equivalence::OrderingEquivalenceProperties;
pub use display::DisplayFormatType;
use futures::stream::{Stream, TryStreamExt};

use datafusion_common::DataFusionError;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// backwards compatibility
pub use datafusion_execution::{
    ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
pub use datafusion_physical_expr::{Distribution, Partitioning};

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
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// Physical planner interface
pub use self::planner::PhysicalPlanner;

/// Indicate whether a data exchange is needed for the input of `plan`, which will be very helpful
/// especially for the distributed engine to judge whether need to deal with shuffling.
/// Currently there are 3 kinds of execution plan which needs data exchange
///     1. RepartitionExec for changing the partition number between two operators
///     2. CoalescePartitionsExec for collapsing all of the partitions into one without ordering guarantee
///     3. SortPreservingMergeExec for collapsing all of the sorted partitions into one with ordering guarantee
pub fn need_data_exchange(plan: Arc<dyn ExecutionPlan>) -> bool {
    if let Some(repart) = plan.as_any().downcast_ref::<RepartitionExec>() {
        !matches!(
            repart.output_partitioning(),
            Partitioning::RoundRobinBatch(_)
        )
    } else if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>()
    {
        coalesce.input().output_partitioning().partition_count() > 1
    } else if let Some(sort_preserving_merge) =
        plan.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        sort_preserving_merge
            .input()
            .output_partitioning()
            .partition_count()
            > 1
    } else {
        false
    }
}

// backwards compatibility
pub use datafusion_execution::plan::with_new_children_if_necessary;

/// Return a [wrapper](DisplayableExecutionPlan) around an
/// [`ExecutionPlan`] which can be displayed in various easier to
/// understand ways.
///
/// ```
/// use datafusion::prelude::*;
/// use datafusion::physical_plan::displayable;
/// use object_store::path::Path;
///
/// #[tokio::main]
/// async fn main() {
///   // Hard code target_partitions as it appears in the RepartitionExec output
///   let config = SessionConfig::new()
///       .with_target_partitions(3);
///   let mut ctx = SessionContext::with_config(config);
///
///   // register the a table
///   ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await.unwrap();
///
///   // create a plan to run a SQL query
///   let dataframe = ctx.sql("SELECT a FROM example WHERE a < 5").await.unwrap();
///   let physical_plan = dataframe.create_physical_plan().await.unwrap();
///
///   // Format using display string
///   let displayable_plan = displayable(physical_plan.as_ref());
///   let plan_string = format!("{}", displayable_plan.indent());
///
///   let working_directory = std::env::current_dir().unwrap();
///   let normalized = Path::from_filesystem_path(working_directory).unwrap();
///   let plan_string = plan_string.replace(normalized.as_ref(), "WORKING_DIR");
///
///   assert_eq!("CoalesceBatchesExec: target_batch_size=8192\
///              \n  FilterExec: a@0 < 5\
///              \n    RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1\
///              \n      CsvExec: file_groups={1 group: [[WORKING_DIR/tests/data/example.csv]]}, projection=[a], has_header=true",
///               plan_string.trim());
///
///   let one_line = format!("{}", displayable_plan.one_line());
///   assert_eq!("CoalesceBatchesExec: target_batch_size=8192", one_line.trim());
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
) -> Result<(), V::Error> {
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
/// ProjectionExec: id
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
    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error>;

    /// Invoked on an `ExecutionPlan` plan *after* all of its child
    /// inputs have been visited. The return value is handled the same
    /// as the return value of `pre_visit`. The provided default
    /// implementation returns `Ok(true)`.
    fn post_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Recursively calls `pre_visit` and `post_visit` for this node and
/// all of its children, as described on [`ExecutionPlanVisitor`]
pub fn visit_execution_plan<V: ExecutionPlanVisitor>(
    plan: &dyn ExecutionPlan,
    visitor: &mut V,
) -> Result<(), V::Error> {
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
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let stream = execute_stream(plan, context)?;
    common::collect(stream).await
}

/// Execute the [ExecutionPlan] and return a single stream of results
pub fn execute_stream(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
        1 => plan.execute(0, context),
        _ => {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(plan.clone());
            // CoalescePartitionsExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0, context)
        }
    }
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<Vec<RecordBatch>>> {
    let streams = execute_stream_partitioned(plan, context)?;

    // Execute the plan and collect the results into batches.
    let handles = streams
        .into_iter()
        .enumerate()
        .map(|(idx, stream)| async move {
            let handle = tokio::task::spawn(stream.try_collect());
            AbortOnDropSingle::new(handle).await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "collect_partitioned partition {idx} panicked: {e}"
                ))
            })?
        });

    futures::future::try_join_all(handles).await
}

/// Execute the [ExecutionPlan] and return a vec with one stream per output partition
pub fn execute_stream_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<SendableRecordBatchStream>> {
    let num_partitions = plan.output_partitioning().partition_count();
    let mut streams = Vec::with_capacity(num_partitions);
    for i in 0..num_partitions {
        streams.push(plan.execute(i, context.clone())?);
    }
    Ok(streams)
}

/// Retrieves the ordering equivalence properties for a given schema and output ordering.
pub fn ordering_equivalence_properties_helper(
    schema: SchemaRef,
    eq_orderings: &[LexOrdering],
) -> OrderingEquivalenceProperties {
    let mut oep = OrderingEquivalenceProperties::new(schema);
    let first_ordering = if let Some(first) = eq_orderings.first() {
        first
    } else {
        // Return an empty OrderingEquivalenceProperties:
        return oep;
    };
    let first_column = first_ordering
        .iter()
        .map(|e| TryFrom::try_from(e.clone()))
        .collect::<Result<Vec<_>>>();
    let checked_column_first = if let Ok(first) = first_column {
        first
    } else {
        // Return an empty OrderingEquivalenceProperties:
        return oep;
    };
    // First entry among eq_orderings is the head, skip it:
    for ordering in eq_orderings.iter().skip(1) {
        let column = ordering
            .iter()
            .map(|e| TryFrom::try_from(e.clone()))
            .collect::<Result<Vec<_>>>();
        if let Ok(column) = column {
            if !column.is_empty() {
                oep.add_equal_conditions((&checked_column_first, &column))
            }
        }
    }
    oep
}

use datafusion_physical_expr::expressions::Column;
pub use datafusion_physical_expr::window::WindowExpr;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_expr::LexOrdering;
pub use datafusion_physical_expr::{AggregateExpr, PhysicalExpr};

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
pub mod coalesce_batches;
pub mod coalesce_partitions;
pub mod common;
pub mod display;
pub mod empty;
pub mod explain;
pub mod file_format;
pub mod filter;
pub mod insert;
pub mod joins;
pub mod limit;
pub mod memory;
pub mod planner;
pub mod projection;
pub mod repartition;
pub mod sorts;
pub mod stream;
pub mod streaming;
pub mod udaf;
pub mod union;
pub mod unnest;
pub mod values;
pub mod windows;

// backwards compatibility
pub use datafusion_execution::metrics;
pub use datafusion_execution::tree_node;

use crate::execution::context::TaskContext;
use crate::physical_plan::common::AbortOnDropSingle;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
pub use datafusion_physical_expr::{
    expressions, functions, hash_utils, type_coercion, udf,
};

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Schema;

    use crate::physical_plan::Distribution;
    use crate::physical_plan::Partitioning;
    use crate::physical_plan::PhysicalExpr;
    use datafusion_physical_expr::expressions::Column;

    use std::sync::Arc;

    #[tokio::test]
    async fn partitioning_satisfy_distribution() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("column_1", DataType::Int64, false),
            arrow::datatypes::Field::new("column_2", DataType::Utf8, false),
        ]));

        let partition_exprs1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new_with_schema("column_1", &schema).unwrap()),
            Arc::new(Column::new_with_schema("column_2", &schema).unwrap()),
        ];

        let partition_exprs2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new_with_schema("column_2", &schema).unwrap()),
            Arc::new(Column::new_with_schema("column_1", &schema).unwrap()),
        ];

        let distribution_types = vec![
            Distribution::UnspecifiedDistribution,
            Distribution::SinglePartition,
            Distribution::HashPartitioned(partition_exprs1.clone()),
        ];

        let single_partition = Partitioning::UnknownPartitioning(1);
        let unspecified_partition = Partitioning::UnknownPartitioning(10);
        let round_robin_partition = Partitioning::RoundRobinBatch(10);
        let hash_partition1 = Partitioning::Hash(partition_exprs1, 10);
        let hash_partition2 = Partitioning::Hash(partition_exprs2, 10);

        for distribution in distribution_types {
            let result = (
                single_partition.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
                unspecified_partition.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
                round_robin_partition.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
                hash_partition1.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
                hash_partition2.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
            );

            match distribution {
                Distribution::UnspecifiedDistribution => {
                    assert_eq!(result, (true, true, true, true, true))
                }
                Distribution::SinglePartition => {
                    assert_eq!(result, (true, false, false, false, false))
                }
                Distribution::HashPartitioned(_) => {
                    assert_eq!(result, (false, false, false, true, false))
                }
            }
        }

        Ok(())
    }
}
