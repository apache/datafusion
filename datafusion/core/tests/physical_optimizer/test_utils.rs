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

//! Test utilities for physical optimizer tests

use std::any::Any;
use std::fmt::Formatter;
use std::sync::{Arc, LazyLock};

use arrow::array::Int32Array;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::{ColumnStatistics, JoinType, NullEquality, Result, Statistics};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{WindowFrame, WindowFunctionDefinition};
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{self, col};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, OrderingRequirements, PhysicalSortExpr,
};
use datafusion_physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use datafusion_physical_optimizer::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::{JoinFilter, JoinOn};
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::windows::{create_window_expr, BoundedWindowAggExec};
use datafusion_physical_plan::{
    displayable, DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode,
    Partitioning, PlanProperties,
};

/// Create a non sorted parquet exec
pub fn parquet_exec(schema: SchemaRef) -> Arc<DataSourceExec> {
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        Arc::new(ParquetSource::new(schema)),
    )
    .with_file(PartitionedFile::new("x".to_string(), 100))
    .build();

    DataSourceExec::from_data_source(config)
}

/// Create a single parquet file that is sorted
pub(crate) fn parquet_exec_with_sort(
    schema: SchemaRef,
    output_ordering: Vec<LexOrdering>,
) -> Arc<DataSourceExec> {
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        Arc::new(ParquetSource::new(schema)),
    )
    .with_file(PartitionedFile::new("x".to_string(), 100))
    .with_output_ordering(output_ordering)
    .build();

    DataSourceExec::from_data_source(config)
}

fn int64_stats() -> ColumnStatistics {
    ColumnStatistics {
        null_count: Precision::Absent,
        sum_value: Precision::Absent,
        max_value: Precision::Exact(1_000_000.into()),
        min_value: Precision::Exact(0.into()),
        distinct_count: Precision::Absent,
    }
}

fn column_stats() -> Vec<ColumnStatistics> {
    vec![
        int64_stats(), // a
        int64_stats(), // b
        int64_stats(), // c
        ColumnStatistics::default(),
        ColumnStatistics::default(),
    ]
}

/// Create parquet datasource exec using schema from [`schema`].
pub(crate) fn parquet_exec_with_stats(file_size: u64) -> Arc<DataSourceExec> {
    let mut statistics = Statistics::new_unknown(&schema());
    statistics.num_rows = Precision::Inexact(10000);
    statistics.column_statistics = column_stats();

    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        Arc::new(ParquetSource::new(schema())),
    )
    .with_file(PartitionedFile::new("x".to_string(), file_size))
    .with_statistics(statistics)
    .build();

    assert_eq!(config.statistics().num_rows, Precision::Inexact(10000));
    DataSourceExec::from_data_source(config)
}

pub fn schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]))
    });
    Arc::clone(&SCHEMA)
}

pub fn create_test_schema() -> Result<SchemaRef> {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        let nullable_column = Field::new("nullable_col", DataType::Int32, true);
        let non_nullable_column = Field::new("non_nullable_col", DataType::Int32, false);
        Arc::new(Schema::new(vec![nullable_column, non_nullable_column]))
    });
    let schema = Arc::clone(&SCHEMA);
    Ok(schema)
}

pub fn create_test_schema2() -> Result<SchemaRef> {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        let col_a = Field::new("col_a", DataType::Int32, true);
        let col_b = Field::new("col_b", DataType::Int32, true);
        Arc::new(Schema::new(vec![col_a, col_b]))
    });
    let schema = Arc::clone(&SCHEMA);
    Ok(schema)
}

// Generate a schema which consists of 5 columns (a, b, c, d, e)
pub fn create_test_schema3() -> Result<SchemaRef> {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, false);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, false);
        let e = Field::new("e", DataType::Int32, false);
        Arc::new(Schema::new(vec![a, b, c, d, e]))
    });
    let schema = Arc::clone(&SCHEMA);
    Ok(schema)
}

pub fn sort_merge_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_on: &JoinOn,
    join_type: &JoinType,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        SortMergeJoinExec::try_new(
            left,
            right,
            join_on.clone(),
            None,
            *join_type,
            vec![SortOptions::default(); join_on.len()],
            NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    )
}

/// make PhysicalSortExpr with default options
pub fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
    sort_expr_options(name, schema, SortOptions::default())
}

/// PhysicalSortExpr with specified options
pub fn sort_expr_options(
    name: &str,
    schema: &Schema,
    options: SortOptions,
) -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: col(name, schema).unwrap(),
        options,
    }
}

pub fn coalesce_partitions_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalescePartitionsExec::new(input))
}

pub fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(schema), None).unwrap()
}

pub fn hash_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    filter: Option<JoinFilter>,
    join_type: &JoinType,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(HashJoinExec::try_new(
        left,
        right,
        on,
        filter,
        join_type,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
    )?))
}

pub fn bounded_window_exec(
    col_name: &str,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    bounded_window_exec_with_partition(col_name, sort_exprs, &[], input)
}

pub fn bounded_window_exec_with_partition(
    col_name: &str,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    partition_by: &[Arc<dyn PhysicalExpr>],
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect::<Vec<_>>();
    let schema = input.schema();
    let window_expr = create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "count".to_owned(),
        &[col(col_name, &schema).unwrap()],
        partition_by,
        &sort_exprs,
        Arc::new(WindowFrame::new(Some(false))),
        schema,
        false,
        false,
        None,
    )
    .unwrap();

    Arc::new(
        BoundedWindowAggExec::try_new(
            vec![window_expr],
            Arc::clone(&input),
            InputOrderMode::Sorted,
            false,
        )
        .unwrap(),
    )
}

pub fn filter_exec(
    predicate: Arc<dyn PhysicalExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(FilterExec::try_new(predicate, input).unwrap())
}

pub fn sort_preserving_merge_exec(
    ordering: LexOrdering,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortPreservingMergeExec::new(ordering, input))
}

pub fn sort_preserving_merge_exec_with_fetch(
    ordering: LexOrdering,
    input: Arc<dyn ExecutionPlan>,
    fetch: usize,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortPreservingMergeExec::new(ordering, input).with_fetch(Some(fetch)))
}

pub fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
    UnionExec::try_new(input).unwrap()
}

pub fn local_limit_exec(
    input: Arc<dyn ExecutionPlan>,
    fetch: usize,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(LocalLimitExec::new(input, fetch))
}

pub fn global_limit_exec(
    input: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(input, skip, fetch))
}

pub fn repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10)).unwrap())
}

pub fn spr_repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10))
            .unwrap()
            .with_preserve_order(),
    )
}

pub fn aggregate_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![],
            vec![],
            input,
            schema,
        )
        .unwrap(),
    )
}

pub fn coalesce_batches_exec(
    input: Arc<dyn ExecutionPlan>,
    batch_size: usize,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalesceBatchesExec::new(input, batch_size))
}

pub fn sort_exec(
    ordering: LexOrdering,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    sort_exec_with_fetch(ordering, None, input)
}

pub fn sort_exec_with_preserve_partitioning(
    ordering: LexOrdering,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortExec::new(ordering, input).with_preserve_partitioning(true))
}

pub fn sort_exec_with_fetch(
    ordering: LexOrdering,
    fetch: Option<usize>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortExec::new(ordering, input).with_fetch(fetch))
}

pub fn projection_exec(
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let proj_exprs: Vec<ProjectionExpr> = expr
        .into_iter()
        .map(|(expr, alias)| ProjectionExpr { expr, alias })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
}

/// A test [`ExecutionPlan`] whose requirements can be configured.
#[derive(Debug)]
pub struct RequirementsTestExec {
    required_input_ordering: Option<LexOrdering>,
    maintains_input_order: bool,
    input: Arc<dyn ExecutionPlan>,
}

impl RequirementsTestExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            required_input_ordering: None,
            maintains_input_order: true,
            input,
        }
    }

    /// sets the required input ordering
    pub fn with_required_input_ordering(
        mut self,
        required_input_ordering: Option<LexOrdering>,
    ) -> Self {
        self.required_input_ordering = required_input_ordering;
        self
    }

    /// set the maintains_input_order flag
    pub fn with_maintains_input_order(mut self, maintains_input_order: bool) -> Self {
        self.maintains_input_order = maintains_input_order;
        self
    }

    /// returns this ExecutionPlan as an `Arc<dyn ExecutionPlan>`
    pub fn into_arc(self) -> Arc<dyn ExecutionPlan> {
        Arc::new(self)
    }
}

impl DisplayAs for RequirementsTestExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RequiredInputOrderingExec")
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for RequirementsTestExec {
    fn name(&self) -> &str {
        "RequiredInputOrderingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self
            .required_input_ordering
            .as_ref()
            .map(|ordering| OrderingRequirements::from(ordering.clone()))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![self.maintains_input_order]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(RequirementsTestExec::new(Arc::clone(&children[0]))
            .with_required_input_ordering(self.required_input_ordering.clone())
            .with_maintains_input_order(self.maintains_input_order)
            .into_arc())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("Test exec does not support execution")
    }
}

/// A [`PlanContext`] object is susceptible to being left in an inconsistent state after
/// untested mutable operations. It is crucial that there be no discrepancies between a plan
/// associated with the root node and the plan generated after traversing all nodes
/// within the [`PlanContext`] tree. In addition to verifying the plans resulting from optimizer
/// rules, it is essential to ensure that the overall tree structure corresponds with the plans
/// contained within the node contexts.
/// TODO: Once [`ExecutionPlan`] implements [`PartialEq`], string comparisons should be
/// replaced with direct plan equality checks.
pub fn check_integrity<T: Clone>(context: PlanContext<T>) -> Result<PlanContext<T>> {
    context
        .transform_up(|node| {
            let children_plans = node.plan.children();
            assert_eq!(node.children.len(), children_plans.len());
            for (child_plan, child_node) in
                children_plans.iter().zip(node.children.iter())
            {
                assert_eq!(
                    displayable(child_plan.as_ref()).one_line().to_string(),
                    displayable(child_node.plan.as_ref()).one_line().to_string()
                );
            }
            Ok(Transformed::no(node))
        })
        .data()
}

// construct a stream partition for test purposes
#[derive(Debug)]
pub struct TestStreamPartition {
    pub schema: SchemaRef,
}

impl PartitionStream for TestStreamPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        unreachable!()
    }
}

/// Create an unbounded stream table without data ordering.
pub fn stream_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        StreamingTableExec::try_new(
            Arc::clone(schema),
            vec![Arc::new(TestStreamPartition {
                schema: Arc::clone(schema),
            }) as _],
            None,
            vec![],
            true,
            None,
        )
        .unwrap(),
    )
}

/// Create an unbounded stream table with data ordering.
pub fn stream_exec_ordered(
    schema: &SchemaRef,
    ordering: LexOrdering,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        StreamingTableExec::try_new(
            Arc::clone(schema),
            vec![Arc::new(TestStreamPartition {
                schema: Arc::clone(schema),
            }) as _],
            None,
            vec![ordering],
            true,
            None,
        )
        .unwrap(),
    )
}

/// Create an unbounded stream table with data ordering and built-in projection.
pub fn stream_exec_ordered_with_projection(
    schema: &SchemaRef,
    ordering: LexOrdering,
) -> Arc<dyn ExecutionPlan> {
    let projection: Vec<usize> = vec![0, 2, 3];

    Arc::new(
        StreamingTableExec::try_new(
            Arc::clone(schema),
            vec![Arc::new(TestStreamPartition {
                schema: Arc::clone(schema),
            }) as _],
            Some(&projection),
            vec![ordering],
            true,
            None,
        )
        .unwrap(),
    )
}

pub fn mock_data() -> Result<Arc<DataSourceExec>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                None,
                Some(1),
                Some(4),
                Some(5),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(6),
                Some(2),
                Some(8),
                Some(9),
            ])),
        ],
    )?;

    MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
}

pub fn build_group_by(input_schema: &SchemaRef, columns: Vec<String>) -> PhysicalGroupBy {
    let mut group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    for column in columns.iter() {
        group_by_expr.push((col(column, input_schema).unwrap(), column.to_string()));
    }
    PhysicalGroupBy::new_single(group_by_expr.clone())
}

pub fn get_optimized_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<String> {
    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());

    let optimized = LimitedDistinctAggregation::new()
        .optimize_plan(Arc::clone(plan), &optimizer_context)?;

    let optimized_result = displayable(optimized.as_ref()).indent(true).to_string();

    Ok(optimized_result)
}

/// Describe the type of aggregate being tested
pub enum TestAggregate {
    /// Testing COUNT(*) type aggregates
    CountStar,

    /// Testing for COUNT(column) aggregate
    ColumnA(Arc<Schema>),
}

impl TestAggregate {
    /// Create a new COUNT(*) aggregate
    pub fn new_count_star() -> Self {
        Self::CountStar
    }

    /// Create a new COUNT(column) aggregate
    pub fn new_count_column(schema: &Arc<Schema>) -> Self {
        Self::ColumnA(Arc::clone(schema))
    }

    /// Return appropriate expr depending if COUNT is for col or table (*)
    pub fn count_expr(&self, schema: &Schema) -> AggregateFunctionExpr {
        AggregateExprBuilder::new(count_udaf(), vec![self.column()])
            .schema(Arc::new(schema.clone()))
            .alias(self.column_name())
            .build()
            .unwrap()
    }

    /// what argument would this aggregate need in the plan?
    fn column(&self) -> Arc<dyn PhysicalExpr> {
        match self {
            Self::CountStar => expressions::lit(COUNT_STAR_EXPANSION),
            Self::ColumnA(s) => col("a", s).unwrap(),
        }
    }

    /// What name would this aggregate produce in a plan?
    pub fn column_name(&self) -> &'static str {
        match self {
            Self::CountStar => "COUNT(*)",
            Self::ColumnA(_) => "COUNT(a)",
        }
    }

    /// What is the expected count?
    pub fn expected_count(&self) -> i64 {
        match self {
            TestAggregate::CountStar => 3,
            TestAggregate::ColumnA(_) => 2,
        }
    }
}
