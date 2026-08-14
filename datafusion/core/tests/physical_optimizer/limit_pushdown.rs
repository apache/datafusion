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

use std::fmt::Formatter;
use std::sync::Arc;

use crate::physical_optimizer::test_utils::{
    TestScan, coalesce_partitions_exec, global_limit_exec, hash_join_exec,
    local_limit_exec, sort_exec, sort_preserving_merge_exec, stream_exec,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Statistics;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::NestedLoopJoinExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, StatisticsArgs,
    get_plan_string,
};

fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Int32, true),
    ]))
}

fn projection_exec(
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(ProjectionExec::try_new(
        vec![
            (col("c1", schema.as_ref()).unwrap(), "c1".to_string()),
            (col("c2", schema.as_ref()).unwrap(), "c2".to_string()),
            (col("c3", schema.as_ref()).unwrap(), "c3".to_string()),
        ],
        input,
    )?))
}

fn filter_exec(
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(FilterExec::try_new(
        Arc::new(BinaryExpr::new(
            col("c3", schema.as_ref()).unwrap(),
            Operator::Gt,
            lit(0),
        )),
        input,
    )?))
}

fn repartition_exec(
    streaming_table: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(RepartitionExec::try_new(
        streaming_table,
        Partitioning::RoundRobinBatch(8),
    )?))
}

fn empty_exec(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(schema))
}

fn nested_loop_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(NestedLoopJoinExec::try_new(
        left, right, None, &join_type, None,
    )?))
}

fn format_plan(plan: &Arc<dyn ExecutionPlan>) -> String {
    get_plan_string(plan).join("\n")
}

#[derive(Debug)]
struct TestCombinerExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl TestCombinerExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            input,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for TestCombinerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TestCombinerExec")
    }
}

impl ExecutionPlan for TestCombinerExec {
    fn name(&self) -> &str {
        "TestCombinerExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&PhysicalExprRef) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // `TestCombinerExec` owns no `PhysicalExpr`s.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!("TestCombinerExec is only used by optimizer tests")
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(self.schema().as_ref())))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

/// Test plan that reports a fixed `fetch` but cannot change it through
/// `with_fetch`. It can optionally allow limits to be pushed to its child.
#[derive(Debug)]
struct TestFetchOnlyExec {
    input: Arc<dyn ExecutionPlan>,
    fetch: Option<usize>,
    supports_limit_pushdown: bool,
    properties: Arc<PlanProperties>,
}

impl TestFetchOnlyExec {
    fn new(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            input,
            fetch,
            supports_limit_pushdown: false,
            properties: Arc::new(properties),
        }
    }

    /// Set whether limits may be pushed through this operator to its child.
    fn with_supports_limit_pushdown(mut self, supports: bool) -> Self {
        self.supports_limit_pushdown = supports;
        self
    }
}

impl DisplayAs for TestFetchOnlyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TestFetchOnlyExec")?;
        if let Some(fetch) = self.fetch {
            write!(f, ": fetch={fetch}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for TestFetchOnlyExec {
    fn name(&self) -> &str {
        "TestFetchOnlyExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&PhysicalExprRef) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // `TestFetchOnlyExec` owns no `PhysicalExpr`s.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(
            Self::new(children[0].clone(), self.fetch)
                .with_supports_limit_pushdown(self.supports_limit_pushdown),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!("TestFetchOnlyExec is only used by optimizer tests")
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(self.schema().as_ref())))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.supports_limit_pushdown
    }
}

/// Test multi-child plan with a single output partition that allows limit
/// pushdown. Optionally absorbs a fetch via `with_fetch`.
#[derive(Debug, Clone)]
struct TestMultiChildExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
    supports_fetch: bool,
    fetch: Option<usize>,
}

impl TestMultiChildExec {
    fn new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(inputs[0].schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            inputs,
            properties: Arc::new(properties),
            supports_fetch: false,
            fetch: None,
        }
    }

    /// Set whether `with_fetch()` returns `Some` (true) or `None` (false).
    fn with_supports_fetch(mut self, supports: bool) -> Self {
        self.supports_fetch = supports;
        self
    }
}

impl DisplayAs for TestMultiChildExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TestMultiChildExec")?;
        if let Some(fetch) = self.fetch {
            write!(f, ": fetch={fetch}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for TestMultiChildExec {
    fn name(&self) -> &str {
        "TestMultiChildExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&PhysicalExprRef) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // `TestMultiChildExec` owns no `PhysicalExpr`s.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), self.inputs.len());
        let mut new_plan = Self::new(children).with_supports_fetch(self.supports_fetch);
        new_plan.fetch = self.fetch;
        Ok(Arc::new(new_plan))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!("TestMultiChildExec is only used by optimizer tests")
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(self.schema().as_ref())))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        if self.supports_fetch {
            let mut new_plan = self.clone();
            new_plan.fetch = fetch;
            Some(Arc::new(new_plan))
        } else {
            None
        }
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

#[test]
fn transforms_streaming_table_exec_into_fetching_version_when_skip_is_zero() -> Result<()>
{
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let global_limit = global_limit_exec(streaming_table, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @"StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true, fetch=5"
    );

    Ok(())
}

#[test]
fn transforms_streaming_table_exec_into_fetching_version_and_keeps_the_global_limit_when_skip_is_nonzero()
-> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let global_limit = global_limit_exec(streaming_table, 2, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=2, fetch=5
      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=2, fetch=5
      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true, fetch=7
    "
    );

    Ok(())
}

#[test]
fn keeps_global_limit_above_fetch_capable_multi_partition_scan() -> Result<()> {
    let schema = create_schema();
    let scan = Arc::new(
        TestScan::new(schema, vec![])
            .with_supports_fetch(true)
            .with_partition_count(2),
    );
    let global_limit = global_limit_exec(scan, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    CoalescePartitionsExec: fetch=5
      TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn keeps_global_offset_limit_above_fetch_capable_multi_partition_scan() -> Result<()> {
    let schema = create_schema();
    let scan = Arc::new(
        TestScan::new(schema, vec![])
            .with_supports_fetch(true)
            .with_partition_count(2),
    );
    let global_limit = global_limit_exec(scan, 2, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    GlobalLimitExec: skip=2, fetch=5
      CoalescePartitionsExec: fetch=7
        TestScan: fetch=7
    "
    );

    Ok(())
}

#[test]
fn preserves_existing_per_partition_fetch_under_global_limit() -> Result<()> {
    let schema = create_schema();
    let scan = Arc::new(
        TestScan::new(schema, vec![])
            .with_supports_fetch(true)
            .with_partition_count(2),
    );
    let scan = scan.with_fetch(Some(3)).unwrap();
    let global_limit = global_limit_exec(scan, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    CoalescePartitionsExec: fetch=5
      TestScan: fetch=3
    "
    );

    Ok(())
}

#[test]
fn adds_global_boundary_above_unfetchable_multi_partition_scan() -> Result<()> {
    let schema = create_schema();
    let scan = Arc::new(TestScan::new(schema, vec![]).with_partition_count(2));
    let global_limit = global_limit_exec(scan, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    CoalescePartitionsExec: fetch=5
      TestScan
    "
    );

    Ok(())
}

#[test]
fn materializes_global_boundary_before_pushing_into_union_children() -> Result<()> {
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let union = UnionExec::try_new(vec![left, right])?;
    let global_limit = global_limit_exec(union, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    CoalescePartitionsExec: fetch=5
      UnionExec
        TestScan: fetch=5
        TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn materializes_global_boundary_for_offset_only_multi_partition_scan() -> Result<()> {
    let schema = create_schema();
    let scan = Arc::new(TestScan::new(schema, vec![]).with_partition_count(2));
    let global_limit = global_limit_exec(scan, 2, None);

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    GlobalLimitExec: skip=2, fetch=None
      CoalescePartitionsExec
        TestScan
    "
    );

    Ok(())
}

#[test]
fn removes_noop_global_limit_without_materializing_boundary() -> Result<()> {
    let schema = create_schema();
    let scan = Arc::new(TestScan::new(schema, vec![]));
    let noop_global_limit = global_limit_exec(scan, 0, None);

    let optimized =
        LimitPushdown::new().optimize(noop_global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @"TestScan"
    );

    Ok(())
}

#[test]
fn preserves_outer_global_limit_across_nested_global_limit() -> Result<()> {
    let schema = create_schema();
    let scan = Arc::new(
        TestScan::new(schema, vec![])
            .with_supports_fetch(true)
            .with_partition_count(2),
    );
    let inner = global_limit_exec(scan, 0, Some(10));
    let outer = global_limit_exec(inner, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(outer, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    CoalescePartitionsExec: fetch=5
      TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn preserves_outer_global_limit_across_noop_global_limit() -> Result<()> {
    let schema = create_schema();
    let scan = Arc::new(
        TestScan::new(schema, vec![])
            .with_supports_fetch(true)
            .with_partition_count(2),
    );
    let noop = global_limit_exec(scan, 0, None);
    let outer = global_limit_exec(noop, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(outer, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    CoalescePartitionsExec: fetch=5
      TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn materializes_pending_global_limit_below_extension_combiner() -> Result<()> {
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let union = UnionExec::try_new(vec![left, right])?;
    let combiner = Arc::new(TestCombinerExec::new(union));
    let global_limit = global_limit_exec(combiner, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    TestCombinerExec
      CoalescePartitionsExec: fetch=5
        UnionExec
          TestScan: fetch=5
          TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn materializes_global_limit_before_multi_child_extension() -> Result<()> {
    // Regression test: a pending global limit used to be cloned to every
    // child of a multi-child node, so each child applied the full LIMIT and
    // the merged output exceeded it. The limit must stay above the node.
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let custom = Arc::new(TestMultiChildExec::new(vec![left, right]));
    let global_limit = global_limit_exec(custom, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    GlobalLimitExec: skip=0, fetch=5
      TestMultiChildExec
        TestScan: fetch=5
        TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn materializes_global_offset_limit_before_multi_child_extension() -> Result<()> {
    // The offset stays in the GlobalLimitExec; children only get a fetch hint
    // of skip + fetch for early stopping.
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let custom = Arc::new(TestMultiChildExec::new(vec![left, right]));
    let global_limit = global_limit_exec(custom, 2, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    GlobalLimitExec: skip=2, fetch=5
      TestMultiChildExec
        TestScan: fetch=7
        TestScan: fetch=7
    "
    );

    Ok(())
}

#[test]
fn multi_child_extension_absorbs_global_limit_and_hints_children() -> Result<()> {
    // When the multi-child node absorbs the fetch itself, no extra limit is
    // needed; children still receive the same fetch for early stopping.
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let custom =
        Arc::new(TestMultiChildExec::new(vec![left, right]).with_supports_fetch(true));
    let global_limit = global_limit_exec(custom, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    TestMultiChildExec: fetch=5
      TestScan: fetch=5
      TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn materializes_local_limit_before_multi_child_extension() -> Result<()> {
    // A local limit also cannot be replicated to every child of a multi-child
    // node with a single output partition.
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let custom = Arc::new(TestMultiChildExec::new(vec![left, right]));
    let local_limit = local_limit_exec(custom, 5);

    let optimized = LimitPushdown::new().optimize(local_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    GlobalLimitExec: skip=0, fetch=5
      TestMultiChildExec
        TestScan: fetch=5
        TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn upgrades_pending_local_limit_before_extension_combiner() -> Result<()> {
    let schema = create_schema();
    let inner_left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let inner_right =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let inner_union = UnionExec::try_new(vec![inner_left, inner_right])?;
    let combiner = Arc::new(TestCombinerExec::new(inner_union));
    let outer_child = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let outer_union = UnionExec::try_new(vec![combiner, outer_child])?;
    let local_limit = local_limit_exec(outer_union, 5);

    let optimized = LimitPushdown::new().optimize(local_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    UnionExec
      TestCombinerExec
        CoalescePartitionsExec: fetch=5
          UnionExec
            TestScan: fetch=5
            TestScan: fetch=5
      TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn upgrades_pending_local_limit_before_noop_global_wrapper() -> Result<()> {
    let schema = create_schema();
    let inner_left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let inner_right =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let inner_union = UnionExec::try_new(vec![inner_left, inner_right])?;
    let noop_global = global_limit_exec(inner_union, 0, None);
    let outer_child = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let outer_union = UnionExec::try_new(vec![noop_global, outer_child])?;
    let local_limit = local_limit_exec(outer_union, 5);

    let optimized = LimitPushdown::new().optimize(local_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    UnionExec
      CoalescePartitionsExec: fetch=5
        UnionExec
          TestScan: fetch=5
          TestScan: fetch=5
      TestScan: fetch=5
    "
    );

    Ok(())
}

#[test]
fn keeps_global_limit_above_local_limit_on_multi_partition_union() -> Result<()> {
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let union = UnionExec::try_new(vec![left, right])?;
    let local_limit = local_limit_exec(union, 3);
    let global_limit = global_limit_exec(local_limit, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    CoalescePartitionsExec: fetch=5
      UnionExec
        TestScan: fetch=3
        TestScan: fetch=3
    "
    );

    Ok(())
}

#[test]
fn keeps_global_offset_limit_above_local_limit_on_multi_partition_union() -> Result<()> {
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let union = UnionExec::try_new(vec![left, right])?;
    let local_limit = local_limit_exec(union, 3);
    let global_limit = global_limit_exec(local_limit, 2, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    GlobalLimitExec: skip=2, fetch=5
      CoalescePartitionsExec: fetch=7
        UnionExec
          TestScan: fetch=3
          TestScan: fetch=3
    "
    );

    Ok(())
}

fn join_on_columns(
    left_col: &str,
    right_col: &str,
) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
    vec![(
        Arc::new(datafusion_physical_expr::expressions::Column::new(
            left_col, 0,
        )) as _,
        Arc::new(datafusion_physical_expr::expressions::Column::new(
            right_col, 0,
        )) as _,
    )]
}

#[test]
fn absorbs_limit_into_hash_join_inner() -> Result<()> {
    // HashJoinExec with Inner join should absorb limit via with_fetch
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let on = join_on_columns("c1", "c1");
    let hash_join = hash_join_exec(left, right, on, None, &JoinType::Inner)?;
    let global_limit = global_limit_exec(hash_join, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)]
        TestScan
        TestScan
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    // The limit should be absorbed by the hash join (not pushed to children)
    insta::assert_snapshot!(
        optimized,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)], fetch=5
      TestScan
      TestScan
    "
    );

    Ok(())
}

#[test]
fn absorbs_limit_into_hash_join_right() -> Result<()> {
    // HashJoinExec with Right join should absorb limit via with_fetch
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let on = join_on_columns("c1", "c1");
    let hash_join = hash_join_exec(left, right, on, None, &JoinType::Right)?;
    let global_limit = global_limit_exec(hash_join, 0, Some(10));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=10
      HashJoinExec: mode=Partitioned, join_type=Right, on=[(c1@0, c1@0)]
        TestScan
        TestScan
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    // The limit should be absorbed by the hash join
    insta::assert_snapshot!(
        optimized,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Right, on=[(c1@0, c1@0)], fetch=10
      TestScan
      TestScan
    "
    );

    Ok(())
}

#[test]
fn absorbs_limit_into_hash_join_left() -> Result<()> {
    // during probing, then unmatched rows at the end, stopping when limit is reached
    let schema = create_schema();
    let left =
        Arc::new(TestScan::new(Arc::clone(&schema), vec![]).with_supports_fetch(true));
    let right = Arc::new(TestScan::new(schema, vec![]).with_supports_fetch(true));
    let on = join_on_columns("c1", "c1");
    let hash_join = hash_join_exec(left, right, on, None, &JoinType::Left)?;
    let global_limit = global_limit_exec(hash_join, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      HashJoinExec: mode=Partitioned, join_type=Left, on=[(c1@0, c1@0)]
        TestScan
        TestScan
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    // Left join now absorbs the limit
    insta::assert_snapshot!(
        optimized,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Left, on=[(c1@0, c1@0)], fetch=5
      TestScan
      TestScan
    "
    );

    Ok(())
}

#[test]
fn absorbs_limit_with_skip_into_hash_join() -> Result<()> {
    let schema = create_schema();
    let left = empty_exec(Arc::clone(&schema));
    let right = empty_exec(Arc::clone(&schema));
    let on = join_on_columns("c1", "c1");
    let hash_join = hash_join_exec(left, right, on, None, &JoinType::Inner)?;
    let global_limit = global_limit_exec(hash_join, 3, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=3, fetch=5
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)]
        EmptyExec
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    // With skip, GlobalLimit is kept but fetch (skip + limit = 8) is absorbed by the join
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=3, fetch=5
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)], fetch=8
        EmptyExec
        EmptyExec
    "
    );

    Ok(())
}

#[test]
fn pushes_global_limit_exec_through_projection_exec() -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let filter = filter_exec(Arc::clone(&schema), streaming_table)?;
    let projection = projection_exec(schema, filter)?;
    let global_limit = global_limit_exec(projection, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
        FilterExec: c3@2 > 0
          StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
      FilterExec: c3@2 > 0, fetch=5
        StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    Ok(())
}

#[test]
fn pushes_global_limit_into_multiple_fetch_plans() -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let projection = projection_exec(Arc::clone(&schema), streaming_table)?;
    let repartition = repartition_exec(projection)?;
    let ordering: LexOrdering = [PhysicalSortExpr {
        expr: col("c1", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let sort = sort_exec(ordering.clone(), repartition);
    let spm = sort_preserving_merge_exec(ordering, sort);
    let global_limit = global_limit_exec(spm, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      SortPreservingMergeExec: [c1@0 ASC]
        SortExec: expr=[c1@0 ASC], preserve_partitioning=[false]
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
              StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    SortPreservingMergeExec: [c1@0 ASC], fetch=5
      SortExec: TopK(fetch=5), expr=[c1@0 ASC], preserve_partitioning=[false]
        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
          ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
            StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    Ok(())
}

#[test]
fn keeps_pushed_local_limit_exec_when_there_are_multiple_input_partitions() -> Result<()>
{
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let repartition = repartition_exec(streaming_table)?;
    let filter = filter_exec(schema, repartition)?;
    let coalesce_partitions = coalesce_partitions_exec(filter);
    let global_limit = global_limit_exec(coalesce_partitions, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      CoalescePartitionsExec
        FilterExec: c3@2 > 0
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    CoalescePartitionsExec: fetch=5
      FilterExec: c3@2 > 0, fetch=5
        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
          StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    Ok(())
}

#[test]
fn merges_local_limit_with_local_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let child_local_limit = local_limit_exec(empty_exec, 10);
    let parent_local_limit = local_limit_exec(child_local_limit, 20);

    let initial = format_plan(&parent_local_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    LocalLimitExec: fetch=20
      LocalLimitExec: fetch=10
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(parent_local_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @"EmptyExec"
    );

    Ok(())
}

#[test]
fn merges_global_limit_with_global_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let child_global_limit = global_limit_exec(empty_exec, 10, Some(30));
    let parent_global_limit = global_limit_exec(child_global_limit, 10, Some(20));

    let initial = format_plan(&parent_global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=10, fetch=20
      GlobalLimitExec: skip=10, fetch=30
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(parent_global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=20, fetch=20
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn merges_global_limit_with_local_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let local_limit = local_limit_exec(empty_exec, 40);
    let global_limit = global_limit_exec(local_limit, 20, Some(30));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=20, fetch=30
      LocalLimitExec: fetch=40
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=20, fetch=20
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn merges_local_limit_with_global_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let global_limit = global_limit_exec(empty_exec, 20, Some(30));
    let local_limit = local_limit_exec(global_limit, 20);

    let initial = format_plan(&local_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    LocalLimitExec: fetch=20
      GlobalLimitExec: skip=20, fetch=30
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(local_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=20, fetch=20
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn preserves_nested_global_limit() -> Result<()> {
    // If there are multiple limits in an execution plan, they all need to be
    // preserved in the optimized plan.
    //
    // Plan structure:
    // GlobalLimitExec: skip=1, fetch=1
    //   NestedLoopJoinExec (Left)
    //     EmptyExec (left side)
    //     GlobalLimitExec: skip=2, fetch=1
    //       NestedLoopJoinExec (Right)
    //         EmptyExec (left side)
    //         EmptyExec (right side)
    let schema = create_schema();

    // Build inner join: NestedLoopJoin(Empty, Empty)
    let inner_left = empty_exec(Arc::clone(&schema));
    let inner_right = empty_exec(Arc::clone(&schema));
    let inner_join = nested_loop_join_exec(inner_left, inner_right, JoinType::Right)?;

    // Add inner limit: GlobalLimitExec: skip=2, fetch=1
    let inner_limit = global_limit_exec(inner_join, 2, Some(1));

    // Build outer join: NestedLoopJoin(Empty, GlobalLimit)
    let outer_left = empty_exec(Arc::clone(&schema));
    let outer_join = nested_loop_join_exec(outer_left, inner_limit, JoinType::Left)?;

    // Add outer limit: GlobalLimitExec: skip=1, fetch=1
    let outer_limit = global_limit_exec(outer_join, 1, Some(1));

    let initial = format_plan(&outer_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=1, fetch=1
      NestedLoopJoinExec: join_type=Left
        EmptyExec
        GlobalLimitExec: skip=2, fetch=1
          NestedLoopJoinExec: join_type=Right
            EmptyExec
            EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(outer_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=1, fetch=1
      NestedLoopJoinExec: join_type=Left
        EmptyExec
        GlobalLimitExec: skip=2, fetch=1
          NestedLoopJoinExec: join_type=Right
            EmptyExec
            EmptyExec
    "
    );

    Ok(())
}

#[test]
fn preserves_skip_before_sort() -> Result<()> {
    // If there's a limit with skip before a node that (1) supports fetch but
    // (2) does not support limit pushdown, that limit should not be removed.
    //
    // Plan structure:
    // GlobalLimitExec: skip=1, fetch=None
    //   SortExec: TopK(fetch=4)
    //     EmptyExec
    let schema = create_schema();

    let empty = empty_exec(Arc::clone(&schema));

    let ordering = [PhysicalSortExpr {
        expr: col("c1", &schema)?,
        options: SortOptions::default(),
    }];
    let sort = sort_exec(ordering.into(), empty)
        .with_fetch(Some(4))
        .unwrap();

    let outer_limit = global_limit_exec(sort, 1, None);

    let initial = format_plan(&outer_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=1, fetch=None
      SortExec: TopK(fetch=4), expr=[c1@0 ASC], preserve_partitioning=[false]
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(outer_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=1, fetch=3
      SortExec: TopK(fetch=4), expr=[c1@0 ASC], preserve_partitioning=[false]
        EmptyExec
    "
    );

    Ok(())
}

#[test]
fn no_limit_preserves_plan_identity() -> Result<()> {
    // When there is no limit in the plan, the optimizer should return the
    // exact same Arc (pointer-equal) for every node, avoiding unnecessary
    // plan reconstruction and property recomputation.
    let schema = create_schema();

    let left = empty_exec(Arc::clone(&schema));
    let right = empty_exec(Arc::clone(&schema));
    let on = join_on_columns("c1", "c1");
    let join = hash_join_exec(left, right, on, None, &JoinType::Inner)?;
    let plan = filter_exec(Arc::clone(&schema), join)?;

    let optimized =
        LimitPushdown::new().optimize(Arc::clone(&plan), &ConfigOptions::new())?;

    assert!(
        Arc::ptr_eq(&plan, &optimized),
        "Expected optimizer to return the same Arc when no limit is present"
    );

    let optimized = format_plan(&optimized);
    insta::assert_snapshot!(
        optimized,
        @r"
    FilterExec: c3@2 > 0
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)]
        EmptyExec
        EmptyExec
    "
    );

    Ok(())
}

#[test]
fn outer_offset_does_not_leak_through_sort_into_inner_limit() -> Result<()> {
    // Regression test for https://github.com/apache/datafusion/issues/22489
    //
    // When an outer OFFSET is separated from an inner LIMIT by a SortExec
    // with different sort keys, the outer skip must not reduce the inner
    // fetch. Before the fix, combine_limit merged them, producing
    // GlobalLimitExec(skip=1, fetch=7) instead of preserving the inner
    // LIMIT 8.
    //
    // Plan structure:
    // GlobalLimitExec: skip=1, fetch=None        (outer OFFSET 1)
    //   SortExec: [c1 DESC]                      (outer sort — different key)
    //     GlobalLimitExec: skip=0, fetch=8        (inner LIMIT 8)
    //       SortExec: [c2 ASC]                    (inner sort — different key)
    //         EmptyExec
    let schema = create_schema();
    let empty = empty_exec(Arc::clone(&schema));

    let inner_ordering: LexOrdering = [PhysicalSortExpr {
        expr: col("c2", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let inner_sort = sort_exec(inner_ordering, empty);
    let inner_limit = global_limit_exec(inner_sort, 0, Some(8));

    let outer_ordering: LexOrdering = [PhysicalSortExpr {
        expr: col("c1", &schema)?,
        options: SortOptions {
            descending: true,
            nulls_first: false,
        },
    }]
    .into();
    let outer_sort = sort_exec(outer_ordering, inner_limit);
    let outer_limit = global_limit_exec(outer_sort, 1, None);

    let initial = format_plan(&outer_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=1, fetch=None
      SortExec: expr=[c1@0 DESC NULLS LAST], preserve_partitioning=[false]
        GlobalLimitExec: skip=0, fetch=8
          SortExec: expr=[c2@1 ASC], preserve_partitioning=[false]
            EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(outer_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=1, fetch=None
      SortExec: expr=[c1@0 DESC NULLS LAST], preserve_partitioning=[false]
        SortExec: TopK(fetch=8), expr=[c2@1 ASC], preserve_partitioning=[false]
          EmptyExec
    "
    );

    Ok(())
}

#[test]
fn outer_offset_with_same_sort_key_still_pushes_limit() -> Result<()> {
    // Companion to outer_offset_does_not_leak_through_sort_into_inner_limit:
    // when both sorts use the *same* key, the inner LIMIT should still be
    // pushed into the SortExec as TopK.
    //
    // Plan structure:
    // GlobalLimitExec: skip=1, fetch=None        (outer OFFSET 1)
    //   SortExec: [c1 ASC]                       (outer sort — same key)
    //     GlobalLimitExec: skip=0, fetch=8        (inner LIMIT 8)
    //       SortExec: [c1 ASC]                    (inner sort — same key)
    //         EmptyExec
    let schema = create_schema();
    let empty = empty_exec(Arc::clone(&schema));

    let ordering: LexOrdering = [PhysicalSortExpr {
        expr: col("c1", &schema)?,
        options: SortOptions::default(),
    }]
    .into();

    let inner_sort = sort_exec(ordering.clone(), empty);
    let inner_limit = global_limit_exec(inner_sort, 0, Some(8));
    let outer_sort = sort_exec(ordering, inner_limit);
    let outer_limit = global_limit_exec(outer_sort, 1, None);

    let initial = format_plan(&outer_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=1, fetch=None
      SortExec: expr=[c1@0 ASC], preserve_partitioning=[false]
        GlobalLimitExec: skip=0, fetch=8
          SortExec: expr=[c1@0 ASC], preserve_partitioning=[false]
            EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(outer_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=1, fetch=None
      SortExec: expr=[c1@0 ASC], preserve_partitioning=[false]
        SortExec: TopK(fetch=8), expr=[c1@0 ASC], preserve_partitioning=[false]
          EmptyExec
    "
    );

    Ok(())
}

#[test]
fn keeps_global_limit_when_existing_fetch_is_looser_than_owed() -> Result<()> {
    // This operator's `fetch=10` is weaker than `LIMIT 5` and cannot be lowered.
    // Keep `GlobalLimitExec` so the query still returns at most five rows.
    let schema = create_schema();
    let scan = Arc::new(TestScan::new(schema, vec![]));
    let fetch_only = Arc::new(TestFetchOnlyExec::new(scan, Some(10)));
    let global_limit = global_limit_exec(fetch_only, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    GlobalLimitExec: skip=0, fetch=5
      TestFetchOnlyExec: fetch=10
        TestScan
    "
    );

    Ok(())
}

#[test]
fn pushes_owed_limit_below_fetch_only_unary_when_limit_pushdown_supported() -> Result<()>
{
    // This operator allows limit pushdown but cannot lower its own `fetch` from
    // 10 to 5. Its child cannot accept a fetch either, so keep
    // `GlobalLimitExec(fetch=5)` between them.
    let schema = create_schema();
    let scan = Arc::new(TestScan::new(schema, vec![]));
    let fetch_only = Arc::new(
        TestFetchOnlyExec::new(scan, Some(10)).with_supports_limit_pushdown(true),
    );
    let global_limit = global_limit_exec(fetch_only, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    TestFetchOnlyExec: fetch=10
      GlobalLimitExec: skip=0, fetch=5
        TestScan
    "
    );

    Ok(())
}

#[test]
fn does_not_add_redundant_wrapper_when_existing_fetch_is_tighter_than_owed() -> Result<()>
{
    // `fetch=3` is stricter than `LIMIT 5`, so no additional limit is needed.
    let schema = create_schema();
    let scan = Arc::new(TestScan::new(schema, vec![]));
    let fetch_only = Arc::new(TestFetchOnlyExec::new(scan, Some(3)));
    let global_limit = global_limit_exec(fetch_only, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    TestFetchOnlyExec: fetch=3
      TestScan
    "
    );

    Ok(())
}

#[test]
fn tightens_existing_sort_fetch_to_owed_limit() -> Result<()> {
    // `SortExec` can lower its `fetch` from 10 to 5, so the separate
    // `GlobalLimitExec` is unnecessary.
    let schema = create_schema();
    let scan = Arc::new(TestScan::new(schema.clone(), vec![]));
    let ordering: LexOrdering = [PhysicalSortExpr {
        expr: col("c1", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let sort = sort_exec(ordering, scan).with_fetch(Some(10)).unwrap();
    let global_limit = global_limit_exec(sort, 0, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    SortExec: TopK(fetch=5), expr=[c1@0 ASC], preserve_partitioning=[false]
      TestScan
    "
    );

    Ok(())
}

#[test]
fn keeps_global_offset_limit_when_existing_fetch_is_looser() -> Result<()> {
    // An operator `fetch` cannot apply `OFFSET 2`; keep `GlobalLimitExec` to
    // enforce both the offset and `LIMIT 5`.
    let schema = create_schema();
    let scan = Arc::new(TestScan::new(schema, vec![]));
    let fetch_only = Arc::new(TestFetchOnlyExec::new(scan, Some(10)));
    let global_limit = global_limit_exec(fetch_only, 2, Some(5));

    let optimized = LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    insta::assert_snapshot!(
        format_plan(&optimized),
        @r"
    GlobalLimitExec: skip=2, fetch=5
      TestFetchOnlyExec: fetch=10
        TestScan
    "
    );

    Ok(())
}
