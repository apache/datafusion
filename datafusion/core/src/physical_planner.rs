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

//! Planner for [`LogicalPlan`] to [`ExecutionPlan`]

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use crate::datasource::file_format::file_type_to_format;
use crate::datasource::listing::ListingTableUrl;
use crate::datasource::physical_plan::FileSinkConfig;
use crate::datasource::{source_as_provider, DefaultTableSource};
use crate::error::{DataFusionError, Result};
use crate::execution::context::{ExecutionProps, SessionState};
use crate::logical_expr::utils::generate_sort_key;
use crate::logical_expr::{
    Aggregate, EmptyRelation, Join, Projection, Sort, TableScan, Unnest, Values, Window,
};
use crate::logical_expr::{
    Expr, LogicalPlan, Partitioning as LogicalPartitioning, PlanType, Repartition,
    UserDefinedLogicalNode,
};
use crate::physical_expr::{create_physical_expr, create_physical_exprs};
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::analyze::AnalyzeExec;
use crate::physical_plan::explain::ExplainExec;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils as join_utils;
use crate::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
};
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::unnest::UnnestExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{
    displayable, windows, ExecutionPlan, ExecutionPlanProperties, InputOrderMode,
    Partitioning, PhysicalExpr, WindowExpr,
};
use crate::schema_equivalence::schema_satisfied_by;

use arrow::array::{builder::StringBuilder, RecordBatch};
use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use arrow_schema::Field;
use datafusion_catalog::ScanArgs;
use datafusion_common::display::ToStringifiedPlan;
use datafusion_common::format::ExplainAnalyzeLevel;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{
    assert_eq_or_internal_err, assert_or_internal_err, TableReference,
};
use datafusion_common::{
    exec_err, internal_datafusion_err, internal_err, not_impl_err, plan_err, DFSchema,
    ScalarValue,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_expr::dml::{CopyTo, InsertOp};
use datafusion_expr::expr::{
    physical_name, AggregateFunction, AggregateFunctionParams, Alias, GroupingSet,
    NullTreatment, WindowFunction, WindowFunctionParams,
};
use datafusion_expr::expr_rewriter::unnormalize_cols;
use datafusion_expr::logical_plan::builder::wrap_projection_for_join_if_necessary;
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{
    Analyze, BinaryExpr, DescribeTable, DmlStatement, Explain, ExplainFormat, Extension,
    FetchType, Filter, JoinType, Operator, RecursiveQuery, SkipType, StringifiedPlan,
    WindowFrame, WindowFrameBound, WriteOp,
};
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr::{
    create_physical_sort_exprs, LexOrdering, PhysicalSortExpr,
};
use datafusion_physical_optimizer::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::execution_plan::InvariantLevel;
use datafusion_physical_plan::joins::PiecewiseMergeJoinExec;
use datafusion_physical_plan::metrics::MetricType;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::recursive_query::RecursiveQueryExec;
use datafusion_physical_plan::unnest::ListUnnest;

use async_trait::async_trait;
use datafusion_physical_plan::async_func::{AsyncFuncExec, AsyncMapper};
use futures::{StreamExt, TryStreamExt};
use itertools::{multiunzip, Itertools};
use log::debug;
use tokio::sync::Mutex;

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
#[async_trait]
pub trait PhysicalPlanner: Send + Sync {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `expr`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `expr`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// This trait exposes the ability to plan an [`ExecutionPlan`] out of a [`LogicalPlan`].
#[async_trait]
pub trait ExtensionPlanner {
    /// Create a physical plan for a [`UserDefinedLogicalNode`].
    ///
    /// `input_dfschema`: the logical plan schema for the inputs to this node
    ///
    /// Returns an error when the planner knows how to plan the concrete
    /// implementation of `node` but errors while doing so.
    ///
    /// Returns `None` when the planner does not know how to plan the
    /// `node` and wants to delegate the planning to another
    /// [`ExtensionPlanner`].
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
}

/// Default single node physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
///
/// This planner will first flatten the `LogicalPlan` tree via a
/// depth first approach, which allows it to identify the leaves
/// of the tree.
///
/// Tasks are spawned from these leaves and traverse back up the
/// tree towards the root, converting each `LogicalPlan` node it
/// reaches into their equivalent `ExecutionPlan` node. When these
/// tasks reach a common node, they will terminate until the last
/// task reaches the node which will then continue building up the
/// tree.
///
/// Up to [`planning_concurrency`] tasks are buffered at once to
/// execute concurrently.
///
/// [`planning_concurrency`]: crate::config::ExecutionOptions::planning_concurrency
#[derive(Default)]
pub struct DefaultPhysicalPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

#[async_trait]
impl PhysicalPlanner for DefaultPhysicalPlanner {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(plan) = self
            .handle_explain_or_analyze(logical_plan, session_state)
            .await?
        {
            return Ok(plan);
        }
        let plan = self
            .create_initial_plan(logical_plan, session_state)
            .await?;

        self.optimize_physical_plan(plan, session_state, |_, _| {})
    }

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `e`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `e`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        create_physical_expr(expr, input_dfschema, session_state.execution_props())
    }
}

#[derive(Debug)]
struct ExecutionPlanChild {
    /// Index needed to order children of parent to ensure consistency with original
    /// `LogicalPlan`
    index: usize,
    plan: Arc<dyn ExecutionPlan>,
}

#[derive(Debug)]
enum NodeState {
    ZeroOrOneChild,
    /// Nodes with multiple children will have multiple tasks accessing it,
    /// and each task will append their contribution until the last task takes
    /// all the children to build the parent node.
    TwoOrMoreChildren(Mutex<Vec<ExecutionPlanChild>>),
}

/// To avoid needing to pass single child wrapped in a Vec for nodes
/// with only one child.
enum ChildrenContainer {
    None,
    One(Arc<dyn ExecutionPlan>),
    Multiple(Vec<Arc<dyn ExecutionPlan>>),
}

impl ChildrenContainer {
    fn one(self) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            Self::One(p) => Ok(p),
            _ => internal_err!("More than one child in ChildrenContainer"),
        }
    }

    fn two(self) -> Result<[Arc<dyn ExecutionPlan>; 2]> {
        match self {
            Self::Multiple(v) if v.len() == 2 => Ok(v.try_into().unwrap()),
            _ => internal_err!("ChildrenContainer doesn't contain exactly 2 children"),
        }
    }

    fn vec(self) -> Vec<Arc<dyn ExecutionPlan>> {
        match self {
            Self::None => vec![],
            Self::One(p) => vec![p],
            Self::Multiple(v) => v,
        }
    }
}

#[derive(Debug)]
struct LogicalNode<'a> {
    node: &'a LogicalPlan,
    // None if root
    parent_index: Option<usize>,
    state: NodeState,
}

impl DefaultPhysicalPlanner {
    /// Create a physical planner that uses `extension_planners` to
    /// plan user-defined logical nodes [`LogicalPlan::Extension`].
    /// The planner uses the first [`ExtensionPlanner`] to return a non-`None`
    /// plan.
    pub fn with_extension_planners(
        extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        Self { extension_planners }
    }

    /// Create a physical plan from a logical plan
    async fn create_initial_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // DFS the tree to flatten it into a Vec.
        // This will allow us to build the Physical Plan from the leaves up
        // to avoid recursion, and also to make it easier to build a valid
        // Physical Plan from the start and not rely on some intermediate
        // representation (since parents need to know their children at
        // construction time).
        let mut flat_tree = vec![];
        let mut dfs_visit_stack = vec![(None, logical_plan)];
        // Use this to be able to find the leaves to start construction bottom
        // up concurrently.
        let mut flat_tree_leaf_indices = vec![];
        while let Some((parent_index, node)) = dfs_visit_stack.pop() {
            let current_index = flat_tree.len();
            // Because of how we extend the visit stack here, we visit the children
            // in reverse order of how they appear, so later we need to reverse
            // the order of children when building the nodes.
            dfs_visit_stack
                .extend(node.inputs().iter().map(|&n| (Some(current_index), n)));
            let state = match node.inputs().len() {
                0 => {
                    flat_tree_leaf_indices.push(current_index);
                    NodeState::ZeroOrOneChild
                }
                1 => NodeState::ZeroOrOneChild,
                _ => {
                    let ready_children = Vec::with_capacity(node.inputs().len());
                    let ready_children = Mutex::new(ready_children);
                    NodeState::TwoOrMoreChildren(ready_children)
                }
            };
            let node = LogicalNode {
                node,
                parent_index,
                state,
            };
            flat_tree.push(node);
        }
        let flat_tree = Arc::new(flat_tree);

        let planning_concurrency = session_state
            .config_options()
            .execution
            .planning_concurrency;
        // Can never spawn more tasks than leaves in the tree, as these tasks must
        // all converge down to the root node, which can only be processed by a
        // single task.
        let max_concurrency = planning_concurrency.min(flat_tree_leaf_indices.len());

        // Spawning tasks which will traverse leaf up to the root.
        let tasks = flat_tree_leaf_indices
            .into_iter()
            .map(|index| self.task_helper(index, Arc::clone(&flat_tree), session_state));
        let mut outputs = futures::stream::iter(tasks)
            .buffer_unordered(max_concurrency)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        // Ideally this never happens if we have a valid LogicalPlan tree
        assert_eq_or_internal_err!(
            outputs.len(),
            1,
            "Failed to convert LogicalPlan to ExecutionPlan: More than one root detected"
        );
        let plan = outputs.pop().unwrap();
        Ok(plan)
    }

    /// These tasks start at a leaf and traverse up the tree towards the root, building
    /// an ExecutionPlan as they go. When they reach a node with two or more children,
    /// they append their current result (a child of the parent node) to the children
    /// vector, and if this is sufficient to create the parent then continues traversing
    /// the tree to create nodes. Otherwise, the task terminates.
    async fn task_helper<'a>(
        &'a self,
        leaf_starter_index: usize,
        flat_tree: Arc<Vec<LogicalNode<'a>>>,
        session_state: &'a SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // We always start with a leaf, so can ignore status and pass empty children
        let mut node = flat_tree.get(leaf_starter_index).ok_or_else(|| {
            internal_datafusion_err!(
                "Invalid index whilst creating initial physical plan"
            )
        })?;
        let mut plan = self
            .map_logical_node_to_physical(
                node.node,
                session_state,
                ChildrenContainer::None,
            )
            .await?;
        let mut current_index = leaf_starter_index;
        // parent_index is None only for root
        while let Some(parent_index) = node.parent_index {
            node = flat_tree.get(parent_index).ok_or_else(|| {
                internal_datafusion_err!(
                    "Invalid index whilst creating initial physical plan"
                )
            })?;
            match &node.state {
                NodeState::ZeroOrOneChild => {
                    plan = self
                        .map_logical_node_to_physical(
                            node.node,
                            session_state,
                            ChildrenContainer::One(plan),
                        )
                        .await?;
                }
                // See if we have all children to build the node.
                NodeState::TwoOrMoreChildren(children) => {
                    let mut children: Vec<ExecutionPlanChild> = {
                        let mut guard = children.lock().await;
                        // Add our contribution to this parent node.
                        // Vec is pre-allocated so no allocation should occur here.
                        guard.push(ExecutionPlanChild {
                            index: current_index,
                            plan,
                        });
                        if guard.len() < node.node.inputs().len() {
                            // This node is not ready yet, still pending more children.
                            // This task is finished forever.
                            return Ok(None);
                        }

                        // With this task's contribution we have enough children.
                        // This task is the only one building this node now, and thus
                        // no other task will need the Mutex for this node, so take
                        // all children.
                        std::mem::take(guard.as_mut())
                    };

                    // Indices refer to position in flat tree Vec, which means they are
                    // guaranteed to be unique, hence unstable sort used.
                    //
                    // We reverse sort because of how we visited the node in the initial
                    // DFS traversal (see above).
                    children.sort_unstable_by_key(|epc| std::cmp::Reverse(epc.index));
                    let children = children.into_iter().map(|epc| epc.plan).collect();
                    let children = ChildrenContainer::Multiple(children);
                    plan = self
                        .map_logical_node_to_physical(node.node, session_state, children)
                        .await?;
                }
            }
            current_index = parent_index;
        }
        // Only one task should ever reach this point for a valid LogicalPlan tree.
        Ok(Some(plan))
    }

    /// Given a single LogicalPlan node, map it to its physical ExecutionPlan counterpart.
    async fn map_logical_node_to_physical(
        &self,
        node: &LogicalPlan,
        session_state: &SessionState,
        children: ChildrenContainer,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec_node: Arc<dyn ExecutionPlan> = match node {
            // Leaves (no children)
            LogicalPlan::TableScan(TableScan {
                source,
                projection,
                filters,
                fetch,
                ..
            }) => {
                let source = source_as_provider(source)?;
                // Remove all qualifiers from the scan as the provider
                // doesn't know (nor should care) how the relation was
                // referred to in the query
                let filters = unnormalize_cols(filters.iter().cloned());
                let filters_vec = filters.into_iter().collect::<Vec<_>>();
                let opts = ScanArgs::default()
                    .with_projection(projection.as_deref())
                    .with_filters(Some(&filters_vec))
                    .with_limit(*fetch);
                let res = source.scan_with_args(session_state, opts).await?;
                Arc::clone(res.plan())
            }
            LogicalPlan::Values(Values { values, schema }) => {
                let exprs = values
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| {
                                self.create_physical_expr(expr, schema, session_state)
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                MemorySourceConfig::try_new_as_values(Arc::clone(schema.inner()), exprs)?
                    as _
            }
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            }) => Arc::new(EmptyExec::new(Arc::clone(schema.inner()))),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema,
            }) => Arc::new(PlaceholderRowExec::new(Arc::clone(schema.inner()))),
            LogicalPlan::DescribeTable(DescribeTable {
                schema,
                output_schema,
            }) => {
                let output_schema = Arc::clone(output_schema.inner());
                self.plan_describe(&Arc::clone(schema), output_schema)?
            }

            // 1 Child
            LogicalPlan::Copy(CopyTo {
                input,
                output_url,
                file_type,
                partition_by,
                options: source_option_tuples,
                output_schema: _,
            }) => {
                let original_url = output_url.clone();
                let input_exec = children.one()?;
                let parsed_url = ListingTableUrl::parse(output_url)?;
                let object_store_url = parsed_url.object_store();

                let schema = Arc::clone(input.schema().inner());

                // Note: the DataType passed here is ignored for the purposes of writing and inferred instead
                // from the schema of the RecordBatch being written. This allows COPY statements to specify only
                // the column name rather than column name + explicit data type.
                let table_partition_cols = partition_by
                    .iter()
                    .map(|s| (s.to_string(), arrow::datatypes::DataType::Null))
                    .collect::<Vec<_>>();

                let keep_partition_by_columns = match source_option_tuples
                    .get("execution.keep_partition_by_columns")
                    .map(|v| v.trim()) {
                    None => session_state.config().options().execution.keep_partition_by_columns,
                    Some("true") => true,
                    Some("false") => false,
                    Some(value) =>
                        return Err(DataFusionError::Configuration(format!("provided value for 'execution.keep_partition_by_columns' was not recognized: \"{value}\""))),
                };

                let sink_format = file_type_to_format(file_type)?
                    .create(session_state, source_option_tuples)?;

                // Determine extension based on format extension and compression
                let file_extension = match sink_format.compression_type() {
                    Some(compression_type) => sink_format
                        .get_ext_with_compression(&compression_type)
                        .unwrap_or_else(|_| sink_format.get_ext()),
                    None => sink_format.get_ext(),
                };

                // Set file sink related options
                let config = FileSinkConfig {
                    original_url,
                    object_store_url,
                    table_paths: vec![parsed_url],
                    file_group: FileGroup::default(),
                    output_schema: schema,
                    table_partition_cols,
                    insert_op: InsertOp::Append,
                    keep_partition_by_columns,
                    file_extension,
                };

                let ordering = input_exec.properties().output_ordering().cloned();

                sink_format
                    .create_writer_physical_plan(
                        input_exec,
                        session_state,
                        config,
                        ordering.map(Into::into),
                    )
                    .await?
            }
            LogicalPlan::Dml(DmlStatement {
                target,
                op: WriteOp::Insert(insert_op),
                ..
            }) => {
                if let Some(provider) =
                    target.as_any().downcast_ref::<DefaultTableSource>()
                {
                    let input_exec = children.one()?;
                    provider
                        .table_provider
                        .insert_into(session_state, input_exec, *insert_op)
                        .await?
                } else {
                    return exec_err!(
                        "Table source can't be downcasted to DefaultTableSource"
                    );
                }
            }
            LogicalPlan::Window(Window { window_expr, .. }) => {
                assert_or_internal_err!(
                    !window_expr.is_empty(),
                    "Impossibly got empty window expression"
                );

                let input_exec = children.one()?;

                let get_sort_keys = |expr: &Expr| match expr {
                    Expr::WindowFunction(window_fun) => {
                        let WindowFunctionParams {
                            ref partition_by,
                            ref order_by,
                            ..
                        } = &window_fun.as_ref().params;
                        generate_sort_key(partition_by, order_by)
                    }
                    Expr::Alias(Alias { expr, .. }) => {
                        // Convert &Box<T> to &T
                        match &**expr {
                            Expr::WindowFunction(window_fun) => {
                                let WindowFunctionParams {
                                    ref partition_by,
                                    ref order_by,
                                    ..
                                } = &window_fun.as_ref().params;
                                generate_sort_key(partition_by, order_by)
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                };
                let sort_keys = get_sort_keys(&window_expr[0])?;
                if window_expr.len() > 1 {
                    debug_assert!(
                            window_expr[1..]
                                .iter()
                                .all(|expr| get_sort_keys(expr).unwrap() == sort_keys),
                            "all window expressions shall have the same sort keys, as guaranteed by logical planning"
                        );
                }

                let logical_schema = node.schema();
                let window_expr = window_expr
                    .iter()
                    .map(|e| {
                        create_window_expr(
                            e,
                            logical_schema,
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let can_repartition = session_state.config().target_partitions() > 1
                    && session_state.config().repartition_window_functions();

                let uses_bounded_memory =
                    window_expr.iter().all(|e| e.uses_bounded_memory());
                // If all window expressions can run with bounded memory,
                // choose the bounded window variant:
                if uses_bounded_memory {
                    Arc::new(BoundedWindowAggExec::try_new(
                        window_expr,
                        input_exec,
                        InputOrderMode::Sorted,
                        can_repartition,
                    )?)
                } else {
                    Arc::new(WindowAggExec::try_new(
                        window_expr,
                        input_exec,
                        can_repartition,
                    )?)
                }
            }
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            }) => {
                let options = session_state.config().options();
                // Initially need to perform the aggregate and then merge the partitions
                let input_exec = children.one()?;
                let physical_input_schema = input_exec.schema();
                let logical_input_schema = input.as_ref().schema();
                let physical_input_schema_from_logical = logical_input_schema.inner();

                if !options.execution.skip_physical_aggregate_schema_check
                    && !schema_satisfied_by(
                        physical_input_schema_from_logical,
                        &physical_input_schema,
                    )
                {
                    let mut differences = Vec::new();
                    if physical_input_schema.fields().len()
                        != physical_input_schema_from_logical.fields().len()
                    {
                        differences.push(format!(
                            "Different number of fields: (physical) {} vs (logical) {}",
                            physical_input_schema.fields().len(),
                            physical_input_schema_from_logical.fields().len()
                        ));
                    }
                    for (i, (physical_field, logical_field)) in physical_input_schema
                        .fields()
                        .iter()
                        .zip(physical_input_schema_from_logical.fields())
                        .enumerate()
                    {
                        if physical_field.name() != logical_field.name() {
                            differences.push(format!(
                                "field name at index {}: (physical) {} vs (logical) {}",
                                i,
                                physical_field.name(),
                                logical_field.name()
                            ));
                        }
                        if physical_field.data_type() != logical_field.data_type() {
                            differences.push(format!("field data type at index {} [{}]: (physical) {} vs (logical) {}", i, physical_field.name(), physical_field.data_type(), logical_field.data_type()));
                        }
                        if physical_field.is_nullable() && !logical_field.is_nullable() {
                            differences.push(format!("field nullability at index {} [{}]: (physical) {} vs (logical) {}", i, physical_field.name(), physical_field.is_nullable(), logical_field.is_nullable()));
                        }
                    }
                    return internal_err!("Physical input schema should be the same as the one converted from logical input schema. Differences: {}", differences
                        .iter()
                        .map(|s| format!("\n\t- {s}"))
                        .join(""));
                }

                let groups = self.create_grouping_physical_expr(
                    group_expr,
                    logical_input_schema,
                    &physical_input_schema,
                    session_state,
                )?;

                let agg_filter = aggr_expr
                    .iter()
                    .map(|e| {
                        create_aggregate_expr_and_maybe_filter(
                            e,
                            logical_input_schema,
                            &physical_input_schema,
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let (mut aggregates, filters, _order_bys): (Vec<_>, Vec<_>, Vec<_>) =
                    multiunzip(agg_filter);

                let mut async_exprs = Vec::new();
                let num_input_columns = physical_input_schema.fields().len();

                for agg_func in &mut aggregates {
                    match self.try_plan_async_exprs(
                        num_input_columns,
                        PlannedExprResult::Expr(agg_func.expressions()),
                        physical_input_schema.as_ref(),
                    )? {
                        PlanAsyncExpr::Async(
                            async_map,
                            PlannedExprResult::Expr(physical_exprs),
                        ) => {
                            async_exprs.extend(async_map.async_exprs);

                            if let Some(new_agg_func) = agg_func.with_new_expressions(
                                physical_exprs,
                                agg_func
                                    .order_bys()
                                    .iter()
                                    .cloned()
                                    .map(|x| x.expr)
                                    .collect(),
                            ) {
                                *agg_func = Arc::new(new_agg_func);
                            } else {
                                return internal_err!("Failed to plan async expression");
                            }
                        }
                        PlanAsyncExpr::Sync(PlannedExprResult::Expr(_)) => {
                            // Do nothing
                        }
                        _ => {
                            return internal_err!(
                                "Unexpected result from try_plan_async_exprs"
                            )
                        }
                    }
                }
                let input_exec = if !async_exprs.is_empty() {
                    Arc::new(AsyncFuncExec::try_new(async_exprs, input_exec)?)
                } else {
                    input_exec
                };

                let initial_aggr = Arc::new(AggregateExec::try_new(
                    AggregateMode::Partial,
                    groups.clone(),
                    aggregates,
                    filters.clone(),
                    input_exec,
                    Arc::clone(&physical_input_schema),
                )?);

                let can_repartition = !groups.is_empty()
                    && session_state.config().target_partitions() > 1
                    && session_state.config().repartition_aggregations();

                // Some aggregators may be modified during initialization for
                // optimization purposes. For example, a FIRST_VALUE may turn
                // into a LAST_VALUE with the reverse ordering requirement.
                // To reflect such changes to subsequent stages, use the updated
                // `AggregateFunctionExpr`/`PhysicalSortExpr` objects.
                let updated_aggregates = initial_aggr.aggr_expr().to_vec();

                let next_partition_mode = if can_repartition {
                    // construct a second aggregation with 'AggregateMode::FinalPartitioned'
                    AggregateMode::FinalPartitioned
                } else {
                    // construct a second aggregation, keeping the final column name equal to the
                    // first aggregation and the expressions corresponding to the respective aggregate
                    AggregateMode::Final
                };

                let final_grouping_set = initial_aggr.group_expr().as_final();

                Arc::new(AggregateExec::try_new(
                    next_partition_mode,
                    final_grouping_set,
                    updated_aggregates,
                    filters,
                    initial_aggr,
                    Arc::clone(&physical_input_schema),
                )?)
            }
            LogicalPlan::Projection(Projection { input, expr, .. }) => self
                .create_project_physical_exec(
                    session_state,
                    children.one()?,
                    input,
                    expr,
                )?,
            LogicalPlan::Filter(Filter {
                predicate, input, ..
            }) => {
                let physical_input = children.one()?;
                let input_dfschema = input.schema();

                let runtime_expr =
                    self.create_physical_expr(predicate, input_dfschema, session_state)?;

                let input_schema = input.schema();
                let filter = match self.try_plan_async_exprs(
                    input_schema.fields().len(),
                    PlannedExprResult::Expr(vec![runtime_expr]),
                    input_schema.as_arrow(),
                )? {
                    PlanAsyncExpr::Sync(PlannedExprResult::Expr(runtime_expr)) => {
                        FilterExec::try_new(Arc::clone(&runtime_expr[0]), physical_input)?
                            .with_batch_size(session_state.config().batch_size())?
                    }
                    PlanAsyncExpr::Async(
                        async_map,
                        PlannedExprResult::Expr(runtime_expr),
                    ) => {
                        let async_exec = AsyncFuncExec::try_new(
                            async_map.async_exprs,
                            physical_input,
                        )?;
                        FilterExec::try_new(
                            Arc::clone(&runtime_expr[0]),
                            Arc::new(async_exec),
                        )?
                        // project the output columns excluding the async functions
                        // The async functions are always appended to the end of the schema.
                        .with_projection(Some(
                            (0..input.schema().fields().len()).collect(),
                        ))?
                        .with_batch_size(session_state.config().batch_size())?
                    }
                    _ => {
                        return internal_err!(
                            "Unexpected result from try_plan_async_exprs"
                        )
                    }
                };

                let selectivity = session_state
                    .config()
                    .options()
                    .optimizer
                    .default_filter_selectivity;
                Arc::new(filter.with_default_selectivity(selectivity)?)
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => {
                let physical_input = children.one()?;
                let input_dfschema = input.as_ref().schema();
                let physical_partitioning = match partitioning_scheme {
                    LogicalPartitioning::RoundRobinBatch(n) => {
                        Partitioning::RoundRobinBatch(*n)
                    }
                    LogicalPartitioning::Hash(expr, n) => {
                        let runtime_expr = expr
                            .iter()
                            .map(|e| {
                                self.create_physical_expr(
                                    e,
                                    input_dfschema,
                                    session_state,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Partitioning::Hash(runtime_expr, *n)
                    }
                    LogicalPartitioning::DistributeBy(_) => {
                        return not_impl_err!(
                            "Physical plan does not support DistributeBy partitioning"
                        );
                    }
                };
                Arc::new(RepartitionExec::try_new(
                    physical_input,
                    physical_partitioning,
                )?)
            }
            LogicalPlan::Sort(Sort {
                expr, input, fetch, ..
            }) => {
                let physical_input = children.one()?;
                let input_dfschema = input.as_ref().schema();
                let sort_exprs = create_physical_sort_exprs(
                    expr,
                    input_dfschema,
                    session_state.execution_props(),
                )?;
                let Some(ordering) = LexOrdering::new(sort_exprs) else {
                    return internal_err!(
                        "SortExec requires at least one sort expression"
                    );
                };
                let new_sort = SortExec::new(ordering, physical_input).with_fetch(*fetch);
                Arc::new(new_sort)
            }
            LogicalPlan::Subquery(_) => todo!(),
            LogicalPlan::SubqueryAlias(_) => children.one()?,
            LogicalPlan::Limit(limit) => {
                let input = children.one()?;
                let SkipType::Literal(skip) = limit.get_skip_type()? else {
                    return not_impl_err!(
                        "Unsupported OFFSET expression: {:?}",
                        limit.skip
                    );
                };
                let FetchType::Literal(fetch) = limit.get_fetch_type()? else {
                    return not_impl_err!(
                        "Unsupported LIMIT expression: {:?}",
                        limit.fetch
                    );
                };

                // GlobalLimitExec requires a single partition for input
                let input = if input.output_partitioning().partition_count() == 1 {
                    input
                } else {
                    // Apply a LocalLimitExec to each partition. The optimizer will also insert
                    // a CoalescePartitionsExec between the GlobalLimitExec and LocalLimitExec
                    if let Some(fetch) = fetch {
                        Arc::new(LocalLimitExec::new(input, fetch + skip))
                    } else {
                        input
                    }
                };

                Arc::new(GlobalLimitExec::new(input, skip, fetch))
            }
            LogicalPlan::Unnest(Unnest {
                list_type_columns,
                struct_type_columns,
                schema,
                options,
                ..
            }) => {
                let input = children.one()?;
                let schema = Arc::clone(schema.inner());
                let list_column_indices = list_type_columns
                    .iter()
                    .map(|(index, unnesting)| ListUnnest {
                        index_in_input_schema: *index,
                        depth: unnesting.depth,
                    })
                    .collect();
                Arc::new(UnnestExec::new(
                    input,
                    list_column_indices,
                    struct_type_columns.clone(),
                    schema,
                    options.clone(),
                )?)
            }

            // 2 Children
            LogicalPlan::Join(Join {
                left: original_left,
                right: original_right,
                on: keys,
                filter,
                join_type,
                null_equality,
                schema: join_schema,
                ..
            }) => {
                let [physical_left, physical_right] = children.two()?;

                // If join has expression equijoin keys, add physical projection.
                let has_expr_join_key = keys.iter().any(|(l, r)| {
                    !(matches!(l, Expr::Column(_)) && matches!(r, Expr::Column(_)))
                });
                let (new_logical, physical_left, physical_right) = if has_expr_join_key {
                    // TODO: Can we extract this transformation to somewhere before physical plan
                    //       creation?
                    let (left_keys, right_keys): (Vec<_>, Vec<_>) =
                        keys.iter().cloned().unzip();

                    let (left, left_col_keys, left_projected) =
                        wrap_projection_for_join_if_necessary(
                            &left_keys,
                            original_left.as_ref().clone(),
                        )?;
                    let (right, right_col_keys, right_projected) =
                        wrap_projection_for_join_if_necessary(
                            &right_keys,
                            original_right.as_ref().clone(),
                        )?;
                    let column_on = (left_col_keys, right_col_keys);

                    let left = Arc::new(left);
                    let right = Arc::new(right);
                    let (new_join, requalified) = Join::try_new_with_project_input(
                        node,
                        Arc::clone(&left),
                        Arc::clone(&right),
                        column_on,
                    )?;

                    let new_join = LogicalPlan::Join(new_join);

                    // If inputs were projected then create ExecutionPlan for these new
                    // LogicalPlan nodes.
                    let physical_left = match (left_projected, left.as_ref()) {
                        // If left_projected is true we are guaranteed that left is a Projection
                        (
                            true,
                            LogicalPlan::Projection(Projection { input, expr, .. }),
                        ) => self.create_project_physical_exec(
                            session_state,
                            physical_left,
                            input,
                            expr,
                        )?,
                        _ => physical_left,
                    };
                    let physical_right = match (right_projected, right.as_ref()) {
                        // If right_projected is true we are guaranteed that right is a Projection
                        (
                            true,
                            LogicalPlan::Projection(Projection { input, expr, .. }),
                        ) => self.create_project_physical_exec(
                            session_state,
                            physical_right,
                            input,
                            expr,
                        )?,
                        _ => physical_right,
                    };

                    // Remove temporary projected columns
                    if left_projected || right_projected {
                        // Re-qualify the join schema only if the inputs were previously requalified in
                        // `try_new_with_project_input`. This ensures that when building the Projection
                        // it can correctly resolve field nullability and data types
                        // by disambiguating fields from the left and right sides of the join.
                        let qualified_join_schema = if requalified {
                            Arc::new(qualify_join_schema_sides(
                                join_schema,
                                original_left,
                                original_right,
                            )?)
                        } else {
                            Arc::clone(join_schema)
                        };

                        let final_join_result = qualified_join_schema
                            .iter()
                            .map(Expr::from)
                            .collect::<Vec<_>>();
                        let projection = LogicalPlan::Projection(Projection::try_new(
                            final_join_result,
                            Arc::new(new_join),
                        )?);
                        // LogicalPlan mutated
                        (Cow::Owned(projection), physical_left, physical_right)
                    } else {
                        // LogicalPlan mutated
                        (Cow::Owned(new_join), physical_left, physical_right)
                    }
                } else {
                    // LogicalPlan unchanged
                    (Cow::Borrowed(node), physical_left, physical_right)
                };

                // Retrieving new left/right and join keys (in case plan was mutated above)
                let (left, right, keys, new_project) = match new_logical.as_ref() {
                    LogicalPlan::Projection(Projection { input, expr, .. }) => {
                        if let LogicalPlan::Join(Join {
                            left, right, on, ..
                        }) = input.as_ref()
                        {
                            (left, right, on, Some((input, expr)))
                        } else {
                            unreachable!()
                        }
                    }
                    LogicalPlan::Join(Join {
                        left, right, on, ..
                    }) => (left, right, on, None),
                    // Should either be the original Join, or Join with a Projection on top
                    _ => unreachable!(),
                };

                // All equi-join keys are columns now, create physical join plan
                let left_df_schema = left.schema();
                let right_df_schema = right.schema();
                let execution_props = session_state.execution_props();
                let join_on = keys
                    .iter()
                    .map(|(l, r)| {
                        let l = create_physical_expr(l, left_df_schema, execution_props)?;
                        let r =
                            create_physical_expr(r, right_df_schema, execution_props)?;
                        Ok((l, r))
                    })
                    .collect::<Result<join_utils::JoinOn>>()?;

                // TODO: `num_range_filters` can be used later on for ASOF joins (`num_range_filters > 1`)
                let mut num_range_filters = 0;
                let mut range_filters: Vec<Expr> = Vec::new();
                let mut total_filters = 0;

                let join_filter = match filter {
                    Some(expr) => {
                        let split_expr = split_conjunction(expr);
                        for expr in split_expr.iter() {
                            match *expr {
                                Expr::BinaryExpr(BinaryExpr {
                                    left: _,
                                    right: _,
                                    op,
                                }) => {
                                    if matches!(
                                        op,
                                        Operator::Lt
                                            | Operator::LtEq
                                            | Operator::Gt
                                            | Operator::GtEq
                                    ) {
                                        range_filters.push((**expr).clone());
                                        num_range_filters += 1;
                                    }
                                    total_filters += 1;
                                }
                                // TODO: Want to deal with `Expr::Between` for IEJoins, it counts as two range predicates
                                // which is why it is not dealt with in PWMJ
                                // Expr::Between(_) => {},
                                _ => {
                                    total_filters += 1;
                                }
                            }
                        }

                        // Extract columns from filter expression and saved in a HashSet
                        let cols = expr.column_refs();

                        // Collect left & right field indices, the field indices are sorted in ascending order
                        let left_field_indices = cols
                            .iter()
                            .filter_map(|c| left_df_schema.index_of_column(c).ok())
                            .sorted()
                            .collect::<Vec<_>>();
                        let right_field_indices = cols
                            .iter()
                            .filter_map(|c| right_df_schema.index_of_column(c).ok())
                            .sorted()
                            .collect::<Vec<_>>();

                        // Collect DFFields and Fields required for intermediate schemas
                        let (filter_df_fields, filter_fields): (Vec<_>, Vec<_>) =
                            left_field_indices
                                .clone()
                                .into_iter()
                                .map(|i| {
                                    (
                                        left_df_schema.qualified_field(i),
                                        physical_left.schema().field(i).clone(),
                                    )
                                })
                                .chain(right_field_indices.clone().into_iter().map(|i| {
                                    (
                                        right_df_schema.qualified_field(i),
                                        physical_right.schema().field(i).clone(),
                                    )
                                }))
                                .unzip();
                        let filter_df_fields = filter_df_fields
                            .into_iter()
                            .map(|(qualifier, field)| {
                                (qualifier.cloned(), Arc::clone(field))
                            })
                            .collect();

                        let metadata: HashMap<_, _> = left_df_schema
                            .metadata()
                            .clone()
                            .into_iter()
                            .chain(right_df_schema.metadata().clone())
                            .collect();

                        // Construct intermediate schemas used for filtering data and
                        // convert logical expression to physical according to filter schema
                        let filter_df_schema = DFSchema::new_with_metadata(
                            filter_df_fields,
                            metadata.clone(),
                        )?;
                        let filter_schema =
                            Schema::new_with_metadata(filter_fields, metadata);

                        let filter_expr = create_physical_expr(
                            expr,
                            &filter_df_schema,
                            session_state.execution_props(),
                        )?;
                        let column_indices = join_utils::JoinFilter::build_column_indices(
                            left_field_indices,
                            right_field_indices,
                        );

                        Some(join_utils::JoinFilter::new(
                            filter_expr,
                            column_indices,
                            Arc::new(filter_schema),
                        ))
                    }
                    _ => None,
                };

                let prefer_hash_join =
                    session_state.config_options().optimizer.prefer_hash_join;

                // TODO: Allow PWMJ to deal with residual equijoin conditions
                let join: Arc<dyn ExecutionPlan> = if join_on.is_empty() {
                    if join_filter.is_none() && matches!(join_type, JoinType::Inner) {
                        // cross join if there is no join conditions and no join filter set
                        Arc::new(CrossJoinExec::new(physical_left, physical_right))
                    } else if num_range_filters == 1
                        && total_filters == 1
                        && !matches!(
                            join_type,
                            JoinType::LeftSemi
                                | JoinType::RightSemi
                                | JoinType::LeftAnti
                                | JoinType::RightAnti
                                | JoinType::LeftMark
                                | JoinType::RightMark
                        )
                        && session_state
                            .config_options()
                            .optimizer
                            .enable_piecewise_merge_join
                    {
                        let Expr::BinaryExpr(be) = &range_filters[0] else {
                            return plan_err!(
                                "Unsupported expression for PWMJ: Expected `Expr::BinaryExpr`"
                            );
                        };

                        let mut op = be.op;
                        if !matches!(
                            op,
                            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
                        ) {
                            return plan_err!(
                                "Unsupported operator for PWMJ: {:?}. Expected one of <, <=, >, >=",
                                op
                            );
                        }

                        fn reverse_ineq(op: Operator) -> Operator {
                            match op {
                                Operator::Lt => Operator::Gt,
                                Operator::LtEq => Operator::GtEq,
                                Operator::Gt => Operator::Lt,
                                Operator::GtEq => Operator::LtEq,
                                _ => op,
                            }
                        }

                        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
                        enum Side {
                            Left,
                            Right,
                            Both,
                        }

                        let side_of = |e: &Expr| -> Result<Side> {
                            let cols = e.column_refs();
                            let any_left = cols
                                .iter()
                                .any(|c| left_df_schema.index_of_column(c).is_ok());
                            let any_right = cols
                                .iter()
                                .any(|c| right_df_schema.index_of_column(c).is_ok());

                            Ok(match (any_left, any_right) {
                                (true, false) => Side::Left,
                                (false, true) => Side::Right,
                                (true, true) => Side::Both,
                                _ => unreachable!(),
                            })
                        };

                        let mut lhs_logical = &be.left;
                        let mut rhs_logical = &be.right;

                        let left_side = side_of(lhs_logical)?;
                        let right_side = side_of(rhs_logical)?;
                        if matches!(left_side, Side::Both)
                            || matches!(right_side, Side::Both)
                        {
                            return Ok(Arc::new(NestedLoopJoinExec::try_new(
                                physical_left,
                                physical_right,
                                join_filter,
                                join_type,
                                None,
                            )?));
                        }

                        if left_side == Side::Right && right_side == Side::Left {
                            std::mem::swap(&mut lhs_logical, &mut rhs_logical);
                            op = reverse_ineq(op);
                        } else if !(left_side == Side::Left && right_side == Side::Right)
                        {
                            return plan_err!(
                                "Unsupported operator for PWMJ: {:?}. Expected one of <, <=, >, >=",
                                op
                            );
                        }

                        let on_left = create_physical_expr(
                            lhs_logical,
                            left_df_schema,
                            session_state.execution_props(),
                        )?;
                        let on_right = create_physical_expr(
                            rhs_logical,
                            right_df_schema,
                            session_state.execution_props(),
                        )?;

                        Arc::new(PiecewiseMergeJoinExec::try_new(
                            physical_left,
                            physical_right,
                            (on_left, on_right),
                            op,
                            *join_type,
                            session_state.config().target_partitions(),
                        )?)
                    } else {
                        // there is no equal join condition, use the nested loop join
                        Arc::new(NestedLoopJoinExec::try_new(
                            physical_left,
                            physical_right,
                            join_filter,
                            join_type,
                            None,
                        )?)
                    }
                } else if session_state.config().target_partitions() > 1
                    && session_state.config().repartition_joins()
                    && !prefer_hash_join
                {
                    // Use SortMergeJoin if hash join is not preferred
                    let join_on_len = join_on.len();
                    Arc::new(SortMergeJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_filter,
                        *join_type,
                        vec![SortOptions::default(); join_on_len],
                        *null_equality,
                    )?)
                } else if session_state.config().target_partitions() > 1
                    && session_state.config().repartition_joins()
                    && prefer_hash_join
                {
                    Arc::new(HashJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_filter,
                        join_type,
                        None,
                        PartitionMode::Auto,
                        *null_equality,
                    )?)
                } else {
                    Arc::new(HashJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_filter,
                        join_type,
                        None,
                        PartitionMode::CollectLeft,
                        *null_equality,
                    )?)
                };

                // If plan was mutated previously then need to create the ExecutionPlan
                // for the new Projection that was applied on top.
                if let Some((input, expr)) = new_project {
                    self.create_project_physical_exec(session_state, join, input, expr)?
                } else {
                    join
                }
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name, is_distinct, ..
            }) => {
                let [static_term, recursive_term] = children.two()?;
                Arc::new(RecursiveQueryExec::try_new(
                    name.clone(),
                    static_term,
                    recursive_term,
                    *is_distinct,
                )?)
            }

            // N Children
            LogicalPlan::Union(_) => UnionExec::try_new(children.vec())?,
            LogicalPlan::Extension(Extension { node }) => {
                let mut maybe_plan = None;
                let children = children.vec();
                for planner in &self.extension_planners {
                    if maybe_plan.is_some() {
                        break;
                    }

                    let logical_input = node.inputs();
                    maybe_plan = planner
                        .plan_extension(
                            self,
                            node.as_ref(),
                            &logical_input,
                            &children,
                            session_state,
                        )
                        .await?;
                }

                let plan = match maybe_plan {
                        Some(v) => Ok(v),
                        _ => plan_err!("No installed planner was able to convert the custom node to an execution plan: {:?}", node)
                    }?;

                // Ensure the ExecutionPlan's schema matches the
                // declared logical schema to catch and warn about
                // logic errors when creating user defined plans.
                if !node.schema().matches_arrow_schema(&plan.schema()) {
                    return plan_err!(
                            "Extension planner for {:?} created an ExecutionPlan with mismatched schema. \
                            LogicalPlan schema: {:?}, ExecutionPlan schema: {:?}",
                            node, node.schema(), plan.schema()
                        );
                } else {
                    plan
                }
            }

            // Other
            LogicalPlan::Statement(statement) => {
                // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                let name = statement.name();
                return not_impl_err!("Unsupported logical plan: Statement({name})");
            }
            LogicalPlan::Dml(dml) => {
                // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                return not_impl_err!("Unsupported logical plan: Dml({0})", dml.op);
            }
            LogicalPlan::Ddl(ddl) => {
                // There is no default plan for DDl statements --
                // it must be handled at a higher level (so that
                // the appropriate table can be registered with
                // the context)
                let name = ddl.name();
                return not_impl_err!("Unsupported logical plan: {name}");
            }
            LogicalPlan::Explain(_) => {
                return internal_err!(
                    "Unsupported logical plan: Explain must be root of the plan"
                )
            }
            LogicalPlan::Distinct(_) => {
                return internal_err!(
                    "Unsupported logical plan: Distinct should be replaced to Aggregate"
                )
            }
            LogicalPlan::Analyze(_) => {
                return internal_err!(
                    "Unsupported logical plan: Analyze must be root of the plan"
                )
            }
        };
        Ok(exec_node)
    }

    fn create_grouping_physical_expr(
        &self,
        group_expr: &[Expr],
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<PhysicalGroupBy> {
        if group_expr.len() == 1 {
            match &group_expr[0] {
                Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)) => {
                    merge_grouping_set_physical_expr(
                        grouping_sets,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
                Expr::GroupingSet(GroupingSet::Cube(exprs)) => create_cube_physical_expr(
                    exprs,
                    input_dfschema,
                    input_schema,
                    session_state,
                ),
                Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                    create_rollup_physical_expr(
                        exprs,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
                expr => Ok(PhysicalGroupBy::new_single(vec![tuple_err((
                    self.create_physical_expr(expr, input_dfschema, session_state),
                    physical_name(expr),
                ))?])),
            }
        } else if group_expr.is_empty() {
            // No GROUP BY clause - create empty PhysicalGroupBy
            Ok(PhysicalGroupBy::new(vec![], vec![], vec![]))
        } else {
            Ok(PhysicalGroupBy::new_single(
                group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(e, input_dfschema, session_state),
                            physical_name(e),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
    }
}

/// Expand and align a GROUPING SET expression.
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
///
/// This will take a list of grouping sets and ensure that each group is
/// properly aligned for the physical execution plan. We do this by
/// identifying all unique expression in each group and conforming each
/// group to the same set of expression types and ordering.
/// For example, if we have something like `GROUPING SETS ((a,b,c),(a),(b),(b,c))`
/// we would expand this to `GROUPING SETS ((a,b,c),(a,NULL,NULL),(NULL,b,NULL),(NULL,b,c))
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn merge_grouping_set_physical_expr(
    grouping_sets: &[Vec<Expr>],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_groups = grouping_sets.len();
    let mut all_exprs: Vec<Expr> = vec![];
    let mut grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

    for expr in grouping_sets.iter().flatten() {
        if !all_exprs.contains(expr) {
            all_exprs.push(expr.clone());

            grouping_set_expr.push(get_physical_expr_pair(
                expr,
                input_dfschema,
                session_state,
            )?);

            null_exprs.push(get_null_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);
        }
    }

    let mut merged_sets: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    for expr_group in grouping_sets.iter() {
        let group: Vec<bool> = all_exprs
            .iter()
            .map(|expr| !expr_group.contains(expr))
            .collect();

        merged_sets.push(group)
    }

    Ok(PhysicalGroupBy::new(
        grouping_set_expr,
        null_exprs,
        merged_sets,
    ))
}

/// Expand and align a CUBE expression. This is a special case of GROUPING SETS
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn create_cube_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();
    let num_groups = num_of_exprs * num_of_exprs;

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(expr, input_dfschema, session_state)?)
    }

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    groups.push(vec![false; num_of_exprs]);

    for null_count in 1..=num_of_exprs {
        for null_idx in (0..num_of_exprs).combinations(null_count) {
            let mut next_group: Vec<bool> = vec![false; num_of_exprs];
            null_idx.into_iter().for_each(|i| next_group[i] = true);
            groups.push(next_group);
        }
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// Expand and align a ROLLUP expression. This is a special case of GROUPING SETS
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn create_rollup_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_of_exprs + 1);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(expr, input_dfschema, session_state)?)
    }

    for total in 0..=num_of_exprs {
        let mut group: Vec<bool> = Vec::with_capacity(num_of_exprs);

        for index in 0..num_of_exprs {
            if index < total {
                group.push(false);
            } else {
                group.push(true);
            }
        }

        groups.push(group)
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// For a given logical expr, get a properly typed NULL ScalarValue physical expression
fn get_null_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    let physical_expr =
        create_physical_expr(expr, input_dfschema, session_state.execution_props())?;
    let physical_name = physical_name(&expr.clone())?;

    let data_type = physical_expr.data_type(input_schema)?;
    let null_value: ScalarValue = (&data_type).try_into()?;

    let null_value = Literal::new(null_value);
    Ok((Arc::new(null_value), physical_name))
}

/// Qualifies the fields in a join schema with "left" and "right" qualifiers
/// without mutating the original schema. This function should only be used when
/// the join inputs have already been requalified earlier in `try_new_with_project_input`.
///
/// The purpose is to avoid ambiguity errors later in planning (e.g., in nullability or data type resolution)
/// when converting expressions to fields.
fn qualify_join_schema_sides(
    join_schema: &DFSchema,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Result<DFSchema> {
    let left_fields = left.schema().fields();
    let right_fields = right.schema().fields();
    let join_fields = join_schema.fields();

    // Validate lengths
    assert_eq_or_internal_err!(
        join_fields.len(),
        left_fields.len() + right_fields.len(),
        "Join schema field count must match left and right field count."
    );

    // Validate field names match
    for (i, (field, expected)) in join_fields
        .iter()
        .zip(left_fields.iter().chain(right_fields.iter()))
        .enumerate()
    {
        assert_eq_or_internal_err!(
            field.name(),
            expected.name(),
            "Field name mismatch at index {}",
            i
        );
    }

    // qualify sides
    let qualifiers = join_fields
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if i < left_fields.len() {
                Some(TableReference::Bare {
                    table: Arc::from("left"),
                })
            } else {
                Some(TableReference::Bare {
                    table: Arc::from("right"),
                })
            }
        })
        .collect();

    join_schema.with_field_specific_qualified_schema(qualifiers)
}

fn get_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    let physical_expr =
        create_physical_expr(expr, input_dfschema, session_state.execution_props())?;
    let physical_name = physical_name(expr)?;
    Ok((physical_expr, physical_name))
}

/// Check if window bounds are valid after schema information is available, and
/// window_frame bounds are casted to the corresponding column type.
/// queries like:
/// OVER (ORDER BY a RANGES BETWEEN 3 PRECEDING AND 5 PRECEDING)
/// OVER (ORDER BY a RANGES BETWEEN INTERVAL '3 DAY' PRECEDING AND '5 DAY' PRECEDING)  are rejected
pub fn is_window_frame_bound_valid(window_frame: &WindowFrame) -> bool {
    match (&window_frame.start_bound, &window_frame.end_bound) {
        (WindowFrameBound::Following(_), WindowFrameBound::Preceding(_))
        | (WindowFrameBound::Following(_), WindowFrameBound::CurrentRow)
        | (WindowFrameBound::CurrentRow, WindowFrameBound::Preceding(_)) => false,
        (WindowFrameBound::Preceding(lhs), WindowFrameBound::Preceding(rhs)) => {
            !rhs.is_null() && (lhs.is_null() || (lhs >= rhs))
        }
        (WindowFrameBound::Following(lhs), WindowFrameBound::Following(rhs)) => {
            !lhs.is_null() && (rhs.is_null() || (lhs <= rhs))
        }
        _ => true,
    }
}

/// Create a window expression with a name from a logical expression
pub fn create_window_expr_with_name(
    e: &Expr,
    name: impl Into<String>,
    logical_schema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    let name = name.into();
    let physical_schema = Arc::clone(logical_schema.inner());
    match e {
        Expr::WindowFunction(window_fun) => {
            let WindowFunction {
                fun,
                params:
                    WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        null_treatment,
                        distinct,
                        filter,
                    },
            } = window_fun.as_ref();
            let physical_args =
                create_physical_exprs(args, logical_schema, execution_props)?;
            let partition_by =
                create_physical_exprs(partition_by, logical_schema, execution_props)?;
            let order_by =
                create_physical_sort_exprs(order_by, logical_schema, execution_props)?;

            if !is_window_frame_bound_valid(window_frame) {
                return plan_err!(
                        "Invalid window frame: start bound ({}) cannot be larger than end bound ({})",
                        window_frame.start_bound, window_frame.end_bound
                    );
            }

            let window_frame = Arc::new(window_frame.clone());
            let ignore_nulls = null_treatment.unwrap_or(NullTreatment::RespectNulls)
                == NullTreatment::IgnoreNulls;
            let physical_filter = filter
                .as_ref()
                .map(|f| create_physical_expr(f, logical_schema, execution_props))
                .transpose()?;

            windows::create_window_expr(
                fun,
                name,
                &physical_args,
                &partition_by,
                &order_by,
                window_frame,
                physical_schema,
                ignore_nulls,
                *distinct,
                physical_filter,
            )
        }
        other => plan_err!("Invalid window expression '{other:?}'"),
    }
}

/// Create a window expression from a logical expression or an alias
pub fn create_window_expr(
    e: &Expr,
    logical_schema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    // unpack aliased logical expressions, e.g. "sum(col) over () as total"
    let (name, e) = match e {
        Expr::Alias(Alias { expr, name, .. }) => (name.clone(), expr.as_ref()),
        _ => (e.schema_name().to_string(), e),
    };
    create_window_expr_with_name(e, name, logical_schema, execution_props)
}

type AggregateExprWithOptionalArgs = (
    Arc<AggregateFunctionExpr>,
    // The filter clause, if any
    Option<Arc<dyn PhysicalExpr>>,
    // Expressions in the ORDER BY clause
    Vec<PhysicalSortExpr>,
);

/// Create an aggregate expression with a name from a logical expression
pub fn create_aggregate_expr_with_name_and_maybe_filter(
    e: &Expr,
    name: Option<String>,
    human_displan: String,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<AggregateExprWithOptionalArgs> {
    match e {
        Expr::AggregateFunction(AggregateFunction {
            func,
            params:
                AggregateFunctionParams {
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
        }) => {
            let name = if let Some(name) = name {
                name
            } else {
                physical_name(e)?
            };

            let physical_args =
                create_physical_exprs(args, logical_input_schema, execution_props)?;
            let filter = match filter {
                Some(e) => Some(create_physical_expr(
                    e,
                    logical_input_schema,
                    execution_props,
                )?),
                None => None,
            };

            let ignore_nulls = null_treatment.unwrap_or(NullTreatment::RespectNulls)
                == NullTreatment::IgnoreNulls;

            let (agg_expr, filter, order_bys) = {
                let order_bys = create_physical_sort_exprs(
                    order_by,
                    logical_input_schema,
                    execution_props,
                )?;

                let agg_expr =
                    AggregateExprBuilder::new(func.to_owned(), physical_args.to_vec())
                        .order_by(order_bys.clone())
                        .schema(Arc::new(physical_input_schema.to_owned()))
                        .alias(name)
                        .human_display(human_displan)
                        .with_ignore_nulls(ignore_nulls)
                        .with_distinct(*distinct)
                        .build()
                        .map(Arc::new)?;

                (agg_expr, filter, order_bys)
            };

            Ok((agg_expr, filter, order_bys))
        }
        other => internal_err!("Invalid aggregate expression '{other:?}'"),
    }
}

/// Create an aggregate expression from a logical expression or an alias
pub fn create_aggregate_expr_and_maybe_filter(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<AggregateExprWithOptionalArgs> {
    // Unpack (potentially nested) aliased logical expressions, e.g. "sum(col) as total"
    // Some functions like `count_all()` create internal aliases,
    // Unwrap all alias layers to get to the underlying aggregate function
    let (name, human_display, e) = match e {
        Expr::Alias(Alias { name, .. }) => {
            let unaliased = e.clone().unalias_nested().data;
            (Some(name.clone()), e.human_display().to_string(), unaliased)
        }
        Expr::AggregateFunction(_) => (
            Some(e.schema_name().to_string()),
            e.human_display().to_string(),
            e.clone(),
        ),
        _ => (None, String::default(), e.clone()),
    };

    create_aggregate_expr_with_name_and_maybe_filter(
        &e,
        name,
        human_display,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

impl DefaultPhysicalPlanner {
    /// Handles capturing the various plans for EXPLAIN queries
    ///
    /// Returns
    /// Some(plan) if optimized, and None if logical_plan was not an
    /// explain (and thus needs to be optimized as normal)
    async fn handle_explain_or_analyze(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let execution_plan = match logical_plan {
            LogicalPlan::Explain(e) => self.handle_explain(e, session_state).await?,
            LogicalPlan::Analyze(a) => self.handle_analyze(a, session_state).await?,
            _ => return Ok(None),
        };
        Ok(Some(execution_plan))
    }

    /// Planner for `LogicalPlan::Explain`
    async fn handle_explain(
        &self,
        e: &Explain,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use PlanType::*;
        let mut stringified_plans = vec![];

        let config = &session_state.config_options().explain;
        let explain_format = &e.explain_format;

        if !e.logical_optimization_succeeded {
            return Ok(Arc::new(ExplainExec::new(
                Arc::clone(e.schema.inner()),
                e.stringified_plans.clone(),
                true,
            )));
        }

        match explain_format {
            ExplainFormat::Indent => { /* fall through */ }
            ExplainFormat::Tree => {
                // Tree render does not try to explain errors,
                let physical_plan = self
                    .create_initial_plan(e.plan.as_ref(), session_state)
                    .await?;

                let optimized_plan = self.optimize_physical_plan(
                    physical_plan,
                    session_state,
                    |_plan, _optimizer| {},
                )?;

                stringified_plans.push(StringifiedPlan::new(
                    FinalPhysicalPlan,
                    displayable(optimized_plan.as_ref())
                        .set_tree_maximum_render_width(config.tree_maximum_render_width)
                        .tree_render()
                        .to_string(),
                ));
            }
            ExplainFormat::PostgresJSON => {
                stringified_plans.push(StringifiedPlan::new(
                    FinalLogicalPlan,
                    e.plan.display_pg_json().to_string(),
                ));
            }
            ExplainFormat::Graphviz => {
                stringified_plans.push(StringifiedPlan::new(
                    FinalLogicalPlan,
                    e.plan.display_graphviz().to_string(),
                ));
            }
        };

        if !stringified_plans.is_empty() {
            return Ok(Arc::new(ExplainExec::new(
                Arc::clone(e.schema.inner()),
                stringified_plans,
                e.verbose,
            )));
        }

        // The indent mode is quite sophisticated, and handles quite a few
        // different cases / options for displaying the plan.
        if !config.physical_plan_only {
            stringified_plans.clone_from(&e.stringified_plans);
            if e.logical_optimization_succeeded {
                stringified_plans.push(e.plan.to_stringified(FinalLogicalPlan));
            }
        }

        if !config.logical_plan_only && e.logical_optimization_succeeded {
            match self
                .create_initial_plan(e.plan.as_ref(), session_state)
                .await
            {
                Ok(input) => {
                    // Include statistics / schema if enabled
                    stringified_plans.push(StringifiedPlan::new(
                        InitialPhysicalPlan,
                        displayable(input.as_ref())
                            .set_show_statistics(config.show_statistics)
                            .set_show_schema(config.show_schema)
                            .indent(e.verbose)
                            .to_string(),
                    ));

                    // Show statistics + schema in verbose output even if not
                    // explicitly requested
                    if e.verbose {
                        if !config.show_statistics {
                            stringified_plans.push(StringifiedPlan::new(
                                InitialPhysicalPlanWithStats,
                                displayable(input.as_ref())
                                    .set_show_statistics(true)
                                    .indent(e.verbose)
                                    .to_string(),
                            ));
                        }
                        if !config.show_schema {
                            stringified_plans.push(StringifiedPlan::new(
                                InitialPhysicalPlanWithSchema,
                                displayable(input.as_ref())
                                    .set_show_schema(true)
                                    .indent(e.verbose)
                                    .to_string(),
                            ));
                        }
                    }

                    let optimized_plan = self.optimize_physical_plan(
                        input,
                        session_state,
                        |plan, optimizer| {
                            let optimizer_name = optimizer.name().to_string();
                            let plan_type = OptimizedPhysicalPlan { optimizer_name };
                            stringified_plans.push(StringifiedPlan::new(
                                plan_type,
                                displayable(plan)
                                    .set_show_statistics(config.show_statistics)
                                    .set_show_schema(config.show_schema)
                                    .indent(e.verbose)
                                    .to_string(),
                            ));
                        },
                    );
                    match optimized_plan {
                        Ok(input) => {
                            // This plan will includes statistics if show_statistics is on
                            stringified_plans.push(StringifiedPlan::new(
                                FinalPhysicalPlan,
                                displayable(input.as_ref())
                                    .set_show_statistics(config.show_statistics)
                                    .set_show_schema(config.show_schema)
                                    .indent(e.verbose)
                                    .to_string(),
                            ));

                            // Show statistics + schema in verbose output even if not
                            // explicitly requested
                            if e.verbose {
                                if !config.show_statistics {
                                    stringified_plans.push(StringifiedPlan::new(
                                        FinalPhysicalPlanWithStats,
                                        displayable(input.as_ref())
                                            .set_show_statistics(true)
                                            .indent(e.verbose)
                                            .to_string(),
                                    ));
                                }
                                if !config.show_schema {
                                    stringified_plans.push(StringifiedPlan::new(
                                        FinalPhysicalPlanWithSchema,
                                        // This will include schema if show_schema is on
                                        // and will be set to true if verbose is on
                                        displayable(input.as_ref())
                                            .set_show_schema(true)
                                            .indent(e.verbose)
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                        Err(DataFusionError::Context(optimizer_name, e)) => {
                            let plan_type = OptimizedPhysicalPlan { optimizer_name };
                            stringified_plans
                                .push(StringifiedPlan::new(plan_type, e.to_string()))
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(err) => {
                    stringified_plans.push(StringifiedPlan::new(
                        PhysicalPlanError,
                        err.strip_backtrace(),
                    ));
                }
            }
        }

        Ok(Arc::new(ExplainExec::new(
            Arc::clone(e.schema.inner()),
            stringified_plans,
            e.verbose,
        )))
    }

    async fn handle_analyze(
        &self,
        a: &Analyze,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = self.create_physical_plan(&a.input, session_state).await?;
        let schema = Arc::clone(a.schema.inner());
        let show_statistics = session_state.config_options().explain.show_statistics;
        let analyze_level = session_state.config_options().explain.analyze_level;
        let metric_types = match analyze_level {
            ExplainAnalyzeLevel::Summary => vec![MetricType::SUMMARY],
            ExplainAnalyzeLevel::Dev => vec![MetricType::SUMMARY, MetricType::DEV],
        };
        Ok(Arc::new(AnalyzeExec::new(
            a.verbose,
            show_statistics,
            metric_types,
            input,
            schema,
        )))
    }

    /// Optimize a physical plan by applying each physical optimizer,
    /// calling observer(plan, optimizer after each one)
    #[expect(clippy::needless_pass_by_value)]
    pub fn optimize_physical_plan<F>(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_state: &SessionState,
        mut observer: F,
    ) -> Result<Arc<dyn ExecutionPlan>>
    where
        F: FnMut(&dyn ExecutionPlan, &dyn PhysicalOptimizerRule),
    {
        let optimizers = session_state.physical_optimizers();
        debug!(
            "Input physical plan:\n{}\n",
            displayable(plan.as_ref()).indent(false)
        );
        debug!(
            "Detailed input physical plan:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        // This runs once before any optimization,
        // to verify that the plan fulfills the base requirements.
        InvariantChecker(InvariantLevel::Always).check(&plan)?;

        // Create optimizer context from session state
        let optimizer_context = OptimizerContext::new(session_state.config().clone());

        let mut new_plan = Arc::clone(&plan);
        for optimizer in optimizers {
            let before_schema = new_plan.schema();
            new_plan = optimizer
                .optimize_plan(new_plan, &optimizer_context)
                .map_err(|e| {
                    DataFusionError::Context(optimizer.name().to_string(), Box::new(e))
                })?;

            // This only checks the schema in release build, and performs additional checks in debug mode.
            OptimizationInvariantChecker::new(optimizer)
                .check(&new_plan, &before_schema)?;

            debug!(
                "Optimized physical plan by {}:\n{}\n",
                optimizer.name(),
                displayable(new_plan.as_ref()).indent(false)
            );
            observer(new_plan.as_ref(), optimizer.as_ref())
        }

        // This runs once after all optimizer runs are complete,
        // to verify that the plan is executable.
        InvariantChecker(InvariantLevel::Executable).check(&new_plan)?;

        debug!(
            "Optimized physical plan:\n{}\n",
            displayable(new_plan.as_ref()).indent(false)
        );

        // Don't print new_plan directly, as that may overflow the stack.
        // For example:
        // thread 'tokio-runtime-worker' has overflowed its stack
        // fatal runtime error: stack overflow, aborting
        debug!(
            "Detailed optimized physical plan:\n{}\n",
            displayable(new_plan.as_ref()).indent(true)
        );
        Ok(new_plan)
    }

    // return an record_batch which describes a table's schema.
    fn plan_describe(
        &self,
        table_schema: &Arc<Schema>,
        output_schema: Arc<Schema>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut column_names = StringBuilder::new();
        let mut data_types = StringBuilder::new();
        let mut is_nullables = StringBuilder::new();
        for field in table_schema.fields() {
            column_names.append_value(field.name());

            // "System supplied type" --> Use debug format of the datatype
            let data_type = field.data_type();
            data_types.append_value(format!("{data_type}"));

            // "YES if the column is possibly nullable, NO if it is known not nullable. "
            let nullable_str = if field.is_nullable() { "YES" } else { "NO" };
            is_nullables.append_value(nullable_str);
        }

        let record_batch = RecordBatch::try_new(
            output_schema,
            vec![
                Arc::new(column_names.finish()),
                Arc::new(data_types.finish()),
                Arc::new(is_nullables.finish()),
            ],
        )?;

        let schema = record_batch.schema();
        let partitions = vec![vec![record_batch]];
        let projection = None;
        let mem_exec = MemorySourceConfig::try_new_exec(&partitions, schema, projection)?;
        Ok(mem_exec)
    }

    fn create_project_physical_exec(
        &self,
        session_state: &SessionState,
        input_exec: Arc<dyn ExecutionPlan>,
        input: &Arc<LogicalPlan>,
        expr: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_logical_schema = input.as_ref().schema();
        let input_physical_schema = input_exec.schema();
        let physical_exprs = expr
            .iter()
            .map(|e| {
                // For projections, SQL planner and logical plan builder may convert user
                // provided expressions into logical Column expressions if their results
                // are already provided from the input plans. Because we work with
                // qualified columns in logical plane, derived columns involve operators or
                // functions will contain qualifiers as well. This will result in logical
                // columns with names like `SUM(t1.c1)`, `t1.c1 + t1.c2`, etc.
                //
                // If we run these logical columns through physical_name function, we will
                // get physical names with column qualifiers, which violates DataFusion's
                // field name semantics. To account for this, we need to derive the
                // physical name from physical input instead.
                //
                // This depends on the invariant that logical schema field index MUST match
                // with physical schema field index.
                let physical_name = if let Expr::Column(col) = e {
                    match input_logical_schema.index_of_column(col) {
                        Ok(idx) => {
                            // index physical field using logical field index
                            Ok(input_exec.schema().field(idx).name().to_string())
                        }
                        // logical column is not a derived column, safe to pass along to
                        // physical_name
                        Err(_) => physical_name(e),
                    }
                } else {
                    physical_name(e)
                };

                let physical_expr =
                    self.create_physical_expr(e, input_logical_schema, session_state);

                tuple_err((physical_expr, physical_name))
            })
            .collect::<Result<Vec<_>>>()?;

        let num_input_columns = input_exec.schema().fields().len();

        match self.try_plan_async_exprs(
            num_input_columns,
            PlannedExprResult::ExprWithName(physical_exprs),
            input_physical_schema.as_ref(),
        )? {
            PlanAsyncExpr::Sync(PlannedExprResult::ExprWithName(physical_exprs)) => {
                let proj_exprs: Vec<ProjectionExpr> = physical_exprs
                    .into_iter()
                    .map(|(expr, alias)| ProjectionExpr { expr, alias })
                    .collect();
                Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input_exec)?))
            }
            PlanAsyncExpr::Async(
                async_map,
                PlannedExprResult::ExprWithName(physical_exprs),
            ) => {
                let async_exec =
                    AsyncFuncExec::try_new(async_map.async_exprs, input_exec)?;
                let proj_exprs: Vec<ProjectionExpr> = physical_exprs
                    .into_iter()
                    .map(|(expr, alias)| ProjectionExpr { expr, alias })
                    .collect();
                let new_proj_exec =
                    ProjectionExec::try_new(proj_exprs, Arc::new(async_exec))?;
                Ok(Arc::new(new_proj_exec))
            }
            _ => internal_err!("Unexpected PlanAsyncExpressions variant"),
        }
    }

    fn try_plan_async_exprs(
        &self,
        num_input_columns: usize,
        physical_expr: PlannedExprResult,
        schema: &Schema,
    ) -> Result<PlanAsyncExpr> {
        let mut async_map = AsyncMapper::new(num_input_columns);
        match &physical_expr {
            PlannedExprResult::ExprWithName(exprs) => {
                exprs
                    .iter()
                    .try_for_each(|(expr, _)| async_map.find_references(expr, schema))?;
            }
            PlannedExprResult::Expr(exprs) => {
                exprs
                    .iter()
                    .try_for_each(|expr| async_map.find_references(expr, schema))?;
            }
        }

        if async_map.is_empty() {
            return Ok(PlanAsyncExpr::Sync(physical_expr));
        }

        let new_exprs = match physical_expr {
            PlannedExprResult::ExprWithName(exprs) => PlannedExprResult::ExprWithName(
                exprs
                    .iter()
                    .map(|(expr, column_name)| {
                        let new_expr = Arc::clone(expr)
                            .transform_up(|e| Ok(async_map.map_expr(e)))?;
                        Ok((new_expr.data, column_name.to_string()))
                    })
                    .collect::<Result<_>>()?,
            ),
            PlannedExprResult::Expr(exprs) => PlannedExprResult::Expr(
                exprs
                    .iter()
                    .map(|expr| {
                        let new_expr = Arc::clone(expr)
                            .transform_up(|e| Ok(async_map.map_expr(e)))?;
                        Ok(new_expr.data)
                    })
                    .collect::<Result<_>>()?,
            ),
        };
        // rewrite the projection's expressions in terms of the columns with the result of async evaluation
        Ok(PlanAsyncExpr::Async(async_map, new_exprs))
    }
}

#[derive(Debug)]
enum PlannedExprResult {
    ExprWithName(Vec<(Arc<dyn PhysicalExpr>, String)>),
    Expr(Vec<Arc<dyn PhysicalExpr>>),
}

#[derive(Debug)]
enum PlanAsyncExpr {
    Sync(PlannedExprResult),
    Async(AsyncMapper, PlannedExprResult),
}

fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

struct OptimizationInvariantChecker<'a> {
    rule: &'a Arc<dyn PhysicalOptimizerRule + Send + Sync>,
}

impl<'a> OptimizationInvariantChecker<'a> {
    /// Create an [`OptimizationInvariantChecker`] that performs checking per tule.
    pub fn new(rule: &'a Arc<dyn PhysicalOptimizerRule + Send + Sync>) -> Self {
        Self { rule }
    }

    /// Checks that the plan change is permitted, returning an Error if not.
    ///
    /// Conditionally performs schema checks per [PhysicalOptimizerRule::schema_check].
    /// In debug mode, this recursively walks the entire physical plan
    /// and performs [`ExecutionPlan::check_invariants`].
    pub fn check(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
        previous_schema: &Arc<Schema>,
    ) -> Result<()> {
        // if the rule is not permitted to change the schema, confirm that it did not change.
        if self.rule.schema_check()
            && !is_allowed_schema_change(previous_schema.as_ref(), plan.schema().as_ref())
        {
            internal_err!("PhysicalOptimizer rule '{}' failed. Schema mismatch. Expected original schema: {:?}, got new schema: {:?}",
                self.rule.name(),
                previous_schema,
                plan.schema()
            )?
        }

        // check invariants per each ExecutionPlan node
        #[cfg(debug_assertions)]
        plan.visit(self)?;

        Ok(())
    }
}

/// Checks if the change from `old` schema to `new` is allowed or not.
///
/// The current implementation only allows nullability of individual fields to change
/// from 'nullable' to 'not nullable'. This can happen due to physical expressions knowing
/// more about their null-ness than their logical counterparts.
/// This change is allowed because for any field the non-nullable domain `F` is a strict subset
/// of the nullable domain `F ∪ { NULL }`. A physical schema that guarantees a stricter subset
/// of values will not violate any assumptions made based on the less strict schema.
fn is_allowed_schema_change(old: &Schema, new: &Schema) -> bool {
    if new.metadata != old.metadata {
        return false;
    }

    if new.fields.len() != old.fields.len() {
        return false;
    }

    let new_fields = new.fields.iter().map(|f| f.as_ref());
    let old_fields = old.fields.iter().map(|f| f.as_ref());
    old_fields
        .zip(new_fields)
        .all(|(old, new)| is_allowed_field_change(old, new))
}

fn is_allowed_field_change(old_field: &Field, new_field: &Field) -> bool {
    new_field.name() == old_field.name()
        && new_field.data_type() == old_field.data_type()
        && new_field.metadata() == old_field.metadata()
        && (new_field.is_nullable() == old_field.is_nullable()
            || !new_field.is_nullable())
}

impl<'n> TreeNodeVisitor<'n> for OptimizationInvariantChecker<'_> {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        // Checks for the more permissive `InvariantLevel::Always`.
        // Plans are not guaranteed to be executable after each physical optimizer run.
        node.check_invariants(InvariantLevel::Always).map_err(|e|
            e.context(format!("Invariant for ExecutionPlan node '{}' failed for PhysicalOptimizer rule '{}'", node.name(), self.rule.name()))
        )?;
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Check [`ExecutionPlan`] invariants per [`InvariantLevel`].
struct InvariantChecker(InvariantLevel);

impl InvariantChecker {
    /// Checks that the plan is executable, returning an Error if not.
    pub fn check(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        // check invariants per each ExecutionPlan node
        plan.visit(self)?;

        Ok(())
    }
}

impl<'n> TreeNodeVisitor<'n> for InvariantChecker {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        node.check_invariants(self.0).map_err(|e| {
            e.context(format!(
                "Invariant for ExecutionPlan node '{}' failed",
                node.name()
            ))
        })?;
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::cmp::Ordering;
    use std::fmt::{self, Debug};
    use std::ops::{BitAnd, Not};

    use super::*;
    use crate::datasource::file_format::options::CsvReadOptions;
    use crate::datasource::MemTable;
    use crate::physical_plan::{
        expressions, DisplayAs, DisplayFormatType, PlanProperties,
        SendableRecordBatchStream,
    };
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test_util::{scan_empty, scan_empty_with_partitions};

    use crate::execution::session_state::SessionStateBuilder;
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use arrow_schema::SchemaRef;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{
        assert_contains, DFSchemaRef, TableReference, ToDFSchema as _,
    };
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_execution::TaskContext;
    use datafusion_expr::builder::subquery_alias;
    use datafusion_expr::{col, lit, LogicalPlanBuilder, UserDefinedLogicalNodeCore};
    use datafusion_functions_aggregate::count::count_all;
    use datafusion_functions_aggregate::expr_fn::sum;
    use datafusion_physical_expr::EquivalenceProperties;
    use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};

    fn make_session_state() -> SessionState {
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(4);
        let config = config.set_bool("datafusion.optimizer.skip_failed_rules", false);
        SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build()
    }

    async fn plan(logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let session_state = make_session_state();
        // optimize the logical plan
        let logical_plan = session_state.optimize(logical_plan)?;
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(&logical_plan, &session_state)
            .await
    }

    #[tokio::test]
    async fn test_all_operators() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c7").lt(lit(5_u8)))?
            .project(vec![col("c1"), col("c2")])?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .sort(vec![col("c1").sort(true, true)])?
            .limit(3, Some(10))?
            .build()?;

        let exec_plan = plan(&logical_plan).await?;

        // verify that the plan correctly casts u8 to i64
        // the cast from u8 to i64 for literal will be simplified, and get lit(int64(5))
        // the cast here is implicit so has CastOptions with safe=true
        let expected = r#"BinaryExpr { left: Column { name: "c7", index: 2 }, op: Lt, right: Literal { value: Int64(5), field: Field { name: "lit", data_type: Int64 } }, fail_on_overflow: false"#;

        assert_contains!(format!("{exec_plan:?}"), expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_cube_expr() -> Result<()> {
        let logical_plan = test_csv_scan().await?.build()?;

        let plan = plan(&logical_plan).await?;

        let exprs = vec![col("c1"), col("c2"), col("c3")];

        let physical_input_schema = plan.schema();
        let physical_input_schema = physical_input_schema.as_ref();
        let logical_input_schema = logical_plan.schema();
        let session_state = make_session_state();

        let cube = create_cube_physical_expr(
            &exprs,
            logical_input_schema,
            physical_input_schema,
            &session_state,
        );

        insta::assert_debug_snapshot!(cube, @r#"
        Ok(
            PhysicalGroupBy {
                expr: [
                    (
                        Column {
                            name: "c1",
                            index: 0,
                        },
                        "c1",
                    ),
                    (
                        Column {
                            name: "c2",
                            index: 1,
                        },
                        "c2",
                    ),
                    (
                        Column {
                            name: "c3",
                            index: 2,
                        },
                        "c3",
                    ),
                ],
                null_expr: [
                    (
                        Literal {
                            value: Utf8(NULL),
                            field: Field {
                                name: "lit",
                                data_type: Utf8,
                                nullable: true,
                            },
                        },
                        "c1",
                    ),
                    (
                        Literal {
                            value: Int64(NULL),
                            field: Field {
                                name: "lit",
                                data_type: Int64,
                                nullable: true,
                            },
                        },
                        "c2",
                    ),
                    (
                        Literal {
                            value: Int64(NULL),
                            field: Field {
                                name: "lit",
                                data_type: Int64,
                                nullable: true,
                            },
                        },
                        "c3",
                    ),
                ],
                groups: [
                    [
                        false,
                        false,
                        false,
                    ],
                    [
                        true,
                        false,
                        false,
                    ],
                    [
                        false,
                        true,
                        false,
                    ],
                    [
                        false,
                        false,
                        true,
                    ],
                    [
                        true,
                        true,
                        false,
                    ],
                    [
                        true,
                        false,
                        true,
                    ],
                    [
                        false,
                        true,
                        true,
                    ],
                    [
                        true,
                        true,
                        true,
                    ],
                ],
            },
        )
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_rollup_expr() -> Result<()> {
        let logical_plan = test_csv_scan().await?.build()?;

        let plan = plan(&logical_plan).await?;

        let exprs = vec![col("c1"), col("c2"), col("c3")];

        let physical_input_schema = plan.schema();
        let physical_input_schema = physical_input_schema.as_ref();
        let logical_input_schema = logical_plan.schema();
        let session_state = make_session_state();

        let rollup = create_rollup_physical_expr(
            &exprs,
            logical_input_schema,
            physical_input_schema,
            &session_state,
        );

        insta::assert_debug_snapshot!(rollup, @r#"
        Ok(
            PhysicalGroupBy {
                expr: [
                    (
                        Column {
                            name: "c1",
                            index: 0,
                        },
                        "c1",
                    ),
                    (
                        Column {
                            name: "c2",
                            index: 1,
                        },
                        "c2",
                    ),
                    (
                        Column {
                            name: "c3",
                            index: 2,
                        },
                        "c3",
                    ),
                ],
                null_expr: [
                    (
                        Literal {
                            value: Utf8(NULL),
                            field: Field {
                                name: "lit",
                                data_type: Utf8,
                                nullable: true,
                            },
                        },
                        "c1",
                    ),
                    (
                        Literal {
                            value: Int64(NULL),
                            field: Field {
                                name: "lit",
                                data_type: Int64,
                                nullable: true,
                            },
                        },
                        "c2",
                    ),
                    (
                        Literal {
                            value: Int64(NULL),
                            field: Field {
                                name: "lit",
                                data_type: Int64,
                                nullable: true,
                            },
                        },
                        "c3",
                    ),
                ],
                groups: [
                    [
                        true,
                        true,
                        true,
                    ],
                    [
                        false,
                        true,
                        true,
                    ],
                    [
                        false,
                        false,
                        true,
                    ],
                    [
                        false,
                        false,
                        false,
                    ],
                ],
            },
        )
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let dfschema = DFSchema::try_from(schema.clone())?;

        let planner = DefaultPhysicalPlanner::default();

        let expr = planner.create_physical_expr(
            &col("a").not(),
            &dfschema,
            &make_session_state(),
        )?;
        let expected = expressions::not(expressions::col("a", &schema)?)?;

        assert_eq!(format!("{expr:?}"), format!("{expected:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn test_with_csv_plan() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            .filter(col("c7").lt(col("c12")))?
            .limit(3, None)?
            .build()?;

        let plan = plan(&logical_plan).await?;

        // c12 is f64, c7 is u8 -> cast c7 to f64
        // the cast here is implicit so has CastOptions with safe=true
        let _expected = "predicate: BinaryExpr { left: TryCastExpr { expr: Column { name: \"c7\", index: 6 }, cast_type: Float64 }, op: Lt, right: Column { name: \"c12\", index: 11 } }";
        let plan_debug_str = format!("{plan:?}");
        assert!(plan_debug_str.contains("GlobalLimitExec"));
        assert!(plan_debug_str.contains("skip: 3"));
        Ok(())
    }

    #[tokio::test]
    async fn error_during_extension_planning() {
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            ErrorExtensionPlanner {},
        )]);

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        match planner
            .create_physical_plan(&logical_plan, &session_state)
            .await
        {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(e.to_string().contains("BOOM"),),
        }
    }

    #[tokio::test]
    async fn test_with_zero_offset_plan() -> Result<()> {
        let logical_plan = test_csv_scan().await?.limit(0, None)?.build()?;
        let plan = plan(&logical_plan).await?;
        assert!(!format!("{plan:?}").contains("limit="));
        Ok(())
    }

    #[tokio::test]
    async fn test_limit_with_partitions() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty_with_partitions(Some("test"), &schema, None, 2)?
            .limit(3, Some(5))?
            .build()?;
        let plan = plan(&logical_plan).await?;

        assert!(format!("{plan:?}").contains("GlobalLimitExec"));
        assert!(format!("{plan:?}").contains("skip: 3, fetch: Some(5)"));

        Ok(())
    }

    #[tokio::test]
    async fn errors() -> Result<()> {
        let bool_expr = col("c1").eq(col("c1"));
        let cases = vec![
            // utf8 = utf8
            col("c1").eq(col("c1")),
            // u8 AND u8
            col("c3").bitand(col("c3")),
            // utf8 = u8
            col("c1").eq(col("c3")),
            // bool AND bool
            bool_expr.clone().and(bool_expr),
        ];
        for case in cases {
            test_csv_scan().await?.project(vec![case.clone()]).unwrap();
        }
        Ok(())
    }

    #[tokio::test]
    async fn default_extension_planner() {
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::default();
        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        let plan = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await;

        let expected_error =
            "No installed planner was able to convert the custom node to an execution plan: NoOp";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{e}' did not contain expected error '{expected_error}'"
            ),
        }
    }

    #[tokio::test]
    async fn bad_extension_planner() {
        // Test that creating an execution plan whose schema doesn't
        // match the logical plan's schema generates an error.
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            BadExtensionPlanner {},
        )]);

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        let e = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await
            .expect_err("planning error")
            .strip_backtrace();

        insta::assert_snapshot!(e, @r#"Error during planning: Extension planner for NoOp created an ExecutionPlan with mismatched schema. LogicalPlan schema: DFSchema { inner: Schema { fields: [Field { name: "a", data_type: Int32 }], metadata: {} }, field_qualifiers: [None], functional_dependencies: FunctionalDependencies { deps: [] } }, ExecutionPlan schema: Schema { fields: [Field { name: "b", data_type: Int32 }], metadata: {} }"#);
    }

    #[tokio::test]
    async fn in_list_types() -> Result<()> {
        // expression: "a in ('a', 1)"
        let list = vec![lit("a"), lit(1i64)];
        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c1").in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan).await?;
        // verify that the plan correctly adds cast from Int64(1) to Utf8, and the const will be evaluated.

        let expected = r#"expr: BinaryExpr { left: BinaryExpr { left: Column { name: "c1", index: 0 }, op: Eq, right: Literal { value: Utf8("a"), field: Field { name: "lit", data_type: Utf8 } }, fail_on_overflow: false }"#;

        assert_contains!(format!("{execution_plan:?}"), expected);

        Ok(())
    }

    #[tokio::test]
    async fn in_list_types_struct_literal() -> Result<()> {
        // expression: "a in (struct::null, 'a')"
        let list = vec![struct_literal(), lit("a")];

        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c12").lt_eq(lit(0.025)).in_list(list, false)])?
            .build()?;
        let e = plan(&logical_plan).await.unwrap_err().to_string();

        assert_contains!(
            &e,
            r#"Error during planning: Can not find compatible types to compare Boolean with [Struct("foo": non-null Boolean), Utf8]"#
        );

        Ok(())
    }

    /// Return a `null` literal representing a struct type like: `{ a: bool }`
    fn struct_literal() -> Expr {
        let struct_literal = ScalarValue::try_from(DataType::Struct(
            vec![Field::new("foo", DataType::Boolean, false)].into(),
        ))
        .unwrap();

        lit(struct_literal)
    }

    #[tokio::test]
    async fn hash_agg_input_schema() -> Result<()> {
        let logical_plan = test_csv_scan_with_name("aggregate_test_100")
            .await?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("hash aggregate");
        assert_eq!(
            "sum(aggregate_test_100.c2)",
            final_hash_agg.schema().field(1).name()
        );
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c2", final_hash_agg.input_schema().field(1).name());

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_grouping_set_input_schema() -> Result<()> {
        let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("c1")],
            vec![col("c2")],
            vec![col("c1"), col("c2")],
        ]));
        let logical_plan = test_csv_scan_with_name("aggregate_test_100")
            .await?
            .aggregate(vec![grouping_set_expr], vec![sum(col("c3"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("hash aggregate");
        assert_eq!(
            "sum(aggregate_test_100.c3)",
            final_hash_agg.schema().field(3).name()
        );
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c3", final_hash_agg.input_schema().field(2).name());

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_group_by_partitioned() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_group_by_partitioned_on_dicts() -> Result<()> {
        let dict_array: DictionaryArray<Int32Type> =
            vec!["A", "B", "A", "A", "C", "A"].into_iter().collect();
        let val_array: Int32Array = vec![1, 2, 2, 4, 1, 1].into();

        let batch = RecordBatch::try_from_iter(vec![
            ("d1", Arc::new(dict_array) as ArrayRef),
            ("d2", Arc::new(val_array) as ArrayRef),
        ])
        .unwrap();

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        let ctx = SessionContext::new();

        let logical_plan = LogicalPlanBuilder::from(
            ctx.read_table(Arc::new(table))?.into_optimized_plan()?,
        )
        .aggregate(vec![col("d1")], vec![sum(col("d2"))])?
        .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));
        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_grouping_set_by_partitioned() -> Result<()> {
        let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("c1")],
            vec![col("c2")],
            vec![col("c1"), col("c2")],
        ]));
        let logical_plan = test_csv_scan()
            .await?
            .aggregate(vec![grouping_set_expr], vec![sum(col("c3"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_with_alias() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
        ]));

        let logical_plan = scan_empty(None, schema.as_ref(), None)?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .project(vec![col("c1"), sum(col("c2")).alias("total_salary")])?
            .build()?;

        let physical_plan = plan(&logical_plan).await?;
        assert_eq!("c1", physical_plan.schema().field(0).name().as_str());
        assert_eq!(
            "total_salary",
            physical_plan.schema().field(1).name().as_str()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_count_all_with_alias() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
        ]));

        let logical_plan = scan_empty(None, schema.as_ref(), None)?
            .aggregate(Vec::<Expr>::new(), vec![count_all().alias("total_rows")])?
            .build()?;

        let physical_plan = plan(&logical_plan).await?;
        assert_eq!(
            "total_rows",
            physical_plan.schema().field(0).name().as_str()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_explain() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .explain(true, false)
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();
        if let Some(plan) = plan.as_any().downcast_ref::<ExplainExec>() {
            let stringified_plans = plan.stringified_plans();
            assert!(stringified_plans.len() >= 4);
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::FinalLogicalPlan)));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::InitialPhysicalPlan)));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::OptimizedPhysicalPlan { .. })));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::FinalPhysicalPlan)));
        } else {
            panic!(
                "Plan was not an explain plan: {}",
                displayable(plan.as_ref()).indent(true)
            );
        }
    }

    #[tokio::test]
    async fn test_explain_indent_err() {
        let planner = DefaultPhysicalPlanner::default();
        let ctx = SessionContext::new();
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let plan = Arc::new(
            scan_empty(Some("employee"), &schema, None)
                .unwrap()
                .explain(true, false)
                .unwrap()
                .build()
                .unwrap(),
        );

        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]));

        // Create invalid indentation in the plan
        let stringified_plans =
            vec![StringifiedPlan::new(PlanType::FinalLogicalPlan, "Test Err")];

        let explain = Explain {
            verbose: false,
            explain_format: ExplainFormat::Indent,
            plan,
            stringified_plans,
            schema: schema.to_dfschema_ref().unwrap(),
            logical_optimization_succeeded: false,
        };
        let plan = planner
            .handle_explain(&explain, &ctx.state())
            .await
            .unwrap();
        if let Some(plan) = plan.as_any().downcast_ref::<ExplainExec>() {
            let stringified_plans = plan.stringified_plans();
            assert_eq!(stringified_plans.len(), 1);
            assert_eq!(stringified_plans[0].plan.as_str(), "Test Err");
        } else {
            panic!(
                "Plan was not an explain plan: {}",
                displayable(plan.as_ref()).indent(true)
            );
        }
    }

    struct ErrorExtensionPlanner {}

    #[async_trait]
    impl ExtensionPlanner for ErrorExtensionPlanner {
        /// Create a physical plan for an extension node
        async fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            _node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session_state: &SessionState,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            internal_err!("BOOM")
        }
    }
    /// An example extension node that doesn't do anything
    #[derive(PartialEq, Eq, Hash)]
    struct NoOpExtensionNode {
        schema: DFSchemaRef,
    }

    impl Default for NoOpExtensionNode {
        fn default() -> Self {
            Self {
                schema: DFSchemaRef::new(
                    DFSchema::from_unqualified_fields(
                        vec![Field::new("a", DataType::Int32, false)].into(),
                        HashMap::new(),
                    )
                    .unwrap(),
                ),
            }
        }
    }

    impl Debug for NoOpExtensionNode {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }
    }

    // Implementation needed for `UserDefinedLogicalNodeCore`, since the only field is
    // a schema, we can't derive `PartialOrd`, and we can't compare these.
    impl PartialOrd for NoOpExtensionNode {
        fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
            None
        }
    }

    impl UserDefinedLogicalNodeCore for NoOpExtensionNode {
        fn name(&self) -> &str {
            "NoOp"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![]
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }

        fn with_exprs_and_inputs(
            &self,
            _exprs: Vec<Expr>,
            _inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            unimplemented!("NoOp");
        }

        fn supports_limit_pushdown(&self) -> bool {
            false // Disallow limit push-down by default
        }
    }

    #[derive(Debug)]
    struct NoOpExecutionPlan {
        cache: PlanProperties,
    }

    impl NoOpExecutionPlan {
        fn new(schema: SchemaRef) -> Self {
            let cache = Self::compute_properties(schema);
            Self { cache }
        }

        /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
        fn compute_properties(schema: SchemaRef) -> PlanProperties {
            PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
        }
    }

    impl DisplayAs for NoOpExecutionPlan {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "NoOpExecutionPlan")
                }
                DisplayFormatType::TreeRender => {
                    // TODO: collect info
                    write!(f, "")
                }
            }
        }
    }

    impl ExecutionPlan for NoOpExecutionPlan {
        fn name(&self) -> &'static str {
            "NoOpExecutionPlan"
        }

        /// Return a reference to Any that can be used for downcasting
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("NoOpExecutionPlan::with_new_children");
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!("NoOpExecutionPlan::execute");
        }
    }

    //  Produces an execution plan where the schema is mismatched from
    //  the logical plan node.
    struct BadExtensionPlanner {}

    #[async_trait]
    impl ExtensionPlanner for BadExtensionPlanner {
        /// Create a physical plan for an extension node
        async fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            _node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session_state: &SessionState,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(Some(Arc::new(NoOpExecutionPlan::new(SchemaRef::new(
                Schema::new(vec![Field::new("b", DataType::Int32, false)]),
            )))))
        }
    }

    async fn test_csv_scan_with_name(name: &str) -> Result<LogicalPlanBuilder> {
        let ctx = SessionContext::new();
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{testdata}/csv/aggregate_test_100.csv");
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan =
            match ctx.read_csv(path, options).await?.into_optimized_plan()? {
                LogicalPlan::TableScan(ref scan) => {
                    let mut scan = scan.clone();
                    let table_reference = TableReference::from(name);
                    scan.table_name = table_reference;
                    let new_schema = scan
                        .projected_schema
                        .as_ref()
                        .clone()
                        .replace_qualifier(name.to_string());
                    scan.projected_schema = Arc::new(new_schema);
                    LogicalPlan::TableScan(scan)
                }
                _ => unimplemented!(),
            };
        Ok(LogicalPlanBuilder::from(logical_plan))
    }

    async fn test_csv_scan() -> Result<LogicalPlanBuilder> {
        let ctx = SessionContext::new();
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{testdata}/csv/aggregate_test_100.csv");
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        Ok(LogicalPlanBuilder::from(
            ctx.read_csv(path, options).await?.into_optimized_plan()?,
        ))
    }

    #[tokio::test]
    async fn test_display_plan_in_graphviz_format() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .project(vec![col("id") + lit(2)])
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();

        let expected_graph = r#"
// Begin DataFusion GraphViz Plan,
// display it online here: https://dreampuf.github.io/GraphvizOnline

digraph {
    1[shape=box label="ProjectionExec: expr=[id@0 + 2 as employee.id + Int32(2)]", tooltip=""]
    2[shape=box label="EmptyExec", tooltip=""]
    1 -> 2 [arrowhead=none, arrowtail=normal, dir=back]
}
// End DataFusion GraphViz Plan
"#;

        let generated_graph = format!("{}", displayable(&*plan).graphviz());

        assert_eq!(expected_graph, generated_graph);
    }

    #[tokio::test]
    async fn test_display_graphviz_with_statistics() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .project(vec![col("id") + lit(2)])
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();

        let expected_tooltip = ", tooltip=\"statistics=[";

        let generated_graph = format!(
            "{}",
            displayable(&*plan).set_show_statistics(true).graphviz()
        );

        assert_contains!(generated_graph, expected_tooltip);
    }

    /// Extension Node which passes invariant checks
    #[derive(Debug)]
    struct OkExtensionNode(Vec<Arc<dyn ExecutionPlan>>);
    impl ExecutionPlan for OkExtensionNode {
        fn name(&self) -> &str {
            "always ok"
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self(children)))
        }
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }
        fn as_any(&self) -> &dyn Any {
            unimplemented!()
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.0.iter().collect::<Vec<_>>()
        }
        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }
    }
    impl DisplayAs for OkExtensionNode {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.name())
        }
    }

    /// Extension Node which fails the [`OptimizationInvariantChecker`].
    #[derive(Debug)]
    struct InvariantFailsExtensionNode;
    impl ExecutionPlan for InvariantFailsExtensionNode {
        fn name(&self) -> &str {
            "InvariantFailsExtensionNode"
        }
        fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
            match check {
                InvariantLevel::Always => plan_err!("extension node failed it's user-defined always-invariant check"),
                InvariantLevel::Executable => panic!("the OptimizationInvariantChecker should not be checking for executableness"),
            }
        }
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }
        fn as_any(&self) -> &dyn Any {
            unimplemented!()
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }
        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }
    }
    impl DisplayAs for InvariantFailsExtensionNode {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.name())
        }
    }

    /// Extension Optimizer rule that requires the schema check
    #[derive(Debug)]
    struct OptimizerRuleWithSchemaCheck;
    impl PhysicalOptimizerRule for OptimizerRuleWithSchemaCheck {
        fn optimize(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            _config: &ConfigOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(plan)
        }
        fn name(&self) -> &str {
            "OptimizerRuleWithSchemaCheck"
        }
        fn schema_check(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_optimization_invariant_checker() -> Result<()> {
        let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            Arc::new(OptimizerRuleWithSchemaCheck);

        // ok plan
        let ok_node: Arc<dyn ExecutionPlan> = Arc::new(OkExtensionNode(vec![]));
        let child = Arc::clone(&ok_node);
        let ok_plan = Arc::clone(&ok_node).with_new_children(vec![
            Arc::clone(&child).with_new_children(vec![Arc::clone(&child)])?,
            Arc::clone(&child),
        ])?;

        // Test: check should pass with same schema
        let equal_schema = ok_plan.schema();
        OptimizationInvariantChecker::new(&rule).check(&ok_plan, &equal_schema)?;

        // Test: should fail with schema changed
        let different_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));
        let expected_err = OptimizationInvariantChecker::new(&rule)
            .check(&ok_plan, &different_schema)
            .unwrap_err();
        assert!(expected_err.to_string().contains("PhysicalOptimizer rule 'OptimizerRuleWithSchemaCheck' failed. Schema mismatch. Expected original schema"));

        // Test: should fail when extension node fails it's own invariant check
        let failing_node: Arc<dyn ExecutionPlan> = Arc::new(InvariantFailsExtensionNode);
        let expected_err = OptimizationInvariantChecker::new(&rule)
            .check(&failing_node, &ok_plan.schema())
            .unwrap_err();
        assert!(expected_err
            .to_string()
            .contains("extension node failed it's user-defined always-invariant check"));

        // Test: should fail when descendent extension node fails
        let failing_node: Arc<dyn ExecutionPlan> = Arc::new(InvariantFailsExtensionNode);
        let invalid_plan = ok_node.with_new_children(vec![
            Arc::clone(&child).with_new_children(vec![Arc::clone(&failing_node)])?,
            Arc::clone(&child),
        ])?;
        let expected_err = OptimizationInvariantChecker::new(&rule)
            .check(&invalid_plan, &ok_plan.schema())
            .unwrap_err();
        assert!(expected_err
            .to_string()
            .contains("extension node failed it's user-defined always-invariant check"));

        Ok(())
    }

    /// Extension Node which fails the [`InvariantChecker`]
    /// if, and only if, [`InvariantLevel::Executable`]
    #[derive(Debug)]
    struct ExecutableInvariantFails;
    impl ExecutionPlan for ExecutableInvariantFails {
        fn name(&self) -> &str {
            "ExecutableInvariantFails"
        }
        fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
            match check {
                InvariantLevel::Always => Ok(()),
                InvariantLevel::Executable => plan_err!(
                    "extension node failed it's user-defined executable-invariant check"
                ),
            }
        }
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }
        fn as_any(&self) -> &dyn Any {
            unimplemented!()
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }
    }
    impl DisplayAs for ExecutableInvariantFails {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.name())
        }
    }

    #[test]
    fn test_invariant_checker_levels() -> Result<()> {
        // plan that passes the always-invariant, but fails the executable check
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ExecutableInvariantFails);

        // Test: check should pass with less stringent Always check
        InvariantChecker(InvariantLevel::Always).check(&plan)?;

        // Test: should fail the executable check
        let expected_err = InvariantChecker(InvariantLevel::Executable)
            .check(&plan)
            .unwrap_err();
        assert!(expected_err.to_string().contains(
            "extension node failed it's user-defined executable-invariant check"
        ));

        // Test: should fail when descendent extension node fails
        let failing_node: Arc<dyn ExecutionPlan> = Arc::new(ExecutableInvariantFails);
        let ok_node: Arc<dyn ExecutionPlan> = Arc::new(OkExtensionNode(vec![]));
        let child = Arc::clone(&ok_node);
        let plan = ok_node.with_new_children(vec![
            Arc::clone(&child).with_new_children(vec![Arc::clone(&failing_node)])?,
            Arc::clone(&child),
        ])?;
        let expected_err = InvariantChecker(InvariantLevel::Executable)
            .check(&plan)
            .unwrap_err();
        assert!(expected_err.to_string().contains(
            "extension node failed it's user-defined executable-invariant check"
        ));

        Ok(())
    }

    // Reproducer for DataFusion issue #17405:
    //
    // The following SQL is semantically invalid. Notably, the `SELECT left_table.a, right_table.a`
    // clause is missing from the explicit logical plan:
    //
    // SELECT a FROM (
    //       -- SELECT left_table.a, right_table.a
    //       FROM left_table
    //       FULL JOIN right_table ON left_table.a = right_table.a
    // ) AS alias
    // GROUP BY a;
    //
    // As a result, the variables within `alias` subquery are not properly distinguished, which
    // leads to a bug for logical and physical planning.
    //
    // The fix is to implicitly insert a Projection node to represent the missing SELECT clause to
    // ensure each field is correctly aliased to a unique name when the SubqueryAlias node is added.
    #[tokio::test]
    async fn subquery_alias_confusing_the_optimizer() -> Result<()> {
        let state = make_session_state();

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let schema = Arc::new(schema);

        let table = MemTable::try_new(schema.clone(), vec![vec![]])?;
        let table = Arc::new(table);

        let source = DefaultTableSource::new(table);
        let source = Arc::new(source);

        let left = LogicalPlanBuilder::scan("left", source.clone(), None)?;
        let right = LogicalPlanBuilder::scan("right", source, None)?.build()?;

        let join_keys = (
            vec![datafusion_common::Column::new(Some("left"), "a")],
            vec![datafusion_common::Column::new(Some("right"), "a")],
        );

        let join = left.join(right, JoinType::Full, join_keys, None)?.build()?;

        let alias = subquery_alias(join, "alias")?;

        let planner = DefaultPhysicalPlanner::default();

        let logical_plan = LogicalPlanBuilder::new(alias)
            .aggregate(vec![col("a:1")], Vec::<Expr>::new())?
            .build()?;
        let _physical_plan = planner.create_physical_plan(&logical_plan, &state).await?;

        let optimized_logical_plan = state.optimize(&logical_plan)?;
        let _optimized_physical_plan = planner
            .create_physical_plan(&optimized_logical_plan, &state)
            .await?;

        Ok(())
    }
}
