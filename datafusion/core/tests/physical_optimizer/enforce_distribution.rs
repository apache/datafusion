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

use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use crate::physical_optimizer::test_utils::parquet_exec_with_sort;
use crate::physical_optimizer::test_utils::{
    check_integrity, coalesce_partitions_exec, repartition_exec, schema,
    sort_merge_join_exec, sort_preserving_merge_exec,
};

use arrow::compute::SortOptions;
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{CsvSource, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::ScalarValue;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::{
    expressions::binary, expressions::lit, LexOrdering, PhysicalSortExpr,
};
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use datafusion_physical_optimizer::enforce_distribution::*;
use datafusion_physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::execution_plan::ExecutionPlan;
use datafusion_physical_plan::expressions::col;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::JoinOn;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_physical_plan::PlanProperties;
use datafusion_physical_plan::{
    get_plan_string, DisplayAs, DisplayFormatType, Statistics,
};

/// Models operators like BoundedWindowExec that require an input
/// ordering but is easy to construct
#[derive(Debug)]
struct SortRequiredExec {
    input: Arc<dyn ExecutionPlan>,
    expr: LexOrdering,
    cache: PlanProperties,
}

impl SortRequiredExec {
    fn new_with_requirement(
        input: Arc<dyn ExecutionPlan>,
        requirement: LexOrdering,
    ) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            expr: requirement,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            input.equivalence_properties().clone(), // Equivalence Properties
            input.output_partitioning().clone(),    // Output Partitioning
            input.pipeline_behavior(),              // Pipeline Behavior
            input.boundedness(),                    // Boundedness
        )
    }
}

impl DisplayAs for SortRequiredExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SortRequiredExec: [{}]", self.expr)
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for SortRequiredExec {
    fn name(&self) -> &'static str {
        "SortRequiredExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    // model that it requires the output ordering of its input
    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        if self.expr.is_empty() {
            vec![None]
        } else {
            vec![Some(LexRequirement::from(self.expr.clone()))]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        let child = children.pop().unwrap();
        Ok(Arc::new(Self::new_with_requirement(
            child,
            self.expr.clone(),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<datafusion_physical_plan::SendableRecordBatchStream> {
        unreachable!();
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

fn parquet_exec() -> Arc<DataSourceExec> {
    parquet_exec_with_sort(vec![])
}

fn parquet_exec_multiple() -> Arc<DataSourceExec> {
    parquet_exec_multiple_sorted(vec![])
}

/// Created a sorted parquet exec with multiple files
fn parquet_exec_multiple_sorted(
    output_ordering: Vec<LexOrdering>,
) -> Arc<DataSourceExec> {
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema(),
        Arc::new(ParquetSource::default()),
    )
    .with_file_groups(vec![
        FileGroup::new(vec![PartitionedFile::new("x".to_string(), 100)]),
        FileGroup::new(vec![PartitionedFile::new("y".to_string(), 100)]),
    ])
    .with_output_ordering(output_ordering)
    .build();

    DataSourceExec::from_data_source(config)
}

fn csv_exec() -> Arc<DataSourceExec> {
    csv_exec_with_sort(vec![])
}

fn csv_exec_with_sort(output_ordering: Vec<LexOrdering>) -> Arc<DataSourceExec> {
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema(),
        Arc::new(CsvSource::new(false, b',', b'"')),
    )
    .with_file(PartitionedFile::new("x".to_string(), 100))
    .with_output_ordering(output_ordering)
    .build();

    DataSourceExec::from_data_source(config)
}

fn csv_exec_multiple() -> Arc<DataSourceExec> {
    csv_exec_multiple_sorted(vec![])
}

// Created a sorted parquet exec with multiple files
fn csv_exec_multiple_sorted(output_ordering: Vec<LexOrdering>) -> Arc<DataSourceExec> {
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema(),
        Arc::new(CsvSource::new(false, b',', b'"')),
    )
    .with_file_groups(vec![
        FileGroup::new(vec![PartitionedFile::new("x".to_string(), 100)]),
        FileGroup::new(vec![PartitionedFile::new("y".to_string(), 100)]),
    ])
    .with_output_ordering(output_ordering)
    .build();

    DataSourceExec::from_data_source(config)
}

fn projection_exec_with_alias(
    input: Arc<dyn ExecutionPlan>,
    alias_pairs: Vec<(String, String)>,
) -> Arc<dyn ExecutionPlan> {
    let mut exprs = vec![];
    for (column, alias) in alias_pairs.iter() {
        exprs.push((col(column, &input.schema()).unwrap(), alias.to_string()));
    }
    Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
}

fn aggregate_exec_with_alias(
    input: Arc<dyn ExecutionPlan>,
    alias_pairs: Vec<(String, String)>,
) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let mut group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    for (column, alias) in alias_pairs.iter() {
        group_by_expr.push((col(column, &input.schema()).unwrap(), alias.to_string()));
    }
    let group_by = PhysicalGroupBy::new_single(group_by_expr.clone());

    let final_group_by_expr = group_by_expr
        .iter()
        .enumerate()
        .map(|(index, (_col, name))| {
            (
                Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                name.clone(),
            )
        })
        .collect::<Vec<_>>();
    let final_grouping = PhysicalGroupBy::new_single(final_group_by_expr);

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            final_grouping,
            vec![],
            vec![],
            Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Partial,
                    group_by,
                    vec![],
                    vec![],
                    input,
                    schema.clone(),
                )
                .unwrap(),
            ),
            schema,
        )
        .unwrap(),
    )
}

fn hash_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_on: &JoinOn,
    join_type: &JoinType,
) -> Arc<dyn ExecutionPlan> {
    crate::physical_optimizer::test_utils::hash_join_exec(
        left,
        right,
        join_on.clone(),
        None,
        join_type,
    )
    .unwrap()
}

fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let predicate = Arc::new(BinaryExpr::new(
        col("c", &schema()).unwrap(),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    ));
    Arc::new(FilterExec::try_new(predicate, input).unwrap())
}

fn sort_exec(
    sort_exprs: LexOrdering,
    input: Arc<dyn ExecutionPlan>,
    preserve_partitioning: bool,
) -> Arc<dyn ExecutionPlan> {
    let new_sort = SortExec::new(sort_exprs, input)
        .with_preserve_partitioning(preserve_partitioning);
    Arc::new(new_sort)
}

fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(
        Arc::new(LocalLimitExec::new(input, 100)),
        0,
        Some(100),
    ))
}

fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
    Arc::new(UnionExec::new(input))
}

fn sort_required_exec_with_req(
    input: Arc<dyn ExecutionPlan>,
    sort_exprs: LexOrdering,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortRequiredExec::new_with_requirement(input, sort_exprs))
}

fn ensure_distribution_helper(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
    prefer_existing_sort: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let distribution_context = DistributionContext::new_default(plan);
    let mut config = ConfigOptions::new();
    config.execution.target_partitions = target_partitions;
    config.optimizer.enable_round_robin_repartition = true;
    config.optimizer.repartition_file_scans = false;
    config.optimizer.repartition_file_min_size = 1024;
    config.optimizer.prefer_existing_sort = prefer_existing_sort;
    ensure_distribution(distribution_context, &config).map(|item| item.data.plan)
}

/// Test whether plan matches with expected plan
macro_rules! plans_matches_expected {
    ($EXPECTED_LINES: expr, $PLAN: expr) => {
        let physical_plan = $PLAN;
        let actual = get_plan_string(&physical_plan);

        let expected_plan_lines: Vec<&str> = $EXPECTED_LINES
            .iter().map(|s| *s).collect();

        assert_eq!(
            expected_plan_lines, actual,
            "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
        );
    }
}

fn test_suite_default_config_options() -> ConfigOptions {
    let mut config = ConfigOptions::new();

    // By default, will not repartition / resort data if it is already sorted.
    config.optimizer.prefer_existing_sort = false;

    // By default, will attempt to convert Union to Interleave.
    config.optimizer.prefer_existing_union = false;

    // By default, will not repartition file scans.
    config.optimizer.repartition_file_scans = false;
    config.optimizer.repartition_file_min_size = 1024;

    // By default, set query execution concurrency to 10.
    config.execution.target_partitions = 10;

    // Use a small batch size, to trigger RoundRobin in tests
    config.execution.batch_size = 1;

    config
}

#[derive(PartialEq, Clone)]
enum Run {
    Distribution,
    Sorting,
}

/// Standard sets of the series of optimizer runs:
const DISTRIB_DISTRIB_SORT: [Run; 3] =
    [Run::Distribution, Run::Distribution, Run::Sorting];
const SORT_DISTRIB_DISTRIB: [Run; 3] =
    [Run::Sorting, Run::Distribution, Run::Distribution];

#[derive(Clone)]
struct TestConfig {
    config: ConfigOptions,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            config: test_suite_default_config_options(),
        }
    }
}

impl TestConfig {
    /// If preferred, will not repartition / resort data if it is already sorted.
    fn with_prefer_existing_sort(mut self) -> Self {
        self.config.optimizer.prefer_existing_sort = true;
        self
    }

    /// If preferred, will not attempt to convert Union to Interleave.
    fn with_prefer_existing_union(mut self) -> Self {
        self.config.optimizer.prefer_existing_union = true;
        self
    }

    /// If preferred, will repartition file scans.
    /// Accepts a minimum file size to repartition.
    fn with_prefer_repartition_file_scans(mut self, file_min_size: usize) -> Self {
        self.config.optimizer.repartition_file_scans = true;
        self.config.optimizer.repartition_file_min_size = file_min_size;
        self
    }

    /// Set the preferred target partitions for query execution concurrency.
    fn with_query_execution_partitions(mut self, target_partitions: usize) -> Self {
        self.config.execution.target_partitions = target_partitions;
        self
    }

    /// Perform a series of runs using the current [`TestConfig`],
    /// assert the expected plan result,
    /// and return the result plan (for potentional subsequent runs).
    fn run(
        &self,
        expected_lines: &[&str],
        plan: Arc<dyn ExecutionPlan>,
        optimizers_to_run: &[Run],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let expected_lines: Vec<&str> = expected_lines.to_vec();

        // Add the ancillary output requirements operator at the start:
        let optimizer = OutputRequirements::new_add_mode();
        let mut optimized = optimizer.optimize(plan.clone(), &self.config)?;

        // This file has 2 rules that use tree node, apply these rules to original plan consecutively
        // After these operations tree nodes should be in a consistent state.
        // This code block makes sure that these rules doesn't violate tree node integrity.
        {
            let adjusted = if self.config.optimizer.top_down_join_key_reordering {
                // Run adjust_input_keys_ordering rule
                let plan_requirements =
                    PlanWithKeyRequirements::new_default(plan.clone());
                let adjusted = plan_requirements
                    .transform_down(adjust_input_keys_ordering)
                    .data()
                    .and_then(check_integrity)?;
                // TODO: End state payloads will be checked here.
                adjusted.plan
            } else {
                // Run reorder_join_keys_to_inputs rule
                plan.clone()
                    .transform_up(|plan| {
                        Ok(Transformed::yes(reorder_join_keys_to_inputs(plan)?))
                    })
                    .data()?
            };

            // Then run ensure_distribution rule
            DistributionContext::new_default(adjusted)
                .transform_up(|distribution_context| {
                    ensure_distribution(distribution_context, &self.config)
                })
                .data()
                .and_then(check_integrity)?;
            // TODO: End state payloads will be checked here.
        }

        for run in optimizers_to_run {
            optimized = match run {
                Run::Distribution => {
                    let optimizer = EnforceDistribution::new();
                    optimizer.optimize(optimized, &self.config)?
                }
                Run::Sorting => {
                    let optimizer = EnforceSorting::new();
                    optimizer.optimize(optimized, &self.config)?
                }
            };
        }

        // Remove the ancillary output requirements operator when done:
        let optimizer = OutputRequirements::new_remove_mode();
        let optimized = optimizer.optimize(optimized, &self.config)?;

        // Now format correctly
        let actual_lines = get_plan_string(&optimized);

        assert_eq!(
            &expected_lines, &actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );

        Ok(optimized)
    }
}

macro_rules! assert_plan_txt {
    ($EXPECTED_LINES: expr, $PLAN: expr) => {
        let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();
        // Now format correctly
        let actual_lines = get_plan_string(&$PLAN);

        assert_eq!(
            &expected_lines, &actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

#[test]
fn multi_hash_joins() -> Result<()> {
    let left = parquet_exec();
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a1".to_string()),
        ("b".to_string(), "b1".to_string()),
        ("c".to_string(), "c1".to_string()),
        ("d".to_string(), "d1".to_string()),
        ("e".to_string(), "e1".to_string()),
    ];
    let right = projection_exec_with_alias(parquet_exec(), alias_pairs);
    let join_types = vec![
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::LeftMark,
        JoinType::RightSemi,
        JoinType::RightAnti,
    ];

    // Join on (a == b1)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
    )];

    for join_type in join_types {
        let join = hash_join_exec(left.clone(), right.clone(), &join_on, &join_type);
        let join_plan = |shift| -> String {
            format!("{}HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(a@0, b1@1)]", " ".repeat(shift))
        };
        let join_plan_indent2 = join_plan(2);
        let join_plan_indent4 = join_plan(4);

        match join_type {
            JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark => {
                // Join on (a == c)
                let top_join_on = vec![(
                    Arc::new(Column::new_with_schema("a", &join.schema()).unwrap()) as _,
                    Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
                )];
                let top_join = hash_join_exec(
                    join.clone(),
                    parquet_exec(),
                    &top_join_on,
                    &join_type,
                );
                let top_join_plan =
                    format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(a@0, c@2)]");

                let expected = match join_type {
                    // Should include 3 RepartitionExecs
                    JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => vec![
                        top_join_plan.as_str(),
                        &join_plan_indent2,
                        "    RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "    RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    ],
                    // Should include 4 RepartitionExecs
                    _ => vec![
                        top_join_plan.as_str(),
                        "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        &join_plan_indent4,
                        "      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "      RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    ],
                };

                let test_config = TestConfig::default();
                test_config.run(&expected, top_join.clone(), &DISTRIB_DISTRIB_SORT)?;
                test_config.run(&expected, top_join, &SORT_DISTRIB_DISTRIB)?;
            }
            JoinType::RightSemi | JoinType::RightAnti => {}
        }

        match join_type {
            JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::RightSemi
            | JoinType::RightAnti => {
                // This time we use (b1 == c) for top join
                // Join on (b1 == c)
                let top_join_on = vec![(
                    Arc::new(Column::new_with_schema("b1", &join.schema()).unwrap()) as _,
                    Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
                )];

                let top_join =
                    hash_join_exec(join, parquet_exec(), &top_join_on, &join_type);
                let top_join_plan = match join_type {
                    JoinType::RightSemi | JoinType::RightAnti =>
                        format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(b1@1, c@2)]"),
                    _ =>
                        format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(b1@6, c@2)]"),
                };

                let expected = match join_type {
                    // Should include 3 RepartitionExecs
                    JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti =>
                        vec![
                            top_join_plan.as_str(),
                            &join_plan_indent2,
                            "    RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                            "    RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                            "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        ],
                    // Should include 4 RepartitionExecs
                    _ =>
                        vec![
                            top_join_plan.as_str(),
                            "  RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10",
                            &join_plan_indent4,
                            "      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                            "      RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                            "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        ],
                };

                let test_config = TestConfig::default();
                test_config.run(&expected, top_join.clone(), &DISTRIB_DISTRIB_SORT)?;
                test_config.run(&expected, top_join, &SORT_DISTRIB_DISTRIB)?;
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {}
        }
    }

    Ok(())
}

#[test]
fn multi_joins_after_alias() -> Result<()> {
    let left = parquet_exec();
    let right = parquet_exec();

    // Join on (a == b)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("b", &schema()).unwrap()) as _,
    )];
    let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

    // Projection(a as a1, a as a2)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a1".to_string()),
        ("a".to_string(), "a2".to_string()),
    ];
    let projection = projection_exec_with_alias(join, alias_pairs);

    // Join on (a1 == c)
    let top_join_on = vec![(
        Arc::new(Column::new_with_schema("a1", &projection.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
    )];

    let top_join = hash_join_exec(
        projection.clone(),
        right.clone(),
        &top_join_on,
        &JoinType::Inner,
    );

    // Output partition need to respect the Alias and should not introduce additional RepartitionExec
    let expected = &[
        "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a1@0, c@2)]",
        "  ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
        "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
        "      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "      RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, top_join.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, top_join, &SORT_DISTRIB_DISTRIB)?;

    // Join on (a2 == c)
    let top_join_on = vec![(
        Arc::new(Column::new_with_schema("a2", &projection.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
    )];

    let top_join = hash_join_exec(projection, right, &top_join_on, &JoinType::Inner);

    // Output partition need to respect the Alias and should not introduce additional RepartitionExec
    let expected = &[
        "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a2@1, c@2)]",
        "  ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
        "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
        "      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "      RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, top_join.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, top_join, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn multi_joins_after_multi_alias() -> Result<()> {
    let left = parquet_exec();
    let right = parquet_exec();

    // Join on (a == b)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("b", &schema()).unwrap()) as _,
    )];

    let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

    // Projection(c as c1)
    let alias_pairs: Vec<(String, String)> = vec![("c".to_string(), "c1".to_string())];
    let projection = projection_exec_with_alias(join, alias_pairs);

    // Projection(c1 as a)
    let alias_pairs: Vec<(String, String)> = vec![("c1".to_string(), "a".to_string())];
    let projection2 = projection_exec_with_alias(projection, alias_pairs);

    // Join on (a == c)
    let top_join_on = vec![(
        Arc::new(Column::new_with_schema("a", &projection2.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
    )];

    let top_join = hash_join_exec(projection2, right, &top_join_on, &JoinType::Inner);

    // The Column 'a' has different meaning now after the two Projections
    // The original Output partition can not satisfy the Join requirements and need to add an additional RepartitionExec
    let expected = &[
        "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, c@2)]",
        "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "    ProjectionExec: expr=[c1@0 as a]",
        "      ProjectionExec: expr=[c@2 as c1]",
        "        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
        "          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "          RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
        "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, top_join.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, top_join, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn join_after_agg_alias() -> Result<()> {
    // group by (a as a1)
    let left = aggregate_exec_with_alias(
        parquet_exec(),
        vec![("a".to_string(), "a1".to_string())],
    );
    // group by (a as a2)
    let right = aggregate_exec_with_alias(
        parquet_exec(),
        vec![("a".to_string(), "a2".to_string())],
    );

    // Join on (a1 == a2)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("a1", &left.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("a2", &right.schema()).unwrap()) as _,
    )];
    let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

    // Only two RepartitionExecs added
    let expected = &[
        "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a1@0, a2@0)]",
        "  AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
        "    RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
        "      AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
        "    RepartitionExec: partitioning=Hash([a2@0], 10), input_partitions=10",
        "      AggregateExec: mode=Partial, gby=[a@0 as a2], aggr=[]",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, join.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, join, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn hash_join_key_ordering() -> Result<()> {
    // group by (a as a1, b as b1)
    let left = aggregate_exec_with_alias(
        parquet_exec(),
        vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
        ],
    );
    // group by (b, a)
    let right = aggregate_exec_with_alias(
        parquet_exec(),
        vec![
            ("b".to_string(), "b".to_string()),
            ("a".to_string(), "a".to_string()),
        ],
    );

    // Join on (b1 == b && a1 == a)
    let join_on = vec![
        (
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("a1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("a", &right.schema()).unwrap()) as _,
        ),
    ];
    let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

    // Only two RepartitionExecs added
    let expected = &[
        "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b1@1, b@0), (a1@0, a@1)]",
        "  ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
        "    AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
        "      RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
        "        AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
        "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
        "    RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
        "      AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, join.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, join, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn multi_hash_join_key_ordering() -> Result<()> {
    let left = parquet_exec();
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a1".to_string()),
        ("b".to_string(), "b1".to_string()),
        ("c".to_string(), "c1".to_string()),
    ];
    let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

    // Join on (a == a1 and b == b1 and c == c1)
    let join_on = vec![
        (
            Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("c1", &right.schema()).unwrap()) as _,
        ),
    ];
    let bottom_left_join =
        hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner);

    // Projection(a as A, a as AA, b as B, c as C)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "A".to_string()),
        ("a".to_string(), "AA".to_string()),
        ("b".to_string(), "B".to_string()),
        ("c".to_string(), "C".to_string()),
    ];
    let bottom_left_projection =
        projection_exec_with_alias(bottom_left_join, alias_pairs);

    // Join on (c == c1 and b == b1 and a == a1)
    let join_on = vec![
        (
            Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("c1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema()).unwrap()) as _,
        ),
    ];
    let bottom_right_join =
        hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

    // Join on (B == b1 and C == c and AA = a1)
    let top_join_on = vec![
        (
            Arc::new(
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap())
                as _,
        ),
        (
            Arc::new(
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("c", &bottom_right_join.schema()).unwrap())
                as _,
        ),
        (
            Arc::new(
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap())
                as _,
        ),
    ];

    let top_join = hash_join_exec(
        bottom_left_projection.clone(),
        bottom_right_join,
        &top_join_on,
        &JoinType::Inner,
    );

    let predicate: Arc<dyn PhysicalExpr> = binary(
        col("c", top_join.schema().deref())?,
        Operator::Gt,
        lit(1i64),
        top_join.schema().deref(),
    )?;

    let filter_top_join: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, top_join)?);

    // The bottom joins' join key ordering is adjusted based on the top join. And the top join should not introduce additional RepartitionExec
    let expected = &[
        "FilterExec: c@6 > 1",
        "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(B@2, b1@6), (C@3, c@2), (AA@1, a1@5)]",
        "    ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
        "      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, b1@1), (c@2, c1@2), (a@0, a1@0)]",
        "        RepartitionExec: partitioning=Hash([b@1, c@2, a@0], 10), input_partitions=10",
        "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "        RepartitionExec: partitioning=Hash([b1@1, c1@2, a1@0], 10), input_partitions=10",
        "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "            ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
        "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, b1@1), (c@2, c1@2), (a@0, a1@0)]",
        "      RepartitionExec: partitioning=Hash([b@1, c@2, a@0], 10), input_partitions=10",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "      RepartitionExec: partitioning=Hash([b1@1, c1@2, a1@0], 10), input_partitions=10",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, filter_top_join.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, filter_top_join, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn reorder_join_keys_to_left_input() -> Result<()> {
    let left = parquet_exec();
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a1".to_string()),
        ("b".to_string(), "b1".to_string()),
        ("c".to_string(), "c1".to_string()),
    ];
    let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

    // Join on (a == a1 and b == b1 and c == c1)
    let join_on = vec![
        (
            Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("c1", &right.schema()).unwrap()) as _,
        ),
    ];

    let bottom_left_join = ensure_distribution_helper(
        hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner),
        10,
        true,
    )?;

    // Projection(a as A, a as AA, b as B, c as C)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "A".to_string()),
        ("a".to_string(), "AA".to_string()),
        ("b".to_string(), "B".to_string()),
        ("c".to_string(), "C".to_string()),
    ];
    let bottom_left_projection =
        projection_exec_with_alias(bottom_left_join, alias_pairs);

    // Join on (c == c1 and b == b1 and a == a1)
    let join_on = vec![
        (
            Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("c1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema()).unwrap()) as _,
        ),
    ];
    let bottom_right_join = ensure_distribution_helper(
        hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner),
        10,
        true,
    )?;

    // Join on (B == b1 and C == c and AA = a1)
    let top_join_on = vec![
        (
            Arc::new(
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap())
                as _,
        ),
        (
            Arc::new(
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("c", &bottom_right_join.schema()).unwrap())
                as _,
        ),
        (
            Arc::new(
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap())
                as _,
        ),
    ];

    let join_types = vec![
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::RightSemi,
        JoinType::RightAnti,
    ];

    for join_type in join_types {
        let top_join = hash_join_exec(
            bottom_left_projection.clone(),
            bottom_right_join.clone(),
            &top_join_on,
            &join_type,
        );
        let top_join_plan =
            format!("HashJoinExec: mode=Partitioned, join_type={:?}, on=[(AA@1, a1@5), (B@2, b1@6), (C@3, c@2)]", &join_type);

        let reordered = reorder_join_keys_to_inputs(top_join)?;

        // The top joins' join key ordering is adjusted based on the children inputs.
        let expected = &[
            top_join_plan.as_str(),
            "  ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a1@0), (b@1, b1@1), (c@2, c1@2)]",
            "      RepartitionExec: partitioning=Hash([a@0, b@1, c@2], 10), input_partitions=10",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
            "      RepartitionExec: partitioning=Hash([a1@0, b1@1, c1@2], 10), input_partitions=10",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c1@2), (b@1, b1@1), (a@0, a1@0)]",
            "    RepartitionExec: partitioning=Hash([c@2, b@1, a@0], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
            "    RepartitionExec: partitioning=Hash([c1@2, b1@1, a1@0], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        ];

        assert_plan_txt!(expected, reordered);
    }

    Ok(())
}

#[test]
fn reorder_join_keys_to_right_input() -> Result<()> {
    let left = parquet_exec();
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a1".to_string()),
        ("b".to_string(), "b1".to_string()),
        ("c".to_string(), "c1".to_string()),
    ];
    let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

    // Join on (a == a1 and b == b1)
    let join_on = vec![
        (
            Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        ),
    ];
    let bottom_left_join = ensure_distribution_helper(
        hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner),
        10,
        true,
    )?;

    // Projection(a as A, a as AA, b as B, c as C)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "A".to_string()),
        ("a".to_string(), "AA".to_string()),
        ("b".to_string(), "B".to_string()),
        ("c".to_string(), "C".to_string()),
    ];
    let bottom_left_projection =
        projection_exec_with_alias(bottom_left_join, alias_pairs);

    // Join on (c == c1 and b == b1 and a == a1)
    let join_on = vec![
        (
            Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("c1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema()).unwrap()) as _,
        ),
    ];
    let bottom_right_join = ensure_distribution_helper(
        hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner),
        10,
        true,
    )?;

    // Join on (B == b1 and C == c and AA = a1)
    let top_join_on = vec![
        (
            Arc::new(
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap())
                as _,
        ),
        (
            Arc::new(
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("c", &bottom_right_join.schema()).unwrap())
                as _,
        ),
        (
            Arc::new(
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
            ) as _,
            Arc::new(Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap())
                as _,
        ),
    ];

    let join_types = vec![
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::RightSemi,
        JoinType::RightAnti,
    ];

    for join_type in join_types {
        let top_join = hash_join_exec(
            bottom_left_projection.clone(),
            bottom_right_join.clone(),
            &top_join_on,
            &join_type,
        );
        let top_join_plan =
            format!("HashJoinExec: mode=Partitioned, join_type={:?}, on=[(C@3, c@2), (B@2, b1@6), (AA@1, a1@5)]", &join_type);

        let reordered = reorder_join_keys_to_inputs(top_join)?;

        // The top joins' join key ordering is adjusted based on the children inputs.
        let expected = &[
            top_join_plan.as_str(),
            "  ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a1@0), (b@1, b1@1)]",
            "      RepartitionExec: partitioning=Hash([a@0, b@1], 10), input_partitions=10",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
            "      RepartitionExec: partitioning=Hash([a1@0, b1@1], 10), input_partitions=10",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c1@2), (b@1, b1@1), (a@0, a1@0)]",
            "    RepartitionExec: partitioning=Hash([c@2, b@1, a@0], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
            "    RepartitionExec: partitioning=Hash([c1@2, b1@1, a1@0], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        ];

        assert_plan_txt!(expected, reordered);
    }

    Ok(())
}

/// These test cases use [`TestConfig::with_prefer_existing_sort`].
#[test]
fn multi_smj_joins() -> Result<()> {
    let test_config = TestConfig::default().with_prefer_existing_sort();

    let left = parquet_exec();
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a1".to_string()),
        ("b".to_string(), "b1".to_string()),
        ("c".to_string(), "c1".to_string()),
        ("d".to_string(), "d1".to_string()),
        ("e".to_string(), "e1".to_string()),
    ];
    let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

    // SortMergeJoin does not support RightSemi and RightAnti join now
    let join_types = vec![
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
    ];

    // Join on (a == b1)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
    )];

    for join_type in join_types {
        let join =
            sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
        let join_plan = |shift| -> String {
            format!(
                "{}SortMergeJoin: join_type={join_type}, on=[(a@0, b1@1)]",
                " ".repeat(shift)
            )
        };
        let join_plan_indent2 = join_plan(2);
        let join_plan_indent6 = join_plan(6);
        let join_plan_indent10 = join_plan(10);

        // Top join on (a == c)
        let top_join_on = vec![(
            Arc::new(Column::new_with_schema("a", &join.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
        )];
        let top_join =
            sort_merge_join_exec(join.clone(), parquet_exec(), &top_join_on, &join_type);
        let top_join_plan =
            format!("SortMergeJoin: join_type={join_type}, on=[(a@0, c@2)]");

        let expected = match join_type {
            // Should include 6 RepartitionExecs (3 hash, 3 round-robin), 3 SortExecs
            JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti =>
                vec![
                    top_join_plan.as_str(),
                    &join_plan_indent2,
                    "    SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
                    "      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                    "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    "    SortExec: expr=[b1@1 ASC], preserve_partitioning=[true]",
                    "      RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                    "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    "  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]",
                    "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                    "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                ],
            // Should include 7 RepartitionExecs (4 hash, 3 round-robin), 4 SortExecs
            // Since ordering of the left child is not preserved after SortMergeJoin
            // when mode is Right, RightSemi, RightAnti, Full
            // - We need to add one additional SortExec after SortMergeJoin in contrast the test cases
            //   when mode is Inner, Left, LeftSemi, LeftAnti
            // Similarly, since partitioning of the left side is not preserved
            // when mode is Right, RightSemi, RightAnti, Full
            // - We need to add one additional Hash Repartition after SortMergeJoin in contrast the test
            //   cases when mode is Inner, Left, LeftSemi, LeftAnti
            _ => vec![
                    top_join_plan.as_str(),
                    // Below 2 operators are differences introduced, when join mode is changed
                    "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
                    "    RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                    &join_plan_indent6,
                    "        SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
                    "          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                    "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    "        SortExec: expr=[b1@1 ASC], preserve_partitioning=[true]",
                    "          RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                    "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "              ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    "  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]",
                    "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                    "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
            ],
        };
        // TODO(wiedld): show different test result if enforce sorting first.
        test_config.run(&expected, top_join.clone(), &DISTRIB_DISTRIB_SORT)?;

        let expected_first_sort_enforcement = match join_type {
            // Should include 6 RepartitionExecs (3 hash, 3 round-robin), 3 SortExecs
            JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti =>
                vec![
                    top_join_plan.as_str(),
                    &join_plan_indent2,
                    "    RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC",
                    "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "        SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
                    "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    "    RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10, preserve_order=true, sort_exprs=b1@1 ASC",
                    "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "        SortExec: expr=[b1@1 ASC], preserve_partitioning=[false]",
                    "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, preserve_order=true, sort_exprs=c@2 ASC",
                    "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "      SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
                    "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                ],
            // Should include 8 RepartitionExecs (4 hash, 8 round-robin), 4 SortExecs
            // Since ordering of the left child is not preserved after SortMergeJoin
            // when mode is Right, RightSemi, RightAnti, Full
            // - We need to add one additional SortExec after SortMergeJoin in contrast the test cases
            //   when mode is Inner, Left, LeftSemi, LeftAnti
            // Similarly, since partitioning of the left side is not preserved
            // when mode is Right, RightSemi, RightAnti, Full
            // - We need to add one additional Hash Repartition and Roundrobin repartition after
            //   SortMergeJoin in contrast the test cases when mode is Inner, Left, LeftSemi, LeftAnti
            _ => vec![
                top_join_plan.as_str(),
                // Below 4 operators are differences introduced, when join mode is changed
                "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC",
                "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "      SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
                "        CoalescePartitionsExec",
                &join_plan_indent10,
                "            RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC",
                "              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "                SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
                "                  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                "            RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10, preserve_order=true, sort_exprs=b1@1 ASC",
                "              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "                SortExec: expr=[b1@1 ASC], preserve_partitioning=[false]",
                "                  ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                "                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, preserve_order=true, sort_exprs=c@2 ASC",
                "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "      SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
                "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
            ],
        };
        // TODO(wiedld): show different test result if enforce distribution first.
        test_config.run(
            &expected_first_sort_enforcement,
            top_join,
            &SORT_DISTRIB_DISTRIB,
        )?;

        match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                // This time we use (b1 == c) for top join
                // Join on (b1 == c)
                let top_join_on = vec![(
                    Arc::new(Column::new_with_schema("b1", &join.schema()).unwrap()) as _,
                    Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
                )];
                let top_join =
                    sort_merge_join_exec(join, parquet_exec(), &top_join_on, &join_type);
                let top_join_plan =
                    format!("SortMergeJoin: join_type={join_type}, on=[(b1@6, c@2)]");

                let expected = match join_type {
                    // Should include 6 RepartitionExecs(3 hash, 3 round-robin) and 3 SortExecs
                    JoinType::Inner | JoinType::Right => vec![
                        top_join_plan.as_str(),
                        &join_plan_indent2,
                        "    SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
                        "      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "    SortExec: expr=[b1@1 ASC], preserve_partitioning=[true]",
                        "      RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]",
                        "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    ],
                    // Should include 7 RepartitionExecs (4 hash, 3 round-robin) and 4 SortExecs
                    JoinType::Left | JoinType::Full => vec![
                        top_join_plan.as_str(),
                        "  SortExec: expr=[b1@6 ASC], preserve_partitioning=[true]",
                        "    RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10",
                        &join_plan_indent6,
                        "        SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
                        "          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "        SortExec: expr=[b1@1 ASC], preserve_partitioning=[true]",
                        "          RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "              ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]",
                        "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    ],
                    // this match arm cannot be reached
                    _ => unreachable!()
                };
                // TODO(wiedld): show different test result if enforce sorting first.
                test_config.run(&expected, top_join.clone(), &DISTRIB_DISTRIB_SORT)?;

                let expected_first_sort_enforcement = match join_type {
                    // Should include 6 RepartitionExecs (3 of them preserves order) and 3 SortExecs
                    JoinType::Inner | JoinType::Right => vec![
                        top_join_plan.as_str(),
                        &join_plan_indent2,
                        "    RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC",
                        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "        SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
                        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "    RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10, preserve_order=true, sort_exprs=b1@1 ASC",
                        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "        SortExec: expr=[b1@1 ASC], preserve_partitioning=[false]",
                        "          ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, preserve_order=true, sort_exprs=c@2 ASC",
                        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "      SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
                        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    ],
                    // Should include 8 RepartitionExecs (4 of them preserves order) and 4 SortExecs
                    JoinType::Left | JoinType::Full => vec![
                        top_join_plan.as_str(),
                        "  RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10, preserve_order=true, sort_exprs=b1@6 ASC",
                        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "      SortExec: expr=[b1@6 ASC], preserve_partitioning=[false]",
                        "        CoalescePartitionsExec",
                        &join_plan_indent10,
                        "            RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC",
                        "              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "                SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
                        "                  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "            RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10, preserve_order=true, sort_exprs=b1@1 ASC",
                        "              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "                SortExec: expr=[b1@1 ASC], preserve_partitioning=[false]",
                        "                  ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                        "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, preserve_order=true, sort_exprs=c@2 ASC",
                        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "      SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
                        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
                    ],
                    // this match arm cannot be reached
                    _ => unreachable!()
                };

                // TODO(wiedld): show different test result if enforce distribution first.
                test_config.run(
                    &expected_first_sort_enforcement,
                    top_join,
                    &SORT_DISTRIB_DISTRIB,
                )?;
            }
            _ => {}
        }
    }

    Ok(())
}

/// These test cases use [`TestConfig::with_prefer_existing_sort`].
#[test]
fn smj_join_key_ordering() -> Result<()> {
    // group by (a as a1, b as b1)
    let left = aggregate_exec_with_alias(
        parquet_exec(),
        vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
        ],
    );
    //Projection(a1 as a3, b1 as b3)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a1".to_string(), "a3".to_string()),
        ("b1".to_string(), "b3".to_string()),
    ];
    let left = projection_exec_with_alias(left, alias_pairs);

    // group by (b, a)
    let right = aggregate_exec_with_alias(
        parquet_exec(),
        vec![
            ("b".to_string(), "b".to_string()),
            ("a".to_string(), "a".to_string()),
        ],
    );

    //Projection(a as a2, b as b2)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a2".to_string()),
        ("b".to_string(), "b2".to_string()),
    ];
    let right = projection_exec_with_alias(right, alias_pairs);

    // Join on (b3 == b2 && a3 == a2)
    let join_on = vec![
        (
            Arc::new(Column::new_with_schema("b3", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        ),
        (
            Arc::new(Column::new_with_schema("a3", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("a2", &right.schema()).unwrap()) as _,
        ),
    ];
    let join = sort_merge_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

    // TestConfig: Prefer existing sort.
    let test_config = TestConfig::default().with_prefer_existing_sort();

    // Test: run EnforceDistribution, then EnforceSort.
    // Only two RepartitionExecs added
    let expected = &[
        "SortMergeJoin: join_type=Inner, on=[(b3@1, b2@1), (a3@0, a2@0)]",
        "  SortExec: expr=[b3@1 ASC, a3@0 ASC], preserve_partitioning=[true]",
        "    ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]",
        "      ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
        "        AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
        "          RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
        "            AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
        "              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  SortExec: expr=[b2@1 ASC, a2@0 ASC], preserve_partitioning=[true]",
        "    ProjectionExec: expr=[a@1 as a2, b@0 as b2]",
        "      AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
        "        RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
        "          AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
        "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(expected, join.clone(), &DISTRIB_DISTRIB_SORT)?;

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let expected_first_sort_enforcement = &[
        "SortMergeJoin: join_type=Inner, on=[(b3@1, b2@1), (a3@0, a2@0)]",
        "  RepartitionExec: partitioning=Hash([b3@1, a3@0], 10), input_partitions=10, preserve_order=true, sort_exprs=b3@1 ASC, a3@0 ASC",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      SortExec: expr=[b3@1 ASC, a3@0 ASC], preserve_partitioning=[false]",
        "        CoalescePartitionsExec",
        "          ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]",
        "            ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
        "              AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
        "                RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
        "                  AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
        "                    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  RepartitionExec: partitioning=Hash([b2@1, a2@0], 10), input_partitions=10, preserve_order=true, sort_exprs=b2@1 ASC, a2@0 ASC",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      SortExec: expr=[b2@1 ASC, a2@0 ASC], preserve_partitioning=[false]",
        "        CoalescePartitionsExec",
        "          ProjectionExec: expr=[a@1 as a2, b@0 as b2]",
        "            AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
        "              RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
        "                AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
        "                  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(expected_first_sort_enforcement, join, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn merge_does_not_need_sort() -> Result<()> {
    // see https://github.com/apache/datafusion/issues/4331
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema).unwrap(),
        options: SortOptions::default(),
    }]);

    // Scan some sorted parquet files
    let exec = parquet_exec_multiple_sorted(vec![sort_key.clone()]);

    // CoalesceBatchesExec to mimic behavior after a filter
    let exec = Arc::new(CoalesceBatchesExec::new(exec, 4096));

    // Merge from multiple parquet files and keep the data sorted
    let exec: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(sort_key, exec));

    // Test: run EnforceDistribution, then EnforceSort.
    //
    // The optimizer should not add an additional SortExec as the
    // data is already sorted
    let expected = &[
        "SortPreservingMergeExec: [a@0 ASC]",
        "  CoalesceBatchesExec: target_batch_size=4096",
        "    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, exec.clone(), &DISTRIB_DISTRIB_SORT)?;

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    //
    // In this case preserving ordering through order preserving operators is not desirable
    // (according to flag: PREFER_EXISTING_SORT)
    // hence in this case ordering lost during CoalescePartitionsExec and re-introduced with
    // SortExec at the top.
    let expected_first_sort_enforcement = &[
        "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    CoalesceBatchesExec: target_batch_size=4096",
        "      DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    test_config.run(expected_first_sort_enforcement, exec, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn union_to_interleave() -> Result<()> {
    // group by (a as a1)
    let left = aggregate_exec_with_alias(
        parquet_exec(),
        vec![("a".to_string(), "a1".to_string())],
    );
    // group by (a as a2)
    let right = aggregate_exec_with_alias(
        parquet_exec(),
        vec![("a".to_string(), "a1".to_string())],
    );

    //  Union
    let plan = Arc::new(UnionExec::new(vec![left, right]));

    // final agg
    let plan =
        aggregate_exec_with_alias(plan, vec![("a1".to_string(), "a2".to_string())]);

    // Only two RepartitionExecs added, no final RepartitionExec required
    let expected = &[
        "AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
        "  AggregateExec: mode=Partial, gby=[a1@0 as a2], aggr=[]",
        "    InterleaveExec",
        "      AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
        "        RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
        "          AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
        "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "      AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
        "        RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
        "          AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
        "            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn union_not_to_interleave() -> Result<()> {
    // group by (a as a1)
    let left = aggregate_exec_with_alias(
        parquet_exec(),
        vec![("a".to_string(), "a1".to_string())],
    );
    // group by (a as a2)
    let right = aggregate_exec_with_alias(
        parquet_exec(),
        vec![("a".to_string(), "a1".to_string())],
    );

    //  Union
    let plan = Arc::new(UnionExec::new(vec![left, right]));

    // final agg
    let plan =
        aggregate_exec_with_alias(plan, vec![("a1".to_string(), "a2".to_string())]);

    // Only two RepartitionExecs added, no final RepartitionExec required
    let expected = &[
        "AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a2@0], 10), input_partitions=20",
        "    AggregateExec: mode=Partial, gby=[a1@0 as a2], aggr=[]",
        "      UnionExec",
        "        AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
        "          RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
        "            AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
        "              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "        AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
        "          RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
        "            AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
        "              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    // TestConfig: Prefer existing union.
    let test_config = TestConfig::default().with_prefer_existing_union();

    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn added_repartition_to_single_partition() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan = aggregate_exec_with_alias(parquet_exec(), alias);

    let expected = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(&expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(&expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_deepest_node() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan = aggregate_exec_with_alias(filter_exec(parquet_exec()), alias);

    let expected = &[
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      FilterExec: c@2 = 0",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_unsorted_limit() -> Result<()> {
    let plan = limit_exec(filter_exec(parquet_exec()));

    let expected = &[
        "GlobalLimitExec: skip=0, fetch=100",
        "  CoalescePartitionsExec",
        "    LocalLimitExec: fetch=100",
        "      FilterExec: c@2 = 0",
        // nothing sorts the data, so the local limit doesn't require sorted data either
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_sorted_limit() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan = limit_exec(sort_exec(sort_key, parquet_exec(), false));

    let expected = &[
        "GlobalLimitExec: skip=0, fetch=100",
        "  LocalLimitExec: fetch=100",
        // data is sorted so can't repartition here
        "    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_sorted_limit_with_filter() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan = sort_required_exec_with_req(
        filter_exec(sort_exec(sort_key.clone(), parquet_exec(), false)),
        sort_key,
    );

    let expected = &[
        "SortRequiredExec: [c@2 ASC]",
        "  FilterExec: c@2 = 0",
        // We can use repartition here, ordering requirement by SortRequiredExec
        // is still satisfied.
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_ignores_limit() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan = aggregate_exec_with_alias(
        limit_exec(filter_exec(limit_exec(parquet_exec()))),
        alias,
    );

    let expected = &[
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        GlobalLimitExec: skip=0, fetch=100",
        "          CoalescePartitionsExec",
        "            LocalLimitExec: fetch=100",
        "              FilterExec: c@2 = 0",
        // repartition should happen prior to the filter to maximize parallelism
        "                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "                  GlobalLimitExec: skip=0, fetch=100",
        "                    LocalLimitExec: fetch=100",
        // Expect no repartition to happen for local limit
        "                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_ignores_union() -> Result<()> {
    let plan = union_exec(vec![parquet_exec(); 5]);

    let expected = &[
        "UnionExec",
        // Expect no repartition of DataSourceExec
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_through_sort_preserving_merge() -> Result<()> {
    // sort preserving merge with non-sorted input
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan = sort_preserving_merge_exec(sort_key, parquet_exec());

    // need resort as the data was not sorted correctly
    let expected = &[
        "SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_ignores_sort_preserving_merge() -> Result<()> {
    // sort preserving merge already sorted input,
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan = sort_preserving_merge_exec(
        sort_key.clone(),
        parquet_exec_multiple_sorted(vec![sort_key]),
    );

    // Test: run EnforceDistribution, then EnforceSort
    //
    // should not sort (as the data was already sorted)
    // should not repartition, since increased parallelism is not beneficial for SortPReservingMerge
    let expected = &[
        "SortPreservingMergeExec: [c@2 ASC]",
        "  DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let expected_first_sort_enforcement = &[
        "SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(expected_first_sort_enforcement, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_ignores_sort_preserving_merge_with_union() -> Result<()> {
    // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = union_exec(vec![parquet_exec_with_sort(vec![sort_key.clone()]); 2]);
    let plan = sort_preserving_merge_exec(sort_key, input);

    // Test: run EnforceDistribution, then EnforceSort.
    //
    // should not repartition / sort (as the data was already sorted)
    let expected = &[
        "SortPreservingMergeExec: [c@2 ASC]",
        "  UnionExec",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;

    // test: result IS DIFFERENT, if EnforceSorting is run first:
    let expected_first_sort_enforcement = &[
        "SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    UnionExec",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(expected_first_sort_enforcement, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

/// These test cases use [`TestConfig::with_prefer_existing_sort`].
#[test]
fn repartition_does_not_destroy_sort() -> Result<()> {
    //  SortRequired
    //    Parquet(sorted)
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("d", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan = sort_required_exec_with_req(
        filter_exec(parquet_exec_with_sort(vec![sort_key.clone()])),
        sort_key,
    );

    // TestConfig: Prefer existing sort.
    let test_config = TestConfig::default().with_prefer_existing_sort();

    // during repartitioning ordering is preserved
    let expected = &[
        "SortRequiredExec: [d@3 ASC]",
        "  FilterExec: c@2 = 0",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[d@3 ASC], file_type=parquet",
    ];

    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_does_not_destroy_sort_more_complex() -> Result<()> {
    // model a more complicated scenario where one child of a union can be repartitioned for performance
    // but the other can not be
    //
    // Union
    //  SortRequired
    //    Parquet(sorted)
    //  Filter
    //    Parquet(unsorted)

    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input1 = sort_required_exec_with_req(
        parquet_exec_with_sort(vec![sort_key.clone()]),
        sort_key,
    );
    let input2 = filter_exec(parquet_exec());
    let plan = union_exec(vec![input1, input2]);

    // should not repartition below the SortRequired as that
    // branch doesn't benefit from increased parallelism
    let expected = &[
        "UnionExec",
        // union input 1: no repartitioning
        "  SortRequiredExec: [c@2 ASC]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
        // union input 2: should repartition
        "  FilterExec: c@2 = 0",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_transitively_with_projection() -> Result<()> {
    let schema = schema();
    let proj_exprs = vec![(
        Arc::new(BinaryExpr::new(
            col("a", &schema).unwrap(),
            Operator::Plus,
            col("b", &schema).unwrap(),
        )) as Arc<dyn PhysicalExpr>,
        "sum".to_string(),
    )];
    // non sorted input
    let proj = Arc::new(ProjectionExec::try_new(proj_exprs, parquet_exec())?);
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("sum", &proj.schema()).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan = sort_preserving_merge_exec(sort_key, proj);

    // Test: run EnforceDistribution, then EnforceSort.
    let expected = &[
        "SortPreservingMergeExec: [sum@0 ASC]",
        "  SortExec: expr=[sum@0 ASC], preserve_partitioning=[true]",
        // Since this projection is not trivial, increasing parallelism is beneficial
        "    ProjectionExec: expr=[a@0 + b@1 as sum]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let expected_first_sort_enforcement = &[
        "SortExec: expr=[sum@0 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        // Since this projection is not trivial, increasing parallelism is beneficial
        "    ProjectionExec: expr=[a@0 + b@1 as sum]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(expected_first_sort_enforcement, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_ignores_transitively_with_projection() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let alias = vec![
        ("a".to_string(), "a".to_string()),
        ("b".to_string(), "b".to_string()),
        ("c".to_string(), "c".to_string()),
    ];
    // sorted input
    let plan = sort_required_exec_with_req(
        projection_exec_with_alias(
            parquet_exec_multiple_sorted(vec![sort_key.clone()]),
            alias,
        ),
        sort_key,
    );

    let expected = &[
        "SortRequiredExec: [c@2 ASC]",
        // Since this projection is trivial, increasing parallelism is not beneficial
        "  ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]",
        "    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_transitively_past_sort_with_projection() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let alias = vec![
        ("a".to_string(), "a".to_string()),
        ("b".to_string(), "b".to_string()),
        ("c".to_string(), "c".to_string()),
    ];
    let plan = sort_preserving_merge_exec(
        sort_key.clone(),
        sort_exec(
            sort_key,
            projection_exec_with_alias(parquet_exec(), alias),
            true,
        ),
    );

    let expected = &[
        "SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        // Since this projection is trivial, increasing parallelism is not beneficial
        "  ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn repartition_transitively_past_sort_with_filter() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan = sort_exec(sort_key, filter_exec(parquet_exec()), false);

    // Test: run EnforceDistribution, then EnforceSort.
    let expected = &[
        "SortPreservingMergeExec: [a@0 ASC]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
        "    FilterExec: c@2 = 0",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let expected_first_sort_enforcement = &[
        "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    FilterExec: c@2 = 0",
        // Expect repartition on the input of the filter (as it can benefit from additional parallelism)
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(expected_first_sort_enforcement, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
#[cfg(feature = "parquet")]
fn repartition_transitively_past_sort_with_projection_and_filter() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan = sort_exec(
        sort_key,
        projection_exec_with_alias(
            filter_exec(parquet_exec()),
            vec![
                ("a".to_string(), "a".to_string()),
                ("b".to_string(), "b".to_string()),
                ("c".to_string(), "c".to_string()),
            ],
        ),
        false,
    );

    // Test: run EnforceDistribution, then EnforceSort.
    let expected = &[
        "SortPreservingMergeExec: [a@0 ASC]",
        // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        "    ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]",
        "      FilterExec: c@2 = 0",
        // repartition is lowest down
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let expected_first_sort_enforcement = &[
        "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]",
        "      FilterExec: c@2 = 0",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(expected_first_sort_enforcement, plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_single_partition() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan_parquet = aggregate_exec_with_alias(parquet_exec(), alias.clone());
    let plan_csv = aggregate_exec_with_alias(csv_exec(), alias);

    let test_config = TestConfig::default()
        .with_prefer_repartition_file_scans(10)
        .with_query_execution_partitions(2);

    // Test: with parquet
    let expected_parquet = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      DataSourceExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(
        &expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(&expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      DataSourceExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    test_config.run(&expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(&expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_multiple_files() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema).unwrap(),
        options: SortOptions::default(),
    }]);

    let plan = filter_exec(parquet_exec_multiple_sorted(vec![sort_key.clone()]));
    let plan = sort_required_exec_with_req(plan, sort_key);

    let test_config = TestConfig::default()
        .with_prefer_existing_sort()
        .with_prefer_repartition_file_scans(1);

    // The groups must have only contiguous ranges of rows from the same file
    // if any group has rows from multiple files, the data is no longer sorted destroyed
    // https://github.com/apache/datafusion/issues/8451
    let expected_with_3_target_partitions = [
        "SortRequiredExec: [a@0 ASC]",
        "  FilterExec: c@2 = 0",
        "    DataSourceExec: file_groups={3 groups: [[x:0..50], [y:0..100], [x:50..100]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    let test_config_concurrency_3 =
        test_config.clone().with_query_execution_partitions(3);
    test_config_concurrency_3.run(
        &expected_with_3_target_partitions,
        plan.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config_concurrency_3.run(
        &expected_with_3_target_partitions,
        plan.clone(),
        &SORT_DISTRIB_DISTRIB,
    )?;

    let expected_with_8_target_partitions = [
        "SortRequiredExec: [a@0 ASC]",
        "  FilterExec: c@2 = 0",
        "    DataSourceExec: file_groups={8 groups: [[x:0..25], [y:0..25], [x:25..50], [y:25..50], [x:50..75], [y:50..75], [x:75..100], [y:75..100]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    let test_config_concurrency_8 = test_config.with_query_execution_partitions(8);
    test_config_concurrency_8.run(
        &expected_with_8_target_partitions,
        plan.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config_concurrency_8.run(
        &expected_with_8_target_partitions,
        plan,
        &SORT_DISTRIB_DISTRIB,
    )?;

    Ok(())
}

#[test]
/// DataSourceExec on compressed csv file will not be partitioned
/// (Not able to decompress chunked csv file)
fn parallelization_compressed_csv() -> Result<()> {
    let compression_types = [
        FileCompressionType::GZIP,
        FileCompressionType::BZIP2,
        FileCompressionType::XZ,
        FileCompressionType::ZSTD,
        FileCompressionType::UNCOMPRESSED,
    ];

    let expected_not_partitioned = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];

    let expected_partitioned = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      DataSourceExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];

    for compression_type in compression_types {
        let expected = if compression_type.is_compressed() {
            &expected_not_partitioned[..]
        } else {
            &expected_partitioned[..]
        };

        let plan = aggregate_exec_with_alias(
            DataSourceExec::from_data_source(
                FileScanConfigBuilder::new(
                    ObjectStoreUrl::parse("test:///").unwrap(),
                    schema(),
                    Arc::new(CsvSource::new(false, b',', b'"')),
                )
                .with_file(PartitionedFile::new("x".to_string(), 100))
                .with_file_compression_type(compression_type)
                .build(),
            ),
            vec![("a".to_string(), "a".to_string())],
        );
        let test_config = TestConfig::default()
            .with_query_execution_partitions(2)
            .with_prefer_repartition_file_scans(10);
        test_config.run(expected, plan.clone(), &DISTRIB_DISTRIB_SORT)?;
        test_config.run(expected, plan, &SORT_DISTRIB_DISTRIB)?;
    }
    Ok(())
}

#[test]
fn parallelization_two_partitions() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan_parquet = aggregate_exec_with_alias(parquet_exec_multiple(), alias.clone());
    let plan_csv = aggregate_exec_with_alias(csv_exec_multiple(), alias);

    let test_config = TestConfig::default()
        .with_query_execution_partitions(2)
        .with_prefer_repartition_file_scans(10);

    // Test: with parquet
    let expected_parquet = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        // Plan already has two partitions
        "      DataSourceExec: file_groups={2 groups: [[x:0..100], [y:0..100]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(
        &expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(&expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        // Plan already has two partitions
        "      DataSourceExec: file_groups={2 groups: [[x:0..100], [y:0..100]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    test_config.run(&expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(&expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_two_partitions_into_four() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan_parquet = aggregate_exec_with_alias(parquet_exec_multiple(), alias.clone());
    let plan_csv = aggregate_exec_with_alias(csv_exec_multiple(), alias);

    let test_config = TestConfig::default()
        .with_query_execution_partitions(4)
        .with_prefer_repartition_file_scans(10);

    // Test: with parquet
    let expected_parquet = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        // Multiple source files splitted across partitions
        "      DataSourceExec: file_groups={4 groups: [[x:0..50], [x:50..100], [y:0..50], [y:50..100]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(
        &expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(&expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = [
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        // Multiple source files splitted across partitions
        "      DataSourceExec: file_groups={4 groups: [[x:0..50], [x:50..100], [y:0..50], [y:50..100]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    test_config.run(&expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(&expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_sorted_limit() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan_parquet = limit_exec(sort_exec(sort_key.clone(), parquet_exec(), false));
    let plan_csv = limit_exec(sort_exec(sort_key, csv_exec(), false));

    let test_config = TestConfig::default();

    // Test: with parquet
    let expected_parquet = &[
        "GlobalLimitExec: skip=0, fetch=100",
        "  LocalLimitExec: fetch=100",
        // data is sorted so can't repartition here
        "    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        // Doesn't parallelize for SortExec without preserve_partitioning
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(
        expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = &[
        "GlobalLimitExec: skip=0, fetch=100",
        "  LocalLimitExec: fetch=100",
        // data is sorted so can't repartition here
        "    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        // Doesn't parallelize for SortExec without preserve_partitioning
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    test_config.run(expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_limit_with_filter() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan_parquet = limit_exec(filter_exec(sort_exec(
        sort_key.clone(),
        parquet_exec(),
        false,
    )));
    let plan_csv = limit_exec(filter_exec(sort_exec(sort_key, csv_exec(), false)));

    let test_config = TestConfig::default();

    // Test: with parquet
    let expected_parquet = &[
        "GlobalLimitExec: skip=0, fetch=100",
        "  CoalescePartitionsExec",
        "    LocalLimitExec: fetch=100",
        "      FilterExec: c@2 = 0",
        // even though data is sorted, we can use repartition here. Since
        // ordering is not used in subsequent stages anyway.
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        // SortExec doesn't benefit from input partitioning
        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(
        expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = &[
        "GlobalLimitExec: skip=0, fetch=100",
        "  CoalescePartitionsExec",
        "    LocalLimitExec: fetch=100",
        "      FilterExec: c@2 = 0",
        // even though data is sorted, we can use repartition here. Since
        // ordering is not used in subsequent stages anyway.
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        // SortExec doesn't benefit from input partitioning
        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    test_config.run(expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_ignores_limit() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan_parquet = aggregate_exec_with_alias(
        limit_exec(filter_exec(limit_exec(parquet_exec()))),
        alias.clone(),
    );
    let plan_csv =
        aggregate_exec_with_alias(limit_exec(filter_exec(limit_exec(csv_exec()))), alias);

    let test_config = TestConfig::default();

    // Test: with parquet
    let expected_parquet = &[
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        GlobalLimitExec: skip=0, fetch=100",
        "          CoalescePartitionsExec",
        "            LocalLimitExec: fetch=100",
        "              FilterExec: c@2 = 0",
        // repartition should happen prior to the filter to maximize parallelism
        "                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "                  GlobalLimitExec: skip=0, fetch=100",
        // Limit doesn't benefit from input partitioning - no parallelism
        "                    LocalLimitExec: fetch=100",
        "                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(
        expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = &[
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
        "    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        GlobalLimitExec: skip=0, fetch=100",
        "          CoalescePartitionsExec",
        "            LocalLimitExec: fetch=100",
        "              FilterExec: c@2 = 0",
        // repartition should happen prior to the filter to maximize parallelism
        "                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "                  GlobalLimitExec: skip=0, fetch=100",
        // Limit doesn't benefit from input partitioning - no parallelism
        "                    LocalLimitExec: fetch=100",
        "                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    test_config.run(expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_union_inputs() -> Result<()> {
    let plan_parquet = union_exec(vec![parquet_exec(); 5]);
    let plan_csv = union_exec(vec![csv_exec(); 5]);

    let test_config = TestConfig::default();

    // Test: with parquet
    let expected_parquet = &[
        "UnionExec",
        // Union doesn't benefit from input partitioning - no parallelism
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    test_config.run(
        expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = &[
        "UnionExec",
        // Union doesn't benefit from input partitioning - no parallelism
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    test_config.run(expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_prior_to_sort_preserving_merge() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    // sort preserving merge already sorted input,
    let plan_parquet = sort_preserving_merge_exec(
        sort_key.clone(),
        parquet_exec_with_sort(vec![sort_key.clone()]),
    );
    let plan_csv =
        sort_preserving_merge_exec(sort_key.clone(), csv_exec_with_sort(vec![sort_key]));

    let test_config = TestConfig::default();

    // Expected Outcome:
    // parallelization is not beneficial for SortPreservingMerge

    // Test: with parquet
    let expected_parquet = &[
        "DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(
        expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = &[
        "DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false",
    ];
    test_config.run(expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_sort_preserving_merge_with_union() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
    let input_parquet =
        union_exec(vec![parquet_exec_with_sort(vec![sort_key.clone()]); 2]);
    let input_csv = union_exec(vec![csv_exec_with_sort(vec![sort_key.clone()]); 2]);
    let plan_parquet = sort_preserving_merge_exec(sort_key.clone(), input_parquet);
    let plan_csv = sort_preserving_merge_exec(sort_key, input_csv);

    let test_config = TestConfig::default();

    // Expected Outcome:
    // should not repartition (union doesn't benefit from increased parallelism)
    // should not sort (as the data was already sorted)

    // Test: with parquet
    let expected_parquet = &[
        "SortPreservingMergeExec: [c@2 ASC]",
        "  UnionExec",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(
        expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    let expected_parquet_first_sort_enforcement = &[
        // no SPM
        "SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        // has coalesce
        "  CoalescePartitionsExec",
        "    UnionExec",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(
        expected_parquet_first_sort_enforcement,
        plan_parquet,
        &SORT_DISTRIB_DISTRIB,
    )?;

    // Test: with csv
    let expected_csv = &[
        "SortPreservingMergeExec: [c@2 ASC]",
        "  UnionExec",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false",
    ];
    test_config.run(expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    let expected_csv_first_sort_enforcement = &[
        // no SPM
        "SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        // has coalesce
        "  CoalescePartitionsExec",
        "    UnionExec",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false",
    ];
    test_config.run(
        expected_csv_first_sort_enforcement,
        plan_csv.clone(),
        &SORT_DISTRIB_DISTRIB,
    )?;

    Ok(())
}

#[test]
fn parallelization_does_not_benefit() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    //  SortRequired
    //    Parquet(sorted)
    let plan_parquet = sort_required_exec_with_req(
        parquet_exec_with_sort(vec![sort_key.clone()]),
        sort_key.clone(),
    );
    let plan_csv =
        sort_required_exec_with_req(csv_exec_with_sort(vec![sort_key.clone()]), sort_key);

    let test_config = TestConfig::default();

    // Expected Outcome:
    // no parallelization, because SortRequiredExec doesn't benefit from increased parallelism

    // Test: with parquet
    let expected_parquet = &[
        "SortRequiredExec: [c@2 ASC]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(
        expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    // Test: with csv
    let expected_csv = &[
        "SortRequiredExec: [c@2 ASC]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false",
    ];
    test_config.run(expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_ignores_transitively_with_projection_parquet() -> Result<()> {
    // sorted input
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);

    //Projection(a as a2, b as b2)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a2".to_string()),
        ("c".to_string(), "c2".to_string()),
    ];
    let proj_parquet =
        projection_exec_with_alias(parquet_exec_with_sort(vec![sort_key]), alias_pairs);
    let sort_key_after_projection = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c2", &proj_parquet.schema()).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan_parquet =
        sort_preserving_merge_exec(sort_key_after_projection, proj_parquet);
    let expected = &[
        "SortPreservingMergeExec: [c2@1 ASC]",
        "  ProjectionExec: expr=[a@0 as a2, c@2 as c2]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    plans_matches_expected!(expected, &plan_parquet);

    // Expected Outcome:
    // data should not be repartitioned / resorted
    let expected_parquet = &[
        "ProjectionExec: expr=[a@0 as a2, c@2 as c2]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(
        expected_parquet,
        plan_parquet.clone(),
        &DISTRIB_DISTRIB_SORT,
    )?;
    test_config.run(expected_parquet, plan_parquet, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn parallelization_ignores_transitively_with_projection_csv() -> Result<()> {
    // sorted input
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);

    //Projection(a as a2, b as b2)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a2".to_string()),
        ("c".to_string(), "c2".to_string()),
    ];

    let proj_csv =
        projection_exec_with_alias(csv_exec_with_sort(vec![sort_key]), alias_pairs);
    let sort_key_after_projection = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c2", &proj_csv.schema()).unwrap(),
        options: SortOptions::default(),
    }]);
    let plan_csv = sort_preserving_merge_exec(sort_key_after_projection, proj_csv);
    let expected = &[
        "SortPreservingMergeExec: [c2@1 ASC]",
        "  ProjectionExec: expr=[a@0 as a2, c@2 as c2]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false",
    ];
    plans_matches_expected!(expected, &plan_csv);

    // Expected Outcome:
    // data should not be repartitioned / resorted
    let expected_csv = &[
        "ProjectionExec: expr=[a@0 as a2, c@2 as c2]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected_csv, plan_csv.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected_csv, plan_csv, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn remove_redundant_roundrobins() -> Result<()> {
    let input = parquet_exec();
    let repartition = repartition_exec(repartition_exec(input));
    let physical_plan = repartition_exec(filter_exec(repartition));
    let expected = &[
        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
        "  FilterExec: c@2 = 0",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    plans_matches_expected!(expected, &physical_plan);

    let expected = &[
        "FilterExec: c@2 = 0",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let test_config = TestConfig::default();
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

/// This test case uses [`TestConfig::with_prefer_existing_sort`].
#[test]
fn remove_unnecessary_spm_after_filter() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

    // TestConfig: Prefer existing sort.
    let test_config = TestConfig::default().with_prefer_existing_sort();

    // Expected Outcome:
    // Original plan expects its output to be ordered by c@2 ASC.
    // This is still satisfied since, after filter that column is constant.
    let expected = &[
        "CoalescePartitionsExec",
        "  FilterExec: c@2 = 0",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2, preserve_order=true, sort_exprs=c@2 ASC",
        "      DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];

    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

/// This test case uses [`TestConfig::with_prefer_existing_sort`].
#[test]
fn preserve_ordering_through_repartition() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("d", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

    // TestConfig: Prefer existing sort.
    let test_config = TestConfig::default().with_prefer_existing_sort();

    let expected = &[
        "SortPreservingMergeExec: [d@3 ASC]",
        "  FilterExec: c@2 = 0",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2, preserve_order=true, sort_exprs=d@3 ASC",
        "      DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[d@3 ASC], file_type=parquet",
    ];
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn do_not_preserve_ordering_through_repartition() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

    let test_config = TestConfig::default();

    // Test: run EnforceDistribution, then EnforceSort.
    let expected = &[
        "SortPreservingMergeExec: [a@0 ASC]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        "    FilterExec: c@2 = 0",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "        DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let expected_first_sort_enforcement = &[
        "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    FilterExec: c@2 = 0",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "        DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    test_config.run(
        expected_first_sort_enforcement,
        physical_plan,
        &SORT_DISTRIB_DISTRIB,
    )?;

    Ok(())
}

#[test]
fn no_need_for_sort_after_filter() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

    let expected = &[
        // After CoalescePartitionsExec c is still constant. Hence c@2 ASC ordering is already satisfied.
        "CoalescePartitionsExec",
        // Since after this stage c is constant. c@2 ASC ordering is already satisfied.
        "  FilterExec: c@2 = 0",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "      DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn do_not_preserve_ordering_through_repartition2() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = parquet_exec_multiple_sorted(vec![sort_key]);

    let sort_req = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let physical_plan = sort_preserving_merge_exec(sort_req, filter_exec(input));

    let test_config = TestConfig::default();

    // Test: run EnforceDistribution, then EnforceSort.
    let expected = &[
        "SortPreservingMergeExec: [a@0 ASC]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        "    FilterExec: c@2 = 0",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "        DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let expected_first_sort_enforcement = &[
        "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        "      FilterExec: c@2 = 0",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "          DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(
        expected_first_sort_enforcement,
        physical_plan,
        &SORT_DISTRIB_DISTRIB,
    )?;

    Ok(())
}

#[test]
fn do_not_preserve_ordering_through_repartition3() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = parquet_exec_multiple_sorted(vec![sort_key]);
    let physical_plan = filter_exec(input);

    let expected = &[
        "FilterExec: c@2 = 0",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn do_not_put_sort_when_input_is_invalid() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = parquet_exec();
    let physical_plan = sort_required_exec_with_req(filter_exec(input), sort_key);
    let expected = &[
        // Ordering requirement of sort required exec is NOT satisfied
        // by existing ordering at the source.
        "SortRequiredExec: [a@0 ASC]",
        "  FilterExec: c@2 = 0",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    assert_plan_txt!(expected, physical_plan);

    let expected = &[
        "SortRequiredExec: [a@0 ASC]",
        // Since at the start of the rule ordering requirement is not satisfied
        // EnforceDistribution rule doesn't satisfy this requirement either.
        "  FilterExec: c@2 = 0",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];

    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 10;
    config.optimizer.enable_round_robin_repartition = true;
    config.optimizer.prefer_existing_sort = false;
    let dist_plan = EnforceDistribution::new().optimize(physical_plan, &config)?;
    assert_plan_txt!(expected, dist_plan);

    Ok(())
}

#[test]
fn put_sort_when_input_is_valid() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_required_exec_with_req(filter_exec(input), sort_key);

    let expected = &[
        // Ordering requirement of sort required exec is satisfied
        // by existing ordering at the source.
        "SortRequiredExec: [a@0 ASC]",
        "  FilterExec: c@2 = 0",
        "    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    assert_plan_txt!(expected, physical_plan);

    let expected = &[
        // Since at the start of the rule ordering requirement is satisfied
        // EnforceDistribution rule satisfy this requirement also.
        "SortRequiredExec: [a@0 ASC]",
        "  FilterExec: c@2 = 0",
        "    DataSourceExec: file_groups={10 groups: [[x:0..20], [y:0..20], [x:20..40], [y:20..40], [x:40..60], [y:40..60], [x:60..80], [y:60..80], [x:80..100], [y:80..100]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];

    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 10;
    config.optimizer.enable_round_robin_repartition = true;
    config.optimizer.prefer_existing_sort = false;
    let dist_plan = EnforceDistribution::new().optimize(physical_plan, &config)?;
    assert_plan_txt!(expected, dist_plan);

    Ok(())
}

#[test]
fn do_not_add_unnecessary_hash() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let alias = vec![("a".to_string(), "a".to_string())];
    let input = parquet_exec_with_sort(vec![sort_key]);
    let physical_plan = aggregate_exec_with_alias(input, alias);

    // TestConfig:
    // Make sure target partition number is 1. In this case hash repartition is unnecessary.
    let test_config = TestConfig::default().with_query_execution_partitions(1);

    let expected = &[
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "  AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn do_not_add_unnecessary_hash2() -> Result<()> {
    let schema = schema();
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("c", &schema).unwrap(),
        options: SortOptions::default(),
    }]);
    let alias = vec![("a".to_string(), "a".to_string())];
    let input = parquet_exec_multiple_sorted(vec![sort_key]);
    let aggregate = aggregate_exec_with_alias(input, alias.clone());
    let physical_plan = aggregate_exec_with_alias(aggregate, alias);

    // TestConfig:
    // Make sure target partition number is larger than 2 (e.g partition number at the source).
    let test_config = TestConfig::default().with_query_execution_partitions(4);

    let expected = &[
        "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        // Since hash requirements of this operator is satisfied. There shouldn't be
        // a hash repartition here
        "  AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "    AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
        "      RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4",
        "        AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
        "          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2",
        "            DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet",
    ];
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn optimize_away_unnecessary_repartition() -> Result<()> {
    let physical_plan = coalesce_partitions_exec(repartition_exec(parquet_exec()));
    let expected = &[
        "CoalescePartitionsExec",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    plans_matches_expected!(expected, physical_plan.clone());

    let expected =
        &["DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet"];

    let test_config = TestConfig::default();
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}

#[test]
fn optimize_away_unnecessary_repartition2() -> Result<()> {
    let physical_plan = filter_exec(repartition_exec(coalesce_partitions_exec(
        filter_exec(repartition_exec(parquet_exec())),
    )));
    let expected = &[
        "FilterExec: c@2 = 0",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "    CoalescePartitionsExec",
        "      FilterExec: c@2 = 0",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    plans_matches_expected!(expected, physical_plan.clone());

    let expected = &[
        "FilterExec: c@2 = 0",
        "  FilterExec: c@2 = 0",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet",
    ];
    let test_config = TestConfig::default();
    test_config.run(expected, physical_plan.clone(), &DISTRIB_DISTRIB_SORT)?;
    test_config.run(expected, physical_plan, &SORT_DISTRIB_DISTRIB)?;

    Ok(())
}
