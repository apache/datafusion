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

use crate::physical_optimizer::test_utils::{
    check_integrity, coalesce_partitions_exec, parquet_exec_with_sort,
    parquet_exec_with_stats, repartition_exec, schema, sort_exec,
    sort_exec_with_preserve_partitioning, sort_merge_join_exec,
    sort_preserving_merge_exec, union_exec,
};

use arrow::array::{RecordBatch, UInt64Array, UInt8Array};
use arrow::compute::SortOptions;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{CsvSource, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::CsvOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::ScalarValue;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::expressions::{binary, lit, BinaryExpr, Column, Literal};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, OrderingRequirements, PhysicalSortExpr,
};
use datafusion_physical_optimizer::enforce_distribution::*;
use datafusion_physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_optimizer::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::execution_plan::ExecutionPlan;
use datafusion_physical_plan::expressions::col;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::JoinOn;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    displayable, DisplayAs, DisplayFormatType, ExecutionPlanProperties, PlanProperties,
    Statistics,
};
use insta::Settings;

/// Helper function to replace only the first occurrence of a regex pattern in a plan
/// Returns (captured_group_1, modified_string)
fn hide_first(
    plan: &dyn ExecutionPlan,
    regex: &str,
    replacement: &str,
) -> (String, String) {
    let plan_str = displayable(plan).indent(true).to_string();
    let pattern = regex::Regex::new(regex).unwrap();

    if let Some(captures) = pattern.captures(&plan_str) {
        let full_match = captures.get(0).unwrap();
        let captured_value = captures
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        let pos = full_match.start();
        let end_pos = full_match.end();
        let mut result = String::with_capacity(plan_str.len());
        result.push_str(&plan_str[..pos]);
        result.push_str(replacement);
        result.push_str(&plan_str[end_pos..]);
        (captured_value, result)
    } else {
        (String::new(), plan_str)
    }
}

macro_rules! assert_plan {
    ($plan: expr, @ $expected:literal) => {
        insta::assert_snapshot!(
            displayable($plan.as_ref()).indent(true).to_string(),
            @ $expected
        )
    };
    ($plan: expr, $another_plan: expr) => {
        let plan1 = displayable($plan.as_ref()).indent(true).to_string();
        let plan2 = displayable($another_plan.as_ref()).indent(true).to_string();
        assert_eq!(plan1, plan2);
    }
}

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
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.expr.clone()))]
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
        self.input.partition_statistics(None)
    }
}

fn parquet_exec() -> Arc<DataSourceExec> {
    parquet_exec_with_sort(schema(), vec![])
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
        Arc::new(ParquetSource::new(schema())),
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
    let config =
        FileScanConfigBuilder::new(ObjectStoreUrl::parse("test:///").unwrap(), {
            let options = CsvOptions {
                has_header: Some(false),
                delimiter: b',',
                quote: b'"',
                ..Default::default()
            };
            Arc::new(CsvSource::new(schema()).with_csv_options(options))
        })
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
    let config =
        FileScanConfigBuilder::new(ObjectStoreUrl::parse("test:///").unwrap(), {
            let options = CsvOptions {
                has_header: Some(false),
                delimiter: b',',
                quote: b'"',
                ..Default::default()
            };
            Arc::new(CsvSource::new(schema()).with_csv_options(options))
        })
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
        exprs.push(ProjectionExpr {
            expr: col(column, &input.schema()).unwrap(),
            alias: alias.to_string(),
        });
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

fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(
        Arc::new(LocalLimitExec::new(input, 100)),
        0,
        Some(100),
    ))
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
    /// and return the result plan (for potential subsequent runs).
    fn try_to_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        optimizers_to_run: &[Run],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Add the ancillary output requirements operator at the start:
        let optimizer = OutputRequirements::new_add_mode();
        let session_config = SessionConfig::from(self.config.clone());
        let optimizer_context = OptimizerContext::new(session_config.clone());
        let mut optimized = optimizer.optimize_plan(plan.clone(), &optimizer_context)?;

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
            let session_config = SessionConfig::from(self.config.clone());
            let optimizer_context = OptimizerContext::new(session_config.clone());
            optimized = match run {
                Run::Distribution => {
                    let optimizer = EnforceDistribution::new();
                    optimizer.optimize_plan(optimized, &optimizer_context)?
                }
                Run::Sorting => {
                    let optimizer = EnforceSorting::new();
                    optimizer.optimize_plan(optimized, &optimizer_context)?
                }
            };
        }

        // Remove the ancillary output requirements operator when done:
        let optimizer = OutputRequirements::new_remove_mode();
        let session_config = SessionConfig::from(self.config.clone());
        let optimizer_context = OptimizerContext::new(session_config.clone());
        let optimized = optimizer.optimize_plan(optimized, &optimizer_context)?;

        Ok(optimized)
    }

    fn to_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        optimizers_to_run: &[Run],
    ) -> Arc<dyn ExecutionPlan> {
        self.try_to_plan(plan, optimizers_to_run).unwrap()
    }
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

    let settings = Settings::clone_current();

    // Join on (a == b1)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("a", &schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
    )];

    for join_type in join_types {
        let join = hash_join_exec(left.clone(), right.clone(), &join_on, &join_type);

        let mut settings = settings.clone();
        settings.add_filter(
            // join_type={} replace with join_type=... to avoid snapshot name issue
            format!("join_type={join_type}").as_str(),
            "join_type=...",
        );

        insta::allow_duplicates! {
            settings.bind( || {


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

                let test_config = TestConfig::default();
                let plan_distrib = test_config.to_plan(top_join.clone(), &DISTRIB_DISTRIB_SORT);

                match join_type {
                    // Should include 3 RepartitionExecs
                    JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {

                                assert_plan!(plan_distrib, @r"
                                HashJoinExec: mode=Partitioned, join_type=..., on=[(a@0, c@2)]
                                  HashJoinExec: mode=Partitioned, join_type=..., on=[(a@0, b1@1)]
                                    RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
                                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                    RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1
                                      ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
                                        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
                                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                ");
                            },
                    // Should include 4 RepartitionExecs
                    _ => {
                                assert_plan!(plan_distrib, @r"
                                HashJoinExec: mode=Partitioned, join_type=..., on=[(a@0, c@2)]
                                  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10
                                    HashJoinExec: mode=Partitioned, join_type=..., on=[(a@0, b1@1)]
                                      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
                                        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                      RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1
                                        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
                                          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
                                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                ");
                            },
                };


                let plan_sort = test_config.to_plan(top_join, &SORT_DISTRIB_DISTRIB);
                assert_plan!(plan_distrib, plan_sort);
            }
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {}
        }



        match join_type {
            JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => {
                // This time we use (b1 == c) for top join
                // Join on (b1 == c)
                let top_join_on = vec![(
                    Arc::new(Column::new_with_schema("b1", &join.schema()).unwrap()) as _,
                    Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
                )];

                let top_join =
                    hash_join_exec(join, parquet_exec(), &top_join_on, &join_type);

                let test_config = TestConfig::default();
                let plan_distrib = test_config.to_plan(top_join.clone(), &DISTRIB_DISTRIB_SORT);

                match join_type {
                    // Should include 3 RepartitionExecs
                    JoinType::Inner | JoinType::Right => {
                            assert_plan!(parquet_exec(), @"DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet");
                            },
                    // Should include 3 RepartitionExecs but have a different "on"
                            JoinType::RightSemi | JoinType::RightAnti => {
                            assert_plan!(plan_distrib, @r"
                            HashJoinExec: mode=Partitioned, join_type=..., on=[(b1@1, c@2)]
                              HashJoinExec: mode=Partitioned, join_type=..., on=[(a@0, b1@1)]
                                RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
                                  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1
                                  ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
                                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                              RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
                                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                            ");

                            }

                    // Should include 4 RepartitionExecs
                    _ => {
                            assert_plan!(plan_distrib, @r"
                            HashJoinExec: mode=Partitioned, join_type=..., on=[(b1@6, c@2)]
                              RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10
                                HashJoinExec: mode=Partitioned, join_type=..., on=[(a@0, b1@1)]
                                  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
                                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                  RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1
                                    ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
                                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                              RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
                                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                            ");

                            },
                };


                let plan_sort = test_config.to_plan(top_join, &SORT_DISTRIB_DISTRIB);
                        assert_plan!(plan_distrib, plan_sort);
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {}
        }

                });
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
    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(top_join.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(
        plan_distrib,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a1@0, c@2)]
      ProjectionExec: expr=[a@0 as a1, a@0 as a2]
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]
          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
          RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=1
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
    let plan_sort = test_config.to_plan(top_join, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    // Join on (a2 == c)
    let top_join_on = vec![(
        Arc::new(Column::new_with_schema("a2", &projection.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
    )];

    let top_join = hash_join_exec(projection, right, &top_join_on, &JoinType::Inner);

    // Output partition need to respect the Alias and should not introduce additional RepartitionExec
    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(top_join.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(
        plan_distrib,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a2@1, c@2)]
      ProjectionExec: expr=[a@0 as a1, a@0 as a2]
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]
          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
          RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=1
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
    let plan_sort = test_config.to_plan(top_join, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

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
    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(top_join.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(
        plan_distrib,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, c@2)]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10
        ProjectionExec: expr=[c1@0 as a]
          ProjectionExec: expr=[c@2 as c1]
            HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]
              RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
              RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=1
                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
    let plan_sort = test_config.to_plan(top_join, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

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
    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(join.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(
        plan_distrib,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a1@0, a2@0)]
      AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]
        RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10
          AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]
            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]
        RepartitionExec: partitioning=Hash([a2@0], 10), input_partitions=10
          AggregateExec: mode=Partial, gby=[a@0 as a2], aggr=[]
            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
    let plan_sort = test_config.to_plan(join, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

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
    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(join.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(
        plan_distrib,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b1@1, b@0), (a1@0, a@1)]
      ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]
        AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]
          RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10
            AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]
        RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10
          AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]
            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
    let plan_sort = test_config.to_plan(join, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

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
    let test_config = TestConfig::default();
    let plan_distrib =
        test_config.to_plan(filter_top_join.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(
        plan_distrib,
        @r"
    FilterExec: c@6 > 1
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(B@2, b1@6), (C@3, c@2), (AA@1, a1@5)]
        ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]
          HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, b1@1), (c@2, c1@2), (a@0, a1@0)]
            RepartitionExec: partitioning=Hash([b@1, c@2, a@0], 10), input_partitions=1
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
            RepartitionExec: partitioning=Hash([b1@1, c1@2, a1@0], 10), input_partitions=1
              ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]
                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, b1@1), (c@2, c1@2), (a@0, a1@0)]
          RepartitionExec: partitioning=Hash([b@1, c@2, a@0], 10), input_partitions=1
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
          RepartitionExec: partitioning=Hash([b1@1, c1@2, a1@0], 10), input_partitions=1
            ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    "
    );
    let plan_sort = test_config.to_plan(filter_top_join, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

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

        let reordered = reorder_join_keys_to_inputs(top_join).unwrap();

        // The top joins' join key ordering is adjusted based on the children inputs.
        let (captured_join_type, modified_plan) =
            hide_first(reordered.as_ref(), r"join_type=(\w+)", "join_type=...");
        assert_eq!(captured_join_type, join_type.to_string());

        insta::allow_duplicates! {insta::assert_snapshot!(modified_plan, @r"
HashJoinExec: mode=Partitioned, join_type=..., on=[(AA@1, a1@5), (B@2, b1@6), (C@3, c@2)]
  ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a1@0), (b@1, b1@1), (c@2, c1@2)]
      RepartitionExec: partitioning=Hash([a@0, b@1, c@2], 10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      RepartitionExec: partitioning=Hash([a1@0, b1@1, c1@2], 10), input_partitions=1
        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c1@2), (b@1, b1@1), (a@0, a1@0)]
    RepartitionExec: partitioning=Hash([c@2, b@1, a@0], 10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    RepartitionExec: partitioning=Hash([c1@2, b1@1, a1@0], 10), input_partitions=1
      ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");}
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

        let reordered = reorder_join_keys_to_inputs(top_join).unwrap();

        // The top joins' join key ordering is adjusted based on the children inputs.
        let (_, plan_str) =
            hide_first(reordered.as_ref(), r"join_type=(\w+)", "join_type=...");
        insta::allow_duplicates! {insta::assert_snapshot!(plan_str, @r"
HashJoinExec: mode=Partitioned, join_type=..., on=[(C@3, c@2), (B@2, b1@6), (AA@1, a1@5)]
  ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a1@0), (b@1, b1@1)]
      RepartitionExec: partitioning=Hash([a@0, b@1], 10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      RepartitionExec: partitioning=Hash([a1@0, b1@1], 10), input_partitions=1
        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c1@2), (b@1, b1@1), (a@0, a1@0)]
    RepartitionExec: partitioning=Hash([c@2, b@1, a@0], 10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    RepartitionExec: partitioning=Hash([c1@2, b1@1, a1@0], 10), input_partitions=1
      ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");}
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

        // Top join on (a == c)
        let top_join_on = vec![(
            Arc::new(Column::new_with_schema("a", &join.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
        )];
        let top_join =
            sort_merge_join_exec(join.clone(), parquet_exec(), &top_join_on, &join_type);

        let mut settings = Settings::clone_current();
        settings.add_filter(&format!("join_type={join_type}"), "join_type=...");

        #[rustfmt::skip]
        insta::allow_duplicates! {
            settings.bind(|| {
                let plan_distrib = test_config.to_plan(top_join.clone(), &DISTRIB_DISTRIB_SORT);

                match join_type {
                    // Should include 6 RepartitionExecs (3 hash, 3 round-robin), 3 SortExecs
                    JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => {
                        assert_plan!(plan_distrib, @r"
SortMergeJoin: join_type=..., on=[(a@0, c@2)]
  SortMergeJoin: join_type=..., on=[(a@0, b1@1)]
    SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    SortExec: expr=[b1@1 ASC], preserve_partitioning=[true]
      RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1
        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]
    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
                    }
                    // Should include 7 RepartitionExecs (4 hash, 3 round-robin), 4 SortExecs
                    // Since ordering of the left child is not preserved after SortMergeJoin
                    // when mode is Right, RightSemi, RightAnti, Full
                    // - We need to add one additional SortExec after SortMergeJoin in contrast the test cases
                    //   when mode is Inner, Left, LeftSemi, LeftAnti
                    // Similarly, since partitioning of the left side is not preserved
                    // when mode is Right, RightSemi, RightAnti, Full
                    // - We need to add one additional Hash Repartition after SortMergeJoin in contrast the test
                    //   cases when mode is Inner, Left, LeftSemi, LeftAnti
                    _ => {
                        assert_plan!(plan_distrib, @r"
SortMergeJoin: join_type=..., on=[(a@0, c@2)]
  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
    RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10
      SortMergeJoin: join_type=..., on=[(a@0, b1@1)]
        SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
        SortExec: expr=[b1@1 ASC], preserve_partitioning=[true]
          RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1
            ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]
    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
                    }
                }

                let plan_sort = test_config.to_plan(top_join.clone(), &SORT_DISTRIB_DISTRIB);

                match join_type {
                    // Should include 6 RepartitionExecs (3 hash, 3 round-robin), 3 SortExecs
                    JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => {
                        // TODO(wiedld): show different test result if enforce distribution first.
                        assert_plan!(plan_sort, @r"
                        SortMergeJoin: join_type=..., on=[(a@0, c@2)]
                          SortMergeJoin: join_type=..., on=[(a@0, b1@1)]
                            RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1, maintains_sort_order=true
                              SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
                                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                            RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1, maintains_sort_order=true
                              SortExec: expr=[b1@1 ASC], preserve_partitioning=[false]
                                ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
                                  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                          RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1, maintains_sort_order=true
                            SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
                              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                        ");
                    }
                    // Should include 8 RepartitionExecs (4 hash, 8 round-robin), 4 SortExecs
                    // Since ordering of the left child is not preserved after SortMergeJoin
                    // when mode is Right, RightSemi, RightAnti, Full
                    // - We need to add one additional SortExec after SortMergeJoin in contrast the test cases
                    //   when mode is Inner, Left, LeftSemi, LeftAnti
                    // Similarly, since partitioning of the left side is not preserved
                    // when mode is Right, RightSemi, RightAnti, Full
                    // - We need to add one additional Hash Repartition and Roundrobin repartition after
                    //   SortMergeJoin in contrast the test cases when mode is Inner, Left, LeftSemi, LeftAnti
                    _ => {
                        // TODO(wiedld): show different test result if enforce distribution first.
                        assert_plan!(plan_sort, @r"
                        SortMergeJoin: join_type=..., on=[(a@0, c@2)]
                          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1, maintains_sort_order=true
                            SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
                              CoalescePartitionsExec
                                SortMergeJoin: join_type=..., on=[(a@0, b1@1)]
                                  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1, maintains_sort_order=true
                                    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
                                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                  RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1, maintains_sort_order=true
                                    SortExec: expr=[b1@1 ASC], preserve_partitioning=[false]
                                      ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
                                        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                          RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1, maintains_sort_order=true
                            SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
                              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                        ");
                    }
                }

                match join_type {
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                        // This time we use (b1 == c) for top join
                        // Join on (b1 == c)
                        let top_join_on = vec![(
                            Arc::new(Column::new_with_schema("b1", &join.schema()).unwrap()) as _,
                            Arc::new(Column::new_with_schema("c", &schema()).unwrap()) as _,
                        )];
                        let top_join = sort_merge_join_exec(join, parquet_exec(), &top_join_on, &join_type);

                        let plan_distrib = test_config.to_plan(top_join.clone(), &DISTRIB_DISTRIB_SORT);

                        match join_type {
                            // Should include 6 RepartitionExecs(3 hash, 3 round-robin) and 3 SortExecs
                            JoinType::Inner | JoinType::Right => {
                                // TODO(wiedld): show different test result if enforce sorting first.
                                assert_plan!(plan_distrib, @r"
SortMergeJoin: join_type=..., on=[(b1@6, c@2)]
  SortMergeJoin: join_type=..., on=[(a@0, b1@1)]
    SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    SortExec: expr=[b1@1 ASC], preserve_partitioning=[true]
      RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1
        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]
    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
                            }
                            // Should include 7 RepartitionExecs (4 hash, 3 round-robin) and 4 SortExecs
                            JoinType::Left | JoinType::Full => {
                                // TODO(wiedld): show different test result if enforce sorting first.
                                assert_plan!(plan_distrib, @r"
SortMergeJoin: join_type=..., on=[(b1@6, c@2)]
  SortExec: expr=[b1@6 ASC], preserve_partitioning=[true]
    RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10
      SortMergeJoin: join_type=..., on=[(a@0, b1@1)]
        SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
        SortExec: expr=[b1@1 ASC], preserve_partitioning=[true]
          RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1
            ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]
    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
                            }
                            // this match arm cannot be reached
                            _ => unreachable!()
                        }

                        let plan_sort = test_config.to_plan(top_join, &SORT_DISTRIB_DISTRIB);

                        match join_type {
                            // Should include 6 RepartitionExecs (3 of them preserves order) and 3 SortExecs
                            JoinType::Inner | JoinType::Right => {
                                // TODO(wiedld): show different test result if enforce distribution first.
                                assert_plan!(plan_sort, @r"
                                SortMergeJoin: join_type=..., on=[(b1@6, c@2)]
                                  SortMergeJoin: join_type=..., on=[(a@0, b1@1)]
                                    RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1, maintains_sort_order=true
                                      SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
                                        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                    RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1, maintains_sort_order=true
                                      SortExec: expr=[b1@1 ASC], preserve_partitioning=[false]
                                        ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
                                          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1, maintains_sort_order=true
                                    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
                                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                ");
                            }
                            // Should include 8 RepartitionExecs (4 of them preserves order) and 4 SortExecs
                            JoinType::Left | JoinType::Full => {
                                // TODO(wiedld): show different test result if enforce distribution first.
                                assert_plan!(plan_sort, @r"
                                SortMergeJoin: join_type=..., on=[(b1@6, c@2)]
                                  RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=1, maintains_sort_order=true
                                    SortExec: expr=[b1@6 ASC], preserve_partitioning=[false]
                                      CoalescePartitionsExec
                                        SortMergeJoin: join_type=..., on=[(a@0, b1@1)]
                                          RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1, maintains_sort_order=true
                                            SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
                                              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                          RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=1, maintains_sort_order=true
                                            SortExec: expr=[b1@1 ASC], preserve_partitioning=[false]
                                              ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]
                                                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=1, maintains_sort_order=true
                                    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
                                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
                                ");
                            }
                            // this match arm cannot be reached
                            _ => unreachable!()
                        }
                    }
                    _ => {}
                }
            });
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
    let plan_distrib = test_config.to_plan(join.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib, @r"
SortMergeJoin: join_type=Inner, on=[(b3@1, b2@1), (a3@0, a2@0)]
  SortExec: expr=[b3@1 ASC, a3@0 ASC], preserve_partitioning=[true]
    ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]
      ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]
        AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]
          RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10
            AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]
              RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  SortExec: expr=[b2@1 ASC, a2@0 ASC], preserve_partitioning=[true]
    ProjectionExec: expr=[a@1 as a2, b@0 as b2]
      AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]
        RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10
          AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]
            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let plan_sort = test_config.to_plan(join, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort, @r"
    SortMergeJoin: join_type=Inner, on=[(b3@1, b2@1), (a3@0, a2@0)]
      RepartitionExec: partitioning=Hash([b3@1, a3@0], 10), input_partitions=1, maintains_sort_order=true
        SortExec: expr=[b3@1 ASC, a3@0 ASC], preserve_partitioning=[false]
          CoalescePartitionsExec
            ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]
              ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]
                AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]
                  RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10
                    AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]
                      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      RepartitionExec: partitioning=Hash([b2@1, a2@0], 10), input_partitions=1, maintains_sort_order=true
        SortExec: expr=[b2@1 ASC, a2@0 ASC], preserve_partitioning=[false]
          CoalescePartitionsExec
            ProjectionExec: expr=[a@1 as a2, b@0 as b2]
              AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]
                RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10
                  AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]
                    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");

    Ok(())
}

#[test]
fn merge_does_not_need_sort() -> Result<()> {
    // see https://github.com/apache/datafusion/issues/4331
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();

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
    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(exec.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                    @r"
SortPreservingMergeExec: [a@0 ASC]
  CoalesceBatchesExec: target_batch_size=4096
    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
");

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    //
    // In this case preserving ordering through order preserving operators is not desirable
    // (according to flag: PREFER_EXISTING_SORT)
    // hence in this case ordering lost during CoalescePartitionsExec and re-introduced with
    // SortExec at the top.
    let plan_sort = test_config.to_plan(exec, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort,
                                                                                    @r"
SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    CoalesceBatchesExec: target_batch_size=4096
      DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
");

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
    let plan = UnionExec::try_new(vec![left, right])?;

    // final agg
    let plan =
        aggregate_exec_with_alias(plan, vec![("a1".to_string(), "a2".to_string())]);

    // Only two RepartitionExecs added, no final RepartitionExec required
    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
        @r"
    AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]
      AggregateExec: mode=Partial, gby=[a1@0 as a2], aggr=[]
        InterleaveExec
          AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]
            RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10
              AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
          AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]
            RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10
              AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

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
    let plan = UnionExec::try_new(vec![left, right])?;

    // final agg
    let plan =
        aggregate_exec_with_alias(plan, vec![("a1".to_string(), "a2".to_string())]);

    // Only two RepartitionExecs added, no final RepartitionExec required
    // TestConfig: Prefer existing union.
    let test_config = TestConfig::default().with_prefer_existing_union();

    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
        @r"
    AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]
      RepartitionExec: partitioning=Hash([a2@0], 10), input_partitions=20
        AggregateExec: mode=Partial, gby=[a1@0 as a2], aggr=[]
          UnionExec
            AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]
              RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10
                AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]
                  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
            AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]
              RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10
                AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]
                  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn added_repartition_to_single_partition() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan = aggregate_exec_with_alias(parquet_exec(), alias);

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
        @r"
    AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10
        AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_deepest_node() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan = aggregate_exec_with_alias(filter_exec(parquet_exec()), alias);

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
        @r"
    AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10
        AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
          FilterExec: c@2 = 0
            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_unsorted_limit() -> Result<()> {
    let plan = limit_exec(filter_exec(parquet_exec()));

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
        @r"
    GlobalLimitExec: skip=0, fetch=100
      CoalescePartitionsExec
        LocalLimitExec: fetch=100
          FilterExec: c@2 = 0
            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
              DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
    // nothing sorts the data, so the local limit doesn't require sorted data either
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_sorted_limit() -> Result<()> {
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let plan = limit_exec(sort_exec(sort_key, parquet_exec()));

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
GlobalLimitExec: skip=0, fetch=100
  LocalLimitExec: fetch=100
    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // data is sorted so can't repartition here
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_sorted_limit_with_filter() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let plan = sort_required_exec_with_req(
        filter_exec(sort_exec(sort_key.clone(), parquet_exec())),
        sort_key,
    );

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
    SortRequiredExec: [c@2 ASC]
      FilterExec: c@2 = 0
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
            DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
    // We can use repartition here, ordering requirement by SortRequiredExec
    // is still satisfied.
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_ignores_limit() -> Result<()> {
    let alias = vec![("a".to_string(), "a".to_string())];
    let plan = aggregate_exec_with_alias(
        limit_exec(filter_exec(limit_exec(parquet_exec()))),
        alias,
    );

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        GlobalLimitExec: skip=0, fetch=100
          CoalescePartitionsExec
            LocalLimitExec: fetch=100
              FilterExec: c@2 = 0
                RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                  GlobalLimitExec: skip=0, fetch=100
                    LocalLimitExec: fetch=100
                      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // repartition should happen prior to the filter to maximize parallelism
    // Expect no repartition to happen for local limit (DataSourceExec)

    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_ignores_union() -> Result<()> {
    let plan = union_exec(vec![parquet_exec(); 5]);

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
UnionExec
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // Expect no repartition of DataSourceExec
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_through_sort_preserving_merge() -> Result<()> {
    // sort preserving merge with non-sorted input
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let plan = sort_preserving_merge_exec(sort_key, parquet_exec());

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_ignores_sort_preserving_merge() -> Result<()> {
    // sort preserving merge already sorted input,
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let plan = sort_preserving_merge_exec(
        sort_key.clone(),
        parquet_exec_multiple_sorted(vec![sort_key]),
    );

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    // Test: run EnforceDistribution, then EnforceSort
    assert_plan!(plan_distrib,
                                                                                        @r"
SortPreservingMergeExec: [c@2 ASC]
  DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    // should not sort (as the data was already sorted)
    // should not repartition, since increased parallelism is not beneficial for SortPReservingMerge

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort,
                                                                                        @r"
SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");

    Ok(())
}

#[test]
fn repartition_ignores_sort_preserving_merge_with_union() -> Result<()> {
    // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = union_exec(vec![
        parquet_exec_with_sort(schema, vec![sort_key.clone()]);
        2
    ]);
    let plan = sort_preserving_merge_exec(sort_key, input);

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    // Test: run EnforceDistribution, then EnforceSort.
    assert_plan!(plan_distrib,
                                                                                        @r"
SortPreservingMergeExec: [c@2 ASC]
  UnionExec
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    //
    // should not repartition / sort (as the data was already sorted)

    // test: result IS DIFFERENT, if EnforceSorting is run first:
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort,
                                                                                        @r"
SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    UnionExec
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");

    Ok(())
}

/// These test cases use [`TestConfig::with_prefer_existing_sort`].
#[test]
fn repartition_does_not_destroy_sort() -> Result<()> {
    //  SortRequired
    //    Parquet(sorted)
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("d", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let plan = sort_required_exec_with_req(
        filter_exec(parquet_exec_with_sort(schema, vec![sort_key.clone()])),
        sort_key,
    );

    // TestConfig: Prefer existing sort.
    let test_config = TestConfig::default().with_prefer_existing_sort();

    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
    SortRequiredExec: [d@3 ASC]
      FilterExec: c@2 = 0
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[d@3 ASC], file_type=parquet
    ");
    // during repartitioning ordering is preserved
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

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
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input1 = sort_required_exec_with_req(
        parquet_exec_with_sort(schema, vec![sort_key.clone()]),
        sort_key,
    );
    let input2 = filter_exec(parquet_exec());
    let plan = union_exec(vec![input1, input2]);

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
UnionExec
  SortRequiredExec: [c@2 ASC]
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
  FilterExec: c@2 = 0
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // union input 1: no repartitioning
    // union input 2: should repartition
    //
    // should not repartition below the SortRequired as that
    // branch doesn't benefit from increased parallelism

    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_transitively_with_projection() -> Result<()> {
    let schema = schema();
    let proj_exprs = vec![ProjectionExpr {
        expr: Arc::new(BinaryExpr::new(
            col("a", &schema)?,
            Operator::Plus,
            col("b", &schema)?,
        )) as _,
        alias: "sum".to_string(),
    }];
    // non sorted input
    let proj = Arc::new(ProjectionExec::try_new(proj_exprs, parquet_exec())?);
    let sort_key = [PhysicalSortExpr {
        expr: col("sum", &proj.schema())?,
        options: SortOptions::default(),
    }]
    .into();
    let plan = sort_preserving_merge_exec(sort_key, proj);

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
SortPreservingMergeExec: [sum@0 ASC]
  SortExec: expr=[sum@0 ASC], preserve_partitioning=[true]
    ProjectionExec: expr=[a@0 + b@1 as sum]
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort,
                                                                                        @r"
SortExec: expr=[sum@0 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    ProjectionExec: expr=[a@0 + b@1 as sum]
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // Since this projection is not trivial, increasing parallelism is beneficial

    Ok(())
}

#[test]
fn repartition_ignores_transitively_with_projection() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
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

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
SortRequiredExec: [c@2 ASC]
  ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]
    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    // Since this projection is trivial, increasing parallelism is not beneficial

    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_transitively_past_sort_with_projection() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let alias = vec![
        ("a".to_string(), "a".to_string()),
        ("b".to_string(), "b".to_string()),
        ("c".to_string(), "c".to_string()),
    ];
    let plan = sort_preserving_merge_exec(
        sort_key.clone(),
        sort_exec_with_preserve_partitioning(
            sort_key,
            projection_exec_with_alias(parquet_exec(), alias),
        ),
    );

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
  ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // Since this projection is trivial, increasing parallelism is not beneficial
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn repartition_transitively_past_sort_with_filter() -> Result<()> {
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let plan = sort_exec(sort_key, filter_exec(parquet_exec()));

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
SortPreservingMergeExec: [a@0 ASC]
  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
    FilterExec: c@2 = 0
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    // Expect repartition on the input to the sort (as it can benefit from additional parallelism)

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort,
                                                                                        @r"
SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    FilterExec: c@2 = 0
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // Expect repartition on the input of the filter (as it can benefit from additional parallelism)

    Ok(())
}

#[test]
#[cfg(feature = "parquet")]
fn repartition_transitively_past_sort_with_projection_and_filter() -> Result<()> {
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
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
    );

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
SortPreservingMergeExec: [a@0 ASC]
  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
    ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]
      FilterExec: c@2 = 0
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
    // repartition is lowest down

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort,
                                                                                        @r"
SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]
      FilterExec: c@2 = 0
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

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
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_parquet_distrib,
                                                                                        @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      DataSourceExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e], file_type=parquet
");
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_csv_distrib,
                                                                                        @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      DataSourceExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
");
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

    Ok(())
}

#[test]
fn parallelization_multiple_files() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();

    let plan = filter_exec(parquet_exec_multiple_sorted(vec![sort_key.clone()]));
    let plan = sort_required_exec_with_req(plan, sort_key);

    let test_config = TestConfig::default()
        .with_prefer_existing_sort()
        .with_prefer_repartition_file_scans(1);

    // The groups must have only contiguous ranges of rows from the same file
    // if any group has rows from multiple files, the data is no longer sorted destroyed
    // https://github.com/apache/datafusion/issues/8451
    let test_config_concurrency_3 =
        test_config.clone().with_query_execution_partitions(3);
    let plan_3_distrib =
        test_config_concurrency_3.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_3_distrib,
                                                                                        @r"
SortRequiredExec: [a@0 ASC]
  FilterExec: c@2 = 0
    DataSourceExec: file_groups={3 groups: [[x:0..50], [y:0..100], [x:50..100]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
");
    let plan_3_sort =
        test_config_concurrency_3.to_plan(plan.clone(), &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_3_distrib, plan_3_sort);

    let test_config_concurrency_8 = test_config.with_query_execution_partitions(8);
    let plan_8_distrib =
        test_config_concurrency_8.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_8_distrib,
                                                                                        @r"
SortRequiredExec: [a@0 ASC]
  FilterExec: c@2 = 0
    DataSourceExec: file_groups={8 groups: [[x:0..25], [y:0..25], [x:25..50], [y:25..50], [x:50..75], [y:50..75], [x:75..100], [y:75..100]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
");
    let plan_8_sort = test_config_concurrency_8.to_plan(plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_8_distrib, plan_8_sort);

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

    #[rustfmt::skip]
    insta::allow_duplicates! {
        for compression_type in compression_types {
            let plan = aggregate_exec_with_alias(
                DataSourceExec::from_data_source(
                    FileScanConfigBuilder::new(ObjectStoreUrl::parse("test:///").unwrap(), {
                        let options = CsvOptions {
                            has_header: Some(false),
                            delimiter: b',',
                            quote: b'"',
                            ..Default::default()
                        };
                        Arc::new(CsvSource::new(schema()).with_csv_options(options))
                    })
                    .with_file(PartitionedFile::new("x".to_string(), 100))
                    .with_file_compression_type(compression_type)
                    .build(),
                ),
                vec![("a".to_string(), "a".to_string())],
            );
            let test_config = TestConfig::default()
                .with_query_execution_partitions(2)
                .with_prefer_repartition_file_scans(10);

            let plan_distrib = test_config.to_plan(plan.clone(), &DISTRIB_DISTRIB_SORT);
            if compression_type.is_compressed() {
                // Compressed files cannot be partitioned
                assert_plan!(plan_distrib,
                    @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
");
            } else {
                // Uncompressed files can be partitioned
                assert_plan!(plan_distrib,
                    @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      DataSourceExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
");
            }

            let plan_sort = test_config.to_plan(plan, &SORT_DISTRIB_DISTRIB);
            assert_plan!(plan_distrib, plan_sort);
        }
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
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_parquet_distrib,
                                                                                    @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      DataSourceExec: file_groups={2 groups: [[x:0..100], [y:0..100]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // Plan already has two partitions
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_csv_distrib, @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      DataSourceExec: file_groups={2 groups: [[x:0..100], [y:0..100]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
");
    // Plan already has two partitions
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

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
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    // Multiple source files split across partitions
    assert_plan!(plan_parquet_distrib,
                                                                                    @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      DataSourceExec: file_groups={4 groups: [[x:0..50], [x:50..100], [y:0..50], [y:50..100]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // Multiple source files split across partitions
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    // Multiple source files split across partitions
    assert_plan!(plan_csv_distrib, @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
      DataSourceExec: file_groups={4 groups: [[x:0..50], [x:50..100], [y:0..50], [y:50..100]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
");
    // Multiple source files split across partitions
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

    Ok(())
}

#[test]
fn parallelization_sorted_limit() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let plan_parquet = limit_exec(sort_exec(sort_key.clone(), parquet_exec()));
    let plan_csv = limit_exec(sort_exec(sort_key, csv_exec()));

    let test_config = TestConfig::default();

    // Test: with parquet
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_parquet_distrib, @r"
GlobalLimitExec: skip=0, fetch=100
  LocalLimitExec: fetch=100
    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // data is sorted so can't repartition here
    // Doesn't parallelize for SortExec without preserve_partitioning
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_csv_distrib,
        @r"
GlobalLimitExec: skip=0, fetch=100
  LocalLimitExec: fetch=100
    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
");
    // data is sorted so can't repartition here
    // Doesn't parallelize for SortExec without preserve_partitioning
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

    Ok(())
}

#[test]
fn parallelization_limit_with_filter() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let plan_parquet =
        limit_exec(filter_exec(sort_exec(sort_key.clone(), parquet_exec())));
    let plan_csv = limit_exec(filter_exec(sort_exec(sort_key, csv_exec())));

    let test_config = TestConfig::default();

    // Test: with parquet
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    // even though data is sorted, we can use repartition here. Since
    // ordering is not used in subsequent stages anyway.
    // SortExec doesn't benefit from input partitioning
    assert_plan!(plan_parquet_distrib,
        @r"
    GlobalLimitExec: skip=0, fetch=100
      CoalescePartitionsExec
        LocalLimitExec: fetch=100
          FilterExec: c@2 = 0
            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
              SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    // even though data is sorted, we can use repartition here. Since
    // ordering is not used in subsequent stages anyway.
    // SortExec doesn't benefit from input partitioning
    assert_plan!(plan_csv_distrib,
                                                                                    @r"
    GlobalLimitExec: skip=0, fetch=100
      CoalescePartitionsExec
        LocalLimitExec: fetch=100
          FilterExec: c@2 = 0
            RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
              SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
                DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
    ");
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

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
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_parquet_distrib,
        @r"
    AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10
        AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
            GlobalLimitExec: skip=0, fetch=100
              CoalescePartitionsExec
                LocalLimitExec: fetch=100
                  FilterExec: c@2 = 0
                    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                      GlobalLimitExec: skip=0, fetch=100
                        LocalLimitExec: fetch=100
                          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
    // repartition should happen prior to the filter to maximize parallelism
    // Limit doesn't benefit from input partitioning - no parallelism
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_csv_distrib,
        @r"
    AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
      RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10
        AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
            GlobalLimitExec: skip=0, fetch=100
              CoalescePartitionsExec
                LocalLimitExec: fetch=100
                  FilterExec: c@2 = 0
                    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
                      GlobalLimitExec: skip=0, fetch=100
                        LocalLimitExec: fetch=100
                          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
    ");
    // repartition should happen prior to the filter to maximize parallelism
    // Limit doesn't benefit from input partitioning - no parallelism
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

    Ok(())
}

#[test]
fn parallelization_union_inputs() -> Result<()> {
    let plan_parquet = union_exec(vec![parquet_exec(); 5]);
    let plan_csv = union_exec(vec![csv_exec(); 5]);

    let test_config = TestConfig::default();

    // Test: with parquet
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_parquet_distrib,
                                                                                    @r"
UnionExec
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    // Union doesn't benefit from input partitioning - no parallelism
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_csv_distrib,
                                                                                    @r"
UnionExec
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false
");
    // Union doesn't benefit from input partitioning - no parallelism
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

    Ok(())
}

#[test]
fn parallelization_prior_to_sort_preserving_merge() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    // sort preserving merge already sorted input,
    let plan_parquet = sort_preserving_merge_exec(
        sort_key.clone(),
        parquet_exec_with_sort(schema, vec![sort_key.clone()]),
    );
    let plan_csv =
        sort_preserving_merge_exec(sort_key.clone(), csv_exec_with_sort(vec![sort_key]));

    let test_config = TestConfig::default();

    // Expected Outcome:
    // parallelization is not beneficial for SortPreservingMerge

    // Test: with parquet
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_parquet_distrib,
        @"DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet"
    );
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_csv_distrib,
        @"DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false"
    );
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

    Ok(())
}

#[test]
fn parallelization_sort_preserving_merge_with_union() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
    let input_parquet =
        union_exec(vec![
            parquet_exec_with_sort(schema, vec![sort_key.clone()]);
            2
        ]);
    let input_csv = union_exec(vec![csv_exec_with_sort(vec![sort_key.clone()]); 2]);
    let plan_parquet = sort_preserving_merge_exec(sort_key.clone(), input_parquet);
    let plan_csv = sort_preserving_merge_exec(sort_key, input_csv);

    let test_config = TestConfig::default();

    // Expected Outcome:
    // should not repartition (union doesn't benefit from increased parallelism)
    // should not sort (as the data was already sorted)

    // Test: with parquet
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_parquet_distrib,
        @r"
    SortPreservingMergeExec: [c@2 ASC]
      UnionExec
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
    ");
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_sort,
        @r"
    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
      CoalescePartitionsExec
        UnionExec
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
    ");
    // no SPM
    // has coalesce

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_csv_distrib,
        @r"
    SortPreservingMergeExec: [c@2 ASC]
      UnionExec
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false
    ");
    let plan_csv_sort = test_config.to_plan(plan_csv.clone(), &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_sort,
        @r"
    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
      CoalescePartitionsExec
        UnionExec
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false
    ");
    // no SPM
    // has coalesce

    Ok(())
}

#[test]
fn parallelization_does_not_benefit() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    //  SortRequired
    //    Parquet(sorted)
    let plan_parquet = sort_required_exec_with_req(
        parquet_exec_with_sort(schema, vec![sort_key.clone()]),
        sort_key.clone(),
    );
    let plan_csv =
        sort_required_exec_with_req(csv_exec_with_sort(vec![sort_key.clone()]), sort_key);

    let test_config = TestConfig::default();

    // Expected Outcome:
    // no parallelization, because SortRequiredExec doesn't benefit from increased parallelism

    // Test: with parquet
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_parquet_distrib,
        @r"
    SortRequiredExec: [c@2 ASC]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
    ");
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    // Test: with csv
    let plan_csv_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_csv_distrib,
        @r"
    SortRequiredExec: [c@2 ASC]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false
    ");
    let plan_csv_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_csv_distrib, plan_csv_sort);

    Ok(())
}

#[test]
fn parallelization_ignores_transitively_with_projection_parquet() -> Result<()> {
    // sorted input
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();

    //Projection(a as a2, b as b2)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a2".to_string()),
        ("c".to_string(), "c2".to_string()),
    ];
    let proj_parquet = projection_exec_with_alias(
        parquet_exec_with_sort(schema, vec![sort_key]),
        alias_pairs,
    );
    let sort_key_after_projection = [PhysicalSortExpr {
        expr: col("c2", &proj_parquet.schema())?,
        options: SortOptions::default(),
    }]
    .into();
    let plan_parquet =
        sort_preserving_merge_exec(sort_key_after_projection, proj_parquet);

    assert_plan!(plan_parquet,
        @r"
    SortPreservingMergeExec: [c2@1 ASC]
      ProjectionExec: expr=[a@0 as a2, c@2 as c2]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
    ");

    let test_config = TestConfig::default();
    let plan_parquet_distrib =
        test_config.to_plan(plan_parquet.clone(), &DISTRIB_DISTRIB_SORT);
    // Expected Outcome:
    // data should not be repartitioned / resorted
    assert_plan!(plan_parquet_distrib,
                                                                                    @r"
ProjectionExec: expr=[a@0 as a2, c@2 as c2]
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    let plan_parquet_sort = test_config.to_plan(plan_parquet, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_parquet_distrib, plan_parquet_sort);

    Ok(())
}

#[test]
fn parallelization_ignores_transitively_with_projection_csv() -> Result<()> {
    // sorted input
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();

    //Projection(a as a2, b as b2)
    let alias_pairs: Vec<(String, String)> = vec![
        ("a".to_string(), "a2".to_string()),
        ("c".to_string(), "c2".to_string()),
    ];

    let proj_csv =
        projection_exec_with_alias(csv_exec_with_sort(vec![sort_key]), alias_pairs);
    let sort_key_after_projection = [PhysicalSortExpr {
        expr: col("c2", &proj_csv.schema())?,
        options: SortOptions::default(),
    }]
    .into();
    let plan_csv = sort_preserving_merge_exec(sort_key_after_projection, proj_csv);
    assert_plan!(plan_csv,
                                                                                        @r"
SortPreservingMergeExec: [c2@1 ASC]
  ProjectionExec: expr=[a@0 as a2, c@2 as c2]
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false
");

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(plan_csv.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
ProjectionExec: expr=[a@0 as a2, c@2 as c2]
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=csv, has_header=false
");
    // Expected Outcome:
    // data should not be repartitioned / resorted
    let plan_sort = test_config.to_plan(plan_csv, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn remove_redundant_roundrobins() -> Result<()> {
    let input = parquet_exec();
    let repartition = repartition_exec(repartition_exec(input));
    let physical_plan = repartition_exec(filter_exec(repartition));
    assert_plan!(physical_plan,
                                                                                        @r"
RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10
  FilterExec: c@2 = 0
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
FilterExec: c@2 = 0
  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

/// This test case uses [`TestConfig::with_prefer_existing_sort`].
#[test]
fn remove_unnecessary_spm_after_filter() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

    // TestConfig: Prefer existing sort.
    let test_config = TestConfig::default().with_prefer_existing_sort();

    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    // Expected Outcome:
    // Original plan expects its output to be ordered by c@2 ASC.
    // This is still satisfied since, after filter that column is constant.
    assert_plan!(plan_distrib,
                                                                                        @r"
CoalescePartitionsExec
  FilterExec: c@2 = 0
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2, preserve_order=true, sort_exprs=c@2 ASC
      DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

/// This test case uses [`TestConfig::with_prefer_existing_sort`].
#[test]
fn preserve_ordering_through_repartition() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("d", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

    // TestConfig: Prefer existing sort.
    let test_config = TestConfig::default().with_prefer_existing_sort();

    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
SortPreservingMergeExec: [d@3 ASC]
  FilterExec: c@2 = 0
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2, preserve_order=true, sort_exprs=d@3 ASC
      DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[d@3 ASC], file_type=parquet
");
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn do_not_preserve_ordering_through_repartition() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

    let test_config = TestConfig::default();

    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    // Test: run EnforceDistribution, then EnforceSort.
    assert_plan!(plan_distrib,
                                                                                        @r"
SortPreservingMergeExec: [a@0 ASC]
  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
    FilterExec: c@2 = 0
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2
        DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
");

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort,
                                                                                        @r"
SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    FilterExec: c@2 = 0
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2
        DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
");

    Ok(())
}

#[test]
fn no_need_for_sort_after_filter() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib, @r"
CoalescePartitionsExec
  FilterExec: c@2 = 0
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2
      DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);
    // After CoalescePartitionsExec c is still constant. Hence c@2 ASC ordering is already satisfied.
    // Since after this stage c is constant. c@2 ASC ordering is already satisfied.

    Ok(())
}

#[test]
fn do_not_preserve_ordering_through_repartition2() -> Result<()> {
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = parquet_exec_multiple_sorted(vec![sort_key]);

    let sort_req = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let physical_plan = sort_preserving_merge_exec(sort_req, filter_exec(input));

    let test_config = TestConfig::default();

    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    // Test: run EnforceDistribution, then EnforceSort.
    assert_plan!(plan_distrib,
                                                                                        @r"
SortPreservingMergeExec: [a@0 ASC]
  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
    FilterExec: c@2 = 0
      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2
        DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");

    // Test: result IS DIFFERENT, if EnforceSorting is run first:
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_sort,
                                                                                        @r"
SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
      FilterExec: c@2 = 0
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2
          DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");

    Ok(())
}

#[test]
fn do_not_preserve_ordering_through_repartition3() -> Result<()> {
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = parquet_exec_multiple_sorted(vec![sort_key]);
    let physical_plan = filter_exec(input);

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
FilterExec: c@2 = 0
  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2
    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn do_not_put_sort_when_input_is_invalid() -> Result<()> {
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = parquet_exec();
    let physical_plan = sort_required_exec_with_req(filter_exec(input), sort_key);
    // Ordering requirement of sort required exec is NOT satisfied
    // by existing ordering at the source.
    assert_plan!(physical_plan, @r"
SortRequiredExec: [a@0 ASC]
  FilterExec: c@2 = 0
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 10;
    config.optimizer.enable_round_robin_repartition = true;
    config.optimizer.prefer_existing_sort = false;
    let session_config = SessionConfig::from(config);
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let dist_plan =
        EnforceDistribution::new().optimize_plan(physical_plan, &optimizer_context)?;
    // Since at the start of the rule ordering requirement is not satisfied
    // EnforceDistribution rule doesn't satisfy this requirement either.
    assert_plan!(dist_plan, @r"
SortRequiredExec: [a@0 ASC]
  FilterExec: c@2 = 0
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    Ok(())
}

#[test]
fn put_sort_when_input_is_valid() -> Result<()> {
    let schema = schema();
    let sort_key: LexOrdering = [PhysicalSortExpr {
        expr: col("a", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
    let physical_plan = sort_required_exec_with_req(filter_exec(input), sort_key);

    // Ordering requirement of sort required exec is satisfied
    // by existing ordering at the source.
    assert_plan!(physical_plan, @r"
SortRequiredExec: [a@0 ASC]
  FilterExec: c@2 = 0
    DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
");

    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 10;
    config.optimizer.enable_round_robin_repartition = true;
    config.optimizer.prefer_existing_sort = false;
    let session_config = SessionConfig::from(config);
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let dist_plan =
        EnforceDistribution::new().optimize_plan(physical_plan, &optimizer_context)?;
    // Since at the start of the rule ordering requirement is satisfied
    // EnforceDistribution rule satisfy this requirement also.
    assert_plan!(dist_plan, @r"
SortRequiredExec: [a@0 ASC]
  FilterExec: c@2 = 0
    DataSourceExec: file_groups={10 groups: [[x:0..20], [y:0..20], [x:20..40], [y:20..40], [x:40..60], [y:40..60], [x:60..80], [y:60..80], [x:80..100], [y:80..100]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet
");

    Ok(())
}

#[test]
fn do_not_add_unnecessary_hash() -> Result<()> {
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let alias = vec![("a".to_string(), "a".to_string())];
    let input = parquet_exec_with_sort(schema, vec![sort_key]);
    let physical_plan = aggregate_exec_with_alias(input, alias);

    // TestConfig:
    // Make sure target partition number is 1. In this case hash repartition is unnecessary.
    let test_config = TestConfig::default().with_query_execution_partitions(1);

    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn do_not_add_unnecessary_hash2() -> Result<()> {
    let schema = schema();
    let sort_key = [PhysicalSortExpr {
        expr: col("c", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let alias = vec![("a".to_string(), "a".to_string())];
    let input = parquet_exec_multiple_sorted(vec![sort_key]);
    let aggregate = aggregate_exec_with_alias(input, alias.clone());
    let physical_plan = aggregate_exec_with_alias(aggregate, alias);

    // TestConfig:
    // Make sure target partition number is larger than 2 (e.g partition number at the source).
    let test_config = TestConfig::default().with_query_execution_partitions(4);

    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
    AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
      RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4
        AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]
          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            DataSourceExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], file_type=parquet
");
    // Since hash requirements of this operator is satisfied. There shouldn't be
    // a hash repartition here
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn optimize_away_unnecessary_repartition() -> Result<()> {
    let physical_plan = coalesce_partitions_exec(repartition_exec(parquet_exec()));
    assert_plan!(physical_plan,
                                                                                        @r"
CoalescePartitionsExec
  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

#[test]
fn optimize_away_unnecessary_repartition2() -> Result<()> {
    let physical_plan = filter_exec(repartition_exec(coalesce_partitions_exec(
        filter_exec(repartition_exec(parquet_exec())),
    )));
    assert_plan!(physical_plan,
                                                                                        @r"
FilterExec: c@2 = 0
  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
    CoalescePartitionsExec
      FilterExec: c@2 = 0
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    let test_config = TestConfig::default();
    let plan_distrib = test_config.to_plan(physical_plan.clone(), &DISTRIB_DISTRIB_SORT);
    assert_plan!(plan_distrib,
                                                                                        @r"
FilterExec: c@2 = 0
  FilterExec: c@2 = 0
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");
    let plan_sort = test_config.to_plan(physical_plan, &SORT_DISTRIB_DISTRIB);
    assert_plan!(plan_distrib, plan_sort);

    Ok(())
}

/// Ensures that `DataSourceExec` has been repartitioned into `target_partitions` file groups
#[tokio::test]
async fn test_distribute_sort_parquet() -> Result<()> {
    let test_config: TestConfig =
        TestConfig::default().with_prefer_repartition_file_scans(1000);
    assert!(
        test_config.config.optimizer.repartition_file_scans,
        "should enable scans to be repartitioned"
    );

    let schema = schema();
    let sort_key = [PhysicalSortExpr::new_default(col("c", &schema)?)].into();
    let physical_plan = sort_exec(sort_key, parquet_exec_with_stats(10000 * 8192));

    // prior to optimization, this is the starting plan
    assert_plan!(physical_plan,
                                                                                        @r"
SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
");

    // what the enforce distribution run does.
    let plan_distribution =
        test_config.to_plan(physical_plan.clone(), &[Run::Distribution]);
    assert_plan!(plan_distribution,
                                                                                        @r"
SortExec: expr=[c@2 ASC], preserve_partitioning=[false]
  CoalescePartitionsExec
    DataSourceExec: file_groups={10 groups: [[x:0..8192000], [x:8192000..16384000], [x:16384000..24576000], [x:24576000..32768000], [x:32768000..40960000], [x:40960000..49152000], [x:49152000..57344000], [x:57344000..65536000], [x:65536000..73728000], [x:73728000..81920000]]}, projection=[a, b, c, d, e], file_type=parquet
");

    // what the sort parallelization (in enforce sorting), does after the enforce distribution changes
    let plan_both =
        test_config.to_plan(physical_plan, &[Run::Distribution, Run::Sorting]);
    assert_plan!(plan_both,
                                                                                        @r"
SortPreservingMergeExec: [c@2 ASC]
  SortExec: expr=[c@2 ASC], preserve_partitioning=[true]
    DataSourceExec: file_groups={10 groups: [[x:0..8192000], [x:8192000..16384000], [x:16384000..24576000], [x:24576000..32768000], [x:32768000..40960000], [x:40960000..49152000], [x:49152000..57344000], [x:57344000..65536000], [x:65536000..73728000], [x:73728000..81920000]]}, projection=[a, b, c, d, e], file_type=parquet
");
    Ok(())
}

/// Ensures that `DataSourceExec` has been repartitioned into `target_partitions` memtable groups
#[tokio::test]
async fn test_distribute_sort_memtable() -> Result<()> {
    let test_config: TestConfig =
        TestConfig::default().with_prefer_repartition_file_scans(1000);
    assert!(
        test_config.config.optimizer.repartition_file_scans,
        "should enable scans to be repartitioned"
    );

    let mem_table = create_memtable()?;
    let session_config = SessionConfig::new()
        .with_repartition_file_min_size(1000)
        .with_target_partitions(3);
    let ctx = SessionContext::new_with_config(session_config);
    ctx.register_table("users", Arc::new(mem_table))?;

    let dataframe = ctx.sql("SELECT * FROM users order by id;").await?;
    let physical_plan = dataframe.create_physical_plan().await?;

    // this is the final, optimized plan
    assert_plan!(physical_plan,
                                                                                        @r"
SortPreservingMergeExec: [id@0 ASC NULLS LAST]
  SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[true]
    DataSourceExec: partitions=3, partition_sizes=[34, 33, 33]
");

    Ok(())
}

/// Create a [`MemTable`] with 100 batches of 8192 rows each, in 1 partition
fn create_memtable() -> Result<MemTable> {
    let mut batches = Vec::with_capacity(100);
    for _ in 0..100 {
        batches.push(create_record_batch()?);
    }
    let partitions = vec![batches];
    MemTable::try_new(get_schema(), partitions)
}

fn create_record_batch() -> Result<RecordBatch> {
    let id_array = UInt8Array::from(vec![1; 8192]);
    let account_array = UInt64Array::from(vec![9000; 8192]);

    Ok(RecordBatch::try_new(
        get_schema(),
        vec![Arc::new(id_array), Arc::new(account_array)],
    )
    .unwrap())
}

fn get_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::UInt8, false),
        Field::new("bank_account", DataType::UInt64, true),
    ]))
}
#[test]
fn test_replace_order_preserving_variants_with_fetch() -> Result<()> {
    // Create a base plan
    let parquet_exec = parquet_exec();

    let sort_expr = PhysicalSortExpr::new_default(Arc::new(Column::new("id", 0)));

    // Create a SortPreservingMergeExec with fetch=5
    let spm_exec = Arc::new(
        SortPreservingMergeExec::new([sort_expr].into(), parquet_exec.clone())
            .with_fetch(Some(5)),
    );

    // Create distribution context
    let dist_context = DistributionContext::new(
        spm_exec,
        true,
        vec![DistributionContext::new(parquet_exec, false, vec![])],
    );

    // Apply the function
    let result = replace_order_preserving_variants(dist_context)?;

    // Verify the plan was transformed to CoalescePartitionsExec
    result
        .plan
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .expect("Expected CoalescePartitionsExec");

    // Verify fetch was preserved
    assert_eq!(
        result.plan.fetch(),
        Some(5),
        "Fetch value was not preserved after transformation"
    );

    Ok(())
}
