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

//! Collection of testing utility functions that are leveraged by the query optimizer rules

use std::sync::Arc;

use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::{FileScanConfig, ParquetExec};
use crate::datasource::stream::{StreamConfig, StreamTable};
use crate::error::Result;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::coalesce_batches::CoalesceBatchesExec;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils::{JoinFilter, JoinOn};
use crate::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::create_window_expr;
use crate::physical_plan::{ExecutionPlan, InputOrderMode, Partitioning};
use crate::prelude::{CsvReadOptions, SessionContext};

use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{JoinType, Statistics};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{AggregateFunction, WindowFrame, WindowFunctionDefinition};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_plan::displayable;
use datafusion_physical_plan::tree_node::PlanContext;

use async_trait::async_trait;

async fn register_current_csv(
    ctx: &SessionContext,
    table_name: &str,
    infinite: bool,
) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();
    let schema = crate::test_util::aggr_test_schema();
    let path = format!("{testdata}/csv/aggregate_test_100.csv");

    match infinite {
        true => {
            let config = StreamConfig::new_file(schema, path.into());
            ctx.register_table(table_name, Arc::new(StreamTable::new(Arc::new(config))))?;
        }
        false => {
            ctx.register_csv(table_name, &path, CsvReadOptions::new().schema(&schema))
                .await?;
        }
    }

    Ok(())
}

#[derive(Eq, PartialEq, Debug)]
pub enum SourceType {
    Unbounded,
    Bounded,
}

#[async_trait]
pub trait SqlTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()>;
    fn expect_fail(&self) -> bool;
}

/// [UnaryTestCase] is designed for single input [ExecutionPlan]s.
pub struct UnaryTestCase {
    pub(crate) source_type: SourceType,
    pub(crate) expect_fail: bool,
}

#[async_trait]
impl SqlTestCase for UnaryTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()> {
        let table_is_infinite = self.source_type == SourceType::Unbounded;
        register_current_csv(ctx, "test", table_is_infinite).await?;
        Ok(())
    }

    fn expect_fail(&self) -> bool {
        self.expect_fail
    }
}
/// [BinaryTestCase] is designed for binary input [ExecutionPlan]s.
pub struct BinaryTestCase {
    pub(crate) source_types: (SourceType, SourceType),
    pub(crate) expect_fail: bool,
}

#[async_trait]
impl SqlTestCase for BinaryTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()> {
        let left_table_is_infinite = self.source_types.0 == SourceType::Unbounded;
        let right_table_is_infinite = self.source_types.1 == SourceType::Unbounded;
        register_current_csv(ctx, "left", left_table_is_infinite).await?;
        register_current_csv(ctx, "right", right_table_is_infinite).await?;
        Ok(())
    }

    fn expect_fail(&self) -> bool {
        self.expect_fail
    }
}

pub struct QueryCase {
    pub(crate) sql: String,
    pub(crate) cases: Vec<Arc<dyn SqlTestCase>>,
    pub(crate) error_operator: String,
}

impl QueryCase {
    /// Run the test cases
    pub(crate) async fn run(&self) -> Result<()> {
        for case in &self.cases {
            let ctx = SessionContext::new();
            case.register_table(&ctx).await?;
            let error = if case.expect_fail() {
                Some(&self.error_operator)
            } else {
                None
            };
            self.run_case(ctx, error).await?;
        }
        Ok(())
    }
    async fn run_case(&self, ctx: SessionContext, error: Option<&String>) -> Result<()> {
        let dataframe = ctx.sql(self.sql.as_str()).await?;
        let plan = dataframe.create_physical_plan().await;
        if let Some(error) = error {
            let plan_error = plan.unwrap_err();
            assert!(
                plan_error.to_string().contains(error.as_str()),
                "plan_error: {:?} doesn't contain message: {:?}",
                plan_error,
                error.as_str()
            );
        } else {
            assert!(plan.is_ok())
        }
        Ok(())
    }
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
            false,
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

pub(crate) fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(MemoryExec::try_new(&[vec![]], schema.clone(), None).unwrap())
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
        PartitionMode::Partitioned,
        true,
    )?))
}

pub fn bounded_window_exec(
    col_name: &str,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs: Vec<_> = sort_exprs.into_iter().collect();
    let schema = input.schema();

    Arc::new(
        crate::physical_plan::windows::BoundedWindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunctionDefinition::AggregateFunction(AggregateFunction::Count),
                "count".to_owned(),
                &[col(col_name, &schema).unwrap()],
                &[],
                &sort_exprs,
                Arc::new(WindowFrame::new(Some(false))),
                schema.as_ref(),
                false,
            )
            .unwrap()],
            input.clone(),
            vec![],
            InputOrderMode::Sorted,
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
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();
    Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
}

/// Create a non sorted parquet exec
pub fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
    Arc::new(ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema: schema.clone(),
            file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
            statistics: Statistics::new_unknown(schema),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
        },
        None,
        None,
    ))
}

// Created a sorted parquet exec
pub fn parquet_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    Arc::new(ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema: schema.clone(),
            file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
            statistics: Statistics::new_unknown(schema),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![sort_exprs],
        },
        None,
        None,
    ))
}

pub fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
    Arc::new(UnionExec::new(input))
}

pub fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    global_limit_exec(local_limit_exec(input))
}

pub fn local_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(LocalLimitExec::new(input, 100))
}

pub fn global_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(input, 0, Some(100)))
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

pub fn coalesce_batches_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalesceBatchesExec::new(input, 128))
}

pub fn sort_exec(
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();
    Arc::new(SortExec::new(sort_exprs, input))
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
    context.transform_up(&|node| {
        let children_plans = node.plan.children();
        assert_eq!(node.children.len(), children_plans.len());
        for (child_plan, child_node) in children_plans.iter().zip(node.children.iter()) {
            assert_eq!(
                displayable(child_plan.as_ref()).one_line().to_string(),
                displayable(child_node.plan.as_ref()).one_line().to_string()
            );
        }
        Ok(Transformed::No(node))
    })
}
