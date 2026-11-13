// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::array::record_batch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Operator, TableType};
use datafusion::physical_expr::expressions::{binary, col, lit, negative};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, InputDistributionRequirements, PlanProperties,
    ReplaceChildrenOptions,
};
use datafusion::prelude::SessionContext;

/// Register custom plans whose native implementations are hidden from the optimizer.
pub(super) fn register_custom_sort_pushdown(ctx: &SessionContext) -> Result<()> {
    let batch = record_batch!(("a", Int32, [2, 1, 3]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let projection = ProjectionExec::try_new(
        [(negative(col("a", &schema)?, &schema)?, "a".to_string())],
        input,
    )?;
    ctx.register_table(
        "custom_negated_projection",
        Arc::new(CustomExec(Arc::new(projection))),
    )?;

    let batch = record_batch!(("a", Int32, [2, 1, 3]), ("b", Int32, [10, 30, 20]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let projection = ProjectionExec::try_new(
        [
            (col("b", &schema)?, "a".to_string()),
            (col("a", &schema)?, "b".to_string()),
        ],
        input,
    )?;
    ctx.register_table(
        "custom_reordered_projection",
        Arc::new(CustomExec(Arc::new(projection))),
    )?;

    let batch = record_batch!(("t", Int32, [1, 2, 3]), ("x", Int32, [0, 2, 4]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let predicate = binary(col("x", &schema)?, Operator::NotEq, lit(0i32), &schema)?;
    ctx.register_table(
        "custom_filtered_input",
        Arc::new(CustomExec(Arc::new(FilterExec::try_new(predicate, input)?))),
    )?;

    let batch = record_batch!(("a", Int32, [1, 2, 3]), ("b", Int32, [30, 20, 10]))?;
    let schema = batch.schema();
    let ordering =
        [PhysicalSortExpr::new_default(col("a", &schema)?).nulls_last()].into();
    let input = Arc::new(
        TestMemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![ordering])?,
    );
    ctx.register_table("custom_ordered_input", Arc::new(CustomExec(input)))?;

    // The custom node hides the final sort key while selecting its first row.
    // Refining its visible ordering must still preserve that hidden key.
    let batch = record_batch!(
        ("a", Int32, [1, 1]),
        ("c", Int32, [10, 20]),
        ("b", Int32, [2, 1])
    )?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let ordering = [
        PhysicalSortExpr::new_default(col("a", &schema)?).nulls_last(),
        PhysicalSortExpr::new_default(col("b", &schema)?).nulls_last(),
    ]
    .into();
    // Keep the selection ordering through earlier sort-removal passes.
    let input = Arc::new(SortExec::new(ordering, input).with_fetch(Some(2)));
    let projected_limit = FilterExecBuilder::new(lit(true), input)
        .apply_projection(Some(vec![0, 1]))?
        .with_fetch(Some(1))
        .build()?;
    ctx.register_table(
        "custom_projected_limit",
        Arc::new(CustomExec(Arc::new(projected_limit))),
    )?;

    // The output fetch excludes the skipped row, so it cannot also bound input.
    let batch = record_batch!(("a", Int32, [2, 1, 3]), ("b", Int32, [20, 30, 10]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let ordering =
        [PhysicalSortExpr::new_default(col("a", &schema)?).nulls_last()].into();
    let input = Arc::new(SortExec::new(ordering, input).with_fetch(Some(3)));
    let limit = GlobalLimitExec::new(input, 1, Some(1));
    ctx.register_table("custom_offset_limit", Arc::new(CustomExec(Arc::new(limit))))?;
    Ok(())
}

/// Replace a redundant sort on the ordered test input with a custom limit, so SQL
/// `ORDER BY ... LIMIT` exercises the generic sort-pushdown path.
#[derive(Debug)]
pub(super) struct CustomLimitRule;

impl PhysicalOptimizerRule for CustomLimitRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(sort) = plan.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(fetch) = sort.fetch() else {
                return Ok(Transformed::no(plan));
            };
            let input = sort.input();
            let source = input
                .downcast_ref::<ProjectionExec>()
                .map_or(input, |projection| projection.input());
            let Some(source) = source.downcast_ref::<CustomExec>() else {
                return Ok(Transformed::no(plan));
            };
            // Only the dedicated ordered source is a wrapped memory scan.
            if !source.0.is::<TestMemoryExec>()
                || input.output_partitioning().partition_count() != 1
                || !input
                    .equivalence_properties()
                    .ordering_satisfy(sort.expr().clone())?
            {
                return Ok(Transformed::no(plan));
            }
            let limit = LocalLimitExec::new(Arc::clone(input), fetch);
            let plan: Arc<dyn ExecutionPlan> = Arc::new(CustomExec(Arc::new(limit)));
            Ok(Transformed::yes(plan))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "CustomLimitRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Exposes a native plan's children and properties under a distinct plan type,
/// exercising the generic custom-plan sort pushdown path.
#[derive(Debug, Clone)]
struct CustomExec(Arc<dyn ExecutionPlan>);

#[async_trait]
impl TableProvider for CustomExec {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&[usize]>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(self.clone());
        // Keep identity projections out of the plan so sort pushdown reaches
        // this custom node before native projection-specific handling.
        if let Some(projection) = projection
            && !projection
                .iter()
                .copied()
                .eq(0..self.0.schema().fields().len())
        {
            let expressions = ProjectionExprs::from_indices(projection, &self.0.schema());
            Ok(Arc::new(ProjectionExec::try_new(
                expressions.iter().cloned(),
                plan,
            )?))
        } else {
            Ok(plan)
        }
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, display_type: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Custom")?;
        self.0.fmt_as(display_type, f)
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &str {
        "CustomExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.0.children()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.0.maintains_input_order()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.0.input_distribution_requirements()
    }

    fn fetch(&self) -> Option<usize> {
        self.0.fetch()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(Arc::clone(&self.0).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.0.apply_expressions(f)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }
}
