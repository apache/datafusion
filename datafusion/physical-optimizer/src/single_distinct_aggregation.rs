// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Native grouped integer distinct counts over a single input partition.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, GroupClusteringMode, PhysicalGroupBy,
};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::PhysicalOptimizerRule;

/// Combines a count over distinct `(group, value)` pairs into one native
/// `COUNT(DISTINCT value)` aggregate when the input has one partition.
///
/// Runs after distribution enforcement and partial/final aggregate combination.
/// Aggregate stages separated by repartitioning retain the GROUP BY rewrite.
#[derive(Debug, Default)]
pub(crate) struct SingleDistinctAggregation {}

impl SingleDistinctAggregation {
    /// Create the single-partition distinct aggregation rule.
    pub(crate) fn new() -> Self {
        Self {}
    }

    fn complete_aggregate(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<AggregateExec>> {
        let Some(aggregate) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(None);
        };
        if matches!(
            aggregate.mode(),
            AggregateMode::Single | AggregateMode::SinglePartitioned
        ) {
            return Ok(Some(aggregate.clone()));
        }
        let combined = AggregateExec::try_combine_partial_final(Arc::clone(plan))?;
        if combined.transformed {
            Ok(combined.data.downcast_ref::<AggregateExec>().cloned())
        } else {
            Ok(None)
        }
    }

    fn fuse(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(outer) = Self::complete_aggregate(&plan)? else {
            return Ok(Transformed::no(plan));
        };
        if outer.group_expr().is_empty()
            || !outer.group_expr().is_single()
            || outer.aggr_expr().len() != 1
            || outer.filter_expr().iter().any(Option::is_some)
            || outer.limit_options().is_some()
            || !outer.dynamic_expressions_produced().is_empty()
        {
            return Ok(Transformed::no(plan));
        }
        let count = &outer.aggr_expr()[0];
        if count.fun() != count_udaf().as_ref()
            || count.is_distinct()
            || !count.order_bys().is_empty()
            || count.expressions().len() != 1
        {
            return Ok(Transformed::no(plan));
        }
        let Some(inner) = Self::complete_aggregate(outer.input())? else {
            return Ok(Transformed::no(plan));
        };
        if !inner.aggr_expr().is_empty()
            || !inner.group_expr().is_single()
            || inner.limit_options().is_some()
            || !inner.dynamic_expressions_produced().is_empty()
            || inner.input().output_partitioning().partition_count() != 1
            || inner.group_expr().expr().len() != outer.group_expr().expr().len() + 1
        {
            return Ok(Transformed::no(plan));
        }
        let args = count.expressions();
        let Some(value_column) = args[0].downcast_ref::<Column>() else {
            return Ok(Transformed::no(plan));
        };
        let Some((value, _)) = inner.group_expr().expr().get(value_column.index()) else {
            return Ok(Transformed::no(plan));
        };
        if !value
            .data_type(inner.input().schema().as_ref())?
            .is_integer()
        {
            return Ok(Transformed::no(plan));
        }
        let mut used = vec![false; inner.group_expr().expr().len()];
        used[value_column.index()] = true;
        let mut group_expr = Vec::with_capacity(outer.group_expr().expr().len());
        for (expr, name) in outer.group_expr().expr() {
            let Some(column) = expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(seen) = used.get_mut(column.index()) else {
                return Ok(Transformed::no(plan));
            };
            if *seen {
                return Ok(Transformed::no(plan));
            }
            *seen = true;
            group_expr.push((
                Arc::clone(&inner.group_expr().expr()[column.index()].0),
                name.clone(),
            ));
        }
        let input = Arc::clone(inner.input());
        let native_count =
            AggregateExprBuilder::new(count_udaf(), vec![Arc::clone(value)])
                .schema(input.schema())
                .alias(count.name())
                .human_display(format!("count(DISTINCT {value})"))
                .distinct()
                .build()?;
        let native = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(group_expr),
            vec![Arc::new(native_count)],
            vec![None],
            Arc::clone(&input),
            input.schema(),
        )?;
        if !matches!(native.group_clustering_mode(), GroupClusteringMode::None)
            || native.schema() != plan.schema()
        {
            return Ok(Transformed::no(plan));
        }
        Ok(Transformed::yes(Arc::new(native)))
    }
}

impl PhysicalOptimizerRule for SingleDistinctAggregation {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if config.optimizer.single_distinct_prefer_native {
            plan.transform_up(Self::fuse).data()
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "SingleDistinctAggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
