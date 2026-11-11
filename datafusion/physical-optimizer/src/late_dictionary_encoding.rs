// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{Column, CompactDictionaryExpr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;

use crate::{PhysicalOptimizerContext, PhysicalOptimizerRule};

/// Compacts dictionary group keys after a selective filter, so downstream
/// aggregation and repartitioning do not repeatedly process unreferenced values.
#[derive(Debug, Default)]
pub struct LateDictionaryEncoding;

impl LateDictionaryEncoding {
    pub fn new() -> Self {
        Self
    }

    fn follows_filter(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.is::<FilterExec>() {
            return true;
        }
        if plan.is::<ProjectionExec>() {
            return Self::follows_filter(plan.children()[0]);
        }
        false
    }

    #[expect(deprecated)]
    fn rewrite(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(aggregate) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(plan));
        };
        if !matches!(
            aggregate.mode(),
            AggregateMode::Partial
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned
        ) || aggregate.group_expr().has_grouping_set()
            || !aggregate.group_expr().null_expr().is_empty()
            || !Self::follows_filter(aggregate.input())
        {
            return Ok(Transformed::no(plan));
        }

        let input_schema = aggregate.input().schema();
        let mut rewritten = false;
        let group_expr = aggregate
            .group_expr()
            .expr()
            .iter()
            .map(|(expr, name)| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return (Arc::clone(expr), name.clone());
                };
                if !matches!(
                    input_schema.field(column.index()).data_type(),
                    DataType::Dictionary(_, _)
                ) {
                    return (Arc::clone(expr), name.clone());
                }
                rewritten = true;
                (
                    Arc::new(CompactDictionaryExpr::new(Arc::clone(expr)))
                        as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect::<Vec<_>>();
        if !rewritten {
            return Ok(Transformed::no(plan));
        }

        let rewritten = AggregateExec::try_new(
            *aggregate.mode(),
            PhysicalGroupBy::new_single(group_expr),
            aggregate.aggr_expr().to_vec(),
            aggregate.filter_expr().to_vec(),
            Arc::clone(aggregate.input()),
            Arc::clone(&aggregate.input_schema),
        )?
        .with_limit_options(aggregate.limit_options());
        Ok(Transformed::yes(Arc::new(rewritten)))
    }
}

impl PhysicalOptimizerRule for LateDictionaryEncoding {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(
            plan,
            &crate::optimizer::ConfigOnlyContext::new(config),
        )
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(Self::rewrite).data()
    }

    fn name(&self) -> &str {
        "LateDictionaryEncoding"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion_physical_expr::expressions::{Column, lit};
    use datafusion_physical_plan::empty::EmptyExec;

    #[test]
    fn compacts_dictionary_group_key_after_filter() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), input)?);
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("key", 0)),
                "key".to_string(),
            )]),
            vec![],
            vec![],
            Arc::clone(&input),
            input.schema(),
        )?);

        let optimized =
            LateDictionaryEncoding::new().optimize(aggregate, &ConfigOptions::new())?;
        let aggregate = optimized.downcast_ref::<AggregateExec>().unwrap();
        assert!(
            aggregate.group_expr().expr()[0]
                .0
                .is::<CompactDictionaryExpr>()
        );
        Ok(())
    }
}
