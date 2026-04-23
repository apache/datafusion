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

//! Physical optimizer rule that enables the precomputed-hash column on a
//! Partial `AggregateExec` whose output is immediately consumed by a
//! `RepartitionExec` with `Partitioning::Hash` over the same group columns.

use std::any::Any;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::repartition::RepartitionExec;

use crate::PhysicalOptimizerRule;

/// Enables [`AggregateExec::with_emit_group_hash`] on Partial aggregates whose
/// immediate downstream consumer is a `RepartitionExec` with
/// `Partitioning::Hash(exprs, _)` where `exprs` matches the Partial's group
/// columns exactly and in order.
#[derive(Default, Debug)]
pub struct EmitPartialAggregateHash {}

impl EmitPartialAggregateHash {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EmitPartialAggregateHash {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.execution.emit_aggregate_group_hash {
            return Ok(plan);
        }
        plan.transform_up(|plan| {
            let Some(repartition) = plan.downcast_ref::<RepartitionExec>() else {
                return Ok(Transformed::no(plan));
            };

            let Partitioning::Hash(hash_exprs, _) = repartition.partitioning() else {
                return Ok(Transformed::no(plan));
            };

            let input = repartition.input();
            let Some(partial) = input.downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(plan));
            };

            if *partial.mode() != AggregateMode::Partial
                || partial.emit_group_hash()
                || partial.group_expr().has_grouping_set()
            {
                return Ok(Transformed::no(plan));
            }

            if !hash_keys_match_group_columns(hash_exprs, partial) {
                return Ok(Transformed::no(plan));
            }

            let new_partial = Arc::new(partial.clone().with_emit_group_hash(true)?)
                as Arc<dyn ExecutionPlan>;
            let new_repartition = Arc::new(RepartitionExec::try_new(
                new_partial,
                repartition.partitioning().clone(),
            )?) as Arc<dyn ExecutionPlan>;

            Ok(Transformed::yes(new_repartition))
        })
        .data()
    }

    fn name(&self) -> &str {
        "EmitPartialAggregateHash"
    }

    fn schema_check(&self) -> bool {
        // We append a trailing column to the Partial's output schema; the
        // RepartitionExec passes it through. The Final's own output schema is
        // unaffected, but intermediate schemas do change — opt out of the
        // global schema equality check.
        false
    }
}

/// `true` when `hash_exprs` is exactly `[Column(i_0), Column(i_1), ...]`
/// matching, in order, the output-column indices that the Partial's group-by
/// emits (i.e. `0..num_group_cols`). Anything else disqualifies the match —
/// the precomputed hash covers only the group-value *arrays* emitted by the
/// Partial, so the partitioning keys must be the same projected columns.
fn hash_keys_match_group_columns(
    hash_exprs: &[Arc<dyn PhysicalExpr>],
    partial: &AggregateExec,
) -> bool {
    let group_exprs = partial.group_expr().expr();
    if hash_exprs.len() != group_exprs.len() {
        return false;
    }
    hash_exprs.iter().enumerate().all(|(i, expr)| {
        let any = expr.as_ref() as &dyn Any;
        any.downcast_ref::<Column>().is_some_and(|c| c.index() == i)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_plan::aggregates::{
        PRECOMPUTED_GROUP_HASH_COLUMN, PhysicalGroupBy,
    };
    use datafusion_physical_plan::empty::EmptyExec;

    fn config_enabled() -> ConfigOptions {
        let mut cfg = ConfigOptions::default();
        cfg.execution.emit_aggregate_group_hash = true;
        cfg
    }

    #[test]
    fn rule_is_disabled_by_default_config() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
        let partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![],
            vec![],
            input,
            Arc::clone(&schema),
        )?);
        let repartition: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            partial as Arc<dyn ExecutionPlan>,
            Partitioning::Hash(vec![col("a", &schema)?], 4),
        )?);
        let optimized = EmitPartialAggregateHash::new()
            .optimize(Arc::clone(&repartition), &ConfigOptions::default())?;
        let rep = optimized.downcast_ref::<RepartitionExec>().unwrap();
        let partial = rep.input().downcast_ref::<AggregateExec>().unwrap();
        assert!(
            !partial.emit_group_hash(),
            "default config leaves the rule disabled"
        );
        Ok(())
    }

    #[test]
    fn enables_emit_group_hash_when_repartition_matches_groups() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let group_by = PhysicalGroupBy::new_single(vec![
            (col("a", &schema)?, "a".to_string()),
            (col("b", &schema)?, "b".to_string()),
        ]);
        let partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![],
            vec![],
            input,
            Arc::clone(&schema),
        )?);

        let partial_schema = partial.schema();
        let hash_exprs: Vec<Arc<dyn PhysicalExpr>> =
            vec![col("a", &partial_schema)?, col("b", &partial_schema)?];
        let repartition: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            partial as Arc<dyn ExecutionPlan>,
            Partitioning::Hash(hash_exprs, 4),
        )?);

        let optimized =
            EmitPartialAggregateHash::new().optimize(repartition, &config_enabled())?;

        let repartition = optimized
            .downcast_ref::<RepartitionExec>()
            .expect("plan root should still be RepartitionExec");
        let new_partial = repartition
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("repartition input should be AggregateExec");
        assert!(new_partial.emit_group_hash());
        let schema = new_partial.schema();
        let last = schema.field(schema.fields().len() - 1);
        assert_eq!(last.name(), PRECOMPUTED_GROUP_HASH_COLUMN);
        Ok(())
    }

    #[test]
    fn skips_when_partitioning_expr_is_not_plain_column_ref() -> Result<()> {
        use datafusion_expr_common::operator::Operator;
        use datafusion_physical_expr::expressions::binary;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
        let partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![],
            vec![],
            input,
            Arc::clone(&schema),
        )?);

        let partial_schema = partial.schema();
        let plus = binary(
            col("a", &partial_schema)?,
            Operator::Plus,
            col("a", &partial_schema)?,
            &partial_schema,
        )?;
        let repartition: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            partial as Arc<dyn ExecutionPlan>,
            Partitioning::Hash(vec![plus], 4),
        )?);

        let optimized = EmitPartialAggregateHash::new()
            .optimize(Arc::clone(&repartition), &config_enabled())?;

        let new_repartition = optimized.downcast_ref::<RepartitionExec>().unwrap();
        let new_partial = new_repartition
            .input()
            .downcast_ref::<AggregateExec>()
            .unwrap();
        assert!(!new_partial.emit_group_hash());
        Ok(())
    }
}
