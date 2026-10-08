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

//! Read-only invariants for regions of the physical optimizer pipeline.

use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, internal_err};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{
    AsOfJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
};

use crate::optimizer::PlanInvariant;

/// Every hash join has a concrete partition mode.
///
/// [`JoinSelection`](crate::join_selection::JoinSelection) establishes this
/// invariant by resolving [`PartitionMode::Auto`]. Later rules may still change
/// a join from one concrete mode to another.
#[derive(Debug)]
pub struct HashJoinModesResolved;

impl PlanInvariant for HashJoinModesResolved {
    fn name(&self) -> &'static str {
        "hash_join_modes_resolved"
    }

    fn check(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        plan.apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>()
                && *join.partition_mode() == PartitionMode::Auto
            {
                return internal_err!("HashJoinExec still has PartitionMode::Auto");
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
}

/// Built-in joins and filters have no embedded output projections.
///
/// Checks every [`FilterExec`], [`HashJoinExec`], [`NestedLoopJoinExec`],
/// [`SortMergeJoinExec`], and [`AsOfJoinExec`]. Even an empty or identity
/// projection is embedded and therefore rejected. Separate
/// [`ProjectionExec`](datafusion_physical_plan::projection::ProjectionExec) nodes
/// and column pruning at data sources are permitted.
///
/// This invariant lets earlier optimizer rules reason about the operators'
/// unprojected outputs until projection pushdown embeds those projections.
#[derive(Debug)]
pub struct NoEmbeddedProjections;

impl PlanInvariant for NoEmbeddedProjections {
    fn name(&self) -> &'static str {
        "no_embedded_projections"
    }

    fn check(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        plan.apply(|node| {
            let projection = if let Some(filter) = node.downcast_ref::<FilterExec>() {
                filter.projection().as_deref()
            } else if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                join.projection.as_deref()
            } else if let Some(join) = node.downcast_ref::<NestedLoopJoinExec>() {
                join.projection().as_deref()
            } else if let Some(join) = node.downcast_ref::<SortMergeJoinExec>() {
                join.projection.as_deref()
            } else if let Some(join) = node.downcast_ref::<AsOfJoinExec>() {
                join.projection().as_deref()
            } else {
                None
            };
            if let Some(projection) = projection {
                return internal_err!(
                    "{} has embedded output projection {projection:?}",
                    node.name()
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{JoinType, NullEquality};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{Column, lit};
    use datafusion_physical_expr::projection::ProjectionExpr;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::filter::FilterExecBuilder;
    use datafusion_physical_plan::joins::{
        AsOfMatchExpr, HashJoinExecBuilder, NestedLoopJoinExecBuilder,
    };
    use datafusion_physical_plan::limit::LocalLimitExec;
    use datafusion_physical_plan::projection::ProjectionExec;

    use crate::PhysicalOptimizerRule;
    use crate::join_selection::JoinSelection;

    fn hash_join(mode: PartitionMode) -> Result<Arc<dyn ExecutionPlan>> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        Ok(Arc::new(
            HashJoinExecBuilder::new(
                Arc::clone(&input),
                input,
                vec![(
                    Arc::new(Column::new("key", 0)),
                    Arc::new(Column::new("key", 0)),
                )],
                JoinType::Inner,
            )
            .with_partition_mode(mode)
            .build()?,
        ))
    }

    #[test]
    fn rejects_auto_at_root_and_below_root() -> Result<()> {
        let plan = hash_join(PartitionMode::Auto)?;
        let nested: Arc<dyn ExecutionPlan> =
            Arc::new(LocalLimitExec::new(Arc::clone(&plan), 1024));
        for plan in [plan, nested] {
            let error = HashJoinModesResolved.check(&plan).unwrap_err().to_string();
            assert!(error.contains("HashJoinExec still has PartitionMode::Auto"));
        }
        Ok(())
    }

    #[test]
    fn permits_both_concrete_modes() -> Result<()> {
        for mode in [PartitionMode::CollectLeft, PartitionMode::Partitioned] {
            HashJoinModesResolved.check(&hash_join(mode)?)?;
        }
        Ok(())
    }

    #[test]
    fn join_selection_establishes_invariant() -> Result<()> {
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(LocalLimitExec::new(hash_join(PartitionMode::Auto)?, 1024));
        let plan = JoinSelection::new().optimize(plan, &ConfigOptions::default())?;
        HashJoinModesResolved.check(&plan)
    }

    fn operators_with_projection(
        projection: impl Fn(usize) -> Option<Vec<usize>>,
    ) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let on = || {
            vec![(
                Arc::new(Column::new("key", 0)) as _,
                Arc::new(Column::new("key", 0)) as _,
            )]
        };
        Ok(vec![
            Arc::new(
                FilterExecBuilder::new(lit(true), Arc::clone(&input))
                    .apply_projection(projection(1))?
                    .build()?,
            ),
            Arc::new(
                HashJoinExecBuilder::new(
                    Arc::clone(&input),
                    Arc::clone(&input),
                    on(),
                    JoinType::Inner,
                )
                .with_projection(projection(2))
                .build()?,
            ),
            Arc::new(
                NestedLoopJoinExecBuilder::new(
                    Arc::clone(&input),
                    Arc::clone(&input),
                    JoinType::Inner,
                )
                .with_projection(projection(2))
                .build()?,
            ),
            Arc::new(
                SortMergeJoinExec::try_new(
                    Arc::clone(&input),
                    Arc::clone(&input),
                    on(),
                    None,
                    JoinType::Inner,
                    vec![SortOptions::default()],
                    NullEquality::NullEqualsNothing,
                )?
                .with_projection(projection(2))?,
            ),
            Arc::new(AsOfJoinExec::try_new(
                Arc::clone(&input),
                input,
                vec![],
                AsOfMatchExpr::new(
                    Arc::new(Column::new("key", 0)),
                    Operator::GtEq,
                    Arc::new(Column::new("key", 0)),
                ),
                projection(2),
            )?),
        ])
    }

    #[test]
    fn rejects_all_embedded_projections_at_root_and_below_root() -> Result<()> {
        for projection in [
            (|_| Some(vec![0])) as fn(usize) -> Option<Vec<usize>>,
            |_| Some(vec![]),
            |columns| Some((0..columns).collect()),
        ] {
            for plan in operators_with_projection(projection)? {
                let name = plan.name().to_owned();
                let nested: Arc<dyn ExecutionPlan> =
                    Arc::new(LocalLimitExec::new(Arc::clone(&plan), 1024));
                for plan in [plan, nested] {
                    let error =
                        NoEmbeddedProjections.check(&plan).unwrap_err().to_string();
                    assert!(
                        error.contains(&format!(
                            "{name} has embedded output projection ["
                        )),
                        "{error}"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn permits_unprojected_operators_and_separate_projection_nodes() -> Result<()> {
        for plan in operators_with_projection(|_| None)? {
            NoEmbeddedProjections.check(&plan)?;
            let projected: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                vec![ProjectionExpr::new(Arc::new(Column::new("key", 0)), "key")],
                plan,
            )?);
            NoEmbeddedProjections.check(&projected)?;
        }
        Ok(())
    }
}
