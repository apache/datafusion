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
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

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

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::JoinType;
    use datafusion_common::config::ConfigOptions;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::joins::HashJoinExecBuilder;
    use datafusion_physical_plan::limit::LocalLimitExec;

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
}
