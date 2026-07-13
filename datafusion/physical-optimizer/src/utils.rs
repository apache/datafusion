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

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_expr::{
    AcrossPartitions, Distribution, EquivalenceProperties, LexOrdering, LexRequirement,
    Partitioning, PhysicalExpr, physical_exprs_equal,
};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Returns `true` when `expr` is a *globally* constant column in `eq_properties`
/// and can therefore be safely dropped from a required ordering.
///
/// Only [`AcrossPartitions::Uniform`] qualifies. An
/// [`AcrossPartitions::Heterogeneous`] value is constant *within* each partition
/// but may differ *across* partitions, so it must be kept in the sort key: the
/// `SortExec` these helpers build preserves partitioning, and its output is
/// merged across partitions downstream, where the column is still an ordering
/// discriminator. Dropping it there silently loses the global ordering.
fn is_uniform_constant(
    eq_properties: &EquivalenceProperties,
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    matches!(
        eq_properties.is_expr_constant(expr),
        Some(AcrossPartitions::Uniform(_))
    )
}

/// This utility function adds a `SortExec` above an operator according to the
/// given ordering requirements while preserving the original partitioning.
///
/// Note that this updates the plan in both the `PlanContext.children` and
/// the `PlanContext.plan`'s children. Therefore its not required to sync
/// the child plans with [`PlanContext::update_plan_from_children`].
pub fn add_sort_above<T: Clone + Default>(
    node: PlanContext<T>,
    sort_requirements: LexRequirement,
    fetch: Option<usize>,
) -> PlanContext<T> {
    let mut sort_reqs: Vec<_> = sort_requirements.into();
    sort_reqs.retain(|sort_expr| {
        !is_uniform_constant(node.plan.equivalence_properties(), &sort_expr.expr)
    });
    let sort_exprs = sort_reqs.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(ordering) = LexOrdering::new(sort_exprs) else {
        return node;
    };
    let mut new_sort = SortExec::new(ordering, Arc::clone(&node.plan)).with_fetch(fetch);
    if node.plan.output_partitioning().partition_count() > 1 {
        new_sort = new_sort.with_preserve_partitioning(true);
    }
    PlanContext::new(Arc::new(new_sort), T::default(), vec![node])
}

/// Like [`add_sort_above`], but also inserts a [`SortPreservingMergeExec`] when
/// the parent distribution requires a single partition and the input has
/// multiple partitions. This prevents `SortExec(preserve_partitioning=true)`
/// from violating `SinglePartition` requirements.
pub fn add_sort_above_with_distribution<T: Clone + Default>(
    node: PlanContext<T>,
    sort_requirements: LexRequirement,
    fetch: Option<usize>,
    required_distribution: &Distribution,
) -> PlanContext<T> {
    let mut sort_reqs: Vec<_> = sort_requirements.into();
    sort_reqs.retain(|sort_expr| {
        !is_uniform_constant(node.plan.equivalence_properties(), &sort_expr.expr)
    });
    let sort_exprs = sort_reqs.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(ordering) = LexOrdering::new(sort_exprs) else {
        return node;
    };
    let input_has_multiple_partitions =
        node.plan.output_partitioning().partition_count() > 1;

    let mut new_sort =
        SortExec::new(ordering.clone(), Arc::clone(&node.plan)).with_fetch(fetch);
    if input_has_multiple_partitions {
        new_sort = new_sort.with_preserve_partitioning(true);
    }

    let sort_node = PlanContext::new(Arc::new(new_sort), T::default(), vec![node]);

    // If the parent requires SinglePartition and the input has multiple partitions,
    // wrap the partition-preserving sort in SortPreservingMergeExec.
    if matches!(required_distribution, Distribution::SinglePartition)
        && input_has_multiple_partitions
    {
        PlanContext::new(
            Arc::new(
                SortPreservingMergeExec::new(ordering, Arc::clone(&sort_node.plan))
                    .with_fetch(fetch),
            ),
            T::default(),
            vec![sort_node],
        )
    } else {
        sort_node
    }
}

/// This utility function adds a `SortExec` above an operator according to the
/// given ordering requirements while preserving the original partitioning. If
/// requirement is already satisfied no `SortExec` is added.
pub fn add_sort_above_with_check<T: Clone + Default>(
    node: PlanContext<T>,
    sort_requirements: LexRequirement,
    fetch: Option<usize>,
) -> Result<PlanContext<T>> {
    if !node
        .plan
        .equivalence_properties()
        .ordering_satisfy_requirement(sort_requirements.clone())?
    {
        Ok(add_sort_above(node, sort_requirements, fetch))
    } else {
        Ok(node)
    }
}

/// Checks whether the given operator is a [`SortExec`].
pub fn is_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<SortExec>()
}

/// Checks whether the given operator is a window;
/// i.e. either a [`WindowAggExec`] or a [`BoundedWindowAggExec`].
pub fn is_window(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<WindowAggExec>() || plan.is::<BoundedWindowAggExec>()
}

/// Checks whether the given operator is a [`UnionExec`].
pub fn is_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<UnionExec>()
}

/// Checks whether the given operator is a [`SortPreservingMergeExec`].
pub fn is_sort_preserving_merge(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<SortPreservingMergeExec>()
}

/// Checks whether the given operator is a [`CoalescePartitionsExec`].
pub fn is_coalesce_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<CoalescePartitionsExec>()
}

/// Checks whether the given operator is a [`RepartitionExec`].
pub fn is_repartition(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<RepartitionExec>()
}

/// TODO: remove once Range generally satisfies KeyPartitioned requirements
/// through Partitioning::satisfaction.
/// See <https://github.com/apache/datafusion/issues/23266>.
///
/// Checks whether range partitioning satisfies a key partitioning requirement.
/// This is intentionally separate from general partitioning satisfaction while
/// range reuse is rolled out operator by operator.
pub(crate) fn range_partitioning_satisfies_key_partitioning(
    partitioning: &Partitioning,
    required_exprs: &[Arc<dyn PhysicalExpr>],
    eq_properties: &EquivalenceProperties,
    allow_subset: bool,
) -> bool {
    match partitioning {
        Partitioning::Range(range) => {
            let partition_exprs = range
                .ordering()
                .iter()
                .map(|sort_expr| Arc::clone(&sort_expr.expr))
                .collect::<Vec<_>>();

            if partition_exprs.is_empty() || required_exprs.is_empty() {
                return false;
            }

            let eq_group = eq_properties.eq_group();
            let normalized_partition_exprs = partition_exprs
                .iter()
                .map(|expr| eq_group.normalize_expr(Arc::clone(expr)))
                .collect::<Vec<_>>();
            let normalized_required_exprs = required_exprs
                .iter()
                .map(|expr| eq_group.normalize_expr(Arc::clone(expr)))
                .collect::<Vec<_>>();

            if physical_exprs_equal(
                &normalized_required_exprs,
                &normalized_partition_exprs,
            ) {
                return true;
            }

            allow_subset
                && normalized_partition_exprs.len() < normalized_required_exprs.len()
                && normalized_partition_exprs.iter().all(|partition_expr| {
                    normalized_required_exprs
                        .iter()
                        .any(|required_expr| partition_expr.eq(required_expr))
                })
        }
        _ => false,
    }
}

/// Checks whether the given operator is a limit;
/// i.e. either a [`LocalLimitExec`] or a [`GlobalLimitExec`].
pub fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<GlobalLimitExec>() || plan.is::<LocalLimitExec>()
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::PhysicalSortRequirement;
    use datafusion_physical_expr::expressions::{col, lit};
    use datafusion_physical_expr::projection::ProjectionExpr;
    use datafusion_physical_plan::displayable;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::projection::ProjectionExec;

    /// A `UnionExec` whose `sample` column is 1 on the left branch and 2 on the
    /// right makes `sample` a `Heterogeneous` constant: constant within each
    /// partition, but different across them. A required ordering of
    /// `[sample, other]` must retain `sample`; the `SortExec` built here
    /// preserves partitioning and its output is merged across partitions
    /// downstream, where `sample` is still the leading ordering discriminator.
    /// Regression test: `add_sort_above` used to drop every constant, uniform
    /// or not, and silently discarded the global ordering.
    #[test]
    fn add_sort_above_keeps_heterogeneous_constant_key() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, true)]));

        let branch = |value: i32| -> Result<Arc<dyn ExecutionPlan>> {
            let source = Arc::new(EmptyExec::new(Arc::clone(&schema)));
            let projection = ProjectionExec::try_new(
                vec![
                    ProjectionExpr {
                        expr: lit(value),
                        alias: "sample".to_string(),
                    },
                    ProjectionExpr {
                        expr: col("c", &schema)?,
                        alias: "other".to_string(),
                    },
                ],
                source,
            )?;
            Ok(Arc::new(projection))
        };

        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![branch(1)?, branch(2)?])?;
        let union_schema = union.schema();

        // Precondition: the union reports `sample` as a heterogeneous constant.
        let sample = col("sample", &union_schema)?;
        assert!(matches!(
            union.equivalence_properties().is_expr_constant(&sample),
            Some(AcrossPartitions::Heterogeneous)
        ));

        let asc = SortOptions::default();
        let requirement: LexRequirement = [
            PhysicalSortRequirement::new(sample, Some(asc)),
            PhysicalSortRequirement::new(col("other", &union_schema)?, Some(asc)),
        ]
        .into();

        let node = PlanContext::<()>::new_default(union);
        let result = add_sort_above(node, requirement, None);

        let plan = displayable(result.plan.as_ref()).indent(true).to_string();
        assert!(
            plan.contains("sample@0 ASC"),
            "add_sort_above dropped the heterogeneous-constant sort key:\n{plan}"
        );

        Ok(())
    }
}
