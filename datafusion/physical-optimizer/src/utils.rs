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
    AcrossPartitions, Distribution, LexOrdering, LexRequirement,
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

/// Builds the ordering of a sort placed above `plan` from `sort_requirements`,
/// leaving out the requirements whose expression is constant in `plan`'s
/// output.
///
/// A constant with the same value in every partition
/// ([`AcrossPartitions::Uniform`]) is always left out. A constant whose value
/// differs between partitions ([`AcrossPartitions::Heterogeneous`]) is left out
/// only when `keep_heterogeneous` is false. Callers set it when an operator
/// compares rows from different partitions on this ordering, since there the
/// constant still decides the order. `EquivalenceGroup::is_uniform_constant`
/// makes the same distinction when it normalizes orderings (#23528).
fn ordering_without_constants(
    plan: &dyn ExecutionPlan,
    sort_requirements: LexRequirement,
    keep_heterogeneous: bool,
) -> Option<LexOrdering> {
    let eq_properties = plan.equivalence_properties();
    let sort_exprs = sort_requirements
        .into_iter()
        .filter(|req| match eq_properties.is_expr_constant(&req.expr) {
            None => true,
            Some(AcrossPartitions::Heterogeneous) => keep_heterogeneous,
            Some(AcrossPartitions::Uniform(_)) => false,
        })
        .map(Into::into)
        .collect::<Vec<_>>();
    LexOrdering::new(sort_exprs)
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
    let preserve_partitioning = node.plan.output_partitioning().partition_count() > 1;
    // The partitions of a `SortExec` with a fetch and `preserve_partitioning`
    // share one TopK threshold, which compares rows across partitions.
    let keep_heterogeneous = preserve_partitioning && fetch.is_some();
    let Some(ordering) = ordering_without_constants(
        node.plan.as_ref(),
        sort_requirements,
        keep_heterogeneous,
    ) else {
        return node;
    };
    let mut new_sort = SortExec::new(ordering, Arc::clone(&node.plan)).with_fetch(fetch);
    if preserve_partitioning {
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
    let input_has_multiple_partitions =
        node.plan.output_partitioning().partition_count() > 1;
    let add_merge = matches!(required_distribution, Distribution::SinglePartition)
        && input_has_multiple_partitions;
    // A merge compares rows across partitions, and so do the partitions of a
    // `SortExec` with a fetch and `preserve_partitioning`, which share one TopK
    // threshold. Both use this ordering.
    let keep_heterogeneous =
        input_has_multiple_partitions && (fetch.is_some() || add_merge);
    let Some(ordering) = ordering_without_constants(
        node.plan.as_ref(),
        sort_requirements,
        keep_heterogeneous,
    ) else {
        return node;
    };

    let mut new_sort =
        SortExec::new(ordering.clone(), Arc::clone(&node.plan)).with_fetch(fetch);
    if input_has_multiple_partitions {
        new_sort = new_sort.with_preserve_partitioning(true);
    }

    let sort_node = PlanContext::new(Arc::new(new_sort), T::default(), vec![node]);

    // If the parent requires SinglePartition and the input has multiple partitions,
    // wrap the partition-preserving sort in SortPreservingMergeExec.
    if add_merge {
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

/// Checks whether the given operator is a limit;
/// i.e. either a [`LocalLimitExec`] or a [`GlobalLimitExec`].
pub fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<GlobalLimitExec>() || plan.is::<LocalLimitExec>()
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::{col, lit};
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::projection::ProjectionExec;

    /// `UnionExec` of one `SELECT <c> AS c0, id AS c1` arm per constant, so
    /// `c0` is constant in every partition, and differs between them unless
    /// the constants are equal.
    fn union_of_constants(constants: &[i64]) -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arms = constants
            .iter()
            .map(|c| {
                let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
                let exprs = vec![
                    (lit(ScalarValue::Int64(Some(*c))), "c0".to_string()),
                    (col("id", &schema).unwrap(), "c1".to_string()),
                ];
                Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
                    as Arc<dyn ExecutionPlan>
            })
            .collect();
        UnionExec::try_new(arms).unwrap()
    }

    /// The requirement `c0 ASC, c1 ASC` on `plan`.
    fn c0_c1(plan: &Arc<dyn ExecutionPlan>) -> LexRequirement {
        let schema = plan.schema();
        LexRequirement::new(["c0", "c1"].map(|name| {
            PhysicalSortExpr::new_default(col(name, &schema).unwrap()).into()
        }))
        .unwrap()
    }

    fn columns(ordering: &LexOrdering) -> Vec<String> {
        ordering.iter().map(|e| e.expr.to_string()).collect()
    }

    /// The ordering of the `SortExec` and of the `SortPreservingMergeExec`, if
    /// any, at the top of `plan`.
    fn orderings(plan: &Arc<dyn ExecutionPlan>) -> (Vec<String>, Option<Vec<String>>) {
        if let Some(merge) = plan.downcast_ref::<SortPreservingMergeExec>() {
            let (sort, _) = orderings(merge.input());
            return (sort, Some(columns(merge.expr())));
        }
        let sort = plan.downcast_ref::<SortExec>().unwrap();
        (columns(sort.expr()), None)
    }

    fn sort_above(
        plan: Arc<dyn ExecutionPlan>,
        fetch: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let reqs = c0_c1(&plan);
        add_sort_above(PlanContext::<()>::new_default(plan), reqs, fetch).plan
    }

    fn sort_above_with_distribution(
        plan: Arc<dyn ExecutionPlan>,
        fetch: Option<usize>,
        distribution: Distribution,
    ) -> Arc<dyn ExecutionPlan> {
        let reqs = c0_c1(&plan);
        let node = PlanContext::<()>::new_default(plan);
        add_sort_above_with_distribution(node, reqs, fetch, &distribution).plan
    }

    #[test]
    fn partitioned_top_k_keeps_a_constant_that_differs_between_partitions() {
        let c0_c1 = vec!["c0@0".to_string(), "c1@1".to_string()];
        assert_eq!(
            orderings(&sort_above(union_of_constants(&[5, 1]), Some(2))),
            (c0_c1.clone(), None)
        );
        assert_eq!(
            orderings(&sort_above_with_distribution(
                union_of_constants(&[5, 1]),
                Some(2),
                Distribution::UnspecifiedDistribution,
            )),
            (c0_c1, None)
        );
    }

    #[test]
    fn merge_keeps_a_constant_that_differs_between_partitions() {
        let c0_c1 = vec!["c0@0".to_string(), "c1@1".to_string()];
        for fetch in [None, Some(2)] {
            assert_eq!(
                orderings(&sort_above_with_distribution(
                    union_of_constants(&[5, 1]),
                    fetch,
                    Distribution::SinglePartition,
                )),
                (c0_c1.clone(), Some(c0_c1.clone())),
                "fetch {fetch:?}"
            );
        }
    }

    #[test]
    fn sort_without_fetch_or_merge_drops_a_constant_that_differs_between_partitions() {
        let c1 = vec!["c1@1".to_string()];
        assert_eq!(
            orderings(&sort_above(union_of_constants(&[5, 1]), None)),
            (c1.clone(), None)
        );
        assert_eq!(
            orderings(&sort_above_with_distribution(
                union_of_constants(&[5, 1]),
                None,
                Distribution::UnspecifiedDistribution,
            )),
            (c1, None)
        );
    }

    #[test]
    fn sort_drops_a_constant_with_the_same_value_in_every_partition() {
        let c1 = vec!["c1@1".to_string()];
        assert_eq!(
            orderings(&sort_above(union_of_constants(&[1, 1]), Some(2))),
            (c1.clone(), None)
        );
        assert_eq!(
            orderings(&sort_above_with_distribution(
                union_of_constants(&[1, 1]),
                Some(2),
                Distribution::SinglePartition,
            )),
            (c1.clone(), Some(c1))
        );
    }
}
