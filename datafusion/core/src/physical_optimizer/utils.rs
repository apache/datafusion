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

//! Collection of utility functions that are leveraged by the query optimizer rules

use super::optimizer::PhysicalOptimizerRule;
use std::collections::HashSet;

use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use datafusion_common::tree_node::Transformed;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::utils::ordering_satisfy;
use datafusion_physical_expr::PhysicalSortExpr;
use itertools::Itertools;
use std::sync::Arc;

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
pub fn optimize_children(
    optimizer: &impl PhysicalOptimizerRule,
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .iter()
        .map(|child| optimizer.optimize(Arc::clone(child), config))
        .collect::<Result<Vec<_>>>()?;

    if children.is_empty() {
        Ok(Arc::clone(&plan))
    } else {
        with_new_children_if_necessary(plan, children).map(Transformed::into)
    }
}

/// This utility function adds a `SortExec` above an operator according to the
/// given ordering requirements while preserving the original partitioning.
pub fn add_sort_above(
    node: &mut Arc<dyn ExecutionPlan>,
    sort_expr: Vec<PhysicalSortExpr>,
) -> Result<()> {
    // If the ordering requirement is already satisfied, do not add a sort.
    if !ordering_satisfy(node.output_ordering(), Some(&sort_expr), || {
        node.equivalence_properties()
    }) {
        let new_sort = SortExec::new(sort_expr, node.clone());

        *node = Arc::new(if node.output_partitioning().partition_count() > 1 {
            new_sort.with_preserve_partitioning(true)
        } else {
            new_sort
        }) as _
    }
    Ok(())
}

// Find the indices of each element if the to_search vector inside the searched vector
// Assumes that each entry in the `to_search` occurs in the `searched`.
pub(crate) fn find_match_indices<T: PartialEq>(
    to_search: &[T],
    searched: &[T],
) -> Result<Vec<usize>> {
    let mut result = vec![];
    for item in to_search {
        if let Some(idx) = searched.iter().position(|e| e.eq(item)) {
            result.push(idx);
        } else {
            return Err(DataFusionError::Execution("item not found".to_string()));
        }
    }
    Ok(result)
}

// Merges vectors `in1` and `in2` (removes duplicates) then sorts the result.
pub(crate) fn get_ordered_merged_indices(in1: &[usize], in2: &[usize]) -> Vec<usize> {
    let set: HashSet<_> = in1.iter().chain(in2.iter()).copied().collect();
    let mut res: Vec<_> = set.into_iter().collect();
    res.sort();
    res
}

// Checks if the vector in the form 1,2,3,..n (Consecutive) not necessarily starting from zero
// Assumes input has ascending order
pub(crate) fn is_ascending_ordered(in1: &[usize]) -> bool {
    if !in1.is_empty() {
        in1.iter()
            .zip(in1.iter().skip(1))
            .all(|(prev, cur)| cur >= prev)
    } else {
        true
    }
}

// Returns the vector consisting of elements inside `in1` that are not inside `in2`.
// Resulting vector have the same ordering as `in1` (except elements inside `in2` are removed.)
pub(crate) fn get_set_diff_indices(in1: &[usize], in2: &[usize]) -> Vec<usize> {
    let mut res = vec![];
    for lhs in in1 {
        if !in2.iter().contains(lhs) {
            res.push(*lhs);
        }
    }
    res
}

// Find the largest range that satisfy 0,1,2 .. n in the `in1`
// For 0,1,2,4,5 we would produce 3. meaning 0,1,2 is the largest consecutive range (starting from zero).
// For 1,2,3,4 we would produce 0. Meaning there is no consecutive range (starting from zero).
pub(crate) fn calc_ordering_range(in1: &[usize]) -> usize {
    let mut count = 0;
    for (idx, elem) in in1.iter().enumerate() {
        if idx != *elem {
            break;
        } else {
            count += 1
        }
    }
    count
}

/// Checks whether the given operator is a limit;
/// i.e. either a [`LocalLimitExec`] or a [`GlobalLimitExec`].
pub fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<GlobalLimitExec>() || plan.as_any().is::<LocalLimitExec>()
}

/// Checks whether the given operator is a window;
/// i.e. either a [`WindowAggExec`] or a [`BoundedWindowAggExec`].
pub fn is_window(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<WindowAggExec>() || plan.as_any().is::<BoundedWindowAggExec>()
}

/// Checks whether the given operator is a [`SortExec`].
pub fn is_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortExec>()
}

/// Checks whether the given operator is a [`SortPreservingMergeExec`].
pub fn is_sort_preserving_merge(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortPreservingMergeExec>()
}

/// Checks whether the given operator is a [`CoalescePartitionsExec`].
pub fn is_coalesce_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<CoalescePartitionsExec>()
}

/// Checks whether the given operator is a [`UnionExec`].
pub fn is_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<UnionExec>()
}

/// Checks whether the given operator is a [`RepartitionExec`].
pub fn is_repartition(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<RepartitionExec>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sorted_merged_indices() -> Result<()> {
        assert_eq!(
            get_ordered_merged_indices(&[0, 3, 4], &[1, 3, 5]),
            vec![0, 1, 3, 4, 5]
        );
        // Result should be ordered, even if inputs are not
        assert_eq!(
            get_ordered_merged_indices(&[3, 0, 4], &[5, 1, 3]),
            vec![0, 1, 3, 4, 5]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_is_ascending_ordered() -> Result<()> {
        assert!(is_ascending_ordered(&[0, 3, 4]));
        assert!(is_ascending_ordered(&[0, 1, 2]));
        assert!(is_ascending_ordered(&[0, 1, 4]));
        assert!(is_ascending_ordered(&[]));
        assert!(is_ascending_ordered(&[1, 2]));
        assert!(!is_ascending_ordered(&[3, 2]));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_set_diff_indices() -> Result<()> {
        assert_eq!(get_set_diff_indices(&[0, 3, 4], &[1, 2]), vec![0, 3, 4]);
        assert_eq!(get_set_diff_indices(&[0, 3, 4], &[1, 2, 4]), vec![0, 3]);
        // return value should have same ordering with the in1
        assert_eq!(get_set_diff_indices(&[3, 4, 0], &[1, 2, 4]), vec![3, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn test_calc_ordering_range() -> Result<()> {
        assert_eq!(calc_ordering_range(&[0, 3, 4]), 1);
        assert_eq!(calc_ordering_range(&[0, 1, 3, 4]), 2);
        assert_eq!(calc_ordering_range(&[0, 1, 2, 3, 4]), 5);
        assert_eq!(calc_ordering_range(&[1, 2, 3, 4]), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_match_indices() -> Result<()> {
        assert_eq!(find_match_indices(&[0, 3, 4], &[0, 3, 4])?, vec![0, 1, 2]);
        assert_eq!(find_match_indices(&[0, 4, 3], &[0, 3, 4])?, vec![0, 2, 1]);
        Ok(())
    }
}
