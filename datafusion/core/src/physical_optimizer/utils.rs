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

use itertools::concat;
use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::error::Result;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{displayable, ExecutionPlan};

use datafusion_common::DataFusionError;
use datafusion_physical_expr::utils::ordering_satisfy;
use datafusion_physical_expr::PhysicalSortExpr;

/// This object implements a tree that we use while keeping track of paths
/// leading to [`SortExec`]s.
#[derive(Debug, Clone)]
pub(crate) struct ExecTree {
    /// The `ExecutionPlan` associated with this node
    pub plan: Arc<dyn ExecutionPlan>,
    /// Child index of the plan in its parent
    pub idx: usize,
    /// Children of the plan that would need updating if we remove leaf executors
    pub children: Vec<ExecTree>,
}

impl fmt::Display for ExecTree {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let plan_string = get_plan_string(&self.plan);
        write!(f, "\nidx: {:?}", self.idx)?;
        write!(f, "\nplan: {:?}", plan_string)?;
        for child in self.children.iter() {
            write!(f, "\nexec_tree:{}", child)?;
        }
        writeln!(f)
    }
}

impl ExecTree {
    /// Create new Exec tree
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        idx: usize,
        children: Vec<ExecTree>,
    ) -> Self {
        ExecTree {
            plan,
            idx,
            children,
        }
    }

    /// This function returns the executors at the leaves of the tree.
    pub fn get_leaves(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        if self.children.is_empty() {
            vec![self.plan.clone()]
        } else {
            concat(self.children.iter().map(|e| e.get_leaves()))
        }
    }
}

/// Get `ExecTree` for each child of the plan if they are tracked.
/// # Arguments
///
/// * `n_children` - Children count of the plan of interest
/// * `onward` - Contains `Some(ExecTree)` of the plan tracked.
///            - Contains `None` is plan is not tracked.
///
/// # Returns
///
/// A `Vec<Option<ExecTree>>` that contains tracking information of each child.
/// If a child is `None`, it is not tracked. If `Some(ExecTree)` child is tracked also.
pub(crate) fn get_children_exectrees(
    n_children: usize,
    onward: &Option<ExecTree>,
) -> Vec<Option<ExecTree>> {
    let mut children_onward = vec![None; n_children];
    if let Some(exec_tree) = &onward {
        for child in &exec_tree.children {
            children_onward[child.idx] = Some(child.clone());
        }
    }
    children_onward
}

// Get output (un)boundedness information for the given `plan`.
pub(crate) fn unbounded_output(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let result = if plan.children().is_empty() {
        plan.unbounded_output(&[])
    } else {
        let children_unbounded_output = plan
            .children()
            .iter()
            .map(unbounded_output)
            .collect::<Vec<_>>();
        plan.unbounded_output(&children_unbounded_output)
    };
    result.unwrap_or(true)
}

/// This utility function adds a `SortExec` above an operator according to the
/// given ordering requirements while preserving the original partitioning.
pub fn add_sort_above(
    node: &mut Arc<dyn ExecutionPlan>,
    sort_expr: Vec<PhysicalSortExpr>,
    fetch: Option<usize>,
) -> Result<()> {
    // If the ordering requirement is already satisfied, do not add a sort.
    if !ordering_satisfy(
        node.output_ordering(),
        Some(&sort_expr),
        || node.equivalence_properties(),
        || node.ordering_equivalence_properties(),
    ) {
        let new_sort = SortExec::new(sort_expr, node.clone()).with_fetch(fetch);

        *node = Arc::new(if node.output_partitioning().partition_count() > 1 {
            new_sort.with_preserve_partitioning(true)
        } else {
            new_sort
        }) as _
    }
    Ok(())
}

/// Find indices of each element in `targets` inside `items`. If one of the
/// elements is absent in `items`, returns an error.
pub fn find_indices<T: PartialEq, S: Borrow<T>>(
    items: &[T],
    targets: impl IntoIterator<Item = S>,
) -> Result<Vec<usize>> {
    targets
        .into_iter()
        .map(|target| items.iter().position(|e| target.borrow().eq(e)))
        .collect::<Option<_>>()
        .ok_or_else(|| DataFusionError::Execution("Target not found".to_string()))
}

/// Merges collections `first` and `second`, removes duplicates and sorts the
/// result, returning it as a [`Vec`].
pub fn merge_and_order_indices<T: Borrow<usize>, S: Borrow<usize>>(
    first: impl IntoIterator<Item = T>,
    second: impl IntoIterator<Item = S>,
) -> Vec<usize> {
    let mut result: Vec<_> = first
        .into_iter()
        .map(|e| *e.borrow())
        .chain(second.into_iter().map(|e| *e.borrow()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    result.sort();
    result
}

/// Checks whether the given index sequence is monotonically non-decreasing.
pub fn is_sorted<T: Borrow<usize>>(sequence: impl IntoIterator<Item = T>) -> bool {
    // TODO: Remove this function when `is_sorted` graduates from Rust nightly.
    let mut previous = 0;
    for item in sequence.into_iter() {
        let current = *item.borrow();
        if current < previous {
            return false;
        }
        previous = current;
    }
    true
}

/// Calculates the set difference between sequences `first` and `second`,
/// returning the result as a [`Vec`]. Preserves the ordering of `first`.
pub fn set_difference<T: Borrow<usize>, S: Borrow<usize>>(
    first: impl IntoIterator<Item = T>,
    second: impl IntoIterator<Item = S>,
) -> Vec<usize> {
    let set: HashSet<_> = second.into_iter().map(|e| *e.borrow()).collect();
    first
        .into_iter()
        .map(|e| *e.borrow())
        .filter(|e| !set.contains(e))
        .collect()
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

/// Utility function yielding a string representation of the given [`ExecutionPlan`].
pub fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let formatted = displayable(plan.as_ref()).indent(true).to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    actual.iter().map(|elem| elem.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_find_indices() -> Result<()> {
        assert_eq!(find_indices(&[0, 3, 4], [0, 3, 4])?, vec![0, 1, 2]);
        assert_eq!(find_indices(&[0, 3, 4], [0, 4, 3])?, vec![0, 2, 1]);
        assert_eq!(find_indices(&[3, 0, 4], [0, 3])?, vec![1, 0]);
        assert!(find_indices(&[0, 3], [0, 3, 4]).is_err());
        assert!(find_indices(&[0, 3, 4], [0, 2]).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_and_order_indices() {
        assert_eq!(
            merge_and_order_indices([0, 3, 4], [1, 3, 5]),
            vec![0, 1, 3, 4, 5]
        );
        // Result should be ordered, even if inputs are not
        assert_eq!(
            merge_and_order_indices([3, 0, 4], [5, 1, 3]),
            vec![0, 1, 3, 4, 5]
        );
    }

    #[tokio::test]
    async fn test_is_sorted() {
        assert!(is_sorted::<usize>([]));
        assert!(is_sorted([0]));
        assert!(is_sorted([0, 3, 4]));
        assert!(is_sorted([0, 1, 2]));
        assert!(is_sorted([0, 1, 4]));
        assert!(is_sorted([0usize; 0]));
        assert!(is_sorted([1, 2]));
        assert!(!is_sorted([3, 2]));
    }

    #[tokio::test]
    async fn test_set_difference() {
        assert_eq!(set_difference([0, 3, 4], [1, 2]), vec![0, 3, 4]);
        assert_eq!(set_difference([0, 3, 4], [1, 2, 4]), vec![0, 3]);
        // return value should have same ordering with the in1
        assert_eq!(set_difference([3, 4, 0], [1, 2, 4]), vec![3, 0]);
        assert_eq!(set_difference([0, 3, 4], [4, 1, 2]), vec![0, 3]);
        assert_eq!(set_difference([3, 4, 0], [4, 1, 2]), vec![3, 0]);
    }
}
