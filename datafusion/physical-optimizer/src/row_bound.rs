// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`RowBound`]: the `OFFSET skip LIMIT fetch` that an optimizer rule wants on
//! an operator's output.
//!
//! A bound is written into a plan in one of two forms: a limit node
//! ([`GlobalLimitExec`], [`LocalLimitExec`]) or an operator's own fetch
//! ([`ExecutionPlan::with_fetch`]). Operators can't skip rows, so a bound with
//! `skip > 0` always keeps a [`GlobalLimitExec`].

use std::sync::Arc;

use datafusion_common::utils::combine_limit;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties, LimitBehavior};

/// `OFFSET skip LIMIT fetch` on the output of one operator.
///
/// `fetch == None` means no row limit (only `skip`). The default bound,
/// `skip = 0, fetch = None`, keeps every row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RowBound {
    pub skip: usize,
    pub fetch: Option<usize>,
}

impl RowBound {
    /// `LIMIT fetch` with no offset.
    pub fn fetch(fetch: Option<usize>) -> Self {
        Self { skip: 0, fetch }
    }

    /// The bound a limit node imposes, or `None` if `plan` is not a limit node.
    pub fn of_limit_node(plan: &Arc<dyn ExecutionPlan>) -> Option<Self> {
        match plan.limit_behavior() {
            LimitBehavior::Limit { skip, fetch } => Some(Self { skip, fetch }),
            _ => None,
        }
    }

    /// `self` applied over the output of `inner`: first `inner`, then `self`.
    pub fn over(self, inner: RowBound) -> RowBound {
        let (skip, fetch) = combine_limit(self.skip, self.fetch, inner.skip, inner.fetch);
        RowBound { skip, fetch }
    }

    /// The tighter of two bounds on the same output. Both must have `skip == 0`.
    pub fn min(self, other: RowBound) -> RowBound {
        debug_assert!(self.skip == 0 && other.skip == 0);
        let fetch = match (self.fetch, other.fetch) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        RowBound::fetch(fetch)
    }

    /// Rows the input must produce so this bound can be applied on top of it:
    /// `skip + fetch`. `None` when there is no row limit.
    ///
    /// Saturates: `usize::MAX` input rows is never a smaller bound than the
    /// real one, so a fetch computed from it stays correct.
    pub fn input_rows(self) -> Option<usize> {
        self.fetch.map(|fetch| fetch.saturating_add(self.skip))
    }
}

/// Writes `bound` on top of `plan`'s output.
///
/// `plan` absorbs `bound.input_rows()` as its own fetch when it can; a
/// [`GlobalLimitExec`] on top still applies the skip, if any. When `plan`
/// can't absorb it, a [`limit_node`] is added instead.
pub(crate) fn place(
    plan: Arc<dyn ExecutionPlan>,
    bound: RowBound,
) -> Arc<dyn ExecutionPlan> {
    match plan.with_fetch(bound.input_rows()) {
        Some(absorbed) if bound.skip == 0 => absorbed,
        Some(absorbed) => {
            Arc::new(GlobalLimitExec::new(absorbed, bound.skip, bound.fetch))
        }
        None => limit_node(plan, bound),
    }
}

/// A limit node applying `bound` to `plan`: a [`GlobalLimitExec`] when it
/// skips rows or `plan` has one output partition, otherwise a per-partition
/// [`LocalLimitExec`].
pub(crate) fn limit_node(
    plan: Arc<dyn ExecutionPlan>,
    bound: RowBound,
) -> Arc<dyn ExecutionPlan> {
    match bound.input_rows() {
        Some(fetch)
            if bound.skip == 0 && plan.output_partitioning().partition_count() > 1 =>
        {
            Arc::new(LocalLimitExec::new(plan, fetch))
        }
        _ => Arc::new(GlobalLimitExec::new(plan, bound.skip, bound.fetch)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_plan::empty::EmptyExec;

    fn bound(skip: usize, fetch: Option<usize>) -> RowBound {
        RowBound { skip, fetch }
    }

    #[test]
    fn over_applies_inner_first() {
        // LIMIT 5 over (OFFSET 2 LIMIT 10): rows 2..7 of the input.
        assert_eq!(
            bound(0, Some(5)).over(bound(2, Some(10))),
            bound(2, Some(5))
        );
        // OFFSET 3 over LIMIT 10: 7 rows remain after the skip.
        assert_eq!(bound(3, None).over(bound(0, Some(10))), bound(3, Some(7)));
    }

    #[test]
    fn min_keeps_the_tighter_fetch() {
        assert_eq!(
            RowBound::fetch(Some(3)).min(RowBound::fetch(None)),
            bound(0, Some(3))
        );
        assert_eq!(
            RowBound::fetch(Some(3)).min(RowBound::fetch(Some(2))),
            bound(0, Some(2))
        );
    }

    #[test]
    fn input_rows_saturates() {
        assert_eq!(bound(5, Some(10)).input_rows(), Some(15));
        assert_eq!(bound(5, None).input_rows(), None);
        assert_eq!(bound(usize::MAX, Some(1)).input_rows(), Some(usize::MAX));
    }

    #[test]
    fn limit_node_is_local_only_without_skip_over_many_partitions() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let one: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let many: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(schema).with_partitions(4));

        let node = limit_node(Arc::clone(&many), bound(0, Some(3)));
        assert_eq!(node.downcast_ref::<LocalLimitExec>().unwrap().fetch(), 3);

        let node = limit_node(Arc::clone(&many), bound(2, Some(3)));
        let global = node.downcast_ref::<GlobalLimitExec>().unwrap();
        assert_eq!((global.skip(), global.fetch()), (2, Some(3)));

        let node = limit_node(one, bound(0, Some(3)));
        assert!(node.is::<GlobalLimitExec>());
    }
}
