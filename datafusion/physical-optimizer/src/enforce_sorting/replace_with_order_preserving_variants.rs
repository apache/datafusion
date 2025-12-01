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

//! Optimizer rule that replaces executors that lose ordering with their
//! order-preserving variants when it is helpful; either in terms of
//! performance or to accommodate unbounded streams by fixing the pipeline.

use std::sync::Arc;

use crate::utils::{
    is_coalesce_partitions, is_repartition, is_sort, is_sort_preserving_merge,
};

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{assert_or_internal_err, Result};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::ExecutionPlanProperties;

use itertools::izip;

/// For a given `plan`, this object carries the information one needs from its
/// descendants to decide whether it is beneficial to replace order-losing (but
/// somewhat faster) variants of certain operators with their order-preserving
/// (but somewhat slower) cousins.
pub type OrderPreservationContext = PlanContext<bool>;

/// Updates order-preservation data for all children of the given node.
pub fn update_order_preservation_ctx_children_data(opc: &mut OrderPreservationContext) {
    for PlanContext {
        plan,
        children,
        data,
    } in opc.children.iter_mut()
    {
        let maintains_input_order = plan.maintains_input_order();
        let inspect_child = |idx| {
            maintains_input_order[idx]
                || is_coalesce_partitions(plan)
                || is_repartition(plan)
        };

        // We cut the path towards nodes that do not maintain ordering.
        for (idx, c) in children.iter_mut().enumerate() {
            c.data &= inspect_child(idx);
        }

        let plan_children = plan.children();
        *data = if plan_children.is_empty() {
            false
        } else if !children[0].data
            && ((is_repartition(plan) && !maintains_input_order[0])
                || (is_coalesce_partitions(plan)
                    && plan_children[0].output_ordering().is_some()))
        {
            // We either have a RepartitionExec or a CoalescePartitionsExec
            // and they lose their input ordering, so initiate connection:
            true
        } else {
            // Maintain connection if there is a child with a connection,
            // and operator can possibly maintain that connection (either
            // in its current form or when we replace it with the corresponding
            // order preserving operator).
            children
                .iter()
                .enumerate()
                .any(|(idx, c)| c.data && inspect_child(idx))
        }
    }
    opc.data = false;
}

/// Calculates the updated plan by replacing operators that lose ordering
/// inside `sort_input` with their order-preserving variants. This will
/// generate an alternative plan, which will be accepted or rejected later on
/// depending on whether it helps us remove a `SortExec`.
pub fn plan_with_order_preserving_variants(
    mut sort_input: OrderPreservationContext,
    // Flag indicating that it is desirable to replace `RepartitionExec`s with
    // `SortPreservingRepartitionExec`s:
    is_spr_better: bool,
    // Flag indicating that it is desirable to replace `CoalescePartitionsExec`s
    // with `SortPreservingMergeExec`s:
    is_spm_better: bool,
    fetch: Option<usize>,
) -> Result<OrderPreservationContext> {
    sort_input.children = sort_input
        .children
        .into_iter()
        .map(|node| {
            // Update descendants in the given tree if there is a connection:
            if node.data {
                plan_with_order_preserving_variants(
                    node,
                    is_spr_better,
                    is_spm_better,
                    fetch,
                )
            } else {
                Ok(node)
            }
        })
        .collect::<Result<_>>()?;
    sort_input.data = false;

    if is_repartition(&sort_input.plan)
        && !sort_input.plan.maintains_input_order()[0]
        && is_spr_better
    {
        // When a `RepartitionExec` doesn't preserve ordering, replace it with
        // a sort-preserving variant if appropriate:
        let child = Arc::clone(&sort_input.children[0].plan);
        let partitioning = sort_input.plan.output_partitioning().clone();
        sort_input.plan = Arc::new(
            RepartitionExec::try_new(child, partitioning)?.with_preserve_order(),
        ) as _;
        sort_input.children[0].data = true;
        return Ok(sort_input);
    } else if is_coalesce_partitions(&sort_input.plan) && is_spm_better {
        let child = &sort_input.children[0].plan;
        if let Some(ordering) = child.output_ordering() {
            let mut fetch = fetch;
            if let Some(coalesce_fetch) = sort_input.plan.fetch() {
                fetch = match fetch {
                    Some(sort_fetch) => {
                        assert_or_internal_err!(
                            coalesce_fetch >= sort_fetch,
                            "CoalescePartitionsExec fetch [{:?}] should be greater than or equal to SortExec fetch [{:?}]",
                            coalesce_fetch,
                            sort_fetch
                        );
                        Some(sort_fetch)
                    }
                    None => {
                        // If the sort node does not have a fetch, we need to keep the coalesce node's fetch.
                        Some(coalesce_fetch)
                    }
                };
            };
            // When the input of a `CoalescePartitionsExec` has an ordering,
            // replace it with a `SortPreservingMergeExec` if appropriate:
            let spm = SortPreservingMergeExec::new(ordering.clone(), Arc::clone(child))
                .with_fetch(fetch);
            sort_input.plan = Arc::new(spm) as _;
            sort_input.children[0].data = true;
            return Ok(sort_input);
        }
    }

    sort_input.update_plan_from_children()
}

/// Calculates the updated plan by replacing operators that preserve ordering
/// inside `sort_input` with their order-breaking variants. This will restore
/// the original plan modified by [`plan_with_order_preserving_variants`].
pub fn plan_with_order_breaking_variants(
    mut sort_input: OrderPreservationContext,
) -> Result<OrderPreservationContext> {
    let plan = &sort_input.plan;
    sort_input.children = izip!(
        sort_input.children,
        plan.maintains_input_order(),
        plan.required_input_ordering()
    )
    .map(|(node, maintains, required_ordering)| {
        // Replace with non-order preserving variants as long as ordering is
        // not required by intermediate operators:
        if !maintains {
            return Ok(node);
        } else if is_sort_preserving_merge(plan) {
            return plan_with_order_breaking_variants(node);
        } else if let Some(required_ordering) = required_ordering {
            let eqp = node.plan.equivalence_properties();
            if eqp.ordering_satisfy_requirement(required_ordering.into_single())? {
                return Ok(node);
            }
        }
        plan_with_order_breaking_variants(node)
    })
    .collect::<Result<_>>()?;
    sort_input.data = false;

    if is_repartition(plan) && plan.maintains_input_order()[0] {
        // When a `RepartitionExec` preserves ordering, replace it with a
        // non-sort-preserving variant:
        let child = Arc::clone(&sort_input.children[0].plan);
        let partitioning = plan.output_partitioning().clone();
        sort_input.plan = Arc::new(RepartitionExec::try_new(child, partitioning)?) as _;
    } else if is_sort_preserving_merge(plan) {
        // Replace `SortPreservingMergeExec` with a `CoalescePartitionsExec`
        // SPM may have `fetch`, so pass it to the `CoalescePartitionsExec`
        let child = Arc::clone(&sort_input.children[0].plan);
        let coalesce =
            Arc::new(CoalescePartitionsExec::new(child).with_fetch(plan.fetch()));
        sort_input.plan = coalesce;
    } else {
        return sort_input.update_plan_from_children();
    }

    sort_input.children[0].data = false;
    Ok(sort_input)
}

/// The `replace_with_order_preserving_variants` optimizer sub-rule tries to
/// remove `SortExec`s from the physical plan by replacing operators that do
/// not preserve ordering with their order-preserving variants; i.e. by replacing
/// ordinary `RepartitionExec`s with their sort-preserving variants or by replacing
/// `CoalescePartitionsExec`s with `SortPreservingMergeExec`s.
///
/// If this replacement is helpful for removing a `SortExec`, it updates the plan.
/// Otherwise, it leaves the plan unchanged.
///
/// NOTE: This optimizer sub-rule will only produce sort-preserving `RepartitionExec`s
/// if the query is bounded or if the config option `prefer_existing_sort` is
/// set to `true`.
///
/// The algorithm flow is simply like this:
/// 1. Visit nodes of the physical plan bottom-up and look for `SortExec` nodes.
///    During the traversal, keep track of operators that maintain ordering (or
///    can maintain ordering when replaced by an order-preserving variant) until
///    a `SortExec` is found.
/// 2. When a `SortExec` is found, update the child of the `SortExec` by replacing
///    operators that do not preserve ordering in the tree with their order
///    preserving variants.
/// 3. Check if the `SortExec` is still necessary in the updated plan by comparing
///    its input ordering with the output ordering it imposes. We do this because
///    replacing operators that lose ordering with their order-preserving variants
///    enables us to preserve the previously lost ordering at the input of `SortExec`.
/// 4. If the `SortExec` in question turns out to be unnecessary, remove it and
///    use updated plan. Otherwise, use the original plan.
/// 5. Continue the bottom-up traversal until another `SortExec` is seen, or the
///    traversal is complete.
pub fn replace_with_order_preserving_variants(
    mut requirements: OrderPreservationContext,
    // A flag indicating that replacing `RepartitionExec`s with sort-preserving
    // variants is desirable when it helps to remove a `SortExec` from the plan.
    // If this flag is `false`, this replacement should only be made to fix the
    // pipeline (streaming).
    is_spr_better: bool,
    // A flag indicating that replacing `CoalescePartitionsExec`s with
    // `SortPreservingMergeExec`s is desirable when it helps to remove a
    // `SortExec` from the plan. If this flag is `false`, this replacement
    // should only be made to fix the pipeline (streaming).
    is_spm_better: bool,
    config: &ConfigOptions,
) -> Result<Transformed<OrderPreservationContext>> {
    update_order_preservation_ctx_children_data(&mut requirements);
    if !(is_sort(&requirements.plan) && requirements.children[0].data) {
        return Ok(Transformed::no(requirements));
    }

    // For unbounded cases, we replace with the order-preserving variant in any
    // case, as doing so helps fix the pipeline. Also replace if config allows.
    let use_order_preserving_variant = config.optimizer.prefer_existing_sort
        || (requirements.plan.boundedness().is_unbounded()
            && requirements.plan.pipeline_behavior() == EmissionType::Final);

    // Create an alternate plan with order-preserving variants:
    let mut alternate_plan = plan_with_order_preserving_variants(
        requirements.children.swap_remove(0),
        is_spr_better || use_order_preserving_variant,
        is_spm_better || use_order_preserving_variant,
        requirements.plan.fetch(),
    )?;

    // If the alternate plan makes this sort unnecessary, accept the alternate:
    if let Some(ordering) = requirements.plan.output_ordering() {
        let eqp = alternate_plan.plan.equivalence_properties();
        if !eqp.ordering_satisfy(ordering.clone())? {
            // The alternate plan does not help, use faster order-breaking variants:
            alternate_plan = plan_with_order_breaking_variants(alternate_plan)?;
            alternate_plan.data = false;
            requirements.children = vec![alternate_plan];
            return Ok(Transformed::yes(requirements));
        }
    }
    for child in alternate_plan.children.iter_mut() {
        child.data = false;
    }
    Ok(Transformed::yes(alternate_plan))
}
