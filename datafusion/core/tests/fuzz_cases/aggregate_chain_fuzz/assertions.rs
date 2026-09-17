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

//! Assertions on plan shape and metrics.

use super::*;

/// All `AggregateExec` nodes in the plan, bottom-up.
pub(super) fn aggregate_nodes(
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut nodes = vec![];
    let mut node = Arc::clone(plan);
    loop {
        if node.downcast_ref::<AggregateExec>().is_some() {
            nodes.push(Arc::clone(&node));
        }
        match node.children().first() {
            Some(child) => node = Arc::clone(child),
            None => break,
        }
    }
    nodes.reverse();
    nodes
}

pub(super) fn as_aggregate(node: &Arc<dyn ExecutionPlan>) -> &AggregateExec {
    node.downcast_ref::<AggregateExec>().unwrap()
}

/// Expected source order seen by each aggregate stage, bottom-up. Ordering is
/// lost at `HashRepartition` and `CoalescePartitions`, and kept by the
/// order-preserving shuffles and by aggregate stages themselves.
pub(super) fn expected_orders(shape: &Shape, source_order: Order) -> Vec<Order> {
    let mut current = source_order;
    let mut expected = vec![];
    for operator in shape.chain.operators {
        match operator {
            HashRepartition | CoalescePartitions => current = Order::Unordered,
            // `AggregateExec::try_new` forces `InputOrderMode::Linear` for
            // partial reduce, since it emits its groups in hash table order,
            // and it advertises no output ordering either. Everything above it
            // is unordered until something sorts again.
            Aggregate(PartialReduce) => {
                expected.push(Order::Unordered);
                current = Order::Unordered;
            }
            Aggregate(_) | TopK(_) => expected.push(current),
            OrderPreservingHashRepartition | SortPreservingMerge => {}
        }
    }
    expected
}

pub(super) fn order_matches(
    query: Query,
    expected: Order,
    actual: &InputOrderMode,
) -> bool {
    // With a single group key, sorting by the first key already covers every
    // group key.
    let single_key = query.keys.columns().len() == 1;
    match (expected, actual) {
        (Order::Unordered, InputOrderMode::Linear) => true,
        (Order::SortedByFirstKey, InputOrderMode::PartiallySorted(indices)) => {
            !single_key && indices == &[0]
        }
        (Order::SortedByFirstKey, InputOrderMode::Sorted) => single_key,
        (Order::SortedByAllKeys, InputOrderMode::Sorted) => true,
        _ => false,
    }
}

/// Whether this stage's stream is allowed to spill.
pub(super) fn can_spill(aggregate: &AggregateExec) -> bool {
    if aggregate.limit_options().is_some() {
        // GroupedTopKAggregateStream keeps a bounded heap and never spills
        return false;
    }
    let spilling_mode = match aggregate.mode() {
        Final | FinalPartitioned | Single | SinglePartitioned => true,
        // Both partial streams emit their state early instead of spilling.
        PartialReduce | Partial => false,
    };
    let has_groups = !aggregate.group_expr().is_empty();
    spilling_mode && has_groups && *aggregate.input_order_mode() != InputOrderMode::Sorted
}

/// Whether this stage runs the skip-partial probe.
pub(super) fn runs_skip_partial_probe(aggregate: &AggregateExec) -> bool {
    *aggregate.mode() == Partial
        && aggregate.limit_options().is_none()
        && !aggregate.group_expr().is_empty()
        && *aggregate.input_order_mode() == InputOrderMode::Linear
}

pub(super) fn check_plan_shape(case: &Case, plan: &Arc<dyn ExecutionPlan>) {
    if case.shape.query.keys == Keys::None {
        return;
    }
    let nodes = aggregate_nodes(plan);
    let expected = expected_orders(&case.shape, case.params.order);
    assert_eq!(nodes.len(), expected.len(), "{case:?}");
    for (node, expected_order) in nodes.iter().zip(expected) {
        let aggregate = as_aggregate(node);
        assert!(
            order_matches(
                case.shape.query,
                expected_order,
                aggregate.input_order_mode()
            ),
            "{case:?}: expected {expected_order:?} got {:?}\n{}",
            aggregate.input_order_mode(),
            displayable(plan.as_ref()).indent(true)
        );
    }
}

/// Returns a description of every stage that spilled, bottom-up, such as
/// `Final(Linear)`.
pub(super) fn check_metrics(case: &Case, plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut spilled = vec![];
    for node in aggregate_nodes(plan) {
        let aggregate = as_aggregate(&node);
        let mode = aggregate.mode();
        let metrics = node.metrics().unwrap();
        let spill_count = metrics.spill_count().unwrap_or(0);
        if spill_count > 0 {
            spilled.push(format!("{mode:?}({:?})", aggregate.input_order_mode()));
        }
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|metric| metric.as_usize())
            .unwrap_or(0);

        match case.params.memory {
            Memory::Unlimited => {
                assert_eq!(spill_count, 0, "{case:?}: unexpected spill in {mode:?}");
            }
            Memory::Limited => {
                // Whether a spilling-capable stage actually spills depends on
                // the pool geometry, so only the run-wide coverage check in the
                // driver requires it. Streams that cannot spill must not.
                if !can_spill(aggregate) {
                    assert_eq!(spill_count, 0, "{case:?}: {mode:?} must never spill");
                }
            }
        }

        // Boolean keys have two groups whatever `cardinality` says, far
        // below the ratio.
        if case.params.memory == Memory::Unlimited
            && case.params.cardinality == Cardinality::VeryHigh
            && case.shape.query.keys.tracks_cardinality()
            && case.params.skip_partial_enabled
            && runs_skip_partial_probe(aggregate)
        {
            assert!(
                skipped_rows > 0,
                "{case:?}: skip-partial probe did not fire"
            );
        }
        if !case.params.skip_partial_enabled || !runs_skip_partial_probe(aggregate) {
            assert_eq!(skipped_rows, 0, "{case:?}: skip-partial fired in {mode:?}");
        }
    }
    spilled
}
