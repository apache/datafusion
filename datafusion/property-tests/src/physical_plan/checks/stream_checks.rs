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

//! `CheckKind::Stream` checks: they compare how a node reports its streams
//! behave (boundedness, emission type, evaluation type) with how they behave
//! in stream experiments, and check that it releases its input streams and
//! propagates errors.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, EvaluationType,
};

use crate::Finding;
use crate::physical_plan::{CheckContext, Experiment, PartitionObservation, RunOutcome};

/// B10: a node that reports `Bounded` ends on unbounded input.
pub(super) fn boundedness_holds(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    // True if the node, without its fetch, passes every input row through. Its
    // output then ends as soon as `fetch` rows reach it, which is the
    // guarantee `GlobalLimitExec` reports as `Bounded`. A node that can drop
    // rows, such as a filter, only ends if enough rows survive.
    let fetch_is_only_limit = || {
        node.with_fetch(None).is_some_and(|plan| {
            matches!(plan.cardinality_effect(), CardinalityEffect::Equal)
        })
    };
    let mut findings = vec![];
    for run in context.stream_runs(node, Experiment::UnboundedInput) {
        let Some(child) = run.varied_child else {
            continue;
        };
        let child_unbounded = run
            .input_boundedness
            .get(child)
            .is_some_and(Boundedness::is_unbounded);
        match (&run.outcome, run.boundedness) {
            // A child that reports Bounded is responsible for its own claim,
            // and a child that delivered nothing gave the node no chance to
            // end. A node with a fetch may need more rows than the leaves were
            // allowed to serve (a large fetch or offset), so a run in which
            // they reached their row limit shows nothing. Without a fetch,
            // nothing justifies needing a particular number of rows before
            // ending.
            (RunOutcome::TimedOut, Boundedness::Bounded)
                if child_unbounded
                    && run.input_rows(child) > 0
                    && !(run.inputs_exhausted && run.fetch.is_some()) =>
            {
                findings.push(Finding::invariant(format!(
                    "rebuilt with child {child} unbounded, the node reports \
                     Boundedness::Bounded, but its output did not end while the child \
                     kept delivering rows"
                )));
            }
            (RunOutcome::Ended, Boundedness::Unbounded { .. })
                if run.fetch.is_some()
                    && run.output_errors() == 0
                    && fetch_is_only_limit() =>
            {
                findings.push(Finding::lint(format!(
                    "rebuilt with child {child} unbounded, the node reports \
                     Boundedness::Unbounded, but it has fetch {:?} and otherwise passes \
                     every row through, so like GlobalLimitExec its output ends once the \
                     fetch is reached",
                    run.fetch
                )));
            }
            _ => {}
        }
    }
    Ok(findings)
}

/// B11: a node that reports `EmissionType::Incremental` produces output
/// before its input ends.
pub(super) fn emission_type_holds(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    // A node that produces no rows from the finite input cannot be expected
    // to produce rows from a repetition of it
    if context
        .output(node)
        .is_none_or(|output| output.num_rows() == 0)
    {
        return Ok(vec![]);
    }
    let mut findings = vec![];
    for run in context.stream_runs(node, Experiment::UnboundedInput) {
        let Some(child) = run.varied_child else {
            continue;
        };
        // Runs where the leaves reached their row limit are not skipped: a
        // node that buffers all of its input always reaches it, and that is
        // the main problem this check finds
        if run.emission_type != EmissionType::Incremental
            || run.outcome != RunOutcome::TimedOut
            || run.output_rows() > 0
            || run.input_rows(child) == 0
        {
            continue;
        }
        findings.push(Finding::invariant(format!(
            "rebuilt with child {child} unbounded, the node reports \
             EmissionType::Incremental, but it produced no rows before timing out \
             while the child kept delivering rows; report EmissionType::Final if the \
             node waits for the end of its input"
        )));
    }
    Ok(findings)
}

/// B12: a node that reports `EvaluationType::Lazy` only polls its inputs
/// while its own output is being polled.
pub(super) fn lazy_evaluation_holds(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    if node.properties().evaluation_type != EvaluationType::Lazy {
        return Ok(vec![]);
    }
    let mut findings = vec![];
    for run in context.stream_runs(node, Experiment::Laziness) {
        if matches!(run.outcome, RunOutcome::Failed(_)) {
            continue;
        }
        for (child, partitions) in run.inputs.iter().enumerate() {
            let undemanded: Vec<usize> = partitions
                .iter()
                .enumerate()
                .filter(|(_, observation)| observation.polls_without_demand > 0)
                .map(|(p, _)| p)
                .collect();
            if !undemanded.is_empty() {
                findings.push(Finding::invariant(format!(
                    "the node reports EvaluationType::Lazy, but polled partitions \
                     {undemanded:?} of child {child} while its own output was not being \
                     polled, for example from a spawned task or in execute(); report \
                     EvaluationType::Eager"
                )));
            }
        }
    }
    Ok(findings)
}

/// Partitions, per child, that still have live streams
fn alive_partitions(inputs: &[Vec<PartitionObservation>]) -> Vec<(usize, Vec<usize>)> {
    inputs
        .iter()
        .enumerate()
        .filter_map(|(child, partitions)| {
            let alive: Vec<usize> = partitions
                .iter()
                .enumerate()
                .filter(|(_, observation)| observation.streams_alive() > 0)
                .map(|(p, _)| p)
                .collect();
            (!alive.is_empty()).then_some((child, alive))
        })
        .collect()
}

/// F1: the input streams of a node are released once its output streams are
/// dropped part way.
pub(super) fn streams_released(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let mut findings = vec![];
    for run in context.stream_runs(node, Experiment::Cancellation) {
        if matches!(run.outcome, RunOutcome::Failed(_) | RunOutcome::Panicked(_)) {
            continue;
        }
        let alive = alive_partitions(&run.inputs);
        if alive.is_empty() {
            continue;
        }
        let after_plan = run
            .inputs_after_plan_dropped
            .as_deref()
            .map(alive_partitions)
            .unwrap_or_else(|| alive.clone());
        if after_plan.is_empty() {
            for (child, partitions) in alive {
                findings.push(Finding::lint(format!(
                    "after the output streams were dropped part way, the streams of \
                     partitions {partitions:?} of child {child} were only dropped once \
                     the plan itself was dropped; the plan keeps input streams, or tasks \
                     that poll them, alive"
                )));
            }
        } else {
            for (child, partitions) in after_plan {
                findings.push(Finding::invariant(format!(
                    "after the output streams were dropped part way, and then the plan \
                     itself, the streams of partitions {partitions:?} of child {child} \
                     were still alive; tie spawned tasks and input streams to the output \
                     stream"
                )));
            }
        }
    }
    Ok(findings)
}

/// F2: an error from an input makes the output return an error.
pub(super) fn errors_propagate(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let mut findings = vec![];
    for run in context.stream_runs(node, Experiment::InputError) {
        // An input error the node never received does not have to be
        // returned. Neither does one received after the node had enough rows
        // for its fetch: an eager node can read the error into a buffer while
        // its output is still serving earlier batches.
        let fetch_satisfied = run
            .fetch
            .is_some_and(|fetch| run.output.iter().any(|o| o.rows >= fetch));
        if run.input_errors() == 0 || fetch_satisfied {
            continue;
        }
        let message = match &run.outcome {
            RunOutcome::Ended if run.output_errors() == 0 => {
                "an input returned an error, but every output partition ended without \
                 returning an error"
                    .to_string()
            }
            RunOutcome::TimedOut if run.output_errors() == 0 => {
                "an input returned an error, but the output neither returned an error \
                 nor ended before timing out"
                    .to_string()
            }
            RunOutcome::Panicked(panic) => {
                format!("an input returned an error, and the node panicked: {panic}")
            }
            _ => continue,
        };
        findings.push(Finding::invariant(message));
    }
    Ok(findings)
}
