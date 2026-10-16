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

//! Tests of the checks that run stream experiments (boundedness, emission,
//! laziness, resource release and error propagation), using plans that break
//! those properties on purpose.

use std::sync::Arc;
use std::time::Duration;

use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::execution_plan::{
    Boundedness, EmissionType, EvaluationType,
};
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{ExecutionPlan, Partitioning};
use datafusion_property_tests::physical_plan::fixtures::StatisticsPrecision;
use datafusion_property_tests::physical_plan::{
    CheckKind, Experiment, PlanCheck, PlanChecker,
};
use datafusion_property_tests::{Finding, Report, Severity};

use crate::common::{
    ConfigurableExec, Effect, FetchMode, HoldInput, OnInputError, checker, exact_source,
    messages, schema, source, summary,
};

/// `checker` with short timeouts, so that plans that never end are reported
/// quickly
fn with_short_timeouts(checker: PlanChecker) -> PlanChecker {
    checker
        .with_timeout(Duration::from_millis(300))
        .with_stream_timeout(Duration::from_millis(300))
}

/// Run only the check named `name`
fn check_with(name: &str, plan: &Arc<dyn ExecutionPlan>) -> Report {
    with_short_timeouts(checker(&[name])).check(plan).unwrap()
}

fn multi_partition_source() -> Arc<dyn ExecutionPlan> {
    source(&[40, 0, 60], StatisticsPrecision::Exact)
}

#[test]
fn correct_plans_are_clean() {
    let mut plans = vec![];
    for input in [exact_source(100), multi_partition_source()] {
        plans.push(ConfigurableExec::new(Arc::clone(&input)).build());

        // Eager and buffering nodes that report it
        let mut eager = ConfigurableExec::new(Arc::clone(&input));
        eager.eager = true;
        eager.evaluation_type = Some(EvaluationType::Eager);
        plans.push(eager.build());

        let mut buffering = ConfigurableExec::new(Arc::clone(&input));
        buffering.buffer_all = true;
        buffering.emission_type = Some(EmissionType::Final);
        plans.push(buffering.build());

        // A fetch that makes the output bounded, and is reported as such
        let mut limited = ConfigurableExec::new(Arc::clone(&input))
            .fetch(5)
            .effect(Effect::LowerEqual);
        limited.fetch_mode = FetchMode::Enforce;
        limited.boundedness = Some(Boundedness::Bounded);
        plans.push(limited.build());
    }
    // The fetch plan does not apply its fetch to its statistics, so only run
    // the stream checks, `execution_succeeds` and `memory_released`
    let stream_checks = checker(&[
        "execution_succeeds",
        "boundedness_holds",
        "emission_type_holds",
        "lazy_evaluation_holds",
        "memory_released",
        "streams_released",
        "errors_propagate",
    ]);
    for plan in plans {
        with_short_timeouts(stream_checks.clone())
            .check(&plan)
            .unwrap()
            .assert_clean();
    }
}

#[test]
fn bounded_claim_that_does_not_hold() {
    let mut exec = ConfigurableExec::new(multi_partition_source());
    exec.boundedness = Some(Boundedness::Bounded);
    let lying = exec.build();
    let report = check_with("boundedness_holds", &lying);
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "boundedness_holds")]
    );
    assert!(messages(&report)[0].contains("did not end"), "{report}");

    // A parent that trusts the child's claim is not reported
    let parent = ConfigurableExec::new(lying).build();
    let report = check_with("boundedness_holds", &parent);
    assert_eq!(report.violations.len(), 1, "{report}");
    assert_eq!(report.violations[0].path, vec![0]);
}

#[test]
fn fetch_that_makes_the_output_bounded_is_a_lint() {
    let mut exec = ConfigurableExec::new(multi_partition_source()).fetch(5);
    exec.fetch_mode = FetchMode::Enforce;
    let plan = exec.build();
    let report = check_with("boundedness_holds", &plan);
    assert_eq!(
        summary(&report),
        vec![(Severity::Lint, "boundedness_holds")]
    );

    // Not for a node that can drop rows, since its output only ends if enough
    // rows remain
    let mut exec = ConfigurableExec::new(multi_partition_source())
        .fetch(5)
        .effect(Effect::LowerEqual);
    exec.fetch_mode = FetchMode::Enforce;
    let plan = exec.build();
    check_with("boundedness_holds", &plan).assert_clean();
}

#[test]
fn incremental_claim_that_does_not_hold() {
    let mut exec = ConfigurableExec::new(multi_partition_source());
    exec.buffer_all = true;
    let plan = exec.build();
    let report = check_with("emission_type_holds", &plan);
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "emission_type_holds")]
    );
    assert!(
        messages(&report)[0].contains("produced no rows"),
        "{report}"
    );
}

#[test]
fn incremental_node_over_a_final_child_is_not_checked() {
    // The child waits for all of its input, and says so, so the parent
    // cannot produce output before the input ends either
    let mut child = ConfigurableExec::new(multi_partition_source());
    child.buffer_all = true;
    child.emission_type = Some(EmissionType::Final);
    let mut parent = ConfigurableExec::new(child.build());
    parent.emission_type = Some(EmissionType::Incremental);
    let plan = parent.build();
    check_with("emission_type_holds", &plan).assert_clean();
}

#[test]
fn lazy_claim_that_does_not_hold() {
    let mut exec = ConfigurableExec::new(multi_partition_source());
    exec.eager = true;
    let eager = exec.build();
    let report = check_with("lazy_evaluation_holds", &eager);
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "lazy_evaluation_holds")]
    );
    assert!(
        messages(&report)[0].contains("partitions [0, 1, 2] of child 0"),
        "{report}"
    );

    // A lazy parent of an eager child only polls the child on demand, even
    // though the child's own input is polled by a spawned task
    let mut exec = ConfigurableExec::new(multi_partition_source());
    exec.eager = true;
    exec.evaluation_type = Some(EvaluationType::Eager);
    let mut parent = ConfigurableExec::new(exec.build());
    parent.evaluation_type = Some(EvaluationType::Lazy);
    let plan = parent.build();
    check_with("lazy_evaluation_holds", &plan).assert_clean();
}

#[test]
fn input_streams_held_by_the_plan_are_a_lint() {
    let mut exec = ConfigurableExec::new(multi_partition_source());
    exec.hold_input = HoldInput::InPlan;
    let plan = exec.build();
    let report = check_with("streams_released", &plan);
    assert_eq!(summary(&report), vec![(Severity::Lint, "streams_released")]);
    assert!(
        messages(&report)[0].contains("only dropped once the plan itself was dropped"),
        "{report}"
    );
}

#[test]
fn input_streams_that_are_never_dropped() {
    let mut exec = ConfigurableExec::new(multi_partition_source());
    exec.hold_input = HoldInput::Forever;
    let plan = exec.build();
    let report = check_with("streams_released", &plan);
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "streams_released")]
    );
    assert!(
        messages(&report)[0].contains("partitions [0, 1, 2] of child 0 were still alive"),
        "{report}"
    );
}

#[test]
fn input_streams_released_later_by_a_task_are_clean() {
    // A spawned task drops the input streams some time after the output
    // streams are dropped. The checker keeps waiting while a task is alive.
    let mut exec = ConfigurableExec::new(multi_partition_source());
    exec.hold_input = HoldInput::InTask;
    let plan = exec.build();
    check_with("streams_released", &plan).assert_clean();
}

#[test]
fn errors_that_do_not_propagate() {
    for (on_error, expected) in [
        (OnInputError::Swallow, "ended without returning an error"),
        (OnInputError::Hang, "neither returned an error nor ended"),
        (
            OnInputError::Panic,
            "panicked: ConfigurableExec got an error",
        ),
    ] {
        let mut exec = ConfigurableExec::new(multi_partition_source());
        exec.on_input_error = on_error;
        let plan = exec.build();
        let report = check_with("errors_propagate", &plan);
        assert_eq!(
            summary(&report),
            vec![(Severity::Invariant, "errors_propagate")],
            "{on_error:?}"
        );
        assert!(
            messages(&report)[0].contains(expected),
            "{on_error:?}: {report}"
        );
    }

    // A parent of a node that swallows errors never sees one
    let mut exec = ConfigurableExec::new(multi_partition_source());
    exec.on_input_error = OnInputError::Swallow;
    let parent = ConfigurableExec::new(exec.build()).build();
    let report = check_with("errors_propagate", &parent);
    assert_eq!(report.violations.len(), 1, "{report}");
    assert_eq!(report.violations[0].path, vec![0]);
}

#[test]
fn error_after_the_fetch_is_satisfied_does_not_have_to_propagate() {
    // The spawned task reads the error into its buffer, but the output has
    // enough rows for its fetch before it gets there
    let mut exec = ConfigurableExec::new(exact_source(100))
        .fetch(1)
        .effect(Effect::LowerEqual);
    exec.eager = true;
    exec.fetch_mode = FetchMode::Enforce;
    exec.on_input_error = OnInputError::Swallow;
    let plan = exec.build();
    check_with("errors_propagate", &plan).assert_clean();
}

/// A user defined check that reads the laziness experiment and reports each
/// input partition that was polled without demand
const COUNT_UNDEMANDED_POLLS: PlanCheck = PlanCheck {
    name: "count_undemanded_polls",
    kind: CheckKind::Stream,
    check: |node, context| {
        Ok(context
            .stream_runs(node, Experiment::Laziness)
            .iter()
            .flat_map(|run| run.inputs.iter().flatten())
            .filter(|observation| observation.polls_without_demand > 0)
            .map(|_| Finding::lint("undemanded"))
            .collect())
    },
};

#[test]
fn laziness_experiment_tells_eager_from_lazy_builtin_plans() {
    let schema = schema();
    let repartition: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            multi_partition_source(),
            Partitioning::RoundRobinBatch(2),
        )
        .unwrap(),
    );
    let projection: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![(col("a", &schema).unwrap(), "a".to_string())],
            multi_partition_source(),
        )
        .unwrap(),
    );
    // The repartition polls every input partition from a spawned task
    let checker = PlanChecker::with_checks(vec![COUNT_UNDEMANDED_POLLS]);
    let report = checker.check(&repartition).unwrap();
    assert_eq!(report.violations.len(), 3, "{report}");
    checker.check(&projection).unwrap().assert_clean();
}

#[test]
fn limit_that_needs_more_rows_than_the_input_serves_is_not_reported() {
    // Unbounded leaves stall after a limit on the rows they serve. A limit
    // that needs more rows than that to end cannot be shown not to end.
    let large_fetch: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(exact_source(40), 0, Some(10_000_000)));
    let large_skip: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(exact_source(40), 10_000_000, Some(10)));
    for plan in [large_fetch, large_skip] {
        check_with("boundedness_holds", &plan).assert_clean();
        check_with("emission_type_holds", &plan).assert_clean();
    }

    // Without a fetch, the limit never ends, whatever the row limit
    let no_fetch: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(exact_source(40), 5, None));
    assert_eq!(
        summary(&check_with("boundedness_holds", &no_fetch)),
        vec![(Severity::Invariant, "boundedness_holds")]
    );
}
