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

use std::sync::{Arc, Mutex};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_optimizer::invariants::{
    HashJoinModesResolved, InputRequirementsSatisfied, NoEmbeddedProjections,
};
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::optimizer::{
    PhysicalOptimizer, PhysicalOptimizerStep as Step, PlanInvariant,
};
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::{PhysicalOptimizerContext, PhysicalOptimizerRule};
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{JoinType, Result, assert_contains, plan_err};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{Partitioning, PhysicalSortExpr};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::joins::{HashJoinExec, HashJoinExecBuilder, PartitionMode};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, displayable};

type Plan = Arc<dyn ExecutionPlan>;

const VALIDATE_INVARIANTS: &str = "datafusion.optimizer.validate_optimizer_invariants";

fn empty(fields: &[&str]) -> Plan {
    Arc::new(EmptyExec::new(Arc::new(Schema::new(
        fields
            .iter()
            .map(|name| Field::new(*name, DataType::Int32, false))
            .collect::<Vec<_>>(),
    ))))
}

fn join(mode: PartitionMode) -> Result<Plan> {
    let left = empty(&["a"]);
    let right = empty(&["b"]);
    Ok(Arc::new(
        HashJoinExecBuilder::new(
            Arc::clone(&left),
            Arc::clone(&right),
            vec![(col("a", &left.schema())?, col("b", &right.schema())?)],
            JoinType::Inner,
        )
        .with_partition_mode(mode)
        .build()?,
    ))
}

fn rule(rule: impl PhysicalOptimizerRule + Send + Sync + 'static) -> Step {
    Step::Rule(Arc::new(rule))
}

fn optimize(plan: Plan, steps: Vec<Step>) -> Result<Plan> {
    optimize_with_config(
        plan,
        steps,
        SessionConfig::new().set_bool(VALIDATE_INVARIANTS, true),
    )
}

fn optimize_with_config(
    plan: Plan,
    steps: Vec<Step>,
    config: SessionConfig,
) -> Result<Plan> {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_physical_optimizer_steps(steps)
        .build();
    DefaultPhysicalPlanner::default().optimize_physical_plan(plan, &state, |_, _| {})
}

#[derive(Debug)]
struct SetJoinMode(PartitionMode);

impl PhysicalOptimizerRule for SetJoinMode {
    fn optimize(&self, plan: Plan, _: &ConfigOptions) -> Result<Plan> {
        plan.transform_up(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                let changed = join.builder().with_partition_mode(self.0).build()?;
                Ok(Transformed::yes(Arc::new(changed) as Plan))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "set_join_mode"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct ReplacePlan {
    name: &'static str,
    plan: Plan,
    schema_check: bool,
}

impl PhysicalOptimizerRule for ReplacePlan {
    fn optimize(&self, _: Plan, _: &ConfigOptions) -> Result<Plan> {
        Ok(Arc::clone(&self.plan))
    }

    fn name(&self) -> &str {
        self.name
    }

    fn schema_check(&self) -> bool {
        self.schema_check
    }
}

#[derive(Debug)]
struct HasField {
    name: &'static str,
    checks: Arc<Mutex<Vec<&'static str>>>,
}

impl PlanInvariant for HasField {
    fn name(&self) -> &'static str {
        self.name
    }

    fn check(&self, plan: &Plan) -> Result<()> {
        self.checks.lock().unwrap().push(self.name);
        if plan.schema().index_of(self.name).is_err() {
            return plan_err!("Missing field {}", self.name);
        }
        Ok(())
    }
}

#[test]
fn runtime_option_controls_all_region_checks() -> Result<()> {
    for enabled in [false, true] {
        let checks = Arc::new(Mutex::new(vec![]));
        let plan = empty(&["a"]);
        let output = optimize_with_config(
            Arc::clone(&plan),
            vec![
                Step::Begin(Arc::new(HasField {
                    name: "a",
                    checks: Arc::clone(&checks),
                })),
                rule(ReplacePlan {
                    name: "keep_a",
                    plan: Arc::clone(&plan),
                    schema_check: true,
                }),
                Step::End("a"),
            ],
            SessionConfig::new().set_bool(VALIDATE_INVARIANTS, enabled),
        )?;
        assert!(Arc::ptr_eq(&plan, &output));
        let expected = if enabled { vec!["a", "a", "a"] } else { vec![] };
        assert_eq!(*checks.lock().unwrap(), expected);
    }
    Ok(())
}

fn unsatisfied_input_requirements() -> Result<Vec<(&'static str, Plan)>> {
    let schema = empty(&["a"]).schema();
    let input: Plan = Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(2));
    let key = col("a", &schema)?;
    let ordering = [PhysicalSortExpr::new_default(Arc::clone(&key))].into();
    let partitioned = |partitions| -> Result<Plan> {
        Ok(Arc::new(RepartitionExec::try_new(
            Arc::clone(&input),
            Partitioning::Hash(vec![Arc::clone(&key)], partitions),
        )?))
    };
    let join = HashJoinExecBuilder::new(
        partitioned(2)?,
        partitioned(3)?,
        vec![(Arc::clone(&key), key)],
        JoinType::Inner,
    )
    .with_partition_mode(PartitionMode::Partitioned)
    .build()?;
    Ok(vec![
        (
            "does not satisfy order requirements",
            Arc::new(SortPreservingMergeExec::new(ordering, Arc::clone(&input))),
        ),
        (
            "does not satisfy distribution requirements",
            Arc::new(GlobalLimitExec::new(input, 0, Some(10))),
        ),
        (
            "requires children [0, 1] to be co-partitioned",
            Arc::new(join),
        ),
    ])
}

#[test]
fn requirements_region_rejects_invalid_plans_at_begin() -> Result<()> {
    for (diagnostic, root) in unsatisfied_input_requirements()? {
        let nested = Arc::new(LocalLimitExec::new(Arc::clone(&root), 10)) as Plan;
        for plan in [root, nested] {
            let error = optimize(
                plan,
                vec![
                    Step::Begin(Arc::new(InputRequirementsSatisfied)),
                    Step::End(InputRequirementsSatisfied.name()),
                ],
            )
            .unwrap_err()
            .to_string();
            assert_contains!(&error, "Beginning invariant input_requirements_satisfied");
            assert_contains!(&error, diagnostic);
        }
    }
    Ok(())
}

#[test]
fn ensure_requirements_establishes_region() -> Result<()> {
    for (_, plan) in unsatisfied_input_requirements()? {
        optimize(
            plan,
            vec![
                rule(EnsureRequirements::new()),
                Step::Begin(Arc::new(InputRequirementsSatisfied)),
                rule(SanityCheckPlan::new()),
                Step::End(InputRequirementsSatisfied.name()),
            ],
        )?;
    }
    Ok(())
}

#[test]
fn default_requirements_region_attributes_later_rule_violations() -> Result<()> {
    // Exercise both the start and the end of the default region. A violation
    // must be attributed to the rule that introduced it, before final sanity.
    for before_rule in ["CombinePartialFinalAggregate", "SanityCheckPlan"] {
        for (diagnostic, invalid) in unsatisfied_input_requirements()? {
            let mut steps = PhysicalOptimizer::new().steps().to_vec();
            let index = steps
                .iter()
                .position(
                    |step| matches!(step, Step::Rule(rule) if rule.name() == before_rule),
                )
                .unwrap();
            steps.insert(
                index,
                rule(ReplacePlan {
                    name: "break_input_requirements",
                    plan: Arc::new(LocalLimitExec::new(Arc::clone(&invalid), 10)),
                    schema_check: true,
                }),
            );
            let error = optimize(invalid, steps).unwrap_err().to_string();
            assert_contains!(
                &error,
                "Rule break_input_requirements violated invariant input_requirements_satisfied"
            );
            assert_contains!(&error, diagnostic);
        }
    }
    Ok(())
}

#[test]
fn requirements_region_end_keeps_final_sanity_independent() -> Result<()> {
    for (diagnostic, invalid) in unsatisfied_input_requirements()? {
        let mut steps = vec![
            rule(EnsureRequirements::new()),
            Step::Begin(Arc::new(InputRequirementsSatisfied)),
            Step::End(InputRequirementsSatisfied.name()),
            rule(ReplacePlan {
                name: "break_input_requirements_after_end",
                plan: Arc::clone(&invalid),
                schema_check: true,
            }),
        ];
        match optimize(Arc::clone(&invalid), steps.clone()) {
            Ok(output) => assert!(Arc::ptr_eq(&invalid, &output)),
            Err(error) => {
                // Co-partitioning also has an independent final executable
                // invariant, even in a pipeline without SanityCheckPlan.
                let error = error.to_string();
                assert_contains!(
                    &error,
                    "Invariant for ExecutionPlan node 'HashJoinExec' failed"
                );
                assert_contains!(&error, diagnostic);
            }
        }

        steps.push(rule(SanityCheckPlan::new()));
        for enabled in [false, true] {
            let error = optimize_with_config(
                Arc::clone(&invalid),
                steps.clone(),
                SessionConfig::new().set_bool(VALIDATE_INVARIANTS, enabled),
            )
            .unwrap_err()
            .to_string();
            assert_contains!(&error, "SanityCheckPlan");
            assert!(
                !error.contains("invariant input_requirements_satisfied"),
                "{error}"
            );
        }
    }
    Ok(())
}

#[test]
fn embedded_projections_are_only_allowed_after_region_end() -> Result<()> {
    let input = join(PartitionMode::CollectLeft)?;
    // The right output column is also a join key, so it must remain available
    // internally even when the result contains only the left column.
    let plan: Plan = Arc::new(ProjectionExec::try_new(
        vec![(col("a", &input.schema())?, "a".to_owned())],
        input,
    )?);
    let error = optimize(
        Arc::clone(&plan),
        vec![
            Step::Begin(Arc::new(NoEmbeddedProjections)),
            rule(ProjectionPushdown::new()),
            Step::End(NoEmbeddedProjections.name()),
        ],
    )
    .unwrap_err();
    assert_contains!(
        error.to_string(),
        "Rule ProjectionPushdown violated invariant no_embedded_projections"
    );
    assert_contains!(error.to_string(), "HashJoinExec");

    let output = optimize(
        plan,
        vec![
            Step::Begin(Arc::new(NoEmbeddedProjections)),
            Step::End(NoEmbeddedProjections.name()),
            rule(ProjectionPushdown::new()),
        ],
    )?;
    assert!(NoEmbeddedProjections.check(&output).is_err());
    Ok(())
}

#[tokio::test]
async fn default_pipeline_allows_nlj_embedding_after_region_end() -> Result<()> {
    for enabled in [false, true] {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().set_bool(VALIDATE_INVARIANTS, enabled),
        );
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
        )])?;
        ctx.register_batch("t", batch.clone())?;
        ctx.register_batch("u", batch)?;
        // Precomputing the left expression adds a temporary input column. The
        // first ProjectionPushdown embeds an output projection to hide it,
        // after the region has ended.
        let output = ctx
            .sql("SELECT t.id, u.id FROM t JOIN u ON t.id + 1 > u.id")
            .await?
            .create_physical_plan()
            .await?;
        assert_contains!(
            NoEmbeddedProjections
                .check(&output)
                .unwrap_err()
                .to_string(),
            "NestedLoopJoinExec"
        );
    }
    Ok(())
}

#[test]
fn default_sessions_use_the_shared_validation_default() {
    let state = SessionStateBuilder::new()
        .with_physical_optimizer_steps(vec![
            Step::Begin(Arc::new(HashJoinModesResolved)),
            Step::End("hash_join_modes_resolved"),
        ])
        .build();
    let default = ConfigOptions::new().optimizer.validate_optimizer_invariants;
    assert_eq!(
        SessionContext::new()
            .state()
            .config_options()
            .optimizer
            .validate_optimizer_invariants,
        default,
    );
    let result = DefaultPhysicalPlanner::default().optimize_physical_plan(
        join(PartitionMode::Auto).unwrap(),
        &state,
        |_, _| {},
    );
    if default {
        assert_contains!(
            result.unwrap_err().to_string(),
            "Beginning invariant hash_join_modes_resolved"
        );
    } else {
        assert!(result.is_ok());
    }
}

#[test]
fn begin_rejects_auto_at_root_and_below_root() -> Result<()> {
    let root = join(PartitionMode::Auto)?;
    let nested = Arc::new(LocalLimitExec::new(Arc::clone(&root), 10)) as Plan;
    for plan in [root, nested] {
        let error = optimize(
            plan,
            vec![
                Step::Begin(Arc::new(HashJoinModesResolved)),
                Step::End("hash_join_modes_resolved"),
            ],
        )
        .unwrap_err();
        assert_contains!(
            error.to_string(),
            "Beginning invariant hash_join_modes_resolved"
        );
        assert_contains!(error.to_string(), "PartitionMode::Auto");
    }
    Ok(())
}

#[test]
fn join_selection_establishes_region_and_concrete_modes_may_change() -> Result<()> {
    let plan = optimize(
        join(PartitionMode::Auto)?,
        vec![
            rule(JoinSelection::new()),
            Step::Begin(Arc::new(HashJoinModesResolved)),
            rule(SetJoinMode(PartitionMode::Partitioned)),
            rule(SetJoinMode(PartitionMode::CollectLeft)),
            Step::End("hash_join_modes_resolved"),
        ],
    )?;
    assert_eq!(
        *plan
            .downcast_ref::<HashJoinExec>()
            .unwrap()
            .partition_mode(),
        PartitionMode::CollectLeft
    );
    Ok(())
}

#[test]
fn rule_reintroducing_auto_is_attributed_below_root() -> Result<()> {
    let plan = Arc::new(LocalLimitExec::new(join(PartitionMode::CollectLeft)?, 10));
    let error = optimize(
        plan,
        vec![
            Step::Begin(Arc::new(HashJoinModesResolved)),
            rule(SetJoinMode(PartitionMode::Auto)),
            Step::End("hash_join_modes_resolved"),
        ],
    )
    .unwrap_err();
    assert_contains!(
        error.to_string(),
        "Rule set_join_mode violated invariant hash_join_modes_resolved"
    );
    assert_contains!(error.to_string(), "PartitionMode::Auto");
    Ok(())
}

#[test]
fn end_allows_temporary_auto_followed_by_repair() -> Result<()> {
    optimize(
        join(PartitionMode::CollectLeft)?,
        vec![
            Step::Begin(Arc::new(HashJoinModesResolved)),
            Step::End("hash_join_modes_resolved"),
            rule(SetJoinMode(PartitionMode::Auto)),
            rule(JoinSelection::new()),
            rule(SanityCheckPlan::new()),
        ],
    )?;
    Ok(())
}

#[test]
fn end_preserves_independent_final_sanity_check() {
    for enabled in [false, true] {
        let input = empty(&["a"]);
        let multiple_partitions =
            Arc::new(EmptyExec::new(input.schema()).with_partitions(2));
        let invalid = Arc::new(GlobalLimitExec::new(multiple_partitions, 0, Some(10)));
        let error = optimize_with_config(
            input,
            vec![
                Step::Begin(Arc::new(HashJoinModesResolved)),
                Step::End("hash_join_modes_resolved"),
                rule(ReplacePlan {
                    name: "unsatisfied_distribution",
                    plan: invalid,
                    schema_check: true,
                }),
                rule(SanityCheckPlan::new()),
            ],
            SessionConfig::new().set_bool(VALIDATE_INVARIANTS, enabled),
        )
        .unwrap_err()
        .to_string();
        assert_contains!(&error, "GlobalLimitExec");
        assert!(!error.contains("hash_join_modes_resolved"), "{error}");
        assert_contains!(error, "SanityCheckPlan");
    }
}

#[test]
fn crossing_regions_keep_checking_the_remaining_invariant() -> Result<()> {
    for preserve_b in [true, false] {
        let checks = Arc::new(Mutex::new(vec![]));
        let a = Arc::new(HasField {
            name: "a",
            checks: Arc::clone(&checks),
        });
        let b = Arc::new(HasField {
            name: "b",
            checks: Arc::clone(&checks),
        });
        let result = optimize(
            empty(&["a", "b"]),
            vec![
                Step::Begin(a),
                Step::Begin(b),
                rule(ReplacePlan {
                    name: "keep_both",
                    plan: empty(&["a", "b"]),
                    schema_check: true,
                }),
                Step::End("a"),
                rule(ReplacePlan {
                    name: "after_end_a",
                    plan: empty(if preserve_b { &["b"] } else { &[] }),
                    schema_check: false,
                }),
                Step::End("b"),
            ],
        );
        if preserve_b {
            result?;
            assert_eq!(*checks.lock().unwrap(), ["a", "b", "a", "b", "a", "b", "b"]);
        } else {
            assert_contains!(
                result.unwrap_err().to_string(),
                "Rule after_end_a violated invariant b"
            );
            assert_eq!(*checks.lock().unwrap(), ["a", "b", "a", "b", "a", "b"]);
        }
    }
    Ok(())
}

#[test]
fn producer_is_checked_against_other_active_invariants() {
    let checks = Arc::new(Mutex::new(vec![]));
    let error = optimize(
        empty(&["a"]),
        vec![
            Step::Begin(Arc::new(HasField {
                name: "a",
                checks: Arc::clone(&checks),
            })),
            rule(ReplacePlan {
                name: "produce_b",
                plan: empty(&["b"]),
                schema_check: false,
            }),
            Step::Begin(Arc::new(HasField {
                name: "b",
                checks: Arc::clone(&checks),
            })),
            Step::End("a"),
            Step::End("b"),
        ],
    )
    .unwrap_err();
    assert_contains!(error.to_string(), "Rule produce_b violated invariant a");
    assert_eq!(*checks.lock().unwrap(), ["a", "a"]);
}

#[test]
fn invalid_marker_sequences_are_rejected() {
    let scenarios = [
        (
            vec![
                Step::Begin(Arc::new(HashJoinModesResolved)),
                Step::Begin(Arc::new(HashJoinModesResolved)),
            ],
            "Invariant already active: hash_join_modes_resolved",
        ),
        (
            vec![Step::End("hash_join_modes_resolved")],
            "Invariant is not active: hash_join_modes_resolved",
        ),
        (
            vec![Step::Begin(Arc::new(HashJoinModesResolved))],
            "Unclosed invariant regions: hash_join_modes_resolved",
        ),
    ];
    for (steps, expected) in scenarios {
        for enabled in [false, true] {
            let error = optimize_with_config(
                empty(&[]),
                steps.clone(),
                SessionConfig::new().set_bool(VALIDATE_INVARIANTS, enabled),
            )
            .unwrap_err();
            assert_contains!(error.to_string(), expected);
        }
    }
}

#[derive(Debug)]
struct ContextRule;

impl PhysicalOptimizerRule for ContextRule {
    fn optimize(&self, _: Plan, _: &ConfigOptions) -> Result<Plan> {
        plan_err!("The context-aware entry point was bypassed")
    }

    fn optimize_with_context(
        &self,
        plan: Plan,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Plan> {
        assert_eq!(context.config_options().execution.batch_size.get(), 37);
        assert!(context.statistics_registry().is_some());
        Ok(plan)
    }

    fn name(&self) -> &str {
        "context_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[test]
fn regions_preserve_rule_context_and_observers() -> Result<()> {
    let state = SessionStateBuilder::new()
        .with_config(
            SessionConfig::new()
                .with_batch_size(37)
                .set_bool(VALIDATE_INVARIANTS, true),
        )
        .with_statistics_registry(StatisticsRegistry::new())
        .with_physical_optimizer_steps(vec![
            Step::Begin(Arc::new(HashJoinModesResolved)),
            rule(ContextRule),
            Step::End("hash_join_modes_resolved"),
            rule(ContextRule),
        ])
        .build();
    let input = empty(&[]);
    let mut observed = vec![];
    let output = DefaultPhysicalPlanner::default().optimize_physical_plan(
        Arc::clone(&input),
        &state,
        |plan, rule| {
            assert_eq!(plan.name(), "EmptyExec");
            observed.push(rule.name().to_owned());
        },
    )?;
    assert!(Arc::ptr_eq(&input, &output));
    assert_eq!(observed, ["context_rule", "context_rule"]);
    Ok(())
}

#[test]
fn regions_preserve_schema_checks() {
    let error = optimize(
        empty(&["a"]),
        vec![
            Step::Begin(Arc::new(HashJoinModesResolved)),
            rule(ReplacePlan {
                name: "change_schema",
                plan: empty(&["b"]),
                schema_check: true,
            }),
            Step::End("hash_join_modes_resolved"),
        ],
    )
    .unwrap_err();
    assert_contains!(
        error.to_string(),
        "PhysicalOptimizer rule 'change_schema' failed. Schema mismatch."
    );
}

#[tokio::test]
async fn default_regions_preserve_existing_plans_and_explain() -> Result<()> {
    let optimizer = PhysicalOptimizer::new();
    let with_regions = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().set_bool(VALIDATE_INVARIANTS, true))
            .with_physical_optimizer_steps(optimizer.steps().to_vec())
            .build(),
    );
    let without_regions = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().set_bool(VALIDATE_INVARIANTS, true))
            .with_physical_optimizer_rules(optimizer.rules().to_vec())
            .build(),
    );
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as _),
        ("value", Arc::new(Int32Array::from(vec![10, 20, 30])) as _),
    ])?;
    for ctx in [&with_regions, &without_regions] {
        ctx.register_batch("t", batch.clone())?;
        ctx.register_batch("u", batch.clone())?;
    }
    for sql in [
        "SELECT t.id, u.value FROM t JOIN u ON t.id = u.id",
        "SELECT t.id, SUM(u.value) FROM t LEFT JOIN u ON t.id = u.id GROUP BY t.id ORDER BY t.id LIMIT 2",
        "SELECT t.id, u.value FROM t FULL JOIN u ON t.id = u.id",
    ] {
        let with = with_regions.sql(sql).await?.create_physical_plan().await?;
        let without = without_regions
            .sql(sql)
            .await?
            .create_physical_plan()
            .await?;
        assert_eq!(
            displayable(with.as_ref()).indent(true).to_string(),
            displayable(without.as_ref()).indent(true).to_string(),
            "{sql}"
        );

        let explain = format!("EXPLAIN VERBOSE {sql}");
        let with = with_regions.sql(&explain).await?.collect().await?;
        let without = without_regions.sql(&explain).await?.collect().await?;
        assert_eq!(with, without, "{explain}");
    }
    Ok(())
}

#[tokio::test]
async fn explain_attributes_region_failures_to_the_rule() -> Result<()> {
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().set_bool(VALIDATE_INVARIANTS, true))
            .with_physical_optimizer_steps(vec![
                rule(JoinSelection::new()),
                Step::Begin(Arc::new(HashJoinModesResolved)),
                rule(SetJoinMode(PartitionMode::Auto)),
                Step::End("hash_join_modes_resolved"),
            ])
            .build(),
    );
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
    )])?;
    ctx.register_batch("t", batch.clone())?;
    ctx.register_batch("u", batch)?;
    let batches = ctx
        .sql("EXPLAIN VERBOSE SELECT t.id FROM t JOIN u ON t.id = u.id")
        .await?
        .collect()
        .await?;
    let mut found_error = false;
    for batch in batches {
        let types = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let plans = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let plan_type = types.value(row);
            assert!(
                !plan_type.contains("hash_join_modes_resolved"),
                "{plan_type}"
            );
            if plans.value(row).contains("violated invariant") {
                assert_eq!(plan_type, "physical_plan after set_join_mode");
                assert_contains!(
                    plans.value(row),
                    "Rule set_join_mode violated invariant hash_join_modes_resolved"
                );
                found_error = true;
            }
        }
    }
    assert!(
        found_error,
        "EXPLAIN must retain the attributed invariant failure"
    );
    Ok(())
}
