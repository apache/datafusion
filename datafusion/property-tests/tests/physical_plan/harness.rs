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

//! Tests of the harness: deriving cases from factories by probing their
//! input requirements, and reporting the findings of each case.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt64Type};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{JoinType, NullEquality, Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::{IsNotNullExpr, col};
use datafusion_physical_expr::{
    Distribution, LexOrdering, LexRequirement, OrderingRequirements, PhysicalExpr,
    PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, InputDistributionRequirements, Partitioning, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream, StatisticsArgs, StatisticsContext,
    collect_partitioned,
};
use datafusion_property_tests::Severity;
use datafusion_property_tests::physical_plan::CheckKind;
use datafusion_property_tests::physical_plan::fixtures::{
    ROW_ID_COLUMN, SourceSpec, StatisticsPrecision,
};
use datafusion_property_tests::physical_plan::harness::{
    FactoryReport, PlanFactory, PlanHarness, Profile, ROW_ID_RANGE,
};

use crate::common::{ConfigurableExec, Effect, FetchMode, checker_of};

type Plan = Arc<dyn ExecutionPlan>;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, true),
    ]))
}

fn spec() -> SourceSpec {
    SourceSpec::new(schema()).with_num_rows(60)
}

fn ordering_on(column: &str, input: &Plan) -> Result<LexOrdering> {
    Ok(LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
        column,
        &input.schema(),
    )?)])
    .unwrap())
}

/// A factory for a plan with one input generated from [`spec`]
fn one_input<F>(name: &str, create: F) -> PlanFactory
where
    F: Fn(Plan) -> Result<Plan> + Send + Sync + 'static,
{
    PlanFactory::new(name, vec![spec()], move |mut inputs| {
        create(inputs.remove(0))
    })
}

/// A filter on `b IS NOT NULL`, which has no findings
fn filter() -> PlanFactory {
    one_input("FilterExec", |input| {
        let predicate = Arc::new(IsNotNullExpr::new(col("b", &input.schema())?));
        Ok(Arc::new(FilterExec::try_new(predicate, input)?))
    })
}

/// A harness with the static checks only
fn static_harness() -> PlanHarness {
    PlanHarness::new().with_checker(checker_of(&[CheckKind::Static]))
}

/// The ids of the row id column of `input`
fn row_ids(input: &Plan) -> Vec<u64> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let partitions = runtime
        .block_on(collect_partitioned(
            Arc::clone(input),
            Arc::new(TaskContext::default()),
        ))
        .unwrap();
    partitions
        .iter()
        .flatten()
        .flat_map(|batch: &RecordBatch| {
            let index = batch.schema().index_of(ROW_ID_COLUMN).unwrap();
            batch
                .column(index)
                .as_primitive::<UInt64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

/// What a [`RequirementExec`] requires of its input
#[derive(Debug, Clone, Copy)]
enum Rule {
    /// Hash partitioning on `a`, and once the input is hash partitioned,
    /// ordering on `a` too, as a node whose requirements depend on its input
    HashThenOrdering,
    /// Ordering on `b` while the input is sorted on `a`, and on `a` otherwise,
    /// which no input meets
    Alternating,
    /// A soft ordering requirement on `a`, which the node can work without
    SoftOrdering,
}

/// A pass-through node with the input requirements of a [`Rule`]
#[derive(Debug)]
struct RequirementExec {
    input: Plan,
    rule: Rule,
}

impl RequirementExec {
    fn plan(input: Plan, rule: Rule) -> Plan {
        Arc::new(Self { input, rule })
    }

    fn input_sorted_on(&self, column: &str) -> bool {
        let ordering = ordering_on(column, &self.input).unwrap();
        self.input
            .equivalence_properties()
            .ordering_satisfy(ordering)
            .unwrap()
    }

    fn requirement_on(&self, column: &str) -> LexRequirement {
        let expr = col(column, &self.input.schema()).unwrap();
        LexRequirement::new(vec![PhysicalSortRequirement::new(expr, None)]).unwrap()
    }
}

impl DisplayAs for RequirementExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RequirementExec: {:?}", self.rule)
    }
}

impl ExecutionPlan for RequirementExec {
    fn name(&self) -> &'static str {
        "RequirementExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Plan> {
        vec![&self.input]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let hashed = matches!(self.input.output_partitioning(), Partitioning::Hash(..));
        vec![match self.rule {
            Rule::HashThenOrdering => {
                hashed.then(|| OrderingRequirements::new(self.requirement_on("a")))
            }
            Rule::Alternating => {
                let column = if self.input_sorted_on("a") { "b" } else { "a" };
                Some(OrderingRequirements::new(self.requirement_on(column)))
            }
            Rule::SoftOrdering => {
                Some(OrderingRequirements::new_soft(self.requirement_on("a")))
            }
        }]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        let distribution = match self.rule {
            Rule::HashThenOrdering => Distribution::KeyPartitioned(vec![
                col("a", &self.input.schema()).unwrap(),
            ]),
            Rule::Alternating | Rule::SoftOrdering => {
                Distribution::UnspecifiedDistribution
            }
        };
        InputDistributionRequirements::new(vec![distribution])
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Plan>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Plan> {
        Ok(Self::plan(children.swap_remove(0), self.rule))
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Plan>) -> Result<Plan> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

#[test]
fn ordering_requirement_sorts_the_input() {
    let factory = one_input("SortPreservingMergeExec", |input| {
        Ok(Arc::new(SortPreservingMergeExec::new(
            ordering_on("a", &input)?,
            input,
        )))
    });
    let cases = static_harness().cases(&factory).unwrap();
    assert_eq!(cases.len(), Profile::defaults().len());
    for case in &cases {
        let ordering = case.inputs[0].ordering().map(ToString::to_string);
        if case.profile.constants.is_some() {
            // A constant column meets any ordering on it, so the input is
            // not sorted
            assert_eq!(ordering, None, "{}", case.profile.name);
        } else if case.profile.column_ordering {
            // The ordering on every column meets the requirement, and is kept
            assert_eq!(
                ordering.as_deref(),
                Some("a@0 ASC, b@1 ASC"),
                "{}",
                case.profile.name
            );
        } else {
            assert_eq!(
                ordering.as_deref(),
                Some("a@0 ASC"),
                "{}",
                case.profile.name
            );
        }
    }
    // The input keeps the profile's partitions
    assert_eq!(cases[0].inputs[0].partition_rows(), Some(&[20, 0, 40][..]));
}

#[test]
fn compatible_base_ordering_is_kept() {
    // The base spec is sorted on (a, b), which meets the requirement on a
    let base_ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new_default(col("a", &schema()).unwrap()),
        PhysicalSortExpr::new_default(col("b", &schema()).unwrap()),
    ])
    .unwrap();
    let sort_preserving_merge = |inputs: Vec<Plan>| {
        let input = Arc::clone(&inputs[0]);
        Ok(Arc::new(SortPreservingMergeExec::new(
            ordering_on("a", &input)?,
            input,
        )) as Plan)
    };
    let factory = PlanFactory::new(
        "SortPreservingMergeExec",
        vec![spec().with_ordering(base_ordering)],
        sort_preserving_merge,
    );
    let case = &static_harness().cases(&factory).unwrap()[0];
    assert_eq!(
        case.inputs[0].ordering().unwrap().to_string(),
        "a@0 ASC, b@1 ASC"
    );

    // A base ordering that does not meet the requirement is replaced
    let factory = PlanFactory::new(
        "SortPreservingMergeExec",
        vec![
            spec().with_ordering(ordering_on("b", &spec().build_arc().unwrap()).unwrap()),
        ],
        sort_preserving_merge,
    );
    let case = &static_harness().cases(&factory).unwrap()[0];
    assert_eq!(case.inputs[0].ordering().unwrap().to_string(), "a@0 ASC");
}

#[test]
fn single_partition_requirement() {
    let factory = one_input("GlobalLimitExec", |input| {
        Ok(Arc::new(GlobalLimitExec::new(input, 0, Some(5))))
    });
    let cases = static_harness().cases(&factory).unwrap();
    assert_eq!(cases.len(), Profile::defaults().len());
    for case in &cases {
        assert_eq!(case.inputs[0].partition_count(), 1, "{}", case.profile.name);
    }
    assert_eq!(cases[0].inputs[0].partition_rows(), Some(&[60][..]));
}

fn hash_join(mode: PartitionMode) -> PlanFactory {
    let right = SourceSpec::new(Arc::new(Schema::new(vec![
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Int32, true),
    ])))
    .with_num_rows(90);
    PlanFactory::new("HashJoinExec", vec![spec(), right], move |inputs| {
        let on = vec![(
            col("a", &inputs[0].schema())?,
            col("c", &inputs[1].schema())?,
        )];
        Ok(Arc::new(HashJoinExec::try_new(
            Arc::clone(&inputs[0]),
            Arc::clone(&inputs[1]),
            on,
            None,
            &JoinType::Inner,
            None,
            mode,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Plan)
    })
}

#[test]
fn hash_requirement_and_co_partitioning() {
    // Both inputs are hash partitioned on their keys, into as many
    // partitions as the profile has
    let harness = static_harness().with_profiles(vec![
        Profile::default_profile(),
        Profile {
            partition_weights: vec![1, 1, 0, 1, 1],
            ..Profile::new("5 partitions")
        },
    ]);
    let cases = harness
        .cases(&hash_join(PartitionMode::Partitioned))
        .unwrap();
    for (case, partitions) in cases.iter().zip([3, 5]) {
        let [left, right] = case.inputs.as_slice() else {
            panic!("two inputs");
        };
        assert_eq!(left.hash_partitioning().unwrap()[0].to_string(), "a@0");
        assert_eq!(right.hash_partitioning().unwrap()[0].to_string(), "c@0");
        assert_eq!(
            (left.partition_count(), right.partition_count()),
            (partitions, partitions)
        );
        assert_eq!((left.num_rows(), right.num_rows()), (60, 90));
    }

    // A collect left join puts its build side in one partition, and leaves
    // the probe side as the profile lays it out
    let cases = static_harness()
        .cases(&hash_join(PartitionMode::CollectLeft))
        .unwrap();
    assert_eq!(cases[0].inputs[0].partition_rows(), Some(&[60][..]));
    assert_eq!(cases[0].inputs[1].partition_rows(), Some(&[30, 0, 60][..]));
}

#[test]
fn requirements_that_depend_on_the_input_are_probed_again() {
    let creates = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&creates);
    let factory = one_input("RequirementExec", move |input| {
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(RequirementExec::plan(input, Rule::HashThenOrdering))
    });
    let harness = static_harness().with_profiles(vec![Profile::default_profile()]);
    let case = &harness.cases(&factory).unwrap()[0];
    let input = &case.inputs[0];
    assert_eq!(input.hash_partitioning().unwrap()[0].to_string(), "a@0");
    assert_eq!(input.ordering().unwrap().to_string(), "a@0 ASC");
    // The first plan asks for hash partitioning, the second, on hash
    // partitioned input, for an ordering, and the third is satisfied
    assert_eq!(creates.load(Ordering::Relaxed), 3);
}

#[test]
fn soft_ordering_requirements_are_not_imposed() {
    let factory = one_input("RequirementExec", |input| {
        Ok(RequirementExec::plan(input, Rule::SoftOrdering))
    });
    let case = &static_harness().cases(&factory).unwrap()[0];
    assert!(case.inputs[0].ordering().is_none());
}

#[test]
fn unsatisfiable_requirements_are_an_error() {
    let factory = one_input("RequirementExec", |input| {
        Ok(RequirementExec::plan(input, Rule::Alternating))
    });
    let error = static_harness().check(&factory).unwrap_err().to_string();
    assert!(
        error.contains("deriving the 'default' case of 'RequirementExec'"),
        "{error}"
    );
    assert!(error.contains("after building the plan 5 times"), "{error}");
}

#[test]
fn requirements_on_children_that_are_not_inputs_are_an_error() {
    // The projection keeps the ordering of its input, but the harness only
    // sorts inputs to meet the requirements of the nodes directly above them
    let factory = one_input("SortPreservingMergeExec over a projection", |input| {
        let schema = input.schema();
        let exprs = vec![
            (col("a", &schema)?, "a".to_string()),
            (col("b", &schema)?, "b".to_string()),
        ];
        let projection: Plan = Arc::new(ProjectionExec::try_new(exprs, input)?);
        Ok(Arc::new(SortPreservingMergeExec::new(
            ordering_on("a", &projection)?,
            projection,
        )))
    });
    let error = static_harness().cases(&factory).unwrap_err().to_string();
    assert!(
        error.contains(
            "SortPreservingMergeExec requires child 0 to be sorted by [a@0 ASC], and the \
             child is not an input"
        ),
        "{error}"
    );
}

#[test]
fn case_inputs_follow_the_profile() {
    let cases = static_harness().cases(&filter()).unwrap();
    for (case, precision) in cases.iter().zip([
        StatisticsPrecision::Exact,
        StatisticsPrecision::Exact,
        StatisticsPrecision::Inexact,
        StatisticsPrecision::Absent,
        StatisticsPrecision::Exact,
    ]) {
        let input = case.inputs[0].build_arc().unwrap();
        let statistics = StatisticsContext::new()
            .compute(input.as_ref(), &StatisticsArgs::new())
            .unwrap();
        let rows = case.inputs[0].num_rows();
        let expected = match precision {
            StatisticsPrecision::Exact => Precision::Exact(rows),
            StatisticsPrecision::Inexact => Precision::Inexact(rows),
            StatisticsPrecision::Absent => Precision::Absent,
        };
        assert_eq!(statistics.num_rows, expected, "{}", case.profile.name);
    }
    assert_eq!(cases[4].inputs[0].partition_rows(), Some(&[0, 0, 0][..]));

    // The extended profiles add more seeds, partition counts and rows
    let extended = static_harness()
        .with_profiles(Profile::extended())
        .cases(&filter())
        .unwrap();
    let partition_rows: Vec<&[usize]> = extended
        .iter()
        .map(|case| case.inputs[0].partition_rows().unwrap())
        .collect();
    assert_eq!(
        partition_rows,
        [
            &[20, 0, 40][..],
            &[20, 0, 40],
            &[30, 30],
            &[17, 8, 0, 25, 10],
            &[160, 0, 320]
        ]
    );
    let data = |spec: &SourceSpec| spec.build().unwrap().partitions().to_vec();
    assert_ne!(
        data(&cases[0].inputs[0]),
        data(&extended[0].inputs[0]),
        "seed 1 generates other data"
    );
}

#[test]
fn row_id_ordering_and_hash_partitioning_profiles() {
    let profile = |name: &str| {
        Profile::defaults()
            .into_iter()
            .find(|profile| profile.name == name)
            .unwrap()
    };
    let harness = static_harness().with_profiles(vec![
        profile("sorted by row id"),
        profile("hash partitioned"),
    ]);
    let cases = harness.cases(&filter()).unwrap();
    let inputs: Vec<Plan> = cases
        .iter()
        .map(|case| Arc::clone(case.plan.children()[0]))
        .collect();

    // Every input declares that it is sorted by its row ids
    let ordering = inputs[0].properties().output_ordering().unwrap();
    assert_eq!(ordering.to_string(), format!("{ROW_ID_COLUMN}@2 ASC"));
    assert_eq!(cases[0].inputs[0].partition_rows(), Some(&[20, 0, 40][..]));

    // Every input is hash partitioned on its first column, into the profile's
    // partitions
    assert_eq!(
        inputs[1].properties().output_partitioning().to_string(),
        "Hash([a@0], 3)"
    );
    assert_eq!(cases[1].inputs[0].num_rows(), 60);

    // Inputs that must be laid out otherwise are
    let cases = harness
        .cases(&hash_join(PartitionMode::CollectLeft))
        .unwrap();
    let [build, probe] = cases[1].inputs.as_slice() else {
        panic!("two inputs");
    };
    assert_eq!(build.partition_rows(), Some(&[60][..]));
    assert_eq!(probe.hash_partitioning().unwrap()[0].to_string(), "c@0");
    let cases = harness
        .cases(&one_input("SortPreservingMergeExec", |input| {
            Ok(Arc::new(SortPreservingMergeExec::new(
                ordering_on("b", &input)?,
                input,
            )))
        }))
        .unwrap();
    assert_eq!(
        cases[0].inputs[0].ordering().unwrap().to_string(),
        "b@1 ASC"
    );
}

#[test]
fn column_ordering_profile() {
    let profile = Profile::defaults()
        .into_iter()
        .find(|profile| profile.name == "sorted by columns")
        .unwrap();
    let harness = static_harness().with_profiles(vec![profile]);

    // Every input declares that it is sorted by its columns, in schema order,
    // without the row id
    let case = &harness.cases(&filter()).unwrap()[0];
    let input = case.plan.children()[0];
    assert_eq!(
        input.properties().output_ordering().unwrap().to_string(),
        "a@0 ASC, b@1 ASC"
    );

    // A base ordering is kept
    let base_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("b", &schema()).unwrap(),
    )])
    .unwrap();
    let factory = PlanFactory::new(
        "FilterExec",
        vec![spec().with_ordering(base_ordering)],
        |inputs| {
            let input = Arc::clone(&inputs[0]);
            let predicate = Arc::new(IsNotNullExpr::new(col("b", &input.schema())?));
            Ok(Arc::new(FilterExec::try_new(predicate, input)?) as Plan)
        },
    );
    let case = &harness.cases(&factory).unwrap()[0];
    assert_eq!(case.inputs[0].ordering().unwrap().to_string(), "b@1 ASC");

    // An ordering the plan requires replaces it
    let case = &harness
        .cases(&one_input("SortPreservingMergeExec", |input| {
            Ok(Arc::new(SortPreservingMergeExec::new(
                ordering_on("b", &input)?,
                input,
            )))
        }))
        .unwrap()[0];
    assert_eq!(case.inputs[0].ordering().unwrap().to_string(), "b@1 ASC");
}

#[test]
fn row_id_columns() {
    let factory = hash_join(PartitionMode::CollectLeft);
    let case = &static_harness().cases(&factory).unwrap()[0];
    let inputs: Vec<Plan> = case
        .inputs
        .iter()
        .map(|spec| spec.build_arc().unwrap())
        .collect();
    let names = |input: &Plan| -> Vec<String> {
        input
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };
    assert_eq!(names(&inputs[0]), ["a", "b", ROW_ID_COLUMN]);
    assert_eq!(names(&inputs[1]), ["c", "d", ROW_ID_COLUMN]);

    // Ids are unique, and each input has its own range
    assert_eq!(row_ids(&inputs[0]), (0..60).collect::<Vec<u64>>());
    assert_eq!(
        row_ids(&inputs[1]),
        (ROW_ID_RANGE..ROW_ID_RANGE + 90).collect::<Vec<u64>>()
    );

    // The plan binds its keys by name, so they refer to the key columns of
    // the inputs that have row ids
    assert_eq!(case.plan.schema().fields().len(), 6);
}

#[test]
fn inputs_with_the_same_base_spec_have_the_same_schema() {
    // So a union needs no projection to rename the row id columns
    let factory = PlanFactory::new("UnionExec", vec![spec(), spec()], UnionExec::try_new);
    let case = &static_harness().cases(&factory).unwrap()[0];
    assert_eq!(case.inputs[0].schema(), case.inputs[1].schema());
    assert!(
        case.plan
            .children()
            .iter()
            .all(|child| child.name() == "MockSourceExec")
    );
}

#[test]
fn factories_without_inputs() {
    let factory = PlanFactory::new("EmptyExec", vec![], |_| {
        Ok(Arc::new(EmptyExec::new(schema())) as Plan)
    });
    let report = PlanHarness::new().check(&factory).unwrap();
    assert_eq!(report.cases.len(), 1);
    assert_eq!(report.cases[0].0.profile.name, "default");
    report.assert_clean();
}

/// A pass-through node over one input that reports `fetch()` 5 and
/// `CardinalityEffect::Equal` (reported in every case), and `num_rows`
/// `Exact(60)`, which is false when the input is empty (reported in the
/// `empty input` case only)
fn fetch_with_equal_effect() -> PlanFactory {
    one_input("ConfigurableExec", |input| {
        let mut exec = ConfigurableExec::new(input)
            .effect(Effect::Equal)
            .num_rows(Precision::Exact(60));
        exec.fetch = Some(5);
        exec.fetch_mode = FetchMode::Ignore;
        Ok(exec.build())
    })
}

/// The profile names of the cases with a violation of `check`
fn cases_with(report: &FactoryReport, check: &str) -> Vec<String> {
    report
        .cases
        .iter()
        .filter(|(_, report)| report.violations.iter().any(|v| v.check == check))
        .map(|(case, _)| case.profile.name.clone())
        .collect()
}

#[test]
fn findings_are_reported_per_case() {
    let report = PlanHarness::new()
        .check(&fetch_with_equal_effect())
        .unwrap();
    assert_eq!(
        cases_with(&report, "fetch_not_equal_cardinality").len(),
        Profile::defaults().len()
    );
    assert_eq!(
        cases_with(&report, "exact_statistics_hold"),
        ["empty input"],
        "{report}"
    );
    // A node that polls its input from a spawned task but reports `Lazy` is
    // only reported in the default case, whose profile runs the stream checks
    let factory = one_input("ConfigurableExec", |input| {
        let mut exec = ConfigurableExec::new(input);
        exec.eager = true;
        Ok(exec.build())
    });
    let report = PlanHarness::new().check(&factory).unwrap();
    assert_eq!(
        cases_with(&report, "lazy_evaluation_holds"),
        ["default"],
        "{report}"
    );
}

#[test]
fn report_display() {
    let factory = filter().allow("fetch_not_equal_cardinality", "a reason");
    let harness = static_harness().with_profiles(vec![
        Profile::default_profile(),
        Profile {
            partition_weights: vec![1],
            ..Profile::new("single partition")
        },
    ]);
    let report = harness.check(&factory).unwrap();
    assert_eq!(
        report.to_string(),
        "FilterExec
  allowed fetch_not_equal_cardinality: a reason
  case default: no violations
  case single partition: no violations"
    );

    // The plan is only shown for a case with violations
    let report = static_harness().check(&fetch_with_equal_effect()).unwrap();
    assert!(
        report.to_string().contains(
            "  case empty input:
    ConfigurableExec
      MockSourceExec: partitioning=UnknownPartitioning(3), partition_rows=[0, 0, 0], statistics=Exact
    [invariant] fetch_not_equal_cardinality at ConfigurableExec (root): fetch() is Some(5)"
        ),
        "{report}"
    );
}

#[test]
fn allowed_checks_are_skipped() {
    let factory = fetch_with_equal_effect()
        .allow("fetch_not_equal_cardinality", "the fetch is only reported");
    let report = static_harness().check(&factory).unwrap();
    assert!(cases_with(&report, "fetch_not_equal_cardinality").is_empty());
}

#[test]
#[should_panic(expected = "the checker has no check named 'no_such_check'")]
fn allowing_an_unknown_check_panics() {
    let factory = filter().allow("no_such_check", "a typo");
    let _ = static_harness().check(&factory);
}

#[test]
fn cases_can_be_reproduced_from_their_specs() {
    let factory = fetch_with_equal_effect();
    let harness = static_harness();
    let report = harness.check(&factory).unwrap();
    for (case, case_report) in &report.cases {
        let inputs = case
            .inputs
            .iter()
            .map(|spec| spec.build_arc().unwrap())
            .collect();
        let plan = factory.create(inputs).unwrap();
        let by_hand = checker_of(&[CheckKind::Static]).check(&plan).unwrap();
        assert_eq!(&by_hand, case_report);
    }
}

#[test]
fn failing_create_is_an_error() {
    let factory = one_input("fails", |_| internal_err!("no plan today"));
    let error = static_harness().check(&factory).unwrap_err().to_string();
    assert!(
        error.contains("deriving the 'default' case of 'fails'"),
        "{error}"
    );
    assert!(error.contains("no plan today"), "{error}");

    // So is a factory that only fails for some inputs
    let factory = one_input("fails on one partition", |input| {
        if input.output_partitioning().partition_count() == 1 {
            return internal_err!("several partitions only");
        }
        Ok(input)
    });
    let error = static_harness().check(&factory).unwrap_err().to_string();
    assert!(
        error.contains("deriving the 'single partition' case"),
        "{error}"
    );
}

#[test]
#[should_panic(expected = "invariant violations found")]
fn invariant_violations_fail_the_assertion() {
    static_harness()
        .check(&fetch_with_equal_effect())
        .unwrap()
        .assert_no_invariant_violations();
}

#[test]
fn lints_do_not_fail_the_invariant_assertion() {
    // A filter with a fetch does not apply the fetch to its row estimate,
    // which is a lint
    let factory = one_input("FilterExec with fetch", |input| {
        let predicate = Arc::new(IsNotNullExpr::new(col("b", &input.schema())?));
        FilterExec::try_new(predicate, input)?
            .with_fetch(Some(1))
            .ok_or_else(|| datafusion_common::internal_datafusion_err!("no fetch"))
    });
    let report = static_harness().check(&factory).unwrap();
    assert!(
        report
            .cases
            .iter()
            .flat_map(|(_, report)| &report.violations)
            .all(|v| v.severity == Severity::Lint),
        "{report}"
    );
    assert!(!cases_with(&report, "fetch_bounds_num_rows").is_empty());
    report.assert_no_invariant_violations();
}
