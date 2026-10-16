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

//! Testing a plan from a description of how to build it.
//!
//! A [`PlanFactory`] says how to build the plan under test from its inputs.
//! For each [`Profile`], the [`PlanHarness`] generates the inputs as the
//! profile lays them out, builds the plan, and reads what each node directly
//! above an input requires of it (`required_input_ordering` and
//! `input_distribution_requirements`). It then changes the inputs to meet the
//! requirements: an input that must be sorted is sorted, one that must be hash
//! partitioned is hash partitioned into the profile's number of partitions
//! (the same number for every input), and one that must be a single partition
//! gets one. A plan's requirements can depend on its inputs, so the harness
//! builds the plan again and repeats until the requirements are met. The
//! result is a [`Case`], and the harness runs a [`PlanChecker`] on the plan of
//! every case.

mod requirements;

use std::fmt;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_plan::{ExecutionPlan, displayable};

use crate::Report;
use crate::physical_plan::exec::runtime;
use crate::physical_plan::fixtures::{ConstantValues, SourceSpec, StatisticsPrecision};
use crate::physical_plan::{CheckKind, PlanChecker};

/// Row ids of input `i` start at `i * ROW_ID_RANGE`
pub const ROW_ID_RANGE: u64 = 1_000_000_000;

/// The function a [`PlanFactory`] builds its plan with
type CreateFn =
    dyn Fn(Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> + Send + Sync;

/// Describes a plan under test by how to build it from its inputs.
///
/// A factory has a name, one base [`SourceSpec`] per input, and a function
/// that builds the plan from input plans. The [`PlanHarness`] generates the
/// inputs for each case from the base specs, calls the function, and checks
/// the plan it returns.
///
/// # The base specs
///
/// A base spec carries what only the author of the plan knows: the schema,
/// the value distribution (for example few distinct values on join keys, so
/// that the inputs of a join match), the number of rows, and optionally an
/// ordering the plan should see, for example to exercise a sorted code path.
/// The harness sets everything else for each case, replacing what the base
/// spec says: the partition layout, hash partitioning, the batch layout, the
/// statistics precision, the seed and the row ids, and in some cases makes
/// every column constant or adds a copy of every column (see [`Profile`]). It
/// keeps the base ordering unless the plan requires another one.
///
/// # The function
///
/// `create` is called several times per case: to probe the plan's input
/// requirements, and once more when they are met. It must be a pure function
/// of its inputs, and must use the input plans it receives as they are (not
/// copies), since the harness recognizes them by identity.
///
/// **Bind expressions to the schemas of the inputs it receives, by name**,
/// for example `col("a", &inputs[0].schema())`, never by a hard-coded column
/// index or a schema captured outside `create`. The harness appends a row id
/// column ([`ROW_ID_COLUMN`]) to every input, and in some cases a copy of
/// every column (named with [`COPY_SUFFIX`]), so the input schemas have more
/// columns than the base specs.
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_physical_expr::expressions::col;
/// # use datafusion_physical_plan::ExecutionPlan;
/// # use datafusion_physical_plan::filter::FilterExec;
/// use datafusion_property_tests::physical_plan::fixtures::SourceSpec;
/// use datafusion_property_tests::physical_plan::harness::{PlanFactory, PlanHarness};
///
/// let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, false)]));
/// let factory = PlanFactory::new("FilterExec", vec![SourceSpec::new(schema)], |inputs| {
///     let input = Arc::clone(&inputs[0]);
///     let predicate = col("b", &input.schema())?;
///     Ok(Arc::new(FilterExec::try_new(predicate, input)?) as Arc<dyn ExecutionPlan>)
/// });
/// let report = PlanHarness::new().check(&factory)?;
/// report.assert_no_invariant_violations();
/// # Ok::<(), datafusion_common::DataFusionError>(())
/// ```
///
/// [`ROW_ID_COLUMN`]: crate::physical_plan::fixtures::ROW_ID_COLUMN
/// [`COPY_SUFFIX`]: crate::physical_plan::fixtures::COPY_SUFFIX
#[derive(Clone)]
pub struct PlanFactory {
    /// The name, used in reports
    pub name: String,
    /// The base spec of each input. A plan without inputs, such as
    /// `EmptyExec`, has none.
    pub inputs: Vec<SourceSpec>,
    /// The name of each check skipped in every case, with the reason, which
    /// the report shows
    pub allowed: Vec<(String, String)>,
    create: Arc<CreateFn>,
}

impl fmt::Debug for PlanFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanFactory")
            .field("name", &self.name)
            .field("inputs", &self.inputs)
            .field("allowed", &self.allowed)
            .finish_non_exhaustive()
    }
}

impl PlanFactory {
    /// A factory named `name` that builds its plan from one input per entry
    /// of `inputs` with `create`
    pub fn new<F>(name: impl Into<String>, inputs: Vec<SourceSpec>, create: F) -> Self
    where
        F: Fn(Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            name: name.into(),
            inputs,
            allowed: vec![],
            create: Arc::new(create),
        }
    }

    /// Skip the check named `check` in every case, because of `reason`,
    /// which the report shows. Use it for a known limitation of the check,
    /// not to hide a problem of the plan.
    pub fn allow(mut self, check: impl Into<String>, reason: impl Into<String>) -> Self {
        self.allowed.push((check.into(), reason.into()));
        self
    }

    /// Build the plan under test on `inputs`
    pub fn create(
        &self,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        (self.create)(inputs)
    }
}

/// How the [`PlanHarness`] lays out the inputs of a factory for one case.
///
/// A profile sets everything about an input that the plan's author does not
/// need to know: how many partitions it has and how its rows are spread over
/// them, the precision of its statistics, the seed, and whether its columns
/// are constant or have copies. Rows are split into
/// random batches of up to 16 rows, with empty batches. The size of each
/// input is the number of rows of its base [`SourceSpec`] times
/// [`Self::row_multiplier`]: only the author of the plan knows what size
/// makes sense, since for example a cross join produces the product of its
/// input sizes.
#[derive(Debug, Clone)]
pub struct Profile {
    /// The name, used to refer to the case in reports
    pub name: String,
    /// Rows are spread over one partition per weight, in proportion to the
    /// weights, and a weight of 0 gives an empty partition. An input that must
    /// be hash partitioned gets as many partitions.
    pub partition_weights: Vec<usize>,
    /// Each input gets this many times the rows of its base spec. 0 gives
    /// inputs without rows.
    pub row_multiplier: usize,
    /// The precision of the statistics the inputs report
    pub statistics: StatisticsPrecision,
    /// Input `i` is generated with seed `seed * 1000 + i`, so that inputs with
    /// the same schema get different data
    pub seed: u64,
    /// If set, every column of every input is constant, with values chosen
    /// this way, and the inputs declare the constants (see
    /// [`SourceSpec::with_constant`]). The row id column is not constant.
    pub constants: Option<ConstantValues>,
    /// Whether every column of every input has a copy, which the input
    /// declares equal to it (see [`SourceSpec::with_copy`]). The row id column
    /// has none.
    pub copies: bool,
    /// Whether every input that does not need another ordering declares that
    /// its partitions are sorted by its row id column, which they are (see
    /// [`SourceSpec::with_row_id_ordering`]), so that every node sees sorted
    /// input
    pub row_id_ordering: bool,
    /// Whether every input that does not need another ordering, and whose base
    /// spec has none, declares that its partitions are sorted by every column
    /// of its base spec, in schema order, which they are. Unlike the row ids,
    /// the values of these columns repeat, so that a node that reports an
    /// ordering longer than the one it keeps between rows that tie on its
    /// input's ordering is caught.
    pub column_ordering: bool,
    /// Whether every input is hash partitioned on the first column of its
    /// base spec into one partition per weight of
    /// [`Self::partition_weights`], and declares it (see
    /// [`SourceSpec::with_hash_partitioning`]), so that every node sees hash
    /// partitioned input. How many rows each partition gets then depends on
    /// the data. An input that must be partitioned otherwise is.
    pub hash_partitioning: bool,
    /// Whether the [`CheckKind::Stream`] checks run. They depend on how a plan
    /// drives its streams rather than on the shape of its data, and several of
    /// them wait for timeouts, so only [`Self::default_profile`] runs them.
    pub stream_checks: bool,
}

impl Profile {
    /// A profile named `name` with three partitions, the second of them empty
    /// and the third with twice the rows of the first, exact statistics, seed
    /// 0, no constant or copied columns, no ordering on the row ids or the
    /// columns and no hash partitioning, that does not run the stream checks
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partition_weights: vec![1, 0, 2],
            row_multiplier: 1,
            statistics: StatisticsPrecision::Exact,
            seed: 0,
            constants: None,
            copies: false,
            row_id_ordering: false,
            column_ordering: false,
            hash_partitioning: false,
            stream_checks: false,
        }
    }

    /// `default`: the layout of [`Self::new`], with every check
    pub fn default_profile() -> Self {
        Self {
            stream_checks: true,
            ..Self::new("default")
        }
    }

    /// The profiles [`PlanHarness::new`] uses: [`Self::default_profile`],
    /// then `single partition`, `inexact statistics`, `absent statistics`,
    /// `empty input`, `uniform constants` (every column has the same value in
    /// every partition), `constants per partition` (every column has one
    /// value per partition), `copied columns` (every column has a copy
    /// that the input declares equal to it), `sorted by row id` (every input
    /// that does not need another ordering declares that it is sorted by its
    /// row ids), `sorted by columns` (every input that does not need another
    /// ordering declares that it is sorted by its columns, whose values
    /// repeat) and `hash partitioned` (every input that does not need
    /// another partitioning is hash partitioned on its first column)
    pub fn defaults() -> Vec<Self> {
        vec![
            Self::default_profile(),
            Self {
                partition_weights: vec![1],
                ..Self::new("single partition")
            },
            Self {
                statistics: StatisticsPrecision::Inexact,
                ..Self::new("inexact statistics")
            },
            Self {
                statistics: StatisticsPrecision::Absent,
                ..Self::new("absent statistics")
            },
            Self {
                row_multiplier: 0,
                ..Self::new("empty input")
            },
            Self {
                constants: Some(ConstantValues::Uniform),
                ..Self::new("uniform constants")
            },
            Self {
                constants: Some(ConstantValues::PerPartition),
                ..Self::new("constants per partition")
            },
            Self {
                copies: true,
                ..Self::new("copied columns")
            },
            Self {
                row_id_ordering: true,
                ..Self::new("sorted by row id")
            },
            Self {
                column_ordering: true,
                ..Self::new("sorted by columns")
            },
            Self {
                hash_partitioning: true,
                ..Self::new("hash partitioned")
            },
        ]
    }

    /// `seed 1` and `seed 2` (other data), `2 partitions` and `5 partitions`
    /// (the latter with an empty partition), and `large input` (8 times the
    /// rows, so that operators with internal buffers fill them several
    /// times). The built-in audit uses them, in addition to
    /// [`Self::defaults`], with the `extended_tests` feature.
    pub fn extended() -> Vec<Self> {
        vec![
            Self {
                seed: 1,
                ..Self::new("seed 1")
            },
            Self {
                seed: 2,
                ..Self::new("seed 2")
            },
            Self {
                partition_weights: vec![1, 1],
                ..Self::new("2 partitions")
            },
            Self {
                partition_weights: vec![2, 1, 0, 3, 1],
                ..Self::new("5 partitions")
            },
            Self {
                row_multiplier: 8,
                ..Self::new("large input")
            },
        ]
    }

    /// The rows of each partition for an input of `num_rows` rows: in
    /// proportion to the weights, with the rows that do not divide evenly in
    /// the last partition with a non-zero weight
    fn partition_rows(&self, num_rows: usize) -> Vec<usize> {
        let total: usize = self.partition_weights.iter().sum();
        if total == 0 {
            return vec![0; self.partition_weights.len()];
        }
        let mut rows: Vec<usize> = self
            .partition_weights
            .iter()
            .map(|weight| num_rows * weight / total)
            .collect();
        let remainder = num_rows - rows.iter().sum::<usize>();
        if let Some(last) = self.partition_weights.iter().rposition(|w| *w > 0) {
            rows[last] += remainder;
        }
        rows
    }
}

/// One test of a [`PlanFactory`]: the inputs derived from its base specs for
/// a [`Profile`] and the plan's input requirements, and the plan built on
/// them. To reproduce a case by hand, build each of [`Self::inputs`] and call
/// [`PlanFactory::create`] on them.
#[derive(Debug, Clone)]
pub struct Case {
    /// The profile the case was derived from
    pub profile: Profile,
    /// The spec of each input
    pub inputs: Vec<SourceSpec>,
    /// The plan built on the inputs
    pub plan: Arc<dyn ExecutionPlan>,
}

/// Checks a [`PlanFactory`] on one case per [`Profile`].
///
/// See [`PlanFactory`] for an example, and the [module documentation](self)
/// for how cases are derived.
#[derive(Debug, Clone)]
pub struct PlanHarness {
    checker: PlanChecker,
    profiles: Vec<Profile>,
}

impl Default for PlanHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanHarness {
    /// A harness that runs every built-in check on the cases of
    /// [`Profile::defaults`]
    pub fn new() -> Self {
        Self {
            checker: PlanChecker::new(),
            profiles: Profile::defaults(),
        }
    }

    /// Run the checks of `checker`, with its settings such as timeouts,
    /// instead of every built-in check
    pub fn with_checker(mut self, checker: PlanChecker) -> Self {
        self.checker = checker;
        self
    }

    /// Derive cases from `profiles` instead of [`Profile::defaults`]
    pub fn with_profiles(mut self, profiles: Vec<Profile>) -> Self {
        self.profiles = profiles;
        self
    }

    /// The cases of `factory`, one per profile, in order. A factory without
    /// inputs has one case, for the first profile, since every profile gives
    /// the same plan. Deriving the cases builds the plans, but does not
    /// execute them.
    ///
    /// Returns an error if `create` fails, an input cannot be generated from
    /// its spec, or the input requirements of the plan cannot be met.
    pub fn cases(&self, factory: &PlanFactory) -> Result<Vec<Case>> {
        let profiles = if factory.inputs.is_empty() {
            &self.profiles[..self.profiles.len().min(1)]
        } else {
            &self.profiles
        };
        profiles
            .iter()
            .map(|profile| {
                requirements::derive_case(factory, profile).map_err(|e| {
                    e.context(format!(
                        "deriving the '{}' case of '{}'",
                        profile.name, factory.name
                    ))
                })
            })
            .collect()
    }

    /// Check every case of `factory`.
    ///
    /// Starts a current-thread Tokio runtime to execute the plans, and so
    /// panics if called from within a Tokio runtime. Use
    /// [`Self::check_async`] in async code.
    ///
    /// Returns an error if the cases cannot be derived (see [`Self::cases`]),
    /// or a check could not run (see [`PlanCheck::check`]).
    ///
    /// # Panics
    ///
    /// If `factory` allows a check that the checker does not have.
    ///
    /// [`PlanCheck::check`]: crate::physical_plan::PlanCheck::check
    pub fn check(&self, factory: &PlanFactory) -> Result<FactoryReport> {
        runtime()?.block_on(self.check_async(factory))
    }

    /// Check every case of `factory` on the current Tokio runtime
    pub async fn check_async(&self, factory: &PlanFactory) -> Result<FactoryReport> {
        let checker = factory
            .allowed
            .iter()
            .fold(self.checker.clone(), |checker, (check, _)| {
                checker.allow(check)
            });
        let mut cases = vec![];
        for case in self.cases(factory)? {
            let checker = checker.clone().retain(|check| {
                case.profile.stream_checks || check.kind != CheckKind::Stream
            });
            let report = checker.check_async(&case.plan).await?;
            cases.push((case, report));
        }
        Ok(FactoryReport {
            name: factory.name.clone(),
            allowed: factory.allowed.clone(),
            cases,
        })
    }
}

/// The result of checking every case of a [`PlanFactory`] with a
/// [`PlanHarness`]. It displays every case, with its plan and violations if
/// it has any.
#[derive(Debug, Clone)]
pub struct FactoryReport {
    /// The name of the factory
    pub name: String,
    /// The checks skipped in every case, with the reasons
    pub allowed: Vec<(String, String)>,
    /// Each case, with the checker's report on its plan
    pub cases: Vec<(Case, Report)>,
}

impl FactoryReport {
    /// Panics if any case has a [`Severity::Invariant`] violation
    ///
    /// [`Severity::Invariant`]: crate::Severity::Invariant
    pub fn assert_no_invariant_violations(&self) {
        assert!(
            !self
                .cases
                .iter()
                .any(|(_, report)| report.has_invariant_violations()),
            "ExecutionPlan invariant violations found:\n{self}"
        );
    }

    /// Panics if any case has a violation, including lints
    pub fn assert_clean(&self) {
        assert!(
            self.cases
                .iter()
                .all(|(_, report)| report.violations.is_empty()),
            "ExecutionPlan check violations found:\n{self}"
        );
    }
}

impl fmt::Display for FactoryReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        for (check, reason) in &self.allowed {
            write!(f, "\n  allowed {check}: {reason}")?;
        }
        for (case, report) in &self.cases {
            write!(f, "\n  case {}:", case.profile.name)?;
            if report.violations.is_empty() {
                write!(f, " no violations")?;
                continue;
            }
            let plan = displayable(case.plan.as_ref()).indent(true).to_string();
            for line in plan.lines().chain(report.to_string().lines()) {
                write!(f, "\n    {line}")?;
            }
        }
        Ok(())
    }
}
