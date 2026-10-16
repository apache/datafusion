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

use std::fmt;
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Result, Statistics, exec_datafusion_err, internal_err, not_impl_err, plan_err,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    ConstExpr, EquivalenceProperties, LexOrdering, PhysicalExpr,
};
use datafusion_physical_plan::coop::make_cooperative;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, StatisticsArgs,
};
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use rand::SeedableRng;
use rand::rngs::StdRng;

use super::BatchLayout;
use super::spec::split_rows;
use crate::physical_plan::oracle;

/// Two expressions that a [`MockSourceExec`] declares equal on every row
pub type Equality = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);

/// How precise the statistics reported by a [`MockSourceExec`] are.
///
/// The values are always computed from the data. Only their precision
/// changes, which lets tests check how a plan handles exact, estimated and
/// missing input statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StatisticsPrecision {
    /// Report the true values as `Precision::Exact`
    #[default]
    Exact,
    /// Report the true values as `Precision::Inexact`
    Inexact,
    /// Report `Statistics::new_unknown`
    Absent,
}

/// How the streams of a [`MockSourceExec`] behave.
///
/// Every behavior serves the same batches in the same order. They differ in
/// what happens after the batches are served, which lets checks observe how
/// a plan reacts to inputs that are slow, fail or never end. The source only
/// reports properties that hold for the behavior; see each variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamBehavior {
    /// Serve the batches, then end
    #[default]
    Finite,
    /// Serve the first `batches` batches of each partition (all of them if it
    /// has fewer), then stay pending forever instead of ending. Models a
    /// bounded source that stops making progress, so the reported properties
    /// and statistics do not change: they describe the data the stream would
    /// produce if it finished.
    PendingAfter(usize),
    /// Serve the first `batches` batches of each partition (all of them if it
    /// has fewer), then return an error instead of the next batch or the end,
    /// and then end. The reported properties and statistics do not change,
    /// since they describe the output of a successful execution.
    ErrorAfter(usize),
    /// Report `Boundedness::Unbounded` and never end. After its batches, a
    /// partition keeps producing rows: it repeats its batches, or, when the
    /// source declares an output ordering, repeats its last row so the
    /// ordering still holds. Partitions without rows end at once. Statistics
    /// are reported as unknown, since the row count is infinite. Row ids, if
    /// any, repeat as well.
    ///
    /// After `max_rows` rows in a partition, the stream stays pending forever.
    /// It still never ends, but this bounds the memory that a plan buffering
    /// the input can use. The stream consumes Tokio task budget, so a plan
    /// that keeps polling it still yields to the runtime.
    ///
    /// A source with no rows at all cannot be unbounded.
    Unbounded { max_rows: usize },
}

impl fmt::Display for StreamBehavior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamBehavior::Finite => write!(f, "Finite"),
            StreamBehavior::PendingAfter(batches) => write!(f, "PendingAfter({batches})"),
            StreamBehavior::ErrorAfter(batches) => write!(f, "ErrorAfter({batches})"),
            StreamBehavior::Unbounded { max_rows } => {
                write!(f, "Unbounded(max_rows={max_rows})")
            }
        }
    }
}

/// A leaf [`ExecutionPlan`] that serves fixed batches and reports properties
/// that are verified against them.
///
/// - Statistics are computed from the batches, with the precision set by
///   [`Self::with_statistics_precision`].
/// - An output ordering set with [`Self::try_with_output_ordering`] is
///   rejected unless every partition is sorted by it.
/// - Constants set with [`Self::try_with_constants`] are rejected unless the
///   data has a single value for each in every partition, the same value in
///   every partition for a uniform one.
/// - Equalities set with [`Self::try_with_equalities`] are rejected unless
///   the two expressions of each are equal on every row.
/// - Hash partitioning set with [`Self::try_with_partitioning`] is rejected
///   unless every row is in the partition `RepartitionExec` would send it to.
/// - [`Self::try_with_stream_behavior`] makes the streams stall, fail or never
///   end, and adjusts the reported boundedness and statistics to match.
///
/// Because of this, checks that compare a plan with its input can treat what
/// a `MockSourceExec` reports as correct. Use [`SourceSpec`] to generate the
/// batches.
///
/// [`SourceSpec`]: crate::physical_plan::fixtures::SourceSpec
#[derive(Debug, Clone)]
pub struct MockSourceExec {
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    partitioning: Partitioning,
    output_ordering: Option<LexOrdering>,
    constants: Vec<ConstExpr>,
    equalities: Vec<Equality>,
    precision: StatisticsPrecision,
    behavior: StreamBehavior,
    /// Exact statistics of all partitions, before `precision` is applied
    statistics: Statistics,
    /// Exact statistics of each partition, before `precision` is applied
    partition_statistics: Vec<Statistics>,
    cache: Arc<PlanProperties>,
}

impl MockSourceExec {
    /// Create a source that serves `partitions`, one inner `Vec` of batches per
    /// output partition, with `UnknownPartitioning` and exact statistics.
    ///
    /// Returns an error if any batch does not have exactly `schema`.
    pub fn try_new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        for (p, batches) in partitions.iter().enumerate() {
            for batch in batches {
                if batch.schema().as_ref() != schema.as_ref() {
                    return plan_err!(
                        "MockSourceExec partition {p} has a batch with schema {:?}, \
                         expected {schema:?}",
                        batch.schema()
                    );
                }
            }
        }
        let statistics = oracle::exact_statistics(&schema, &partitions.concat())?;
        let partition_statistics = partitions
            .iter()
            .map(|batches| oracle::exact_statistics(&schema, batches))
            .collect::<Result<Vec<_>>>()?;
        let partitioning = Partitioning::UnknownPartitioning(partitions.len());
        let behavior = StreamBehavior::Finite;
        let cache =
            Self::compute_properties(&schema, &partitioning, None, &[], &[], behavior)?;
        Ok(Self {
            schema,
            partitions,
            partitioning,
            output_ordering: None,
            constants: vec![],
            equalities: vec![],
            precision: StatisticsPrecision::Exact,
            behavior,
            statistics,
            partition_statistics,
            cache,
        })
    }

    /// Set the precision of the reported statistics. An unbounded source
    /// always reports unknown statistics.
    pub fn with_statistics_precision(mut self, precision: StatisticsPrecision) -> Self {
        self.precision = precision;
        self
    }

    /// Set how the streams behave after serving their batches.
    ///
    /// Returns an error for [`StreamBehavior::Unbounded`] if the source has no
    /// rows, since it could not produce infinite data.
    pub fn try_with_stream_behavior(mut self, behavior: StreamBehavior) -> Result<Self> {
        let unbounded = matches!(behavior, StreamBehavior::Unbounded { .. });
        if unbounded && !self.has_rows() {
            return plan_err!("an unbounded MockSourceExec needs at least one row");
        }
        let boundedness_changed = self.is_unbounded() != unbounded;
        self.behavior = behavior;
        // Keep the same properties when they do not change, so that plans
        // rebuilt on this source can reuse their own properties
        if boundedness_changed {
            self.cache = self.recompute_properties()?;
        }
        Ok(self)
    }

    /// A copy of the source with the rows of each partition split into
    /// batches according to `layout`, using `seed` for random layouts.
    ///
    /// Every partition keeps the same rows in the same order, so the copy
    /// reports the same properties and statistics. It shares the same
    /// `PlanProperties`, so plans rebuilt on it can reuse their own
    /// properties.
    pub fn with_batch_layout(&self, layout: BatchLayout, seed: u64) -> Result<Self> {
        let mut rng = StdRng::seed_from_u64(seed);
        let partitions = self
            .partitions
            .iter()
            .map(|batches| {
                let rows = concat_batches(&self.schema, batches)?;
                Ok(split_rows(&rows, layout, &mut rng))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            partitions,
            ..self.clone()
        })
    }

    /// Returns true if the source has at least one row
    pub fn has_rows(&self) -> bool {
        self.partitions.iter().flatten().any(|b| b.num_rows() > 0)
    }

    fn is_unbounded(&self) -> bool {
        matches!(self.behavior, StreamBehavior::Unbounded { .. })
    }

    /// Declare that every partition is sorted by `ordering`.
    ///
    /// Returns an error if a partition is not sorted by `ordering`.
    pub fn try_with_output_ordering(mut self, ordering: LexOrdering) -> Result<Self> {
        for (p, batches) in self.partitions.iter().enumerate() {
            if let Some(row) = oracle::first_unsorted_row(batches, &ordering)? {
                return plan_err!(
                    "MockSourceExec partition {p} is not sorted by [{ordering}] at row {row}"
                );
            }
        }
        self.output_ordering = Some(ordering);
        self.cache = self.recompute_properties()?;
        Ok(self)
    }

    /// Declare that each of `constants` is constant: within every partition,
    /// and across partitions as its `across_partitions` says. Replaces the
    /// constants declared before.
    ///
    /// Returns an error if the data contradicts a constant (see
    /// [`oracle::constant_violations`]).
    pub fn try_with_constants(mut self, constants: Vec<ConstExpr>) -> Result<Self> {
        for constant in &constants {
            if let Some(violation) =
                oracle::constant_violations(&self.partitions, constant)?.first()
            {
                return plan_err!("MockSourceExec constant does not hold: {violation}");
            }
        }
        self.constants = constants;
        self.cache = self.recompute_properties()?;
        Ok(self)
    }

    /// Declare that the two expressions of each of `equalities` are equal on
    /// every row, which puts them in the same equivalence class. Replaces the
    /// equalities declared before.
    ///
    /// Returns an error if a row has different values for the two expressions
    /// (see [`oracle::first_unequal_row`]).
    pub fn try_with_equalities(mut self, equalities: Vec<Equality>) -> Result<Self> {
        for (left, right) in &equalities {
            for (p, batches) in self.partitions.iter().enumerate() {
                if let Some((row, left_value, right_value)) =
                    oracle::first_unequal_row(batches, left, right)?
                {
                    return plan_err!(
                        "MockSourceExec equality does not hold: {left} and {right} are \
                         {left_value} and {right_value} in row {row} of partition {p}"
                    );
                }
            }
        }
        self.equalities = equalities;
        self.cache = self.recompute_properties()?;
        Ok(self)
    }

    /// Declare the output partitioning.
    ///
    /// The partition count must match the number of partitions. For
    /// `Partitioning::Hash`, every row must be in the partition that
    /// `RepartitionExec` would send it to. `Partitioning::Range` is not
    /// supported yet.
    pub fn try_with_partitioning(mut self, partitioning: Partitioning) -> Result<Self> {
        let count = partitioning.partition_count();
        if count != self.partitions.len() {
            return plan_err!(
                "MockSourceExec has {} partitions, but the partitioning {partitioning} \
                 has {count}",
                self.partitions.len()
            );
        }
        match &partitioning {
            Partitioning::Hash(exprs, n) => {
                for (p, batches) in self.partitions.iter().enumerate() {
                    let misplaced =
                        oracle::rows_outside_hash_partition(batches, exprs, *n, p)?;
                    if misplaced > 0 {
                        return plan_err!(
                            "MockSourceExec partition {p} has {misplaced} rows that do \
                             not belong to it under {partitioning}"
                        );
                    }
                }
            }
            Partitioning::Range(_) => {
                return not_impl_err!(
                    "MockSourceExec does not support range partitioning"
                );
            }
            Partitioning::RoundRobinBatch(_) | Partitioning::UnknownPartitioning(_) => {}
        }
        self.partitioning = partitioning;
        self.cache = self.recompute_properties()?;
        Ok(self)
    }

    /// The batches served by each partition
    pub fn partitions(&self) -> &[Vec<RecordBatch>] {
        &self.partitions
    }

    fn recompute_properties(&self) -> Result<Arc<PlanProperties>> {
        Self::compute_properties(
            &self.schema,
            &self.partitioning,
            self.output_ordering.as_ref(),
            &self.constants,
            &self.equalities,
            self.behavior,
        )
    }

    fn compute_properties(
        schema: &SchemaRef,
        partitioning: &Partitioning,
        output_ordering: Option<&LexOrdering>,
        constants: &[ConstExpr],
        equalities: &[Equality],
        behavior: StreamBehavior,
    ) -> Result<Arc<PlanProperties>> {
        let mut eq_properties = match output_ordering {
            Some(ordering) => EquivalenceProperties::new_with_orderings(
                Arc::clone(schema),
                [ordering.clone()],
            ),
            None => EquivalenceProperties::new(Arc::clone(schema)),
        };
        eq_properties.add_constants(constants.iter().cloned())?;
        for (left, right) in equalities {
            eq_properties.add_equal_conditions(Arc::clone(left), Arc::clone(right))?;
        }
        let boundedness = match behavior {
            StreamBehavior::Unbounded { .. } => Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
            _ => Boundedness::Bounded,
        };
        Ok(Arc::new(PlanProperties::new(
            eq_properties,
            partitioning.clone(),
            EmissionType::Incremental,
            boundedness,
        )))
    }

    /// The batches of a partition, followed by what `self.behavior` adds
    fn stream_items(
        &self,
        batches: &[RecordBatch],
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let partition = batches.to_vec();
        match self.behavior {
            StreamBehavior::Finite => stream::iter(partition.into_iter().map(Ok)).boxed(),
            StreamBehavior::PendingAfter(k) => {
                stream::iter(partition.into_iter().take(k).map(Ok))
                    .chain(stream::pending())
                    .boxed()
            }
            StreamBehavior::ErrorAfter(k) => {
                let served = k.min(partition.len());
                stream::iter(partition.into_iter().take(k).map(Ok))
                    .chain(stream::once(async move {
                        Err(exec_datafusion_err!(
                            "MockSourceExec injected error after {served} batches"
                        ))
                    }))
                    .boxed()
            }
            StreamBehavior::Unbounded { max_rows } => {
                let Some(last) = partition.iter().rev().find(|b| b.num_rows() > 0) else {
                    return stream::empty().boxed();
                };
                let repeated: Box<dyn Iterator<Item = RecordBatch> + Send> =
                    if self.output_ordering.is_some() {
                        // Repeating the last row keeps every partition sorted
                        let last_row = last.num_rows() as u32 - 1;
                        let indices = UInt32Array::from(vec![last_row; last.num_rows()]);
                        match take_record_batch(last, &indices) {
                            Ok(batch) => Box::new(std::iter::repeat(batch)),
                            Err(e) => {
                                return stream::once(async move { Err(e.into()) })
                                    .boxed();
                            }
                        }
                    } else {
                        Box::new(partition.clone().into_iter().cycle())
                    };
                let mut served = 0;
                let batches =
                    partition.into_iter().chain(repeated).take_while(move |b| {
                        let more = served < max_rows;
                        served += b.num_rows();
                        more
                    });
                stream::iter(batches.map(Ok))
                    .chain(stream::pending())
                    .boxed()
            }
        }
    }
}

impl DisplayAs for MockSourceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let rows: Vec<usize> = self
            .partitions
            .iter()
            .map(|batches| batches.iter().map(RecordBatch::num_rows).sum())
            .collect();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MockSourceExec: partitioning={}, partition_rows={rows:?}, \
                     statistics={:?}",
                    self.partitioning, self.precision
                )?;
                if let Some(ordering) = &self.output_ordering {
                    write!(f, ", output_ordering=[{ordering}]")?;
                }
                if !self.constants.is_empty() {
                    write!(
                        f,
                        ", constants=[{}]",
                        ConstExpr::format_list(&self.constants)
                    )?;
                }
                if !self.equalities.is_empty() {
                    let equalities: Vec<String> = self
                        .equalities
                        .iter()
                        .map(|(left, right)| format!("{left} = {right}"))
                        .collect();
                    write!(f, ", equalities=[{}]", equalities.join(", "))?;
                }
                if self.behavior != StreamBehavior::Finite {
                    write!(f, ", stream={}", self.behavior)?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partition_rows={rows:?}")
            }
        }
    }
}

impl ExecutionPlan for MockSourceExec {
    fn name(&self) -> &'static str {
        "MockSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("MockSourceExec does not have children");
        }
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(batches) = self.partitions.get(partition) else {
            return internal_err!(
                "Invalid partition index: {partition}, the partition count is {}",
                self.partitions.len()
            );
        };
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            self.stream_items(batches),
        ));
        Ok(if self.is_unbounded() {
            make_cooperative(stream)
        } else {
            stream
        })
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let statistics = match args.partition() {
            None => &self.statistics,
            Some(partition) => match self.partition_statistics.get(partition) {
                Some(statistics) => statistics,
                None => {
                    return internal_err!(
                        "Invalid partition index: {partition}, the partition count is {}",
                        self.partition_statistics.len()
                    );
                }
            },
        };
        let precision = if self.is_unbounded() {
            StatisticsPrecision::Absent
        } else {
            self.precision
        };
        Ok(Arc::new(match precision {
            StatisticsPrecision::Exact => statistics.clone(),
            StatisticsPrecision::Inexact => statistics.clone().to_inexact(),
            StatisticsPrecision::Absent => Statistics::new_unknown(&self.schema),
        }))
    }
}

/// Replace each [`MockSourceExec`] leaf of `plan`, including `plan` itself,
/// by the plan `replace` returns for it, if any, and rebuild the nodes above
/// the replaced leaves. A replacement that reports the same properties as the
/// original lets the rebuilt nodes keep theirs.
pub(crate) fn map_mock_leaves(
    plan: &Arc<dyn ExecutionPlan>,
    mut replace: impl FnMut(&MockSourceExec) -> Result<Option<Arc<dyn ExecutionPlan>>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::clone(plan)
        .transform_up(|plan| {
            let Some(source) = plan.downcast_ref::<MockSourceExec>() else {
                return Ok(Transformed::no(plan));
            };
            Ok(match replace(source)? {
                Some(replacement) => Transformed::yes(replacement),
                None => Transformed::no(plan),
            })
        })?
        .data)
}
