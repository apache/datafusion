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

//! [`Partitioning`] and [`Distribution`] for `ExecutionPlans`

use crate::expressions::{Literal, UnKnownColumn};
use crate::simplifier::const_evaluator::create_dummy_batch;
use crate::{
    EquivalenceProperties, PhysicalExpr, ScalarFunctionExpr,
    equivalence::ProjectionMapping, physical_exprs_contains, physical_exprs_equal,
};
use arrow::datatypes::Schema;
pub use datafusion_common::SplitPoint;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, ScalarValue, plan_err, validate_range_split_points};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::format_physical_expr_list;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
#[cfg(feature = "proto")]
use datafusion_physical_expr_common::sort_expr::{
    sort_exprs_try_from_proto, sort_exprs_try_to_proto,
};
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

/// Output partitioning supported by [`ExecutionPlan`]s.
///
/// Calling [`ExecutionPlan::execute`] produce one or more independent streams of
/// [`RecordBatch`]es in parallel, referred to as partitions. The streams are Rust
/// `async` [`Stream`]s (a special kind of future). The number of output
/// partitions varies based on the input and the operation performed.
///
/// For example, an `ExecutionPlan` that has output partitioning of 3 will
/// produce 3 distinct output streams as the result of calling
/// `ExecutionPlan::execute(0)`, `ExecutionPlan::execute(1)`, and
/// `ExecutionPlan::execute(2)`, as shown below:
///
/// ```text
///                                                   ...         ...        ...
///               ...                                  ▲           ▲           ▲
///                                                    │           │           │
///                ▲                                   │           │           │
///                │                                   │           │           │
///                │                               ┌───┴────┐  ┌───┴────┐  ┌───┴────┐
///     ┌────────────────────┐                     │ Stream │  │ Stream │  │ Stream │
///     │   ExecutionPlan    │                     │  (0)   │  │  (1)   │  │  (2)   │
///     └────────────────────┘                     └────────┘  └────────┘  └────────┘
///                ▲                                   ▲           ▲           ▲
///                │                                   │           │           │
///     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─                          │           │           │
///             Input        │                         │           │           │
///     └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─                          │           │           │
///                ▲                               ┌ ─ ─ ─ ─   ┌ ─ ─ ─ ─   ┌ ─ ─ ─ ─
///                │                                 Input  │    Input  │    Input  │
///                │                               │ Stream    │ Stream    │ Stream
///                                                   (0)   │     (1)   │     (2)   │
///               ...                              └ ─ ▲ ─ ─   └ ─ ▲ ─ ─   └ ─ ▲ ─ ─
///                                                    │           │           │
///                                                    │           │           │
///                                                    │           │           │
///
/// ExecutionPlan with 1 input                      3 (async) streams, one for each
/// that has 3 partitions, which itself             output partition
/// has 3 output partitions
/// ```
///
/// It is common (but not required) that an `ExecutionPlan` has the same number
/// of input partitions as output partitions. However, some plans have different
/// numbers such as the `RepartitionExec` that redistributes batches from some
/// number of inputs to some number of outputs
///
/// ```text
///               ...                                     ...         ...        ...
///
///                                                        ▲           ▲           ▲
///                ▲                                       │           │           │
///                │                                       │           │           │
///       ┌────────┴───────────┐                           │           │           │
///       │  RepartitionExec   │                      ┌────┴───┐  ┌────┴───┐  ┌────┴───┐
///       └────────────────────┘                      │ Stream │  │ Stream │  │ Stream │
///                ▲                                  │  (0)   │  │  (1)   │  │  (2)   │
///                │                                  └────────┘  └────────┘  └────────┘
///                │                                       ▲           ▲           ▲
///                ...                                     │           │           │
///                                                        └──────────┐│┌──────────┘
///                                                                   │││
///                                                                   │││
/// RepartitionExec with 1 input
/// partition and 3 output partitions                 3 (async) streams, that internally
///                                                    pull from the same input stream
///                                                                  ...
/// ```
///
/// # Additional Examples
///
/// A simple `FileScanExec` might produce one output stream (partition) for each
/// file (note the actual DataFusion file scanners can read individual files in
/// parallel, potentially producing multiple partitions per file)
///
/// Plans such as `SortPreservingMerge` produce a single output stream
/// (1 output partition) by combining some number of input streams (input partitions)
///
/// Plans such as `FilterExec` produce the same number of output streams
/// (partitions) as input streams (partitions).
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
/// [`ExecutionPlan::execute`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#tymethod.execute
/// [`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
/// [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number of
    /// partitions
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Partition rows by source-declared ranges
    Range(RangePartitioning),
    /// Unknown partitioning scheme with a known number of partitions
    UnknownPartitioning(usize),
}

impl Display for Partitioning {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Partitioning::RoundRobinBatch(size) => write!(f, "RoundRobinBatch({size})"),
            Partitioning::Hash(phy_exprs, size) => {
                let phy_exprs_str = phy_exprs
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "Hash([{phy_exprs_str}], {size})")
            }
            Partitioning::Range(range) => write!(f, "{range}"),
            Partitioning::UnknownPartitioning(size) => {
                write!(f, "UnknownPartitioning({size})")
            }
        }
    }
}

/// Physical range partitioning.
///
/// [`RangePartitioning`] describes an ordered key space with sampled split points.
///
/// - `ordering` defines the partitioning key and ordering.
/// - `samples` are the maximum-resolution split points supplied by the caller.
/// - The effective `split_points` are derived from those samples for the selected
///   partition count; the count itself is `split_points.len() + 1` and is not
///   stored separately.
///
/// Comparisons use the lexicographic order defined by `ordering`, including
/// `ASC`/`DESC` and null ordering. Samples must be strictly ordered according
/// to that ordering, and each sample must have one value per ordering
/// expression. See [`SplitPoint`] for the shared boundary convention.
///
/// When `partition_count` is smaller than [`Self::max_partition_count`], the
/// samples are evenly down-sampled to derive the effective split points. This
/// allows planners to reduce or later restore the number of partitions without
/// losing the original distribution sample.
///
/// Like other user-specified data properties such as sortedness, if a source
/// declares range partitioning, it is responsible for placing each row in the
/// partition described by the split points. DataFusion will not validate this is
/// upheld.
///
/// For a single range key:
///
/// ```text
/// ordering = [date ASC NULLS LAST]
/// split_points = [
///   (2022-01-01),
///   (2023-01-01),
/// ]
///
/// partition 0: date before 2022-01-01
/// partition 1: date between 2022-01-01 (inclusive) and 2023-01-01 (exclusive)
/// partition 2: date at/after 2023-01-01
/// ```
///
/// The same model extends to compound keys.
/// For `ordering = [time ASC, city ASC]`, split points are ordered
/// lexicographically by `(time, city)`:
///
/// ```text
/// ordering = [time ASC NULLS LAST, city ASC NULLS LAST]
/// split_points = [
///   (2022, Allston),
///   (2023, Allston),
/// ]
///
/// partition 0: keys before  (2022, Allston)
/// partition 1: keys between (2022, Allston) and (2023, Allston)
/// partition 2: keys at/after (2023, Allston)
/// ```
///
/// Equality includes retained samples, since they determine which future scales
/// are possible. Use [`Self::has_same_layout`] to compare only the current layout.
#[derive(Debug, Clone, PartialEq)]
pub struct RangePartitioning {
    /// Ordered partitioning key.
    ordering: LexOrdering,
    /// Caller-supplied maximum-resolution split points used to derive the
    /// effective boundaries.
    samples: Arc<[SplitPoint]>,
    /// Effective boundaries for the current partition count.
    split_points: Arc<[SplitPoint]>,
}

impl RangePartitioning {
    /// Creates range partitioning metadata without validating split points.
    ///
    /// Use [`Self::try_new`] to validate exact boundaries, or
    /// [`Self::try_new_with_samples`] to retain additional samples for scaling up.
    #[deprecated(
        since = "56.0.0",
        note = "Use RangePartitioning::try_new or try_new_with_samples instead"
    )]
    pub fn new(ordering: LexOrdering, split_points: Vec<SplitPoint>) -> Self {
        let split_points: Arc<[SplitPoint]> = Arc::from(split_points);
        Self {
            ordering,
            samples: Arc::clone(&split_points),
            split_points,
        }
    }

    /// Creates range partitioning metadata and validates split point shape and
    /// ordering.
    ///
    /// The exact boundaries are also the retained samples. This allows scaling
    /// down and back up to the original count, but not beyond it. Prefer
    /// [`Self::try_new_with_samples`] when additional sample points are available.
    pub fn try_new(ordering: LexOrdering, split_points: Vec<SplitPoint>) -> Result<Self> {
        let partition_count = split_points.len() + 1;
        Self::try_new_with_samples(ordering, split_points, partition_count)
    }

    /// Creates sample-backed range partitioning and validates the sample shape,
    /// ordering, and target partition count.
    ///
    /// `partition_count` must be at least one and no larger than
    /// `samples.len() + 1`. When it is smaller than that maximum, the samples
    /// are evenly down-sampled to derive the effective split points.
    ///
    /// For a single range key:
    ///
    /// ```text
    /// ordering = [key ASC NULLS LAST]
    /// samples = [(10), (20), (30), (40), (50)]
    /// partition_count = 3
    /// split_points = [(20), (40)]
    ///
    /// partition 0: key before 20
    /// partition 1: key between 20 (inclusive) and 40 (exclusive)
    /// partition 2: key at/after 40
    ///
    /// scale(5): split_points = [(20), (30), (40), (50)]
    /// scale(7): None (at most 6 partitions)
    /// ```
    ///
    /// Retain at least `maximum_expected_partitions - 1` distinct samples to
    /// support that many partitions later. Small inputs may not have enough
    /// distinct values to do so.
    pub fn try_new_with_samples(
        ordering: LexOrdering,
        samples: Vec<SplitPoint>,
        partition_count: usize,
    ) -> Result<Self> {
        validate_range_split_points(
            &samples,
            &ordering
                .iter()
                .map(|sort_expr| sort_expr.options)
                .collect::<Vec<_>>(),
        )?;
        validate_range_partition_count(partition_count, samples.len() + 1)?;
        let samples: Arc<[SplitPoint]> = Arc::from(samples);
        let split_points = downsample_split_points(&samples, partition_count);
        Ok(Self {
            ordering,
            samples,
            split_points,
        })
    }

    /// Returns the ordering that defines the range key.
    pub fn ordering(&self) -> &LexOrdering {
        &self.ordering
    }

    /// Returns the maximum-resolution sample points.
    pub fn samples(&self) -> &[SplitPoint] {
        &self.samples
    }

    /// Returns the effective split points between partitions.
    pub fn split_points(&self) -> &[SplitPoint] {
        &self.split_points
    }

    /// Returns the number of partitions.
    pub fn partition_count(&self) -> usize {
        self.split_points.len() + 1
    }

    /// Returns the largest partition count supported by the stored samples.
    pub fn max_partition_count(&self) -> usize {
        self.samples.len() + 1
    }

    /// Whether two range partitionings have the same current key ordering and
    /// effective boundaries, irrespective of their retained samples.
    ///
    /// This does not imply equal scaling capacity. In particular, a plan that
    /// combines inputs must not use this comparison to inherit one input's samples
    /// for all inputs. Structural equality is required for that use case.
    pub fn has_same_layout(&self, other: &Self) -> bool {
        self.ordering == other.ordering && self.split_points == other.split_points
    }

    /// Returns this range partitioning scaled to `target_partitions`.
    ///
    /// Scaling retains the original samples, so a range partitioning that was
    /// scaled down can later be scaled back up to [`Self::max_partition_count`].
    /// Returns `None` when `target_partitions` is zero or the retained samples do
    /// not support that many partitions. Insufficient samples are an expected
    /// condition; callers should retain the layout or choose another partitioning.
    /// This method changes metadata only; the caller must ensure that the actual
    /// row distribution matches the resulting boundaries.
    pub fn scale(&self, target_partitions: usize) -> Option<Self> {
        if target_partitions == 0 || target_partitions > self.max_partition_count() {
            return None;
        }
        if target_partitions == self.partition_count() {
            return Some(self.clone());
        }
        Some(Self {
            ordering: self.ordering.clone(),
            samples: Arc::clone(&self.samples),
            split_points: downsample_split_points(&self.samples, target_partitions),
        })
    }

    /// Calculates the range partitioning after applying the given projection.
    ///
    /// Returns `None` if any range key cannot be projected or if projection
    /// collapses distinct range keys into duplicate output expressions.
    fn project(
        &self,
        mapping: &ProjectionMapping,
        input_eq_properties: &EquivalenceProperties,
    ) -> Option<Self> {
        let exprs = self
            .ordering
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect::<Vec<_>>();
        let projected_exprs = input_eq_properties
            .project_expressions(&exprs, mapping)
            .collect::<Option<Vec<_>>>()?;
        let sort_exprs = self
            .ordering
            .iter()
            .zip(projected_exprs)
            .map(|(sort_expr, expr)| PhysicalSortExpr::new(expr, sort_expr.options))
            .collect::<Vec<_>>();
        let ordering = LexOrdering::new(sort_exprs)?;
        if ordering.len() != self.ordering.len() {
            return None;
        }

        Some(Self {
            ordering,
            samples: Arc::clone(&self.samples),
            split_points: Arc::clone(&self.split_points),
        })
    }

    /// Checks whether the types of the given expressions match the data types of the split points in this range partitioning.
    pub fn is_compatible_with_expressions(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
        schema: &Schema,
    ) -> bool {
        if self.ordering.len() != exprs.len() {
            return false;
        }
        if let Some(first_split) = self.samples.first() {
            exprs.iter().zip(first_split.values()).all(|(expr, val)| {
                expr.data_type(schema)
                    .map(|dt| dt == val.data_type())
                    .unwrap_or(false)
            })
        } else {
            true
        }
    }

    /// Adapts this range partitioning to the given expressions, preserving split points and sort options.
    /// Returns `None` if `exprs` count doesn't match ordering length or expression types don't match split points.
    pub fn adapt(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
        schema: &Schema,
    ) -> Option<Self> {
        if !self.is_compatible_with_expressions(exprs, schema) {
            return None;
        }
        let new_ordering = LexOrdering::new(
            exprs
                .iter()
                .zip(&self.ordering)
                .map(|(expr, sort_expr)| PhysicalSortExpr {
                    expr: Arc::clone(expr),
                    options: sort_expr.options,
                })
                .collect::<Vec<_>>(),
        )?;
        if new_ordering.len() != self.ordering.len() {
            return None;
        }
        Some(Self {
            ordering: new_ordering,
            samples: Arc::clone(&self.samples),
            split_points: Arc::clone(&self.split_points),
        })
    }
}

impl Display for RangePartitioning {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let split_points = format_range_split_points(&self.split_points);
        write!(
            f,
            "Range([{}], [{}], {}",
            self.ordering,
            split_points,
            self.partition_count()
        )?;
        if self.max_partition_count() != self.partition_count() {
            write!(f, ", max {}", self.max_partition_count())?;
        }
        write!(f, ")")
    }
}

fn downsample_split_points(
    samples: &Arc<[SplitPoint]>,
    partition_count: usize,
) -> Arc<[SplitPoint]> {
    if partition_count == samples.len() + 1 {
        return Arc::clone(samples);
    }

    let sample_count = samples.len();
    (1..partition_count)
        .map(|partition| {
            // Use a wider intermediate so valid slice lengths cannot overflow
            // when calculating the evenly spaced sample index.
            let sample_index = ((partition as u128 * sample_count as u128)
                / partition_count as u128) as usize;
            samples[sample_index].clone()
        })
        .collect::<Vec<_>>()
        .into()
}

fn validate_range_partition_count(
    partition_count: usize,
    max_partition_count: usize,
) -> Result<()> {
    if partition_count == 0 {
        return plan_err!("Range partitioning partition count must be at least 1");
    }
    if partition_count > max_partition_count {
        return plan_err!(
            "Range partitioning partition count {partition_count} exceeds maximum {max_partition_count}"
        );
    }
    Ok(())
}

fn format_range_split_points(split_points: &[SplitPoint]) -> String {
    split_points
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn equivalent_exprs(
    left: &[Arc<dyn PhysicalExpr>],
    right: &[Arc<dyn PhysicalExpr>],
    eq_properties: &EquivalenceProperties,
) -> bool {
    if physical_exprs_equal(left, right) {
        return true;
    }

    let eq_groups = eq_properties.eq_group();
    if eq_groups.is_empty() {
        return false;
    }

    let normalized_left = normalize_exprs(left, eq_properties);
    let normalized_right = normalize_exprs(right, eq_properties);

    physical_exprs_equal(&normalized_left, &normalized_right)
}

fn normalize_exprs(
    exprs: &[Arc<dyn PhysicalExpr>],
    eq_properties: &EquivalenceProperties,
) -> Vec<Arc<dyn PhysicalExpr>> {
    let eq_groups = eq_properties.eq_group();
    exprs
        .iter()
        .map(|expr| eq_groups.normalize_expr(Arc::clone(expr)))
        .collect()
}

fn evaluate_with_range_value(
    expr: &Arc<dyn PhysicalExpr>,
    range_key: &Arc<dyn PhysicalExpr>,
    value: &ScalarValue,
) -> Option<ScalarValue> {
    let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(value.clone()));
    let rewritten = Arc::clone(expr)
        .transform(|node| {
            if node.eq(range_key) {
                Ok(Transformed::yes(Arc::clone(&literal)))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .ok()?;
    if !rewritten.transformed {
        return None;
    }

    match rewritten.data.evaluate(create_dummy_batch().ok()?).ok()? {
        ColumnarValue::Scalar(value) => Some(value),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0).ok(),
    }
}

fn checked_adjacent_timestamp(
    value: &ScalarValue,
    is_successor: bool,
) -> Option<ScalarValue> {
    let offset = if is_successor { 1 } else { -1 };
    match value {
        ScalarValue::TimestampSecond(Some(value), None) => value
            .checked_add(offset)
            .map(|value| ScalarValue::TimestampSecond(Some(value), None)),
        ScalarValue::TimestampMillisecond(Some(value), None) => value
            .checked_add(offset)
            .map(|value| ScalarValue::TimestampMillisecond(Some(value), None)),
        ScalarValue::TimestampMicrosecond(Some(value), None) => value
            .checked_add(offset)
            .map(|value| ScalarValue::TimestampMicrosecond(Some(value), None)),
        ScalarValue::TimestampNanosecond(Some(value), None) => value
            .checked_add(offset)
            .map(|value| ScalarValue::TimestampNanosecond(Some(value), None)),
        _ => None,
    }
}

/// Determines whether a nondecreasing transform keeps equal output keys local.
///
/// For one ascending range key, split points `x₁, ..., xₙ` define:
///
/// ```text
/// P₀ = (-∞, x₁)
/// Pᵢ = [xᵢ, xᵢ₊₁) for 1 ≤ i < n
/// Pₙ = [xₙ, +∞)
/// ```
///
/// Aggregation can reuse these partitions after applying `f` only if no output
/// key occurs in two partitions. In other words, the partition images must be
/// disjoint: `f(Pᵢ) ∩ f(Pⱼ) = ∅` for `i ≠ j`.
///
/// Monotonicity alone is insufficient. Ordered metadata gives only
///
/// ```text
/// f(predecessor(xᵢ)) ≤ f(xᵢ)
/// ```
///
/// but locality requires strict separation at every split:
///
/// ```text
/// f(predecessor(xᵢ)) < f(xᵢ)
/// ```
///
/// Since ordering already supplies the non-strict inequality, the analysis
/// proves strictness by explicitly checking that the two outputs are not equal.
/// Otherwise, the same transformed key could occur in both adjacent partitions,
/// so the transform would not preserve key locality. For example, an hourly
/// truncation maps timestamps immediately before and at a `01:30` split to the
/// same `01:00` bucket. A split at `01:00` is safe because its predecessor maps
/// to `00:00`, while the split itself maps to `01:00`.
///
/// `predecessor(xᵢ)` is the greatest representable timestamp below the split.
/// Therefore, for every `x < xᵢ` and `y ≥ xᵢ`, ordering and the strict boundary
/// check give
///
/// ```text
/// f(x) ≤ f(predecessor(xᵢ)) < f(xᵢ) ≤ f(y)
/// ```
///
/// Thus no transformed key can occur on both sides of the split. The successor of
/// an ascending split is already in the same partition as `xᵢ`, so checking it
/// would not say anything about groups crossing that split.
///
/// The argument is the same for descending ranges, but `successor(xᵢ)` must be
/// used instead. Null preservation is also required so the transform cannot
/// introduce a null group outside the source partition selected by null order.
fn range_splits_separate_transformed_keys(
    function: &Arc<dyn PhysicalExpr>,
    range_key: &Arc<dyn PhysicalExpr>,
    range: &RangePartitioning,
    schema: &Schema,
) -> bool {
    let Ok(range_type) = range_key.data_type(schema) else {
        return false;
    };
    let sort_options = range.ordering().first().options;
    if validate_range_split_points(range.split_points(), &[sort_options]).is_err() {
        return false;
    }
    range.split_points().iter().all(|split_point| {
        let Some(split_value) = split_point.values().first() else {
            return false;
        };
        if split_value.data_type() != range_type {
            return false;
        }
        let Some(adjacent) =
            checked_adjacent_timestamp(split_value, sort_options.descending)
        else {
            return false;
        };
        let Some(at_split) = evaluate_with_range_value(function, range_key, split_value)
        else {
            return false;
        };
        let Some(across_split) =
            evaluate_with_range_value(function, range_key, &adjacent)
        else {
            return false;
        };

        !at_split.is_null()
            && !across_split.is_null()
            && at_split.data_type() == across_split.data_type()
            && at_split != across_split
    })
}

fn range_transform_proves_key_locality(
    candidate: &Arc<dyn PhysicalExpr>,
    range_key: &Arc<dyn PhysicalExpr>,
    range: &RangePartitioning,
    eq_properties: &EquivalenceProperties,
) -> bool {
    let Some(function) = candidate.downcast_ref::<ScalarFunctionExpr>() else {
        return false;
    };
    let schema = eq_properties.schema();
    if datafusion_physical_expr_common::physical_expr::is_volatile(candidate)
        || !function
            .supports_range_key_locality_analysis(schema)
            .unwrap_or(false)
        || !eq_properties.is_same_direction_ordered_transform(candidate, range_key)
    {
        return false;
    }

    let mut range_argument_count = 0;
    for argument in function.args() {
        if eq_properties.eq_group().exprs_equal(argument, range_key) {
            range_argument_count += 1;
        } else if !argument.is::<Literal>() {
            return false;
        }
    }

    range_argument_count == 1
        && range_splits_separate_transformed_keys(candidate, range_key, range, schema)
}

fn range_partitioning_keeps_keys_local(
    range: &RangePartitioning,
    required_exprs: &[Arc<dyn PhysicalExpr>],
    eq_properties: &EquivalenceProperties,
    allow_subset: bool,
) -> bool {
    // Currently, transform locality analysis only supports single-key ranges.
    // Extending the proof to compound range keys is tracked in #24644.
    if range.ordering().len() != 1 || required_exprs.is_empty() {
        return false;
    }

    let range_key = &range.ordering()[0].expr;
    let has_key_locality_proof = required_exprs.iter().any(|required| {
        range_transform_proves_key_locality(required, range_key, range, eq_properties)
    });

    has_key_locality_proof && (required_exprs.len() == 1 || allow_subset)
}

/// Represents how a [`Partitioning`] satisfies a [`Distribution`] requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitioningSatisfaction {
    /// The partitioning does not satisfy the distribution requirement
    NotSatisfied,
    /// The partitioning exactly matches the distribution requirement
    Exact,
    /// The partitioning satisfies the distribution requirement via subset logic
    Subset,
}

impl PartitioningSatisfaction {
    pub fn is_satisfied(&self) -> bool {
        matches!(self, Self::Exact | Self::Subset)
    }

    pub fn is_subset(&self) -> bool {
        *self == Self::Subset
    }
}

impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            RoundRobinBatch(n) | Hash(_, n) | UnknownPartitioning(n) => *n,
            Range(range) => range.partition_count(),
        }
    }

    /// Returns true if `subset_exprs` is a subset of `exprs`.
    /// For example: Hash(a, b) is subset of Hash(a) since a partition with all occurrences of
    /// a distinct (a) must also contain all occurrences of a distinct (a, b) with the same (a).
    fn is_subset_partitioning(
        subset_exprs: &[Arc<dyn PhysicalExpr>],
        superset_exprs: &[Arc<dyn PhysicalExpr>],
    ) -> bool {
        // Require strict subset: fewer expressions, not equal
        if subset_exprs.is_empty() || subset_exprs.len() >= superset_exprs.len() {
            return false;
        }

        subset_exprs
            .iter()
            .all(|subset_expr| physical_exprs_contains(superset_exprs, subset_expr))
    }

    #[deprecated(since = "52.0.0", note = "Use satisfaction instead")]
    pub fn satisfy(
        &self,
        required: &Distribution,
        eq_properties: &EquivalenceProperties,
    ) -> bool {
        self.satisfaction(required, eq_properties, false)
            == PartitioningSatisfaction::Exact
    }

    /// Returns how this [`Partitioning`] satisfies the partitioning scheme mandated
    /// by the `required` [`Distribution`].
    #[expect(
        deprecated,
        reason = "HashPartitioned is accepted during the KeyPartitioned migration"
    )]
    pub fn satisfaction(
        &self,
        required: &Distribution,
        eq_properties: &EquivalenceProperties,
        allow_subset: bool,
    ) -> PartitioningSatisfaction {
        match required {
            Distribution::UnspecifiedDistribution => PartitioningSatisfaction::Exact,
            Distribution::SinglePartition if self.partition_count() == 1 => {
                PartitioningSatisfaction::Exact
            }
            // When partition count is 1, key partitioning is satisfied.
            Distribution::HashPartitioned(_) | Distribution::KeyPartitioned(_)
                if self.partition_count() == 1 =>
            {
                PartitioningSatisfaction::Exact
            }
            Distribution::HashPartitioned(required_exprs)
            | Distribution::KeyPartitioned(required_exprs) => match self {
                // Here we do not check the partition count for hash partitioning and assumes the partition count
                // and hash functions in the system are the same. In future if we plan to support storage partition-wise joins,
                // then we need to have the partition count and hash functions validation.
                Partitioning::Hash(partition_exprs, _) => Self::key_satisfaction(
                    partition_exprs,
                    required_exprs,
                    eq_properties,
                    allow_subset,
                ),
                Partitioning::Range(range) => {
                    let partition_exprs = range
                        .ordering()
                        .iter()
                        .map(|sort_expr| Arc::clone(&sort_expr.expr))
                        .collect::<Vec<_>>();
                    Self::key_satisfaction(
                        &partition_exprs,
                        required_exprs,
                        eq_properties,
                        allow_subset,
                    )
                }
                Partitioning::RoundRobinBatch(_)
                | Partitioning::UnknownPartitioning(_) => {
                    PartitioningSatisfaction::NotSatisfied
                }
            },
            Distribution::SinglePartition => PartitioningSatisfaction::NotSatisfied,
        }
    }

    /// Returns true when it can prove that equal tuples of `required_exprs`
    /// cannot occur in different partitions.
    ///
    /// Unlike [`Self::satisfaction`], this does not claim a concrete partition
    /// layout. This weaker guarantee is sufficient for independent keyed
    /// operations such as aggregation, but not for co-partitioning multiple
    /// inputs. Transform analysis is currently limited to single-key range
    /// partitioning.
    pub fn keeps_keys_local(
        &self,
        required_exprs: &[Arc<dyn PhysicalExpr>],
        eq_properties: &EquivalenceProperties,
        allow_subset: bool,
    ) -> bool {
        let required = Distribution::KeyPartitioned(required_exprs.to_vec());
        if self
            .satisfaction(&required, eq_properties, allow_subset)
            .is_satisfied()
        {
            return true;
        }

        match self {
            Self::Range(range) => range_partitioning_keeps_keys_local(
                range,
                required_exprs,
                eq_properties,
                allow_subset,
            ),
            Self::RoundRobinBatch(_)
            | Self::Hash(_, _)
            | Self::UnknownPartitioning(_) => false,
        }
    }

    fn key_satisfaction(
        partition_exprs: &[Arc<dyn PhysicalExpr>],
        required_exprs: &[Arc<dyn PhysicalExpr>],
        eq_properties: &EquivalenceProperties,
        allow_subset: bool,
    ) -> PartitioningSatisfaction {
        if partition_exprs.is_empty() || required_exprs.is_empty() {
            return PartitioningSatisfaction::NotSatisfied;
        }

        if equivalent_exprs(required_exprs, partition_exprs, eq_properties) {
            return PartitioningSatisfaction::Exact;
        }

        let eq_groups = eq_properties.eq_group();
        if !eq_groups.is_empty() {
            if allow_subset {
                let normalized_partition_exprs =
                    normalize_exprs(partition_exprs, eq_properties);
                let normalized_required_exprs =
                    normalize_exprs(required_exprs, eq_properties);
                if Self::is_subset_partitioning(
                    &normalized_partition_exprs,
                    &normalized_required_exprs,
                ) {
                    return PartitioningSatisfaction::Subset;
                }
            }
        } else if allow_subset
            && Self::is_subset_partitioning(partition_exprs, required_exprs)
        {
            return PartitioningSatisfaction::Subset;
        }

        PartitioningSatisfaction::NotSatisfied
    }

    /// Calculate the output partitioning after applying the given projection.
    pub fn project(
        &self,
        mapping: &ProjectionMapping,
        input_eq_properties: &EquivalenceProperties,
    ) -> Self {
        match self {
            Partitioning::Hash(exprs, part) => {
                let normalized_exprs = input_eq_properties
                    .project_expressions(exprs, mapping)
                    .zip(exprs)
                    .map(|(proj_expr, expr)| {
                        proj_expr.unwrap_or_else(|| {
                            Arc::new(UnKnownColumn::new(&expr.to_string()))
                        })
                    })
                    .collect();
                Partitioning::Hash(normalized_exprs, *part)
            }
            Partitioning::Range(range) => {
                if let Some(projected) = range.project(mapping, input_eq_properties) {
                    Partitioning::Range(projected)
                } else {
                    Partitioning::UnknownPartitioning(range.partition_count())
                }
            }
            Partitioning::RoundRobinBatch(_) | Partitioning::UnknownPartitioning(_) => {
                self.clone()
            }
        }
    }

    /// Adapts this partitioning scheme to satisfy a required [`Distribution`] on the given schema.
    ///
    /// - For `Partitioning::Hash`: creates `Partitioning::Hash(exprs, partition_count)`.
    /// - For `Partitioning::Range`: adapts the range partitioning to the requirement's expressions using [`RangePartitioning::adapt`].
    /// - For other partitioning schemes: returns `None`.
    #[expect(
        deprecated,
        reason = "HashPartitioned is accepted during the KeyPartitioned migration"
    )]
    pub fn adapt(
        &self,
        child_requirement: &Distribution,
        child_schema: &Schema,
    ) -> Option<Self> {
        let (Distribution::HashPartitioned(exprs) | Distribution::KeyPartitioned(exprs)) =
            child_requirement
        else {
            return None;
        };

        match self {
            Partitioning::Range(ref_range) => ref_range
                .adapt(exprs, child_schema)
                .map(Partitioning::Range),
            Partitioning::Hash(_, ref_count) => {
                Some(Partitioning::Hash(exprs.to_vec(), *ref_count))
            }
            _ => None,
        }
    }
}

/// Protobuf conversions for [`Partitioning`].
///
/// Child expressions (hash keys, range orderings) and `ScalarValue` split
/// points are (de)serialized through the expression-level context, so this is
/// the single copy of the partitioning wire format: `RepartitionExec` and
/// `datafusion-proto`'s central serializer route through it, and the remaining
/// per-plan migrations (`FileScanConfig` and friends) are meant to do the same
/// rather than grow another copy.
///
/// [`protobuf::Partitioning`]: datafusion_proto_models::protobuf::Partitioning
#[cfg(feature = "proto")]
impl Partitioning {
    /// Serialize this partitioning into its protobuf representation.
    pub fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<datafusion_proto_models::protobuf::Partitioning> {
        use datafusion_common::utils::usize_to_wire;
        use datafusion_proto_models::protobuf;

        let partition_count =
            |count: usize| usize_to_wire::<u64>(count, "Partitioning", "partition_count");
        let partition_method = match self {
            Partitioning::RoundRobinBatch(n) => {
                protobuf::partitioning::PartitionMethod::RoundRobin(partition_count(*n)?)
            }
            Partitioning::Hash(exprs, n) => {
                protobuf::partitioning::PartitionMethod::Hash(
                    protobuf::PhysicalHashRepartition {
                        hash_expr: ctx.encode_children_expressions(exprs)?,
                        partition_count: partition_count(*n)?,
                    },
                )
            }
            Partitioning::Range(range) => {
                let sort_expr = sort_exprs_try_to_proto(range.ordering().iter(), ctx)?;
                let encode_split_points = |split_points: &[SplitPoint]| {
                    split_points
                        .iter()
                        .map(|split_point| {
                            let value = split_point
                                .values()
                                .iter()
                                .map(|value| value.try_into().map_err(Into::into))
                                .collect::<Result<Vec<_>>>()?;
                            Ok(protobuf::PhysicalRangeSplitPoint { value })
                        })
                        .collect::<Result<Vec<_>>>()
                };
                let split_point = encode_split_points(range.split_points())?;
                let sample_point = encode_split_points(range.samples())?;
                protobuf::partitioning::PartitionMethod::Range(
                    protobuf::PhysicalRangePartitioning {
                        sort_expr,
                        split_point,
                        sample_point,
                        partition_count: partition_count(range.partition_count())?,
                    },
                )
            }
            Partitioning::UnknownPartitioning(n) => {
                protobuf::partitioning::PartitionMethod::Unknown(partition_count(*n)?)
            }
        };
        Ok(protobuf::Partitioning {
            partition_method: Some(partition_method),
        })
    }

    /// Reconstruct a [`Partitioning`] from its protobuf representation.
    ///
    /// Returns `Ok(None)` when the message carries no `partition_method`, which
    /// the wire format uses to mean "no output partitioning declared"; callers
    /// for which it is required should turn that into their own error.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::Partitioning,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Option<Self>> {
        use datafusion_common::utils::usize_from_wire;
        use datafusion_common::{ScalarValue, internal_datafusion_err, internal_err};
        use datafusion_proto_models::protobuf;

        let partition_count =
            |count: u64| usize_from_wire(count, "Partitioning", "partition_count");
        let protobuf::Partitioning { partition_method } = node;
        let Some(partition_method) = partition_method.as_ref() else {
            return Ok(None);
        };
        let partitioning = match partition_method {
            protobuf::partitioning::PartitionMethod::RoundRobin(n) => {
                Partitioning::RoundRobinBatch(partition_count(*n)?)
            }
            protobuf::partitioning::PartitionMethod::Hash(hash) => {
                let protobuf::PhysicalHashRepartition {
                    hash_expr,
                    partition_count: hash_partition_count,
                } = hash;
                let exprs = hash_expr
                    .iter()
                    .map(|expr| ctx.decode(expr))
                    .collect::<Result<Vec<_>>>()?;
                Partitioning::Hash(exprs, partition_count(*hash_partition_count)?)
            }
            protobuf::partitioning::PartitionMethod::Unknown(n) => {
                Partitioning::UnknownPartitioning(partition_count(*n)?)
            }
            protobuf::partitioning::PartitionMethod::Range(range) => {
                let sort_exprs = sort_exprs_try_from_proto(&range.sort_expr, ctx)?;
                let sort_expr_count = sort_exprs.len();
                let ordering = LexOrdering::new(sort_exprs).ok_or_else(|| {
                    internal_datafusion_err!(
                        "Range partitioning requires non-empty ordering"
                    )
                })?;
                if ordering.len() != sort_expr_count {
                    return internal_err!(
                        "Range partitioning ordering must not contain duplicate expressions"
                    );
                }
                let decode_split_points =
                    |split_points: &[protobuf::PhysicalRangeSplitPoint]| {
                        split_points
                            .iter()
                            .map(|split_point| {
                                let values = split_point
                                    .value
                                    .iter()
                                    .map(|value| {
                                        ScalarValue::try_from(value).map_err(Into::into)
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                Ok(SplitPoint::new(values))
                            })
                            .collect::<Result<Vec<_>>>()
                    };
                let split_points = decode_split_points(&range.split_point)?;
                if range.partition_count == 0 {
                    // Older payloads derive their partition count from the exact
                    // split points and do not carry this field.
                    Partitioning::Range(RangePartitioning::try_new(
                        ordering,
                        split_points,
                    )?)
                } else {
                    let samples = decode_split_points(&range.sample_point)?;
                    let range_partitioning = RangePartitioning::try_new_with_samples(
                        ordering,
                        samples,
                        partition_count(range.partition_count)?,
                    )?;
                    if range_partitioning.split_points() != split_points {
                        return internal_err!(
                            "Range partitioning effective split points do not match its samples and partition count"
                        );
                    }
                    Partitioning::Range(range_partitioning)
                }
            }
        };
        Ok(Some(partitioning))
    }
}

impl PartialEq for Partitioning {
    fn eq(&self, other: &Partitioning) -> bool {
        match (self, other) {
            (
                Partitioning::RoundRobinBatch(count1),
                Partitioning::RoundRobinBatch(count2),
            ) if count1 == count2 => true,
            (Partitioning::Hash(exprs1, count1), Partitioning::Hash(exprs2, count2))
                if physical_exprs_equal(exprs1, exprs2) && (count1 == count2) =>
            {
                true
            }
            (Partitioning::Range(left), Partitioning::Range(right)) => left == right,
            _ => false,
        }
    }
}

/// How data is distributed amongst partitions. See [`Partitioning`] for more
/// details.
#[derive(Debug, Clone)]
pub enum Distribution {
    /// Unspecified distribution
    UnspecifiedDistribution,
    /// A single partition is required
    SinglePartition,
    /// Deprecated historical name for [`Distribution::KeyPartitioned`].
    /// See <https://github.com/apache/datafusion/issues/23236> for details.
    #[deprecated(since = "55.0.0", note = "Use Distribution::KeyPartitioned")]
    HashPartitioned(Vec<Arc<dyn PhysicalExpr>>),
    /// Requires children to be distributed in such a way that the same
    /// values of the keys end up in the same partition
    KeyPartitioned(Vec<Arc<dyn PhysicalExpr>>),
}

#[expect(
    deprecated,
    reason = "HashPartitioned is accepted during the KeyPartitioned migration"
)]
impl Distribution {
    /// Creates a `Partitioning` that satisfies this `Distribution`
    pub fn create_partitioning(self, partition_count: usize) -> Partitioning {
        match self {
            Distribution::UnspecifiedDistribution => {
                Partitioning::UnknownPartitioning(partition_count)
            }
            Distribution::SinglePartition => Partitioning::UnknownPartitioning(1),
            Distribution::HashPartitioned(expr) | Distribution::KeyPartitioned(expr) => {
                Partitioning::Hash(expr, partition_count)
            }
        }
    }
}

#[expect(
    deprecated,
    reason = "HashPartitioned display is preserved during the KeyPartitioned migration"
)]
impl Display for Distribution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Distribution::UnspecifiedDistribution => write!(f, "Unspecified"),
            Distribution::SinglePartition => write!(f, "SinglePartition"),
            Distribution::HashPartitioned(exprs) => {
                write!(f, "HashPartitioned[{}])", format_physical_expr_list(exprs))
            }
            Distribution::KeyPartitioned(exprs) => {
                write!(f, "KeyPartitioned[{}])", format_physical_expr_list(exprs))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::expressions::Column;
    use crate::projection::ProjectionTargets;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue};

    struct PartitioningTestFixture {
        schema: SchemaRef,
        cols: Vec<Arc<dyn PhysicalExpr>>,
        eq_properties: EquivalenceProperties,
    }

    impl PartitioningTestFixture {
        fn new(fields: Vec<(&str, DataType)>) -> Result<Self> {
            let schema = Arc::new(Schema::new(
                fields
                    .iter()
                    .map(|(name, data_type)| Field::new(*name, data_type.clone(), false))
                    .collect::<Vec<_>>(),
            ));
            let cols = fields
                .iter()
                .map(|(name, _)| {
                    Ok(Arc::new(Column::new_with_schema(name, &schema)?)
                        as Arc<dyn PhysicalExpr>)
                })
                .collect::<Result<_>>()?;
            let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

            Ok(Self {
                schema,
                cols,
                eq_properties,
            })
        }

        fn int64(names: &[&str]) -> Result<Self> {
            Self::new(names.iter().map(|name| (*name, DataType::Int64)).collect())
        }

        fn col(&self, index: usize) -> Arc<dyn PhysicalExpr> {
            Arc::clone(&self.cols[index])
        }

        fn cols(
            &self,
            indices: impl IntoIterator<Item = usize>,
        ) -> Vec<Arc<dyn PhysicalExpr>> {
            indices.into_iter().map(|index| self.col(index)).collect()
        }

        fn hash_partitioning(
            &self,
            indices: impl IntoIterator<Item = usize>,
            partition_count: usize,
        ) -> Partitioning {
            Partitioning::Hash(self.cols(indices), partition_count)
        }

        fn key_distribution(
            &self,
            indices: impl IntoIterator<Item = usize>,
        ) -> Distribution {
            Distribution::KeyPartitioned(self.cols(indices))
        }

        fn range_sort_expr(
            &self,
            index: usize,
            options: SortOptions,
        ) -> PhysicalSortExpr {
            PhysicalSortExpr::new(self.col(index), options)
        }

        fn range_ordering(
            &self,
            indices: impl IntoIterator<Item = usize>,
        ) -> LexOrdering {
            LexOrdering::new(
                indices
                    .into_iter()
                    .map(|index| PhysicalSortExpr::new_default(self.col(index))),
            )
            .expect("ordering must not be empty")
        }

        fn range(
            &self,
            indices: impl IntoIterator<Item = usize>,
            split_points: Vec<SplitPoint>,
        ) -> RangePartitioning {
            RangePartitioning::try_new(self.range_ordering(indices), split_points)
                .expect("test range partitioning should be valid")
        }

        fn range_partitioning(
            &self,
            indices: impl IntoIterator<Item = usize>,
            split_points: Vec<SplitPoint>,
        ) -> Partitioning {
            Partitioning::Range(self.range(indices, split_points))
        }

        fn range_partitioning_with_ordering(
            &self,
            ordering: LexOrdering,
            split_points: Vec<SplitPoint>,
        ) -> Partitioning {
            Partitioning::Range(
                RangePartitioning::try_new(ordering, split_points)
                    .expect("test range partitioning should be valid"),
            )
        }
    }

    fn assert_satisfaction(
        desc: &str,
        partitioning: &Partitioning,
        required: &Distribution,
        eq_properties: &EquivalenceProperties,
        expected_with_subset: PartitioningSatisfaction,
        expected_without_subset: PartitioningSatisfaction,
    ) {
        assert_eq!(
            partitioning.satisfaction(required, eq_properties, true),
            expected_with_subset,
            "Failed for {desc} with subset enabled"
        );
        assert_eq!(
            partitioning.satisfaction(required, eq_properties, false),
            expected_without_subset,
            "Failed for {desc} with subset disabled"
        );
    }

    fn assert_key_locality(
        desc: &str,
        partitioning: &Partitioning,
        required_exprs: &[Arc<dyn PhysicalExpr>],
        eq_properties: &EquivalenceProperties,
        expected_with_subset: bool,
        expected_without_subset: bool,
    ) {
        assert_eq!(
            partitioning.keeps_keys_local(required_exprs, eq_properties, true),
            expected_with_subset,
            "Failed for {desc} with subset enabled"
        );
        assert_eq!(
            partitioning.keeps_keys_local(required_exprs, eq_properties, false),
            expected_without_subset,
            "Failed for {desc} with subset disabled"
        );
    }

    fn date_trunc_of(
        timestamp: Arc<dyn PhysicalExpr>,
        precision: &str,
        timezone: Option<&str>,
    ) -> Arc<dyn PhysicalExpr> {
        let timezone = timezone.map(Into::into);
        Arc::new(ScalarFunctionExpr::new(
            "date_trunc",
            datafusion_functions::datetime::date_trunc(),
            vec![
                Arc::new(Literal::new(ScalarValue::Utf8(Some(precision.into())))),
                timestamp,
            ],
            Field::new(
                "date_trunc",
                DataType::Timestamp(TimeUnit::Nanosecond, timezone),
                true,
            )
            .into(),
            Arc::new(ConfigOptions::default()),
        ))
    }

    fn timestamp_split(value: Option<i64>, timezone: Option<&str>) -> SplitPoint {
        SplitPoint::new(vec![ScalarValue::TimestampNanosecond(
            value,
            timezone.map(Into::into),
        )])
    }

    /// Assigns sample rows to source ranges and evaluates whole arrays,
    /// independently of the optimizer's boundary-adjacency proof.
    fn date_trunc_partition_images_are_disjoint<
        T: arrow::array::types::ArrowTimestampType,
    >(
        function: &Arc<dyn PhysicalExpr>,
        splits: &[i64],
        options: SortOptions,
        schema: &SchemaRef,
    ) -> Result<bool> {
        use arrow::array::{PrimitiveArray, RecordBatch};
        use datafusion_common::HashSet;

        let mut partitions = vec![vec![]; splits.len() + 1];
        let rows = splits
            .iter()
            .flat_map(|split| [Some(split - 1), Some(*split), Some(split + 1)])
            .chain([None, None]);
        for value in rows {
            let index = match value {
                None if options.nulls_first => 0,
                None => splits.len(),
                Some(value) => splits
                    .iter()
                    .filter(|split| {
                        if options.descending {
                            value <= **split
                        } else {
                            value >= **split
                        }
                    })
                    .count(),
            };
            partitions[index].push(value);
        }
        let images = partitions
            .into_iter()
            .map(|values| {
                let input = Arc::new(PrimitiveArray::<T>::from_iter(values));
                let batch = RecordBatch::try_new(Arc::clone(schema), vec![input])?;
                let output = function.evaluate(&batch)?.into_array(batch.num_rows())?;
                (0..output.len())
                    .map(|i| ScalarValue::try_from_array(&output, i))
                    .collect::<Result<HashSet<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(images.iter().enumerate().all(|(i, left)| {
            images
                .iter()
                .skip(i + 1)
                .all(|right| left.is_disjoint(right))
        }))
    }

    fn assert_date_trunc_partition_images<T: arrow::array::types::ArrowTimestampType>()
    -> Result<()> {
        let ticks_per_second = match T::UNIT {
            TimeUnit::Second => 1,
            TimeUnit::Millisecond => 1_000,
            TimeUnit::Microsecond => 1_000_000,
            TimeUnit::Nanosecond => 1_000_000_000,
        };
        let hour = 3600 * ticks_per_second;
        let data_type = DataType::Timestamp(T::UNIT, None);
        let schema =
            Arc::new(Schema::new(vec![Field::new("ts", data_type.clone(), true)]));
        let timestamp: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));
        let properties = EquivalenceProperties::new(Arc::clone(&schema));

        let mut cases = vec![
            (
                "aligned ascending hours",
                "hour",
                false,
                vec![-hour, 0, hour],
                true,
            ),
            (
                "unaligned ascending hours",
                "hour",
                false,
                vec![-hour + 1, 1, hour + 1],
                false,
            ),
            (
                "days cross hourly splits",
                "day",
                false,
                vec![-hour, 0, hour],
                false,
            ),
            (
                "descending splits inside hour buckets",
                "hour",
                true,
                vec![-hour, 0, hour],
                false,
            ),
            (
                "descending splits between hour buckets",
                "hour",
                true,
                vec![-hour - 1, -1, hour - 1],
                true,
            ),
        ];
        for (precision, divisor) in [("millisecond", 1_000), ("microsecond", 1_000_000)] {
            let width = (ticks_per_second / divisor).max(1);
            cases.extend([
                (
                    "aligned ascending subsecond splits",
                    precision,
                    false,
                    vec![-width, 0, width],
                    true,
                ),
                // In particular, microsecond input with millisecond precision
                // must reject split -999: both adjacent array values map to -1000.
                (
                    "offset ascending subsecond split",
                    precision,
                    false,
                    vec![-width + 1],
                    width == 1,
                ),
                (
                    "descending splits at subsecond bucket starts",
                    precision,
                    true,
                    vec![-width, 0, width],
                    width == 1,
                ),
                (
                    "descending splits at subsecond bucket ends",
                    precision,
                    true,
                    vec![-width - 1, -1, width - 1],
                    true,
                ),
            ]);
        }
        for (description, precision, descending, source_splits, expect_disjoint) in cases
        {
            for nulls_first in [false, true] {
                let options = SortOptions::new(descending, nulls_first);
                let mut splits = source_splits.clone();
                if descending {
                    splits.reverse();
                }
                let range = Partitioning::Range(RangePartitioning::try_new(
                    [PhysicalSortExpr::new(Arc::clone(&timestamp), options)].into(),
                    splits
                        .iter()
                        .map(|value| {
                            SplitPoint::new(vec![ScalarValue::new_timestamp::<T>(
                                Some(*value),
                                None,
                            )])
                        })
                        .collect(),
                )?);
                let function: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
                    "date_trunc",
                    datafusion_functions::datetime::date_trunc(),
                    vec![
                        Arc::new(Literal::new(ScalarValue::from(precision))),
                        Arc::clone(&timestamp),
                    ],
                    Field::new("bucket", data_type.clone(), true).into(),
                    Arc::new(ConfigOptions::default()),
                ));
                assert_eq!(
                    range.keeps_keys_local(&[Arc::clone(&function)], &properties, false),
                    expect_disjoint,
                    "optimizer: {description}, {precision}, {:?}, {options:?}, splits={splits:?}",
                    T::UNIT
                );
                assert_eq!(
                    date_trunc_partition_images_are_disjoint::<T>(
                        &function, &splits, options, &schema,
                    )?,
                    expect_disjoint,
                    "array evaluation: {description}, {precision}, {:?}, {options:?}, splits={splits:?}",
                    T::UNIT
                );
            }
        }
        Ok(())
    }

    #[test]
    fn date_trunc_partition_images_are_disjoint_only_for_safe_splits() -> Result<()> {
        use arrow::array::types::{
            TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
            TimestampSecondType,
        };
        assert_date_trunc_partition_images::<TimestampSecondType>()?;
        assert_date_trunc_partition_images::<TimestampMillisecondType>()?;
        assert_date_trunc_partition_images::<TimestampMicrosecondType>()?;
        assert_date_trunc_partition_images::<TimestampNanosecondType>()
    }

    #[test]
    fn range_transform_requires_literal_parameters() -> Result<()> {
        use crate::expressions::CastExpr;
        let fixture = PartitioningTestFixture::new(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )])?;
        let function = date_trunc_of(fixture.col(0), "hour", None);
        let scalar_but_not_literal: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
            Arc::new(Literal::new(ScalarValue::from("hour"))),
            DataType::Utf8View,
            None,
        ));
        let function =
            function.with_new_children(vec![scalar_but_not_literal, fixture.col(0)])?;
        let range = fixture.range_partitioning([0], vec![timestamp_split(Some(0), None)]);
        assert!(!range.keeps_keys_local(&[function], &fixture.eq_properties, false));
        Ok(())
    }

    #[test]
    fn range_transform_establishes_key_locality_only() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ("tag", DataType::Utf8),
        ])?;
        let hour = 1_704_070_800_000_000_000;
        let aligned =
            fixture.range_partitioning([0], vec![timestamp_split(Some(hour), None)]);
        let trunc_hour = date_trunc_of(fixture.col(0), "hour", None);

        let exact_key_exprs = vec![Arc::clone(&trunc_hour)];
        assert_eq!(
            aligned.satisfaction(
                &Distribution::KeyPartitioned(exact_key_exprs.clone()),
                &fixture.eq_properties,
                false,
            ),
            PartitioningSatisfaction::NotSatisfied
        );
        assert_key_locality(
            "aligned transformed key",
            &aligned,
            &exact_key_exprs,
            &fixture.eq_properties,
            true,
            true,
        );

        assert_key_locality(
            "additional grouping keys require subset permission",
            &aligned,
            &[trunc_hour, fixture.col(1)],
            &fixture.eq_properties,
            true,
            false,
        );

        let trunc_day = date_trunc_of(fixture.col(0), "day", None);
        assert_key_locality(
            "an hour split straddles a day bucket",
            &aligned,
            &[trunc_day],
            &fixture.eq_properties,
            false,
            false,
        );

        let two_aligned = fixture.range_partitioning(
            [0],
            vec![
                timestamp_split(Some(hour), None),
                timestamp_split(Some(hour + 3_600_000_000_000), None),
            ],
        );
        assert_key_locality(
            "every split must be aligned",
            &two_aligned,
            &exact_key_exprs,
            &fixture.eq_properties,
            true,
            true,
        );

        let unaligned =
            fixture.range_partitioning([0], vec![timestamp_split(Some(hour + 1), None)]);
        assert_key_locality(
            "an unaligned split straddles an hour bucket",
            &unaligned,
            &exact_key_exprs,
            &fixture.eq_properties,
            false,
            false,
        );
        Ok(())
    }

    #[test]
    fn range_transform_rejects_compound_range_keys() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("key", DataType::Utf8),
            ("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None)),
        ])?;
        let split = SplitPoint::new(vec![
            ScalarValue::Utf8(Some("m".into())),
            ScalarValue::TimestampNanosecond(Some(1_704_070_800_000_000_000), None),
        ]);
        let range = fixture.range_partitioning([0, 1], vec![split]);

        // Start with single-key ranges; compound transforms can follow once
        // tuple-boundary safety is established.
        assert!(!range.keeps_keys_local(
            &[fixture.col(0), date_trunc_of(fixture.col(1), "hour", None),],
            &fixture.eq_properties,
            false,
        ));
        Ok(())
    }

    #[test]
    #[expect(
        deprecated,
        reason = "test intentionally constructs malformed range splits"
    )]
    fn range_transform_rejects_malformed_split_points() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )])?;
        let required = vec![date_trunc_of(fixture.col(0), "hour", None)];
        let hour = 3_600_000_000_000;
        let timestamp = |value| ScalarValue::TimestampNanosecond(Some(value), None);

        for (description, descending, splits) in [
            (
                "reversed ascending splits",
                false,
                vec![vec![timestamp(2 * hour)], vec![timestamp(hour)]],
            ),
            (
                "reversed descending splits",
                true,
                vec![vec![timestamp(hour - 1)], vec![timestamp(2 * hour - 1)]],
            ),
            (
                "duplicate ascending splits",
                false,
                vec![vec![timestamp(hour)], vec![timestamp(hour)]],
            ),
            (
                "duplicate descending splits",
                true,
                vec![vec![timestamp(hour - 1)], vec![timestamp(hour - 1)]],
            ),
            (
                "overwide split tuple",
                false,
                vec![vec![timestamp(hour), ScalarValue::Int64(Some(0))]],
            ),
            ("empty split tuple", false, vec![vec![]]),
        ] {
            let range = Partitioning::Range(RangePartitioning::new(
                [PhysicalSortExpr::new(
                    fixture.col(0),
                    SortOptions::new(descending, false),
                )]
                .into(),
                splits.into_iter().map(SplitPoint::new).collect(),
            ));
            assert_key_locality(
                description,
                &range,
                &required,
                &fixture.eq_properties,
                false,
                false,
            );
        }
        Ok(())
    }

    #[test]
    fn range_transform_rejects_null_or_minimum_timestamp_splits() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )])?;
        let required = vec![date_trunc_of(fixture.col(0), "hour", None)];

        for (description, split) in [
            ("null", timestamp_split(None, None)),
            (
                "minimum has no predecessor",
                timestamp_split(Some(i64::MIN), None),
            ),
        ] {
            assert_key_locality(
                description,
                &fixture.range_partitioning([0], vec![split]),
                &required,
                &fixture.eq_properties,
                false,
                false,
            );
        }
        Ok(())
    }

    #[test]
    #[expect(
        deprecated,
        reason = "test intentionally constructs a malformed range split"
    )]
    fn range_transform_rejects_split_type_mismatch() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )])?;
        let required = vec![date_trunc_of(fixture.col(0), "hour", None)];
        let mismatched_split = Partitioning::Range(RangePartitioning::new(
            fixture.range_ordering([0]),
            vec![SplitPoint::new(vec![ScalarValue::Int64(Some(10))])],
        ));
        assert_key_locality(
            "split scalar type must match the range key",
            &mismatched_split,
            &required,
            &fixture.eq_properties,
            false,
            false,
        );
        Ok(())
    }

    #[test]
    fn range_transform_rejects_boundary_evaluation_errors() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )])?;
        let invalid_precision =
            vec![date_trunc_of(fixture.col(0), "not-a-precision", None)];
        let aligned = fixture.range_partitioning(
            [0],
            vec![timestamp_split(Some(1_704_070_800_000_000_000), None)],
        );
        assert_key_locality(
            "split evaluation errors fail closed",
            &aligned,
            &invalid_precision,
            &fixture.eq_properties,
            false,
            false,
        );
        Ok(())
    }

    #[test]
    fn range_transform_rejects_timezone_annotated_timestamps() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        )])?;
        let trunc_hour = date_trunc_of(fixture.col(0), "hour", Some("UTC"));
        let range = fixture.range_partitioning(
            [0],
            vec![timestamp_split(
                Some(1_704_070_800_000_000_000),
                Some("UTC"),
            )],
        );
        assert_key_locality(
            "timezone-aware date_trunc is not audited",
            &range,
            &[trunc_hour],
            &fixture.eq_properties,
            false,
            false,
        );
        Ok(())
    }

    #[test]
    fn descending_range_uses_the_open_side_successor() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )])?;
        let hour = 1_704_070_800_000_000_000;
        let ordering: LexOrdering =
            [fixture.range_sort_expr(0, SortOptions::new(true, false))].into();
        let required = vec![date_trunc_of(fixture.col(0), "hour", None)];

        assert_key_locality(
            "the successor of an hour boundary is in the same bucket",
            &fixture.range_partitioning_with_ordering(
                ordering.clone(),
                vec![timestamp_split(Some(hour), None)],
            ),
            &required,
            &fixture.eq_properties,
            false,
            false,
        );
        assert_key_locality(
            "the successor of the final nanosecond starts a new bucket",
            &fixture.range_partitioning_with_ordering(
                ordering.clone(),
                vec![timestamp_split(Some(hour + 3_600_000_000_000 - 1), None)],
            ),
            &required,
            &fixture.eq_properties,
            true,
            true,
        );
        assert_key_locality(
            "the maximum timestamp has no successor",
            &fixture.range_partitioning_with_ordering(
                ordering,
                vec![timestamp_split(Some(i64::MAX), None)],
            ),
            &required,
            &fixture.eq_properties,
            false,
            false,
        );
        Ok(())
    }

    #[test]
    #[expect(
        deprecated,
        reason = "test intentionally covers deprecated HashPartitioned compatibility"
    )]
    fn partitioning_satisfy_distribution() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("column_1", DataType::Int64),
            ("column_2", DataType::Utf8),
        ])?;

        let distribution_types = vec![
            Distribution::UnspecifiedDistribution,
            Distribution::SinglePartition,
            Distribution::HashPartitioned(fixture.cols([0, 1])),
            fixture.key_distribution([0, 1]),
        ];

        let single_partition = Partitioning::UnknownPartitioning(1);
        let unspecified_partition = Partitioning::UnknownPartitioning(10);
        let round_robin_partition = Partitioning::RoundRobinBatch(10);
        let hash_partition1 = fixture.hash_partitioning([0, 1], 10);
        let hash_partition2 = fixture.hash_partitioning([1, 0], 10);

        for distribution in distribution_types {
            let result = (
                single_partition
                    .satisfaction(&distribution, &fixture.eq_properties, true)
                    .is_satisfied(),
                unspecified_partition
                    .satisfaction(&distribution, &fixture.eq_properties, true)
                    .is_satisfied(),
                round_robin_partition
                    .satisfaction(&distribution, &fixture.eq_properties, true)
                    .is_satisfied(),
                hash_partition1
                    .satisfaction(&distribution, &fixture.eq_properties, true)
                    .is_satisfied(),
                hash_partition2
                    .satisfaction(&distribution, &fixture.eq_properties, true)
                    .is_satisfied(),
            );

            match distribution {
                Distribution::UnspecifiedDistribution => {
                    assert_eq!(result, (true, true, true, true, true))
                }
                Distribution::SinglePartition => {
                    assert_eq!(result, (true, false, false, false, false))
                }
                Distribution::HashPartitioned(_) | Distribution::KeyPartitioned(_) => {
                    assert_eq!(result, (true, false, false, true, false))
                }
            }
        }

        Ok(())
    }

    #[test]
    #[expect(
        deprecated,
        reason = "test intentionally covers deprecated HashPartitioned compatibility"
    )]
    fn deprecated_hash_partitioned_matches_key_partitioned() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let partitioning = fixture.hash_partitioning([0, 1], 4);
        let hash_distribution = Distribution::HashPartitioned(fixture.cols([0, 1]));
        let key_distribution = fixture.key_distribution([0, 1]);

        assert_eq!(
            partitioning.satisfaction(&hash_distribution, &fixture.eq_properties, false),
            partitioning.satisfaction(&key_distribution, &fixture.eq_properties, false)
        );
        assert_eq!(
            hash_distribution.create_partitioning(4),
            key_distribution.create_partitioning(4)
        );

        Ok(())
    }

    #[test]
    fn hash_partitioning_key_distribution_satisfaction() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b", "c"])?;
        let unknown: Arc<dyn PhysicalExpr> = Arc::new(UnKnownColumn::new("dropped"));

        let test_cases = vec![
            (
                "exact: KeyPartitioned([a, b]) satisfied by Hash([a, b])",
                fixture.hash_partitioning([0, 1], 4),
                fixture.key_distribution([0, 1]),
                PartitioningSatisfaction::Exact,
                PartitioningSatisfaction::Exact,
            ),
            (
                "subset: KeyPartitioned([a, b]) satisfied by Hash([a])",
                fixture.hash_partitioning([0], 4),
                fixture.key_distribution([0, 1]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "subset: KeyPartitioned([a, b, c]) satisfied by Hash([b])",
                fixture.hash_partitioning([1], 4),
                fixture.key_distribution([0, 1, 2]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "subset reordered: KeyPartitioned([a, b, c]) satisfied by Hash([b, a])",
                fixture.hash_partitioning([1, 0], 4),
                fixture.key_distribution([0, 1, 2]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "superset: KeyPartitioned([a]) not satisfied by Hash([a, b])",
                fixture.hash_partitioning([0, 1], 4),
                fixture.key_distribution([0]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "superset: KeyPartitioned([a, b]) not satisfied by Hash([a, b, c])",
                fixture.hash_partitioning([0, 1, 2], 4),
                fixture.key_distribution([0, 1]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "partial overlap: KeyPartitioned([a, b]) not satisfied by Hash([a, c])",
                fixture.hash_partitioning([0, 2], 4),
                fixture.key_distribution([0, 1]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "no overlap: KeyPartitioned([b, c]) not satisfied by Hash([a])",
                fixture.hash_partitioning([0], 4),
                fixture.key_distribution([1, 2]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "unknown partition expr",
                Partitioning::Hash(vec![Arc::clone(&unknown)], 4),
                fixture.key_distribution([0, 1]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "unknown required expr",
                fixture.hash_partitioning([0, 1], 4),
                Distribution::KeyPartitioned(vec![Arc::clone(&unknown)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "same unknown expr",
                Partitioning::Hash(vec![Arc::clone(&unknown)], 4),
                Distribution::KeyPartitioned(vec![Arc::clone(&unknown)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "unknown partition expr is not a valid subset",
                Partitioning::Hash(vec![Arc::clone(&unknown)], 4),
                Distribution::KeyPartitioned(vec![Arc::clone(&unknown), fixture.col(0)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "empty hash partitioning",
                Partitioning::Hash(vec![], 4),
                fixture.key_distribution([0]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "empty key distribution",
                fixture.hash_partitioning([0], 4),
                Distribution::KeyPartitioned(vec![]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            assert_satisfaction(
                desc,
                &partition,
                &required,
                &fixture.eq_properties,
                expected_with_subset,
                expected_without_subset,
            );
        }

        Ok(())
    }

    fn int_split_point(values: impl IntoIterator<Item = i64>) -> SplitPoint {
        SplitPoint::new(
            values
                .into_iter()
                .map(|value| ScalarValue::Int64(Some(value)))
                .collect(),
        )
    }

    fn assert_range_try_new_error(
        ordering: LexOrdering,
        split_points: Vec<SplitPoint>,
        expected: &str,
    ) {
        let error = RangePartitioning::try_new(ordering, split_points)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }

    #[test]
    fn test_range_partitioning_metadata() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;

        let range_partitioning =
            fixture.range([0], vec![int_split_point([10]), int_split_point([20])]);
        assert_eq!(range_partitioning.ordering()[0].to_string(), "a@0 ASC");
        assert_eq!(
            range_partitioning.split_points(),
            &[int_split_point([10]), int_split_point([20])]
        );
        let partitioning = Partitioning::Range(range_partitioning);

        assert_eq!(partitioning.partition_count(), 3);
        assert_eq!(
            partitioning.to_string(),
            "Range([a@0 ASC], [(10), (20)], 3)"
        );

        Ok(())
    }

    #[test]
    fn test_range_partitioning_scales_from_samples() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a"])?;
        let samples = (10..=90)
            .step_by(10)
            .map(|value| int_split_point([value]))
            .collect::<Vec<_>>();
        let range = RangePartitioning::try_new_with_samples(
            fixture.range_ordering([0]),
            samples.clone(),
            4,
        )?;

        assert_eq!(range.partition_count(), 4);
        assert_eq!(range.max_partition_count(), 10);
        assert_eq!(range.samples(), samples);
        assert_eq!(
            range.split_points(),
            vec![
                int_split_point([30]),
                int_split_point([50]),
                int_split_point([70]),
            ]
        );
        assert_eq!(
            range.to_string(),
            "Range([a@0 ASC], [(30), (50), (70)], 4, max 10)"
        );

        let single = range.scale(1).expect("one partition is supported");
        assert_eq!(single.partition_count(), 1);
        assert!(single.split_points().is_empty());
        assert_eq!(single.max_partition_count(), 10);
        assert_eq!(single.to_string(), "Range([a@0 ASC], [], 1, max 10)");

        let restored = single
            .scale(single.max_partition_count())
            .expect("retained samples support restoration");
        assert_eq!(restored.split_points(), samples);
        assert_eq!(restored.samples(), samples);

        Ok(())
    }

    #[test]
    fn test_range_partitioning_rejects_invalid_partition_count() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a"])?;
        let ordering = fixture.range_ordering([0]);
        let samples = vec![int_split_point([10]), int_split_point([20])];

        let error =
            RangePartitioning::try_new_with_samples(ordering.clone(), samples.clone(), 0)
                .unwrap_err()
                .to_string();
        assert!(error.contains("must be at least 1"), "{error}");

        let error =
            RangePartitioning::try_new_with_samples(ordering.clone(), samples.clone(), 4)
                .unwrap_err()
                .to_string();
        assert!(error.contains("exceeds maximum 3"), "{error}");

        let range = RangePartitioning::try_new(ordering, samples)?;
        assert!(range.scale(4).is_none());

        Ok(())
    }

    #[test]
    fn test_range_partitioning_equality_includes_scaling_capacity() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a"])?;
        let ordering = fixture.range_ordering([0]);
        let sampled = RangePartitioning::try_new_with_samples(
            ordering.clone(),
            (10..=90)
                .step_by(10)
                .map(|value| int_split_point([value]))
                .collect(),
            4,
        )?;
        let exact = RangePartitioning::try_new(
            ordering,
            vec![
                int_split_point([30]),
                int_split_point([50]),
                int_split_point([70]),
            ],
        )?;

        assert_eq!(sampled.split_points(), exact.split_points());
        assert!(sampled.has_same_layout(&exact));
        assert!(exact.has_same_layout(&sampled));
        assert!(sampled.scale(10).is_some());
        assert!(exact.scale(10).is_none());
        assert_ne!(sampled, exact);
        assert_ne!(Partitioning::Range(sampled), Partitioning::Range(exact));

        Ok(())
    }

    #[test]
    fn test_range_partitioning_layout_requires_keys_options_and_boundaries() -> Result<()>
    {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let range = RangePartitioning::try_new_with_samples(
            fixture.range_ordering([0]),
            vec![int_split_point([10])],
            2,
        )?;
        assert!(range.has_same_layout(&range.clone()));
        let different_boundary = RangePartitioning::try_new_with_samples(
            fixture.range_ordering([0]),
            vec![int_split_point([20])],
            2,
        )?;
        assert!(!range.has_same_layout(&different_boundary));
        assert!(!range.has_same_layout(&range.scale(1).unwrap()));
        let singleton = range.scale(1).unwrap();
        for ordering in [
            fixture.range_ordering([1]),
            [fixture.range_sort_expr(0, SortOptions::new(true, false))].into(),
            [fixture.range_sort_expr(0, SortOptions::new(false, false))].into(),
        ] {
            let other = RangePartitioning::try_new_with_samples(ordering, vec![], 1)?;
            assert!(!singleton.has_same_layout(&other));
        }
        Ok(())
    }

    #[test]
    fn test_range_partitioning_scaling_limits() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a"])?;
        let range = RangePartitioning::try_new_with_samples(
            fixture.range_ordering([0]),
            vec![int_split_point([10])],
            1,
        )?;
        assert_eq!(range.scale(0), None);
        for target_partitions in [3, usize::MAX] {
            assert_eq!(range.scale(target_partitions), None);
        }
        assert_eq!(range.scale(1).unwrap(), range);
        assert_eq!(range.scale(2).unwrap().scale(1).unwrap(), range);
        assert_eq!(range.scale(2).unwrap().partition_count(), 2);
        let empty = RangePartitioning::try_new_with_samples(
            fixture.range_ordering([0]),
            vec![],
            1,
        )?;
        assert_eq!(empty.scale(1).unwrap(), empty);
        assert_eq!(empty.scale(2), None);
        Ok(())
    }

    #[test]
    fn test_range_partitioning_try_new_validates_split_points() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let asc_a = fixture.range_ordering([0]);
        let ordering_ab = fixture.range_ordering([0, 1]);

        assert_range_try_new_error(
            ordering_ab.clone(),
            vec![int_split_point([10])],
            "split point 0 has width 1, but ordering has width 2",
        );

        RangePartitioning::try_new(
            [fixture.range_sort_expr(0, SortOptions::new(true, false))].into(),
            vec![int_split_point([20]), int_split_point([10])],
        )?;

        assert_range_try_new_error(
            asc_a,
            vec![int_split_point([20]), int_split_point([10])],
            "split points must be strictly ordered",
        );

        assert_range_try_new_error(
            [fixture.range_sort_expr(0, SortOptions::new(false, false))].into(),
            vec![
                SplitPoint::new(vec![ScalarValue::Int64(None)]),
                int_split_point([10]),
            ],
            "split points must be strictly ordered",
        );

        RangePartitioning::try_new(
            ordering_ab.clone(),
            vec![int_split_point([10, 20]), int_split_point([10, 30])],
        )?;

        assert_range_try_new_error(
            ordering_ab,
            vec![int_split_point([10, 30]), int_split_point([10, 20])],
            "split points must be strictly ordered",
        );

        Ok(())
    }

    #[test]
    fn test_range_partitioning_project_preserves_or_degrades() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let range_partitioning =
            Partitioning::Range(RangePartitioning::try_new_with_samples(
                [fixture.range_sort_expr(1, SortOptions::new(true, false))].into(),
                vec![
                    int_split_point([30]),
                    int_split_point([20]),
                    int_split_point([10]),
                ],
                2,
            )?);

        let keep_b_mapping = ProjectionMapping::from_indices(&[1], &fixture.schema)?;
        let projected =
            range_partitioning.project(&keep_b_mapping, &fixture.eq_properties);
        assert_eq!(
            projected.to_string(),
            "Range([b@0 DESC NULLS LAST], [(20)], 2, max 4)"
        );
        let Partitioning::Range(projected_range) = &projected else {
            panic!("expected range partitioning, got {projected:?}");
        };
        assert_eq!(projected_range.max_partition_count(), 4);
        assert_eq!(projected_range.scale(4).unwrap().split_points().len(), 3);

        let drop_b_mapping = ProjectionMapping::from_indices(&[0], &fixture.schema)?;
        let projected =
            range_partitioning.project(&drop_b_mapping, &fixture.eq_properties);
        let Partitioning::UnknownPartitioning(partition_count) = projected else {
            panic!("expected UnknownPartitioning, got {projected:?}");
        };
        assert_eq!(partition_count, 2);

        Ok(())
    }

    #[test]
    fn test_range_partitioning_project_degrades_if_ordering_collapses() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let target: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 0));
        let range_partitioning =
            fixture.range_partitioning([0, 1], vec![int_split_point([10, 100])]);
        let mapping = ProjectionMapping::from_iter([
            (
                fixture.col(0),
                ProjectionTargets::from(vec![(Arc::clone(&target), 0)]),
            ),
            (
                fixture.col(1),
                ProjectionTargets::from(vec![(Arc::clone(&target), 0)]),
            ),
        ]);

        let projected = range_partitioning.project(&mapping, &fixture.eq_properties);
        let Partitioning::UnknownPartitioning(partition_count) = projected else {
            panic!("expected UnknownPartitioning, got {projected:?}");
        };
        assert_eq!(partition_count, 2);

        Ok(())
    }

    #[test]
    fn range_partitioning_key_distribution_satisfaction() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b", "c"])?;
        let range_a = fixture.range_partitioning([0], vec![int_split_point([10])]);
        let range_ab =
            fixture.range_partitioning([0, 1], vec![int_split_point([10, 100])]);

        assert_satisfaction(
            "exact single key",
            &range_a,
            &fixture.key_distribution([0]),
            &fixture.eq_properties,
            PartitioningSatisfaction::Exact,
            PartitioningSatisfaction::Exact,
        );
        assert_satisfaction(
            "exact compound key",
            &range_ab,
            &fixture.key_distribution([0, 1]),
            &fixture.eq_properties,
            PartitioningSatisfaction::Exact,
            PartitioningSatisfaction::Exact,
        );
        assert_satisfaction(
            "subset key",
            &range_a,
            &fixture.key_distribution([0, 1]),
            &fixture.eq_properties,
            PartitioningSatisfaction::Subset,
            PartitioningSatisfaction::NotSatisfied,
        );
        assert_satisfaction(
            "incompatible key",
            &range_a,
            &fixture.key_distribution([1]),
            &fixture.eq_properties,
            PartitioningSatisfaction::NotSatisfied,
            PartitioningSatisfaction::NotSatisfied,
        );

        let mut eq_properties = fixture.eq_properties.clone();
        eq_properties.add_equal_conditions(fixture.col(0), fixture.col(2))?;
        assert_satisfaction(
            "equivalent subset key",
            &range_a,
            &fixture.key_distribution([1, 2]),
            &eq_properties,
            PartitioningSatisfaction::Subset,
            PartitioningSatisfaction::NotSatisfied,
        );

        let mut eq_properties = fixture.eq_properties.clone();
        eq_properties.add_equal_conditions(fixture.col(0), fixture.col(1))?;
        assert_satisfaction(
            "equivalent exact key",
            &range_a,
            &fixture.key_distribution([1]),
            &eq_properties,
            PartitioningSatisfaction::Exact,
            PartitioningSatisfaction::Exact,
        );

        Ok(())
    }

    #[test]
    fn test_range_partitioning_adapt() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("a", DataType::Int32),
            ("b", DataType::Int64),
            ("c", DataType::Int32),
        ])?;

        let range = RangePartitioning::try_new_with_samples(
            fixture.range_ordering([0]),
            vec![
                SplitPoint::new(vec![ScalarValue::Int32(Some(10))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(15))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(20))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(25))]),
            ],
            3,
        )?;

        // Adapting to col_c (same type Int32) succeeds
        let adapted = range.adapt(&[fixture.col(2)], &fixture.schema).unwrap();
        assert_eq!(adapted.ordering().len(), 1);
        assert!(adapted.ordering()[0].expr.eq(&fixture.col(2)));
        assert_eq!(adapted.partition_count(), 3);
        assert_eq!(adapted.max_partition_count(), 5);
        assert_eq!(adapted.scale(5).unwrap().partition_count(), 5);

        // Adapting to col_b (different type Int64) fails
        assert!(range.adapt(&[fixture.col(1)], &fixture.schema).is_none());
        // Scaling to one partition removes effective boundaries, not sample types.
        let singleton = range.scale(1).unwrap();
        assert!(
            singleton
                .adapt(&[fixture.col(1)], &fixture.schema)
                .is_none()
        );
        let adapted_singleton =
            singleton.adapt(&[fixture.col(2)], &fixture.schema).unwrap();
        assert_eq!(
            adapted_singleton.scale(5).unwrap().samples(),
            range.samples()
        );

        // Adapting to empty or mismatch count fails
        assert!(range.adapt(&[], &fixture.schema).is_none());
        assert!(
            range
                .adapt(&fixture.cols([0, 2]), &fixture.schema)
                .is_none()
        );

        // Partitioning::adapt works with Distribution::KeyPartitioned
        let part = Partitioning::Range(range);
        assert!(
            part.adapt(&fixture.key_distribution([1]), &fixture.schema)
                .is_none()
        );

        let adapted_part = part
            .adapt(&fixture.key_distribution([2]), &fixture.schema)
            .unwrap();
        match adapted_part {
            Partitioning::Range(r) => assert!(r.ordering()[0].expr.eq(&fixture.col(2))),
            _ => panic!("expected Range partitioning"),
        }

        // Partitioning::Hash adaptation
        let hash_part = fixture.hash_partitioning([1], 4);
        let adapted_hash = hash_part
            .adapt(&fixture.key_distribution([2]), &fixture.schema)
            .unwrap();
        match adapted_hash {
            Partitioning::Hash(exprs, count) => {
                assert_eq!(count, 4);
                assert_eq!(exprs.len(), 1);
                assert!(exprs[0].eq(&fixture.col(2)));
            }
            _ => panic!("expected Hash partitioning"),
        }

        Ok(())
    }

    #[test]
    fn test_range_partitioning_adapt_rejects_duplicate_keys() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("a", DataType::Int32),
            ("b", DataType::Int32),
            ("c", DataType::Int32),
        ])?;
        let range = RangePartitioning::try_new_with_samples(
            fixture.range_ordering([0, 1]),
            vec![SplitPoint::new(vec![
                ScalarValue::Int32(Some(10)),
                ScalarValue::Int32(Some(20)),
            ])],
            1,
        )?;
        for count in [1, 2] {
            assert!(
                range
                    .scale(count)
                    .unwrap()
                    .adapt(&fixture.cols([2, 2]), &fixture.schema)
                    .is_none()
            );
        }
        Ok(())
    }

    #[test]
    fn test_range_partitioning_adapt_multi_key() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("k1", DataType::Int32),
            ("k2", DataType::Utf8),
            ("t1", DataType::Int32),
            ("t2", DataType::Utf8),
        ])?;

        let opt_k1 = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let opt_k2 = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let ordering = LexOrdering::new(vec![
            fixture.range_sort_expr(0, opt_k1),
            fixture.range_sort_expr(1, opt_k2),
        ])
        .unwrap();

        let split_points = vec![
            SplitPoint::new(vec![ScalarValue::Int32(Some(20)), ScalarValue::Utf8(None)]),
            SplitPoint::new(vec![
                ScalarValue::Int32(Some(10)),
                ScalarValue::Utf8(Some("foo".to_string())),
            ]),
        ];

        let range = RangePartitioning::try_new(ordering, split_points.clone())?;
        let adapted = range.adapt(&fixture.cols([2, 3]), &fixture.schema).unwrap();

        assert_eq!(adapted.ordering().len(), 2);
        assert!(adapted.ordering()[0].expr.eq(&fixture.col(2)));
        assert_eq!(adapted.ordering()[0].options, opt_k1);
        assert!(adapted.ordering()[1].expr.eq(&fixture.col(3)));
        assert_eq!(adapted.ordering()[1].options, opt_k2);
        assert_eq!(adapted.split_points(), &split_points);
        assert_eq!(adapted.partition_count(), 3);

        Ok(())
    }
}

#[cfg(all(test, feature = "proto"))]
mod ordering_proto_tests {
    use std::sync::Arc;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_physical_expr_common::sort_expr::{
        LexRequirement, PhysicalSortExpr, PhysicalSortRequirement,
        sort_exprs_try_from_proto, sort_exprs_try_to_proto,
    };

    use crate::expressions::Column;
    use crate::proto_test_util::{StubDecoder, StubEncoder};

    fn schema() -> Schema {
        Schema::new(vec![Field::new("a", DataType::Int32, false)])
    }

    fn sort_expr(descending: bool, nulls_first: bool) -> PhysicalSortExpr {
        PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending,
                nulls_first,
            },
        )
    }

    #[test]
    fn sort_exprs_round_trip_preserves_options_and_order() {
        let encoder = StubEncoder::ok();
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let exprs = vec![sort_expr(true, false), sort_expr(false, true)];

        let nodes = sort_exprs_try_to_proto(&exprs, &encode_ctx).unwrap();
        // `asc` is the inverse of `descending` on the wire.
        assert_eq!(
            nodes
                .iter()
                .map(|node| (node.asc, node.nulls_first))
                .collect::<Vec<_>>(),
            vec![(false, false), (true, true)]
        );

        let schema = schema();
        let decoder = StubDecoder::ok();
        let decode_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let decoded = sort_exprs_try_from_proto(&nodes, &decode_ctx).unwrap();
        assert_eq!(
            decoded.iter().map(|expr| expr.options).collect::<Vec<_>>(),
            exprs.iter().map(|expr| expr.options).collect::<Vec<_>>()
        );
    }

    #[test]
    fn sort_exprs_accepts_owned_requirements() {
        let encoder = StubEncoder::ok();
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let requirement = LexRequirement::from([PhysicalSortRequirement::new(
            Arc::new(Column::new("a", 0)),
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
        )]);

        let nodes = sort_exprs_try_to_proto(
            requirement
                .iter()
                .map(|req| PhysicalSortExpr::from(req.clone())),
            &encode_ctx,
        )
        .unwrap();

        assert_eq!(nodes.len(), 1);
        assert!(!nodes[0].asc);
        assert!(nodes[0].nulls_first);
    }

    #[test]
    fn sort_exprs_propagate_encode_errors() {
        let encoder = StubEncoder::failing_on(2);
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let exprs = vec![sort_expr(false, false), sort_expr(true, true)];

        let err = sort_exprs_try_to_proto(&exprs, &encode_ctx).unwrap_err();
        assert!(err.to_string().contains("stub encode failure on call 2"));
    }

    #[test]
    fn sort_exprs_reject_missing_inner_expr() {
        let encoder = StubEncoder::ok();
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let mut nodes =
            sort_exprs_try_to_proto(&[sort_expr(false, false)], &encode_ctx).unwrap();
        nodes[0].expr = None;

        let schema = schema();
        let decoder = StubDecoder::ok();
        let decode_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = sort_exprs_try_from_proto(&nodes, &decode_ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("PhysicalSortExpr is missing required field 'expr'")
        );
    }
}

#[cfg(all(test, feature = "proto"))]
mod range_partitioning_proto_tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, ScalarValue, SplitPoint};
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_proto_models::protobuf;

    use super::{Partitioning, RangePartitioning};
    use crate::expressions::Column;
    use crate::proto_test_util::{StubDecoder, StubEncoder};

    fn sampled_partitioning() -> Result<Partitioning> {
        let ordering = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(
            Column::new("a", 0),
        ))])
        .expect("non-empty ordering");
        let samples = [10, 20, 30, 40, 50]
            .into_iter()
            .map(|value| SplitPoint::new(vec![ScalarValue::Int32(Some(value))]))
            .collect();
        Ok(Partitioning::Range(
            RangePartitioning::try_new_with_samples(ordering, samples, 3)?,
        ))
    }

    fn decode(partitioning: &protobuf::Partitioning) -> Result<Partitioning> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let decoder = StubDecoder::ok();
        let decode_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        Ok(Partitioning::try_from_proto(partitioning, &decode_ctx)?
            .expect("partitioning method is present"))
    }

    #[test]
    fn sampled_range_partitioning_round_trip_preserves_resolution() -> Result<()> {
        let partitioning = sampled_partitioning()?;
        let encoder = StubEncoder::ok();
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let encoded = partitioning.try_to_proto(&encode_ctx)?;
        let Some(protobuf::partitioning::PartitionMethod::Range(encoded_range)) =
            encoded.partition_method.as_ref()
        else {
            panic!("expected range partitioning");
        };

        // Field 2 remains the effective boundary list for older readers.
        assert_eq!(encoded_range.split_point.len(), 2);
        assert_eq!(encoded_range.sample_point.len(), 5);
        assert_eq!(encoded_range.partition_count, 3);

        let decoded = decode(&encoded)?;
        let Partitioning::Range(decoded) = decoded else {
            panic!("expected range partitioning");
        };
        let Partitioning::Range(original) = partitioning else {
            panic!("expected range partitioning");
        };
        assert_eq!(decoded.partition_count(), original.partition_count());
        assert_eq!(decoded.split_points(), original.split_points());
        assert_eq!(
            decoded.ordering()[0].options,
            original.ordering()[0].options
        );
        assert_eq!(decoded.samples(), original.samples());
        assert_eq!(decoded.max_partition_count(), 6);

        Ok(())
    }

    #[test]
    fn legacy_range_partitioning_payload_remains_exact() -> Result<()> {
        let partitioning = sampled_partitioning()?;
        let encoder = StubEncoder::ok();
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let mut encoded = partitioning.try_to_proto(&encode_ctx)?;
        let Some(protobuf::partitioning::PartitionMethod::Range(encoded_range)) =
            encoded.partition_method.as_mut()
        else {
            panic!("expected range partitioning");
        };
        encoded_range.sample_point.clear();
        encoded_range.partition_count = 0;

        let decoded = decode(&encoded)?;
        let Partitioning::Range(decoded) = decoded else {
            panic!("expected range partitioning");
        };
        assert_eq!(decoded.partition_count(), 3);
        assert_eq!(decoded.max_partition_count(), 3);
        assert_eq!(decoded.split_points().len(), 2);

        Ok(())
    }

    #[test]
    fn sampled_range_partitioning_rejects_inconsistent_effective_points() -> Result<()> {
        let partitioning = sampled_partitioning()?;
        let encoder = StubEncoder::ok();
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let mut encoded = partitioning.try_to_proto(&encode_ctx)?;
        let Some(protobuf::partitioning::PartitionMethod::Range(encoded_range)) =
            encoded.partition_method.as_mut()
        else {
            panic!("expected range partitioning");
        };
        encoded_range.split_point.pop();

        let error = decode(&encoded).unwrap_err().to_string();
        assert!(
            error.contains("effective split points do not match"),
            "{error}"
        );

        Ok(())
    }
}

/// Partition counts are `usize` in memory and `u64` on the wire, so every
/// counted [`Partitioning`] variant crosses a width boundary in both
/// directions. These pin that neither crossing wraps or panics.
#[cfg(all(test, feature = "proto"))]
mod partition_count_proto_tests {
    use std::sync::Arc;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ScalarValue, SplitPoint};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_proto_models::protobuf;

    use super::{Partitioning, RangePartitioning};
    use crate::expressions::Column;
    use crate::proto_test_util::{StubDecoder, StubEncoder, column_node};

    fn partitioning_node(
        method: protobuf::partitioning::PartitionMethod,
    ) -> protobuf::Partitioning {
        protobuf::Partitioning {
            partition_method: Some(method),
        }
    }

    /// The counted variants, each carrying `count`. `Range` is excluded: it
    /// derives its partition count from its split points rather than reading
    /// one off the wire.
    fn counted_methods(count: u64) -> Vec<protobuf::partitioning::PartitionMethod> {
        use protobuf::partitioning::PartitionMethod;

        vec![
            PartitionMethod::RoundRobin(count),
            PartitionMethod::Unknown(count),
            PartitionMethod::Hash(protobuf::PhysicalHashRepartition {
                hash_expr: vec![column_node("a")],
                partition_count: count,
            }),
        ]
    }

    #[test]
    fn try_from_proto_narrows_every_counted_variant() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let decoder = StubDecoder::ok();
        let decode_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        for method in counted_methods(u64::MAX) {
            let decoded =
                Partitioning::try_from_proto(&partitioning_node(method), &decode_ctx);

            #[cfg(target_pointer_width = "64")]
            assert_eq!(decoded.unwrap().unwrap().partition_count(), usize::MAX);

            #[cfg(not(target_pointer_width = "64"))]
            assert!(
                decoded
                    .unwrap_err()
                    .to_string()
                    .contains("is out of range for usize")
            );
        }
    }

    #[test]
    fn try_to_proto_widens_every_counted_variant() {
        use protobuf::partitioning::PartitionMethod;

        let encoder = StubEncoder::ok();
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let hash_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));

        let encoded = [
            Partitioning::RoundRobinBatch(usize::MAX),
            Partitioning::UnknownPartitioning(usize::MAX),
            Partitioning::Hash(vec![hash_key], usize::MAX),
        ]
        .iter()
        .map(|partitioning| {
            match partitioning
                .try_to_proto(&encode_ctx)
                .unwrap()
                .partition_method
            {
                Some(PartitionMethod::RoundRobin(n) | PartitionMethod::Unknown(n)) => n,
                Some(PartitionMethod::Hash(hash)) => hash.partition_count,
                other => panic!("expected a counted partition method, got {other:?}"),
            }
        })
        .collect::<Vec<_>>();

        // Every variant widens to the same wire value, with no truncation.
        assert_eq!(encoded, vec![u64::try_from(usize::MAX).unwrap(); 3]);
    }

    #[test]
    fn range_partitioning_round_trip_preserves_split_points_and_options() {
        // Single-column ordering so the stub encoder (which emits an identical
        // placeholder for every expression) cannot produce duplicates that the
        // decoder's deduplication step would collapse.
        let sort_options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let ordering = LexOrdering::from([PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            sort_options,
        )]);
        let split_points = vec![
            SplitPoint::new(vec![ScalarValue::Int32(Some(10))]),
            SplitPoint::new(vec![ScalarValue::Int32(Some(30))]),
        ];
        let range =
            RangePartitioning::try_new(ordering.clone(), split_points.clone()).unwrap();

        let encoder = StubEncoder::ok();
        let encode_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let node = Partitioning::Range(range)
            .try_to_proto(&encode_ctx)
            .unwrap();

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let decoder = StubDecoder::ok();
        let decode_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let decoded = Partitioning::try_from_proto(&node, &decode_ctx)
            .unwrap()
            .unwrap();

        let Partitioning::Range(decoded_range) = decoded else {
            panic!("expected Range partitioning, got {decoded:?}");
        };
        assert_eq!(
            decoded_range
                .ordering()
                .iter()
                .map(|e| e.options)
                .collect::<Vec<_>>(),
            [sort_options],
        );
        assert_eq!(decoded_range.split_points(), split_points);
    }
}
