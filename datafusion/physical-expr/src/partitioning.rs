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
    EquivalenceProperties, PhysicalExpr, equivalence::ProjectionMapping,
    physical_exprs_contains, physical_exprs_equal,
};
pub use datafusion_common::SplitPoint;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, ScalarValue, validate_range_split_points};
use datafusion_expr::ColumnarValue;
use datafusion_expr::interval_arithmetic::checked_predecessor;
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
/// [`RangePartitioning`] describes an ordered key space with split points.
///
/// - `ordering` defines the partitioning key and ordering.
/// - `split_points` define the boundaries between adjacent partitions.
///
/// Comparisons use the lexicographic order defined by `ordering`, including
/// `ASC`/`DESC` and null ordering. Split points must be strictly ordered
/// according to that ordering, and each split point must have one value per
/// ordering expression. See [`SplitPoint`] for the shared boundary convention.
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
/// NOTE: Optimizer and execution behavior for this partitioning is intentionally
/// not implemented and will be introduced incrementally. See
/// <https://github.com/apache/datafusion/issues/22395>.
#[derive(Debug, Clone, PartialEq)]
pub struct RangePartitioning {
    /// Ordered partitioning key.
    ordering: LexOrdering,
    /// Boundaries between adjacent partitions.
    split_points: Vec<SplitPoint>,
}

impl RangePartitioning {
    /// Creates range partitioning metadata without validating split points.
    ///
    /// Use [`Self::try_new`] to validate the contract documented on
    /// [`RangePartitioning`].
    pub fn new(ordering: LexOrdering, split_points: Vec<SplitPoint>) -> Self {
        Self {
            ordering,
            split_points,
        }
    }

    /// Creates range partitioning metadata and validates split point shape and
    /// ordering.
    pub fn try_new(ordering: LexOrdering, split_points: Vec<SplitPoint>) -> Result<Self> {
        validate_range_split_points(
            &split_points,
            &ordering
                .iter()
                .map(|sort_expr| sort_expr.options)
                .collect::<Vec<_>>(),
        )?;
        Ok(Self::new(ordering, split_points))
    }

    /// Returns the ordering that defines the range key.
    pub fn ordering(&self) -> &LexOrdering {
        &self.ordering
    }

    /// Returns the ordered split points between partitions.
    pub fn split_points(&self) -> &[SplitPoint] {
        &self.split_points
    }

    /// Returns the number of partitions.
    pub fn partition_count(&self) -> usize {
        self.split_points.len() + 1
    }

    /// Calculates the range partitioning after applying the given projection.
    ///
    /// Returns `None` if any range key cannot be projected or if projection
    /// collapses distinct range keys into duplicate output expressions.
    ///
    /// If a projection drops a range key but keeps a monotonic function of it
    /// (for example `date_bin(interval, timestamp)` or `date_trunc(unit, timestamp)`
    /// while range-partitioned on `timestamp`), the range can still be projected.
    /// Adjacent partitions stay disjoint only when evaluating the function at
    /// each split point and its predecessor yields different values, so bins
    /// do not straddle file groups.
    fn project(
        &self,
        mapping: &ProjectionMapping,
        input_eq_properties: &EquivalenceProperties,
    ) -> Option<Self> {
        let mut split_points = self.split_points.clone();
        let mut sort_exprs = Vec::with_capacity(self.ordering.len());
        for (key_idx, sort_expr) in self.ordering.iter().enumerate() {
            if let Some(projected) =
                input_eq_properties.project_expr(&sort_expr.expr, mapping)
            {
                sort_exprs.push(PhysicalSortExpr::new(projected, sort_expr.options));
                continue;
            }

            let (target, source) =
                monotonic_range_key_projection(sort_expr, mapping, input_eq_properties)?;
            if !monotonic_fn_keeps_partitions_disjoint(
                &source,
                &sort_expr.expr,
                &split_points,
                key_idx,
            ) {
                return None;
            }
            if let Some(updated) = project_split_points_through_fn(
                &source,
                &sort_expr.expr,
                &split_points,
                key_idx,
            ) {
                split_points = updated;
            }
            sort_exprs.push(PhysicalSortExpr::new(target, sort_expr.options));
        }
        let ordering = LexOrdering::new(sort_exprs)?;
        if ordering.len() != self.ordering.len() {
            return None;
        }

        Some(Self {
            ordering,
            split_points,
        })
    }
}

/// Finds a projection mapping whose source is a monotonic function of `sort_expr`.
fn monotonic_range_key_projection(
    sort_expr: &PhysicalSortExpr,
    mapping: &ProjectionMapping,
    eq_properties: &EquivalenceProperties,
) -> Option<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    mapping.iter().find_map(|(source, targets)| {
        eq_properties
            .is_monotonic_function_of(source, &sort_expr.expr)
            .then(|| (Arc::clone(&targets.first().0), Arc::clone(source)))
    })
}

/// Adjacent range partitions remain disjoint on `fn_expr` when the function
/// value at each split differs from the value immediately below the split.
fn monotonic_fn_keeps_partitions_disjoint(
    fn_expr: &Arc<dyn PhysicalExpr>,
    range_key: &Arc<dyn PhysicalExpr>,
    split_points: &[SplitPoint],
    key_idx: usize,
) -> bool {
    split_points.iter().all(|split_point| {
        let Some(split_value) = split_point.values().get(key_idx) else {
            return false;
        };
        let Some(predecessor) = checked_predecessor(split_value) else {
            return false;
        };
        let Some(at_split) = evaluate_expr_on_key(fn_expr, range_key, split_value) else {
            return false;
        };
        let Some(below_split) = evaluate_expr_on_key(fn_expr, range_key, &predecessor)
        else {
            return false;
        };
        at_split != below_split
    })
}

fn project_split_points_through_fn(
    fn_expr: &Arc<dyn PhysicalExpr>,
    range_key: &Arc<dyn PhysicalExpr>,
    split_points: &[SplitPoint],
    key_idx: usize,
) -> Option<Vec<SplitPoint>> {
    split_points
        .iter()
        .map(|split_point| {
            let split_value = split_point.values().get(key_idx)?;
            let projected = evaluate_expr_on_key(fn_expr, range_key, split_value)?;
            let mut values = split_point.values().to_vec();
            values[key_idx] = projected;
            Some(SplitPoint::new(values))
        })
        .collect()
}

/// Evaluates `expr` after substituting `range_key` with `value`.
fn evaluate_expr_on_key(
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
    let batch = create_dummy_batch().ok()?;
    match rewritten.data.evaluate(batch).ok()? {
        ColumnarValue::Scalar(scalar) => Some(scalar),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0).ok(),
    }
}

/// `Range([x])` satisfies grouping by `(..., f(x), ...)` when `f` is monotonic in
/// `x` and adjacent partitions do not share `f` values (bins do not straddle
/// split points). That makes `(key, date_bin(timestamp))` and
/// `(key, date_trunc(timestamp))` partition-disjoint when the table is
/// range-partitioned on `timestamp` and the split is aligned to the bin.
fn range_monotonic_fn_satisfies_keys(
    range: &RangePartitioning,
    required_exprs: &[Arc<dyn PhysicalExpr>],
    eq_properties: &EquivalenceProperties,
) -> bool {
    if range.ordering().len() != 1 {
        return false;
    }
    let range_key = &range.ordering()[0].expr;
    required_exprs.iter().any(|required| {
        eq_properties.is_monotonic_function_of(required, range_key)
            && monotonic_fn_keeps_partitions_disjoint(
                required,
                range_key,
                range.split_points(),
                0,
            )
    })
}

impl Display for RangePartitioning {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let split_points = format_range_split_points(&self.split_points);
        write!(
            f,
            "Range([{}], [{}], {})",
            self.ordering,
            split_points,
            self.partition_count()
        )
    }
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
                    let satisfaction = Self::key_satisfaction(
                        &partition_exprs,
                        required_exprs,
                        eq_properties,
                        allow_subset,
                    );
                    if satisfaction == PartitioningSatisfaction::NotSatisfied
                        && allow_subset
                        && range_monotonic_fn_satisfies_keys(
                            range,
                            required_exprs,
                            eq_properties,
                        )
                    {
                        PartitioningSatisfaction::Subset
                    } else {
                        satisfaction
                    }
                }
                Partitioning::RoundRobinBatch(_)
                | Partitioning::UnknownPartitioning(_) => {
                    PartitioningSatisfaction::NotSatisfied
                }
            },
            Distribution::SinglePartition => PartitioningSatisfaction::NotSatisfied,
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
        use datafusion_proto_models::protobuf;

        let partition_method = match self {
            Partitioning::RoundRobinBatch(n) => {
                protobuf::partitioning::PartitionMethod::RoundRobin(wire_partition_count(
                    *n,
                )?)
            }
            Partitioning::Hash(exprs, n) => {
                protobuf::partitioning::PartitionMethod::Hash(
                    protobuf::PhysicalHashRepartition {
                        hash_expr: ctx.encode_children_expressions(exprs)?,
                        partition_count: wire_partition_count(*n)?,
                    },
                )
            }
            Partitioning::Range(range) => {
                let sort_expr = sort_exprs_try_to_proto(range.ordering().iter(), ctx)?;
                let split_point = range
                    .split_points()
                    .iter()
                    .map(|split_point| {
                        let value = split_point
                            .values()
                            .iter()
                            .map(|value| value.try_into().map_err(Into::into))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(protobuf::PhysicalRangeSplitPoint { value })
                    })
                    .collect::<Result<Vec<_>>>()?;
                protobuf::partitioning::PartitionMethod::Range(
                    protobuf::PhysicalRangePartitioning {
                        sort_expr,
                        split_point,
                    },
                )
            }
            Partitioning::UnknownPartitioning(n) => {
                protobuf::partitioning::PartitionMethod::Unknown(wire_partition_count(
                    *n,
                )?)
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
        use datafusion_common::{ScalarValue, internal_datafusion_err, internal_err};
        use datafusion_proto_models::protobuf;

        let Some(partition_method) = node.partition_method.as_ref() else {
            return Ok(None);
        };
        let partitioning = match partition_method {
            protobuf::partitioning::PartitionMethod::RoundRobin(n) => {
                Partitioning::RoundRobinBatch(partition_count(*n)?)
            }
            protobuf::partitioning::PartitionMethod::Hash(hash) => {
                let exprs = hash
                    .hash_expr
                    .iter()
                    .map(|expr| ctx.decode(expr))
                    .collect::<Result<Vec<_>>>()?;
                Partitioning::Hash(exprs, partition_count(hash.partition_count)?)
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
                let split_points = range
                    .split_point
                    .iter()
                    .map(|split_point| {
                        let values = split_point
                            .value
                            .iter()
                            .map(|value| ScalarValue::try_from(value).map_err(Into::into))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(SplitPoint::new(values))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Partitioning::Range(RangePartitioning::try_new(ordering, split_points)?)
            }
        };
        Ok(Some(partitioning))
    }
}

/// Narrow a wire partition count to `usize`.
#[cfg(feature = "proto")]
fn partition_count(count: u64) -> Result<usize> {
    usize::try_from(count).map_err(|_| {
        datafusion_common::internal_datafusion_err!(
            "Partition count {count} exceeds usize::MAX"
        )
    })
}

/// Widen a partition count to its `u64` wire representation.
///
/// The mirror of [`partition_count`]: an out-of-range count is an error on both
/// sides rather than a silent truncation on the way out.
#[cfg(feature = "proto")]
fn wire_partition_count(count: usize) -> Result<u64> {
    u64::try_from(count).map_err(|_| {
        datafusion_common::internal_datafusion_err!(
            "Partition count {count} exceeds u64::MAX"
        )
    })
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
    use crate::ScalarFunctionExpr;
    use crate::expressions::{Column, Literal};
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
        let range_partitioning = fixture.range_partitioning_with_ordering(
            [fixture.range_sort_expr(1, SortOptions::new(true, false))].into(),
            vec![int_split_point([10])],
        );

        let keep_b_mapping = ProjectionMapping::from_indices(&[1], &fixture.schema)?;
        let projected =
            range_partitioning.project(&keep_b_mapping, &fixture.eq_properties);
        assert_eq!(
            projected.to_string(),
            "Range([b@0 DESC NULLS LAST], [(10)], 2)"
        );

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

    fn date_bin_of(
        timestamp: Arc<dyn PhysicalExpr>,
        stride_ns: i64,
    ) -> Arc<dyn PhysicalExpr> {
        datetime_fn(
            "date_bin",
            datafusion_functions::datetime::date_bin(),
            vec![
                Arc::new(Literal::new(ScalarValue::new_interval_mdn(0, 0, stride_ns))),
                timestamp,
            ],
        )
    }

    fn date_trunc_of(
        timestamp: Arc<dyn PhysicalExpr>,
        precision: &str,
    ) -> Arc<dyn PhysicalExpr> {
        datetime_fn(
            "date_trunc",
            datafusion_functions::datetime::date_trunc(),
            vec![
                Arc::new(Literal::new(ScalarValue::Utf8(Some(precision.to_string())))),
                timestamp,
            ],
        )
    }

    fn datetime_fn(
        name: &str,
        fun: Arc<datafusion_expr::ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(ScalarFunctionExpr::new(
            name,
            fun,
            args,
            Field::new(
                "time_bin",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )
            .into(),
            Arc::new(ConfigOptions::default()),
        ))
    }

    fn ts_ns_split(ns: i64) -> SplitPoint {
        SplitPoint::new(vec![ScalarValue::TimestampNanosecond(Some(ns), None)])
    }

    #[test]
    fn range_partitioning_satisfies_monotonic_date_bin_grouping() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("key", DataType::Utf8),
            ("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None)),
        ])?;
        // 2024-01-01T01:00:00, aligned to a 60-second date_bin.
        let hour_ns = 1_704_070_800_000_000_000i64;
        let aligned = fixture.range_partitioning([1], vec![ts_ns_split(hour_ns)]);
        let unaligned =
            fixture.range_partitioning([1], vec![ts_ns_split(hour_ns + 30_000_000_000)]);

        let required = Distribution::KeyPartitioned(vec![
            fixture.col(0),
            date_bin_of(fixture.col(1), 60_000_000_000),
        ]);

        assert_satisfaction(
            "aligned hour split: Range(timestamp) subset-satisfies GROUP BY (key, date_bin(60s, timestamp))",
            &aligned,
            &required,
            &fixture.eq_properties,
            PartitioningSatisfaction::Subset,
            PartitioningSatisfaction::NotSatisfied,
        );
        assert_satisfaction(
            "unaligned split does not satisfy date_bin grouping",
            &unaligned,
            &required,
            &fixture.eq_properties,
            PartitioningSatisfaction::NotSatisfied,
            PartitioningSatisfaction::NotSatisfied,
        );

        let trunc_hour = Distribution::KeyPartitioned(vec![
            fixture.col(0),
            date_trunc_of(fixture.col(1), "hour"),
        ]);
        assert_satisfaction(
            "aligned hour split: Range(timestamp) subset-satisfies GROUP BY (key, date_trunc(hour, timestamp))",
            &aligned,
            &trunc_hour,
            &fixture.eq_properties,
            PartitioningSatisfaction::Subset,
            PartitioningSatisfaction::NotSatisfied,
        );

        let trunc_day = Distribution::KeyPartitioned(vec![
            fixture.col(0),
            date_trunc_of(fixture.col(1), "day"),
        ]);
        assert_satisfaction(
            "hour split straddles date_trunc(day) bins",
            &aligned,
            &trunc_day,
            &fixture.eq_properties,
            PartitioningSatisfaction::NotSatisfied,
            PartitioningSatisfaction::NotSatisfied,
        );

        let min_ts = fixture.range_partitioning([1], vec![ts_ns_split(i64::MIN)]);
        assert_satisfaction(
            "type-minimum split has no predecessor so date_bin grouping is not disjoint",
            &min_ts,
            &required,
            &fixture.eq_properties,
            PartitioningSatisfaction::NotSatisfied,
            PartitioningSatisfaction::NotSatisfied,
        );

        let compound = fixture.range_partitioning(
            [0, 1],
            vec![SplitPoint::new(vec![
                ScalarValue::Utf8(Some("k".into())),
                ScalarValue::TimestampNanosecond(Some(hour_ns), None),
            ])],
        );
        assert_satisfaction(
            "multi-key Range([key, timestamp]) does not use single-key date_bin subset logic",
            &compound,
            &required,
            &fixture.eq_properties,
            PartitioningSatisfaction::NotSatisfied,
            PartitioningSatisfaction::NotSatisfied,
        );

        Ok(())
    }

    #[test]
    fn test_range_partitioning_project_through_date_bin() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )])?;
        let hour_ns = 1_704_070_800_000_000_000i64;
        let date_bin = date_bin_of(fixture.col(0), 60_000_000_000);
        let target: Arc<dyn PhysicalExpr> = Arc::new(Column::new("time_bin", 0));
        let mapping = ProjectionMapping::from_iter([(
            Arc::clone(&date_bin),
            ProjectionTargets::from(vec![(Arc::clone(&target), 0)]),
        )]);

        let aligned = fixture.range_partitioning([0], vec![ts_ns_split(hour_ns)]);
        let projected = aligned.project(&mapping, &fixture.eq_properties);
        assert_eq!(
            projected.to_string(),
            "Range([time_bin@0 ASC], [(1704070800000000000)], 2)"
        );

        let unaligned =
            fixture.range_partitioning([0], vec![ts_ns_split(hour_ns + 30_000_000_000)]);
        let projected = unaligned.project(&mapping, &fixture.eq_properties);
        let Partitioning::UnknownPartitioning(partition_count) = projected else {
            panic!("expected UnknownPartitioning, got {projected:?}");
        };
        assert_eq!(partition_count, 2);

        let min_ts = fixture.range_partitioning([0], vec![ts_ns_split(i64::MIN)]);
        let projected = min_ts.project(&mapping, &fixture.eq_properties);
        let Partitioning::UnknownPartitioning(partition_count) = projected else {
            panic!("expected UnknownPartitioning, got {projected:?}");
        };
        assert_eq!(partition_count, 2);

        Ok(())
    }

    #[test]
    fn test_range_partitioning_project_through_date_trunc() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )])?;
        let hour_ns = 1_704_070_800_000_000_000i64;
        let trunc_hour = date_trunc_of(fixture.col(0), "hour");
        let target: Arc<dyn PhysicalExpr> = Arc::new(Column::new("time_bin", 0));
        let mapping = ProjectionMapping::from_iter([(
            Arc::clone(&trunc_hour),
            ProjectionTargets::from(vec![(Arc::clone(&target), 0)]),
        )]);

        let aligned = fixture.range_partitioning([0], vec![ts_ns_split(hour_ns)]);
        let projected = aligned.project(&mapping, &fixture.eq_properties);
        assert_eq!(
            projected.to_string(),
            "Range([time_bin@0 ASC], [(1704070800000000000)], 2)"
        );

        let trunc_day = date_trunc_of(fixture.col(0), "day");
        let day_mapping = ProjectionMapping::from_iter([(
            Arc::clone(&trunc_day),
            ProjectionTargets::from(vec![(Arc::clone(&target), 0)]),
        )]);
        let projected = aligned.project(&day_mapping, &fixture.eq_properties);
        let Partitioning::UnknownPartitioning(partition_count) = projected else {
            panic!("expected UnknownPartitioning, got {projected:?}");
        };
        assert_eq!(partition_count, 2);

        Ok(())
    }

    #[test]
    fn test_range_partitioning_project_compound_through_date_bin() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("key", DataType::Utf8),
            ("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None)),
        ])?;
        let hour_ns = 1_704_070_800_000_000_000i64;
        let date_bin = date_bin_of(fixture.col(1), 60_000_000_000);
        let key_target: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let bin_target: Arc<dyn PhysicalExpr> = Arc::new(Column::new("time_bin", 1));
        let mapping = ProjectionMapping::from_iter([
            (
                fixture.col(0),
                ProjectionTargets::from(vec![(Arc::clone(&key_target), 0)]),
            ),
            (
                Arc::clone(&date_bin),
                ProjectionTargets::from(vec![(Arc::clone(&bin_target), 1)]),
            ),
        ]);

        let aligned = fixture.range_partitioning(
            [0, 1],
            vec![SplitPoint::new(vec![
                ScalarValue::Utf8(Some("k".into())),
                ScalarValue::TimestampNanosecond(Some(hour_ns), None),
            ])],
        );
        let projected = aligned.project(&mapping, &fixture.eq_properties);
        assert_eq!(
            projected.to_string(),
            "Range([key@0 ASC, time_bin@1 ASC], [(k, 1704070800000000000)], 2)"
        );

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

/// Partition counts are `usize` in memory and `u64` on the wire, so every
/// counted [`Partitioning`] variant crosses a width boundary in both
/// directions. These pin that neither crossing wraps or panics.
#[cfg(all(test, feature = "proto"))]
mod partition_count_proto_tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_proto_models::protobuf;

    use super::{Partitioning, partition_count, wire_partition_count};
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
    fn partition_count_round_trips_at_the_usize_ceiling() {
        // `usize::MAX` is the largest count that can exist in memory, so it has
        // to widen onto the wire and narrow back unchanged.
        let wire = wire_partition_count(usize::MAX).unwrap();
        assert_eq!(wire, u64::try_from(usize::MAX).unwrap());
        assert_eq!(partition_count(wire).unwrap(), usize::MAX);
    }

    #[test]
    fn out_of_range_partition_count_is_reported_not_wrapped() {
        // A count wider than the target's `usize` can only be reached by
        // decoding on a narrower host than the one that encoded. That used to
        // wrap (`as usize`) or panic (`unwrap`); it is an error now. On a
        // 64-bit target every `u64` fits, so the same input has to decode
        // losslessly instead of being rejected.
        let narrowed = partition_count(u64::MAX);

        #[cfg(target_pointer_width = "64")]
        assert_eq!(narrowed.unwrap(), usize::MAX);

        #[cfg(not(target_pointer_width = "64"))]
        assert!(
            narrowed
                .unwrap_err()
                .to_string()
                .contains("Partition count 18446744073709551615 exceeds usize::MAX")
        );
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
                    .contains("exceeds usize::MAX")
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
}
