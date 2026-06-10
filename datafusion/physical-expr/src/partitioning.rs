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

use crate::{
    EquivalenceProperties, PhysicalExpr, equivalence::ProjectionMapping,
    expressions::UnKnownColumn, physical_exprs_equal,
};
pub use datafusion_common::SplitPoint;
use datafusion_common::{Result, validate_range_split_points};
use datafusion_physical_expr_common::physical_expr::format_physical_expr_list;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
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

    /// Returns true when `self` and `other` describe the same range partition
    /// map.
    ///
    /// Single-partition range partitionings are always compatible. Otherwise,
    /// the two partitionings must have identical split points and equivalent
    /// ordering expressions with the same sort options.
    pub fn compatible_with(
        &self,
        other: &Self,
        eq_properties: &EquivalenceProperties,
    ) -> bool {
        if self.partition_count() == 1 && other.partition_count() == 1 {
            return true;
        }

        if self.split_points != other.split_points
            || self.ordering.len() != other.ordering.len()
        {
            return false;
        }

        if !self
            .ordering
            .iter()
            .zip(other.ordering.iter())
            .all(|(left, right)| left.options == right.options)
        {
            return false;
        }

        let left_exprs = self
            .ordering
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect::<Vec<_>>();
        let right_exprs = other
            .ordering
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect::<Vec<_>>();

        equivalent_exprs(&left_exprs, &right_exprs, eq_properties)
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
            split_points: self.split_points.clone(),
        })
    }
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

    /// Returns true when `self` and `other` describe compatible partition maps.
    ///
    /// Compatible partition maps can be used for partition-local behavior: if
    /// this returns true, partition `i` from both partitionings can be treated
    /// as covering the same partition domain. This is stricter than
    /// [`Self::satisfaction`], which only answers whether this partitioning can
    /// satisfy a required distribution.
    pub fn compatible_with(
        &self,
        other: &Self,
        eq_properties: &EquivalenceProperties,
    ) -> bool {
        if self.partition_count() == 1 && other.partition_count() == 1 {
            return true;
        }

        match (self, other) {
            (
                Partitioning::Hash(left_exprs, left_count),
                Partitioning::Hash(right_exprs, right_count),
            ) => {
                if left_count != right_count {
                    return false;
                }
                if left_exprs.is_empty() || right_exprs.is_empty() {
                    return false;
                }
                equivalent_exprs(left_exprs, right_exprs, eq_properties)
            }
            (Partitioning::Range(left), Partitioning::Range(right)) => {
                left.compatible_with(right, eq_properties)
            }
            _ => false,
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

        subset_exprs.iter().all(|subset_expr| {
            superset_exprs
                .iter()
                .any(|superset_expr| subset_expr.eq(superset_expr))
        })
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
            // When partition count is 1, hash requirement is satisfied.
            Distribution::HashPartitioned(_) if self.partition_count() == 1 => {
                PartitioningSatisfaction::Exact
            }
            Distribution::HashPartitioned(required_exprs) => match self {
                // Here we do not check the partition count for hash partitioning and assumes the partition count
                // and hash functions in the system are the same. In future if we plan to support storage partition-wise joins,
                // then we need to have the partition count and hash functions validation.
                Partitioning::Hash(partition_exprs, _) => {
                    // Empty hash partitioning is invalid
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
                Partitioning::RoundRobinBatch(_)
                | Partitioning::Range(_)
                | Partitioning::UnknownPartitioning(_) => {
                    PartitioningSatisfaction::NotSatisfied
                }
            },
            Distribution::SinglePartition => PartitioningSatisfaction::NotSatisfied,
        }
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
    /// Requires children to be distributed in such a way that the same
    /// values of the keys end up in the same partition
    HashPartitioned(Vec<Arc<dyn PhysicalExpr>>),
}

impl Distribution {
    /// Creates a `Partitioning` that satisfies this `Distribution`
    pub fn create_partitioning(self, partition_count: usize) -> Partitioning {
        match self {
            Distribution::UnspecifiedDistribution => {
                Partitioning::UnknownPartitioning(partition_count)
            }
            Distribution::SinglePartition => Partitioning::UnknownPartitioning(1),
            Distribution::HashPartitioned(expr) => {
                Partitioning::Hash(expr, partition_count)
            }
        }
    }
}

impl Display for Distribution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Distribution::UnspecifiedDistribution => write!(f, "Unspecified"),
            Distribution::SinglePartition => write!(f, "SinglePartition"),
            Distribution::HashPartitioned(exprs) => {
                write!(f, "HashPartitioned[{}])", format_physical_expr_list(exprs))
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
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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

        fn hash_distribution(
            &self,
            indices: impl IntoIterator<Item = usize>,
        ) -> Distribution {
            Distribution::HashPartitioned(self.cols(indices))
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

    #[test]
    fn partitioning_satisfy_distribution() -> Result<()> {
        let fixture = PartitioningTestFixture::new(vec![
            ("column_1", DataType::Int64),
            ("column_2", DataType::Utf8),
        ])?;

        let distribution_types = vec![
            Distribution::UnspecifiedDistribution,
            Distribution::SinglePartition,
            fixture.hash_distribution([0, 1]),
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
                Distribution::HashPartitioned(_) => {
                    assert_eq!(result, (true, false, false, true, false))
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_satisfy_by_subset() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b", "c"])?;

        let test_cases = vec![
            (
                "Hash([a]) vs Hash([a, b])",
                fixture.hash_partitioning([0], 4),
                fixture.hash_distribution([0, 1]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a]) vs Hash([a, b, c])",
                fixture.hash_partitioning([0], 4),
                fixture.hash_distribution([0, 1, 2]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b]) vs Hash([a, b, c])",
                fixture.hash_partitioning([0, 1], 4),
                fixture.hash_distribution([0, 1, 2]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([b]) vs Hash([a, b, c])",
                fixture.hash_partitioning([1], 4),
                fixture.hash_distribution([0, 1, 2]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([b, a]) vs Hash([a, b, c])",
                fixture.hash_partitioning([1, 0], 4),
                fixture.hash_distribution([0, 1, 2]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &fixture.eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &fixture.eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_current_superset() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b", "c"])?;

        let test_cases = vec![
            (
                "Hash([a, b]) vs Hash([a])",
                fixture.hash_partitioning([0, 1], 4),
                fixture.hash_distribution([0]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b, c]) vs Hash([a])",
                fixture.hash_partitioning([0, 1, 2], 4),
                fixture.hash_distribution([0]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b, c]) vs Hash([a, b])",
                fixture.hash_partitioning([0, 1, 2], 4),
                fixture.hash_distribution([0, 1]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &fixture.eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &fixture.eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_partial_overlap() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b", "c"])?;

        let test_cases = vec![(
            "Partial overlap: Hash([a, c]) vs Hash([a, b])",
            fixture.hash_partitioning([0, 2], 4),
            fixture.hash_distribution([0, 1]),
            PartitioningSatisfaction::NotSatisfied,
            PartitioningSatisfaction::NotSatisfied,
        )];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &fixture.eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &fixture.eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_no_overlap() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b", "c"])?;

        let test_cases = vec![
            (
                "Hash([a]) vs Hash([b, c])",
                fixture.hash_partitioning([0], 4),
                fixture.hash_distribution([1, 2]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b]) vs Hash([c])",
                fixture.hash_partitioning([0, 1], 4),
                fixture.hash_distribution([2]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &fixture.eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &fixture.eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_exact_match() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;

        let test_cases = vec![
            (
                "Hash([a, b]) vs Hash([a, b])",
                fixture.hash_partitioning([0, 1], 4),
                fixture.hash_distribution([0, 1]),
                PartitioningSatisfaction::Exact,
                PartitioningSatisfaction::Exact,
            ),
            (
                "Hash([a]) vs Hash([a])",
                fixture.hash_partitioning([0], 4),
                fixture.hash_distribution([0]),
                PartitioningSatisfaction::Exact,
                PartitioningSatisfaction::Exact,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &fixture.eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &fixture.eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_unknown() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let unknown: Arc<dyn PhysicalExpr> = Arc::new(UnKnownColumn::new("dropped"));

        let test_cases = vec![
            (
                "Hash([unknown]) vs Hash([a, b])",
                Partitioning::Hash(vec![Arc::clone(&unknown)], 4),
                fixture.hash_distribution([0, 1]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b]) vs Hash([unknown])",
                fixture.hash_partitioning([0, 1], 4),
                Distribution::HashPartitioned(vec![Arc::clone(&unknown)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([unknown]) vs Hash([unknown])",
                Partitioning::Hash(vec![Arc::clone(&unknown)], 4),
                Distribution::HashPartitioned(vec![Arc::clone(&unknown)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &fixture.eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &fixture.eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_empty_hash() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a"])?;

        let test_cases = vec![
            (
                "Hash([]) vs Hash([a])",
                Partitioning::Hash(vec![], 4),
                fixture.hash_distribution([0]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a]) vs Hash([])",
                fixture.hash_partitioning([0], 4),
                Distribution::HashPartitioned(vec![]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([]) vs Hash([])",
                Partitioning::Hash(vec![], 4),
                Distribution::HashPartitioned(vec![]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &fixture.eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &fixture.eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
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

    #[test]
    fn test_range_partitioning_compatible_with() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let mut eq_properties = fixture.eq_properties.clone();
        eq_properties.add_equal_conditions(fixture.col(0), fixture.col(1))?;

        let split_points = vec![int_split_point([10]), int_split_point([20])];
        let range_a = fixture.range([0], split_points.clone());
        let range_a_same = fixture.range([0], split_points.clone());
        let range_b_equivalent = fixture.range([1], split_points.clone());
        let range_b_different_split = fixture.range([1], vec![int_split_point([30])]);
        let range_a_desc = RangePartitioning::try_new(
            [fixture.range_sort_expr(0, SortOptions::new(true, false))].into(),
            vec![int_split_point([10])],
        )?;
        let single_partition_range_a = fixture.range([0], vec![]);
        let single_partition_range_b = fixture.range([1], vec![]);

        assert!(range_a.compatible_with(&range_a_same, &fixture.eq_properties));
        assert!(range_a.compatible_with(&range_b_equivalent, &eq_properties));
        assert!(!range_a.compatible_with(&range_b_equivalent, &fixture.eq_properties));
        assert!(!range_a.compatible_with(&range_b_different_split, &eq_properties));
        assert!(!range_a.compatible_with(&range_a_desc, &eq_properties));
        assert!(
            single_partition_range_a
                .compatible_with(&single_partition_range_b, &fixture.eq_properties)
        );

        assert!(
            fixture
                .range_partitioning([0], vec![int_split_point([10])])
                .compatible_with(
                    &fixture.range_partitioning([1], vec![int_split_point([10])]),
                    &eq_properties
                )
        );
        assert!(
            !fixture
                .range_partitioning([0], vec![int_split_point([10])])
                .compatible_with(
                    &fixture.range_partitioning([0], vec![int_split_point([20])]),
                    &fixture.eq_properties
                )
        );
        assert!(
            !fixture
                .range_partitioning([0], vec![int_split_point([10])])
                .compatible_with(
                    &fixture.hash_partitioning([0], 2),
                    &fixture.eq_properties
                )
        );

        Ok(())
    }

    #[test]
    fn test_hash_partitioning_compatible_with() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let mut eq_properties = fixture.eq_properties.clone();
        eq_properties.add_equal_conditions(fixture.col(0), fixture.col(1))?;

        assert!(
            fixture.hash_partitioning([0], 2).compatible_with(
                &fixture.hash_partitioning([0], 2),
                &fixture.eq_properties
            )
        );
        assert!(
            fixture
                .hash_partitioning([0], 2)
                .compatible_with(&fixture.hash_partitioning([1], 2), &eq_properties)
        );
        assert!(
            !fixture.hash_partitioning([0], 2).compatible_with(
                &fixture.hash_partitioning([1], 2),
                &fixture.eq_properties
            )
        );
        assert!(
            !fixture.hash_partitioning([0], 2).compatible_with(
                &fixture.hash_partitioning([0], 3),
                &fixture.eq_properties
            )
        );
        assert!(!fixture.hash_partitioning([0], 2).compatible_with(
            &fixture.hash_partitioning([0, 1], 2),
            &fixture.eq_properties
        ));
        assert!(
            !Partitioning::Hash(vec![], 2)
                .compatible_with(&Partitioning::Hash(vec![], 2), &fixture.eq_properties)
        );
        assert!(!fixture.hash_partitioning([0], 2).compatible_with(
            &fixture.range_partitioning([0], vec![int_split_point([10])]),
            &fixture.eq_properties
        ));
        assert!(
            fixture.hash_partitioning([0], 1).compatible_with(
                &Partitioning::RoundRobinBatch(1),
                &fixture.eq_properties
            )
        );

        Ok(())
    }

    #[test]
    fn test_round_robin_partitioning_compatible_with() {
        let eq_properties = EquivalenceProperties::new(Arc::new(Schema::empty()));

        assert!(
            Partitioning::RoundRobinBatch(1)
                .compatible_with(&Partitioning::RoundRobinBatch(1), &eq_properties)
        );
        assert!(
            !Partitioning::RoundRobinBatch(2)
                .compatible_with(&Partitioning::RoundRobinBatch(2), &eq_properties)
        );
        assert!(
            Partitioning::RoundRobinBatch(1)
                .compatible_with(&Partitioning::UnknownPartitioning(1), &eq_properties)
        );
        assert!(
            !Partitioning::RoundRobinBatch(2)
                .compatible_with(&Partitioning::UnknownPartitioning(2), &eq_properties)
        );
    }

    #[test]
    fn test_unknown_partitioning_compatible_with() {
        let eq_properties = EquivalenceProperties::new(Arc::new(Schema::empty()));

        assert!(
            Partitioning::UnknownPartitioning(1)
                .compatible_with(&Partitioning::UnknownPartitioning(1), &eq_properties)
        );
        assert!(
            !Partitioning::UnknownPartitioning(2)
                .compatible_with(&Partitioning::UnknownPartitioning(2), &eq_properties)
        );
        assert!(
            Partitioning::UnknownPartitioning(1)
                .compatible_with(&Partitioning::RoundRobinBatch(1), &eq_properties)
        );
        assert!(
            !Partitioning::UnknownPartitioning(2)
                .compatible_with(&Partitioning::RoundRobinBatch(2), &eq_properties)
        );
    }

    #[test]
    fn test_multi_partition_range_does_not_satisfy_hash_distribution() -> Result<()> {
        let fixture = PartitioningTestFixture::int64(&["a", "b"])?;
        let range_partitioning =
            fixture.range_partitioning([0, 1], vec![int_split_point([10, 100])]);
        let required = fixture.hash_distribution([0, 1]);

        assert_eq!(
            range_partitioning.satisfaction(&required, &fixture.eq_properties, false),
            PartitioningSatisfaction::NotSatisfied
        );

        Ok(())
    }
}
