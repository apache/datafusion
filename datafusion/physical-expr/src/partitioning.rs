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
use datafusion_common::ScalarValue;
use datafusion_physical_expr_common::physical_expr::format_physical_expr_list;
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
/// [`RangePartitioning`] describes range bounds over one or more physical
/// expressions. Each [`RangePartition`] represents one output partition and must
/// contain exactly one [`RangeInterval`] for each partition expression.
///
/// The source declaring this partitioning is responsible for ensuring that, for
/// every emitted row, the row belongs to exactly one partition and is emitted by
/// that partition. The declared ranges do not need to cover values that the plan
/// cannot emit.
///
/// Each lower and upper bound explicitly records whether it is inclusive.
/// Unbounded sides are represented with `None`, bound values should be non-null
/// until null routing semantics are defined.
///
/// For example, a scan can declare date and city range partitions as:
///
/// ```text
/// exprs = [date, city]
///
/// partition 0:
///   date in [2021-01-01, 2022-01-01)
///   city in [Allston, Boston)
///
/// partition 1:
///   date in [2021-01-01, 2022-01-01)
///   city in [Boston, NYC)
/// ```
///
/// NOTE: Optimizer and execution behavior for this partitioning is intentionally
/// not implemented and will be introduced incrementally. This public API keeps
/// the partition ranges explicit for users. Repartitioning may compile the same
/// metadata into a more efficient internal router.
#[derive(Debug, Clone)]
pub struct RangePartitioning {
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    partitions: Vec<RangePartition>,
}

impl RangePartitioning {
    /// Creates range partitioning metadata.
    ///
    /// The caller is responsible for ensuring each partition has one range per
    /// partition expression and for satisfying the contract documented on
    /// [`RangePartitioning`].
    pub fn new(
        partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
        partitions: Vec<RangePartition>,
    ) -> Self {
        Self {
            partition_exprs,
            partitions,
        }
    }

    /// Returns the partition expressions.
    pub fn partition_exprs(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_exprs
    }

    /// Returns the declared range partitions.
    pub fn partitions(&self) -> &[RangePartition] {
        &self.partitions
    }

    /// Returns the number of partitions.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    fn project(
        &self,
        mapping: &ProjectionMapping,
        input_eq_properties: &EquivalenceProperties,
    ) -> Option<Self> {
        let partition_exprs = input_eq_properties
            .project_expressions(&self.partition_exprs, mapping)
            .collect::<Option<Vec<_>>>()?;

        Some(Self {
            partition_exprs,
            partitions: self.partitions.clone(),
        })
    }
}

impl Display for RangePartitioning {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let partitions = self
            .partitions
            .iter()
            .map(|partition| format!("{partition}"))
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "Range({}, [{}], {})",
            format_physical_expr_list(&self.partition_exprs),
            partitions,
            self.partition_count()
        )
    }
}

impl PartialEq for RangePartitioning {
    fn eq(&self, other: &Self) -> bool {
        physical_exprs_equal(&self.partition_exprs, &other.partition_exprs)
            && self.partitions == other.partitions
    }
}

/// Ranges for one output partition in a [`RangePartitioning`].
#[derive(Debug, Clone, PartialEq)]
pub struct RangePartition {
    ranges: Vec<RangeInterval>,
}

impl RangePartition {
    /// Creates a partition from one range per partition expression.
    pub fn new(ranges: Vec<RangeInterval>) -> Self {
        Self { ranges }
    }

    /// Returns the ranges for this partition.
    pub fn ranges(&self) -> &[RangeInterval] {
        &self.ranges
    }
}

impl Display for RangePartition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ranges = self
            .ranges
            .iter()
            .map(|range| format!("{range}"))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "({ranges})")
    }
}

/// A scalar interval in one range partition dimension.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeInterval {
    lower: Option<RangeBound>,
    upper: Option<RangeBound>,
}

impl RangeInterval {
    /// Creates a range interval with optional lower and upper bounds.
    pub fn new(lower: Option<RangeBound>, upper: Option<RangeBound>) -> Self {
        Self { lower, upper }
    }

    /// Returns the lower bound, if any.
    pub fn lower(&self) -> Option<&RangeBound> {
        self.lower.as_ref()
    }

    /// Returns the upper bound, if any.
    pub fn upper(&self) -> Option<&RangeBound> {
        self.upper.as_ref()
    }
}

impl Display for RangeInterval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let lower_bracket = match self.lower() {
            Some(bound) if bound.inclusive() => "[",
            _ => "(",
        };
        let lower = self
            .lower()
            .map(|bound| bound.value().to_string())
            .unwrap_or_else(|| "-inf".to_string());
        let upper = self
            .upper()
            .map(|bound| bound.value().to_string())
            .unwrap_or_else(|| "+inf".to_string());
        let upper_bracket = match self.upper() {
            Some(bound) if bound.inclusive() => "]",
            _ => ")",
        };
        write!(f, "{lower_bracket}{lower}, {upper}{upper_bracket}")
    }
}

/// A scalar range bound.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeBound {
    value: ScalarValue,
    inclusive: bool,
}

impl RangeBound {
    /// Creates a scalar range bound.
    pub fn new(value: ScalarValue, inclusive: bool) -> Self {
        Self { value, inclusive }
    }

    /// Returns the bound value.
    pub fn value(&self) -> &ScalarValue {
        &self.value
    }

    /// Returns whether this bound is inclusive.
    pub fn inclusive(&self) -> bool {
        self.inclusive
    }
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

                    // Fast path: exact match
                    if physical_exprs_equal(required_exprs, partition_exprs) {
                        return PartitioningSatisfaction::Exact;
                    }

                    // Normalization path using equivalence groups
                    let eq_groups = eq_properties.eq_group();
                    if !eq_groups.is_empty() {
                        let normalized_required_exprs = required_exprs
                            .iter()
                            .map(|e| eq_groups.normalize_expr(Arc::clone(e)))
                            .collect::<Vec<_>>();
                        let normalized_partition_exprs = partition_exprs
                            .iter()
                            .map(|e| eq_groups.normalize_expr(Arc::clone(e)))
                            .collect::<Vec<_>>();
                        if physical_exprs_equal(
                            &normalized_required_exprs,
                            &normalized_partition_exprs,
                        ) {
                            return PartitioningSatisfaction::Exact;
                        }

                        if allow_subset
                            && Self::is_subset_partitioning(
                                &normalized_partition_exprs,
                                &normalized_required_exprs,
                            )
                        {
                            return PartitioningSatisfaction::Subset;
                        }
                    } else if allow_subset
                        && Self::is_subset_partitioning(partition_exprs, required_exprs)
                    {
                        return PartitioningSatisfaction::Subset;
                    }

                    PartitioningSatisfaction::NotSatisfied
                }
                _ => PartitioningSatisfaction::NotSatisfied,
            },
            _ => PartitioningSatisfaction::NotSatisfied,
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
            _ => self.clone(),
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

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;

    #[test]
    fn partitioning_satisfy_distribution() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("column_1", DataType::Int64, false),
            Field::new("column_2", DataType::Utf8, false),
        ]));

        let partition_exprs1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new_with_schema("column_1", &schema).unwrap()),
            Arc::new(Column::new_with_schema("column_2", &schema).unwrap()),
        ];

        let partition_exprs2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new_with_schema("column_2", &schema).unwrap()),
            Arc::new(Column::new_with_schema("column_1", &schema).unwrap()),
        ];

        let distribution_types = vec![
            Distribution::UnspecifiedDistribution,
            Distribution::SinglePartition,
            Distribution::HashPartitioned(partition_exprs1.clone()),
        ];

        let single_partition = Partitioning::UnknownPartitioning(1);
        let unspecified_partition = Partitioning::UnknownPartitioning(10);
        let round_robin_partition = Partitioning::RoundRobinBatch(10);
        let hash_partition1 = Partitioning::Hash(partition_exprs1, 10);
        let hash_partition2 = Partitioning::Hash(partition_exprs2, 10);
        let eq_properties = EquivalenceProperties::new(schema);

        for distribution in distribution_types {
            let result = (
                single_partition
                    .satisfaction(&distribution, &eq_properties, true)
                    .is_satisfied(),
                unspecified_partition
                    .satisfaction(&distribution, &eq_properties, true)
                    .is_satisfied(),
                round_robin_partition
                    .satisfaction(&distribution, &eq_properties, true)
                    .is_satisfied(),
                hash_partition1
                    .satisfaction(&distribution, &eq_properties, true)
                    .is_satisfied(),
                hash_partition2
                    .satisfaction(&distribution, &eq_properties, true)
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));

        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("b", &schema)?);
        let col_c: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("c", &schema)?);
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let test_cases = vec![
            (
                "Hash([a]) vs Hash([a, b])",
                Partitioning::Hash(vec![Arc::clone(&col_a)], 4),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_a),
                    Arc::clone(&col_b),
                ]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a]) vs Hash([a, b, c])",
                Partitioning::Hash(vec![Arc::clone(&col_a)], 4),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_a),
                    Arc::clone(&col_b),
                    Arc::clone(&col_c),
                ]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b]) vs Hash([a, b, c])",
                Partitioning::Hash(vec![Arc::clone(&col_a), Arc::clone(&col_b)], 4),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_a),
                    Arc::clone(&col_b),
                    Arc::clone(&col_c),
                ]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([b]) vs Hash([a, b, c])",
                Partitioning::Hash(vec![Arc::clone(&col_b)], 4),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_a),
                    Arc::clone(&col_b),
                    Arc::clone(&col_c),
                ]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([b, a]) vs Hash([a, b, c])",
                Partitioning::Hash(vec![Arc::clone(&col_a)], 4),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_a),
                    Arc::clone(&col_b),
                    Arc::clone(&col_c),
                ]),
                PartitioningSatisfaction::Subset,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_current_superset() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));

        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("b", &schema)?);
        let col_c: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("c", &schema)?);
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let test_cases = vec![
            (
                "Hash([a, b]) vs Hash([a])",
                Partitioning::Hash(vec![Arc::clone(&col_a), Arc::clone(&col_b)], 4),
                Distribution::HashPartitioned(vec![Arc::clone(&col_a)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b, c]) vs Hash([a])",
                Partitioning::Hash(
                    vec![Arc::clone(&col_a), Arc::clone(&col_b), Arc::clone(&col_c)],
                    4,
                ),
                Distribution::HashPartitioned(vec![Arc::clone(&col_a)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b, c]) vs Hash([a, b])",
                Partitioning::Hash(
                    vec![Arc::clone(&col_a), Arc::clone(&col_b), Arc::clone(&col_c)],
                    4,
                ),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_a),
                    Arc::clone(&col_b),
                ]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_partial_overlap() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));

        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("b", &schema)?);
        let col_c: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("c", &schema)?);
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let test_cases = vec![(
            "Partial overlap: Hash([a, c]) vs Hash([a, b])",
            Partitioning::Hash(vec![Arc::clone(&col_a), Arc::clone(&col_c)], 4),
            Distribution::HashPartitioned(vec![Arc::clone(&col_a), Arc::clone(&col_b)]),
            PartitioningSatisfaction::NotSatisfied,
            PartitioningSatisfaction::NotSatisfied,
        )];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_no_overlap() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));

        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("b", &schema)?);
        let col_c: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("c", &schema)?);
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let test_cases = vec![
            (
                "Hash([a]) vs Hash([b, c])",
                Partitioning::Hash(vec![Arc::clone(&col_a)], 4),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_b),
                    Arc::clone(&col_c),
                ]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b]) vs Hash([c])",
                Partitioning::Hash(vec![Arc::clone(&col_a), Arc::clone(&col_b)], 4),
                Distribution::HashPartitioned(vec![Arc::clone(&col_c)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_exact_match() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("b", &schema)?);
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let test_cases = vec![
            (
                "Hash([a, b]) vs Hash([a, b])",
                Partitioning::Hash(vec![Arc::clone(&col_a), Arc::clone(&col_b)], 4),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_a),
                    Arc::clone(&col_b),
                ]),
                PartitioningSatisfaction::Exact,
                PartitioningSatisfaction::Exact,
            ),
            (
                "Hash([a]) vs Hash([a])",
                Partitioning::Hash(vec![Arc::clone(&col_a)], 4),
                Distribution::HashPartitioned(vec![Arc::clone(&col_a)]),
                PartitioningSatisfaction::Exact,
                PartitioningSatisfaction::Exact,
            ),
        ];

        for (desc, partition, required, expected_with_subset, expected_without_subset) in
            test_cases
        {
            let result = partition.satisfaction(&required, &eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_unknown() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("b", &schema)?);
        let unknown: Arc<dyn PhysicalExpr> = Arc::new(UnKnownColumn::new("dropped"));
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let test_cases = vec![
            (
                "Hash([unknown]) vs Hash([a, b])",
                Partitioning::Hash(vec![Arc::clone(&unknown)], 4),
                Distribution::HashPartitioned(vec![
                    Arc::clone(&col_a),
                    Arc::clone(&col_b),
                ]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a, b]) vs Hash([unknown])",
                Partitioning::Hash(vec![Arc::clone(&col_a), Arc::clone(&col_b)], 4),
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
            let result = partition.satisfaction(&required, &eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    #[test]
    fn test_partitioning_empty_hash() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let test_cases = vec![
            (
                "Hash([]) vs Hash([a])",
                Partitioning::Hash(vec![], 4),
                Distribution::HashPartitioned(vec![Arc::clone(&col_a)]),
                PartitioningSatisfaction::NotSatisfied,
                PartitioningSatisfaction::NotSatisfied,
            ),
            (
                "Hash([a]) vs Hash([])",
                Partitioning::Hash(vec![Arc::clone(&col_a)], 4),
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
            let result = partition.satisfaction(&required, &eq_properties, true);
            assert_eq!(
                result, expected_with_subset,
                "Failed for {desc} with subset enabled"
            );

            let result = partition.satisfaction(&required, &eq_properties, false);
            assert_eq!(
                result, expected_without_subset,
                "Failed for {desc} with subset disabled"
            );
        }

        Ok(())
    }

    fn int_bound(value: i64, inclusive: bool) -> RangeBound {
        RangeBound::new(ScalarValue::Int64(Some(value)), inclusive)
    }

    fn int_interval(lower: Option<i64>, upper: Option<i64>) -> RangeInterval {
        RangeInterval::new(
            lower.map(|value| int_bound(value, true)),
            upper.map(|value| int_bound(value, false)),
        )
    }

    fn range_partition(ranges: Vec<RangeInterval>) -> RangePartition {
        RangePartition::new(ranges)
    }

    #[test]
    fn test_range_partitioning_metadata() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);

        let range_partitioning = RangePartitioning::new(
            vec![Arc::clone(&col_a)],
            vec![
                range_partition(vec![int_interval(None, Some(10))]),
                range_partition(vec![int_interval(Some(10), None)]),
            ],
        );
        let partitioning = Partitioning::Range(range_partitioning);

        assert_eq!(partitioning.partition_count(), 2);
        assert_eq!(
            partitioning.to_string(),
            "Range([a@0], [((-inf, 10)), ([10, +inf))], 2)"
        );

        Ok(())
    }

    #[test]
    fn test_range_partitioning_project_preserves_or_degrades() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("b", &schema)?);
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let range_partitioning = Partitioning::Range(RangePartitioning::new(
            vec![col_b],
            vec![range_partition(vec![int_interval(None, Some(10))])],
        ));

        let keep_b_mapping = ProjectionMapping::from_indices(&[1], &schema)?;
        let projected = range_partitioning.project(&keep_b_mapping, &eq_properties);
        assert_eq!(projected.to_string(), "Range([b@0], [((-inf, 10))], 1)");

        let drop_b_mapping = ProjectionMapping::from_indices(&[0], &schema)?;
        let projected = range_partitioning.project(&drop_b_mapping, &eq_properties);
        let Partitioning::UnknownPartitioning(partition_count) = projected else {
            panic!("expected UnknownPartitioning, got {projected:?}");
        };
        assert_eq!(partition_count, 1);

        Ok(())
    }

    #[test]
    fn test_multi_partition_range_does_not_satisfy_hash_distribution() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("a", &schema)?);
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("b", &schema)?);

        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let range_partitioning = Partitioning::Range(RangePartitioning::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            vec![
                range_partition(vec![
                    int_interval(None, Some(10)),
                    int_interval(None, Some(100)),
                ]),
                range_partition(vec![
                    int_interval(Some(10), None),
                    int_interval(Some(100), None),
                ]),
            ],
        ));
        let required = Distribution::HashPartitioned(vec![col_a, col_b]);

        assert_eq!(
            range_partitioning.satisfaction(&required, &eq_properties, false),
            PartitioningSatisfaction::NotSatisfied
        );

        Ok(())
    }
}
