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
            Partitioning::UnknownPartitioning(size) => {
                write!(f, "UnknownPartitioning({size})")
            }
        }
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
        if let Partitioning::Hash(exprs, part) = self {
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
        } else {
            self.clone()
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
}
