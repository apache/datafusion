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
    equivalence::ProjectionMapping, expressions::UnKnownColumn, physical_exprs_equal,
    EquivalenceProperties, PhysicalExpr,
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
    /// Partitioning where each partition contains exactly one distinct value of the partitioning
    SingleValuePartitioned(Vec<Arc<dyn PhysicalExpr>>, usize),
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
            Partitioning::SingleValuePartitioned(phy_exprs, size) => {
                let phy_exprs_str = phy_exprs
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "SingleValuePartitioned([{phy_exprs_str}], {size})")
            }
            Partitioning::UnknownPartitioning(size) => {
                write!(f, "UnknownPartitioning({size})")
            }
        }
    }
}
impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            RoundRobinBatch(n)
            | Hash(_, n)
            | SingleValuePartitioned(_, n)
            | UnknownPartitioning(n) => *n,
        }
    }

    /// Returns true when the guarantees made by this [`Partitioning`] are sufficient to
    /// satisfy the partitioning scheme mandated by the `required` [`Distribution`].
    pub fn satisfy(
        &self,
        required: &Distribution,
        eq_properties: &EquivalenceProperties,
    ) -> bool {
        match required {
            Distribution::UnspecifiedDistribution => true,
            Distribution::SinglePartition if self.partition_count() == 1 => true,
            // When partition count is 1, hash requirement is satisfied.
            Distribution::HashPartitioned(_) if self.partition_count() == 1 => true,
            Distribution::HashPartitioned(required_exprs) => {
                match self {
                    // Here we do not check the partition count for hash partitioning and assumes the partition count
                    // and hash functions in the system are the same. In future if we plan to support storage partition-wise joins,
                    // then we need to have the partition count and hash functions validation.
                    Partitioning::Hash(partition_exprs, _) => {
                        let fast_match =
                            physical_exprs_equal(required_exprs, partition_exprs);
                        // If the required exprs do not match, need to leverage the eq_properties provided by the child
                        // and normalize both exprs based on the equivalent groups.
                        if !fast_match {
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

                                // Only exact match for Hash partitioning
                                // Subset matching is NOT safe for general hash partitioning
                                return physical_exprs_equal(
                                    &normalized_required_exprs,
                                    &normalized_partition_exprs,
                                );
                            }
                        }
                        fast_match
                    }
                    // SingleValuePartitioned supports subset matching because each partition
                    // contains exactly one distinct value of the partition columns
                    Partitioning::SingleValuePartitioned(partition_exprs, _) => {
                        let fast_match =
                            physical_exprs_equal(required_exprs, partition_exprs);
                        if fast_match {
                            return true;
                        }

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

                            normalized_partition_exprs.iter().all(|part_expr| {
                                normalized_required_exprs
                                    .iter()
                                    .any(|req_expr| req_expr.eq(part_expr))
                            })
                        } else {
                            // No equivalence groups, check subset matching with original expressions
                            partition_exprs.iter().all(|part_expr| {
                                required_exprs
                                    .iter()
                                    .any(|req_expr| req_expr.eq(part_expr))
                            })
                        }
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    /// Calculate the output partitioning after applying the given projection.
    pub fn project(
        &self,
        mapping: &ProjectionMapping,
        input_eq_properties: &EquivalenceProperties,
    ) -> Self {
        match self {
            Partitioning::Hash(exprs, part)
            | Partitioning::SingleValuePartitioned(exprs, part) => {
                let normalized_exprs: Vec<Arc<dyn PhysicalExpr>> = input_eq_properties
                    .project_expressions(exprs, mapping)
                    .zip(exprs.iter())
                    .map(|(proj_expr, expr)| {
                        if let Some(projected) = proj_expr {
                            projected
                        } else {
                            Arc::new(UnKnownColumn::new(&expr.to_string()))
                        }
                    })
                    .collect();
                // Preserve the partitioning type through projection
                match self {
                    Partitioning::Hash(_, _) => {
                        Partitioning::Hash(normalized_exprs, *part)
                    }
                    Partitioning::SingleValuePartitioned(_, _) => {
                        Partitioning::SingleValuePartitioned(normalized_exprs, *part)
                    }
                    _ => unreachable!(),
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
            (
                Partitioning::SingleValuePartitioned(exprs1, count1),
                Partitioning::SingleValuePartitioned(exprs2, count2),
            ) if physical_exprs_equal(exprs1, exprs2) && (count1 == count2) => true,
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
                single_partition.satisfy(&distribution, &eq_properties),
                unspecified_partition.satisfy(&distribution, &eq_properties),
                round_robin_partition.satisfy(&distribution, &eq_properties),
                hash_partition1.satisfy(&distribution, &eq_properties),
                hash_partition2.satisfy(&distribution, &eq_properties),
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
    fn partitioning_satisfy_hash_vs_single_value() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Float64, false),
        ]));

        let col_a = Arc::new(Column::new_with_schema("a", &schema).unwrap())
            as Arc<dyn PhysicalExpr>;
        let col_b = Arc::new(Column::new_with_schema("b", &schema).unwrap())
            as Arc<dyn PhysicalExpr>;
        let col_c = Arc::new(Column::new_with_schema("c", &schema).unwrap())
            as Arc<dyn PhysicalExpr>;

        let eq_properties = EquivalenceProperties::new(schema);

        // Case 1: Hash([a]) should NOT satisfy Hash([a, b])
        let hash_a = Partitioning::Hash(vec![Arc::clone(&col_a)], 4);
        let required_ab =
            Distribution::HashPartitioned(vec![Arc::clone(&col_a), Arc::clone(&col_b)]);
        assert!(
            !hash_a.satisfy(&required_ab, &eq_properties),
            "Hash([a]) should NOT satisfy Hash([a,b]) - subset matching not safe for hash partitioning"
        );

        // Case 2: Hash([a]) SHOULD satisfy Hash([a])
        let required_a = Distribution::HashPartitioned(vec![Arc::clone(&col_a)]);
        assert!(
            hash_a.satisfy(&required_a, &eq_properties),
            "Hash([a]) should satisfy Hash([a]) - exact match"
        );

        // Case 3: Hash([a, b]) should satisfy Hash([a, b])
        let hash_ab = Partitioning::Hash(vec![Arc::clone(&col_a), Arc::clone(&col_b)], 4);
        assert!(
            hash_ab.satisfy(&required_ab, &eq_properties),
            "Hash([a,b]) should satisfy Hash([a,b]) - exact match"
        );

        // Case 4: Hash([a, b]) should NOT satisfy Hash([a])
        assert!(
            !hash_ab.satisfy(&required_a, &eq_properties),
            "Hash([a,b]) should NOT satisfy Hash([a]) - subset matching not allowed for Hash"
        );

        // Case 5: SingleValuePartitioned([a]) should satisfy Hash([a, b])
        let single_a = Partitioning::SingleValuePartitioned(vec![Arc::clone(&col_a)], 4);
        assert!(
            single_a.satisfy(&required_ab, &eq_properties),
            "SingleValuePartitioned([a]) should satisfy Hash([a,b]) - subset matching is safe"
        );

        // Case 6: SingleValuePartitioned([a]) should satisfy Hash([a, b, c])
        let required_abc = Distribution::HashPartitioned(vec![
            Arc::clone(&col_a),
            Arc::clone(&col_b),
            Arc::clone(&col_c),
        ]);
        assert!(
            single_a.satisfy(&required_abc, &eq_properties),
            "SingleValuePartitioned([a]) should satisfy Hash([a,b,c])"
        );

        // Case 7: SingleValuePartitioned([a, b]) should satisfy Hash([a, b])
        let single_ab = Partitioning::SingleValuePartitioned(
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            4,
        );
        assert!(
            single_ab.satisfy(&required_ab, &eq_properties),
            "SingleValuePartitioned([a,b]) should satisfy Hash([a,b]) - exact match"
        );

        // Case 8: SingleValuePartitioned([a, b]) should NOT satisfy Hash([a])
        assert!(
            !single_ab.satisfy(&required_a, &eq_properties),
            "SingleValuePartitioned([a,b]) should NOT satisfy Hash([a]) - partition has extra columns"
        );

        // Case 9: SingleValuePartitioned([a, c]) should satisfy Hash([a, b, c])
        let single_ac = Partitioning::SingleValuePartitioned(
            vec![Arc::clone(&col_a), Arc::clone(&col_c)],
            4,
        );
        assert!(
            single_ac.satisfy(&required_abc, &eq_properties),
            "SingleValuePartitioned([a,c]) should satisfy Hash([a,b,c]) - subset match"
        );

        // Case 10: SingleValuePartitioned([a, c]) should NOT satisfy Hash([b])
        let required_b = Distribution::HashPartitioned(vec![Arc::clone(&col_b)]);
        assert!(
            !single_ac.satisfy(&required_b, &eq_properties),
            "SingleValuePartitioned([a,c]) should NOT satisfy Hash([b]) - no common columns"
        );

        Ok(())
    }
}
