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

use std::fmt;
use std::sync::Arc;

use crate::{expr_list_eq_strict_order, EquivalenceProperties, PhysicalExpr};

/// Output partitioning supported by [`ExecutionPlan`]s.
///
/// When `executed`, `ExecutionPlan`s  produce one or more independent stream of
/// data batches in parallel, referred to as partitions. The streams are Rust
/// `aync` [`Stream`]s (a special kind of future). The number of output
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
// ExecutionPlan with 3 output                     3 (async) streams, one for each
// partitions                                      output partition
/// ```
///
/// It is common (but not required) that an `ExecutionPlan` has the same number
/// of input partitions as output partitons. However, some plans have different
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
/// RepartitionExec with 3 output
/// partitions and 1 input partition                   3 (async) streams, that internally
///                                                    pull from the same input stream
///                                                                  ...
/// ```
///
/// # Additional Examples
///
/// A simple `FileScanExec` might produce one output stream (partition) for each
/// file (note the actual DataFusion file scaners can read individual files in
/// parallel, potentially producing multiple partitions per file)
///
/// Plans such as `SortPreservingMerge` produce a single output stream
/// (1 output partition) by combining some number of input streams (input partitions)
///
/// Plans such as `FilterExec` produce the same number of output streams
/// (partitions) as input streams (partitions).
///
/// [`ExecutionPlan`]: crate::physical_plan::ExecutionPlan
/// [`Stream`]: futures::stream::Stream
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

impl fmt::Display for Partitioning {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            RoundRobinBatch(n) | Hash(_, n) | UnknownPartitioning(n) => *n,
        }
    }

    /// Returns true when the guarantees made by this [[Partitioning]] are sufficient to
    /// satisfy the partitioning scheme mandated by the `required` [[Distribution]]
    pub fn satisfy<F: FnOnce() -> EquivalenceProperties>(
        &self,
        required: Distribution,
        equal_properties: F,
    ) -> bool {
        match required {
            Distribution::UnspecifiedDistribution => true,
            Distribution::SinglePartition if self.partition_count() == 1 => true,
            Distribution::HashPartitioned(required_exprs) => {
                match self {
                    // Here we do not check the partition count for hash partitioning and assumes the partition count
                    // and hash functions in the system are the same. In future if we plan to support storage partition-wise joins,
                    // then we need to have the partition count and hash functions validation.
                    Partitioning::Hash(partition_exprs, _) => {
                        let fast_match =
                            expr_list_eq_strict_order(&required_exprs, partition_exprs);
                        // If the required exprs do not match, need to leverage the eq_properties provided by the child
                        // and normalize both exprs based on the eq_properties
                        if !fast_match {
                            let eq_properties = equal_properties();
                            let eq_classes = eq_properties.classes();
                            if !eq_classes.is_empty() {
                                let normalized_required_exprs = required_exprs
                                    .iter()
                                    .map(|e| eq_properties.normalize_expr(e.clone()))
                                    .collect::<Vec<_>>();
                                let normalized_partition_exprs = partition_exprs
                                    .iter()
                                    .map(|e| eq_properties.normalize_expr(e.clone()))
                                    .collect::<Vec<_>>();
                                expr_list_eq_strict_order(
                                    &normalized_required_exprs,
                                    &normalized_partition_exprs,
                                )
                            } else {
                                fast_match
                            }
                        } else {
                            fast_match
                        }
                    }
                    _ => false,
                }
            }
            _ => false,
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
                if expr_list_eq_strict_order(exprs1, exprs2) && (count1 == count2) =>
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
    pub fn create_partitioning(&self, partition_count: usize) -> Partitioning {
        match self {
            Distribution::UnspecifiedDistribution => {
                Partitioning::UnknownPartitioning(partition_count)
            }
            Distribution::SinglePartition => Partitioning::UnknownPartitioning(1),
            Distribution::HashPartitioned(expr) => {
                Partitioning::Hash(expr.clone(), partition_count)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expressions::Column;

    use super::*;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use datafusion_common::Result;

    use std::sync::Arc;

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

        for distribution in distribution_types {
            let result = (
                single_partition.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
                unspecified_partition.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
                round_robin_partition.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
                hash_partition1.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
                hash_partition2.satisfy(distribution.clone(), || {
                    EquivalenceProperties::new(schema.clone())
                }),
            );

            match distribution {
                Distribution::UnspecifiedDistribution => {
                    assert_eq!(result, (true, true, true, true, true))
                }
                Distribution::SinglePartition => {
                    assert_eq!(result, (true, false, false, false, false))
                }
                Distribution::HashPartitioned(_) => {
                    assert_eq!(result, (false, false, false, true, false))
                }
            }
        }

        Ok(())
    }
}
