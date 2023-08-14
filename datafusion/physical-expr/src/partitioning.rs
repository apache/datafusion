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

//! [`Partitioning`] and [`Distribution`] for physical expressions

use std::fmt;
use std::sync::Arc;

use crate::{
    expr_list_eq_strict_order, normalize_expr_with_equivalence_properties,
    EquivalenceProperties, PhysicalExpr,
};

/// Partitioning schemes supported by operators.
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
                                    .map(|e| {
                                        normalize_expr_with_equivalence_properties(
                                            e.clone(),
                                            eq_classes,
                                        )
                                    })
                                    .collect::<Vec<_>>();
                                let normalized_partition_exprs = partition_exprs
                                    .iter()
                                    .map(|e| {
                                        normalize_expr_with_equivalence_properties(
                                            e.clone(),
                                            eq_classes,
                                        )
                                    })
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

/// Distribution schemes
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
    /// Creates a Partitioning for this Distribution to satisfy itself
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
