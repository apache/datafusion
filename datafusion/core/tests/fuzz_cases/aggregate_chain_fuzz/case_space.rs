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
//! The case space: every axis a case varies along and the valid combinations.

use super::*;

pub(super) const ROWS: usize = 32 * 1024;
pub(super) const PARTITIONS: usize = 4;
pub(super) const BATCH_SIZE: usize = 64;
/// The fair pool caps every spillable consumer at `pool / consumers`, and a
/// chain registers up to twenty consumers (aggregate streams plus one per
/// repartition channel). The cap has to clear a small table's legitimate
/// footprint, which at very low cardinality is dominated by the `count
/// distinct` sets and grows in steps of roughly 100 KB, while a final table at
/// very high cardinality must still exceed it.
pub(super) const LIMITED_POOL_BYTES: usize = 4 * 1024 * 1024;

/// How the source data is ordered relative to the group keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Order {
    /// Not ordered. Aggregates see `InputOrderMode::Linear`.
    Unordered,
    /// Sorted by the first key only. Aggregates see
    /// `InputOrderMode::PartiallySorted([0])`.
    SortedByFirstKey,
    /// Sorted by all keys. Aggregates see `InputOrderMode::Sorted`.
    SortedByAllKeys,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Cardinality {
    VeryHigh,
    Medium,
    Low,
    VeryLow,
}

impl Cardinality {
    pub(super) const ALL: [Self; 4] =
        [Self::VeryHigh, Self::Medium, Self::Low, Self::VeryLow];

    /// Number of distinct `(k1, k2)` groups.
    pub(super) fn groups(self) -> usize {
        match self {
            Self::VeryHigh => ROWS,
            Self::Medium => ROWS / 32,
            Self::Low => 16,
            Self::VeryLow => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Memory {
    /// Unlimited pool. Nothing spills or emits early.
    Unlimited,
    /// Pool sized so final and single hash tables cannot fit.
    Limited,
}

/// One operator in a chain, listed bottom to top.
#[derive(Clone, Copy, Debug)]
pub(super) enum Operator {
    Aggregate(AggregateMode),
    /// `AggregateExec` with `limit_options` set, which selects
    /// `GroupedTopKAggregateStream` regardless of mode.
    TopK(AggregateMode),
    /// `RepartitionExec` hashed on the group keys. Destroys ordering.
    HashRepartition,
    /// `RepartitionExec` hashed on the group keys with `preserve_order`.
    OrderPreservingHashRepartition,
    /// `CoalescePartitionsExec`. Destroys ordering.
    CoalescePartitions,
    /// `SortPreservingMergeExec` on the current ordering.
    SortPreservingMerge,
}

/// The `GROUP BY` keys. Every key type has its own `GroupValues`
/// implementation, so each is a value of this axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Keys {
    /// No `GROUP BY`.
    None,
    /// `k1, k2` (two Int64), handled by `GroupValuesColumn`.
    TwoInts,
    /// `b` (Boolean), handled by `GroupValuesBoolean`.
    Boolean,
    /// `s` (Utf8), handled by `GroupValuesBytes`.
    Bytes,
    /// `sv` (Utf8View), handled by `GroupValuesBytesView`.
    BytesView,
    /// `p` (Int64 with as many distinct values as groups), handled by
    /// `GroupValuesPrimitive`.
    Primitive,
    /// `b, s, sv, p`, handled by `GroupValuesColumn` with mixed column types.
    Mixed,
    /// `st` (Struct of a List<Int64> and an Int64), which no specialized
    /// implementation supports, so it falls back to the row format
    /// `GroupValuesRows`.
    Struct,
}

impl Keys {
    pub(super) const ALL: [Self; 8] = [
        Self::None,
        Self::TwoInts,
        Self::Boolean,
        Self::Bytes,
        Self::BytesView,
        Self::Primitive,
        Self::Mixed,
        Self::Struct,
    ];

    /// Key columns, in `GROUP BY` order.
    pub(super) fn columns(self) -> &'static [&'static str] {
        match self {
            Keys::None => &[],
            Keys::TwoInts => &["k1", "k2"],
            Keys::Boolean => &["b"],
            Keys::Bytes => &["s"],
            Keys::BytesView => &["sv"],
            Keys::Primitive => &["p"],
            Keys::Mixed => &["b", "s", "sv", "p"],
            Keys::Struct => &["st"],
        }
    }

    /// Whether the source can be sorted by the keys. Struct columns cannot be
    /// sorted by the arrow sort kernels, so those keys only run unordered.
    pub(super) fn sortable(self) -> bool {
        self != Keys::Struct
    }

    /// Whether the number of groups is `Cardinality::groups()`. Every key
    /// column has one distinct value per group except the Boolean one.
    pub(super) fn tracks_cardinality(self) -> bool {
        !matches!(self, Keys::None | Keys::Boolean)
    }

    /// Whether `GroupedTopKAggregateStream` supports these keys: exactly one
    /// primitive or string column.
    pub(super) fn top_k_supported(self) -> bool {
        matches!(self, Keys::Bytes | Keys::BytesView | Keys::Primitive)
    }
}

/// The aggregate expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Aggregates {
    /// count, count distinct, sum, avg, min, max: non-trivial partial state so
    /// the Partial, PartialReduce and Final stages are actually exercised.
    /// `avg` (two-field state) and `count distinct` (set state) matter most.
    All,
    /// No aggregate expressions, as `SELECT DISTINCT` plans: the
    /// accumulator-free path of every stream.
    None,
    /// `max(v)` only, the one aggregate the TopK stream supports. Chains using
    /// `Operator::TopK` set a limit larger than any possible group count, so
    /// the result must still be the complete aggregate.
    Max,
}

impl Aggregates {
    pub(super) const ALL: [Self; 3] = [Self::All, Self::None, Self::Max];
}

/// The logical query a chain computes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Query {
    pub(super) keys: Keys,
    pub(super) aggregates: Aggregates,
}

/// Larger than any possible number of groups, so TopK keeps every group.
pub(super) const TOP_K_LIMIT: usize = 2 * ROWS;

/// Everything that varies for a case apart from the shape itself. Passed as a
/// struct so a new dimension does not change every shape predicate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct CaseParams {
    pub(super) order: Order,
    pub(super) cardinality: Cardinality,
    pub(super) memory: Memory,
    /// Whether the skip-partial probe may fire. Only varied for shapes with a
    /// grouped `Partial` stage on Linear input, since nothing else runs it.
    pub(super) skip_partial_enabled: bool,
}

/// An operator chain, independent of the query it computes.
#[derive(Clone, Copy, Debug)]
pub(super) struct Chain {
    pub(super) name: &'static str,
    pub(super) operators: &'static [Operator],
    /// Source partition count.
    pub(super) source_partitions: usize,
}

pub(super) const fn chain(
    name: &'static str,
    operators: &'static [Operator],
    source_partitions: usize,
) -> Chain {
    Chain {
        name,
        operators,
        source_partitions,
    }
}

impl Chain {
    /// Whether the chain hashes or sorts on the group keys, so it cannot run
    /// without any.
    pub(super) fn needs_keys(&self) -> bool {
        self.operators.iter().any(|operator| {
            matches!(
                operator,
                HashRepartition
                    | OrderPreservingHashRepartition
                    | SortPreservingMerge
                    | TopK(_)
            )
        })
    }

    pub(super) fn is_top_k(&self) -> bool {
        self.operators
            .iter()
            .any(|operator| matches!(operator, TopK(_)))
    }

    /// Whether the chain keeps the source ordering through its shuffles, so
    /// it only makes sense on ordered input.
    pub(super) fn preserves_order(&self) -> bool {
        self.operators.iter().any(|operator| {
            matches!(
                operator,
                OrderPreservingHashRepartition | SortPreservingMerge
            )
        })
    }

    /// Whether some case of this chain must spill: a final or single hash
    /// stage on unordered input, whose very-high-cardinality table cannot fit
    /// the limited pool. Ordered stages emit early or are bounded, and TopK
    /// keeps a bounded heap, so those chains never spill.
    pub(super) fn expects_spill(&self) -> bool {
        !self.preserves_order()
            && self.operators.iter().any(|operator| {
                matches!(
                    operator,
                    Aggregate(Final | FinalPartitioned | Single | SinglePartitioned)
                )
            })
    }
}

/// A plan shape: a chain computing a query.
#[derive(Clone, Copy, Debug)]
pub(super) struct Shape {
    pub(super) chain: Chain,
    pub(super) query: Query,
}

impl Shape {
    pub(super) fn name(&self) -> String {
        format!(
            "{} {:?} {:?}",
            self.chain.name, self.query.keys, self.query.aggregates
        )
    }

    /// Source orders that make sense for this shape. Order-preserving shuffles
    /// need an ordering to preserve; no-grouping chains ignore ordering.
    pub(super) fn orders(&self) -> Vec<Order> {
        let keys = self.query.keys.columns();
        let mut orders = vec![];
        if !self.chain.preserves_order() {
            orders.push(Order::Unordered);
        }
        if self.query.keys.sortable() && !keys.is_empty() {
            // With a single key, sorting by the first key is already sorting
            // by all keys.
            if keys.len() > 1 {
                orders.push(Order::SortedByFirstKey);
            }
            orders.push(Order::SortedByAllKeys);
        }
        orders
    }

    /// Whether some `Partial` stage of this shape runs the skip-partial probe
    /// for the given source order: grouped, not TopK, and Linear input.
    pub(super) fn has_skip_partial_candidate(&self, order: Order) -> bool {
        if self.query.keys == Keys::None {
            return false;
        }
        let mut current = order;
        for operator in self.chain.operators {
            match operator {
                HashRepartition | CoalescePartitions => current = Order::Unordered,
                Aggregate(Partial) if current == Order::Unordered => return true,
                _ => {}
            }
        }
        false
    }
}

/// Every query the chain can be planned for.
///
/// - Without keys a chain can neither hash nor sort, and `max` alone is a
///   subset of the full aggregate list, so only that list runs.
/// - The TopK stream needs one primitive or string key with either a single
///   `max` or no aggregates at all (the `DISTINCT ... LIMIT` form).
/// - Every other chain runs the full aggregate list and the accumulator-free
///   form; `max` alone adds nothing there.
pub(super) fn shapes(chain: Chain) -> Vec<Shape> {
    let mut shapes = vec![];
    for keys in Keys::ALL {
        for aggregates in Aggregates::ALL {
            let valid = if keys == Keys::None {
                !chain.needs_keys() && aggregates == Aggregates::All
            } else if chain.is_top_k() {
                keys.top_k_supported() && aggregates != Aggregates::All
            } else {
                aggregates != Aggregates::Max
            };
            if valid {
                shapes.push(Shape {
                    chain,
                    query: Query { keys, aggregates },
                });
            }
        }
    }
    shapes
}

#[derive(Clone, Debug)]
pub(super) struct Case {
    pub(super) shape: Shape,
    pub(super) params: CaseParams,
}

/// Every case of the chain: each valid query over every source order,
/// cardinality, memory budget and skip-partial setting.
pub(super) fn cases(chain: Chain) -> Vec<Case> {
    // `AGGREGATE_CHAIN_SHAPES=a,b` restricts the run to shapes whose name
    // contains one of the given substrings, to reproduce or bisect quickly.
    let shape_filter: Vec<String> = std::env::var("AGGREGATE_CHAIN_SHAPES")
        .map(|value| value.split(',').map(str::to_string).collect())
        .unwrap_or_default();
    let mut cases = vec![];
    for shape in shapes(chain).into_iter().filter(|shape| {
        shape_filter.is_empty()
            || shape_filter
                .iter()
                .any(|needle| shape.name().contains(needle))
    }) {
        for order in shape.orders() {
            let skip_partial_variants: &[bool] =
                if shape.has_skip_partial_candidate(order) {
                    &[true, false]
                } else {
                    &[true]
                };
            for cardinality in Cardinality::ALL {
                for memory in [Memory::Unlimited, Memory::Limited] {
                    for &skip_partial_enabled in skip_partial_variants {
                        cases.push(Case {
                            shape,
                            params: CaseParams {
                                order,
                                cardinality,
                                memory,
                                skip_partial_enabled,
                            },
                        });
                    }
                }
            }
        }
    }
    cases
}
