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

impl Order {
    pub(super) const ALL: [Self; 3] = [
        Self::Unordered,
        Self::SortedByFirstKey,
        Self::SortedByAllKeys,
    ];
    /// For chains that preserve ordering, which need an ordering to preserve.
    pub(super) const SORTED: [Self; 2] = [Self::SortedByFirstKey, Self::SortedByAllKeys];
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

impl Memory {
    pub(super) const ALL: [Self; 2] = [Self::Unlimited, Self::Limited];
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
    /// Every `GROUP BY`, for chains that hash or sort on the keys.
    pub(super) const GROUPED: [Self; 7] = [
        Self::TwoInts,
        Self::Boolean,
        Self::Bytes,
        Self::BytesView,
        Self::Primitive,
        Self::Mixed,
        Self::Struct,
    ];
    /// The keys `GroupedTopKAggregateStream` supports: one primitive or
    /// string column.
    pub(super) const TOP_K: [Self; 3] = [Self::Bytes, Self::BytesView, Self::Primitive];

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
    /// For hash chains: `max` alone is a subset of `All` and adds nothing.
    pub(super) const HASH: [Self; 2] = [Self::All, Self::None];
    /// For TopK chains, which support a single `max` or no aggregates.
    pub(super) const TOP_K: [Self; 2] = [Self::Max, Self::None];
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

    /// The subset of `requested` the source can be arranged in for these keys.
    /// Struct keys cannot be sorted, and with a single key sorting by the
    /// first key is already sorting by all keys.
    pub(super) fn orders(&self, requested: &[Order]) -> Vec<Order> {
        let keys = self.query.keys;
        requested
            .iter()
            .copied()
            .filter(|order| match order {
                Order::Unordered => {
                    assert!(
                        !self.chain.preserves_order(),
                        "{}: an order-preserving chain needs sorted input",
                        self.chain.name
                    );
                    true
                }
                Order::SortedByFirstKey => keys.sortable() && keys.columns().len() > 1,
                Order::SortedByAllKeys => keys.sortable() && !keys.columns().is_empty(),
            })
            .collect()
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

/// One test: a chain and the axes it runs over. Every field is a list so a
/// test reads as the cases it covers, and narrowing a list runs just those.
pub(super) struct ChainTest {
    pub(super) chain: Chain,
    pub(super) group_by: &'static [Keys],
    pub(super) aggregates: &'static [Aggregates],
    pub(super) orders: &'static [Order],
    pub(super) cardinalities: &'static [Cardinality],
    pub(super) memory: &'static [Memory],
    /// Whether the skip-partial probe may fire. Only a grouped `Partial`
    /// stage on unordered input runs it; elsewhere the setting changes
    /// nothing and only the first value runs.
    pub(super) skip_partial_config: &'static [bool],
}

#[derive(Clone, Debug)]
pub(super) struct Case {
    pub(super) shape: Shape,
    pub(super) params: CaseParams,
}

impl ChainTest {
    /// Every query of the test: each key set with each aggregate list.
    /// Without keys only the full aggregate list runs: `max` alone is a
    /// subset of it and no aggregates at all is not a query.
    fn shapes(&self) -> Vec<Shape> {
        let chain = self.chain;
        let mut shapes = vec![];
        for &keys in self.group_by {
            assert!(
                keys != Keys::None || !chain.needs_keys(),
                "{}: the chain hashes or sorts on group keys, so it needs some",
                chain.name
            );
            assert!(
                !chain.is_top_k() || keys.top_k_supported(),
                "{}: TopK needs one primitive or string key, not {keys:?}",
                chain.name
            );
            for &aggregates in self.aggregates {
                assert!(
                    !chain.is_top_k() || aggregates != Aggregates::All,
                    "{}: TopK supports a single max or no aggregates, not {aggregates:?}",
                    chain.name
                );
                if keys == Keys::None && aggregates != Aggregates::All {
                    continue;
                }
                shapes.push(Shape {
                    chain,
                    query: Query { keys, aggregates },
                });
            }
        }
        shapes
    }

    /// Every case of the test: each query over each source order,
    /// cardinality, memory budget and skip-partial setting.
    pub(super) fn cases(&self) -> Vec<Case> {
        let mut cases = vec![];
        for shape in self.shapes() {
            for order in shape.orders(self.orders) {
                let skip_partial = if shape.has_skip_partial_candidate(order) {
                    self.skip_partial_config
                } else {
                    &self.skip_partial_config[..1]
                };
                for &cardinality in self.cardinalities {
                    for &memory in self.memory {
                        for &skip_partial_enabled in skip_partial {
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

    /// Whether some case must spill: the chain has a spill-capable stage on
    /// unordered input and the axes include the very-high-cardinality table
    /// under the limited pool that cannot fit.
    pub(super) fn expects_spill(&self) -> bool {
        self.chain.expects_spill()
            && self.orders.contains(&Order::Unordered)
            && self.cardinalities.contains(&Cardinality::VeryHigh)
            && self.memory.contains(&Memory::Limited)
            && self.group_by.iter().any(|keys| keys.tracks_cardinality())
    }
}
