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

//! Fuzz test that runs every valid `AggregateExec` chain over the same data
//! and asserts identical results.
//!
//! See `aggregation_fuzzer/AGGREGATE_CHAINS.md` for the chain catalogue. Each
//! chain there is a `Shape` below plus a source `Order`. Cases are generated
//! as `shapes × orders × migration flag × cardinality × memory`.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    BooleanArray, Int64Array, Int64Builder, ListBuilder, RecordBatch, StringArray,
    StringViewArray, StructArray, UInt32Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{SortColumn, lexsort_to_indices, take_record_batch};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, SortOptions};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::prelude::SessionConfig;
use datafusion_common::Result;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_common_runtime::JoinSet;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{FairSpillPool, TrackConsumersPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{cast, col};
use datafusion_physical_expr::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, InputOrderMode, collect, displayable};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use AggregateMode::*;
use Operator::*;

// ---------------------------------------------------------------------------
// Case space
// ---------------------------------------------------------------------------

const ROWS: usize = 64 * 1024;
const PARTITIONS: usize = 4;
const BATCH_SIZE: usize = 64;
/// The fair pool caps every spillable consumer at `pool / consumers`, and a
/// chain registers up to twenty consumers (aggregate streams plus one per
/// repartition channel). The cap has to clear a small table's legitimate
/// footprint, which at very low cardinality is dominated by the `count
/// distinct` sets and grows in steps of roughly 100 KB, while a final table at
/// very high cardinality must still exceed it.
const LIMITED_POOL_BYTES: usize = 4 * 1024 * 1024;

/// How the source data is ordered relative to the group keys `(k1, k2)`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Order {
    /// Not ordered. Aggregates see `InputOrderMode::Linear`.
    Unordered,
    /// Sorted by `k1` only. Aggregates see `InputOrderMode::PartiallySorted([0])`.
    SortedByFirstKey,
    /// Sorted by `k1, k2`. Aggregates see `InputOrderMode::Sorted`.
    SortedByAllKeys,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Cardinality {
    VeryHigh,
    High,
    Medium,
    Low,
    VeryLow,
}

impl Cardinality {
    const ALL: [Self; 5] = [
        Self::VeryHigh,
        Self::High,
        Self::Medium,
        Self::Low,
        Self::VeryLow,
    ];

    /// Number of distinct `(k1, k2)` groups.
    fn groups(self) -> usize {
        match self {
            Self::VeryHigh => ROWS,
            Self::High => ROWS / 2,
            Self::Medium => ROWS / 32,
            Self::Low => 16,
            Self::VeryLow => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Memory {
    /// Unlimited pool. Nothing spills or emits early.
    Unlimited,
    /// Pool sized so final and single hash tables cannot fit.
    Limited,
}

/// One operator in a chain, listed bottom to top.
#[derive(Clone, Copy, Debug)]
enum Operator {
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

/// The logical query a chain computes.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Query {
    /// `GROUP BY k1, k2` with count, count distinct, sum, avg, min, max.
    /// Two primitive keys, handled by `GroupValuesColumn`.
    Grouped,
    /// The same aggregates without `GROUP BY`.
    NoGrouping,
    /// `GROUP BY k1` with `max(v)` only, the shape the TopK stream supports.
    /// Chains using `Operator::TopK` set a limit larger than any possible
    /// group count, so the result must still be the complete aggregate.
    TopK,
    /// `GROUP BY b` (Boolean), handled by `GroupValuesBoolean`.
    BooleanKey,
    /// `GROUP BY s` (Utf8), handled by `GroupValuesBytes`.
    BytesKey,
    /// `GROUP BY sv` (Utf8View), handled by `GroupValuesBytesView`.
    BytesViewKey,
    /// `GROUP BY p` (Int64 with as many distinct values as groups), handled
    /// by `GroupValuesPrimitive`.
    PrimitiveKey,
    /// `GROUP BY b, s, sv, p`, handled by `GroupValuesColumn` with mixed
    /// column types.
    MixedKeys,
    /// `GROUP BY st` (Struct of a List<Int64> and an Int64), which no
    /// specialized implementation supports, so it falls back to the row format
    /// `GroupValuesRows`.
    StructKey,
}

impl Query {
    /// Group key columns, in `GROUP BY` order.
    fn keys(self) -> &'static [&'static str] {
        match self {
            Query::Grouped => &["k1", "k2"],
            Query::NoGrouping => &[],
            Query::TopK => &["k1"],
            Query::BooleanKey => &["b"],
            Query::BytesKey => &["s"],
            Query::BytesViewKey => &["sv"],
            Query::PrimitiveKey => &["p"],
            Query::MixedKeys => &["b", "s", "sv", "p"],
            Query::StructKey => &["st"],
        }
    }

    /// Whether the source can be sorted by the keys. Struct columns cannot be
    /// sorted by the arrow sort kernels, so that query only runs unordered.
    fn sortable(self) -> bool {
        self != Query::StructKey
    }
}

/// Larger than any possible number of groups, so TopK keeps every group.
const TOP_K_LIMIT: usize = 2 * ROWS;

/// A plan shape.
#[derive(Debug)]
struct Shape {
    name: &'static str,
    operators: &'static [Operator],
    /// Source partition count.
    source_partitions: usize,
    query: Query,
}

const fn shape(
    name: &'static str,
    operators: &'static [Operator],
    source_partitions: usize,
    query: Query,
) -> Shape {
    Shape {
        name,
        operators,
        source_partitions,
        query,
    }
}

/// Every shape from AGGREGATE_CHAINS.md. The ordering variants there come from
/// crossing a shape with `Order`, so one entry here covers several rows.
const SHAPES: &[Shape] = &[
    shape("single", &[Aggregate(Single)], 1, Query::Grouped),
    shape(
        "single_partitioned",
        &[HashRepartition, Aggregate(SinglePartitioned)],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "single_partitioned_order_preserving",
        &[OrderPreservingHashRepartition, Aggregate(SinglePartitioned)],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "partial_repartition_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "partial_coalesce_final",
        &[Aggregate(Partial), CoalescePartitions, Aggregate(Final)],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "partial_order_preserving_repartition_final",
        &[
            Aggregate(Partial),
            OrderPreservingHashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "partial_sort_preserving_merge_final",
        &[Aggregate(Partial), SortPreservingMerge, Aggregate(Final)],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "partial_final_single_partition",
        &[Aggregate(Partial), Aggregate(Final)],
        1,
        Query::Grouped,
    ),
    shape(
        "partial_repartition_reduce_repartition_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(PartialReduce),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "partial_repartition_reduce_coalesce_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(PartialReduce),
            CoalescePartitions,
            Aggregate(Final),
        ],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "partial_local_reduce_repartition_final",
        &[
            Aggregate(Partial),
            Aggregate(PartialReduce),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::Grouped,
    ),
    // ordered PartialReduce has no dedicated stream, lands on the fallback
    shape(
        "partial_reduce_final_order_preserving",
        &[
            Aggregate(Partial),
            OrderPreservingHashRepartition,
            Aggregate(PartialReduce),
            OrderPreservingHashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::Grouped,
    ),
    shape(
        "no_grouping_single",
        &[Aggregate(Single)],
        1,
        Query::NoGrouping,
    ),
    // TopK: same query without a limit is the reference for the TopK chains
    shape("top_k_query_single", &[Aggregate(Single)], 1, Query::TopK),
    shape("top_k_single", &[TopK(Single)], 1, Query::TopK),
    // planner shape: the limit lands on the aggregate under the sort
    shape(
        "top_k_partial_repartition_final",
        &[Aggregate(Partial), HashRepartition, TopK(FinalPartitioned)],
        PARTITIONS,
        Query::TopK,
    ),
    shape(
        "top_k_partial_coalesce_final",
        &[Aggregate(Partial), CoalescePartitions, TopK(Final)],
        PARTITIONS,
        Query::TopK,
    ),
    shape(
        "top_k_both_stages",
        &[TopK(Partial), HashRepartition, TopK(FinalPartitioned)],
        PARTITIONS,
        Query::TopK,
    ),
    // group key types: single stage and the default two-stage planner shape
    shape(
        "boolean_key_single",
        &[Aggregate(Single)],
        1,
        Query::BooleanKey,
    ),
    shape(
        "boolean_key_partial_repartition_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::BooleanKey,
    ),
    shape("bytes_key_single", &[Aggregate(Single)], 1, Query::BytesKey),
    shape(
        "bytes_key_partial_repartition_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::BytesKey,
    ),
    shape(
        "bytes_view_key_single",
        &[Aggregate(Single)],
        1,
        Query::BytesViewKey,
    ),
    shape(
        "bytes_view_key_partial_repartition_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::BytesViewKey,
    ),
    shape(
        "primitive_key_single",
        &[Aggregate(Single)],
        1,
        Query::PrimitiveKey,
    ),
    shape(
        "primitive_key_partial_repartition_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::PrimitiveKey,
    ),
    shape(
        "mixed_keys_single",
        &[Aggregate(Single)],
        1,
        Query::MixedKeys,
    ),
    shape(
        "mixed_keys_partial_repartition_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::MixedKeys,
    ),
    // ordered multi-column group values
    shape(
        "mixed_keys_partial_order_preserving_repartition_final",
        &[
            Aggregate(Partial),
            OrderPreservingHashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::MixedKeys,
    ),
    shape(
        "struct_key_single",
        &[Aggregate(Single)],
        1,
        Query::StructKey,
    ),
    shape(
        "struct_key_partial_repartition_final",
        &[
            Aggregate(Partial),
            HashRepartition,
            Aggregate(FinalPartitioned),
        ],
        PARTITIONS,
        Query::StructKey,
    ),
    shape(
        "no_grouping_partial_coalesce_final",
        &[Aggregate(Partial), CoalescePartitions, Aggregate(Final)],
        PARTITIONS,
        Query::NoGrouping,
    ),
    shape(
        "no_grouping_partial_reduce_final",
        &[
            Aggregate(Partial),
            CoalescePartitions,
            Aggregate(PartialReduce),
            CoalescePartitions,
            Aggregate(Final),
        ],
        PARTITIONS,
        Query::NoGrouping,
    ),
    shape(
        "no_grouping_partial_final_single_partition",
        &[Aggregate(Partial), Aggregate(Final)],
        1,
        Query::NoGrouping,
    ),
];

fn shape_by_name(name: &str) -> &'static Shape {
    SHAPES.iter().find(|shape| shape.name == name).unwrap()
}

impl Shape {
    /// Source orders that make sense for this shape. Order-preserving shuffles
    /// need an ordering to preserve; no-grouping chains ignore ordering.
    fn orders(&self) -> Vec<Order> {
        let needs_ordered_input = self.operators.iter().any(|operator| {
            matches!(
                operator,
                OrderPreservingHashRepartition | SortPreservingMerge
            )
        });
        let keys = self.query.keys();
        let mut orders = vec![];
        if !needs_ordered_input {
            orders.push(Order::Unordered);
        }
        if self.query.sortable() && !keys.is_empty() {
            // With a single key, sorting by the first key is already sorting
            // by all keys.
            if keys.len() > 1 {
                orders.push(Order::SortedByFirstKey);
            }
            orders.push(Order::SortedByAllKeys);
        }
        orders
    }
}

#[derive(Clone, Debug)]
struct Case {
    shape: &'static Shape,
    order: Order,
    migration_enabled: bool,
    cardinality: Cardinality,
    memory: Memory,
    /// Whether the skip-partial probe may fire. Only varied for shapes with a
    /// grouped `Partial` stage on Linear input, since nothing else runs the
    /// probe.
    skip_partial_enabled: bool,
}

impl Shape {
    /// Whether some `Partial` stage of this shape runs the skip-partial probe
    /// for the given source order: grouped, not TopK, and Linear input.
    fn has_skip_partial_candidate(&self, order: Order) -> bool {
        if self.query == Query::NoGrouping {
            return false;
        }
        let mut current = order;
        for operator in self.operators {
            match operator {
                HashRepartition | CoalescePartitions => current = Order::Unordered,
                Aggregate(Partial) if current == Order::Unordered => return true,
                _ => {}
            }
        }
        false
    }
}

fn all_cases() -> Vec<Case> {
    // `AGGREGATE_CHAIN_SHAPES=a,b` restricts the run to shapes whose name
    // contains one of the given substrings, to reproduce or bisect quickly.
    let shape_filter: Vec<String> = std::env::var("AGGREGATE_CHAIN_SHAPES")
        .map(|value| value.split(',').map(str::to_string).collect())
        .unwrap_or_default();
    let mut cases = vec![];
    for shape in SHAPES.iter().filter(|shape| {
        shape_filter.is_empty()
            || shape_filter
                .iter()
                .any(|needle| shape.name.contains(needle))
    }) {
        for order in shape.orders() {
            let skip_partial_variants: &[bool] =
                if shape.has_skip_partial_candidate(order) {
                    &[true, false]
                } else {
                    &[true]
                };
            for migration_enabled in [true, false] {
                for cardinality in Cardinality::ALL {
                    for memory in [Memory::Unlimited, Memory::Limited] {
                        for &skip_partial_enabled in skip_partial_variants {
                            cases.push(Case {
                                shape,
                                order,
                                migration_enabled,
                                cardinality,
                                memory,
                                skip_partial_enabled,
                            });
                        }
                    }
                }
            }
        }
    }
    cases
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

/// `k1 Int64 nullable, k2 Int64 nullable, v Int64`
/// `k1, k2 Int64` (two-key query), `v Int64` (aggregated), and one column per
/// key type: `b Boolean`, `s Utf8`, `sv Utf8View`, `p Int64`, and
/// `st Struct<list: List<Int64>, num: Int64>`.
/// Every key column is nullable.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, true),
        Field::new("k2", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
        Field::new("b", DataType::Boolean, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("sv", DataType::Utf8View, true),
        Field::new("p", DataType::Int64, true),
        Field::new_struct("st", struct_fields(), true),
    ]))
}

/// About 3% nulls.
fn not_null(rng: &mut StdRng) -> bool {
    rng.random_range(0..100) >= 3
}

fn struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("list", DataType::new_list(DataType::Int64, true), true),
        Field::new("num", DataType::Int64, true),
    ])
}

/// The raw rows for one cardinality, deterministic per seed. The same multiset
/// is used for every `Order` and `Shape` so results are comparable.
///
/// Requirements:
/// - exactly `ROWS` rows
/// - `cardinality.groups()` distinct `(k1, k2)` pairs, spread so that `k1`
///   alone has fewer distinct values than `(k1, k2)`. Otherwise
///   `SortedByFirstKey` degenerates into `SortedByAllKeys`.
/// - some nulls in `k1` and `k2`
fn generate_rows(cardinality: Cardinality, seed: u64) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(seed);
    let groups = cardinality.groups();
    // `k2` cycles through at most sqrt(groups) values, so `k1` alone has fewer
    // distinct values than the `(k1, k2)` pair.
    let k2_values = (groups as f64).sqrt().ceil().max(2.0) as i64;

    let mut k1 = Vec::with_capacity(ROWS);
    let mut k2 = Vec::with_capacity(ROWS);
    let mut v = Vec::with_capacity(ROWS);
    let mut b = Vec::with_capacity(ROWS);
    let mut s = Vec::with_capacity(ROWS);
    let mut sv = Vec::with_capacity(ROWS);
    let mut p = Vec::with_capacity(ROWS);
    let mut st_list = ListBuilder::new(Int64Builder::new());
    let mut st_num = Vec::with_capacity(ROWS);
    let mut st_valid = Vec::with_capacity(ROWS);
    for row in 0..ROWS {
        let group = (row % groups) as i64;
        k1.push(not_null(&mut rng).then_some(group / k2_values));
        k2.push(not_null(&mut rng).then_some(group % k2_values));
        v.push(rng.random_range(-1_000i64..1_000));
        // every key-type column has `groups` distinct values (boolean: two)
        b.push(not_null(&mut rng).then_some(group % 2 == 0));
        s.push(not_null(&mut rng).then(|| format!("s{group:06}")));
        sv.push(not_null(&mut rng).then(|| format!("sv{group:06}")));
        p.push(not_null(&mut rng).then_some(group));
        // struct { list: [group, group + 1], [] or null; num: group or null }
        st_valid.push(not_null(&mut rng));
        match rng.random_range(0..100) {
            0..3 => st_list.append_null(),
            3..6 => st_list.append(true),
            _ => {
                st_list.values().append_value(group);
                st_list.values().append_value(group + 1);
                st_list.append(true);
            }
        }
        st_num.push(not_null(&mut rng).then_some(group));
    }
    let st = StructArray::try_new(
        struct_fields(),
        vec![
            Arc::new(st_list.finish()),
            Arc::new(Int64Array::from(st_num)),
        ],
        Some(NullBuffer::from(st_valid)),
    )
    .unwrap();

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(k1)),
            Arc::new(Int64Array::from(k2)),
            Arc::new(Int64Array::from(v)),
            Arc::new(BooleanArray::from(b)),
            Arc::new(StringArray::from(s)),
            Arc::new(StringViewArray::from(sv)),
            Arc::new(Int64Array::from(p)),
            Arc::new(st),
        ],
    )
    .unwrap()
}

/// Arrange `rows` for the given `order` and split into `partitions` partitions
/// of `BATCH_SIZE` batches.
///
/// - `Unordered`: shuffle rows, round-robin into partitions
/// - `SortedByFirstKey`: sort by `k1` (nulls first), contiguous slice per partition
/// - `SortedByAllKeys`: sort by `k1, k2` (nulls first), contiguous slice per partition
///
/// Every partition individually satisfies the ordering.
fn arrange(
    rows: &RecordBatch,
    query: Query,
    order: Order,
    partitions: usize,
) -> Vec<Vec<RecordBatch>> {
    let schema = rows.schema();
    let per_partition: Vec<RecordBatch> = match source_ordering(&schema, query, order) {
        None => {
            let mut permutation: Vec<u32> = (0..rows.num_rows() as u32).collect();
            permutation.shuffle(&mut StdRng::seed_from_u64(0));
            let shuffled =
                take_record_batch(rows, &UInt32Array::from(permutation)).unwrap();
            (0..partitions)
                .map(|partition| {
                    let indices: UInt32Array = (partition as u32
                        ..shuffled.num_rows() as u32)
                        .step_by(partitions)
                        .collect();
                    take_record_batch(&shuffled, &indices).unwrap()
                })
                .collect()
        }
        Some(ordering) => {
            let sort_columns: Vec<SortColumn> = ordering
                .iter()
                .map(|sort_expr| SortColumn {
                    values: sort_expr
                        .expr
                        .evaluate(rows)
                        .unwrap()
                        .into_array(rows.num_rows())
                        .unwrap(),
                    options: Some(sort_expr.options),
                })
                .collect();
            let indices = lexsort_to_indices(&sort_columns, None).unwrap();
            let sorted = take_record_batch(rows, &indices).unwrap();
            let per_partition = sorted.num_rows().div_ceil(partitions);
            (0..partitions)
                .map(|partition| {
                    let start = (partition * per_partition).min(sorted.num_rows());
                    let length = per_partition.min(sorted.num_rows() - start);
                    sorted.slice(start, length)
                })
                .collect()
        }
    };

    per_partition
        .iter()
        .map(|partition| {
            (0..partition.num_rows())
                .step_by(BATCH_SIZE)
                .map(|start| {
                    partition.slice(start, BATCH_SIZE.min(partition.num_rows() - start))
                })
                .collect()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Plan construction
// ---------------------------------------------------------------------------

fn sort_expr(schema: &Schema, column: &str) -> PhysicalSortExpr {
    PhysicalSortExpr::new(
        col(column, schema).unwrap(),
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )
}

/// The ordering the source declares for `order`.
fn source_ordering(schema: &Schema, query: Query, order: Order) -> Option<LexOrdering> {
    let keys = query.keys();
    let sort_columns: &[&str] = match order {
        Order::Unordered => return None,
        Order::SortedByFirstKey => &keys[..1],
        Order::SortedByAllKeys => keys,
    };
    LexOrdering::new(sort_columns.iter().map(|column| sort_expr(schema, column)))
}

fn source(
    partitions: &[Vec<RecordBatch>],
    query: Query,
    order: Order,
) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let mut memory_source =
        MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap();
    if let Some(ordering) = source_ordering(&schema, query, order) {
        memory_source = memory_source
            .try_with_sort_information(vec![ordering])
            .unwrap();
    }
    DataSourceExec::from_data_source(memory_source)
}

fn group_by(schema: &Schema, query: Query) -> PhysicalGroupBy {
    PhysicalGroupBy::new_single(
        query
            .keys()
            .iter()
            .map(|key| (col(key, schema).unwrap(), key.to_string()))
            .collect(),
    )
}

/// Aggregates with non-trivial partial state so the Partial, PartialReduce and
/// Final stages are actually exercised. `avg` (two-field state) and
/// `count distinct` (set state) matter most.
fn aggregates(schema: &SchemaRef, query: Query) -> Vec<Arc<AggregateFunctionExpr>> {
    let value_column = || vec![col("v", schema).unwrap()];
    let build = |builder: AggregateExprBuilder, alias: &str| {
        Arc::new(
            builder
                .schema(Arc::clone(schema))
                .alias(alias)
                .build()
                .unwrap(),
        )
    };
    if query == Query::TopK {
        // TopK supports exactly one min/max aggregate over a non-nullable input
        return vec![build(
            AggregateExprBuilder::new(max_udaf(), value_column()),
            "max",
        )];
    }
    vec![
        build(
            AggregateExprBuilder::new(count_udaf(), value_column()),
            "count",
        ),
        build(
            AggregateExprBuilder::new(count_udaf(), value_column()).distinct(),
            "count_distinct",
        ),
        build(AggregateExprBuilder::new(sum_udaf(), value_column()), "sum"),
        // avg has no Int64 groups accumulator; the values are small integers so
        // the Float64 sum stays exact and the result is order-independent.
        build(
            AggregateExprBuilder::new(
                avg_udaf(),
                vec![cast(col("v", schema).unwrap(), schema, DataType::Float64).unwrap()],
            ),
            "avg",
        ),
        build(AggregateExprBuilder::new(min_udaf(), value_column()), "min"),
        build(AggregateExprBuilder::new(max_udaf(), value_column()), "max"),
    ]
}

/// Folds `shape.operators` bottom-up into a plan. The group-by, aggregate
/// expressions and hash keys are rewritten after every aggregate stage so the
/// next stage consumes that stage's output.
fn build_plan(shape: &Shape, input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let input_schema = schema();
    let mut plan = input;
    let mut group_by = group_by(&input_schema, shape.query);
    let mut aggregates = aggregates(&input_schema, shape.query);
    let mut hash_keys: Vec<Arc<dyn PhysicalExpr>> = group_by.input_exprs();

    for operator in shape.operators {
        plan = match operator {
            Aggregate(mode) | TopK(mode) => {
                let limit_options = matches!(operator, TopK(_))
                    .then(|| LimitOptions::new_with_order(TOP_K_LIMIT, true));
                let aggregate = Arc::new(
                    AggregateExec::try_new(
                        *mode,
                        group_by.clone(),
                        aggregates.clone(),
                        vec![None; aggregates.len()],
                        plan,
                        Arc::clone(&input_schema),
                    )
                    .unwrap()
                    .with_limit_options(limit_options),
                );
                group_by = aggregate.group_expr().as_final();
                aggregates = aggregate.aggr_expr().to_vec();
                hash_keys = aggregate.output_group_expr();
                aggregate
            }
            HashRepartition => Arc::new(
                RepartitionExec::try_new(
                    plan,
                    Partitioning::Hash(hash_keys.clone(), PARTITIONS),
                )
                .unwrap(),
            ),
            OrderPreservingHashRepartition => Arc::new(
                RepartitionExec::try_new(
                    plan,
                    Partitioning::Hash(hash_keys.clone(), PARTITIONS),
                )
                .unwrap()
                .with_preserve_order(),
            ),
            CoalescePartitions => Arc::new(CoalescePartitionsExec::new(plan)),
            SortPreservingMerge => {
                let ordering = plan.properties().output_ordering().cloned().unwrap();
                Arc::new(SortPreservingMergeExec::new(ordering, plan))
            }
        };
    }
    plan
}

// ---------------------------------------------------------------------------
// Execution context
// ---------------------------------------------------------------------------

fn task_context(case: &Case) -> Arc<TaskContext> {
    let config = SessionConfig::new()
        .with_batch_size(BATCH_SIZE)
        .with_target_partitions(PARTITIONS)
        .set_bool(
            "datafusion.execution.enable_migration_aggregate",
            case.migration_enabled,
        )
        // The default is 100k rows. Lower it so the skip-partial probe can
        // fire on our per-partition row counts. A ratio threshold of 1.0
        // disables the probe entirely.
        .set_usize(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            1024,
        );
    let mut config = config;
    config
        .options_mut()
        .execution
        .skip_partial_aggregation_probe_ratio_threshold =
        if case.skip_partial_enabled { 0.8 } else { 1.0 };

    let runtime = match case.memory {
        Memory::Unlimited => RuntimeEnvBuilder::new(),
        // Small enough that a very-high-cardinality final table spills, large
        // enough that the legacy stream can still reserve its sort headroom
        // and that RepartitionExec / SortPreservingMergeExec succeed. The
        // fair pool keeps one stage from starving the others.
        Memory::Limited => {
            RuntimeEnvBuilder::new().with_memory_pool(Arc::new(TrackConsumersPool::new(
                FairSpillPool::new(LIMITED_POOL_BYTES),
                NonZeroUsize::new(5).unwrap(),
            )))
        }
    }
    .build_arc()
    .unwrap();

    Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    )
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/// All `AggregateExec` nodes in the plan, bottom-up.
fn aggregate_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut nodes = vec![];
    let mut node = Arc::clone(plan);
    loop {
        if node.downcast_ref::<AggregateExec>().is_some() {
            nodes.push(Arc::clone(&node));
        }
        match node.children().first() {
            Some(child) => node = Arc::clone(child),
            None => break,
        }
    }
    nodes.reverse();
    nodes
}

fn as_aggregate(node: &Arc<dyn ExecutionPlan>) -> &AggregateExec {
    node.downcast_ref::<AggregateExec>().unwrap()
}

/// Expected source order seen by each aggregate stage, bottom-up. Ordering is
/// lost at `HashRepartition` and `CoalescePartitions`, and kept by the
/// order-preserving shuffles and by aggregate stages themselves.
fn expected_orders(shape: &Shape, source_order: Order) -> Vec<Order> {
    let mut current = source_order;
    let mut expected = vec![];
    for operator in shape.operators {
        match operator {
            HashRepartition | CoalescePartitions => current = Order::Unordered,
            Aggregate(_) | TopK(_) => expected.push(current),
            OrderPreservingHashRepartition | SortPreservingMerge => {}
        }
    }
    expected
}

fn order_matches(query: Query, expected: Order, actual: &InputOrderMode) -> bool {
    // With a single group key, sorting by the first key already covers every
    // group key.
    let single_key = query.keys().len() == 1;
    match (expected, actual) {
        (Order::Unordered, InputOrderMode::Linear) => true,
        (Order::SortedByFirstKey, InputOrderMode::PartiallySorted(indices)) => {
            !single_key && indices == &[0]
        }
        (Order::SortedByFirstKey, InputOrderMode::Sorted) => single_key,
        (Order::SortedByAllKeys, InputOrderMode::Sorted) => true,
        _ => false,
    }
}

/// Whether this stage's stream is allowed to spill. See the memory table in
/// AGGREGATE_CHAINS.md.
fn can_spill(case: &Case, aggregate: &AggregateExec) -> bool {
    if aggregate.limit_options().is_some() {
        // GroupedTopKAggregateStream keeps a bounded heap and never spills
        return false;
    }
    let spilling_mode = match aggregate.mode() {
        Final | FinalPartitioned | Single | SinglePartitioned => true,
        // The dedicated PartialReduce stream emits early and only exists for
        // Linear input; ordered input or migration off run the legacy stream,
        // which spills.
        PartialReduce => {
            !case.migration_enabled
                || *aggregate.input_order_mode() != InputOrderMode::Linear
        }
        Partial => false,
    };
    let has_groups = !aggregate.group_expr().is_empty();
    spilling_mode && has_groups && *aggregate.input_order_mode() != InputOrderMode::Sorted
}

/// Whether this stage runs the skip-partial probe.
fn runs_skip_partial_probe(aggregate: &AggregateExec) -> bool {
    *aggregate.mode() == Partial
        && aggregate.limit_options().is_none()
        && !aggregate.group_expr().is_empty()
        && *aggregate.input_order_mode() == InputOrderMode::Linear
}

fn check_plan_shape(case: &Case, plan: &Arc<dyn ExecutionPlan>) {
    if case.shape.query == Query::NoGrouping {
        return;
    }
    let nodes = aggregate_nodes(plan);
    let expected = expected_orders(case.shape, case.order);
    assert_eq!(nodes.len(), expected.len(), "{case:?}");
    for (node, expected_order) in nodes.iter().zip(expected) {
        let aggregate = as_aggregate(node);
        assert!(
            order_matches(
                case.shape.query,
                expected_order,
                aggregate.input_order_mode()
            ),
            "{case:?}: expected {expected_order:?} got {:?}\n{}",
            aggregate.input_order_mode(),
            displayable(plan.as_ref()).indent(true)
        );
    }
}

/// Returns a description of every stage that spilled, bottom-up, such as
/// `Final(Linear)`.
fn check_metrics(case: &Case, plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut spilled = vec![];
    for node in aggregate_nodes(plan) {
        let aggregate = as_aggregate(&node);
        let mode = aggregate.mode();
        let metrics = node.metrics().unwrap();
        let spill_count = metrics.spill_count().unwrap_or(0);
        if spill_count > 0 {
            spilled.push(format!("{mode:?}({:?})", aggregate.input_order_mode()));
        }
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|metric| metric.as_usize())
            .unwrap_or(0);

        match case.memory {
            Memory::Unlimited => {
                assert_eq!(spill_count, 0, "{case:?}: unexpected spill in {mode:?}");
            }
            Memory::Limited => {
                // Whether a spilling-capable stage actually spills depends on
                // the pool geometry, so only the run-wide coverage check in the
                // driver requires it. Streams that cannot spill must not.
                if !can_spill(case, aggregate) {
                    assert_eq!(spill_count, 0, "{case:?}: {mode:?} must never spill");
                }
            }
        }

        // Only the two-key query has as many groups as `cardinality` says;
        // the TopK query groups by `k1` alone and stays far below the ratio.
        if case.memory == Memory::Unlimited
            && case.cardinality == Cardinality::VeryHigh
            && case.shape.query == Query::Grouped
            && case.skip_partial_enabled
            && runs_skip_partial_probe(aggregate)
        {
            assert!(
                skipped_rows > 0,
                "{case:?}: skip-partial probe did not fire"
            );
        }
        if !case.skip_partial_enabled || !runs_skip_partial_probe(aggregate) {
            assert_eq!(skipped_rows, 0, "{case:?}: skip-partial fired in {mode:?}");
        }
    }
    spilled
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Sorted output plus the stages that spilled, empty if none did.
struct Outcome {
    output: String,
    spilled: Vec<String>,
}

/// Runs one case, checks plan shape and metrics, and returns its outcome.
///
/// Running out of memory is never accepted, not even under the limited pool:
/// every stream either spills, emits early, or is bounded, so an error there
/// is a bug in a stream's memory handling or in how stages share the pool.
async fn run_case(case: Case, rows: Arc<RecordBatch>) -> Outcome {
    log::debug!("start {case:?}");
    let outcome = run_case_inner(&case, rows).await;
    log::debug!("done  {case:?}");
    outcome
}

async fn run_case_inner(case: &Case, rows: Arc<RecordBatch>) -> Outcome {
    let partitions = arrange(
        &rows,
        case.shape.query,
        case.order,
        case.shape.source_partitions,
    );
    let plan = build_plan(
        case.shape,
        source(&partitions, case.shape.query, case.order),
    );
    check_plan_shape(case, &plan);

    // A hang is a failure too: name the case instead of stalling the run.
    let collected = tokio::time::timeout(
        Duration::from_secs(CASE_TIMEOUT_SECS),
        collect(Arc::clone(&plan), task_context(case)),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{case:?} did not finish within {CASE_TIMEOUT_SECS}s\n{}",
            displayable(plan.as_ref()).indent(true)
        )
    });
    let batches = match collected {
        Ok(batches) => batches,
        Err(error) => panic!(
            "{case:?} failed: {error}\n{}",
            displayable(plan.as_ref()).indent(true)
        ),
    };
    let spilled = check_metrics(case, &plan);
    Outcome {
        output: batches_to_sort_string(&batches),
        spilled,
    }
}

/// Reference result: the single-stage shape of `query` without a limit, one
/// partition, unordered input, unlimited memory and migration on.
async fn reference(
    query: Query,
    rows: Arc<RecordBatch>,
    cardinality: Cardinality,
) -> String {
    let shape = SHAPES
        .iter()
        .find(|shape| {
            shape.query == query && matches!(shape.operators, [Aggregate(Single)])
        })
        .unwrap();
    let outcome = run_case(
        Case {
            shape,
            order: Order::Unordered,
            migration_enabled: true,
            cardinality,
            memory: Memory::Unlimited,
            skip_partial_enabled: true,
        },
        rows,
    )
    .await;
    outcome.output
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_chain_fuzz() {
    let seed = rand::random::<u64>();
    println!("aggregate_chain_fuzz seed = {seed}");
    let mut total_spilled = 0;
    let mut failures: Vec<String> = vec![];

    for cardinality in Cardinality::ALL {
        let rows = Arc::new(generate_rows(cardinality, seed));
        let mut expected_by_query: Vec<(Query, String)> = Vec::new();
        for shape in SHAPES {
            if expected_by_query
                .iter()
                .any(|(query, _)| *query == shape.query)
            {
                continue;
            }
            let expected = reference(shape.query, Arc::clone(&rows), cardinality).await;
            expected_by_query.push((shape.query, expected));
        }

        let mut join_set = JoinSet::new();
        let (mut spilled, mut finished) = (vec![], vec![]);
        for case in all_cases()
            .into_iter()
            .filter(|case| case.cardinality == cardinality)
        {
            let rows = Arc::clone(&rows);
            let expected = expected_by_query
                .iter()
                .find(|(query, _)| *query == case.shape.query)
                .map(|(_, expected)| expected.clone())
                .unwrap();
            // Every in-flight case holds several copies of the dataset, so
            // bound the concurrency instead of spawning the whole matrix.
            while join_set.len() >= MAX_CONCURRENT_CASES {
                collect_finished(
                    &mut join_set,
                    &mut spilled,
                    &mut finished,
                    &mut failures,
                )
                .await;
            }
            join_set.spawn(async move {
                let outcome = run_case(case.clone(), rows).await;
                assert_eq!(outcome.output, expected, "{case:?} (seed {seed})");
                (case, outcome.spilled)
            });
        }
        while !join_set.is_empty() {
            collect_finished(&mut join_set, &mut spilled, &mut finished, &mut failures)
                .await;
        }
        print_cases(cardinality, "spilled", &spilled);
        print_cases(cardinality, "finished without spilling", &finished);
        total_spilled += spilled.len();
    }
    // A shape filter may select only shapes that cannot spill
    if std::env::var("AGGREGATE_CHAIN_SHAPES").is_err() {
        assert!(total_spilled > 0, "no case exercised the spill path");
    }
    assert!(
        failures.is_empty(),
        "{} cases failed (seed {seed}):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

/// One line per case; `spilled_stages` names the aggregate operators that
/// spilled and flags when more than one did.
const MAX_CONCURRENT_CASES: usize = 16;
/// Generous: a case takes well under a second in debug builds.
const CASE_TIMEOUT_SECS: u64 = 60;

/// Waits for one case and files it under spilled, finished or failed. A
/// failure does not stop the run, so one run reports every failing case.
async fn collect_finished(
    join_set: &mut JoinSet<(Case, Vec<String>)>,
    spilled: &mut Vec<(Case, Vec<String>)>,
    finished: &mut Vec<(Case, Vec<String>)>,
    failures: &mut Vec<String>,
) {
    let Some(result) = join_set.join_next().await else {
        return;
    };
    match result {
        Ok((case, stages)) if stages.is_empty() => finished.push((case, stages)),
        Ok((case, stages)) => spilled.push((case, stages)),
        Err(error) => failures.push(error.to_string()),
    }
}

fn print_cases(cardinality: Cardinality, outcome: &str, cases: &[(Case, Vec<String>)]) {
    let mut lines: Vec<String> = cases
        .iter()
        .map(|(case, spilled_stages)| {
            let spilled = match spilled_stages.len() {
                0 => String::new(),
                1 => format!("  spilled: {}", spilled_stages[0]),
                _ => format!(
                    "  spilled: {} (multiple stages)",
                    spilled_stages.join(" + ")
                ),
            };
            let skip_partial = if case.shape.has_skip_partial_candidate(case.order) {
                format!(" skip_partial={:<5}", case.skip_partial_enabled)
            } else {
                " ".repeat(19)
            };
            format!(
                "  {:<45} {:<17} migration={:<5} memory={:<9}{skip_partial}{spilled}",
                case.shape.name,
                format!("{:?}", case.order),
                case.migration_enabled,
                format!("{:?}", case.memory),
            )
        })
        .collect();
    lines.sort();
    // Enable with `RUST_LOG=debug`
    log::debug!("{cardinality:?}: {} cases {outcome}", lines.len());
    for line in lines {
        log::debug!("{line}");
    }
}

/// Reproduces one failing cell from its seed.
#[expect(dead_code)]
async fn run_single_case(
    shape_name: &str,
    order: Order,
    migration_enabled: bool,
    cardinality: Cardinality,
    memory: Memory,
    seed: u64,
) -> Result<()> {
    let shape = shape_by_name(shape_name);
    let rows = Arc::new(generate_rows(cardinality, seed));
    let expected = reference(shape.query, Arc::clone(&rows), cardinality).await;
    let actual = run_case(
        Case {
            shape,
            order,
            migration_enabled,
            cardinality,
            memory,
            skip_partial_enabled: true,
        },
        rows,
    )
    .await;
    assert_eq!(actual.output, expected);
    Ok(())
}
