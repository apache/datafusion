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

//! Benchmarks for Merge and sort performance
//!
//! Each benchmark:
//! 1. Creates a list of tuples (sorted if necessary)
//!
//! 2. Divides those tuples across some number of streams of [`RecordBatch`]
//!    preserving any ordering
//!
//! 3. Times how long it takes for a given sort plan to process the input
//!
//! Pictorially:
//!
//! ```
//!                           Rows are randomly
//!                          divided into separate
//!                         RecordBatch "streams",
//! ┌────┐ ┌────┐ ┌────┐     preserving the order        ┌────┐ ┌────┐ ┌────┐
//! │    │ │    │ │    │                                 │    │ │    │ │    │
//! │    │ │    │ │    │ ──────────────┐                 │    │ │    │ │    │
//! │    │ │    │ │    │               └─────────────▶   │ C1 │ │... │ │ CN │
//! │    │ │    │ │    │ ───────────────┐                │    │ │    │ │    │
//! │    │ │    │ │    │               ┌┼─────────────▶  │    │ │    │ │    │
//! │    │ │    │ │    │               ││                │    │ │    │ │    │
//! │    │ │    │ │    │               ││                └────┘ └────┘ └────┘
//! │    │ │    │ │    │               ││                ┌────┐ ┌────┐ ┌────┐
//! │    │ │    │ │    │               │└───────────────▶│    │ │    │ │    │
//! │    │ │    │ │    │               │                 │    │ │    │ │    │
//! │    │ │    │ │    │         ...   │                 │ C1 │ │... │ │ CN │
//! │    │ │    │ │    │ ──────────────┘                 │    │ │    │ │    │
//! │    │ │    │ │    │                ┌──────────────▶ │    │ │    │ │    │
//! │ C1 │ │... │ │ CN │                │                │    │ │    │ │    │
//! │    │ │    │ │    │───────────────┐│                └────┘ └────┘ └────┘
//! │    │ │    │ │    │               ││
//! │    │ │    │ │    │               ││
//! │    │ │    │ │    │               ││                         ...
//! │    │ │    │ │    │   ────────────┼┼┐
//! │    │ │    │ │    │               │││
//! │    │ │    │ │    │               │││               ┌────┐ ┌────┐ ┌────┐
//! │    │ │    │ │    │ ──────────────┼┘│               │    │ │    │ │    │
//! │    │ │    │ │    │               │ │               │    │ │    │ │    │
//! │    │ │    │ │    │               │ │               │ C1 │ │... │ │ CN │
//! │    │ │    │ │    │               └─┼────────────▶  │    │ │    │ │    │
//! │    │ │    │ │    │                 │               │    │ │    │ │    │
//! │    │ │    │ │    │                 └─────────────▶ │    │ │    │ │    │
//! └────┘ └────┘ └────┘                                 └────┘ └────┘ └────┘
//!    Input RecordBatch                                  NUM_STREAMS input
//!      Columns 1..N                                       RecordBatches
//! INPUT_SIZE sorted rows                                (still INPUT_SIZE total
//!     ~10% duplicates                                          rows)
//! ```

use std::sync::Arc;

use arrow::array::StringViewArray;
use arrow::{
    array::{DictionaryArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Int32Type, Schema},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::{
    execution::context::TaskContext,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        coalesce_partitions::CoalescePartitionsExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
    },
    prelude::SessionContext,
};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};
use datafusion_physical_expr_common::sort_expr::LexOrdering;

/// Benchmarks for SortPreservingMerge stream
use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand::seq::SliceRandom;
use tokio::runtime::Runtime;

/// Total number of streams to divide each input into
/// models 8 partition plan (should it be 16??)
const NUM_STREAMS: usize = 8;

/// The size of each batch within each stream
const BATCH_SIZE: usize = 1024;

/// Input sizes to benchmark. The small size (100K) exercises the
/// in-memory concat-and-sort path; the large size (1M) exercises
/// the sort-then-merge path with high fan-in.
const INPUT_SIZES: &[(u64, &str)] = &[(100_000, "100k"), (1_000_000, "1M")];

/// Number of extra (non-sort-key) payload columns to carry alongside the sort
/// keys in the axis benchmarks. Measures the cost of reordering wide batches.
const EXTRA_COLUMN_COUNTS: &[usize] = &[0, 5, 20, 100];

/// Input ordering profiles for the SortExec axis benchmarks.
#[derive(Clone, Copy, Debug)]
enum DataProfile {
    Sorted,
    Unsorted,
    /// Fully sorted, then 10% of rows swapped to random positions.
    NearlySorted,
}

impl DataProfile {
    fn label(self) -> &'static str {
        match self {
            DataProfile::Sorted => "sorted",
            DataProfile::Unsorted => "unsorted",
            DataProfile::NearlySorted => "nearly sorted",
        }
    }

    /// Arrange `v` (whose initial order is irrelevant) into this profile.
    fn apply<T: Ord>(self, mut v: Vec<T>) -> Vec<T> {
        let mut rng = StdRng::seed_from_u64(99);
        match self {
            DataProfile::Sorted => v.sort_unstable(),
            DataProfile::Unsorted => {
                v.shuffle(&mut rng)
            }
            DataProfile::NearlySorted => {
                v.sort_unstable();
                let n = v.len();

                // 10% is globally misplaced
                for _ in 0..n / 10 {
                    v.swap(rng.random_range(0..n), rng.random_range(0..n));
                }
            }
        }
        v
    }
}

/// Sort-key cardinality, i.e. how much the key values overlap across rows and
/// partitions. Only affects the sort keys, not the extra payload columns.
#[derive(Clone, Copy, Debug)]
enum Cardinality {
    /// Heavy overlap: i64 in `0..input_size` (~1/3 duplicates), 100 distinct
    /// strings repeated across all rows.
    Low,
    /// Minimal overlap: full-range i64 and random strings (~no duplicates).
    High,
}

impl Cardinality {
    fn label(self) -> &'static str {
        match self {
            Cardinality::Low => "low card",
            Cardinality::High => "high card",
        }
    }
}

type PartitionedBatches = Vec<Vec<RecordBatch>>;
type StreamGenerator = Box<dyn Fn(bool) -> PartitionedBatches>;

fn criterion_benchmark(c: &mut Criterion) {
    for &(input_size, size_label) in INPUT_SIZES {
        let cases: Vec<(&str, StreamGenerator)> = vec![
            (
                "i64",
                Box::new(move |sorted| i64_streams(sorted, input_size)),
            ),
            (
                "f64",
                Box::new(move |sorted| f64_streams(sorted, input_size)),
            ),
            (
                "utf8 low cardinality",
                Box::new(move |sorted| utf8_low_cardinality_streams(sorted, input_size)),
            ),
            (
                "utf8 high cardinality",
                Box::new(move |sorted| utf8_high_cardinality_streams(sorted, input_size)),
            ),
            (
                "utf8 view low cardinality",
                Box::new(move |sorted| {
                    utf8_view_low_cardinality_streams(sorted, input_size)
                }),
            ),
            (
                "utf8 view high cardinality",
                Box::new(move |sorted| {
                    utf8_view_high_cardinality_streams(sorted, input_size)
                }),
            ),
            (
                "utf8 tuple",
                Box::new(move |sorted| utf8_tuple_streams(sorted, input_size)),
            ),
            (
                "utf8 view tuple",
                Box::new(move |sorted| utf8_view_tuple_streams(sorted, input_size)),
            ),
            (
                "utf8 dictionary",
                Box::new(move |sorted| dictionary_streams(sorted, input_size)),
            ),
            (
                "utf8 dictionary tuple",
                Box::new(move |sorted| dictionary_tuple_streams(sorted, input_size)),
            ),
            (
                "mixed dictionary tuple",
                Box::new(move |sorted| {
                    mixed_dictionary_tuple_streams(sorted, input_size)
                }),
            ),
            (
                "mixed tuple",
                Box::new(move |sorted| mixed_tuple_streams(sorted, input_size)),
            ),
            (
                "mixed tuple with utf8 view",
                Box::new(move |sorted| {
                    mixed_tuple_with_utf8_view_streams(sorted, input_size)
                }),
            ),
        ];

        for (name, f) in &cases {
            c.bench_function(&format!("merge sorted {name} {size_label}"), |b| {
                let data = f(true);
                let case = BenchCase::merge_sorted(&data);
                b.iter(move || case.run())
            });

            c.bench_function(&format!("sort merge {name} {size_label}"), |b| {
                let data = f(false);
                let case = BenchCase::sort_merge(&data);
                b.iter(move || case.run())
            });

            c.bench_function(&format!("sort {name} {size_label}"), |b| {
                let data = f(false);
                let case = BenchCase::sort(&data);
                b.iter(move || case.run())
            });

            c.bench_function(&format!("sort partitioned {name} {size_label}"), |b| {
                let data = f(false);
                let case = BenchCase::sort_partitioned(&data);
                b.iter(move || case.run())
            });
        }
    }
}

/// Encapsulates running each test case
struct BenchCase {
    runtime: Runtime,
    task_ctx: Arc<TaskContext>,

    // The plan to run
    plan: Arc<dyn ExecutionPlan>,
}

impl BenchCase {
    /// Prepare to run a benchmark that merges the specified
    /// pre-sorted partitions (streams) together using all keys
    fn merge_sorted(partitions: &[Vec<RecordBatch>]) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let schema = partitions[0][0].schema();
        let sort = make_sort_exprs(schema.as_ref());

        let exec = MemorySourceConfig::try_new_exec(partitions, schema, None).unwrap();
        let plan = Arc::new(SortPreservingMergeExec::new(sort, exec));

        Self {
            runtime,
            task_ctx,
            plan,
        }
    }

    /// Test SortExec in  "partitioned" mode followed by a SortPreservingMerge
    fn sort_merge(partitions: &[Vec<RecordBatch>]) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let schema = partitions[0][0].schema();
        let sort = make_sort_exprs(schema.as_ref());

        let source = MemorySourceConfig::try_new_exec(partitions, schema, None).unwrap();
        let exec = SortExec::new(sort.clone(), source).with_preserve_partitioning(true);
        let plan = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        Self {
            runtime,
            task_ctx,
            plan,
        }
    }

    /// Test SortExec in "partitioned" mode which sorts the input streams
    /// individually into some number of output streams
    fn sort(partitions: &[Vec<RecordBatch>]) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let schema = partitions[0][0].schema();
        let sort = make_sort_exprs(schema.as_ref());

        let exec = MemorySourceConfig::try_new_exec(partitions, schema, None).unwrap();
        let exec = Arc::new(CoalescePartitionsExec::new(exec));
        let plan = Arc::new(SortExec::new(sort, exec));

        Self {
            runtime,
            task_ctx,
            plan,
        }
    }

    /// Test SortExec in "partitioned" mode which sorts the input streams
    /// individually into some number of output streams
    fn sort_partitioned(partitions: &[Vec<RecordBatch>]) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let schema = partitions[0][0].schema();
        let sort = make_sort_exprs(schema.as_ref());

        let source = MemorySourceConfig::try_new_exec(partitions, schema, None).unwrap();
        let exec = SortExec::new(sort, source).with_preserve_partitioning(true);
        let plan = Arc::new(CoalescePartitionsExec::new(Arc::new(exec)));

        Self {
            runtime,
            task_ctx,
            plan,
        }
    }

    /// runs the specified plan to completion, draining all input and
    /// panic'ing on error
    fn run(&self) {
        let plan = Arc::clone(&self.plan);
        let task_ctx = Arc::clone(&self.task_ctx);

        assert_eq!(plan.output_partitioning().partition_count(), 1);

        self.runtime.block_on(async move {
            let mut stream = plan.execute(0, task_ctx).unwrap();
            while let Some(b) = stream.next().await {
                b.expect("unexpected execution error");
            }
        })
    }
}

const EXTRA_COLUMN_NAME_PREFIX: &str = "extra_";

/// Make sort exprs for each column in `schema`, skipping non-sort payload
/// columns added by [`with_extra_columns`].
fn make_sort_exprs(schema: &Schema) -> LexOrdering {
    let sort_exprs = schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with(EXTRA_COLUMN_NAME_PREFIX))
        .map(|f| PhysicalSortExpr::new_default(col(f.name(), schema).unwrap()));
    LexOrdering::new(sort_exprs).unwrap()
}

/// Create streams of int64 (where approximately 1/3 values is repeated)
fn i64_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut values = DataGenerator::new(input_size).i64_values();
    if sorted {
        values.sort_unstable();
    }

    split_tuples(values, build_i64_batch)
}

/// Build a single-column i64 [`RecordBatch`].
fn build_i64_batch(v: Vec<i64>) -> RecordBatch {
    let array = Int64Array::from(v);
    RecordBatch::try_from_iter(vec![("i64", Arc::new(array) as _)]).unwrap()
}

/// Build a single-column utf8 view [`RecordBatch`] under the given column name.
fn build_utf8_view_batch(name: &str, v: Vec<Option<Arc<str>>>) -> RecordBatch {
    let array: StringViewArray = v.into_iter().collect();
    RecordBatch::try_from_iter(vec![(name, Arc::new(array) as _)]).unwrap()
}

/// Create streams of f64 (where approximately 1/3 values are repeated)
/// with the same distribution as i64_streams
fn f64_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut values = DataGenerator::new(input_size).f64_values();
    if sorted {
        values.sort_unstable_by(|a, b| a.total_cmp(b));
    }

    split_tuples(values, |v| {
        let array = Float64Array::from(v);
        RecordBatch::try_from_iter(vec![("f64", Arc::new(array) as _)]).unwrap()
    })
}

/// Create streams of random low cardinality utf8 values
fn utf8_low_cardinality_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut values = DataGenerator::new(input_size).utf8_low_cardinality_values();
    if sorted {
        values.sort_unstable();
    }
    split_tuples(values, |v| {
        let array: StringArray = v.into_iter().collect();
        RecordBatch::try_from_iter(vec![("utf_low", Arc::new(array) as _)]).unwrap()
    })
}

/// Create streams of random low cardinality utf8_view values
fn utf8_view_low_cardinality_streams(
    sorted: bool,
    input_size: u64,
) -> PartitionedBatches {
    let mut values = DataGenerator::new(input_size).utf8_low_cardinality_values();
    if sorted {
        values.sort_unstable();
    }
    split_tuples(values, |v| build_utf8_view_batch("utf_view_low", v))
}

/// Create streams of high  cardinality (~ no duplicates) utf8_view values
fn utf8_view_high_cardinality_streams(
    sorted: bool,
    input_size: u64,
) -> PartitionedBatches {
    let mut values = DataGenerator::new(input_size).utf8_high_cardinality_values();
    if sorted {
        values.sort_unstable();
    }
    split_tuples(values, |v| build_utf8_view_batch("utf_view_high", v))
}

/// Create streams of high  cardinality (~ no duplicates) utf8 values
fn utf8_high_cardinality_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut values = DataGenerator::new(input_size).utf8_high_cardinality_values();
    if sorted {
        values.sort_unstable();
    }
    split_tuples(values, |v| {
        let array: StringArray = v.into_iter().collect();
        RecordBatch::try_from_iter(vec![("utf_high", Arc::new(array) as _)]).unwrap()
    })
}

/// Create a batch of (utf8_low, utf8_low, utf8_high)
fn utf8_tuple_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut data_gen = DataGenerator::new(input_size);

    // need to sort by the combined key, so combine them together
    let mut tuples: Vec<_> = data_gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.utf8_high_cardinality_values())
        .collect();

    if sorted {
        tuples.sort_unstable();
    }

    split_tuples(tuples, |tuples| {
        let (tuples, utf8_high): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        let (utf8_low1, utf8_low2): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

        let utf8_high: StringArray = utf8_high.into_iter().collect();
        let utf8_low1: StringArray = utf8_low1.into_iter().collect();
        let utf8_low2: StringArray = utf8_low2.into_iter().collect();

        RecordBatch::try_from_iter(vec![
            ("utf_low1", Arc::new(utf8_low1) as _),
            ("utf_low2", Arc::new(utf8_low2) as _),
            ("utf_high", Arc::new(utf8_high) as _),
        ])
        .unwrap()
    })
}

/// Create a batch of (utf8_view_low, utf8_view_low, utf8_view_high)
fn utf8_view_tuple_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut data_gen = DataGenerator::new(input_size);

    // need to sort by the combined key, so combine them together
    let mut tuples: Vec<_> = data_gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.utf8_high_cardinality_values())
        .collect();

    if sorted {
        tuples.sort_unstable();
    }

    split_tuples(tuples, |tuples| {
        let (tuples, utf8_high): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        let (utf8_low1, utf8_low2): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

        let utf8_view_high: StringViewArray = utf8_high.into_iter().collect();
        let utf8_view_low1: StringViewArray = utf8_low1.into_iter().collect();
        let utf8_view_low2: StringViewArray = utf8_low2.into_iter().collect();

        RecordBatch::try_from_iter(vec![
            ("utf_view_low1", Arc::new(utf8_view_low1) as _),
            ("utf_view_low2", Arc::new(utf8_view_low2) as _),
            ("utf_view_high", Arc::new(utf8_view_high) as _),
        ])
        .unwrap()
    })
}

/// Create a batch of (f64, utf8_low, utf8_low, i64)
fn mixed_tuple_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut data_gen = DataGenerator::new(input_size);

    // need to sort by the combined key, so combine them together
    let mut tuples: Vec<_> = data_gen
        .i64_values()
        .into_iter()
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.i64_values())
        .collect();

    if sorted {
        tuples.sort_unstable();
    }

    split_tuples(tuples, build_mixed_tuple_batch)
}

/// The tuple shape used by the `mixed tuple` case: (i64, utf8_low, utf8_low, i64)
type MixedTuple = (((i64, Option<Arc<str>>), Option<Arc<str>>), i64);

/// Build a (f64, utf8_low, utf8_low, i64) batch from [`MixedTuple`]s
/// (the leading i64 becomes the f64 column).
fn build_mixed_tuple_batch(tuples: Vec<MixedTuple>) -> RecordBatch {
    let (tuples, i64_values): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (tuples, utf8_low2): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (f64_values, utf8_low1): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let f64_values: Float64Array = f64_values.into_iter().map(|v| v as f64).collect();

    let utf8_low1: StringArray = utf8_low1.into_iter().collect();
    let utf8_low2: StringArray = utf8_low2.into_iter().collect();
    let i64_values: Int64Array = i64_values.into_iter().collect();

    RecordBatch::try_from_iter(vec![
        ("f64", Arc::new(f64_values) as _),
        ("utf_low1", Arc::new(utf8_low1) as _),
        ("utf_low2", Arc::new(utf8_low2) as _),
        ("i64", Arc::new(i64_values) as _),
    ])
    .unwrap()
}

/// Create a batch of (f64, utf8_view_low, utf8_view_low, i64)
fn mixed_tuple_with_utf8_view_streams(
    sorted: bool,
    input_size: u64,
) -> PartitionedBatches {
    let mut data_gen = DataGenerator::new(input_size);

    // need to sort by the combined key, so combine them together
    let mut tuples: Vec<_> = data_gen
        .i64_values()
        .into_iter()
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.i64_values())
        .collect();

    if sorted {
        tuples.sort_unstable();
    }

    split_tuples(tuples, |tuples| {
        let (tuples, i64_values): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        let (tuples, utf8_low2): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        let (f64_values, utf8_low1): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

        let f64_values: Float64Array = f64_values.into_iter().map(|v| v as f64).collect();

        let utf8_view_low1: StringViewArray = utf8_low1.into_iter().collect();
        let utf8_view_low2: StringViewArray = utf8_low2.into_iter().collect();
        let i64_values: Int64Array = i64_values.into_iter().collect();

        RecordBatch::try_from_iter(vec![
            ("f64", Arc::new(f64_values) as _),
            ("utf_view_low1", Arc::new(utf8_view_low1) as _),
            ("utf_view_low2", Arc::new(utf8_view_low2) as _),
            ("i64", Arc::new(i64_values) as _),
        ])
        .unwrap()
    })
}

/// Create a batch of (utf8_dict)
fn dictionary_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut data_gen = DataGenerator::new(input_size);
    let mut values = data_gen.utf8_low_cardinality_values();
    if sorted {
        values.sort_unstable();
    }

    split_tuples(values, |v| {
        let dictionary: DictionaryArray<Int32Type> =
            v.iter().map(Option::as_deref).collect();
        RecordBatch::try_from_iter(vec![("dict", Arc::new(dictionary) as _)]).unwrap()
    })
}

/// Create a batch of (utf8_dict, utf8_dict, utf8_dict)
fn dictionary_tuple_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut data_gen = DataGenerator::new(input_size);
    let mut tuples: Vec<_> = data_gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.utf8_low_cardinality_values())
        .collect();

    if sorted {
        tuples.sort_unstable();
    }

    split_tuples(tuples, |tuples| {
        let (tuples, c): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        let (a, b): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

        let a: DictionaryArray<Int32Type> = a.iter().map(Option::as_deref).collect();
        let b: DictionaryArray<Int32Type> = b.iter().map(Option::as_deref).collect();
        let c: DictionaryArray<Int32Type> = c.iter().map(Option::as_deref).collect();

        RecordBatch::try_from_iter(vec![
            ("a", Arc::new(a) as _),
            ("b", Arc::new(b) as _),
            ("c", Arc::new(c) as _),
        ])
        .unwrap()
    })
}

/// Create a batch of (utf8_dict, utf8_dict, utf8_dict, i64)
fn mixed_dictionary_tuple_streams(sorted: bool, input_size: u64) -> PartitionedBatches {
    let mut data_gen = DataGenerator::new(input_size);
    let mut tuples: Vec<_> = data_gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.utf8_low_cardinality_values())
        .zip(data_gen.i64_values())
        .collect();

    if sorted {
        tuples.sort_unstable();
    }

    split_tuples(tuples, |tuples| {
        let (tuples, d): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        let (tuples, c): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        let (a, b): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

        let a: DictionaryArray<Int32Type> = a.iter().map(Option::as_deref).collect();
        let b: DictionaryArray<Int32Type> = b.iter().map(Option::as_deref).collect();
        let c: DictionaryArray<Int32Type> = c.iter().map(Option::as_deref).collect();
        let d: Int64Array = d.into_iter().collect();

        RecordBatch::try_from_iter(vec![
            ("a", Arc::new(a) as _),
            ("b", Arc::new(b) as _),
            ("c", Arc::new(c) as _),
            ("d", Arc::new(d) as _),
        ])
        .unwrap()
    })
}

/// Encapsulates creating data for this test
struct DataGenerator {
    rng: StdRng,
    input_size: u64,
}

impl DataGenerator {
    fn new(input_size: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(42),
            input_size,
        }
    }

    /// Create an array of i64 sorted values (where approximately 1/3 values is repeated)
    fn i64_values(&mut self) -> Vec<i64> {
        let mut vec: Vec<_> = (0..self.input_size)
            .map(|_| self.rng.random_range(0..self.input_size as i64))
            .collect();

        vec.sort_unstable();

        // 6287 distinct / 10000 total
        //let num_distinct = vec.iter().collect::<HashSet<_>>().len();
        //println!("{} distinct / {} total", num_distinct, vec.len());
        vec
    }

    /// Create an array of f64 sorted values (with same distribution of `i64_values`)
    fn f64_values(&mut self) -> Vec<f64> {
        self.i64_values().into_iter().map(|v| v as f64).collect()
    }

    /// array of low cardinality (100 distinct) values
    fn utf8_low_cardinality_values(&mut self) -> Vec<Option<Arc<str>>> {
        let strings = (0..100)
            .map(|s| format!("value{s}").into())
            .collect::<Vec<_>>();

        // pick from the 100 strings randomly
        let mut input = (0..self.input_size)
            .map(|_| {
                let idx = self.rng.random_range(0..strings.len());
                let s = Arc::clone(&strings[idx]);
                Some(s)
            })
            .collect::<Vec<_>>();

        input.sort_unstable();
        input
    }

    /// Create sorted values of high  cardinality (~ no duplicates) utf8 values
    fn utf8_high_cardinality_values(&mut self) -> Vec<Option<String>> {
        // make random strings
        let mut input = (0..self.input_size)
            .map(|_| Some(self.random_string()))
            .collect::<Vec<_>>();

        input.sort_unstable();
        input
    }

    fn random_string(&mut self) -> String {
        let rng = &mut self.rng;
        rng.sample_iter(rand::distr::Alphanumeric)
            .filter(|c| c.is_ascii_alphabetic())
            .take(20)
            .map(char::from)
            .collect::<String>()
    }

    /// i64 values with the given cardinality (initial order is irrelevant since
    /// callers reorder via [`DataProfile::apply`]).
    fn i64_values_by(&mut self, card: Cardinality) -> Vec<i64> {
        match card {
            Cardinality::Low => self.i64_values(),
            // Full i64 range -> effectively unique (minimal overlap)
            Cardinality::High => {
                (0..self.input_size).map(|_| self.rng.random()).collect()
            }
        }
    }

    /// utf8 values with the given cardinality, unified to `Arc<str>`.
    fn utf8_values_by(&mut self, card: Cardinality) -> Vec<Option<Arc<str>>> {
        match card {
            Cardinality::Low => self.utf8_low_cardinality_values(),
            Cardinality::High => self
                .utf8_high_cardinality_values()
                .into_iter()
                .map(|o| o.map(Arc::from))
                .collect(),
        }
    }
}

/// Splits the `input` tuples randomly into batches of `BATCH_SIZE` distributed across
/// `NUM_STREAMS` partitions, preserving any ordering
///
/// `f` is function that takes a list of tuples and produces a [`RecordBatch`]
fn split_tuples<T, F>(input: Vec<T>, f: F) -> PartitionedBatches
where
    F: Fn(Vec<T>) -> RecordBatch,
{
    // figure out which inputs go where
    let mut rng = StdRng::seed_from_u64(1337);

    let mut outputs: Vec<Vec<Vec<T>>> = (0..NUM_STREAMS).map(|_| Vec::new()).collect();

    for i in input {
        let stream_idx = rng.random_range(0..NUM_STREAMS);
        let stream = &mut outputs[stream_idx];
        match stream.last_mut() {
            Some(x) if x.len() < BATCH_SIZE => x.push(i),
            _ => {
                let mut v = Vec::with_capacity(BATCH_SIZE);
                v.push(i);
                stream.push(v)
            }
        }
    }

    outputs
        .into_iter()
        .map(|stream| stream.into_iter().map(&f).collect())
        .collect()
}

type AxisGenerator = Box<dyn Fn(DataProfile, Cardinality, usize) -> PartitionedBatches>;

/// Benchmarks `SortExec` (at the 1M input size) across the following axes:
/// 1. Sort columns
///     - single column with a specialized impl (primitive or byte(view))
///     - multiple columns, which will use fallback impl
/// 2. Number of columns in the record batch - more columns mean more data to
///    copy while reordering and more memory to hold
/// 3. Value cardinality - whether the sort-key values overlap or not
/// 4. Input ordering - already sorted / unsorted / nearly sorted
fn sort_axis_benchmark(c: &mut Criterion) {
    let input_size = 1_000_000u64;
    let size_label = "1M";

    let cases: Vec<(&str, AxisGenerator)> = vec![
        (
            "i64",
            Box::new(move |p, card, extra| i64_axis(p, card, extra, input_size)),
        ),
        (
            "utf8 view",
            Box::new(move |p, card, extra| utf8_view_axis(p, card, extra, input_size)),
        ),
        (
            "mixed tuple",
            Box::new(move |p, card, extra| mixed_tuple_axis(p, card, extra, input_size)),
        ),
    ];

    for (name, f) in &cases {
        for card in [Cardinality::Low, Cardinality::High] {
            for &extra in EXTRA_COLUMN_COUNTS {
                for profile in [
                    DataProfile::Sorted,
                    DataProfile::Unsorted,
                    DataProfile::NearlySorted,
                ] {
                    c.bench_function(
                        &format!(
                            "sort {name} {size_label} {card:?} cardinality {profile:?} +{extra}cols",
                        ),
                        |b| {
                            let data = f(profile, card, extra);
                            let case = BenchCase::sort(&data);
                            b.iter(move || case.run())
                        },
                    );
                }
            }
        }
    }
}

/// Single-column i64 batches
fn i64_axis(
    profile: DataProfile,
    card: Cardinality,
    extra: usize,
    input_size: u64,
) -> PartitionedBatches {
    let values = profile.apply(DataGenerator::new(input_size).i64_values_by(card));
    let batches = split_tuples(values, build_i64_batch);
    with_extra_columns(batches, extra)
}

/// Single-column utf8 view batches
fn utf8_view_axis(
    profile: DataProfile,
    card: Cardinality,
    extra: usize,
    input_size: u64,
) -> PartitionedBatches {
    let values = profile.apply(DataGenerator::new(input_size).utf8_values_by(card));
    let batches = split_tuples(values, |v| build_utf8_view_batch("utf_view", v));
    with_extra_columns(batches, extra)
}

/// Multi-column (f64, utf8, utf8, i64) batches.
fn mixed_tuple_axis(
    profile: DataProfile,
    card: Cardinality,
    extra: usize,
    input_size: u64,
) -> PartitionedBatches {
    let mut data_gen = DataGenerator::new(input_size);
    let tuples: Vec<MixedTuple> = data_gen
        .i64_values_by(card)
        .into_iter()
        .zip(data_gen.utf8_values_by(card))
        .zip(data_gen.utf8_values_by(card))
        .zip(data_gen.i64_values_by(card))
        .collect();
    let batches = split_tuples(profile.apply(tuples), build_mixed_tuple_batch);
    with_extra_columns(batches, extra)
}

/// Append `n` extra non-sort-key `Int64` payload columns
fn with_extra_columns(batches: PartitionedBatches, n: usize) -> PartitionedBatches {
    if n == 0 {
        return batches;
    }
    let mut rng = StdRng::seed_from_u64(7);
    batches
        .into_iter()
        .map(|stream| {
            stream
                .into_iter()
                .map(|batch| {
                    let num_rows = batch.num_rows();
                    let mut fields =
                        batch.schema().fields().iter().cloned().collect::<Vec<_>>();
                    let mut columns = batch.columns().to_vec();
                    for i in 0..n {
                        let arr = Int64Array::from_iter_values(
                            (0..num_rows).map(|_| rng.random::<i64>()),
                        );
                        columns.push(Arc::new(arr) as _);
                        fields.push(Arc::new(Field::new(
                            format!("{}{i}", EXTRA_COLUMN_NAME_PREFIX),
                            DataType::Int64,
                            false,
                        )));
                    }
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
                })
                .collect()
        })
        .collect()
}

criterion_group!(benches, criterion_benchmark, sort_axis_benchmark);
criterion_main!(benches);
