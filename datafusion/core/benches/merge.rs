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

//! Benchmarks for Merge performance
//!
//! Each benchmark:
//! 1. Creates a sorted RecordBatch of some number of columns
//!
//! 2. Divides that `RecordBatch` into some number of "streams"
//! (`RecordBatch`s with a subset of the rows, still ordered)
//!
//! 3. Times how long it takes for [`SortPreservingMergeExec`] to
//! merge the "streams" back together into the original RecordBatch.
//!
//! Pictorally:
//!
//! ```
//!                           Rows are randombly
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

use arrow::array::DictionaryArray;
use arrow::datatypes::Int32Type;
use arrow::{
    array::{Float64Array, Int64Array, StringArray, UInt64Array},
    compute::{self, SortOptions, TakeOptions},
    datatypes::Schema,
    record_batch::RecordBatch,
};

/// Benchmarks for SortPreservingMerge stream
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::{
    execution::context::TaskContext,
    physical_plan::{
        memory::MemoryExec, sorts::sort_preserving_merge::SortPreservingMergeExec,
        ExecutionPlan,
    },
    prelude::SessionContext,
};
use datafusion_physical_expr::{expressions::col, PhysicalSortExpr};
use futures::StreamExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::runtime::Runtime;

use lazy_static::lazy_static;

/// Total number of streams to divide each input into
/// models 8 partition plan (should it be 16??)
const NUM_STREAMS: u64 = 8;

/// Total number of input rows to generate
const INPUT_SIZE: u64 = 100000;
// cases:

// * physical sort expr (X, Y Z, NULLS FIRST, ASC) (not parameterized)
//
// streams of distinct values
// streams with 10% duplicated values (within each stream, and across streams)
// These cases are intended to model important usecases in TPCH
// parameters:
//
// Input schemas
lazy_static! {
    static ref I64_STREAMS: Vec<Vec<RecordBatch>> = i64_streams();
    static ref F64_STREAMS: Vec<Vec<RecordBatch>> = f64_streams();

    static ref UTF8_LOW_CARDINALITY_STREAMS: Vec<Vec<RecordBatch>> = utf8_low_cardinality_streams();
    static ref UTF8_HIGH_CARDINALITY_STREAMS: Vec<Vec<RecordBatch>> = utf8_high_cardinality_streams();

    static ref DICTIONARY_STREAMS: Vec<Vec<RecordBatch>> = dictionary_streams();
    static ref DICTIONARY_TUPLE_STREAMS: Vec<Vec<RecordBatch>> = dictionary_tuple_streams();
    static ref MIXED_DICTIONARY_TUPLE_STREAMS: Vec<Vec<RecordBatch>> = mixed_dictionary_tuple_streams();
    // * (string(low), string(low), string(high)) -- tpch q1 + iox
    static ref UTF8_TUPLE_STREAMS: Vec<Vec<RecordBatch>> = utf8_tuple_streams();
    // * (f64, string, string, int) -- tpch q2
    static ref MIXED_TUPLE_STREAMS: Vec<Vec<RecordBatch>> = mixed_tuple_streams();

}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("merge i64", |b| {
        let case = MergeBenchCase::new(&I64_STREAMS);

        b.iter(move || case.run())
    });

    c.bench_function("merge f64", |b| {
        let case = MergeBenchCase::new(&F64_STREAMS);

        b.iter(move || case.run())
    });

    c.bench_function("merge utf8 low cardinality", |b| {
        let case = MergeBenchCase::new(&UTF8_LOW_CARDINALITY_STREAMS);

        b.iter(move || case.run())
    });

    c.bench_function("merge utf8 high cardinality", |b| {
        let case = MergeBenchCase::new(&UTF8_HIGH_CARDINALITY_STREAMS);

        b.iter(move || case.run())
    });

    c.bench_function("merge utf8 tuple", |b| {
        let case = MergeBenchCase::new(&UTF8_TUPLE_STREAMS);

        b.iter(move || case.run())
    });

    c.bench_function("merge utf8 dictionary", |b| {
        let case = MergeBenchCase::new(&DICTIONARY_STREAMS);

        b.iter(move || case.run())
    });

    c.bench_function("merge utf8 dictionary tuple", |b| {
        let case = MergeBenchCase::new(&DICTIONARY_TUPLE_STREAMS);
        b.iter(move || case.run())
    });

    c.bench_function("merge mixed utf8 dictionary tuple", |b| {
        let case = MergeBenchCase::new(&MIXED_DICTIONARY_TUPLE_STREAMS);
        b.iter(move || case.run())
    });

    c.bench_function("merge mixed tuple", |b| {
        let case = MergeBenchCase::new(&MIXED_TUPLE_STREAMS);

        b.iter(move || case.run())
    });
}

/// Encapsulates running each test case
struct MergeBenchCase {
    runtime: Runtime,
    task_ctx: Arc<TaskContext>,

    // The plan to run
    plan: Arc<dyn ExecutionPlan>,
}

impl MergeBenchCase {
    /// Prepare to run a benchmark that merges the specified
    /// partitions (streams) together using all keyes
    fn new(partitions: &[Vec<RecordBatch>]) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let schema = partitions[0][0].schema();
        let sort = make_sort_exprs(schema.as_ref());

        let projection = None;
        let exec = MemoryExec::try_new(partitions, schema, projection).unwrap();
        let plan = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

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

/// Make sort exprs for each column in `schema`
fn make_sort_exprs(schema: &Schema) -> Vec<PhysicalSortExpr> {
    schema
        .fields()
        .iter()
        .map(|f| PhysicalSortExpr {
            expr: col(f.name(), schema).unwrap(),
            options: SortOptions::default(),
        })
        .collect()
}

/// Create streams of int64 (where approximately 1/3 values is repeated)
fn i64_streams() -> Vec<Vec<RecordBatch>> {
    let array: Int64Array = DataGenerator::new().i64_values().into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![("i64", Arc::new(array) as _)]).unwrap();

    split_batch(batch)
}

/// Create streams of f64 (where approximately 1/3 values are repeated)
/// with the same distribution as i64_streams
fn f64_streams() -> Vec<Vec<RecordBatch>> {
    let array: Float64Array = DataGenerator::new().f64_values().into_iter().collect();
    let batch = RecordBatch::try_from_iter(vec![("f64", Arc::new(array) as _)]).unwrap();

    split_batch(batch)
}

/// Create streams of random low cardinality utf8 values
fn utf8_low_cardinality_streams() -> Vec<Vec<RecordBatch>> {
    let array: StringArray = DataGenerator::new()
        .utf8_low_cardinality_values()
        .into_iter()
        .collect();

    let batch =
        RecordBatch::try_from_iter(vec![("utf_low", Arc::new(array) as _)]).unwrap();

    split_batch(batch)
}

/// Create streams of high  cardinality (~ no duplicates) utf8 values
fn utf8_high_cardinality_streams() -> Vec<Vec<RecordBatch>> {
    let array: StringArray = DataGenerator::new()
        .utf8_high_cardinality_values()
        .into_iter()
        .collect();

    let batch =
        RecordBatch::try_from_iter(vec![("utf_high", Arc::new(array) as _)]).unwrap();

    split_batch(batch)
}

/// Create a batch of (utf8_low, utf8_low, utf8_high)
fn utf8_tuple_streams() -> Vec<Vec<RecordBatch>> {
    let mut gen = DataGenerator::new();

    // need to sort by the combined key, so combine them together
    let mut tuples: Vec<_> = gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(gen.utf8_low_cardinality_values().into_iter())
        .zip(gen.utf8_high_cardinality_values().into_iter())
        .collect();

    tuples.sort_unstable();

    let (tuples, utf8_high): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (utf8_low1, utf8_low2): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let utf8_high: StringArray = utf8_high.into_iter().collect();
    let utf8_low1: StringArray = utf8_low1.into_iter().collect();
    let utf8_low2: StringArray = utf8_low2.into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("utf_low1", Arc::new(utf8_low1) as _),
        ("utf_low2", Arc::new(utf8_low2) as _),
        ("utf_high", Arc::new(utf8_high) as _),
    ])
    .unwrap();

    split_batch(batch)
}

/// Create a batch of (f64, utf8_low, utf8_low, i64)
fn mixed_tuple_streams() -> Vec<Vec<RecordBatch>> {
    let mut gen = DataGenerator::new();

    // need to sort by the combined key, so combine them together
    let mut tuples: Vec<_> = gen
        .i64_values()
        .into_iter()
        .zip(gen.utf8_low_cardinality_values().into_iter())
        .zip(gen.utf8_low_cardinality_values().into_iter())
        .zip(gen.i64_values().into_iter())
        .collect();
    tuples.sort_unstable();

    let (tuples, i64_values): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (tuples, utf8_low2): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (f64_values, utf8_low1): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let f64_values: Float64Array = f64_values.into_iter().map(|v| v as f64).collect();
    let utf8_low1: StringArray = utf8_low1.into_iter().collect();
    let utf8_low2: StringArray = utf8_low2.into_iter().collect();
    let i64_values: Int64Array = i64_values.into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("f64", Arc::new(f64_values) as _),
        ("utf_low1", Arc::new(utf8_low1) as _),
        ("utf_low2", Arc::new(utf8_low2) as _),
        ("i64", Arc::new(i64_values) as _),
    ])
    .unwrap();

    split_batch(batch)
}

/// Create a batch of (utf8_dict)
fn dictionary_streams() -> Vec<Vec<RecordBatch>> {
    let mut gen = DataGenerator::new();
    let values = gen.utf8_low_cardinality_values();
    let dictionary: DictionaryArray<Int32Type> =
        values.iter().map(Option::as_deref).collect();

    let batch =
        RecordBatch::try_from_iter(vec![("dict", Arc::new(dictionary) as _)]).unwrap();

    split_batch(batch)
}

/// Create a batch of (utf8_dict, utf8_dict, utf8_dict)
fn dictionary_tuple_streams() -> Vec<Vec<RecordBatch>> {
    let mut gen = DataGenerator::new();
    let mut tuples: Vec<_> = gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(gen.utf8_low_cardinality_values())
        .zip(gen.utf8_low_cardinality_values())
        .collect();
    tuples.sort_unstable();

    let (tuples, c): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (a, b): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let a: DictionaryArray<Int32Type> = a.iter().map(Option::as_deref).collect();
    let b: DictionaryArray<Int32Type> = b.iter().map(Option::as_deref).collect();
    let c: DictionaryArray<Int32Type> = c.iter().map(Option::as_deref).collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(a) as _),
        ("b", Arc::new(b) as _),
        ("c", Arc::new(c) as _),
    ])
    .unwrap();

    split_batch(batch)
}

/// Create a batch of (utf8_dict, utf8_dict, utf8_dict, i64)
fn mixed_dictionary_tuple_streams() -> Vec<Vec<RecordBatch>> {
    let mut gen = DataGenerator::new();
    let mut tuples: Vec<_> = gen
        .utf8_low_cardinality_values()
        .into_iter()
        .zip(gen.utf8_low_cardinality_values())
        .zip(gen.utf8_low_cardinality_values())
        .zip(gen.i64_values())
        .collect();
    tuples.sort_unstable();

    let (tuples, d): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (tuples, c): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
    let (a, b): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();

    let a: DictionaryArray<Int32Type> = a.iter().map(Option::as_deref).collect();
    let b: DictionaryArray<Int32Type> = b.iter().map(Option::as_deref).collect();
    let c: DictionaryArray<Int32Type> = c.iter().map(Option::as_deref).collect();
    let d: Int64Array = d.into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(a) as _),
        ("b", Arc::new(b) as _),
        ("c", Arc::new(c) as _),
        ("d", Arc::new(d) as _),
    ])
    .unwrap();

    split_batch(batch)
}

/// Encapsulates creating data for this test
struct DataGenerator {
    rng: StdRng,
}

impl DataGenerator {
    fn new() -> Self {
        Self {
            rng: StdRng::seed_from_u64(42),
        }
    }

    /// Create an array of i64 sorted values (where approximately 1/3 values is repeated)
    fn i64_values(&mut self) -> Vec<i64> {
        let mut vec: Vec<_> = (0..INPUT_SIZE)
            .map(|_| self.rng.gen_range(0..INPUT_SIZE as i64))
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
            .map(|s| format!("value{}", s).into())
            .collect::<Vec<_>>();

        // pick from the 100 strings randomly
        let mut input = (0..INPUT_SIZE)
            .map(|_| {
                let idx = self.rng.gen_range(0..strings.len());
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
        let mut input = (0..INPUT_SIZE)
            .map(|_| Some(self.random_string()))
            .collect::<Vec<_>>();

        input.sort_unstable();
        input
    }

    fn random_string(&mut self) -> String {
        let rng = &mut self.rng;
        rng.sample_iter(rand::distributions::Alphanumeric)
            .filter(|c| c.is_ascii_alphabetic())
            .take(20)
            .map(char::from)
            .collect::<String>()
    }
}

/// Splits the (sorted) `input_batch` randomly into `NUM_STREAMS` approximately evenly sorted streams
fn split_batch(input_batch: RecordBatch) -> Vec<Vec<RecordBatch>> {
    // figure out which inputs go where
    let mut rng = StdRng::seed_from_u64(1337);

    // randomly assign rows to streams
    let stream_assignments = (0..input_batch.num_rows())
        .map(|_| rng.gen_range(0..NUM_STREAMS))
        .collect();

    // split the inputs into streams
    (0..NUM_STREAMS)
        .map(|stream| {
            // make a "stream" of 1 record batch
            vec![take_columns(&input_batch, &stream_assignments, stream)]
        })
        .collect::<Vec<_>>()
}

/// returns a record batch that contains all there values where
/// stream_assignment[i] = stream (aka this is the equivalent of
/// calling take(indicies) where indicies[i] == stream_index)
fn take_columns(
    input_batch: &RecordBatch,
    stream_assignments: &UInt64Array,
    stream: u64,
) -> RecordBatch {
    // find just the indicies needed from record batches to extract
    let stream_indices: UInt64Array = stream_assignments
        .iter()
        .enumerate()
        .filter_map(|(idx, stream_idx)| {
            if stream_idx.unwrap() == stream {
                Some(idx as u64)
            } else {
                None
            }
        })
        .collect();

    let options = Some(TakeOptions { check_bounds: true });

    // now, get the columns from each array
    let new_columns = input_batch
        .columns()
        .iter()
        .map(|array| compute::take(array, &stream_indices, options.clone()).unwrap())
        .collect();

    RecordBatch::try_new(input_batch.schema(), new_columns).unwrap()
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
