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

#[macro_use]
extern crate criterion;
use criterion::{BatchSize, Criterion};
extern crate arrow;
extern crate datafusion;

use std::{iter::FromIterator, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use tokio::runtime::Runtime;

use datafusion::physical_plan::{
    collect,
    expressions::{col, PhysicalSortExpr},
    memory::MemoryExec,
    sort_preserving_merge::SortPreservingMergeExec,
};

// Initialise the operator using the provided record batches and the sort key
// as inputs. All record batches must have the same schema.
fn sort_preserving_merge_operator(batches: Vec<RecordBatch>, sort: &[&str]) {
    let schema = batches[0].schema();

    let sort = sort
        .iter()
        .map(|name| PhysicalSortExpr {
            expr: col(name, &schema).unwrap(),
            options: Default::default(),
        })
        .collect::<Vec<_>>();

    let exec = MemoryExec::try_new(
        &batches.into_iter().map(|rb| vec![rb]).collect::<Vec<_>>(),
        schema,
        None,
    )
    .unwrap();
    let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec), 8192));

    let rt = Runtime::new().unwrap();
    rt.block_on(collect(merge)).unwrap();
}

// Produces `n` record batches of row size `m`. Each record batch will have
// identical contents except for if the `batch_offset` is set. In that case the
// values for column "d" in each subsequent record batch will be offset in
// value.
//
// The `rows_per_key` value controls how many rows are generated per "key",
// which is defined as columns a, b and c.
fn batches(
    n: usize,
    m: usize,
    rows_per_sort_key: usize,
    batch_offset: usize,
) -> Vec<RecordBatch> {
    let mut rbs = Vec::with_capacity(n);
    let mut curr_batch_offset = 0;

    for _ in 0..n {
        let mut col_a = Vec::with_capacity(m);
        let mut col_b = Vec::with_capacity(m);
        let mut col_c = Vec::with_capacity(m);
        let mut col_d = Vec::with_capacity(m);

        let mut j = 0;
        let mut current_rows_per_sort_key = 0;

        for i in 0..m {
            if current_rows_per_sort_key == rows_per_sort_key {
                current_rows_per_sort_key = 0;
                j = i;
            }

            col_a.push(Some(format!("a-{:?}", j)));
            col_b.push(Some(format!("b-{:?}", j)));
            col_c.push(Some(format!("c-{:?}", j)));
            col_d.push(Some((i + curr_batch_offset) as i64));

            current_rows_per_sort_key += 1;
        }

        col_a.sort();
        col_b.sort();
        col_c.sort();

        let col_a: ArrayRef = Arc::new(StringArray::from_iter(col_a));
        let col_b: ArrayRef = Arc::new(StringArray::from_iter(col_b));
        let col_c: ArrayRef = Arc::new(StringArray::from_iter(col_c));
        let col_d: ArrayRef = Arc::new(Int64Array::from(col_d));

        let rb = RecordBatch::try_from_iter(vec![
            ("a", col_a),
            ("b", col_b),
            ("c", col_c),
            ("d", col_d),
        ])
        .unwrap();
        rbs.push(rb);

        curr_batch_offset += batch_offset;
    }

    rbs
}

fn criterion_benchmark(c: &mut Criterion) {
    let small_batch = batches(1, 100, 10, 0).remove(0);
    let large_batch = batches(1, 1000, 1, 0).remove(0);

    let benches = vec![
        // Two batches with identical rows. They will need to be merged together
        // with one row from each batch being taken until both batches are
        // drained.
        ("interleave_batches", batches(2, 1000, 10, 1)),
        // Two batches with a small overlapping region of rows for each unique
        // sort key.
        ("merge_batches_some_overlap_small", batches(2, 1000, 10, 5)),
        // Two batches with a large overlapping region of rows for each unique
        // sort key.
        (
            "merge_batches_some_overlap_large",
            batches(2, 1000, 250, 125),
        ),
        // Two batches with no overlapping region of rows for each unique
        // sort key. For a given unique sort key all rows are drained from one
        // batch, then all the rows for the same key from the second batch.
        // This repeats until all rows are drained. There are a small number of
        // rows (10) for each unique sort key.
        ("merge_batches_no_overlap_small", batches(2, 1000, 10, 12)),
        // As above but this time there are a larger number of rows (250) for
        // each unique sort key - still no overlaps.
        ("merge_batches_no_overlap_large", batches(2, 1000, 250, 252)),
        // Merges two batches where one batch is significantly larger than the
        // other.
        (
            "merge_batches_small_into_large",
            vec![large_batch, small_batch],
        ),
    ];

    for (name, input) in benches {
        c.bench_function(name, move |b| {
            b.iter_batched(
                || input.clone(),
                |input| {
                    sort_preserving_merge_operator(input, &["a", "b", "c", "d"]);
                },
                BatchSize::LargeInput,
            )
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
