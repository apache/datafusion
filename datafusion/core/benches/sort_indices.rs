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

//! Microbenchmark isolating the *sorted-index computation* strategy that
//! underpins coalescing multi-column sort runs.
//!
//! For a single run of `N` rows, comparing two ways to produce the sorted
//! permutation of row indices:
//!
//! * `lexsort` — `arrow::compute::lexsort_to_indices`, which compares rows
//!   column-by-column with a per-comparison dynamic-dispatched comparator.
//! * `rowsort` — encode the key columns once into the Arrow row format
//!   (`RowConverter`) and argsort the row indices with a cheap `memcmp`.
//!
//! `take` is deliberately excluded: it is identical for both strategies, so
//! the benchmark measures only the comparison/encoding cost that differs.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Int64Array, StringArray, StringViewArray, UInt32Array,
};
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices};
use arrow::datatypes::Int32Type;
use arrow::row::{RowConverter, SortField};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Run sizes: 1k models a single small input batch (the current per-batch run
/// size in the merge path), the larger sizes model coalesced runs.
const SIZES: &[(usize, &str)] = &[(1_024, "1k"), (65_536, "64k"), (1_048_576, "1M")];

/// Baseline: column-by-column lexicographic comparator.
fn lexsort_indices(cols: &[SortColumn]) -> UInt32Array {
    lexsort_to_indices(cols, None).unwrap()
}

/// Candidate: encode once into row format, then argsort indices via `memcmp`.
fn rowsort_indices(cols: &[SortColumn]) -> UInt32Array {
    let fields: Vec<SortField> = cols
        .iter()
        .map(|c| {
            SortField::new_with_options(
                c.values.data_type().clone(),
                c.options.unwrap_or_default(),
            )
        })
        .collect();
    let converter = RowConverter::new(fields).unwrap();
    let arrays: Vec<ArrayRef> = cols.iter().map(|c| Arc::clone(&c.values)).collect();
    let rows = converter.convert_columns(&arrays).unwrap();
    let mut indices: Vec<u32> = (0..rows.num_rows() as u32).collect();
    indices.sort_unstable_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));
    UInt32Array::from(indices)
}

fn sort_col(values: ArrayRef) -> SortColumn {
    SortColumn {
        values,
        options: Some(SortOptions::default()),
    }
}

// ----------------------------------------------------------------------------
// Data generation (unsorted, fixed seed) for each key shape.
// ----------------------------------------------------------------------------

fn low_card(rng: &mut StdRng, n: usize) -> Vec<String> {
    let dict: Vec<String> = (0..100).map(|s| format!("value{s}")).collect();
    (0..n)
        .map(|_| dict[rng.random_range(0..dict.len())].clone())
        .collect()
}

fn high_card(rng: &mut StdRng, n: usize) -> Vec<String> {
    (0..n)
        .map(|_| {
            (0..20)
                .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
                .collect::<String>()
        })
        .collect()
}

fn i64_vals(rng: &mut StdRng, n: usize) -> Vec<i64> {
    (0..n).map(|_| rng.random_range(0..n as i64)).collect()
}

fn str_array(v: &[String]) -> StringArray {
    v.iter().map(|s| Some(s.as_str())).collect()
}

fn view_array(v: &[String]) -> StringViewArray {
    v.iter().map(|s| Some(s.as_str())).collect()
}

fn dict_array(v: &[String]) -> DictionaryArray<Int32Type> {
    v.iter().map(|s| Some(s.as_str())).collect()
}

/// Build the sort-key columns for a given key shape and size. Data is random
/// (unsorted) with a fixed seed so the comparison is deterministic and the sort
/// does real work.
fn columns(kind: &str, n: usize) -> Vec<SortColumn> {
    let mut rng = StdRng::seed_from_u64(42);
    match kind {
        // Single-column controls (expected: lexsort wins / ties).
        "i64 (single)" => {
            vec![sort_col(Arc::new(Int64Array::from(i64_vals(&mut rng, n))))]
        }
        "utf8 high (single)" => {
            vec![sort_col(Arc::new(str_array(&high_card(&mut rng, n))))]
        }
        // Multi-column keys.
        "utf8 tuple" => {
            vec![
                sort_col(Arc::new(str_array(&low_card(&mut rng, n)))),
                sort_col(Arc::new(str_array(&low_card(&mut rng, n)))),
                sort_col(Arc::new(str_array(&high_card(&mut rng, n)))),
            ]
        }
        "utf8 view tuple" => {
            vec![
                sort_col(Arc::new(view_array(&low_card(&mut rng, n)))),
                sort_col(Arc::new(view_array(&low_card(&mut rng, n)))),
                sort_col(Arc::new(view_array(&high_card(&mut rng, n)))),
            ]
        }
        "dict tuple" => {
            vec![
                sort_col(Arc::new(dict_array(&low_card(&mut rng, n)))),
                sort_col(Arc::new(dict_array(&low_card(&mut rng, n)))),
                sort_col(Arc::new(dict_array(&low_card(&mut rng, n)))),
            ]
        }
        // Primitive-leading mixed tuple: (i64, utf8, utf8, i64).
        "mixed tuple" => {
            vec![
                sort_col(Arc::new(Int64Array::from(i64_vals(&mut rng, n)))),
                sort_col(Arc::new(str_array(&low_card(&mut rng, n)))),
                sort_col(Arc::new(str_array(&low_card(&mut rng, n)))),
                sort_col(Arc::new(Int64Array::from(i64_vals(&mut rng, n)))),
            ]
        }
        other => panic!("unknown kind {other}"),
    }
}

const KINDS: &[&str] = &[
    "i64 (single)",
    "utf8 high (single)",
    "utf8 tuple",
    "utf8 view tuple",
    "dict tuple",
    "mixed tuple",
];

fn bench(c: &mut Criterion) {
    for kind in KINDS {
        let mut group = c.benchmark_group(*kind);
        for &(n, label) in SIZES {
            let cols = columns(kind, n);
            // Sanity: both strategies must agree on the produced ordering of keys.
            debug_assert_eq!(lexsort_indices(&cols).len(), rowsort_indices(&cols).len());
            group.bench_with_input(
                BenchmarkId::new("lexsort", label),
                &cols,
                |b, cols| b.iter(|| lexsort_indices(cols)),
            );
            group.bench_with_input(
                BenchmarkId::new("rowsort", label),
                &cols,
                |b, cols| b.iter(|| rowsort_indices(cols)),
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
