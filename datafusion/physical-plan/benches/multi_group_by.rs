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

//! Benchmarks for multi-column GROUP BY performance comparing vectorized
//! (`GroupValuesColumn`) vs row-based (`GroupValuesRows`) implementations.
//!
//! Motivated by <https://github.com/apache/datafusion/issues/17850> which
//! showed vectorized can regress for low-cardinality, high-row-count scenarios.
//!
//! Uses the direct `GroupValues::intern()` API with identical data for both
//! implementations — a fair apples-to-apples comparison with the same hashing
//! and data layout. Most experiments use `Int32` columns; `bench_fixed_size_binary`
//! covers a `(FixedSizeBinary, Int32)` key to exercise the
//! `FixedSizeBinaryGroupValueBuilder`.

use arrow::array::{
    ArrayRef, BinaryArray, Int32Array, LargeBinaryArray, LargeStringArray, StringArray,
    UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::bench_util::create_fsb_array;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::EmitTo;
use datafusion_physical_plan::aggregates::group_values::GroupValues;
use datafusion_physical_plan::aggregates::group_values::GroupValuesRows;
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupValuesColumn;
use std::hint::black_box;
use std::sync::Arc;

const DEFAULT_BATCH_SIZE: usize = 8192;

fn make_schema(num_cols: usize) -> SchemaRef {
    let fields: Vec<Field> = (0..num_cols)
        .map(|i| Field::new(format!("col_{i}"), DataType::Int32, false))
        .collect();
    Arc::new(Schema::new(fields))
}

fn generate_batches(
    num_cols: usize,
    num_distinct_groups: usize,
    num_rows: usize,
    batch_size: usize,
) -> Vec<Vec<ArrayRef>> {
    let per_col_card = (num_distinct_groups as f64)
        .powf(1.0 / num_cols as f64)
        .ceil() as usize;

    let num_full_batches = num_rows / batch_size;
    let remainder = num_rows % batch_size;
    let num_batches = num_full_batches + if remainder > 0 { 1 } else { 0 };

    (0..num_batches)
        .map(|batch_idx| {
            let batch_start = batch_idx * batch_size;
            let current_batch_size = if batch_idx == num_batches - 1 && remainder > 0 {
                remainder
            } else {
                batch_size
            };
            (0..num_cols)
                .map(|col_idx| {
                    let values: Vec<i32> = (0..current_batch_size)
                        .map(|row| {
                            let global_row = batch_start + row;
                            let group_id = global_row % num_distinct_groups;
                            let divisor = per_col_card.pow(col_idx as u32);
                            ((group_id / divisor) % per_col_card) as i32
                        })
                        .collect();
                    Arc::new(Int32Array::from(values)) as ArrayRef
                })
                .collect()
        })
        .collect()
}

fn create_group_values(schema: &SchemaRef, vectorized: bool) -> Box<dyn GroupValues> {
    if vectorized {
        Box::new(GroupValuesColumn::<false>::try_new(Arc::clone(schema)).unwrap())
    } else {
        Box::new(GroupValuesRows::try_new(Arc::clone(schema)).unwrap())
    }
}

fn bench_intern(
    gv: &mut Box<dyn GroupValues>,
    batches: &[Vec<ArrayRef>],
    groups: &mut Vec<usize>,
) {
    for batch in batches {
        groups.clear();
        gv.intern(batch, groups).unwrap();
    }
    black_box(&*groups);
}

/// Experiment 1: Issue #17850 regression scenario.
/// 3 columns, 64 groups (4^3), scaling row count.
fn bench_issue_17850_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("issue_17850_regression");
    group.sample_size(10);

    let num_cols = 3;
    let num_groups = 64;
    let schema = make_schema(num_cols);

    for num_rows in [1_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000] {
        let batches =
            generate_batches(num_cols, num_groups, num_rows, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("{num_rows}_rows")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 2: Low cardinality sweep.
fn bench_low_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("low_cardinality");
    group.sample_size(15);

    for (num_cols, per_col_card) in
        [(3usize, 2usize), (3, 4), (3, 8), (4, 2), (4, 4), (4, 8)]
    {
        let num_groups = per_col_card.pow(num_cols as u32);
        let schema = make_schema(num_cols);
        let batches =
            generate_batches(num_cols, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(
                    label,
                    format!("cols_{num_cols}_card_{per_col_card}_grp_{num_groups}"),
                ),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 3: Batch size sensitivity.
fn bench_batch_size_sensitivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_size_sensitivity");
    group.sample_size(10);

    let num_cols = 3;
    let num_groups = 64;
    let schema = make_schema(num_cols);

    for batch_size in [1024, 4096, 8192, 16384, 32768] {
        let batches = generate_batches(num_cols, num_groups, 1_000_000, batch_size);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("batch_{batch_size}")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(batch_size),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 4: Column count scaling with low groups.
fn bench_column_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_scaling");
    group.sample_size(15);

    let cases: &[(usize, usize)] =
        &[(2, 100), (3, 125), (4, 81), (6, 729), (8, 256), (10, 1024)];

    for &(num_cols, num_groups) in cases {
        let schema = make_schema(num_cols);
        let batches =
            generate_batches(num_cols, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("cols_{num_cols}_grp_{num_groups}")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 5: High cardinality column scaling (~1M groups).
fn bench_high_cardinality_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_cardinality_scaling");
    group.sample_size(10);

    for num_cols in [2, 3, 4, 6, 8, 10] {
        let num_groups = 1_000_000;
        let schema = make_schema(num_cols);
        let batches =
            generate_batches(num_cols, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("cols_{num_cols}_grp_1M")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 6: Group count sweep with fixed 4 columns.
fn bench_group_count_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_count_sweep");
    group.sample_size(15);

    let num_cols = 4;
    let schema = make_schema(num_cols);

    for num_groups in [
        16, 64, 256, 1000, 5000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
    ] {
        let batches =
            generate_batches(num_cols, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("grp_{num_groups}")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Width in bytes of the FixedSizeBinary group column (UUID-sized).
const FSB_WIDTH: usize = 16;

/// Schema for the FixedSizeBinary experiment: a `FixedSizeBinary` group column
/// paired with an `Int32` column, exercising a multi-column GROUP BY that
/// includes a fixed-width binary key (e.g. grouping on a UUID).
fn make_fsb_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("fsb", DataType::FixedSizeBinary(FSB_WIDTH as i32), false),
        Field::new("id", DataType::Int32, false),
    ]))
}

/// Generate `(FixedSizeBinary, Int32)` batches with exactly
/// `num_distinct_groups` distinct keys.
///
/// The distinct FixedSizeBinary values come from arrow-rs's `create_fsb_array`
/// benchmark generator; rows cycle through that pool (mirroring how
/// `generate_batches` controls Int32 cardinality) so the group count is
/// controlled. The `Int32` column is keyed identically, keeping the combined
/// cardinality equal to `num_distinct_groups`.
fn generate_fsb_batches(
    num_distinct_groups: usize,
    num_rows: usize,
    batch_size: usize,
) -> Vec<Vec<ArrayRef>> {
    // Pool of distinct FixedSizeBinary values (fixed seed, no nulls).
    let pool = create_fsb_array(num_distinct_groups, 0.0, FSB_WIDTH);

    let num_full_batches = num_rows / batch_size;
    let remainder = num_rows % batch_size;
    let num_batches = num_full_batches + if remainder > 0 { 1 } else { 0 };

    (0..num_batches)
        .map(|batch_idx| {
            let batch_start = batch_idx * batch_size;
            let current_batch_size = if batch_idx == num_batches - 1 && remainder > 0 {
                remainder
            } else {
                batch_size
            };

            let group_ids = (0..current_batch_size)
                .map(|row| (batch_start + row) % num_distinct_groups);

            let indices: UInt32Array = group_ids.clone().map(|g| g as u32).collect();
            let fsb = take(&pool, &indices, None).unwrap();
            let id: Int32Array = group_ids.map(|g| g as i32).collect();

            vec![fsb, Arc::new(id) as ArrayRef]
        })
        .collect()
}

/// Experiment 7: Group count sweep for a `(FixedSizeBinary, Int32)` key.
///
/// Exercises the `FixedSizeBinaryGroupValueBuilder` used by multi-column
/// GROUP BY. Before FixedSizeBinary support, such a schema fell back to the
/// row-based `GroupValuesRows`; this compares the vectorized columnar path
/// (`vectorized`) against that baseline (`row_based`).
fn bench_fixed_size_binary(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_size_binary");
    group.sample_size(15);

    let schema = make_fsb_schema();

    for num_groups in [1_000, 1_000_000] {
        let batches = generate_fsb_batches(num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("grp_{num_groups}")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Offset width of the byte group columns used by the emit experiment.
///
/// Both variants use two byte columns, so both route through
/// `ByteGroupValueBuilder` and its `take_n` (the code under test). `Large` uses
/// 64-bit offsets, which doubles the size of the offset vector that
/// `take_n_offsets` splits/shifts — making that path a larger share of `take_n`.
#[derive(Clone, Copy)]
enum OffsetWidth {
    /// `(Binary, Utf8)` — 32-bit offsets.
    Normal,
    /// `(LargeBinary, LargeUtf8)` — 64-bit offsets.
    Large,
}

impl OffsetWidth {
    fn label(self) -> &'static str {
        match self {
            OffsetWidth::Normal => "i32_offsets",
            OffsetWidth::Large => "i64_offsets",
        }
    }

    /// The `(binary, utf8)` arrow types for this width.
    fn data_types(self) -> (DataType, DataType) {
        match self {
            OffsetWidth::Normal => (DataType::Binary, DataType::Utf8),
            OffsetWidth::Large => (DataType::LargeBinary, DataType::LargeUtf8),
        }
    }
}

/// Schema for the emit experiment: a byte (`Binary`/`LargeBinary`) group column
/// paired with a string (`Utf8`/`LargeUtf8`) column. Both columns route the
/// multi-column GROUP BY through the `ByteGroupValueBuilder`, whose `take_n`
/// implementation is what the emit benchmark below exercises.
fn make_bytes_schema(width: OffsetWidth) -> SchemaRef {
    let (binary, utf8) = width.data_types();
    Arc::new(Schema::new(vec![
        Field::new("b", binary, false),
        Field::new("s", utf8, false),
    ]))
}

/// Generate byte-column batches with exactly `num_distinct_groups` distinct
/// keys, for the given [`OffsetWidth`]. Rows cycle through the group pool so the
/// accumulated group count is deterministic; values are short, variable-length
/// labels (`g0`, `g1`, …) — realistic keys without letting the value-buffer copy
/// dominate the offset work in `take_n`. The two columns carry identical labels
/// (as bytes / as str), so the combined cardinality equals `num_distinct_groups`.
fn generate_bytes_batches(
    width: OffsetWidth,
    num_distinct_groups: usize,
    num_rows: usize,
    batch_size: usize,
) -> Vec<Vec<ArrayRef>> {
    let labels: Vec<String> = (0..num_distinct_groups).map(|g| format!("g{g}")).collect();

    let num_full_batches = num_rows / batch_size;
    let remainder = num_rows % batch_size;
    let num_batches = num_full_batches + if remainder > 0 { 1 } else { 0 };

    (0..num_batches)
        .map(|batch_idx| {
            let batch_start = batch_idx * batch_size;
            let current_batch_size = if batch_idx == num_batches - 1 && remainder > 0 {
                remainder
            } else {
                batch_size
            };

            let group_ids = (0..current_batch_size)
                .map(|row| (batch_start + row) % num_distinct_groups);
            let strings = group_ids.clone().map(|g| labels[g].as_str());
            let bytes = group_ids.map(|g| g.to_be_bytes());

            let (binary, utf8): (ArrayRef, ArrayRef) = match width {
                OffsetWidth::Normal => (
                    Arc::new(BinaryArray::from_iter_values(bytes)),
                    Arc::new(StringArray::from_iter_values(strings)),
                ),
                OffsetWidth::Large => (
                    Arc::new(LargeBinaryArray::from_iter_values(bytes)),
                    Arc::new(LargeStringArray::from_iter_values(strings)),
                ),
            };
            vec![binary, utf8]
        })
        .collect()
}

/// Populate a fresh vectorized `GroupValuesColumn` by interning every batch,
/// returning it ready to emit. Used as the (untimed) setup for the emit bench.
///
/// The builder's final state depends only on the set of distinct keys, not on
/// how many rows carry them — so to populate `g` groups it is enough to intern a
/// single batch of `g` distinct rows (see [`generate_bytes_batches`] with
/// `num_rows == num_distinct_groups`), which is what the small-cardinality
/// caller passes.
fn populate_group_values(
    schema: &SchemaRef,
    batches: &[Vec<ArrayRef>],
) -> Box<dyn GroupValues> {
    let mut gv = create_group_values(schema, /* vectorized */ true);
    let mut groups = Vec::with_capacity(DEFAULT_BATCH_SIZE);
    for batch in batches {
        groups.clear();
        gv.intern(batch, &mut groups).unwrap();
    }
    gv
}

/// The `emit(EmitTo::First(n))` split-branch cases, keyed on `n` vs the
/// accumulated group count `g`. `take_n_offsets`
/// allocates for whichever side is smaller:
///
/// * `n = g / 4`: emit the smaller prefix, shift remaining in place.
/// * `n = g / 2`: emit the half-sized prefix, shift remaining in place.
/// * `n = g / 2 + 1`: emit the just-over-half-sized prefix in existing buffer,
///   allocate and shift remaining.
/// * `n = g - g / 4`: emit the larger prefix in existing buffer,
///   allocate and shift remaining.
fn emit_first_cases(num_groups: usize) -> [usize; 4] {
    [
        num_groups / 4,
        num_groups / 2,
        num_groups / 2 + 1,
        num_groups - num_groups / 4,
    ]
}

/// Experiment 8a: `emit(EmitTo::First(n))` on a low-cardinality (1K groups)
/// `(Binary, Utf8)` key — reaches `ByteGroupValueBuilder::take_n`.
fn bench_emit_first_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("emit_first_small");
    group.sample_size(200);

    let num_groups = 1_000usize;
    for width in [OffsetWidth::Normal, OffsetWidth::Large] {
        let schema = make_bytes_schema(width);
        let batches =
            generate_bytes_batches(width, num_groups, num_groups, DEFAULT_BATCH_SIZE);

        for n in emit_first_cases(num_groups) {
            group.bench_with_input(
                BenchmarkId::new(format!("grp_{num_groups}_emit_{n}",), width.label()),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || populate_group_values(&schema, batches),
                        |gv| {
                            drop(black_box(gv.emit(EmitTo::First(n)).unwrap()));
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 8b: `emit(EmitTo::First(n))` on a high-cardinality (1M groups)
/// `(Binary, Utf8)` key — reaches `ByteGroupValueBuilder::take_n`.
fn bench_emit_first_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("emit_first_large");
    group.sample_size(50);

    let num_groups = 1_000_000usize;
    for width in [OffsetWidth::Normal, OffsetWidth::Large] {
        let schema = make_bytes_schema(width);
        let batches =
            generate_bytes_batches(width, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for n in emit_first_cases(num_groups) {
            group.bench_with_input(
                BenchmarkId::new(format!("grp_{num_groups}_emit_{n}",), width.label()),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || populate_group_values(&schema, batches),
                        |gv| {
                            drop(black_box(gv.emit(EmitTo::First(n)).unwrap()));
                        },
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_issue_17850_regression,
    bench_low_cardinality,
    bench_batch_size_sensitivity,
    bench_column_scaling,
    bench_high_cardinality_scaling,
    bench_group_count_sweep,
    bench_fixed_size_binary,
    bench_emit_first_small,
    bench_emit_first_large,
);
criterion_main!(benches);
