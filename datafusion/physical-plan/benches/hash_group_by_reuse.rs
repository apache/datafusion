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

//! Benchmarks for comparing one large hash GROUP BY map with a reused map over
//! aggregate partitions.
//!
//! The input is a plain `Vec<RecordBatch>`. The `single` benchmark interns the
//! batches directly. The `reuse_clear_shrink` benchmark first slices the same
//! batches into `Vec<Vec<RecordBatch>>`, where the outer vector is aggregate
//! partition, then interns one partition at a time and clears the group values
//! for reuse.

use arrow::array::{ArrayRef, Int32Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_physical_plan::aggregates::group_values::GroupValues;
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupValuesColumn;
use std::env;
use std::hint::black_box;
use std::sync::Arc;

const DEFAULT_BATCH_SIZE: usize = 8129;
const DEFAULT_NUM_ROWS: usize = 10_000_000;
const DEFAULT_NUM_AGGR_PARTITIONS: usize = 16;
const BENCH_NUM_ROWS_ENV: &str = "DF_HASH_GROUP_BY_REUSE_ROWS";
const BENCH_NUM_AGGR_PARTITIONS_ENV: &str = "DF_HASH_GROUP_BY_REUSE_PARTITIONS";

#[derive(Clone, Copy)]
enum SchemaLayout {
    IntOnly,
    MixedStringView,
}

impl SchemaLayout {
    fn schema(self) -> SchemaRef {
        let fields = match self {
            Self::IntOnly => vec![
                Field::new("col_0", DataType::Int32, false),
                Field::new("col_1", DataType::Int32, false),
                Field::new("col_2", DataType::Int32, false),
            ],
            Self::MixedStringView => vec![
                Field::new("col_0", DataType::Int32, false),
                Field::new("col_1", DataType::Utf8View, false),
                Field::new("col_2", DataType::Int32, false),
            ],
        };
        Arc::new(Schema::new(fields))
    }

    fn num_cols(self) -> usize {
        3
    }
}

struct GroupByInput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    num_rows: usize,
}

impl GroupByInput {
    fn new(layout: SchemaLayout, num_rows: usize, num_groups: usize) -> Self {
        let schema = layout.schema();
        let batches = make_batches(&schema, layout, num_rows, num_groups);
        Self {
            schema,
            batches,
            num_rows,
        }
    }
}

fn make_batches(
    schema: &SchemaRef,
    layout: SchemaLayout,
    num_rows: usize,
    num_groups: usize,
) -> Vec<RecordBatch> {
    let num_batches = num_rows.div_ceil(DEFAULT_BATCH_SIZE);
    (0..num_batches)
        .map(|batch_idx| {
            let batch_start = batch_idx * DEFAULT_BATCH_SIZE;
            let batch_size = DEFAULT_BATCH_SIZE.min(num_rows - batch_start);
            let columns = make_columns(layout, batch_start, batch_size, num_groups);
            RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
        })
        .collect()
}

fn make_columns(
    layout: SchemaLayout,
    batch_start: usize,
    batch_size: usize,
    num_groups: usize,
) -> Vec<ArrayRef> {
    (0..layout.num_cols())
        .map(|col_idx| make_column(layout, batch_start, batch_size, col_idx, num_groups))
        .collect()
}

fn make_column(
    layout: SchemaLayout,
    batch_start: usize,
    batch_size: usize,
    col_idx: usize,
    num_groups: usize,
) -> ArrayRef {
    match (layout, col_idx) {
        (SchemaLayout::MixedStringView, 1) => {
            make_string_view_column(batch_start, batch_size, col_idx, num_groups)
        }
        _ => make_int32_column(batch_start, batch_size, col_idx, num_groups),
    }
}

fn make_int32_column(
    batch_start: usize,
    batch_size: usize,
    col_idx: usize,
    num_groups: usize,
) -> ArrayRef {
    let values: Vec<i32> = (0..batch_size)
        .map(|row_idx| group_value(batch_start + row_idx, col_idx, num_groups) as i32)
        .collect();
    Arc::new(Int32Array::from(values))
}

fn make_string_view_column(
    batch_start: usize,
    batch_size: usize,
    col_idx: usize,
    num_groups: usize,
) -> ArrayRef {
    let values: Vec<String> = (0..batch_size)
        .map(|row_idx| {
            let value = group_value(batch_start + row_idx, col_idx, num_groups);
            format!("group_key_{value:016}")
        })
        .collect();
    Arc::new(StringViewArray::from(values))
}

fn group_value(row_idx: usize, col_idx: usize, num_groups: usize) -> usize {
    let group_idx = row_idx % num_groups;
    let per_col_card = (num_groups as f64).powf(1.0 / 3.0).ceil() as usize;
    let divisor = per_col_card.pow(col_idx as u32);
    (group_idx / divisor) % per_col_card
}

fn split_by_aggr_partition(
    batches: &[RecordBatch],
    _num_rows: usize,
    num_aggr_partitions: usize,
) -> Vec<Vec<RecordBatch>> {
    let mut partitions = vec![Vec::new(); num_aggr_partitions];

    for batch in batches {
        // Step 1: Split each input batch into N contiguous slices, where N is
        // the configured number of aggregate partitions. This models the
        // coalesced final aggregate input layout directly and keeps the
        // slicing rule local to each batch.
        let rows_per_slice = batch.num_rows().div_ceil(num_aggr_partitions);

        // Step 2: Append slice K of this batch to aggregate partition K.
        // Empty trailing slices are skipped when the batch has fewer rows than
        // the configured number of aggregate partitions.
        for (partition_idx, partition_batches) in partitions.iter_mut().enumerate() {
            let offset = partition_idx * rows_per_slice;
            if offset >= batch.num_rows() {
                break;
            }

            let slice_len = rows_per_slice.min(batch.num_rows() - offset);
            partition_batches.push(batch.slice(offset, slice_len));
        }
    }

    partitions
}

fn intern_batch(
    group_values: &mut GroupValuesColumn<false>,
    batch: &RecordBatch,
    groups: &mut Vec<usize>,
) {
    groups.clear();
    group_values.intern(batch.columns(), groups).unwrap();
    black_box(groups.len());
}

fn bench_single(input: &GroupByInput) {
    let mut group_values =
        GroupValuesColumn::<false>::try_new(Arc::clone(&input.schema)).unwrap();
    let mut groups = Vec::with_capacity(DEFAULT_BATCH_SIZE);

    for batch in &input.batches {
        intern_batch(&mut group_values, batch, &mut groups);
    }

    black_box(group_values.len());
}

fn bench_reuse_clear_shrink(input: &GroupByInput, num_aggr_partitions: usize) {
    let partitions =
        split_by_aggr_partition(&input.batches, input.num_rows, num_aggr_partitions);
    let rows_per_partition = input.num_rows.div_ceil(num_aggr_partitions);
    let mut group_values =
        GroupValuesColumn::<false>::try_new(Arc::clone(&input.schema)).unwrap();
    let mut groups = Vec::with_capacity(DEFAULT_BATCH_SIZE);

    for partition_batches in partitions {
        for batch in partition_batches {
            intern_batch(&mut group_values, &batch, &mut groups);
        }
        black_box(group_values.len());
        group_values.clear_shrink(rows_per_partition);
    }
}

fn num_rows_from_env() -> usize {
    env::var(BENCH_NUM_ROWS_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_NUM_ROWS)
}

fn num_aggr_partitions_from_env() -> usize {
    env::var(BENCH_NUM_AGGR_PARTITIONS_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_NUM_AGGR_PARTITIONS)
}

fn bench_hash_group_by_reuse(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_group_by_reuse");
    group.sample_size(10);

    let num_rows = num_rows_from_env();
    let num_aggr_partitions = num_aggr_partitions_from_env();
    let cases = [
        ("int_low_cardinality", SchemaLayout::IntOnly, 1024),
        ("int_high_cardinality", SchemaLayout::IntOnly, num_rows / 2),
        (
            "mixed_string_view_low_cardinality",
            SchemaLayout::MixedStringView,
            1024,
        ),
        (
            "mixed_string_view_high_cardinality",
            SchemaLayout::MixedStringView,
            num_rows / 2,
        ),
    ];

    for (case_name, layout, num_groups) in cases {
        let input = GroupByInput::new(layout, num_rows, num_groups);
        group.throughput(Throughput::Elements(input.num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("single", case_name),
            &input,
            |bencher, input| bencher.iter(|| bench_single(input)),
        );
        group.bench_with_input(
            BenchmarkId::new("reuse_clear_shrink", case_name),
            &input,
            |bencher, input| {
                bencher.iter(|| bench_reuse_clear_shrink(input, num_aggr_partitions));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_hash_group_by_reuse);
criterion_main!(benches);
