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

//! Benchmarks the cost of splitting input batches across many coalescers.
//!
//! This benchmark intentionally runs without locks. It isolates the local cost
//! of slicing each input batch into many pieces and pushing those slices into
//! independent `LimitedBatchCoalescer` instances.

use std::env;
use std::fmt::Display;
use std::hint::black_box;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_physical_plan::coalesce::LimitedBatchCoalescer;

const DEFAULT_NUM_ROWS: usize = 8_324_096;
const DEFAULT_NUM_COLS: usize = 4;
const DEFAULT_NUM_ROWS_PER_BATCH: usize = 8129;
const DEFAULT_TARGET_BATCH_SIZE: usize = 8192;
const NUM_COALESCERS_CASES: [usize; 3] = [8, 64, 256];

const NUM_ROWS_ENV: &str = "DF_LIMITED_COALESCER_SPLIT_ROWS";
const NUM_COLS_ENV: &str = "DF_LIMITED_COALESCER_SPLIT_COLS";
const LAYOUT_ENV: &str = "DF_LIMITED_COALESCER_SPLIT_LAYOUT";
const NUM_ROWS_PER_BATCH_ENV: &str = "DF_LIMITED_COALESCER_SPLIT_BATCH_ROWS";
const TARGET_BATCH_SIZE_ENV: &str = "DF_LIMITED_COALESCER_SPLIT_TARGET_BATCH_SIZE";

#[derive(Clone, Copy)]
enum ColumnLayout {
    IntOnly,
    StringOnly,
    IntStringMixed,
}

impl ColumnLayout {
    fn data_type(self, col_idx: usize) -> DataType {
        match self {
            Self::IntOnly => DataType::Int32,
            Self::StringOnly => DataType::Utf8View,
            Self::IntStringMixed => {
                if col_idx.is_multiple_of(2) {
                    DataType::Int32
                } else {
                    DataType::Utf8View
                }
            }
        }
    }
}

impl Display for ColumnLayout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IntOnly => formatter.write_str("int_only"),
            Self::StringOnly => formatter.write_str("string_only"),
            Self::IntStringMixed => formatter.write_str("int_string_mixed"),
        }
    }
}

impl FromStr for ColumnLayout {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "int" | "int_only" => Ok(Self::IntOnly),
            "string" | "string_only" => Ok(Self::StringOnly),
            "mixed" | "int_string_mixed" => Ok(Self::IntStringMixed),
            other => Err(format!(
                "unsupported column layout {other:?}; expected int, string, or mixed"
            )),
        }
    }
}

struct BenchConfig {
    num_rows: usize,
    num_cols: usize,
    layout: ColumnLayout,
    num_rows_per_batch: usize,
    target_batch_size: usize,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            num_rows: env_usize(NUM_ROWS_ENV, DEFAULT_NUM_ROWS),
            num_cols: env_usize(NUM_COLS_ENV, DEFAULT_NUM_COLS),
            layout: env_layout(),
            num_rows_per_batch: env_usize(
                NUM_ROWS_PER_BATCH_ENV,
                DEFAULT_NUM_ROWS_PER_BATCH,
            ),
            target_batch_size: env_usize(
                TARGET_BATCH_SIZE_ENV,
                DEFAULT_TARGET_BATCH_SIZE,
            ),
        }
    }

    fn schema(&self) -> SchemaRef {
        let fields = (0..self.num_cols)
            .map(|col_idx| {
                Field::new(
                    format!("col_{col_idx}"),
                    self.layout.data_type(col_idx),
                    false,
                )
            })
            .collect::<Vec<_>>();
        Arc::new(Schema::new(fields))
    }

    fn bench_name(&self, num_coalescers: usize) -> String {
        format!(
            "layout={}/cols={}/rows={}/batch_rows={}/coalescers={}",
            self.layout,
            self.num_cols,
            self.num_rows,
            self.num_rows_per_batch,
            num_coalescers
        )
    }
}

struct CoalescerSplitInput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    target_batch_size: usize,
}

impl CoalescerSplitInput {
    fn new(config: &BenchConfig) -> Self {
        let schema = config.schema();
        let num_batches = config.num_rows.div_ceil(config.num_rows_per_batch);
        let batches = (0..num_batches)
            .map(|batch_idx| make_batch(Arc::clone(&schema), config, batch_idx))
            .collect();

        Self {
            schema,
            batches,
            target_batch_size: config.target_batch_size,
        }
    }

    fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

fn make_batch(schema: SchemaRef, config: &BenchConfig, batch_idx: usize) -> RecordBatch {
    let batch_start = batch_idx * config.num_rows_per_batch;
    let num_rows = config
        .num_rows_per_batch
        .min(config.num_rows.saturating_sub(batch_start));
    let columns = (0..config.num_cols)
        .map(|col_idx| {
            make_column(
                config.layout.data_type(col_idx),
                batch_start,
                num_rows,
                col_idx,
            )
        })
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

fn make_column(
    data_type: DataType,
    batch_start: usize,
    num_rows: usize,
    col_idx: usize,
) -> ArrayRef {
    match data_type {
        DataType::Int32 => make_int32_column(batch_start, num_rows, col_idx),
        DataType::Utf8View => make_string_view_column(batch_start, num_rows, col_idx),
        other => unreachable!("unexpected benchmark data type {other:?}"),
    }
}

fn make_int32_column(batch_start: usize, num_rows: usize, col_idx: usize) -> ArrayRef {
    let values: Vec<i32> = (0..num_rows)
        .map(|row_idx| ((batch_start + row_idx + col_idx) % 4096) as i32)
        .collect();
    Arc::new(Int32Array::from(values))
}

fn make_string_view_column(
    batch_start: usize,
    num_rows: usize,
    col_idx: usize,
) -> ArrayRef {
    let values: Vec<String> = (0..num_rows)
        .map(|row_idx| {
            let value = (batch_start + row_idx + col_idx) % 4096;
            format!("group_key_{value:016}")
        })
        .collect();
    Arc::new(StringViewArray::from(values))
}

fn bench_split_push(input: &CoalescerSplitInput, num_coalescers: usize) {
    let mut coalescers =
        make_coalescers(&input.schema, num_coalescers, input.target_batch_size);
    let mut num_output_rows = 0;

    for batch in &input.batches {
        push_split_batch(batch, &mut coalescers, num_coalescers, &mut num_output_rows);
    }

    for coalescer in &mut coalescers {
        coalescer.finish().unwrap();
        drain_coalescer(coalescer, &mut num_output_rows);
    }

    black_box(num_output_rows);
}

fn make_coalescers(
    schema: &SchemaRef,
    num_coalescers: usize,
    target_batch_size: usize,
) -> Vec<LimitedBatchCoalescer> {
    (0..num_coalescers)
        .map(|_| LimitedBatchCoalescer::new(Arc::clone(schema), target_batch_size, None))
        .collect()
}

fn push_split_batch(
    batch: &RecordBatch,
    coalescers: &mut [LimitedBatchCoalescer],
    num_coalescers: usize,
    num_output_rows: &mut usize,
) {
    let rows_per_slice = batch.num_rows().div_ceil(num_coalescers);

    for (coalescer_idx, coalescer) in coalescers.iter_mut().enumerate() {
        let offset = coalescer_idx * rows_per_slice;
        if offset >= batch.num_rows() {
            break;
        }

        let slice_len = rows_per_slice.min(batch.num_rows() - offset);
        coalescer
            .push_batch(batch.slice(offset, slice_len))
            .unwrap();
        drain_coalescer(coalescer, num_output_rows);
    }
}

fn drain_coalescer(coalescer: &mut LimitedBatchCoalescer, num_output_rows: &mut usize) {
    while let Some(batch) = coalescer.next_completed_batch() {
        *num_output_rows += batch.num_rows();
        black_box(batch);
    }
}

fn env_usize(name: &str, default_value: usize) -> usize {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|err| panic!("failed to parse {name}={value:?}: {err}"))
        })
        .unwrap_or(default_value)
}

fn env_layout() -> ColumnLayout {
    env::var(LAYOUT_ENV)
        .ok()
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|err| panic!("failed to parse {LAYOUT_ENV}: {err}"))
        })
        .unwrap_or(ColumnLayout::IntStringMixed)
}

fn bench_limited_coalescer_split(c: &mut Criterion) {
    let config = BenchConfig::from_env();
    let input = CoalescerSplitInput::new(&config);
    let mut group = c.benchmark_group("limited_coalescer_split");
    group.sample_size(10);
    group.throughput(Throughput::Elements(input.num_rows() as u64));

    for num_coalescers in NUM_COALESCERS_CASES {
        group.bench_with_input(
            BenchmarkId::from_parameter(config.bench_name(num_coalescers)),
            &num_coalescers,
            |bencher, &num_coalescers| {
                bencher.iter(|| bench_split_push(&input, num_coalescers));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_limited_coalescer_split);
criterion_main!(benches);
