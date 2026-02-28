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

//! This module provides the in-memory table for more realistic benchmarking.

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, RecordBatch, StringArray, StringViewBuilder,
    UInt64Array,
    builder::{Int64Builder, StringBuilder},
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use rand::prelude::IndexedRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use rand_distr::{Normal, Pareto};
use std::fmt::Write;
use std::sync::Arc;

/// create an in-memory table given the partition len, array len, and batch size,
/// and the result table will be of array_len in total, and then partitioned, and batched.
#[expect(clippy::allow_attributes)] // some issue where expect(dead_code) doesn't fire properly
#[allow(dead_code)]
pub fn create_table_provider(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<MemTable>> {
    let schema = Arc::new(create_schema());
    let partitions =
        create_record_batches(&schema, array_len, partitions_len, batch_size);
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    MemTable::try_new(schema, partitions).map(Arc::new)
}

/// Create test data schema
pub fn create_schema() -> Schema {
    Schema::new(vec![
        Field::new("utf8", DataType::Utf8, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, true),
        // Integers randomly selected from a wide range of values, i.e. [0,
        // u64::MAX], such that there are ~no repeated values.
        Field::new("u64_wide", DataType::UInt64, false),
        // Integers randomly selected from a mid-range of values [0, 1000),
        // providing ~1000 distinct groups.
        Field::new("u64_mid", DataType::UInt64, false),
        // Integers randomly selected from a narrow range of values such that
        // there are a few distinct values, but they are repeated often.
        Field::new("u64_narrow", DataType::UInt64, false),
    ])
}

fn create_data(rng: &mut StdRng, size: usize, null_density: f64) -> Vec<Option<f64>> {
    (0..size)
        .map(|_| {
            if rng.random::<f64>() > null_density {
                None
            } else {
                Some(rng.random::<f64>())
            }
        })
        .collect()
}

fn create_record_batch(
    schema: SchemaRef,
    rng: &mut StdRng,
    batch_size: usize,
    batch_index: usize,
) -> RecordBatch {
    // Randomly choose from 4 distinct key values; a higher number increases sparseness.
    let key_suffixes = [0, 1, 2, 3];
    let keys = StringArray::from_iter_values(
        (0..batch_size).map(|_| format!("hi{}", key_suffixes.choose(rng).unwrap())),
    );

    let values = create_data(rng, batch_size, 0.5);

    // Integer values between [0, u64::MAX].
    let integer_values_wide = (0..batch_size)
        .map(|_| rng.random::<u64>())
        .collect::<Vec<_>>();

    // Integer values between [0, 1000).
    let integer_values_mid = (0..batch_size)
        .map(|_| rng.random_range(0..1000))
        .collect::<Vec<_>>();

    // Integer values between [0, 10).
    let integer_values_narrow = (0..batch_size)
        .map(|_| rng.random_range(0..10))
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(keys),
            Arc::new(Float32Array::from(vec![batch_index as f32; batch_size])),
            Arc::new(Float64Array::from(values)),
            Arc::new(UInt64Array::from(integer_values_wide)),
            Arc::new(UInt64Array::from(integer_values_mid)),
            Arc::new(UInt64Array::from(integer_values_narrow)),
        ],
    )
    .unwrap()
}

/// Create record batches of `partitions_len` partitions and `batch_size` for each batch,
/// with a total number of `array_len` records
pub fn create_record_batches(
    schema: &SchemaRef,
    array_len: usize,
    partitions_len: usize,
    batch_size: usize,
) -> Vec<Vec<RecordBatch>> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut partitions = Vec::with_capacity(partitions_len);
    let batches_per_partition = array_len / batch_size / partitions_len;

    for _ in 0..partitions_len {
        let mut batches = Vec::with_capacity(batches_per_partition);
        for batch_index in 0..batches_per_partition {
            batches.push(create_record_batch(
                schema.clone(),
                &mut rng,
                batch_size,
                batch_index,
            ));
        }
        partitions.push(batches);
    }
    partitions
}

/// An enum that wraps either a regular StringBuilder or a GenericByteViewBuilder
/// so that both can be used interchangeably.
enum TraceIdBuilder {
    Utf8(StringBuilder),
    Utf8View(StringViewBuilder),
}

impl TraceIdBuilder {
    /// Append a value to the builder.
    fn append_value(&mut self, value: &str) {
        match self {
            TraceIdBuilder::Utf8(builder) => builder.append_value(value),
            TraceIdBuilder::Utf8View(builder) => builder.append_value(value),
        }
    }

    /// Finish building and return the ArrayRef.
    fn finish(self) -> ArrayRef {
        match self {
            TraceIdBuilder::Utf8(mut builder) => Arc::new(builder.finish()),
            TraceIdBuilder::Utf8View(mut builder) => Arc::new(builder.finish()),
        }
    }
}

/// Create time series data with `partition_cnt` partitions and `sample_cnt` rows per partition
/// in ascending order, if `asc` is true, otherwise randomly sampled using a Pareto distribution
#[expect(clippy::allow_attributes)] // some issue where expect(dead_code) doesn't fire properly
#[allow(dead_code)]
pub(crate) fn make_data(
    partition_cnt: i32,
    sample_cnt: i32,
    asc: bool,
    use_view: bool,
) -> Result<(Arc<Schema>, Vec<Vec<RecordBatch>>), DataFusionError> {
    // constants observed from trace data
    let simultaneous_group_cnt = 2000;
    let fitted_shape = 12f64;
    let fitted_scale = 5f64;
    let mean = 0.1;
    let stddev = 1.1;
    let pareto = Pareto::new(fitted_scale, fitted_shape).unwrap();
    let normal = Normal::new(mean, stddev).unwrap();
    let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);

    // populate data
    let schema = test_schema(use_view);
    let mut partitions = vec![];
    let mut cur_time = 16909000000000i64;
    for _ in 0..partition_cnt {
        // Choose the appropriate builder based on use_view.
        let mut id_builder = if use_view {
            TraceIdBuilder::Utf8View(StringViewBuilder::new())
        } else {
            TraceIdBuilder::Utf8(StringBuilder::new())
        };

        let mut ts_builder = Int64Builder::new();
        let gen_id = |rng: &mut rand::rngs::SmallRng| {
            rng.random::<[u8; 16]>()
                .iter()
                .fold(String::new(), |mut output, b| {
                    let _ = write!(output, "{b:02X}");
                    output
                })
        };
        let gen_sample_cnt =
            |mut rng: &mut rand::rngs::SmallRng| pareto.sample(&mut rng).ceil() as u32;
        let mut group_ids = (0..simultaneous_group_cnt)
            .map(|_| gen_id(&mut rng))
            .collect::<Vec<_>>();
        let mut group_sample_cnts = (0..simultaneous_group_cnt)
            .map(|_| gen_sample_cnt(&mut rng))
            .collect::<Vec<_>>();
        for _ in 0..sample_cnt {
            let random_index = rng.random_range(0..simultaneous_group_cnt);
            let trace_id = &mut group_ids[random_index];
            let sample_cnt = &mut group_sample_cnts[random_index];
            *sample_cnt -= 1;
            if *sample_cnt == 0 {
                *trace_id = gen_id(&mut rng);
                *sample_cnt = gen_sample_cnt(&mut rng);
            }

            id_builder.append_value(trace_id);
            ts_builder.append_value(cur_time);

            if asc {
                cur_time += 1;
            } else {
                let samp: f64 = normal.sample(&mut rng);
                let samp = samp.round();
                cur_time += samp as i64;
            }
        }

        // convert to MemTable
        let id_col = Arc::new(id_builder.finish());
        let ts_col = Arc::new(ts_builder.finish());
        let batch = RecordBatch::try_new(schema.clone(), vec![id_col, ts_col])?;
        partitions.push(vec![batch]);
    }
    Ok((schema, partitions))
}

/// Returns a Schema based on the use_view flag
fn test_schema(use_view: bool) -> SchemaRef {
    if use_view {
        // Return Utf8View schema
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8View, false),
            Field::new("timestamp_ms", DataType::Int64, false),
        ]))
    } else {
        // Return regular Utf8 schema
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("timestamp_ms", DataType::Int64, false),
        ]))
    }
}
