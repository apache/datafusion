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

use arrow::{
    array::Float32Array,
    array::Float64Array,
    array::StringArray,
    array::UInt64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use arrow_array::builder::{Int64Builder, StringBuilder};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use rand_distr::{Normal, Pareto};
use std::fmt::Write;
use std::sync::Arc;

/// create an in-memory table given the partition len, array len, and batch size,
/// and the result table will be of array_len in total, and then partitioned, and batched.
#[allow(dead_code)]
pub fn create_table_provider(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<MemTable>> {
    let schema = Arc::new(create_schema());
    let partitions =
        create_record_batches(schema.clone(), array_len, partitions_len, batch_size);
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    MemTable::try_new(schema, partitions).map(Arc::new)
}

/// create a seedable [`StdRng`](rand::StdRng)
fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

/// Create test data schema
pub fn create_schema() -> Schema {
    Schema::new(vec![
        Field::new("utf8", DataType::Utf8, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, true),
        // This field will contain integers randomly selected from a large
        // range of values, i.e. [0, u64::MAX], such that there are none (or
        // very few) repeated values.
        Field::new("u64_wide", DataType::UInt64, true),
        // This field will contain integers randomly selected from a narrow
        // range of values such that there are a few distinct values, but they
        // are repeated often.
        Field::new("u64_narrow", DataType::UInt64, false),
    ])
}

fn create_data(size: usize, null_density: f64) -> Vec<Option<f64>> {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f64>() > null_density {
                None
            } else {
                Some(rng.gen::<f64>())
            }
        })
        .collect()
}

fn create_integer_data(size: usize, value_density: f64) -> Vec<Option<u64>> {
    // use random numbers to avoid spurious compiler optimizations wrt to branching
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f64>() > value_density {
                None
            } else {
                Some(rng.gen::<u64>())
            }
        })
        .collect()
}

fn create_record_batch(
    schema: SchemaRef,
    rng: &mut StdRng,
    batch_size: usize,
    i: usize,
) -> RecordBatch {
    // the 4 here is the number of different keys.
    // a higher number increase sparseness
    let vs = [0, 1, 2, 3];
    let keys: Vec<String> = (0..batch_size)
        .map(
            // use random numbers to avoid spurious compiler optimizations wrt to branching
            |_| format!("hi{:?}", vs.choose(rng)),
        )
        .collect();
    let keys: Vec<&str> = keys.iter().map(|e| &**e).collect();

    let values = create_data(batch_size, 0.5);

    // Integer values between [0, u64::MAX].
    let integer_values_wide = create_integer_data(batch_size, 9.0);

    // Integer values between [0, 9].
    let integer_values_narrow = (0..batch_size)
        .map(|_| rng.gen_range(0_u64..10))
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Float32Array::from(vec![i as f32; batch_size])),
            Arc::new(Float64Array::from(values)),
            Arc::new(UInt64Array::from(integer_values_wide)),
            Arc::new(UInt64Array::from(integer_values_narrow)),
        ],
    )
    .unwrap()
}

/// Create record batches of `partitions_len` partitions and `batch_size` for each batch,
/// with a total number of `array_len` records
pub fn create_record_batches(
    schema: SchemaRef,
    array_len: usize,
    partitions_len: usize,
    batch_size: usize,
) -> Vec<Vec<RecordBatch>> {
    let mut rng = seedable_rng();
    (0..partitions_len)
        .map(|_| {
            (0..array_len / batch_size / partitions_len)
                .map(|i| create_record_batch(schema.clone(), &mut rng, batch_size, i))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

/// Create time series data with `partition_cnt` partitions and `sample_cnt` rows per partition
/// in ascending order, if `asc` is true, otherwise randomly sampled using a Pareto distribution
#[allow(dead_code)]
pub(crate) fn make_data(
    partition_cnt: i32,
    sample_cnt: i32,
    asc: bool,
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
    let schema = test_schema();
    let mut partitions = vec![];
    let mut cur_time = 16909000000000i64;
    for _ in 0..partition_cnt {
        let mut id_builder = StringBuilder::new();
        let mut ts_builder = Int64Builder::new();
        let gen_id = |rng: &mut rand::rngs::SmallRng| {
            rng.gen::<[u8; 16]>()
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
            let random_index = rng.gen_range(0..simultaneous_group_cnt);
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

/// The Schema used by make_data
fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::Int64, false),
    ]))
}
