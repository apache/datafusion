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

//! What it costs to run an [`Accumulator`] through [`GroupsAccumulatorAdapter`],
//! over a sweep of `GROUP BY` cardinalities.
//!
//! Two accumulators, because the adapter's cost and the aggregate's cost move
//! in opposite directions as the group count grows:
//!
//! * `routing` wraps an accumulator that only counts the rows it is handed, so
//!   what the benchmark measures is the adapter routing the batch and nothing
//!   else. It is the upper bound on what a change to the routing can do.
//! * `covar_samp` wraps a real aggregate that has no native `GroupsAccumulator`,
//!   so it shows how much of that upper bound a query actually sees.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{Accumulator, EmitTo, GroupsAccumulator};
use datafusion_functions_aggregate::covariance::covar_samp_udaf;
use datafusion_physical_expr::GroupsAccumulatorAdapter;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const BATCH_SIZE: usize = 8192;
const NUM_BATCHES: usize = 128;
const CARDINALITIES: [usize; 5] = [64, 1024, 16_384, 262_144, 1_000_000];

/// The cheapest thing that is still an [`Accumulator`]: it only counts the rows
/// it is handed, so what a benchmark over it measures is the adapter.
#[derive(Debug, Default)]
struct CountingAccumulator {
    count: i64,
}

impl Accumulator for CountingAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.count += values[0].len() as i64;
        Ok(())
    }
    fn merge_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.update_batch(values)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }
    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

/// A factory for the `covar_samp` [`Accumulator`], which is what the aggregate
/// operator falls back to because `covar_samp` has no native
/// `GroupsAccumulator`.
fn covar_samp_factory() -> impl Fn() -> Result<Box<dyn Accumulator>> + Send + 'static {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
    ]));
    let args = vec![col("a", &schema).unwrap(), col("b", &schema).unwrap()];
    let agg = Arc::new(
        AggregateExprBuilder::new(covar_samp_udaf(), args)
            .schema(schema)
            .alias("covar_samp(a, b)")
            .build()
            .unwrap(),
    );
    move || agg.create_accumulator()
}

/// `num_columns` copies of the same values, so the same batches drive both a
/// one-argument and a two-argument aggregate.
fn make_batches(
    num_groups: usize,
    num_columns: usize,
) -> Vec<(Vec<ArrayRef>, Vec<usize>)> {
    let mut rng = StdRng::seed_from_u64(7);
    (0..NUM_BATCHES)
        .map(|_| {
            let group_indices: Vec<usize> = (0..BATCH_SIZE)
                .map(|_| rng.random_range(0..num_groups))
                .collect();
            let values: ArrayRef = Arc::new(
                (0..BATCH_SIZE)
                    .map(|_| Some(rng.random::<f64>()))
                    .collect::<Float64Array>(),
            );
            (vec![values; num_columns], group_indices)
        })
        .collect()
}

/// Feeds every batch into a fresh adapter and emits every group, which is what
/// one partition of an `AggregateExec` does.
fn run(
    factory: impl Fn() -> Result<Box<dyn Accumulator>> + Send + 'static,
    data: &[(Vec<ArrayRef>, Vec<usize>)],
    num_groups: usize,
) {
    let mut accumulator = GroupsAccumulatorAdapter::new(factory);
    for (values, group_indices) in data {
        accumulator
            .update_batch(values, group_indices, None, num_groups)
            .unwrap();
    }
    black_box(accumulator.evaluate(EmitTo::All).unwrap());
}

fn adapter_benchmark(c: &mut Criterion) {
    for (name, num_columns) in [("routing", 1), ("covar_samp", 2)] {
        let mut group = c.benchmark_group(format!("adapter_{name}"));
        group.sample_size(20);
        group.throughput(Throughput::Elements((BATCH_SIZE * NUM_BATCHES) as u64));

        for num_groups in CARDINALITIES {
            let data = make_batches(num_groups, num_columns);
            group.bench_with_input(
                BenchmarkId::from_parameter(num_groups),
                &data,
                |b, data| {
                    b.iter(|| {
                        if num_columns == 1 {
                            run(
                                || Ok(Box::new(CountingAccumulator::default()) as _),
                                data,
                                num_groups,
                            )
                        } else {
                            run(covar_samp_factory(), data, num_groups)
                        }
                    })
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, adapter_benchmark);
criterion_main!(benches);
