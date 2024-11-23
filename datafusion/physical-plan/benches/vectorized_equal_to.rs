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

use std::sync::Arc;

use arrow::compute;
use arrow::datatypes::{Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray, UInt64Array};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::aggregates::group_values::multi_group_by::primitive::PrimitiveGroupValueBuilder;
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupColumn;
use datafusion_physical_plan::memory::MemoryExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{collect, ExecutionPlan};

use criterion::async_executor::FuturesExecutor;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{thread_rng, Rng, SeedableRng};
use test_utils::array_gen::PrimitiveArrayGenerator;

struct PrimitiveEqualToBench {
    group_column: Box<dyn GroupColumn>,
    lhs_rows: Vec<usize>,
    input_array: ArrayRef,
    rhs_rows: Vec<usize>,
    results: Vec<bool>,
    vectorized: bool,
}

impl PrimitiveEqualToBench {
    pub fn new(
        num_rows: usize,
        num_distinct: usize,
        num_compared_rows: usize,
        nullable: bool,
        vectorized: bool,
    ) -> Self {
        let mut rng = thread_rng();

        // Generate group array
        let null_pct = if nullable { 0.5 } else { 0.0 };
        let mut data_gen = PrimitiveArrayGenerator {
            num_primitives: num_rows,
            num_distinct_primitives: num_distinct,
            null_pct,
            rng: StdRng::from_seed(rng.gen()),
        };
        let group_array = data_gen.gen_data::<Int64Type>();
        let group_len = group_array.len();

        // Generate compared array
        let mut compared_indices = Vec::with_capacity(num_compared_rows);
        for _ in 0..num_compared_rows {
            let compared_idx = rng.gen_range(0..group_len);
            compared_indices.push(compared_idx as u64);
        }
        let compared_indices_array = UInt64Array::from(compared_indices.clone());
        let compared_array =
            compute::take(&group_array, &compared_indices_array, None).unwrap();

        // Create group column, insert the group array
        let mut group_column: Box<dyn GroupColumn> = if nullable {
            Box::new(PrimitiveGroupValueBuilder::<Int64Type, true>::new())
        } else {
            Box::new(PrimitiveGroupValueBuilder::<Int64Type, false>::new())
        };
        let group_rows = (0..group_len).into_iter().collect::<Vec<_>>();
        group_column.vectorized_append(&group_array, &group_rows);

        // Build bench
        let lhs_rows = compared_indices
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();
        let rhs_rows = (0..num_compared_rows).into_iter().collect::<Vec<_>>();

        Self {
            group_column,
            lhs_rows,
            input_array: compared_array,
            rhs_rows,
            results: vec![true; num_compared_rows],
            vectorized,
        }
    }

    fn run(&mut self) {
        if self.vectorized {
            self.group_column.vectorized_equal_to(
                &self.lhs_rows,
                &self.input_array,
                &self.rhs_rows,
                &mut self.results,
            );
        } else {
            let iter = self.lhs_rows.iter().zip(self.rhs_rows.iter());
            for (&lhs_row, &rhs_row) in iter {
                self.group_column
                    .equal_to(lhs_row, &self.input_array, rhs_row);
            }
        }
    }
}

fn run_bench(c: &mut Criterion, description: &str, nullable: bool, vectorized: bool) {
    let mut bench = PrimitiveEqualToBench::new(81920, 8192, 8192, nullable, vectorized);
    c.bench_function(description, |b| b.iter(|| black_box(bench.run())));
}

fn criterion_benchmark(c: &mut Criterion) {
    run_bench(c, "### vectorized + nullable", true, true);
    run_bench(c, "### vectorized + non-nullable", false, true);
    run_bench(c, "### scalarized + nullable", true, false);
    run_bench(c, "### scalarized + non-nullable", false, false);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
