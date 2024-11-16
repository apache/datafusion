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

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray};
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
    lhr_rows: Vec<usize>,
    input_array: ArrayRef,
    rhs_rows: Vec<usize>,
}

impl PrimitiveEqualToBench {
    pub fn new(null_pct: f64, nullable: bool) {
        // Generate array
        let mut data_gen = PrimitiveArrayGenerator {
            num_primitives: 8192,
            num_distinct_primitives: 64,
            null_pct,
            rng: StdRng::from_seed(thread_rng().gen()),
        };
        // let primitive_array = data_gen.gen_data();

        // 
        let group_colum: Box<dyn GroupColumn> = if nullable {
            Box::new(PrimitiveGroupValueBuilder::<Int64Type, true>::new())
        } else {
            Box::new(PrimitiveGroupValueBuilder::<Int64Type, false>::new())
        };
    }
}

// fn run_bench(
//     c: &mut Criterion,
//     has_same_value: bool,
//     enable_round_robin_repartition: bool,
//     batch_count: usize,
//     partition_count: usize,
//     description: &str,
// ) {
//     let task_ctx = TaskContext::default();
//     let task_ctx = Arc::new(task_ctx);

//     let spm = Arc::new(generate_spm_for_round_robin_tie_breaker(
//         has_same_value,
//         enable_round_robin_repartition,
//         batch_count,
//         partition_count,
//     )) as Arc<dyn ExecutionPlan>;

//     c.bench_function(description, |b| {
//         b.to_async(FuturesExecutor)
//             .iter(|| black_box(collect(Arc::clone(&spm), Arc::clone(&task_ctx))))
//     });
// }



// fn criterion_benchmark(c: &mut Criterion) {
//     let params = [
//         (true, false, "low_card_without_tiebreaker"), // low cardinality, no tie breaker
//         (true, true, "low_card_with_tiebreaker"),     // low cardinality, with tie breaker
//         (false, false, "high_card_without_tiebreaker"), // high cardinality, no tie breaker
//         (false, true, "high_card_with_tiebreaker"), // high cardinality, with tie breaker
//     ];

//     let batch_counts = [1, 25, 625];
//     let partition_counts = [2, 8, 32];

//     for &(has_same_value, enable_round_robin_repartition, cardinality_label) in &params {
//         for &batch_count in &batch_counts {
//             for &partition_count in &partition_counts {
//                 let description = format!(
//                     "{}_batch_count_{}_partition_count_{}",
//                     cardinality_label, batch_count, partition_count
//                 );
//                 run_bench(
//                     c,
//                     has_same_value,
//                     enable_round_robin_repartition,
//                     batch_count,
//                     partition_count,
//                     &description,
//                 );
//             }
//         }
//     }
// }

// criterion_group!(benches, criterion_benchmark);
// criterion_main!(benches);
