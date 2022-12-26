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

use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::{concat_batches, SortOptions};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use hashbrown::HashMap;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::runtime::Builder;

use datafusion::physical_plan::collect;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::windows::{
    create_window_expr, BoundedWindowAggExec, WindowAggExec,
};
use datafusion_expr::{
    AggregateFunction, BuiltInWindowFunction, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunction,
};

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::PhysicalSortExpr;
use test_utils::add_empty_batches;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_order_by_test() {
        let rt = Builder::new_multi_thread()
            .worker_threads(8)
            .build()
            .unwrap();
        let n = 100;
        let handles_low_cardinality = (1..n).map(|i| {
            rt.spawn(run_window_test(
                make_staggered_batches::<true, 1>(1000, i),
                i,
                vec!["a"],
                vec![],
            ))
        });
        let handles_high_cardinality = (1..n).map(|i| {
            rt.spawn(run_window_test(
                make_staggered_batches::<true, 100>(1000, i),
                i,
                vec!["a"],
                vec![],
            ))
        });
        let handles = handles_low_cardinality
            .into_iter()
            .chain(handles_high_cardinality.into_iter())
            .collect::<Vec<tokio::task::JoinHandle<_>>>();
        rt.block_on(async {
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }

    #[test]
    fn order_by_with_partition_test() {
        let rt = Builder::new_multi_thread()
            .worker_threads(8)
            .build()
            .unwrap();
        let n = 100;
        // since we have sorted pairs (a,b) to not violate per partition soring
        // partition should be field a, order by should be field b
        let handles_low_cardinality = (1..n).map(|i| {
            rt.spawn(run_window_test(
                make_staggered_batches::<true, 1>(1000, i),
                i,
                vec!["b"],
                vec!["a"],
            ))
        });
        let handles_high_cardinality = (1..n).map(|i| {
            rt.spawn(run_window_test(
                make_staggered_batches::<true, 100>(1000, i),
                i,
                vec!["b"],
                vec!["a"],
            ))
        });
        let handles = handles_low_cardinality
            .into_iter()
            .chain(handles_high_cardinality.into_iter())
            .collect::<Vec<tokio::task::JoinHandle<_>>>();
        rt.block_on(async {
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
}

/// Perform batch and running window same input
/// and verify two outputs are equal
async fn run_window_test(
    input1: Vec<RecordBatch>,
    random_seed: u64,
    orderby_columns: Vec<&str>,
    partition_by_columns: Vec<&str>,
) {
    let mut func_name_to_window_fn = HashMap::new();
    func_name_to_window_fn.insert(
        "sum",
        WindowFunction::AggregateFunction(AggregateFunction::Sum),
    );
    func_name_to_window_fn.insert(
        "count",
        WindowFunction::AggregateFunction(AggregateFunction::Count),
    );
    func_name_to_window_fn.insert(
        "min",
        WindowFunction::AggregateFunction(AggregateFunction::Min),
    );
    func_name_to_window_fn.insert(
        "max",
        WindowFunction::AggregateFunction(AggregateFunction::Max),
    );
    func_name_to_window_fn.insert(
        "row_number",
        WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
    );

    let mut rng = StdRng::seed_from_u64(random_seed);
    let session_config = SessionConfig::new().with_batch_size(50);
    let ctx = SessionContext::with_config(session_config);
    let schema = input1[0].schema();
    let rand_fn_idx = rng.gen_range(0..func_name_to_window_fn.len());
    let fn_name = func_name_to_window_fn.keys().collect::<Vec<_>>()[rand_fn_idx];
    let window_fn = func_name_to_window_fn.values().collect::<Vec<_>>()[rand_fn_idx];
    let preceding = rng.gen_range(0..50);
    let following = rng.gen_range(0..50);
    let rand_num = rng.gen_range(0..3);
    let units = if rand_num < 1 {
        WindowFrameUnits::Range
    } else if rand_num < 2 {
        WindowFrameUnits::Rows
    } else {
        // For now we do not support GROUPS in BoundedWindowAggExec implementation
        // TODO: once GROUPS handling is available, use WindowFrameUnits::GROUPS in randomized tests also.
        WindowFrameUnits::Range
    };
    let window_frame = match units {
        // In range queries window frame boundaries should match column type
        WindowFrameUnits::Range => WindowFrame {
            units,
            start_bound: WindowFrameBound::Preceding(ScalarValue::Int32(Some(preceding))),
            end_bound: WindowFrameBound::Following(ScalarValue::Int32(Some(following))),
        },
        // In window queries, window frame boundary should be Uint64
        WindowFrameUnits::Rows => WindowFrame {
            units,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                preceding as u64,
            ))),
            end_bound: WindowFrameBound::Following(ScalarValue::UInt64(Some(
                following as u64,
            ))),
        },
        // Once GROUPS support is added construct window frame for this case also
        _ => todo!(),
    };
    let mut orderby_exprs = vec![];
    for column in orderby_columns {
        orderby_exprs.push(PhysicalSortExpr {
            expr: col(column, &schema).unwrap(),
            options: SortOptions::default(),
        })
    }
    let mut partitionby_exprs = vec![];
    for column in partition_by_columns {
        partitionby_exprs.push(col(column, &schema).unwrap());
    }
    let mut sort_keys = vec![];
    for partition_by_expr in &partitionby_exprs {
        sort_keys.push(PhysicalSortExpr {
            expr: partition_by_expr.clone(),
            options: SortOptions::default(),
        })
    }
    for order_by_expr in &orderby_exprs {
        sort_keys.push(order_by_expr.clone())
    }

    let concat_input_record = concat_batches(&schema, &input1).unwrap();
    let exec1 = Arc::new(
        MemoryExec::try_new(&[vec![concat_input_record]], schema.clone(), None).unwrap(),
    );
    let usual_window_exec = Arc::new(
        WindowAggExec::try_new(
            vec![create_window_expr(
                window_fn,
                fn_name.to_string(),
                &[col("x", &schema).unwrap()],
                &partitionby_exprs,
                &orderby_exprs,
                Arc::new(window_frame.clone()),
                schema.as_ref(),
            )
            .unwrap()],
            exec1,
            schema.clone(),
            vec![],
            Some(sort_keys.clone()),
        )
        .unwrap(),
    );
    let exec2 =
        Arc::new(MemoryExec::try_new(&[input1.clone()], schema.clone(), None).unwrap());
    let running_window_exec = Arc::new(
        BoundedWindowAggExec::try_new(
            vec![create_window_expr(
                window_fn,
                fn_name.to_string(),
                &[col("x", &schema).unwrap()],
                &partitionby_exprs,
                &orderby_exprs,
                Arc::new(window_frame.clone()),
                schema.as_ref(),
            )
            .unwrap()],
            exec2,
            schema.clone(),
            vec![],
            Some(sort_keys),
        )
        .unwrap(),
    );

    let task_ctx = ctx.task_ctx();
    let collected_usual = collect(usual_window_exec, task_ctx.clone()).await.unwrap();

    let collected_running = collect(running_window_exec, task_ctx.clone())
        .await
        .unwrap();
    // compare
    let usual_formatted = pretty_format_batches(&collected_usual).unwrap().to_string();
    let running_formatted = pretty_format_batches(&collected_running)
        .unwrap()
        .to_string();

    let mut usual_formatted_sorted: Vec<&str> = usual_formatted.trim().lines().collect();
    usual_formatted_sorted.sort_unstable();

    let mut running_formatted_sorted: Vec<&str> =
        running_formatted.trim().lines().collect();
    running_formatted_sorted.sort_unstable();
    for (i, (usual_line, running_line)) in usual_formatted_sorted
        .iter()
        .zip(&running_formatted_sorted)
        .enumerate()
    {
        assert_eq!((i, usual_line), (i, running_line));
    }
}

/// Return randomly sized record batches with:
/// two sorted int32 columns 'a', 'b' ranged from 0..len / DISTINCT as columns
/// two random int32 columns 'x', 'y' as other columns
fn make_staggered_batches<const STREAM: bool, const DISTINCT: usize>(
    len: usize,
    random_seed: u64,
) -> Vec<RecordBatch> {
    // use a random number generator to pick a random sized output
    let mut rng = StdRng::seed_from_u64(random_seed);
    let mut input12: Vec<(i32, i32)> = vec![(0, 0); len];
    let mut input3: Vec<i32> = vec![0; len];
    let mut input4: Vec<i32> = vec![0; len];
    input12.iter_mut().for_each(|v| {
        *v = (
            rng.gen_range(0..(len / DISTINCT)) as i32,
            rng.gen_range(0..(len / DISTINCT)) as i32,
        )
    });
    rng.fill(&mut input3[..]);
    rng.fill(&mut input4[..]);
    input12.sort();
    let input1 = Int32Array::from_iter_values(input12.clone().into_iter().map(|k| k.0));
    let input2 = Int32Array::from_iter_values(input12.clone().into_iter().map(|k| k.1));
    let input3 = Int32Array::from_iter_values(input3.into_iter());
    let input4 = Int32Array::from_iter_values(input4.into_iter());

    // split into several record batches
    let mut remainder = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(input1) as ArrayRef),
        ("b", Arc::new(input2) as ArrayRef),
        ("x", Arc::new(input3) as ArrayRef),
        ("y", Arc::new(input4) as ArrayRef),
    ])
    .unwrap();

    let mut batches = vec![];
    if STREAM {
        while remainder.num_rows() > 0 {
            let batch_size = rng.gen_range(0..50);
            if remainder.num_rows() < batch_size {
                break;
            }
            batches.push(remainder.slice(0, batch_size));
            remainder = remainder.slice(batch_size, remainder.num_rows() - batch_size);
        }
    } else {
        while remainder.num_rows() > 0 {
            let batch_size = rng.gen_range(0..remainder.num_rows() + 1);
            batches.push(remainder.slice(0, batch_size));
            remainder = remainder.slice(batch_size, remainder.num_rows() - batch_size);
        }
    }
    add_empty_batches(batches, &mut rng)
}
