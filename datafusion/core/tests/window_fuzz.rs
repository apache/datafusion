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
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use hashbrown::HashMap;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{
    create_window_expr, BoundedWindowAggExec, PartitionSearchMode, WindowAggExec,
};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion_expr::{
    AggregateFunction, BuiltInWindowFunction, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunction,
};

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::expressions::{col, lit};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use test_utils::add_empty_batches;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::windows::PartitionSearchMode::{
        Linear, PartiallySorted, Sorted,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn window_bounded_window_random_comparison() -> Result<()> {
        // make_staggered_batches gives result sorted according to a, b, c
        // In the test cases first entry represents partition by columns
        // Second entry represents order by columns.
        // Third entry represents search mode.
        // In sorted mode physical plans are in the form for WindowAggExec
        //```
        // WindowAggExec
        //   MemoryExec]
        // ```
        // and in the form for BoundedWindowAggExec
        // ```
        // BoundedWindowAggExec
        //   MemoryExec
        // ```
        // In Linear and PartiallySorted mode physical plans are in the form for WindowAggExec
        //```
        // WindowAggExec
        //   SortExec(required by window function)
        //     MemoryExec]
        // ```
        // and in the form for BoundedWindowAggExec
        // ```
        // BoundedWindowAggExec
        //   MemoryExec
        // ```
        let test_cases = vec![
            (vec!["a"], vec!["a"], Sorted),
            (vec!["a"], vec!["b"], Sorted),
            (vec!["a"], vec!["a", "b"], Sorted),
            (vec!["a"], vec!["b", "c"], Sorted),
            (vec!["a"], vec!["a", "b", "c"], Sorted),
            (vec!["b"], vec!["a"], Linear),
            (vec!["b"], vec!["a", "b"], Linear),
            (vec!["b"], vec!["a", "c"], Linear),
            (vec!["b"], vec!["a", "b", "c"], Linear),
            (vec!["c"], vec!["a"], Linear),
            (vec!["c"], vec!["a", "b"], Linear),
            (vec!["c"], vec!["a", "c"], Linear),
            (vec!["c"], vec!["a", "b", "c"], Linear),
            (vec!["b", "a"], vec!["a"], Sorted),
            (vec!["b", "a"], vec!["b"], Sorted),
            (vec!["b", "a"], vec!["c"], Sorted),
            (vec!["b", "a"], vec!["a", "b"], Sorted),
            (vec!["b", "a"], vec!["b", "c"], Sorted),
            (vec!["b", "a"], vec!["a", "c"], Sorted),
            (vec!["b", "a"], vec!["a", "b", "c"], Sorted),
            (vec!["c", "b"], vec!["a"], Linear),
            (vec!["c", "b"], vec!["a", "b"], Linear),
            (vec!["c", "b"], vec!["a", "c"], Linear),
            (vec!["c", "b"], vec!["a", "b", "c"], Linear),
            (vec!["c", "a"], vec!["a"], PartiallySorted(vec![1])),
            (vec!["c", "a"], vec!["b"], PartiallySorted(vec![1])),
            (vec!["c", "a"], vec!["c"], PartiallySorted(vec![1])),
            (vec!["c", "a"], vec!["a", "b"], PartiallySorted(vec![1])),
            (vec!["c", "a"], vec!["b", "c"], PartiallySorted(vec![1])),
            (vec!["c", "a"], vec!["a", "c"], PartiallySorted(vec![1])),
            (
                vec!["c", "a"],
                vec!["a", "b", "c"],
                PartiallySorted(vec![1]),
            ),
            (vec!["c", "b", "a"], vec!["a"], Sorted),
            (vec!["c", "b", "a"], vec!["b"], Sorted),
            (vec!["c", "b", "a"], vec!["c"], Sorted),
            (vec!["c", "b", "a"], vec!["a", "b"], Sorted),
            (vec!["c", "b", "a"], vec!["b", "c"], Sorted),
            (vec!["c", "b", "a"], vec!["a", "c"], Sorted),
            (vec!["c", "b", "a"], vec!["a", "b", "c"], Sorted),
        ];
        let n = 300;
        let n_distincts = vec![10, 20];
        for n_distinct in n_distincts {
            let mut handles = Vec::new();
            for i in 0..n {
                let idx = i % test_cases.len();
                let (pb_cols, ob_cols, search_mode) = test_cases[idx].clone();
                let job = tokio::spawn(run_window_test(
                    make_staggered_batches::<true>(1000, n_distinct, i as u64),
                    i as u64,
                    pb_cols,
                    ob_cols,
                    search_mode,
                ));
                handles.push(job);
            }
            for job in handles {
                job.await.unwrap()?;
            }
        }
        Ok(())
    }
}

fn get_random_function(
    schema: &SchemaRef,
    rng: &mut StdRng,
    is_linear: bool,
) -> (WindowFunction, Vec<Arc<dyn PhysicalExpr>>, String) {
    let mut args = if is_linear {
        // In linear test for the test version with WindowAggExec we use insert SortExecs to the plan to be able to generate
        // same result with BoundedWindowAggExec which doesn't use any SortExec. To make result
        // non-dependent on table order. We should use column a in the window function
        // (Given that we do not use ROWS for the window frame. ROWS also introduces dependency to the table order.).
        vec![col("a", schema).unwrap()]
    } else {
        vec![col("x", schema).unwrap()]
    };
    let mut window_fn_map = HashMap::new();
    // HashMap values consists of tuple first element is WindowFunction, second is additional argument
    // window function requires if any. For most of the window functions additional argument is empty
    window_fn_map.insert(
        "sum",
        (
            WindowFunction::AggregateFunction(AggregateFunction::Sum),
            vec![],
        ),
    );
    window_fn_map.insert(
        "count",
        (
            WindowFunction::AggregateFunction(AggregateFunction::Count),
            vec![],
        ),
    );
    window_fn_map.insert(
        "min",
        (
            WindowFunction::AggregateFunction(AggregateFunction::Min),
            vec![],
        ),
    );
    window_fn_map.insert(
        "max",
        (
            WindowFunction::AggregateFunction(AggregateFunction::Max),
            vec![],
        ),
    );
    if !is_linear {
        // row_number, rank, lead, lag doesn't use its window frame to calculate result. Their results are calculated
        // according to table scan order. This adds the dependency to table order. Hence do not use these functions in
        // Partition by linear test.
        window_fn_map.insert(
            "row_number",
            (
                WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
                vec![],
            ),
        );
        window_fn_map.insert(
            "rank",
            (
                WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::Rank),
                vec![],
            ),
        );
        window_fn_map.insert(
            "lead",
            (
                WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::Lead),
                vec![
                    lit(ScalarValue::Int64(Some(rng.gen_range(1..10)))),
                    lit(ScalarValue::Int64(Some(rng.gen_range(1..1000)))),
                ],
            ),
        );
        window_fn_map.insert(
            "lag",
            (
                WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::Lag),
                vec![
                    lit(ScalarValue::Int64(Some(rng.gen_range(1..10)))),
                    lit(ScalarValue::Int64(Some(rng.gen_range(1..1000)))),
                ],
            ),
        );
    }
    window_fn_map.insert(
        "first_value",
        (
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::FirstValue),
            vec![],
        ),
    );
    window_fn_map.insert(
        "last_value",
        (
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::LastValue),
            vec![],
        ),
    );
    window_fn_map.insert(
        "nth_value",
        (
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::NthValue),
            vec![lit(ScalarValue::Int64(Some(rng.gen_range(1..10))))],
        ),
    );

    let rand_fn_idx = rng.gen_range(0..window_fn_map.len());
    let fn_name = window_fn_map.keys().collect::<Vec<_>>()[rand_fn_idx];
    let (window_fn, new_args) = window_fn_map.values().collect::<Vec<_>>()[rand_fn_idx];
    for new_arg in new_args {
        args.push(new_arg.clone());
    }

    (window_fn.clone(), args, fn_name.to_string())
}

fn get_random_window_frame(rng: &mut StdRng, is_linear: bool) -> WindowFrame {
    struct Utils {
        val: i32,
        is_preceding: bool,
    }
    let first_bound = Utils {
        val: rng.gen_range(0..10),
        is_preceding: rng.gen_range(0..2) == 0,
    };
    let second_bound = Utils {
        val: rng.gen_range(0..10),
        is_preceding: rng.gen_range(0..2) == 0,
    };
    let (start_bound, end_bound) =
        if first_bound.is_preceding == second_bound.is_preceding {
            if (first_bound.val > second_bound.val && first_bound.is_preceding)
                || (first_bound.val < second_bound.val && !first_bound.is_preceding)
            {
                (first_bound, second_bound)
            } else {
                (second_bound, first_bound)
            }
        } else if first_bound.is_preceding {
            (first_bound, second_bound)
        } else {
            (second_bound, first_bound)
        };
    // 0 means Range, 1 means Rows, 2 means GROUPS
    let rand_num = rng.gen_range(0..3);
    let units = if rand_num < 1 {
        WindowFrameUnits::Range
    } else if rand_num < 2 {
        if is_linear {
            // In linear test we sort data for WindowAggExec
            // However, we do not sort data for BoundedWindowExec
            // Since sorting is unstable, to make sure final result are comparable after sorting
            // We shouldn't use Rows. This would add dependency to the table order.
            WindowFrameUnits::Range
        } else {
            WindowFrameUnits::Rows
        }
    } else {
        WindowFrameUnits::Groups
    };
    match units {
        // In range queries window frame boundaries should match column type
        WindowFrameUnits::Range => {
            let start_bound = if start_bound.is_preceding {
                WindowFrameBound::Preceding(ScalarValue::Int32(Some(start_bound.val)))
            } else {
                WindowFrameBound::Following(ScalarValue::Int32(Some(start_bound.val)))
            };
            let end_bound = if end_bound.is_preceding {
                WindowFrameBound::Preceding(ScalarValue::Int32(Some(end_bound.val)))
            } else {
                WindowFrameBound::Following(ScalarValue::Int32(Some(end_bound.val)))
            };
            let mut window_frame = WindowFrame {
                units,
                start_bound,
                end_bound,
            };
            // with 10% use unbounded preceding in tests
            if rng.gen_range(0..10) == 0 {
                window_frame.start_bound =
                    WindowFrameBound::Preceding(ScalarValue::Int32(None));
            }
            window_frame
        }
        // Window frame boundary should be UInt64 for both ROWS and GROUPS frames:
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => {
            let start_bound = if start_bound.is_preceding {
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                    start_bound.val as u64,
                )))
            } else {
                WindowFrameBound::Following(ScalarValue::UInt64(Some(
                    start_bound.val as u64,
                )))
            };
            let end_bound = if end_bound.is_preceding {
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                    end_bound.val as u64,
                )))
            } else {
                WindowFrameBound::Following(ScalarValue::UInt64(Some(
                    end_bound.val as u64,
                )))
            };
            let mut window_frame = WindowFrame {
                units,
                start_bound,
                end_bound,
            };
            // with 10% use unbounded preceding in tests
            if rng.gen_range(0..10) == 0 {
                window_frame.start_bound =
                    WindowFrameBound::Preceding(ScalarValue::UInt64(None));
            }
            // We never use UNBOUNDED FOLLOWING in test. Because that case is not prunable and
            // should work only with WindowAggExec
            window_frame
        }
    }
}

/// Perform batch and running window same input
/// and verify outputs of `WindowAggExec` and `BoundedWindowAggExec` are equal
async fn run_window_test(
    input1: Vec<RecordBatch>,
    random_seed: u64,
    partition_by_columns: Vec<&str>,
    orderby_columns: Vec<&str>,
    search_mode: PartitionSearchMode,
) -> Result<()> {
    let is_linear = !matches!(search_mode, PartitionSearchMode::Sorted);
    let mut rng = StdRng::seed_from_u64(random_seed);
    let schema = input1[0].schema();
    let session_config = SessionConfig::new().with_batch_size(50);
    let ctx = SessionContext::with_config(session_config);
    let (window_fn, args, fn_name) = get_random_function(&schema, &mut rng, is_linear);

    let window_frame = get_random_window_frame(&mut rng, is_linear);
    let mut orderby_exprs = vec![];
    for column in &orderby_columns {
        orderby_exprs.push(PhysicalSortExpr {
            expr: col(column, &schema).unwrap(),
            options: SortOptions::default(),
        })
    }
    let mut partitionby_exprs = vec![];
    for column in &partition_by_columns {
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
        if !sort_keys.contains(order_by_expr) {
            sort_keys.push(order_by_expr.clone())
        }
    }

    let concat_input_record = concat_batches(&schema, &input1).unwrap();
    let source_sort_keys = vec![
        PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: Default::default(),
        },
        PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: Default::default(),
        },
        PhysicalSortExpr {
            expr: col("c", &schema)?,
            options: Default::default(),
        },
    ];
    let memory_exec =
        MemoryExec::try_new(&[vec![concat_input_record]], schema.clone(), None).unwrap();
    let memory_exec = memory_exec.with_sort_information(source_sort_keys.clone());
    let mut exec1 = Arc::new(memory_exec) as Arc<dyn ExecutionPlan>;
    // Table is ordered according to ORDER BY a, b, c In linear test we use PARTITION BY b, ORDER BY a
    // For WindowAggExec  to produce correct result it need table to be ordered by b,a. Hence add a sort.
    if is_linear {
        exec1 = Arc::new(SortExec::new(sort_keys.clone(), exec1)) as _;
    }
    let usual_window_exec = Arc::new(
        WindowAggExec::try_new(
            vec![create_window_expr(
                &window_fn,
                fn_name.clone(),
                &args,
                &partitionby_exprs,
                &orderby_exprs,
                Arc::new(window_frame.clone()),
                schema.as_ref(),
            )
            .unwrap()],
            exec1,
            schema.clone(),
            vec![],
        )
        .unwrap(),
    ) as _;
    let exec2 = Arc::new(
        MemoryExec::try_new(&[input1.clone()], schema.clone(), None)
            .unwrap()
            .with_sort_information(source_sort_keys.clone()),
    );
    let running_window_exec = Arc::new(
        BoundedWindowAggExec::try_new(
            vec![create_window_expr(
                &window_fn,
                fn_name,
                &args,
                &partitionby_exprs,
                &orderby_exprs,
                Arc::new(window_frame.clone()),
                schema.as_ref(),
            )
            .unwrap()],
            exec2,
            schema.clone(),
            vec![],
            search_mode,
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    let task_ctx = ctx.task_ctx();
    let collected_usual = collect(usual_window_exec, task_ctx.clone()).await.unwrap();

    let collected_running = collect(running_window_exec, task_ctx.clone())
        .await
        .unwrap();

    // BoundedWindowAggExec should produce more chunk than the usual WindowAggExec.
    // Otherwise it means that we cannot generate result in running mode.
    assert!(collected_running.len() > collected_usual.len());
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
        assert_eq!(
            (i, usual_line),
            (i, running_line),
            "Inconsistent result for window_frame: {window_frame:?}, window_fn: {window_fn:?}, args:{args:?}"
        );
    }
    Ok(())
}

/// Return randomly sized record batches with:
/// three sorted int32 columns 'a', 'b', 'c' ranged from 0..DISTINCT as columns
/// one random int32 column x
fn make_staggered_batches<const STREAM: bool>(
    len: usize,
    n_distinct: usize,
    random_seed: u64,
) -> Vec<RecordBatch> {
    // use a random number generator to pick a random sized output
    let mut rng = StdRng::seed_from_u64(random_seed);
    let mut input123: Vec<(i32, i32, i32)> = vec![(0, 0, 0); len];
    let mut input4: Vec<i32> = vec![0; len];
    input123.iter_mut().for_each(|v| {
        *v = (
            rng.gen_range(0..n_distinct) as i32,
            rng.gen_range(0..n_distinct) as i32,
            rng.gen_range(0..n_distinct) as i32,
        )
    });
    input123.sort();
    rng.fill(&mut input4[..]);
    let input1 = Int32Array::from_iter_values(input123.iter().map(|k| k.0));
    let input2 = Int32Array::from_iter_values(input123.iter().map(|k| k.1));
    let input3 = Int32Array::from_iter_values(input123.iter().map(|k| k.2));
    let input4 = Int32Array::from_iter_values(input4.into_iter());

    // split into several record batches
    let mut remainder = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(input1) as ArrayRef),
        ("b", Arc::new(input2) as ArrayRef),
        ("c", Arc::new(input3) as ArrayRef),
        ("x", Arc::new(input4) as ArrayRef),
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
