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

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::compute::{concat_batches, SortOptions};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{
    create_window_expr, schema_add_window_field, BoundedWindowAggExec, WindowAggExec,
};
use datafusion::physical_plan::InputOrderMode::{Linear, PartiallySorted, Sorted};
use datafusion::physical_plan::{collect, InputOrderMode};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::HashMap;
use datafusion_common::{Result, ScalarValue};
use datafusion_common_runtime::SpawnedTask;
use datafusion_expr::type_coercion::functions::fields_with_aggregate_udf;
use datafusion_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion_functions_window::nth_value::{
    first_value_udwf, last_value_udwf, nth_value_udwf,
};
use datafusion_functions_window::rank::{dense_rank_udwf, rank_udwf};
use datafusion_physical_expr::expressions::{cast, col, lit};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_utils::add_empty_batches;

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn window_bounded_window_random_comparison() -> Result<()> {
    // make_staggered_batches gives result sorted according to a, b, c
    // In the test cases first entry represents partition by columns
    // Second entry represents order by columns.
    // Third entry represents search mode.
    // In sorted mode physical plans are in the form for WindowAggExec
    //```
    // WindowAggExec
    //   DataSourceExec]
    // ```
    // and in the form for BoundedWindowAggExec
    // ```
    // BoundedWindowAggExec
    //   DataSourceExec
    // ```
    // In Linear and PartiallySorted mode physical plans are in the form for WindowAggExec
    //```
    // WindowAggExec
    //   SortExec(required by window function)
    //     DataSourceExec]
    // ```
    // and in the form for BoundedWindowAggExec
    // ```
    // BoundedWindowAggExec
    //   DataSourceExec
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
            let job = SpawnedTask::spawn(run_window_test(
                make_staggered_batches::<true>(1000, n_distinct, i as u64),
                i as u64,
                pb_cols,
                ob_cols,
                search_mode,
            ));
            handles.push(job);
        }
        for job in handles {
            job.join().await.unwrap()?;
        }
    }
    Ok(())
}

// This tests whether we can generate bounded window results for each input
// batch immediately for causal window frames.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn bounded_window_causal_non_causal() -> Result<()> {
    let session_config = SessionConfig::new();
    let ctx = SessionContext::new_with_config(session_config);
    let mut batches = make_staggered_batches::<true>(1000, 10, 23_u64);
    // Remove empty batches:
    batches.retain(|batch| batch.num_rows() > 0);
    let schema = batches[0].schema();
    let memory_exec =
        MemorySourceConfig::try_new_exec(&[batches.clone()], schema.clone(), None)?;

    // Different window functions to test causality
    let window_functions = vec![
        // Simulate cases of the following form:
        // COUNT(x) OVER (
        //     ROWS BETWEEN UNBOUNDED PRECEDING AND <end_bound> PRECEDING/FOLLOWING
        // )
        (
            // Window function
            WindowFunctionDefinition::AggregateUDF(count_udaf()),
            // its name
            "COUNT",
            // window function argument
            vec![col("x", &schema)?],
            // Expected causality, for None cases causality will be determined from window frame boundaries
            None,
        ),
        // Simulate cases of the following form:
        // ROW_NUMBER() OVER (
        //     ROWS BETWEEN UNBOUNDED PRECEDING AND <end_bound> PRECEDING/FOLLOWING
        // )
        (
            // user-defined window function
            WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            // its name
            "row_number",
            // no argument
            vec![],
            // Expected causality, for None cases causality will be determined from window frame boundaries
            Some(true),
        ),
        // Simulate cases of the following form:
        // LAG(x) OVER (
        //     ROWS BETWEEN UNBOUNDED PRECEDING AND <end_bound> PRECEDING/FOLLOWING
        // )
        (
            // Window function
            WindowFunctionDefinition::WindowUDF(lag_udwf()),
            // its name
            "LAG",
            // no argument
            vec![col("x", &schema)?],
            // Expected causality, for None cases causality will be determined from window frame boundaries
            Some(true),
        ),
        // Simulate cases of the following form:
        // LEAD(x) OVER (
        //     ROWS BETWEEN UNBOUNDED PRECEDING AND <end_bound> PRECEDING/FOLLOWING
        // )
        (
            // Window function
            WindowFunctionDefinition::WindowUDF(lead_udwf()),
            // its name
            "LEAD",
            // no argument
            vec![col("x", &schema)?],
            // Expected causality, for None cases causality will be determined from window frame boundaries
            Some(false),
        ),
        // Simulate cases of the following form:
        // RANK() OVER (
        //     ROWS BETWEEN UNBOUNDED PRECEDING AND <end_bound> PRECEDING/FOLLOWING
        // )
        (
            // Window function
            WindowFunctionDefinition::WindowUDF(rank_udwf()),
            // its name
            "rank",
            // no argument
            vec![],
            // Expected causality, for None cases causality will be determined from window frame boundaries
            Some(true),
        ),
        // Simulate cases of the following form:
        // DENSE_RANK() OVER (
        //     ROWS BETWEEN UNBOUNDED PRECEDING AND <end_bound> PRECEDING/FOLLOWING
        // )
        (
            // Window function
            WindowFunctionDefinition::WindowUDF(dense_rank_udwf()),
            // its name
            "dense_rank",
            // no argument
            vec![],
            // Expected causality, for None cases causality will be determined from window frame boundaries
            Some(true),
        ),
    ];

    let partitionby_exprs = vec![];
    // Window frame starts with "UNBOUNDED PRECEDING":
    let start_bound = WindowFrameBound::Preceding(ScalarValue::UInt64(None));

    for (window_fn, fn_name, args, expected_causal) in window_functions {
        for is_preceding in [false, true] {
            for end_bound in [0, 1, 2, 3] {
                let end_bound = if is_preceding {
                    WindowFrameBound::Preceding(ScalarValue::UInt64(Some(end_bound)))
                } else {
                    WindowFrameBound::Following(ScalarValue::UInt64(Some(end_bound)))
                };
                let window_frame = WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    start_bound.clone(),
                    end_bound,
                );
                let causal = if let Some(expected_causal) = expected_causal {
                    expected_causal
                } else {
                    // If there is no expected causality
                    // calculate it from window frame
                    window_frame.is_causal()
                };

                let extended_schema =
                    schema_add_window_field(&args, &schema, &window_fn, fn_name)?;

                let window_expr = create_window_expr(
                    &window_fn,
                    fn_name.to_string(),
                    &args,
                    &partitionby_exprs,
                    &[],
                    Arc::new(window_frame),
                    &extended_schema,
                    false,
                    false,
                )?;
                let running_window_exec = Arc::new(BoundedWindowAggExec::try_new(
                    vec![window_expr],
                    memory_exec.clone(),
                    Linear,
                    false,
                )?);
                let task_ctx = ctx.task_ctx();
                let collected_results = collect(running_window_exec, task_ctx).await?;
                let input_batch_sizes = batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .collect::<Vec<_>>();
                let result_batch_sizes = collected_results
                    .iter()
                    .map(|batch| batch.num_rows())
                    .collect::<Vec<_>>();
                // There should be no empty batches at results
                assert!(result_batch_sizes.iter().all(|e| *e > 0));
                if causal {
                    // For causal window frames, we can generate results immediately
                    // for each input batch. Hence, batch sizes should match.
                    assert_eq!(input_batch_sizes, result_batch_sizes);
                } else {
                    // For non-causal window frames, we cannot generate results
                    // immediately for each input batch. Hence, batch sizes shouldn't
                    // match.
                    assert_ne!(input_batch_sizes, result_batch_sizes);
                }
            }
        }
    }

    Ok(())
}

fn get_random_function(
    schema: &SchemaRef,
    rng: &mut StdRng,
    is_linear: bool,
) -> (WindowFunctionDefinition, Vec<Arc<dyn PhysicalExpr>>, String) {
    let arg = if is_linear {
        // In linear test for the test version with WindowAggExec we use insert SortExecs to the plan to be able to generate
        // same result with BoundedWindowAggExec which doesn't use any SortExec. To make result
        // non-dependent on table order. We should use column a in the window function
        // (Given that we do not use ROWS for the window frame. ROWS also introduces dependency to the table order.).
        col("a", schema).unwrap()
    } else {
        col("x", schema).unwrap()
    };
    let mut window_fn_map = HashMap::new();
    // HashMap values consists of tuple first element is WindowFunction, second is additional argument
    // window function requires if any. For most of the window functions additional argument is empty
    window_fn_map.insert(
        "sum",
        (
            WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            vec![arg.clone()],
        ),
    );
    window_fn_map.insert(
        "count",
        (
            WindowFunctionDefinition::AggregateUDF(count_udaf()),
            vec![arg.clone()],
        ),
    );
    window_fn_map.insert(
        "min",
        (
            WindowFunctionDefinition::AggregateUDF(min_udaf()),
            vec![arg.clone()],
        ),
    );
    window_fn_map.insert(
        "max",
        (
            WindowFunctionDefinition::AggregateUDF(max_udaf()),
            vec![arg.clone()],
        ),
    );
    if !is_linear {
        // row_number, rank, lead, lag doesn't use its window frame to calculate result. Their results are calculated
        // according to table scan order. This adds the dependency to table order. Hence do not use these functions in
        // Partition by linear test.
        window_fn_map.insert(
            "row_number",
            (
                WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                vec![],
            ),
        );
        window_fn_map.insert(
            "rank",
            (WindowFunctionDefinition::WindowUDF(rank_udwf()), vec![]),
        );
        window_fn_map.insert(
            "dense_rank",
            (
                WindowFunctionDefinition::WindowUDF(dense_rank_udwf()),
                vec![],
            ),
        );
        window_fn_map.insert(
            "lead",
            (
                WindowFunctionDefinition::WindowUDF(lead_udwf()),
                vec![
                    arg.clone(),
                    lit(ScalarValue::Int64(Some(rng.random_range(1..10)))),
                    lit(ScalarValue::Int64(Some(rng.random_range(1..1000)))),
                ],
            ),
        );
        window_fn_map.insert(
            "lag",
            (
                WindowFunctionDefinition::WindowUDF(lag_udwf()),
                vec![
                    arg.clone(),
                    lit(ScalarValue::Int64(Some(rng.random_range(1..10)))),
                    lit(ScalarValue::Int64(Some(rng.random_range(1..1000)))),
                ],
            ),
        );
    }
    window_fn_map.insert(
        "first_value",
        (
            WindowFunctionDefinition::WindowUDF(first_value_udwf()),
            vec![arg.clone()],
        ),
    );
    window_fn_map.insert(
        "last_value",
        (
            WindowFunctionDefinition::WindowUDF(last_value_udwf()),
            vec![arg.clone()],
        ),
    );
    window_fn_map.insert(
        "nth_value",
        (
            WindowFunctionDefinition::WindowUDF(nth_value_udwf()),
            vec![
                arg.clone(),
                lit(ScalarValue::Int64(Some(rng.random_range(1..10)))),
            ],
        ),
    );

    let rand_fn_idx = rng.random_range(0..window_fn_map.len());
    let fn_name = window_fn_map.keys().collect::<Vec<_>>()[rand_fn_idx];
    let (window_fn, args) = window_fn_map.values().collect::<Vec<_>>()[rand_fn_idx];
    let mut args = args.clone();
    if let WindowFunctionDefinition::AggregateUDF(udf) = window_fn {
        if !args.is_empty() {
            // Do type coercion first argument
            let a = args[0].clone();
            let dt = a.return_field(schema.as_ref()).unwrap();
            let coerced = fields_with_aggregate_udf(&[dt], udf).unwrap();
            args[0] = cast(a, schema, coerced[0].data_type().clone()).unwrap();
        }
    }

    (window_fn.clone(), args, fn_name.to_string())
}

fn get_random_window_frame(rng: &mut StdRng, is_linear: bool) -> WindowFrame {
    struct Utils {
        val: i32,
        is_preceding: bool,
    }
    let first_bound = Utils {
        val: rng.random_range(0..10),
        is_preceding: rng.random_range(0..2) == 0,
    };
    let second_bound = Utils {
        val: rng.random_range(0..10),
        is_preceding: rng.random_range(0..2) == 0,
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
    let rand_num = rng.random_range(0..3);
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

    let mut window_frame = match units {
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
            let mut window_frame = WindowFrame::new_bounds(units, start_bound, end_bound);
            // with 10% use unbounded preceding in tests
            if rng.random_range(0..10) == 0 {
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
            let mut window_frame = WindowFrame::new_bounds(units, start_bound, end_bound);
            // with 10% use unbounded preceding in tests
            if rng.random_range(0..10) == 0 {
                window_frame.start_bound =
                    WindowFrameBound::Preceding(ScalarValue::UInt64(None));
            }
            // We never use UNBOUNDED FOLLOWING in test. Because that case is not prunable and
            // should work only with WindowAggExec
            window_frame
        }
    };
    convert_bound_to_current_row_if_applicable(rng, &mut window_frame.start_bound);
    convert_bound_to_current_row_if_applicable(rng, &mut window_frame.end_bound);
    window_frame
}

/// This utility converts `PRECEDING(0)` or `FOLLOWING(0)` specifiers in window
/// frame bounds to `CURRENT ROW` with 50% probability. This enables us to test
/// behaviour of the system in the `CURRENT ROW` mode.
fn convert_bound_to_current_row_if_applicable(
    rng: &mut StdRng,
    bound: &mut WindowFrameBound,
) {
    match bound {
        WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value) => {
            if let Ok(zero) = ScalarValue::new_zero(&value.data_type()) {
                if value == &zero && rng.random_range(0..2) == 0 {
                    *bound = WindowFrameBound::CurrentRow;
                }
            }
        }
        _ => {}
    }
}

/// Perform batch and running window same input
/// and verify outputs of `WindowAggExec` and `BoundedWindowAggExec` are equal
async fn run_window_test(
    input1: Vec<RecordBatch>,
    random_seed: u64,
    partition_by_columns: Vec<&str>,
    orderby_columns: Vec<&str>,
    search_mode: InputOrderMode,
) -> Result<()> {
    let is_linear = !matches!(search_mode, Sorted);
    let mut rng = StdRng::seed_from_u64(random_seed);
    let schema = input1[0].schema();
    let session_config = SessionConfig::new().with_batch_size(50);
    let ctx = SessionContext::new_with_config(session_config);
    let (window_fn, args, fn_name) = get_random_function(&schema, &mut rng, is_linear);
    let window_frame = get_random_window_frame(&mut rng, is_linear);
    let mut orderby_exprs = vec![];
    for column in &orderby_columns {
        orderby_exprs.push(PhysicalSortExpr {
            expr: col(column, &schema)?,
            options: SortOptions::default(),
        })
    }
    if orderby_exprs.len() > 1 && !window_frame.can_accept_multi_orderby() {
        orderby_exprs.truncate(1);
    }
    let mut partitionby_exprs = vec![];
    for column in &partition_by_columns {
        partitionby_exprs.push(col(column, &schema)?);
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

    let concat_input_record = concat_batches(&schema, &input1)?;
    let source_sort_keys: LexOrdering = [
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
    ]
    .into();
    let mut exec1 = DataSourceExec::from_data_source(
        MemorySourceConfig::try_new(&[vec![concat_input_record]], schema.clone(), None)?
            .try_with_sort_information(vec![source_sort_keys.clone()])?,
    ) as _;
    // Table is ordered according to ORDER BY a, b, c In linear test we use PARTITION BY b, ORDER BY a
    // For WindowAggExec  to produce correct result it need table to be ordered by b,a. Hence add a sort.
    if is_linear {
        if let Some(ordering) = LexOrdering::new(sort_keys) {
            exec1 = Arc::new(SortExec::new(ordering, exec1)) as _;
        }
    }

    let extended_schema = schema_add_window_field(&args, &schema, &window_fn, &fn_name)?;

    let usual_window_exec = Arc::new(WindowAggExec::try_new(
        vec![create_window_expr(
            &window_fn,
            fn_name.clone(),
            &args,
            &partitionby_exprs,
            &orderby_exprs.clone(),
            Arc::new(window_frame.clone()),
            &extended_schema,
            false,
            false,
        )?],
        exec1,
        false,
    )?) as _;
    let exec2 = DataSourceExec::from_data_source(
        MemorySourceConfig::try_new(&[input1], schema, None)?
            .try_with_sort_information(vec![source_sort_keys])?,
    );
    let running_window_exec = Arc::new(BoundedWindowAggExec::try_new(
        vec![create_window_expr(
            &window_fn,
            fn_name,
            &args,
            &partitionby_exprs,
            &orderby_exprs,
            Arc::new(window_frame.clone()),
            &extended_schema,
            false,
            false,
        )?],
        exec2,
        search_mode.clone(),
        false,
    )?) as _;
    let task_ctx = ctx.task_ctx();
    let collected_usual = collect(usual_window_exec, task_ctx.clone()).await?;
    let collected_running = collect(running_window_exec, task_ctx)
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    assert!(collected_running.iter().all(|rb| rb.num_rows() > 0));

    // BoundedWindowAggExec should produce more chunk than the usual WindowAggExec.
    // Otherwise it means that we cannot generate result in running mode.
    let err_msg = format!("Inconsistent result for window_frame: {window_frame:?}, window_fn: {window_fn:?}, args:{args:?}, random_seed: {random_seed:?}, search_mode: {search_mode:?}, partition_by_columns:{partition_by_columns:?}, orderby_columns: {orderby_columns:?}");
    // Below check makes sure that, streaming execution generates more chunks than the bulk execution.
    // Since algorithms and operators works on sliding windows in the streaming execution.
    // However, in the current test setup for some random generated window frame clauses: It is not guaranteed
    // for streaming execution to generate more chunk than its non-streaming counter part in the Linear mode.
    // As an example window frame `OVER(PARTITION BY d ORDER BY a RANGE BETWEEN CURRENT ROW AND 9 FOLLOWING)`
    // needs to receive a=10 to generate result for the rows where a=0. If the input data generated is between the range [0, 9].
    // even in streaming mode, generated result will be single bulk as in the non-streaming version.
    if search_mode != Linear {
        assert!(
            collected_running.len() > collected_usual.len(),
            "{}",
            err_msg
        );
    }

    // compare
    let usual_formatted = pretty_format_batches(&collected_usual)?.to_string();
    let running_formatted = pretty_format_batches(&collected_running)?.to_string();

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
        if !usual_line.eq(running_line) {
            println!("Inconsistent result for window_frame at line:{i:?}: {window_frame:?}, window_fn: {window_fn:?}, args:{args:?}, pb_cols:{partition_by_columns:?}, ob_cols:{orderby_columns:?}, search_mode:{search_mode:?}");
            println!("--------usual_formatted_sorted----------------running_formatted_sorted--------");
            for (line1, line2) in
                usual_formatted_sorted.iter().zip(running_formatted_sorted)
            {
                println!("{line1:?}   ---   {line2:?}");
            }
            unreachable!();
        }
    }
    Ok(())
}

fn generate_random_string(rng: &mut StdRng, length: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/// Return randomly sized record batches with:
/// three sorted int32 columns 'a', 'b', 'c' ranged from 0..DISTINCT as columns
/// one random int32 column x
pub(crate) fn make_staggered_batches<const STREAM: bool>(
    len: usize,
    n_distinct: usize,
    random_seed: u64,
) -> Vec<RecordBatch> {
    // use a random number generator to pick a random sized output
    let mut rng = StdRng::seed_from_u64(random_seed);
    let mut input123: Vec<(i32, i32, i32)> = vec![(0, 0, 0); len];
    let mut input4: Vec<i32> = vec![0; len];
    let mut input5: Vec<String> = vec!["".to_string(); len];
    input123.iter_mut().for_each(|v| {
        *v = (
            rng.random_range(0..n_distinct) as i32,
            rng.random_range(0..n_distinct) as i32,
            rng.random_range(0..n_distinct) as i32,
        )
    });
    input123.sort();
    rng.fill(&mut input4[..]);
    input5.iter_mut().for_each(|v| {
        *v = generate_random_string(&mut rng, 1);
    });
    input5.sort();
    let input1 = Int32Array::from_iter_values(input123.iter().map(|k| k.0));
    let input2 = Int32Array::from_iter_values(input123.iter().map(|k| k.1));
    let input3 = Int32Array::from_iter_values(input123.iter().map(|k| k.2));
    let input4 = Int32Array::from_iter_values(input4);
    let input5 = StringArray::from_iter_values(input5);

    // split into several record batches
    let mut remainder = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(input1) as ArrayRef),
        ("b", Arc::new(input2) as ArrayRef),
        ("c", Arc::new(input3) as ArrayRef),
        ("x", Arc::new(input4) as ArrayRef),
        ("string_field", Arc::new(input5) as ArrayRef),
    ])
    .unwrap();

    let mut batches = vec![];
    if STREAM {
        while remainder.num_rows() > 0 {
            let batch_size = rng.random_range(0..50);
            if remainder.num_rows() < batch_size {
                batches.push(remainder);
                break;
            }
            batches.push(remainder.slice(0, batch_size));
            remainder = remainder.slice(batch_size, remainder.num_rows() - batch_size);
        }
    } else {
        while remainder.num_rows() > 0 {
            let batch_size = rng.random_range(0..remainder.num_rows() + 1);
            batches.push(remainder.slice(0, batch_size));
            remainder = remainder.slice(batch_size, remainder.num_rows() - batch_size);
        }
    }
    add_empty_batches(batches, &mut rng)
}
