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

use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::{concat_batches, SortOptions};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_physical_expr::expressions::{col, Sum};
use datafusion_physical_expr::{AggregateExpr, PhysicalSortExpr};
use test_utils::add_empty_batches;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn aggregate_test() {
    let test_cases = vec![
        vec!["a"],
        vec!["b", "a"],
        vec!["c", "a"],
        vec!["c", "b", "a"],
        vec!["d", "a"],
        vec!["d", "b", "a"],
        vec!["d", "c", "a"],
        vec!["d", "c", "b", "a"],
    ];
    let n = 300;
    let distincts = vec![10, 20];
    for distinct in distincts {
        let mut handles = Vec::new();
        for i in 0..n {
            let test_idx = i % test_cases.len();
            let group_by_columns = test_cases[test_idx].clone();
            let job = tokio::spawn(run_aggregate_test(
                make_staggered_batches::<true>(1000, distinct, i as u64),
                group_by_columns,
            ));
            handles.push(job);
        }
        for job in handles {
            job.await.unwrap();
        }
    }
}

/// Perform batch and streaming aggregation with same input
/// and verify outputs of `AggregateExec` with pipeline breaking stream `GroupedHashAggregateStream`
/// and non-pipeline breaking stream `BoundedAggregateStream` produces same result.
async fn run_aggregate_test(input1: Vec<RecordBatch>, group_by_columns: Vec<&str>) {
    let schema = input1[0].schema();
    let session_config = SessionConfig::new().with_batch_size(50);
    let ctx = SessionContext::new_with_config(session_config);
    let mut sort_keys = vec![];
    for ordering_col in ["a", "b", "c"] {
        sort_keys.push(PhysicalSortExpr {
            expr: col(ordering_col, &schema).unwrap(),
            options: SortOptions::default(),
        })
    }

    let concat_input_record = concat_batches(&schema, &input1).unwrap();
    let usual_source = Arc::new(
        MemoryExec::try_new(&[vec![concat_input_record]], schema.clone(), None).unwrap(),
    );

    let running_source = Arc::new(
        MemoryExec::try_new(&[input1.clone()], schema.clone(), None)
            .unwrap()
            .with_sort_information(vec![sort_keys]),
    );

    let aggregate_expr = vec![Arc::new(Sum::new(
        col("d", &schema).unwrap(),
        "sum1",
        DataType::Int64,
    )) as Arc<dyn AggregateExpr>];
    let expr = group_by_columns
        .iter()
        .map(|elem| (col(elem, &schema).unwrap(), elem.to_string()))
        .collect::<Vec<_>>();
    let group_by = PhysicalGroupBy::new_single(expr);

    let aggregate_exec_running = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggregate_expr.clone(),
            vec![None],
            vec![None],
            running_source,
            schema.clone(),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;

    let aggregate_exec_usual = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggregate_expr.clone(),
            vec![None],
            vec![None],
            usual_source,
            schema.clone(),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;

    let task_ctx = ctx.task_ctx();
    let collected_usual = collect(aggregate_exec_usual.clone(), task_ctx.clone())
        .await
        .unwrap();

    let collected_running = collect(aggregate_exec_running.clone(), task_ctx.clone())
        .await
        .unwrap();
    assert!(collected_running.len() > 2);
    // Running should produce more chunk than the usual AggregateExec.
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
            "Inconsistent result\n\n\
             Aggregate_expr: {aggregate_expr:?}\n\
             group_by: {group_by:?}\n\
             Left Plan:\n{}\n\
             Right Plan:\n{}\n\
             schema:\n{schema}\n\
             Left Ouptut:\n{}\n\
             Right Output:\n{}\n\
             input:\n{}\n\
             ",
            displayable(aggregate_exec_usual.as_ref()).indent(false),
            displayable(aggregate_exec_running.as_ref()).indent(false),
            usual_formatted,
            running_formatted,
            pretty_format_batches(&input1).unwrap(),
        );
    }
}

/// Return randomly sized record batches with:
/// three sorted int64 columns 'a', 'b', 'c' ranged from 0..'n_distinct' as columns
/// one random int64 column 'd' as other columns
pub(crate) fn make_staggered_batches<const STREAM: bool>(
    len: usize,
    n_distinct: usize,
    random_seed: u64,
) -> Vec<RecordBatch> {
    // use a random number generator to pick a random sized output
    let mut rng = StdRng::seed_from_u64(random_seed);
    let mut input123: Vec<(i64, i64, i64)> = vec![(0, 0, 0); len];
    let mut input4: Vec<i64> = vec![0; len];
    input123.iter_mut().for_each(|v| {
        *v = (
            rng.gen_range(0..n_distinct) as i64,
            rng.gen_range(0..n_distinct) as i64,
            rng.gen_range(0..n_distinct) as i64,
        )
    });
    input4.iter_mut().for_each(|v| {
        *v = rng.gen_range(0..n_distinct) as i64;
    });
    input123.sort();
    let input1 = Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.0));
    let input2 = Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.1));
    let input3 = Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.2));
    let input4 = Int64Array::from_iter_values(input4);

    // split into several record batches
    let mut remainder = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(input1) as ArrayRef),
        ("b", Arc::new(input2) as ArrayRef),
        ("c", Arc::new(input3) as ArrayRef),
        ("d", Arc::new(input4) as ArrayRef),
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
