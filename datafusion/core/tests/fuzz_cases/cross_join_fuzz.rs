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

//! Fuzz Test for CrossJoinExec

use std::sync::Arc;
use arrow::compute::concat_batches;

use arrow::datatypes::Int32Type;
use arrow::util::pretty::print_batches;
use arrow_array::{ArrayRef, Int32Array, PrimitiveArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion_execution::config::SessionConfig;
use datafusion_physical_plan::{collect, joins::CrossJoinExec, memory::MemoryExec};
use datafusion_common::{JoinType, Result};

use rand::{rngs::ThreadRng, Rng};
use datafusion_common::utils::get_arrayref_at_indices;
use datafusion_physical_plan::joins::utils::build_join_schema;

#[tokio::test]
async fn test_cross_fuzz() {
    for _ in 0..50 {
        run_test().await;
    }
}

async fn run_test() {
    let mut rng = rand::thread_rng();
    let cfg = SessionConfig::new();
    let ctx = SessionContext::new_with_config(cfg);

    let left_batch_count = rng.gen_range(1..=10);
    let mut left_row_counts = vec![];
    for _ in 0..left_batch_count {
        left_row_counts.push(rng.gen_range(0..=5));
    }
    let right_batch_count = rng.gen_range(1..=10);
    let mut right_row_counts = vec![];
    for _ in 0..right_batch_count {
        right_row_counts.push(rng.gen_range(0..=5));
    }

    let (left_data, right_data) = generate_data(
        left_batch_count,
        left_row_counts.clone(),
        right_batch_count,
        right_row_counts.clone(),
        &mut rng,
    );

    let left_schema =
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let right_schema =
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));

    let left_memory_exec =
        Arc::new(MemoryExec::try_new(&[left_data.clone()], left_schema, None).unwrap())
            as _;
    let right_memory_exec =
        Arc::new(MemoryExec::try_new(&[right_data.clone()], right_schema, None).unwrap())
            as _;

    let cross_join = Arc::new(CrossJoinExec::new(left_memory_exec, right_memory_exec));

    let collected = collect(cross_join, ctx.task_ctx()).await.unwrap();

    assert_results2(
        collected,
        left_data,
        right_data,
    ).unwrap();

    // assert_results(
    //     collected,
    //     left_data,
    //     left_batch_count,
    //     left_row_counts,
    //     right_data,
    // );
}

fn generate_data(
    left_batch_count: i32,
    left_row_counts: Vec<i32>,
    right_batch_count: i32,
    right_row_counts: Vec<i32>,
    rng: &mut ThreadRng,
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let mut left_data = vec![];
    for batch_idx in 0..left_batch_count {
        let mut values_a = vec![];
        for _ in 0..left_row_counts[batch_idx as usize] {
            let value = rng.gen_range(0..1000000);
            values_a.push(value);
        }
        let column_a = Arc::new(Int32Array::from_iter_values(values_a)) as ArrayRef;

        left_data.push(RecordBatch::try_from_iter(vec![("a", column_a)]).unwrap());
    }

    let mut right_data = vec![];
    for batch_idx in 0..right_batch_count {
        let mut values_x = vec![];
        for _ in 0..right_row_counts[batch_idx as usize] {
            let value = rng.gen_range(0..1000000);
            values_x.push(value);
        }
        let column_x = Arc::new(Int32Array::from_iter_values(values_x)) as ArrayRef;

        right_data.push(RecordBatch::try_from_iter(vec![("x", column_x)]).unwrap());
    }
    (left_data, right_data)
}

fn assert_results2(
    collected: Vec<RecordBatch>,
    left_data: Vec<RecordBatch>,
    right_data: Vec<RecordBatch>,
) -> Result<()>{
    let left_row_counts = left_data.iter().map(|batch| batch.num_rows()).collect::<Vec<_>>();
    let left_data = concat_batches(&left_data[0].schema(), &left_data)?;
    let right_data = concat_batches(&right_data[0].schema(), &right_data)?;
    let n_left = left_data.num_rows();
    let n_right = right_data.num_rows();
    print_batches(&[left_data.clone()])?;
    print_batches(&[right_data.clone()])?;
    println!("left_row_counts: {:?}", left_row_counts);
    let (result_schema, _) = build_join_schema(&left_data.schema(), &right_data.schema(), &JoinType::Inner);
    let mut all_results = vec![];
    for idx in 0..right_data.num_rows(){
        let left_cols = left_data.columns();
        let indices = PrimitiveArray::from_iter_values(vec![idx as u32; n_left]);
        let right_cols = get_arrayref_at_indices(right_data.columns(), &indices)?;
        let mut join_cols = vec![];
        join_cols.extend(left_cols.to_vec());
        join_cols.extend(right_cols);
        let batch = RecordBatch::try_new(Arc::new(result_schema.clone()), join_cols)?;
        all_results.push(batch);
    }
    let join_result = concat_batches(&Arc::new(result_schema.clone()), &all_results)?;

    // sanity check whether result have expected size.
    assert_eq!(join_result.num_rows(), n_left*n_right);

    let result_chunk_sizes = collected.iter().map(|batch| batch.num_rows()).collect::<Vec<_>>();

    let collected= concat_batches(&Arc::new(result_schema), &collected)?;
    // Result is as expected
    // assert!(join_result.eq(&collected));
    assert_batches_same(&join_result, &collected)?;

    // Keep non-empty chunks
    // left_row_counts.retain(|&row_count| row_count > 0);

    // Result is generated at expected chunks
    for (idx, chunk_size) in result_chunk_sizes.iter().enumerate(){
        let expected_size = left_row_counts[idx%left_row_counts.len()] as usize;
        // assert_eq!(*chunk_size, expected_size);
    }


    Ok(())
}

fn assert_batches_same(lhs: &RecordBatch, rhs: &RecordBatch) -> Result<()> {
    if lhs.eq(rhs){
        // Equal as expected
        return Ok(())
    } else {
        println!("-------------LHS--------------");
        print_batches(&[lhs.clone()])?;
        println!("-------------RHS--------------");
        print_batches(&[rhs.clone()])?;
        unreachable!()
    }
}

fn assert_results(
    collected: Vec<RecordBatch>,
    left_data: Vec<RecordBatch>,
    left_batch_count: i32,
    left_row_counts: Vec<i32>,
    right_data: Vec<RecordBatch>,
) {

    let mut collected_a = vec![];
    let mut collected_x = vec![];
    for collected_batch in collected {
        collected_a.push(
            collected_batch.columns()[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()
                .unwrap()
                .clone(),
        );
        collected_x.push(
            collected_batch.columns()[1]
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()
                .unwrap()
                .clone(),
        );
    }

    for (batch_idx, batch_a) in collected_a.into_iter().enumerate() {
        let curr_left_data_batch = left_data[batch_idx % left_batch_count as usize]
            .columns()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap()
            .clone();
        for (row_idx, value_a) in batch_a.into_iter().enumerate() {
            assert_eq!(value_a.unwrap(), curr_left_data_batch.value(row_idx));
        }
    }

    let total_left_row_count = left_row_counts.iter().sum::<i32>();
    let mut checked_row = 0;
    let mut checked_batch = 0;
    for curr_right_batch in right_data.iter() {
        for value in curr_right_batch.columns()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap()
            .iter()
        {
            for x_batch in collected_x[checked_batch..].iter() {
                for x_value in x_batch.iter() {
                    assert_eq!(value, x_value);
                    checked_row += 1;
                }
                checked_batch += 1;
                if checked_row % total_left_row_count as usize == 0 {
                    break;
                }
            }
        }
    }
}
