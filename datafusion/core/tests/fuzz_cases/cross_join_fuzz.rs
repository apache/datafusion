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

use arrow::{compute::concat_batches, util::pretty::print_batches};
use arrow_array::{ArrayRef, Datum, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion_execution::config::SessionConfig;
use datafusion_physical_plan::{collect, joins::CrossJoinExec, memory::MemoryExec};

use rand::{rngs::ThreadRng, Rng};

#[tokio::test]
async fn test_cross_fuzz() {
    let mut rng = rand::thread_rng();
    let cfg = SessionConfig::new();
    let ctx = SessionContext::new_with_config(cfg);

    let left_batch_count = rng.gen_range(0..=20);
    let mut left_row_counts = vec![];
    for _ in 0..left_batch_count {
        left_row_counts.push(rng.gen_range(0..=50));
    }

    let right_batch_count = rng.gen_range(0..=20);
    let mut right_row_counts = vec![];
    for _ in 0..right_batch_count {
        right_row_counts.push(rng.gen_range(0..=50));
    }

    let (left_data, right_data) = get_data(
        left_batch_count,
        left_row_counts,
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

    let mut collected_a = vec![];
    let mut collected_x = vec![];
    for collected_batch in collected {
        collected_a.push(
            collected_batch.columns()[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone(),
        );
        collected_x.push(
            collected_batch.columns()[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone(),
        );
    }

    for (batch_idx, batch_a) in collected_a.into_iter().enumerate() {
        let curr_left_data_batch = left_data[batch_idx % left_batch_count as usize]
            .columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        for (row_idx, value_a) in batch_a.into_iter().enumerate() {
            assert_eq!(value_a.unwrap(), curr_left_data_batch.value(row_idx));
        }
    }

    for (batch_idx, batch_x) in collected_x.into_iter().enumerate() {
        let curr_right_data_batch = right_data[batch_idx % right_batch_count as usize]
            .columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        for (row_idx, value_x) in batch_x.into_iter().enumerate() {
            assert_eq!(value_x.unwrap(), curr_right_data_batch.value(row_idx));
        }
    }
}

fn get_data(
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
