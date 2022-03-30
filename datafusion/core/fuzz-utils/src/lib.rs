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

//! Common utils for fuzz tests
use arrow::{array::Int32Array, record_batch::RecordBatch};
use rand::prelude::StdRng;
use rand::Rng;

pub use env_logger;

/// Extracts the i32 values from the set of batches and returns them as a single Vec
pub fn batches_to_vec(batches: &[RecordBatch]) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            assert_eq!(batch.num_columns(), 1);
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
        })
        .collect()
}

/// extract values from batches and sort them
pub fn partitions_to_sorted_vec(partitions: &[Vec<RecordBatch>]) -> Vec<Option<i32>> {
    let mut values: Vec<_> = partitions
        .iter()
        .flat_map(|batches| batches_to_vec(batches).into_iter())
        .collect();

    values.sort_unstable();
    values
}

/// Adds a random number of empty record batches into the stream
pub fn add_empty_batches(
    batches: Vec<RecordBatch>,
    rng: &mut StdRng,
) -> Vec<RecordBatch> {
    let schema = batches[0].schema();

    batches
        .into_iter()
        .flat_map(|batch| {
            // insert 0, or 1 empty batches before and after the current batch
            let empty_batch = RecordBatch::new_empty(schema.clone());
            std::iter::repeat(empty_batch.clone())
                .take(rng.gen_range(0..2))
                .chain(std::iter::once(batch))
                .chain(std::iter::repeat(empty_batch).take(rng.gen_range(0..2)))
        })
        .collect()
}
