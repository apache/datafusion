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

//! Fuzz Test for various corner cases merging streams of RecordBatches

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int32Array},
    compute::SortOptions,
    record_batch::RecordBatch,
};
use datafusion::physical_plan::{
    collect,
    expressions::{col, PhysicalSortExpr},
    memory::MemoryExec,
    sorts::sort_preserving_merge::SortPreservingMergeExec,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use test_utils::{batches_to_vec, partitions_to_sorted_vec, stagger_batch_with_seed};

#[tokio::test]
async fn test_merge_2() {
    run_merge_test(vec![
        // (0..100)
        // (0..100)
        make_staggered_batches(0, 100, 2),
        make_staggered_batches(0, 100, 3),
    ])
    .await
}

#[tokio::test]
async fn test_merge_2_no_overlap() {
    run_merge_test(vec![
        // (0..20)
        //        (20..40)
        make_staggered_batches(0, 20, 2),
        make_staggered_batches(20, 40, 3),
    ])
    .await
}

#[tokio::test]
async fn test_merge_3() {
    run_merge_test(vec![
        // (0        ..  100)
        // (0        ..  100)
        // (0  .. 51)
        make_staggered_batches(0, 100, 2),
        make_staggered_batches(0, 100, 3),
        make_staggered_batches(0, 51, 4),
    ])
    .await
}

#[tokio::test]
async fn test_merge_3_gaps() {
    run_merge_test(vec![
        // (0  .. 50)(50 .. 100)
        // (0 ..33)  (50 .. 100)
        // (0  .. 51)
        concat(
            make_staggered_batches(0, 50, 2),
            make_staggered_batches(50, 100, 7),
        ),
        concat(
            make_staggered_batches(0, 33, 21),
            make_staggered_batches(50, 123, 31),
        ),
        make_staggered_batches(0, 51, 11),
    ])
    .await
}

/// Merge a set of input streams using SortPreservingMergeExec and
/// `Vec::sort` and ensure the results are the same.
///
/// For each case, the `input` streams are turned into a set of
/// streams which are then merged together by [SortPreservingMerge]
///
/// Each `Vec<RecordBatch>` in `input` must be sorted and have a
/// single Int32 field named 'x'.
async fn run_merge_test(input: Vec<Vec<RecordBatch>>) {
    // Produce output with the specified output batch sizes
    let batch_sizes = [1, 2, 7, 49, 50, 51, 100];

    for batch_size in batch_sizes {
        let first_batch = input
            .iter()
            .flat_map(|p| p.iter())
            .next()
            .expect("at least one batch");
        let schema = first_batch.schema();

        let sort = vec![PhysicalSortExpr {
            expr: col("x", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];

        let exec = MemoryExec::try_new(&input, schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let session_config = SessionConfig::new().with_batch_size(batch_size);
        let ctx = SessionContext::new_with_config(session_config);
        let task_ctx = ctx.task_ctx();
        let collected = collect(merge, task_ctx).await.unwrap();

        // verify the output batch size: all batches except the last
        // should contain `batch_size` rows
        for (i, batch) in collected.iter().enumerate() {
            if i < collected.len() - 1 {
                assert_eq!(
                    batch.num_rows(),
                    batch_size,
                    "Expected batch {} to have {} rows, got {}",
                    i,
                    batch_size,
                    batch.num_rows()
                );
            }
        }

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);

        assert_eq!(expected, actual, "failure in @ batch_size {batch_size}");
    }
}

/// Return the values `low..high` in order, in randomly sized
/// record batches in a field named 'x' of type `Int32`
fn make_staggered_batches(low: i32, high: i32, seed: u64) -> Vec<RecordBatch> {
    let input: Int32Array = (low..high).map(Some).collect();

    // split into several record batches
    let batch =
        RecordBatch::try_from_iter(vec![("x", Arc::new(input) as ArrayRef)]).unwrap();

    stagger_batch_with_seed(batch, seed)
}

fn concat(mut v1: Vec<RecordBatch>, v2: Vec<RecordBatch>) -> Vec<RecordBatch> {
    v1.extend(v2);
    v1
}
