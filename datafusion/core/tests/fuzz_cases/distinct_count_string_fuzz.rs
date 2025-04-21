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

//! Compare DistinctCount for string with naive HashSet and Short String Optimized HashSet

use std::sync::Arc;

use arrow::array::{cast::AsArray, Array, OffsetSizeTrait, RecordBatch};

use datafusion::datasource::MemTable;
use datafusion_common_runtime::JoinSet;
use std::collections::HashSet;

use datafusion::prelude::{SessionConfig, SessionContext};
use test_utils::StringBatchGenerator;

#[tokio::test(flavor = "multi_thread")]
async fn distinct_count_string_test() {
    let mut join_set = JoinSet::new();
    for generator in StringBatchGenerator::interesting_cases() {
        join_set.spawn(async move { run_distinct_count_test(generator).await });
    }
    while let Some(join_handle) = join_set.join_next().await {
        // propagate errors
        join_handle.unwrap();
    }
}

/// Run COUNT DISTINCT using SQL and compare the result to computing the
/// distinct count using HashSet<String>
async fn run_distinct_count_test(mut generator: StringBatchGenerator) {
    let input = generator.make_input_batches();

    let schema = input[0].schema();
    let session_config = SessionConfig::new().with_batch_size(50);
    let ctx = SessionContext::new_with_config(session_config);

    // split input into two partitions
    let partition_len = input.len() / 2;
    let partitions = vec![
        input[0..partition_len].to_vec(),
        input[partition_len..].to_vec(),
    ];

    let provider = MemTable::try_new(schema, partitions).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    // input has two columns, a and b.  The result is the number of distinct
    // values in each column.
    //
    // Note, we need  at least two count distinct aggregates to trigger the
    // count distinct aggregate. Otherwise, the optimizer will rewrite the
    // `COUNT(DISTINCT a)` to `COUNT(*) from (SELECT DISTINCT a FROM t)`
    let results = ctx
        .sql("SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // get all the strings from the first column of the result (distinct a)
    let expected_a = extract_distinct_strings::<i32>(&input, 0).len();
    let result_a = extract_i64(&results, 0);
    assert_eq!(expected_a, result_a);

    // get all the strings from the second column of the result (distinct b(
    let expected_b = extract_distinct_strings::<i64>(&input, 1).len();
    let result_b = extract_i64(&results, 1);
    assert_eq!(expected_b, result_b);
}

/// Return all (non null) distinct strings from  column col_idx
fn extract_distinct_strings<O: OffsetSizeTrait>(
    results: &[RecordBatch],
    col_idx: usize,
) -> Vec<String> {
    results
        .iter()
        .flat_map(|batch| {
            let array = batch.column(col_idx).as_string::<O>();
            // remove nulls via 'flatten'
            array.iter().flatten().map(|s| s.to_string())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

// extract the value from the Int64 column in col_idx in batch and return
// it as a usize
fn extract_i64(results: &[RecordBatch], col_idx: usize) -> usize {
    assert_eq!(results.len(), 1);
    let array = results[0]
        .column(col_idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(array.len(), 1);
    assert!(!array.is_null(0));
    array.value(0).try_into().unwrap()
}
