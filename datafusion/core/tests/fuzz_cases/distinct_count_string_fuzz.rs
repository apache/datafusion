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

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, GenericStringArray, OffsetSizeTrait, UInt32Array};

use arrow_array::cast::AsArray;
use datafusion::datasource::MemTable;
use rand::rngs::StdRng;
use rand::{thread_rng, Rng, SeedableRng};
use std::collections::HashSet;
use tokio::task::JoinSet;

use datafusion::prelude::{SessionConfig, SessionContext};
use test_utils::stagger_batch;

#[tokio::test(flavor = "multi_thread")]
async fn distinct_count_string_test() {
    // max length of generated strings
    let mut join_set = JoinSet::new();
    let mut rng = thread_rng();
    for null_pct in [0.0, 0.01, 0.1, 0.5] {
        for _ in 0..100 {
            let max_len = rng.gen_range(1..50);
            let num_strings = rng.gen_range(1..100);
            let num_distinct_strings = if num_strings > 1 {
                rng.gen_range(1..num_strings)
            } else {
                num_strings
            };
            let generator = BatchGenerator {
                max_len,
                num_strings,
                num_distinct_strings,
                null_pct,
                rng: StdRng::from_seed(rng.gen()),
            };
            join_set.spawn(async move { run_distinct_count_test(generator).await });
        }
    }
    while let Some(join_handle) = join_set.join_next().await {
        // propagate errors
        join_handle.unwrap();
    }
}

/// Run COUNT DISTINCT using SQL and compare the result to computing the
/// distinct count using HashSet<String>
async fn run_distinct_count_test(mut generator: BatchGenerator) {
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

struct BatchGenerator {
    //// The maximum length of the strings
    max_len: usize,
    /// the total number of strings in the output
    num_strings: usize,
    /// The number of distinct strings in the columns
    num_distinct_strings: usize,
    /// The percentage of nulls in the columns
    null_pct: f64,
    /// Random number generator
    rng: StdRng,
}

impl BatchGenerator {
    /// Make batches of random strings with a random length columns "a" and "b":
    ///
    /// * "a" is a StringArray
    /// * "b" is a LargeStringArray
    fn make_input_batches(&mut self) -> Vec<RecordBatch> {
        // use a random number generator to pick a random sized output

        let batch = RecordBatch::try_from_iter(vec![
            ("a", self.gen_data::<i32>()),
            ("b", self.gen_data::<i64>()),
        ])
        .unwrap();

        stagger_batch(batch)
    }

    /// Creates a StringArray or LargeStringArray with random strings according
    /// to the parameters of the BatchGenerator
    fn gen_data<O: OffsetSizeTrait>(&mut self) -> ArrayRef {
        // table of strings from which to draw
        let distinct_strings: GenericStringArray<O> = (0..self.num_distinct_strings)
            .map(|_| Some(random_string(&mut self.rng, self.max_len)))
            .collect();

        // pick num_strings randomly from the distinct string table
        let indicies: UInt32Array = (0..self.num_strings)
            .map(|_| {
                if self.rng.gen::<f64>() < self.null_pct {
                    None
                } else if self.num_distinct_strings > 1 {
                    let range = 1..(self.num_distinct_strings as u32);
                    Some(self.rng.gen_range(range))
                } else {
                    Some(0)
                }
            })
            .collect();

        let options = None;
        arrow::compute::take(&distinct_strings, &indicies, options).unwrap()
    }
}

/// Return a string of random characters of length 1..=max_len
fn random_string(rng: &mut StdRng, max_len: usize) -> String {
    // pick characters at random (not just ascii)
    match max_len {
        0 => "".to_string(),
        1 => String::from(rng.gen::<char>()),
        _ => {
            let len = rng.gen_range(1..=max_len);
            rng.sample_iter::<char, _>(rand::distributions::Standard)
                .take(len)
                .map(char::from)
                .collect::<String>()
        }
    }
}
