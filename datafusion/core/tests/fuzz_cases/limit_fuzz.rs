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

//! Fuzz Test for Sort + Fetch/Limit (TopK!)

use arrow::compute::concat_batches;
use arrow::util::pretty::pretty_format_batches;
use arrow::{array::Int32Array, record_batch::RecordBatch};
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::assert_contains;
use rand::{thread_rng, Rng};
use std::sync::Arc;
use test_utils::stagger_batch;

#[tokio::test]
async fn test_sort_topk_i32() {
    run_limit_fuzz_test(SortedData::new_i32).await
}

#[tokio::test]
async fn test_sort_topk_f64() {
    run_limit_fuzz_test(SortedData::new_f64).await
}

#[tokio::test]
async fn test_sort_topk_str() {
    run_limit_fuzz_test(SortedData::new_str).await
}

#[tokio::test]
async fn test_sort_topk_i64str() {
    run_limit_fuzz_test(SortedData::new_i64str).await
}

/// Run TopK fuzz tests the specified input data with different
/// different test functions so they can run in parallel)
async fn run_limit_fuzz_test<F>(make_data: F)
where
    F: Fn(usize) -> SortedData,
{
    let mut rng = thread_rng();
    for size in [10, 1_0000, 10_000, 100_000] {
        let data = make_data(size);
        // test various limits including some random ones
        for limit in [1, 3, 7, 17, 10000, rng.gen_range(1..size * 2)] {
            //  limit can be larger than the number of rows in the input
            run_limit_test(limit, &data).await;
        }
    }
}

/// The data column(s) to use for the TopK test
///
/// Each variants stores the input batches and the expected sorted values
/// compute the expected output for a given fetch (limit) value.
#[derive(Debug)]
enum SortedData {
    // single Int32 column
    I32 {
        batches: Vec<RecordBatch>,
        sorted: Vec<Option<i32>>,
    },
    /// Single Float64 column
    F64 {
        batches: Vec<RecordBatch>,
        sorted: Vec<Option<f64>>,
    },
    /// Single sorted String column
    Str {
        batches: Vec<RecordBatch>,
        sorted: Vec<Option<String>>,
    },
    /// (i64, string) columns
    I64Str {
        batches: Vec<RecordBatch>,
        sorted: Vec<(Option<i64>, Option<String>)>,
    },
}

impl SortedData {
    /// Create an i32 column of random values, with the specified number of
    /// rows, sorted the default
    fn new_i32(size: usize) -> Self {
        let mut rng = thread_rng();
        // have some repeats (approximately 1/3 of the values are the same)
        let max = size as i32 / 3;
        let data: Vec<Option<i32>> = (0..size)
            .map(|_| {
                // no nulls for now
                Some(rng.gen_range(0..max))
            })
            .collect();

        let batches = stagger_batch(int32_batch(data.iter().cloned()));

        let mut sorted = data;
        sorted.sort_unstable();

        Self::I32 { batches, sorted }
    }

    /// Create an f64 column of random values, with the specified number of
    /// rows, sorted the default
    fn new_f64(size: usize) -> Self {
        let mut rng = thread_rng();
        let mut data: Vec<Option<f64>> = (0..size / 3)
            .map(|_| {
                // no nulls for now
                Some(rng.gen_range(0.0..1.0f64))
            })
            .collect();

        // have some repeats (approximately 1/3 of the values are the same)
        while data.len() < size {
            data.push(data[rng.gen_range(0..data.len())]);
        }

        let batches = stagger_batch(f64_batch(data.iter().cloned()));

        let mut sorted = data;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        Self::F64 { batches, sorted }
    }

    /// Create an string column of random values, with the specified number of
    /// rows, sorted the default
    fn new_str(size: usize) -> Self {
        let mut rng = thread_rng();
        let mut data: Vec<Option<String>> = (0..size / 3)
            .map(|_| {
                // no nulls for now
                Some(get_random_string(16))
            })
            .collect();

        // have some repeats (approximately 1/3 of the values are the same)
        while data.len() < size {
            data.push(data[rng.gen_range(0..data.len())].clone());
        }

        let batches = stagger_batch(string_batch(data.iter()));

        let mut sorted = data;
        sorted.sort_unstable();

        Self::Str { batches, sorted }
    }

    /// Create two  columns of random values (int64, string), with the specified number of
    /// rows, sorted the default
    fn new_i64str(size: usize) -> Self {
        let mut rng = thread_rng();

        // 100 distinct values
        let strings: Vec<Option<String>> = (0..100)
            .map(|_| {
                // no nulls for now
                Some(get_random_string(16))
            })
            .collect();

        // form inputs, with only 10 distinct integer values , to force collision checks
        let data = (0..size)
            .map(|_| {
                (
                    Some(rng.gen_range(0..10)),
                    strings[rng.gen_range(0..strings.len())].clone(),
                )
            })
            .collect::<Vec<_>>();

        let batches = stagger_batch(i64string_batch(data.iter()));

        let mut sorted = data;
        sorted.sort_unstable();

        Self::I64Str { batches, sorted }
    }

    /// Return top top `limit` values as a RecordBatch
    fn topk_values(&self, limit: usize) -> RecordBatch {
        match self {
            Self::I32 { sorted, .. } => int32_batch(sorted.iter().take(limit).cloned()),
            Self::F64 { sorted, .. } => f64_batch(sorted.iter().take(limit).cloned()),
            Self::Str { sorted, .. } => string_batch(sorted.iter().take(limit)),
            Self::I64Str { sorted, .. } => i64string_batch(sorted.iter().take(limit)),
        }
    }

    /// Return the input data to sort
    fn batches(&self) -> Vec<RecordBatch> {
        match self {
            Self::I32 { batches, .. } => batches.clone(),
            Self::F64 { batches, .. } => batches.clone(),
            Self::Str { batches, .. } => batches.clone(),
            Self::I64Str { batches, .. } => batches.clone(),
        }
    }

    /// Return the schema of the input data
    fn schema(&self) -> SchemaRef {
        match self {
            Self::I32 { batches, .. } => batches[0].schema(),
            Self::F64 { batches, .. } => batches[0].schema(),
            Self::Str { batches, .. } => batches[0].schema(),
            Self::I64Str { batches, .. } => batches[0].schema(),
        }
    }

    /// Return the sort expression to use for this data, depending on the type
    fn sort_expr(&self) -> Vec<datafusion_expr::SortExpr> {
        match self {
            Self::I32 { .. } | Self::F64 { .. } | Self::Str { .. } => {
                vec![datafusion_expr::col("x").sort(true, true)]
            }
            Self::I64Str { .. } => {
                vec![
                    datafusion_expr::col("x").sort(true, true),
                    datafusion_expr::col("y").sort(true, true),
                ]
            }
        }
    }
}

/// Create a record batch with a single column of type `Int32` named "x"
fn int32_batch(values: impl IntoIterator<Item = Option<i32>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Int32Array::from_iter(values.into_iter())) as _,
    )])
    .unwrap()
}

/// Create a record batch with a single column of type `Float64` named "x"
fn f64_batch(values: impl IntoIterator<Item = Option<f64>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Float64Array::from_iter(values.into_iter())) as _,
    )])
    .unwrap()
}

/// Create a record batch with a single column of type `StringArray` named "x"
fn string_batch<'a>(values: impl IntoIterator<Item = &'a Option<String>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(StringArray::from_iter(values.into_iter())) as _,
    )])
    .unwrap()
}

/// Create a record batch with i64 column "x" and utf8 column "y"
fn i64string_batch<'a>(
    values: impl IntoIterator<Item = &'a (Option<i64>, Option<String>)> + Clone,
) -> RecordBatch {
    let ints = values.clone().into_iter().map(|(i, _)| *i);
    let strings = values.into_iter().map(|(_, s)| s);
    RecordBatch::try_from_iter(vec![
        ("x", Arc::new(Int64Array::from_iter(ints)) as _),
        ("y", Arc::new(StringArray::from_iter(strings)) as _),
    ])
    .unwrap()
}

/// Run the TopK test, sorting the input batches with the specified ftch
/// (limit) and compares the results to the expected values.
async fn run_limit_test(fetch: usize, data: &SortedData) {
    let input = data.batches();
    let schema = data.schema();

    let table = MemTable::try_new(schema, vec![input]).unwrap();

    let ctx = SessionContext::new();
    let df = ctx
        .read_table(Arc::new(table))
        .unwrap()
        .sort(data.sort_expr())
        .unwrap()
        .limit(0, Some(fetch))
        .unwrap();

    // Verify the plan contains a TopK node
    {
        let explain = df
            .clone()
            .explain(false, false)
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan_text = pretty_format_batches(&explain).unwrap().to_string();
        let expected = format!("TopK(fetch={fetch})");
        assert_contains!(plan_text, expected);
    }

    let results = df.collect().await.unwrap();
    let expected = data.topk_values(fetch);

    // Verify that all output batches conform to the specified batch size
    let max_batch_size = ctx.copied_config().batch_size();
    for batch in &results {
        assert!(batch.num_rows() <= max_batch_size);
    }

    let results = concat_batches(&results[0].schema(), &results).unwrap();

    let results = [results];
    let expected = [expected];

    assert_eq!(
        &expected,
        &results,
        "TopK mismatch fetch {fetch} \n\
                expected rows {}, actual rows {}.\
                \n\nExpected:\n{}\n\nActual:\n{}",
        expected[0].num_rows(),
        results[0].num_rows(),
        pretty_format_batches(&expected).unwrap(),
        pretty_format_batches(&results).unwrap(),
    );
}

/// Return random ASCII String with len
fn get_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(rand::distributions::Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
