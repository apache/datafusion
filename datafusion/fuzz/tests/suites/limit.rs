// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Fuzz Test for Sort + Fetch/Limit (TopK!)

use arrow::array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::assert_contains;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use test_utils::fuzz::{FuzzTest, RunConfig};
use test_utils::stagger_batch;

const SIZES: [usize; 4] = [10, 1_0000, 10_000, 100_000];

/// Runs TopK on random data of one type. Case seed `k` selects the input size
/// `SIZES[k % SIZES.len()]` and the data seed `k / SIZES.len()`.
pub(crate) struct LimitFuzzer {
    name: &'static str,
    make_data: fn(usize, &mut StdRng) -> SortedData,
}

impl LimitFuzzer {
    /// Runs each input size once. Each run uses a new random run seed.
    pub(crate) fn run_config(&self) -> RunConfig {
        RunConfig {
            run_seed: None,
            max_cases: Some(SIZES.len()),
            time_limit: None,
            cases_per_dataset: SIZES.len(),
        }
    }
}

impl FuzzTest for LimitFuzzer {
    type Dataset = ();

    fn name(&self) -> &str {
        self.name
    }

    fn prepare_dataset(&self, _: u64) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn run_case(&self, _: &(), case_seed: u64) -> Result<()> {
        let n_sizes = SIZES.len() as u64;
        let size = SIZES[(case_seed % n_sizes) as usize];
        let mut rng = StdRng::seed_from_u64(case_seed / n_sizes);
        let data = (self.make_data)(size, &mut rng);
        // test various limits including some random ones
        for limit in [1, 3, 7, 17, 10000, rng.random_range(1..size * 2)] {
            //  limit can be larger than the number of rows in the input
            run_limit_test(limit, &data).await;
        }
        Ok(())
    }
}

pub(crate) static FUZZERS: [LimitFuzzer; 4] = [
    LimitFuzzer {
        name: "test_sort_topk_i32",
        make_data: SortedData::new_i32,
    },
    LimitFuzzer {
        name: "test_sort_topk_f64",
        make_data: SortedData::new_f64,
    },
    LimitFuzzer {
        name: "test_sort_topk_str",
        make_data: SortedData::new_str,
    },
    LimitFuzzer {
        name: "test_sort_topk_i64str",
        make_data: SortedData::new_i64str,
    },
];

/// The data column(s) to use for the TopK test
///
/// Each variants stores the input batches and the expected sorted values
/// compute the expected output for a given fetch (limit) value.
#[derive(Debug)]
pub(crate) enum SortedData {
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
    fn new_i32(size: usize, rng: &mut StdRng) -> Self {
        // have some repeats (approximately 1/3 of the values are the same)
        let max = size as i32 / 3;
        let data: Vec<Option<i32>> = (0..size)
            .map(|_| {
                // no nulls for now
                Some(rng.random_range(0..max))
            })
            .collect();

        let batches = stagger_batch(int32_batch(data.iter().copied()));

        let mut sorted = data;
        sorted.sort_unstable();

        Self::I32 { batches, sorted }
    }

    /// Create an f64 column of random values, with the specified number of
    /// rows, sorted the default
    fn new_f64(size: usize, rng: &mut StdRng) -> Self {
        let mut data: Vec<Option<f64>> = (0..size / 3)
            .map(|_| {
                // no nulls for now
                Some(rng.random_range(0.0..1.0f64))
            })
            .collect();

        // have some repeats (approximately 1/3 of the values are the same)
        while data.len() < size {
            data.push(data[rng.random_range(0..data.len())]);
        }

        let batches = stagger_batch(f64_batch(data.iter().copied()));

        let mut sorted = data;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        Self::F64 { batches, sorted }
    }

    /// Create an string column of random values, with the specified number of
    /// rows, sorted the default
    fn new_str(size: usize, rng: &mut StdRng) -> Self {
        let mut data: Vec<Option<String>> = (0..size / 3)
            .map(|_| {
                // no nulls for now
                Some(get_random_string(rng, 16))
            })
            .collect();

        // have some repeats (approximately 1/3 of the values are the same)
        while data.len() < size {
            data.push(data[rng.random_range(0..data.len())].clone());
        }

        let batches = stagger_batch(string_batch(data.iter()));

        let mut sorted = data;
        sorted.sort_unstable();

        Self::Str { batches, sorted }
    }

    /// Create two  columns of random values (int64, string), with the specified number of
    /// rows, sorted the default
    fn new_i64str(size: usize, rng: &mut StdRng) -> Self {
        // 100 distinct values
        let strings: Vec<Option<String>> = (0..100)
            .map(|_| {
                // no nulls for now
                Some(get_random_string(rng, 16))
            })
            .collect();

        // form inputs, with only 10 distinct integer values , to force collision checks
        let data = (0..size)
            .map(|_| {
                (
                    Some(rng.random_range(0..10)),
                    strings[rng.random_range(0..strings.len())].clone(),
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
            Self::I32 { sorted, .. } => int32_batch(sorted.iter().take(limit).copied()),
            Self::F64 { sorted, .. } => f64_batch(sorted.iter().take(limit).copied()),
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

/// Run the TopK test, sorting the input batches with the specified fetch
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
fn get_random_string(rng: &mut StdRng, len: usize) -> String {
    rng.sample_iter(rand::distr::Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
