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

//! non trivial integration testing for parquet predicate pushdown
//!
//! Testing hints: If you run this test with --nocapture it will tell you where
//! the generated parquet file went. You can then test it and try out various queries
//! datafusion-cli like:
//!
//! ```sql
//! create external table data stored as parquet location 'data.parquet';
//! select * from data limit 10;
//! ```

use std::time::Instant;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{lit, Expr};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::prelude::{col, SessionContext};
use datafusion_optimizer::utils::{conjunction, disjunction, split_conjunction};
use itertools::Itertools;
use parquet_test_utils::{ParquetScanOptions, TestParquetFile};
use tempfile::TempDir;
use test_utils::AccessLogGenerator;

/// how many rows of generated data to write to our parquet file (arbitrary)
const NUM_ROWS: usize = 53819;

#[cfg(not(target_family = "windows"))]
#[tokio::test]
async fn single_file() {
    // Only create the parquet file once as it is fairly large
    let tempdir = TempDir::new().unwrap();

    let generator = AccessLogGenerator::new().with_row_limit(Some(NUM_ROWS));

    // TODO: set the max page rows with some various / arbitrary sizes 8311
    // (using https://github.com/apache/arrow-rs/issues/2941) to ensure we get multiple pages
    let page_size = None;
    let row_group_size = None;
    let file = tempdir.path().join("data.parquet");

    let start = Instant::now();
    println!("Writing test data to {:?}", file);
    let test_parquet_file =
        TestParquetFile::try_new(file, generator, page_size, row_group_size).unwrap();
    println!(
        "Completed generating test data in {:?}",
        Instant::now() - start
    );

    TestCase::new(&test_parquet_file)
        .with_name("selective")
        // request_method = 'GET'
        .with_filter(col("request_method").eq(lit("GET")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(8886)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("non_selective")
        // request_method != 'GET'
        .with_filter(col("request_method").not_eq(lit("GET")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(44933)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("basic_conjunction")
        // request_method = 'POST' AND
        //   response_status = 503
        .with_filter(
            conjunction([
                col("request_method").eq(lit("POST")),
                col("response_status").eq(lit(503_u16)),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(1729)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("everything")
        // filter filters everything (no row has this status)
        // response_status = 429
        .with_filter(col("response_status").eq(lit(429_u16)))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(0)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("nothing")
        // No rows are filtered out -- all are returned
        // response_status > 0
        .with_filter(col("response_status").gt(lit(0_u16)))
        .with_pushdown_expected(PushdownExpected::None)
        .with_expected_rows(NUM_ROWS)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("dict_selective")
        // container = 'backend_container_0'
        .with_filter(col("container").eq(lit("backend_container_0")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(37856)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("not eq")
        // container != 'backend_container_0'
        .with_filter(col("container").not_eq(lit("backend_container_0")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(15963)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("dict_conjunction")
        // container == 'backend_container_0' AND
        //   pod = 'aqcathnxqsphdhgjtgvxsfyiwbmhlmg'
        .with_filter(
            conjunction([
                col("container").eq(lit("backend_container_0")),
                col("pod").eq(lit("aqcathnxqsphdhgjtgvxsfyiwbmhlmg")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(3052)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("dict_very_selective")
        // request_bytes > 2B AND
        //   container == 'backend_container_0' AND
        //   pod = 'aqcathnxqsphdhgjtgvxsfyiwbmhlmg'
        .with_filter(
            conjunction([
                col("request_bytes").gt(lit(2000000000)),
                col("container").eq(lit("backend_container_0")),
                col("pod").eq(lit("aqcathnxqsphdhgjtgvxsfyiwbmhlmg")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(88)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("dict_very_selective2")
        // picks only 2 rows
        // client_addr = '204.47.29.82' AND
        //   container == 'backend_container_0' AND
        //   pod = 'aqcathnxqsphdhgjtgvxsfyiwbmhlmg'
        .with_filter(
            conjunction(vec![
                col("request_bytes").gt(lit(2000000000)),
                col("container").eq(lit("backend_container_0")),
                col("pod").eq(lit("aqcathnxqsphdhgjtgvxsfyiwbmhlmg")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(88)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("dict_disjunction")
        // container = 'backend_container_0' OR
        //   pod = 'aqcathnxqsphdhgjtgvxsfyiwbmhlmg'
        .with_filter(
            disjunction([
                col("container").eq(lit("backend_container_0")),
                col("pod").eq(lit("aqcathnxqsphdhgjtgvxsfyiwbmhlmg")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(39982)
        .run()
        .await;

    TestCase::new(&test_parquet_file)
        .with_name("dict_disjunction3")
        // request_method != 'GET' OR
        //   response_status = 400 OR
        //   service = 'backend'
        .with_filter(
            disjunction([
                col("request_method").not_eq(lit("GET")),
                col("response_status").eq(lit(400_u16)),
                col("service").eq(lit("backend")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::None)
        .with_expected_rows(NUM_ROWS)
        .run()
        .await;
}
/// Expected pushdown behavior
#[derive(Debug, Clone, Copy)]
enum PushdownExpected {
    /// Did not expect filter pushdown to filter any rows
    None,
    /// Expected that some rows were filtered by pushdown
    Some,
}

/// parameters for running a test
struct TestCase<'a> {
    test_parquet_file: &'a TestParquetFile,
    /// Human readable name to help debug failures
    name: String,
    /// The filter to apply
    filter: Expr,
    /// Did we expect the pushdown filtering to have filtered any rows?
    pushdown_expected: PushdownExpected,
    /// How many rows are expected to pass the predicate overall?
    expected_rows: usize,
}

impl<'a> TestCase<'a> {
    fn new(test_parquet_file: &'a TestParquetFile) -> Self {
        Self {
            test_parquet_file,
            name: "<NOT SPECIFIED>".into(),
            // default to a filter that passes everything
            filter: lit(true),
            pushdown_expected: PushdownExpected::None,
            expected_rows: 0,
        }
    }

    fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the filter expression to use
    fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = filter;
        self
    }

    /// Set the expected predicate pushdown
    fn with_pushdown_expected(mut self, pushdown_expected: PushdownExpected) -> Self {
        self.pushdown_expected = pushdown_expected;
        self
    }

    /// Set the number of expected rows (to ensure the predicates have
    /// a good range of selectivity
    fn with_expected_rows(mut self, expected_rows: usize) -> Self {
        self.expected_rows = expected_rows;
        self
    }

    async fn run(&self) {
        println!("Running test case {}", self.name);

        // Also try and reorder the filters
        // aka if the filter is `A AND B`
        // this code will also try  `B AND A`
        let filters = split_conjunction(&self.filter);

        for perm in filters.iter().permutations(filters.len()) {
            let perm: Vec<Expr> = perm.iter().map(|e| (**e).clone()).collect();
            let filter = conjunction(perm).expect("had at least one conjunction");
            self.run_with_filter(&filter).await
        }
    }

    /// Scan the parquet file with the filters with various pushdown options
    async fn run_with_filter(&self, filter: &Expr) {
        let no_pushdown = self
            .read_with_options(
                ParquetScanOptions {
                    pushdown_filters: false,
                    reorder_filters: false,
                    enable_page_index: false,
                },
                // always expect no pushdown
                PushdownExpected::None,
                filter,
            )
            .await;

        let only_pushdown = self
            .read_with_options(
                ParquetScanOptions {
                    pushdown_filters: true,
                    reorder_filters: false,
                    enable_page_index: false,
                },
                self.pushdown_expected,
                filter,
            )
            .await;

        assert_eq!(no_pushdown, only_pushdown);

        let pushdown_and_reordering = self
            .read_with_options(
                ParquetScanOptions {
                    pushdown_filters: true,
                    reorder_filters: true,
                    enable_page_index: false,
                },
                self.pushdown_expected,
                filter,
            )
            .await;

        assert_eq!(no_pushdown, pushdown_and_reordering);

        // page index filtering is not correct:
        // https://github.com/apache/arrow-datafusion/issues/4002
        // when it is fixed we can add these tests too

        // let page_index_only = self
        //     .read_with_options(
        //         ParquetScanOptions {
        //             pushdown_filters: false,
        //             reorder_filters: false,
        //             enable_page_index: true,
        //         },
        //     )
        //     .await;
        //assert_eq!(no_pushdown, page_index_only);

        // let pushdown_reordering_and_page_index = self
        //     .read_with_options(
        //         ParquetScanOptions {
        //             pushdown_filters: false,
        //             reorder_filters: false,
        //             enable_page_index: true,
        //         },
        //     )
        //     .await;

        //assert_eq!(no_pushdown, pushdown_reordering_and_page_index);
    }

    /// Reads data from a test parquet file using the specified scan options
    async fn read_with_options(
        &self,
        scan_options: ParquetScanOptions,
        pushdown_expected: PushdownExpected,
        filter: &Expr,
    ) -> RecordBatch {
        println!("  scan options: {scan_options:?}");
        println!("  reading with filter {:?}", filter);
        let ctx = SessionContext::new();
        let exec = self
            .test_parquet_file
            .create_scan(filter.clone(), scan_options)
            .await
            .unwrap();
        let result = collect(exec.clone(), ctx.task_ctx()).await.unwrap();

        // Concatenate the results back together
        let batch = concat_batches(&self.test_parquet_file.schema(), &result).unwrap();

        let total_rows = batch.num_rows();

        println!(
            "Filter: {}, total records: {}, after filter: {}, selectivty: {}",
            filter,
            NUM_ROWS,
            total_rows,
            (total_rows as f64) / (NUM_ROWS as f64),
        );
        assert_eq!(total_rows, self.expected_rows);

        // verify expected pushdown
        let metrics =
            TestParquetFile::parquet_metrics(exec).expect("found parquet metrics");
        let pushdown_rows_filtered = get_value(&metrics, "pushdown_rows_filtered");
        println!("  pushdown_rows_filtered: {}", pushdown_rows_filtered);

        match pushdown_expected {
            PushdownExpected::None => {
                assert_eq!(pushdown_rows_filtered, 0);
            }
            PushdownExpected::Some => {
                assert!(
                    pushdown_rows_filtered > 0,
                    "Expected to filter rows via pushdown, but none were"
                );
            }
        };

        batch
    }
}

fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
    match metrics.sum_by_name(metric_name) {
        Some(v) => v.as_usize(),
        _ => {
            panic!(
                "Expected metric not found. Looking for '{}' in\n\n{:#?}",
                metric_name, metrics
            );
        }
    }
}
