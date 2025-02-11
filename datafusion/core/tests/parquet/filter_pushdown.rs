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

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::prelude::{col, lit, lit_timestamp_nano, Expr, SessionContext};
use datafusion::test_util::parquet::{ParquetScanOptions, TestParquetFile};
use datafusion_common::instant::Instant;
use datafusion_expr::utils::{conjunction, disjunction, split_conjunction};

use itertools::Itertools;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use test_utils::AccessLogGenerator;

/// how many rows of generated data to write to our parquet file (arbitrary)
const NUM_ROWS: usize = 4096;

fn generate_file(tempdir: &TempDir, props: WriterProperties) -> TestParquetFile {
    // Tune down the generator for smaller files
    let generator = AccessLogGenerator::new()
        .with_row_limit(NUM_ROWS)
        .with_pods_per_host(1..4)
        .with_containers_per_pod(1..2)
        .with_entries_per_container(128..256);

    let file = tempdir.path().join("data.parquet");

    let start = Instant::now();
    println!("Writing test data to {file:?}");
    let test_parquet_file = TestParquetFile::try_new(file, props, generator).unwrap();
    println!(
        "Completed generating test data in {:?}",
        Instant::now() - start
    );
    test_parquet_file
}

#[tokio::test]
async fn single_file() {
    // Only create the parquet file once as it is fairly large

    let tempdir = TempDir::new().unwrap();
    // Set row group size smaller so can test with fewer rows
    let props = WriterProperties::builder()
        .set_max_row_group_size(1024)
        .build();
    let test_parquet_file = generate_file(&tempdir, props);

    let case = TestCase::new(&test_parquet_file)
        .with_name("selective")
        // request_method = 'GET'
        .with_filter(col("request_method").eq(lit("GET")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(688);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("non_selective")
        // request_method != 'GET'
        .with_filter(col("request_method").not_eq(lit("GET")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(3408);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
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
        .with_expected_rows(135);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("everything")
        // filter filters everything (no row has this status)
        // response_status = 429
        .with_filter(col("response_status").eq(lit(429_u16)))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(0);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("nothing")
        // No rows are filtered out -- all are returned
        // response_status > 0
        .with_filter(col("response_status").gt(lit(0_u16)))
        .with_pushdown_expected(PushdownExpected::None)
        .with_expected_rows(NUM_ROWS);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("dict_selective")
        // container = 'backend_container_0'
        .with_filter(col("container").eq(lit("backend_container_0")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(802);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("not eq")
        // container != 'backend_container_0'
        .with_filter(col("container").not_eq(lit("backend_container_0")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(3294);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("dict_conjunction")
        // container == 'backend_container_0' AND
        //   pod = 'cvcjfhwtjttxhiugepoojxrplihywu'
        .with_filter(
            conjunction([
                col("container").eq(lit("backend_container_0")),
                col("pod").eq(lit("cvcjfhwtjttxhiugepoojxrplihywu")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(134);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("dict_very_selective")
        // request_bytes > 2B AND
        //   container == 'backend_container_0' AND
        //   pod = 'cvcjfhwtjttxhiugepoojxrplihywu'
        .with_filter(
            conjunction([
                col("request_bytes").gt(lit(2000000000)),
                col("container").eq(lit("backend_container_0")),
                col("pod").eq(lit("cvcjfhwtjttxhiugepoojxrplihywu")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(2);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("dict_very_selective2")
        // client_addr = '204.47.29.82' AND
        //   container == 'backend_container_0' AND
        //   pod = 'cvcjfhwtjttxhiugepoojxrplihywu'
        .with_filter(
            conjunction(vec![
                col("client_addr").eq(lit("58.242.143.99")),
                col("container").eq(lit("backend_container_0")),
                col("pod").eq(lit("cvcjfhwtjttxhiugepoojxrplihywu")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(1);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
        .with_name("dict_disjunction")
        // container = 'backend_container_0' OR
        //   pod = 'cvcjfhwtjttxhiugepoojxrplihywu'
        .with_filter(
            disjunction([
                col("container").eq(lit("backend_container_0")),
                col("pod").eq(lit("cvcjfhwtjttxhiugepoojxrplihywu")),
            ])
            .unwrap(),
        )
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(802);
    case.run().await;

    let case = TestCase::new(&test_parquet_file)
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
        .with_pushdown_expected(PushdownExpected::Some)
        .with_expected_rows(3672);
    case.run().await;
}

#[tokio::test]
async fn single_file_small_data_pages() {
    let tempdir = TempDir::new().unwrap();

    // Set low row count limit to improve page filtering
    let props = WriterProperties::builder()
        .set_max_row_group_size(2048)
        .set_data_page_row_count_limit(512)
        .set_write_batch_size(512)
        .build();
    let test_parquet_file = generate_file(&tempdir, props);

    // The statistics on the 'pod' column are as follows:
    //
    // docker run -v /tmp:/tmp nathanhowell/parquet-tools dump -d -c pod -n /tmp/.tmppkTohR/data.parquet
    //
    // ```
    // row group 0
    //     --------------------------------------------------------------------------------
    //     pod:  BINARY UNCOMPRESSED DO:782 FPO:1215 SZ:744/744/1.00 VC:2048 ENC:RLE,RLE_DICTIONARY,PLAIN ST:[min: azvagebjesrqboyqxmgaskvpwddebuptqyy, max: zamirxzhihhfqdvhuxeziuukkqyutmczbhfgx, num_nulls not defined]
    //
    // pod TV=2048 RL=0 DL=0 DS: 11 DE:PLAIN
    //     ----------------------------------------------------------------------------
    //     page 0:                    DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: azvagebjesrqboyqxmgaskvpwddebuptqyy, max: ksjzzqfxvawhmlkopjsbponfdwsurxff, num_nulls not defined] CRC:[none] SZ:10 VC:512
    //     page 1:                    DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: azvagebjesrqboyqxmgaskvpwddebuptqyy, max: wlftgepiwhnmzqrsyijhqbauhjplru, num_nulls not defined] CRC:[none] SZ:18 VC:1013
    //     page 2:                    DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: ewzlijvnljqeqhqhftfalqbqfsyidw, max: zamirxzhihhfqdvhuxeziuukkqyutmczbhfgx, num_nulls not defined] CRC:[none] SZ:12 VC:523
    //
    // row group 1
    //     --------------------------------------------------------------------------------
    //     pod:  BINARY UNCOMPRESSED DO:249244 FPO:249724 SZ:901/901/1.00 VC:2048 ENC:RLE,RLE_DICTIONARY,PLAIN ST:[min: csvnvrdcuzoftoidzmczrtqnrzgfpj, max: zamirxzhihhfqdvhuxeziuukkqyutmczbhfgx, num_nulls not defined]
    //
    // pod TV=2048 RL=0 DL=0 DS: 12 DE:PLAIN
    //     ----------------------------------------------------------------------------
    //     page 0:                    DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: dhhqgbsjutqdvqpikmnwqdnrhkqnjyieoviujkj, max: zamirxzhihhfqdvhuxeziuukkqyutmczbhfgx, num_nulls not defined] CRC:[none] SZ:12 VC:512
    //     page 1:                    DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: dlowgwtqjiifqajbobiuqoflbmsbobwsqtrc, max: uipgzhbptpinjcwbdwhkfdjzdfzrlffrifzh, num_nulls not defined] CRC:[none] SZ:12 VC:671
    //     page 2:                    DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: csvnvrdcuzoftoidzmczrtqnrzgfpj, max: xacatvakpxztzuucoxhjiofxykryoxc, num_nulls not defined] CRC:[none] SZ:16 VC:781
    //     page 3:                    DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: nxihlfujkdzymexwpqurhawwchvkdrntixjs, max: xacatvakpxztzuucoxhjiofxykryoxc, num_nulls not defined] CRC:[none] SZ:9 VC:84
    // ```

    TestCase::new(&test_parquet_file)
        .with_name("selective")
        // predicate is chosen carefully to prune all bar 0-2 and 1-0
        // pod = 'zamirxzhihhfqdvhuxeziuukkqyutmczbhfgx'
        .with_filter(col("pod").eq(lit("zamirxzhihhfqdvhuxeziuukkqyutmczbhfgx")))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_page_index_filtering_expected(PageIndexFilteringExpected::Some)
        .with_expected_rows(174)
        .run()
        .await;

    // row group 0
    //     --------------------------------------------------------------------------------
    //     time:  INT64 UNCOMPRESSED DO:3317 FPO:5334 SZ:4209/4209/1.00 VC:2048 ENC:RLE,RLE_DICTIONARY,PLAIN ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000254976, num_nulls not defined]
    //
    // time TV=2048 RL=0 DL=0 DS: 250 DE:PLAIN
    //     ----------------------------------------------------------------------------
    //     page 0:                     DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000203776, num_nulls not defined] CRC:[none] SZ:515 VC:512
    //     page 1:                     DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000254976, num_nulls not defined] CRC:[none] SZ:1020 VC:1013
    //     page 2:                     DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000216064, num_nulls not defined] CRC:[none] SZ:531 VC:523
    //
    // row group 1
    //     --------------------------------------------------------------------------------
    //     time:  INT64 UNCOMPRESSED DO:252201 FPO:254186 SZ:4220/4220/1.00 VC:2048 ENC:RLE,RLE_DICTIONARY,PLAIN ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000250880, num_nulls not defined]
    //
    // time TV=2048 RL=0 DL=0 DS: 246 DE:PLAIN
    //     ----------------------------------------------------------------------------
    //     page 0:                     DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000231424, num_nulls not defined] CRC:[none] SZ:515 VC:512
    //     page 1:                     DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000250880, num_nulls not defined] CRC:[none] SZ:675 VC:671
    //     page 2:                     DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000211968, num_nulls not defined] CRC:[none] SZ:787 VC:781
    //     page 3:                     DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[min: 1970-01-01T00:00:00.000000000, max: 1970-01-01T00:00:00.000177152, num_nulls not defined] CRC:[none] SZ:90 VC:84

    TestCase::new(&test_parquet_file)
        .with_name("selective")
        // predicate is chosen carefully to prune all bar 0-1, 1-0, 1-1
        // time > 1970-01-01T00:00:00.000216064
        .with_filter(col("time").gt(lit_timestamp_nano(216064)))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_page_index_filtering_expected(PageIndexFilteringExpected::Some)
        .with_expected_rows(178)
        .run()
        .await;

    // row group 0
    //     --------------------------------------------------------------------------------
    //     decimal_price:  FIXED_LEN_BYTE_ARRAY UNCOMPRESSED DO:0 FPO:215263 SZ:32948/32948/1.00 VC:2048 ENC:RLE,PLAIN ST:[min: 1, max: 1013, num_nulls not defined]
    //
    // decimal_price TV=2048 RL=0 DL=0
    //     ----------------------------------------------------------------------------
    //     page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 1, max: 512, num_nulls not defined] CRC:[none] SZ:8192 VC:512
    //     page 1:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 1, max: 1013, num_nulls not defined] CRC:[none] SZ:16208 VC:1013
    //     page 2:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 1, max: 919, num_nulls not defined] CRC:[none] SZ:8368 VC:523
    //
    // row group 1
    //     --------------------------------------------------------------------------------
    //     decimal_price:  FIXED_LEN_BYTE_ARRAY UNCOMPRESSED DO:0 FPO:461433 SZ:33006/33006/1.00 VC:2048 ENC:RLE,PLAIN ST:[min: 1, max: 787, num_nulls not defined]
    //
    // decimal_price TV=2048 RL=0 DL=0
    //     ----------------------------------------------------------------------------
    //     page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 117, max: 628, num_nulls not defined] CRC:[none] SZ:8192 VC:512
    //     page 1:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 1, max: 787, num_nulls not defined] CRC:[none] SZ:10736 VC:671
    //     page 2:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 1, max: 781, num_nulls not defined] CRC:[none] SZ:12496 VC:781
    //     page 3:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 1, max: 515, num_nulls not defined] CRC:[none] SZ:1344 VC:84

    TestCase::new(&test_parquet_file)
        .with_name("selective_on_decimal")
        // predicate is chosen carefully to prune all bar 0-1
        // decimal_price < 9200
        .with_filter(col("decimal_price").gt(lit(919)))
        .with_pushdown_expected(PushdownExpected::Some)
        .with_page_index_filtering_expected(PageIndexFilteringExpected::Some)
        .with_expected_rows(94)
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

/// Expected pushdown behavior
#[derive(Debug, Clone, Copy)]
enum PageIndexFilteringExpected {
    /// How many pages were expected to be pruned
    None,
    /// Expected that more than 0 were pruned
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

    /// Did we expect page filtering to filter out pages
    page_index_filtering_expected: PageIndexFilteringExpected,

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
            page_index_filtering_expected: PageIndexFilteringExpected::None,
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
    fn with_pushdown_expected(mut self, v: PushdownExpected) -> Self {
        self.pushdown_expected = v;
        self
    }

    /// Set the expected page filtering
    fn with_page_index_filtering_expected(
        mut self,
        v: PageIndexFilteringExpected,
    ) -> Self {
        self.page_index_filtering_expected = v;
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
                filter,
            )
            .await;

        assert_eq!(no_pushdown, pushdown_and_reordering);

        let page_index_only = self
            .read_with_options(
                ParquetScanOptions {
                    pushdown_filters: false,
                    reorder_filters: false,
                    enable_page_index: true,
                },
                filter,
            )
            .await;
        assert_eq!(no_pushdown, page_index_only);

        let pushdown_reordering_and_page_index = self
            .read_with_options(
                ParquetScanOptions {
                    pushdown_filters: true,
                    reorder_filters: true,
                    enable_page_index: true,
                },
                filter,
            )
            .await;

        assert_eq!(no_pushdown, pushdown_reordering_and_page_index);
    }

    /// Reads data from a test parquet file using the specified scan options
    async fn read_with_options(
        &self,
        scan_options: ParquetScanOptions,
        filter: &Expr,
    ) -> RecordBatch {
        println!("  scan options: {scan_options:?}");
        println!("  reading with filter {filter:?}");
        let ctx = SessionContext::new_with_config(scan_options.config());
        let exec = self
            .test_parquet_file
            .create_scan(&ctx, Some(filter.clone()))
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
            TestParquetFile::parquet_metrics(&exec).expect("found parquet metrics");

        let pushdown_expected = if scan_options.pushdown_filters {
            self.pushdown_expected
        } else {
            // if filter pushdown is not enabled we don't expect it to filter rows
            PushdownExpected::None
        };

        let pushdown_rows_pruned = get_value(&metrics, "pushdown_rows_pruned");
        println!("  pushdown_rows_pruned: {pushdown_rows_pruned}");
        let pushdown_rows_matched = get_value(&metrics, "pushdown_rows_matched");
        println!("  pushdown_rows_matched: {pushdown_rows_matched}");

        match pushdown_expected {
            PushdownExpected::None => {
                assert_eq!(pushdown_rows_pruned, 0, "{}", self.name);
            }
            PushdownExpected::Some => {
                assert!(
                    pushdown_rows_pruned > 0,
                    "{}: Expected to filter rows via pushdown, but none were",
                    self.name
                );
            }
        };

        let page_index_rows_pruned = get_value(&metrics, "page_index_rows_pruned");
        println!(" page_index_rows_pruned: {page_index_rows_pruned}");
        let page_index_rows_matched = get_value(&metrics, "page_index_rows_matched");
        println!(" page_index_rows_matched: {page_index_rows_matched}");

        let page_index_filtering_expected = if scan_options.enable_page_index {
            self.page_index_filtering_expected
        } else {
            // if page index filtering is not enabled, don't expect it
            // to filter rows
            PageIndexFilteringExpected::None
        };

        match page_index_filtering_expected {
            PageIndexFilteringExpected::None => {
                assert_eq!(page_index_rows_pruned, 0);
            }
            PageIndexFilteringExpected::Some => {
                assert!(
                    page_index_rows_pruned > 0,
                    "Expected to filter rows via page index but none were",
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
                "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
            );
        }
    }
}
