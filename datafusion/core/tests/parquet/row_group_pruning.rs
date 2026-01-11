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

//! This file contains an end to end test of parquet pruning. It writes
//! data into a parquet file and then verifies row groups are pruned as
//! expected.
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionConfig;
use datafusion_common::{DataFusionError, ScalarValue};
use itertools::Itertools;

use crate::parquet::Unit::RowGroup;
use crate::parquet::{ContextWithParquet, Scenario};
use datafusion_expr::{col, lit};
struct RowGroupPruningTest {
    scenario: Scenario,
    query: String,
    expected_errors: Option<usize>,
    expected_row_group_matched_by_statistics: Option<usize>,
    expected_row_group_fully_matched_by_statistics: Option<usize>,
    expected_row_group_pruned_by_statistics: Option<usize>,
    expected_files_pruned_by_statistics: Option<usize>,
    expected_row_group_matched_by_bloom_filter: Option<usize>,
    expected_row_group_pruned_by_bloom_filter: Option<usize>,
    expected_limit_pruned_row_groups: Option<usize>,
    expected_rows: usize,
}
impl RowGroupPruningTest {
    // Start building the test configuration
    fn new() -> Self {
        Self {
            scenario: Scenario::Timestamps, // or another default
            query: String::new(),
            expected_errors: None,
            expected_row_group_matched_by_statistics: None,
            expected_row_group_pruned_by_statistics: None,
            expected_row_group_fully_matched_by_statistics: None,
            expected_files_pruned_by_statistics: None,
            expected_row_group_matched_by_bloom_filter: None,
            expected_row_group_pruned_by_bloom_filter: None,
            expected_limit_pruned_row_groups: None,
            expected_rows: 0,
        }
    }

    // Set the scenario for the test
    fn with_scenario(mut self, scenario: Scenario) -> Self {
        self.scenario = scenario;
        self
    }

    // Set the SQL query for the test
    fn with_query(mut self, query: &str) -> Self {
        self.query = query.to_string();
        self
    }

    // Set the expected errors for the test
    fn with_expected_errors(mut self, errors: Option<usize>) -> Self {
        self.expected_errors = errors;
        self
    }

    // Set the expected matched row groups by statistics
    fn with_matched_by_stats(mut self, matched_by_stats: Option<usize>) -> Self {
        self.expected_row_group_matched_by_statistics = matched_by_stats;
        self
    }

    // Set the expected fully matched row groups by statistics
    fn with_fully_matched_by_stats(
        mut self,
        fully_matched_by_stats: Option<usize>,
    ) -> Self {
        self.expected_row_group_fully_matched_by_statistics = fully_matched_by_stats;
        self
    }

    // Set the expected pruned row groups by statistics
    fn with_pruned_by_stats(mut self, pruned_by_stats: Option<usize>) -> Self {
        self.expected_row_group_pruned_by_statistics = pruned_by_stats;
        self
    }

    fn with_pruned_files(mut self, pruned_files: Option<usize>) -> Self {
        self.expected_files_pruned_by_statistics = pruned_files;
        self
    }

    // Set the expected matched row groups by bloom filter
    fn with_matched_by_bloom_filter(mut self, matched_by_bf: Option<usize>) -> Self {
        self.expected_row_group_matched_by_bloom_filter = matched_by_bf;
        self
    }

    // Set the expected pruned row groups by bloom filter
    fn with_pruned_by_bloom_filter(mut self, pruned_by_bf: Option<usize>) -> Self {
        self.expected_row_group_pruned_by_bloom_filter = pruned_by_bf;
        self
    }

    fn with_limit_pruned_row_groups(mut self, pruned_by_limit: Option<usize>) -> Self {
        self.expected_limit_pruned_row_groups = pruned_by_limit;
        self
    }

    /// Set the number of expected rows from the output of this test
    fn with_expected_rows(mut self, rows: usize) -> Self {
        self.expected_rows = rows;
        self
    }

    // Execute the test with the current configuration
    async fn test_row_group_prune(self) {
        let output = ContextWithParquet::new(self.scenario, RowGroup(5))
            .await
            .query(&self.query)
            .await;

        println!("{}", output.description());
        assert_eq!(
            output.predicate_evaluation_errors(),
            self.expected_errors,
            "mismatched predicate_evaluation error"
        );
        assert_eq!(
            output.row_groups_matched_statistics(),
            self.expected_row_group_matched_by_statistics,
            "mismatched row_groups_matched_statistics",
        );
        assert_eq!(
            output.row_groups_pruned_statistics(),
            self.expected_row_group_pruned_by_statistics,
            "mismatched row_groups_pruned_statistics",
        );
        assert_eq!(
            output.files_ranges_pruned_statistics(),
            self.expected_files_pruned_by_statistics,
            "mismatched files_ranges_pruned_statistics",
        );
        let bloom_filter_metrics = output.row_groups_bloom_filter();
        assert_eq!(
            bloom_filter_metrics.as_ref().map(|pm| pm.total_matched()),
            self.expected_row_group_matched_by_bloom_filter,
            "mismatched row_groups_matched_bloom_filter",
        );
        assert_eq!(
            bloom_filter_metrics.map(|pm| pm.total_pruned()),
            self.expected_row_group_pruned_by_bloom_filter,
            "mismatched row_groups_pruned_bloom_filter",
        );

        assert_eq!(
            output.result_rows,
            self.expected_rows,
            "Expected {} rows, got {}: {}",
            output.result_rows,
            self.expected_rows,
            output.description(),
        );
    }

    // Execute the test with the current configuration
    async fn test_row_group_prune_with_custom_data(
        self,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        max_row_per_group: usize,
    ) {
        let output = ContextWithParquet::with_custom_data(
            self.scenario,
            RowGroup(max_row_per_group),
            schema,
            batches,
        )
        .await
        .query(&self.query)
        .await;

        println!("{}", output.description());
        assert_eq!(
            output.predicate_evaluation_errors(),
            self.expected_errors,
            "mismatched predicate_evaluation error"
        );
        assert_eq!(
            output.row_groups_matched_statistics(),
            self.expected_row_group_matched_by_statistics,
            "mismatched row_groups_matched_statistics",
        );
        assert_eq!(
            output.row_groups_fully_matched_statistics(),
            self.expected_row_group_fully_matched_by_statistics,
            "mismatched row_groups_fully_matched_statistics",
        );
        assert_eq!(
            output.row_groups_pruned_statistics(),
            self.expected_row_group_pruned_by_statistics,
            "mismatched row_groups_pruned_statistics",
        );
        assert_eq!(
            output.files_ranges_pruned_statistics(),
            self.expected_files_pruned_by_statistics,
            "mismatched files_ranges_pruned_statistics",
        );
        assert_eq!(
            output.limit_pruned_row_groups(),
            self.expected_limit_pruned_row_groups,
            "mismatched limit_pruned_row_groups",
        );
        assert_eq!(
            output.result_rows,
            self.expected_rows,
            "Expected {} rows, got {}: {}",
            output.result_rows,
            self.expected_rows,
            output.description(),
        );
    }
}

#[tokio::test]
async fn prune_timestamps_nanos() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Timestamps)
        .with_query("SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(10)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_timestamps_micros() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Timestamps)
        .with_query(
            "SELECT * FROM t where micros < to_timestamp_micros('2020-01-02 01:01:11Z')",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(10)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_timestamps_millis() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Timestamps)
        .with_query(
            "SELECT * FROM t where micros < to_timestamp_millis('2020-01-02 01:01:11Z')",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(10)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_timestamps_seconds() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Timestamps)
        .with_query(
            "SELECT * FROM t where seconds < to_timestamp_seconds('2020-01-02 01:01:11Z')",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(10)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_date32() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dates)
        .with_query("SELECT * FROM t where date32 < cast('2020-01-02' as date)")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(3))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_date64() {
    // work around for not being able to cast Date32 to Date64 automatically

    let date = "2020-01-02"
        .parse::<chrono::NaiveDate>()
        .unwrap()
        .and_time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    let date = ScalarValue::Date64(Some(date.and_utc().timestamp_millis()));

    let output = ContextWithParquet::new(Scenario::Dates, RowGroup(5))
        .await
        .query_with_expr(col("date64").lt(lit(date)))
        // .query(
        //     "SELECT * FROM t where date64 < cast('2020-01-02' as date)",
        // query results in Plan("'Date64 < Date32' can't be evaluated because there isn't a common type to coerce the types to")
        // )
        .await;

    println!("{}", output.description());
    // This should prune out groups  without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    // 'dates' table has 4 row groups, and only the first one is matched by the predicate
    assert_eq!(output.row_groups_matched_statistics(), Some(1));
    assert_eq!(output.row_groups_pruned_statistics(), Some(3));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_disabled() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Timestamps)
        .with_query("SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(10)
        .test_row_group_prune()
        .await;

    // test without pruning
    let query = "SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')";
    let expected_rows = 10;
    let config = SessionConfig::new().with_parquet_pruning(false);

    let output = ContextWithParquet::with_config(
        Scenario::Timestamps,
        RowGroup(5),
        config,
        None,
        None,
    )
    .await
    .query(query)
    .await;
    println!("{}", output.description());

    // This should not prune any
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_matched(), Some(4));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(
        output.result_rows,
        expected_rows,
        "{}",
        output.description()
    );
}

// $bits: number of bits of the integer to test (8, 16, 32, 64)
// $correct_bloom_filters: if false, replicates the
// https://github.com/apache/datafusion/issues/9779 bug so that tests pass
// if and only if Bloom filters on Int8 and Int16 columns are still buggy.
macro_rules! int_tests {
    ($bits:expr) => {
        paste::item! {
            #[tokio::test]
            async fn [<prune_int $bits _lt >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where i{} < 1", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(3))
                    .with_pruned_by_stats(Some(1))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(3))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(11)
                    .test_row_group_prune()
                    .await;

                // result of sql "SELECT * FROM t where i < 1" is same as
                // "SELECT * FROM t where -i > -1"
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where -i{} > -1", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(3))
                    .with_pruned_by_stats(Some(1))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(3))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(11)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_int $bits _eq >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where i{} = 1", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(1))
                    .with_pruned_by_stats(Some(3))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(1))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(1)
                    .test_row_group_prune()
                    .await;
            }
            #[tokio::test]
            async fn [<prune_int $bits _scalar_fun_and_eq >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where abs(i{}) = 1 and i{} = 1", $bits, $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(1))
                    .with_pruned_by_stats(Some(3))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(1))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(1)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_int $bits _scalar_fun >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where abs(i{}) = 1", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(4))
                    .with_pruned_by_stats(Some(0))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(4))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(3)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_int $bits _complex_expr >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where i{}+1 = 1", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(4))
                    .with_pruned_by_stats(Some(0))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(4))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(2)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_int $bits _complex_expr_subtract >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where 1-i{} > 1", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(4))
                    .with_pruned_by_stats(Some(0))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(4))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(9)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_int $bits _eq_in_list >]() {
                // result of sql "SELECT * FROM t where in (1)"
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where i{} in (1)", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(1))
                    .with_pruned_by_stats(Some(3))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(1))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(1)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_int $bits _eq_in_list_2 >]() {
                // result of sql "SELECT * FROM t where in (1000)", prune all
                // test whether statistics works
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where i{} in (100)", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(0))
                    .with_pruned_by_stats(Some(0))
                    .with_pruned_files(Some(1))
                    .with_matched_by_bloom_filter(Some(0))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(0)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_int $bits _eq_in_list_negated >]() {
                // result of sql "SELECT * FROM t where not in (1)" prune nothing
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::Int)
                    .with_query(&format!("SELECT * FROM t where i{} not in (1)", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(4))
                    .with_pruned_by_stats(Some(0))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(4))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(19)
                    .test_row_group_prune()
                    .await;
            }
        }
    };
}

// int8/int16 are incorrect: https://github.com/apache/datafusion/issues/9779
int_tests!(32);
int_tests!(64);

// $bits: number of bits of the integer to test (8, 16, 32, 64)
// $correct_bloom_filters: if false, replicates the
// https://github.com/apache/datafusion/issues/9779 bug so that tests pass
// if and only if Bloom filters on UInt8 and UInt16 columns are still buggy.
macro_rules! uint_tests {
    ($bits:expr) => {
        paste::item! {
            #[tokio::test]
            async fn [<prune_uint $bits _lt >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::UInt)
                    .with_query(&format!("SELECT * FROM t where u{} < 6", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(3))
                    .with_pruned_by_stats(Some(1))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(3))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(11)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_uint $bits _eq >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::UInt)
                    .with_query(&format!("SELECT * FROM t where u{} = 6", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(1))
                    .with_pruned_by_stats(Some(3))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(1))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(1)
                    .test_row_group_prune()
                    .await;
            }
            #[tokio::test]
            async fn [<prune_uint $bits _scalar_fun_and_eq >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::UInt)
                    .with_query(&format!("SELECT * FROM t where power(u{}, 2) = 36 and u{} = 6", $bits, $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(1))
                    .with_pruned_by_stats(Some(3))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(1))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(1)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_uint $bits _scalar_fun >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::UInt)
                    .with_query(&format!("SELECT * FROM t where power(u{}, 2) = 25", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(4))
                    .with_pruned_by_stats(Some(0))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(4))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(2)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_uint $bits _complex_expr >]() {
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::UInt)
                    .with_query(&format!("SELECT * FROM t where u{}+1 = 6", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(4))
                    .with_pruned_by_stats(Some(0))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(4))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(2)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_uint $bits _eq_in_list >]() {
                // result of sql "SELECT * FROM t where in (1)"
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::UInt)
                    .with_query(&format!("SELECT * FROM t where u{} in (6)", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(1))
                    .with_pruned_by_stats(Some(3))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(1))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(1)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_uint $bits _eq_in_list_2 >]() {
                // result of sql "SELECT * FROM t where in (1000)", prune all
                // test whether statistics works
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::UInt)
                    .with_query(&format!("SELECT * FROM t where u{} in (100)", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(0))
                    .with_pruned_by_stats(Some(4))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(0))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(0)
                    .test_row_group_prune()
                    .await;
            }

            #[tokio::test]
            async fn [<prune_uint $bits _eq_in_list_negated >]() {
                // result of sql "SELECT * FROM t where not in (1)" prune nothing
                RowGroupPruningTest::new()
                    .with_scenario(Scenario::UInt)
                    .with_query(&format!("SELECT * FROM t where u{} not in (6)", $bits))
                    .with_expected_errors(Some(0))
                    .with_matched_by_stats(Some(4))
                    .with_pruned_by_stats(Some(0))
                    .with_pruned_files(Some(0))
                    .with_matched_by_bloom_filter(Some(4))
                    .with_pruned_by_bloom_filter(Some(0))
                    .with_expected_rows(19)
                    .test_row_group_prune()
                    .await;
            }
        }
    };
}

// uint8/uint16 are incorrect: https://github.com/apache/datafusion/issues/9779
uint_tests!(32);
uint_tests!(64);

#[tokio::test]
async fn prune_int32_eq_large_in_list() {
    // result of sql "SELECT * FROM t where i in (2050...2582)", prune all
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32Range)
        .with_query(
            format!(
                "SELECT * FROM t where i in ({})",
                (200050..200082).join(",")
            )
            .as_str(),
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(0))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_uint32_eq_large_in_list() {
    // result of sql "SELECT * FROM t where i in (2050...2582)", prune all
    RowGroupPruningTest::new()
        .with_scenario(Scenario::UInt32Range)
        .with_query(
            format!(
                "SELECT * FROM t where u in ({})",
                (200050..200082).join(",")
            )
            .as_str(),
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(0))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_f64_lt() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Float64)
        .with_query("SELECT * FROM t where f < 1")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(11)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Float64)
        .with_query("SELECT * FROM t where -f > -1")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(11)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_f64_scalar_fun_and_gt() {
    // result of sql "SELECT * FROM t where abs(f - 1) <= 0.000001  and f >= 0.1"
    // only use "f >= 0" to prune
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Float64)
        .with_query("SELECT * FROM t where abs(f - 1) <= 0.000001  and f >= 0.1")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_f64_scalar_fun() {
    // result of sql "SELECT * FROM t where abs(f-1) <= 0.000001" is not supported
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Float64)
        .with_query("SELECT * FROM t where abs(f-1) <= 0.000001")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(4))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(4))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_f64_complex_expr() {
    // result of sql "SELECT * FROM t where f+1 > 1.1"" is not supported
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Float64)
        .with_query("SELECT * FROM t where f+1 > 1.1")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(4))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(4))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(9)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_f64_complex_expr_subtract() {
    // result of sql "SELECT * FROM t where 1-f > 1" is not supported
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Float64)
        .with_query("SELECT * FROM t where 1-f > 1")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(4))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(4))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(9)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_decimal_lt() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col < 4")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(6)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col < cast(4.55 as decimal(20,2))")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(8)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col < 4")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(6)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col < cast(4.55 as decimal(20,2))")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(8)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_decimal_eq() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col = 4")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col = 4.00")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col = 4")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col = 4.00")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;
    // The data type of decimal_col is decimal(38,2)
}

#[tokio::test]
async fn prune_decimal_in_list() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col in (4,3,2,123456789123)")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(5)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col in (4.00,3.00,11.2345,1)")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(6)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col in (4,3,2,123456789123)")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(5)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col in (4.00,3.00,11.2345,1)")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(6)
        .test_row_group_prune()
        .await;

    // test data -> r1: {1,2,3,4,5}, r2: {1,2,3,4,6}, r3: {1,2,3,4,6}
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalBloomFilterInt32)
        .with_query("SELECT * FROM t where decimal_col in (5)")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(2))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;

    // test data -> r1: {1,2,3,4,5}, r2: {1,2,3,4,6}, r3: {1,2,3,4,6}
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalBloomFilterInt64)
        .with_query("SELECT * FROM t where decimal_col in (5)")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(2))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;

    // test data -> r1: {1,2,3,4,5}, r2: {1,2,3,4,6}, r3: {1,2,3,4,6}
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecisionBloomFilter)
        .with_query("SELECT * FROM t where decimal_col in (5)")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(2))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_string_eq_match() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_string FROM t WHERE service_string = 'backend one'",
        )
        .with_expected_errors(Some(0))
        // false positive on 'all backends' batch: 'backend five' < 'backend one' < 'backend three'
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_string_eq_no_match() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_string FROM t WHERE service_string = 'backend nine'",
        )
        .with_expected_errors(Some(0))
        // false positive on 'all backends' batch: 'backend five' < 'backend one' < 'backend three'
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(0))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_string FROM t WHERE service_string = 'frontend nine'",
        )
        .with_expected_errors(Some(0))
        // false positive on 'all frontends' batch: 'frontend five' < 'frontend nine' < 'frontend two'
        // false positive on 'mixed' batch: 'backend one' < 'frontend nine' < 'frontend six'
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(0))
        .with_pruned_by_bloom_filter(Some(2))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_string_neq() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_string FROM t WHERE service_string != 'backend one'",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(14)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_string_lt() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_string FROM t WHERE service_string < 'backend one'",
        )
        .with_expected_errors(Some(0))
        // matches 'all backends' only
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(3)
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_string FROM t WHERE service_string < 'backend zero'",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        // all backends from 'mixed' and 'all backends'
        .with_expected_rows(8)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_binary_eq_match() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_binary FROM t WHERE service_binary = CAST('backend one' AS bytea)",
        )
        .with_expected_errors(Some(0))
        // false positive on 'all backends' batch: 'backend five' < 'backend one' < 'backend three'
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_binary_eq_no_match() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_binary FROM t WHERE service_binary = CAST('backend nine' AS bytea)",
        )
        .with_expected_errors(Some(0))
        // false positive on 'all backends' batch: 'backend five' < 'backend one' < 'backend three'
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(0))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_binary FROM t WHERE service_binary = CAST('frontend nine' AS bytea)",
        )
        .with_expected_errors(Some(0))
        // false positive on 'all frontends' batch: 'frontend five' < 'frontend nine' < 'frontend two'
        // false positive on 'mixed' batch: 'backend one' < 'frontend nine' < 'frontend six'
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(0))
        .with_pruned_by_bloom_filter(Some(2))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_binary_neq() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_binary FROM t WHERE service_binary != CAST('backend one' AS bytea)",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(14)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_binary_lt() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_binary FROM t WHERE service_binary < CAST('backend one' AS bytea)",
        )
        .with_expected_errors(Some(0))
        // matches 'all backends' only
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(3)
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_binary FROM t WHERE service_binary < CAST('backend zero' AS bytea)",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        // all backends from 'mixed' and 'all backends'
        .with_expected_rows(8)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_fixedsizebinary_eq_match() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_fixedsize FROM t WHERE service_fixedsize = ARROW_CAST(CAST('fe6' AS bytea), 'FixedSizeBinary(3)')",
        )
        .with_expected_errors(Some(0))
        // false positive on 'all frontends' batch: 'fe1' < 'fe6' < 'fe7'
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_fixedsize FROM t WHERE service_fixedsize = ARROW_CAST(CAST('fe6' AS bytea), 'FixedSizeBinary(3)')",
        )
        .with_expected_errors(Some(0))
        // false positive on 'all frontends' batch: 'fe1' < 'fe6' < 'fe7'
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_fixedsizebinary_eq_no_match() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_fixedsize FROM t WHERE service_fixedsize = ARROW_CAST(CAST('be9' AS bytea), 'FixedSizeBinary(3)')",
        )
        .with_expected_errors(Some(0))
        // false positive on 'mixed' batch: 'be1' < 'be9' < 'fe4'
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(0))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_fixedsizebinary_neq() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_fixedsize FROM t WHERE service_fixedsize != ARROW_CAST(CAST('be1' AS bytea), 'FixedSizeBinary(3)')",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(14)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_fixedsizebinary_lt() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_fixedsize FROM t WHERE service_fixedsize < ARROW_CAST(CAST('be3' AS bytea), 'FixedSizeBinary(3)')",
        )
        .with_expected_errors(Some(0))
        // matches 'all backends' only
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::ByteArray)
        .with_query(
            "SELECT name, service_fixedsize FROM t WHERE service_fixedsize < ARROW_CAST(CAST('be9' AS bytea), 'FixedSizeBinary(3)')",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        // all backends from 'mixed' and 'all backends'
        .with_expected_rows(8)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_periods_in_column_names() {
    // There are three row groups for "service.name", each with 5 rows = 15 rows total
    // name = "HTTP GET / DISPATCH", service.name = ['frontend', 'frontend'],
    // name = "HTTP PUT / DISPATCH", service.name = ['backend',  'frontend'],
    // name = "HTTP GET / DISPATCH", service.name = ['backend',  'backend' ],
    RowGroupPruningTest::new()
        .with_scenario(Scenario::PeriodsInColumnNames)
        .with_query(  "SELECT \"name\", \"service.name\" FROM t WHERE \"service.name\" = 'frontend'")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(7)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::PeriodsInColumnNames)
        .with_query(  "SELECT \"name\", \"service.name\" FROM t WHERE \"name\" != 'HTTP GET / DISPATCH'")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(5)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::PeriodsInColumnNames)
        .with_query(  "SELECT \"name\", \"service.name\" FROM t WHERE \"service.name\" = 'frontend' AND \"name\" != 'HTTP GET / DISPATCH'")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn test_row_group_with_null_values() {
    // Three row groups:
    // 1. all Null values
    // 2. values from 1 to 5
    // 3. all Null values

    // After pruning, only row group 2 should be selected
    RowGroupPruningTest::new()
        .with_scenario(Scenario::WithNullValues)
        .with_query("SELECT * FROM t WHERE \"i8\" <= 5")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_pruned_by_stats(Some(2))
        .with_expected_rows(5)
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;

    // After pruning, only row group 1,3 should be selected
    RowGroupPruningTest::new()
        .with_scenario(Scenario::WithNullValues)
        .with_query("SELECT * FROM t WHERE \"i8\" is Null")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(2))
        .with_pruned_files(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_expected_rows(10)
        .with_matched_by_bloom_filter(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;

    // After pruning, only row group 2 should be selected
    RowGroupPruningTest::new()
        .with_scenario(Scenario::WithNullValues)
        .with_query("SELECT * FROM t WHERE \"i16\" is Not Null")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_pruned_by_stats(Some(2))
        .with_expected_rows(5)
        .with_matched_by_bloom_filter(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;

    // All row groups will be pruned
    RowGroupPruningTest::new()
        .with_scenario(Scenario::WithNullValues)
        .with_query("SELECT * FROM t WHERE \"i32\" > 7")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(0))
        .with_pruned_by_stats(Some(0))
        .with_pruned_files(Some(1))
        .with_expected_rows(0)
        .with_matched_by_bloom_filter(Some(0))
        .with_pruned_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn test_bloom_filter_utf8_dict() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE utf8 = 'h'")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(1)
        .with_pruned_by_bloom_filter(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE utf8 = 'ab'")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(0)
        .with_pruned_by_bloom_filter(Some(1))
        .with_matched_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE large_utf8 = 'b'")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(1)
        .with_pruned_by_bloom_filter(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE large_utf8 = 'cd'")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(0)
        .with_pruned_by_bloom_filter(Some(1))
        .with_matched_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn test_bloom_filter_integer_dict() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE int32 = arrow_cast(8, 'Int32')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(1)
        .with_pruned_by_bloom_filter(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE int32 = arrow_cast(7, 'Int32')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(0)
        .with_pruned_by_bloom_filter(Some(1))
        .with_matched_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE int64 = 8")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(1)
        .with_pruned_by_bloom_filter(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE int64 = 7")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(0)
        .with_pruned_by_bloom_filter(Some(1))
        .with_matched_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn test_bloom_filter_unsigned_integer_dict() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE uint32 = arrow_cast(8, 'UInt32')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(1)
        .with_pruned_by_bloom_filter(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE uint32 = arrow_cast(7, 'UInt32')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(0)
        .with_pruned_by_bloom_filter(Some(1))
        .with_matched_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn test_bloom_filter_binary_dict() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE binary = arrow_cast('b', 'Binary')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(1)
        .with_pruned_by_bloom_filter(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE binary = arrow_cast('banana', 'Binary')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(0)
        .with_pruned_by_bloom_filter(Some(1))
        .with_matched_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE large_binary = arrow_cast('d', 'LargeBinary')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(1)
        .with_pruned_by_bloom_filter(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query(
            "SELECT * FROM t WHERE large_binary = arrow_cast('dre', 'LargeBinary')",
        )
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(0)
        .with_pruned_by_bloom_filter(Some(1))
        .with_matched_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn test_bloom_filter_decimal_dict() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE decimal = arrow_cast(8, 'Decimal128(6, 2)')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(1)
        .with_pruned_by_bloom_filter(Some(0))
        .with_matched_by_bloom_filter(Some(1))
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Dictionary)
        .with_query("SELECT * FROM t WHERE decimal = arrow_cast(7, 'Decimal128(6, 2)')")
        .with_expected_errors(Some(0))
        .with_matched_by_stats(Some(1))
        .with_pruned_by_stats(Some(1))
        .with_pruned_files(Some(0))
        .with_expected_rows(0)
        .with_pruned_by_bloom_filter(Some(1))
        .with_matched_by_bloom_filter(Some(0))
        .test_row_group_prune()
        .await;
}

// Helper function to create a batch with a single Int32 column.
fn make_i32_batch(
    name: &str,
    values: Vec<i32>,
) -> datafusion_common::error::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
    let array: ArrayRef = Arc::new(Int32Array::from(values));
    RecordBatch::try_new(schema, vec![array]).map_err(DataFusionError::from)
}

// Helper function to create a batch with two Int32 columns
fn make_two_col_i32_batch(
    name_a: &str,
    name_b: &str,
    values_a: Vec<i32>,
    values_b: Vec<i32>,
) -> datafusion_common::error::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(name_a, DataType::Int32, false),
        Field::new(name_b, DataType::Int32, false),
    ]));
    let array_a: ArrayRef = Arc::new(Int32Array::from(values_a));
    let array_b: ArrayRef = Arc::new(Int32Array::from(values_b));
    RecordBatch::try_new(schema, vec![array_a, array_b]).map_err(DataFusionError::from)
}

#[tokio::test]
async fn test_limit_pruning_basic() -> datafusion_common::error::Result<()> {
    // Scenario: Simple integer column, multiple row groups
    // Query: SELECT c1 FROM  t WHERE c1 = 0 LIMIT 2
    // We expect 2 rows in total.

    // Row Group 0: c1 = [0, -2] -> Partially matched, 1 row
    // Row Group 1: c1 = [1, 2] -> Fully matched, 2 rows
    // Row Group 2: c1 = [3, 4] -> Fully matched, 2 rows
    // Row Group 3: c1 = [5, 6] -> Fully matched, 2 rows
    // Row Group 4: c1 = [-1, -2] -> Not matched

    // If limit = 2, and RG1 is fully matched and has 2 rows, we should
    // only scan RG1 and prune other row groups
    // RG4 is pruned by statistics. RG2 and RG3 are pruned by limit.
    // So 2 row groups are effectively pruned due to limit pruning.

    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, false)]));
    let query = "SELECT c1 FROM t WHERE c1 >= 0 LIMIT 2";

    let batches = vec![
        make_i32_batch("c1", vec![0, -2])?,
        make_i32_batch("c1", vec![0, 0])?,
        make_i32_batch("c1", vec![0, 0])?,
        make_i32_batch("c1", vec![0, 0])?,
        make_i32_batch("c1", vec![-1, -2])?,
    ];

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int) // Assuming Scenario::Int can handle this data
        .with_query(query)
        .with_expected_errors(Some(0))
        .with_expected_rows(2)
        .with_pruned_files(Some(0))
        .with_matched_by_stats(Some(4))
        .with_fully_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(1))
        .with_limit_pruned_row_groups(Some(3))
        .test_row_group_prune_with_custom_data(schema, batches, 2)
        .await;

    Ok(())
}

#[tokio::test]
async fn test_limit_pruning_complex_filter() -> datafusion_common::error::Result<()> {
    // Test Case 1: Complex filter with two columns (a = 1 AND b > 1 AND b < 4)
    // Row Group 0: a=[1,1,1], b=[0,2,3] -> Partially matched, 2 rows match (b=2,3)
    // Row Group 1: a=[1,1,1], b=[2,2,2] -> Fully matched, 3 rows
    // Row Group 2: a=[1,1,1], b=[2,3,3] -> Fully matched, 3 rows
    // Row Group 3: a=[1,1,1], b=[2,2,3] -> Fully matched, 3 rows
    // Row Group 4: a=[2,2,2], b=[2,2,2] -> Not matched (a != 1)
    // Row Group 5: a=[1,1,1], b=[5,6,7] -> Not matched (b >= 4)

    // With LIMIT 5, we need RG1 (3 rows) + RG2 (2 rows from 3) = 5 rows
    // RG4 and RG5 should be pruned by statistics
    // RG3 should be pruned by limit
    // RG0 is partially matched, so it depends on the order

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let query = "SELECT a, b FROM t WHERE a = 1 AND b > 1 AND b < 4 LIMIT 5";

    let batches = vec![
        make_two_col_i32_batch("a", "b", vec![1, 1, 1], vec![0, 2, 3])?,
        make_two_col_i32_batch("a", "b", vec![1, 1, 1], vec![2, 2, 2])?,
        make_two_col_i32_batch("a", "b", vec![1, 1, 1], vec![2, 3, 3])?,
        make_two_col_i32_batch("a", "b", vec![1, 1, 1], vec![2, 2, 3])?,
        make_two_col_i32_batch("a", "b", vec![2, 2, 2], vec![2, 2, 2])?,
        make_two_col_i32_batch("a", "b", vec![1, 1, 1], vec![5, 6, 7])?,
    ];

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int)
        .with_query(query)
        .with_expected_errors(Some(0))
        .with_expected_rows(5)
        .with_pruned_files(Some(0))
        .with_matched_by_stats(Some(4)) // RG0,1,2,3 are matched
        .with_fully_matched_by_stats(Some(3))
        .with_pruned_by_stats(Some(2)) // RG4,5 are pruned
        .with_limit_pruned_row_groups(Some(2)) // RG0, RG3 is pruned by limit
        .test_row_group_prune_with_custom_data(schema, batches, 3)
        .await;

    Ok(())
}

#[tokio::test]
async fn test_limit_pruning_multiple_fully_matched()
-> datafusion_common::error::Result<()> {
    // Test Case 2: Limit requires multiple fully matched row groups
    // Row Group 0: a=[5,5,5,5] -> Fully matched, 4 rows
    // Row Group 1: a=[5,5,5,5] -> Fully matched, 4 rows
    // Row Group 2: a=[5,5,5,5] -> Fully matched, 4 rows
    // Row Group 3: a=[5,5,5,5] -> Fully matched, 4 rows
    // Row Group 4: a=[1,2,3,4] -> Not matched

    // With LIMIT 8, we need RG0 (4 rows) + RG1 (4 rows)  8 rows
    // RG2,3 should be pruned by limit
    // RG4 should be pruned by statistics

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let query = "SELECT a FROM t WHERE a = 5 LIMIT 8";

    let batches = vec![
        make_i32_batch("a", vec![5, 5, 5, 5])?,
        make_i32_batch("a", vec![5, 5, 5, 5])?,
        make_i32_batch("a", vec![5, 5, 5, 5])?,
        make_i32_batch("a", vec![5, 5, 5, 5])?,
        make_i32_batch("a", vec![1, 2, 3, 4])?,
    ];

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int)
        .with_query(query)
        .with_expected_errors(Some(0))
        .with_expected_rows(8)
        .with_pruned_files(Some(0))
        .with_matched_by_stats(Some(4)) // RG0,1,2,3 matched
        .with_fully_matched_by_stats(Some(4))
        .with_pruned_by_stats(Some(1)) // RG4 pruned
        .with_limit_pruned_row_groups(Some(2)) // RG2,3 pruned by limit
        .test_row_group_prune_with_custom_data(schema, batches, 4)
        .await;

    Ok(())
}

#[tokio::test]
async fn test_limit_pruning_no_fully_matched() -> datafusion_common::error::Result<()> {
    // Test Case 3: No fully matched row groups - all are partially matched
    // Row Group 0: a=[1,2,3] -> Partially matched, 1 row (a=2)
    // Row Group 1: a=[2,3,4] -> Partially matched, 1 row (a=2)
    // Row Group 2: a=[2,5,6] -> Partially matched, 1 row (a=2)
    // Row Group 3: a=[2,7,8] -> Partially matched, 1 row (a=2)
    // Row Group 4: a=[9,10,11] -> Not matched

    // With LIMIT 3, we need to scan RG0,1,2 to get 3 matching rows
    // Cannot prune much by limit since all matching RGs are partial
    // RG4 should be pruned by statistics

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let query = "SELECT a FROM t WHERE a = 2 LIMIT 3";

    let batches = vec![
        make_i32_batch("a", vec![1, 2, 3])?,
        make_i32_batch("a", vec![2, 3, 4])?,
        make_i32_batch("a", vec![2, 5, 6])?,
        make_i32_batch("a", vec![2, 7, 8])?,
        make_i32_batch("a", vec![9, 10, 11])?,
    ];

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int)
        .with_query(query)
        .with_expected_errors(Some(0))
        .with_expected_rows(3)
        .with_pruned_files(Some(0))
        .with_matched_by_stats(Some(4)) // RG0,1,2,3 matched
        .with_fully_matched_by_stats(Some(0))
        .with_pruned_by_stats(Some(1)) // RG4 pruned
        .with_limit_pruned_row_groups(Some(0)) // RG3 pruned by limit
        .test_row_group_prune_with_custom_data(schema, batches, 3)
        .await;

    Ok(())
}

#[tokio::test]
async fn test_limit_pruning_exceeds_fully_matched() -> datafusion_common::error::Result<()>
{
    // Test Case 4: Limit exceeds all fully matched rows, need partially matched
    // Row Group 0: a=[10,11,12,12] -> Partially matched, 1 row (a=10)
    // Row Group 1: a=[10,10,10,10] -> Fully matched, 4 rows
    // Row Group 2: a=[10,10,10,10] -> Fully matched, 4 rows
    // Row Group 3: a=[10,13,14,11] -> Partially matched, 1 row (a=10)
    // Row Group 4: a=[20,21,22,22] -> Not matched

    // With LIMIT 10, we need RG1 (4) + RG2 (4) = 8 from fully matched
    // Still need 2 more, so we need to scan partially matched RG0 and RG3
    // All matching row groups should be scanned, only RG4 pruned by statistics

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let query = "SELECT a FROM t WHERE a = 10 LIMIT 10";

    let batches = vec![
        make_i32_batch("a", vec![10, 11, 12, 12])?,
        make_i32_batch("a", vec![10, 10, 10, 10])?,
        make_i32_batch("a", vec![10, 10, 10, 10])?,
        make_i32_batch("a", vec![10, 13, 14, 11])?,
        make_i32_batch("a", vec![20, 21, 22, 22])?,
    ];

    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int)
        .with_query(query)
        .with_expected_errors(Some(0))
        .with_expected_rows(10) // Total: 1 + 4 + 4 + 1 = 10
        .with_pruned_files(Some(0))
        .with_matched_by_stats(Some(4)) // RG0,1,2,3 matched
        .with_fully_matched_by_stats(Some(2))
        .with_pruned_by_stats(Some(1)) // RG4 pruned
        .with_limit_pruned_row_groups(Some(0)) // No limit pruning since we need all RGs
        .test_row_group_prune_with_custom_data(schema, batches, 4)
        .await;
    Ok(())
}
