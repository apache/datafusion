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
use datafusion::prelude::SessionConfig;
use datafusion_common::ScalarValue;
use itertools::Itertools;

use crate::parquet::Unit::RowGroup;
use crate::parquet::{ContextWithParquet, Scenario};
use datafusion_expr::{col, lit};
struct RowGroupPruningTest {
    scenario: Scenario,
    query: String,
    expected_errors: Option<usize>,
    expected_row_group_pruned_by_statistics: Option<usize>,
    expected_row_group_pruned_by_bloom_filter: Option<usize>,
    expected_results: usize,
}
impl RowGroupPruningTest {
    // Start building the test configuration
    fn new() -> Self {
        Self {
            scenario: Scenario::Timestamps, // or another default
            query: String::new(),
            expected_errors: None,
            expected_row_group_pruned_by_statistics: None,
            expected_row_group_pruned_by_bloom_filter: None,
            expected_results: 0,
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

    // Set the expected pruned row groups by statistics
    fn with_pruned_by_stats(mut self, pruned_by_stats: Option<usize>) -> Self {
        self.expected_row_group_pruned_by_statistics = pruned_by_stats;
        self
    }

    // Set the expected pruned row groups by bloom filter
    fn with_pruned_by_bloom_filter(mut self, pruned_by_bf: Option<usize>) -> Self {
        self.expected_row_group_pruned_by_bloom_filter = pruned_by_bf;
        self
    }

    // Set the expected rows for the test
    fn with_expected_rows(mut self, rows: usize) -> Self {
        self.expected_results = rows;
        self
    }

    // Execute the test with the current configuration
    async fn test_row_group_prune(self) {
        let output = ContextWithParquet::new(self.scenario, RowGroup)
            .await
            .query(&self.query)
            .await;

        println!("{}", output.description());
        assert_eq!(output.predicate_evaluation_errors(), self.expected_errors);
        assert_eq!(
            output.row_groups_pruned_statistics(),
            self.expected_row_group_pruned_by_statistics
        );
        assert_eq!(
            output.row_groups_pruned_bloom_filter(),
            self.expected_row_group_pruned_by_bloom_filter
        );
        assert_eq!(
            output.result_rows,
            self.expected_results,
            "{}",
            output.description()
        );
    }
}

#[tokio::test]
async fn prune_timestamps_nanos() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Timestamps)
        .with_query("SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
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
        .with_pruned_by_stats(Some(1))
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
        .with_pruned_by_stats(Some(1))
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
        .with_pruned_by_stats(Some(1))
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
        .with_pruned_by_stats(Some(3))
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
    let date = ScalarValue::Date64(Some(date.timestamp_millis()));

    let output = ContextWithParquet::new(Scenario::Dates, RowGroup)
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
    assert_eq!(output.row_groups_pruned(), Some(3));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_disabled() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Timestamps)
        .with_query("SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(10)
        .test_row_group_prune()
        .await;

    // test without pruning
    let query = "SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')";
    let expected_rows = 10;
    let config = SessionConfig::new().with_parquet_pruning(false);

    let output = ContextWithParquet::with_config(Scenario::Timestamps, RowGroup, config)
        .await
        .query(query)
        .await;
    println!("{}", output.description());

    // This should not prune any
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(
        output.result_rows,
        expected_rows,
        "{}",
        output.description()
    );
}

#[tokio::test]
async fn prune_int32_lt() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where i < 1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(11)
        .test_row_group_prune()
        .await;

    // result of sql "SELECT * FROM t where i < 1" is same as
    // "SELECT * FROM t where -i > -1"
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where -i > -1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(11)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_int32_eq() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where i = 1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}
#[tokio::test]
async fn prune_int32_scalar_fun_and_eq() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where i = 1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_int32_scalar_fun() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where abs(i) = 1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(0))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(3)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_int32_complex_expr() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where i+1 = 1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(0))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_int32_complex_expr_subtract() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where 1-i > 1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(0))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(9)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_f64_lt() {
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Float64)
        .with_query("SELECT * FROM t where f < 1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(11)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Float64)
        .with_query("SELECT * FROM t where -f > -1")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
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
        .with_pruned_by_stats(Some(2))
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
        .with_pruned_by_stats(Some(0))
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
        .with_pruned_by_stats(Some(0))
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
        .with_pruned_by_stats(Some(0))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(9)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_int32_eq_in_list() {
    // result of sql "SELECT * FROM t where in (1)"
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where i in (1)")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(3))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(1)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_int32_eq_in_list_2() {
    // result of sql "SELECT * FROM t where in (1000)", prune all
    // test whether statistics works
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where i in (1000)")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(4))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;
}

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
        .with_pruned_by_stats(Some(0))
        .with_pruned_by_bloom_filter(Some(1))
        .with_expected_rows(0)
        .test_row_group_prune()
        .await;
}

#[tokio::test]
async fn prune_int32_eq_in_list_negated() {
    // result of sql "SELECT * FROM t where not in (1)" prune nothing
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Int32)
        .with_query("SELECT * FROM t where i not in (1)")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(0))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(19)
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
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(6)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col < cast(4.55 as decimal(20,2))")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(8)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col < 4")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(6)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col < cast(4.55 as decimal(20,2))")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
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
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col = 4.00")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;

    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col = 4")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col = 4.00")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
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
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(5)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col in (4.00,3.00,11.2345,1)")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(6)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::Decimal)
        .with_query("SELECT * FROM t where decimal_col in (4,3,2,123456789123)")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(5)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::DecimalLargePrecision)
        .with_query("SELECT * FROM t where decimal_col in (4.00,3.00,11.2345,1)")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(6)
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
        .with_pruned_by_stats(Some(1))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(7)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::PeriodsInColumnNames)
        .with_query(  "SELECT \"name\", \"service.name\" FROM t WHERE \"name\" != 'HTTP GET / DISPATCH'")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(5)
        .test_row_group_prune()
        .await;
    RowGroupPruningTest::new()
        .with_scenario(Scenario::PeriodsInColumnNames)
        .with_query(  "SELECT \"name\", \"service.name\" FROM t WHERE \"service.name\" = 'frontend' AND \"name\" != 'HTTP GET / DISPATCH'")
        .with_expected_errors(Some(0))
        .with_pruned_by_stats(Some(2))
        .with_pruned_by_bloom_filter(Some(0))
        .with_expected_rows(2)
        .test_row_group_prune()
        .await;
}
