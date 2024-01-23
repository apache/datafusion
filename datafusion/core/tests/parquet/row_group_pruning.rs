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

async fn test_row_group_prune(
    case_data_type: Scenario,
    sql: &str,
    expected_errors: Option<usize>,
    expected_row_group_pruned_by_statistics: Option<usize>,
    expected_row_group_pruned_by_bloom_filter: Option<usize>,
    expected_results: usize,
) {
    let output = ContextWithParquet::new(case_data_type, RowGroup)
        .await
        .query(sql)
        .await;

    println!("{}", output.description());
    assert_eq!(output.predicate_evaluation_errors(), expected_errors);
    assert_eq!(
        output.row_groups_pruned_statistics(),
        expected_row_group_pruned_by_statistics
    );
    assert_eq!(
        output.row_groups_pruned_bloom_filter(),
        expected_row_group_pruned_by_bloom_filter
    );
    assert_eq!(
        output.result_rows,
        expected_results,
        "{}",
        output.description()
    );
}

/// check row group pruning by bloom filter and statistics independently
async fn test_prune_verbose(
    case_data_type: Scenario,
    sql: &str,
    expected_errors: Option<usize>,
    expected_row_group_pruned_sbbf: Option<usize>,
    expected_row_group_pruned_statistics: Option<usize>,
    expected_results: usize,
) {
    let output = ContextWithParquet::new(case_data_type, RowGroup)
        .await
        .query(sql)
        .await;

    println!("{}", output.description());
    assert_eq!(output.predicate_evaluation_errors(), expected_errors);
    assert_eq!(
        output.row_groups_pruned_bloom_filter(),
        expected_row_group_pruned_sbbf
    );
    assert_eq!(
        output.row_groups_pruned_statistics(),
        expected_row_group_pruned_statistics
    );
    assert_eq!(
        output.result_rows,
        expected_results,
        "{}",
        output.description()
    );
}

#[tokio::test]
async fn prune_timestamps_nanos() {
    test_row_group_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')",
        Some(0),
        Some(1),
        Some(0),
        10,
    )
    .await;
}

#[tokio::test]
async fn prune_timestamps_micros() {
    test_row_group_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where micros < to_timestamp_micros('2020-01-02 01:01:11Z')",
        Some(0),
        Some(1),
        Some(0),
        10,
    )
    .await;
}

#[tokio::test]
async fn prune_timestamps_millis() {
    test_row_group_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where millis < to_timestamp_millis('2020-01-02 01:01:11Z')",
        Some(0),
        Some(1),
        Some(0),
        10,
    )
    .await;
}

#[tokio::test]
async fn prune_timestamps_seconds() {
    test_row_group_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where seconds < to_timestamp_seconds('2020-01-02 01:01:11Z')",
        Some(0),
        Some(1),
        Some(0),
        10,
    )
    .await;
}

#[tokio::test]
async fn prune_date32() {
    test_row_group_prune(
        Scenario::Dates,
        "SELECT * FROM t where date32 < cast('2020-01-02' as date)",
        Some(0),
        Some(3),
        Some(0),
        1,
    )
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
    test_row_group_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')",
        Some(0),
        Some(1),
        Some(0),
        10,
    )
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
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where i < 1",
        Some(0),
        Some(1),
        Some(0),
        11,
    )
    .await;
    // result of sql "SELECT * FROM t where i < 1" is same as
    // "SELECT * FROM t where -i > -1"
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where -i > -1",
        Some(0),
        Some(1),
        Some(0),
        11,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_eq() {
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where i = 1",
        Some(0),
        Some(3),
        Some(0),
        1,
    )
    .await;
}
#[tokio::test]
async fn prune_int32_scalar_fun_and_eq() {
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where abs(i) = 1  and i = 1",
        Some(0),
        Some(3),
        Some(0),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_scalar_fun() {
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where abs(i) = 1",
        Some(0),
        Some(0),
        Some(0),
        3,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_complex_expr() {
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where i+1 = 1",
        Some(0),
        Some(0),
        Some(0),
        2,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_complex_expr_subtract() {
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where 1-i > 1",
        Some(0),
        Some(0),
        Some(0),
        9,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_lt() {
    test_row_group_prune(
        Scenario::Float64,
        "SELECT * FROM t where f < 1",
        Some(0),
        Some(1),
        Some(0),
        11,
    )
    .await;
    test_row_group_prune(
        Scenario::Float64,
        "SELECT * FROM t where -f > -1",
        Some(0),
        Some(1),
        Some(0),
        11,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_scalar_fun_and_gt() {
    // result of sql "SELECT * FROM t where abs(f - 1) <= 0.000001  and f >= 0.1"
    // only use "f >= 0" to prune
    test_row_group_prune(
        Scenario::Float64,
        "SELECT * FROM t where abs(f - 1) <= 0.000001  and f >= 0.1",
        Some(0),
        Some(2),
        Some(0),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_scalar_fun() {
    // result of sql "SELECT * FROM t where abs(f-1) <= 0.000001" is not supported
    test_row_group_prune(
        Scenario::Float64,
        "SELECT * FROM t where abs(f-1) <= 0.000001",
        Some(0),
        Some(0),
        Some(0),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_complex_expr() {
    // result of sql "SELECT * FROM t where f+1 > 1.1"" is not supported
    test_row_group_prune(
        Scenario::Float64,
        "SELECT * FROM t where f+1 > 1.1",
        Some(0),
        Some(0),
        Some(0),
        9,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_complex_expr_subtract() {
    // result of sql "SELECT * FROM t where 1-f > 1" is not supported
    test_row_group_prune(
        Scenario::Float64,
        "SELECT * FROM t where 1-f > 1",
        Some(0),
        Some(0),
        Some(0),
        9,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_eq_in_list() {
    // result of sql "SELECT * FROM t where in (1)"
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where i in (1)",
        Some(0),
        Some(3),
        Some(0),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_eq_in_list_2() {
    // result of sql "SELECT * FROM t where in (1000)", prune all
    // test whether statistics works
    test_prune_verbose(
        Scenario::Int32,
        "SELECT * FROM t where i in (1000)",
        Some(0),
        Some(0),
        Some(4),
        0,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_eq_large_in_list() {
    // result of sql "SELECT * FROM t where i in (2050...2582)", prune all
    // test whether sbbf works
    test_prune_verbose(
        Scenario::Int32Range,
        format!(
            "SELECT * FROM t where i in ({})",
            (200050..200082).join(",")
        )
        .as_str(),
        Some(0),
        Some(1),
        // we don't support pruning by statistics for in_list with more than 20 elements currently
        Some(0),
        0,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_eq_in_list_negated() {
    // result of sql "SELECT * FROM t where not in (1)" prune nothing
    test_row_group_prune(
        Scenario::Int32,
        "SELECT * FROM t where i not in (1)",
        Some(0),
        Some(0),
        Some(0),
        19,
    )
    .await;
}

#[tokio::test]
async fn prune_decimal_lt() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test_row_group_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col < 4",
        Some(0),
        Some(1),
        Some(0),
        6,
    )
    .await;
    // compare with the casted decimal value
    test_row_group_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col < cast(4.55 as decimal(20,2))",
        Some(0),
        Some(1),
        Some(0),
        8,
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test_row_group_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col < 4",
        Some(0),
        Some(1),
        Some(0),
        6,
    )
    .await;
    // compare with the casted decimal value
    test_row_group_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col < cast(4.55 as decimal(20,2))",
        Some(0),
        Some(1),
        Some(0),
        8,
    )
    .await;
}

#[tokio::test]
async fn prune_decimal_eq() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test_row_group_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col = 4",
        Some(0),
        Some(1),
        Some(0),
        2,
    )
    .await;
    test_row_group_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col = 4.00",
        Some(0),
        Some(1),
        Some(0),
        2,
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test_row_group_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col = 4",
        Some(0),
        Some(1),
        Some(0),
        2,
    )
    .await;
    test_row_group_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col = 4.00",
        Some(0),
        Some(1),
        Some(0),
        2,
    )
    .await;
}

#[tokio::test]
async fn prune_decimal_in_list() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test_row_group_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col in (4,3,2,123456789123)",
        Some(0),
        Some(1),
        Some(0),
        5,
    )
    .await;
    test_row_group_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col in (4.00,3.00,11.2345,1)",
        Some(0),
        Some(1),
        Some(0),
        6,
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test_row_group_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col in (4,3,2,123456789123)",
        Some(0),
        Some(1),
        Some(0),
        5,
    )
    .await;
    test_row_group_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col in (4.00,3.00,11.2345,1)",
        Some(0),
        Some(1),
        Some(0),
        6,
    )
    .await;
}

#[tokio::test]
async fn prune_periods_in_column_names() {
    // There are three row groups for "service.name", each with 5 rows = 15 rows total
    // name = "HTTP GET / DISPATCH", service.name = ['frontend', 'frontend'],
    // name = "HTTP PUT / DISPATCH", service.name = ['backend',  'frontend'],
    // name = "HTTP GET / DISPATCH", service.name = ['backend',  'backend' ],
    test_row_group_prune(
        Scenario::PeriodsInColumnNames,
        // use double quotes to use column named "service.name"
        "SELECT \"name\", \"service.name\" FROM t WHERE \"service.name\" = 'frontend'",
        Some(0),
        Some(1), // prune out last row group
        Some(0),
        7,
    )
    .await;
    test_row_group_prune(
        Scenario::PeriodsInColumnNames,
        "SELECT \"name\", \"service.name\" FROM t WHERE \"name\" != 'HTTP GET / DISPATCH'",
        Some(0),
        Some(2), // prune out first and last row group
        Some(0),
        5,
    )
    .await;
    test_row_group_prune(
        Scenario::PeriodsInColumnNames,
        "SELECT \"name\", \"service.name\" FROM t WHERE \"service.name\" = 'frontend' AND \"name\" != 'HTTP GET / DISPATCH'",
        Some(0),
        Some(2), // prune out middle and last row group
        Some(0),
        2,
    )
    .await;
}
