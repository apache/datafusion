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

use super::*;
use datafusion::scalar::ScalarValue;
use datafusion::test_util::scan_empty;

#[tokio::test]
async fn csv_query_avg_multi_batch() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.unwrap();
    let batch = &results[0];
    let column = batch.column(0);
    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    let actual = array.value(0);
    let expected = 0.5089725;
    // Due to float number's accuracy, different batch size will lead to different
    // answers.
    assert!((expected - actual).abs() < 0.01);
    Ok(())
}

#[tokio::test]
async fn csv_query_avg() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.5089725099127211"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_covariance_1() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT covar_pop(c2, c12) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["-0.07916932235380847"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_covariance_2() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT covar(c2, c12) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["-0.07996901247859442"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_correlation() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT corr(c2, c12) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["-0.19064544190576607"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_1() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT var_pop(c2) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["1.8675"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_2() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT var_pop(c6) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["26156334342021890000000000000000000000"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_3() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT var_pop(c12) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.09234223721582163"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_4() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT var(c2) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["1.8863636363636365"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_5() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT var_samp(c2) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["1.8863636363636365"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_1() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT stddev_pop(c2) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["1.3665650368716449"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_2() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT stddev_pop(c6) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["5114326382039172000"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_3() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT stddev_pop(c12) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.30387865541334363"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_4() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT stddev(c12) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.3054095399405338"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_5() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT stddev_samp(c12) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.3054095399405338"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_6() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "select stddev(sq.column1) from (values (1.1), (2.0), (3.0)) as sq";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.9504384952922168"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_median_1() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT approx_median(c2) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["3"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_median_2() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT approx_median(c6) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["1146409980542786560"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_median_3() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT approx_median(c12) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["0.5550065410522981"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_external_table_count() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "SELECT COUNT(c12) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| COUNT(aggregate_test_100.c12) |",
        "+-------------------------------+",
        "| 100                           |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_external_table_sum() {
    let ctx = SessionContext::new();
    // cast smallint and int to bigint to avoid overflow during calculation
    register_aggregate_csv_by_sql(&ctx).await;
    let sql =
        "SELECT SUM(CAST(c7 AS BIGINT)), SUM(CAST(c8 AS BIGINT)) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------------------------+-------------------------------------------+",
        "| SUM(CAST(aggregate_test_100.c7 AS Int64)) | SUM(CAST(aggregate_test_100.c8 AS Int64)) |",
        "+-------------------------------------------+-------------------------------------------+",
        "| 13060                                     | 3017641                                   |",
        "+-------------------------------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_count() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT count(c12) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| COUNT(aggregate_test_100.c12) |",
        "+-------------------------------+",
        "| 100                           |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_count_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT count(distinct c2) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------------------+",
        "| COUNT(DISTINCT aggregate_test_100.c2) |",
        "+---------------------------------------+",
        "| 5                                     |",
        "+---------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_count_distinct_expr() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT count(distinct c2 % 2) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------------+",
        "| COUNT(DISTINCT aggregate_test_100.c2 % Int64(2)) |",
        "+--------------------------------------------------+",
        "| 2                                                |",
        "+--------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_count_star() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "SELECT COUNT(*) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_count_one() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_approx_count() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT approx_distinct(c9) count_c9, approx_distinct(cast(c9 as varchar)) count_c9_str FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+--------------+",
        "| count_c9 | count_c9_str |",
        "+----------+--------------+",
        "| 100      | 99           |",
        "+----------+--------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

// This test executes the APPROX_PERCENTILE_CONT aggregation against the test
// data, asserting the estimated quantiles are ±5% their actual values.
//
// Actual quantiles calculated with:
//
// ```r
// read_csv("./testing/data/csv/aggregate_test_100.csv") |>
//     select_if(is.numeric) |>
//     summarise_all(~ quantile(., c(0.1, 0.5, 0.9)))
// ```
//
// Giving:
//
// ```text
//      c2    c3      c4           c5       c6    c7     c8          c9     c10   c11    c12
//   <dbl> <dbl>   <dbl>        <dbl>    <dbl> <dbl>  <dbl>       <dbl>   <dbl> <dbl>  <dbl>
// 1     1 -95.3 -22925. -1882606710  -7.25e18  18.9  2671.  472608672. 1.83e18 0.109 0.0714
// 2     3  15.5   4599    377164262   1.13e18 134.  30634  2365817608. 9.30e18 0.491 0.551
// 3     5 102.   25334.  1991374996.  7.37e18 231   57518. 3776538487. 1.61e19 0.834 0.946
// ```
//
// Column `c12` is omitted due to a large relative error (~10%) due to the small
// float values.
#[tokio::test]
async fn csv_query_approx_percentile_cont() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;

    // Generate an assertion that the estimated $percentile value for $column is
    // within 5% of the $actual percentile value.
    macro_rules! percentile_test {
        ($ctx:ident, column=$column:literal, percentile=$percentile:literal, actual=$actual:literal) => {
            let sql = format!("SELECT (ABS(1 - CAST(approx_percentile_cont({}, {}) AS DOUBLE) / {}) < 0.05) AS q FROM aggregate_test_100", $column, $percentile, $actual);
            let actual = execute_to_batches(&ctx, &sql).await;
            let want = [
                "+------+",
                "| q    |",
                "+------+",
                "| true |",
                "+------+"
            ];
            assert_batches_eq!(want, &actual);
        };
    }

    percentile_test!(ctx, column = "c2", percentile = 0.1, actual = 1.0);
    percentile_test!(ctx, column = "c2", percentile = 0.5, actual = 3.0);
    percentile_test!(ctx, column = "c2", percentile = 0.9, actual = 5.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c3", percentile = 0.1, actual = -95.3);
    percentile_test!(ctx, column = "c3", percentile = 0.5, actual = 15.5);
    percentile_test!(ctx, column = "c3", percentile = 0.9, actual = 102.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c4", percentile = 0.1, actual = -22925.0);
    percentile_test!(ctx, column = "c4", percentile = 0.5, actual = 4599.0);
    percentile_test!(ctx, column = "c4", percentile = 0.9, actual = 25334.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c5", percentile = 0.1, actual = -1882606710.0);
    percentile_test!(ctx, column = "c5", percentile = 0.5, actual = 377164262.0);
    percentile_test!(ctx, column = "c5", percentile = 0.9, actual = 1991374996.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c6", percentile = 0.1, actual = -7.25e18);
    percentile_test!(ctx, column = "c6", percentile = 0.5, actual = 1.13e18);
    percentile_test!(ctx, column = "c6", percentile = 0.9, actual = 7.37e18);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c7", percentile = 0.1, actual = 18.9);
    percentile_test!(ctx, column = "c7", percentile = 0.5, actual = 134.0);
    percentile_test!(ctx, column = "c7", percentile = 0.9, actual = 231.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c8", percentile = 0.1, actual = 2671.0);
    percentile_test!(ctx, column = "c8", percentile = 0.5, actual = 30634.0);
    percentile_test!(ctx, column = "c8", percentile = 0.9, actual = 57518.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c9", percentile = 0.1, actual = 472608672.0);
    percentile_test!(ctx, column = "c9", percentile = 0.5, actual = 2365817608.0);
    percentile_test!(ctx, column = "c9", percentile = 0.9, actual = 3776538487.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c10", percentile = 0.1, actual = 1.83e18);
    percentile_test!(ctx, column = "c10", percentile = 0.5, actual = 9.30e18);
    percentile_test!(ctx, column = "c10", percentile = 0.9, actual = 1.61e19);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c11", percentile = 0.1, actual = 0.109);
    percentile_test!(ctx, column = "c11", percentile = 0.5, actual = 0.491);
    percentile_test!(ctx, column = "c11", percentile = 0.9, actual = 0.834);

    Ok(())
}

#[tokio::test]
async fn csv_query_approx_percentile_cont_with_weight() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;

    // compare approx_percentile_cont and approx_percentile_cont_with_weight
    let sql = "SELECT c1, approx_percentile_cont(c3, 0.95) AS c3_p95 FROM aggregate_test_100 GROUP BY 1 ORDER BY 1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+--------+",
        "| c1 | c3_p95 |",
        "+----+--------+",
        "| a  | 73     |",
        "| b  | 68     |",
        "| c  | 122    |",
        "| d  | 124    |",
        "| e  | 115    |",
        "+----+--------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT c1, approx_percentile_cont_with_weight(c3, 1, 0.95) AS c3_p95 FROM aggregate_test_100 GROUP BY 1 ORDER BY 1";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT c1, approx_percentile_cont_with_weight(c3, c2, 0.95) AS c3_p95 FROM aggregate_test_100 GROUP BY 1 ORDER BY 1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+--------+",
        "| c1 | c3_p95 |",
        "+----+--------+",
        "| a  | 74     |",
        "| b  | 68     |",
        "| c  | 123    |",
        "| d  | 124    |",
        "| e  | 115    |",
        "+----+--------+",
    ];
    assert_batches_eq!(expected, &actual);

    let results = plan_and_collect(
        &ctx,
        "SELECT approx_percentile_cont_with_weight(c1, c2, 0.95) FROM aggregate_test_100",
    )
    .await
    .unwrap_err();
    assert_eq!(results.to_string(), "Error during planning: The function ApproxPercentileContWithWeight does not support inputs of type Utf8.");

    let results = plan_and_collect(
        &ctx,
        "SELECT approx_percentile_cont_with_weight(c3, c1, 0.95) FROM aggregate_test_100",
    )
    .await
    .unwrap_err();
    assert_eq!(results.to_string(), "Error during planning: The weight argument for ApproxPercentileContWithWeight does not support inputs of type Utf8.");

    let results = plan_and_collect(
        &ctx,
        "SELECT approx_percentile_cont_with_weight(c3, c2, c1) FROM aggregate_test_100",
    )
    .await
    .unwrap_err();
    assert_eq!(results.to_string(), "Error during planning: The percentile argument for ApproxPercentileContWithWeight must be Float64, not Utf8.");

    Ok(())
}

#[tokio::test]
async fn csv_query_sum_crossjoin() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "SELECT a.c1, b.c1, SUM(a.c2) FROM aggregate_test_100 as a CROSS JOIN aggregate_test_100 as b GROUP BY a.c1, b.c1 ORDER BY a.c1, b.c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+-----------+",
        "| c1 | c1 | SUM(a.c2) |",
        "+----+----+-----------+",
        "| a  | a  | 1260      |",
        "| a  | b  | 1140      |",
        "| a  | c  | 1260      |",
        "| a  | d  | 1080      |",
        "| a  | e  | 1260      |",
        "| b  | a  | 1302      |",
        "| b  | b  | 1178      |",
        "| b  | c  | 1302      |",
        "| b  | d  | 1116      |",
        "| b  | e  | 1302      |",
        "| c  | a  | 1176      |",
        "| c  | b  | 1064      |",
        "| c  | c  | 1176      |",
        "| c  | d  | 1008      |",
        "| c  | e  | 1176      |",
        "| d  | a  | 924       |",
        "| d  | b  | 836       |",
        "| d  | c  | 924       |",
        "| d  | d  | 792       |",
        "| d  | e  | 924       |",
        "| e  | a  | 1323      |",
        "| e  | b  | 1197      |",
        "| e  | c  | 1323      |",
        "| e  | d  | 1134      |",
        "| e  | e  | 1323      |",
        "+----+----+-----------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn query_count_without_from() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT count(1 + 1)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| COUNT(Int64(1) + Int64(1)) |",
        "+----------------------------+",
        "| 1                          |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 2) test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------------------------------------------------------------+",
        "| ARRAYAGG(test.c13)                                               |",
        "+------------------------------------------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm, 0keZ5G8BffGwgF2RwQD59TFzMStxCB] |",
        "+------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_empty() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 LIMIT 0) test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------+",
        "| ARRAYAGG(test.c13) |",
        "+--------------------+",
        "| []                 |",
        "+--------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_one() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 1) test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------------+",
        "| ARRAYAGG(test.c13)               |",
        "+----------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm] |",
        "+----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_with_overflow() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "select c2, sum(c3) sum_c3, avg(c3) avg_c3, max(c3) max_c3, min(c3) min_c3, count(c3) count_c3 from aggregate_test_100 group by c2 order by c2";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+--------+---------------------+--------+--------+----------+",
        "| c2 | sum_c3 | avg_c3              | max_c3 | min_c3 | count_c3 |",
        "+----+--------+---------------------+--------+--------+----------+",
        "| 1  | 367    | 16.681818181818183  | 125    | -99    | 22       |",
        "| 2  | 184    | 8.363636363636363   | 122    | -117   | 22       |",
        "| 3  | 395    | 20.789473684210527  | 123    | -101   | 19       |",
        "| 4  | 29     | 1.2608695652173914  | 123    | -117   | 23       |",
        "| 5  | -194   | -13.857142857142858 | 118    | -101   | 14       |",
        "+----+--------+---------------------+--------+--------+----------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT array_agg(distinct c2) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;

    // The results for this query should be something like the following:
    //    +------------------------------------------+
    //    | ARRAYAGG(DISTINCT aggregate_test_100.c2) |
    //    +------------------------------------------+
    //    | [4, 2, 3, 5, 1]                          |
    //    +------------------------------------------+
    // Since ARRAY_AGG(DISTINCT) ordering is nondeterministic, check the schema and contents.
    assert_eq!(
        *actual[0].schema(),
        Schema::new(vec![Field::new(
            "ARRAYAGG(DISTINCT aggregate_test_100.c2)",
            DataType::List(Box::new(Field::new("item", DataType::UInt32, true))),
            false
        ),])
    );

    // We should have 1 row containing a list
    let column = actual[0].column(0);
    assert_eq!(column.len(), 1);

    if let ScalarValue::List(Some(mut v), _) = ScalarValue::try_from_array(column, 0)? {
        // workaround lack of Ord of ScalarValue
        let cmp = |a: &ScalarValue, b: &ScalarValue| {
            a.partial_cmp(b).expect("Can compare ScalarValues")
        };
        v.sort_by(cmp);
        assert_eq!(
            *v,
            vec![
                ScalarValue::UInt32(Some(1)),
                ScalarValue::UInt32(Some(2)),
                ScalarValue::UInt32(Some(3)),
                ScalarValue::UInt32(Some(4)),
                ScalarValue::UInt32(Some(5))
            ]
        );
    } else {
        unreachable!();
    }

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_sum() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = plan_and_collect(
        &ctx,
        "SELECT sum(nanos), sum(micros), sum(millis), sum(secs) FROM t",
    )
    .await
    .unwrap_err();

    assert_eq!(results.to_string(), "Error during planning: The function Sum does not support inputs of type Timestamp(Nanosecond, None).");

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_count() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT count(nanos), count(micros), count(millis), count(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+----------------+-----------------+-----------------+---------------+",
        "| COUNT(t.nanos) | COUNT(t.micros) | COUNT(t.millis) | COUNT(t.secs) |",
        "+----------------+-----------------+-----------------+---------------+",
        "| 3              | 3               | 3               | 3             |",
        "+----------------+-----------------+-----------------+---------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_min() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT min(nanos), min(micros), min(millis), min(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+----------------------------+----------------------------+-------------------------+---------------------+",
        "| MIN(t.nanos)               | MIN(t.micros)              | MIN(t.millis)           | MIN(t.secs)         |",
        "+----------------------------+----------------------------+-------------------------+---------------------+",
        "| 2011-12-13 11:13:10.123450 | 2011-12-13 11:13:10.123450 | 2011-12-13 11:13:10.123 | 2011-12-13 11:13:10 |",
        "+----------------------------+----------------------------+-------------------------+---------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_max() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT max(nanos), max(micros), max(millis), max(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+-------------------------+-------------------------+-------------------------+---------------------+",
        "| MAX(t.nanos)            | MAX(t.micros)           | MAX(t.millis)           | MAX(t.secs)         |",
        "+-------------------------+-------------------------+-------------------------+---------------------+",
        "| 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10 |",
        "+-------------------------+-------------------------+-------------------------+---------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_avg() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = plan_and_collect(
        &ctx,
        "SELECT avg(nanos), avg(micros), avg(millis), avg(secs) FROM t",
    )
    .await
    .unwrap_err();

    assert_eq!(results.to_string(), "Error during planning: The function Avg does not support inputs of type Timestamp(Nanosecond, None).");
    Ok(())
}

#[tokio::test]
async fn aggregate_decimal_min() -> Result<()> {
    let ctx = SessionContext::new();
    // the data type of c1 is decimal(10,3)
    ctx.register_table("d_table", table_with_decimal()).unwrap();
    let result = plan_and_collect(&ctx, "select min(c1) from d_table")
        .await
        .unwrap();
    let expected = vec![
        "+-----------------+",
        "| MIN(d_table.c1) |",
        "+-----------------+",
        "| -100.009        |",
        "+-----------------+",
    ];
    assert_eq!(
        &DataType::Decimal(10, 3),
        result[0].schema().field(0).data_type()
    );
    assert_batches_sorted_eq!(expected, &result);
    Ok(())
}

#[tokio::test]
async fn aggregate_decimal_max() -> Result<()> {
    let ctx = SessionContext::new();
    // the data type of c1 is decimal(10,3)
    ctx.register_table("d_table", table_with_decimal()).unwrap();

    let result = plan_and_collect(&ctx, "select max(c1) from d_table")
        .await
        .unwrap();
    let expected = vec![
        "+-----------------+",
        "| MAX(d_table.c1) |",
        "+-----------------+",
        "| 110.009         |",
        "+-----------------+",
    ];
    assert_eq!(
        &DataType::Decimal(10, 3),
        result[0].schema().field(0).data_type()
    );
    assert_batches_sorted_eq!(expected, &result);
    Ok(())
}

#[tokio::test]
async fn aggregate_decimal_sum() -> Result<()> {
    let ctx = SessionContext::new();
    // the data type of c1 is decimal(10,3)
    ctx.register_table("d_table", table_with_decimal()).unwrap();
    let result = plan_and_collect(&ctx, "select sum(c1) from d_table")
        .await
        .unwrap();
    let expected = vec![
        "+-----------------+",
        "| SUM(d_table.c1) |",
        "+-----------------+",
        "| 100.000         |",
        "+-----------------+",
    ];
    assert_eq!(
        &DataType::Decimal(20, 3),
        result[0].schema().field(0).data_type()
    );
    assert_batches_sorted_eq!(expected, &result);
    Ok(())
}

#[tokio::test]
async fn aggregate_decimal_avg() -> Result<()> {
    let ctx = SessionContext::new();
    // the data type of c1 is decimal(10,3)
    ctx.register_table("d_table", table_with_decimal()).unwrap();
    let result = plan_and_collect(&ctx, "select avg(c1) from d_table")
        .await
        .unwrap();
    let expected = vec![
        "+-----------------+",
        "| AVG(d_table.c1) |",
        "+-----------------+",
        "| 5.0000000       |",
        "+-----------------+",
    ];
    assert_eq!(
        &DataType::Decimal(14, 7),
        result[0].schema().field(0).data_type()
    );
    assert_batches_sorted_eq!(expected, &result);
    Ok(())
}

#[tokio::test]
async fn aggregate() -> Result<()> {
    let results = execute_with_partition("SELECT SUM(c1), SUM(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| SUM(test.c1) | SUM(test.c2) |",
        "+--------------+--------------+",
        "| 60           | 220          |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_empty() -> Result<()> {
    // The predicate on this query purposely generates no results
    let results =
        execute_with_partition("SELECT SUM(c1), SUM(c2) FROM test where c1 > 100000", 4)
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| SUM(test.c1) | SUM(test.c2) |",
        "+--------------+--------------+",
        "|              |              |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_avg() -> Result<()> {
    let results = execute_with_partition("SELECT AVG(c1), AVG(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| AVG(test.c1) | AVG(test.c2) |",
        "+--------------+--------------+",
        "| 1.5          | 5.5          |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_max() -> Result<()> {
    let results = execute_with_partition("SELECT MAX(c1), MAX(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| MAX(test.c1) | MAX(test.c2) |",
        "+--------------+--------------+",
        "| 3            | 10           |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_min() -> Result<()> {
    let results = execute_with_partition("SELECT MIN(c1), MIN(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| MIN(test.c1) | MIN(test.c2) |",
        "+--------------+--------------+",
        "| 0            | 1            |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, SUM(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | SUM(test.c2) |",
        "+----+--------------+",
        "| 0  | 55           |",
        "| 1  | 55           |",
        "| 2  | 55           |",
        "| 3  | 55           |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_avg() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, AVG(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | AVG(test.c2) |",
        "+----+--------------+",
        "| 0  | 5.5          |",
        "| 1  | 5.5          |",
        "| 2  | 5.5          |",
        "| 3  | 5.5          |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_empty() -> Result<()> {
    let results = execute_with_partition(
        "SELECT c1, AVG(c2) FROM test WHERE c1 = 123 GROUP BY c1",
        4,
    )
    .await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | AVG(test.c2) |",
        "+----+--------------+",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_max() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, MAX(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | MAX(test.c2) |",
        "+----+--------------+",
        "| 0  | 10           |",
        "| 1  | 10           |",
        "| 2  | 10           |",
        "| 3  | 10           |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_min() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, MIN(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | MIN(test.c2) |",
        "+----+--------------+",
        "| 0  | 1            |",
        "| 1  | 1            |",
        "| 2  | 1            |",
        "| 3  | 1            |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_avg_add() -> Result<()> {
    let results = execute_with_partition(
        "SELECT AVG(c1), AVG(c1) + 1, AVG(c1) + 2, 1 + AVG(c1) FROM test",
        4,
    )
    .await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+----------------------------+----------------------------+----------------------------+",
        "| AVG(test.c1) | AVG(test.c1) Plus Int64(1) | AVG(test.c1) Plus Int64(2) | Int64(1) Plus AVG(test.c1) |",
        "+--------------+----------------------------+----------------------------+----------------------------+",
        "| 1.5          | 2.5                        | 3.5                        | 2.5                        |",
        "+--------------+----------------------------+----------------------------+----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn case_sensitive_identifiers_aggregates() {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let expected = vec![
        "+----------+",
        "| MAX(t.i) |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];

    let results = plan_and_collect(&ctx, "SELECT max(i) FROM t")
        .await
        .unwrap();

    assert_batches_sorted_eq!(expected, &results);

    let results = plan_and_collect(&ctx, "SELECT MAX(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);

    // Using double quotes allows specifying the function name with capitalization
    let err = plan_and_collect(&ctx, "SELECT \"MAX\"(i) FROM t")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Error during planning: Invalid function 'MAX'"
    );

    let results = plan_and_collect(&ctx, "SELECT \"max\"(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);
}

#[tokio::test]
async fn count_basic() -> Result<()> {
    let results =
        execute_with_partition("SELECT COUNT(c1), COUNT(c2) FROM test", 1).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----------------+----------------+",
        "| COUNT(test.c1) | COUNT(test.c2) |",
        "+----------------+----------------+",
        "| 10             | 10             |",
        "+----------------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn count_partitioned() -> Result<()> {
    let results =
        execute_with_partition("SELECT COUNT(c1), COUNT(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----------------+----------------+",
        "| COUNT(test.c1) | COUNT(test.c2) |",
        "+----------------+----------------+",
        "| 40             | 40             |",
        "+----------------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn count_aggregated() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, COUNT(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+----------------+",
        "| c1 | COUNT(test.c2) |",
        "+----+----------------+",
        "| 0  | 10             |",
        "| 1  | 10             |",
        "| 2  | 10             |",
        "| 3  | 10             |",
        "+----+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn simple_avg() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice(&[1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice(&[4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let result = plan_and_collect(&ctx, "SELECT AVG(a) FROM t").await?;

    let batch = &result[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(1, batch.num_rows());

    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("failed to cast version");
    assert_eq!(values.len(), 1);
    // avg(1,2,3,4,5) = 3.0
    assert_eq!(values.value(0), 3.0_f64);
    Ok(())
}

#[tokio::test]
async fn query_sum_distinct() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, true),
        Field::new("c2", DataType::Int64, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                Some(0),
                Some(1),
                None,
                Some(3),
                Some(3),
            ])),
            Arc::new(Int64Array::from(vec![
                None,
                Some(1),
                Some(1),
                Some(2),
                Some(2),
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;
    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    // 2 different aggregate functions: avg and sum(distinct)
    let sql = "SELECT AVG(c1), SUM(DISTINCT c2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------+-----------------------+",
        "| AVG(test.c1) | SUM(DISTINCT test.c2) |",
        "+--------------+-----------------------+",
        "| 1.75         | 3                     |",
        "+--------------+-----------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // 2 sum(distinct) functions
    let sql = "SELECT SUM(DISTINCT c1), SUM(DISTINCT c2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------+-----------------------+",
        "| SUM(DISTINCT test.c1) | SUM(DISTINCT test.c2) |",
        "+-----------------------+-----------------------+",
        "| 4                     | 3                     |",
        "+-----------------------+-----------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_count_distinct() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
            Some(3),
        ]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COUNT(DISTINCT c1) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------+",
        "| COUNT(DISTINCT test.c1) |",
        "+-------------------------+",
        "| 3                       |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

async fn run_count_distinct_integers_aggregated_scenario(
    partitions: Vec<Vec<(&str, u64)>>,
) -> Result<Vec<RecordBatch>> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_group", DataType::Utf8, false),
        Field::new("c_int8", DataType::Int8, false),
        Field::new("c_int16", DataType::Int16, false),
        Field::new("c_int32", DataType::Int32, false),
        Field::new("c_int64", DataType::Int64, false),
        Field::new("c_uint8", DataType::UInt8, false),
        Field::new("c_uint16", DataType::UInt16, false),
        Field::new("c_uint32", DataType::UInt32, false),
        Field::new("c_uint64", DataType::UInt64, false),
    ]));

    for (i, partition) in partitions.iter().enumerate() {
        let filename = format!("partition-{}.csv", i);
        let file_path = tmp_dir.path().join(&filename);
        let mut file = File::create(file_path)?;
        for row in partition {
            let row_str = format!(
                "{},{}\n",
                row.0,
                // Populate values for each of the integer fields in the
                // schema.
                (0..8)
                    .map(|_| { row.1.to_string() })
                    .collect::<Vec<_>>()
                    .join(","),
            );
            file.write_all(row_str.as_bytes())?;
        }
    }
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;

    let results = plan_and_collect(
        &ctx,
        "
          SELECT
            c_group,
            COUNT(c_uint64),
            COUNT(DISTINCT c_int8),
            COUNT(DISTINCT c_int16),
            COUNT(DISTINCT c_int32),
            COUNT(DISTINCT c_int64),
            COUNT(DISTINCT c_uint8),
            COUNT(DISTINCT c_uint16),
            COUNT(DISTINCT c_uint32),
            COUNT(DISTINCT c_uint64)
          FROM test
          GROUP BY c_group
        ",
    )
    .await?;

    Ok(results)
}

#[tokio::test]
async fn count_distinct_integers_aggregated_single_partition() -> Result<()> {
    let partitions = vec![
        // The first member of each tuple will be the value for the
        // `c_group` column, and the second member will be the value for
        // each of the int/uint fields.
        vec![
            ("a", 1),
            ("a", 1),
            ("a", 2),
            ("b", 9),
            ("c", 9),
            ("c", 10),
            ("c", 9),
        ],
    ];

    let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

    let expected = vec![
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        "| c_group | COUNT(test.c_uint64) | COUNT(DISTINCT test.c_int8) | COUNT(DISTINCT test.c_int16) | COUNT(DISTINCT test.c_int32) | COUNT(DISTINCT test.c_int64) | COUNT(DISTINCT test.c_uint8) | COUNT(DISTINCT test.c_uint16) | COUNT(DISTINCT test.c_uint32) | COUNT(DISTINCT test.c_uint64) |",
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        "| a       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |",
        "| b       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |",
        "| c       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |",
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn count_distinct_integers_aggregated_multiple_partitions() -> Result<()> {
    let partitions = vec![
        // The first member of each tuple will be the value for the
        // `c_group` column, and the second member will be the value for
        // each of the int/uint fields.
        vec![("a", 1), ("a", 1), ("a", 2), ("b", 9), ("c", 9)],
        vec![("a", 1), ("a", 3), ("b", 8), ("b", 9), ("b", 10), ("b", 11)],
    ];

    let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

    let expected = vec![
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        "| c_group | COUNT(test.c_uint64) | COUNT(DISTINCT test.c_int8) | COUNT(DISTINCT test.c_int16) | COUNT(DISTINCT test.c_int32) | COUNT(DISTINCT test.c_int64) | COUNT(DISTINCT test.c_uint8) | COUNT(DISTINCT test.c_uint16) | COUNT(DISTINCT test.c_uint32) | COUNT(DISTINCT test.c_uint64) |",
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        "| a       | 5                    | 3                           | 3                            | 3                            | 3                            | 3                            | 3                             | 3                             | 3                             |",
        "| b       | 5                    | 4                           | 4                            | 4                            | 4                            | 4                            | 4                             | 4                             | 4                             |",
        "| c       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |",
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_with_alias() -> Result<()> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::UInt32, false),
    ]));

    let plan = scan_empty(None, schema.as_ref(), None)?
        .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
        .project(vec![col("c1"), sum(col("c2")).alias("total_salary")])?
        .build()?;

    let plan = ctx.optimize(&plan)?;

    let physical_plan = ctx.create_physical_plan(&Arc::new(plan)).await?;
    assert_eq!("c1", physical_plan.schema().field(0).name().as_str());
    assert_eq!(
        "total_salary",
        physical_plan.schema().field(1).name().as_str()
    );
    Ok(())
}
