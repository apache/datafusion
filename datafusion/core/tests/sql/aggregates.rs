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
use datafusion_common::cast::as_float64_array;

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
    let array = as_float64_array(column)?;
    let actual = array.value(0);
    let expected = 0.5089725;
    // Due to float number's accuracy, different batch size will lead to different
    // answers.
    assert!((expected - actual).abs() < 0.01);
    Ok(())
}

#[tokio::test]
#[ignore] // https://github.com/apache/arrow-datafusion/issues/3353
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

#[tokio::test]
async fn csv_query_approx_percentile_cont_with_weight() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;

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
async fn csv_query_approx_percentile_cont_with_histogram_bins() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;

    let results = plan_and_collect(
        &ctx,
        "SELECT c1, approx_percentile_cont(c3, 0.95, -1000) AS c3_p95 FROM aggregate_test_100 GROUP BY 1 ORDER BY 1",
    )
        .await
        .unwrap_err();
    assert_eq!(results.to_string(), "This feature is not implemented: Tdigest max_size value for 'APPROX_PERCENTILE_CONT' must be UInt > 0 literal (got data type Int64).");

    let results = plan_and_collect(
        &ctx,
        "SELECT approx_percentile_cont(c3, 0.95, c1) FROM aggregate_test_100",
    )
    .await
    .unwrap_err();
    assert_eq!(results.to_string(), "Error during planning: The percentile sample points count for ApproxPercentileCont must be integer, not Utf8.");

    let results = plan_and_collect(
        &ctx,
        "SELECT approx_percentile_cont(c3, 0.95, 111.1) FROM aggregate_test_100",
    )
    .await
    .unwrap_err();
    assert_eq!(results.to_string(), "Error during planning: The percentile sample points count for ApproxPercentileCont must be integer, not Float64.");

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
async fn csv_query_array_agg_unsupported() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;

    let results = plan_and_collect(
        &ctx,
        "SELECT array_agg(c13 ORDER BY c1) FROM aggregate_test_100",
    )
    .await
    .unwrap_err();

    assert_eq!(
        results.to_string(),
        "This feature is not implemented: ORDER BY not supported in ARRAY_AGG: c1"
    );

    let results = plan_and_collect(
        &ctx,
        "SELECT array_agg(c13 LIMIT 1) FROM aggregate_test_100",
    )
    .await
    .unwrap_err();

    assert_eq!(
        results.to_string(),
        "This feature is not implemented: LIMIT not supported in ARRAY_AGG: 1"
    );

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
        "| 2011-12-13T11:13:10.123450 | 2011-12-13T11:13:10.123450 | 2011-12-13T11:13:10.123 | 2011-12-13T11:13:10 |",
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
        "| 2021-01-01T05:11:10.432 | 2021-01-01T05:11:10.432 | 2021-01-01T05:11:10.432 | 2021-01-01T05:11:10 |",
        "+-------------------------+-------------------------+-------------------------+---------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_times_sum() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_times()).unwrap();

    let results = plan_and_collect(
        &ctx,
        "SELECT sum(nanos), sum(micros), sum(millis), sum(secs) FROM t",
    )
    .await
    .unwrap_err();

    assert_eq!(results.to_string(), "Error during planning: The function Sum does not support inputs of type Time64(Nanosecond).");

    Ok(())
}

#[tokio::test]
async fn aggregate_times_count() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_times()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT count(nanos), count(micros), count(millis), count(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+----------------+-----------------+-----------------+---------------+",
        "| COUNT(t.nanos) | COUNT(t.micros) | COUNT(t.millis) | COUNT(t.secs) |",
        "+----------------+-----------------+-----------------+---------------+",
        "| 4              | 4               | 4               | 4             |",
        "+----------------+-----------------+-----------------+---------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_times_min() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_times()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT min(nanos), min(micros), min(millis), min(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+--------------------+-----------------+---------------+-------------+",
        "| MIN(t.nanos)       | MIN(t.micros)   | MIN(t.millis) | MIN(t.secs) |",
        "+--------------------+-----------------+---------------+-------------+",
        "| 18:06:30.243620451 | 18:06:30.243620 | 18:06:30.243  | 18:06:30    |",
        "+--------------------+-----------------+---------------+-------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_times_max() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_times()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT max(nanos), max(micros), max(millis), max(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+--------------------+-----------------+---------------+-------------+",
        "| MAX(t.nanos)       | MAX(t.micros)   | MAX(t.millis) | MAX(t.secs) |",
        "+--------------------+-----------------+---------------+-------------+",
        "| 21:06:28.247821084 | 21:06:28.247821 | 21:06:28.247  | 21:06:28    |",
        "+--------------------+-----------------+---------------+-------------+",
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
        &DataType::Decimal128(20, 3),
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
        &DataType::Decimal128(14, 7),
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
        "+--------------+-------------------------+-------------------------+-------------------------+",
        "| AVG(test.c1) | AVG(test.c1) + Int64(1) | AVG(test.c1) + Int64(2) | Int64(1) + AVG(test.c1) |",
        "+--------------+-------------------------+-------------------------+-------------------------+",
        "| 1.5          | 2.5                     | 3.5                     | 2.5                     |",
        "+--------------+-------------------------+-------------------------+-------------------------+",
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
async fn count_aggregated_cube() -> Result<()> {
    let results = execute_with_partition(
        "SELECT c1, c2, COUNT(c3) FROM test GROUP BY CUBE (c1, c2) ORDER BY c1, c2",
        4,
    )
    .await?;

    let expected = vec![
        "+----+----+----------------+",
        "| c1 | c2 | COUNT(test.c3) |",
        "+----+----+----------------+",
        "|    |    | 40             |",
        "|    | 1  | 4              |",
        "|    | 10 | 4              |",
        "|    | 2  | 4              |",
        "|    | 3  | 4              |",
        "|    | 4  | 4              |",
        "|    | 5  | 4              |",
        "|    | 6  | 4              |",
        "|    | 7  | 4              |",
        "|    | 8  | 4              |",
        "|    | 9  | 4              |",
        "| 0  |    | 10             |",
        "| 0  | 1  | 1              |",
        "| 0  | 10 | 1              |",
        "| 0  | 2  | 1              |",
        "| 0  | 3  | 1              |",
        "| 0  | 4  | 1              |",
        "| 0  | 5  | 1              |",
        "| 0  | 6  | 1              |",
        "| 0  | 7  | 1              |",
        "| 0  | 8  | 1              |",
        "| 0  | 9  | 1              |",
        "| 1  |    | 10             |",
        "| 1  | 1  | 1              |",
        "| 1  | 10 | 1              |",
        "| 1  | 2  | 1              |",
        "| 1  | 3  | 1              |",
        "| 1  | 4  | 1              |",
        "| 1  | 5  | 1              |",
        "| 1  | 6  | 1              |",
        "| 1  | 7  | 1              |",
        "| 1  | 8  | 1              |",
        "| 1  | 9  | 1              |",
        "| 2  |    | 10             |",
        "| 2  | 1  | 1              |",
        "| 2  | 10 | 1              |",
        "| 2  | 2  | 1              |",
        "| 2  | 3  | 1              |",
        "| 2  | 4  | 1              |",
        "| 2  | 5  | 1              |",
        "| 2  | 6  | 1              |",
        "| 2  | 7  | 1              |",
        "| 2  | 8  | 1              |",
        "| 2  | 9  | 1              |",
        "| 3  |    | 10             |",
        "| 3  | 1  | 1              |",
        "| 3  | 10 | 1              |",
        "| 3  | 2  | 1              |",
        "| 3  | 3  | 1              |",
        "| 3  | 4  | 1              |",
        "| 3  | 5  | 1              |",
        "| 3  | 6  | 1              |",
        "| 3  | 7  | 1              |",
        "| 3  | 8  | 1              |",
        "| 3  | 9  | 1              |",
        "+----+----+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn simple_avg() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let result = plan_and_collect(&ctx, "SELECT AVG(a) FROM t").await?;

    let batch = &result[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(1, batch.num_rows());

    let values = as_float64_array(batch.column(0)).expect("failed to cast version");
    assert_eq!(values.len(), 1);
    // avg(1,2,3,4,5) = 3.0
    assert_eq!(values.value(0), 3.0_f64);
    Ok(())
}

#[tokio::test]
async fn simple_mean() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let result = plan_and_collect(&ctx, "SELECT MEAN(a) FROM t").await?;

    let batch = &result[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(1, batch.num_rows());

    let values = as_float64_array(batch.column(0)).expect("failed to cast version");
    assert_eq!(values.len(), 1);
    // mean(1,2,3,4,5) = 3.0
    assert_eq!(values.value(0), 3.0_f64);
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
        let file_path = tmp_dir.path().join(filename);
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

#[tokio::test]
async fn array_agg_zero() -> Result<()> {
    let ctx = SessionContext::new();
    // 2 different aggregate functions: avg and sum(distinct)
    let sql = "SELECT ARRAY_AGG([]);";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------------------+",
        "| ARRAYAGG(List([NULL])) |",
        "+------------------------+",
        "| []                     |",
        "+------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn array_agg_one() -> Result<()> {
    let ctx = SessionContext::new();
    // 2 different aggregate functions: avg and sum(distinct)
    let sql = "SELECT ARRAY_AGG([1]);";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+",
        "| ARRAYAGG(List([1])) |",
        "+---------------------+",
        "| [[1]]               |",
        "+---------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
