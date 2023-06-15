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
use crate::sql::execute_to_batches;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;
use log::debug;

#[tokio::test]
async fn simple_uncorrelated_scalar_subquery2() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "select (select count(*) from t1) as b, (select count(1) from t2)";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: __scalar_sq_1.COUNT(UInt8(1)) AS b, __scalar_sq_2.COUNT(Int64(1)) AS COUNT(Int64(1)) [b:Int64;N, COUNT(Int64(1)):Int64;N]",
        "  Left Join:  [COUNT(UInt8(1)):Int64;N, COUNT(Int64(1)):Int64;N]",
        "    SubqueryAlias: __scalar_sq_1 [COUNT(UInt8(1)):Int64;N]",
        "      Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]] [COUNT(UInt8(1)):Int64;N]",
        "        TableScan: t1 projection=[t1_id] [t1_id:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_2 [COUNT(Int64(1)):Int64;N]",
        "      Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1))]] [COUNT(Int64(1)):Int64;N]",
        "        TableScan: t2 projection=[t2_id] [t2_id:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---+-----------------+",
        "| b | COUNT(Int64(1)) |",
        "+---+-----------------+",
        "| 4 | 4               |",
        "+---+-----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql =
        "SELECT t1_id, (SELECT count(*) FROM t2 WHERE t2.t2_int = t1.t1_int) from t1";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_id, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.COUNT(UInt8(1)) END AS COUNT(UInt8(1)) [t1_id:UInt32;N, COUNT(UInt8(1)):Int64;N]",
        "  Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_id:UInt32;N, t1_int:UInt32;N, COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean;N]",
        "    TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_1 [COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "      Projection: COUNT(UInt8(1)), t2.t2_int, __always_true [COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "        Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "          TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+-----------------+",
        "| t1_id | COUNT(UInt8(1)) |",
        "+-------+-----------------+",
        "| 33    | 3               |",
        "| 22    | 0               |",
        "| 11    | 1               |",
        "| 44    | 0               |",
        "+-------+-----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg2() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, (SELECT count(*) FROM t2 WHERE t2.t2_int = t1.t1_int) as cnt from t1";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_id, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.COUNT(UInt8(1)) END AS cnt [t1_id:UInt32;N, cnt:Int64;N]",
        "  Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_id:UInt32;N, t1_int:UInt32;N, COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean;N]",
        "    TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_1 [COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "      Projection: COUNT(UInt8(1)), t2.t2_int, __always_true [COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "        Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "          TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+-----+",
        "| t1_id | cnt |",
        "+-------+-----+",
        "| 33    | 3   |",
        "| 22    | 0   |",
        "| 11    | 1   |",
        "| 44    | 0   |",
        "+-------+-----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_with_alias() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, (SELECT count(*) as _cnt FROM t2 WHERE t2.t2_int = t1.t1_int) as cnt from t1";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_id, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) AS _cnt ELSE __scalar_sq_1._cnt END AS cnt [t1_id:UInt32;N, cnt:Int64;N]",
        "  Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_id:UInt32;N, t1_int:UInt32;N, _cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean;N]",
        "    TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_1 [_cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "      Projection: COUNT(UInt8(1)) AS _cnt, t2.t2_int, __always_true [_cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "        Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "          TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+-----+",
        "| t1_id | cnt |",
        "+-------+-----+",
        "| 33    | 3   |",
        "| 22    | 0   |",
        "| 11    | 1   |",
        "| 44    | 0   |",
        "+-------+-----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_complex_expr() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, (SELECT count(*) + 2 as _cnt FROM t2 WHERE t2.t2_int = t1.t1_int) from t1";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_id, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(2) AS _cnt ELSE __scalar_sq_1._cnt END AS _cnt [t1_id:UInt32;N, _cnt:Int64;N]",
        "  Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_id:UInt32;N, t1_int:UInt32;N, _cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean;N]",
        "    TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_1 [_cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "      Projection: COUNT(UInt8(1)) + Int64(2) AS _cnt, t2.t2_int, __always_true [_cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "        Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "          TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+------+",
        "| t1_id | _cnt |",
        "+-------+------+",
        "| 11    | 3    |",
        "| 22    | 2    |",
        "| 33    | 5    |",
        "| 44    | 2    |",
        "+-------+------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_where_clause() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "select t1.t1_int from t1 where (select count(*) from t2 where t1.t1_id = t2.t2_id) < t1.t1_int";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_int [t1_int:UInt32;N]",
        "  Filter: CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.COUNT(UInt8(1)) END < CAST(t1.t1_int AS Int64) [t1_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean;N]",
        "    Projection: t1.t1_int, __scalar_sq_1.COUNT(UInt8(1)), __scalar_sq_1.__always_true [t1_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean;N]",
        "      Left Join: t1.t1_id = __scalar_sq_1.t2_id [t1_id:UInt32;N, t1_int:UInt32;N, COUNT(UInt8(1)):Int64;N, t2_id:UInt32;N, __always_true:Boolean;N]",
        "        TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "        SubqueryAlias: __scalar_sq_1 [COUNT(UInt8(1)):Int64;N, t2_id:UInt32;N, __always_true:Boolean]",
        "          Projection: COUNT(UInt8(1)), t2.t2_id, __always_true [COUNT(UInt8(1)):Int64;N, t2_id:UInt32;N, __always_true:Boolean]",
        "            Aggregate: groupBy=[[t2.t2_id, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_id:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "              TableScan: t2 projection=[t2_id] [t2_id:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------+",
        "| t1_int |",
        "+--------+",
        "| 2      |",
        "| 4      |",
        "| 3      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn correlated_scalar_subquery_sum_agg_bug() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "select t1.t1_int from t1 where (select sum(t2_int) is null from t2 where t1.t1_id = t2.t2_id)";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_int [t1_int:UInt32;N]",
        "  Inner Join: t1.t1_id = __scalar_sq_1.t2_id [t1_id:UInt32;N, t1_int:UInt32;N, t2_id:UInt32;N]",
        "    TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_1 [t2_id:UInt32;N]",
        "      Projection: t2.t2_id [t2_id:UInt32;N]",
        "        Filter: SUM(t2.t2_int) IS NULL [t2_id:UInt32;N, SUM(t2.t2_int):UInt64;N]",
        "          Aggregate: groupBy=[[t2.t2_id]], aggr=[[SUM(t2.t2_int)]] [t2_id:UInt32;N, SUM(t2.t2_int):UInt64;N]",
        "            TableScan: t2 projection=[t2_id, t2_int] [t2_id:UInt32;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------+",
        "| t1_int |",
        "+--------+",
        "| 2      |",
        "| 4      |",
        "| 3      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_with_having() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, (SELECT count(*) + 2 as cnt_plus_2 FROM t2 WHERE t2.t2_int = t1.t1_int having count(*) >1) from t1";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    // the having condition is kept as the normal filter condition, no need to pull up
    let expected = vec![
        "Projection: t1.t1_id, __scalar_sq_1.cnt_plus_2 AS cnt_plus_2 [t1_id:UInt32;N, cnt_plus_2:Int64;N]",
        "  Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_id:UInt32;N, t1_int:UInt32;N, cnt_plus_2:Int64;N, t2_int:UInt32;N]",
        "    TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_1 [cnt_plus_2:Int64;N, t2_int:UInt32;N]",
        "      Projection: COUNT(UInt8(1)) + Int64(2) AS cnt_plus_2, t2.t2_int [cnt_plus_2:Int64;N, t2_int:UInt32;N]",
        "        Filter: COUNT(UInt8(1)) > Int64(1) [t2_int:UInt32;N, COUNT(UInt8(1)):Int64;N]",
        "          Projection: t2.t2_int, COUNT(UInt8(1)) [t2_int:UInt32;N, COUNT(UInt8(1)):Int64;N]",
        "            Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "              TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+------------+",
        "| t1_id | cnt_plus_2 |",
        "+-------+------------+",
        "| 11    |            |",
        "| 22    |            |",
        "| 33    | 5          |",
        "| 44    |            |",
        "+-------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_with_pull_up_having() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, (SELECT count(*) + 2 as cnt_plus_2 FROM t2 WHERE t2.t2_int = t1.t1_int having count(*) = 0) from t1";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    // the having condition need to pull up and evaluated after the left out join
    let expected = vec![
        "Projection: t1.t1_id, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(2) AS cnt_plus_2 WHEN __scalar_sq_1.COUNT(UInt8(1)) != Int64(0) THEN NULL ELSE __scalar_sq_1.cnt_plus_2 END AS cnt_plus_2 [t1_id:UInt32;N, cnt_plus_2:Int64;N]",
        "  Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_id:UInt32;N, t1_int:UInt32;N, cnt_plus_2:Int64;N, t2_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean;N]",
        "    TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_1 [cnt_plus_2:Int64;N, t2_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean]",
        "      Projection: COUNT(UInt8(1)) + Int64(2) AS cnt_plus_2, t2.t2_int, COUNT(UInt8(1)), __always_true [cnt_plus_2:Int64;N, t2_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean]",
        "        Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "          TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+------------+",
        "| t1_id | cnt_plus_2 |",
        "+-------+------------+",
        "| 11    |            |",
        "| 22    | 2          |",
        "| 33    |            |",
        "| 44    | 2          |",
        "+-------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_in_having() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "select t1.t1_int from t1 group by t1.t1_int having (select count(*) from t2 where t1.t1_int = t2.t2_int) = 0";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_int [t1_int:UInt32;N]",
        "  Filter: CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.COUNT(UInt8(1)) END = Int64(0) [t1_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean;N]",
        "    Projection: t1.t1_int, __scalar_sq_1.COUNT(UInt8(1)), __scalar_sq_1.__always_true [t1_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean;N]",
        "      Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_int:UInt32;N, COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean;N]",
        "        Aggregate: groupBy=[[t1.t1_int]], aggr=[[]] [t1_int:UInt32;N]",
        "          TableScan: t1 projection=[t1_int] [t1_int:UInt32;N]",
        "        SubqueryAlias: __scalar_sq_1 [COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "          Projection: COUNT(UInt8(1)), t2.t2_int, __always_true [COUNT(UInt8(1)):Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "            Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "              TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------+",
        "| t1_int |",
        "+--------+",
        "| 2      |",
        "| 4      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_in_nested_projection() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "select t1.t1_int from t1 where (select cnt from (select count(*) as cnt, sum(t2_int) from t2 where t1.t1_int = t2.t2_int)) = 0";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_int [t1_int:UInt32;N]",
        "  Filter: CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.cnt END = Int64(0) [t1_int:UInt32;N, cnt:Int64;N, __always_true:Boolean;N]",
        "    Projection: t1.t1_int, __scalar_sq_1.cnt, __scalar_sq_1.__always_true [t1_int:UInt32;N, cnt:Int64;N, __always_true:Boolean;N]",
        "      Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_int:UInt32;N, cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean;N]",
        "        TableScan: t1 projection=[t1_int] [t1_int:UInt32;N]",
        "        SubqueryAlias: __scalar_sq_1 [cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "          Projection: COUNT(UInt8(1)) AS cnt, t2.t2_int, __always_true [cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "            Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "              TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------+",
        "| t1_int |",
        "+--------+",
        "| 2      |",
        "| 4      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_in_nested_subquery() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "select t1.t1_int from t1 where \
                        (select cnt_plus_one + 1 as cnt_plus_two from \
                            (select cnt + 1 as cnt_plus_one from \
                                (select count(*) as cnt, sum(t2_int) s from t2 where t1.t1_int = t2.t2_int having cnt = 0)\
                            )\
                        ) = 2";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    // pull up the deeply nested having condition
    let expected = vec![
        "Projection: t1.t1_int [t1_int:UInt32;N]",
        "  Filter: CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(2) WHEN __scalar_sq_1.COUNT(UInt8(1)) != Int64(0) THEN NULL ELSE __scalar_sq_1.cnt_plus_two END = Int64(2) [t1_int:UInt32;N, cnt_plus_two:Int64;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean;N]",
        "    Projection: t1.t1_int, __scalar_sq_1.cnt_plus_two, __scalar_sq_1.COUNT(UInt8(1)), __scalar_sq_1.__always_true [t1_int:UInt32;N, cnt_plus_two:Int64;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean;N]",
        "      Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_int:UInt32;N, cnt_plus_two:Int64;N, t2_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean;N]",
        "        TableScan: t1 projection=[t1_int] [t1_int:UInt32;N]",
        "        SubqueryAlias: __scalar_sq_1 [cnt_plus_two:Int64;N, t2_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean]",
        "          Projection: COUNT(UInt8(1)) + Int64(1) + Int64(1) AS cnt_plus_two, t2.t2_int, COUNT(UInt8(1)), __always_true [cnt_plus_two:Int64;N, t2_int:UInt32;N, COUNT(UInt8(1)):Int64;N, __always_true:Boolean]",
        "            Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "              TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------+",
        "| t1_int |",
        "+--------+",
        "| 2      |",
        "| 4      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_scalar_subquery_count_agg_in_case_when() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "select t1.t1_int from t1 where \
                    (select case when count(*) = 1 then null else count(*) end as cnt from t2 where t2.t2_int = t1.t1_int)\
                    = 0";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_int [t1_int:UInt32;N]",
        "  Filter: CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.cnt END = Int64(0) [t1_int:UInt32;N, cnt:Int64;N, __always_true:Boolean;N]",
        "    Projection: t1.t1_int, __scalar_sq_1.cnt, __scalar_sq_1.__always_true [t1_int:UInt32;N, cnt:Int64;N, __always_true:Boolean;N]",
        "      Left Join: t1.t1_int = __scalar_sq_1.t2_int [t1_int:UInt32;N, cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean;N]",
        "        TableScan: t1 projection=[t1_int] [t1_int:UInt32;N]",
        "        SubqueryAlias: __scalar_sq_1 [cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "          Projection: CASE WHEN COUNT(UInt8(1)) = Int64(1) THEN Int64(NULL) ELSE COUNT(UInt8(1)) END AS cnt, t2.t2_int, __always_true [cnt:Int64;N, t2_int:UInt32;N, __always_true:Boolean]",
        "            Aggregate: groupBy=[[t2.t2_int, Boolean(true) AS __always_true]], aggr=[[COUNT(UInt8(1))]] [t2_int:UInt32;N, __always_true:Boolean, COUNT(UInt8(1)):Int64;N]",
        "              TableScan: t2 projection=[t2_int] [t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------+",
        "| t1_int |",
        "+--------+",
        "| 2      |",
        "| 4      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}
