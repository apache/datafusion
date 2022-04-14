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
use datafusion::from_slice::FromSlice;

#[tokio::test]
async fn equijoin() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    let ctx = create_join_context_qualified()?;
    let equivalent_sql = [
        "SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t1.a = t2.a ORDER BY t1.a",
        "SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t2.a = t1.a ORDER BY t1.a",
    ];
    let expected = vec![
        "+---+-----+",
        "| a | b   |",
        "+---+-----+",
        "| 1 | 100 |",
        "| 2 | 200 |",
        "| 4 | 400 |",
        "+---+-----+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_multiple_condition_ordering() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t1_name <> t2_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t2_name <> t1_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id AND t1_name <> t2_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id AND t2_name <> t1_name ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_and_other_condition() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t2_name >= 'y' ORDER BY t1_id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_left_and_condition_from_right() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id AND t2_name >= 'y' ORDER BY t1_id";
    let res = ctx.create_logical_plan(sql);
    assert!(res.is_ok());
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       |         |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn equijoin_right_and_condition_from_left() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t1_id = t2_id AND t1_id >= 22 ORDER BY t2_name";
    let res = ctx.create_logical_plan(sql);
    assert!(res.is_ok());
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "|       |         | w       |",
        "| 44    | d       | x       |",
        "| 22    | b       | y       |",
        "|       |         | z       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_and_unsupported_condition() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id AND t1_id >= '44' ORDER BY t1_id";
    let res = ctx.create_logical_plan(sql);

    assert!(res.is_err());
    assert_eq!(format!("{}", res.unwrap_err()), "This feature is not implemented: Unsupported expressions in Left JOIN: [#t1_id >= Utf8(\"44\")]");

    Ok(())
}

#[tokio::test]
async fn left_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn left_join_unbalanced() -> Result<()> {
    // the t1_id is larger than t2_id so the hash_build_probe_order optimizer should kick in
    let ctx = create_join_context_unbalanced("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "| 77    | e       |         |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn left_join_null_filter() -> Result<()> {
    // Since t2 is the non-preserved side of the join, we cannot push down a NULL filter.
    // Note that this is only true because IS NULL does not remove nulls. For filters that
    // remove nulls, we can rewrite the join as an inner join and then push down the filter.
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t2_id, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id WHERE t2_name IS NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+-------+---------+",
        "| t1_id | t2_id | t2_name |",
        "+-------+-------+---------+",
        "| 22    | 22    |         |",
        "| 33    |       |         |",
        "| 77    |       |         |",
        "| 88    |       |         |",
        "+-------+-------+---------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn left_join_null_filter_on_join_column() -> Result<()> {
    // Again, since t2 is the non-preserved side of the join, we cannot push down a NULL filter.
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t2_id, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id WHERE t2_id IS NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+-------+---------+",
        "| t1_id | t2_id | t2_name |",
        "+-------+-------+---------+",
        "| 33    |       |         |",
        "| 77    |       |         |",
        "| 88    |       |         |",
        "+-------+-------+---------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn left_join_not_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t2_id, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id WHERE t2_name IS NOT NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+-------+---------+",
        "| t1_id | t2_id | t2_name |",
        "+-------+-------+---------+",
        "| 11    | 11    | z       |",
        "| 44    | 44    | x       |",
        "| 99    | 99    | u       |",
        "+-------+-------+---------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn left_join_not_null_filter_on_join_column() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t2_id, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id WHERE t2_id IS NOT NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+-------+---------+",
        "| t1_id | t2_id | t2_name |",
        "+-------+-------+---------+",
        "| 11    | 11    | z       |",
        "| 22    | 22    |         |",
        "| 44    | 44    | x       |",
        "| 99    | 99    | u       |",
        "+-------+-------+---------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn self_join_non_equijoin() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql =
        "SELECT x.t1_id, y.t1_id FROM t1 x JOIN t1 y ON x.t1_id = 11 AND y.t1_id = 44";
    let expected = vec![
        "+-------+-------+",
        "| t1_id | t1_id |",
        "+-------+-------+",
        "| 11    | 44    |",
        "+-------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id WHERE t1_name IS NULL ORDER BY t2_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "|       |         | 55    |",
        "| 99    |         | 99    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join_null_filter_on_join_column() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id WHERE t1_id IS NULL ORDER BY t2_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "|       |         | 55    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join_not_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id WHERE t1_name IS NOT NULL ORDER BY t2_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "| 11    | a       | 11    |",
        "| 22    | b       | 22    |",
        "| 44    | d       | 44    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join_not_null_filter_on_join_column() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id WHERE t1_id IS NOT NULL ORDER BY t2_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "| 11    | a       | 11    |",
        "| 22    | b       | 22    |",
        "| 44    | d       | 44    |",
        "| 99    |         | 99    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn full_join_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 FULL OUTER JOIN t2 ON t1_id = t2_id WHERE t1_name IS NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "| 88    |         |       |",
        "| 99    |         | 99    |",
        "|       |         | 55    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn full_join_not_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 FULL OUTER JOIN t2 ON t1_id = t2_id WHERE t1_name IS NOT NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "| 11    | a       | 11    |",
        "| 22    | b       | 22    |",
        "| 33    | c       |       |",
        "| 44    | d       | 44    |",
        "| 77    | e       |       |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t2_id = t1_id ORDER BY t1_id"
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "|       |         | w       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn full_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "|       |         | w       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL OUTER JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL OUTER JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}

#[tokio::test]
async fn left_join_using() -> Result<()> {
    let ctx = create_join_context("id", "id")?;
    let sql = "SELECT id, t1_name, t2_name FROM t1 LEFT JOIN t2 USING (id) ORDER BY id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+---------+---------+",
        "| id | t1_name | t2_name |",
        "+----+---------+---------+",
        "| 11 | a       | z       |",
        "| 22 | b       | y       |",
        "| 33 | c       |         |",
        "| 44 | d       | x       |",
        "+----+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax_with_filter() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql = "SELECT t1_id, t1_name, t2_name \
        FROM t1, t2 \
        WHERE t1_id > 0 \
        AND t1_id = t2_id \
        AND t2_id < 99 \
        ORDER BY t1_id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax_reversed() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t2_id = t1_id ORDER BY t1_id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cross_join() {
    let ctx = create_join_context("t1_id", "t2_id").unwrap();

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1, t2 ORDER BY t1_id";
    let actual = execute(&ctx, sql).await;

    assert_eq!(4 * 4, actual.len());

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE 1=1 ORDER BY t1_id";
    let actual = execute(&ctx, sql).await;

    assert_eq!(4 * 4, actual.len());

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN t2";

    let actual = execute(&ctx, sql).await;
    assert_eq!(4 * 4, actual.len());

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 11    | a       | y       |",
        "| 11    | a       | x       |",
        "| 11    | a       | w       |",
        "| 22    | b       | z       |",
        "| 22    | b       | y       |",
        "| 22    | b       | x       |",
        "| 22    | b       | w       |",
        "| 33    | c       | z       |",
        "| 33    | c       | y       |",
        "| 33    | c       | x       |",
        "| 33    | c       | w       |",
        "| 44    | d       | z       |",
        "| 44    | d       | y       |",
        "| 44    | d       | x       |",
        "| 44    | d       | w       |",
        "+-------+---------+---------+",
    ];

    assert_batches_eq!(expected, &actual);

    // Two partitions (from UNION) on the left
    let sql = "SELECT * FROM (SELECT t1_id, t1_name FROM t1 UNION ALL SELECT t1_id, t1_name FROM t1) AS t1 CROSS JOIN t2";
    let actual = execute(&ctx, sql).await;

    assert_eq!(4 * 4 * 2, actual.len());

    // Two partitions (from UNION) on the right
    let sql = "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN (SELECT t2_name FROM t2 UNION ALL SELECT t2_name FROM t2) AS t2";
    let actual = execute(&ctx, sql).await;

    assert_eq!(4 * 4 * 2, actual.len());
}

#[tokio::test]
async fn cross_join_unbalanced() {
    // the t1_id is larger than t2_id so the hash_build_probe_order optimizer should kick in
    let ctx = create_join_context_unbalanced("t1_id", "t2_id").unwrap();

    // the order of the values is not determinisitic, so we need to sort to check the values
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN t2 ORDER BY t1_id, t1_name, t2_name";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | w       |",
        "| 11    | a       | x       |",
        "| 11    | a       | y       |",
        "| 11    | a       | z       |",
        "| 22    | b       | w       |",
        "| 22    | b       | x       |",
        "| 22    | b       | y       |",
        "| 22    | b       | z       |",
        "| 33    | c       | w       |",
        "| 33    | c       | x       |",
        "| 33    | c       | y       |",
        "| 33    | c       | z       |",
        "| 44    | d       | w       |",
        "| 44    | d       | x       |",
        "| 44    | d       | y       |",
        "| 44    | d       | z       |",
        "| 77    | e       | w       |",
        "| 77    | e       | x       |",
        "| 77    | e       | y       |",
        "| 77    | e       | z       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_join_timestamp() -> Result<()> {
    let ctx = SessionContext::new();

    // register time table
    let timestamp_schema = Arc::new(Schema::new(vec![Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let timestamp_data = RecordBatch::try_new(
        timestamp_schema.clone(),
        vec![Arc::new(TimestampNanosecondArray::from(vec![
            131964190213133,
            131964190213134,
            131964190213135,
        ]))],
    )?;
    let timestamp_table =
        MemTable::try_new(timestamp_schema, vec![vec![timestamp_data]])?;
    ctx.register_table("timestamp", Arc::new(timestamp_table))?;

    let sql = "SELECT * \
                     FROM timestamp as a \
                     JOIN (SELECT * FROM timestamp) as b \
                     ON a.time = b.time \
                     ORDER BY a.time";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-------------------------------+-------------------------------+",
        "| time                          | time                          |",
        "+-------------------------------+-------------------------------+",
        "| 1970-01-02 12:39:24.190213133 | 1970-01-02 12:39:24.190213133 |",
        "| 1970-01-02 12:39:24.190213134 | 1970-01-02 12:39:24.190213134 |",
        "| 1970-01-02 12:39:24.190213135 | 1970-01-02 12:39:24.190213135 |",
        "+-------------------------------+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_join_float32() -> Result<()> {
    let ctx = SessionContext::new();

    // register population table
    let population_schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("population", DataType::Float32, true),
    ]));
    let population_data = RecordBatch::try_new(
        population_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            Arc::new(Float32Array::from_slice(&[838.698, 1778.934, 626.443])),
        ],
    )?;
    let population_table =
        MemTable::try_new(population_schema, vec![vec![population_data]])?;
    ctx.register_table("population", Arc::new(population_table))?;

    let sql = "SELECT * \
                     FROM population as a \
                     JOIN (SELECT * FROM population) as b \
                     ON a.population = b.population \
                     ORDER BY a.population";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------+------------+------+------------+",
        "| city | population | city | population |",
        "+------+------------+------+------------+",
        "| c    | 626.443    | c    | 626.443    |",
        "| a    | 838.698    | a    | 838.698    |",
        "| b    | 1778.934   | b    | 1778.934   |",
        "+------+------------+------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_join_float64() -> Result<()> {
    let ctx = SessionContext::new();

    // register population table
    let population_schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("population", DataType::Float64, true),
    ]));
    let population_data = RecordBatch::try_new(
        population_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            Arc::new(Float64Array::from_slice(&[838.698, 1778.934, 626.443])),
        ],
    )?;
    let population_table =
        MemTable::try_new(population_schema, vec![vec![population_data]])?;
    ctx.register_table("population", Arc::new(population_table))?;

    let sql = "SELECT * \
                     FROM population as a \
                     JOIN (SELECT * FROM population) as b \
                     ON a.population = b.population \
                     ORDER BY a.population";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------+------------+------+------------+",
        "| city | population | city | population |",
        "+------+------------+------+------------+",
        "| c    | 626.443    | c    | 626.443    |",
        "| a    | 838.698    | a    | 838.698    |",
        "| b    | 1778.934   | b    | 1778.934   |",
        "+------+------------+------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

// TODO Tests to prove correct implementation of INNER JOIN's with qualified names.
//  https://issues.apache.org/jira/projects/ARROW/issues/ARROW-11432.
#[tokio::test]
#[ignore]
async fn inner_join_qualified_names() -> Result<()> {
    // Setup the statements that test qualified names function correctly.
    let equivalent_sql = [
        "SELECT t1.a, t1.b, t1.c, t2.a, t2.b, t2.c
            FROM t1
            INNER JOIN t2 ON t1.a = t2.a
            ORDER BY t1.a",
        "SELECT t1.a, t1.b, t1.c, t2.a, t2.b, t2.c
            FROM t1
            INNER JOIN t2 ON t2.a = t1.a
            ORDER BY t1.a",
    ];

    let expected = vec![
        "+---+----+----+---+-----+-----+",
        "| a | b  | c  | a | b   | c   |",
        "+---+----+----+---+-----+-----+",
        "| 1 | 10 | 50 | 1 | 100 | 500 |",
        "| 2 | 20 | 60 | 2 | 200 | 600 |",
        "| 4 | 40 | 80 | 4 | 400 | 800 |",
        "+---+----+----+---+-----+-----+",
    ];

    for sql in equivalent_sql.iter() {
        let ctx = create_join_context_qualified()?;
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn inner_join_nulls() {
    let sql = "SELECT * FROM (SELECT null AS id1) t1
            INNER JOIN (SELECT null AS id2) t2 ON id1 = id2";

    let expected = vec!["++", "++"];

    let ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&ctx, sql).await;

    // left and right shouldn't match anything
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn join_tables_with_duplicated_column_name_not_in_on_constraint() -> Result<()> {
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from_slice(&[1, 2, 3])) as _),
        (
            "country",
            Arc::new(StringArray::from_slice(&["Germany", "Sweden", "Japan"])) as _,
        ),
    ])
    .unwrap();
    let countries = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from_slice(&[1, 2, 3, 4, 5, 6, 7])) as _,
        ),
        (
            "city",
            Arc::new(StringArray::from_slice(&[
                "Hamburg",
                "Stockholm",
                "Osaka",
                "Berlin",
                "Göteborg",
                "Tokyo",
                "Kyoto",
            ])) as _,
        ),
        (
            "country_id",
            Arc::new(Int32Array::from_slice(&[1, 2, 3, 1, 2, 3, 3])) as _,
        ),
    ])
    .unwrap();
    let cities = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let ctx = SessionContext::new();
    ctx.register_table("countries", Arc::new(countries))?;
    ctx.register_table("cities", Arc::new(cities))?;

    // city.id is not in the on constraint, but the output result will contain both city.id and
    // country.id
    let sql = "SELECT t1.id, t2.id, t1.city, t2.country FROM cities AS t1 JOIN countries AS t2 ON t1.country_id = t2.id ORDER BY t1.id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+-----------+---------+",
        "| id | id | city      | country |",
        "+----+----+-----------+---------+",
        "| 1  | 1  | Hamburg   | Germany |",
        "| 2  | 2  | Stockholm | Sweden  |",
        "| 3  | 3  | Osaka     | Japan   |",
        "| 4  | 1  | Berlin    | Germany |",
        "| 5  | 2  | Göteborg  | Sweden  |",
        "| 6  | 3  | Tokyo     | Japan   |",
        "| 7  | 3  | Kyoto     | Japan   |",
        "+----+----+-----------+---------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn join_timestamp() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let expected = vec![
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
        "| nanos                         | micros                     | millis                  | secs                | name  | nanos                         | micros                     | millis                  | secs                | name  |",
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
        "| 2011-12-13 11:13:10.123450    | 2011-12-13 11:13:10.123450 | 2011-12-13 11:13:10.123 | 2011-12-13 11:13:10 | Row 1 | 2011-12-13 11:13:10.123450    | 2011-12-13 11:13:10.123450 | 2011-12-13 11:13:10.123 | 2011-12-13 11:13:10 | Row 1 |",
        "| 2018-11-13 17:11:10.011375885 | 2018-11-13 17:11:10.011375 | 2018-11-13 17:11:10.011 | 2018-11-13 17:11:10 | Row 0 | 2018-11-13 17:11:10.011375885 | 2018-11-13 17:11:10.011375 | 2018-11-13 17:11:10.011 | 2018-11-13 17:11:10 | Row 0 |",
        "| 2021-01-01 05:11:10.432       | 2021-01-01 05:11:10.432    | 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10 | Row 3 | 2021-01-01 05:11:10.432       | 2021-01-01 05:11:10.432    | 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10 | Row 3 |",
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
    ];

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.nanos = t2.nanos",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.micros = t2.micros",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.millis = t2.millis",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}
