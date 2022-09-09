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

#[tokio::test]
async fn csv_query_with_predicate() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, c12 FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+---------------------+",
        "| c1 | c12                 |",
        "+----+---------------------+",
        "| e  | 0.39144436569161134 |",
        "| d  | 0.38870280983958583 |",
        "+----+---------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_negative_predicate() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, c4 FROM aggregate_test_100 WHERE c3 < -55 AND -c4 > 30000";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+--------+",
        "| c1 | c4     |",
        "+----+--------+",
        "| e  | -31500 |",
        "| c  | -30187 |",
        "+----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_negated_predicate() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE NOT(c1 != 'a')";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 21              |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_is_not_null_predicate() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE c1 IS NOT NULL";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_is_null_predicate() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE c1 IS NULL";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 0               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_where_neg_num() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;

    // Negative numbers do not parse correctly as of Arrow 2.0.0
    let sql = "select c7, c8 from aggregate_test_100 where c7 >= -2 and c7 < 10";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-------+",
        "| c7 | c8    |",
        "+----+-------+",
        "| 7  | 45465 |",
        "| 5  | 40622 |",
        "| 0  | 61069 |",
        "| 2  | 20120 |",
        "| 4  | 39363 |",
        "+----+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Also check floating point neg numbers
    let sql = "select c7, c8 from aggregate_test_100 where c7 >= -2.9 and c7 < 10";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "SELECT COUNT(c1) FROM aggregate_test_100 WHERE c13 LIKE '%FB%'";
    // check that the physical and logical schemas are equal
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------------------------+",
        "| COUNT(aggregate_test_100.c1) |",
        "+------------------------------+",
        "| 1                            |",
        "+------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_between_expr() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c4 FROM aggregate_test_100 WHERE c12 BETWEEN 0.995 AND 1.0";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c4    |",
        "+-------+",
        "| 10837 |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_between_expr_negated() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c4 FROM aggregate_test_100 WHERE c12 NOT BETWEEN 0 AND 0.995";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c4    |",
        "+-------+",
        "| 10837 |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like_on_strings() -> Result<()> {
    let input = vec![Some("foo"), Some("bar"), None, Some("fazzz")]
        .into_iter()
        .collect::<StringArray>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 LIKE '%a%'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| bar   |",
        "| fazzz |",
        "+-------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like_on_string_dictionaries() -> Result<()> {
    let input = vec![Some("foo"), Some("bar"), None, Some("fazzz")]
        .into_iter()
        .collect::<DictionaryArray<Int32Type>>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 LIKE '%a%'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| bar   |",
        "| fazzz |",
        "+-------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_regexp_is_match() -> Result<()> {
    let input = vec![Some("foo"), Some("Barrr"), Some("Bazzz"), Some("ZZZZZ")]
        .into_iter()
        .collect::<StringArray>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 ~ 'z'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| Bazzz |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 ~* 'z'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| Bazzz |",
        "| ZZZZZ |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 !~ 'z'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| foo   |",
        "| Barrr |",
        "| ZZZZZ |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 !~* 'z'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| foo   |",
        "| Barrr |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn except_with_null_not_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            EXCEPT SELECT * FROM (SELECT null AS id1, 2 AS id2) t2";

    let expected = vec![
        "+-----+-----+",
        "| id1 | id2 |",
        "+-----+-----+",
        "|     | 1   |",
        "+-----+-----+",
    ];

    let ctx = create_join_context_qualified("t1", "t2").unwrap();
    let actual = execute_to_batches(&ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn except_with_null_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            EXCEPT SELECT * FROM (SELECT null AS id1, 1 AS id2) t2";

    let expected = vec!["++", "++"];
    let ctx = create_join_context_qualified("t1", "t2").unwrap();
    let actual = execute_to_batches(&ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_expect_all() -> Result<()> {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 EXCEPT ALL SELECT int_col, double_col FROM alltypes_plain where int_col < 1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_expect_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 EXCEPT SELECT int_col, double_col FROM alltypes_plain where int_col < 1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_in_set_test() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT count(*) FROM aggregate_test_100 WHERE c7 in ('25','155','204','77','208','67','139','191','26','7','202','113','129','197','249','146','129','220','154','163','220','19','71','243','150','231','196','170','99','255');";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 36              |",
        "+-----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn multiple_or_predicates() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "lineitem").await?;
    register_tpch_csv(&ctx, "part").await?;
    let sql = "explain select
    l_partkey
    from
    lineitem,
    part
    where
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#12'
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
        )
    or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#23'
            and l_quantity >= 10 and l_quantity <= 10 + 10
            and p_size between 1 and 10
        )
    or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#34'
            and l_quantity >= 20 and l_quantity <= 20 + 10
            and p_size between 1 and 15
        )";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx.create_logical_plan(sql).expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    // Note that we expect `#part.p_partkey = #lineitem.l_partkey` to have been
    // factored out and appear only once in the following plan
    let expected =vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #lineitem.l_partkey [l_partkey:Int64]",
        "    Projection: #part.p_partkey = #lineitem.l_partkey AS #part.p_partkey = #lineitem.l_partkey#lineitem.l_partkey#part.p_partkey, #part.p_size >= Int32(1) AS #part.p_size >= Int32(1)Int32(1)#part.p_size, #lineitem.l_partkey, #lineitem.l_quantity, #part.p_brand, #part.p_size [#part.p_partkey = #lineitem.l_partkey#lineitem.l_partkey#part.p_partkey:Boolean;N, #part.p_size >= Int32(1)Int32(1)#part.p_size:Boolean;N, l_partkey:Int64, l_quantity:Float64, p_brand:Utf8, p_size:Int32]",
        "      Filter: #part.p_partkey = #lineitem.l_partkey AND #part.p_brand = Utf8(\"Brand#12\") AND #lineitem.l_quantity >= CAST(Int64(1) AS Float64) AND #lineitem.l_quantity <= CAST(Int64(11) AS Float64) AND #part.p_size <= Int32(5) OR #part.p_brand = Utf8(\"Brand#23\") AND #lineitem.l_quantity >= CAST(Int64(10) AS Float64) AND #lineitem.l_quantity <= CAST(Int64(20) AS Float64) AND #part.p_size <= Int32(10) OR #part.p_brand = Utf8(\"Brand#34\") AND #lineitem.l_quantity >= CAST(Int64(20) AS Float64) AND #lineitem.l_quantity <= CAST(Int64(30) AS Float64) AND #part.p_size <= Int32(15) [l_partkey:Int64, l_quantity:Float64, p_partkey:Int64, p_brand:Utf8, p_size:Int32]",
        "        CrossJoin: [l_partkey:Int64, l_quantity:Float64, p_partkey:Int64, p_brand:Utf8, p_size:Int32]",
        "          TableScan: lineitem projection=[l_partkey, l_quantity] [l_partkey:Int64, l_quantity:Float64]",
        "          Filter: #part.p_size >= Int32(1) [p_partkey:Int64, p_brand:Utf8, p_size:Int32]",
        "            TableScan: part projection=[p_partkey, p_brand, p_size], partial_filters=[#part.p_size >= Int32(1)] [p_partkey:Int64, p_brand:Utf8, p_size:Int32]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    Ok(())
}
