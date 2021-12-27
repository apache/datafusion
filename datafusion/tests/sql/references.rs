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
async fn qualified_table_references() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;

    for table_ref in &[
        "aggregate_test_100",
        "public.aggregate_test_100",
        "datafusion.public.aggregate_test_100",
    ] {
        let sql = format!("SELECT COUNT(*) FROM {}", table_ref);
        let actual = execute_to_batches(&mut ctx, &sql).await;
        let expected = vec![
            "+-----------------+",
            "| COUNT(UInt8(1)) |",
            "+-----------------+",
            "| 100             |",
            "+-----------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn qualified_table_references_and_fields() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let c1: StringArray = vec!["foofoo", "foobar", "foobaz"]
        .into_iter()
        .map(Some)
        .collect();
    let c2: Int64Array = vec![1, 2, 3].into_iter().map(Some).collect();
    let c3: Int64Array = vec![10, 20, 30].into_iter().map(Some).collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("f.c1", Arc::new(c1) as ArrayRef),
        //  evil -- use the same name as the table
        ("test.c2", Arc::new(c2) as ArrayRef),
        //  more evil still
        ("....", Arc::new(c3) as ArrayRef),
    ])?;

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("test", Arc::new(table))?;

    // referring to the unquoted column is an error
    let sql = r#"SELECT f1.c1 from test"#;
    let error = ctx.create_logical_plan(sql).unwrap_err();
    assert_contains!(
        error.to_string(),
        "No field named 'f1.c1'. Valid fields are 'test.f.c1', 'test.test.c2'"
    );

    // however, enclosing it in double quotes is ok
    let sql = r#"SELECT "f.c1" from test"#;
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------+",
        "| f.c1   |",
        "+--------+",
        "| foofoo |",
        "| foobar |",
        "| foobaz |",
        "+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    // Works fully qualified too
    let sql = r#"SELECT test."f.c1" from test"#;
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    // check that duplicated table name and column name are ok
    let sql = r#"SELECT "test.c2" as expr1, test."test.c2" as expr2 from test"#;
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+-------+",
        "| expr1 | expr2 |",
        "+-------+-------+",
        "| 1     | 1     |",
        "| 2     | 2     |",
        "| 3     | 3     |",
        "+-------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // check that '....' is also an ok column name (in the sense that
    // datafusion should run the query, not that someone should write
    // this
    let sql = r#"SELECT "....", "...." as c3 from test order by "....""#;
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------+----+",
        "| .... | c3 |",
        "+------+----+",
        "| 10   | 10 |",
        "| 20   | 20 |",
        "| 30   | 30 |",
        "+------+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_partial_qualified_name() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let sql = "SELECT t1.t1_id, t1_name FROM public.t1";
    let expected = vec![
        "+-------+---------+",
        "| t1_id | t1_name |",
        "+-------+---------+",
        "| 11    | a       |",
        "| 22    | b       |",
        "| 33    | c       |",
        "| 44    | d       |",
        "+-------+---------+",
    ];
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}
