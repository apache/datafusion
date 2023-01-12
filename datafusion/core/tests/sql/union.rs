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
async fn union_all() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT 1 as x UNION ALL SELECT 2 as x";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| x |",
        "+---+",
        "| 1 |",
        "| 2 |",
        "+---+"
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_union_all() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "SELECT c1 FROM aggregate_test_100 UNION ALL SELECT c1 FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    assert_eq!(actual.len(), 200);
    Ok(())
}

#[tokio::test]
async fn union_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT 1 as x UNION SELECT 1 as x";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| x |",
        "+---+",
        "| 1 |",
        "+---+"
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn union_all_with_aggregate() -> Result<()> {
    let ctx = SessionContext::new();
    let sql =
        "SELECT SUM(d) FROM (SELECT 1 as c, 2 as d UNION ALL SELECT 1 as c, 3 AS d) as a";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| SUM(a.d) |",
        "+----------+",
        "| 5        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn union_all_with_count() -> Result<()> {
    let ctx = SessionContext::new();
    execute_to_batches(&ctx, "CREATE table t as SELECT 1 as a").await;
    let sql = "SELECT COUNT(*) FROM (SELECT a from t UNION ALL SELECT a from t)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn union_schemas() -> Result<()> {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    let result = ctx
        .sql("SELECT 1 A UNION ALL SELECT 2 order by 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| a |",
        "+---+",
        "| 1 |",
        "| 2 |",
        "+---+"
    ];
    assert_batches_eq!(expected, &result);

    let result = ctx
        .sql("SELECT 1 UNION SELECT 2 order by 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = vec![
        "+----------+",
        "| Int64(1) |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &result);
    Ok(())
}

#[tokio::test]
async fn union_with_except_input() -> Result<()> {
    let ctx = create_union_context()?;
    let sql = "(
        SELECT name FROM t1
        EXCEPT
        SELECT name FROM t2
    )
    UNION ALL
    (
        SELECT name FROM t2
        EXCEPT
        SELECT name FROM t1
    )";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Union [name:UInt8;N]",
        "    LeftAnti Join: t1.name = t2.name [name:UInt8;N]",
        "      Distinct: [name:UInt8;N]",
        "        Projection: t1.name [name:UInt8;N]",
        "          TableScan: t1 projection=[name] [name:UInt8;N]",
        "      Projection: t2.name [name:UInt8;N]",
        "        TableScan: t2 projection=[name] [name:UInt8;N]",
        "    LeftAnti Join: t2.name = t1.name [name:UInt8;N]",
        "      Distinct: [name:UInt8;N]",
        "        Projection: t2.name [name:UInt8;N]",
        "          TableScan: t2 projection=[name] [name:UInt8;N]",
        "      Projection: t1.name [name:UInt8;N]",
        "        TableScan: t1 projection=[name] [name:UInt8;N]",
    ];

    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Union [name:UInt8;N]",
        "    LeftAnti Join: t1.name = t2.name [name:UInt8;N]",
        "      Distinct: [name:UInt8;N]",
        "        TableScan: t1 projection=[name] [name:UInt8;N]",
        "      Projection: t2.name [name:UInt8;N]",
        "        TableScan: t2 projection=[name] [name:UInt8;N]",
        "    LeftAnti Join: t2.name = t1.name [name:UInt8;N]",
        "      Distinct: [name:UInt8;N]",
        "        TableScan: t2 projection=[name] [name:UInt8;N]",
        "      Projection: t1.name [name:UInt8;N]",
        "        TableScan: t1 projection=[name] [name:UInt8;N]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    Ok(())
}

#[tokio::test]
async fn union_with_type_coercion() -> Result<()> {
    let ctx = create_union_context()?;
    let sql = "(
        SELECT id, name FROM t1
        EXCEPT
        SELECT id, name FROM t2
    )
    UNION ALL
    (
        SELECT id, name FROM t2
        EXCEPT
        SELECT id, name FROM t1
    )";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Union [id:Int32;N, name:UInt8;N]",
        "    LeftAnti Join: t1.id = CAST(t2.id AS Int32), t1.name = t2.name [id:Int32;N, name:UInt8;N]",
        "      Distinct: [id:Int32;N, name:UInt8;N]",
        "        TableScan: t1 projection=[id, name] [id:Int32;N, name:UInt8;N]",
        "      Projection: t2.id, t2.name [id:UInt8;N, name:UInt8;N]",
        "        TableScan: t2 projection=[id, name] [id:UInt8;N, name:UInt8;N]",
        "    Projection: CAST(t2.id AS Int32) AS id, t2.name [id:Int32;N, name:UInt8;N]",
        "      LeftAnti Join: CAST(t2.id AS Int32) = t1.id, t2.name = t1.name [id:UInt8;N, name:UInt8;N]",
        "        Distinct: [id:UInt8;N, name:UInt8;N]",
        "          TableScan: t2 projection=[id, name] [id:UInt8;N, name:UInt8;N]",
        "        Projection: t1.id, t1.name [id:Int32;N, name:UInt8;N]",
        "          TableScan: t1 projection=[id, name] [id:Int32;N, name:UInt8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    Ok(())
}
