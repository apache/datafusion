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
        "      Aggregate: groupBy=[[t1.name]], aggr=[[]] [name:UInt8;N]",
        "        TableScan: t1 projection=[name] [name:UInt8;N]",
        "      TableScan: t2 projection=[name] [name:UInt8;N]",
        "    LeftAnti Join: t2.name = t1.name [name:UInt8;N]",
        "      Aggregate: groupBy=[[t2.name]], aggr=[[]] [name:UInt8;N]",
        "        TableScan: t2 projection=[name] [name:UInt8;N]",
        "      TableScan: t1 projection=[name] [name:UInt8;N]",
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
        "      Aggregate: groupBy=[[t1.id, t1.name]], aggr=[[]] [id:Int32;N, name:UInt8;N]",
        "        TableScan: t1 projection=[id, name] [id:Int32;N, name:UInt8;N]",
        "      TableScan: t2 projection=[id, name] [id:UInt8;N, name:UInt8;N]",
        "    Projection: CAST(t2.id AS Int32) AS id, t2.name [id:Int32;N, name:UInt8;N]",
        "      LeftAnti Join: CAST(t2.id AS Int32) = t1.id, t2.name = t1.name [id:UInt8;N, name:UInt8;N]",
        "        Aggregate: groupBy=[[t2.id, t2.name]], aggr=[[]] [id:UInt8;N, name:UInt8;N]",
        "          TableScan: t2 projection=[id, name] [id:UInt8;N, name:UInt8;N]",
        "        TableScan: t1 projection=[id, name] [id:Int32;N, name:UInt8;N]",
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
async fn test_union_upcast_types() -> Result<()> {
    let config = SessionConfig::new()
        .with_repartition_windows(false)
        .with_target_partitions(1);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, c9 FROM aggregate_test_100 
                     UNION ALL 
                     SELECT c1, c3 FROM aggregate_test_100 
                     ORDER BY c9 DESC LIMIT 5";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);

    let expected_logical_plan = vec![
        "Limit: skip=0, fetch=5 [c1:Utf8, c9:Int64]",
        "  Sort: c9 DESC NULLS FIRST [c1:Utf8, c9:Int64]",
        "    Union [c1:Utf8, c9:Int64]",
        "      Projection: aggregate_test_100.c1, CAST(aggregate_test_100.c9 AS Int64) AS c9 [c1:Utf8, c9:Int64]",
        "        TableScan: aggregate_test_100 [c1:Utf8, c2:UInt32, c3:Int8, c4:Int16, c5:Int32, c6:Int64, c7:UInt8, c8:UInt16, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
        "      Projection: aggregate_test_100.c1, CAST(aggregate_test_100.c3 AS Int64) AS c9 [c1:Utf8, c9:Int64]",
        "        TableScan: aggregate_test_100 [c1:Utf8, c2:UInt32, c3:Int8, c4:Int16, c5:Int32, c6:Int64, c7:UInt8, c8:UInt16, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
    ];
    let formatted_logical_plan =
        dataframe.logical_plan().display_indent_schema().to_string();
    let actual_logical_plan: Vec<&str> = formatted_logical_plan.trim().lines().collect();
    assert_eq!(expected_logical_plan, actual_logical_plan, "\n\nexpected:\n\n{expected_logical_plan:#?}\nactual:\n\n{actual_logical_plan:#?}\n\n");

    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+----+------------+",
        "| c1 | c9         |",
        "+----+------------+",
        "| c  | 4268716378 |",
        "| e  | 4229654142 |",
        "| d  | 4216440507 |",
        "| e  | 4144173353 |",
        "| b  | 4076864659 |",
        "+----+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}
