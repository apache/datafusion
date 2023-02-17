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
    println!("{:#?}", dataframe.logical_plan());
    let physical_plan = dataframe.create_physical_plan().await?;
    let formatted = displayable(physical_plan.as_ref()).indent().to_string();
    let expected = {
        vec![
            "GlobalLimitExec: skip=0, fetch=5",
            "  SortPreservingMergeExec: [c9@1 DESC]",
            "    SortExec: fetch=5, expr=[c9@1 DESC]",
            "      UnionExec",
            "        ProjectionExec: expr=[c1@0 as c1, CAST(c9@1 AS Int64) as c9]",
            "          CsvExec: files={1 group: [[Users/berkaysahin/Desktop/arrow-datafusion/testing/data/csv/aggregate_test_100.csv]]}, has_header=true, limit=None, projection=[c1, c9]",
            "        ProjectionExec: expr=[c1@0 as c1, CAST(c3@1 AS Int64) as c9]",
            "          CsvExec: files={1 group: [[Users/berkaysahin/Desktop/arrow-datafusion/testing/data/csv/aggregate_test_100.csv]]}, has_header=true, limit=None, projection=[c1, c3]",
        ]
    };
    let actual: Vec<&str> = formatted.trim().lines().collect();
    let actual_len = actual.len();
    let actual_trim_last = &actual[..actual_len];
    assert_eq!(
        expected, actual_trim_last,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual_trim_last:#?}\n\n"
    );

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
