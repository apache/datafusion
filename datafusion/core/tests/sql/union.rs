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
