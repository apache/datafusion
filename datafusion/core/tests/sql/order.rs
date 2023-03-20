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
use test_utils::{batches_to_vec, partitions_to_sorted_vec};

#[tokio::test]
async fn sort_with_lots_of_repetition_values() -> Result<()> {
    let ctx = SessionContext::new();
    let filename = "tests/parquet/data/repeat_much.snappy.parquet";

    ctx.register_parquet("rep", filename, ParquetReadOptions::default())
        .await?;
    let sql = "select a from rep order by a";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = batches_to_vec(&actual);

    let sql1 = "select a from rep";
    let expected = execute_to_batches(&ctx, sql1).await;
    let expected = partitions_to_sorted_vec(&[expected]);

    assert_eq!(actual.len(), expected.len());
    for i in 0..actual.len() {
        assert_eq!(actual[i], expected[i]);
    }
    Ok(())
}

#[tokio::test]
async fn sort_with_duplicate_sort_exprs() -> Result<()> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2, 4, 9, 3, 4])),
            Arc::new(StringArray::from_slice(["a", "b", "c", "d", "e"])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let sql = "select * from t1 order by id desc, id, name, id asc";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan().unwrap();
    let expected = vec![
        "Sort: t1.id DESC NULLS FIRST, t1.name ASC NULLS LAST [id:Int32;N, name:Utf8;N]",
        "  TableScan: t1 projection=[id, name] [id:Int32;N, name:Utf8;N]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 9  | c    |",
        "| 4  | b    |",
        "| 4  | e    |",
        "| 3  | d    |",
        "| 2  | a    |",
        "+----+------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &results);

    let sql = "select * from t1 order by id asc, id, name, id desc;";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan().unwrap();
    let expected = vec![
        "Sort: t1.id ASC NULLS LAST, t1.name ASC NULLS LAST [id:Int32;N, name:Utf8;N]",
        "  TableScan: t1 projection=[id, name] [id:Int32;N, name:Utf8;N]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = vec![
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 2  | a    |",
        "| 3  | d    |",
        "| 4  | b    |",
        "| 4  | e    |",
        "| 9  | c    |",
        "+----+------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &results);

    Ok(())
}
