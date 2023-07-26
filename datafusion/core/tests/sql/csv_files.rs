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
async fn csv_custom_quote() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Utf8, false),
    ]));
    let filename = format!("partition.{}", "csv");
    let file_path = tmp_dir.path().join(filename);
    let mut file = File::create(file_path)?;

    // generate some data
    for index in 0..10 {
        let text1 = format!("id{index:}");
        let text2 = format!("value{index:}");
        let data = format!("~{text1}~,~{text2}~\r\n");
        file.write_all(data.as_bytes())?;
    }
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new()
            .schema(&schema)
            .has_header(false)
            .quote(b'~'),
    )
    .await?;

    let results = plan_and_collect(&ctx, "SELECT * from test").await?;

    let expected = vec![
        "+-----+--------+",
        "| c1  | c2     |",
        "+-----+--------+",
        "| id0 | value0 |",
        "| id1 | value1 |",
        "| id2 | value2 |",
        "| id3 | value3 |",
        "| id4 | value4 |",
        "| id5 | value5 |",
        "| id6 | value6 |",
        "| id7 | value7 |",
        "| id8 | value8 |",
        "| id9 | value9 |",
        "+-----+--------+",
    ];

    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn csv_custom_escape() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Utf8, false),
    ]));
    let filename = format!("partition.{}", "csv");
    let file_path = tmp_dir.path().join(filename);
    let mut file = File::create(file_path)?;

    // generate some data
    for index in 0..10 {
        let text1 = format!("id{index:}");
        let text2 = format!("value\\\"{index:}");
        let data = format!("\"{text1}\",\"{text2}\"\r\n");
        file.write_all(data.as_bytes())?;
    }

    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new()
            .schema(&schema)
            .has_header(false)
            .escape(b'\\'),
    )
    .await?;

    let results = plan_and_collect(&ctx, "SELECT * from test").await?;

    let expected = vec![
        "+-----+---------+",
        "| c1  | c2      |",
        "+-----+---------+",
        "| id0 | value\"0 |",
        "| id1 | value\"1 |",
        "| id2 | value\"2 |",
        "| id3 | value\"3 |",
        "| id4 | value\"4 |",
        "| id5 | value\"5 |",
        "| id6 | value\"6 |",
        "| id7 | value\"7 |",
        "| id8 | value\"8 |",
        "| id9 | value\"9 |",
        "+-----+---------+",
    ];

    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}
