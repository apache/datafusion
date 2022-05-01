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

use std::sync::Arc;

use arrow::{array::StringArray, record_batch::RecordBatch};
use datafusion::{
    assert_batches_sorted_eq, assert_contains, datasource::MemTable, prelude::*,
};

use crate::sql::plan_and_collect;

#[tokio::test]
async fn normalized_column_identifiers() {
    // create local execution context
    let ctx = SessionContext::new();

    // register csv file with the execution context
    ctx.register_csv(
        "case_insensitive_test",
        "tests/example.csv",
        CsvReadOptions::new(),
    )
    .await
    .unwrap();

    let sql = "SELECT A, b FROM case_insensitive_test";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = "SELECT t.A, b FROM case_insensitive_test AS t";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Aliases

    let sql = "SELECT t.A as x, b FROM case_insensitive_test AS t";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = "SELECT t.A AS X, b FROM case_insensitive_test AS t";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = r#"SELECT t.A AS "X", b FROM case_insensitive_test AS t"#;
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| X | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Order by

    let sql = "SELECT t.A AS x, b FROM case_insensitive_test AS t ORDER BY x";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = "SELECT t.A AS x, b FROM case_insensitive_test AS t ORDER BY X";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = r#"SELECT t.A AS "X", b FROM case_insensitive_test AS t ORDER BY "X""#;
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| X | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Where

    let sql = "SELECT a, b FROM case_insensitive_test where A IS NOT null";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Group by

    let sql = "SELECT a as x, count(*) as c FROM case_insensitive_test GROUP BY X";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | c |",
        "+---+---+",
        "| 1 | 1 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = r#"SELECT a as "X", count(*) as c FROM case_insensitive_test GROUP BY "X""#;
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| X | c |",
        "+---+---+",
        "| 1 | 1 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn case_insensitive_in_sql_errors() {
    let record_batch = RecordBatch::try_from_iter(vec![
        // The proper way to refer to this column is "Column1" -- it
        // should not be possible to use `column1` or `COLUMN1` or
        // other variants
        (
            "Column1",
            Arc::new(StringArray::from(vec!["content1"])) as _,
        ),
    ])
    .unwrap();

    let table =
        MemTable::try_new(record_batch.schema(), vec![vec![record_batch]]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table)).unwrap();

    // None of these tests shoud pass
    let actual = ctx
        .sql("SELECT COLumn1 from test")
        .await
        .unwrap_err()
        .to_string();
    assert_contains!(actual, "No field named 'column1'");

    let actual = ctx
        .sql("SELECT Column1 from test")
        .await
        .unwrap_err()
        .to_string();
    assert_contains!(actual, "No field named 'column1'");

    let actual = ctx
        .sql("SELECT column1 from test")
        .await
        .unwrap_err()
        .to_string();
    assert_contains!(actual, "No field named 'column1'");

    let actual = ctx
        .sql(r#"SELECT "column1" from test"#)
        .await
        .unwrap_err()
        .to_string();
    assert_contains!(actual, "No field named 'column1'");

    // This should pass (note the quotes)
    ctx.sql(r#"SELECT "Column1" from test"#).await.unwrap();
}
