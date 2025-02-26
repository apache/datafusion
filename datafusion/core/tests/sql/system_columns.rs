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
use datafusion_common::FieldExt;

#[tokio::test]
async fn test_system_column_select() {
    let ctx = setup_test_context().await;

    // System columns are not included in wildcard select
    let select = "SELECT * FROM test order by id";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+--------------+",
        "| id | bank_account |",
        "+----+--------------+",
        "| 1  | 9000         |",
        "| 2  | 100          |",
        "| 3  | 1000         |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // But they are if explicitly selected
    let select = "SELECT _rowid FROM test order by _rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+--------+",
        "| _rowid |",
        "+--------+",
        "| 0      |",
        "| 1      |",
        "| 2      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // They can be selected alongside regular columns
    let select = "SELECT _rowid, id FROM test order by _rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+--------+----+",
        "| _rowid | id |",
        "+--------+----+",
        "| 0      | 1  |",
        "| 1      | 2  |",
        "| 2      | 3  |",
        "+--------+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // As well as wildcard select
    let select = "SELECT *, _rowid FROM test order by _rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+--------------+--------+",
        "| id | bank_account | _rowid |",
        "+----+--------------+--------+",
        "| 1  | 9000         | 0      |",
        "| 2  | 100          | 1      |",
        "| 3  | 1000         | 2      |",
        "+----+--------------+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_filtering() {
    let ctx = setup_test_context().await;

    // Filter by exact _rowid
    let select = "SELECT _rowid, id FROM test WHERE _rowid = 0";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+--------+----+",
        "| _rowid | id |",
        "+--------+----+",
        "| 0      | 1  |",
        "+--------+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Filter by _rowid with and operator
    let select = "SELECT _rowid, id FROM test WHERE _rowid % 2 = 1";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+--------+----+",
        "| _rowid | id |",
        "+--------+----+",
        "| 1      | 2  |",
        "+--------+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Filter without selecting
    let select = "SELECT id FROM test WHERE _rowid = 0";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| id |",
        "+----+",
        "| 1  |",
        "+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_ordering() {
    let ctx = setup_test_context().await;

    let select = "SELECT id FROM test order by _rowid asc";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| id |",
        "+----+",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_joins() {
    let ctx = setup_test_context().await;

    let batch = record_batch!(
        ("other_id", UInt8, [1, 2, 3]),
        ("bank_account", UInt64, [9, 10, 11]),
        ("_rowid", UInt32, [0, 1, 3])
    )
    .unwrap();
    let _ = ctx.register_batch("test2", batch);

    let batch = record_batch!(
        ("other_id", UInt8, [1, 2, 3]),
        ("bank_account", UInt64, [9, 10, 11]),
        ("_rowid", UInt32, [0, 1, 3])
    )
    .unwrap();
    let batch = batch
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("other_id", DataType::UInt8, true),
            Field::new("bank_account", DataType::UInt64, true),
            Field::new("_rowid", DataType::UInt32, true).to_system_column(),
        ])))
        .unwrap();
    let _ = ctx.register_batch("test2sys", batch);

    let select = "SELECT id, other_id FROM test INNER JOIN test2 USING (_rowid)";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+",
        "| id | other_id |",
        "+----+----------+",
        "| 1  | 1        |",
        "| 2  | 2        |",
        "+----+----------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
    let select = "SELECT id, other_id, _rowid FROM test INNER JOIN test2 USING (_rowid)";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+--------+",
        "| id | other_id | _rowid |",
        "+----+----------+--------+",
        "| 1  | 1        | 0      |",
        "| 2  | 2        | 1      |",
        "+----+----------+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
    let select =
        "SELECT id, other_id FROM test LEFT JOIN test2 ON test._rowid = test2._rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+",
        "| id | other_id |",
        "+----+----------+",
        "| 1  | 1        |",
        "| 2  | 2        |",
        "| 3  |          |",
        "+----+----------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
    let select =
        "SELECT id, other_id, _rowid FROM test LEFT JOIN test2 ON test._rowid = test2._rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+--------+",
        "| id | other_id | _rowid |",
        "+----+----------+--------+",
        "| 1  | 1        | 0      |",
        "| 2  | 2        | 1      |",
        "| 3  |          |        |",
        "+----+----------+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    let select =
        "SELECT id, other_id FROM test JOIN test2 ON test._rowid = test2._rowid % 2";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+",
        "| id | other_id |",
        "+----+----------+",
        "| 1  | 1        |",
        "| 2  | 2        |",
        "| 2  | 3        |",
        "+----+----------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Join with non-system _rowid column and select _row_id
    // Normally this would result in an AmbiguousReference error because both tables have a _rowid column
    // But when this conflict is between a system column and a regular column, the regular column is chosen
    // to resolve the conflict without error.
    let select =
        "SELECT id, other_id, _rowid FROM test INNER JOIN test2 ON id = other_id";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+--------+",
        "| id | other_id | _rowid |",
        "+----+----------+--------+",
        "| 1  | 1        | 0      |",
        "| 2  | 2        | 1      |",
        "| 3  | 3        | 3      |",
        "+----+----------+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
    // but if it's a conflict between two system columns we do get an error
    let select =
        "SELECT id, other_id, _rowid FROM test INNER JOIN test2sys ON id = other_id";
    assert!(ctx.sql(select).await.is_err());
    // Same in this case, `test._rowid` is discarded because it is a system column
    let select =
        "SELECT test.*, test2._rowid FROM test INNER JOIN test2 ON id = other_id";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+--------------+--------+",
        "| id | bank_account | _rowid |",
        "+----+--------------+--------+",
        "| 1  | 9000         | 0      |",
        "| 2  | 100          | 1      |",
        "| 3  | 1000         | 3      |",
        "+----+--------------+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Join on system _rowid columns
    let select =
        "SELECT id, other_id FROM test JOIN test2sys ON test._rowid = test2sys._rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+",
        "| id | other_id |",
        "+----+----------+",
        "| 1  | 1        |",
        "| 2  | 2        |",
        "+----+----------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // there should be an ambiguity error since two system columns are joined with the same name
    let select = "SELECT id, other_id FROM test JOIN test2sys ON _rowid = _rowid";
    assert!(ctx.sql(select).await.is_err());
}

#[tokio::test]
async fn test_system_column_with_cte() {
    let ctx = setup_test_context().await;

    // System columns not available after CTE
    let select = r"
        WITH cte AS (SELECT * FROM test)
        SELECT _rowid FROM cte
    ";
    assert!(ctx.sql(select).await.is_err());

    // Explicitly selected system columns become regular columns
    let select = r"
        WITH cte AS (SELECT id, _rowid FROM test)
        SELECT * FROM cte
    ";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+--------+",
        "| id | _rowid |",
        "+----+--------+",
        "| 1  | 0      |",
        "| 2  | 1      |",
        "| 3  | 2      |",
        "+----+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_in_subquery() {
    let ctx = setup_test_context().await;

    // System columns not available in subquery
    let select = r"
        SELECT _rowid FROM (SELECT * FROM test)
    ";
    assert!(ctx.sql(select).await.is_err());

    // Explicitly selected system columns become regular columns
    let select = r"
        SELECT * FROM (SELECT id, _rowid FROM test)
    ";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+--------+",
        "| id | _rowid |",
        "+----+--------+",
        "| 1  | 0      |",
        "| 2  | 1      |",
        "| 3  | 2      |",
        "+----+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

async fn setup_test_context() -> SessionContext {
    let batch = record_batch!(
        ("id", UInt8, [1, 2, 3]),
        ("bank_account", UInt64, [9000, 100, 1000]),
        ("_rowid", UInt32, [0, 1, 2]),
        ("_file", Utf8, ["file-0", "file-1", "file-2"])
    )
    .unwrap();
    let batch = batch
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt8, true),
            Field::new("bank_account", DataType::UInt64, true),
            Field::new("_rowid", DataType::UInt32, true).to_system_column(),
            Field::new("_file", DataType::Utf8, true).to_system_column(),
        ])))
        .unwrap();

    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_information_schema(true),
    );
    let _ = ctx.register_batch("test", batch);
    ctx
}
