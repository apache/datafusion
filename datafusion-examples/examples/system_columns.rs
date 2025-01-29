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

use arrow::array::record_batch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use datafusion::common::FieldExt;
use datafusion::{assert_batches_eq, prelude::*};

/// This example shows how to mark fields as system columns.
/// System columns are columns which meant to be semi-public stores of the internal details of the table.
/// For example, `ctid` in Postgres would be considered a metadata column
/// (Postgres calls these "system columns", see [the Postgres docs](https://www.postgresql.org/docs/current/ddl-system-columns.html) for more information and examples.
/// Spark has a `_metadata` column that it uses to include details about each file read in a query (see [Spark's docs](https://docs.databricks.com/en/ingestion/file-metadata-column.html)).
///
/// DataFusion allows fields to be declared as metadata columns by setting the `datafusion.system_column` key in the field's metadata
/// to `true`.
///
/// As an example of how this works in practice, if you have the following Postgres table:
///
/// ```sql
/// CREATE TABLE t (x int);
/// INSERT INTO t VALUES (1);
/// ```
///
/// And you do a `SELECT * FROM t`, you would get the following schema:
///
/// ```text
/// +---+
/// | x |
/// +---+
/// | 1 |
/// +---+
/// ```
///
/// But if you do `SELECT ctid, * FROM t`, you would get the following schema (ignore the meaning of the value of `ctid`, this is just an example):
///
/// ```text
/// +-----+---+
/// | ctid| x |
/// +-----+---+
/// | 0   | 1 |
/// +-----+---+
/// ```
#[tokio::main]
async fn main() {
    let batch = record_batch!(
        ("a", Int32, [1, 2, 3]),
        ("b", Utf8, ["foo", "bar", "baz"]),
        ("_row_num", UInt32, [1, 2, 3])
    )
    .unwrap();
    let batch = batch
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("_row_num", DataType::UInt32, true).as_system_column(),
        ])))
        .unwrap();

    let ctx = SessionContext::new();
    let _ = ctx.register_batch("t", batch);

    let res = ctx
        .sql("SELECT a, b FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    #[rustfmt::skip]
    let expected: Vec<&str> = vec![
        "+---+-----+",
        "| a | b   |",
        "+---+-----+",
        "| 1 | foo |",
        "| 2 | bar |",
        "| 3 | baz |",
        "+---+-----+",
    ];
    assert_batches_eq!(expected, &res);

    let res = ctx
        .sql("SELECT _row_num FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    #[rustfmt::skip]
    let expected: Vec<&str> = vec![
        "+----------+",
        "| _row_num |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "| 3        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &res);

    let res = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // does not include _row_num
    #[rustfmt::skip]
    let expected: Vec<&str> = vec![
        "+---+-----+",
        "| a | b   |",
        "+---+-----+",
        "| 1 | foo |",
        "| 2 | bar |",
        "| 3 | baz |",
        "+---+-----+",
    ];
    assert_batches_eq!(expected, &res);

    let res = ctx
        .sql("SELECT *, _row_num FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    #[rustfmt::skip]
    let expected: Vec<&str> = vec![
        "+---+-----+----------+",
        "| a | b   | _row_num |",
        "+---+-----+----------+",
        "| 1 | foo | 1        |",
        "| 2 | bar | 2        |",
        "| 3 | baz | 3        |",
        "+---+-----+----------+",
    ];
    assert_batches_eq!(expected, &res);

    let res = ctx
        .sql("SELECT t._row_num FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    #[rustfmt::skip]
    let expected: Vec<&str> = vec![
        "+----------+",
        "| _row_num |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "| 3        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &res);

    let res = ctx
        .sql("SELECT t.* FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // does not include _row_num
    #[rustfmt::skip]
    let expected: Vec<&str> = vec![
        "+---+-----+",
        "| a | b   |",
        "+---+-----+",
        "| 1 | foo |",
        "| 2 | bar |",
        "| 3 | baz |",
        "+---+-----+",
    ];
    assert_batches_eq!(expected, &res);

    let res = ctx
        .sql("SELECT t.*, _row_num FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    #[rustfmt::skip]
    let expected: Vec<&str> = vec![
        "+---+-----+----------+",
        "| a | b   | _row_num |",
        "+---+-----+----------+",
        "| 1 | foo | 1        |",
        "| 2 | bar | 2        |",
        "| 3 | baz | 3        |",
        "+---+-----+----------+",
    ];
    assert_batches_eq!(expected, &res);
}
