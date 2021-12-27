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

/// sqrt(f32) is slightly different than sqrt(CAST(f32 AS double)))
#[tokio::test]
async fn sqrt_f32_vs_f64() -> Result<()> {
    let mut ctx = create_ctx()?;
    register_aggregate_csv(&mut ctx).await?;
    // sqrt(f32)'s plan passes
    let sql = "SELECT avg(sqrt(c11)) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["0.6584407806396484"]];

    assert_eq!(actual, expected);
    let sql = "SELECT avg(sqrt(CAST(c11 AS double))) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["0.6584408483418833"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_cast() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT CAST(c12 AS float) FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----------------------------------------+",
        "| CAST(aggregate_test_100.c12 AS Float32) |",
        "+-----------------------------------------+",
        "| 0.39144436                              |",
        "| 0.3887028                               |",
        "+-----------------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_cast_literal() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT c12, CAST(1 AS float) FROM aggregate_test_100 WHERE c12 > CAST(0 AS float) LIMIT 2";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+--------------------+---------------------------+",
        "| c12                | CAST(Int64(1) AS Float32) |",
        "+--------------------+---------------------------+",
        "| 0.9294097332465232 | 1                         |",
        "| 0.3114712539863804 | 1                         |",
        "+--------------------+---------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_concat() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["", "a", "aa", "aaa"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT concat(c1, '-hi-', cast(c2 as varchar)) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------------------------------+",
        "| concat(test.c1,Utf8(\"-hi-\"),CAST(test.c2 AS Utf8)) |",
        "+----------------------------------------------------+",
        "| -hi-0                                              |",
        "| a-hi-1                                             |",
        "| aa-hi-                                             |",
        "| aaa-hi-3                                           |",
        "+----------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

// Revisit after implementing https://github.com/apache/arrow-rs/issues/925
#[tokio::test]
async fn query_array() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["", "a", "aa", "aaa"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT array(c1, cast(c2 as varchar)) FROM test";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![
        vec!["[,0]"],
        vec!["[a,1]"],
        vec!["[aa,NULL]"],
        vec!["[aaa,3]"],
    ];
    assert_eq!(expected, actual);
    Ok(())
}

#[tokio::test]
async fn query_count_distinct() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
            Some(3),
        ]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COUNT(DISTINCT c1) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------+",
        "| COUNT(DISTINCT test.c1) |",
        "+-------------------------+",
        "| 3                       |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
