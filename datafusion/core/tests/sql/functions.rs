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
    let ctx = create_ctx()?;
    register_aggregate_csv(&ctx).await?;
    // sqrt(f32)'s plan passes
    let sql = "SELECT avg(sqrt(c11)) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["0.6584407806396484"]];

    assert_eq!(actual, expected);
    let sql = "SELECT avg(sqrt(CAST(c11 AS double))) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["0.6584408483418833"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_cast() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT CAST(c12 AS float) FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "SELECT c12, CAST(1 AS float) FROM aggregate_test_100 WHERE c12 > CAST(0 AS float) LIMIT 2";
    let actual = execute_to_batches(&ctx, sql).await;

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
            Arc::new(StringArray::from_slice(&["", "a", "aa", "aaa"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT concat(c1, '-hi-', cast(c2 as varchar)) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
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
            Arc::new(StringArray::from_slice(&["", "a", "aa", "aaa"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT array(c1, cast(c2 as varchar)) FROM test";
    let actual = execute(&ctx, sql).await;
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
async fn query_array_scalar() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "SELECT array(1, 2, 3);";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["[1, 2, 3]"]];
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

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COUNT(DISTINCT c1) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
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

#[tokio::test]
async fn coalesce_static_empty_value() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT COALESCE('', 'test')";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------------+",
        "| coalesce(Utf8(\"\"),Utf8(\"test\")) |",
        "+---------------------------------+",
        "|                                 |",
        "+---------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_static_value_with_null() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT COALESCE(NULL, 'test')";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------------------+",
        "| coalesce(Utf8(NULL),Utf8(\"test\")) |",
        "+-----------------------------------+",
        "| test                              |",
        "+-----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_result() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(0), None, Some(1), None, None])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(0),
                Some(1),
                None,
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COALESCE(c1, c2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------+",
        "| coalesce(test.c1,test.c2) |",
        "+---------------------------+",
        "| 0                         |",
        "| 1                         |",
        "| 1                         |",
        "| 1                         |",
        "|                           |",
        "+---------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_result_with_default_value() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(0), None, Some(1), None, None])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(0),
                Some(1),
                None,
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COALESCE(c1, c2, '-1') FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------+",
        "| coalesce(test.c1,test.c2,Utf8(\"-1\")) |",
        "+--------------------------------------+",
        "| 0                                    |",
        "| 1                                    |",
        "| 1                                    |",
        "| 1                                    |",
        "| -1                                   |",
        "+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_sum_with_default_value() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(1), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(2), None, None])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT SUM(COALESCE(c1, c2, 0)) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------------------------+",
        "| SUM(coalesce(test.c1,test.c2,Int64(0))) |",
        "+-----------------------------------------+",
        "| 4                                       |",
        "+-----------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_mul_with_default_value() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(1), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(2), None, None])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COALESCE(c1 * c2, 0) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------------------------+",
        "| coalesce(test.c1 Multiply test.c2,Int64(0)) |",
        "+---------------------------------------------+",
        "| 2                                           |",
        "| 0                                           |",
        "| 0                                           |",
        "| 0                                           |",
        "+---------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn count_basic() -> Result<()> {
    let results =
        execute_with_partition("SELECT COUNT(c1), COUNT(c2) FROM test", 1).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----------------+----------------+",
        "| COUNT(test.c1) | COUNT(test.c2) |",
        "+----------------+----------------+",
        "| 10             | 10             |",
        "+----------------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn count_partitioned() -> Result<()> {
    let results =
        execute_with_partition("SELECT COUNT(c1), COUNT(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----------------+----------------+",
        "| COUNT(test.c1) | COUNT(test.c2) |",
        "+----------------+----------------+",
        "| 40             | 40             |",
        "+----------------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn count_aggregated() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, COUNT(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+----------------+",
        "| c1 | COUNT(test.c2) |",
        "+----+----------------+",
        "| 0  | 10             |",
        "| 1  | 10             |",
        "| 2  | 10             |",
        "| 3  | 10             |",
        "+----+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn simple_avg() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice(&[1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice(&[4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let result = plan_and_collect(&ctx, "SELECT AVG(a) FROM t").await?;

    let batch = &result[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(1, batch.num_rows());

    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("failed to cast version");
    assert_eq!(values.len(), 1);
    // avg(1,2,3,4,5) = 3.0
    assert_eq!(values.value(0), 3.0_f64);
    Ok(())
}

#[tokio::test]
async fn case_sensitive_identifiers_functions() {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let expected = vec![
        "+-----------+",
        "| sqrt(t.i) |",
        "+-----------+",
        "| 1         |",
        "+-----------+",
    ];

    let results = plan_and_collect(&ctx, "SELECT sqrt(i) FROM t")
        .await
        .unwrap();

    assert_batches_sorted_eq!(expected, &results);

    let results = plan_and_collect(&ctx, "SELECT SQRT(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);

    // Using double quotes allows specifying the function name with capitalization
    let err = plan_and_collect(&ctx, "SELECT \"SQRT\"(i) FROM t")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Error during planning: Invalid function 'SQRT'"
    );

    let results = plan_and_collect(&ctx, "SELECT \"sqrt\"(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);
}

#[tokio::test]
async fn case_builtin_math_expression() {
    let ctx = SessionContext::new();

    let type_values = vec![
        (
            DataType::Int8,
            Arc::new(Int8Array::from_slice(&[1])) as ArrayRef,
        ),
        (
            DataType::Int16,
            Arc::new(Int16Array::from_slice(&[1])) as ArrayRef,
        ),
        (
            DataType::Int32,
            Arc::new(Int32Array::from_slice(&[1])) as ArrayRef,
        ),
        (
            DataType::Int64,
            Arc::new(Int64Array::from_slice(&[1])) as ArrayRef,
        ),
        (
            DataType::UInt8,
            Arc::new(UInt8Array::from_slice(&[1])) as ArrayRef,
        ),
        (
            DataType::UInt16,
            Arc::new(UInt16Array::from_slice(&[1])) as ArrayRef,
        ),
        (
            DataType::UInt32,
            Arc::new(UInt32Array::from_slice(&[1])) as ArrayRef,
        ),
        (
            DataType::UInt64,
            Arc::new(UInt64Array::from_slice(&[1])) as ArrayRef,
        ),
        (
            DataType::Float32,
            Arc::new(Float32Array::from_slice(&[1.0_f32])) as ArrayRef,
        ),
        (
            DataType::Float64,
            Arc::new(Float64Array::from_slice(&[1.0_f64])) as ArrayRef,
        ),
    ];

    for (data_type, array) in type_values.iter() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", data_type.clone(), false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.deregister_table("t").unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        let expected = vec![
            "+-----------+",
            "| sqrt(t.v) |",
            "+-----------+",
            "| 1         |",
            "+-----------+",
        ];
        let results = plan_and_collect(&ctx, "SELECT sqrt(v) FROM t")
            .await
            .unwrap();

        assert_batches_sorted_eq!(expected, &results);
    }
}

#[tokio::test]
async fn case_sensitive_identifiers_aggregates() {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let expected = vec![
        "+----------+",
        "| MAX(t.i) |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];

    let results = plan_and_collect(&ctx, "SELECT max(i) FROM t")
        .await
        .unwrap();

    assert_batches_sorted_eq!(expected, &results);

    let results = plan_and_collect(&ctx, "SELECT MAX(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);

    // Using double quotes allows specifying the function name with capitalization
    let err = plan_and_collect(&ctx, "SELECT \"MAX\"(i) FROM t")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Error during planning: Invalid function 'MAX'"
    );

    let results = plan_and_collect(&ctx, "SELECT \"max\"(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);
}
