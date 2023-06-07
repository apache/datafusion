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
async fn sqrt_f32_vs_f64() -> Result<()> {
    let ctx = create_ctx();
    register_aggregate_csv(&ctx).await?;
    // sqrt(f32)'s plan passes
    let sql = "SELECT avg(sqrt(c11)) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let sql = "SELECT avg(CAST(sqrt(c11) AS double)) FROM aggregate_test_100";
    let expected = execute(&ctx, sql).await;

    assert_eq!(actual, expected);
    let sql = "SELECT avg(sqrt(CAST(c11 AS double))) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["0.6584408483418833"]];
    assert_float_eq(&expected, &actual);
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
        "| 1.0       |",
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
    assert!(err
        .to_string()
        .contains("Error during planning: Invalid function 'SQRT'"));

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
            Arc::new(Int8Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::Int16,
            Arc::new(Int16Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::Int32,
            Arc::new(Int32Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::Int64,
            Arc::new(Int64Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::UInt8,
            Arc::new(UInt8Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::UInt16,
            Arc::new(UInt16Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::UInt32,
            Arc::new(UInt32Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::UInt64,
            Arc::new(UInt64Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::Float32,
            Arc::new(Float32Array::from_slice([1.0_f32])) as ArrayRef,
        ),
        (
            DataType::Float64,
            Arc::new(Float64Array::from_slice([1.0_f64])) as ArrayRef,
        ),
    ];

    for (data_type, array) in type_values.iter() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", data_type.clone(), false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
        ctx.deregister_table("t").unwrap();
        ctx.register_batch("t", batch).unwrap();
        let expected = vec![
            "+-----------+",
            "| sqrt(t.v) |",
            "+-----------+",
            "| 1.0       |",
            "+-----------+",
        ];
        let results = plan_and_collect(&ctx, "SELECT sqrt(v) FROM t")
            .await
            .unwrap();

        assert_batches_sorted_eq!(expected, &results);
    }
}
