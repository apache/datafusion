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
use datafusion_common::ScalarValue;

#[tokio::test]
async fn test_list_query_parameters() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = create_ctx_with_partition(&tmp_dir, partition_count).await?;

    let results = ctx
        .sql("SELECT * FROM test WHERE c1 = $1")
        .await?
        .with_param_values(vec![ScalarValue::from(3i32)])?
        .collect()
        .await?;
    let expected = vec![
        "+----+----+-------+",
        "| c1 | c2 | c3    |",
        "+----+----+-------+",
        "| 3  | 1  | false |",
        "| 3  | 10 | true  |",
        "| 3  | 2  | true  |",
        "| 3  | 3  | false |",
        "| 3  | 4  | true  |",
        "| 3  | 5  | false |",
        "| 3  | 6  | true  |",
        "| 3  | 7  | false |",
        "| 3  | 8  | true  |",
        "| 3  | 9  | false |",
        "+----+----+-------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn test_named_query_parameters() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = create_ctx_with_partition(&tmp_dir, partition_count).await?;

    // sql to statement then to logical plan with parameters
    // c1 defined as UINT32, c2 defined as UInt64
    let results = ctx
        .sql("SELECT c1, c2 FROM test WHERE c1 > $coo AND c1 < $foo")
        .await?
        .with_param_values(vec![
            ("foo", ScalarValue::UInt32(Some(3))),
            ("coo", ScalarValue::UInt32(Some(0))),
        ])?
        .collect()
        .await?;
    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 1  | 1  |",
        "| 1  | 2  |",
        "| 1  | 3  |",
        "| 1  | 4  |",
        "| 1  | 5  |",
        "| 1  | 6  |",
        "| 1  | 7  |",
        "| 1  | 8  |",
        "| 1  | 9  |",
        "| 1  | 10 |",
        "| 2  | 1  |",
        "| 2  | 2  |",
        "| 2  | 3  |",
        "| 2  | 4  |",
        "| 2  | 5  |",
        "| 2  | 6  |",
        "| 2  | 7  |",
        "| 2  | 8  |",
        "| 2  | 9  |",
        "| 2  | 10 |",
        "+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

// Test prepare statement from sql to final result
// This test is equivalent with the test parallel_query_with_filter below but using prepare statement
#[tokio::test]
async fn test_prepare_statement() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = create_ctx_with_partition(&tmp_dir, partition_count).await?;

    // sql to statement then to prepare logical plan with parameters
    // c1 defined as UINT32, c2 defined as UInt64 but the params are Int32 and Float64
    let dataframe =
        ctx.sql("PREPARE my_plan(INT, DOUBLE) AS SELECT c1, c2 FROM test WHERE c1 > $2 AND c1 < $1").await?;

    // prepare logical plan to logical plan without parameters
    let param_values = vec![ScalarValue::Int32(Some(3)), ScalarValue::Float64(Some(0.0))];
    let dataframe = dataframe.with_param_values(param_values)?;
    let results = dataframe.collect().await?;

    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 1  | 1  |",
        "| 1  | 10 |",
        "| 1  | 2  |",
        "| 1  | 3  |",
        "| 1  | 4  |",
        "| 1  | 5  |",
        "| 1  | 6  |",
        "| 1  | 7  |",
        "| 1  | 8  |",
        "| 1  | 9  |",
        "| 2  | 1  |",
        "| 2  | 10 |",
        "| 2  | 2  |",
        "| 2  | 3  |",
        "| 2  | 4  |",
        "| 2  | 5  |",
        "| 2  | 6  |",
        "| 2  | 7  |",
        "| 2  | 8  |",
        "| 2  | 9  |",
        "+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn prepared_statement_type_coercion() -> Result<()> {
    let ctx = SessionContext::new();
    let signed_ints: Int32Array = vec![-1, 0, 1].into();
    let unsigned_ints: UInt64Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_from_iter(vec![
        ("signed", Arc::new(signed_ints) as ArrayRef),
        ("unsigned", Arc::new(unsigned_ints) as ArrayRef),
    ])?;
    ctx.register_batch("test", batch)?;
    let results = ctx.sql("PREPARE my_plan(BIGINT, INT, TEXT) AS SELECT signed, unsigned FROM test WHERE $1 >= signed AND signed <= $2 AND unsigned = $3")
        .await?
        .with_param_values(vec![
            ScalarValue::from(1_i64),
            ScalarValue::from(-1_i32),
            ScalarValue::from("1"),
        ])?
        .collect()
        .await?;
    let expected = [
        "+--------+----------+",
        "| signed | unsigned |",
        "+--------+----------+",
        "| -1     | 1        |",
        "+--------+----------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn prepared_statement_invalid_types() -> Result<()> {
    let ctx = SessionContext::new();
    let signed_ints: Int32Array = vec![-1, 0, 1].into();
    let unsigned_ints: UInt64Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_from_iter(vec![
        ("signed", Arc::new(signed_ints) as ArrayRef),
        ("unsigned", Arc::new(unsigned_ints) as ArrayRef),
    ])?;
    ctx.register_batch("test", batch)?;
    let results = ctx
        .sql("PREPARE my_plan(INT) AS SELECT signed FROM test WHERE signed = $1")
        .await?
        .with_param_values(vec![ScalarValue::from("1")]);
    assert_eq!(
        results.unwrap_err().strip_backtrace(),
        "Error during planning: Expected parameter of type Int32, got Utf8 at index 0"
    );
    Ok(())
}

#[tokio::test]
async fn test_parameter_type_coercion() -> Result<()> {
    let ctx = SessionContext::new();
    let signed_ints: Int32Array = vec![-1, 0, 1].into();
    let unsigned_ints: UInt64Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_from_iter(vec![
        ("signed", Arc::new(signed_ints) as ArrayRef),
        ("unsigned", Arc::new(unsigned_ints) as ArrayRef),
    ])?;
    ctx.register_batch("test", batch)?;
    let results = ctx.sql("SELECT signed, unsigned FROM test WHERE $foo >= signed AND signed <= $bar AND unsigned <= $baz AND unsigned = $str")
        .await?
        .with_param_values(vec![
            ("foo", ScalarValue::from(1_u64)),
            ("bar", ScalarValue::from(-1_i64)),
            ("baz", ScalarValue::from(2_i32)),
            ("str", ScalarValue::from("1")),
        ])?
        .collect().await?;
    let expected = [
        "+--------+----------+",
        "| signed | unsigned |",
        "+--------+----------+",
        "| -1     | 1        |",
        "+--------+----------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn test_parameter_invalid_types() -> Result<()> {
    let ctx = SessionContext::new();
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
        Some(1),
        Some(2),
        Some(3),
    ])]);
    let batch =
        RecordBatch::try_from_iter(vec![("list", Arc::new(list_array) as ArrayRef)])?;
    ctx.register_batch("test", batch)?;
    let results = ctx
        .sql("SELECT list FROM test WHERE list = $1")
        .await?
        .with_param_values(vec![ScalarValue::from(4_i32)])?
        .collect()
        .await;
    assert_eq!(
        results.unwrap_err().strip_backtrace(),
        "type_coercion\ncaused by\nError during planning: Cannot infer common argument type for comparison operation List(Field { name: \"item\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }) = Int32"
);
    Ok(())
}

#[tokio::test]
async fn test_version_function() {
    let expected_version = format!(
        "Apache DataFusion {}, {} on {}",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::ARCH,
        std::env::consts::OS,
    );

    let ctx = SessionContext::new();
    let results = ctx
        .sql("select version()")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // since width of columns varies between platforms, we can't compare directly
    // so we just check that the version string is present

    // expect a single string column with a single row
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_columns(), 1);
    let version = results[0].column(0).as_string::<i32>();
    assert_eq!(version.len(), 1);

    assert_eq!(version.value(0), expected_version);
}
