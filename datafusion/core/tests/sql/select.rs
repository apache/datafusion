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
use tempfile::TempDir;

// Test prepare statement from sql to final result
// This test is equivalent with the test parallel_query_with_filter below but using prepare statement
#[tokio::test]
async fn test_prepare_statement() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = partitioned_csv::create_ctx(&tmp_dir, partition_count).await?;

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
async fn test_named_query_parameters() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = partitioned_csv::create_ctx(&tmp_dir, partition_count).await?;

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

#[tokio::test]
async fn parallel_query_with_filter() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = partitioned_csv::create_ctx(&tmp_dir, partition_count).await?;

    let dataframe = ctx
        .sql("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")
        .await?;
    let results = dataframe.collect().await.unwrap();
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
async fn boolean_literal() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, c3 FROM test WHERE c1 > 2 AND c3 = true", 4)
            .await?;

    let expected = [
        "+----+------+",
        "| c1 | c3   |",
        "+----+------+",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "+----+------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unprojected_filter() {
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config(config);
    let df = ctx.read_table(table_with_sequence(1, 3).unwrap()).unwrap();

    let df = df
        .filter(col("i").gt(lit(2)))
        .unwrap()
        .select(vec![col("i") + col("i")])
        .unwrap();

    let plan = df.clone().into_optimized_plan().unwrap();
    println!("{}", plan.display_indent());

    let results = df.collect().await.unwrap();

    let expected = [
        "+-----------------------+",
        "| ?table?.i + ?table?.i |",
        "+-----------------------+",
        "| 6                     |",
        "+-----------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
}
