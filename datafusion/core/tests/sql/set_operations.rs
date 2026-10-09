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

use datafusion::{assert_batches_sorted_eq, physical_plan::collect};

use super::*;

#[tokio::test]
async fn intersect_all_preserves_duplicate_counts_and_nulls() -> Result<()> {
    let ctx = SessionContext::new();
    let batches = collect(
        ctx.sql(
            "SELECT * FROM VALUES (NULL), (NULL), (1), (1), (2)
             INTERSECT ALL
             SELECT * FROM VALUES (NULL), (1), (1), (1), (3)",
        )
        .await?
        .create_physical_plan()
        .await?,
        ctx.task_ctx(),
    )
    .await?;

    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 1       |",
            "|         |",
            "+---------+",
        ],
        &batches
    );
    Ok(())
}

#[tokio::test]
async fn except_all_preserves_duplicate_counts_and_nulls() -> Result<()> {
    let ctx = SessionContext::new();
    let batches = collect(
        ctx.sql(
            "SELECT * FROM VALUES (NULL), (NULL), (1), (1), (2)
             EXCEPT ALL
             SELECT * FROM VALUES (NULL), (1), (1), (1), (3)",
        )
        .await?
        .create_physical_plan()
        .await?,
        ctx.task_ctx(),
    )
    .await?;

    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 2       |",
            "|         |",
            "+---------+",
        ],
        &batches
    );
    Ok(())
}

#[tokio::test]
async fn set_operation_all_preserves_synthetic_column_names() -> Result<()> {
    let ctx = SessionContext::new();
    let select = "SELECT column1 AS __datafusion_set_operation_left_count,
                         column1 AS __datafusion_set_operation_right_count,
                         column1 AS __datafusion_set_operation_copies";
    for operation in ["INTERSECT ALL", "EXCEPT ALL"] {
        let sql =
            format!("{select} FROM VALUES (1), (1) {operation} {select} FROM VALUES (1)");
        let result = ctx.sql(&sql).await?.collect().await?;
        assert_batches_sorted_eq!(
            [
                "+---------------------------------------+----------------------------------------+-----------------------------------+",
                "| __datafusion_set_operation_left_count | __datafusion_set_operation_right_count | __datafusion_set_operation_copies |",
                "+---------------------------------------+----------------------------------------+-----------------------------------+",
                "| 1                                     | 1                                      | 1                                 |",
                "+---------------------------------------+----------------------------------------+-----------------------------------+",
            ],
            &result
        );
    }
    Ok(())
}

#[tokio::test]
async fn set_operation_all_keeps_left_qualifiers() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (x INT) AS VALUES (1), (1), (2)")
        .await?
        .collect()
        .await?;
    ctx.sql("CREATE TABLE u (x INT) AS VALUES (1), (3)")
        .await?
        .collect()
        .await?;

    let result = ctx
        .sql("SELECT t.x FROM t INTERSECT ALL SELECT u.x FROM u ORDER BY t.x")
        .await?
        .collect()
        .await?;
    assert_batches_sorted_eq!(["+---+", "| x |", "+---+", "| 1 |", "+---+"], &result);

    let t = ctx.table("t").await?;
    let u = ctx.table("u").await?;
    let result = t
        .clone()
        .intersect(u.clone())?
        .select(vec![col("t.x")])?
        .collect()
        .await?;
    assert_batches_sorted_eq!(["+---+", "| x |", "+---+", "| 1 |", "+---+"], &result);

    let result = t.except(u)?.select(vec![col("t.x")])?.collect().await?;
    assert_batches_sorted_eq!(
        ["+---+", "| x |", "+---+", "| 1 |", "| 2 |", "+---+"],
        &result
    );
    Ok(())
}

#[tokio::test]
async fn set_operation_all_with_same_name_from_two_relations() -> Result<()> {
    let ctx = SessionContext::new();
    let side = "SELECT t1.x, t2.x FROM (VALUES (1)) t1(x) CROSS JOIN (VALUES (2)) t2(x)";
    let result = ctx
        .sql(&format!("{side} INTERSECT ALL {side}"))
        .await?
        .collect()
        .await?;
    assert_batches_sorted_eq!(
        [
            "+---+---+",
            "| x | x |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+"
        ],
        &result
    );

    let result = ctx
        .sql(&format!("{side} EXCEPT ALL {side}"))
        .await?
        .collect()
        .await?;
    assert!(result.iter().all(|batch| batch.num_rows() == 0));
    Ok(())
}

#[tokio::test]
async fn set_operation_all_without_count_or_range_is_a_plan_error() -> Result<()> {
    let state = datafusion::execution::SessionStateBuilder::new().build();
    let ctx = SessionContext::new_with_state(state);
    for operation in ["INTERSECT ALL", "EXCEPT ALL"] {
        let err = ctx
            .sql(&format!("SELECT 1 {operation} SELECT 1"))
            .await
            .unwrap_err();
        assert_contains!(
            err.strip_backtrace(),
            "Error during planning: INTERSECT ALL and EXCEPT ALL require the count aggregate function and the range scalar function to be registered"
        );
    }

    let df = ctx.sql("SELECT 1").await?;
    let err = df.clone().intersect(df.clone()).unwrap_err();
    assert_contains!(
        err.strip_backtrace(),
        "DataFrame::intersect requires the count aggregate function"
    );
    let err = df.clone().except(df).unwrap_err();
    assert_contains!(
        err.strip_backtrace(),
        "DataFrame::except requires the count aggregate function"
    );
    Ok(())
}
