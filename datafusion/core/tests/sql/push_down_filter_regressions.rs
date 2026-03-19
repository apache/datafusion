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

use super::*;
use datafusion::{assert_batches_eq, assert_batches_sorted_eq};

#[tokio::test]
async fn window_scalar_subquery_regression() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = r#"
        WITH suppliers AS (
          SELECT *
          FROM (VALUES (1, 10.0), (1, 20.0)) AS t(nation, acctbal)
        )
        SELECT
          ROW_NUMBER() OVER (PARTITION BY nation ORDER BY acctbal DESC) AS rn
        FROM suppliers AS s
        WHERE acctbal > (
          SELECT AVG(acctbal) FROM suppliers
        )
    "#;

    let results = ctx.sql(sql).await?.collect().await?;

    assert_batches_eq!(
        &["+----+", "| rn |", "+----+", "| 1  |", "+----+",],
        &results
    );

    Ok(())
}

#[tokio::test]
async fn aggregate_regr_functions_regression() -> Result<()> {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "c11",
            Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
        ),
        (
            "c12",
            Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0])) as ArrayRef,
        ),
    ])?;
    ctx.register_batch("aggregate_test_100", batch)?;

    let sql = r#"
        select
            regr_slope(c12, c11),
            regr_intercept(c12, c11),
            regr_count(c12, c11),
            regr_r2(c12, c11),
            regr_avgx(c12, c11),
            regr_avgy(c12, c11),
            regr_sxx(c12, c11),
            regr_syy(c12, c11),
            regr_sxy(c12, c11)
        from aggregate_test_100
    "#;

    let rows = execute(&ctx, sql).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 9);
    assert!(rows[0].iter().all(|value| value != "NULL"));

    Ok(())
}

#[tokio::test]
async fn correlated_in_subquery_regression() -> Result<()> {
    let ctx = SessionContext::new();
    let t1 = RecordBatch::try_from_iter(vec![
        ("t1_id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        (
            "t1_name",
            Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
        ),
        ("t1_int", Arc::new(Int32Array::from(vec![1, 0])) as ArrayRef),
    ])?;
    let t2 = RecordBatch::try_from_iter(vec![(
        "t2_id",
        Arc::new(Int32Array::from(vec![12, 99])) as ArrayRef,
    )])?;
    ctx.register_batch("t1", t1)?;
    ctx.register_batch("t2", t2)?;

    let sql = r#"
        select t1.t1_id,
               t1.t1_name,
               t1.t1_int
        from t1
        where t1.t1_id + 12 in (
                                   select t2.t2_id + 1 from t2 where t1.t1_int > 0
                               )
    "#;

    let results = ctx.sql(sql).await?.collect().await?;

    assert_batches_sorted_eq!(
        &[
            "+-------+---------+--------+",
            "| t1_id | t1_name | t1_int |",
            "+-------+---------+--------+",
            "| 1     | alpha   | 1      |",
            "+-------+---------+--------+",
        ],
        &results
    );

    Ok(())
}

#[tokio::test]
async fn natural_join_union_regression() -> Result<()> {
    let ctx = SessionContext::new();
    let t1 = RecordBatch::try_from_iter(vec![
        ("v0", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        (
            "v2",
            Arc::new(Int32Array::from(vec![None, Some(5)])) as ArrayRef,
        ),
    ])?;
    // Keep `v2` only on the left side so the natural join key remains `v0`.
    let t2 = RecordBatch::try_from_iter(vec![(
        "v0",
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
    )])?;
    ctx.register_batch("t1", t1)?;
    ctx.register_batch("t2", t2)?;

    let sql = r#"
        SELECT t1.v2, t1.v0 FROM t2 NATURAL JOIN t1
            UNION ALL
        SELECT t1.v2, t1.v0 FROM t2 NATURAL JOIN t1 WHERE (t1.v2 IS NULL)
    "#;

    let results = ctx.sql(sql).await?.collect().await?;

    assert_batches_sorted_eq!(
        &[
            "+----+----+",
            "| v2 | v0 |",
            "+----+----+",
            "|    | 1  |",
            "|    | 1  |",
            "| 5  | 2  |",
            "+----+----+",
        ],
        &results
    );

    Ok(())
}
