// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The optimizer must not evaluate a volatile call at more sites than the
//! input plan does, see `datafusion_optimizer::evaluation_sites`.

use super::*;
use datafusion_optimizer::evaluation_sites::check_no_new_volatile_evaluations;

/// Plan `sql`, optimize it, and check that the optimizer added no evaluation
/// site of a volatile call.
async fn check(sql: &str) -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b DOUBLE) AS VALUES (1, 1.0), (2, 2.0)")
        .await?
        .collect()
        .await?;
    let input = ctx.sql(sql).await?.into_unoptimized_plan();
    let optimized = ctx.state().optimize(&input)?;
    check_no_new_volatile_evaluations(&input, &optimized)
}

#[tokio::test]
async fn filter_on_a_volatile_column() {
    check("SELECT * FROM (SELECT random() AS r, a FROM t) WHERE r < 0.5")
        .await
        .unwrap();
}

#[tokio::test]
async fn volatile_column_used_two_times() {
    check("SELECT r, r + 1 FROM (SELECT random() AS r FROM t)")
        .await
        .unwrap();
}

#[tokio::test]
async fn filter_on_a_volatile_column_above_a_join() {
    check(
        "SELECT * FROM (SELECT a, random() AS r FROM t) x \
         JOIN t y ON x.a = y.a WHERE x.r < 0.5",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn having_on_a_volatile_group_key() {
    check("SELECT r FROM (SELECT random() AS r FROM t) GROUP BY r HAVING r > 0.5")
        .await
        .unwrap();
}

#[tokio::test]
async fn filter_on_a_volatile_column_above_a_union() {
    check(
        "SELECT * FROM (SELECT random() AS r FROM t UNION ALL SELECT b FROM t) \
         WHERE r < 0.5",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn preimage_of_a_volatile_argument() {
    // see https://github.com/apache/datafusion/issues/26910
    assert!(
        check("SELECT a FROM t WHERE floor(random() * 10) = 3")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn coalesce_of_a_volatile_argument() {
    // see https://github.com/apache/datafusion/issues/25477
    assert!(
        check("SELECT coalesce(nullif(random(), 0.5), b) FROM t")
            .await
            .is_err()
    );
}
