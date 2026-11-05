// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

async fn check_schema(df: DataFrame, expected: DataType) -> Result<()> {
    assert_eq!(df.schema().field(0).data_type(), &expected);
    let schema = df.schema().as_arrow().clone();
    let batches = df.collect().await?;
    assert!(!batches.is_empty());
    for batch in batches {
        assert_eq!(batch.schema().as_ref(), &schema);
    }
    Ok(())
}

#[tokio::test]
async fn union_schema_matches_execution() -> Result<()> {
    let ctx = SessionContext::new();
    let cases = [
        (
            "SELECT t FROM (SELECT arrow_cast(arrow_cast(1, 'Timestamp(Microsecond, None)'), 'Timestamp(Microsecond, Some(\"UTC\"))') AS t UNION ALL SELECT CAST(NULL AS TIMESTAMP) AS t) u",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ),
        (
            "SELECT arrow_cast('a', 'Utf8') AS x UNION ALL SELECT arrow_cast('b', 'LargeUtf8') AS x",
            DataType::LargeUtf8,
        ),
        (
            "SELECT arrow_cast('a', 'Utf8') AS x UNION ALL SELECT arrow_cast('b', 'Utf8View') AS x",
            DataType::Utf8View,
        ),
        (
            "SELECT x FROM (SELECT 1::INT AS x UNION ALL SELECT 2::BIGINT AS x) u UNION ALL SELECT 3::SMALLINT AS x",
            DataType::Int64,
        ),
        (
            "SELECT * FROM (SELECT 1::INT AS x) a UNION ALL SELECT * FROM (SELECT 2::BIGINT AS x) b",
            DataType::Int64,
        ),
        (
            "SELECT 1::INT AS x UNION SELECT 2::BIGINT AS x",
            DataType::Int64,
        ),
        (
            "SELECT 1::INT AS x UNION ALL BY NAME SELECT 2::BIGINT AS x",
            DataType::Int64,
        ),
        (
            "SELECT 1::INT AS x UNION ALL BY NAME SELECT 'a' AS y, 2::BIGINT AS x",
            DataType::Int64,
        ),
    ];
    for (sql, expected) in cases {
        check_schema(ctx.sql(sql).await?, expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn union_schema_after_binding() -> Result<()> {
    let ctx = SessionContext::new();
    for sql in [
        "SELECT $1 AS x UNION ALL SELECT 2::BIGINT AS x",
        "SELECT CASE WHEN false THEN 1::INT ELSE $1 END AS x UNION ALL SELECT 2::BIGINT AS x",
        "SELECT x FROM (SELECT $1 AS x UNION ALL SELECT 2::BIGINT AS x) u",
        "SELECT $1 AS x UNION ALL BY NAME SELECT 2::BIGINT AS x",
    ] {
        let df = ctx
            .sql(sql)
            .await?
            .with_param_values(vec![ScalarValue::Float64(Some(0.5))])?;
        check_fractional_values(df).await?;
    }
    Ok(())
}

#[tokio::test]
async fn dataframe_union_schema() -> Result<()> {
    let ctx = SessionContext::new();
    let left = ctx.sql("SELECT 1::INT AS x").await?;
    let right = ctx.sql("SELECT 2::BIGINT AS x").await?;
    check_schema(left.union(right)?, DataType::Int64).await
}

// These expressions previously made early UNION casts truncate the fractional
// result. Both inference and execution must retain the common floating type.
#[tokio::test]
async fn union_preserves_fractional_results() -> Result<()> {
    let ctx = SessionContext::new();
    for sql in [
        "SELECT CASE WHEN false THEN 1::INT ELSE 0.5 END AS x UNION ALL SELECT 2::BIGINT AS x",
        "WITH t(v) AS (VALUES (2::INT), (NULL::INT)) SELECT nvl(v, 0.5) AS x FROM t UNION ALL SELECT NULL AS x WHERE false",
    ] {
        check_fractional_values(ctx.sql(sql).await?).await?;
    }
    Ok(())
}

async fn check_fractional_values(df: DataFrame) -> Result<()> {
    assert_eq!(df.schema().field(0).data_type(), &DataType::Float64);
    let mut values = Vec::new();
    for batch in df.collect().await? {
        // Removing the empty NULL input can tighten nullability.
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(batch.column(0).null_count(), 0);
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
        );
    }
    values.sort_by(f64::total_cmp);
    assert_eq!(values, vec![0.5, 2.0]);
    Ok(())
}
