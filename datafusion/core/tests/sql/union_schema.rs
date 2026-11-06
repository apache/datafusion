// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The pre-analysis UNION schema must describe the output types (#27023).

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};

fn assert_union_schema(df: &DataFrame, expected: &[(&str, DataType)]) {
    let fields = df.schema().fields();
    assert_eq!(fields.len(), expected.len());
    for (field, (name, data_type)) in fields.iter().zip(expected) {
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), data_type);
    }
}

#[tokio::test]
async fn union_infers_types_before_analysis() -> Result<()> {
    let ctx = SessionContext::new();
    let cases = [
        (
            r#"arrow_cast(arrow_cast(1, 'Timestamp(Microsecond, None)'), 'Timestamp(Microsecond, Some("UTC"))')"#,
            "CAST(NULL AS TIMESTAMP)",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ),
        (
            "arrow_cast('a', 'Utf8')",
            "arrow_cast('b', 'LargeUtf8')",
            DataType::LargeUtf8,
        ),
        (
            "arrow_cast('a', 'Utf8')",
            "arrow_cast('b', 'Utf8View')",
            DataType::Utf8View,
        ),
        ("CAST(1 AS SMALLINT)", "2.5", DataType::Float64),
        (
            "CASE WHEN false THEN 1 ELSE 2.5 END",
            "NULL",
            DataType::Float64,
        ),
        (
            "CASE WHEN false THEN 1 WHEN true THEN 2.5 END",
            "NULL",
            DataType::Float64,
        ),
    ];
    for (left, right, expected) in cases {
        for (left, right) in [(left, right), (right, left)] {
            for operation in ["ALL", "DISTINCT", "ALL BY NAME", "BY NAME"] {
                let sql = format!(
                    "SELECT x FROM (SELECT {left} AS x UNION {operation} SELECT {right} AS x) u"
                );
                assert_union_schema(&ctx.sql(&sql).await?, &[("x", expected.clone())]);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn union_infers_dictionary_types_before_analysis() -> Result<()> {
    let ctx = SessionContext::new();
    for (sql, expected) in [
        (
            "SELECT NULL AS x UNION ALL SELECT arrow_cast('a', 'Dictionary(Int32, Utf8)') AS x",
            DataType::Utf8,
        ),
        (
            "SELECT CAST(1 AS SMALLINT) AS x UNION ALL SELECT arrow_cast(2, 'Dictionary(Int32, Int32)') AS x",
            DataType::Int32,
        ),
    ] {
        assert_union_schema(&ctx.sql(sql).await?, &[("x", expected)]);
    }
    for sql in [
        "SELECT 1 AS y UNION ALL BY NAME SELECT arrow_cast('a', 'Dictionary(Int32, Utf8)') AS x",
        "SELECT arrow_cast('a', 'Dictionary(Int32, Utf8)') AS x UNION ALL BY NAME SELECT 1 AS y",
    ] {
        let df = ctx.sql(sql).await?.select_columns(&["x", "y"])?;
        assert_union_schema(&df, &[("x", DataType::Utf8), ("y", DataType::Int64)]);
    }
    Ok(())
}

#[tokio::test]
async fn union_dataframe_and_by_name_schemas() -> Result<()> {
    let ctx = SessionContext::new();
    for variant in 0..4 {
        let left = ctx.sql("SELECT CAST(1 AS SMALLINT) AS x").await?;
        let right = ctx.sql("SELECT 2.5 AS x").await?;
        let df = match variant {
            0 => left.union(right)?,
            1 => left.union_distinct(right)?,
            2 => left.union_by_name(right)?,
            _ => left.union_by_name_distinct(right)?,
        };
        assert_union_schema(&df.select_columns(&["x"])?, &[("x", DataType::Float64)]);
    }
    for sql in [
        "SELECT CAST(1 AS SMALLINT) AS x, 1 AS y UNION ALL BY NAME SELECT 2 AS y, 2.5 AS x",
        "SELECT CAST(1 AS SMALLINT) AS x UNION ALL BY NAME SELECT 2.5 AS x, 2 AS y",
    ] {
        assert_union_schema(
            &ctx.sql(sql).await?,
            &[("x", DataType::Float64), ("y", DataType::Int64)],
        );
    }
    let df = ctx
        .sql(
            "SELECT CAST(NULL AS SMALLINT) AS x WHERE false \
         UNION ALL SELECT CAST(NULL AS BIGINT) AS x WHERE false",
        )
        .await?;
    assert_union_schema(&df, &[("x", DataType::Int64)]);
    let df = ctx
        .sql(
            "SELECT max(x) AS x FROM (SELECT 1 AS x UNION ALL SELECT 2.5 AS x \
         UNION ALL SELECT 3 AS x) u",
        )
        .await?;
    assert_union_schema(&df, &[("x", DataType::Float64)]);
    Ok(())
}

#[tokio::test]
async fn union_bound_parameters_keep_inferred_type() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .sql("SELECT CAST($1 AS INT) AS x UNION ALL SELECT 2.5 AS x")
        .await?
        .with_param_values(vec![ScalarValue::Int32(Some(1))])?;
    assert_union_schema(&df, &[("x", DataType::Float64)]);
    Ok(())
}
