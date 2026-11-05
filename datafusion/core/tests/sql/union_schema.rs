// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The pre-analysis UNION schema must describe the output types (#27023).

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Schema, TimeUnit};
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};

async fn assert_union_schema(
    df: DataFrame,
    expected: &[(&str, DataType)],
    rows: usize,
) -> Result<Vec<RecordBatch>> {
    let assert_fields = |schema: &Schema| {
        assert_eq!(schema.fields().len(), expected.len());
        for (field, (name, data_type)) in schema.fields().iter().zip(expected) {
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), data_type);
        }
    };
    assert_fields(df.schema().as_arrow());
    // Empty results must also have the advertised type.
    assert_fields(&df.clone().create_physical_plan().await?.schema());
    let batches = df.collect().await?;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        rows
    );
    for batch in &batches {
        assert_fields(&batch.schema());
    }
    Ok(batches)
}

#[tokio::test]
async fn union_declared_types_match_execution() -> Result<()> {
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
                assert_union_schema(ctx.sql(&sql).await?, &[("x", expected.clone())], 2)
                    .await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn union_dictionary_types_use_actual_inputs() -> Result<()> {
    let ctx = SessionContext::new();
    // Reseeding coercion with the inferred output can select Dictionary again.
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
        assert_union_schema(ctx.sql(sql).await?, &[("x", expected)], 2).await?;
    }
    // A missing left column previously preserved Dictionary. Both orders now
    // match positional UNION and return Utf8.
    for sql in [
        "SELECT 1 AS y UNION ALL BY NAME SELECT arrow_cast('a', 'Dictionary(Int32, Utf8)') AS x",
        "SELECT arrow_cast('a', 'Dictionary(Int32, Utf8)') AS x UNION ALL BY NAME SELECT 1 AS y",
    ] {
        let df = ctx.sql(sql).await?.select_columns(&["x", "y"])?;
        assert_union_schema(df, &[("x", DataType::Utf8), ("y", DataType::Int64)], 2)
            .await?;
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
        assert_union_schema(df.select_columns(&["x"])?, &[("x", DataType::Float64)], 2)
            .await?;
    }
    for sql in [
        "SELECT CAST(1 AS SMALLINT) AS x, 1 AS y UNION ALL BY NAME SELECT 2 AS y, 2.5 AS x",
        "SELECT CAST(1 AS SMALLINT) AS x UNION ALL BY NAME SELECT 2.5 AS x, 2 AS y",
    ] {
        assert_union_schema(
            ctx.sql(sql).await?,
            &[("x", DataType::Float64), ("y", DataType::Int64)],
            2,
        )
        .await?;
    }
    let df = ctx
        .sql(
            "SELECT CAST(NULL AS SMALLINT) AS x WHERE false \
         UNION ALL SELECT CAST(NULL AS BIGINT) AS x WHERE false",
        )
        .await?;
    assert_union_schema(df, &[("x", DataType::Int64)], 0).await?;
    Ok(())
}

#[tokio::test]
async fn union_coercion_preserves_values() -> Result<()> {
    let ctx = SessionContext::new();
    let batches = ctx
        .sql(
            "SELECT arrow_cast(x, 'Int64') AS x FROM (\
         SELECT arrow_cast(1, 'Timestamp(Microsecond, None)') AS x UNION ALL \
         SELECT arrow_cast(1001, 'Timestamp(Nanosecond, None)') AS x) u",
        )
        .await?
        .collect()
        .await?;
    assert_batches_sorted_eq!(
        [
            "+------+", "| x    |", "+------+", "| 1000 |", "| 1001 |", "+------+"
        ],
        &batches
    );
    for (sql, expected) in [
        (
            "SELECT nvl(v1, 0.5) AS x FROM (VALUES (CAST(1 AS INT)), (NULL)) t(v1) \
             UNION ALL SELECT NULL WHERE false",
            vec![0.5, 1.0],
        ),
        (
            "SELECT CASE WHEN false THEN 1 ELSE 2.5 END AS x \
             UNION ALL SELECT CAST(3 AS INT)",
            vec![2.5, 3.0],
        ),
    ] {
        let batches =
            assert_union_schema(ctx.sql(sql).await?, &[("x", DataType::Float64)], 2)
                .await?;
        let mut values = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<Float64Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        values.sort_by(f64::total_cmp);
        assert_eq!(values, expected);
    }
    let df = ctx
        .sql(
            "SELECT max(x) AS x FROM (SELECT 1 AS x UNION ALL SELECT 2.5 AS x \
         UNION ALL SELECT 3 AS x) u",
        )
        .await?;
    assert_union_schema(df, &[("x", DataType::Float64)], 1).await?;
    Ok(())
}

#[tokio::test]
async fn union_bound_parameters_keep_inferred_type() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .sql("SELECT CAST($1 AS INT) AS x UNION ALL SELECT 2.5 AS x")
        .await?
        .with_param_values(vec![ScalarValue::Int32(Some(1))])?;
    let batches = assert_union_schema(df, &[("x", DataType::Float64)], 2).await?;
    assert_batches_sorted_eq!(
        [
            "+-----+", "| x   |", "+-----+", "| 1.0 |", "| 2.5 |", "+-----+"
        ],
        &batches
    );
    Ok(())
}
