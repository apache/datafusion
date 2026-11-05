// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

#[tokio::test]
async fn case_schema_matches_execution() -> Result<()> {
    let ctx = SessionContext::new();
    let cases = [
        // The ELSE branch widens the first THEN branch.
        ("CASE WHEN p THEN 1::INT ELSE 0.5 END", DataType::Float64),
        // A later THEN branch widens the result even without ELSE.
        (
            "CASE WHEN p THEN 1::INT WHEN NOT p THEN 0.5 END",
            DataType::Float64,
        ),
        // The inferred type must propagate through nested expressions.
        (
            "CASE WHEN p THEN 2::INT ELSE (CASE WHEN p THEN 1::INT ELSE 0.5 END) END + 1",
            DataType::Float64,
        ),
        // No concrete result type is available.
        ("CASE WHEN p THEN NULL ELSE NULL END", DataType::Null),
        // Use the same timestamp precision rule as the analyzer.
        (
            "CASE WHEN p THEN arrow_cast(1, 'Timestamp(Microsecond, None)') ELSE arrow_cast(1, 'Timestamp(Nanosecond, None)') END",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
    ];
    for (expr, expected_type) in cases {
        let df = ctx
            .sql(&format!(
                "SELECT {expr} AS x FROM (VALUES (true), (false)) AS t(p)"
            ))
            .await?;
        assert_eq!(df.schema().field(0).data_type(), &expected_type, "{expr}");
        let declared = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        for batch in batches {
            assert_eq!(batch.schema().as_ref(), &declared, "{expr}");
        }
    }
    Ok(())
}
