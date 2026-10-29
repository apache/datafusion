// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests for field metadata precedence when planning a `UNION`.
//!
//! When the same metadata key appears with different values on different
//! union legs, the logical and physical planners disagree on which value
//! wins:
//!
//! - Logical (`coerce_union_schema_with_schema` in
//!   `datafusion/optimizer/src/analyzer/type_coercion.rs`): merges field
//!   metadata in input order, so the **last** input wins.
//! - Physical (`union_schema` in `datafusion/physical-plan/src/union.rs`):
//!   takes the field of the first nullable input (or input 0) and extends
//!   its metadata with every *other* input's, so when input 0 is `NOT NULL`
//!   and a later input is nullable, **input 0** wins.
//!
//! Operators that verify the planned physical input schema exactly matches
//! the schema derived from the logical plan -- such as the `Aggregate` that
//! `UNION DISTINCT` is planned as, checked by `schema_satisfied_by` in
//! `datafusion/core/src/physical_planner.rs` -- therefore fail planning with
//! "Physical input schema should be the same as the one converted from
//! logical input schema". The same query plans and runs fine without field
//! metadata, which is what the paired tests below pin down.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::*;
use datafusion_common::{Result, assert_batches_sorted_eq, assert_contains};

/// Runs `SELECT a FROM t UNION SELECT b FROM t` where leg 1 (`a`) is
/// `NOT NULL` and leg 2 (`b`) is nullable, optionally attaching the *same*
/// metadata key with a different value to each leg's field.
///
/// This is the minimal shape that makes the two planners' metadata
/// precedence rules disagree: the physical planner picks input 0's field
/// metadata (`id = "1"`) while the logical planner picks the last input's
/// (`id = "2"`).
async fn run_union_distinct(with_metadata: bool) -> Result<Vec<RecordBatch>> {
    let field = |name: &str, nullable: bool, id: Option<&str>| match id {
        Some(id) => Field::new(name, DataType::Int64, nullable)
            .with_metadata(HashMap::from([("id".to_string(), id.to_string())])),
        None => Field::new(name, DataType::Int64, nullable),
    };

    let schema: SchemaRef = Arc::new(Schema::new(if with_metadata {
        vec![field("a", false, Some("1")), field("b", true, Some("2"))]
    } else {
        vec![field("a", false, None), field("b", true, None)]
    }));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![Some(2), None])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    ctx.sql("SELECT a FROM t UNION SELECT b FROM t")
        .await?
        .collect()
        .await
}

/// Without field metadata, both planners agree and `UNION DISTINCT` over
/// mixed-nullability legs plans and runs.
#[tokio::test]
async fn test_union_distinct_metadata() -> Result<()> {
    for with_metdata in [false, true] {
        let batches = run_union_distinct(with_metdata).await?;
        assert_batches_sorted_eq!(
            [
                "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "|   |", "+---+",
            ],
            &batches
        );
    }

    Ok(())
}
