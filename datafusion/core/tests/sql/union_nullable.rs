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

//! Regression tests for `UNION ALL` between inputs where the same column is
//! nullable on one side and `NOT NULL` on the other.
//!
//! DataFusion's analyzer computes the union's output schema by OR-ing the
//! nullability of each input's field (`coerce_union_schema`), so the
//! *declared* output field is nullable whenever any leg is. But before this
//! fix, `UnionExec::execute` handed out each child's `RecordBatch`es
//! completely unchanged, so a leg whose column was already `NOT NULL` (and
//! therefore needed no `CAST`) kept producing batches with a `NOT NULL`
//! field, contradicting the union's own declared (nullable) schema. Any
//! consumer that checks schema equality across batches from the same
//! stream -- e.g. `pyarrow.Table.from_batches` via the Arrow C Stream FFI
//! used by the `datafusion` Python bindings -- then rejects the stream with
//! `ArrowInvalid: Schema at index N was different`, even though every
//! individual `SELECT` runs fine on its own. See
//! <https://github.com/apache/datafusion/issues/15394>.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_common::Result;

/// Builds two single-partition tables that agree on `id`/`status` types but
/// disagree on whether `status` is nullable, then runs `UNION ALL` over them.
async fn union_all_mismatched_nullable(
    left_nullable: bool,
    right_nullable: bool,
) -> Result<DataFrame> {
    let ctx = SessionContext::new();

    let schema_a = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, left_nullable),
    ]));
    let batch_a = RecordBatch::try_new(
        Arc::clone(&schema_a),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["ok", "ok"])),
        ],
    )?;
    ctx.register_batch("table_a", batch_a)?;

    let schema_b = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, right_nullable),
    ]));
    let status_values: Vec<Option<&str>> = if right_nullable {
        vec![Some("done"), None]
    } else {
        vec![Some("done"), Some("also-done")]
    };
    let batch_b = RecordBatch::try_new(
        Arc::clone(&schema_b),
        vec![
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(StringArray::from(status_values)),
        ],
    )?;
    ctx.register_batch("table_b", batch_b)?;

    ctx.sql(
        "SELECT id, status FROM table_a \
         UNION ALL \
         SELECT id, status FROM table_b",
    )
    .await
}

/// The schema DataFusion actually commits to for a query: the logical plan
/// after the `Analyzer` (which includes the `UNION` nullability/type
/// coercion this test targets) and `Optimizer` have run. `DataFrame::schema`
/// alone is not enough here -- it reflects the raw, pre-`Analyzer` plan (see
/// `SessionState::create_logical_plan`), which for a `UNION` still has the
/// first leg's un-coerced type.
async fn analyzed_schema(df: &DataFrame) -> Result<Schema> {
    Ok(df
        .clone()
        .into_optimized_plan()?
        .schema()
        .as_arrow()
        .clone())
}

/// Every `RecordBatch` actually produced by a `UNION ALL` must match the
/// query's analyzed output schema field-for-field -- including
/// nullability -- no matter which leg it came from.
async fn assert_every_batch_matches_declared_schema(df: DataFrame) -> Result<()> {
    let declared_schema = analyzed_schema(&df).await?;

    let batches = df.collect().await?;
    assert!(!batches.is_empty());
    for batch in &batches {
        assert_eq!(
            batch.schema().as_ref(),
            &declared_schema,
            "a UNION ALL leg produced a RecordBatch whose schema disagrees \
             with the union's declared output schema (commonly a dropped \
             nullable flag) -- this is what downstream consumers that check \
             schema equality across batches (e.g. pyarrow) reject with \
             `ArrowInvalid: Schema at index N was different`"
        );
    }
    Ok(())
}

#[tokio::test]
async fn union_all_same_type_left_not_null_right_nullable() -> Result<()> {
    let df = union_all_mismatched_nullable(false, true).await?;
    assert!(
        analyzed_schema(&df)
            .await?
            .field_with_name("status")?
            .is_nullable()
    );
    assert_every_batch_matches_declared_schema(df).await
}

#[tokio::test]
async fn union_all_same_type_left_nullable_right_not_null() -> Result<()> {
    let df = union_all_mismatched_nullable(true, false).await?;
    assert!(
        analyzed_schema(&df)
            .await?
            .field_with_name("status")?
            .is_nullable()
    );
    assert_every_batch_matches_declared_schema(df).await
}

#[tokio::test]
async fn union_all_same_type_both_not_null_stays_not_null() -> Result<()> {
    let df = union_all_mismatched_nullable(false, false).await?;
    let declared_schema = analyzed_schema(&df).await?;
    assert!(
        !declared_schema.field_with_name("status")?.is_nullable(),
        "status should remain NOT NULL when neither leg is nullable"
    );
    assert_every_batch_matches_declared_schema(df).await
}

/// Same bug, but the coercion also has to widen the *type* (Int32 -> Int64)
/// on one leg. The leg that already matched the target type still needed
/// its nullability reconciled at execution time, independent of whichever
/// legs needed a `CAST`.
#[tokio::test]
async fn union_all_widening_cast_also_fixes_nullable() -> Result<()> {
    use arrow::array::Int32Array;

    let ctx = SessionContext::new();

    let schema_a = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int32, false),
    ]));
    let batch_a = RecordBatch::try_new(
        Arc::clone(&schema_a),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )?;
    ctx.register_batch("table_a", batch_a)?;

    let schema_b = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, true),
    ]));
    let batch_b = RecordBatch::try_new(
        Arc::clone(&schema_b),
        vec![
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(Int64Array::from(vec![Some(30), None])),
        ],
    )?;
    ctx.register_batch("table_b", batch_b)?;

    let df = ctx
        .sql(
            "SELECT id, val FROM table_a \
             UNION ALL \
             SELECT id, val FROM table_b",
        )
        .await?;

    let declared_schema = analyzed_schema(&df).await?;
    assert_eq!(
        declared_schema.field_with_name("val")?.data_type(),
        &DataType::Int64
    );
    assert!(declared_schema.field_with_name("val")?.is_nullable());

    let batches = df.collect().await?;
    for batch in &batches {
        assert_eq!(batch.schema().as_ref(), &declared_schema);
    }
    Ok(())
}
