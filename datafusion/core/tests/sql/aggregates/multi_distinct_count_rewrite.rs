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

//! End-to-end SQL tests for the multi-`COUNT(DISTINCT)` logical optimizer rewrite.

use super::*;
use arrow::array::{Float64Array, Int32Array, StringArray};
use datafusion::common::test_util::batches_to_sort_string;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion_catalog::MemTable;

fn session_with_multi_distinct_count_rewrite() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().set_bool(
        "datafusion.optimizer.enable_multi_distinct_count_rewrite",
        true,
    ))
}

#[tokio::test]
async fn multi_count_distinct_matches_expected_with_nulls() -> Result<()> {
    let ctx = session_with_multi_distinct_count_rewrite();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int32, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 1])),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("x")])),
            Arc::new(StringArray::from(vec![None, Some("y"), Some("y")])),
        ],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let sql =
        "SELECT g, COUNT(DISTINCT b) AS cb, COUNT(DISTINCT c) AS cc FROM t GROUP BY g";
    let batches = ctx.sql(sql).await?.collect().await?;
    let out = batches_to_sort_string(&batches);

    assert_eq!(
        out,
        "+---+----+----+\n\
         | g | cb | cc |\n\
         +---+----+----+\n\
         | 1 | 1  | 1  |\n\
         +---+----+----+"
    );
    Ok(())
}

/// `COUNT(*)` + two `COUNT(DISTINCT …)` per group (BI-style); must match non-rewritten semantics.
#[tokio::test]
async fn multi_count_distinct_with_count_star_matches_expected() -> Result<()> {
    let ctx = session_with_multi_distinct_count_rewrite();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 1])),
            Arc::new(Int32Array::from(vec![1, 2, 1])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let sql = "SELECT g, COUNT(*) AS n, COUNT(DISTINCT b) AS db, COUNT(DISTINCT c) AS dc \
               FROM t GROUP BY g";
    let batches = ctx.sql(sql).await?.collect().await?;
    let out = batches_to_sort_string(&batches);

    assert_eq!(
        out,
        "+---+---+----+----+\n\
         | g | n | db | dc |\n\
         +---+---+----+----+\n\
         | 1 | 3 | 2  | 3  |\n\
         +---+---+----+----+"
    );
    Ok(())
}

/// Multiple `GROUP BY` keys: join must align on all keys.
#[tokio::test]
async fn multi_count_distinct_two_group_keys_matches_expected() -> Result<()> {
    let ctx = session_with_multi_distinct_count_rewrite();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g1", DataType::Int32, false),
        Field::new("g2", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 1])),
            Arc::new(Int32Array::from(vec![1, 1, 2])),
            Arc::new(Int32Array::from(vec![1, 1, 3])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        ],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let sql = "SELECT g1, g2, COUNT(DISTINCT b) AS db, COUNT(DISTINCT c) AS dc \
               FROM t GROUP BY g1, g2";
    let batches = ctx.sql(sql).await?.collect().await?;
    let out = batches_to_sort_string(&batches);

    assert_eq!(
        out,
        "+----+----+----+----+\n\
         | g1 | g2 | db | dc |\n\
         +----+----+----+----+\n\
         | 1  | 1  | 1  | 2  |\n\
         | 1  | 2  | 1  | 1  |\n\
         +----+----+----+----+"
    );
    Ok(())
}

/// `COUNT(DISTINCT lower(b))` with `'Abc'` / `'aBC'`: distinct is on the **lowered** value (one bucket).
/// Two `COUNT(DISTINCT …)` so the rewrite applies; semantics match plain aggregation.
#[tokio::test]
async fn multi_count_distinct_lower_matches_expected_case_collapsing() -> Result<()> {
    let ctx = session_with_multi_distinct_count_rewrite();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec!["Abc", "aBC"])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let sql = "SELECT g, COUNT(DISTINCT lower(b)) AS lb, COUNT(DISTINCT c) AS cc \
               FROM t GROUP BY g";
    let batches = ctx.sql(sql).await?.collect().await?;
    let out = batches_to_sort_string(&batches);

    assert_eq!(
        out,
        "+---+----+----+\n\
         | g | lb | cc |\n\
         +---+----+----+\n\
         | 1 | 1  | 2  |\n\
         +---+----+----+"
    );
    Ok(())
}

/// `COUNT(DISTINCT CAST(x AS INT))` with `1.2` and `1.3`: both truncate to `1` → one distinct.
/// Exercises the same “expression in distinct, not raw column” path as `CAST` in the rule.
#[tokio::test]
async fn multi_count_distinct_cast_float_to_int_collapses_nearby_values() -> Result<()> {
    let ctx = session_with_multi_distinct_count_rewrite();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int32, false),
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Float64Array::from(vec![1.2, 1.3])),
            Arc::new(Float64Array::from(vec![10.0, 20.0])),
        ],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let sql = "SELECT g, COUNT(DISTINCT CAST(x AS INT)) AS cx, COUNT(DISTINCT CAST(y AS INT)) AS cy \
               FROM t GROUP BY g";
    let batches = ctx.sql(sql).await?.collect().await?;
    let out = batches_to_sort_string(&batches);

    assert_eq!(
        out,
        "+---+----+----+\n\
         | g | cx | cy |\n\
         +---+----+----+\n\
         | 1 | 1  | 2  |\n\
         +---+----+----+"
    );
    Ok(())
}
