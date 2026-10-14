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

//! A multi-column `NOT IN` over a small subquery must build its hash join on
//! the subquery, so a large outer table runs under a memory limit that only
//! fits the subquery side.
//!
//! The null-aware join of a multi-column `NOT IN` used to stay a `LeftAnti`
//! that builds on the outer side, so it ran out of memory where the scalar
//! `NOT IN`, swapped to a `RightAnti` that builds on the subquery, ran.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use datafusion_common::{Result, assert_contains};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

const OUTER_ROWS: i64 = 200_000;
/// Far below the outer side's build state, far above the subquery's.
const MEMORY_LIMIT: usize = 2 * 1024 * 1024;

/// A table `(a, b)` of `a = v, b = v % 1000` for `v` in `1..=rows`, followed by
/// `extra` rows.
fn table(rows: i64, extra: &[(Option<i64>, Option<i64>)]) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let a = (1..=rows).map(Some).chain(extra.iter().map(|row| row.0));
    let b = (1..=rows)
        .map(|v| Some(v % 1000))
        .chain(extra.iter().map(|row| row.1));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(a.collect::<Int64Array>()),
            Arc::new(b.collect::<Int64Array>()),
        ],
    )
    .unwrap();
    // Several batches, so the outer side streams through the probe.
    let batches = (0..batch.num_rows())
        .step_by(8192)
        .map(|offset| batch.slice(offset, 8192.min(batch.num_rows() - offset)))
        .collect();
    Arc::new(MemTable::try_new(schema, vec![batches]).unwrap())
}

async fn count_not_in(sql: &str, join_reordering: bool) -> Result<i64> {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .set_bool("datafusion.optimizer.join_reordering", join_reordering);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(MEMORY_LIMIT, 1.0)
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_table("big", table(OUTER_ROWS, &[]))?;
    // A NULL-valued subquery row makes `NOT IN` UNKNOWN for every outer row
    // with `b = 7` (and no definite mismatch in `a`).
    ctx.register_table("small", table(100, &[(None, Some(7))]))?;

    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0))
}

#[tokio::test]
async fn multi_column_not_in_builds_on_small_subquery() -> Result<()> {
    let sql = "SELECT count(*) FROM big \
               WHERE (a, b) NOT IN (SELECT a, b FROM small)";

    // Rows 1..=100 match exactly; of the rest, those with `b = 7` are
    // UNKNOWN: 199 of the 200 values `v % 1000 = 7`, all but `v = 7`.
    assert_eq!(count_not_in(sql, true).await?, OUTER_ROWS - 100 - 199);

    // Building on the outer side does not fit, which shows the limit binds.
    let err = count_not_in(sql, false).await.unwrap_err();
    assert_contains!(err.to_string(), "Resources exhausted");
    assert_contains!(err.to_string(), "HashJoinInput");

    Ok(())
}
