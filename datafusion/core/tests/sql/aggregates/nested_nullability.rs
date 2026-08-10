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

//! Regression tests for aggregating batches whose data types are *stricter*
//! than the table's declared schema. See
//! <https://github.com/apache/datafusion/issues/24069>.
//!
//! A `RecordBatch` is a valid instance of a schema that is a superset of its
//! own (see [`Schema::contains`] / `Field::contains`): most commonly the
//! schema declares a (possibly nested) field as nullable while the batch's
//! arrays mark it non-nullable. `MemTable::try_new` accepts such batches via
//! exactly that check, and engines embedding DataFusion (e.g. Comet) feed
//! such batches over FFI. Aggregations must therefore not fail when the
//! runtime arrays are stricter than the planned schema.
//!
//! [`Schema::contains`]: arrow::datatypes::Schema::contains

use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, StructArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

/// Registers table `t(a UInt32, b Struct("colA" Boolean))` where the declared
/// schema marks the nested field `colA` as nullable, but the batches carry a
/// stricter, non-nullable `colA`, then runs `sql` and returns the collected
/// result.
///
/// With `memory_limit`, the context uses a small [`FairSpillPool`] so the
/// aggregation is forced to spill.
async fn run_aggregate_over_stricter_batches(
    sql: &str,
    num_rows: u32,
    memory_limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    // The table's declared schema: `b.colA` is nullable
    let declared_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new(
            "b",
            DataType::Struct(Fields::from(vec![Field::new(
                "colA",
                DataType::Boolean,
                true,
            )])),
            false,
        ),
    ]));

    // The batches are stricter: `b.colA` is non-nullable. `MemTable::try_new`
    // accepts this via `Schema::contains`.
    let batch_struct_fields =
        Fields::from(vec![Field::new("colA", DataType::Boolean, false)]);
    let batch_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Struct(batch_struct_fields.clone()), false),
    ]));
    let batch = RecordBatch::try_new(
        batch_schema,
        vec![
            Arc::new(UInt32Array::from_iter_values(0..num_rows)),
            Arc::new(StructArray::new(
                batch_struct_fields,
                vec![Arc::new(BooleanArray::from_iter(
                    (0..num_rows).map(|i| Some(i % 2 == 0)),
                ))],
                None,
            )),
        ],
    )?;

    let table = MemTable::try_new(declared_schema, vec![vec![batch]])?;

    let ctx = match memory_limit {
        Some(limit) => {
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(limit)))
                .build_arc()?;
            SessionContext::new_with_config_rt(
                SessionConfig::new().with_batch_size(100),
                runtime,
            )
        }
        None => SessionContext::new(),
    };
    ctx.register_table("t", Arc::new(table))?;

    ctx.sql(sql).await?.collect().await
}

#[tokio::test]
async fn array_agg_struct_from_stricter_batches() -> Result<()> {
    let num_rows = 100;
    let result = run_aggregate_over_stricter_batches(
        "SELECT a, array_agg(b) FROM t GROUP BY a",
        num_rows,
        None,
    )
    .await?;
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, num_rows as usize);
    Ok(())
}

#[tokio::test]
async fn array_agg_distinct_struct_from_stricter_batches() -> Result<()> {
    let num_rows = 100;
    let result = run_aggregate_over_stricter_batches(
        "SELECT a, array_agg(DISTINCT b) FROM t GROUP BY a",
        num_rows,
        None,
    )
    .await?;
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, num_rows as usize);
    Ok(())
}

#[tokio::test]
async fn array_agg_struct_from_stricter_batches_with_spilling() -> Result<()> {
    let num_rows = 10_000;
    let result = run_aggregate_over_stricter_batches(
        "SELECT a, array_agg(b) FROM t GROUP BY a",
        num_rows,
        Some(4_000_000),
    )
    .await?;
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, num_rows as usize);
    Ok(())
}

#[tokio::test]
async fn array_agg_distinct_struct_from_stricter_batches_with_spilling() -> Result<()> {
    let num_rows = 10_000;
    let result = run_aggregate_over_stricter_batches(
        "SELECT a, array_agg(DISTINCT b) FROM t GROUP BY a",
        num_rows,
        Some(4_000_000),
    )
    .await?;
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, num_rows as usize);
    Ok(())
}
