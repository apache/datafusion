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
//! Builds on the end-to-end reproducer from #24278 by @alamb.
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
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::expressions::col;
use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::array_agg::array_agg_udaf;

/// Returns the fields of the struct column `b`: a single `colA Boolean`.
///
/// `col_a_nullable` controls whether `colA` is declared nullable — the only
/// difference between the table's declared schema (`true`) and the actual
/// batches (`false`).
fn make_struct_fields(col_a_nullable: bool) -> Fields {
    Fields::from(vec![Field::new("colA", DataType::Boolean, col_a_nullable)])
}

/// Returns the schema `(a UInt32 NOT NULL, b Struct("colA" Boolean) NOT NULL)`
/// with the nested field `b.colA` nullable per `col_a_nullable`.
///
/// See [`make_struct_fields`].
fn make_schema(col_a_nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new(
            "b",
            DataType::Struct(make_struct_fields(col_a_nullable)),
            false,
        ),
    ]))
}

/// Runs a SQL aggregation over a table whose batches are stricter than its
/// declared schema.
///
/// [`Self::run`] registers table `t(a UInt32, b Struct("colA" Boolean))`
/// where the declared schema marks the nested field `colA` as nullable, but
/// the batches carry a stricter, non-nullable `colA`, then runs the query
/// and returns the collected result.
struct AggregateBatchesTest {
    /// Number of rows in the table. `a` is `0..num_rows` (so also the number
    /// of groups for `GROUP BY a`) and `b.colA` alternates `true` / `false`.
    num_rows: u32,
    /// If set, the context uses a [`FairSpillPool`] of this size (and a small
    /// batch size) so the aggregation is forced to spill.
    memory_limit: Option<usize>,
}

impl AggregateBatchesTest {
    fn new() -> Self {
        Self {
            num_rows: 100,
            memory_limit: None,
        }
    }

    fn with_num_rows(mut self, num_rows: u32) -> Self {
        self.num_rows = num_rows;
        self
    }

    fn with_memory_limit(mut self, memory_limit: usize) -> Self {
        self.memory_limit = Some(memory_limit);
        self
    }

    /// Runs `sql` against the table described above and asserts the result
    /// has one output row per group (i.e. [`Self::num_rows`] rows in total).
    async fn run(self, sql: &str) -> Result<()> {
        // The table's declared schema: the nested field `b.colA` is
        // nullable ...
        let declared_schema = make_schema(true);

        // ... while the batches are stricter: `b.colA` is non-nullable.
        // `MemTable::try_new` accepts this combination via
        // `Schema::contains`.
        let batch_struct_fields = make_struct_fields(false);
        let batch = RecordBatch::try_new(
            make_schema(false),
            vec![
                Arc::new(UInt32Array::from_iter_values(0..self.num_rows)),
                Arc::new(StructArray::new(
                    batch_struct_fields,
                    vec![Arc::new(BooleanArray::from_iter(
                        (0..self.num_rows).map(|i| Some(i % 2 == 0)),
                    ))],
                    None,
                )),
            ],
        )?;

        let table = MemTable::try_new(declared_schema, vec![vec![batch]])?;

        let ctx = match self.memory_limit {
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

        let result = ctx.sql(sql).await?.collect().await?;

        let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, self.num_rows as usize);
        Ok(())
    }
}

#[tokio::test]
async fn array_agg_struct_from_stricter_batches() -> Result<()> {
    AggregateBatchesTest::new()
        .run("SELECT a, array_agg(b) FROM t GROUP BY a")
        .await
}

#[tokio::test]
async fn array_agg_distinct_struct_from_stricter_batches() -> Result<()> {
    AggregateBatchesTest::new()
        .run("SELECT a, array_agg(DISTINCT b) FROM t GROUP BY a")
        .await
}

#[tokio::test]
async fn array_agg_struct_from_stricter_batches_with_spilling() -> Result<()> {
    AggregateBatchesTest::new()
        .with_num_rows(10_000)
        .with_memory_limit(4_000_000)
        .run("SELECT a, array_agg(b) FROM t GROUP BY a")
        .await
}

#[tokio::test]
async fn array_agg_distinct_struct_from_stricter_batches_with_spilling() -> Result<()> {
    AggregateBatchesTest::new()
        .with_num_rows(10_000)
        .with_memory_limit(4_000_000)
        .run("SELECT a, array_agg(DISTINCT b) FROM t GROUP BY a")
        .await
}

/// Direct unit test for `AggregateExec` boundary adaptation:
/// Feeds `AggregateExec` directly from a `MemorySourceConfig` whose batches carry
/// a stricter nested struct nullability than the plan schema without going
/// through `MemTable`.
#[tokio::test]
async fn test_aggregate_exec_direct_input_adaptation() -> Result<()> {
    let declared_schema = make_schema(true);
    let batch_struct_fields = make_struct_fields(false);
    let num_rows = 100_u32;
    let stricter_batch = RecordBatch::try_new(
        make_schema(false),
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

    let input_plan: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
        &[vec![stricter_batch]],
        Arc::clone(&declared_schema),
        None,
    )?;

    let grouping_set =
        PhysicalGroupBy::new_single(vec![(col("a", &declared_schema)?, "a".to_string())]);
    let aggregates = vec![Arc::new(
        AggregateExprBuilder::new(array_agg_udaf(), vec![col("b", &declared_schema)?])
            .schema(Arc::clone(&declared_schema))
            .alias("array_agg(b)")
            .build()?,
    )];

    let agg_exec = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        grouping_set,
        aggregates,
        vec![None],
        input_plan,
        Arc::clone(&declared_schema),
    )?);

    let task_ctx = Arc::new(TaskContext::default());
    let results = collect(agg_exec, task_ctx).await?;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_rows as usize);
    Ok(())
}
