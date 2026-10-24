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

//! A partial aggregate over input sorted on every grouping key must finish
//! under memory pressure once its completed groups are emitted.
//!
//! On a refused reservation the clustered partial stream emits the completed
//! groups, leaving one active group. The table still holds the capacity it
//! grew for the whole batch, so re-reserving it can fail although the active
//! group alone fits easily.
//!
//! The downstream merge and final aggregate cannot spill and need at least
//! a batch of memory themselves, so a shared limit cannot single out the
//! partial stream. The pool here limits only the partial streams.

use std::fmt::{self, Display};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::*;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_common::{Result, resources_datafusion_err};
use datafusion_execution::memory_pool::{
    MemoryPool, MemoryReservation, UnboundedMemoryPool,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

const ROWS: usize = 8192;
const PARTITIONS: usize = 4;
const PARTIAL_STREAM: &str = "ClusteredPartialAggregateStream";

/// Unbounded pool except that each partial aggregate stream may reserve at
/// most `limit` bytes.
#[derive(Debug)]
struct PartialStreamLimitPool {
    limit: usize,
    used: AtomicUsize,
}

impl Display for PartialStreamLimitPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(limit: {})", self.name(), self.limit)
    }
}

impl MemoryPool for PartialStreamLimitPool {
    fn name(&self) -> &str {
        "partial_stream_limit"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        if reservation.consumer().name().starts_with(PARTIAL_STREAM)
            && reservation.size() + additional > self.limit
        {
            return Err(resources_datafusion_err!(
                "Failed to allocate additional {additional} bytes for {} with {} bytes \
                 already allocated, limit {}",
                reservation.consumer().name(),
                reservation.size(),
                self.limit
            ));
        }
        self.grow(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

/// Each partition holds three batches sorted on every key, and every row is
/// a distinct group.
fn table(num_keys: usize) -> Result<MemTable> {
    let mut fields: Vec<_> = (0..num_keys)
        .map(|i| Field::new(format!("k{i}"), DataType::Int64, false))
        .collect();
    fields.push(Field::new("v", DataType::Int64, false));
    let schema = Arc::new(Schema::new(fields));
    let partitions = (0..PARTITIONS)
        .map(|p| {
            (0..3)
                .map(|b| {
                    let start = ((p * 3 + b) * ROWS) as i64;
                    let ids = start..start + ROWS as i64;
                    let columns = (0..=num_keys)
                        .map(|_| {
                            Arc::new(Int64Array::from_iter_values(ids.clone()))
                                as ArrayRef
                        })
                        .collect();
                    Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let order = (0..num_keys)
        .map(|i| col(format!("k{i}")).sort(true, false))
        .collect();
    Ok(MemTable::try_new(schema, partitions)?.with_sort_order(vec![order]))
}

async fn run(num_keys: usize, pool: Arc<dyn MemoryPool>) -> Result<Vec<RecordBatch>> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()?;
    let config = SessionConfig::new().with_target_partitions(PARTITIONS);
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_table("t", Arc::new(table(num_keys)?))?;
    let keys = (0..num_keys)
        .map(|i| format!("k{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let df = ctx
        .sql(&format!("select {keys}, sum(v) from t group by {keys}"))
        .await?;
    let plan = df.clone().create_physical_plan().await?;
    let plan = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan.contains("mode=Partial") && plan.contains("group_clustering_mode=Full"),
        "{plan}"
    );
    df.collect().await
}

/// One retained table needs about 344 KB for a single key and 131 KB for two
/// keys, while the active group needs a few KB.
#[tokio::test]
async fn sorted_partial_aggregate_completes_under_memory_pressure() -> Result<()> {
    for num_keys in [1, 2] {
        let pool = Arc::new(PartialStreamLimitPool {
            limit: 100 * 1024,
            used: AtomicUsize::new(0),
        });
        let actual = run(num_keys, pool)
            .await
            .map_err(|e| e.context(format!("num_keys = {num_keys}")))?;
        let expected = run(num_keys, Arc::new(UnboundedMemoryPool::default())).await?;
        assert_eq!(
            batches_to_sort_string(&actual),
            batches_to_sort_string(&expected)
        );
    }
    Ok(())
}
